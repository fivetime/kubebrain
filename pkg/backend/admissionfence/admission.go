// Package admissionfence coordinates KubeBrain process admission in PD's
// embedded etcd. These keys are deliberately outside TiKV and survive a BR
// whole-cluster transactional restore.
package admissionfence

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"strings"
	"sync/atomic"
	"time"
	"unicode/utf8"

	clientv3 "go.etcd.io/etcd/client/v3"
)

const (
	TokenFormat = "kubebrain.restore-admission-token.v1"
	Open        = "open"
	rootPrefix  = "/kubebrain/native-pitr/admission/v1"
)

var (
	idRE                     = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._:-]{0,255}$`)
	sha256RE                 = regexp.MustCompile(`^[0-9a-f]{64}$`)
	errSessionIdentityActive = errors.New("restore admission process identity is already active")
)

type Token struct {
	Format          string `json:"format"`
	OperationID     string `json:"operation_id"`
	PlanSHA256      string `json:"plan_sha256"`
	TargetClusterID uint64 `json:"target_cluster_id"`
	Keyspace        string `json:"keyspace"`
}

func NewToken(operationID, planSHA string, targetClusterID uint64, keyspace string) (Token, error) {
	t := Token{Format: TokenFormat, OperationID: operationID, PlanSHA256: planSHA, TargetClusterID: targetClusterID, Keyspace: keyspace}
	return t, t.Validate()
}

func (t Token) Validate() error {
	if t.Format != TokenFormat || !idRE.MatchString(t.OperationID) || !sha256RE.MatchString(t.PlanSHA256) || t.TargetClusterID == 0 || (t.Keyspace != "" && !idRE.MatchString(t.Keyspace)) {
		return errors.New("invalid restore admission token")
	}
	return nil
}

func (t Token) Bytes() ([]byte, error) {
	if err := t.Validate(); err != nil {
		return nil, err
	}
	return json.Marshal(t)
}

func (t Token) SHA256() (string, error) {
	b, err := t.Bytes()
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:]), nil
}

func ScopePrefix(keyspace string) string {
	if keyspace == "" {
		return rootPrefix + "/_default"
	}
	return rootPrefix + "/" + keyspace
}
func GateKey(keyspace string) string        { return ScopePrefix(keyspace) + "/gate" }
func SessionsPrefix(keyspace string) string { return ScopePrefix(keyspace) + "/sessions/" }
func SessionKey(keyspace, identity string) string {
	sum := sha256.Sum256([]byte(identity))
	return SessionsPrefix(keyspace) + hex.EncodeToString(sum[:])
}

// Acquire closes process admission only if no leased KubeBrain session exists.
// The range compare and gate mutation execute in one PD-etcd transaction, so a
// concurrent process registration and restore acquisition cannot both succeed.
func Acquire(ctx context.Context, cli *clientv3.Client, keyspace string, token Token) (bool, error) {
	if err := token.Validate(); err != nil {
		return false, err
	}
	if token.Keyspace != keyspace {
		return false, errors.New("restore admission token keyspace mismatch")
	}
	tokenBytes, _ := token.Bytes()
	admission := newResponseAdmission(0)
	noSessions := clientv3.Compare(clientv3.Version(SessionsPrefix(keyspace)), "=", 0).WithPrefix()
	for _, attempt := range []struct {
		cmp     clientv3.Cmp
		resumed bool
	}{
		{clientv3.Compare(clientv3.Value(GateKey(keyspace)), "=", string(tokenBytes)), true},
		{clientv3.Compare(clientv3.Value(GateKey(keyspace)), "=", Open), false},
		{clientv3.Compare(clientv3.Version(GateKey(keyspace)), "=", 0), false},
	} {
		resp, err := cli.Txn(ctx).If(noSessions, attempt.cmp).Then(clientv3.OpPut(GateKey(keyspace), string(tokenBytes))).Commit()
		if err != nil {
			return false, fmt.Errorf("acquire restore admission: %w", err)
		}
		succeeded, err := admission.admitPutTxn(resp, 1, "acquire restore admission")
		if err != nil {
			return false, err
		}
		if succeeded {
			return attempt.resumed, nil
		}
	}
	return false, errors.New("restore admission is held by another operation or active KubeBrain sessions remain")
}

func Verify(ctx context.Context, cli *clientv3.Client, keyspace string, token Token) error {
	if token.Keyspace != keyspace {
		return errors.New("restore admission token keyspace mismatch")
	}
	b, err := token.Bytes()
	if err != nil {
		return err
	}
	admission := newResponseAdmission(0)
	resp, err := cli.Get(ctx, GateKey(keyspace))
	if err != nil {
		return err
	}
	if err := admission.admitExactGet(resp, GateKey(keyspace), string(b), "restore admission ownership"); err != nil {
		return err
	}
	sessions, err := cli.Get(ctx, SessionsPrefix(keyspace), clientv3.WithPrefix(), clientv3.WithLimit(1))
	if err != nil {
		return err
	}
	if err := admission.admitEmptyGet(sessions, "restore admission sessions"); err != nil {
		return err
	}
	return nil
}

func Release(ctx context.Context, cli *clientv3.Client, keyspace string, token Token) error {
	if token.Keyspace != keyspace {
		return errors.New("restore admission token keyspace mismatch")
	}
	b, err := token.Bytes()
	if err != nil {
		return err
	}
	admission := newResponseAdmission(0)
	resp, err := cli.Txn(ctx).If(clientv3.Compare(clientv3.Value(GateKey(keyspace)), "=", string(b))).Then(clientv3.OpPut(GateKey(keyspace), Open)).Commit()
	if err != nil {
		return fmt.Errorf("release restore admission: %w", err)
	}
	succeeded, err := admission.admitPutTxn(resp, 1, "release restore admission")
	if err != nil {
		return err
	}
	if !succeeded {
		return errors.New("restore admission release lost ownership")
	}
	return verifyOpen(ctx, cli, keyspace, admission)
}

func VerifyOpen(ctx context.Context, cli *clientv3.Client, keyspace string) error {
	return verifyOpen(ctx, cli, keyspace, newResponseAdmission(0))
}

func verifyOpen(ctx context.Context, cli *clientv3.Client, keyspace string, admission *responseAdmission) error {
	resp, err := cli.Get(ctx, GateKey(keyspace))
	if err != nil {
		return err
	}
	return admission.admitExactGet(resp, GateKey(keyspace), Open, "open restore admission")
}

// Session is a leased process registration. Fresh becomes false well before
// PD may expire the key, preventing a partitioned process from writing after a
// restore can observe the sessions prefix as empty.
type Session struct {
	cli     *clientv3.Client
	leaseID atomic.Int64
	admit   atomic.Pointer[responseAdmission]
	ttl     time.Duration
	lastAck atomic.Int64
	cancel  context.CancelFunc
	done    chan struct{}
}

func StartSession(ctx context.Context, cli *clientv3.Client, keyspace, identity string, ttl time.Duration) (*Session, error) {
	return startSession(ctx, cli, keyspace, identity, ttl, false)
}

// StartSessionWithIdentityHandoff waits for a previous lease with the same
// process identity to disappear before registering. StatefulSet replacement
// Pods reuse the stable peer identity, so an abruptly killed process can leave
// this key alive until PD expires its lease. The registration transaction
// continues to require an open restore gate and an absent session key; this
// function never steals a live lease or waits through a closed restore gate.
func StartSessionWithIdentityHandoff(ctx context.Context, cli *clientv3.Client, keyspace, identity string, ttl time.Duration) (*Session, error) {
	return startSession(ctx, cli, keyspace, identity, ttl, true)
}

func startSession(ctx context.Context, cli *clientv3.Client, keyspace, identity string, ttl time.Duration, handoff bool) (*Session, error) {
	if identity == "" || len(identity) > 512 || !utf8.ValidString(identity) || strings.ContainsRune(identity, '\x00') || ttl < 3*time.Second {
		return nil, errors.New("invalid restore admission session parameters")
	}
	runCtx, cancel := context.WithCancel(ctx)
	leaseID, keepalive, admission, err := establishSession(runCtx, runCtx, cli, keyspace, identity, ttl)
	if err != nil && handoff && errors.Is(err, errSessionIdentityActive) {
		// A lease can remain visible for up to its full TTL after an abrupt
		// process death. Allow another full TTL for PD expiry scheduling and
		// request latency, but never leave Pod startup waiting indefinitely.
		handoffTimeout := ttl
		if ttl <= time.Duration((1<<63-1)/2) {
			handoffTimeout = 2 * ttl
		}
		handoffCtx, stopHandoff := context.WithTimeout(runCtx, handoffTimeout)
		defer stopHandoff()
		for err != nil && errors.Is(err, errSessionIdentityActive) {
			if waitErr := waitForSessionIdentityRelease(handoffCtx, cli, keyspace, identity); waitErr != nil {
				cancel()
				return nil, fmt.Errorf("wait for previous restore admission process identity: %w", waitErr)
			}
			leaseID, keepalive, admission, err = establishSession(handoffCtx, runCtx, cli, keyspace, identity, ttl)
		}
	}
	if err != nil {
		cancel()
		return nil, err
	}
	s := &Session{cli: cli, ttl: ttl, cancel: cancel, done: make(chan struct{})}
	s.leaseID.Store(int64(leaseID))
	s.admit.Store(admission)
	s.lastAck.Store(time.Now().UnixNano())
	go s.run(runCtx, keyspace, identity, keepalive, admission)
	return s, nil
}

func inspectSessionRegistrationConflict(ctx context.Context, cli *clientv3.Client, keyspace, identity string,
	admission *responseAdmission,
) (bool, error) {
	gate, err := cli.Get(ctx, GateKey(keyspace))
	if err != nil {
		return false, fmt.Errorf("read restore admission gate after registration conflict: %w", err)
	}
	if err := admission.admitExactGet(gate, GateKey(keyspace), Open, "open restore admission after session registration conflict"); err != nil {
		return false, errors.New("restore admission is closed")
	}
	session, err := cli.Get(ctx, SessionKey(keyspace, identity))
	if err != nil {
		return false, fmt.Errorf("read restore admission process identity after registration conflict: %w", err)
	}
	if session.Count == 0 && len(session.Kvs) == 0 && !session.More {
		if err := admission.admitEmptyGet(session, "restore admission process identity after registration conflict"); err != nil {
			return false, err
		}
		return false, nil
	}
	if err := admission.admitExactLeasedGet(session, SessionKey(keyspace, identity), identity,
		"restore admission process identity after registration conflict"); err != nil {
		return false, err
	}
	return true, nil
}

func classifySessionRegistrationConflict(ctx context.Context, cli *clientv3.Client, keyspace, identity string,
	admission *responseAdmission,
) error {
	_, err := inspectSessionRegistrationConflict(ctx, cli, keyspace, identity, admission)
	if err != nil {
		return err
	}
	// Whether the old key is still present or expired between the transaction
	// and the read, retry through the same handoff path. Every retry remains
	// serialized against gate closure by the registration transaction.
	return errSessionIdentityActive
}

func waitForSessionIdentityRelease(ctx context.Context, cli *clientv3.Client, keyspace, identity string) error {
	admission := newResponseAdmission(0)
	for {
		active, err := inspectSessionRegistrationConflict(ctx, cli, keyspace, identity, admission)
		if err != nil {
			return err
		}
		if !active {
			return nil
		}
		timer := time.NewTimer(100 * time.Millisecond)
		select {
		case <-ctx.Done():
			if !timer.Stop() {
				<-timer.C
			}
			return context.Cause(ctx)
		case <-timer.C:
		}
	}
}

func establishSession(requestCtx, keepaliveCtx context.Context, cli *clientv3.Client, keyspace, identity string, ttl time.Duration) (clientv3.LeaseID, <-chan *clientv3.LeaseKeepAliveResponse, *responseAdmission, error) {
	requestedTTL := int64(ttl / time.Second)
	grant, err := cli.Grant(requestCtx, requestedTTL)
	if err != nil {
		return 0, nil, nil, err
	}
	admission := newResponseAdmission(0)
	if err := admission.admitGrant(grant, requestedTTL); err != nil {
		if grant != nil && grant.ID != clientv3.NoLease {
			revokeBestEffort(cli, grant.ID)
		}
		return 0, nil, nil, err
	}
	register := func(gateCmp clientv3.Cmp, initialize bool) (bool, error) {
		ops := []clientv3.Op{clientv3.OpPut(SessionKey(keyspace, identity), identity, clientv3.WithLease(grant.ID))}
		if initialize {
			ops = append([]clientv3.Op{clientv3.OpPut(GateKey(keyspace), Open)}, ops...)
		}
		resp, err := cli.Txn(requestCtx).If(gateCmp, clientv3.Compare(clientv3.Version(SessionKey(keyspace, identity)), "=", 0)).Then(ops...).Commit()
		if err != nil {
			return false, err
		}
		return admission.admitPutTxn(resp, len(ops), "register restore admission session")
	}
	ok, err := register(clientv3.Compare(clientv3.Value(GateKey(keyspace)), "=", Open), false)
	if err == nil && !ok {
		ok, err = register(clientv3.Compare(clientv3.Version(GateKey(keyspace)), "=", 0), true)
		if err == nil && !ok {
			// Another process may have initialized the missing gate between the
			// two transactions above. Re-check the now-open gate before treating
			// the failed initializer as a closed gate or duplicate identity.
			ok, err = register(clientv3.Compare(clientv3.Value(GateKey(keyspace)), "=", Open), false)
		}
	}
	if err != nil || !ok {
		conflictErr := err
		if conflictErr == nil {
			conflictErr = classifySessionRegistrationConflict(requestCtx, cli, keyspace, identity, admission)
		}
		revokeBestEffort(cli, grant.ID)
		if conflictErr != nil {
			return 0, nil, nil, fmt.Errorf("register restore admission session: %w", conflictErr)
		}
		return 0, nil, nil, errors.New("restore admission session registration conflict was not classified")
	}
	keepalive, err := cli.KeepAlive(keepaliveCtx, grant.ID)
	if err != nil {
		revokeBestEffort(cli, grant.ID)
		return 0, nil, nil, err
	}
	return grant.ID, keepalive, admission, nil
}

func revokeBestEffort(cli *clientv3.Client, leaseID clientv3.LeaseID) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	_, _ = cli.Revoke(ctx, leaseID)
}

func (s *Session) run(ctx context.Context, keyspace, identity string, keepalive <-chan *clientv3.LeaseKeepAliveResponse, admission *responseAdmission) {
	defer close(s.done)
	for {
	keepaliveLoop:
		for response := range keepalive {
			leaseID := clientv3.LeaseID(s.leaseID.Load())
			if err := admission.admitKeepAlive(response, leaseID); err != nil {
				s.lastAck.Store(0)
				revokeBestEffort(s.cli, leaseID)
				break keepaliveLoop
			}
			s.lastAck.Store(time.Now().UnixNano())
		}
		// The stream ending is an uncertainty boundary. Stop admitting writes
		// immediately; a replacement registration will mark the session fresh.
		s.lastAck.Store(0)
		for {
			if ctx.Err() != nil {
				return
			}
			leaseID, next, nextAdmission, err := establishSession(ctx, ctx, s.cli, keyspace, identity, s.ttl)
			if err == nil {
				s.leaseID.Store(int64(leaseID))
				s.lastAck.Store(time.Now().UnixNano())
				keepalive = next
				admission = nextAdmission
				s.admit.Store(nextAdmission)
				break
			}
			timer := time.NewTimer(500 * time.Millisecond)
			select {
			case <-ctx.Done():
				if !timer.Stop() {
					<-timer.C
				}
				return
			case <-timer.C:
			}
		}
	}
}

func (s *Session) Fresh() bool {
	last := s.lastAck.Load()
	return last > 0 && time.Since(time.Unix(0, last)) < s.ttl/2
}

func (s *Session) Close(ctx context.Context) error {
	s.cancel()
	select {
	case <-s.done:
	case <-ctx.Done():
		return ctx.Err()
	}
	response, err := s.cli.Revoke(ctx, clientv3.LeaseID(s.leaseID.Load()))
	if err != nil {
		return err
	}
	admission := s.admit.Load()
	if admission == nil {
		return errors.New("restore admission session has no response identity")
	}
	return admission.admitRevoke(response, "close restore admission session")
}
