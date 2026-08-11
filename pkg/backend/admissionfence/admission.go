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
	idRE     = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._:-]{0,255}$`)
	sha256RE = regexp.MustCompile(`^[0-9a-f]{64}$`)
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
		if resp.Succeeded {
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
	resp, err := cli.Get(ctx, GateKey(keyspace))
	if err != nil {
		return err
	}
	if len(resp.Kvs) != 1 || string(resp.Kvs[0].Value) != string(b) {
		return errors.New("restore admission ownership lost")
	}
	sessions, err := cli.Get(ctx, SessionsPrefix(keyspace), clientv3.WithPrefix(), clientv3.WithLimit(1))
	if err != nil {
		return err
	}
	if sessions.Count != 0 {
		return errors.New("restore admission has active KubeBrain sessions")
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
	resp, err := cli.Txn(ctx).If(clientv3.Compare(clientv3.Value(GateKey(keyspace)), "=", string(b))).Then(clientv3.OpPut(GateKey(keyspace), Open)).Commit()
	if err != nil {
		return fmt.Errorf("release restore admission: %w", err)
	}
	if !resp.Succeeded {
		return errors.New("restore admission release lost ownership")
	}
	return VerifyOpen(ctx, cli, keyspace)
}

func VerifyOpen(ctx context.Context, cli *clientv3.Client, keyspace string) error {
	resp, err := cli.Get(ctx, GateKey(keyspace))
	if err != nil {
		return err
	}
	if len(resp.Kvs) != 1 || string(resp.Kvs[0].Value) != Open {
		return errors.New("restore admission is not open")
	}
	return nil
}

// Session is a leased process registration. Fresh becomes false well before
// PD may expire the key, preventing a partitioned process from writing after a
// restore can observe the sessions prefix as empty.
type Session struct {
	cli     *clientv3.Client
	leaseID atomic.Int64
	ttl     time.Duration
	lastAck atomic.Int64
	cancel  context.CancelFunc
	done    chan struct{}
}

func StartSession(ctx context.Context, cli *clientv3.Client, keyspace, identity string, ttl time.Duration) (*Session, error) {
	if identity == "" || len(identity) > 512 || !utf8.ValidString(identity) || strings.ContainsRune(identity, '\x00') || ttl < 3*time.Second {
		return nil, errors.New("invalid restore admission session parameters")
	}
	runCtx, cancel := context.WithCancel(ctx)
	leaseID, keepalive, err := establishSession(runCtx, cli, keyspace, identity, ttl)
	if err != nil {
		cancel()
		return nil, err
	}
	s := &Session{cli: cli, ttl: ttl, cancel: cancel, done: make(chan struct{})}
	s.leaseID.Store(int64(leaseID))
	s.lastAck.Store(time.Now().UnixNano())
	go s.run(runCtx, keyspace, identity, keepalive)
	return s, nil
}

func establishSession(ctx context.Context, cli *clientv3.Client, keyspace, identity string, ttl time.Duration) (clientv3.LeaseID, <-chan *clientv3.LeaseKeepAliveResponse, error) {
	grant, err := cli.Grant(ctx, int64(ttl/time.Second))
	if err != nil {
		return 0, nil, err
	}
	register := func(gateCmp clientv3.Cmp, initialize bool) (bool, error) {
		ops := []clientv3.Op{clientv3.OpPut(SessionKey(keyspace, identity), identity, clientv3.WithLease(grant.ID))}
		if initialize {
			ops = append([]clientv3.Op{clientv3.OpPut(GateKey(keyspace), Open)}, ops...)
		}
		resp, err := cli.Txn(ctx).If(gateCmp, clientv3.Compare(clientv3.Version(SessionKey(keyspace, identity)), "=", 0)).Then(ops...).Commit()
		return err == nil && resp.Succeeded, err
	}
	ok, err := register(clientv3.Compare(clientv3.Value(GateKey(keyspace)), "=", Open), false)
	if err == nil && !ok {
		ok, err = register(clientv3.Compare(clientv3.Version(GateKey(keyspace)), "=", 0), true)
	}
	if err != nil || !ok {
		revokeBestEffort(cli, grant.ID)
		if err != nil {
			return 0, nil, fmt.Errorf("register restore admission session: %w", err)
		}
		return 0, nil, errors.New("restore admission is closed or this process identity is already active")
	}
	keepalive, err := cli.KeepAlive(ctx, grant.ID)
	if err != nil {
		revokeBestEffort(cli, grant.ID)
		return 0, nil, err
	}
	return grant.ID, keepalive, nil
}

func revokeBestEffort(cli *clientv3.Client, leaseID clientv3.LeaseID) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	_, _ = cli.Revoke(ctx, leaseID)
}

func (s *Session) run(ctx context.Context, keyspace, identity string, keepalive <-chan *clientv3.LeaseKeepAliveResponse) {
	defer close(s.done)
	for {
		for response := range keepalive {
			if response != nil {
				s.lastAck.Store(time.Now().UnixNano())
			}
		}
		// The stream ending is an uncertainty boundary. Stop admitting writes
		// immediately; a replacement registration will mark the session fresh.
		s.lastAck.Store(0)
		for {
			if ctx.Err() != nil {
				return
			}
			leaseID, next, err := establishSession(ctx, s.cli, keyspace, identity, s.ttl)
			if err == nil {
				s.leaseID.Store(int64(leaseID))
				s.lastAck.Store(time.Now().UnixNano())
				keepalive = next
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
	_, err := s.cli.Revoke(ctx, clientv3.LeaseID(s.leaseID.Load()))
	return err
}
