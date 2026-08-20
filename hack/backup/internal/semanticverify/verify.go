package semanticverify

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/kubewharf/kubebrain/hack/backup/internal/backupfile"
	"github.com/kubewharf/kubebrain/hack/backup/internal/record"
	"github.com/kubewharf/kubebrain/hack/backup/internal/targetverify"
	"github.com/kubewharf/kubebrain/pkg/backend"
	"github.com/kubewharf/kubebrain/pkg/backend/coder"
	storagetikv "github.com/kubewharf/kubebrain/pkg/storage/tikv"
	"go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"
)

type Observation struct {
	HistoricalHeaderRevision int64
	CurrentHeaderRevision    int64
	ProbePutRevision         int64
	ProbeDeleteRevision      int64
	HistoricalExact          bool
	CurrentExact             bool
	LeaseIdentityExact       bool
	WatchProbeSucceeded      bool
	ProbeKey                 []byte
	ProbeValue               []byte
	ProbeLeaseID             int64
}

// VerifyTargetProbeHistory binds the etcd endpoint observation to the exact
// target PD/TiKV cluster by reading the retained probe PUT version directly.
func VerifyTargetProbeHistory(ctx context.Context, addrs []string, security storagetikv.Security, keyspace string, observation Observation) (retErr error) {
	if len(observation.ProbeKey) == 0 || len(observation.ProbeValue) == 0 || observation.ProbeLeaseID == 0 || observation.ProbePutRevision <= 0 {
		return errors.New("semantic watch probe returned incomplete target-binding evidence")
	}
	storage, err := storagetikv.NewKvStorageWithContext(ctx, addrs, 1, security)
	if err != nil {
		return fmt.Errorf("connect target TiKV for probe history: %w", err)
	}
	defer func() { retErr = errors.Join(retErr, storage.Close()) }()
	ks, err := coder.NewKeyspace(keyspace)
	if err != nil {
		return err
	}
	stored, err := storage.Get(ctx, ks.NewCoder().EncodeObjectKey(observation.ProbeKey, uint64(observation.ProbePutRevision)))
	if err != nil {
		return fmt.Errorf("read target probe history: %w", err)
	}
	meta, value, inlined, err := backend.DecodeInlineValueChecked(stored)
	if err != nil || !inlined || !bytes.Equal(value, observation.ProbeValue) || meta.CreateRevision != uint64(observation.ProbePutRevision) || meta.Version != 1 || meta.Lease != observation.ProbeLeaseID {
		return errors.New("endpoint probe history does not match the plan-bound target TiKV cluster")
	}
	return nil
}

type expectedKV struct {
	key, value                  []byte
	createRevision, modRevision int64
	version, lease              int64
}

func Verify(ctx context.Context, cli *clientv3.Client, verified *backupfile.Verified, probePrefix string) (Observation, error) {
	if err := ValidateProbePrefix(probePrefix); err != nil {
		return Observation{}, err
	}
	status := verified.Status()
	expected, leases, leaseKeys, err := loadWitness(verified)
	if err != nil {
		return Observation{}, err
	}
	for id, lease := range leases {
		if lease.GrantedTTL <= 0 {
			return Observation{}, fmt.Errorf("witness lease %d lacks granted_ttl", id)
		}
	}
	responseAdmission := &targetverify.ResponseAdmission{}
	historical, historicalRevision, err := fetchRange(ctx, cli, status.Prefix, status.Revision, responseAdmission)
	if err != nil {
		return Observation{}, fmt.Errorf("read witness revision %d: %w", status.Revision, err)
	}
	if err := compareKVs(expected, historical); err != nil {
		return Observation{}, fmt.Errorf("historical witness mismatch: %w", err)
	}
	current, currentRevision, err := fetchRange(ctx, cli, status.Prefix, 0, responseAdmission)
	if err != nil {
		return Observation{}, fmt.Errorf("read current range: %w", err)
	}
	if historicalRevision < status.Revision || currentRevision < status.Revision {
		return Observation{}, fmt.Errorf("restored revision moved backwards: witness=%d historical=%d current=%d", status.Revision, historicalRevision, currentRevision)
	}
	if err := compareKVs(expected, current); err != nil {
		return Observation{}, fmt.Errorf("current witness mismatch: %w", err)
	}
	if err := verifyLeases(ctx, cli, leases, leaseKeys, currentRevision, responseAdmission); err != nil {
		return Observation{}, err
	}
	putRevision, deleteRevision, probeKey, probeValue, probeLeaseID, err := runWatchProbe(ctx, cli, probePrefix, responseAdmission)
	if err != nil {
		return Observation{}, err
	}
	return Observation{HistoricalHeaderRevision: historicalRevision, CurrentHeaderRevision: currentRevision, ProbePutRevision: putRevision, ProbeDeleteRevision: deleteRevision, HistoricalExact: true, CurrentExact: true, LeaseIdentityExact: true, WatchProbeSucceeded: true, ProbeKey: probeKey, ProbeValue: probeValue, ProbeLeaseID: probeLeaseID}, nil
}

func loadWitness(verified *backupfile.Verified) (map[string]expectedKV, map[int64]record.Lease, map[int64][]string, error) {
	expected := make(map[string]expectedKV)
	leases := make(map[int64]record.Lease)
	leaseKeys := make(map[int64][]string)
	if err := verified.Leases(func(lease record.Lease) error { leases[lease.ID] = lease; return nil }); err != nil {
		return nil, nil, nil, err
	}
	if err := verified.Records(func(rec record.Record) error {
		key, err := base64.StdEncoding.DecodeString(rec.Key)
		if err != nil {
			return err
		}
		value, err := base64.StdEncoding.DecodeString(rec.Value)
		if err != nil {
			return err
		}
		expected[string(key)] = expectedKV{key: key, value: value, createRevision: rec.CreateRevision, modRevision: rec.ModRevision, version: rec.Version, lease: rec.Lease}
		if rec.Lease != 0 {
			leaseKeys[rec.Lease] = append(leaseKeys[rec.Lease], string(key))
		}
		return nil
	}); err != nil {
		return nil, nil, nil, err
	}
	for id := range leaseKeys {
		sort.Strings(leaseKeys[id])
	}
	return expected, leases, leaseKeys, nil
}

func fetchRange(ctx context.Context, cli *clientv3.Client, prefix string, revision int64, admission *targetverify.ResponseAdmission) ([]*mvccpb.KeyValue, int64, error) {
	return targetverify.FetchPrefix(ctx, cli, prefix, revision, admission)
}

func compareKVs(expected map[string]expectedKV, actual []*mvccpb.KeyValue) error {
	if len(expected) != len(actual) {
		return fmt.Errorf("record count: expected %d, got %d", len(expected), len(actual))
	}
	seen := make(map[string]struct{}, len(actual))
	for _, got := range actual {
		want, exists := expected[string(got.Key)]
		if !exists {
			return fmt.Errorf("unexpected key %q", got.Key)
		}
		if _, duplicate := seen[string(got.Key)]; duplicate {
			return fmt.Errorf("duplicate key %q", got.Key)
		}
		seen[string(got.Key)] = struct{}{}
		if !bytes.Equal(want.value, got.Value) || want.createRevision != got.CreateRevision || want.modRevision != got.ModRevision || want.version != got.Version || want.lease != got.Lease {
			return fmt.Errorf("metadata/value mismatch for key %q", got.Key)
		}
	}
	return nil
}

func verifyLeases(ctx context.Context, cli *clientv3.Client, leases map[int64]record.Lease, expectedKeys map[int64][]string, minRevision int64, admission *targetverify.ResponseAdmission) error {
	for id, expected := range leases {
		response, err := cli.TimeToLive(ctx, clientv3.LeaseID(id), clientv3.WithAttachedKeys())
		if err != nil {
			return fmt.Errorf("read physical lease %d: %w", id, err)
		}
		if err := targetverify.ValidateLease(response, id, expected.GrantedTTL, minRevision, expectedKeys[id]); err != nil {
			return err
		}
		if err := admission.AdmitLeaseTTL(response); err != nil {
			return err
		}
	}
	return nil
}

func runWatchProbe(ctx context.Context, cli *clientv3.Client, prefix string, admission *targetverify.ResponseAdmission) (putRevision int64, deleteRevision int64, keyBytes []byte, valueBytes []byte, leaseID int64, retErr error) {
	token, err := randomHex(24)
	if err != nil {
		return 0, 0, nil, nil, 0, err
	}
	key, value := strings.TrimRight(prefix, "/")+"/"+token, "restore-semantic-probe:"+token
	watch := cli.Watch(ctx, key, clientv3.WithCreatedNotify())
	created, ok := <-watch
	if !ok {
		return 0, 0, nil, nil, 0, errors.New("watch did not acknowledge creation")
	}
	if err := targetverify.ValidateProbeWatchCreated(created); err != nil {
		return 0, 0, nil, nil, 0, err
	}
	if err := admission.AdmitWatch(created); err != nil {
		return 0, 0, nil, nil, 0, err
	}
	lease, err := cli.Grant(ctx, 60)
	if err != nil {
		return 0, 0, nil, nil, 0, fmt.Errorf("grant probe lease: %w", err)
	}
	cleanupLeaseID := clientv3.NoLease
	if lease != nil {
		cleanupLeaseID = lease.ID
	}
	revoked := false
	defer func() {
		if !revoked && cleanupLeaseID != clientv3.NoLease {
			retErr = errors.Join(retErr, revokeProbeLease(func(cleanup context.Context) error {
				response, err := cli.Revoke(cleanup, cleanupLeaseID)
				if err != nil {
					return err
				}
				if err := targetverify.ValidateProbeRevoke(response, 0); err != nil {
					return err
				}
				return admission.AdmitRevoke(response)
			}))
		}
	}()
	if err := admission.AdmitGrant(lease); err != nil {
		return 0, 0, nil, nil, 0, err
	}
	probeLeaseID, err := targetverify.ValidateProbeGrant(lease, 60)
	if err != nil {
		return 0, 0, nil, nil, 0, err
	}
	put, err := cli.Txn(ctx).If(clientv3.Compare(clientv3.CreateRevision(key), "=", 0)).Then(clientv3.OpPut(key, value, clientv3.WithLease(probeLeaseID))).Commit()
	if err != nil {
		return 0, 0, nil, nil, 0, err
	}
	if err := admission.AdmitTxn(put, true); err != nil {
		return 0, 0, nil, nil, 0, err
	}
	putRevision, err = targetverify.ValidateProbePutTxn(put)
	if err != nil {
		return 0, 0, nil, nil, 0, err
	}
	if err := expectWatchEvent(watch, mvccpb.PUT, []byte(key), []byte(value), probeLeaseID, putRevision, admission); err != nil {
		return 0, 0, nil, nil, 0, err
	}
	read, err := cli.Get(ctx, key)
	if err != nil {
		return 0, 0, nil, nil, 0, err
	}
	readRevision, err := targetverify.ValidateProbeGet(read, []byte(key), []byte(value), probeLeaseID, putRevision)
	if err != nil {
		return 0, 0, nil, nil, 0, err
	}
	if err := admission.AdmitRange(read); err != nil {
		return 0, 0, nil, nil, 0, err
	}
	deleted, err := cli.Txn(ctx).If(clientv3.Compare(clientv3.Value(key), "=", value)).Then(clientv3.OpDelete(key)).Commit()
	if err != nil {
		return 0, 0, nil, nil, 0, err
	}
	if err := admission.AdmitTxn(deleted, true); err != nil {
		return 0, 0, nil, nil, 0, err
	}
	deleteRevision, err = targetverify.ValidateProbeDeleteTxn(deleted, readRevision)
	if err != nil {
		return 0, 0, nil, nil, 0, err
	}
	if err := expectWatchEvent(watch, mvccpb.DELETE, []byte(key), nil, clientv3.NoLease, deleteRevision, admission); err != nil {
		return 0, 0, nil, nil, 0, err
	}
	revoke, err := cli.Revoke(ctx, probeLeaseID)
	if err != nil {
		return 0, 0, nil, nil, 0, err
	}
	if err := targetverify.ValidateProbeRevoke(revoke, deleteRevision); err != nil {
		return 0, 0, nil, nil, 0, err
	}
	if err := admission.AdmitRevoke(revoke); err != nil {
		return 0, 0, nil, nil, 0, err
	}
	revoked = true
	return putRevision, deleteRevision, []byte(key), []byte(value), int64(probeLeaseID), nil
}

func revokeProbeLease(revoke func(context.Context) error) error {
	cleanup, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := revoke(cleanup); err != nil {
		return fmt.Errorf("revoke probe lease during cleanup: %w", err)
	}
	return nil
}

func ValidateProbePrefix(prefix string) error {
	if prefix == "" || !strings.HasPrefix(prefix, "/") || strings.IndexFunc(prefix, func(r rune) bool { return r < 0x20 || r == 0x7f }) >= 0 {
		return errors.New("probe prefix must be absolute and contain no control characters")
	}
	trimmed := strings.TrimRight(prefix, "/")
	if trimmed == "" || trimmed == "/registry" || strings.HasPrefix(trimmed+"/", "/registry/") {
		return errors.New("probe prefix must not target Kubernetes /registry data")
	}
	return nil
}

func randomHex(n int) (string, error) {
	b := make([]byte, n)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	return hex.EncodeToString(b), nil
}
func expectWatchEvent(watch clientv3.WatchChan, typ mvccpb.Event_EventType, key, value []byte, leaseID clientv3.LeaseID, revision int64, admission *targetverify.ResponseAdmission) error {
	for response := range watch {
		if err := targetverify.ValidateProbeWatchEvent(response, typ, key, value, leaseID, revision); err != nil {
			return err
		}
		if err := admission.AdmitWatch(response); err != nil {
			return err
		}
		return nil
	}
	return errors.New("watch closed before expected event")
}
