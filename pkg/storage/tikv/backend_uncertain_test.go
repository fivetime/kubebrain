package tikv

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	proto "github.com/kubewharf/kubebrain-client/api/v2rpc"
	"github.com/kubewharf/kubebrain/pkg/backend"
	"github.com/kubewharf/kubebrain/pkg/backend/coder"
	"github.com/kubewharf/kubebrain/pkg/metrics"
	metricmock "github.com/kubewharf/kubebrain/pkg/metrics/mock"
	"github.com/kubewharf/kubebrain/pkg/storage"
	"github.com/kubewharf/kubebrain/pkg/storage/memkv"
	"github.com/stretchr/testify/require"
	tikvconfig "github.com/tikv/client-go/v2/config"
)

// Derive BOTH physical ranges from the validated dedicated nonce. Backend
// objects/internal keys use the named keyspace; coordination uses raw Prefix.
func protocolBackendScope(prefix string) (*coder.Keyspace, []coder.KeyRange, error) {
	if _, err := validateProtocolSmokeScope("1", prefix, "1pc"); err != nil {
		return nil, nil, err
	}
	nonce := strings.TrimSuffix(strings.TrimPrefix(prefix, "kubebrain/protocol-smoke/"), "/")
	ks, err := coder.NewKeyspace("protocol-backend-" + nonce)
	if err != nil {
		return nil, nil, err
	}
	return ks, []coder.KeyRange{
		{Start: []byte(prefix), End: []byte(strings.TrimSuffix(prefix, "/") + "0")},
		{Start: ks.ObjectKeyspaceStart(), End: ks.ObjectKeyspaceEnd()},
	}, nil
}

// Bounded collection deliberately refuses unexpectedly large fixtures rather
// than deleting an unbounded namespace. Called only before writers start or
// after backend.Close has joined every worker.
func protocolBackendKeys(ctx context.Context, kv storage.KvStorage, prefix string) ([][]byte, error) {
	_, ranges, err := protocolBackendScope(prefix)
	if err != nil {
		return nil, err
	}
	var keys [][]byte
	for _, r := range ranges {
		iter, err := kv.Iter(ctx, r.Start, r.End, 0, 129)
		if err != nil {
			return nil, err
		}
		for {
			err = iter.Next(ctx)
			if err != nil {
				break
			}
			key := iter.Key()
			if bytes.Compare(key, r.Start) < 0 || bytes.Compare(key, r.End) >= 0 {
				err = fmt.Errorf("fixture iterator escaped its range")
				break
			}
			keys = append(keys, bytes.Clone(key))
			if len(keys) > 128 {
				err = fmt.Errorf("fixture exceeds 128-key cleanup bound")
				break
			}
		}
		closeErr := iter.Close()
		if errors.Is(err, io.EOF) {
			err = nil
		}
		if err = errors.Join(err, closeErr); err != nil {
			return nil, err
		}
	}
	return keys, nil
}

func cleanupProtocolBackend(ctx context.Context, kv storage.KvStorage, prefix string, owner []byte) error {
	if _, _, err := protocolBackendScope(prefix); err != nil {
		return err
	}
	if len(owner) != 32 {
		return fmt.Errorf("invalid owner token")
	}
	claim := []byte(prefix + "owner")
	got, claimErr := kv.Get(ctx, claim)
	if claimErr != nil && !errors.Is(claimErr, storage.ErrKeyNotFound) {
		return claimErr
	}
	if claimErr == nil && !bytes.Equal(got, owner) {
		return fmt.Errorf("ownership changed: refuse backend cleanup")
	}
	keys, err := protocolBackendKeys(ctx, kv, prefix)
	if err != nil {
		return err
	}
	if errors.Is(claimErr, storage.ErrKeyNotFound) {
		if len(keys) != 0 {
			return fmt.Errorf("owner absent but backend fixture is not empty")
		}
		return nil
	}
	batch := kv.BeginBatchWrite()
	batch.CAS(claim, owner, owner, 0)
	for _, key := range keys {
		batch.Del(key)
	}
	if err := batch.Commit(ctx); err != nil {
		return err
	}
	keys, err = protocolBackendKeys(ctx, kv, prefix)
	if err == nil && len(keys) != 0 {
		err = fmt.Errorf("backend cleanup left keys behind")
	}
	return err
}

type protocolResolutionMetrics struct {
	metrics.Metrics
	committed, absent atomic.Int32
}

func (m *protocolResolutionMetrics) EmitCounter(name string, value interface{}, tags ...metrics.T) error {
	positive := false
	switch n := value.(type) {
	case int:
		positive = n > 0
	case int64:
		positive = n > 0
	}
	if positive && name == "txn.uncertain.resolve.committed" {
		m.committed.Add(1)
	}
	if positive && name == "txn.uncertain.resolve.not_committed" {
		m.absent.Add(1)
	}
	return m.Metrics.EmitCounter(name, value, tags...)
}

func protocolNextMutation(t *testing.T, ctx context.Context, watch <-chan []*proto.Event) []*proto.Event {
	t.Helper()
	for {
		select {
		case events, ok := <-watch:
			require.True(t, ok, "watch closed before mutation")
			if backend.IsProgressMarker(events) {
				continue
			}
			require.NotEmpty(t, events)
			for _, event := range events {
				require.NotNil(t, event)
				require.NotNil(t, event.Kv)
			}
			return events
		case <-ctx.Done():
			t.Fatalf("watch mutation deadline: %v", ctx.Err())
		}
	}
}

// Opt-in real adapter AND backend, with no network/server/global-failpoint
// modification. This does not cover process restart, Region split or Raft loss.
func TestRealTiKVBackendResolvesCancelledOnePC(t *testing.T) {
	pd := os.Getenv("KUBEBRAIN_TIKV_PROTOCOL_PD")
	if pd == "" {
		t.Skip("explicit protocol PD endpoint required")
	}
	prefix := os.Getenv("KUBEBRAIN_TIKV_PROTOCOL_PREFIX")
	expected, err := validateProtocolSmokeScope(os.Getenv("KUBEBRAIN_TIKV_PROTOCOL_CLUSTER_ID"), prefix, os.Getenv("KUBEBRAIN_TIKV_PROTOCOL_MODE"))
	require.NoError(t, err)
	require.Equal(t, "1pc", os.Getenv("KUBEBRAIN_TIKV_PROTOCOL_MODE"))
	ks, _, err := protocolBackendScope(prefix)
	require.NoError(t, err)
	ctrl := gomock.NewController(t) // Finish only after backend workers stop.
	t.Cleanup(tikvconfig.UpdateGlobal(func(cfg *tikvconfig.Config) {
		cfg.Enable1PC = true
		cfg.EnableAsyncCommit = false
	}))
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	kv, err := NewKvStorageWithContext(ctx, strings.Split(pd, ","), 1, Security{})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, kv.Close()) })
	require.Equal(t, expected, kv.(storage.ClusterIdentifier).ClusterID())
	// Backend.Close owns kv. A separate real client is required for cleanup
	// AFTER all backend workers stop, preserving every adapter capability.
	cleanupKV, err := NewKvStorageWithContext(ctx, strings.Split(pd, ","), 1, Security{})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, cleanupKV.Close()) })
	require.Equal(t, expected, cleanupKV.(storage.ClusterIdentifier).ClusterID())
	keys, err := protocolBackendKeys(ctx, cleanupKV, prefix)
	require.NoError(t, err)
	require.Empty(t, keys, "both fixture ranges must be empty before claiming")
	owner := make([]byte, 32)
	_, err = rand.Read(owner)
	require.NoError(t, err)
	var closer interface{ Close() error }
	t.Cleanup(func() {
		if closer != nil {
			if err := closer.Close(); err != nil {
				t.Errorf("backend close failed; preserve fixture for recovery: %v", err)
				return
			}
		}
		cleanupCtx, stop := context.WithTimeout(context.Background(), 30*time.Second)
		defer stop()
		require.NoError(t, cleanupProtocolBackend(cleanupCtx, cleanupKV, prefix, owner))
		t.Logf("PROTOCOL_BACKEND_CLEANUP_OK prefix=%s keyspace=%s", prefix, ks.Name())
	})
	t.Logf("PROTOCOL_BACKEND_STARTED cluster=%d prefix=%s keyspace=%s owner_sha256=%x", expected, prefix, ks.Name(), sha256.Sum256(owner))
	claim := cleanupKV.BeginBatchWrite()
	claim.PutIfNotExist([]byte(prefix+"owner"), owner, 0)
	require.NoError(t, claim.Commit(ctx))
	commitCtx, cancelCommit := context.WithCancel(context.WithValue(ctx, protocolCommitMarker{}, true))
	defer cancelCommit()
	client := kv.(*store).getClient()
	loss := &protocolResponseLoss{Client: client.GetTiKVClient(), cancelAfterLoss: cancelCommit}
	client.SetTiKVClient(loss)
	m := &protocolResolutionMetrics{Metrics: metricmock.NewMinimalMetrics(ctrl)}
	b := backend.NewBackend(kv, backend.Config{
		Prefix: prefix + "backend", Keyspace: ks.Name(), Identity: ks.Name(),
		EnableEtcdCompatibility: true, StorageGCLifetime: 0,
	}, m)
	closer = b.(interface{ Close() error })
	b.SetCurrentRevision(100)
	watch, err := b.Watch(ctx, "/integration/onepc/", 101)
	require.NoError(t, err)
	left, right := []byte("/integration/onepc/left"), []byte("/integration/onepc/right")
	result, revision, err := b.TxnApply(commitCtx, []backend.TxnWriteOp{
		{Key: left, Value: []byte("left-value")}, {Key: right, Value: []byte("right-value")},
	}, nil)
	require.ErrorIs(t, err, storage.ErrUncertainResult)
	require.Nil(t, result)
	require.EqualValues(t, 101, revision)
	require.ErrorIs(t, commitCtx.Err(), context.Canceled)
	stats := loss.snapshot()
	require.Equal(t, 1, stats.Drops)
	require.Equal(t, 1, stats.Attempts)
	require.Greater(t, stats.CommitTS, stats.StartTS)
	require.False(t, stats.Changed)
	require.Eventually(t, func() bool {
		return m.committed.Load() == 1 && b.GetCurrentRevision() == revision
	}, 10*time.Second, 10*time.Millisecond, "real durable witness resolution must publish revision")
	require.Zero(t, m.absent.Load())
	events := protocolNextMutation(t, ctx, watch)
	require.Len(t, events, 2)
	seen := make(map[string]string)
	for _, event := range events {
		require.Equal(t, proto.Event_CREATE, event.Type)
		require.Equal(t, revision, event.Revision)
		require.Equal(t, revision, event.Kv.Revision)
		seen[string(event.Kv.Key)] = string(backend.StripInlineValue(event.Kv.Value))
	}
	require.Equal(t, map[string]string{string(left): "left-value", string(right): "right-value"}, seen)
	for key, value := range seen {
		got, err := b.Get(ctx, &proto.GetRequest{Key: []byte(key)})
		require.NoError(t, err)
		require.NotNil(t, got.Kv)
		require.Equal(t, revision, got.Kv.Revision)
		require.Equal(t, value, string(backend.StripInlineValue(got.Kv.Value)))
	}
	_, nextRevision, err := b.TxnApply(ctx, []backend.TxnWriteOp{{Key: left, Value: []byte("next")}}, nil)
	require.NoError(t, err)
	require.Equal(t, revision+1, nextRevision)
	next := protocolNextMutation(t, ctx, watch)
	require.Len(t, next, 1, "uncertain transaction must not replay ahead of next write")
	require.Equal(t, proto.Event_PUT, next[0].Type)
	require.Equal(t, nextRevision, next[0].Revision)
	require.Equal(t, left, next[0].Kv.Key)
	require.Equal(t, []byte("next"), backend.StripInlineValue(next[0].Kv.Value))
	require.EqualValues(t, 1, m.committed.Load())
	require.Zero(t, m.absent.Load())
	t.Logf("PROTOCOL_BACKEND_RESOLVED committed=1 absent=0 revision=%d next=%d attempts=%d drops=%d start_ts=%d commit_ts=%d", revision, nextRevision, stats.Attempts, stats.Drops, stats.StartTS, stats.CommitTS)
}

func TestProtocolBackendCleanupOwnership(t *testing.T) {
	ctx := context.Background()
	prefix := "kubebrain/protocol-smoke/0123456789abcdef0123456789abcdef/"
	ks, _, err := protocolBackendScope(prefix)
	require.NoError(t, err)
	owner := bytes.Repeat([]byte{1}, 32)
	for _, mode := range []string{"owned", "absent", "foreign", "raced", "orphan", "oversized"} {
		t.Run(mode, func(t *testing.T) {
			kv := memkv.NewKvStorage()
			t.Cleanup(func() { require.NoError(t, kv.Close()) })
			batch := kv.BeginBatchWrite()
			if mode != "absent" && mode != "orphan" {
				token := owner
				if mode == "foreign" {
					token = bytes.Repeat([]byte{2}, 32)
				}
				batch.Put([]byte(prefix+"owner"), token, 0)
			}
			if mode != "absent" {
				batch.Put(ks.EncodeInternalKey([]byte("fixture")), []byte("keep"), 0)
				batch.Put([]byte(prefix+"backend/election"), []byte("keep"), 0)
			}
			if mode == "oversized" {
				for i := 0; i < 129; i++ {
					batch.Put(ks.EncodeInternalKey([]byte(fmt.Sprint(i))), []byte("keep"), 0)
				}
			}
			batch.Put([]byte("unrelated-fixture-key"), []byte("keep"), 0)
			require.NoError(t, batch.Commit(ctx))
			cleanupStore := &protocolCleanupRaceStore{KvStorage: kv}
			if mode == "raced" {
				cleanupStore.afterGet = func() error {
					batch := kv.BeginBatchWrite()
					batch.Put([]byte(prefix+"owner"), bytes.Repeat([]byte{2}, 32), 0)
					return batch.Commit(ctx)
				}
			}
			err := cleanupProtocolBackend(ctx, cleanupStore, prefix, owner)
			if mode == "raced" {
				require.ErrorIs(t, err, storage.ErrCASFailed)
			}
			if mode == "owned" || mode == "absent" {
				require.NoError(t, err)
				require.NoError(t, cleanupProtocolBackend(ctx, kv, prefix, owner))
				keys, err := protocolBackendKeys(ctx, kv, prefix)
				require.NoError(t, err)
				require.Empty(t, keys)
			} else {
				require.Error(t, err)
				got, err := kv.Get(ctx, ks.EncodeInternalKey([]byte("fixture")))
				require.NoError(t, err)
				require.Equal(t, []byte("keep"), got)
			}
			got, err := kv.Get(ctx, []byte("unrelated-fixture-key"))
			require.NoError(t, err)
			require.Equal(t, []byte("keep"), got)
		})
	}
	_, _, err = protocolBackendScope("/registry/")
	require.Error(t, err)
	require.Error(t, cleanupProtocolBackend(ctx, nil, "/registry/", owner))
}
