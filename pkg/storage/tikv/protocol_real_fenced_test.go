package tikv

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	proto "github.com/kubewharf/kubebrain-client/api/v2rpc"
	"github.com/kubewharf/kubebrain/pkg/backend"
	"github.com/kubewharf/kubebrain/pkg/backend/coder"
	"github.com/kubewharf/kubebrain/pkg/backend/election"
	"github.com/kubewharf/kubebrain/pkg/storage"
	"github.com/kubewharf/kubebrain/pkg/storage/memkv"
	"github.com/stretchr/testify/require"
	"k8s.io/client-go/tools/leaderelection/resourcelock"
)

// Runs only in an explicitly opted-in disposable real fixture, with 1PC and
// async commit OFF. This verifies real guarded writes, not multi-Region Raft
// failures or production throughput.
func TestRealTiKVBackendProductionFences(t *testing.T) {
	testRealTiKVBackendScenario(t, "fenced")
}

func TestRealTiKVPrefetchedLeadershipConflict(t *testing.T) {
	testRealTiKVBackendScenario(t, "fenced-election-fence")
}

func TestRealTiKVPrefetchedRestorationConflict(t *testing.T) {
	testRealTiKVBackendScenario(t, "fenced-restoration-fence-shard")
}

func verifyRealProtocolFenceConflict(t *testing.T, ctx context.Context, b backend.Backend, wrapped *fenceChangeAfterPrefetch, ks *coder.Keyspace, target string) {
	t.Helper()
	// Registered after the scenario cleanup: restore our exact modification
	// before backend.Close and the owned fixture's bounded deletion run.
	t.Cleanup(func() {
		if wrapped.changedKey == nil {
			return
		}
		cleanupCtx, stop := context.WithTimeout(context.Background(), 5*time.Second)
		defer stop()
		batch := wrapped.KvStorage.BeginBatchWrite()
		batch.CAS(wrapped.changedKey, wrapped.oldValue, []byte("changed-by-test"), 0)
		require.NoError(t, batch.Commit(cleanupCtx))
	})
	require.NoError(t, b.EnsureQuotaInitialized(ctx))
	require.NoError(t, b.GetResourceLock().Create(ctx, resourcelock.LeaderElectionRecord{HolderIdentity: b.GetResourceLock().Identity(), LeaseDurationSeconds: 30}))
	_, _, ok := b.GetResourceLock().(election.StorageFenceTokenProvider).StorageFenceToken(0)
	require.True(t, ok)
	_, _, ok = b.GetResourceLock().(election.RestorationFenceTokenProvider).RestorationFenceToken(0)
	require.True(t, ok)
	b.SetLeadershipFence(func() (uint64, bool) { return 1, true })
	ctx = backend.WithLeadershipEpoch(ctx, 1)
	metadataBefore := make(map[string][]byte)
	metadataAbsent := make(map[string]bool)
	for _, name := range []string{"revision/committed", "quota/usage"} {
		value, err := wrapped.KvStorage.Get(ctx, ks.EncodeInternalKey([]byte(name)))
		require.True(t, err == nil || errors.Is(err, storage.ErrKeyNotFound))
		metadataBefore[name] = bytes.Clone(value)
		metadataAbsent[name] = errors.Is(err, storage.ErrKeyNotFound)
	}
	key := []byte("/integration/fenced/conflict")
	wrapped.armed.Store(true)
	_, _, err := b.TxnApply(context.WithValue(ctx, protocolLatencyMarker{}, true), []backend.TxnWriteOp{{Key: key, Value: []byte("must-not-publish")}}, nil)
	want := backend.ErrLeadershipFenced
	if target == "restoration-fence-shard" {
		want = backend.ErrRestorationFenced
	}
	require.ErrorIs(t, err, want)
	require.NotEmpty(t, wrapped.changedKey, "competitor must commit after real snapshot prefetch")
	for _, physical := range [][]byte{ks.NewCoder().EncodeRevisionKey(key), ks.NewCoder().EncodeObjectKey(key, 101), ks.EncodeEventLogKey(101, key)} {
		_, err := wrapped.KvStorage.Get(ctx, physical)
		require.ErrorIs(t, err, storage.ErrKeyNotFound)
	}
	require.EqualValues(t, 100, b.GetCurrentRevision())
	for name, before := range metadataBefore {
		value, err := wrapped.KvStorage.Get(ctx, ks.EncodeInternalKey([]byte(name)))
		if metadataAbsent[name] {
			require.ErrorIs(t, err, storage.ErrKeyNotFound)
		} else {
			require.NoError(t, err)
			require.Equal(t, before, value, "failed guard must not publish %s", name)
		}
	}
	t.Logf("PROTOCOL_PREFETCH_CONFLICT_OK target=%s user_and_metadata_unpublished=true", target)
}

func verifyRealProtocolProductionFences(t *testing.T, ctx context.Context, b backend.Backend, rpc *fencedShapeClient) {
	t.Helper()
	require.NoError(t, b.EnsureQuotaInitialized(ctx))
	require.NoError(t, b.GetResourceLock().Create(ctx, resourcelock.LeaderElectionRecord{HolderIdentity: b.GetResourceLock().Identity(), LeaseDurationSeconds: 30}))
	_, _, ok := b.GetResourceLock().(election.StorageFenceTokenProvider).StorageFenceToken(0)
	require.True(t, ok)
	_, _, ok = b.GetResourceLock().(election.RestorationFenceTokenProvider).RestorationFenceToken(0)
	require.True(t, ok)
	b.SetLeadershipFence(func() (uint64, bool) { return 1, true })
	ctx = backend.WithLeadershipEpoch(ctx, 1)
	var observations []storage.BatchCommitObservation
	measured := storage.WithBatchCommitObserver(context.WithValue(ctx, protocolLatencyMarker{}, true), func(o storage.BatchCommitObservation) {
		observations = append(observations, o)
	})
	key := []byte("/integration/fenced/watch")
	for i := 0; i < 3; i++ {
		value := []byte(fmt.Sprintf("value-%d", i))
		_, revision, err := b.TxnApply(measured, []backend.TxnWriteOp{{Key: key, Value: value}}, nil)
		require.NoError(t, err)
		require.EqualValues(t, 101+i, revision)
		got, err := b.Get(ctx, &proto.GetRequest{Key: key})
		require.NoError(t, err)
		require.NotNil(t, got.Kv)
		require.Equal(t, revision, got.Kv.Revision)
		require.Equal(t, value, backend.StripInlineValue(got.Kv.Value))
	}
	require.Len(t, observations, 3)
	for _, o := range observations {
		require.NoError(t, o.Err)
		require.True(t, o.HasWriteDetails)
		require.Positive(t, o.PrewriteRegionGroups)
	}
	// Retries can add attempts; do not equate RPC count with transaction count.
	require.GreaterOrEqual(t, rpc.fenceBatchGets.Load(), int32(3))
	require.Zero(t, rpc.fencePointGets.Load())
	require.GreaterOrEqual(t, rpc.leadership.Load(), int32(3))
	require.GreaterOrEqual(t, rpc.restoration.Load(), int32(3))
	t.Logf("PROTOCOL_PRODUCTION_FENCES_OK writes=3 pair_batch_gets=%d fence_point_gets=%d leadership_mutations=%d restoration_mutations=%d", rpc.fenceBatchGets.Load(), rpc.fencePointGets.Load(), rpc.leadership.Load(), rpc.restoration.Load())
	// Exercise the persisted witness/index/object validation against actual
	// TiKV after repeated writes of one key, not only the committing RPC path.
	// No startup prevalidation was supplied, so promotion must scan all seals.
	require.NoError(t, b.InitializeLeadershipRevision(ctx, 0))
	require.EqualValues(t, 103, b.GetCurrentRevision())
	got, err := b.Get(ctx, &proto.GetRequest{Key: key})
	require.NoError(t, err)
	require.NotNil(t, got.Kv)
	require.EqualValues(t, 103, got.Kv.Revision)
	require.Equal(t, []byte("value-2"), backend.StripInlineValue(got.Kv.Value))
	alarms, err := b.CorruptAlarms(ctx)
	require.NoError(t, err)
	require.Empty(t, alarms)
	t.Log("PROTOCOL_PRODUCTION_WITNESS_VALIDATION_OK repeated_key_writes=3 current_revision=103 corrupt_alarms=0")
}

func TestProtocolFencedCleanupBudget(t *testing.T) {
	ctx := context.Background()
	prefix := "kubebrain/protocol-smoke/0123456789abcdef0123456789abcdef/"
	owner := bytes.Repeat([]byte{1}, 32)
	for _, mode := range []string{"owned", "boundary", "foreign", "raced", "orphan", "oversized", "near-match"} {
		t.Run(mode, func(t *testing.T) {
			kv := memkv.NewKvStorage()
			t.Cleanup(func() { require.NoError(t, kv.Close()) })
			batch := kv.BeginBatchWrite()
			if mode != "orphan" {
				token := owner
				if mode == "foreign" {
					token = bytes.Repeat([]byte{2}, 32)
				}
				batch.Put([]byte(prefix+"owner"), token, 0)
			}
			for i := 0; i < 256; i++ {
				for _, family := range []string{"election-fence", "restoration-fence-shard"} {
					batch.Put([]byte(fmt.Sprintf("%sbackend/%s/%02x", prefix, family, i)), []byte("keep"), 0)
				}
			}
			if mode == "oversized" || mode == "near-match" || mode == "boundary" {
				count := 128
				if mode == "boundary" {
					count = 127
				}
				for i := 0; i < count; i++ {
					key := fmt.Sprintf("%sbackend/ordinary/%d", prefix, i)
					if mode == "near-match" {
						key = fmt.Sprintf("%sbackend/election-fence/%02x-extra", prefix, i)
					}
					batch.Put([]byte(key), []byte("keep"), 0)
				}
			}
			batch.Put([]byte("unrelated-fixture-key"), []byte("keep"), 0)
			require.NoError(t, batch.Commit(ctx))
			_, err := protocolBackendKeys(ctx, kv, prefix)
			require.Error(t, err, "legacy fixture must still reject 512 shards")
			wrapped := &protocolCleanupRaceStore{KvStorage: kv}
			if mode == "raced" {
				wrapped.afterGet = func() error {
					b := kv.BeginBatchWrite()
					b.Put([]byte(prefix+"owner"), bytes.Repeat([]byte{2}, 32), 0)
					return b.Commit(ctx)
				}
			}
			err = cleanupProtocolBackendWithFences(ctx, wrapped, prefix, owner, true)
			if mode == "owned" || mode == "boundary" {
				require.NoError(t, err)
				require.NoError(t, cleanupProtocolBackendWithFences(ctx, kv, prefix, owner, true))
			} else {
				require.Error(t, err)
				if mode == "raced" {
					require.ErrorIs(t, err, storage.ErrCASFailed)
				}
				_, err = kv.Get(ctx, []byte(prefix+"backend/election-fence/00"))
				require.NoError(t, err, "failed cleanup must preserve shards")
			}
			got, err := kv.Get(ctx, []byte("unrelated-fixture-key"))
			require.NoError(t, err)
			require.Equal(t, []byte("keep"), got)
		})
	}
}
