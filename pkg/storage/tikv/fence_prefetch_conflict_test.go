package tikv

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/kubewharf/kubebrain/pkg/backend"
	"github.com/kubewharf/kubebrain/pkg/backend/coder"
	metricmock "github.com/kubewharf/kubebrain/pkg/metrics/mock"
	"github.com/kubewharf/kubebrain/pkg/storage"
	"github.com/stretchr/testify/require"
	tikvconfig "github.com/tikv/client-go/v2/config"
	"github.com/tikv/client-go/v2/testutils"
	clienttikv "github.com/tikv/client-go/v2/tikv"
	"k8s.io/client-go/tools/leaderelection/resourcelock"
)

type fenceChangeAfterPrefetch struct {
	storage.KvStorage
	armed                atomic.Bool
	target               string
	changedKey, oldValue []byte
}

func (s *fenceChangeAfterPrefetch) UnwrapKvStorage() storage.KvStorage { return s.KvStorage }
func (s *fenceChangeAfterPrefetch) BeginBatchWrite() storage.BatchWrite {
	return &fenceChangeBatch{BatchWrite: s.KvStorage.BeginBatchWrite(), owner: s}
}

type fenceChangeBatch struct {
	storage.BatchWrite
	owner *fenceChangeAfterPrefetch
}

func (b *fenceChangeBatch) Atomic(fn func(context.Context, storage.AtomicBatch) error) {
	b.BatchWrite.Atomic(func(ctx context.Context, txn storage.AtomicBatch) error {
		return fn(ctx, fenceChangeTxn{AtomicBatch: txn, owner: b.owner})
	})
}

type fenceChangeTxn struct {
	storage.AtomicBatch
	owner *fenceChangeAfterPrefetch
}

func (t fenceChangeTxn) Prefetch(ctx context.Context, keys [][]byte) error {
	p, ok := t.AtomicBatch.(storage.AtomicBatchPrefetcher)
	if !ok {
		return fmt.Errorf("test requires actual TiKV snapshot prefetch")
	}
	if err := p.Prefetch(ctx, keys); err != nil {
		return err
	}
	if ctx.Value(protocolLatencyMarker{}) != true {
		return nil
	}
	for _, key := range keys {
		if !strings.HasPrefix(string(key), t.owner.target) || !t.owner.armed.CompareAndSwap(true, false) {
			continue
		}
		old, err := t.Get(ctx, key)
		if err != nil {
			return err
		}
		competitor := t.owner.KvStorage.BeginBatchWrite()
		competitor.CAS(key, []byte("changed-by-test"), old, 0)
		// This separate transaction commits AFTER prefetch but BEFORE the
		// original CAS. Its stale cached read must not bypass prewrite conflict.
		if err := competitor.Commit(ctx); err != nil {
			return err
		}
		t.owner.changedKey = bytes.Clone(key)
		t.owner.oldValue = bytes.Clone(old)
		cached, err := t.Get(ctx, key)
		if err != nil {
			return err
		}
		if !bytes.Equal(cached, old) {
			return fmt.Errorf("prefetch did not retain original snapshot")
		}
	}
	return nil
}

func TestPrefetchedProductionFenceRejectsChangedToken(t *testing.T) {
	t.Cleanup(tikvconfig.UpdateGlobal(func(c *tikvconfig.Config) { c.Enable1PC = false; c.EnableAsyncCommit = false }))
	for _, target := range []string{"election-fence", "restoration-fence-shard"} {
		t.Run(target, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
			defer cancel()
			ks, err := coder.NewKeyspace("fence-conflict")
			require.NoError(t, err)
			key := []byte("/probe/watch")
			client, cluster, pd, err := testutils.NewMockTiKV("", nil)
			require.NoError(t, err)
			_, _, first := testutils.BootstrapWithSingleStore(cluster)
			middle, peer := cluster.AllocID(), cluster.AllocID()
			cluster.Split(first, middle, ks.EventLogRangeStart(0), []uint64{peer}, peer)
			last, peer := cluster.AllocID(), cluster.AllocID()
			cluster.Split(middle, last, ks.NewCoder().EncodeRevisionKey(key), []uint64{peer}, peer)
			store, err := clienttikv.NewKVStore("fence-prefetch-conflict", clienttikv.NewCodecPDClient(clienttikv.ModeTxn, pd), clienttikv.NewMockSafePointKV(), client)
			require.NoError(t, err)
			raw := NewKvStoreWithStorage([]*clienttikv.KVStore{store})
			wrapped := &fenceChangeAfterPrefetch{KvStorage: raw, target: "/coord/" + target + "/"}
			b := backend.NewBackend(wrapped, backend.Config{Prefix: "/coord", Keyspace: ks.Name(), Identity: "leader", EnableEtcdCompatibility: true, QuotaBackendBytes: 2 << 30}, metricmock.NewMinimalMetrics(gomock.NewController(t)))
			t.Cleanup(func() { require.NoError(t, b.(interface{ Close() error }).Close()) })
			// Restore only the test-mutated guard before normal backend cleanup,
			// including when a later assertion fails.
			t.Cleanup(func() {
				if wrapped.changedKey == nil {
					return
				}
				cleanupCtx, stop := context.WithTimeout(context.Background(), 5*time.Second)
				defer stop()
				batch := raw.BeginBatchWrite()
				batch.CAS(wrapped.changedKey, wrapped.oldValue, []byte("changed-by-test"), 0)
				require.NoError(t, batch.Commit(cleanupCtx))
			})
			b.SetCurrentRevision(100)
			require.NoError(t, b.EnsureQuotaInitialized(ctx))
			require.NoError(t, b.GetResourceLock().Create(ctx, resourcelock.LeaderElectionRecord{HolderIdentity: "leader", LeaseDurationSeconds: 30}))
			b.SetLeadershipFence(func() (uint64, bool) { return 1, true })
			ctx = backend.WithLeadershipEpoch(ctx, 1)
			metadataBefore := make(map[string][]byte)
			metadataAbsent := make(map[string]bool)
			for _, name := range []string{"revision/committed", "quota/usage"} {
				value, readErr := raw.Get(ctx, ks.EncodeInternalKey([]byte(name)))
				require.True(t, readErr == nil || errors.Is(readErr, storage.ErrKeyNotFound))
				metadataBefore[name] = bytes.Clone(value)
				metadataAbsent[name] = errors.Is(readErr, storage.ErrKeyNotFound)
			}
			wrapped.armed.Store(true)
			_, _, err = b.TxnApply(context.WithValue(ctx, protocolLatencyMarker{}, true), []backend.TxnWriteOp{{Key: key, Value: []byte("must-not-publish")}}, nil)
			want := backend.ErrLeadershipFenced
			if target == "restoration-fence-shard" {
				want = backend.ErrRestorationFenced
			}
			require.ErrorIs(t, err, want)
			require.NotEmpty(t, wrapped.changedKey, "conflict must occur after actual snapshot prefetch")
			for _, physical := range [][]byte{ks.NewCoder().EncodeRevisionKey(key), ks.NewCoder().EncodeObjectKey(key, 101), ks.EncodeEventLogKey(101, key)} {
				_, err := raw.Get(ctx, physical)
				require.ErrorIs(t, err, storage.ErrKeyNotFound)
			}
			require.EqualValues(t, 100, b.GetCurrentRevision(), "failed fenced transaction cannot advance visibility")
			for name, before := range metadataBefore {
				value, readErr := raw.Get(ctx, ks.EncodeInternalKey([]byte(name)))
				if metadataAbsent[name] {
					require.ErrorIs(t, readErr, storage.ErrKeyNotFound)
				} else {
					require.NoError(t, readErr)
					require.Equal(t, before, value, "failed fence cannot publish metadata: %s", name)
				}
			}
		})
	}
}
