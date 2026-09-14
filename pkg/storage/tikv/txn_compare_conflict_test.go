package tikv

import (
	"context"
	"encoding/binary"
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
)

// Commit a competing physical index mutation after all atomic reads/staging,
// but before prewrite. Do not mutate the shared revision/quota keys: a conflict
// on those would hide a missing compare-only guard mutation.
type compareChangeStorage struct {
	storage.KvStorage
	afterStage func(context.Context) error
}

func (s *compareChangeStorage) UnwrapKvStorage() storage.KvStorage { return s.KvStorage }
func (s *compareChangeStorage) BeginBatchWrite() storage.BatchWrite {
	return &compareChangeBatch{BatchWrite: s.KvStorage.BeginBatchWrite(), owner: s}
}

type compareChangeBatch struct {
	storage.BatchWrite
	owner *compareChangeStorage
}

func (b *compareChangeBatch) Atomic(fn func(context.Context, storage.AtomicBatch) error) {
	b.BatchWrite.Atomic(func(ctx context.Context, txn storage.AtomicBatch) error {
		if err := fn(ctx, txn); err != nil {
			return err
		}
		if hook := b.owner.afterStage; hook != nil {
			b.owner.afterStage = nil
			return hook(ctx)
		}
		return nil
	})
}

func verifyTxnCompareCommitConflict(t *testing.T, ctx context.Context, b backend.Backend, wrapped *compareChangeStorage, ks *coder.Keyspace) {
	t.Helper()
	for _, state := range []string{"present", "missing", "tombstone"} {
		t.Run(state, func(t *testing.T) {
			key := []byte("/integration/compare/" + state)
			guardKey := ks.NewCoder().EncodeRevisionKey(key)
			var initial []byte
			if state != "missing" {
				initial = binary.BigEndian.AppendUint64(nil, 50)
				if state == "tombstone" {
					initial = append(initial, 1)
				}
				seed := wrapped.KvStorage.BeginBatchWrite()
				seed.Put(guardKey, initial, 0)
				require.NoError(t, seed.Commit(ctx))
			}
			guard := backend.TxnGuard{Key: key, Absent: state != "present"}
			if !guard.Absent {
				guard.Revision = 50
			}
			// The same guard without interference must succeed and retain its
			// exact physical state, including a tombstone or a missing index.
			_, _, err := b.TxnApply(ctx, []backend.TxnWriteOp{{Key: append(key, []byte("/control")...), Value: []byte("ok")}}, []backend.TxnGuard{guard})
			require.NoError(t, err)
			got, err := wrapped.KvStorage.Get(ctx, guardKey)
			if initial == nil {
				require.ErrorIs(t, err, storage.ErrKeyNotFound)
			} else {
				require.NoError(t, err)
				require.Equal(t, initial, got)
			}
			before := b.GetCurrentRevision()
			metadata := make(map[string][]byte)
			for _, name := range []string{"revision/committed", "quota/usage"} {
				metadata[name], err = wrapped.KvStorage.Get(ctx, ks.EncodeInternalKey([]byte(name)))
				require.NoError(t, err)
			}
			changed := binary.BigEndian.AppendUint64(nil, 75)
			injected := false
			wrapped.afterStage = func(ctx context.Context) error {
				competitor := wrapped.KvStorage.BeginBatchWrite()
				competitor.Put(guardKey, changed, 0)
				if err := competitor.Commit(ctx); err != nil {
					return err
				}
				injected = true
				return nil
			}
			written := []byte(string(key) + "/rejected")
			_, _, err = b.TxnApply(ctx, []backend.TxnWriteOp{{Key: written, Value: []byte("must-not-publish")}}, []backend.TxnGuard{guard})
			require.True(t, injected, "competitor must commit after the original atomic callback")
			require.ErrorIs(t, err, backend.ErrTxnGuardConflict)
			require.Equal(t, before, b.GetCurrentRevision())
			for _, physical := range [][]byte{ks.NewCoder().EncodeRevisionKey(written), ks.NewCoder().EncodeObjectKey(written, before+1), ks.EncodeEventLogKey(before+1, written)} {
				_, err := wrapped.KvStorage.Get(ctx, physical)
				require.ErrorIs(t, err, storage.ErrKeyNotFound)
			}
			for name, value := range metadata {
				got, err := wrapped.KvStorage.Get(ctx, ks.EncodeInternalKey([]byte(name)))
				require.NoError(t, err)
				require.Equal(t, value, got, "failed transaction must not publish %s", name)
			}
			got, err = wrapped.KvStorage.Get(ctx, guardKey)
			require.NoError(t, err)
			require.Equal(t, changed, got, "rollback must preserve the competitor's committed index")
		})
	}
}

func TestTiKVTxnCompareCommitConflict(t *testing.T) {
	t.Cleanup(tikvconfig.UpdateGlobal(func(c *tikvconfig.Config) { c.Enable1PC = false; c.EnableAsyncCommit = false }))
	client, cluster, pd, err := testutils.NewMockTiKV("", nil)
	require.NoError(t, err)
	testutils.BootstrapWithSingleStore(cluster)
	kv, err := clienttikv.NewKVStore("compare-conflict", clienttikv.NewCodecPDClient(clienttikv.ModeTxn, pd), clienttikv.NewMockSafePointKV(), client)
	require.NoError(t, err)
	wrapped := &compareChangeStorage{KvStorage: NewKvStoreWithStorage([]*clienttikv.KVStore{kv})}
	ks, err := coder.NewKeyspace("compare-conflict")
	require.NoError(t, err)
	b := backend.NewBackend(wrapped, backend.Config{Prefix: "/compare-coord", Keyspace: ks.Name(), EnableEtcdCompatibility: true, QuotaBackendBytes: 2 << 30}, metricmock.NewMinimalMetrics(gomock.NewController(t)))
	t.Cleanup(func() { require.NoError(t, b.(interface{ Close() error }).Close()) })
	b.SetCurrentRevision(100)
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	require.NoError(t, b.EnsureQuotaInitialized(ctx))
	verifyTxnCompareCommitConflict(t, ctx, b, wrapped, ks)
}

func TestRealTiKVTxnCompareCommitConflict(t *testing.T) {
	testRealTiKVBackendScenario(t, "compare-conflict")
}
