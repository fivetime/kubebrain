package tikv

import (
	"context"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	proto "github.com/kubewharf/kubebrain-client/api/v2rpc"
	"github.com/kubewharf/kubebrain/pkg/backend"
	"github.com/kubewharf/kubebrain/pkg/backend/coder"
	metricmock "github.com/kubewharf/kubebrain/pkg/metrics/mock"
	"github.com/kubewharf/kubebrain/pkg/storage"
	"github.com/stretchr/testify/require"
	tikvconfig "github.com/tikv/client-go/v2/config"
	"github.com/tikv/client-go/v2/testutils"
	clienttikv "github.com/tikv/client-go/v2/tikv"
)

type authGuardChangeStorage struct {
	storage.KvStorage
	beforeCommit func(context.Context) error
}

func (s *authGuardChangeStorage) UnwrapKvStorage() storage.KvStorage { return s.KvStorage }

func (s *authGuardChangeStorage) BeginBatchWrite() storage.BatchWrite {
	return &authGuardChangeBatch{BatchWrite: s.KvStorage.BeginBatchWrite(), owner: s}
}

type authGuardChangeBatch struct {
	storage.BatchWrite
	owner *authGuardChangeStorage
}

func (b *authGuardChangeBatch) Commit(ctx context.Context) error {
	// Append after every backend callback, including the absent-key read/delete.
	b.BatchWrite.Atomic(func(ctx context.Context, _ storage.AtomicBatch) error {
		if hook := b.owner.beforeCommit; hook != nil {
			b.owner.beforeCommit = nil
			return hook(ctx)
		}
		return nil
	})
	return b.BatchWrite.Commit(ctx)
}

// This uses the real client transaction implementation with a mock TiKV RPC
// server. It proves the missing-key delete survives client mutation selection;
// it is not a substitute for the disposable real-TiKV protocol suite.
func TestTiKVAbsentAuthGuardConflictsAfterStaging(t *testing.T) {
	t.Cleanup(tikvconfig.UpdateGlobal(func(c *tikvconfig.Config) { c.Enable1PC = false; c.EnableAsyncCommit = false }))
	client, cluster, pd, err := testutils.NewMockTiKV("", nil)
	require.NoError(t, err)
	testutils.BootstrapWithSingleStore(cluster)
	kv, err := clienttikv.NewKVStore("absent-auth-guard", clienttikv.NewCodecPDClient(clienttikv.ModeTxn, pd), clienttikv.NewMockSafePointKV(), client)
	require.NoError(t, err)
	wrapped := &authGuardChangeStorage{KvStorage: NewKvStoreWithStorage([]*clienttikv.KVStore{kv})}
	ks, err := coder.NewKeyspace("absent-auth-guard")
	require.NoError(t, err)
	b := backend.NewBackend(wrapped, backend.Config{Prefix: "/auth-coord", Keyspace: ks.Name(), EnableEtcdCompatibility: true, QuotaBackendBytes: 2 << 30}, metricmock.NewMinimalMetrics(gomock.NewController(t)))
	t.Cleanup(func() { require.NoError(t, b.(interface{ Close() error }).Close()) })
	b.SetCurrentRevision(100)
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	require.NoError(t, b.EnsureQuotaInitialized(ctx))
	verifyAbsentAuthGuardCommitConflict(t, ctx, b, wrapped, ks)
}

func TestRealTiKVAbsentAuthGuardConflictsAfterStaging(t *testing.T) {
	testRealTiKVBackendScenario(t, "absent-auth-guard")
}

func verifyAbsentAuthGuardCommitConflict(t *testing.T, ctx context.Context, b backend.Backend, wrapped *authGuardChangeStorage, ks *coder.Keyspace) {
	t.Helper()
	guardKey := []byte("auth/config")
	guarded := backend.WithAbsentInternalWriteGuard(ctx, guardKey)
	_, _, err := b.TxnApply(guarded, []backend.TxnWriteOp{{Key: []byte("/control"), Value: []byte("ok")}}, nil)
	require.NoError(t, err)
	_, err = b.InternalGet(ctx, guardKey)
	require.ErrorIs(t, err, storage.ErrKeyNotFound)
	before := b.GetCurrentRevision()
	metadata := make(map[string][]byte)
	for _, name := range []string{"revision/committed", "quota/usage"} {
		metadata[name], err = wrapped.KvStorage.Get(ctx, ks.EncodeInternalKey([]byte(name)))
		require.NoError(t, err)
	}
	injected := false
	wrapped.beforeCommit = func(ctx context.Context) error {
		competitor := wrapped.KvStorage.BeginBatchWrite()
		competitor.Put(ks.EncodeInternalKey(guardKey), []byte("enabled"), 0)
		if err := competitor.Commit(ctx); err != nil {
			return err
		}
		injected = true
		return nil
	}
	_, _, err = b.TxnApply(guarded, []backend.TxnWriteOp{{Key: []byte("/rejected"), Value: []byte("no")}}, nil)
	require.True(t, injected)
	require.ErrorIs(t, err, backend.ErrInternalWriteGuardConflict)
	require.Equal(t, before, b.GetCurrentRevision())
	for name, expected := range metadata {
		value, getErr := wrapped.KvStorage.Get(ctx, ks.EncodeInternalKey([]byte(name)))
		require.NoError(t, getErr)
		require.Equal(t, expected, value, "failed write must not publish %s", name)
	}
	got, err := b.Get(ctx, &proto.GetRequest{Key: []byte("/rejected")})
	require.NoError(t, err)
	require.Nil(t, got.Kv)
	config, err := b.InternalGet(ctx, guardKey)
	require.NoError(t, err)
	require.Equal(t, []byte("enabled"), config)
	t.Log("AUTH_ABSENT_GUARD_CONFLICT_OK competitor_after_staging=true user_and_metadata_unpublished=true")
}
