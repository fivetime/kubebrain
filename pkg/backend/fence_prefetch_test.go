package backend

import (
	"context"
	"errors"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/kubewharf/kubebrain/pkg/backend/election"
	"github.com/kubewharf/kubebrain/pkg/metrics/mock"
	"github.com/kubewharf/kubebrain/pkg/storage"
	"github.com/kubewharf/kubebrain/pkg/storage/memkv"
	"github.com/stretchr/testify/require"
	"k8s.io/client-go/tools/leaderelection/resourcelock"
)

type failingFencePrefetchStorage struct {
	storage.KvStorage
	err  error
	keys [][]byte
}

func (s *failingFencePrefetchStorage) BeginBatchWrite() storage.BatchWrite {
	return &failingFencePrefetchBatch{BatchWrite: s.KvStorage.BeginBatchWrite(), owner: s}
}

type failingFencePrefetchBatch struct {
	storage.BatchWrite
	owner *failingFencePrefetchStorage
}

func (b *failingFencePrefetchBatch) Atomic(fn func(context.Context, storage.AtomicBatch) error) {
	b.BatchWrite.Atomic(func(ctx context.Context, txn storage.AtomicBatch) error {
		return fn(ctx, failingFencePrefetchTxn{AtomicBatch: txn, owner: b.owner})
	})
}

type failingFencePrefetchTxn struct {
	storage.AtomicBatch
	owner *failingFencePrefetchStorage
}

func (t failingFencePrefetchTxn) Prefetch(_ context.Context, keys [][]byte) error {
	t.owner.keys = keys
	return t.owner.err
}

func TestFencePrefetchFailureDoesNotPublishUserMutation(t *testing.T) {
	want := errors.New("guard prefetch failed")
	kv := &failingFencePrefetchStorage{KvStorage: memkv.NewKvStorage(), err: want}
	b := NewBackend(kv, Config{Prefix: "/prefetch-fence", Identity: "leader"}, mock.NewMinimalMetrics(gomock.NewController(t))).(*backend)
	t.Cleanup(func() { require.NoError(t, b.Close()) })
	ctx := WithLeadershipEpoch(t.Context(), 7)
	require.NoError(t, b.GetResourceLock().Create(ctx, resourcelock.LeaderElectionRecord{HolderIdentity: "leader", LeaseDurationSeconds: 30}))
	b.SetLeadershipFence(func() (uint64, bool) { return 7, true })
	leaderKey, _, ok := b.GetResourceLock().(election.StorageFenceTokenProvider).StorageFenceToken(0)
	require.True(t, ok)
	restoreKey, _, ok := b.GetResourceLock().(election.RestorationFenceTokenProvider).RestorationFenceToken(0)
	require.True(t, ok)
	batch := b.kv.BeginBatchWrite()
	key := []byte("/prefetch-fence/user")
	batch.Put(key, []byte("must-not-publish"), 0)
	require.ErrorIs(t, batch.Commit(ctx), want)
	require.Equal(t, [][]byte{restoreKey, leaderKey}, kv.keys)
	_, err := kv.Get(ctx, key)
	require.ErrorIs(t, err, storage.ErrKeyNotFound)
}
