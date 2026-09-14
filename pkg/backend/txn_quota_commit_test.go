package backend

import (
	"context"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/kubewharf/kubebrain/pkg/metrics/mock"
	"github.com/kubewharf/kubebrain/pkg/storage/memkv"
	"github.com/stretchr/testify/require"
)

func TestTxnQuotaCommitReadAvoidsSeparateSnapshot(t *testing.T) {
	s := &writeCostStore{KvStorage: memkv.NewKvStorage()}
	b := NewBackend(s, Config{Prefix: "/quota-commit-cost", EnableEtcdCompatibility: true,
		QuotaBackendBytes: 1024}, mock.NewMinimalMetrics(gomock.NewController(t))).(*backend)
	t.Cleanup(func() { require.NoError(t, b.Close()) })
	b.SetCurrentRevision(100)
	ctx := context.Background()
	require.NoError(t, b.EnsureQuotaInitialized(ctx))
	_, before, err := b.TxnApply(ctx, []TxnWriteOp{{Key: []byte("key"), Value: []byte("old")}}, nil)
	require.NoError(t, err)
	marked := context.WithValue(ctx, writeCostContextKey{}, s)
	_, revision, err := b.TxnApply(marked, []TxnWriteOp{{Key: []byte("key"), Value: []byte("new-value")}}, nil)
	require.NoError(t, err)
	require.Equal(t, before+1, revision)
	require.EqualValues(t, 1, s.batchGets.Load(), "only the index/corruption preparation snapshot remains")
	require.EqualValues(t, 1, s.gets.Load(), "previous object still needs validation")
	require.EqualValues(t, 1, s.commits.Load())
	usage, _, alarm, err := b.QuotaStatus(ctx)
	require.NoError(t, err)
	require.EqualValues(t, len("key")+len("new-value"), usage)
	require.False(t, alarm)
}

func TestTxnQuotaCommitRejectionRollsBackDurableAllocator(t *testing.T) {
	s := memkv.NewKvStorage()
	b := NewBackend(s, Config{Prefix: "/quota-commit-rollback", EnableEtcdCompatibility: true,
		QuotaBackendBytes: 8}, mock.NewMinimalMetrics(gomock.NewController(t))).(*backend)
	t.Cleanup(func() { require.NoError(t, b.Close()) })
	b.SetCurrentRevision(100)
	ctx := context.Background()
	require.NoError(t, b.EnsureQuotaInitialized(ctx))
	_, before, err := b.TxnApply(ctx, []TxnWriteOp{{Key: []byte("k"), Value: []byte("v")}}, nil)
	require.NoError(t, err)
	allocatorKey := b.ks.EncodeInternalKey(durableRevisionKey)
	allocatorBefore, err := s.Get(ctx, allocatorKey)
	require.NoError(t, err)
	_, _, err = b.TxnApply(ctx, []TxnWriteOp{{Key: []byte("k"), Value: []byte("too-large")}}, nil)
	require.ErrorIs(t, err, ErrNoSpace)
	allocatorAfter, err := s.Get(ctx, allocatorKey)
	require.NoError(t, err)
	require.Equal(t, allocatorBefore, allocatorAfter)
	value, revision := liveValue(t, b, ctx, []byte("k"))
	require.Equal(t, "v", value)
	require.Equal(t, before, revision)
	require.Equal(t, before, b.GetCurrentRevision())
	usage, _, alarm, err := b.QuotaStatus(ctx)
	require.NoError(t, err)
	require.EqualValues(t, 2, usage)
	require.True(t, alarm, "overflow alarm must be persisted outside the rolled-back transaction")
	_, revision, err = b.TxnApply(ctx, []TxnWriteOp{{Key: []byte("k"), Delete: true}}, nil)
	require.NoError(t, err, "NOSPACE must still permit reclaiming space")
	require.Equal(t, before+1, revision, "rejected allocation must leave no hole")
}
