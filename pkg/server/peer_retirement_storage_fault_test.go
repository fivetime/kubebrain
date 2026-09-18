package server

import (
	"context"
	"sync/atomic"
	"testing"

	"github.com/kubewharf/kubebrain/pkg/storage"
	"github.com/kubewharf/kubebrain/pkg/storage/memkv"
	"github.com/stretchr/testify/require"
)

// No embedded KvStorage and no UnwrapKvStorage: optional capability discovery
// must not bypass the fault. Calls already executing at activation may finish;
// new calls, existing iterator advances, and staged batch commits are rejected.
type retirementFaultStorage struct {
	base     storage.KvStorage
	failed   atomic.Bool
	rejected atomic.Int32
}

func (s *retirementFaultStorage) deny() bool {
	if s.failed.Load() {
		s.rejected.Add(1)
		return true
	}
	return false
}
func (s *retirementFaultStorage) ClusterID() uint64 {
	return s.base.(storage.ClusterIdentifier).ClusterID()
}
func (s *retirementFaultStorage) GetTimestampOracle(ctx context.Context) (uint64, error) {
	if s.deny() {
		return 0, storage.ErrUnavailable
	}
	return s.base.GetTimestampOracle(ctx)
}
func (s *retirementFaultStorage) GetPartitions(ctx context.Context, start, end []byte) ([]storage.Partition, error) {
	if s.deny() {
		return nil, storage.ErrUnavailable
	}
	return s.base.GetPartitions(ctx, start, end)
}
func (s *retirementFaultStorage) Get(ctx context.Context, key []byte) ([]byte, error) {
	if s.deny() {
		return nil, storage.ErrUnavailable
	}
	return s.base.Get(ctx, key)
}
func (s *retirementFaultStorage) Iter(ctx context.Context, start, end []byte, timestamp, limit uint64) (storage.Iter, error) {
	if s.deny() {
		return nil, storage.ErrUnavailable
	}
	it, err := s.base.Iter(ctx, start, end, timestamp, limit)
	if err != nil {
		return nil, err
	}
	return &retirementFaultIterator{Iter: it, owner: s}, nil
}
func (s *retirementFaultStorage) BeginBatchWrite() storage.BatchWrite {
	batch := s.base.BeginBatchWrite()
	// memkv holds its store mutex from BeginBatchWrite through Commit. Reject
	// inside the transaction, not instead of Commit, so abort releases it.
	// Register first so no caller-provided atomic callback runs after denial.
	batch.Atomic(func(context.Context, storage.AtomicBatch) error {
		if s.deny() {
			return storage.ErrUnavailable
		}
		return nil
	})
	return batch
}
func (s *retirementFaultStorage) Close() error     { return s.base.Close() }
func (s *retirementFaultStorage) SupportTTL() bool { return s.base.SupportTTL() }
func (s *retirementFaultStorage) Del(ctx context.Context, key []byte) error {
	if s.deny() {
		return storage.ErrUnavailable
	}
	return s.base.Del(ctx, key)
}
func (s *retirementFaultStorage) DelCurrent(ctx context.Context, it storage.Iter) error {
	if s.deny() {
		return storage.ErrUnavailable
	}
	return s.base.DelCurrent(ctx, it)
}

type retirementFaultIterator struct {
	storage.Iter
	owner *retirementFaultStorage
}

func (i *retirementFaultIterator) Next(ctx context.Context) error {
	if i.owner.deny() {
		return storage.ErrUnavailable
	}
	return i.Iter.Next(ctx)
}

func TestRetirementStorageFaultCoversInterfaceAndStagedWork(t *testing.T) {
	ctx := context.Background()
	raw := retirementStorageFixture{memkv.NewKvStorage(), 42}
	s := &retirementFaultStorage{base: raw}
	defer func() { require.NoError(t, s.Close()) }()
	batch := s.BeginBatchWrite()
	batch.Put([]byte("key"), []byte("old"), 0)
	require.NoError(t, batch.Commit(ctx))
	iterator, err := s.Iter(ctx, []byte("key"), []byte("keyz"), 0, 1)
	require.NoError(t, err)
	defer func() { require.NoError(t, iterator.Close()) }()
	staged := s.BeginBatchWrite()
	staged.Put([]byte("key"), []byte("new"), 0)
	callbackRan := false
	staged.Atomic(func(context.Context, storage.AtomicBatch) error {
		callbackRan = true
		return nil
	})
	s.failed.Store(true)
	_, err = s.GetTimestampOracle(ctx)
	require.ErrorIs(t, err, storage.ErrUnavailable)
	_, err = s.GetPartitions(ctx, []byte("key"), []byte("keyz"))
	require.ErrorIs(t, err, storage.ErrUnavailable)
	_, err = s.Get(ctx, []byte("key"))
	require.ErrorIs(t, err, storage.ErrUnavailable)
	_, err = s.Iter(ctx, []byte("key"), []byte("keyz"), 0, 1)
	require.ErrorIs(t, err, storage.ErrUnavailable)
	require.ErrorIs(t, iterator.Next(ctx), storage.ErrUnavailable)
	require.ErrorIs(t, staged.Commit(ctx), storage.ErrUnavailable)
	require.False(t, callbackRan, "denial must precede caller atomic work")
	require.ErrorIs(t, s.BeginBatchWrite().Commit(ctx), storage.ErrUnavailable)
	require.ErrorIs(t, s.Del(ctx, []byte("key")), storage.ErrUnavailable)
	require.ErrorIs(t, s.DelCurrent(ctx, iterator), storage.ErrUnavailable)
	_, canUnwrap := any(s).(storage.KvStorageUnwrapper)
	require.False(t, canUnwrap)
	require.Equal(t, int32(9), s.rejected.Load())
	s.failed.Store(false)
	value, err := s.Get(ctx, []byte("key"))
	require.NoError(t, err)
	require.Equal(t, []byte("old"), value)
}
