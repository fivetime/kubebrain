package backend

import (
	"bytes"
	"context"
	"encoding/binary"
	"hash/crc32"
	"sync"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"

	proto "github.com/kubewharf/kubebrain-client/api/v2rpc"
	metricsmock "github.com/kubewharf/kubebrain/pkg/metrics/mock"
	"github.com/kubewharf/kubebrain/pkg/storage"
	"github.com/kubewharf/kubebrain/pkg/storage/memkv"
)

type hashKVPartitionTestStorage struct {
	storage.KvStorage
	mu             sync.Mutex
	partitions     []storage.Partition
	partitionStart []byte
	partitionEnd   []byte
	gatedStarts    map[string]struct{}
	started        chan struct{}
	release        chan struct{}
	partitionCalls int
	iterTimestamps []uint64
	iterBatchSizes []int
}

func (s *hashKVPartitionTestStorage) GetPartitions(
	ctx context.Context, start, end []byte,
) ([]storage.Partition, error) {
	s.mu.Lock()
	s.partitionCalls++
	partitions := append([]storage.Partition(nil), s.partitions...)
	configured := bytes.Equal(start, s.partitionStart) && bytes.Equal(end, s.partitionEnd)
	s.mu.Unlock()
	if configured && len(partitions) != 0 {
		return partitions, nil
	}
	return s.KvStorage.GetPartitions(ctx, start, end)
}

func (s *hashKVPartitionTestStorage) Iter(
	ctx context.Context, start, end []byte, timestamp, limit uint64,
) (storage.Iter, error) {
	it, err := s.KvStorage.Iter(ctx, start, end, timestamp, limit)
	if err != nil {
		return nil, err
	}
	s.mu.Lock()
	s.iterTimestamps = append(s.iterTimestamps, timestamp)
	batchSize, _ := storage.ScanBatchSizeFromContext(ctx)
	s.iterBatchSizes = append(s.iterBatchSizes, batchSize)
	_, gated := s.gatedStarts[string(start)]
	started, release := s.started, s.release
	s.mu.Unlock()
	if !gated {
		return it, nil
	}
	return &hashKVGatedIter{Iter: it, started: started, release: release}, nil
}

type hashKVGatedIter struct {
	storage.Iter
	started chan<- struct{}
	release <-chan struct{}
	once    sync.Once
}

func (i *hashKVGatedIter) Next(ctx context.Context) error {
	var waitErr error
	i.once.Do(func() {
		select {
		case i.started <- struct{}{}:
		case <-ctx.Done():
			waitErr = ctx.Err()
			return
		}
		select {
		case <-i.release:
		case <-ctx.Done():
			waitErr = ctx.Err()
		}
	})
	if waitErr != nil {
		return waitErr
	}
	return i.Iter.Next(ctx)
}

func TestHashKVTracksDataAndPreservesHistoricalRevision(t *testing.T) {
	b, ctx := newTxnApplyBackend(t)
	key := []byte(prefix + "/hash/key")

	created, err := b.Create(ctx, &proto.CreateRequest{Key: key, Value: []byte("v1")})
	require.NoError(t, err)
	require.True(t, created.Succeeded)
	rev1 := created.Header.Revision
	waitCommitted(t, b, rev1)

	hash1, err := b.HashKV(ctx, int64(rev1))
	require.NoError(t, err)
	require.Equal(t, int64(rev1), hash1.HashRevision)
	require.Equal(t, int64(rev1), hash1.CurrentRevision)
	require.Equal(t, int64(-1), hash1.CompactRevision)

	updated, err := b.Update(ctx, &proto.UpdateRequest{Kv: &proto.KeyValue{
		Key: key, Value: []byte("v2"), Revision: rev1,
	}})
	require.NoError(t, err)
	require.True(t, updated.Succeeded)
	rev2 := updated.Header.Revision
	waitCommitted(t, b, rev2)

	hash2, err := b.HashKV(ctx, 0)
	require.NoError(t, err)
	require.Equal(t, int64(rev2), hash2.HashRevision)
	require.Equal(t, int64(rev2), hash2.CurrentRevision)
	require.NotEqual(t, hash1.Hash, hash2.Hash, "a retained MVCC version must change the hash")

	historical, err := b.HashKV(ctx, int64(rev1))
	require.NoError(t, err)
	require.Equal(t, hash1.Hash, historical.Hash, "later writes must not alter an earlier revision hash")
	require.Equal(t, int64(rev2), historical.CurrentRevision)

	negative, err := b.HashKV(ctx, -1)
	require.NoError(t, err)
	require.Equal(t, int64(-1), negative.HashRevision)
	require.Equal(t, int64(rev2), negative.CurrentRevision)
	require.Equal(t, crc32.Checksum([]byte("key"), hashKVTable), negative.Hash)
	require.NotEqual(t, hash2.Hash, negative.Hash, "negative revision must not select current data")
}

func TestHashKVDollarExtensionIsStableAcrossPhysicalCompaction(t *testing.T) {
	for name, storageType := range map[string]storageType{
		"memory": memKvStorage,
		"tikv":   tiKvStorage,
	} {
		t.Run(name, func(t *testing.T) {
			s, closeSuite := newTestSuites(t, storageType)
			defer closeSuite()
			b := s.backend.(*backend)

			shortKey := []byte("/registry/hash/a")
			const firstRevision = uint64(0x1800000000000100)
			const foreignBoundary = uint64(0x2800000000000000)
			const latestRevision = uint64(0x3800000000000100)
			foreignSuffix := make([]byte, 8)
			binary.BigEndian.PutUint64(foreignSuffix, foreignBoundary)
			foreignKey := append(append(append([]byte(nil), shortKey...), '$'), foreignSuffix...)
			foreignKey = append(foreignKey, 'x')

			batch := s.kv.BeginBatchWrite()
			batch.Put(b.coder.EncodeObjectKey(shortKey, firstRevision), []byte("short-v1"), 0)
			batch.Put(b.coder.EncodeObjectKey(foreignKey, foreignBoundary), []byte("foreign-v1"), 0)
			batch.Put(b.coder.EncodeObjectKey(shortKey, latestRevision), []byte("short-v2"), 0)
			require.NoError(t, batch.Commit(s.ctx))
			b.SetCurrentRevision(latestRevision)

			advanced, err := b.setCompactRecord(s.ctx, latestRevision)
			require.NoError(t, err)
			require.True(t, advanced)
			logical, err := b.HashKV(s.ctx, 0)
			require.NoError(t, err)

			require.NoError(t, b.physicalCompact(s.ctx, latestRevision))
			physical, err := b.HashKV(s.ctx, 0)
			require.NoError(t, err)
			require.Equal(t, logical, physical,
				"physical GC must not change the logical HashKV snapshot")
		})
	}
}

func TestHashKVHonorsCancellation(t *testing.T) {
	b, _ := newTxnApplyBackend(t)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err := b.HashKV(ctx, 0)
	require.ErrorIs(t, err, context.Canceled)
}

func TestHashKVPrefetchesPartitionsAndPreservesEncodedOrder(t *testing.T) {
	ctrl := gomock.NewController(t)
	rawStore := memkv.NewKvStorage()
	store := &hashKVPartitionTestStorage{KvStorage: rawStore}
	b := NewBackend(store, Config{
		Prefix: prefix, Identity: getStorageIdentity(), EnableEtcdCompatibility: true,
	}, metricsmock.NewMinimalMetrics(ctrl)).(*backend)
	t.Cleanup(func() { require.NoError(t, b.Close()) })
	b.SetCurrentRevision(uint64(time.Now().UnixNano()))
	ctx := context.Background()
	keys := [][]byte{
		[]byte(prefix + "/hash/partition/a"),
		[]byte(prefix + "/hash/partition/b"),
		[]byte(prefix + "/hash/partition/c"),
	}
	var revisions []uint64
	for index, key := range keys {
		created, err := b.Create(ctx, &proto.CreateRequest{Key: key, Value: []byte{byte('a' + index)}})
		require.NoError(t, err)
		revisions = append(revisions, created.Header.Revision)
	}
	waitCommitted(t, b, revisions[len(revisions)-1])
	want, err := b.HashKV(ctx, 0)
	require.NoError(t, err)

	split1 := b.coder.EncodeObjectKey(keys[1], revisions[1])
	split2 := b.coder.EncodeObjectKey(keys[2], revisions[2])
	var start, end []byte
	for _, scanRange := range b.ks.HashKVScanRanges() {
		if bytes.Compare(scanRange.Start, split1) <= 0 && bytes.Compare(split2, scanRange.End) < 0 {
			start, end = scanRange.Start, scanRange.End
			break
		}
	}
	require.NotEmpty(t, start)
	partitions := []storage.Partition{
		{Start: start, End: split1},
		{Start: split1, End: split2},
		{Start: split2, End: end},
	}
	started := make(chan struct{}, len(partitions))
	release := make(chan struct{})
	store.mu.Lock()
	store.partitions = partitions
	store.partitionStart = start
	store.partitionEnd = end
	store.gatedStarts = make(map[string]struct{}, len(partitions))
	for _, partition := range partitions {
		store.gatedStarts[string(partition.Start)] = struct{}{}
	}
	store.started = started
	store.release = release
	store.iterTimestamps = nil
	store.iterBatchSizes = nil
	store.mu.Unlock()

	type hashResult struct {
		result HashKVResult
		err    error
	}
	done := make(chan hashResult, 1)
	go func() {
		result, hashErr := b.HashKV(ctx, 0)
		done <- hashResult{result: result, err: hashErr}
	}()
	for range 2 {
		select {
		case <-started:
		case <-time.After(time.Second):
			t.Fatal("HashKV did not start two partition iterators concurrently")
		}
	}
	close(release)
	got := <-done
	require.NoError(t, got.err)
	require.Equal(t, want, got.result, "partition prefetch must not change CRC input order")
	store.mu.Lock()
	timestamps := append([]uint64(nil), store.iterTimestamps...)
	store.mu.Unlock()
	require.Len(t, timestamps, len(b.ks.HashKVScanRanges())-1+len(partitions))
	require.NotZero(t, timestamps[0])
	for _, timestamp := range timestamps[1:] {
		require.Equal(t, timestamps[0], timestamp, "every partition must use one engine snapshot")
	}
	store.mu.Lock()
	batchSizes := append([]int(nil), store.iterBatchSizes...)
	store.mu.Unlock()
	require.Len(t, batchSizes, len(timestamps))
	for _, batchSize := range batchSizes {
		require.Equal(t, hashKVScanBatchSize, batchSize,
			"live HashKV partitions must carry the bounded bulk-scan hint")
	}
}

func TestHashKVPinnedSnapshotSkipsPartitionDiscovery(t *testing.T) {
	ctrl := gomock.NewController(t)
	rawStore := memkv.NewKvStorage()
	store := &hashKVPartitionTestStorage{KvStorage: rawStore}
	b := NewBackend(store, Config{
		Prefix: prefix, Identity: getStorageIdentity(), EnableEtcdCompatibility: true,
	}, metricsmock.NewMinimalMetrics(ctrl)).(*backend)
	t.Cleanup(func() { require.NoError(t, b.Close()) })
	b.SetCurrentRevision(uint64(time.Now().UnixNano()))
	ctx := context.Background()
	created, err := b.Create(ctx, &proto.CreateRequest{
		Key: []byte(prefix + "/hash/pinned-partition"), Value: []byte("value"),
	})
	require.NoError(t, err)
	waitCommitted(t, b, created.Header.Revision)

	const timestamp = uint64(987654321)
	store.mu.Lock()
	store.partitionCalls = 0
	store.iterTimestamps = nil
	store.iterBatchSizes = nil
	store.mu.Unlock()
	pinned := WithSerializableCheckpoint(ctx, SerializableCheckpoint{
		Revision: created.Header.Revision, Timestamp: timestamp,
	})
	_, err = b.HashKV(pinned, int64(created.Header.Revision))
	require.NoError(t, err)
	store.mu.Lock()
	defer store.mu.Unlock()
	require.Zero(t, store.partitionCalls, "protected HashKV must not require PD partition discovery")
	require.Len(t, store.iterTimestamps, len(b.ks.HashKVScanRanges()))
	for _, gotTimestamp := range store.iterTimestamps {
		require.Equal(t, timestamp, gotTimestamp)
	}
	require.Equal(t, make([]int, len(b.ks.HashKVScanRanges())), store.iterBatchSizes,
		"protected HashKV must retain TiKV's conservative default scan batch")
}

func TestHashKVUsesPinnedSnapshotTimestampForObjectScan(t *testing.T) {
	ctrl := gomock.NewController(t)
	rawStore := memkv.NewKvStorage()
	store := &rangeSnapshotTraceStorage{KvStorage: rawStore}
	b := NewBackend(store, Config{
		Prefix: prefix, Identity: getStorageIdentity(), EnableEtcdCompatibility: true,
	}, metricsmock.NewMinimalMetrics(ctrl)).(*backend)
	t.Cleanup(func() { require.NoError(t, b.Close()) })
	b.SetCurrentRevision(uint64(time.Now().UnixNano()))
	ctx := context.Background()
	created, err := b.Create(ctx, &proto.CreateRequest{Key: []byte(prefix + "/hash/pinned"), Value: []byte("value")})
	require.NoError(t, err)
	waitCommitted(t, b, created.Header.Revision)

	store.resetTrace()
	const timestamp = uint64(987654321)
	pinned := WithSerializableCheckpoint(ctx, SerializableCheckpoint{
		Revision: created.Header.Revision, Timestamp: timestamp,
	})
	b.logicalWriteMu.Lock()
	_, err = b.HashKV(pinned, int64(created.Header.Revision))
	b.logicalWriteMu.Unlock()
	require.NoError(t, err)
	require.Len(t, store.iterTimestamps, len(b.ks.HashKVScanRanges()))
	for _, gotTimestamp := range store.iterTimestamps {
		require.Equal(t, timestamp, gotTimestamp,
			"every disjoint object interval must use the protected engine snapshot")
	}
}

func TestHashKVArmsCorruptForWitnessedInvalidObjectValue(t *testing.T) {
	b, ctx := newTxnApplyBackend(t)
	key := []byte(prefix + "/hash/witnessed-corrupt")
	created, err := b.Create(ctx, &proto.CreateRequest{Key: key, Value: []byte("value")})
	require.NoError(t, err)
	waitCommitted(t, b, created.Header.Revision)

	objectKey := b.coder.EncodeObjectKey(key, created.Header.Revision)
	objectValue, err := b.kv.Get(ctx, objectKey)
	require.NoError(t, err)
	corrupt := b.kv.BeginBatchWrite()
	corrupt.Put(objectKey, []byte{0, 'k', 'b', 3}, 0)
	require.NoError(t, corrupt.Commit(ctx))

	hash, err := b.HashKV(ctx, 0)
	require.ErrorIs(t, err, ErrInvalidMVCCMetadata)
	require.Equal(t, HashKVResult{}, hash, "corrupt bytes must not produce a successful diagnostic hash")
	alarms, alarmErr := b.CorruptAlarms(ctx)
	require.NoError(t, alarmErr)
	require.Equal(t, []uint64{b.localAlarmMemberID()}, alarms)
	_, writeErr := b.Create(ctx, &proto.CreateRequest{Key: []byte(prefix + "/hash/blocked"), Value: []byte("blocked")})
	require.ErrorIs(t, writeErr, ErrCorruptAlarmActive)
	removed, disarmErr := b.DisarmCorrupt(ctx, b.localAlarmMemberID())
	require.ErrorIs(t, disarmErr, ErrTxnWitnessCorrupt)
	require.False(t, removed)

	repair := b.kv.BeginBatchWrite()
	repair.Put(objectKey, objectValue, 0)
	require.NoError(t, repair.Commit(ctx))
	removed, disarmErr = b.DisarmCorrupt(ctx, b.localAlarmMemberID())
	require.NoError(t, disarmErr)
	require.True(t, removed)
	hash, err = b.HashKV(ctx, 0)
	require.NoError(t, err)
	require.Equal(t, int64(created.Header.Revision), hash.HashRevision)
}

func TestHashKVDoesNotValidateCompactedShadowVersion(t *testing.T) {
	b, ctx := newTxnApplyBackend(t)
	key := []byte(prefix + "/hash/compacted-shadow")
	created, err := b.Create(ctx, &proto.CreateRequest{Key: key, Value: []byte("v1")})
	require.NoError(t, err)
	updated, err := b.Update(ctx, &proto.UpdateRequest{Kv: &proto.KeyValue{
		Key: key, Value: []byte("v2"), Revision: created.Header.Revision,
	}})
	require.NoError(t, err)
	waitCommitted(t, b, updated.Header.Revision)
	advanced, err := b.setCompactRecord(ctx, updated.Header.Revision)
	require.NoError(t, err)
	require.True(t, advanced)

	corrupt := b.kv.BeginBatchWrite()
	corrupt.Put(b.coder.EncodeObjectKey(key, created.Header.Revision), []byte{0, 'k', 'b', 3}, 0)
	require.NoError(t, corrupt.Commit(ctx))

	_, err = b.HashKV(ctx, 0)
	require.NoError(t, err, "a shadowed version outside the logical hash domain must not fail HashKV")
	alarms, alarmErr := b.CorruptAlarms(ctx)
	require.NoError(t, alarmErr)
	require.Empty(t, alarms)
}

func TestHashKVInvalidObjectWithoutWitnessDoesNotArmCorrupt(t *testing.T) {
	b, ctx := newTxnApplyBackend(t)
	key := []byte(prefix + "/hash/unwitnessed-corrupt")
	created, err := b.Create(ctx, &proto.CreateRequest{Key: key, Value: []byte("value")})
	require.NoError(t, err)
	waitCommitted(t, b, created.Header.Revision)

	corrupt := b.kv.BeginBatchWrite()
	corrupt.Del(b.ks.EncodeInternalKey(txnWitnessLogicalKey(created.Header.Revision)))
	corrupt.Put(b.coder.EncodeObjectKey(key, created.Header.Revision), []byte{0, 'k', 'b', 3}, 0)
	require.NoError(t, corrupt.Commit(ctx))

	_, err = b.HashKV(ctx, 0)
	require.ErrorIs(t, err, ErrInvalidMVCCMetadata)
	alarms, alarmErr := b.CorruptAlarms(ctx)
	require.NoError(t, alarmErr)
	require.Empty(t, alarms, "unsealed history has no durable repair evidence for a safe CORRUPT disarm")
}

func TestBackendHashIncludesInternalStateExcludedFromHashKV(t *testing.T) {
	b, ctx := newTxnApplyBackend(t)
	key := []byte(prefix + "/hash/backend")
	created, err := b.Create(ctx, &proto.CreateRequest{Key: key, Value: []byte("value")})
	require.NoError(t, err)
	waitCommitted(t, b, created.Header.Revision)

	before, err := b.Hash(ctx)
	require.NoError(t, err)
	logicalBefore, err := b.HashKV(ctx, 0)
	require.NoError(t, err)
	require.Equal(t, logicalBefore.CurrentRevision, before.CurrentRevision)
	require.NotEqual(t, logicalBefore.Hash, before.Hash,
		"backend Hash and user-MVCC HashKV must retain distinct checksum domains")

	require.NoError(t, b.InternalPut(ctx, []byte("hash/backend-only"), []byte("metadata")))
	after, err := b.Hash(ctx)
	require.NoError(t, err)
	logicalAfter, err := b.HashKV(ctx, 0)
	require.NoError(t, err)
	require.Equal(t, before.CurrentRevision, after.CurrentRevision,
		"internal metadata must not consume a user-visible revision")
	require.NotEqual(t, before.Hash, after.Hash, "backend Hash must include internal metadata")
	require.Equal(t, logicalBefore, logicalAfter, "HashKV must exclude internal metadata")
}

func TestBackendHashIgnoresSerializableCheckpointRefresh(t *testing.T) {
	b, ctx := newTxnApplyBackend(t)
	created, err := b.Create(ctx, &proto.CreateRequest{Key: []byte(prefix + "/hash/checkpoint"), Value: []byte("value")})
	require.NoError(t, err)
	waitCommitted(t, b, created.Header.Revision)

	require.NoError(t, b.InternalPut(ctx, serializableCheckpointKey, []byte("checkpoint-one")))
	before, err := b.Hash(ctx)
	require.NoError(t, err)
	require.NoError(t, b.InternalPut(ctx, serializableCheckpointKey, []byte("checkpoint-two")))
	after, err := b.Hash(ctx)
	require.NoError(t, err)

	require.Equal(t, before, after,
		"background serializable-checkpoint timestamps are etcd-style ignored bookkeeping")
}

func TestBackendHashHonorsCancellation(t *testing.T) {
	b, _ := newTxnApplyBackend(t)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := b.Hash(ctx)
	require.ErrorIs(t, err, context.Canceled)
}

func TestBackendHashUsesPinnedSnapshotTimestampAndRevision(t *testing.T) {
	ctrl := gomock.NewController(t)
	rawStore := memkv.NewKvStorage()
	store := &rangeSnapshotTraceStorage{KvStorage: rawStore}
	b := NewBackend(store, Config{
		Prefix: prefix, Identity: getStorageIdentity(), EnableEtcdCompatibility: true,
	}, metricsmock.NewMinimalMetrics(ctrl)).(*backend)
	t.Cleanup(func() { require.NoError(t, b.Close()) })
	b.SetCurrentRevision(uint64(time.Now().UnixNano()))

	store.resetTrace()
	const timestamp = uint64(987654321)
	const revision = uint64(77)
	pinned := WithSerializableCheckpoint(context.Background(), SerializableCheckpoint{
		Revision: revision, Timestamp: timestamp,
	})
	// A protected immutable snapshot must not queue behind a live logical write.
	b.logicalWriteMu.Lock()
	result, err := b.Hash(pinned)
	b.logicalWriteMu.Unlock()
	require.NoError(t, err)
	require.Equal(t, int64(revision), result.CurrentRevision)
	require.Equal(t, []uint64{timestamp}, store.iterTimestamps)
}

func TestHashKVIsStableAcrossPhysicalCompaction(t *testing.T) {
	b, ctx := newTxnApplyBackend(t)
	key := []byte(prefix + "/hash/compact")

	created, err := b.Create(ctx, &proto.CreateRequest{Key: key, Value: []byte("v1")})
	require.NoError(t, err)
	last := created.Header.Revision
	for _, value := range []string{"v2", "v3"} {
		updated, updateErr := b.Update(ctx, &proto.UpdateRequest{Kv: &proto.KeyValue{
			Key: key, Value: []byte(value), Revision: last,
		}})
		require.NoError(t, updateErr)
		require.True(t, updated.Succeeded)
		last = updated.Header.Revision
	}
	deletedKey := []byte(prefix + "/hash/compact-deleted")
	deletedCreated, err := b.Create(ctx, &proto.CreateRequest{Key: deletedKey, Value: []byte("gone")})
	require.NoError(t, err)
	deleted, err := b.Delete(ctx, &proto.DeleteRequest{Key: deletedKey, Revision: deletedCreated.Header.Revision})
	require.NoError(t, err)
	require.True(t, deleted.Succeeded)
	last = deleted.Header.Revision
	waitCommitted(t, b, last)

	beforeLogicalCompact, err := b.HashKV(ctx, 0)
	require.NoError(t, err)
	require.Equal(t, int64(-1), beforeLogicalCompact.CompactRevision)

	advanced, err := b.setCompactRecord(ctx, last)
	require.NoError(t, err)
	require.True(t, advanced)
	afterLogicalCompact, err := b.HashKV(ctx, 0)
	require.NoError(t, err)
	require.Equal(t, int64(last), afterLogicalCompact.CompactRevision)
	require.NotEqual(t, beforeLogicalCompact.Hash, afterLogicalCompact.Hash,
		"logical compaction must exclude versions scheduled for physical GC")

	require.NoError(t, b.physicalCompact(ctx, last))
	afterPhysicalCompact, err := b.HashKV(ctx, 0)
	require.NoError(t, err)
	require.Equal(t, afterLogicalCompact, afterPhysicalCompact,
		"HashKV must describe logical state and remain stable when physical GC catches up")

	_, err = b.HashKV(ctx, int64(last)-1)
	require.ErrorIs(t, err, ErrHashKVCompacted)
	_, err = b.HashKV(ctx, int64(b.GetCurrentRevision())+1)
	require.ErrorIs(t, err, ErrHashKVFuture)
}
