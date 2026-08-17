// Copyright 2026 ByteDance and/or its affiliates
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
package backend

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	proto "github.com/kubewharf/kubebrain-client/api/v2rpc"
	"github.com/stretchr/testify/require"
	tikvcfg "github.com/tikv/client-go/v2/config"

	backendscanner "github.com/kubewharf/kubebrain/pkg/backend/scanner"
	"github.com/kubewharf/kubebrain/pkg/metrics/mock"
	"github.com/kubewharf/kubebrain/pkg/storage"
	"github.com/kubewharf/kubebrain/pkg/storage/memkv"
)

type checkpointCountScanner struct {
	backendscanner.Scanner
	mu                  sync.Mutex
	rangeCalled         bool
	rangeFilteredCalled bool
	rangeStreamCalled   bool
	rangeStreamCalls    int
	countFilteredCalled bool
}

func (s *checkpointCountScanner) RangeStream(ctx context.Context, start, end []byte, revision uint64, keysOnly bool) chan *proto.StreamRangeResponse {
	s.mu.Lock()
	s.rangeStreamCalled = true
	s.rangeStreamCalls++
	s.mu.Unlock()
	return s.Scanner.RangeStream(ctx, start, end, revision, keysOnly)
}

func (s *checkpointCountScanner) Range(ctx context.Context, start, end []byte, revision uint64, limit int64) ([]*proto.KeyValue, error) {
	s.mu.Lock()
	s.rangeCalled = true
	s.mu.Unlock()
	return s.Scanner.Range(ctx, start, end, revision, limit)
}

func (s *checkpointCountScanner) CountFiltered(ctx context.Context, start, end, userStart, userEnd []byte, revision uint64) (int, error) {
	s.mu.Lock()
	s.countFilteredCalled = true
	s.mu.Unlock()
	return s.Scanner.CountFiltered(ctx, start, end, userStart, userEnd, revision)
}

func (s *checkpointCountScanner) CountFilteredExcluding(ctx context.Context, start, end, userStart, userEnd []byte, excluded [][]byte, revision uint64) (int, error) {
	s.mu.Lock()
	s.countFilteredCalled = true
	s.mu.Unlock()
	return s.Scanner.CountFilteredExcluding(ctx, start, end, userStart, userEnd, excluded, revision)
}

func (s *checkpointCountScanner) RangeFiltered(ctx context.Context, start, end, userStart, userEnd []byte, revision uint64) ([]*proto.KeyValue, error) {
	s.mu.Lock()
	s.rangeFilteredCalled = true
	s.mu.Unlock()
	return s.Scanner.RangeFiltered(ctx, start, end, userStart, userEnd, revision)
}

func (s *checkpointCountScanner) RangeFilteredExcluding(ctx context.Context, start, end, userStart, userEnd []byte, excluded [][]byte, revision uint64) ([]*proto.KeyValue, error) {
	s.mu.Lock()
	s.rangeFilteredCalled = true
	s.mu.Unlock()
	return s.Scanner.RangeFilteredExcluding(ctx, start, end, userStart, userEnd, excluded, revision)
}

type checkpointTestStorage struct {
	storage.KvStorage
	mu              sync.Mutex
	timestamp       uint64
	minimum         uint64
	protectedID     string
	protectedTS     uint64
	protectedTTL    time.Duration
	protections     []checkpointProtection
	warmReads       int
	getAtErr        error
	releasedIDs     []string
	tsoReads        int
	partitions      int
	partitionStarts [][]byte
	warmerCalls     int
	batchTimestamps []uint64
	readinessErr    error
	readyTimestamp  uint64
	readinessCalls  int
	readinessStart  []byte
	readinessEnd    []byte
	snapshotValues  map[string][]byte
}

type checkpointProtection struct {
	id        string
	timestamp uint64
}

func (s *checkpointTestStorage) GetTimestampOracle(context.Context) (uint64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.timestamp++
	s.tsoReads++
	return s.timestamp, nil
}

func (s *checkpointTestStorage) GetPartitions(ctx context.Context, start, end []byte) ([]storage.Partition, error) {
	s.mu.Lock()
	s.partitions++
	starts := append([][]byte(nil), s.partitionStarts...)
	s.mu.Unlock()
	if len(starts) != 0 {
		partitions := make([]storage.Partition, 0, len(starts))
		for index, partitionStart := range starts {
			partitionEnd := end
			if index+1 < len(starts) {
				partitionEnd = starts[index+1]
			}
			partitions = append(partitions, storage.Partition{Start: partitionStart, End: partitionEnd})
		}
		return partitions, nil
	}
	return s.KvStorage.GetPartitions(ctx, start, end)
}

func (s *checkpointTestStorage) GetAt(ctx context.Context, key []byte, _ uint64) ([]byte, error) {
	s.mu.Lock()
	s.warmReads++
	err := s.getAtErr
	s.mu.Unlock()
	if err != nil {
		return nil, err
	}
	return s.KvStorage.Get(ctx, key)
}

func (s *checkpointTestStorage) BatchGetAt(ctx context.Context, keys [][]byte, timestamp uint64) (map[string][]byte, error) {
	s.mu.Lock()
	s.batchTimestamps = append(s.batchTimestamps, timestamp)
	if s.snapshotValues != nil {
		values := make(map[string][]byte, len(keys))
		for _, key := range keys {
			if value, ok := s.snapshotValues[string(key)]; ok {
				values[string(key)] = append([]byte(nil), value...)
			}
		}
		s.mu.Unlock()
		return values, nil
	}
	s.mu.Unlock()
	return s.KvStorage.(storage.BatchGetter).BatchGet(ctx, keys)
}

func (s *checkpointTestStorage) WarmSnapshotRegions(ctx context.Context, starts [][]byte, timestamp uint64) error {
	s.mu.Lock()
	s.warmerCalls++
	s.mu.Unlock()
	for _, start := range starts {
		if _, err := s.GetAt(ctx, start, timestamp); err != nil && !errors.Is(err, storage.ErrKeyNotFound) {
			return err
		}
	}
	return nil
}

func (s *checkpointTestStorage) SnapshotReadyTimestamp(_ context.Context, start, end []byte) (uint64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.readinessCalls++
	s.readinessStart = append([]byte(nil), start...)
	s.readinessEnd = append([]byte(nil), end...)
	if s.readinessErr != nil {
		return 0, s.readinessErr
	}
	if s.readyTimestamp != 0 {
		return s.readyTimestamp, nil
	}
	s.timestamp++
	return s.timestamp, nil
}

func (s *checkpointTestStorage) ProtectSnapshot(_ context.Context, id string, ttl time.Duration, timestamp uint64) (uint64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.protectedID, s.protectedTS, s.protectedTTL = id, timestamp, ttl
	s.protections = append(s.protections, checkpointProtection{id: id, timestamp: timestamp})
	return s.minimum, nil
}

func (s *checkpointTestStorage) ReleaseSnapshot(_ context.Context, id string) error {
	s.mu.Lock()
	s.releasedIDs = append(s.releasedIDs, id)
	s.mu.Unlock()
	return nil
}

func newCheckpointBackend(t *testing.T, store storage.KvStorage) *backend {
	t.Helper()
	ctrl := gomock.NewController(t)
	b := NewBackend(store, Config{Prefix: "/registry", Keyspace: "checkpoint-test", Identity: "peer-a", EnableEtcdCompatibility: true}, mock.NewMinimalMetrics(ctrl)).(*backend)
	// These tests invoke checkpoint transitions synchronously; stop the eager
	// background refresh so it cannot consume a test timestamp concurrently.
	b.stopWorkers()
	t.Cleanup(func() { require.NoError(t, b.Close()) })
	return b
}

func TestSerializableCheckpointBindsRevisionCompactAndAuthAtSnapshot(t *testing.T) {
	store := &checkpointTestStorage{KvStorage: memkv.NewKvStorage(), timestamp: 100}
	b := newCheckpointBackend(t, store)
	b.SetCurrentRevision(17)
	require.NoError(t, b.InternalPut(context.Background(), []byte("auth/config"), append([]byte{1}, uint64ToBytes(9)...)))
	compact := store.BeginBatchWrite()
	compact.Put(getCompactKey("/registry"), uint64ToBytes(11), 0)
	require.NoError(t, compact.Commit(context.Background()))

	c, err := b.createSerializableCheckpoint(context.Background())
	require.NoError(t, err)
	require.Equal(t, uint64(17), c.Revision)
	require.Equal(t, uint64(101), c.Timestamp)
	require.Equal(t, uint64(11), c.CompactRevision)
	require.Equal(t, uint64(9), c.AuthRevision)
	loaded, err := b.loadSerializableCheckpoint(context.Background())
	require.NoError(t, err)
	require.Equal(t, c, loaded)
	require.NoError(t, b.protectSerializableCheckpoint(context.Background(), c))
	served, err := b.GetSerializableCheckpoint()
	require.NoError(t, err)
	require.Equal(t, c.Timestamp, served.Timestamp)
	require.NotEmpty(t, store.protectedID)
	require.Equal(t, c.Timestamp, store.protectedTS)
	require.Equal(t, serializableCheckpointTTL, store.protectedTTL)
	require.Positive(t, store.partitions)
	require.Positive(t, store.warmReads)
}

func TestSerializableCheckpointUsesRevisionWatermarksAtReadyTimestamp(t *testing.T) {
	store := &checkpointTestStorage{KvStorage: memkv.NewKvStorage(), readyTimestamp: 300}
	b := newCheckpointBackend(t, store)
	b.SetCurrentRevision(9)
	store.snapshotValues = map[string][]byte{
		string(b.ks.EncodeInternalKey(durableRevisionKey)):    uint64ToBytes(7),
		string(getCompactKey(b.config.Prefix)):                uint64ToBytes(3),
		string(b.ks.EncodeInternalKey([]byte("auth/config"))): append([]byte{1}, uint64ToBytes(5)...),
	}

	c, err := b.createSerializableCheckpoint(context.Background())
	require.NoError(t, err)
	require.Equal(t, SerializableCheckpoint{Revision: 7, Timestamp: 300, CompactRevision: 3, AuthRevision: 5}, c)
	require.Equal(t, b.ks.ObjectKeyspaceStart(), store.readinessStart)
	require.Equal(t, b.ks.ObjectKeyspaceEnd(), store.readinessEnd)
}

func TestSerializableCheckpointFailsClosedAfterGCPassedOrLocalDeadline(t *testing.T) {
	store := &checkpointTestStorage{KvStorage: memkv.NewKvStorage(), minimum: 201}
	b := newCheckpointBackend(t, store)
	c := SerializableCheckpoint{Revision: 5, Timestamp: 200}
	err := b.protectSerializableCheckpoint(context.Background(), c)
	require.ErrorIs(t, err, ErrSerializableCheckpointUnavailable)
	_, err = b.GetSerializableCheckpoint()
	require.ErrorIs(t, err, ErrSerializableCheckpointUnavailable)

	c.ValidUntil = time.Now().Add(-time.Second)
	b.serializableCheckpoint.Store(&c)
	_, err = b.GetSerializableCheckpoint()
	require.True(t, errors.Is(err, ErrSerializableCheckpointUnavailable))
}

func TestSerializableCheckpointFailsClosedWhenRegionWarmupFails(t *testing.T) {
	store := &checkpointTestStorage{KvStorage: memkv.NewKvStorage(), getAtErr: storage.ErrUnavailable}
	b := newCheckpointBackend(t, store)
	err := b.protectSerializableCheckpoint(context.Background(), SerializableCheckpoint{Revision: 5, Timestamp: 200})
	require.ErrorIs(t, err, storage.ErrUnavailable)
	_, err = b.GetSerializableCheckpoint()
	require.ErrorIs(t, err, ErrSerializableCheckpointUnavailable)
}

func TestSerializableCheckpointWarmFailureKeepsOldProtection(t *testing.T) {
	store := &checkpointTestStorage{KvStorage: memkv.NewKvStorage()}
	b := newCheckpointBackend(t, store)
	old := SerializableCheckpoint{Revision: 5, Timestamp: 200}
	require.NoError(t, b.protectSerializableCheckpoint(context.Background(), old))

	store.mu.Lock()
	store.getAtErr = storage.ErrUnavailable
	store.mu.Unlock()
	b.serializableCheckpointRegionsWarmedAt.Store(0)
	err := b.protectSerializableCheckpoint(context.Background(), SerializableCheckpoint{Revision: 6, Timestamp: 201})
	require.ErrorIs(t, err, storage.ErrUnavailable)

	served, err := b.GetSerializableCheckpoint()
	require.NoError(t, err)
	require.Equal(t, old.Timestamp, served.Timestamp)
	store.mu.Lock()
	require.Equal(t, old.Timestamp, store.protectedTS)
	store.mu.Unlock()
}

func TestSerializableCheckpointRetainsPreviousProtectionDuringUnaryGrace(t *testing.T) {
	store := &checkpointTestStorage{KvStorage: memkv.NewKvStorage()}
	b := newCheckpointBackend(t, store)
	first := SerializableCheckpoint{Revision: 5, Timestamp: 200}
	require.NoError(t, b.protectSerializableCheckpoint(context.Background(), first))

	second := SerializableCheckpoint{Revision: 6, Timestamp: 201}
	require.NoError(t, b.protectSerializableCheckpoint(context.Background(), second))
	served, err := b.GetSerializableCheckpoint()
	require.NoError(t, err)
	require.Equal(t, first.Timestamp, served.Timestamp)

	store.mu.Lock()
	require.Len(t, store.protections, 2)
	require.Equal(t, store.protections[0].id, store.protections[1].id)
	require.Equal(t, []uint64{first.Timestamp, first.Timestamp}, []uint64{
		store.protections[0].timestamp, store.protections[1].timestamp,
	})
	store.mu.Unlock()
}

func TestSerializableCheckpointRotatesProtectionAfterUnaryGrace(t *testing.T) {
	store := &checkpointTestStorage{KvStorage: memkv.NewKvStorage()}
	b := newCheckpointBackend(t, store)
	first := SerializableCheckpoint{Revision: 5, Timestamp: 200}
	require.NoError(t, b.protectSerializableCheckpoint(context.Background(), first))
	b.serializableCheckpointSwitchedAt = time.Now().Add(-serializableCheckpointProtectionGrace)

	second := SerializableCheckpoint{Revision: 6, Timestamp: 201}
	require.NoError(t, b.protectSerializableCheckpoint(context.Background(), second))
	served, err := b.GetSerializableCheckpoint()
	require.NoError(t, err)
	require.Equal(t, second.Timestamp, served.Timestamp)

	store.mu.Lock()
	require.Len(t, store.protections, 2)
	require.NotEqual(t, store.protections[0].id, store.protections[1].id)
	require.Equal(t, []uint64{first.Timestamp, second.Timestamp}, []uint64{
		store.protections[0].timestamp, store.protections[1].timestamp,
	})
	require.Empty(t, store.releasedIDs, "the previous slot must remain protected for in-flight reads")
	store.mu.Unlock()
}

func TestSerializableCheckpointReleaseRemovesBothProtectionSlots(t *testing.T) {
	store := &checkpointTestStorage{KvStorage: memkv.NewKvStorage()}
	b := newCheckpointBackend(t, store)
	require.NoError(t, b.protectSerializableCheckpoint(
		context.Background(), SerializableCheckpoint{Revision: 5, Timestamp: 200},
	))
	b.serializableCheckpointSwitchedAt = time.Now().Add(-serializableCheckpointProtectionGrace)
	require.NoError(t, b.protectSerializableCheckpoint(
		context.Background(), SerializableCheckpoint{Revision: 6, Timestamp: 201},
	))

	require.NoError(t, b.releaseSerializableCheckpoint(context.Background()))
	store.mu.Lock()
	require.ElementsMatch(t, b.serializableCheckpointServiceIDs[:], store.releasedIDs)
	store.mu.Unlock()
	_, err := b.GetSerializableCheckpoint()
	require.ErrorIs(t, err, ErrSerializableCheckpointUnavailable)
}

func TestSerializableCheckpointWarmsEveryTenantRegionBeforePublication(t *testing.T) {
	store := &checkpointTestStorage{
		KvStorage: memkv.NewKvStorage(), partitionStarts: [][]byte{{0x10}, {0x20}, {0x30}},
	}
	b := newCheckpointBackend(t, store)
	require.NoError(t, b.protectSerializableCheckpoint(
		context.Background(), SerializableCheckpoint{Revision: 5, Timestamp: 200},
	))
	require.NoError(t, b.protectSerializableCheckpoint(
		context.Background(), SerializableCheckpoint{Revision: 6, Timestamp: 201},
	))
	store.mu.Lock()
	require.Equal(t, 3, store.warmReads)
	require.Equal(t, 1, store.warmerCalls)
	store.mu.Unlock()
	_, err := b.GetSerializableCheckpoint()
	require.NoError(t, err)
}

func TestSerializableCheckpointUsableWindowExpiresBeforeDefaultRegionCacheTTL(t *testing.T) {
	regionCacheTTL := time.Duration(tikvcfg.GetGlobalConfig().TiKVClient.RegionCacheTTL) * time.Second
	require.Positive(t, regionCacheTTL)
	require.Less(t, serializableCheckpointUsable, regionCacheTTL,
		"a protected checkpoint must fail closed before an idle warmed Region can be evicted")
}

func TestSerializableCheckpointMetricsExposePerReplicaUsableWindow(t *testing.T) {
	recorder := &compactMetricRecorder{}
	initSerializableCheckpointMetrics(recorder)
	b := &backend{metricCli: recorder}
	now := time.Unix(1_700_000_000, 0)
	b.serializableCheckpoint.Store(&SerializableCheckpoint{Revision: 42, ValidUntil: now.Add(90 * time.Second)})

	b.emitSerializableCheckpointMetrics(now)
	b.emitSerializableCheckpointMetrics(now.Add(91 * time.Second))

	require.Equal(t, []compactMetricRecord{
		{kind: "gauge", name: "serializable.checkpoint.available", value: int64(0)},
		{kind: "gauge", name: "serializable.checkpoint.revision", value: int64(0)},
		{kind: "gauge", name: "serializable.checkpoint.remaining_seconds", value: int64(0)},
		{kind: "gauge", name: "serializable.checkpoint.available", value: int64(1)},
		{kind: "gauge", name: "serializable.checkpoint.revision", value: int64(42)},
		{kind: "gauge", name: "serializable.checkpoint.remaining_seconds", value: int64(90)},
		{kind: "gauge", name: "serializable.checkpoint.available", value: int64(0)},
		{kind: "gauge", name: "serializable.checkpoint.revision", value: int64(0)},
		{kind: "gauge", name: "serializable.checkpoint.remaining_seconds", value: int64(0)},
	}, recorder.records)
}

func TestSerializableCheckpointCodecRejectsUnsafeMetadata(t *testing.T) {
	_, err := decodeSerializableCheckpoint(nil)
	require.Error(t, err)
	encoded := encodeSerializableCheckpoint(SerializableCheckpoint{Revision: 4, Timestamp: 10, CompactRevision: 5})
	_, err = decodeSerializableCheckpoint(encoded)
	require.Error(t, err)
}

func TestSerializableCheckpointReusesUnchangedSnapshot(t *testing.T) {
	store := &checkpointTestStorage{KvStorage: memkv.NewKvStorage(), timestamp: 400}
	b := newCheckpointBackend(t, store)
	b.SetCurrentRevision(8)

	first, err := b.createSerializableCheckpoint(context.Background())
	require.NoError(t, err)
	require.NoError(t, b.protectSerializableCheckpoint(context.Background(), first))
	second, err := b.createSerializableCheckpoint(context.Background())
	require.NoError(t, err)
	require.Equal(t, first.Timestamp, second.Timestamp)
	store.mu.Lock()
	tsoReads := store.tsoReads
	store.mu.Unlock()
	require.Zero(t, tsoReads)

	b.SetCurrentRevision(9)
	third, err := b.createSerializableCheckpoint(context.Background())
	require.NoError(t, err)
	require.NotEqual(t, first.Timestamp, third.Timestamp)
}

func TestSerializableCheckpointServiceIDIsProcessUnique(t *testing.T) {
	first := newSerializableCheckpointServiceID("shared", "peer-a")
	second := newSerializableCheckpointServiceID("shared", "peer-a")
	require.NotEqual(t, first, second)
	require.LessOrEqual(t, len(first), 64)
}

func TestSerializableCheckpointContextReadsWithoutOracleOrPartitionDiscovery(t *testing.T) {
	store := &checkpointTestStorage{KvStorage: memkv.NewKvStorage(), timestamp: 300}
	b := newCheckpointBackend(t, store)
	ctx := context.Background()
	seed := store.BeginBatchWrite()
	seed.Put(b.coder.EncodeRevisionKey([]byte("/a")), uint64ToBytes(2), 0)
	seed.Put(b.coder.EncodeObjectKey([]byte("/a"), 2), []byte("one"), 0)
	require.NoError(t, seed.Commit(ctx))
	b.SetCurrentRevision(2)
	c, err := b.createSerializableCheckpoint(ctx)
	require.NoError(t, err)
	store.mu.Lock()
	tsoBefore, partitionsBefore := store.tsoReads, store.partitions
	store.mu.Unlock()

	checkpointCtx := WithSerializableCheckpoint(ctx, c)
	point, err := b.Get(checkpointCtx, &proto.GetRequest{Key: []byte("/a")})
	require.NoError(t, err)
	require.Equal(t, c.Revision, point.Header.Revision)
	require.Equal(t, []byte("one"), point.Kv.Value)
	listed, err := b.List(checkpointCtx, &proto.RangeRequest{Key: []byte("/"), End: []byte("0")})
	require.NoError(t, err)
	require.Equal(t, c.Revision, listed.Header.Revision)
	require.Len(t, listed.Kvs, 1)
	store.mu.Lock()
	require.Equal(t, tsoBefore, store.tsoReads)
	require.Equal(t, partitionsBefore, store.partitions)
	store.mu.Unlock()
}

func TestSerializableCheckpointCountLowByteBoundaryDoesNotMaterializeRange(t *testing.T) {
	store := &checkpointTestStorage{KvStorage: memkv.NewKvStorage(), timestamp: 400}
	b := newCheckpointBackend(t, store)
	ctx := context.Background()
	seed := store.BeginBatchWrite()
	for i, key := range []string{"$a", "$b", "&outside"} {
		revision := uint64(i + 2)
		seed.Put(b.coder.EncodeRevisionKey([]byte(key)), uint64ToBytes(revision), 0)
		seed.Put(b.coder.EncodeObjectKey([]byte(key), revision), []byte("value"), 0)
	}
	require.NoError(t, seed.Commit(ctx))
	b.SetCurrentRevision(4)
	checkpoint, err := b.createSerializableCheckpoint(ctx)
	require.NoError(t, err)

	probe := &checkpointCountScanner{Scanner: b.scanner}
	b.scanner = probe
	b.countIndex = nil // force the storage fallback exercised during index rebuild/miss
	response, err := b.Count(WithSerializableCheckpoint(ctx, checkpoint), &proto.CountRequest{
		Key: []byte("$"), End: []byte("%"),
	})
	require.NoError(t, err)
	require.Equal(t, checkpoint.Revision, response.Header.Revision)
	require.Equal(t, uint64(2), response.Count)
	require.True(t, probe.countFilteredCalled)
	require.False(t, probe.rangeCalled, "CountOnly must not materialize decodedUserRange")

	listed, err := b.List(WithSerializableCheckpoint(ctx, checkpoint), &proto.RangeRequest{
		Key: []byte("$"), End: []byte("%"), Limit: 1,
	})
	require.NoError(t, err)
	require.Equal(t, checkpoint.Revision, listed.Header.Revision)
	require.Len(t, listed.Kvs, 1)
	require.Equal(t, []byte("$a"), listed.Kvs[0].Key)
	require.True(t, listed.More)
	require.True(t, probe.rangeFilteredCalled)
	require.False(t, probe.rangeCalled, "low-boundary List must not materialize the full tenant range")
}

func TestSerializableCheckpointBatchesEscapedAncestorsWithoutOracleOrPartitionDiscovery(t *testing.T) {
	store := &checkpointTestStorage{KvStorage: memkv.NewKvStorage(), timestamp: 500}
	b := newCheckpointBackend(t, store)
	ctx := context.Background()
	lower := []byte("$checkpoint-batch")
	end := append(append([]byte(nil), lower...), make([]byte, 17)...)
	seed := store.BeginBatchWrite()
	for index := 0; index < 17; index++ {
		key := append(append([]byte(nil), lower...), make([]byte, index)...)
		revision := uint64(index + 2)
		seed.Put(b.coder.EncodeRevisionKey(key), uint64ToBytes(revision), 0)
		seed.Put(b.coder.EncodeObjectKey(key, revision), []byte{byte(index)}, 0)
	}
	require.NoError(t, seed.Commit(ctx))
	b.SetCurrentRevision(18)
	checkpoint, err := b.createSerializableCheckpoint(ctx)
	require.NoError(t, err)
	store.mu.Lock()
	tsoBefore, partitionsBefore := store.tsoReads, store.partitions
	store.batchTimestamps = nil
	store.mu.Unlock()

	listed, err := b.List(WithSerializableCheckpoint(ctx, checkpoint), &proto.RangeRequest{Key: lower, End: end})
	require.NoError(t, err)
	require.Equal(t, checkpoint.Revision, listed.Header.Revision)
	require.Len(t, listed.Kvs, 17)
	for index, kv := range listed.Kvs {
		require.Equal(t, []byte{byte(index)}, kv.Value)
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	require.Equal(t, tsoBefore, store.tsoReads)
	require.Equal(t, partitionsBefore, store.partitions)
	require.Equal(t, []uint64{checkpoint.Timestamp, checkpoint.Timestamp}, store.batchTimestamps)
}
