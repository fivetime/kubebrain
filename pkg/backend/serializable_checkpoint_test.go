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

	"github.com/kubewharf/kubebrain/pkg/metrics/mock"
	"github.com/kubewharf/kubebrain/pkg/storage"
	"github.com/kubewharf/kubebrain/pkg/storage/memkv"
)

type checkpointTestStorage struct {
	storage.KvStorage
	mu              sync.Mutex
	timestamp       uint64
	minimum         uint64
	protectedID     string
	protectedTS     uint64
	protectedTTL    time.Duration
	warmReads       int
	getAtErr        error
	releasedID      string
	tsoReads        int
	partitions      int
	partitionStarts [][]byte
	warmerCalls     int
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

func (s *checkpointTestStorage) BatchGetAt(ctx context.Context, keys [][]byte, _ uint64) (map[string][]byte, error) {
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

func (s *checkpointTestStorage) ProtectSnapshot(_ context.Context, id string, ttl time.Duration, timestamp uint64) (uint64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.protectedID, s.protectedTS, s.protectedTTL = id, timestamp, ttl
	return s.minimum, nil
}

func (s *checkpointTestStorage) ReleaseSnapshot(_ context.Context, id string) error {
	s.mu.Lock()
	s.releasedID = id
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
	require.Equal(t, 1, store.tsoReads)
	store.mu.Unlock()

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
