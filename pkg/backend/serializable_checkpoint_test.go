// Copyright 2026 ByteDance and/or its affiliates
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
package backend

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
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
	closeCalls      int
	tsoReads        int
	partitions      int
	partitionStarts [][]byte
	warmerCalls     int
	batchTimestamps []uint64
	readinessErr    error
	releaseErr      error
	readyTimestamp  uint64
	readyTimestamps []uint64
	readinessCalls  int
	readinessStarts [][]byte
	readinessEnds   [][]byte
	snapshotValues  map[string][]byte
}

type checkpointProtection struct {
	id        string
	timestamp uint64
}

type publicationValidatingCheckpointStorage struct {
	*checkpointTestStorage
	publicationMu         sync.Mutex
	publicationTimestamps []uint64
	publicationCalls      int
	publicationStarts     [][]byte
	publicationEnds       [][]byte
}

type blockingCheckpointReadinessStorage struct {
	*checkpointTestStorage
	armed   atomic.Bool
	once    sync.Once
	entered chan struct{}
	release chan struct{}
}

type blockingCheckpointProtectionStorage struct {
	*checkpointTestStorage
	armed   atomic.Bool
	once    sync.Once
	entered chan struct{}
	release chan struct{}
}

func (s *blockingCheckpointProtectionStorage) ProtectSnapshot(
	ctx context.Context, id string, ttl time.Duration, timestamp uint64,
) (uint64, error) {
	if !s.armed.Load() {
		return s.checkpointTestStorage.ProtectSnapshot(ctx, id, ttl, timestamp)
	}
	s.once.Do(func() { close(s.entered) })
	<-s.release // Deliberately ignore ctx to model a storage call racing cancellation.
	return s.checkpointTestStorage.ProtectSnapshot(ctx, id, ttl, timestamp)
}

func (s *blockingCheckpointReadinessStorage) SnapshotReadyTimestamp(
	ctx context.Context, start, end []byte,
) (uint64, error) {
	if !s.armed.Load() {
		return s.checkpointTestStorage.SnapshotReadyTimestamp(ctx, start, end)
	}
	s.once.Do(func() { close(s.entered) })
	select {
	case <-ctx.Done():
		return 0, ctx.Err()
	case <-s.release:
		return s.checkpointTestStorage.SnapshotReadyTimestamp(ctx, start, end)
	}
}

func (s *publicationValidatingCheckpointStorage) ValidateSnapshotPublication(
	_ context.Context, start, end []byte, timestamp uint64,
) error {
	s.publicationMu.Lock()
	defer s.publicationMu.Unlock()
	s.publicationStarts = append(s.publicationStarts, append([]byte(nil), start...))
	s.publicationEnds = append(s.publicationEnds, append([]byte(nil), end...))
	index := s.publicationCalls
	s.publicationCalls++
	if index >= len(s.publicationTimestamps) {
		return errors.New("missing publication timestamp")
	}
	if ready := s.publicationTimestamps[index]; ready < timestamp {
		return errors.New("current topology has not reached checkpoint timestamp")
	}
	return nil
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
	s.readinessStarts = append(s.readinessStarts, append([]byte(nil), start...))
	s.readinessEnds = append(s.readinessEnds, append([]byte(nil), end...))
	if s.readinessErr != nil {
		return 0, s.readinessErr
	}
	if len(s.readyTimestamps) >= s.readinessCalls {
		return s.readyTimestamps[s.readinessCalls-1], nil
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
	return s.releaseErr
}

func (s *checkpointTestStorage) Close() error {
	s.mu.Lock()
	s.closeCalls++
	s.mu.Unlock()
	return s.KvStorage.Close()
}

func TestBackendCloseReturnsSerializableCheckpointReleaseFailure(t *testing.T) {
	releaseErr := errors.New("injected checkpoint release failure")
	store := &checkpointTestStorage{KvStorage: memkv.NewKvStorage(), releaseErr: releaseErr}
	recorder := &compactMetricRecorder{}
	b := NewBackend(store, Config{
		Prefix: "/registry", Keyspace: "checkpoint-close-test", Identity: "peer-a", EnableEtcdCompatibility: true,
	}, recorder).(*backend)
	b.stopWorkers()
	b.serializableCheckpoint.Store(&SerializableCheckpoint{Revision: 7, Timestamp: 11, ValidUntil: time.Now().Add(time.Minute)})

	err := b.Close()
	require.ErrorIs(t, err, releaseErr)
	require.ErrorIs(t, b.Close(), releaseErr, "Close must retain the first shutdown result")
	require.Len(t, store.releasedIDs, 2, "both alternating PD service safepoints must be released")
	require.Equal(t, 1, store.closeCalls, "storage client must close despite checkpoint release failure")
	require.Contains(t, recorder.snapshot(), compactMetricRecord{
		kind: "counter", name: "serializable.checkpoint.release_err", value: 1,
	})
}

func TestBackendCloseReleasesUnpublishedSerializableCheckpointSlots(t *testing.T) {
	releaseErr := errors.New("injected unpublished checkpoint release failure")
	store := &checkpointTestStorage{KvStorage: memkv.NewKvStorage(), releaseErr: releaseErr}
	recorder := &compactMetricRecorder{}
	b := NewBackend(store, Config{
		Prefix: "/registry", Keyspace: "checkpoint-unpublished-close-test", Identity: "peer-a", EnableEtcdCompatibility: true,
	}, recorder).(*backend)
	b.stopWorkers()
	b.SetLeadershipFence(func() (uint64, bool) { return 2, false })
	err := b.protectSerializableCheckpoint(
		WithLeadershipEpoch(context.Background(), 1),
		SerializableCheckpoint{Revision: 5, Timestamp: 200},
	)
	require.ErrorIs(t, err, ErrLeadershipFenced)
	require.ErrorIs(t, err, releaseErr)
	require.Contains(t, recorder.snapshot(), compactMetricRecord{
		kind: "counter", name: "serializable.checkpoint.release_err", value: 1,
	})

	store.mu.Lock()
	store.releaseErr = nil
	store.mu.Unlock()

	require.NoError(t, b.Close())
	store.mu.Lock()
	releasedIDs := append([]string(nil), store.releasedIDs...)
	store.mu.Unlock()
	require.Equal(t, []string{
		b.serializableCheckpointServiceIDs[0], b.serializableCheckpointServiceIDs[0],
	}, releasedIDs, "shutdown must retry a failed compensation even when the checkpoint was never published locally")
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

func TestSerializableCheckpointBackgroundRefreshWaitsForLeadershipFence(t *testing.T) {
	store := &checkpointTestStorage{KvStorage: memkv.NewKvStorage(), readyTimestamp: 300}
	recorder := &compactMetricRecorder{}
	b := NewBackend(store, Config{
		Prefix: "/registry", Keyspace: "checkpoint-unfenced-test", Identity: "peer-a", EnableEtcdCompatibility: true,
	}, recorder).(*backend)
	b.stopWorkers()
	t.Cleanup(func() { require.NoError(t, b.Close()) })
	b.SetCurrentRevision(17)

	err := b.refreshSerializableCheckpoint(context.Background())
	require.ErrorIs(t, err, storage.ErrKeyNotFound,
		"an unconfigured background worker must load a leader-owned checkpoint, never create one")
	_, loadErr := b.loadSerializableCheckpoint(context.Background())
	require.ErrorIs(t, loadErr, storage.ErrKeyNotFound)
	require.Empty(t, store.protections)
}

func TestSerializableCheckpointBackgroundRefreshWaitsForLeadershipLifecycle(t *testing.T) {
	store := &checkpointTestStorage{KvStorage: memkv.NewKvStorage(), readyTimestamp: 300}
	recorder := &compactMetricRecorder{}
	b := NewBackend(store, Config{
		Prefix: "/registry", Keyspace: "checkpoint-no-term-test", Identity: "peer-a", EnableEtcdCompatibility: true,
	}, recorder).(*backend)
	b.stopWorkers()
	t.Cleanup(func() { require.NoError(t, b.Close()) })
	b.SetCurrentRevision(17)
	b.SetLeadershipFence(func() (uint64, bool) { return 1, true })

	err := b.refreshSerializableCheckpoint(context.Background())
	require.ErrorIs(t, err, storage.ErrKeyNotFound,
		"fresh election state must not create until its exact lifecycle context is registered")
	_, loadErr := b.loadSerializableCheckpoint(context.Background())
	require.ErrorIs(t, loadErr, storage.ErrKeyNotFound)
	require.Empty(t, store.protections)
}

func TestSerializableCheckpointActiveLeaderBackgroundRefreshPublishes(t *testing.T) {
	store := &checkpointTestStorage{KvStorage: memkv.NewKvStorage(), readyTimestamp: 300}
	recorder := &compactMetricRecorder{}
	b := NewBackend(store, Config{
		Prefix: "/registry", Keyspace: "checkpoint-active-term-test", Identity: "peer-a", EnableEtcdCompatibility: true,
	}, recorder).(*backend)
	b.stopWorkers()
	t.Cleanup(func() { require.NoError(t, b.Close()) })
	b.SetCurrentRevision(17)
	b.SetLeadershipFence(func() (uint64, bool) { return 1, true })
	require.NoError(t, b.ResumePhysicalCompaction(context.Background()))

	require.NoError(t, b.refreshSerializableCheckpoint(context.Background()))
	checkpoint, err := b.GetSerializableCheckpoint()
	require.NoError(t, err)
	require.Equal(t, uint64(17), checkpoint.Revision)
	require.Equal(t, uint64(300), checkpoint.Timestamp)
	loaded, err := b.loadSerializableCheckpoint(context.Background())
	require.NoError(t, err)
	require.Equal(t, checkpoint.Timestamp, loaded.Timestamp)
	require.NotEmpty(t, store.protections)
}

func TestSerializableCheckpointLeaderRefreshStopsWithLeadershipContext(t *testing.T) {
	base := &checkpointTestStorage{KvStorage: memkv.NewKvStorage(), readyTimestamp: 300}
	store := &blockingCheckpointReadinessStorage{
		checkpointTestStorage: base,
		entered:               make(chan struct{}),
		release:               make(chan struct{}),
	}
	recorder := &compactMetricRecorder{}
	b := NewBackend(store, Config{
		Prefix: "/registry", Keyspace: "checkpoint-leadership-test", Identity: "peer-a", EnableEtcdCompatibility: true,
	}, recorder).(*backend)
	b.stopWorkers()
	t.Cleanup(func() { require.NoError(t, b.Close()) })
	b.SetCurrentRevision(17)
	var leading atomic.Bool
	leading.Store(true)
	b.SetLeadershipFence(func() (uint64, bool) { return 1, leading.Load() })
	leaderCtx, stopLeader := context.WithCancel(context.Background())
	b.compactCtx.Store(compactContextHolder{ctx: leaderCtx, termOwned: true})
	store.armed.Store(true)

	done := make(chan error, 1)
	go func() { done <- b.refreshSerializableCheckpoint(context.Background()) }()
	<-store.entered
	leading.Store(false)
	stopLeader()
	select {
	case err := <-done:
		require.NoError(t, err, "normal leadership retirement must not become a refresh failure")
	case <-time.After(250 * time.Millisecond):
		close(store.release)
		<-done
		t.Fatal("serializable checkpoint refresh outlived its leadership context")
	}
	require.NotContains(t, recorder.snapshot(), compactMetricRecord{
		kind: "counter", name: "serializable.checkpoint.refresh_err", value: 1,
	})
	require.NotContains(t, recorder.snapshot(), compactMetricRecord{
		kind: "counter", name: "serializable.checkpoint.release_err", value: 1,
	})
	_, err := b.GetSerializableCheckpoint()
	require.ErrorIs(t, err, ErrSerializableCheckpointUnavailable,
		"a retired leader must not publish a local checkpoint")
}

func TestSerializableCheckpointRetiredLeaderSuppressesLateProtection(t *testing.T) {
	base := &checkpointTestStorage{KvStorage: memkv.NewKvStorage(), readyTimestamp: 300}
	store := &blockingCheckpointProtectionStorage{
		checkpointTestStorage: base,
		entered:               make(chan struct{}),
		release:               make(chan struct{}),
	}
	recorder := &compactMetricRecorder{}
	b := NewBackend(store, Config{
		Prefix: "/registry", Keyspace: "checkpoint-late-protection-test", Identity: "peer-a", EnableEtcdCompatibility: true,
	}, recorder).(*backend)
	b.stopWorkers()
	t.Cleanup(func() { require.NoError(t, b.Close()) })
	b.SetCurrentRevision(17)
	var leading atomic.Bool
	leading.Store(true)
	b.SetLeadershipFence(func() (uint64, bool) { return 1, leading.Load() })
	leaderCtx, stopLeader := context.WithCancel(context.Background())
	b.compactCtx.Store(compactContextHolder{ctx: leaderCtx, termOwned: true})
	store.armed.Store(true)

	done := make(chan error, 1)
	go func() { done <- b.refreshSerializableCheckpoint(context.Background()) }()
	<-store.entered
	leading.Store(false)
	stopLeader()
	close(store.release)
	require.NoError(t, <-done)
	require.NotContains(t, recorder.snapshot(), compactMetricRecord{
		kind: "counter", name: "serializable.checkpoint.refresh_err", value: 1,
	})
	require.NotContains(t, recorder.snapshot(), compactMetricRecord{
		kind: "counter", name: "serializable.checkpoint.release_err", value: 1,
	})
	_, err := b.GetSerializableCheckpoint()
	require.ErrorIs(t, err, ErrSerializableCheckpointUnavailable,
		"a late protection success must not publish follower-owned availability")
	store.mu.Lock()
	protections := append([]checkpointProtection(nil), store.protections...)
	releasedIDs := append([]string(nil), store.releasedIDs...)
	store.mu.Unlock()
	require.Len(t, protections, 1)
	require.Equal(t, []string{protections[0].id}, releasedIDs,
		"a retired leader must compensate the unpublished PD service safepoint immediately")
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
	compactKey := getCompactKey(b.config.Prefix)
	require.Equal(t, [][]byte{b.ks.ObjectKeyspaceStart(), compactKey}, store.readinessStarts)
	require.Equal(t, [][]byte{b.ks.ObjectKeyspaceEnd(), append(append([]byte(nil), compactKey...), 0)}, store.readinessEnds)
}

func TestSerializableCheckpointWaitsForDurableWatermarkVisibility(t *testing.T) {
	for _, present := range []bool{false, true} {
		name := "absent at safe timestamp"
		if present {
			name = "present but empty is corrupt"
		}
		t.Run(name, func(t *testing.T) {
			store := &checkpointTestStorage{KvStorage: memkv.NewKvStorage(), readyTimestamp: 300}
			b := newCheckpointBackend(t, store)
			b.SetCurrentRevision(1)
			durableKey := string(b.ks.EncodeInternalKey(durableRevisionKey))
			store.snapshotValues = map[string][]byte{}
			if present {
				store.snapshotValues[durableKey] = nil
			}

			checkpoint, err := b.createSerializableCheckpoint(context.Background())
			require.Equal(t, SerializableCheckpoint{}, checkpoint)
			if present {
				require.ErrorIs(t, err, ErrInvalidMVCCMetadata)
				require.NotErrorIs(t, err, ErrSerializableCheckpointUnavailable)
			} else {
				require.ErrorIs(t, err, ErrSerializableCheckpointUnavailable)
				require.NotErrorIs(t, err, ErrInvalidMVCCMetadata)
			}
			// The latest marker exists, but it must not stand in for the marker
			// at the common safe timestamp, or admit an unverified checkpoint.
			current, err := b.GetDurableRevision(context.Background())
			require.NoError(t, err)
			require.Equal(t, uint64(1), current)
			_, err = b.kv.Get(context.Background(), b.ks.EncodeInternalKey(serializableCheckpointKey))
			require.ErrorIs(t, err, storage.ErrKeyNotFound)

			if !present {
				store.readyTimestamp = 301
				store.snapshotValues[durableKey] = uint64ToBytes(1)
				checkpoint, err = b.createSerializableCheckpoint(context.Background())
				require.NoError(t, err)
				require.Equal(t, SerializableCheckpoint{Revision: 1, Timestamp: 301}, checkpoint)
				require.Equal(t, []uint64{300, 301}, store.batchTimestamps)
			}
		})
	}
}

func TestSerializableCheckpointUsesCommonObjectAndCompactRegionSafeTimestamp(t *testing.T) {
	store := &checkpointTestStorage{KvStorage: memkv.NewKvStorage(), readyTimestamps: []uint64{320, 300}}
	b := newCheckpointBackend(t, store)
	b.SetCurrentRevision(9)
	store.snapshotValues = map[string][]byte{
		string(b.ks.EncodeInternalKey(durableRevisionKey)):    uint64ToBytes(7),
		string(getCompactKey(b.config.Prefix)):                uint64ToBytes(3),
		string(b.ks.EncodeInternalKey([]byte("auth/config"))): append([]byte{1}, uint64ToBytes(5)...),
	}

	c, err := b.createSerializableCheckpoint(context.Background())
	require.NoError(t, err)
	require.Equal(t, uint64(300), c.Timestamp)
	require.Equal(t, []uint64{300}, store.batchTimestamps)
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
	require.Equal(t, 4, store.warmReads)
	require.Equal(t, 1, store.warmerCalls)
	store.mu.Unlock()
	_, err := b.GetSerializableCheckpoint()
	require.NoError(t, err)
}

func TestSerializableCheckpointRevalidatesTopologyAfterWarmBeforePublication(t *testing.T) {
	base := &checkpointTestStorage{KvStorage: memkv.NewKvStorage()}
	store := &publicationValidatingCheckpointStorage{
		checkpointTestStorage: base,
		publicationTimestamps: []uint64{200, 199},
	}
	b := newCheckpointBackend(t, store)
	err := b.protectSerializableCheckpoint(
		context.Background(), SerializableCheckpoint{Revision: 5, Timestamp: 200},
	)
	require.ErrorIs(t, err, ErrSerializableCheckpointUnavailable)
	_, err = b.GetSerializableCheckpoint()
	require.ErrorIs(t, err, ErrSerializableCheckpointUnavailable)

	base.mu.Lock()
	protections := append([]checkpointProtection(nil), base.protections...)
	releasedIDs := append([]string(nil), base.releasedIDs...)
	base.mu.Unlock()
	require.Len(t, protections, 1, "GC protection must precede the final publication fence")
	require.Equal(t, uint64(200), protections[0].timestamp)
	require.Equal(t, []string{protections[0].id}, releasedIDs,
		"a candidate rejected by the final publication fence must not retain a PD GC pin")
	compactKey := getCompactKey(b.config.Prefix)
	require.Equal(t, [][]byte{b.ks.ObjectKeyspaceStart(), compactKey}, store.publicationStarts)
	require.Equal(t, [][]byte{
		b.ks.ObjectKeyspaceEnd(), append(append([]byte(nil), compactKey...), 0),
	}, store.publicationEnds)
}

func TestSerializableCheckpointPublicationFailureRetainsPreviousGeneration(t *testing.T) {
	base := &checkpointTestStorage{KvStorage: memkv.NewKvStorage()}
	store := &publicationValidatingCheckpointStorage{
		checkpointTestStorage: base,
		publicationTimestamps: []uint64{300, 300, 300, 200},
	}
	b := newCheckpointBackend(t, store)
	first := SerializableCheckpoint{Revision: 5, Timestamp: 200}
	require.NoError(t, b.protectSerializableCheckpoint(context.Background(), first))
	b.serializableCheckpointSwitchedAt = time.Now().Add(-serializableCheckpointProtectionGrace)
	b.serializableCheckpointRegionsWarmedAt.Store(0)

	err := b.protectSerializableCheckpoint(
		context.Background(), SerializableCheckpoint{Revision: 6, Timestamp: 201},
	)
	require.ErrorIs(t, err, ErrSerializableCheckpointUnavailable)
	served, serveErr := b.GetSerializableCheckpoint()
	require.NoError(t, serveErr)
	require.Equal(t, first.Timestamp, served.Timestamp)
	base.mu.Lock()
	protections := append([]checkpointProtection(nil), base.protections...)
	releasedIDs := append([]string(nil), base.releasedIDs...)
	base.mu.Unlock()
	require.Len(t, protections, 2)
	require.NotEqual(t, protections[0].id, protections[1].id)
	require.Equal(t, []string{protections[1].id}, releasedIDs,
		"a rejected next generation must be removed without releasing the served generation")
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
		{kind: "counter", name: "serializable.checkpoint.refresh_err", value: int64(0)},
		{kind: "counter", name: "serializable.checkpoint.release_err", value: int64(0)},
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

func TestSerializableCheckpointContextSeparatesProtectedFromOrdinaryPinnedSnapshot(t *testing.T) {
	ordinary := storage.WithSnapshotTimestamp(context.Background(), 200)
	_, pinned := storage.SnapshotTimestampFromContext(ordinary)
	require.True(t, pinned)
	require.False(t, storage.ProtectedSnapshotFromContext(ordinary))

	checkpoint := SerializableCheckpoint{Revision: 5, Timestamp: 200}
	protected := WithSerializableCheckpoint(context.Background(), checkpoint)
	timestamp, pinned := storage.SnapshotTimestampFromContext(protected)
	require.True(t, pinned)
	require.Equal(t, checkpoint.Timestamp, timestamp)
	require.True(t, storage.ProtectedSnapshotFromContext(protected))
	served, ok := SerializableCheckpointFromContext(protected)
	require.True(t, ok)
	require.Equal(t, checkpoint, served)
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
