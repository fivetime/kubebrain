// Copyright 2026 ByteDance and/or its affiliates
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package backend

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"

	"github.com/kubewharf/kubebrain/pkg/metrics/mock"
	"github.com/kubewharf/kubebrain/pkg/storage"
	imemkv "github.com/kubewharf/kubebrain/pkg/storage/memkv"
)

// gcRecordingKV wraps a KvStorage with a storage.GarbageCollector that records
// calls, standing in for the TiKV store.
type gcRecordingKV struct {
	storage.KvStorage
	calls    atomic.Int64
	lifetime atomic.Int64
}

func (g *gcRecordingKV) GC(ctx context.Context, lifetime time.Duration) (uint64, error) {
	g.calls.Add(1)
	g.lifetime.Store(int64(lifetime))
	return uint64(g.calls.Load()), nil
}

type lifecycleGCKV struct {
	storage.KvStorage
	calls     atomic.Int64
	successes atomic.Int64
	entered   chan struct{}
	canceled  chan struct{}
}

type failingGCKV struct {
	storage.KvStorage
	calls atomic.Int64
}

func (g *failingGCKV) GC(context.Context, time.Duration) (uint64, error) {
	g.calls.Add(1)
	return 0, errors.New("injected storage GC failure")
}

type lateSuccessGCKV struct {
	storage.KvStorage
	entered  chan struct{}
	returned chan struct{}
}

func (g *lateSuccessGCKV) GC(ctx context.Context, _ time.Duration) (uint64, error) {
	close(g.entered)
	<-ctx.Done()
	close(g.returned)
	return 42, nil
}

func (g *lifecycleGCKV) GC(ctx context.Context, _ time.Duration) (uint64, error) {
	call := g.calls.Add(1)
	if call == 1 {
		close(g.entered)
		<-ctx.Done()
		close(g.canceled)
		return 0, ctx.Err()
	}
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	return uint64(g.successes.Add(1)), nil
}

// TestStorageGCDriverAdvancesSafepoint pins #37: when the storage implements
// GarbageCollector and a lifetime is configured, the leader periodically calls
// GC with that lifetime — the gc_worker role on a bare PD+TiKV deployment,
// without which MVCC versions accumulate forever and reads degrade.
func TestStorageGCDriverAdvancesSafepoint(t *testing.T) {
	m := &compactMetricRecorder{}
	kv := &gcRecordingKV{KvStorage: imemkv.NewKvStorage()}
	defer func() { require.NoError(t, kv.Close()) }()

	lifetime := 60 * time.Millisecond
	b := NewBackend(kv, Config{
		Prefix: prefix, Identity: getStorageIdentity(),
		EnableEtcdCompatibility: true, StorageGCLifetime: lifetime,
	}, m).(*backend)
	b.SetCurrentRevision(uint64(time.Now().UnixNano()))

	// No fence registered => leadingFresh() is true (single-node), so the
	// driver runs. It must call GC repeatedly with the configured lifetime.
	require.Eventually(t, func() bool { return kv.calls.Load() >= 2 },
		5*time.Second, 10*time.Millisecond, "GC driver must fire periodically on the leader")
	require.Equal(t, int64(lifetime), kv.lifetime.Load(), "GC must receive the configured lifetime")
	require.Contains(t, m.snapshot(), compactMetricRecord{
		kind: "gauge", name: "storage.gc.enabled", value: int64(1),
	}, "a capable configured backend must publish enabled before the first GC tick")
	var started, succeeded bool
	for _, record := range m.snapshot() {
		value, ok := record.value.(int64)
		if !ok || value <= 0 {
			continue
		}
		started = started || record.name == "storage.gc.driver_started_timestamp_seconds"
		succeeded = succeeded || record.name == "storage.gc.last_success_timestamp_seconds"
	}
	require.True(t, started, "an enabled driver must publish its process-local start time")
	require.True(t, succeeded, "a successful GC call must publish a wall-clock success time")
}

func TestStorageGCMetricsInitializeAuthoritativeDisabledAndZeroError(t *testing.T) {
	recorder := &compactMetricRecorder{}
	initStorageGCMetrics(recorder)

	require.Equal(t, []compactMetricRecord{
		{kind: "gauge", name: "storage.gc.enabled", value: int64(0)},
		{kind: "gauge", name: "storage.gc.driver_started_timestamp_seconds", value: int64(0)},
		{kind: "gauge", name: "storage.gc.last_success_timestamp_seconds", value: int64(0)},
		{kind: "counter", name: "storage.gc.err", value: int64(0)},
	}, recorder.records)
}

// TestStorageGCDriverDisabledByZeroLifetime pins the off switch: lifetime 0
// must not drive GC even when the storage supports it.
func TestStorageGCDriverDisabledByZeroLifetime(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	m := mock.NewMinimalMetrics(ctrl)
	kv := &gcRecordingKV{KvStorage: imemkv.NewKvStorage()}
	defer func() { require.NoError(t, kv.Close()) }()

	NewBackend(kv, Config{
		Prefix: prefix, Identity: getStorageIdentity(),
		EnableEtcdCompatibility: true, // StorageGCLifetime unset
	}, m)
	time.Sleep(150 * time.Millisecond)
	require.Zero(t, kv.calls.Load(), "GC must not run when lifetime is 0")
}

func TestStorageGCTransfersAcrossLeadershipContexts(t *testing.T) {
	m := newRecordCounters()
	kv := &lifecycleGCKV{
		KvStorage: imemkv.NewKvStorage(),
		entered:   make(chan struct{}),
		canceled:  make(chan struct{}),
	}
	defer func() { require.NoError(t, kv.Close()) }()

	const lifetime = 50 * time.Millisecond
	b := NewBackend(kv, Config{
		Prefix: prefix, Identity: getStorageIdentity(),
		EnableEtcdCompatibility: true, StorageGCLifetime: lifetime,
	}, m).(*backend)
	defer stopBackendWorkersForTest(b)
	var epoch atomic.Uint64
	var leading atomic.Bool
	epoch.Store(1)
	leading.Store(true)
	b.SetLeadershipFence(func() (uint64, bool) { return epoch.Load(), leading.Load() })

	oldCtx, stopOldLeader := context.WithCancel(context.Background())
	require.NoError(t, b.ResumePhysicalCompaction(oldCtx))
	<-kv.entered
	leading.Store(false)
	stopOldLeader()
	<-kv.canceled
	time.Sleep(2 * lifetime)
	require.Zero(t, kv.successes.Load(), "a follower must not advance the storage GC safepoint")
	require.Zero(t, m.get("storage.gc.err"),
		"leadership cancellation is normal lifecycle retirement, not a storage GC failure")

	epoch.Store(2)
	leading.Store(true)
	newCtx, stopNewLeader := context.WithCancel(context.Background())
	defer stopNewLeader()
	require.NoError(t, b.ResumePhysicalCompaction(newCtx))
	require.Eventually(t, func() bool {
		return kv.successes.Load() > 0
	}, 2*time.Second, 10*time.Millisecond,
		"the new leader must resume safepoint advancement without restarting the driver")
}

func TestStorageGCStorageFailureRemainsObservable(t *testing.T) {
	m := newRecordCounters()
	kv := &failingGCKV{KvStorage: imemkv.NewKvStorage()}
	defer func() { require.NoError(t, kv.Close()) }()

	b := NewBackend(kv, Config{
		Prefix: prefix, Identity: getStorageIdentity(),
		EnableEtcdCompatibility: true, StorageGCLifetime: 20 * time.Millisecond,
	}, m).(*backend)
	defer stopBackendWorkersForTest(b)

	require.Eventually(t, func() bool {
		return kv.calls.Load() > 0 && m.get("storage.gc.err") > 0
	}, time.Second, 5*time.Millisecond,
		"an active leader's real storage GC failure must remain observable")
}

func TestStorageGCLateSuccessAfterLeadershipLossIsNotPublished(t *testing.T) {
	m := newRecordCounters()
	kv := &lateSuccessGCKV{
		KvStorage: imemkv.NewKvStorage(), entered: make(chan struct{}), returned: make(chan struct{}),
	}
	defer func() { require.NoError(t, kv.Close()) }()

	b := NewBackend(kv, Config{
		Prefix: prefix, Identity: getStorageIdentity(),
		EnableEtcdCompatibility: true, StorageGCLifetime: 20 * time.Millisecond,
	}, m).(*backend)
	defer stopBackendWorkersForTest(b)
	var leading atomic.Bool
	leading.Store(true)
	b.SetLeadershipFence(func() (uint64, bool) { return 1, leading.Load() })
	leaderCtx, stopLeader := context.WithCancel(context.Background())
	require.NoError(t, b.ResumePhysicalCompaction(leaderCtx))

	<-kv.entered
	leading.Store(false)
	stopLeader()
	<-kv.returned
	time.Sleep(2 * b.config.StorageGCLifetime)
	require.Zero(t, m.get("storage.gc.err"))
	require.Zero(t, m.gauge("storage.gc.safepoint"),
		"a deposed leader must not publish a late GC success")
	require.Zero(t, m.gauge("storage.gc.last_success_timestamp_seconds"),
		"a deposed leader must not refresh the success timestamp")
}
