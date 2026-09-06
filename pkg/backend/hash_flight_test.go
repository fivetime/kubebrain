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

type hashKVFlightTestResult struct {
	result HashKVResult
	err    error
}

func newGatedHashKVFlightBackend(
	t *testing.T, started chan struct{}, release chan struct{},
) (*backend, *hashKVPartitionTestStorage, context.Context, HashKVResult, int) {
	t.Helper()
	ctrl := gomock.NewController(t)
	store := &hashKVPartitionTestStorage{KvStorage: memkv.NewKvStorage()}
	b := NewBackend(store, Config{
		Prefix: prefix, Identity: getStorageIdentity(), EnableEtcdCompatibility: true,
	}, metricsmock.NewMinimalMetrics(ctrl)).(*backend)
	t.Cleanup(func() { require.NoError(t, b.Close()) })
	b.SetCurrentRevision(uint64(time.Now().UnixNano()))
	ctx := context.Background()
	created, err := b.Create(ctx, &proto.CreateRequest{
		Key: []byte(prefix + "/hash/flight"), Value: []byte("value"),
	})
	require.NoError(t, err)
	waitCommitted(t, b, created.Header.Revision)
	want, err := b.HashKV(ctx, 0)
	require.NoError(t, err)

	store.mu.Lock()
	baselinePartitionCalls := store.partitionCalls
	store.partitionCalls = 0
	store.gatedStarts = map[string]struct{}{
		string(b.ks.HashKVScanRanges()[0].Start): {},
	}
	store.started = started
	store.release = release
	store.mu.Unlock()
	require.Positive(t, baselinePartitionCalls)
	return b, store, ctx, want, baselinePartitionCalls
}

func hashKVFlightWaiters(b *backend, key hashKVFlightKey) int {
	b.hashKVFlights.mu.Lock()
	defer b.hashKVFlights.mu.Unlock()
	if call := b.hashKVFlights.calls[key]; call != nil {
		return call.waiters
	}
	return 0
}

func hashKVFlightCount(b *backend) int {
	b.hashKVFlights.mu.Lock()
	defer b.hashKVFlights.mu.Unlock()
	return len(b.hashKVFlights.calls)
}

func TestHashKVFlightKeySeparatesSnapshotExecutionContracts(t *testing.T) {
	b, ctx := newTxnApplyBackend(t)
	originalRevision := b.GetCurrentRevision()
	base := b.newHashKVFlightKey(ctx, 7)
	tests := []struct {
		name string
		ctx  context.Context
		rev  int64
	}{
		{name: "requested revision", ctx: ctx, rev: 8},
		{name: "snapshot timestamp", ctx: storage.WithSnapshotTimestamp(ctx, 100), rev: 7},
		{name: "protected snapshot", ctx: storage.WithProtectedSnapshotTimestamp(ctx, 100), rev: 7},
		{name: "iterator fallback", ctx: storage.WithSnapshotIteratorFallback(ctx), rev: 7},
		{name: "scan batch", ctx: storage.WithScanBatchSize(ctx, 128), rev: 7},
		{name: "checkpoint", ctx: WithSerializableCheckpoint(ctx, SerializableCheckpoint{
			Revision: 42, Timestamp: 100, CompactRevision: 11,
		}), rev: 7},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.NotEqual(t, base, b.newHashKVFlightKey(tt.ctx, tt.rev))
		})
	}

	b.SetCurrentRevision(originalRevision + 1)
	require.NotEqual(t, base, b.newHashKVFlightKey(ctx, 7),
		"a caller arriving after revision advancement must use a new flight")
}

func TestHashKVCoalescesConcurrentSameSnapshot(t *testing.T) {
	const callers = 12
	started := make(chan struct{}, callers)
	release := make(chan struct{})
	var releaseOnce sync.Once
	t.Cleanup(func() { releaseOnce.Do(func() { close(release) }) })
	b, store, ctx, want, baselineCalls := newGatedHashKVFlightBackend(t, started, release)
	key := b.newHashKVFlightKey(ctx, 0)
	results := make(chan hashKVFlightTestResult, callers)

	go func() {
		result, err := b.HashKV(ctx, 0)
		results <- hashKVFlightTestResult{result: result, err: err}
	}()
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("the executing HashKV did not reach its first iterator")
	}
	for range callers - 1 {
		go func() {
			result, err := b.HashKV(ctx, 0)
			results <- hashKVFlightTestResult{result: result, err: err}
		}()
	}
	require.Eventually(t, func() bool {
		return hashKVFlightWaiters(b, key) == callers
	}, time.Second, time.Millisecond)
	releaseOnce.Do(func() { close(release) })

	for range callers {
		got := <-results
		require.NoError(t, got.err)
		require.Equal(t, want, got.result)
	}
	store.mu.Lock()
	partitionCalls := store.partitionCalls
	store.mu.Unlock()
	require.Equal(t, baselineCalls, partitionCalls,
		"an identical concurrent HashKV herd must issue one physical scan")
	require.Zero(t, hashKVFlightCount(b))
}

func TestHashKVDoesNotCoalesceAcrossObservedRevisionAdvance(t *testing.T) {
	started := make(chan struct{}, 2)
	release := make(chan struct{})
	var releaseOnce sync.Once
	t.Cleanup(func() { releaseOnce.Do(func() { close(release) }) })
	b, store, ctx, _, baselineCalls := newGatedHashKVFlightBackend(t, started, release)
	originalRevision := b.GetCurrentRevision()
	results := make(chan hashKVFlightTestResult, 2)

	go func() {
		result, err := b.HashKV(ctx, 0)
		results <- hashKVFlightTestResult{result: result, err: err}
	}()
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("the first HashKV did not reach its iterator")
	}
	b.SetCurrentRevision(originalRevision + 1)
	go func() {
		result, err := b.HashKV(ctx, 0)
		results <- hashKVFlightTestResult{result: result, err: err}
	}()
	require.Eventually(t, func() bool {
		return hashKVFlightCount(b) == 2
	}, time.Second, time.Millisecond)
	releaseOnce.Do(func() { close(release) })

	first, second := <-results, <-results
	require.NoError(t, first.err)
	require.NoError(t, second.err)
	if first.result.CurrentRevision > second.result.CurrentRevision {
		first, second = second, first
	}
	require.Equal(t, int64(originalRevision), first.result.CurrentRevision)
	require.Equal(t, int64(originalRevision+1), second.result.CurrentRevision)
	store.mu.Lock()
	partitionCalls := store.partitionCalls
	store.mu.Unlock()
	require.Equal(t, baselineCalls*2, partitionCalls,
		"a caller that observes a newer revision must run a fresh physical scan")
	require.Zero(t, hashKVFlightCount(b))
}

func TestHashKVFlightRetiresBeforeConcurrentCompactionCompletes(t *testing.T) {
	started := make(chan struct{}, 1)
	release := make(chan struct{})
	var releaseOnce sync.Once
	t.Cleanup(func() { releaseOnce.Do(func() { close(release) }) })
	b, _, ctx, _, _ := newGatedHashKVFlightBackend(t, started, release)
	target := b.GetCurrentRevision()
	hashDone := make(chan hashKVFlightTestResult, 1)
	compactDone := make(chan error, 1)

	go func() {
		result, err := b.HashKV(ctx, 0)
		hashDone <- hashKVFlightTestResult{result: result, err: err}
	}()
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("the executing HashKV did not reach its first iterator")
	}
	go func() {
		advanced, err := b.setCompactRecord(ctx, target)
		if err == nil && !advanced {
			err = errors.New("compact watermark did not advance")
		}
		compactDone <- err
	}()
	select {
	case err := <-compactDone:
		t.Fatalf("compaction crossed an active HashKV flight: %v", err)
	case <-time.After(20 * time.Millisecond):
	}

	releaseOnce.Do(func() { close(release) })
	hashed := <-hashDone
	require.NoError(t, hashed.err)
	require.NoError(t, <-compactDone)
	require.Zero(t, hashKVFlightCount(b),
		"the old flight must be removed before compaction can report success")

	after, err := b.HashKV(ctx, 0)
	require.NoError(t, err)
	require.Equal(t, int64(target), after.CompactRevision,
		"a caller after compaction must observe the new compact watermark")
}

func TestHashKVWaiterRetriesAfterExecutorCancellation(t *testing.T) {
	started := make(chan struct{}, 2)
	release := make(chan struct{})
	var releaseOnce sync.Once
	t.Cleanup(func() { releaseOnce.Do(func() { close(release) }) })
	b, _, ctx, want, _ := newGatedHashKVFlightBackend(t, started, release)
	key := b.newHashKVFlightKey(ctx, 0)
	executorCtx, cancelExecutor := context.WithCancel(ctx)
	executorDone := make(chan hashKVFlightTestResult, 1)
	waiterDone := make(chan hashKVFlightTestResult, 1)

	go func() {
		result, err := b.HashKV(executorCtx, 0)
		executorDone <- hashKVFlightTestResult{result: result, err: err}
	}()
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("the executing HashKV did not reach its first iterator")
	}
	go func() {
		result, err := b.HashKV(ctx, 0)
		waiterDone <- hashKVFlightTestResult{result: result, err: err}
	}()
	require.Eventually(t, func() bool {
		return hashKVFlightWaiters(b, key) == 2
	}, time.Second, time.Millisecond)
	cancelExecutor()
	executor := <-executorDone
	require.ErrorIs(t, executor.err, context.Canceled)
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("the live waiter did not retry under its own context")
	}
	releaseOnce.Do(func() { close(release) })
	waiter := <-waiterDone
	require.NoError(t, waiter.err)
	require.Equal(t, want, waiter.result)
	require.Zero(t, hashKVFlightCount(b))
}
