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
	"bytes"
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"

	proto "github.com/kubewharf/kubebrain-client/api/v2rpc"

	"github.com/kubewharf/kubebrain/pkg/metrics/mock"
	"github.com/kubewharf/kubebrain/pkg/storage"
	imemkv "github.com/kubewharf/kubebrain/pkg/storage/memkv"
)

// gatedIterKV counts Iter calls whose start key contains gateSubstr and blocks
// them until release is closed, delegating everything else straight through. It
// lets a test hold the first history scan open while a herd of callers piles up,
// so the singleflight collapse is observable deterministically.
type gatedIterKV struct {
	storage.KvStorage
	gateSubstr []byte
	iterCount  int32
	release    chan struct{}
}

func (g *gatedIterKV) Iter(ctx context.Context, start []byte, end []byte, timestamp uint64, limit uint64) (storage.Iter, error) {
	if len(g.gateSubstr) > 0 && bytes.Contains(start, g.gateSubstr) {
		atomic.AddInt32(&g.iterCount, 1)
		select {
		case <-g.release:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	return g.KvStorage.Iter(ctx, start, end, timestamp, limit)
}

// TestHistoryScanHerdSharesOneScan pins #30: a reconnect herd of watchers that
// all fall to the storage history fallback for the SAME prefix must share a
// single scan (singleflight), not issue one scan each. The gated storage blocks
// the first scan so all N callers register on the group before it completes;
// exactly one Iter of the prefix must occur, and every caller must get the
// correct events.
func TestHistoryScanHerdSharesOneScan(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	m := mock.NewMinimalMetrics(ctrl)
	gkv := &gatedIterKV{KvStorage: imemkv.NewKvStorage(), release: make(chan struct{})}
	b := NewBackend(gkv, Config{Prefix: prefix, Identity: getStorageIdentity()}, m).(*backend)
	defer func() { require.NoError(t, gkv.Close()) }()
	b.SetCurrentRevision(uint64(time.Now().UnixNano()))
	ctx := context.Background()

	baseKey := prefix + "/herd"
	keyA := baseKey + "/a"
	createA, err := b.Create(ctx, newCreateRequest(keyA, "a1"))
	require.NoError(t, err)
	updateA, err := b.Update(ctx, &proto.UpdateRequest{Kv: &proto.KeyValue{Key: []byte(keyA), Value: []byte("a2"), Revision: createA.Header.Revision}})
	require.NoError(t, err)
	require.True(t, updateA.Succeeded)
	waitUntilRevisionEqualOrTimeout(b, updateA.Header.Revision)
	cur := b.GetCurrentRevision()

	// Force the low-cache history path: the ring holds only the latest event, so
	// a watch starting at createA misses it and falls to storage.
	b.watchCache.Reset()
	b.watchCache.Add(newEvent(proto.Event_PUT, updateA.Header.Revision, newKeyValue(keyA, "a2", updateA.Header.Revision)))

	// Gate the history scan of baseKey so every caller piles onto the group.
	gkv.gateSubstr, _ = b.historyPrefixBounds([]byte(baseKey))

	const N = 50
	results := make([][]*proto.Event, N)
	errs := make([]error, N)
	var wg sync.WaitGroup
	for i := 0; i < N; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			evs, e := b.historyWatchEvents(ctx, baseKey, createA.Header.Revision, cur, 0)
			results[idx], errs[idx] = evs, e
		}(i)
	}
	// Give all N goroutines time to register on the singleflight, then let the
	// one in-flight scan complete.
	time.Sleep(300 * time.Millisecond)
	close(gkv.release)
	wg.Wait()

	require.EqualValues(t, 1, atomic.LoadInt32(&gkv.iterCount),
		"a herd of %d watchers on one prefix must share ONE storage scan", N)
	for i := 0; i < N; i++ {
		require.NoErrorf(t, errs[i], "watcher %d", i)
		// Both versions (CREATE createA + PUT updateA) are at/above createA's
		// revision, so each watcher recovers exactly two events.
		require.Lenf(t, results[i], 2, "watcher %d event count", i)
		require.EqualValues(t, createA.Header.Revision, results[i][0].Revision)
		require.EqualValues(t, updateA.Header.Revision, results[i][1].Revision)
	}
}

// TestHistoryScanBucketSharesNearbyRevisions pins the revision bucketing (#30):
// two watchers reconnecting to the same prefix at DIFFERENT revisions that fall
// in the same HistoryScanRevBucket — HA-apiserver replicas relisting at slightly
// different revisions — must still share ONE storage scan, and each must receive
// only the events at or after its own revision.
func TestHistoryScanBucketSharesNearbyRevisions(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	m := mock.NewMinimalMetrics(ctrl)
	gkv := &gatedIterKV{KvStorage: imemkv.NewKvStorage(), release: make(chan struct{})}
	b := NewBackend(gkv, Config{Prefix: prefix, Identity: getStorageIdentity()}, m).(*backend)
	defer func() { require.NoError(t, gkv.Close()) }()

	const bucket = 4096
	b.config.HistoryScanRevBucket = bucket
	// Anchor at a bucket-aligned base so the two writes land in one bucket.
	base := uint64(bucket) * 1000000
	b.SetCurrentRevision(base)
	ctx := context.Background()

	baseKey := prefix + "/bucket"
	keyA := baseKey + "/a"
	createA, err := b.Create(ctx, newCreateRequest(keyA, "a1"))
	require.NoError(t, err)
	updateA, err := b.Update(ctx, &proto.UpdateRequest{Kv: &proto.KeyValue{Key: []byte(keyA), Value: []byte("a2"), Revision: createA.Header.Revision}})
	require.NoError(t, err)
	require.True(t, updateA.Succeeded)
	waitUntilRevisionEqualOrTimeout(b, updateA.Header.Revision)
	cur := b.GetCurrentRevision()
	r1, r2 := createA.Header.Revision, updateA.Header.Revision
	require.NotEqual(t, r1, r2)
	require.Equal(t, r1/bucket, r2/bucket, "test setup: both revisions must share a bucket")

	gkv.gateSubstr, _ = b.historyPrefixBounds([]byte(baseKey))

	// Two callers at DIFFERENT revisions (r1, r2) within the same bucket.
	type res struct {
		evs []*proto.Event
		err error
	}
	out := make([]res, 2)
	froms := []uint64{r1, r2}
	var wg sync.WaitGroup
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			e, err := b.historyWatchEvents(ctx, baseKey, froms[idx], cur, 0)
			out[idx] = res{e, err}
		}(i)
	}
	time.Sleep(200 * time.Millisecond)
	close(gkv.release)
	wg.Wait()

	require.EqualValues(t, 1, atomic.LoadInt32(&gkv.iterCount),
		"nearby-revision reconnects in one bucket must share ONE storage scan")
	require.NoError(t, out[0].err)
	require.NoError(t, out[1].err)
	require.Len(t, out[0].evs, 2, "caller@r1 recovers createA + updateA")
	require.Len(t, out[1].evs, 1, "caller@r2 recovers only updateA (its own revision filters out createA)")
	require.EqualValues(t, r2, out[1].evs[0].Revision)
}

// setupMidScanWrite builds the shared-scan-under-writes scenario: keyA exists at
// r1; the executor's history scan is gated open at snapshot c1; while it is gated
// a second event keyB is committed at r2 (> c1); a late waiter that needs
// coverage up to r2 joins the same bucket. It returns after both keyA and keyB
// are committed, with the executor still gated and the waiter not yet launched.
func setupMidScanWrite(t *testing.T, b *backend, gkv *gatedIterKV) (baseKey string, r1, c1, r2, c2 uint64, launchExecutor func() (*[]*proto.Event, *error, *sync.WaitGroup)) {
	ctx := context.Background()
	baseKey = prefix + "/guard"
	keyA := baseKey + "/a"
	createA, err := b.Create(ctx, newCreateRequest(keyA, "a1"))
	require.NoError(t, err)
	waitUntilRevisionEqualOrTimeout(b, createA.Header.Revision)
	r1 = createA.Header.Revision
	c1 = b.GetCurrentRevision()
	gkv.gateSubstr, _ = b.historyPrefixBounds([]byte(baseKey))

	launchExecutor = func() (*[]*proto.Event, *error, *sync.WaitGroup) {
		var evs []*proto.Event
		var e error
		var wg sync.WaitGroup
		wg.Add(1)
		go func() { defer wg.Done(); evs, e = b.historyWatchEvents(ctx, baseKey, r1, c1, 0) }()
		return &evs, &e, &wg
	}
	return
}

// TestHistoryScanRingExtendCoversMidScanWrites pins the ring extension (#30): a
// shared scan snapshotted at c1 while writes continue must still cover a later
// event r2 for a waiter that joins mid-scan — WITHOUT that waiter re-scanning —
// because the executor extends coverage from the warm ring (where the collector
// has already placed r2). One Iter total; both callers see keyB; no event loss.
func TestHistoryScanRingExtendCoversMidScanWrites(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	m := mock.NewMinimalMetrics(ctrl)
	gkv := &gatedIterKV{KvStorage: imemkv.NewKvStorage(), release: make(chan struct{})}
	b := NewBackend(gkv, Config{Prefix: prefix, Identity: getStorageIdentity()}, m).(*backend)
	defer func() { require.NoError(t, gkv.Close()) }()
	b.SetCurrentRevision(uint64(time.Now().UnixNano()))
	ctx := context.Background()

	baseKey, r1, c1, _, _, launchExec := setupMidScanWrite(t, b, gkv)
	execEvs, execErr, execDone := launchExec()
	time.Sleep(150 * time.Millisecond) // executor registers + gates

	// keyB commits after c1 while the scan is gated; the collector adds it to the
	// ring, so the executor's ring extension will recover it.
	createB, err := b.Create(ctx, newCreateRequest(baseKey+"/b", "b1"))
	require.NoError(t, err)
	waitUntilRevisionEqualOrTimeout(b, createB.Header.Revision)
	r2 := createB.Header.Revision
	require.Greater(t, r2, c1)
	c2 := b.GetCurrentRevision()

	var waitEvs []*proto.Event
	var waitErr error
	var waitDone sync.WaitGroup
	waitDone.Add(1)
	go func() { defer waitDone.Done(); waitEvs, waitErr = b.historyWatchEvents(ctx, baseKey, r1, c2, r2) }()
	time.Sleep(150 * time.Millisecond) // waiter joins the group

	close(gkv.release)
	execDone.Wait()
	waitDone.Wait()

	require.NoError(t, *execErr)
	require.NoError(t, waitErr)
	require.EqualValues(t, 1, atomic.LoadInt32(&gkv.iterCount),
		"ring extension must cover the mid-scan write so the waiter shares, not re-scans")
	require.Len(t, *execEvs, 2, "executor recovers keyB from the ring extension")
	require.Len(t, waitEvs, 2, "waiter shares the extended result — no lost event")
	require.EqualValues(t, r2, waitEvs[1].Revision)
}

// TestHistoryScanGuardReScansWhenRingCold pins the completeness guard's re-scan
// fallback: when the ring CANNOT cover the gap (cold/evicted), a waiter needing
// coverage past the shared snapshot must re-scan storage rather than silently
// drop the events. Same scenario, but the ring is reset before release so the
// extension finds nothing.
func TestHistoryScanGuardReScansWhenRingCold(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	m := mock.NewMinimalMetrics(ctrl)
	gkv := &gatedIterKV{KvStorage: imemkv.NewKvStorage(), release: make(chan struct{})}
	b := NewBackend(gkv, Config{Prefix: prefix, Identity: getStorageIdentity()}, m).(*backend)
	defer func() { require.NoError(t, gkv.Close()) }()
	b.SetCurrentRevision(uint64(time.Now().UnixNano()))
	ctx := context.Background()

	baseKey, r1, c1, _, _, launchExec := setupMidScanWrite(t, b, gkv)
	execEvs, execErr, execDone := launchExec()
	time.Sleep(150 * time.Millisecond)

	createB, err := b.Create(ctx, newCreateRequest(baseKey+"/b", "b1"))
	require.NoError(t, err)
	waitUntilRevisionEqualOrTimeout(b, createB.Header.Revision)
	r2 := createB.Header.Revision
	require.Greater(t, r2, c1)
	c2 := b.GetCurrentRevision()
	// Force the ring cold so the extension can't cover (r2 falls out): the guard
	// must fall back to a storage re-scan.
	b.watchCache.Reset()

	var waitEvs []*proto.Event
	var waitErr error
	var waitDone sync.WaitGroup
	waitDone.Add(1)
	go func() { defer waitDone.Done(); waitEvs, waitErr = b.historyWatchEvents(ctx, baseKey, r1, c2, r2) }()
	time.Sleep(150 * time.Millisecond)

	close(gkv.release)
	execDone.Wait()
	waitDone.Wait()

	require.NoError(t, *execErr)
	require.NoError(t, waitErr)
	// Executor's snapshot excluded keyB and the ring was cold, so it saw only keyA.
	require.Len(t, *execEvs, 1, "executor's cold-ring result excludes the later event")
	require.EqualValues(t, r1, (*execEvs)[0].Revision)
	// The guard made the waiter re-scan and recover keyB — no lost event.
	require.Len(t, waitEvs, 2, "guard re-scan must recover the event the cold ring could not")
	require.EqualValues(t, r2, waitEvs[1].Revision)
	require.EqualValues(t, 2, atomic.LoadInt32(&gkv.iterCount),
		"waiter must re-scan when the ring cannot extend coverage")
}

// TestHistoryScanWaiterHonorsOwnCtx pins that a waiter blocked on someone else's
// in-flight scan returns promptly when ITS OWN ctx is cancelled, rather than
// hanging until the shared scan finishes.
func TestHistoryScanWaiterHonorsOwnCtx(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	m := mock.NewMinimalMetrics(ctrl)
	gkv := &gatedIterKV{KvStorage: imemkv.NewKvStorage(), release: make(chan struct{})}
	b := NewBackend(gkv, Config{Prefix: prefix, Identity: getStorageIdentity()}, m).(*backend)
	defer func() { require.NoError(t, gkv.Close()) }()
	b.SetCurrentRevision(uint64(time.Now().UnixNano()))
	ctx := context.Background()

	baseKey := prefix + "/herd-ctx"
	keyA := baseKey + "/a"
	createA, err := b.Create(ctx, newCreateRequest(keyA, "a1"))
	require.NoError(t, err)
	waitUntilRevisionEqualOrTimeout(b, createA.Header.Revision)
	cur := b.GetCurrentRevision()
	gkv.gateSubstr, _ = b.historyPrefixBounds([]byte(baseKey))

	// Executor: holds the scan open (gated) in the background.
	var execDone sync.WaitGroup
	execDone.Add(1)
	go func() {
		defer execDone.Done()
		_, _ = b.historyWatchEvents(ctx, baseKey, createA.Header.Revision, cur, 0)
	}()
	time.Sleep(150 * time.Millisecond) // let the executor take the slot

	// Waiter with a short ctx must bail out on its own deadline, not hang.
	wctx, wcancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer wcancel()
	start := time.Now()
	_, err = b.historyWatchEvents(wctx, baseKey, createA.Header.Revision, cur, 0)
	elapsed := time.Since(start)
	require.ErrorIs(t, err, context.DeadlineExceeded)
	require.Less(t, elapsed, 2*time.Second, "waiter must return on its own ctx, not wait for the shared scan")

	close(gkv.release)
	execDone.Wait()
}
