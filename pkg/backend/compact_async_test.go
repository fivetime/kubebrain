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
	"fmt"
	"io"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"

	proto "github.com/kubewharf/kubebrain-client/api/v2rpc"

	"github.com/kubewharf/kubebrain/pkg/metrics/mock"
	imemkv "github.com/kubewharf/kubebrain/pkg/storage/memkv"
)

// TestCompactAsyncAdvancesWatermarkSyncThenGCsInBackground pins the async
// compaction contract: CompactAsync advances the logical compact watermark
// synchronously (so reads immediately see the compaction) but performs the
// physical version GC in the background (so the RPC never blocks on a big scan).
func TestCompactAsyncAdvancesWatermarkSyncThenGCsInBackground(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	m := mock.NewMinimalMetrics(ctrl)
	kv := imemkv.NewKvStorage()
	defer func() { require.NoError(t, kv.Close()) }()
	b := NewBackend(kv, Config{Prefix: prefix, Identity: getStorageIdentity(), EnableEtcdCompatibility: true}, m).(*backend)
	b.SetCurrentRevision(uint64(time.Now().UnixNano()))
	ctx := context.Background()

	key := []byte(prefix + "/reg/async")
	cr, err := b.Create(ctx, &proto.CreateRequest{Key: key, Value: []byte("v1")})
	require.NoError(t, err)
	last := cr.Header.Revision
	for i := 2; i <= 6; i++ {
		u, err := b.Update(ctx, &proto.UpdateRequest{Kv: &proto.KeyValue{Key: key, Value: []byte(fmt.Sprintf("v%d", i)), Revision: last}})
		require.NoError(t, err)
		require.True(t, u.Succeeded)
		last = u.Header.Revision
	}
	require.Eventually(t, func() bool { return b.GetCurrentRevision() >= last }, 5*time.Second, 2*time.Millisecond)

	countVersions := func() int {
		iter, err := b.kv.Iter(ctx, b.coder.EncodeObjectKey(key, ^uint64(0)), b.coder.EncodeObjectKey(key, 0), 0, 0)
		require.NoError(t, err)
		defer iter.Close()
		n := 0
		for {
			if err := iter.Next(ctx); err != nil {
				if err == io.EOF {
					break
				}
				require.NoError(t, err)
			}
			if _, rev, derr := b.coder.Decode(iter.Key()); derr == nil && rev != 0 {
				n++
			}
		}
		return n
	}
	require.Equal(t, 6, countVersions(), "6 writes -> 6 versions before compaction")

	compactedRev, err := b.CompactAsync(ctx, last)
	require.NoError(t, err)
	require.Equal(t, last, compactedRev, "CompactAsync returns the actual compacted revision")

	// Watermark is advanced synchronously: visible the instant CompactAsync returns.
	wm, err := b.GetCompactRevision(ctx)
	require.NoError(t, err)
	require.GreaterOrEqual(t, wm, last, "logical compact watermark must be advanced synchronously")

	// Physical GC happens in the background; wait for the worker to finish this
	// revision (via the internal completion marker) before iterating, so the
	// version count is read while the compactor is idle.
	require.Eventually(t, func() bool { return atomic.LoadUint64(&b.compactDoneRev) >= last }, 5*time.Second, 5*time.Millisecond,
		"background compactor must complete the physical GC scan")
	require.Equal(t, 1, countVersions(), "background compactor must retire superseded versions")
	wm, err = b.GetCompactRevisionFresh(ctx)
	require.NoError(t, err)
	require.GreaterOrEqual(t, wm, last, "physical GC must preserve the durable compact watermark")

	// Latest value stays correct after background GC.
	got, err := b.Get(ctx, &proto.GetRequest{Key: key})
	require.NoError(t, err)
	require.NotNil(t, got.Kv)
	require.Equal(t, "v6", string(StripInlineValue(got.Kv.Value)))
}

func TestCompactAsyncFullScanPreservesSkippedPrefixVersions(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	m := mock.NewMinimalMetrics(ctrl)
	kv := imemkv.NewKvStorage()
	defer func() { require.NoError(t, kv.Close()) }()

	skippedPrefix := prefix + "/skip-gc/excluded"
	b := NewBackend(kv, Config{
		Prefix:                  prefix,
		Identity:                getStorageIdentity(),
		EnableEtcdCompatibility: true,
		SkippedPrefixes:         []string{skippedPrefix},
	}, m).(*backend)
	b.SetCurrentRevision(uint64(time.Now().UnixNano()))
	ctx := context.Background()

	includedKey := []byte(prefix + "/skip-gc/included/key")
	excludedKey := []byte(skippedPrefix + "/key")
	writeVersions := func(key []byte) uint64 {
		created, err := b.Create(ctx, &proto.CreateRequest{Key: key, Value: []byte("v1")})
		require.NoError(t, err)
		last := created.Header.Revision
		for i := 2; i <= 4; i++ {
			updated, err := b.Update(ctx, &proto.UpdateRequest{
				Kv: &proto.KeyValue{
					Key:      key,
					Value:    []byte(fmt.Sprintf("v%d", i)),
					Revision: last,
				},
			})
			require.NoError(t, err)
			require.True(t, updated.Succeeded)
			last = updated.Header.Revision
		}
		return last
	}
	includedRevision := writeVersions(includedKey)
	excludedRevision := writeVersions(excludedKey)
	target := max(includedRevision, excludedRevision)
	require.Eventually(t, func() bool { return b.GetCurrentRevision() >= target }, 5*time.Second, 2*time.Millisecond)

	countVersions := func(key []byte) int {
		iter, err := b.kv.Iter(ctx, b.coder.EncodeObjectKey(key, ^uint64(0)), b.coder.EncodeObjectKey(key, 0), 0, 0)
		require.NoError(t, err)
		defer iter.Close()
		count := 0
		for {
			if err := iter.Next(ctx); err != nil {
				if err == io.EOF {
					return count
				}
				require.NoError(t, err)
			}
			if _, revision, decodeErr := b.coder.Decode(iter.Key()); decodeErr == nil && revision != 0 {
				count++
			}
		}
	}
	require.Equal(t, 4, countVersions(includedKey))
	require.Equal(t, 4, countVersions(excludedKey))

	// Force this compaction through the periodic full-keyspace scanner. The
	// incremental path has an independent skipped-key filter.
	b.incrementalStreak = incrementalCompactMaxStreak
	compactedRevision, err := b.CompactAsync(ctx, target)
	require.NoError(t, err)
	require.Equal(t, target, compactedRevision)
	require.Eventually(t, func() bool {
		return atomic.LoadUint64(&b.compactDoneRev) >= target
	}, 5*time.Second, 5*time.Millisecond)

	require.Equal(t, 1, countVersions(includedKey))
	require.Equal(t, 4, countVersions(excludedKey),
		"full-scan physical compaction must not enter a configured carve-out")
}

// TestCompactAsyncCoalescesConcurrentRequests drives many overlapping
// CompactAsync calls and asserts they all resolve (no deadlock, monotonic
// watermark, background GC still converges).
func TestCompactAsyncCoalescesConcurrentRequests(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	m := mock.NewMinimalMetrics(ctrl)
	kv := imemkv.NewKvStorage()
	defer func() { require.NoError(t, kv.Close()) }()
	b := NewBackend(kv, Config{Prefix: prefix, Identity: getStorageIdentity(), EnableEtcdCompatibility: true}, m).(*backend)
	b.SetCurrentRevision(uint64(time.Now().UnixNano()))
	ctx := context.Background()

	key := []byte(prefix + "/reg/coalesce")
	cr, err := b.Create(ctx, &proto.CreateRequest{Key: key, Value: []byte("v1")})
	require.NoError(t, err)
	last := cr.Header.Revision
	for i := 2; i <= 8; i++ {
		u, err := b.Update(ctx, &proto.UpdateRequest{Kv: &proto.KeyValue{Key: key, Value: []byte(fmt.Sprintf("v%d", i)), Revision: last}})
		require.NoError(t, err)
		require.True(t, u.Succeeded)
		last = u.Header.Revision
	}
	require.Eventually(t, func() bool { return b.GetCurrentRevision() >= last }, 5*time.Second, 2*time.Millisecond)

	for i := 0; i < 20; i++ {
		_, err := b.CompactAsync(ctx, last)
		require.NoError(t, err)
	}
	wm, err := b.GetCompactRevision(ctx)
	require.NoError(t, err)
	require.GreaterOrEqual(t, wm, last)
}

// TestSetCompactRecordConcurrentCASNoError fires many CompactAsync to the same
// already-current target simultaneously. The losers of the watermark CAS must not
// surface a user-visible error: compaction is idempotent (etcd), so a concurrent
// compactor that already reached >= target means the desired end state holds.
func TestSetCompactRecordConcurrentCASNoError(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	m := mock.NewMinimalMetrics(ctrl)
	kv := imemkv.NewKvStorage()
	defer func() { require.NoError(t, kv.Close()) }()
	b := NewBackend(kv, Config{Prefix: prefix, Identity: getStorageIdentity(), EnableEtcdCompatibility: true}, m).(*backend)
	b.SetCurrentRevision(uint64(time.Now().UnixNano()))
	ctx := context.Background()

	key := []byte(prefix + "/reg/concas")
	cr, err := b.Create(ctx, &proto.CreateRequest{Key: key, Value: []byte("v1")})
	require.NoError(t, err)
	last := cr.Header.Revision
	for i := 2; i <= 8; i++ {
		u, err := b.Update(ctx, &proto.UpdateRequest{Kv: &proto.KeyValue{Key: key, Value: []byte(fmt.Sprintf("v%d", i)), Revision: last}})
		require.NoError(t, err)
		last = u.Header.Revision
	}
	require.Eventually(t, func() bool { return b.GetCurrentRevision() >= last }, 5*time.Second, 2*time.Millisecond)

	const N = 64
	start := make(chan struct{})
	var wg sync.WaitGroup
	errs := make([]error, N)
	for i := 0; i < N; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			_, errs[i] = b.CompactAsync(ctx, last)
		}(i)
	}
	close(start) // fire all at once → early readers pass, then race the CAS
	wg.Wait()
	for i, e := range errs {
		require.NoErrorf(t, e, "concurrent CompactAsync[%d] to an already-target revision must not surface a CAS conflict", i)
	}
	wm, err := b.GetCompactRevision(ctx)
	require.NoError(t, err)
	require.GreaterOrEqual(t, wm, last)
}

func TestCompactAsyncFailureDoesNotAdvanceDoneAndRetries(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	m := mock.NewMinimalMetrics(ctrl)
	kv := &armedFailPartitionsKV{KvStorage: imemkv.NewKvStorage()}
	defer func() { require.NoError(t, kv.Close()) }()
	b := NewBackend(kv, Config{Prefix: prefix, Identity: getStorageIdentity(), EnableEtcdCompatibility: true}, m).(*backend)
	target := uint64(time.Now().UnixNano())
	b.SetCurrentRevision(target)
	atomic.StoreInt32(&kv.armed, 1)

	_, err := b.CompactAsync(context.Background(), target)
	require.NoError(t, err)
	time.Sleep(100 * time.Millisecond)
	require.Zero(t, atomic.LoadUint64(&b.compactDoneRev), "failed scan must not be reported complete")

	atomic.StoreInt32(&kv.armed, 0)
	require.Eventually(t, func() bool {
		return atomic.LoadUint64(&b.compactDoneRev) >= target
	}, 3*time.Second, 10*time.Millisecond, "background compactor must retry without a newer compact request")
}

func TestResumePhysicalCompactionFromPersistedWatermark(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	m := mock.NewMinimalMetrics(ctrl)
	kv := imemkv.NewKvStorage()
	defer func() { require.NoError(t, kv.Close()) }()
	target := uint64(time.Now().UnixNano())

	old := NewBackend(kv, Config{Prefix: prefix, Identity: "old", EnableEtcdCompatibility: true}, m).(*backend)
	old.SetCurrentRevision(target)
	advanced, err := old.setCompactRecord(context.Background(), target)
	require.NoError(t, err)
	require.True(t, advanced)

	fresh := NewBackend(kv, Config{Prefix: prefix, Identity: "fresh", EnableEtcdCompatibility: true}, m).(*backend)
	fresh.SetCurrentRevision(target)
	require.NoError(t, fresh.ResumePhysicalCompaction(context.Background()))
	require.Eventually(t, func() bool {
		return atomic.LoadUint64(&fresh.compactDoneRev) >= target
	}, 3*time.Second, 10*time.Millisecond, "new leader must resume GC from durable logical watermark")
}

func TestPhysicalCompactionTransfersAcrossLeadershipContexts(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	m := mock.NewMinimalMetrics(ctrl)
	kv := &cancelFirstPartitionsKV{
		KvStorage: imemkv.NewKvStorage(),
		entered:   make(chan struct{}),
	}
	defer func() { require.NoError(t, kv.Close()) }()
	target := uint64(time.Now().UnixNano())

	b := NewBackend(kv, Config{Prefix: prefix, Identity: "leader-transfer", EnableEtcdCompatibility: true}, m).(*backend)
	b.SetCurrentRevision(target)
	advanced, err := b.setCompactRecord(context.Background(), target)
	require.NoError(t, err)
	require.True(t, advanced)

	oldCtx, stopOldLeader := context.WithCancel(context.Background())
	require.NoError(t, b.ResumePhysicalCompaction(oldCtx))
	<-kv.entered
	stopOldLeader()
	require.Never(t, func() bool {
		return atomic.LoadUint64(&b.compactDoneRev) >= target
	}, 100*time.Millisecond, 5*time.Millisecond,
		"a scan canceled with the old leadership term must not report completion")

	newCtx, stopNewLeader := context.WithCancel(context.Background())
	defer stopNewLeader()
	require.NoError(t, b.ResumePhysicalCompaction(newCtx))
	require.Eventually(t, func() bool {
		return atomic.LoadUint64(&b.compactDoneRev) >= target
	}, 3*time.Second, 10*time.Millisecond,
		"the new leadership term must resume the durable physical-GC target")
}

func TestCompactAsyncCapturesEpochForInternalCaller(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	m := mock.NewMinimalMetrics(ctrl)
	kv := imemkv.NewKvStorage()
	defer func() { require.NoError(t, kv.Close()) }()
	b := NewBackend(kv, Config{Prefix: prefix, Identity: "auto-compact-fence", EnableEtcdCompatibility: true}, m).(*backend)
	target := uint64(time.Now().UnixNano())
	b.SetCurrentRevision(target)

	var checks atomic.Uint64
	b.SetLeadershipFence(func() (uint64, bool) {
		if checks.Add(1) == 1 {
			return 1, true
		}
		return 2, true
	})

	_, err := b.CompactAsync(context.Background(), target)
	require.ErrorIs(t, err, ErrLeadershipFenced)
	hasMarker, markerErr := b.HasCompactRevision(context.Background())
	require.NoError(t, markerErr)
	require.False(t, hasMarker, "internal compaction must be fenced if leadership changes before CAS")
}
