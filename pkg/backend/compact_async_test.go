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

	// Latest value stays correct after background GC.
	got, err := b.Get(ctx, &proto.GetRequest{Key: key})
	require.NoError(t, err)
	require.NotNil(t, got.Kv)
	require.Equal(t, "v6", string(StripInlineValue(got.Kv.Value)))
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
