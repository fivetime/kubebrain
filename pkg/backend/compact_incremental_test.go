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
	"io"
	"sync/atomic"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"

	proto "github.com/kubewharf/kubebrain-client/api/v2rpc"

	mock "github.com/kubewharf/kubebrain/pkg/metrics/mock"
	imemkv "github.com/kubewharf/kubebrain/pkg/storage/memkv"
)

// TestIncrementalCompactScopesToTouchedKeys locks the incremental physical-GC
// contract: with a valid baseline and a covering event log, a compaction pass
// GCs exactly the keys written since the baseline (superseded versions and
// tombstones) while old garbage on untouched keys survives until the next FULL
// scan — proving the pass really is incremental, and that the full-scan
// fallback still reclaims everything.
func TestIncrementalCompactScopesToTouchedKeys(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	m := mock.NewMinimalMetrics(ctrl)
	kv := imemkv.NewKvStorage()
	defer func() { require.NoError(t, kv.Close()) }()
	b := NewBackend(kv, Config{Prefix: prefix, Identity: getStorageIdentity(), EnableEtcdCompatibility: true}, m).(*backend)
	b.SetCurrentRevision(uint64(time.Now().UnixNano()))
	ctx := context.Background()
	require.NoError(t, b.EnsureEventLogStart(ctx))

	countVersions := func(key []byte) int {
		iter, err := b.kv.Iter(ctx,
			b.coder.EncodeObjectKey(key, ^uint64(0)),
			b.coder.EncodeObjectKey(key, 0), 0, 0)
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
			n++
		}
		return n
	}
	waitPublished := func(rev uint64) {
		require.Eventually(t, func() bool { return b.GetCurrentRevision() >= rev }, 5*time.Second, 2*time.Millisecond)
	}

	// k0: two versions written BEFORE the baseline — its superseded version is
	// pre-baseline garbage no incremental pass may touch.
	k0 := []byte("/registry/incr/untouched")
	cr, err := b.Create(ctx, &proto.CreateRequest{Key: k0, Value: []byte("v1")})
	require.NoError(t, err)
	u, err := b.Update(ctx, &proto.UpdateRequest{Kv: &proto.KeyValue{Key: k0, Value: []byte("v2"), Revision: cr.Header.Revision}})
	require.NoError(t, err)
	require.True(t, u.Succeeded)
	base := uint64(u.Header.Revision)
	waitPublished(base)
	require.Equal(t, 2, countVersions(k0))

	// Pretend a completed physical pass ended exactly at base.
	atomic.StoreUint64(&b.physicalBaseRev, base)

	// k1 superseded after the baseline; k2 created then deleted (tombstone).
	k1, k2 := []byte("/registry/incr/updated"), []byte("/registry/incr/deleted")
	c1, err := b.Create(ctx, &proto.CreateRequest{Key: k1, Value: []byte("a")})
	require.NoError(t, err)
	u1, err := b.Update(ctx, &proto.UpdateRequest{Kv: &proto.KeyValue{Key: k1, Value: []byte("b"), Revision: c1.Header.Revision}})
	require.NoError(t, err)
	require.True(t, u1.Succeeded)
	c2, err := b.Create(ctx, &proto.CreateRequest{Key: k2, Value: []byte("x")})
	require.NoError(t, err)
	d2, err := b.Delete(ctx, &proto.DeleteRequest{Key: k2, Revision: uint64(c2.Header.Revision)})
	require.NoError(t, err)
	require.True(t, d2.Succeeded)
	target := uint64(d2.Header.Revision)
	waitPublished(target)

	require.GreaterOrEqual(t, countVersions(k1), 2)
	require.GreaterOrEqual(t, countVersions(k2), 2)

	// Compact at target: with the baseline set and the event log covering
	// (base, target], this must take the incremental path.
	_, err = b.Compact(ctx, target)
	require.NoError(t, err)
	require.Equal(t, 1, b.incrementalStreak, "pass must have run incrementally")
	require.Equal(t, target, atomic.LoadUint64(&b.physicalBaseRev))

	require.Equal(t, 1, countVersions(k1), "touched key: superseded version must be GC'd")
	require.Equal(t, 0, countVersions(k2), "touched key: tombstone must be fully GC'd")
	require.Equal(t, 2, countVersions(k0), "untouched key's pre-baseline garbage must survive an incremental pass")

	// Invalidate the baseline to force the full-scan path: it reclaims the
	// pre-baseline garbage the incremental pass correctly skipped.
	atomic.StoreUint64(&b.physicalBaseRev, 0)
	require.NoError(t, b.physicalCompact(ctx, target))
	require.Equal(t, 1, countVersions(k0), "full scan must reclaim pre-baseline garbage")
	require.Equal(t, 0, b.incrementalStreak, "full scan resets the incremental streak")
	require.Equal(t, target, atomic.LoadUint64(&b.physicalBaseRev), "full scan re-establishes the baseline")
}
