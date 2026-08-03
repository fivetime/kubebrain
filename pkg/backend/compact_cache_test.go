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

// countingGetKV counts storage Get calls on a specific key.
type countingGetKV struct {
	storage.KvStorage
	matchKey []byte
	gets     int64
}

func (c *countingGetKV) Get(ctx context.Context, key []byte) ([]byte, error) {
	if bytes.Equal(key, c.matchKey) {
		atomic.AddInt64(&c.gets, 1)
	}
	return c.KvStorage.Get(ctx, key)
}

// TestGetCompactRevisionCachesAndUpdatesEagerly pins #48: GetCompactRevision must
// serve from an in-memory cache (no storage Get per revisioned request within the
// TTL), the cache must reflect a compaction eagerly, and it must refresh after
// the TTL.
func TestGetCompactRevisionCachesAndUpdatesEagerly(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	m := mock.NewMinimalMetrics(ctrl)
	kv := &countingGetKV{KvStorage: imemkv.NewKvStorage()}
	defer func() { require.NoError(t, kv.Close()) }()
	b := NewBackend(kv, Config{Prefix: prefix, Identity: getStorageIdentity(), EnableEtcdCompatibility: true}, m).(*backend)
	kv.matchKey = getCompactKey(prefix)
	b.SetCurrentRevision(uint64(time.Now().UnixNano()))
	ctx := context.Background()

	// Seed some data and compact.
	cr, err := b.Create(ctx, &proto.CreateRequest{Key: []byte(prefix + "/a"), Value: []byte("v")})
	require.NoError(t, err)
	waitCommitted(t, b, cr.Header.Revision)
	resp, err := b.Compact(ctx, cr.Header.Revision)
	require.NoError(t, err)
	compactedRev := resp.Header.Revision
	require.NotZero(t, compactedRev)

	// The compaction updated the cache eagerly, so GetCompactRevision returns the
	// new value with no extra storage read.
	before := atomic.LoadInt64(&kv.gets)
	for i := 0; i < 10; i++ {
		got, err := b.GetCompactRevision(ctx)
		require.NoError(t, err)
		require.Equal(t, compactedRev, got)
	}
	require.Equal(t, before, atomic.LoadInt64(&kv.gets),
		"GetCompactRevision must be served from cache within the TTL, doing no storage Get")

	// After the TTL it refreshes from storage exactly once for a burst of reads.
	time.Sleep(compactRevCacheTTL + 50*time.Millisecond)
	got, err := b.GetCompactRevision(ctx)
	require.NoError(t, err)
	require.Equal(t, compactedRev, got)
	require.Equal(t, before+1, atomic.LoadInt64(&kv.gets), "one refresh after TTL expiry")
}

// TestGetCompactRevisionFreshBypassesCache pins #33: on a follower, the cached
// compact revision can lag a compaction that a peer (the leader) just persisted.
// GetCompactRevisionFresh must read through to storage — returning the peer's
// value while the TTL cache still holds the stale one — and refresh the cache.
func TestGetCompactRevisionFreshBypassesCache(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	m := mock.NewMinimalMetrics(ctrl)
	kv := &countingGetKV{KvStorage: imemkv.NewKvStorage()}
	defer func() { require.NoError(t, kv.Close()) }()
	b := NewBackend(kv, Config{Prefix: prefix, Identity: getStorageIdentity(), EnableEtcdCompatibility: true}, m).(*backend)
	b.SetCurrentRevision(uint64(time.Now().UnixNano()))
	ctx := context.Background()

	// Warm the cache at the current (zero) compact revision.
	got, err := b.GetCompactRevision(ctx)
	require.NoError(t, err)
	require.Zero(t, got)

	// A "peer" (the leader, via proxy) persists a newer compact revision directly
	// in storage; our TTL cache still holds 0.
	peerRev := uint64(time.Now().UnixNano())
	bw := kv.BeginBatchWrite()
	bw.Put(getCompactKey(prefix), uint64ToBytes(peerRev), 0)
	require.NoError(t, bw.Commit(ctx))
	got, err = b.GetCompactRevision(ctx)
	require.NoError(t, err)
	require.Zero(t, got, "cached read must still serve the stale value within the TTL (the #33 setup)")

	// The fresh read must bypass the cache and return the peer's value...
	fresh, err := b.GetCompactRevisionFresh(ctx)
	require.NoError(t, err)
	require.Equal(t, peerRev, fresh, "fresh read must see the peer-persisted compaction immediately")

	// ...and refresh the cache as a side effect.
	got, err = b.GetCompactRevision(ctx)
	require.NoError(t, err)
	require.Equal(t, peerRev, got, "fresh read must refresh the TTL cache")
}

func TestWatchHistoryBypassesStaleCompactRevisionCache(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	m := mock.NewMinimalMetrics(ctrl)
	kv := imemkv.NewKvStorage()
	defer func() { require.NoError(t, kv.Close()) }()
	b := NewBackend(kv, Config{Prefix: prefix, Identity: getStorageIdentity(), EnableEtcdCompatibility: true}, m).(*backend)
	ctx := context.Background()

	// Model a newly promoted replica: its one-second cache predates a Compact
	// persisted by the previous leader.
	got, err := b.GetCompactRevision(ctx)
	require.NoError(t, err)
	require.Zero(t, got)
	const durableRevision = uint64(10)
	b.SetCurrentRevision(20)
	bw := kv.BeginBatchWrite()
	bw.Put(getCompactKey(prefix), uint64ToBytes(durableRevision), 0)
	require.NoError(t, bw.Commit(ctx))

	events, err := b.historyWatchEvents(ctx, prefix+"/stale-watch/", 9, 20, 20)
	require.Nil(t, events)
	require.ErrorContains(t, err, "compacted at 10 newer than requested revision 9")
	got, err = b.GetCompactRevision(ctx)
	require.NoError(t, err)
	require.Equal(t, durableRevision, got, "the authoritative history fence must refresh the cache")
}

func TestSafeCurrentRevisionDoesNotAdvancePastCompactWatermark(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	m := mock.NewMinimalMetrics(ctrl)
	kv := imemkv.NewKvStorage()
	defer func() { require.NoError(t, kv.Close()) }()
	b := NewBackend(kv, Config{Prefix: prefix, Identity: getStorageIdentity(), EnableEtcdCompatibility: true}, m).(*backend)
	ctx := context.Background()

	const compactRevision = uint64(41)
	b.SetCurrentRevision(compactRevision - 1)
	batch := kv.BeginBatchWrite()
	batch.Put(getCompactKey(prefix), uint64ToBytes(compactRevision), 0)
	require.NoError(t, batch.Commit(ctx))

	got, err := b.safeCurrentRevision(ctx)
	require.NoError(t, err)
	require.Equal(t, compactRevision, got,
		"a cold revision cache may catch up to the proven compact watermark")
	require.Equal(t, compactRevision, b.GetCurrentRevision())

	got, err = b.safeCurrentRevision(ctx)
	require.NoError(t, err)
	require.Equal(t, compactRevision, got,
		"compact == current must not manufacture compact+1")
	require.Equal(t, compactRevision, b.GetCurrentRevision())
}

func TestSafeCurrentRevisionRestoresDurableUserWatermarkFromColdCache(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	m := mock.NewMinimalMetrics(ctrl)
	kv := imemkv.NewKvStorage()
	defer func() { require.NoError(t, kv.Close()) }()
	b := NewBackend(kv, Config{Prefix: prefix, Identity: getStorageIdentity(), EnableEtcdCompatibility: true}, m).(*backend)
	ctx := context.Background()

	// Model a newly started serving replica. Its local user-revision cache is
	// empty and its compact cache was populated before another replica committed
	// the durable user watermark. A latest serializable read may arrive without a
	// leader read barrier, but its response header must still describe the shared
	// TiKV snapshot rather than regress to etcd's empty-store revision 1.
	got, err := b.GetCompactRevision(ctx)
	require.NoError(t, err)
	require.Zero(t, got)
	const durableRevision = uint64(73)
	batch := kv.BeginBatchWrite()
	batch.Put(b.ks.EncodeInternalKey(durableRevisionKey), uint64ToBytes(durableRevision), 0)
	require.NoError(t, batch.Commit(ctx))

	got, err = b.safeCurrentRevision(ctx)
	require.NoError(t, err)
	require.Equal(t, durableRevision, got)
	require.Equal(t, durableRevision, b.GetCurrentRevision())
}
