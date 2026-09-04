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
	"errors"
	"fmt"
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

type historicalCountFailCompactKV struct {
	storage.KvStorage
	compactKey []byte
	armed      atomic.Bool
}

func (s *historicalCountFailCompactKV) Get(ctx context.Context, key []byte) ([]byte, error) {
	if s.armed.Load() && bytes.Equal(key, s.compactKey) {
		return nil, errors.New("injected compact watermark failure")
	}
	return s.KvStorage.Get(ctx, key)
}

// TestCountIndexMatchesScanAcrossRevisions verifies the in-memory count index
// returns exactly the same live-key count as a storage scan (List length),
// across the current and pinned historical revisions after creates/updates/
// deletes — the correctness the paginated-count optimization depends on.
func TestCountIndexMatchesScanAcrossRevisions(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	m := mock.NewMinimalMetrics(ctrl)
	kv := imemkv.NewKvStorage()
	defer func() { require.NoError(t, kv.Close()) }()
	b := NewBackend(kv, Config{
		Prefix:                  prefix,
		Identity:                getStorageIdentity(),
		EnableEtcdCompatibility: true,
		EnableCountIndex:        true,
	}, m).(*backend)
	b.SetCurrentRevision(uint64(time.Now().UnixNano()))
	ctx := context.Background()

	p := prefix + "/reg/"
	end := PrefixEnd([]byte(p))

	put := func(name string) uint64 {
		r, err := b.Update(ctx, &proto.UpdateRequest{Kv: &proto.KeyValue{Key: []byte(p + name), Value: []byte("v")}})
		require.NoError(t, err)
		return r.Header.Revision
	}
	del := func(name string) uint64 {
		r, err := b.Delete(ctx, &proto.DeleteRequest{Key: []byte(p + name)})
		require.NoError(t, err)
		return r.Header.Revision
	}
	waitCommitted := func(rev uint64) {
		require.Eventually(t, func() bool { return b.GetCurrentRevision() >= rev }, 5*time.Second, 2*time.Millisecond)
	}
	scanCount := func(rev uint64) int {
		resp, err := b.List(ctx, &proto.RangeRequest{Key: []byte(p), End: end, Revision: rev})
		require.NoError(t, err)
		return len(resp.Kvs)
	}
	assertIndexMatches := func(rev uint64) {
		c, _, served := b.CountAtRevision(ctx, []byte(p), end, rev)
		require.True(t, served, "index should serve count at rev %d", rev)
		require.Equal(t, scanCount(rev), int(c), "index count != scan count at rev %d", rev)
	}

	// create 5, snapshot a revision, then mutate.
	for i := 0; i < 5; i++ {
		put(fmt.Sprintf("k%02d", i))
	}
	var revAfter5 uint64
	require.Eventually(t, func() bool { revAfter5 = b.GetCurrentRevision(); return scanCount(0) == 5 }, 5*time.Second, 2*time.Millisecond)

	require.NoError(t, b.RebuildCountIndex(ctx))
	assertIndexMatches(revAfter5) // 5 live
	currentCount, headerRevision, served := b.CountAtRevision(ctx, []byte(p), end, 0)
	require.True(t, served, "index should resolve a current-revision count")
	require.Equal(t, int64(5), currentCount)
	require.Equal(t, b.GetCurrentRevision(), headerRevision,
		"rev=0 count must return the exact current revision selected for its index snapshot")

	// delete one, add two more.
	del("k02")
	put("k05")
	rLast := put("k06")
	waitCommitted(rLast)

	assertIndexMatches(b.GetCurrentRevision()) // current: 5 - 1 + 2 = 6
	assertIndexMatches(revAfter5)              // pinned historical: still 5 live there

	// sub-range count also matches.
	subEnd := PrefixEnd([]byte(p + "k03"))
	c, _, served := b.CountAtRevision(ctx, []byte(p+"k00"), subEnd, b.GetCurrentRevision())
	require.True(t, served)
	sub, err := b.List(ctx, &proto.RangeRequest{Key: []byte(p + "k00"), End: subEnd, Revision: b.GetCurrentRevision()})
	require.NoError(t, err)
	require.Equal(t, len(sub.Kvs), int(c))
}

func TestCountAtRevisionScanMatchesHistoricalStateWithoutIndex(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	m := mock.NewMinimalMetrics(ctrl)
	kv := imemkv.NewKvStorage()
	defer func() { require.NoError(t, kv.Close()) }()
	b := NewBackend(kv, Config{
		Prefix:                  prefix,
		Identity:                getStorageIdentity(),
		EnableEtcdCompatibility: true,
	}, m).(*backend)
	b.SetCurrentRevision(uint64(time.Now().UnixNano()))
	ctx := context.Background()

	p := prefix + "/historical-scan/"
	end := PrefixEnd([]byte(p))
	put := func(name string) uint64 {
		response, err := b.Create(ctx, &proto.CreateRequest{
			Key: []byte(p + name), Value: []byte("large-value-that-count-must-not-retain"),
		})
		require.NoError(t, err)
		waitCommitted(t, b, response.Header.Revision)
		return response.Header.Revision
	}

	put("a")
	historicalRevision := put("b")
	currentRevision := put("c")
	_, _, served := b.CountAtRevision(ctx, []byte(p), end, historicalRevision)
	require.False(t, served, "fixture must force the scanner fallback")

	count, headerRevision, err := b.CountAtRevisionScan(ctx, []byte(p), end, historicalRevision)
	require.NoError(t, err)
	require.Equal(t, int64(2), count)
	require.GreaterOrEqual(t, headerRevision, currentRevision)

	count, headerRevision, err = b.CountAtRevisionScan(ctx, []byte(p), end, 0)
	require.NoError(t, err)
	require.Equal(t, int64(3), count)
	require.GreaterOrEqual(t, headerRevision, currentRevision)

	point := []byte(p + "b")
	count, _, err = b.CountAtRevisionScan(ctx, point, append(append([]byte(nil), point...), 0), historicalRevision)
	require.NoError(t, err)
	require.Equal(t, int64(1), count)
}

func TestListKeysOnlyDropsPayloadAndPreservesInlineMetadata(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	m := mock.NewMinimalMetrics(ctrl)
	kv := imemkv.NewKvStorage()
	defer func() { require.NoError(t, kv.Close()) }()
	b := NewBackend(kv, Config{
		Prefix: prefix, Identity: getStorageIdentity(), EnableEtcdCompatibility: true,
	}, m).(*backend)
	b.SetCurrentRevision(uint64(time.Now().UnixNano()))
	ctx := context.Background()

	p := prefix + "/keys-only/"
	key := []byte(p + "large")
	first, err := b.Update(ctx, &proto.UpdateRequest{Kv: &proto.KeyValue{
		Key: key, Value: bytes.Repeat([]byte("a"), 2<<20),
	}})
	require.NoError(t, err)
	second, err := b.Update(ctx, &proto.UpdateRequest{Kv: &proto.KeyValue{
		Key: key, Value: bytes.Repeat([]byte("b"), 2<<20),
	}})
	require.NoError(t, err)
	require.Eventually(t, func() bool { return b.GetCurrentRevision() >= second.Header.Revision }, 5*time.Second, 2*time.Millisecond)

	request := &proto.RangeRequest{Key: []byte(p), End: PrefixEnd([]byte(p))}
	full, err := b.List(ctx, request)
	require.NoError(t, err)
	projected, err := b.ListKeysOnly(ctx, request)
	require.NoError(t, err)
	require.Equal(t, full.Header.Revision, projected.Header.Revision)
	require.Len(t, full.Kvs, 1)
	require.Len(t, projected.Kvs, 1)
	require.Equal(t, full.Kvs[0].Key, projected.Kvs[0].Key)
	require.Equal(t, full.Kvs[0].Revision, projected.Kvs[0].Revision)
	require.Greater(t, len(full.Kvs[0].Value), 2<<20)
	require.Less(t, len(projected.Kvs[0].Value), 64)

	fullMeta, _, fullInline, err := DecodeInlineValueChecked(full.Kvs[0].Value)
	require.NoError(t, err)
	projectedMeta, raw, projectedInline, err := DecodeInlineValueChecked(projected.Kvs[0].Value)
	require.NoError(t, err)
	require.True(t, fullInline)
	require.True(t, projectedInline)
	require.Empty(t, raw)
	require.Equal(t, first.Header.Revision, fullMeta.CreateRevision)
	require.Equal(t, fullMeta.CreateRevision, projectedMeta.CreateRevision)
	require.Equal(t, fullMeta.Version, projectedMeta.Version)
}

// TestCountIndexRebuildIgnoresSystemNamespace pins the rebuild scan to the
// whole object keyspace: with a system namespace (--system-namespace, né
// --key-prefix) that sorts AFTER the client's key prefix, the old
// prefix-bounded rebuild silently skipped every user key and the half-loaded
// index then served wrong counts as authoritative (#62 family, #75).
func TestCountIndexRebuildIgnoresSystemNamespace(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	m := mock.NewMinimalMetrics(ctrl)
	kv := imemkv.NewKvStorage()
	defer func() { require.NoError(t, kv.Close()) }()
	b := NewBackend(kv, Config{
		Prefix:                  "/zzz-system", // sorts after every /registry/* user key
		Identity:                getStorageIdentity(),
		EnableEtcdCompatibility: true,
		EnableCountIndex:        true,
	}, m).(*backend)
	b.SetCurrentRevision(uint64(time.Now().UnixNano()))
	ctx := context.Background()

	p := "/registry/pods/"
	end := PrefixEnd([]byte(p))
	var last uint64
	for i := 0; i < 7; i++ {
		r, err := b.Update(ctx, &proto.UpdateRequest{Kv: &proto.KeyValue{Key: []byte(fmt.Sprintf("%sns/p%02d", p, i)), Value: []byte("v")}})
		require.NoError(t, err)
		last = r.Header.Revision
	}
	require.Eventually(t, func() bool { return b.GetCurrentRevision() >= last }, 5*time.Second, 2*time.Millisecond)

	require.NoError(t, b.RebuildCountIndex(ctx))
	c, _, served := b.CountAtRevision(ctx, []byte(p), end, b.GetCurrentRevision())
	require.True(t, served, "index should serve after rebuild")
	require.Equal(t, 7, int(c), "rebuild bounded by the system namespace would miss all user keys")
}

func TestHistoricalCountIndexDoesNotReadCompactWatermark(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	m := mock.NewMinimalMetrics(ctrl)
	kv := &historicalCountFailCompactKV{
		KvStorage:  imemkv.NewKvStorage(),
		compactKey: getCompactKey(prefix),
	}
	defer func() { require.NoError(t, kv.Close()) }()
	b := NewBackend(kv, Config{
		Prefix:                  prefix,
		Identity:                getStorageIdentity(),
		EnableEtcdCompatibility: true,
		EnableCountIndex:        true,
	}, m).(*backend)
	b.SetCurrentRevision(uint64(time.Now().UnixNano()))
	ctx := context.Background()

	key := []byte(prefix + "/historical-count/a")
	end := PrefixEnd([]byte(prefix + "/historical-count/"))
	created, err := b.Create(ctx, &proto.CreateRequest{Key: key, Value: []byte("v")})
	require.NoError(t, err)
	waitCommitted(t, b, created.Header.Revision)
	require.NoError(t, b.RebuildCountIndex(ctx))
	revision := b.GetCurrentRevision()

	// Expire the compact cache and make the metadata read fail. A historical
	// index hit already names its snapshot and must remain a pure in-memory read.
	b.compactRevCache.mu.Lock()
	b.compactRevCache.loaded = time.Time{}
	b.compactRevCache.mu.Unlock()
	kv.armed.Store(true)

	count, headerRevision, served := b.CountAtRevision(ctx, []byte(prefix+"/historical-count/"), end, revision)
	require.True(t, served, "ready historical index must not depend on compact metadata availability")
	require.Equal(t, int64(1), count)
	require.Equal(t, revision, headerRevision)
}

func TestCountIndexRebuildBypassesStaleCompactRevisionCache(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	m := mock.NewMinimalMetrics(ctrl)
	kv := imemkv.NewKvStorage()
	defer func() { require.NoError(t, kv.Close()) }()
	b := NewBackend(kv, Config{
		Prefix:                  prefix,
		Identity:                getStorageIdentity(),
		EnableEtcdCompatibility: true,
		EnableCountIndex:        true,
	}, m).(*backend)
	ctx := context.Background()
	b.SetCurrentRevision(5)

	// Model a follower that cached the old zero watermark before the leader
	// persisted Compact(10), then became leader while its local committed
	// revision was still behind that durable boundary.
	got, err := b.GetCompactRevision(ctx)
	require.NoError(t, err)
	require.Zero(t, got)
	batch := kv.BeginBatchWrite()
	batch.Put(getCompactKey(prefix), uint64ToBytes(10), 0)
	require.NoError(t, batch.Commit(ctx))

	require.NoError(t, b.RebuildCountIndex(ctx))
	require.Equal(t, uint64(11), b.countIndex.BaseRev(),
		"a rebuilt index must not claim completeness at or below durable compaction")
	got, err = b.GetCompactRevision(ctx)
	require.NoError(t, err)
	require.Equal(t, uint64(10), got, "the authoritative rebuild fence must refresh the cache")
}
