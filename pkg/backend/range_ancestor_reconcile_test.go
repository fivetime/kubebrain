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
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	proto "github.com/kubewharf/kubebrain-client/api/v2rpc"
	"github.com/stretchr/testify/require"

	"github.com/kubewharf/kubebrain/pkg/metrics/mock"
	"github.com/kubewharf/kubebrain/pkg/storage"
	"github.com/kubewharf/kubebrain/pkg/storage/memkv"
)

type rangeSnapshotTraceStorage struct {
	storage.KvStorage
	mu              sync.Mutex
	iterTimestamps  []uint64
	getTimestamps   []uint64
	batchTimestamps []uint64
	batchKeys       [][][]byte
	getDelay        time.Duration
	activeGets      int
	maxActiveGets   int
}

func (s *rangeSnapshotTraceStorage) Iter(ctx context.Context, start, end []byte, timestamp, limit uint64) (storage.Iter, error) {
	s.mu.Lock()
	s.iterTimestamps = append(s.iterTimestamps, timestamp)
	s.mu.Unlock()
	return s.KvStorage.Iter(ctx, start, end, timestamp, limit)
}

func (s *rangeSnapshotTraceStorage) GetAt(ctx context.Context, key []byte, timestamp uint64) ([]byte, error) {
	s.mu.Lock()
	s.getTimestamps = append(s.getTimestamps, timestamp)
	s.activeGets++
	if s.activeGets > s.maxActiveGets {
		s.maxActiveGets = s.activeGets
	}
	delay := s.getDelay
	s.mu.Unlock()
	defer func() {
		s.mu.Lock()
		s.activeGets--
		s.mu.Unlock()
	}()
	if delay != 0 {
		select {
		case <-time.After(delay):
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	return s.KvStorage.Get(ctx, key)
}

func (s *rangeSnapshotTraceStorage) BatchGetAt(ctx context.Context, keys [][]byte, timestamp uint64) (map[string][]byte, error) {
	s.mu.Lock()
	s.batchTimestamps = append(s.batchTimestamps, timestamp)
	keyCopy := make([][]byte, len(keys))
	for index, key := range keys {
		keyCopy[index] = append([]byte(nil), key...)
	}
	s.batchKeys = append(s.batchKeys, keyCopy)
	s.mu.Unlock()
	values := make(map[string][]byte, len(keys))
	for _, key := range keys {
		value, err := s.KvStorage.Get(ctx, key)
		if err == storage.ErrKeyNotFound {
			continue
		}
		if err != nil {
			return nil, err
		}
		values[string(key)] = value
	}
	return values, nil
}

func (s *rangeSnapshotTraceStorage) resetTrace() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.iterTimestamps = nil
	s.getTimestamps = nil
	s.batchTimestamps = nil
	s.batchKeys = nil
	s.maxActiveGets = 0
}

func TestDecodedRangeExactKeysUseSnapshotBatchesAndBoundedParallelFallback(t *testing.T) {
	ctrl := gomock.NewController(t)
	rawStore := memkv.NewKvStorage()
	store := &rangeSnapshotTraceStorage{KvStorage: rawStore, getDelay: 5 * time.Millisecond}
	t.Cleanup(func() { require.NoError(t, rawStore.Close()) })
	b := NewBackend(store, Config{
		Prefix: "/kubebrain/range-ancestor-parallel", Identity: getStorageIdentity(), EnableEtcdCompatibility: true,
	}, mock.NewMinimalMetrics(ctrl)).(*backend)
	ctx := context.Background()
	keys := make([][]byte, maxDecodedRangeExactReadWorkers+1)
	for index := range keys {
		keys[index] = []byte{byte('a' + index)}
		_, err := b.Create(ctx, &proto.CreateRequest{Key: keys[index], Value: []byte{byte(index)}})
		require.NoError(t, err)
	}
	pinned, err := b.withRangeSnapshotTimestamp(ctx)
	require.NoError(t, err)
	store.resetTrace()
	result, err := b.readDecodedRangeExactKeys(pinned, keys, b.GetCurrentRevision())
	require.NoError(t, err)
	require.Len(t, result, len(keys))
	for index, kv := range result {
		require.Equal(t, keys[index], kv.Key)
		require.Equal(t, []byte{byte(index)}, StripInlineValue(kv.Value))
	}
	store.mu.Lock()
	require.Empty(t, store.getTimestamps)
	require.Len(t, store.batchKeys, 2)
	require.Len(t, store.batchTimestamps, 2)
	require.Equal(t, []int{len(keys), len(keys)}, []int{len(store.batchKeys[0]), len(store.batchKeys[1])})
	require.Equal(t, store.batchTimestamps[0], store.batchTimestamps[1])
	for index, key := range keys {
		require.Equal(t, b.coder.EncodeRevisionKey(key), store.batchKeys[0][index])
		require.Equal(t, b.coder.EncodeObjectKey(key, result[index].Revision), store.batchKeys[1][index])
	}
	store.mu.Unlock()

	store.resetTrace()
	result, err = b.readDecodedRangeExactKeysParallel(pinned, keys, b.GetCurrentRevision())
	require.NoError(t, err)
	require.Len(t, result, len(keys))
	store.mu.Lock()
	defer store.mu.Unlock()
	require.Equal(t, maxDecodedRangeExactReadWorkers, store.maxActiveGets)
	require.NotEmpty(t, store.getTimestamps)
	for _, timestamp := range store.getTimestamps {
		require.Equal(t, store.getTimestamps[0], timestamp)
	}
}

func (s *rangeSnapshotTraceStorage) requireOneSnapshot(t *testing.T) {
	t.Helper()
	s.mu.Lock()
	defer s.mu.Unlock()
	require.NotEmpty(t, s.iterTimestamps)
	require.NotEmpty(t, s.batchTimestamps)
	timestamp := s.iterTimestamps[0]
	require.NotZero(t, timestamp)
	observedTimestamps := append(append([]uint64(nil), s.iterTimestamps...), s.getTimestamps...)
	observedTimestamps = append(observedTimestamps, s.batchTimestamps...)
	for _, observed := range observedTimestamps {
		require.Equal(t, timestamp, observed)
	}
}

func TestDecodedRangeReconcilesAncestorWhoseTombstoneIsPastRawEnd(t *testing.T) {
	ctrl := gomock.NewController(t)
	rawStore := memkv.NewKvStorage()
	store := &rangeSnapshotTraceStorage{KvStorage: rawStore}
	t.Cleanup(func() { require.NoError(t, rawStore.Close()) })
	b := NewBackend(store, Config{
		Prefix: "/kubebrain/range-ancestor", Identity: getStorageIdentity(), EnableEtcdCompatibility: true,
	}, mock.NewMinimalMetrics(ctrl)).(*backend)
	b.SetCurrentRevision(0x78ffffffffffffff)
	ctx := context.Background()
	ancestor := []byte("a")
	child := []byte("a$x")
	end := []byte("a$z")

	created, err := b.Create(ctx, &proto.CreateRequest{Key: ancestor, Value: []byte("ancestor")})
	require.NoError(t, err)
	_, err = b.Create(ctx, &proto.CreateRequest{Key: child, Value: []byte("child")})
	require.NoError(t, err)
	// The create revision begins with 0x79 and lies below raw end's 0x7a,
	// while the tombstone begins with 0x7b and lies beyond it.
	b.SetCurrentRevision(0x7affffffffffffff)
	deleted, err := b.Delete(ctx, &proto.DeleteRequest{Key: ancestor})
	require.NoError(t, err)
	require.True(t, deleted.Succeeded)

	store.resetTrace()
	latest, err := b.List(ctx, &proto.RangeRequest{Key: ancestor, End: end})
	require.NoError(t, err)
	require.Equal(t, [][]byte{child}, [][]byte{latest.Kvs[0].Key})
	store.requireOneSnapshot(t)
	store.resetTrace()
	count, err := b.Count(ctx, &proto.CountRequest{Key: ancestor, End: end})
	require.NoError(t, err)
	require.Equal(t, uint64(1), count.Count)
	store.requireOneSnapshot(t)

	store.resetTrace()
	historical, err := b.List(ctx, &proto.RangeRequest{Key: ancestor, End: end, Revision: created.Header.Revision})
	require.NoError(t, err)
	require.Equal(t, [][]byte{ancestor}, [][]byte{historical.Kvs[0].Key})
	store.requireOneSnapshot(t)
}

func TestDecodedRangeStreamUsesOrderedIndexWithoutMaterializingRange(t *testing.T) {
	ctrl := gomock.NewController(t)
	rawStore := memkv.NewKvStorage()
	store := &rangeSnapshotTraceStorage{KvStorage: rawStore}
	t.Cleanup(func() { require.NoError(t, rawStore.Close()) })
	b := NewBackend(store, Config{
		Prefix: "/kubebrain/range-stream-index", Identity: getStorageIdentity(),
		EnableEtcdCompatibility: true, EnableCountIndex: true,
	}, mock.NewMinimalMetrics(ctrl)).(*backend)
	ctx := context.Background()
	keys := [][]byte{[]byte("a"), []byte("a\x00"), []byte("a$")}
	for index := 0; index < decodedRangeStreamIndexPage; index++ {
		keys = append(keys, []byte(fmt.Sprintf("a/%03d", index)))
	}
	keys = append(keys, []byte("b"))
	revisions := make([]uint64, len(keys))
	seed := rawStore.BeginBatchWrite()
	for index, key := range keys {
		revisions[index] = uint64(index + 2)
		mutation := b.encodeCreateMutation(
			key, []byte{byte(index)}, EtcdMetadata{CreateRevision: revisions[index], Version: 1}, revisions[index], 0, 1,
		)
		seed.Put(mutation.revisionKey, mutation.newRevisionValue, 0)
		for _, objectMutation := range mutation.objectMutations {
			seed.Put(objectMutation.key, objectMutation.value, 0)
		}
	}
	require.NoError(t, seed.Commit(ctx))
	current := revisions[len(revisions)-1]
	b.SetCurrentRevision(current)
	b.countIndex.Reset(func() uint64 { return current }, func(_ uint64, emit func(key []byte, rev uint64, tombstone bool)) error {
		for index, key := range keys {
			emit(key, revisions[index], false)
		}
		return nil
	})

	tracked := &checkpointCountScanner{Scanner: b.scanner}
	b.scanner = tracked
	store.resetTrace()
	stream, err := b.RangeStream(ctx, []byte("a"), []byte("b"), current)
	require.NoError(t, err)
	var got []*proto.KeyValue
	var terminal int
	var maxBackendChunk int
	for response := range stream {
		require.Empty(t, response.Err)
		maxBackendChunk = max(maxBackendChunk, len(response.RangeResponse.Kvs))
		got = append(got, response.RangeResponse.Kvs...)
		if len(response.RangeResponse.Kvs) == 0 {
			terminal++
		}
	}
	require.Len(t, got, len(keys)-1)
	for index := range got {
		require.Equal(t, keys[index], got[index].Key)
	}
	require.Equal(t, [][]byte{{0}, {1}, {2}}, [][]byte{
		StripInlineValue(got[0].Value), StripInlineValue(got[1].Value), StripInlineValue(got[2].Value),
	})
	require.Equal(t, 1, terminal)
	require.LessOrEqual(t, maxBackendChunk, 16, "large values must be released in bounded resolver windows")
	require.True(t, tracked.rangeCalled, "decoded-boundary detection should still run its bounded extension probes")
	require.False(t, tracked.rangeFilteredCalled, "ordered-index RangeStream must not materialize RangeFiltered")
	require.False(t, tracked.rangeStreamCalled, "ordered-index RangeStream resolves bounded key pages directly")
	store.mu.Lock()
	require.Greater(t, len(store.batchKeys), 4)
	require.Len(t, store.batchKeys[0], decodedRangeStreamIndexPage, "revision metadata remains one lightweight directory-page batch")
	for _, batch := range store.batchKeys {
		objectBatch := false
		for _, key := range batch {
			_, revision, decodeErr := b.coder.Decode(key)
			require.NoError(t, decodeErr)
			objectBatch = objectBatch || revision != 0
		}
		if objectBatch {
			require.LessOrEqual(t, len(batch), 16, "object-value BatchGet must be bounded independently of key page size")
		}
	}
	require.NotZero(t, store.batchTimestamps[0])
	for _, timestamp := range store.batchTimestamps[1:] {
		require.Equal(t, store.batchTimestamps[0], timestamp, "every bounded value window must use the stream's pinned TSO")
	}
	store.mu.Unlock()
}

func TestColdFollowerDecodedRangeStreamRebuildsOnceThenReplaysEventLog(t *testing.T) {
	ctrl := gomock.NewController(t)
	rawStore := memkv.NewKvStorage()
	store := &rangeSnapshotTraceStorage{KvStorage: rawStore}
	t.Cleanup(func() { require.NoError(t, rawStore.Close()) })
	b := NewBackend(store, Config{
		Prefix: "/kubebrain/range-stream-follower-index", Identity: getStorageIdentity(),
		EnableEtcdCompatibility: true, EnableCountIndex: true,
	}, mock.NewMinimalMetrics(ctrl)).(*backend)
	ctx := context.Background()
	seedPut := func(key, value []byte, revision uint64) {
		mutation := b.encodeCreateMutation(
			key, value, EtcdMetadata{CreateRevision: revision, Version: 1}, revision, 0, 1,
		)
		batch := rawStore.BeginBatchWrite()
		batch.Put(mutation.revisionKey, mutation.newRevisionValue, 0)
		for _, objectMutation := range mutation.objectMutations {
			batch.Put(objectMutation.key, objectMutation.value, 0)
		}
		require.NoError(t, batch.Commit(ctx))
	}
	initial := []struct {
		key   []byte
		value byte
	}{
		{[]byte("a"), 0}, {[]byte("a\x00"), 1}, {[]byte("a$"), 2}, {[]byte("a/x"), 3},
	}
	for index, item := range initial {
		seedPut(item.key, []byte{item.value}, uint64(index+2))
	}
	b.SetCurrentRevision(5)
	require.NoError(t, b.advanceEventLogStartStorage(ctx, 5))
	tracked := &checkpointCountScanner{Scanner: b.scanner}
	b.scanner = tracked

	collect := func(revision uint64) []*proto.KeyValue {
		stream, err := b.RangeStream(ctx, []byte("a"), []byte("b"), revision)
		require.NoError(t, err)
		var result []*proto.KeyValue
		for response := range stream {
			require.Empty(t, response.Err)
			result = append(result, response.RangeResponse.Kvs...)
		}
		return result
	}
	bootstrapResults := make([][]*proto.KeyValue, 8)
	var bootstrap sync.WaitGroup
	for index := range bootstrapResults {
		bootstrap.Add(1)
		go func(index int) {
			defer bootstrap.Done()
			bootstrapResults[index] = collect(5)
		}(index)
	}
	bootstrap.Wait()
	first := bootstrapResults[0]
	require.Equal(t, [][]byte{[]byte("a"), []byte("a\x00"), []byte("a$"), []byte("a/x")}, [][]byte{
		first[0].Key, first[1].Key, first[2].Key, first[3].Key,
	})
	for _, result := range bootstrapResults[1:] {
		require.Equal(t, first, result)
	}
	require.True(t, b.countIndex.Ready(5))
	tracked.mu.Lock()
	require.Equal(t, 1, tracked.rangeStreamCalls, "a cold follower must perform one bounded bootstrap scan")
	tracked.mu.Unlock()

	created := b.encodeCreateMutation(
		[]byte("a!"), []byte{4}, EtcdMetadata{CreateRevision: 6, Version: 1}, 6, 0, 1,
	)
	createBatch := rawStore.BeginBatchWrite()
	createBatch.Put(created.revisionKey, created.newRevisionValue, 0)
	for _, objectMutation := range created.objectMutations {
		createBatch.Put(objectMutation.key, objectMutation.value, 0)
	}
	createBatch.Put(created.eventKey, created.eventValue, 0)
	require.NoError(t, createBatch.Commit(ctx))
	deleted := b.encodeDeleteMutation([]byte("a$"), 4, 7)
	deleteBatch := rawStore.BeginBatchWrite()
	deleteBatch.Put(deleted.revisionKey, deleted.newRevisionValue, 0)
	deleteBatch.Put(deleted.objectKey, deleted.objectValue, 0)
	deleteBatch.Put(deleted.eventKey, deleted.eventValue, 0)
	require.NoError(t, deleteBatch.Commit(ctx))
	b.SetCurrentRevision(7)

	second := collect(7)
	require.Equal(t, [][]byte{[]byte("a"), []byte("a\x00"), []byte("a!"), []byte("a/x")}, [][]byte{
		second[0].Key, second[1].Key, second[2].Key, second[3].Key,
	})
	require.True(t, b.countIndex.Ready(7))
	tracked.mu.Lock()
	require.Equal(t, 1, tracked.rangeStreamCalls,
		"a trusted event-log delta must catch the follower up without another whole-keyspace scan")
	tracked.mu.Unlock()

	// Simulate a new term whose writer cannot vouch for the previous log window:
	// the watermark advances across revision 8 and that revision has no event-log
	// row. The follower must rebuild instead of advancing Ready over the hole.
	seedPut([]byte("a#"), []byte{5}, 8)
	b.SetCurrentRevision(8)
	require.NoError(t, b.advanceEventLogStartStorage(ctx, 8))
	third := collect(8)
	require.Equal(t, [][]byte{[]byte("a"), []byte("a\x00"), []byte("a!"), []byte("a#"), []byte("a/x")}, [][]byte{
		third[0].Key, third[1].Key, third[2].Key, third[3].Key, third[4].Key,
	})
	tracked.mu.Lock()
	require.Equal(t, 2, tracked.rangeStreamCalls,
		"an untrusted event-log window must force one fresh bounded rebuild")
	tracked.mu.Unlock()
}

func TestDecodedHistoricalRangeStreamExtendsOrderingIndexBackToRequestedRevision(t *testing.T) {
	ctrl := gomock.NewController(t)
	rawStore := memkv.NewKvStorage()
	store := &rangeSnapshotTraceStorage{KvStorage: rawStore}
	t.Cleanup(func() { require.NoError(t, rawStore.Close()) })
	b := NewBackend(store, Config{
		Prefix: "/kubebrain/range-stream-historical-index", Identity: getStorageIdentity(),
		EnableEtcdCompatibility: true, EnableCountIndex: true,
	}, mock.NewMinimalMetrics(ctrl)).(*backend)
	ctx := context.Background()
	spillDir := t.TempDir()
	t.Setenv("TMPDIR", spillDir)
	commitCreate := func(key string, value byte, revision uint64) {
		mutation := b.encodeCreateMutation(
			[]byte(key), []byte{value}, EtcdMetadata{CreateRevision: revision, Version: 1}, revision, 0, 1,
		)
		batch := rawStore.BeginBatchWrite()
		batch.Put(mutation.revisionKey, mutation.newRevisionValue, 0)
		for _, objectMutation := range mutation.objectMutations {
			batch.Put(objectMutation.key, objectMutation.value, 0)
		}
		batch.Put(mutation.eventKey, mutation.eventValue, 0)
		require.NoError(t, batch.Commit(ctx))
	}
	commitCreate("a", 0, 2)
	commitCreate("a\x00", 1, 3)
	commitCreate("a$", 2, 4)
	commitCreate("a!", 3, 5)
	deleted := b.encodeDeleteMutation([]byte("a$"), 4, 6)
	deleteBatch := rawStore.BeginBatchWrite()
	deleteBatch.Put(deleted.revisionKey, deleted.newRevisionValue, 0)
	deleteBatch.Put(deleted.objectKey, deleted.objectValue, 0)
	deleteBatch.Put(deleted.eventKey, deleted.eventValue, 0)
	require.NoError(t, deleteBatch.Commit(ctx))
	commitCreate("a#", 4, 7)
	require.NoError(t, b.advanceEventLogStartStorage(ctx, 1))
	b.SetCurrentRevision(7)

	tracked := &checkpointCountScanner{Scanner: b.scanner}
	b.scanner = tracked
	require.NoError(t, b.RebuildCountIndex(ctx))
	require.EqualValues(t, 7, b.countIndex.BaseRev())

	collect := func(revision uint64) []*proto.KeyValue {
		stream, err := b.RangeStream(ctx, []byte("a"), []byte("b"), revision)
		require.NoError(t, err)
		var result []*proto.KeyValue
		for response := range stream {
			require.Empty(t, response.Err)
			result = append(result, response.RangeResponse.Kvs...)
		}
		return result
	}
	historical := collect(4)
	require.Equal(t, [][]byte{[]byte("a"), []byte("a\x00"), []byte("a$")}, [][]byte{
		historical[0].Key, historical[1].Key, historical[2].Key,
	})
	require.EqualValues(t, 4, b.countIndex.BaseRev(),
		"the durable ordering index must extend backwards instead of materializing the result")
	require.True(t, b.countIndex.Ready(7), "the historical rebuild must replay through the prior ready watermark")
	latest := collect(7)
	require.Equal(t, [][]byte{[]byte("a"), []byte("a\x00"), []byte("a!"), []byte("a#")}, [][]byte{
		latest[0].Key, latest[1].Key, latest[2].Key, latest[3].Key,
	})
	tracked.mu.Lock()
	require.Equal(t, 2, tracked.rangeStreamCalls,
		"one current rebuild plus one historical snapshot scan; the later current stream must reuse the extended index")
	require.False(t, tracked.rangeFilteredCalled, "historical decoded streaming must not materialize RangeFiltered")
	tracked.mu.Unlock()

	// Once cleanup has crossed the required replay window, semantic fallback is
	// still necessary. The preflight must preserve the useful current index
	// instead of replacing it with a historical tree that cannot be completed.
	require.NoError(t, b.RebuildCountIndex(ctx))
	require.NoError(t, b.advanceEventLogStartStorage(ctx, 4))
	tooOld := collect(3)
	require.Equal(t, [][]byte{[]byte("a"), []byte("a\x00")}, [][]byte{tooOld[0].Key, tooOld[1].Key})
	require.EqualValues(t, 7, b.countIndex.BaseRev(), "an untrusted historical window must not discard the current index")
	tracked.mu.Lock()
	require.Equal(t, 4, tracked.rangeStreamCalls,
		"an unverifiable old window must add one keys-only spill scan without rebuilding the useful current index")
	require.False(t, tracked.rangeFilteredCalled, "an unverifiable old window must not materialize the decoded KV range")
	tracked.mu.Unlock()
	entries, err := os.ReadDir(spillDir)
	require.NoError(t, err)
	require.Empty(t, entries)
}

func TestDecodedRangeStreamSpillsOrderedKeysWhenCountIndexOverflows(t *testing.T) {
	ctrl := gomock.NewController(t)
	rawStore := memkv.NewKvStorage()
	store := &rangeSnapshotTraceStorage{KvStorage: rawStore}
	t.Cleanup(func() { require.NoError(t, rawStore.Close()) })
	b := NewBackend(store, Config{
		Prefix: "/kubebrain/range-stream-spill", Identity: getStorageIdentity(),
		EnableEtcdCompatibility: true, EnableCountIndex: true, CountIndexMaxKeys: 2,
	}, mock.NewMinimalMetrics(ctrl)).(*backend)
	ctx := context.Background()
	keys := make([][]byte, 303)
	keys[0] = []byte("a")
	keys[1] = []byte("a\x00")
	keys[2] = []byte("a$")
	for index := 3; index < len(keys); index++ {
		keys[index] = []byte(fmt.Sprintf("a/%03d", index))
	}
	keys[len(keys)-1] = append([]byte("a/"), bytes.Repeat([]byte{'z'}, 40<<10)...)
	seed := rawStore.BeginBatchWrite()
	for index, key := range keys {
		revision := uint64(index + 2)
		mutation := b.encodeCreateMutation(
			key, []byte{byte(index)}, EtcdMetadata{CreateRevision: revision, Version: 1}, revision, 0, 1,
		)
		seed.Put(mutation.revisionKey, mutation.newRevisionValue, 0)
		for _, objectMutation := range mutation.objectMutations {
			seed.Put(objectMutation.key, objectMutation.value, 0)
		}
	}
	require.NoError(t, seed.Commit(ctx))
	b.SetCurrentRevision(304)
	b.countIndex.Reset(func() uint64 { return 304 }, func(_ uint64, emit func(key []byte, rev uint64, tombstone bool)) error {
		for index, key := range keys {
			emit(key, uint64(index+2), false)
		}
		return nil
	})
	require.True(t, b.countIndex.Overflowed())

	spillDir := t.TempDir()
	t.Setenv("TMPDIR", spillDir)
	tracked := &checkpointCountScanner{Scanner: b.scanner}
	b.scanner = tracked
	store.resetTrace()
	stream, err := b.RangeStream(ctx, []byte("a"), []byte("b"), 304)
	require.NoError(t, err)
	var got []*proto.KeyValue
	var maxBackendChunk int
	for response := range stream {
		require.Empty(t, response.Err)
		maxBackendChunk = max(maxBackendChunk, len(response.RangeResponse.Kvs))
		got = append(got, response.RangeResponse.Kvs...)
	}
	require.Len(t, got, len(keys))
	require.LessOrEqual(t, maxBackendChunk, 16)
	for index := range keys {
		require.Equal(t, keys[index], got[index].Key)
		require.Equal(t, []byte{byte(index)}, StripInlineValue(got[index].Value))
	}
	tracked.mu.Lock()
	require.Equal(t, 1, tracked.rangeStreamCalls, "overflow fallback must perform one bounded keys-only scan")
	require.False(t, tracked.rangeFilteredCalled, "overflow fallback must not materialize the decoded KV range")
	tracked.mu.Unlock()
	store.mu.Lock()
	for _, batch := range store.batchKeys {
		objectBatch := false
		for _, key := range batch {
			_, revision, decodeErr := b.coder.Decode(key)
			require.NoError(t, decodeErr)
			objectBatch = objectBatch || revision != 0
		}
		if objectBatch {
			require.LessOrEqual(t, len(batch), 16)
		}
	}
	store.mu.Unlock()
	entries, err := os.ReadDir(spillDir)
	require.NoError(t, err)
	require.Empty(t, entries, "the per-stream spill file must be removed after terminal delivery")
}
