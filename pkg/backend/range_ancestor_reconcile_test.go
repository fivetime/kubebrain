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
