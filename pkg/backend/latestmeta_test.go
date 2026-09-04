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
	"math"
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

type latestMetadataReadProbeStorage struct {
	storage.KvStorage
	mu          sync.RWMutex
	objectKey   []byte
	objectReads atomic.Int64
	batchReads  atomic.Int64
	iterReads   atomic.Int64
}

func (s *latestMetadataReadProbeStorage) Iter(
	ctx context.Context, start, end []byte, timestamp, limit uint64,
) (storage.Iter, error) {
	s.iterReads.Add(1)
	return s.KvStorage.Iter(ctx, start, end, timestamp, limit)
}

func (s *latestMetadataReadProbeStorage) setObjectKey(key []byte) {
	s.mu.Lock()
	s.objectKey = append(s.objectKey[:0], key...)
	s.mu.Unlock()
}

func (s *latestMetadataReadProbeStorage) isObjectKey(key []byte) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return bytes.Equal(key, s.objectKey)
}

func (s *latestMetadataReadProbeStorage) Get(ctx context.Context, key []byte) ([]byte, error) {
	if s.isObjectKey(key) {
		s.objectReads.Add(1)
	}
	return s.KvStorage.Get(ctx, key)
}

func (s *latestMetadataReadProbeStorage) GetAt(ctx context.Context, key []byte, _ uint64) ([]byte, error) {
	if s.isObjectKey(key) {
		s.objectReads.Add(1)
	}
	return s.KvStorage.Get(ctx, key)
}

func (s *latestMetadataReadProbeStorage) BatchGetAt(ctx context.Context, keys [][]byte, _ uint64) (map[string][]byte, error) {
	s.batchReads.Add(1)
	for _, key := range keys {
		if s.isObjectKey(key) {
			s.objectReads.Add(1)
		}
	}
	return s.KvStorage.(storage.BatchGetter).BatchGet(ctx, keys)
}

func TestLatestMetadataEncodingRejectsImpossibleState(t *testing.T) {
	live := latestMetadata{
		ModRevision: 9,
		Metadata:    EtcdMetadata{CreateRevision: 4, Version: 3, Lease: 17},
	}
	raw := encodeLatestMetadata(live)
	decoded, err := decodeLatestMetadata(raw)
	require.NoError(t, err)
	require.Equal(t, live, decoded)

	tombstone := latestMetadata{ModRevision: 10, Tombstone: true}
	decoded, err = decodeLatestMetadata(encodeLatestMetadata(tombstone))
	require.NoError(t, err)
	require.Equal(t, tombstone, decoded)

	for _, test := range []struct {
		name string
		raw  []byte
	}{
		{name: "short", raw: raw[:len(raw)-1]},
		{name: "unknown version", raw: append([]byte(nil), raw...)},
		{name: "revision overflow", raw: encodeLatestMetadata(latestMetadata{ModRevision: uint64(math.MaxInt64) + 1, Tombstone: true})},
		{name: "live lifecycle", raw: encodeLatestMetadata(latestMetadata{ModRevision: 9, Metadata: EtcdMetadata{CreateRevision: 8, Version: 3}})},
		{name: "tombstone metadata", raw: encodeLatestMetadata(latestMetadata{ModRevision: 9, Tombstone: true, Metadata: EtcdMetadata{CreateRevision: 9, Version: 1}})},
	} {
		t.Run(test.name, func(t *testing.T) {
			if test.name == "unknown version" {
				test.raw[3]++
			}
			_, err := decodeLatestMetadata(test.raw)
			require.ErrorIs(t, err, ErrInvalidMVCCMetadata)
		})
	}
}

func TestLatestKeysOnlyUsesMetadataIndexAndFallsBackSafely(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	store := &latestMetadataReadProbeStorage{KvStorage: imemkv.NewKvStorage()}
	defer func() { require.NoError(t, store.Close()) }()
	b := NewBackend(store, Config{
		Prefix: prefix, Identity: getStorageIdentity(), EnableEtcdCompatibility: true,
	}, mock.NewMinimalMetrics(ctrl)).(*backend)
	defer b.stopWorkers()
	b.SetCurrentRevision(uint64(time.Now().UnixNano()))
	ctx := context.Background()
	key := []byte(prefix + "/latest-metadata/large")

	created, err := b.Update(ctx, &proto.UpdateRequest{Kv: &proto.KeyValue{
		Key: key, Value: bytes.Repeat([]byte("v"), 2<<20),
	}})
	require.NoError(t, err)
	require.Eventually(t, func() bool {
		return b.GetCurrentRevision() >= created.Header.Revision
	}, 5*time.Second, 2*time.Millisecond)
	store.setObjectKey(b.coder.EncodeObjectKey(key, created.Header.Revision))

	indexKey := b.ks.EncodeLatestMetadataKey(key)
	indexValue, err := store.Get(ctx, indexKey)
	require.NoError(t, err)
	indexed, err := decodeLatestMetadata(indexValue)
	require.NoError(t, err)
	require.Equal(t, created.Header.Revision, indexed.ModRevision)
	require.Equal(t, EtcdMetadata{CreateRevision: created.Header.Revision, Version: 1}, indexed.Metadata)

	projected, err := b.GetKeysOnly(ctx, &proto.GetRequest{Key: key})
	require.NoError(t, err)
	require.NotNil(t, projected.Kv)
	require.Equal(t, created.Header.Revision, projected.Kv.Revision)
	require.Less(t, len(projected.Kv.Value), 64)
	require.Zero(t, store.objectReads.Load(), "metadata-index hit must not fetch the object payload")
	require.EqualValues(t, 1, store.batchReads.Load(), "the two small indexes should share one snapshot batch")

	full, err := b.Get(ctx, &proto.GetRequest{Key: key})
	require.NoError(t, err)
	require.Greater(t, len(full.Kv.Value), 2<<20)
	require.EqualValues(t, 1, store.objectReads.Load())

	for _, test := range []struct {
		name  string
		value []byte
	}{
		{name: "missing"},
		{name: "stale", value: encodeLatestMetadata(latestMetadata{
			ModRevision: created.Header.Revision - 1,
			Metadata:    EtcdMetadata{CreateRevision: created.Header.Revision - 1, Version: 1},
		})},
		{name: "malformed", value: []byte("not-an-index")},
	} {
		t.Run(test.name, func(t *testing.T) {
			batch := store.BeginBatchWrite()
			if test.value == nil {
				batch.Del(indexKey)
			} else {
				batch.Put(indexKey, test.value, 0)
			}
			require.NoError(t, batch.Commit(ctx))
			store.objectReads.Store(0)

			response, err := b.GetKeysOnly(ctx, &proto.GetRequest{Key: key})
			require.NoError(t, err)
			require.NotNil(t, response.Kv)
			require.Equal(t, created.Header.Revision, response.Kv.Revision)
			require.EqualValues(t, 1, store.objectReads.Load(), "optional-index miss must use the authoritative object path")
			require.Eventually(t, func() bool {
				raw, getErr := store.Get(ctx, indexKey)
				if getErr != nil {
					return false
				}
				meta, decodeErr := decodeLatestMetadata(raw)
				return decodeErr == nil && meta.ModRevision == created.Header.Revision
			}, 5*time.Second, 2*time.Millisecond)
			store.objectReads.Store(0)
			response, err = b.GetKeysOnly(ctx, &proto.GetRequest{Key: key})
			require.NoError(t, err)
			require.NotNil(t, response.Kv)
			require.Zero(t, store.objectReads.Load(), "the healed point metadata must avoid another object fetch")
		})
	}
}

func TestLatestKeysOnlyUsesTombstoneMetadataWithoutObjectRead(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	store := &latestMetadataReadProbeStorage{KvStorage: imemkv.NewKvStorage()}
	defer func() { require.NoError(t, store.Close()) }()
	b := NewBackend(store, Config{
		Prefix: prefix, Identity: getStorageIdentity(), EnableEtcdCompatibility: true,
	}, mock.NewMinimalMetrics(ctrl)).(*backend)
	b.SetCurrentRevision(uint64(time.Now().UnixNano()))
	ctx := context.Background()
	key := []byte(prefix + "/latest-metadata/deleted")

	created, err := b.Update(ctx, &proto.UpdateRequest{Kv: &proto.KeyValue{Key: key, Value: []byte("v")}})
	require.NoError(t, err)
	deleted, err := b.Delete(ctx, &proto.DeleteRequest{Key: key})
	require.NoError(t, err)
	require.Eventually(t, func() bool {
		return b.GetCurrentRevision() >= deleted.Header.Revision
	}, 5*time.Second, 2*time.Millisecond)
	require.Greater(t, deleted.Header.Revision, created.Header.Revision)
	store.setObjectKey(b.coder.EncodeObjectKey(key, deleted.Header.Revision))

	indexValue, err := store.Get(ctx, b.ks.EncodeLatestMetadataKey(key))
	require.NoError(t, err)
	indexed, err := decodeLatestMetadata(indexValue)
	require.NoError(t, err)
	require.Equal(t, latestMetadata{ModRevision: deleted.Header.Revision, Tombstone: true}, indexed)

	projected, err := b.GetKeysOnly(ctx, &proto.GetRequest{Key: key})
	require.NoError(t, err)
	require.Nil(t, projected.Kv)
	require.Equal(t, deleted.Header.Revision, projected.Header.Revision)
	require.Zero(t, store.objectReads.Load())
}

func TestLatestMetadataTracksUpdateLifecycleAndLease(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	store := imemkv.NewKvStorage()
	defer func() { require.NoError(t, store.Close()) }()
	b := NewBackend(store, Config{
		Prefix: prefix, Identity: getStorageIdentity(), EnableEtcdCompatibility: true,
	}, mock.NewMinimalMetrics(ctrl)).(*backend)
	b.SetCurrentRevision(uint64(time.Now().UnixNano()))
	ctx := context.Background()
	key := []byte(prefix + "/latest-metadata/update")

	_, createRevision, err := b.TxnApply(ctx, []TxnWriteOp{{Key: key, Value: []byte("v1")}}, nil)
	require.NoError(t, err)
	_, updateRevision, err := b.TxnApply(ctx, []TxnWriteOp{{
		Key: key, Value: []byte("v2"), Lease: -17,
	}}, nil)
	require.NoError(t, err)

	raw, err := store.Get(ctx, b.ks.EncodeLatestMetadataKey(key))
	require.NoError(t, err)
	indexed, err := decodeLatestMetadata(raw)
	require.NoError(t, err)
	require.Equal(t, latestMetadata{
		ModRevision: updateRevision,
		Metadata: EtcdMetadata{
			CreateRevision: createRevision,
			Version:        2,
			Lease:          -17,
		},
	}, indexed)

	projected, err := b.GetKeysOnly(ctx, &proto.GetRequest{Key: key})
	require.NoError(t, err)
	require.Equal(t, updateRevision, projected.Kv.Revision)
	meta, value, inlined, err := DecodeInlineValueChecked(projected.Kv.Value)
	require.NoError(t, err)
	require.True(t, inlined)
	require.Empty(t, value)
	require.Equal(t, EtcdMetadata{CreateRevision: createRevision, Version: 2}, meta,
		"FastKeysOnly must project the lease while preserving lifecycle metadata")
}
