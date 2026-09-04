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
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"

	proto "github.com/kubewharf/kubebrain-client/api/v2rpc"

	"github.com/kubewharf/kubebrain/pkg/metrics/mock"
	"github.com/kubewharf/kubebrain/pkg/storage"
	imemkv "github.com/kubewharf/kubebrain/pkg/storage/memkv"
)

func newLatestMetadataRangeBackend(t *testing.T) (*backend, *latestMetadataReadProbeStorage) {
	t.Helper()
	ctrl := gomock.NewController(t)
	store := &latestMetadataReadProbeStorage{KvStorage: imemkv.NewKvStorage()}
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	b := NewBackend(store, Config{
		Prefix: prefix, Identity: getStorageIdentity(), EnableEtcdCompatibility: true,
		EnableCountIndex: true,
	}, mock.NewMinimalMetrics(ctrl)).(*backend)
	b.SetCurrentRevision(uint64(time.Now().UnixNano()))
	return b, store
}

func TestLatestListKeysOnlyUsesMetadataDirectoryWithoutObjectScan(t *testing.T) {
	b, store := newLatestMetadataRangeBackend(t)
	ctx := context.Background()
	p := prefix + "/latest-metadata-range/"
	end := PrefixEnd([]byte(p))
	keys := [][]byte{[]byte(p + "$a"), []byte(p + "a"), []byte(p + "b")}
	createRevisions := make([]uint64, len(keys))
	for i, key := range keys {
		response, err := b.Update(ctx, &proto.UpdateRequest{Kv: &proto.KeyValue{
			Key: key, Value: bytes.Repeat([]byte{byte('a' + i)}, 2<<20),
		}})
		require.NoError(t, err)
		createRevisions[i] = response.Header.Revision
	}
	updated, err := b.Update(ctx, &proto.UpdateRequest{Kv: &proto.KeyValue{
		Key: keys[1], Value: bytes.Repeat([]byte("z"), 2<<20), Revision: createRevisions[1],
	}})
	require.NoError(t, err)
	require.True(t, updated.Succeeded)
	require.Eventually(t, func() bool { return b.GetCurrentRevision() >= updated.Header.Revision }, 5*time.Second, 2*time.Millisecond)
	require.NoError(t, b.RebuildCountIndex(ctx))

	store.iterReads.Store(0)
	store.objectReads.Store(0)
	store.batchReads.Store(0)
	response, err := b.ListKeysOnly(ctx, &proto.RangeRequest{Key: []byte(p), End: end})
	require.NoError(t, err)
	require.Len(t, response.Kvs, len(keys))
	for i, kv := range response.Kvs {
		require.Equal(t, keys[i], kv.Key)
		require.Empty(t, metadataRawValue(t, kv.Value))
		meta, _, _, decodeErr := DecodeInlineValueChecked(kv.Value)
		require.NoError(t, decodeErr)
		require.Equal(t, createRevisions[i], meta.CreateRevision)
		if i == 1 {
			require.Equal(t, uint64(2), meta.Version)
		} else {
			require.Equal(t, uint64(1), meta.Version)
		}
	}
	require.Zero(t, store.iterReads.Load(), "metadata directory hit must not scan physical object versions")
	require.Zero(t, store.objectReads.Load(), "metadata directory hit must not fetch object payloads")
	require.Positive(t, store.batchReads.Load())

	store.iterReads.Store(0)
	limited, err := b.ListKeysOnly(ctx, &proto.RangeRequest{Key: []byte(p), End: end, Limit: 2})
	require.NoError(t, err)
	require.True(t, limited.More)
	require.Len(t, limited.Kvs, 2)
	require.Equal(t, keys[:2], [][]byte{limited.Kvs[0].Key, limited.Kvs[1].Key})
	require.Zero(t, store.iterReads.Load(), "limited metadata range must retain the directory fast path")

	// A node written by an older binary has no auxiliary row. Keep the
	// count-index ordering fast path, but resolve that key through its
	// authoritative object row instead of omitting it.
	missingMetadata := store.BeginBatchWrite()
	missingMetadata.Del(b.ks.EncodeLatestMetadataKey(keys[2]))
	require.NoError(t, missingMetadata.Commit(ctx))
	store.setObjectKey(b.coder.EncodeObjectKey(keys[2], createRevisions[2]))
	store.objectReads.Store(0)
	response, err = b.ListKeysOnly(ctx, &proto.RangeRequest{Key: []byte(p), End: end})
	require.NoError(t, err)
	require.Len(t, response.Kvs, len(keys))
	require.Equal(t, keys[2], response.Kvs[2].Key)
	require.EqualValues(t, 1, store.objectReads.Load(), "an old-writer row must fall back to its object")
}

func TestLatestRangeStreamKeysOnlyPaginatesMetadataDirectory(t *testing.T) {
	b, store := newLatestMetadataRangeBackend(t)
	ctx := context.Background()
	p := prefix + "/latest-metadata-pages/"
	const total = latestRangeStreamIndexPage + 1
	var lastRevision uint64
	for i := 0; i < total; i++ {
		key := []byte(fmt.Sprintf("%s%03d", p, i))
		response, err := b.Update(ctx, &proto.UpdateRequest{Kv: &proto.KeyValue{Key: key, Value: []byte("v")}})
		require.NoError(t, err)
		require.True(t, response.Succeeded)
		lastRevision = response.Header.Revision
	}
	require.Eventually(t, func() bool { return b.GetCurrentRevision() >= lastRevision }, 5*time.Second, 2*time.Millisecond)
	require.NoError(t, b.RebuildCountIndex(ctx))
	store.iterReads.Store(0)

	stream, err := b.RangeStreamKeysOnly(ctx, []byte(p), PrefixEnd([]byte(p)), 0)
	require.NoError(t, err)
	var keys [][]byte
	for chunk := range stream {
		require.Empty(t, chunk.Err)
		for _, kv := range chunk.RangeResponse.Kvs {
			keys = append(keys, kv.Key)
		}
	}
	require.Len(t, keys, total)
	for i, key := range keys {
		require.Equal(t, []byte(fmt.Sprintf("%s%03d", p, i)), key)
	}
	require.Zero(t, store.iterReads.Load())
}

func TestLatestRangeStreamKeysOnlyFallsBackAtPinnedOlderRevision(t *testing.T) {
	b, store := newLatestMetadataRangeBackend(t)
	ctx := context.Background()
	p := prefix + "/latest-metadata-stream/"
	key := []byte(p + "key")
	end := PrefixEnd([]byte(p))
	created, err := b.Update(ctx, &proto.UpdateRequest{Kv: &proto.KeyValue{
		Key: key, Value: bytes.Repeat([]byte("a"), 2<<20),
	}})
	require.NoError(t, err)
	require.Eventually(t, func() bool { return b.GetCurrentRevision() >= created.Header.Revision }, 5*time.Second, 2*time.Millisecond)
	require.NoError(t, b.RebuildCountIndex(ctx))
	updated, err := b.Update(ctx, &proto.UpdateRequest{Kv: &proto.KeyValue{
		Key: key, Value: bytes.Repeat([]byte("b"), 2<<20), Revision: created.Header.Revision,
	}})
	require.NoError(t, err)
	require.True(t, updated.Succeeded)
	require.Greater(t, updated.Header.Revision, created.Header.Revision)
	require.Eventually(t, func() bool { return b.GetCurrentRevision() >= updated.Header.Revision }, 5*time.Second, 2*time.Millisecond)

	store.setObjectKey(b.coder.EncodeObjectKey(key, updated.Header.Revision))
	store.iterReads.Store(0)
	store.objectReads.Store(0)
	latestStream, err := b.RangeStreamKeysOnly(context.Background(), []byte(p), end, 0)
	require.NoError(t, err)
	var latest []*proto.KeyValue
	for chunk := range latestStream {
		require.Empty(t, chunk.Err)
		latest = append(latest, chunk.RangeResponse.Kvs...)
	}
	require.Len(t, latest, 1)
	latestMeta, latestValue, latestInlined, err := DecodeInlineValueChecked(latest[0].Value)
	require.NoError(t, err)
	require.True(t, latestInlined)
	require.Empty(t, latestValue)
	require.Equal(t, EtcdMetadata{CreateRevision: created.Header.Revision, Version: 2}, latestMeta)
	require.Zero(t, store.iterReads.Load(), "latest metadata stream must not scan object versions")
	require.Zero(t, store.objectReads.Load(), "latest metadata stream must not fetch the current object")

	store.iterReads.Store(0)
	stream, err := b.RangeStreamKeysOnly(WithLatestRangeStream(ctx), []byte(p), end, created.Header.Revision)
	require.NoError(t, err)
	var kvs []*proto.KeyValue
	for chunk := range stream {
		require.Empty(t, chunk.Err)
		kvs = append(kvs, chunk.RangeResponse.Kvs...)
	}
	require.Len(t, kvs, 1)
	require.Equal(t, key, kvs[0].Key)
	require.Equal(t, created.Header.Revision, kvs[0].Revision)
	meta, value, inlined, err := DecodeInlineValueChecked(kvs[0].Value)
	require.NoError(t, err)
	require.True(t, inlined)
	require.Empty(t, value)
	require.Equal(t, EtcdMetadata{CreateRevision: created.Header.Revision, Version: 1}, meta)
	require.Positive(t, store.iterReads.Load(), "a newer latest-directory row must fall back to the historical object")
}

func metadataRawValue(t *testing.T, value []byte) []byte {
	t.Helper()
	_, raw, inlined, err := DecodeInlineValueChecked(value)
	require.NoError(t, err)
	require.True(t, inlined)
	return raw
}

var _ storage.SnapshotGetter = (*latestMetadataReadProbeStorage)(nil)
