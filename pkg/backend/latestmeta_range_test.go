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
	"sort"
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
	t.Cleanup(b.stopWorkers)
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
	store.setObjectKey(b.coder.EncodeObjectKey(keys[2], createRevisions[2]))
	for _, test := range []struct {
		name  string
		value []byte
	}{
		{name: "missing"},
		{name: "stale", value: encodeLatestMetadata(latestMetadata{
			ModRevision: createRevisions[2] - 1,
			Metadata: EtcdMetadata{
				CreateRevision: createRevisions[2] - 1,
				Version:        1,
			},
		})},
		{name: "malformed", value: []byte("not-an-index")},
	} {
		t.Run(test.name, func(t *testing.T) {
			seed := store.BeginBatchWrite()
			if test.value == nil {
				seed.Del(b.ks.EncodeLatestMetadataKey(keys[2]))
			} else {
				seed.Put(b.ks.EncodeLatestMetadataKey(keys[2]), test.value, 0)
			}
			require.NoError(t, seed.Commit(ctx))
			store.objectReads.Store(0)
			response, readErr := b.ListKeysOnly(ctx, &proto.RangeRequest{Key: []byte(p), End: end})
			require.NoError(t, readErr)
			require.Len(t, response.Kvs, len(keys))
			require.Equal(t, keys[2], response.Kvs[2].Key)
			require.EqualValues(t, 1, store.objectReads.Load(), "an old-writer row must fall back to its object")
			require.Eventually(t, func() bool {
				raw, getErr := store.Get(ctx, b.ks.EncodeLatestMetadataKey(keys[2]))
				if getErr != nil {
					return false
				}
				meta, decodeErr := decodeLatestMetadata(raw)
				return decodeErr == nil && meta.ModRevision == createRevisions[2]
			}, 5*time.Second, 2*time.Millisecond, "the bounded backfill worker must heal an old-writer row")
			store.objectReads.Store(0)
			response, readErr = b.ListKeysOnly(ctx, &proto.RangeRequest{Key: []byte(p), End: end})
			require.NoError(t, readErr)
			require.Len(t, response.Kvs, len(keys))
			require.Zero(t, store.objectReads.Load(), "the healed metadata row must make the next range payload-free")
		})
	}
}

func TestLatestListKeysOnlyUsesKeyOnlyFallbackWhenCountIndexUnavailable(t *testing.T) {
	b, store := newLatestMetadataRangeBackend(t)
	ctx := context.Background()
	p := prefix + "/latest-metadata-key-scan/"
	end := PrefixEnd([]byte(p))
	keys := [][]byte{[]byte(p + "$a"), []byte(p + "a"), []byte(p + "b")}
	for i, key := range keys {
		_, err := b.Update(ctx, &proto.UpdateRequest{Kv: &proto.KeyValue{
			Key: key, Value: bytes.Repeat([]byte{byte('a' + i)}, 2<<20),
		}})
		require.NoError(t, err)
	}
	require.NoError(t, b.RebuildCountIndex(ctx))
	b.countIndex.Invalidate()

	store.iterReads.Store(0)
	store.keyIterReads.Store(0)
	store.objectReads.Store(0)
	response, err := b.ListKeysOnly(ctx, &proto.RangeRequest{Key: []byte(p), End: end})
	require.NoError(t, err)
	require.Len(t, response.Kvs, len(keys))
	for i, kv := range response.Kvs {
		require.Equal(t, keys[i], kv.Key)
		require.Empty(t, metadataRawValue(t, kv.Value))
	}
	require.Positive(t, store.keyIterReads.Load(), "index miss must scan physical keys without values")
	require.Zero(t, store.iterReads.Load(), "key-only fallback must not use a value-carrying iterator")
	require.Zero(t, store.objectReads.Load(), "complete metadata rows must not fetch object payloads")

	// A row from an old writer is still discovered from its physical object
	// keys. Only that key fetches its object and the ordinary bounded healer
	// restores the auxiliary row.
	missingMetadata := b.ks.EncodeLatestMetadataKey(keys[1])
	remove := store.BeginBatchWrite()
	remove.Del(missingMetadata)
	require.NoError(t, remove.Commit(ctx))
	store.setObjectKey(b.coder.EncodeObjectKey(keys[1], response.Kvs[1].Revision))
	store.keyIterReads.Store(0)
	store.objectReads.Store(0)
	response, err = b.ListKeysOnly(ctx, &proto.RangeRequest{Key: []byte(p), End: end})
	require.NoError(t, err)
	require.Len(t, response.Kvs, len(keys))
	require.Positive(t, store.keyIterReads.Load())
	require.EqualValues(t, 1, store.objectReads.Load(), "only the old-writer key may fetch its object")
	require.Eventually(t, func() bool {
		raw, getErr := store.Get(ctx, missingMetadata)
		if getErr != nil {
			return false
		}
		meta, decodeErr := decodeLatestMetadata(raw)
		return decodeErr == nil && meta.ModRevision == response.Kvs[1].Revision
	}, 5*time.Second, 2*time.Millisecond)

	deleted, err := b.Delete(ctx, &proto.DeleteRequest{Key: keys[2]})
	require.NoError(t, err)
	require.Eventually(t, func() bool { return b.GetCurrentRevision() >= deleted.Header.Revision }, 5*time.Second, 2*time.Millisecond)
	store.objectReads.Store(0)
	response, err = b.ListKeysOnly(ctx, &proto.RangeRequest{Key: []byte(p), End: end})
	require.NoError(t, err)
	require.Len(t, response.Kvs, 2, "matching small tombstone directories must suppress the deleted key")
	require.Zero(t, store.objectReads.Load())

	limited, err := b.ListKeysOnly(ctx, &proto.RangeRequest{Key: []byte(p), End: end, Limit: 1})
	require.NoError(t, err)
	require.True(t, limited.More)
	require.Len(t, limited.Kvs, 1)
	require.Equal(t, keys[0], limited.Kvs[0].Key)
}

func TestLatestMetadataBackfillCannotReplaceConcurrentUpdate(t *testing.T) {
	b, store := newLatestMetadataRangeBackend(t)
	ctx := context.Background()
	key := []byte(prefix + "/latest-metadata-backfill/concurrent")
	created, err := b.Update(ctx, &proto.UpdateRequest{Kv: &proto.KeyValue{Key: key, Value: []byte("one")}})
	require.NoError(t, err)
	remove := store.BeginBatchWrite()
	remove.Del(b.ks.EncodeLatestMetadataKey(key))
	require.NoError(t, remove.Commit(ctx))

	task := latestMetadataBackfillTask{
		key: key, revision: created.Header.Revision,
		metadata: EtcdMetadata{CreateRevision: created.Header.Revision, Version: 1},
	}
	updated, err := b.Update(ctx, &proto.UpdateRequest{Kv: &proto.KeyValue{
		Key: key, Value: []byte("two"), Revision: created.Header.Revision,
	}})
	require.NoError(t, err)
	require.True(t, updated.Succeeded)
	require.ErrorIs(t, b.applyLatestMetadataBackfill(ctx, task), storage.ErrCASFailed)

	raw, err := store.Get(ctx, b.ks.EncodeLatestMetadataKey(key))
	require.NoError(t, err)
	meta, err := decodeLatestMetadata(raw)
	require.NoError(t, err)
	require.Equal(t, updated.Header.Revision, meta.ModRevision)
	require.Equal(t, EtcdMetadata{CreateRevision: created.Header.Revision, Version: 2}, meta.Metadata)
}

func TestLatestMetadataBackfillHonorsCorruptAlarm(t *testing.T) {
	b, store := newLatestMetadataRangeBackend(t)
	ctx := context.Background()
	key := []byte(prefix + "/latest-metadata-backfill/corrupt")
	created, err := b.Update(ctx, &proto.UpdateRequest{Kv: &proto.KeyValue{Key: key, Value: []byte("value")}})
	require.NoError(t, err)
	remove := store.BeginBatchWrite()
	remove.Del(b.ks.EncodeLatestMetadataKey(key))
	require.NoError(t, remove.Commit(ctx))
	require.NoError(t, b.ArmCorrupt(ctx, 5731001))

	err = b.applyLatestMetadataBackfill(ctx, latestMetadataBackfillTask{
		key: key, revision: created.Header.Revision,
		metadata: EtcdMetadata{CreateRevision: created.Header.Revision, Version: 1},
	})
	require.ErrorIs(t, err, ErrCorruptAlarmActive)
	_, err = store.Get(ctx, b.ks.EncodeLatestMetadataKey(key))
	require.ErrorIs(t, err, storage.ErrKeyNotFound)
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

func TestLatestRangeStreamKeysOnlyUsesKeyOnlySpillWhenCountIndexUnavailable(t *testing.T) {
	b, store := newLatestMetadataRangeBackend(t)
	ctx := context.Background()
	p := prefix + "/latest-metadata-key-spill/"
	end := PrefixEnd([]byte(p))
	keys := make([][]byte, 0, latestRangeStreamIndexPage+2)
	keys = append(keys, []byte(p+"$a"), []byte(p+"a"), []byte(p+"b"))
	for i := 0; len(keys) < latestRangeStreamIndexPage+2; i++ {
		keys = append(keys, []byte(fmt.Sprintf("%sk%03d", p, i)))
	}
	var missingMetadataRevision uint64
	for i, key := range keys {
		value := []byte("v")
		if i < 3 {
			value = bytes.Repeat([]byte{byte('a' + i)}, 2<<20)
		}
		response, err := b.Update(ctx, &proto.UpdateRequest{Kv: &proto.KeyValue{Key: key, Value: value}})
		require.NoError(t, err)
		if i == 1 {
			missingMetadataRevision = response.Header.Revision
		}
	}
	require.NoError(t, b.RebuildCountIndex(ctx))

	remove := store.BeginBatchWrite()
	remove.Del(b.ks.EncodeLatestMetadataKey(keys[1]))
	require.NoError(t, remove.Commit(ctx))
	store.setObjectKey(b.coder.EncodeObjectKey(keys[1], missingMetadataRevision))
	deleted, err := b.Delete(ctx, &proto.DeleteRequest{Key: keys[len(keys)-1]})
	require.NoError(t, err)
	require.Eventually(t, func() bool { return b.GetCurrentRevision() >= deleted.Header.Revision }, 5*time.Second, 2*time.Millisecond)
	b.countIndex.Invalidate()
	spillDir := t.TempDir()
	t.Setenv("TMPDIR", spillDir)

	store.iterReads.Store(0)
	store.keyIterReads.Store(0)
	store.objectReads.Store(0)
	store.batchReads.Store(0)
	stream, err := b.RangeStreamKeysOnly(ctx, []byte(p), end, 0)
	require.NoError(t, err)
	var actual [][]byte
	var dataChunks, terminalChunks int
	for chunk := range stream {
		require.Empty(t, chunk.Err)
		require.NotNil(t, chunk.RangeResponse)
		if len(chunk.RangeResponse.Kvs) == 0 {
			terminalChunks++
			require.False(t, chunk.RangeResponse.More)
			continue
		}
		dataChunks++
		require.True(t, chunk.RangeResponse.More)
		for _, kv := range chunk.RangeResponse.Kvs {
			actual = append(actual, kv.Key)
			require.Empty(t, metadataRawValue(t, kv.Value))
		}
	}
	expected := append([][]byte(nil), keys[:len(keys)-1]...)
	sort.Slice(expected, func(i, j int) bool { return bytes.Compare(expected[i], expected[j]) < 0 })
	require.Equal(t, expected, actual)
	require.GreaterOrEqual(t, dataChunks, 2, "the spill join must emit bounded pages")
	require.Equal(t, 1, terminalChunks)
	require.Positive(t, store.keyIterReads.Load(), "index miss must scan TiKV physical keys without values")
	require.Zero(t, store.iterReads.Load(), "streaming key-only fallback must not use a value-carrying iterator")
	require.EqualValues(t, 1, store.objectReads.Load(), "only the old-writer key may fetch its object")
	require.Positive(t, store.batchReads.Load())
	entries, err := os.ReadDir(spillDir)
	require.NoError(t, err)
	require.Empty(t, entries, "the key-only stream spill must be removed after terminal delivery")
	require.Eventually(t, func() bool {
		raw, getErr := store.Get(ctx, b.ks.EncodeLatestMetadataKey(keys[1]))
		if getErr != nil {
			return false
		}
		meta, decodeErr := decodeLatestMetadata(raw)
		return decodeErr == nil && meta.ModRevision == missingMetadataRevision
	}, 5*time.Second, 2*time.Millisecond)
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
