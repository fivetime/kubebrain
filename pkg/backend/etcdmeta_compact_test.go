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
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"

	proto "github.com/kubewharf/kubebrain-client/api/v2rpc"

	"github.com/kubewharf/kubebrain/pkg/metrics/mock"
	"github.com/kubewharf/kubebrain/pkg/storage"
	imemkv "github.com/kubewharf/kubebrain/pkg/storage/memkv"
)

type metadataIterTimestampStorage struct {
	storage.KvStorage
	iterTimestamps []uint64
	getTimestamps  []uint64
}

func (s *metadataIterTimestampStorage) Iter(
	ctx context.Context, start, end []byte, timestamp, limit uint64,
) (storage.Iter, error) {
	s.iterTimestamps = append(s.iterTimestamps, timestamp)
	return s.KvStorage.Iter(ctx, start, end, timestamp, limit)
}

func (s *metadataIterTimestampStorage) GetAt(ctx context.Context, key []byte, timestamp uint64) ([]byte, error) {
	s.getTimestamps = append(s.getTimestamps, timestamp)
	return s.KvStorage.Get(ctx, key)
}

func (s *metadataIterTimestampStorage) BatchGetAt(
	ctx context.Context, keys [][]byte, timestamp uint64,
) (map[string][]byte, error) {
	result := make(map[string][]byte, len(keys))
	for _, key := range keys {
		value, err := s.GetAt(ctx, key, timestamp)
		if errors.Is(err, storage.ErrKeyNotFound) {
			continue
		}
		if err != nil {
			return nil, err
		}
		result[string(key)] = value
	}
	return result, nil
}

// TestCompactRetiresLegacyEtcdMetadata verifies that the etcdmeta keyspace,
// which used to grow without bound outside the compaction borders (#6/#15), is
// now GC'd by compaction: superseded metadata versions are removed while the
// latest survives and metadata reads stay correct.
func TestCompactRetiresLegacyEtcdMetadata(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	m := mock.NewMinimalMetrics(ctrl)
	kv := imemkv.NewKvStorage()
	defer func() { require.NoError(t, kv.Close()) }()
	// Non-compat mode writes the separate etcdmeta keyspace (legacy behavior).
	b := NewBackend(kv, Config{Prefix: prefix, Identity: getStorageIdentity()}, m).(*backend)
	b.SetCurrentRevision(uint64(time.Now().UnixNano()))
	ctx := context.Background()

	key := []byte(prefix + "/reg/a")
	cr, err := b.Create(ctx, &proto.CreateRequest{Key: key, Value: []byte("v1")})
	require.NoError(t, err)
	createRev := cr.Header.Revision
	last := createRev
	for i := 2; i <= 5; i++ {
		u, err := b.Update(ctx, &proto.UpdateRequest{Kv: &proto.KeyValue{Key: key, Value: []byte(fmt.Sprintf("v%d", i)), Revision: last}})
		require.NoError(t, err)
		require.True(t, u.Succeeded)
		last = u.Header.Revision
	}
	require.Eventually(t, func() bool { return b.GetCurrentRevision() >= last }, 5*time.Second, 2*time.Millisecond)

	countMeta := func() int {
		iter, err := b.kv.Iter(ctx,
			b.coder.EncodeObjectKey(etcdMetadataPrefix, 0),
			b.coder.EncodeObjectKey(PrefixEnd(etcdMetadataPrefix), 0), 0, 0)
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

	require.Equal(t, 5, countMeta(), "5 writes -> 5 etcdmeta versions before compact")
	mb, err := b.GetEtcdMetadata(ctx, key, last)
	require.NoError(t, err)
	require.Equal(t, createRev, mb.CreateRevision)
	require.Equal(t, uint64(5), mb.Version)

	_, err = b.Compact(ctx, last)
	require.NoError(t, err)

	require.Equal(t, 1, countMeta(), "compaction should retire superseded etcdmeta versions, keeping only the latest")

	// Metadata for the current version is still correct after compaction.
	ma, err := b.GetEtcdMetadata(ctx, key, last)
	require.NoError(t, err)
	require.Equal(t, createRev, ma.CreateRevision)
	require.Equal(t, uint64(5), ma.Version)
}

// Legacy metadata used a second object-key namespace with the same unescaped
// '$' delimiter as user objects. A metadata key whose suffix begins with bytes
// between two timestamp revisions sorts inside the reverse interval for "a";
// the fallback must not return that foreign row as "a"'s metadata.
func TestLegacyEtcdMetadataDollarExtensionDoesNotShadowShorterKey(t *testing.T) {
	for name, storageType := range map[string]storageType{
		"memory": memKvStorage,
		"tikv":   tiKvStorage,
	} {
		t.Run(name, func(t *testing.T) {
			s, closeSuite := newTestSuites(t, storageType)
			defer closeSuite()
			b := s.backend.(*backend)

			lower := []byte("/registry/items/a")
			const lowerRevision = uint64(0x1800000000000100)
			const foreignBoundary = uint64(0x1800000000000200)
			const requestedRevision = uint64(0x1800000000000300)
			foreignSuffix := make([]byte, 8)
			binary.BigEndian.PutUint64(foreignSuffix, foreignBoundary)
			target := append(append(append([]byte(nil), lower...), '$'), foreignSuffix...)
			target = append(target, 'x')
			const targetRevision = uint64(0x1800000000000400)
			lowerMetadata := EtcdMetadata{CreateRevision: lowerRevision - 10, Version: 3}
			targetMetadata := EtcdMetadata{CreateRevision: targetRevision - 20, Version: 7}

			batch := s.kv.BeginBatchWrite()
			b.putEtcdMetadata(batch, lower, lowerRevision, lowerMetadata)
			b.putEtcdMetadata(batch, target, targetRevision, targetMetadata)
			require.NoError(t, batch.Commit(s.ctx))

			got, err := b.getEtcdMetadata(s.ctx, lower, lowerRevision)
			require.NoError(t, err)
			require.Equal(t, lowerMetadata, got)
			_, err = b.getEtcdMetadata(s.ctx, lower, requestedRevision)
			require.ErrorIs(t, err, storage.ErrKeyNotFound,
				"a missing exact metadata row must not borrow its predecessor")
		})
	}
}

func TestGetEtcdMetadataClassifiesInvalidLegacyEncoding(t *testing.T) {
	s, closeSuite := newTestSuites(t, memKvStorage)
	defer closeSuite()
	b := s.backend.(*backend)

	key := []byte("/registry/items/corrupt-metadata")
	const revision = uint64(42)
	batch := s.kv.BeginBatchWrite()
	batch.Put(b.coder.EncodeObjectKey(b.etcdMetadataUserKey(key), revision), []byte{1}, 0)
	require.NoError(t, batch.Commit(s.ctx))

	_, err := b.GetEtcdMetadata(s.ctx, key, revision)
	require.ErrorIs(t, err, ErrInvalidMVCCMetadata)
	require.ErrorContains(t, err, "invalid etcd metadata length 1")
}

func TestGetEtcdMetadataRejectsLegacyWireOverflow(t *testing.T) {
	s, closeSuite := newTestSuites(t, memKvStorage)
	defer closeSuite()
	b := s.backend.(*backend)
	key := []byte("/registry/items/overflow-metadata")
	const revision = uint64(42)
	raw := make([]byte, 16)
	binary.BigEndian.PutUint64(raw[:8], uint64(math.MaxInt64)+1)
	binary.BigEndian.PutUint64(raw[8:], 1)
	batch := s.kv.BeginBatchWrite()
	batch.Put(b.coder.EncodeObjectKey(b.etcdMetadataUserKey(key), revision), raw, 0)
	require.NoError(t, batch.Commit(s.ctx))

	_, err := b.GetEtcdMetadata(s.ctx, key, revision)
	require.ErrorIs(t, err, ErrInvalidMVCCMetadata)
	require.ErrorContains(t, err, "create revision 9223372036854775808 exceeds MaxInt64")
}

func TestGetEtcdMetadataRecoversProvenRetainedLegacyGeneration(t *testing.T) {
	tests := []struct {
		name            string
		compactRevision uint64
		key             []byte
		rows            []struct {
			revision uint64
			value    []byte
		}
		modRevision uint64
		want        EtcdMetadata
	}{
		{
			name: "uncompacted raw history",
			key:  []byte("/registry/items/recovered"),
			rows: []struct {
				revision uint64
				value    []byte
			}{{2, []byte("v1")}, {4, []byte("v2")}, {7, []byte("v3")}},
			modRevision: 7,
			want:        EtcdMetadata{CreateRevision: 2, Version: 3},
		},
		{
			name:            "visible tombstone starts generation",
			compactRevision: 3,
			key:             []byte("/registry/items/recreated"),
			rows: []struct {
				revision uint64
				value    []byte
			}{{2, []byte("old")}, {3, tombStoneBytes}, {5, []byte("new-v1")}, {7, []byte("new-v2")}},
			modRevision: 7,
			want:        EtcdMetadata{CreateRevision: 5, Version: 2},
		},
		{
			name:            "inline anchor before compact watermark",
			compactRevision: 3,
			key:             []byte("/registry/items/inline-anchor"),
			rows: []struct {
				revision uint64
				value    []byte
			}{{2, encodeValueWithMeta([]byte("v4"), EtcdMetadata{CreateRevision: 1, Version: 4})}, {5, []byte("v5")}},
			modRevision: 5,
			want:        EtcdMetadata{CreateRevision: 1, Version: 5},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			s, closeSuite := newTestSuites(t, memKvStorage)
			defer closeSuite()
			b := s.backend.(*backend)
			batch := s.kv.BeginBatchWrite()
			if test.compactRevision != 0 {
				batch.Put(getCompactKey(b.config.Prefix), uint64ToBytes(test.compactRevision), 0)
			}
			for _, row := range test.rows {
				batch.Put(b.coder.EncodeObjectKey(test.key, row.revision), row.value, 0)
			}
			require.NoError(t, batch.Commit(s.ctx))

			got, err := b.GetEtcdMetadata(s.ctx, test.key, test.modRevision)
			require.NoError(t, err)
			require.Equal(t, test.want, got)
		})
	}
}

func TestGetEtcdMetadataDoesNotInventMetadataFromCompactedLiveAnchor(t *testing.T) {
	s, closeSuite := newTestSuites(t, memKvStorage)
	defer closeSuite()
	b := s.backend.(*backend)
	key := []byte("/registry/items/unknown-anchor")
	batch := s.kv.BeginBatchWrite()
	batch.Put(getCompactKey(b.config.Prefix), uint64ToBytes(3), 0)
	batch.Put(b.coder.EncodeObjectKey(key, 2), []byte("unknown-version"), 0)
	batch.Put(b.coder.EncodeObjectKey(key, 5), []byte("later-update"), 0)
	require.NoError(t, batch.Commit(s.ctx))

	got, err := b.GetEtcdMetadata(s.ctx, key, 5)
	require.NoError(t, err)
	require.Equal(t, EtcdMetadata{CreateRevision: 5, Version: 1}, got,
		"an unproven compact anchor must retain the conservative legacy fallback")
}

func TestGetEtcdMetadataUsesRetainedLegacyAnchorWithoutBorrowingIt(t *testing.T) {
	s, closeSuite := newTestSuites(t, memKvStorage)
	defer closeSuite()
	b := s.backend.(*backend)
	key := []byte("/registry/items/proven-legacy-anchor")
	batch := s.kv.BeginBatchWrite()
	batch.Put(getCompactKey(b.config.Prefix), uint64ToBytes(3), 0)
	batch.Put(b.coder.EncodeObjectKey(key, 2), []byte("v1"), 0)
	b.putEtcdMetadata(batch, key, 2, EtcdMetadata{CreateRevision: 2, Version: 1})
	batch.Put(b.coder.EncodeObjectKey(key, 5), []byte("v2"), 0)
	require.NoError(t, batch.Commit(s.ctx))

	got, err := b.GetEtcdMetadata(s.ctx, key, 5)
	require.NoError(t, err)
	require.Equal(t, EtcdMetadata{CreateRevision: 2, Version: 2}, got,
		"the exact predecessor is an anchor, not the target version's metadata")
}

func TestGetEtcdMetadataDoesNotBorrowPredecessorInlineMetadata(t *testing.T) {
	s, closeSuite := newTestSuites(t, memKvStorage)
	defer closeSuite()
	b := s.backend.(*backend)
	key := []byte("/registry/items/predecessor-inline")
	batch := s.kv.BeginBatchWrite()
	batch.Put(b.coder.EncodeObjectKey(key, 2), encodeValueWithMeta(
		[]byte("v1"), EtcdMetadata{CreateRevision: 2, Version: 1}), 0)
	require.NoError(t, batch.Commit(s.ctx))

	got, err := b.GetEtcdMetadata(s.ctx, key, 5)
	require.NoError(t, err)
	require.Equal(t, EtcdMetadata{CreateRevision: 5, Version: 1}, got,
		"inline metadata belongs only to its exact object revision")
}

func TestRecoverRetainedEtcdMetadataFiltersArbitraryByteKeyAndRejectsDiscontinuity(t *testing.T) {
	s, closeSuite := newTestSuites(t, memKvStorage)
	defer closeSuite()
	b := s.backend.(*backend)
	key := []byte{'/', 'a', '$', 0x00, 0xff}
	foreign := append(append([]byte(nil), key...), 0x01)
	batch := s.kv.BeginBatchWrite()
	batch.Put(b.coder.EncodeObjectKey(key, 2), []byte("v1"), 0)
	batch.Put(b.coder.EncodeObjectKey(foreign, 3), []byte("foreign"), 0)
	batch.Put(b.coder.EncodeObjectKey(key, 4), encodeValueWithMeta([]byte("bad"), EtcdMetadata{CreateRevision: 2, Version: 9}), 0)
	require.NoError(t, batch.Commit(s.ctx))

	_, proven, err := b.recoverRetainedEtcdMetadata(s.ctx, key, 4)
	require.False(t, proven)
	require.ErrorIs(t, err, ErrInvalidMVCCMetadata)
	require.ErrorContains(t, err, "retained metadata discontinuity")
}

func TestLegacyEtcdMetadataReadsUsePinnedSnapshotTimestamp(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	metrics := mock.NewMinimalMetrics(ctrl)
	raw := imemkv.NewKvStorage()
	defer func() { require.NoError(t, raw.Close()) }()
	store := &metadataIterTimestampStorage{KvStorage: raw}
	b := NewBackend(store, Config{Prefix: prefix, Identity: getStorageIdentity()}, metrics).(*backend)
	key := []byte("/registry/items/pinned-meta")
	const revision = uint64(8)
	batch := raw.BeginBatchWrite()
	b.putEtcdMetadata(batch, key, revision, EtcdMetadata{CreateRevision: 3, Version: 2})
	require.NoError(t, batch.Commit(context.Background()))

	ctx := storage.WithSnapshotTimestamp(context.Background(), 4242)
	got, err := b.getEtcdMetadata(ctx, key, revision)
	require.NoError(t, err)
	require.Equal(t, EtcdMetadata{CreateRevision: 3, Version: 2}, got)
	require.Equal(t, []uint64{4242}, store.getTimestamps)
	require.Empty(t, store.iterTimestamps)
}

func TestRecoverRetainedEtcdMetadataReadsUsePinnedSnapshotTimestamp(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	metrics := mock.NewMinimalMetrics(ctrl)
	raw := imemkv.NewKvStorage()
	defer func() { require.NoError(t, raw.Close()) }()
	store := &metadataIterTimestampStorage{KvStorage: raw}
	b := NewBackend(store, Config{Prefix: prefix, Identity: getStorageIdentity()}, metrics).(*backend)
	key := []byte("/registry/items/pinned-recovery")
	batch := raw.BeginBatchWrite()
	batch.Put(b.coder.EncodeObjectKey(key, 2), []byte("v1"), 0)
	batch.Put(b.coder.EncodeObjectKey(key, 4), []byte("v2"), 0)
	require.NoError(t, batch.Commit(context.Background()))

	ctx := storage.WithSnapshotTimestamp(context.Background(), 4343)
	got, proven, err := b.recoverRetainedEtcdMetadata(ctx, key, 4)
	require.NoError(t, err)
	require.True(t, proven)
	require.Equal(t, EtcdMetadata{CreateRevision: 2, Version: 2}, got)
	require.Equal(t, []uint64{4343}, store.getTimestamps)
	require.Equal(t, []uint64{4343, 4343}, store.iterTimestamps)
}
