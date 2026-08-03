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
	"fmt"
	"io"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"

	proto "github.com/kubewharf/kubebrain-client/api/v2rpc"

	"github.com/kubewharf/kubebrain/pkg/metrics/mock"
	imemkv "github.com/kubewharf/kubebrain/pkg/storage/memkv"
)

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

			got, err := b.getEtcdMetadata(s.ctx, lower, requestedRevision)
			require.NoError(t, err)
			require.Equal(t, lowerMetadata, got)
		})
	}
}
