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
	"encoding/binary"
	"testing"

	"github.com/stretchr/testify/require"

	proto "github.com/kubewharf/kubebrain-client/api/v2rpc"
)

// A legacy object key is {magic}{raw user key}${revision}. Therefore versions
// of "a$target" sort inside the reverse-scan interval historically used by an
// exact Get("a"). Exact lookup must skip those overlapping foreign versions.
func TestGetDollarExtensionDoesNotHideShorterKey(t *testing.T) {
	s, closeSuite := newTestSuites(t, memKvStorage)
	defer closeSuite()

	lower := []byte("/registry/items/a")
	target := []byte("/registry/items/a$target")

	results, _, err := s.backend.TxnApply(s.ctx, []TxnWriteOp{
		{Key: lower, Value: []byte("lower")},
		{Key: target, Value: []byte("target-v1")},
	}, nil)
	require.NoError(t, err)
	require.Len(t, results, 2)
	lowerRevision := results[0].Revision

	_, _, err = s.backend.TxnApply(s.ctx, []TxnWriteOp{
		{Key: target, Value: []byte("target-v2")},
	}, nil)
	require.NoError(t, err)
	_, _, err = s.backend.TxnApply(s.ctx, []TxnWriteOp{
		{Delete: true, Key: target},
	}, nil)
	require.NoError(t, err)

	current, err := s.backend.Get(s.ctx, newGetRequest(0, string(lower)))
	require.NoError(t, err)
	require.NotNil(t, current.Kv)
	require.Equal(t, lower, current.Kv.Key)
	require.Equal(t, []byte("lower"), current.Kv.Value)
	require.Equal(t, lowerRevision, current.Kv.Revision)

	historical, err := s.backend.Get(s.ctx, newGetRequest(lowerRevision, string(lower)))
	require.NoError(t, err)
	require.NotNil(t, historical.Kv)
	require.Equal(t, lower, historical.Kv.Key)
	require.Equal(t, []byte("lower"), historical.Kv.Value)
	require.Equal(t, lowerRevision, historical.Kv.Revision)
}

// A dollar-extension key can physically split two versions of a shorter key.
// A logical Range must still emit the shorter key exactly once at its newest
// visible revision.
func TestRangeDollarExtensionDoesNotDuplicateShorterKey(t *testing.T) {
	for name, storageType := range map[string]storageType{
		"memory": memKvStorage,
		"tikv":   tiKvStorage,
	} {
		t.Run(name, func(t *testing.T) {
			s, closeSuite := newTestSuites(t, storageType)
			defer closeSuite()
			b := s.backend.(*backend)

			shortKey := []byte("/registry/items/a")
			const firstRevision = uint64(0x1800000000000100)
			const foreignBoundary = uint64(0x2800000000000000)
			const latestRevision = uint64(0x3800000000000100)
			foreignSuffix := make([]byte, 8)
			binary.BigEndian.PutUint64(foreignSuffix, foreignBoundary)
			foreignKey := append(append(append([]byte(nil), shortKey...), '$'), foreignSuffix...)
			foreignKey = append(foreignKey, 'x')

			batch := s.kv.BeginBatchWrite()
			batch.Put(b.coder.EncodeObjectKey(shortKey, firstRevision), []byte("short-v1"), 0)
			batch.Put(b.coder.EncodeObjectKey(foreignKey, foreignBoundary), []byte("foreign-v1"), 0)
			batch.Put(b.coder.EncodeObjectKey(shortKey, latestRevision), []byte("short-v2"), 0)
			require.NoError(t, batch.Commit(s.ctx))
			b.SetCurrentRevision(latestRevision)

			response, err := b.List(s.ctx, &proto.RangeRequest{
				Key: shortKey,
				End: PrefixEnd(shortKey),
			})
			require.NoError(t, err)
			require.Len(t, response.Kvs, 2)
			require.Equal(t, shortKey, response.Kvs[0].Key)
			require.Equal(t, latestRevision, response.Kvs[0].Revision)
			require.Equal(t, []byte("short-v2"), response.Kvs[0].Value)
			require.Equal(t, foreignKey, response.Kvs[1].Key)
		})
	}
}

func TestRangeDollarExtensionDoesNotResurrectDeletedShorterKey(t *testing.T) {
	for name, storageType := range map[string]storageType{
		"memory": memKvStorage,
		"tikv":   tiKvStorage,
	} {
		t.Run(name, func(t *testing.T) {
			s, closeSuite := newTestSuites(t, storageType)
			defer closeSuite()
			b := s.backend.(*backend)
			b.config.EnableEtcdCompatibility = true

			shortKey := []byte("/registry/items/deleted")
			const liveRevision = uint64(0x1800000000000100)
			const foreignBoundary = uint64(0x2800000000000000)
			const deleteRevision = uint64(0x3800000000000100)
			foreignSuffix := make([]byte, 8)
			binary.BigEndian.PutUint64(foreignSuffix, foreignBoundary)
			foreignKey := append(append(append([]byte(nil), shortKey...), '$'), foreignSuffix...)
			foreignKey = append(foreignKey, 'x')

			batch := s.kv.BeginBatchWrite()
			batch.Put(b.coder.EncodeObjectKey(shortKey, liveRevision), []byte("must-stay-deleted"), 0)
			batch.Put(b.coder.EncodeObjectKey(foreignKey, foreignBoundary), []byte("foreign-live"), 0)
			batch.Put(b.coder.EncodeObjectKey(shortKey, deleteRevision), tombStoneBytes, 0)
			require.NoError(t, batch.Commit(s.ctx))
			b.SetCurrentRevision(deleteRevision)

			end := PrefixEnd(shortKey)
			response, err := b.List(s.ctx, &proto.RangeRequest{Key: shortKey, End: end})
			require.NoError(t, err)
			require.Len(t, response.Kvs, 1)
			require.Equal(t, foreignKey, response.Kvs[0].Key)

			count, err := b.Count(s.ctx, &proto.CountRequest{Key: shortKey, End: end})
			require.NoError(t, err)
			require.Equal(t, uint64(1), count.Count)

			stream, err := b.RangeStream(s.ctx, shortKey, end, 0)
			require.NoError(t, err)
			var streamed []*proto.KeyValue
			for chunk := range stream {
				require.Empty(t, chunk.Err)
				streamed = append(streamed, chunk.RangeResponse.Kvs...)
			}
			require.Len(t, streamed, 1)
			require.Equal(t, foreignKey, streamed[0].Key)
		})
	}
}
