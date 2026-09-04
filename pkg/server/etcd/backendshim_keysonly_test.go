// Copyright 2026 ByteDance and/or its affiliates
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package etcd

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/api/v3/etcdserverpb"

	proto "github.com/kubewharf/kubebrain-client/api/v2rpc"

	"github.com/kubewharf/kubebrain/pkg/backend"
)

type keysOnlyProbeBackend struct {
	backend.Backend
	keysOnlyCalled bool
	getCalled      bool
	listCalled     bool
	batchFlags     []bool
	batchRevision  uint64
	shortBatch     bool
}

type valueSortPointProbeBackend struct {
	backend.Backend
	getCalled      bool
	keysOnlyCalled bool
}

func (b *valueSortPointProbeBackend) Get(_ context.Context, request *proto.GetRequest) (*proto.GetResponse, error) {
	b.getCalled = true
	return &proto.GetResponse{
		Header: &proto.ResponseHeader{Revision: 11},
		Kv: &proto.KeyValue{
			Key: request.Key, Value: []byte("value-used-for-ordering"), Revision: 7,
		},
	}, nil
}

func (b *valueSortPointProbeBackend) GetKeysOnly(context.Context, *proto.GetRequest) (*proto.GetResponse, error) {
	b.keysOnlyCalled = true
	return nil, errors.New("VALUE-sort KeysOnly unexpectedly used metadata Get")
}

func (b *valueSortPointProbeBackend) GetEtcdMetadata(context.Context, []byte, uint64) (backend.EtcdMetadata, error) {
	return backend.EtcdMetadata{CreateRevision: 3, Version: 2}, nil
}

func (b *keysOnlyProbeBackend) Get(context.Context, *proto.GetRequest) (*proto.GetResponse, error) {
	b.getCalled = true
	return nil, errors.New("fast point KeysOnly unexpectedly materialized values")
}

func (b *keysOnlyProbeBackend) GetKeysOnly(_ context.Context, request *proto.GetRequest) (*proto.GetResponse, error) {
	b.keysOnlyCalled = true
	return &proto.GetResponse{
		Header: &proto.ResponseHeader{Revision: 11},
		Kv: &proto.KeyValue{
			Key: request.Key, Value: nil, Revision: 7,
		},
	}, nil
}

func (b *keysOnlyProbeBackend) GetBatchProjected(
	_ context.Context, keys [][]byte, revision uint64, metadataOnly []bool,
) ([]*proto.GetResponse, error) {
	b.batchFlags = append([]bool(nil), metadataOnly...)
	b.batchRevision = revision
	responses := make([]*proto.GetResponse, len(keys))
	for index, key := range keys {
		value := []byte("full-" + string(key))
		if metadataOnly[index] {
			value = nil
		}
		responses[index] = &proto.GetResponse{
			Header: &proto.ResponseHeader{Revision: revision},
			Kv:     &proto.KeyValue{Key: key, Value: value, Revision: 7},
		}
	}
	if b.shortBatch {
		responses = responses[:len(responses)-1]
	}
	return responses, nil
}

func (b *keysOnlyProbeBackend) List(context.Context, *proto.RangeRequest) (*proto.RangeResponse, error) {
	b.listCalled = true
	return nil, errors.New("fast KeysOnly unexpectedly materialized values")
}

func (b *keysOnlyProbeBackend) ListKeysOnly(_ context.Context, request *proto.RangeRequest) (*proto.RangeResponse, error) {
	b.keysOnlyCalled = true
	return &proto.RangeResponse{
		Header: &proto.ResponseHeader{Revision: 11},
		Kvs: []*proto.KeyValue{{
			Key: []byte("/keys-only/a"), Value: nil, Revision: 7,
		}},
	}, nil
}

func (b *keysOnlyProbeBackend) GetEtcdMetadata(context.Context, []byte, uint64) (backend.EtcdMetadata, error) {
	return backend.EtcdMetadata{CreateRevision: 3, Version: 2}, nil
}

func TestBackendShimFastKeysOnlyUsesMetadataList(t *testing.T) {
	probe := &keysOnlyProbeBackend{}
	shim := NewBackendShim(probe, &recordingMetrics{})

	response, err := shim.List(context.Background(), &etcdserverpb.RangeRequest{
		Key: []byte("/keys-only/"), RangeEnd: []byte("/keys-only0"),
		KeysOnly: true, SortTarget: etcdserverpb.RangeRequest_VERSION,
	})
	require.NoError(t, err)
	require.True(t, probe.keysOnlyCalled)
	require.False(t, probe.listCalled)
	require.Equal(t, int64(11), response.GetHeader().GetRevision())
	require.Equal(t, int64(1), response.Count)
	require.Len(t, response.Kvs, 1)
	require.Equal(t, []byte("/keys-only/a"), response.Kvs[0].Key)
	require.Empty(t, response.Kvs[0].Value)
	require.Equal(t, int64(3), response.Kvs[0].CreateRevision)
	require.Equal(t, int64(7), response.Kvs[0].ModRevision)
	require.Equal(t, int64(2), response.Kvs[0].Version)
	require.Zero(t, response.Kvs[0].Lease)
}

func TestBackendShimFastPointKeysOnlyUsesMetadataGet(t *testing.T) {
	probe := &keysOnlyProbeBackend{}
	shim := NewBackendShim(probe, &recordingMetrics{})

	response, err := shim.Get(context.Background(), &etcdserverpb.RangeRequest{
		Key: []byte("/keys-only/point"), KeysOnly: true,
		SortTarget: etcdserverpb.RangeRequest_VERSION,
	})
	require.NoError(t, err)
	require.True(t, probe.keysOnlyCalled)
	require.False(t, probe.getCalled)
	require.Equal(t, int64(11), response.GetHeader().GetRevision())
	require.Equal(t, int64(1), response.Count)
	require.Len(t, response.Kvs, 1)
	require.Equal(t, []byte("/keys-only/point"), response.Kvs[0].Key)
	require.Empty(t, response.Kvs[0].Value)
	require.Equal(t, int64(3), response.Kvs[0].CreateRevision)
	require.Equal(t, int64(7), response.Kvs[0].ModRevision)
	require.Equal(t, int64(2), response.Kvs[0].Version)
	require.Zero(t, response.Kvs[0].Lease)
}

func TestBackendShimValueSortPointKeysOnlyUsesOrdinaryGet(t *testing.T) {
	probe := &valueSortPointProbeBackend{}
	shim := NewBackendShim(probe, &recordingMetrics{})

	response, err := shim.Get(context.Background(), &etcdserverpb.RangeRequest{
		Key: []byte("/keys-only/value-sort"), KeysOnly: true,
		SortTarget: etcdserverpb.RangeRequest_VALUE,
	})
	require.NoError(t, err)
	require.True(t, probe.getCalled)
	require.False(t, probe.keysOnlyCalled)
	require.Len(t, response.Kvs, 1)
	require.Empty(t, response.Kvs[0].Value)
}

func TestBackendShimProjectedPointBatchPreservesPerKeyModes(t *testing.T) {
	probe := &keysOnlyProbeBackend{}
	publicShim := NewBackendShim(probe, &recordingMetrics{})
	shim, ok := publicShim.(txnPointBatchProjectedGetter)
	require.True(t, ok)
	keys := [][]byte{[]byte("/keys-only/batch-a"), []byte("/keys-only/batch-b")}

	response, err := shim.BatchGetAtRevisionProjected(context.Background(), keys, []bool{true, false}, 13)
	require.NoError(t, err)
	require.Equal(t, uint64(13), probe.batchRevision)
	require.Equal(t, []bool{true, false}, probe.batchFlags)
	require.Len(t, response, 2)
	require.Empty(t, response[string(keys[0])].Value)
	require.Equal(t, []byte("full-/keys-only/batch-b"), response[string(keys[1])].Value)
	for _, kv := range response {
		require.Equal(t, int64(3), kv.CreateRevision)
		require.Equal(t, int64(7), kv.ModRevision)
		require.Equal(t, int64(2), kv.Version)
	}

	probe.shortBatch = true
	_, err = shim.BatchGetAtRevisionProjected(context.Background(), keys, []bool{true, false}, 13)
	require.EqualError(t, err, "backend point batch returned 1 responses for 2 keys")
}
