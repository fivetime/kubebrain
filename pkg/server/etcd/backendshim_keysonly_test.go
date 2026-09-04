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
	listCalled     bool
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
