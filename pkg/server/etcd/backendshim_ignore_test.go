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

package etcd

import (
	"context"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/kubewharf/kubebrain/pkg/backend"
	"github.com/kubewharf/kubebrain/pkg/metrics/mock"
	memkv "github.com/kubewharf/kubebrain/pkg/storage/memkv"
)

func newBackendShimIgnoreTest(t *testing.T) *backendShim {
	t.Helper()
	ctrl := gomock.NewController(t)
	t.Cleanup(ctrl.Finish)
	m := mock.NewMinimalMetrics(ctrl)
	kv := memkv.NewKvStorage()
	t.Cleanup(func() { require.NoError(t, kv.Close()) })
	b := backend.NewBackend(kv, backend.Config{Identity: "test", EnableEtcdCompatibility: true}, m)
	return NewBackendShim(b, m).(*backendShim)
}

func TestBackendShimPutExpandsIgnoreOptions(t *testing.T) {
	shim := newBackendShimIgnoreTest(t)
	ctx := context.Background()
	key := []byte("/registry/backendshim/ignore-put")

	_, err := shim.Put(ctx, &etcdserverpb.PutRequest{Key: key, Value: []byte("old"), Lease: 11})
	require.NoError(t, err)

	resp, err := shim.Put(ctx, &etcdserverpb.PutRequest{
		Key:         key,
		Value:       []byte("new"),
		IgnoreLease: true,
		PrevKv:      true,
	})
	require.NoError(t, err)
	require.NotNil(t, resp.PrevKv)
	require.Equal(t, []byte("old"), resp.PrevKv.Value)
	require.Equal(t, int64(11), resp.PrevKv.Lease)

	got, err := shim.Get(ctx, &etcdserverpb.RangeRequest{Key: key})
	require.NoError(t, err)
	require.Len(t, got.Kvs, 1)
	require.Equal(t, []byte("new"), got.Kvs[0].Value)
	require.Equal(t, int64(11), got.Kvs[0].Lease)

	resp, err = shim.Put(ctx, &etcdserverpb.PutRequest{
		Key:         key,
		Lease:       22,
		IgnoreValue: true,
		PrevKv:      true,
	})
	require.NoError(t, err)
	require.NotNil(t, resp.PrevKv)
	require.Equal(t, []byte("new"), resp.PrevKv.Value)
	require.Equal(t, int64(11), resp.PrevKv.Lease)

	got, err = shim.Get(ctx, &etcdserverpb.RangeRequest{Key: key})
	require.NoError(t, err)
	require.Len(t, got.Kvs, 1)
	require.Equal(t, []byte("new"), got.Kvs[0].Value)
	require.Equal(t, int64(22), got.Kvs[0].Lease)
}

func TestBackendShimComparePutsExpandIgnoreOptions(t *testing.T) {
	shim := newBackendShimIgnoreTest(t)
	ctx := context.Background()
	key := []byte("/registry/backendshim/ignore-compare-put")

	_, err := shim.Put(ctx, &etcdserverpb.PutRequest{Key: key, Value: []byte("old"), Lease: 11})
	require.NoError(t, err)
	got, err := shim.Get(ctx, &etcdserverpb.RangeRequest{Key: key})
	require.NoError(t, err)
	require.Len(t, got.Kvs, 1)

	resp, err := shim.Update(ctx, got.Kvs[0].ModRevision, &etcdserverpb.PutRequest{
		Key:         key,
		Value:       []byte("new"),
		IgnoreLease: true,
		PrevKv:      true,
	}, true)
	require.NoError(t, err)
	require.True(t, resp.Succeeded)
	require.Len(t, resp.Responses, 1)
	putResp := resp.Responses[0].GetResponsePut()
	require.NotNil(t, putResp)
	require.NotNil(t, putResp.PrevKv)
	require.Equal(t, []byte("old"), putResp.PrevKv.Value)
	require.Equal(t, int64(11), putResp.PrevKv.Lease)

	got, err = shim.Get(ctx, &etcdserverpb.RangeRequest{Key: key})
	require.NoError(t, err)
	require.Len(t, got.Kvs, 1)
	require.Equal(t, []byte("new"), got.Kvs[0].Value)
	require.Equal(t, int64(11), got.Kvs[0].Lease)

	resp, err = shim.Update(ctx, got.Kvs[0].ModRevision, &etcdserverpb.PutRequest{
		Key:         key,
		Lease:       22,
		IgnoreValue: true,
		PrevKv:      true,
	}, true)
	require.NoError(t, err)
	require.True(t, resp.Succeeded)
	require.Len(t, resp.Responses, 1)
	putResp = resp.Responses[0].GetResponsePut()
	require.NotNil(t, putResp)
	require.NotNil(t, putResp.PrevKv)
	require.Equal(t, []byte("new"), putResp.PrevKv.Value)
	require.Equal(t, int64(11), putResp.PrevKv.Lease)

	got, err = shim.Get(ctx, &etcdserverpb.RangeRequest{Key: key})
	require.NoError(t, err)
	require.Len(t, got.Kvs, 1)
	require.Equal(t, []byte("new"), got.Kvs[0].Value)
	require.Equal(t, int64(22), got.Kvs[0].Lease)
}

func TestBackendShimIgnoreOptionsRequireExistingKey(t *testing.T) {
	shim := newBackendShimIgnoreTest(t)
	ctx := context.Background()
	key := []byte("/registry/backendshim/ignore-missing")

	for _, tc := range []struct {
		name string
		run  func() error
	}{
		{
			name: "put",
			run: func() error {
				_, err := shim.Put(ctx, &etcdserverpb.PutRequest{Key: key, IgnoreValue: true})
				return err
			},
		},
		{
			name: "create",
			run: func() error {
				_, err := shim.Create(ctx, &etcdserverpb.PutRequest{Key: key, IgnoreLease: true, Value: []byte("new")}, true)
				return err
			},
		},
		{
			name: "update",
			run: func() error {
				_, err := shim.Update(ctx, 123, &etcdserverpb.PutRequest{Key: key, IgnoreValue: true}, true)
				return err
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.run()
			require.ErrorIs(t, err, rpctypes.ErrGRPCKeyNotFound)
			require.Equal(t, codes.InvalidArgument, status.Code(err))
			require.Equal(t, "etcdserver: key not found", status.Convert(err).Message())
		})
	}
}
