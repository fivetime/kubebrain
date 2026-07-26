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
	"math"
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
)

func TestClientPutIgnoreValueIgnoreLeaseAndErrors(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()

	grpcServer := grpc.NewServer(server.ClientServerOptions()...)
	server.Register(grpcServer)
	listener := bufconn.Listen(1 << 20)
	go func() { _ = grpcServer.Serve(listener) }()
	t.Cleanup(grpcServer.Stop)

	client, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{"bufnet"},
		DialTimeout: time.Second,
		DialOptions: []grpc.DialOption{
			grpc.WithTransportCredentials(insecure.NewCredentials()),
			grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
				return listener.Dial()
			}),
		},
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, client.Close()) })

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	prefix := "/a1051/client-put/"
	key := prefix + "key"
	missing := prefix + "missing"
	base, err := client.Get(ctx, prefix, clientv3.WithPrefix())
	require.NoError(t, err)
	baseRev := base.Header.Revision
	leaseA, err := client.Grant(ctx, 300)
	require.NoError(t, err)
	leaseB, err := client.Grant(ctx, 300)
	require.NoError(t, err)
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cleanupCancel()
		_, _ = client.Delete(cleanupCtx, prefix, clientv3.WithPrefix())
	})

	create, err := client.Put(ctx, key, "one", clientv3.WithLease(leaseA.ID), clientv3.WithPrevKV())
	require.NoError(t, err)
	require.Equal(t, int64(1), create.Header.Revision-baseRev)
	require.Nil(t, create.PrevKv)

	rebind, err := client.Put(ctx, key, "two", clientv3.WithLease(leaseB.ID), clientv3.WithPrevKV())
	require.NoError(t, err)
	require.Equal(t, int64(2), rebind.Header.Revision-baseRev)
	require.NotNil(t, rebind.PrevKv)
	require.Equal(t, "one", string(rebind.PrevKv.Value))
	require.Equal(t, int64(leaseA.ID), rebind.PrevKv.Lease)

	ignoreValue, err := client.Put(ctx, key, "", clientv3.WithIgnoreValue(), clientv3.WithLease(leaseA.ID), clientv3.WithPrevKV())
	require.NoError(t, err)
	require.Equal(t, int64(3), ignoreValue.Header.Revision-baseRev)
	require.NotNil(t, ignoreValue.PrevKv)
	require.Equal(t, "two", string(ignoreValue.PrevKv.Value))
	require.Equal(t, int64(leaseB.ID), ignoreValue.PrevKv.Lease)

	ignoreLease, err := client.Put(ctx, key, "three", clientv3.WithIgnoreLease(), clientv3.WithPrevKV())
	require.NoError(t, err)
	require.Equal(t, int64(4), ignoreLease.Header.Revision-baseRev)
	require.NotNil(t, ignoreLease.PrevKv)
	require.Equal(t, "two", string(ignoreLease.PrevKv.Value))
	require.Equal(t, int64(leaseA.ID), ignoreLease.PrevKv.Lease)

	current, err := client.Get(ctx, key)
	require.NoError(t, err)
	require.Len(t, current.Kvs, 1)
	require.Equal(t, "three", string(current.Kvs[0].Value))
	require.Equal(t, int64(leaseA.ID), current.Kvs[0].Lease)
	require.Equal(t, create.Header.Revision, current.Kvs[0].CreateRevision)
	require.Equal(t, ignoreLease.Header.Revision, current.Kvs[0].ModRevision)
	require.Equal(t, int64(4), current.Kvs[0].Version)

	ttlA, err := client.TimeToLive(ctx, leaseA.ID, clientv3.WithAttachedKeys())
	require.NoError(t, err)
	require.Equal(t, []string{key}, leaseClientAttachedKeys(ttlA.Keys))
	ttlB, err := client.TimeToLive(ctx, leaseB.ID, clientv3.WithAttachedKeys())
	require.NoError(t, err)
	require.Empty(t, ttlB.Keys)

	tests := []struct {
		name        string
		call        func() error
		wantCode    codes.Code
		wantMessage string
	}{
		{
			name: "missing lease",
			call: func() error {
				_, err := client.Put(ctx, key, "bad", clientv3.WithLease(clientv3.LeaseID(math.MaxInt64)))
				return err
			},
			wantCode:    codes.Unknown,
			wantMessage: "etcdserver: requested lease not found",
		},
		{
			name: "missing ignore value key",
			call: func() error {
				_, err := client.Put(ctx, missing, "", clientv3.WithIgnoreValue())
				return err
			},
			wantCode:    codes.Unknown,
			wantMessage: "etcdserver: key not found",
		},
		{
			name: "missing key and lease",
			call: func() error {
				_, err := client.Put(ctx, missing, "", clientv3.WithIgnoreValue(), clientv3.WithLease(clientv3.LeaseID(math.MaxInt64)))
				return err
			},
			wantCode:    codes.Unknown,
			wantMessage: "etcdserver: requested lease not found",
		},
		{
			name: "value with ignore value",
			call: func() error {
				_, err := client.Put(ctx, key, "bad", clientv3.WithIgnoreValue())
				return err
			},
			wantCode:    codes.Unknown,
			wantMessage: "etcdserver: value is provided",
		},
		{
			name: "lease with ignore lease",
			call: func() error {
				_, err := client.Put(ctx, key, "bad", clientv3.WithIgnoreLease(), clientv3.WithLease(leaseA.ID))
				return err
			},
			wantCode:    codes.Unknown,
			wantMessage: "etcdserver: lease is provided",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.call()
			require.Error(t, err)
			require.Equal(t, tt.wantCode, status.Code(err))
			require.Equal(t, tt.wantMessage, status.Convert(err).Message())
		})
	}
}

func leaseClientAttachedKeys(keys [][]byte) []string {
	out := make([]string, 0, len(keys))
	for _, key := range keys {
		out = append(out, string(key))
	}
	return out
}
