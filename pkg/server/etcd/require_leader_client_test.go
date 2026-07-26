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
	"fmt"
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
)

func TestClientRequireLeaderKVLeaseAndWatch(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()

	grpcServer := grpc.NewServer(server.ClientServerOptions()...)
	etcdserverpb.RegisterKVServer(grpcServer, server)
	etcdserverpb.RegisterLeaseServer(grpcServer, server)
	etcdserverpb.RegisterWatchServer(grpcServer, server)
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
	key := fmt.Sprintf("/a1107/require-leader/%d", time.Now().UnixNano())
	requireLeaderCtx := clientv3.WithRequireLeader(ctx)

	empty, err := client.Get(requireLeaderCtx, key)
	require.NoError(t, err)
	require.Empty(t, empty.Kvs)
	lease, err := client.Grant(requireLeaderCtx, 30)
	require.NoError(t, err)
	keepAlive, err := client.KeepAliveOnce(requireLeaderCtx, lease.ID)
	require.NoError(t, err)
	require.Equal(t, lease.ID, keepAlive.ID)
	require.Positive(t, keepAlive.TTL)

	watch := client.Watch(requireLeaderCtx, key, clientv3.WithCreatedNotify())
	select {
	case response := <-watch:
		require.NoError(t, response.Err())
		require.True(t, response.Created)
	case <-ctx.Done():
		t.Fatalf("timed out waiting for require-leader watch create: %v", ctx.Err())
	}
	put, err := client.Put(ctx, key, "value", clientv3.WithLease(lease.ID))
	require.NoError(t, err)
	select {
	case response := <-watch:
		require.NoError(t, response.Err())
		require.Len(t, response.Events, 1)
		require.Equal(t, key, string(response.Events[0].Kv.Key))
		require.Equal(t, "value", string(response.Events[0].Kv.Value))
		require.Equal(t, put.Header.Revision, response.Events[0].Kv.ModRevision)
	case <-ctx.Done():
		t.Fatalf("timed out waiting for require-leader watch event: %v", ctx.Err())
	}
}
