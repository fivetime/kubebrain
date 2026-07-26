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
	"go.etcd.io/etcd/client/v3/leasing"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
)

func TestClientLeasingFromKeyDeleteRemovesOwnerCache(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()

	grpcServer := grpc.NewServer(server.ClientServerOptions()...)
	etcdserverpb.RegisterKVServer(grpcServer, server)
	etcdserverpb.RegisterWatchServer(grpcServer, server)
	etcdserverpb.RegisterLeaseServer(grpcServer, server)
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
	base := fmt.Sprintf("\xff\xff/a977/leasing-from-key/%d/", time.Now().UnixNano())
	ownerPrefix := base + "0owners/"
	dataPrefix := base + "data/"
	keys := []string{
		dataPrefix + "a",
		dataPrefix + "m",
		dataPrefix + "n",
		dataPrefix + "z",
	}
	leased, closeLeased, err := leasing.NewKV(client, ownerPrefix)
	require.NoError(t, err)
	t.Cleanup(closeLeased)

	for index, key := range keys {
		_, err = client.Put(ctx, key, fmt.Sprintf("value-%d", index))
		require.NoError(t, err)
		cached, getErr := leased.Get(ctx, key)
		require.NoError(t, getErr)
		require.Len(t, cached.Kvs, 1)
	}

	watchCtx, watchCancel := context.WithTimeout(ctx, 5*time.Second)
	defer watchCancel()
	watch := client.Watch(watchCtx, dataPrefix, clientv3.WithPrefix())
	opResponse, err := leased.Do(ctx, clientv3.OpDelete(keys[1], clientv3.WithFromKey()))
	require.NoError(t, err)
	deleted := opResponse.Del()
	require.NotNil(t, deleted)
	require.Equal(t, int64(3), deleted.Deleted)

	deleteEvents := 0
	for deleteEvents < 3 {
		select {
		case response, ok := <-watch:
			require.True(t, ok)
			require.NoError(t, response.Err())
			for _, event := range response.Events {
				if event.Type != clientv3.EventTypeDelete {
					continue
				}
				require.Equal(t, deleted.Header.Revision, event.Kv.ModRevision)
				deleteEvents++
			}
		case <-watchCtx.Done():
			t.Fatalf("timed out after %d/3 delete events at revision %d", deleteEvents, deleted.Header.Revision)
		}
	}

	unaffected, err := leased.Get(ctx, keys[0])
	require.NoError(t, err)
	require.Len(t, unaffected.Kvs, 1)
	require.Equal(t, []byte("value-0"), unaffected.Kvs[0].Value)
	unaffectedOwner, err := client.Get(ctx, ownerPrefix+keys[0])
	require.NoError(t, err)
	require.Len(t, unaffectedOwner.Kvs, 1)

	for _, key := range keys[1:] {
		direct, getErr := client.Get(ctx, key)
		require.NoError(t, getErr)
		require.Empty(t, direct.Kvs)
		require.Eventually(t, func() bool {
			cached, cacheErr := leased.Get(ctx, key)
			return cacheErr == nil && len(cached.Kvs) == 0
		}, time.Second, 10*time.Millisecond)
		owner, getErr := client.Get(ctx, ownerPrefix+key)
		require.NoError(t, getErr)
		require.Len(t, owner.Kvs, 1)
	}
}
