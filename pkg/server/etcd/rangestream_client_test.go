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

func TestClientRangeStreamCommonShapesMatchUnaryRange(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()

	grpcServer := grpc.NewServer(server.ClientServerOptions()...)
	etcdserverpb.RegisterKVServer(grpcServer, server)
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
	prefix := fmt.Sprintf("\xff\xfe/a988/rangestream-client/%d/", time.Now().UnixNano())
	first, err := client.Put(ctx, prefix+"a", "v1")
	require.NoError(t, err)
	_, err = client.Put(ctx, prefix+"b", "v2")
	require.NoError(t, err)
	_, err = client.Put(ctx, prefix+"a", "v3")
	require.NoError(t, err)
	prefixEnd := clientv3.GetPrefixRangeEnd(prefix)

	tests := []struct {
		name string
		key  string
		opts []clientv3.OpOption
	}{
		{name: "point-hit", key: prefix + "a"},
		{name: "point-miss", key: prefix + "missing"},
		{name: "equal-empty", key: prefix + "a", opts: []clientv3.OpOption{clientv3.WithRange(prefix + "a")}},
		{name: "reversed-empty", key: prefix + "z", opts: []clientv3.OpOption{clientv3.WithRange(prefix + "a")}},
		{name: "prefix-limit-one", key: prefix, opts: []clientv3.OpOption{clientv3.WithRange(prefixEnd), clientv3.WithLimit(1)}},
		{name: "historical-after-first-put", key: prefix, opts: []clientv3.OpOption{clientv3.WithRange(prefixEnd), clientv3.WithRev(first.Header.Revision)}},
		{name: "from-key", key: prefix, opts: []clientv3.OpOption{clientv3.WithFromKey()}},
		{name: "keys-only", key: prefix, opts: []clientv3.OpOption{clientv3.WithRange(prefixEnd), clientv3.WithKeysOnly()}},
		{name: "count-only-limit", key: prefix, opts: []clientv3.OpOption{clientv3.WithRange(prefixEnd), clientv3.WithCountOnly(), clientv3.WithLimit(1)}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			unary, err := client.Get(ctx, test.key, test.opts...)
			require.NoError(t, err)
			stream, err := client.GetStream(ctx, test.key, test.opts...)
			require.NoError(t, err)
			merged, err := clientv3.GetStreamToGetResponse(stream)
			require.NoError(t, err)
			requireClientRangeStreamMatchesUnary(t, (*clientv3.GetResponse)(merged), unary)
		})
	}
}

func requireClientRangeStreamMatchesUnary(t *testing.T, streamed, unary *clientv3.GetResponse) {
	t.Helper()
	require.NotNil(t, streamed.Header)
	require.NotNil(t, unary.Header)
	require.Equal(t, unary.Header.Revision, streamed.Header.Revision)
	require.Equal(t, unary.Count, streamed.Count)
	require.Equal(t, unary.More, streamed.More)
	require.Len(t, streamed.Kvs, len(unary.Kvs))
	for index := range unary.Kvs {
		require.Equal(t, unary.Kvs[index].Key, streamed.Kvs[index].Key)
		require.Equal(t, unary.Kvs[index].Value, streamed.Kvs[index].Value)
		require.Equal(t, unary.Kvs[index].CreateRevision, streamed.Kvs[index].CreateRevision)
		require.Equal(t, unary.Kvs[index].ModRevision, streamed.Kvs[index].ModRevision)
		require.Equal(t, unary.Kvs[index].Version, streamed.Kvs[index].Version)
		require.Equal(t, unary.Kvs[index].Lease, streamed.Kvs[index].Lease)
	}
}
