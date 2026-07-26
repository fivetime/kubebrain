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

func TestClientSerializableReadCurrentHistoricalAndTxn(t *testing.T) {
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

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	key := fmt.Sprintf("/a991/serializable-client/%d", time.Now().UnixNano())
	first, err := client.Put(ctx, key, "v1")
	require.NoError(t, err)
	second, err := client.Put(ctx, key, "v2")
	require.NoError(t, err)

	current, err := client.Get(ctx, key, clientv3.WithSerializable())
	require.NoError(t, err)
	require.Len(t, current.Kvs, 1)
	require.Equal(t, "v2", string(current.Kvs[0].Value))
	require.Equal(t, second.Header.Revision, current.Header.Revision)

	historical, err := client.Get(ctx, key, clientv3.WithRev(first.Header.Revision), clientv3.WithSerializable())
	require.NoError(t, err)
	require.Len(t, historical.Kvs, 1)
	require.Equal(t, "v1", string(historical.Kvs[0].Value))
	require.Equal(t, second.Header.Revision, historical.Header.Revision)

	txn, err := client.Txn(ctx).
		If(clientv3.Compare(clientv3.Version(key), ">", 0)).
		Then(clientv3.OpGet(key, clientv3.WithSerializable())).
		Else(clientv3.OpGet(key, clientv3.WithSerializable())).
		Commit()
	require.NoError(t, err)
	require.True(t, txn.Succeeded)
	require.Equal(t, second.Header.Revision, txn.Header.Revision)
	require.Len(t, txn.Responses, 1)
	txnRange := txn.Responses[0].GetResponseRange()
	require.NotNil(t, txnRange)
	require.Len(t, txnRange.Kvs, 1)
	require.Equal(t, "v2", string(txnRange.Kvs[0].Value))
	require.Equal(t, second.Header.Revision, txnRange.Header.Revision)
}
