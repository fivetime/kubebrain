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
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/ordering"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
)

func TestClientOrderingWrapperGetTxnAndUnsupportedStream(t *testing.T) {
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
	prefix := fmt.Sprintf("/a1124/ordering/%d/", time.Now().UnixNano())
	key := prefix + "key"
	var violations atomic.Int32
	orderedKV := ordering.NewKV(client.KV, func(clientv3.Op, clientv3.OpResponse, int64) error {
		violations.Add(1)
		return fmt.Errorf("ordering violation")
	})

	firstPut, err := client.Put(ctx, key, "one")
	require.NoError(t, err)
	firstGet, err := orderedKV.Get(ctx, key)
	require.NoError(t, err)
	require.Len(t, firstGet.Kvs, 1)
	require.Equal(t, "one", string(firstGet.Kvs[0].Value))
	require.GreaterOrEqual(t, firstGet.Header.Revision, firstPut.Header.Revision)

	secondPut, err := client.Put(ctx, key, "two")
	require.NoError(t, err)
	secondGet, err := orderedKV.Get(ctx, key, clientv3.WithSerializable())
	require.NoError(t, err)
	require.Len(t, secondGet.Kvs, 1)
	require.Equal(t, "two", string(secondGet.Kvs[0].Value))
	require.GreaterOrEqual(t, secondGet.Header.Revision, firstGet.Header.Revision)
	require.GreaterOrEqual(t, secondGet.Header.Revision, secondPut.Header.Revision)

	txn, err := orderedKV.Txn(ctx).
		If(clientv3.Compare(clientv3.Value(key), "=", "two")).
		Then(clientv3.OpGet(key)).
		Commit()
	require.NoError(t, err)
	require.True(t, txn.Succeeded)
	require.GreaterOrEqual(t, txn.Header.Revision, secondGet.Header.Revision)
	require.Len(t, txn.Responses, 1)
	require.Equal(t, "two", string(txn.Responses[0].GetResponseRange().Kvs[0].Value))
	require.Zero(t, violations.Load())

	stream, err := orderedKV.GetStream(ctx, key)
	require.Nil(t, stream)
	require.Equal(t, codes.Unimplemented, status.Code(err))
	require.Equal(t, "GetStream is not supported by kvOrdering", status.Convert(err).Message())
}
