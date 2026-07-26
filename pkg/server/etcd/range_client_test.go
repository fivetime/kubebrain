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
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
)

func TestClientRangeRevisionFilterCountAndTxnStagedView(t *testing.T) {
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
	prefix := fmt.Sprintf("/a993/range-revision-filter-client/%d/", time.Now().UnixNano())
	_, err = client.Put(ctx, prefix+"a", "old-a")
	require.NoError(t, err)
	_, err = client.Put(ctx, prefix+"b", "old-b")
	require.NoError(t, err)
	updateB, err := client.Put(ctx, prefix+"b", "new-b")
	require.NoError(t, err)

	filtered, err := client.Get(ctx, prefix, clientv3.WithPrefix(), clientv3.WithMinModRev(updateB.Header.Revision))
	require.NoError(t, err)
	require.Equal(t, int64(2), filtered.Count)
	require.Equal(t, []string{"b"}, rangeClientRelativeKeys(filtered.Kvs, prefix))
	require.Equal(t, "new-b", string(filtered.Kvs[0].Value))

	counted, err := client.Get(
		ctx, prefix, clientv3.WithPrefix(), clientv3.WithMinModRev(updateB.Header.Revision),
		clientv3.WithCountOnly(),
	)
	require.NoError(t, err)
	require.Equal(t, int64(2), counted.Count)
	require.Empty(t, counted.Kvs)

	txn, err := client.Txn(ctx).
		Then(
			clientv3.OpPut(prefix+"c", "new-c"),
			clientv3.OpGet(prefix, clientv3.WithPrefix(), clientv3.WithMinModRev(updateB.Header.Revision+1)),
		).
		Commit()
	require.NoError(t, err)
	require.True(t, txn.Succeeded)
	require.Len(t, txn.Responses, 2)
	txnRange := txn.Responses[1].GetResponseRange()
	require.NotNil(t, txnRange)
	require.Equal(t, int64(3), txnRange.Count)
	require.False(t, txnRange.More)
	require.Equal(t, []string{"c"}, rangeClientRelativeKeys(txnRange.Kvs, prefix))
	require.Equal(t, "new-c", string(txnRange.Kvs[0].Value))
}

func rangeClientRelativeKeys(kvs []*mvccpb.KeyValue, prefix string) []string {
	keys := make([]string, 0, len(kvs))
	for _, kv := range kvs {
		keys = append(keys, strings.TrimPrefix(string(kv.Key), prefix))
	}
	return keys
}
