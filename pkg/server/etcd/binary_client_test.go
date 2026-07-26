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
	"encoding/hex"
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/mvccpb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
)

func TestRawGRPCBinaryRangeTxnAndDeleteBoundaries(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()

	grpcServer := grpc.NewServer(server.ClientServerOptions()...)
	etcdserverpb.RegisterKVServer(grpcServer, server)
	listener := bufconn.Listen(1 << 20)
	go func() { _ = grpcServer.Serve(listener) }()
	t.Cleanup(grpcServer.Stop)

	conn, err := grpc.NewClient("passthrough:///binary-client",
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
			return listener.Dial()
		}))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, conn.Close()) })
	client := etcdserverpb.NewKVClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	keys := [][]byte{
		{0x00},
		{0x00, 0x00},
		{0x00, 0x01},
		{0x01},
		{0xfe},
		{0xfe, 0x00},
		{0xfe, 0x01},
		{0xff},
	}
	for index, key := range keys {
		_, err = client.Put(ctx, &etcdserverpb.PutRequest{
			Key: key, Value: []byte{byte('a' + index)},
		})
		require.NoError(t, err)
	}

	txnRange, err := client.Txn(ctx, &etcdserverpb.TxnRequest{
		Success: []*etcdserverpb.RequestOp{{
			Request: &etcdserverpb.RequestOp_RequestRange{
				RequestRange: &etcdserverpb.RangeRequest{Key: []byte{0x00}, RangeEnd: []byte{0x01}},
			},
		}},
	})
	require.NoError(t, err)
	require.Len(t, txnRange.Responses, 1)
	require.Equal(t, []string{"00", "0000", "0001"}, binaryClientKeys(txnRange.Responses[0].GetResponseRange().Kvs))
	require.Equal(t, []string{"a", "b", "c"}, binaryClientValues(txnRange.Responses[0].GetResponseRange().Kvs))

	txnDelete, err := client.Txn(ctx, &etcdserverpb.TxnRequest{
		Success: []*etcdserverpb.RequestOp{{
			Request: &etcdserverpb.RequestOp_RequestDeleteRange{
				RequestDeleteRange: &etcdserverpb.DeleteRangeRequest{
					Key: []byte{0x00}, RangeEnd: []byte{0x01}, PrevKv: true,
				},
			},
		}},
	})
	require.NoError(t, err)
	require.Len(t, txnDelete.Responses, 1)
	deleted := txnDelete.Responses[0].GetResponseDeleteRange()
	require.Equal(t, int64(3), deleted.Deleted)
	require.Equal(t, []string{"00", "0000", "0001"}, binaryClientKeys(deleted.PrevKvs))
	require.Equal(t, []string{"a", "b", "c"}, binaryClientValues(deleted.PrevKvs))

	afterTxn, err := client.Range(ctx, &etcdserverpb.RangeRequest{Key: []byte{0x00}, RangeEnd: []byte{0x01}})
	require.NoError(t, err)
	require.Zero(t, afterTxn.Count)
	require.Empty(t, afterTxn.Kvs)

	standalone, err := client.DeleteRange(ctx, &etcdserverpb.DeleteRangeRequest{
		Key: []byte{0xfe}, RangeEnd: []byte{0xff}, PrevKv: true,
	})
	require.NoError(t, err)
	require.Equal(t, int64(3), standalone.Deleted)
	require.Equal(t, []string{"fe", "fe00", "fe01"}, binaryClientKeys(standalone.PrevKvs))
	require.Equal(t, []string{"e", "f", "g"}, binaryClientValues(standalone.PrevKvs))

	afterStandalone, err := client.Range(ctx, &etcdserverpb.RangeRequest{Key: []byte{0xfe}, RangeEnd: []byte{0xff}})
	require.NoError(t, err)
	require.Zero(t, afterStandalone.Count)
	require.Empty(t, afterStandalone.Kvs)
	pointFF, err := client.Range(ctx, &etcdserverpb.RangeRequest{Key: []byte{0xff}})
	require.NoError(t, err)
	require.Equal(t, []string{"ff"}, binaryClientKeys(pointFF.Kvs))
	require.Equal(t, []string{"h"}, binaryClientValues(pointFF.Kvs))
}

func binaryClientKeys(kvs []*mvccpb.KeyValue) []string {
	keys := make([]string, 0, len(kvs))
	for _, kv := range kvs {
		keys = append(keys, hex.EncodeToString(kv.Key))
	}
	return keys
}

func binaryClientValues(kvs []*mvccpb.KeyValue) []string {
	values := make([]string, 0, len(kvs))
	for _, kv := range kvs {
		values = append(values, string(kv.Value))
	}
	return values
}
