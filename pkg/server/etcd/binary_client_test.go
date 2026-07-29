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
	clientv3 "go.etcd.io/etcd/client/v3"
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
	var lastPutRevision int64
	for index, key := range keys {
		put, err := client.Put(ctx, &etcdserverpb.PutRequest{
			Key: key, Value: []byte{byte('a' + index)},
		})
		require.NoError(t, err)
		lastPutRevision = put.Header.Revision
	}

	txnRange, err := client.Txn(ctx, &etcdserverpb.TxnRequest{
		Success: []*etcdserverpb.RequestOp{{
			Request: &etcdserverpb.RequestOp_RequestRange{
				RequestRange: &etcdserverpb.RangeRequest{Key: []byte{0x00}, RangeEnd: []byte{0x01}},
			},
		}},
	})
	require.NoError(t, err)
	require.NotNil(t, txnRange.Header)
	require.Equal(t, lastPutRevision, txnRange.Header.Revision)
	require.Len(t, txnRange.Responses, 1)
	rangedNUL := txnRange.Responses[0].GetResponseRange()
	require.NotNil(t, rangedNUL)
	require.NotNil(t, rangedNUL.Header)
	require.Equal(t, txnRange.Header.Revision, rangedNUL.Header.Revision)
	require.Equal(t, []string{"00", "0000", "0001"}, binaryClientKeys(rangedNUL.Kvs))
	require.Equal(t, []string{"a", "b", "c"}, binaryClientValues(rangedNUL.Kvs))

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
	require.NotNil(t, txnDelete.Header)
	require.Equal(t, lastPutRevision+1, txnDelete.Header.Revision)
	require.Len(t, txnDelete.Responses, 1)
	deleted := txnDelete.Responses[0].GetResponseDeleteRange()
	require.NotNil(t, deleted)
	require.NotNil(t, deleted.Header)
	require.Equal(t, txnDelete.Header.Revision, deleted.Header.Revision)
	require.Equal(t, int64(3), deleted.Deleted)
	require.Equal(t, []string{"00", "0000", "0001"}, binaryClientKeys(deleted.PrevKvs))
	require.Equal(t, []string{"a", "b", "c"}, binaryClientValues(deleted.PrevKvs))

	afterTxn, err := client.Range(ctx, &etcdserverpb.RangeRequest{Key: []byte{0x00}, RangeEnd: []byte{0x01}})
	require.NoError(t, err)
	require.Zero(t, afterTxn.Count)
	require.Empty(t, afterTxn.Kvs)
	pointOne, err := client.Range(ctx, &etcdserverpb.RangeRequest{Key: []byte{0x01}})
	require.NoError(t, err)
	require.Equal(t, []string{"01"}, binaryClientKeys(pointOne.Kvs))
	require.Equal(t, []string{"d"}, binaryClientValues(pointOne.Kvs))

	standalone, err := client.DeleteRange(ctx, &etcdserverpb.DeleteRangeRequest{
		Key: []byte{0xfe}, RangeEnd: []byte{0xff}, PrevKv: true,
	})
	require.NoError(t, err)
	require.NotNil(t, standalone.Header)
	require.Equal(t, txnDelete.Header.Revision+1, standalone.Header.Revision)
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

func TestClientBinaryKeyRangeStreamAndHistoricalRead(t *testing.T) {
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
	keys := [][]byte{
		{0x00},
		{0x00, 0x00},
		{0x00, 0x01},
		{0x7f, 0x00},
		{0xfe},
		{0xfe, 0x00},
		{0xfe, 0x01},
		{0xff},
		{0xff, 0x00},
		{0xff, 0x01},
	}
	for index, key := range keys {
		_, err = client.Put(ctx, string(key), string([]byte{byte('a' + index)}))
		require.NoError(t, err)
	}

	nul, err := client.Get(ctx, string([]byte{0x00}), clientv3.WithRange(string([]byte{0x01})))
	require.NoError(t, err)
	require.Equal(t, int64(3), nul.Count)
	require.Equal(t, []string{"00", "0000", "0001"}, binaryClientKeys(nul.Kvs))
	require.Equal(t, []string{"a", "b", "c"}, binaryClientValues(nul.Kvs))
	nulStream, err := client.GetStream(ctx, string([]byte{0x00}), clientv3.WithRange(string([]byte{0x01})))
	require.NoError(t, err)
	nulMerged, err := clientv3.GetStreamToGetResponse(nulStream)
	require.NoError(t, err)
	require.Equal(t, nul.Count, nulMerged.Count)
	require.Equal(t, binaryClientKeys(nul.Kvs), binaryClientKeys(nulMerged.Kvs))
	require.Equal(t, binaryClientValues(nul.Kvs), binaryClientValues(nulMerged.Kvs))

	highPrefix, err := client.Get(ctx, string([]byte{0xfe}), clientv3.WithRange(string([]byte{0xff})))
	require.NoError(t, err)
	require.Equal(t, int64(3), highPrefix.Count)
	require.Equal(t, []string{"fe", "fe00", "fe01"}, binaryClientKeys(highPrefix.Kvs))
	require.Equal(t, []string{"e", "f", "g"}, binaryClientValues(highPrefix.Kvs))
	highPrefixStream, err := client.GetStream(ctx, string([]byte{0xfe}), clientv3.WithRange(string([]byte{0xff})))
	require.NoError(t, err)
	highPrefixMerged, err := clientv3.GetStreamToGetResponse(highPrefixStream)
	require.NoError(t, err)
	require.Equal(t, highPrefix.Count, highPrefixMerged.Count)
	require.Equal(t, binaryClientKeys(highPrefix.Kvs), binaryClientKeys(highPrefixMerged.Kvs))
	require.Equal(t, binaryClientValues(highPrefix.Kvs), binaryClientValues(highPrefixMerged.Kvs))

	fromFF, err := client.Get(ctx, string([]byte{0xff}), clientv3.WithFromKey(), clientv3.WithLimit(2))
	require.NoError(t, err)
	require.Equal(t, int64(3), fromFF.Count)
	require.True(t, fromFF.More)
	require.Equal(t, []string{"ff", "ff00"}, binaryClientKeys(fromFF.Kvs))
	require.Equal(t, []string{"h", "i"}, binaryClientValues(fromFF.Kvs))
	fromFFStream, err := client.GetStream(ctx, string([]byte{0xff}), clientv3.WithFromKey(), clientv3.WithLimit(2))
	require.NoError(t, err)
	fromFFMerged, err := clientv3.GetStreamToGetResponse(fromFFStream)
	require.NoError(t, err)
	require.Equal(t, fromFF.Count, fromFFMerged.Count)
	require.Equal(t, fromFF.More, fromFFMerged.More)
	require.Equal(t, binaryClientKeys(fromFF.Kvs), binaryClientKeys(fromFFMerged.Kvs))
	require.Equal(t, binaryClientValues(fromFF.Kvs), binaryClientValues(fromFFMerged.Kvs))

	descendingFF, err := client.Get(ctx, string([]byte{0xff}),
		clientv3.WithFromKey(),
		clientv3.WithLimit(2),
		clientv3.WithSort(clientv3.SortByKey, clientv3.SortDescend),
	)
	require.NoError(t, err)
	require.Equal(t, int64(3), descendingFF.Count)
	require.True(t, descendingFF.More)
	require.Equal(t, []string{"ff01", "ff00"}, binaryClientKeys(descendingFF.Kvs))
	require.Equal(t, []string{"j", "i"}, binaryClientValues(descendingFF.Kvs))

	beforeDelete, err := client.Get(ctx, string([]byte{0xff}))
	require.NoError(t, err)
	require.Len(t, beforeDelete.Kvs, 1)
	_, err = client.Delete(ctx, string([]byte{0xff}))
	require.NoError(t, err)
	current, err := client.Get(ctx, string([]byte{0xff}))
	require.NoError(t, err)
	require.Empty(t, current.Kvs)
	historical, err := client.Get(ctx, string([]byte{0xff}), clientv3.WithRev(beforeDelete.Header.Revision))
	require.NoError(t, err)
	require.Equal(t, []string{"ff"}, binaryClientKeys(historical.Kvs))
	require.Equal(t, []string{"h"}, binaryClientValues(historical.Kvs))
}

func TestClientBinaryKeyTxnAndDeleteMutations(t *testing.T) {
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
	var lastPutRevision int64
	for index, key := range keys {
		put, err := client.Put(ctx, string(key), string([]byte{byte('a' + index)}))
		require.NoError(t, err)
		lastPutRevision = put.Header.Revision
	}

	txnRange, err := client.Txn(ctx).Then(clientv3.OpGet(
		string([]byte{0x00}),
		clientv3.WithRange(string([]byte{0x01})),
	)).Commit()
	require.NoError(t, err)
	require.True(t, txnRange.Succeeded)
	require.NotNil(t, txnRange.Header)
	require.Equal(t, lastPutRevision, txnRange.Header.Revision)
	require.Len(t, txnRange.Responses, 1)
	nulRange := txnRange.Responses[0].GetResponseRange()
	require.NotNil(t, nulRange)
	require.NotNil(t, nulRange.Header)
	require.Equal(t, txnRange.Header.Revision, nulRange.Header.Revision)
	require.Equal(t, []string{"00", "0000", "0001"}, binaryClientKeys(nulRange.Kvs))
	require.Equal(t, []string{"a", "b", "c"}, binaryClientValues(nulRange.Kvs))

	txnDelete, err := client.Txn(ctx).Then(clientv3.OpDelete(
		string([]byte{0x00}),
		clientv3.WithRange(string([]byte{0x01})),
		clientv3.WithPrevKV(),
	)).Commit()
	require.NoError(t, err)
	require.True(t, txnDelete.Succeeded)
	require.NotNil(t, txnDelete.Header)
	require.Equal(t, lastPutRevision+1, txnDelete.Header.Revision)
	require.Len(t, txnDelete.Responses, 1)
	deletedNUL := txnDelete.Responses[0].GetResponseDeleteRange()
	require.NotNil(t, deletedNUL)
	require.NotNil(t, deletedNUL.Header)
	require.Equal(t, txnDelete.Header.Revision, deletedNUL.Header.Revision)
	require.Equal(t, int64(3), deletedNUL.Deleted)
	require.Equal(t, []string{"00", "0000", "0001"}, binaryClientKeys(deletedNUL.PrevKvs))
	require.Equal(t, []string{"a", "b", "c"}, binaryClientValues(deletedNUL.PrevKvs))

	afterTxnDelete, err := client.Get(ctx, string([]byte{0x00}), clientv3.WithRange(string([]byte{0x01})))
	require.NoError(t, err)
	require.Zero(t, afterTxnDelete.Count)
	require.Empty(t, afterTxnDelete.Kvs)
	pointOne, err := client.Get(ctx, string([]byte{0x01}))
	require.NoError(t, err)
	require.Equal(t, []string{"01"}, binaryClientKeys(pointOne.Kvs))
	require.Equal(t, []string{"d"}, binaryClientValues(pointOne.Kvs))

	standaloneDelete, err := client.Delete(
		ctx,
		string([]byte{0xfe}),
		clientv3.WithRange(string([]byte{0xff})),
		clientv3.WithPrevKV(),
	)
	require.NoError(t, err)
	require.NotNil(t, standaloneDelete.Header)
	require.Equal(t, txnDelete.Header.Revision+1, standaloneDelete.Header.Revision)
	require.Equal(t, int64(3), standaloneDelete.Deleted)
	require.Equal(t, []string{"fe", "fe00", "fe01"}, binaryClientKeys(standaloneDelete.PrevKvs))
	require.Equal(t, []string{"e", "f", "g"}, binaryClientValues(standaloneDelete.PrevKvs))

	afterStandaloneDelete, err := client.Get(ctx, string([]byte{0xfe}), clientv3.WithRange(string([]byte{0xff})))
	require.NoError(t, err)
	require.Zero(t, afterStandaloneDelete.Count)
	require.Empty(t, afterStandaloneDelete.Kvs)
	pointFF, err := client.Get(ctx, string([]byte{0xff}))
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
