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
	"math"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
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

func TestClientRangeOptionInteractionsMatchEtcd(t *testing.T) {
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
	prefix := fmt.Sprintf("/a1041/range-options-client/%d/", time.Now().UnixNano())
	for _, seed := range []struct {
		key   string
		value string
	}{
		{key: "d", value: "n"},
		{key: "c", value: "a"},
		{key: "b", value: "m"},
		{key: "a", value: "z"},
	} {
		_, err = client.Put(ctx, prefix+seed.key, seed.value)
		require.NoError(t, err)
	}
	updateB, err := client.Put(ctx, prefix+"b", "y")
	require.NoError(t, err)

	filtered, err := client.Get(ctx, prefix,
		clientv3.WithPrefix(),
		clientv3.WithMinModRev(updateB.Header.Revision),
		clientv3.WithLimit(1),
		clientv3.WithSort(clientv3.SortByKey, clientv3.SortAscend),
	)
	require.NoError(t, err)
	require.Equal(t, int64(4), filtered.Count)
	require.False(t, filtered.More)
	require.Equal(t, []string{"b"}, rangeClientRelativeKeys(filtered.Kvs, prefix))
	require.Equal(t, []string{"y"}, rangeClientValues(filtered.Kvs))

	counted, err := client.Get(ctx, prefix,
		clientv3.WithPrefix(),
		clientv3.WithMinModRev(updateB.Header.Revision),
		clientv3.WithLimit(1),
		clientv3.WithCountOnly(),
	)
	require.NoError(t, err)
	require.Equal(t, int64(4), counted.Count)
	require.Empty(t, counted.Kvs)
	require.False(t, counted.More)

	contradictory, err := client.Get(ctx, prefix,
		clientv3.WithPrefix(),
		clientv3.WithMinModRev(updateB.Header.Revision),
		clientv3.WithMaxModRev(updateB.Header.Revision-1),
		clientv3.WithLimit(1),
	)
	require.NoError(t, err)
	require.Equal(t, int64(4), contradictory.Count)
	require.Empty(t, contradictory.Kvs)
	require.False(t, contradictory.More)

	valueSorted, err := client.Get(ctx, prefix,
		clientv3.WithPrefix(),
		clientv3.WithLimit(2),
		clientv3.WithSort(clientv3.SortByValue, clientv3.SortNone),
	)
	require.NoError(t, err)
	require.Equal(t, int64(4), valueSorted.Count)
	require.True(t, valueSorted.More)
	require.Equal(t, []string{"c", "b"}, rangeClientRelativeKeys(valueSorted.Kvs, prefix))
	require.Equal(t, []string{"a", "y"}, rangeClientValues(valueSorted.Kvs))

	keysOnlyValueSorted, err := client.Get(ctx, prefix,
		clientv3.WithPrefix(),
		clientv3.WithLimit(2),
		clientv3.WithKeysOnly(),
		clientv3.WithSort(clientv3.SortByValue, clientv3.SortDescend),
	)
	require.NoError(t, err)
	require.Equal(t, int64(4), keysOnlyValueSorted.Count)
	require.True(t, keysOnlyValueSorted.More)
	require.Equal(t, []string{"a", "b"}, rangeClientRelativeKeys(keysOnlyValueSorted.Kvs, prefix))
	for _, kv := range keysOnlyValueSorted.Kvs {
		require.Empty(t, kv.Value)
	}

	versionSorted, err := client.Get(ctx, prefix,
		clientv3.WithPrefix(),
		clientv3.WithSort(clientv3.SortByVersion, clientv3.SortDescend),
	)
	require.NoError(t, err)
	require.Equal(t, int64(4), versionSorted.Count)
	require.False(t, versionSorted.More)
	require.Len(t, versionSorted.Kvs, 4)
	require.Equal(t, prefix+"b", string(versionSorted.Kvs[0].Key))
	require.Equal(t, int64(2), versionSorted.Kvs[0].Version)

	maxLimit, err := client.Get(ctx, prefix,
		clientv3.WithPrefix(),
		clientv3.WithLimit(math.MaxInt64),
		clientv3.WithSort(clientv3.SortByValue, clientv3.SortNone),
	)
	require.NoError(t, err)
	require.Equal(t, int64(4), maxLimit.Count)
	require.False(t, maxLimit.More)
	require.Equal(t, []string{"c", "d", "b", "a"}, rangeClientRelativeKeys(maxLimit.Kvs, prefix))

	txn, err := client.Txn(ctx).Then(
		clientv3.OpPut(prefix+"e", "0"),
		clientv3.OpGet(prefix,
			clientv3.WithPrefix(),
			clientv3.WithMinModRev(updateB.Header.Revision+1),
			clientv3.WithLimit(1),
			clientv3.WithSort(clientv3.SortByKey, clientv3.SortAscend),
		),
		clientv3.OpPut(prefix+"b", "updated"),
		clientv3.OpGet(prefix,
			clientv3.WithPrefix(),
			clientv3.WithMinModRev(updateB.Header.Revision+1),
			clientv3.WithLimit(1),
			clientv3.WithCountOnly(),
		),
	).Commit()
	require.NoError(t, err)
	require.True(t, txn.Succeeded)
	require.Len(t, txn.Responses, 4)
	stagedFiltered := txn.Responses[1].GetResponseRange()
	require.NotNil(t, stagedFiltered)
	require.Equal(t, int64(5), stagedFiltered.Count)
	require.False(t, stagedFiltered.More)
	require.Equal(t, []string{"e"}, rangeClientRelativeKeys(stagedFiltered.Kvs, prefix))
	require.Equal(t, []bool{true}, rangeClientAtRevision(stagedFiltered.Kvs, txn.Header.Revision))

	stagedCounted := txn.Responses[3].GetResponseRange()
	require.NotNil(t, stagedCounted)
	require.Equal(t, int64(5), stagedCounted.Count)
	require.Empty(t, stagedCounted.Kvs)
	require.False(t, stagedCounted.More)
}

func TestClientRangeKeysOnlyLimitAcrossTombstones(t *testing.T) {
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
	prefix := fmt.Sprintf("/a995/range-tombstone-client/%d/", time.Now().UnixNano())
	var beforeDeletes int64
	for _, suffix := range []string{"a", "b", "c", "d"} {
		response, putErr := client.Put(ctx, prefix+suffix, "value-"+suffix)
		require.NoError(t, putErr)
		beforeDeletes = response.Header.Revision
	}
	_, err = client.Delete(ctx, prefix+"b")
	require.NoError(t, err)
	deleted, err := client.Delete(ctx, prefix+"d")
	require.NoError(t, err)
	_, err = client.Put(ctx, prefix+"c", "updated-c")
	require.NoError(t, err)
	_, err = client.Put(ctx, prefix+"b", "recreated-b")
	require.NoError(t, err)
	_, err = client.Put(ctx, prefix+"e", "value-e")
	require.NoError(t, err)

	assertPage := func(stage string, revision, limit int64, wantKeys []string, wantCount int64, wantMore bool) {
		t.Helper()
		options := []clientv3.OpOption{
			clientv3.WithPrefix(), clientv3.WithKeysOnly(), clientv3.WithLimit(limit),
		}
		if revision != 0 {
			options = append(options, clientv3.WithRev(revision))
		}
		response, getErr := client.Get(ctx, prefix, options...)
		require.NoError(t, getErr, stage)
		require.Equal(t, wantCount, response.Count, stage)
		require.Equal(t, wantMore, response.More, stage)
		require.Equal(t, wantKeys, rangeClientRelativeKeys(response.Kvs, prefix), stage)
		for _, kv := range response.Kvs {
			require.Empty(t, kv.Value, stage)
		}
	}
	assertPage("before-deletes", beforeDeletes, 2, []string{"a", "b"}, 4, true)
	assertPage("after-deletes", deleted.Header.Revision, 1, []string{"a"}, 2, true)
	assertPage("after-recreate", 0, 2, []string{"a", "b"}, 4, true)
}

func TestClientRangeKeysOnlyLimitDifferentialPages(t *testing.T) {
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
	prefix := fmt.Sprintf("/a1042/keys-limit/%d/", time.Now().UnixNano())
	for i := 0; i < 8; i++ {
		_, err = client.Put(ctx, fmt.Sprintf("%s%02d", prefix, i), strings.Repeat("value", 100))
		require.NoError(t, err)
	}
	historical, err := client.Get(ctx, prefix, clientv3.WithPrefix(), clientv3.WithLimit(1))
	require.NoError(t, err)
	for i := 8; i < 12; i++ {
		_, err = client.Put(ctx, fmt.Sprintf("%s%02d", prefix, i), strings.Repeat("later", 100))
		require.NoError(t, err)
	}
	_, err = client.Delete(ctx, prefix+"02")
	require.NoError(t, err)

	assertKeysOnlyPage := func(name string, opts []clientv3.OpOption, wantKeys []string, wantCount int64, wantMore bool) {
		t.Helper()
		response, getErr := client.Get(ctx, prefix, opts...)
		require.NoError(t, getErr, name)
		require.Equal(t, wantCount, response.Count, name)
		require.Equal(t, wantMore, response.More, name)
		require.Equal(t, wantKeys, rangeClientRelativeKeys(response.Kvs, prefix), name)
		for _, kv := range response.Kvs {
			require.Empty(t, kv.Value, name)
		}
	}

	assertKeysOnlyPage("current-limited",
		[]clientv3.OpOption{clientv3.WithPrefix(), clientv3.WithKeysOnly(), clientv3.WithLimit(3)},
		[]string{"00", "01", "03"}, 11, true)
	assertKeysOnlyPage("historical-limited",
		[]clientv3.OpOption{
			clientv3.WithPrefix(), clientv3.WithKeysOnly(), clientv3.WithLimit(3),
			clientv3.WithRev(historical.Header.Revision),
		},
		[]string{"00", "01", "02"}, 8, true)
	assertKeysOnlyPage("current-wide-limit",
		[]clientv3.OpOption{clientv3.WithPrefix(), clientv3.WithKeysOnly(), clientv3.WithLimit(20)},
		[]string{"00", "01", "03", "04", "05", "06", "07", "08", "09", "10", "11"}, 11, false)
}

func TestRawGRPCRangeKeysOnlyLimitAcrossTombstones(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()

	grpcServer := grpc.NewServer(server.ClientServerOptions()...)
	etcdserverpb.RegisterKVServer(grpcServer, server)
	listener := bufconn.Listen(1 << 20)
	go func() { _ = grpcServer.Serve(listener) }()
	t.Cleanup(grpcServer.Stop)

	conn, err := grpc.NewClient("passthrough:///range-tombstone-client",
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
			return listener.Dial()
		}))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, conn.Close()) })
	kv := etcdserverpb.NewKVClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	prefix := fmt.Sprintf("/a1019/range-tombstone-raw/%d/", time.Now().UnixNano())
	end := []byte(clientv3.GetPrefixRangeEnd(prefix))
	var beforeDeletes int64
	for _, suffix := range []string{"a", "b", "c", "d"} {
		response, putErr := kv.Put(ctx, &etcdserverpb.PutRequest{
			Key: []byte(prefix + suffix), Value: []byte("value-" + suffix),
		})
		require.NoError(t, putErr)
		beforeDeletes = response.Header.Revision
	}
	_, err = kv.DeleteRange(ctx, &etcdserverpb.DeleteRangeRequest{Key: []byte(prefix + "b")})
	require.NoError(t, err)
	deleted, err := kv.DeleteRange(ctx, &etcdserverpb.DeleteRangeRequest{Key: []byte(prefix + "d")})
	require.NoError(t, err)
	_, err = kv.Put(ctx, &etcdserverpb.PutRequest{Key: []byte(prefix + "c"), Value: []byte("updated-c")})
	require.NoError(t, err)
	_, err = kv.Put(ctx, &etcdserverpb.PutRequest{Key: []byte(prefix + "b"), Value: []byte("recreated-b")})
	require.NoError(t, err)
	_, err = kv.Put(ctx, &etcdserverpb.PutRequest{Key: []byte(prefix + "e"), Value: []byte("value-e")})
	require.NoError(t, err)

	assertPage := func(stage string, revision, limit int64, wantKeys []string, wantCount int64, wantMore bool) {
		t.Helper()
		response, getErr := kv.Range(ctx, &etcdserverpb.RangeRequest{
			Key: []byte(prefix), RangeEnd: end, Revision: revision,
			Limit: limit, KeysOnly: true,
		})
		require.NoError(t, getErr, stage)
		require.Equal(t, wantCount, response.Count, stage)
		require.Equal(t, wantMore, response.More, stage)
		require.Equal(t, wantKeys, rangeClientRelativeKeys(response.Kvs, prefix), stage)
		for _, item := range response.Kvs {
			require.Empty(t, item.Value, stage)
		}
	}
	assertPage("before-deletes", beforeDeletes, 2, []string{"a", "b"}, 4, true)
	assertPage("after-deletes", deleted.Header.Revision, 1, []string{"a"}, 2, true)
	assertPage("after-recreate", 0, 2, []string{"a", "b"}, 4, true)
}

func TestRawGRPCRangeCountOnlyTakesPrecedenceOverKeysOnly(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()

	grpcServer := grpc.NewServer(server.ClientServerOptions()...)
	etcdserverpb.RegisterKVServer(grpcServer, server)
	listener := bufconn.Listen(1 << 20)
	go func() { _ = grpcServer.Serve(listener) }()
	t.Cleanup(grpcServer.Stop)

	conn, err := grpc.NewClient("passthrough:///range-keys-count-client",
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
			return listener.Dial()
		}))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, conn.Close()) })
	kv := etcdserverpb.NewKVClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	prefix := fmt.Sprintf("/a1020/range-keys-count/%d/", time.Now().UnixNano())
	end := []byte(clientv3.GetPrefixRangeEnd(prefix))
	for _, suffix := range []string{"a", "b", "c"} {
		_, err = kv.Put(ctx, &etcdserverpb.PutRequest{
			Key: []byte(prefix + suffix), Value: []byte("hidden-" + suffix),
		})
		require.NoError(t, err)
	}

	response, err := kv.Range(ctx, &etcdserverpb.RangeRequest{
		Key: []byte(prefix), RangeEnd: end, CountOnly: true, KeysOnly: true, Limit: 1,
	})
	require.NoError(t, err)
	require.Equal(t, int64(3), response.Count)
	require.Empty(t, response.Kvs)
	require.False(t, response.More)
	requireClientRangeHeaderWellFormed(t, response.Header)
}

func TestRawGRPCRangeRevisionBoundaries(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()

	grpcServer := grpc.NewServer(server.ClientServerOptions()...)
	etcdserverpb.RegisterKVServer(grpcServer, server)
	listener := bufconn.Listen(1 << 20)
	go func() { _ = grpcServer.Serve(listener) }()
	t.Cleanup(grpcServer.Stop)

	conn, err := grpc.NewClient("passthrough:///range-revision-client",
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
			return listener.Dial()
		}))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, conn.Close()) })
	kv := etcdserverpb.NewKVClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	prefix := []byte(fmt.Sprintf("/a1000/range-revision-client/%d/", time.Now().UnixNano()))
	end := []byte(clientv3.GetPrefixRangeEnd(string(prefix)))
	key := append(append([]byte{}, prefix...), 'a')
	_, err = kv.Put(ctx, &etcdserverpb.PutRequest{Key: key, Value: []byte("value")})
	require.NoError(t, err)
	current, err := kv.Range(ctx, &etcdserverpb.RangeRequest{Key: key})
	require.NoError(t, err)
	_, err = kv.Compact(ctx, &etcdserverpb.CompactionRequest{Revision: current.Header.Revision})
	require.NoError(t, err)

	for _, req := range []*etcdserverpb.RangeRequest{
		{Key: key, Revision: -1},
		{Key: key, Revision: -2},
		{Key: prefix, RangeEnd: end, Revision: -1},
		{Key: prefix, RangeEnd: end, Revision: -1, CountOnly: true},
		{Key: prefix, RangeEnd: end, Revision: -1, KeysOnly: true},
	} {
		_, rangeErr := kv.Range(ctx, req)
		require.NoError(t, rangeErr)
	}
	_, err = kv.Range(ctx, &etcdserverpb.RangeRequest{Key: key, Revision: math.MaxInt64})
	requireRawGRPCRangeRevisionError(t, err, codes.OutOfRange, "etcdserver: mvcc: required revision is a future revision")

	selectedNegative := &etcdserverpb.TxnRequest{
		Success: []*etcdserverpb.RequestOp{rawGRPCRangeRequestOp(&etcdserverpb.RangeRequest{
			Key: key, Revision: -1,
		})},
	}
	_, err = kv.Txn(ctx, selectedNegative)
	requireRawGRPCRangeRevisionError(t, err, codes.OutOfRange, "etcdserver: mvcc: required revision has been compacted")

	unselectedNegative := &etcdserverpb.TxnRequest{
		Compare: []*etcdserverpb.Compare{{
			Key: key, Target: etcdserverpb.Compare_VERSION, Result: etcdserverpb.Compare_GREATER,
			TargetUnion: &etcdserverpb.Compare_Version{Version: 0},
		}},
		Success: []*etcdserverpb.RequestOp{rawGRPCRangeRequestOp(&etcdserverpb.RangeRequest{Key: key})},
		Failure: []*etcdserverpb.RequestOp{rawGRPCRangeRequestOp(&etcdserverpb.RangeRequest{Key: key, Revision: -1})},
	}
	_, err = kv.Txn(ctx, unselectedNegative)
	require.NoError(t, err)

	selectedMaximum := &etcdserverpb.TxnRequest{
		Success: []*etcdserverpb.RequestOp{rawGRPCRangeRequestOp(&etcdserverpb.RangeRequest{
			Key: key, Revision: math.MaxInt64,
		})},
	}
	_, err = kv.Txn(ctx, selectedMaximum)
	requireRawGRPCRangeRevisionError(t, err, codes.OutOfRange, "etcdserver: mvcc: required revision is a future revision")
}

func TestClientRangeRevisionBoundaries(t *testing.T) {
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
	prefix := fmt.Sprintf("/a1036/range-revision-client/%d/", time.Now().UnixNano())
	key := prefix + "a"
	_, err = client.Put(ctx, key, "value")
	require.NoError(t, err)
	current, err := client.Get(ctx, key)
	require.NoError(t, err)
	require.Len(t, current.Kvs, 1)
	_, err = client.Compact(ctx, current.Header.Revision)
	require.NoError(t, err)

	point, err := client.Get(ctx, key, clientv3.WithRev(-1))
	require.NoError(t, err)
	require.Len(t, point.Kvs, 1)
	require.Equal(t, "value", string(point.Kvs[0].Value))
	belowNegativeOne, err := client.Get(ctx, key, clientv3.WithRev(-2))
	require.NoError(t, err)
	require.Len(t, belowNegativeOne.Kvs, 1)
	prefixRange, err := client.Get(ctx, prefix, clientv3.WithPrefix(), clientv3.WithRev(-1))
	require.NoError(t, err)
	require.Equal(t, int64(1), prefixRange.Count)
	countOnly, err := client.Get(ctx, prefix, clientv3.WithPrefix(), clientv3.WithRev(-1), clientv3.WithCountOnly())
	require.NoError(t, err)
	require.Equal(t, int64(1), countOnly.Count)
	require.Empty(t, countOnly.Kvs)
	keysOnly, err := client.Get(ctx, prefix, clientv3.WithPrefix(), clientv3.WithRev(-1), clientv3.WithKeysOnly())
	require.NoError(t, err)
	require.Equal(t, int64(1), keysOnly.Count)
	require.Len(t, keysOnly.Kvs, 1)
	require.Empty(t, keysOnly.Kvs[0].Value)

	_, err = client.Get(ctx, key, clientv3.WithRev(math.MaxInt64))
	requireClientRangeError(t, err, codes.Unknown, "etcdserver: mvcc: required revision is a future revision")
}

func TestRawGRPCRangeOptionInteractions(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()

	grpcServer := grpc.NewServer(server.ClientServerOptions()...)
	etcdserverpb.RegisterKVServer(grpcServer, server)
	listener := bufconn.Listen(1 << 20)
	go func() { _ = grpcServer.Serve(listener) }()
	t.Cleanup(grpcServer.Stop)

	conn, err := grpc.NewClient("passthrough:///range-options-client",
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
			return listener.Dial()
		}))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, conn.Close()) })
	kv := etcdserverpb.NewKVClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	prefix := fmt.Sprintf("/a1003/range-options-client/%d/", time.Now().UnixNano())
	end := clientv3.GetPrefixRangeEnd(prefix)
	for _, seed := range []struct {
		key   string
		value string
	}{
		{key: "d", value: "n"},
		{key: "c", value: "a"},
		{key: "b", value: "m"},
		{key: "a", value: "z"},
	} {
		_, err = kv.Put(ctx, &etcdserverpb.PutRequest{
			Key: []byte(prefix + seed.key), Value: []byte(seed.value),
		})
		require.NoError(t, err)
	}
	updateB, err := kv.Put(ctx, &etcdserverpb.PutRequest{
		Key: []byte(prefix + "b"), Value: []byte("y"),
	})
	require.NoError(t, err)

	filtered, err := kv.Range(ctx, &etcdserverpb.RangeRequest{
		Key: []byte(prefix), RangeEnd: []byte(end), MinModRevision: updateB.Header.Revision,
		Limit: 1, SortOrder: etcdserverpb.RangeRequest_ASCEND, SortTarget: etcdserverpb.RangeRequest_KEY,
	})
	require.NoError(t, err)
	require.Equal(t, int64(4), filtered.Count)
	require.False(t, filtered.More)
	require.Equal(t, []string{"b"}, rangeClientRelativeKeys(filtered.Kvs, prefix))
	require.Equal(t, []string{"y"}, rangeClientValues(filtered.Kvs))

	counted, err := kv.Range(ctx, &etcdserverpb.RangeRequest{
		Key: []byte(prefix), RangeEnd: []byte(end), MinModRevision: updateB.Header.Revision,
		Limit: 1, CountOnly: true,
	})
	require.NoError(t, err)
	require.Equal(t, int64(4), counted.Count)
	require.Empty(t, counted.Kvs)
	require.False(t, counted.More)

	valueSorted, err := kv.Range(ctx, &etcdserverpb.RangeRequest{
		Key: []byte(prefix), RangeEnd: []byte(end), Limit: 2,
		SortTarget: etcdserverpb.RangeRequest_VALUE,
	})
	require.NoError(t, err)
	require.Equal(t, int64(4), valueSorted.Count)
	require.True(t, valueSorted.More)
	require.Equal(t, []string{"c", "b"}, rangeClientRelativeKeys(valueSorted.Kvs, prefix))
	require.Equal(t, []string{"a", "y"}, rangeClientValues(valueSorted.Kvs))

	keysOnlyValueSorted, err := kv.Range(ctx, &etcdserverpb.RangeRequest{
		Key: []byte(prefix), RangeEnd: []byte(end), Limit: 2, KeysOnly: true,
		SortOrder: etcdserverpb.RangeRequest_DESCEND, SortTarget: etcdserverpb.RangeRequest_VALUE,
	})
	require.NoError(t, err)
	require.Equal(t, int64(4), keysOnlyValueSorted.Count)
	require.True(t, keysOnlyValueSorted.More)
	require.Equal(t, []string{"a", "b"}, rangeClientRelativeKeys(keysOnlyValueSorted.Kvs, prefix))
	for _, got := range keysOnlyValueSorted.Kvs {
		require.Empty(t, got.Value)
	}

	contradictory, err := kv.Range(ctx, &etcdserverpb.RangeRequest{
		Key: []byte(prefix), RangeEnd: []byte(end),
		MinModRevision: updateB.Header.Revision, MaxModRevision: updateB.Header.Revision - 1, Limit: 1,
	})
	require.NoError(t, err)
	require.Equal(t, int64(4), contradictory.Count)
	require.Empty(t, contradictory.Kvs)
	require.False(t, contradictory.More)

	txn, err := kv.Txn(ctx, &etcdserverpb.TxnRequest{Success: []*etcdserverpb.RequestOp{
		rawGRPCPutRequestOp(&etcdserverpb.PutRequest{Key: []byte(prefix + "e"), Value: []byte("0")}),
		rawGRPCRangeRequestOp(&etcdserverpb.RangeRequest{
			Key: []byte(prefix), RangeEnd: []byte(end), MinModRevision: updateB.Header.Revision + 1,
			Limit: 1, SortOrder: etcdserverpb.RangeRequest_ASCEND, SortTarget: etcdserverpb.RangeRequest_KEY,
		}),
		rawGRPCPutRequestOp(&etcdserverpb.PutRequest{Key: []byte(prefix + "b"), Value: []byte("updated")}),
		rawGRPCRangeRequestOp(&etcdserverpb.RangeRequest{
			Key: []byte(prefix), RangeEnd: []byte(end), MinModRevision: updateB.Header.Revision + 1,
			Limit: 1, CountOnly: true,
		}),
	}})
	require.NoError(t, err)
	require.Len(t, txn.Responses, 4)
	stagedFiltered := txn.Responses[1].GetResponseRange()
	require.NotNil(t, stagedFiltered)
	require.Equal(t, int64(5), stagedFiltered.Count)
	require.False(t, stagedFiltered.More)
	require.Equal(t, []string{"e"}, rangeClientRelativeKeys(stagedFiltered.Kvs, prefix))
	require.Equal(t, []bool{true}, rangeClientAtRevision(stagedFiltered.Kvs, txn.Header.Revision))

	stagedCounted := txn.Responses[3].GetResponseRange()
	require.NotNil(t, stagedCounted)
	require.Equal(t, int64(5), stagedCounted.Count)
	require.Empty(t, stagedCounted.Kvs)
	require.False(t, stagedCounted.More)
}

func rawGRPCRangeRequestOp(request *etcdserverpb.RangeRequest) *etcdserverpb.RequestOp {
	return &etcdserverpb.RequestOp{Request: &etcdserverpb.RequestOp_RequestRange{RequestRange: request}}
}

func rawGRPCPutRequestOp(request *etcdserverpb.PutRequest) *etcdserverpb.RequestOp {
	return &etcdserverpb.RequestOp{Request: &etcdserverpb.RequestOp_RequestPut{RequestPut: request}}
}

func requireRawGRPCRangeRevisionError(t *testing.T, err error, code codes.Code, message string) {
	t.Helper()
	require.Error(t, err)
	require.Equal(t, code, status.Code(err))
	require.Equal(t, message, status.Convert(err).Message())
}

func requireClientRangeError(t *testing.T, err error, code codes.Code, message string) {
	t.Helper()
	require.Error(t, err)
	require.Equal(t, code, status.Code(err))
	require.Equal(t, message, status.Convert(err).Message())
}

func requireClientRangeHeaderWellFormed(t *testing.T, header *etcdserverpb.ResponseHeader) {
	t.Helper()
	require.NotNil(t, header)
	require.NotZero(t, header.ClusterId)
	require.NotZero(t, header.MemberId)
	require.Positive(t, header.Revision)
	require.Positive(t, header.RaftTerm)
}

func rangeClientRelativeKeys(kvs []*mvccpb.KeyValue, prefix string) []string {
	keys := make([]string, 0, len(kvs))
	for _, kv := range kvs {
		keys = append(keys, strings.TrimPrefix(string(kv.Key), prefix))
	}
	return keys
}

func rangeClientValues(kvs []*mvccpb.KeyValue) []string {
	values := make([]string, 0, len(kvs))
	for _, kv := range kvs {
		values = append(values, string(kv.Value))
	}
	return values
}

func rangeClientAtRevision(kvs []*mvccpb.KeyValue, revision int64) []bool {
	matches := make([]bool, 0, len(kvs))
	for _, kv := range kvs {
		matches = append(matches, kv.ModRevision == revision)
	}
	return matches
}
