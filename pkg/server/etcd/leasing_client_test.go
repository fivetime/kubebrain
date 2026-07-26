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
	"sync"
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

func TestClientLeasingCachedCompareTypedDoAndNestedBranches(t *testing.T) {
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
	prefix := fmt.Sprintf("/a978/leasing-branching/%d/", time.Now().UnixNano())
	leased, closeLeased, err := leasing.NewKV(client, prefix+"owners/")
	require.NoError(t, err)
	t.Cleanup(closeLeased)

	compareKey := prefix + "compare"
	_, err = client.Put(ctx, compareKey, "abc")
	require.NoError(t, err)
	cached, err := leased.Get(ctx, compareKey)
	require.NoError(t, err)
	require.Len(t, cached.Kvs, 1)
	cachedKV := cached.Kvs[0]
	comparisons := []struct {
		compare clientv3.Cmp
		want    bool
	}{
		{clientv3.Compare(clientv3.Value(compareKey), "=", "abc"), true},
		{clientv3.Compare(clientv3.CreateRevision(compareKey), "=", cachedKV.CreateRevision), true},
		{clientv3.Compare(clientv3.ModRevision(compareKey), "=", cachedKV.ModRevision), true},
		{clientv3.Compare(clientv3.Version(compareKey), "=", cachedKV.Version), true},
		{clientv3.Compare(clientv3.Value(compareKey), ">", "abc"), false},
		{clientv3.Compare(clientv3.CreateRevision(compareKey), ">", cachedKV.CreateRevision), false},
		{clientv3.Compare(clientv3.ModRevision(compareKey), "<", cachedKV.ModRevision), false},
		{clientv3.Compare(clientv3.Version(compareKey), "<", cachedKV.Version), false},
	}
	for index, comparison := range comparisons {
		response, compareErr := leased.Txn(ctx).
			If(comparison.compare).
			Then(clientv3.OpGet(compareKey)).
			Commit()
		require.NoError(t, compareErr, "comparison %d", index)
		require.Equal(t, comparison.want, response.Succeeded, "comparison %d", index)
		expectedResponses := 0
		if comparison.want {
			expectedResponses = 1
		}
		require.Len(t, response.Responses, expectedResponses, "comparison %d", index)
	}

	typedKey := prefix + "typed/value"
	typedOps := []clientv3.Op{
		clientv3.OpTxn(nil, nil, nil),
		clientv3.OpGet(typedKey),
		clientv3.OpPut(typedKey, "typed"),
		clientv3.OpDelete(prefix+"typed/", clientv3.WithPrefix()),
		clientv3.OpTxn(nil, nil, nil),
	}
	for index, operation := range typedOps {
		response, doErr := leased.Do(ctx, operation)
		require.NoError(t, doErr, "typed operation %d", index)
		switch {
		case operation.IsTxn():
			require.NotNil(t, response.Txn(), "typed operation %d", index)
		case operation.IsGet():
			require.NotNil(t, response.Get(), "typed operation %d", index)
		case operation.IsPut():
			require.NotNil(t, response.Put(), "typed operation %d", index)
		case operation.IsDelete():
			require.NotNil(t, response.Del(), "typed operation %d", index)
		}
	}

	treePrefix := prefix + "tree/"
	next := 0
	expected := make(map[string]string)
	treeOperation := deterministicLeasingTree(treePrefix, 3, &next, expected)
	require.Equal(t, 15, next)
	require.Len(t, expected, 4)
	for index := 0; index < next; index++ {
		key := fmt.Sprintf("%s%02d", treePrefix, index)
		_, err = client.Put(ctx, key, "initial")
		require.NoError(t, err)
		_, err = leased.Get(ctx, key)
		require.NoError(t, err)
	}
	treeResponse, err := leased.Do(ctx, treeOperation)
	require.NoError(t, err)
	require.NotNil(t, treeResponse.Txn())
	treeRevision := treeResponse.Txn().Header.Revision
	require.Positive(t, treeRevision)

	for index := 0; index < next; index++ {
		key := fmt.Sprintf("%s%02d", treePrefix, index)
		leasedResponse, leasedErr := leased.Get(ctx, key)
		directResponse, directErr := client.Get(ctx, key)
		require.NoError(t, leasedErr)
		require.NoError(t, directErr)
		require.Len(t, leasedResponse.Kvs, 1)
		require.Len(t, directResponse.Kvs, 1)
		require.Equal(t, directResponse.Kvs[0], leasedResponse.Kvs[0])
		expectedValue, selected := expected[key]
		if selected {
			require.Equal(t, expectedValue, string(directResponse.Kvs[0].Value))
			require.Equal(t, treeRevision, directResponse.Kvs[0].ModRevision)
			continue
		}
		require.Equal(t, "initial", string(directResponse.Kvs[0].Value))
	}
}

func deterministicLeasingTree(prefix string, depth int, next *int, expected map[string]string) clientv3.Op {
	index := *next
	*next = *next + 1
	key := fmt.Sprintf("%s%02d", prefix, index)
	if depth == 0 {
		expected[key] = "leaf"
		return clientv3.OpPut(key, "leaf")
	}

	thenExpected := make(map[string]string)
	thenOperation := deterministicLeasingTree(prefix, depth-1, next, thenExpected)
	elseExpected := make(map[string]string)
	elseOperation := deterministicLeasingTree(prefix, depth-1, next, elseExpected)
	if index%2 == 0 {
		for selectedKey, value := range thenExpected {
			expected[selectedKey] = value
		}
		expected[key] = "then"
		return clientv3.OpTxn(
			[]clientv3.Cmp{clientv3.Compare(clientv3.Version(prefix+"missing"), "=", 0)},
			[]clientv3.Op{thenOperation, clientv3.OpPut(key, "then")},
			[]clientv3.Op{elseOperation, clientv3.OpPut(key, "else")},
		)
	}
	for selectedKey, value := range elseExpected {
		expected[selectedKey] = value
	}
	expected[key] = "else"
	return clientv3.OpTxn(
		[]clientv3.Cmp{clientv3.Compare(clientv3.Version(prefix+"missing"), ">", 0)},
		[]clientv3.Op{thenOperation, clientv3.OpPut(key, "then")},
		[]clientv3.Op{elseOperation, clientv3.OpPut(key, "else")},
	)
}

func TestClientLeasingPointKeyInvalidationPrevKVAndConcurrency(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()

	grpcServer := grpc.NewServer(server.ClientServerOptions()...)
	etcdserverpb.RegisterKVServer(grpcServer, server)
	etcdserverpb.RegisterWatchServer(grpcServer, server)
	etcdserverpb.RegisterLeaseServer(grpcServer, server)
	listener := bufconn.Listen(1 << 20)
	go func() { _ = grpcServer.Serve(listener) }()
	t.Cleanup(grpcServer.Stop)

	newClient := func() *clientv3.Client {
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
		return client
	}
	first := newClient()
	second := newClient()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	prefix := fmt.Sprintf("/a980/leasing-client/%d/", time.Now().UnixNano())
	key := prefix + "data/key"
	firstKV, closeFirst, err := leasing.NewKV(first, prefix+"owners/")
	require.NoError(t, err)
	t.Cleanup(closeFirst)
	secondKV, closeSecond, err := leasing.NewKV(second, prefix+"owners/")
	require.NoError(t, err)
	t.Cleanup(closeSecond)

	missing, err := firstKV.Get(ctx, key)
	require.NoError(t, err)
	require.Empty(t, missing.Kvs)

	_, err = firstKV.Put(ctx, key, "one")
	require.NoError(t, err)
	remote, err := secondKV.Get(ctx, key)
	require.NoError(t, err)
	require.Len(t, remote.Kvs, 1)
	require.Equal(t, []byte("one"), remote.Kvs[0].Value)

	cached, err := firstKV.Get(ctx, key)
	require.NoError(t, err)
	require.Len(t, cached.Kvs, 1)
	require.Equal(t, []byte("one"), cached.Kvs[0].Value)

	_, err = secondKV.Put(ctx, key, "two")
	require.NoError(t, err)
	invalidated, err := firstKV.Get(ctx, key)
	require.NoError(t, err)
	require.Len(t, invalidated.Kvs, 1)
	require.Equal(t, []byte("two"), invalidated.Kvs[0].Value)

	previous, err := firstKV.Put(ctx, key, "three", clientv3.WithPrevKV())
	require.NoError(t, err)
	require.NotNil(t, previous.PrevKv)
	require.Equal(t, []byte("three"), previous.PrevKv.Value)
	historical, err := firstKV.Get(ctx, key, clientv3.WithRev(previous.PrevKv.ModRevision))
	require.NoError(t, err)
	require.Len(t, historical.Kvs, 1)
	require.Equal(t, previous.PrevKv.Value, historical.Kvs[0].Value)

	const workers = 8
	responses := make(chan *clientv3.PutResponse, workers)
	errors := make(chan error, workers)
	var wait sync.WaitGroup
	for worker := 0; worker < workers; worker++ {
		wait.Add(1)
		go func(worker int) {
			defer wait.Done()
			response, putErr := firstKV.Put(ctx, key, fmt.Sprintf("worker-%d", worker))
			if putErr != nil {
				errors <- putErr
				return
			}
			responses <- response
		}(worker)
	}
	wait.Wait()
	close(responses)
	close(errors)
	for err := range errors {
		require.NoError(t, err)
	}
	var maxRevision int64
	for response := range responses {
		require.NotNil(t, response.Header)
		if response.Header.Revision > maxRevision {
			maxRevision = response.Header.Revision
		}
	}
	require.Positive(t, maxRevision)

	current, err := firstKV.Get(ctx, key)
	require.NoError(t, err)
	require.Len(t, current.Kvs, 1)
	require.GreaterOrEqual(t, len(current.Kvs[0].Value), len("worker-"))
	require.Equal(t, "worker-", string(current.Kvs[0].Value[:len("worker-")]))
	require.Equal(t, maxRevision, current.Kvs[0].ModRevision)
	directCurrent, err := first.Get(ctx, key)
	require.NoError(t, err)
	require.Len(t, directCurrent.Kvs, 1)
	require.Equal(t, current.Kvs[0].Value, directCurrent.Kvs[0].Value)
	require.GreaterOrEqual(t, directCurrent.Kvs[0].Version, int64(workers+3))
	require.Equal(t, maxRevision, directCurrent.Kvs[0].ModRevision)

	deleted, err := secondKV.Delete(ctx, key)
	require.NoError(t, err)
	require.NotNil(t, deleted.Header)
	require.Eventually(t, func() bool {
		afterDelete, getErr := firstKV.Get(ctx, key)
		return getErr == nil && len(afterDelete.Kvs) == 0
	}, time.Second, 10*time.Millisecond)
}

func TestClientLeasingRangeOwnershipAndDelete(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()

	grpcServer := grpc.NewServer(server.ClientServerOptions()...)
	etcdserverpb.RegisterKVServer(grpcServer, server)
	etcdserverpb.RegisterWatchServer(grpcServer, server)
	etcdserverpb.RegisterLeaseServer(grpcServer, server)
	listener := bufconn.Listen(1 << 20)
	go func() { _ = grpcServer.Serve(listener) }()
	t.Cleanup(grpcServer.Stop)

	newClient := func() *clientv3.Client {
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
		return client
	}
	first := newClient()
	second := newClient()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	prefix := fmt.Sprintf("/a979/leasing-range/%d/", time.Now().UnixNano())
	dataPrefix := prefix + "data/"
	outsideKey := prefix + "outside"
	firstKV, closeFirst, err := leasing.NewKV(first, prefix+"owners/")
	require.NoError(t, err)
	t.Cleanup(closeFirst)
	secondKV, closeSecond, err := leasing.NewKV(second, prefix+"owners/")
	require.NoError(t, err)
	t.Cleanup(closeSecond)

	const initialKeys = 4
	for index := 0; index < initialKeys; index++ {
		key := fmt.Sprintf("%s%d", dataPrefix, index)
		_, err = first.Put(ctx, key, fmt.Sprintf("initial-%d", index))
		require.NoError(t, err)
	}
	_, err = first.Put(ctx, dataPrefix+"1", "version-two")
	require.NoError(t, err)
	_, err = first.Put(ctx, outsideKey, "outside")
	require.NoError(t, err)

	cached, err := firstKV.Get(ctx, dataPrefix, clientv3.WithPrefix())
	require.NoError(t, err)
	require.Len(t, cached.Kvs, initialKeys)
	rangeCompare, err := firstKV.Txn(ctx).
		If(clientv3.Compare(clientv3.Version(dataPrefix).WithPrefix(), "=", 1)).
		Commit()
	require.NoError(t, err)
	require.False(t, rangeCompare.Succeeded)

	nested, err := secondKV.Txn(ctx).Then(clientv3.OpTxn(
		nil,
		[]clientv3.Op{
			clientv3.OpPut(dataPrefix+"0", "nested-zero"),
			clientv3.OpPut(dataPrefix+"4", "nested-four"),
		},
		nil,
	)).Commit()
	require.NoError(t, err)
	require.Len(t, nested.Responses, 1)
	nestedResponse := nested.Responses[0].GetResponseTxn()
	require.NotNil(t, nestedResponse)
	require.Len(t, nestedResponse.Responses, 2)
	for _, response := range nestedResponse.Responses {
		require.Equal(t, nested.Header.Revision, response.GetResponsePut().Header.Revision)
	}

	afterNested, err := firstKV.Get(ctx, dataPrefix, clientv3.WithPrefix())
	require.NoError(t, err)
	require.Len(t, afterNested.Kvs, initialKeys+1)
	require.Equal(t, [][]byte{
		[]byte("nested-zero"),
		[]byte("version-two"),
		[]byte("initial-2"),
		[]byte("initial-3"),
		[]byte("nested-four"),
	}, [][]byte{
		afterNested.Kvs[0].Value,
		afterNested.Kvs[1].Value,
		afterNested.Kvs[2].Value,
		afterNested.Kvs[3].Value,
		afterNested.Kvs[4].Value,
	})

	deleted, err := secondKV.Delete(ctx, dataPrefix, clientv3.WithPrefix())
	require.NoError(t, err)
	require.Equal(t, int64(initialKeys+1), deleted.Deleted)
	watchCtx, watchCancel := context.WithTimeout(ctx, 5*time.Second)
	defer watchCancel()
	watch := second.Watch(
		watchCtx, dataPrefix, clientv3.WithPrefix(), clientv3.WithRev(deleted.Header.Revision),
	)
	deleteEvents := 0
	for deleteEvents < initialKeys+1 {
		select {
		case response, ok := <-watch:
			require.True(t, ok)
			require.NoError(t, response.Err())
			for _, event := range response.Events {
				require.Equal(t, clientv3.EventTypeDelete, event.Type)
				require.Equal(t, deleted.Header.Revision, event.Kv.ModRevision)
				deleteEvents++
			}
		case <-watchCtx.Done():
			t.Fatalf("timed out after %d/%d delete events at revision %d", deleteEvents, initialKeys+1, deleted.Header.Revision)
		}
	}

	afterDelete, err := firstKV.Get(ctx, dataPrefix, clientv3.WithPrefix())
	require.NoError(t, err)
	require.Empty(t, afterDelete.Kvs)
	outside, err := first.Get(ctx, outsideKey)
	require.NoError(t, err)
	require.Len(t, outside.Kvs, 1)
	require.Equal(t, []byte("outside"), outside.Kvs[0].Value)
}
