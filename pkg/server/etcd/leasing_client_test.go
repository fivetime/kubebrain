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
	"errors"
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/concurrency"
	"go.etcd.io/etcd/client/v3/leasing"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
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

func TestClientLeasingCacheIsolationAndGetOptions(t *testing.T) {
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
	prefix := fmt.Sprintf("/a981/leasing-cache-contract/%d/", time.Now().UnixNano())
	key := prefix + "cached"
	leased, closeLeased, err := leasing.NewKV(client, prefix+"owners/")
	require.NoError(t, err)
	t.Cleanup(closeLeased)

	_, err = client.Put(ctx, key, "initial")
	require.NoError(t, err)
	first, err := leased.Get(ctx, key)
	require.NoError(t, err)
	require.Len(t, first.Kvs, 1)
	first.Kvs[0].Key[0] ^= 0xff
	first.Kvs[0].Value[0] ^= 0xff
	isolated, err := leased.Get(ctx, key)
	require.NoError(t, err)
	require.Len(t, isolated.Kvs, 1)
	require.Equal(t, []byte(key), isolated.Kvs[0].Key)
	require.Equal(t, []byte("initial"), isolated.Kvs[0].Value)

	put, err := leased.Put(ctx, key, "offline")
	require.NoError(t, err)
	require.NotNil(t, put.Header)
	offline, err := leased.Get(ctx, key)
	require.NoError(t, err)
	require.Len(t, offline.Kvs, 1)
	require.Equal(t, []byte("offline"), offline.Kvs[0].Value)
	require.Equal(t, put.Header.Revision, offline.Kvs[0].ModRevision)

	keysOnly, err := leased.Get(ctx, key, clientv3.WithKeysOnly())
	require.NoError(t, err)
	require.Len(t, keysOnly.Kvs, 1)
	require.Empty(t, keysOnly.Kvs[0].Value)
	countOnly, err := leased.Get(ctx, key, clientv3.WithCountOnly())
	require.NoError(t, err)
	require.Empty(t, countOnly.Kvs)
	require.Equal(t, int64(1), countOnly.Count)
	limited, err := leased.Get(ctx, key, clientv3.WithLimit(1))
	require.NoError(t, err)
	require.Len(t, limited.Kvs, 1)
	sorted, err := leased.Get(ctx, key, clientv3.WithSort(clientv3.SortByKey, clientv3.SortAscend))
	require.NoError(t, err)
	require.Len(t, sorted.Kvs, 1)
	require.Equal(t, []byte("offline"), sorted.Kvs[0].Value)
	minModFiltered, err := leased.Get(ctx, key, clientv3.WithMinModRev(put.Header.Revision+1))
	require.NoError(t, err)
	require.Empty(t, minModFiltered.Kvs)
	maxModFiltered, err := leased.Get(ctx, key, clientv3.WithMaxModRev(put.Header.Revision-1))
	require.NoError(t, err)
	require.Empty(t, maxModFiltered.Kvs)
	minCreateFiltered, err := leased.Get(ctx, key, clientv3.WithMinCreateRev(offline.Kvs[0].CreateRevision+1))
	require.NoError(t, err)
	require.Empty(t, minCreateFiltered.Kvs)
	maxCreateFiltered, err := leased.Get(ctx, key, clientv3.WithMaxCreateRev(offline.Kvs[0].CreateRevision-1))
	require.NoError(t, err)
	require.Empty(t, maxCreateFiltered.Kvs)
	serializable, err := leased.Get(ctx, key, clientv3.WithSerializable())
	require.NoError(t, err)
	require.Len(t, serializable.Kvs, 1)
	require.Equal(t, []byte("offline"), serializable.Kvs[0].Value)
}

func TestClientLeasingNestedNonOwnerTxnInvalidatesOwnerCache(t *testing.T) {
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
	ownerClient := newClient()
	writerClient := newClient()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	prefix := fmt.Sprintf("/a982/leasing-non-owner-nested/%d/", time.Now().UnixNano())
	keys := []string{prefix + "a", prefix + "b", prefix + "c"}
	ownerKV, closeOwner, err := leasing.NewKV(ownerClient, prefix+"owners/")
	require.NoError(t, err)
	t.Cleanup(closeOwner)
	writerKV, closeWriter, err := leasing.NewKV(writerClient, prefix+"owners/")
	require.NoError(t, err)
	t.Cleanup(closeWriter)

	for index, key := range keys {
		_, err = writerClient.Put(ctx, key, fmt.Sprintf("initial-%d", index))
		require.NoError(t, err)
		cached, getErr := ownerKV.Get(ctx, key)
		require.NoError(t, getErr)
		require.Len(t, cached.Kvs, 1)
		require.Equal(t, []byte(fmt.Sprintf("initial-%d", index)), cached.Kvs[0].Value)
	}

	watchCtx, watchCancel := context.WithTimeout(ctx, 5*time.Second)
	defer watchCancel()
	watch := writerClient.Watch(watchCtx, prefix, clientv3.WithPrefix())
	txn, err := writerKV.Txn(ctx).Then(
		clientv3.OpTxn(nil, []clientv3.Op{
			clientv3.OpPut(keys[1], "updated-1"),
		}, nil),
		clientv3.OpPut(keys[0], "updated-0"),
		clientv3.OpPut(keys[2], "updated-2"),
	).Commit()
	require.NoError(t, err)
	require.True(t, txn.Succeeded)
	require.Len(t, txn.Responses, 3)

	eventsAtRevision := 0
	for eventsAtRevision < len(keys) {
		select {
		case response, ok := <-watch:
			require.True(t, ok)
			require.NoError(t, response.Err())
			for _, event := range response.Events {
				if event.Kv.ModRevision == txn.Header.Revision {
					eventsAtRevision++
				}
			}
		case <-watchCtx.Done():
			t.Fatalf("timed out after %d/%d events at revision %d", eventsAtRevision, len(keys), txn.Header.Revision)
		}
	}

	for index, key := range keys {
		want := fmt.Sprintf("updated-%d", index)
		owner, getErr := ownerKV.Get(ctx, key)
		require.NoError(t, getErr)
		require.Len(t, owner.Kvs, 1)
		direct, getErr := writerClient.Get(ctx, key)
		require.NoError(t, getErr)
		require.Len(t, direct.Kvs, 1)
		require.Equal(t, []byte(want), owner.Kvs[0].Value)
		require.Equal(t, direct.Kvs[0], owner.Kvs[0])
		require.Equal(t, txn.Header.Revision, direct.Kvs[0].ModRevision)
	}
}

func TestClientLeasingRangeBoundsAndContention(t *testing.T) {
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
	prefix := fmt.Sprintf("/a983/leasing-range-contention/%d/", time.Now().UnixNano())
	ownerPrefix := prefix + "owners/"
	boundsReader, closeBoundsReader, err := leasing.NewKV(client, ownerPrefix)
	require.NoError(t, err)
	t.Cleanup(closeBoundsReader)
	boundsDeleter, closeBoundsDeleter, err := leasing.NewKV(client, ownerPrefix)
	require.NoError(t, err)
	t.Cleanup(closeBoundsDeleter)

	for _, key := range []string{prefix + "j", prefix + "m"} {
		_, err = client.Put(ctx, key, "bound")
		require.NoError(t, err)
		cached, getErr := boundsReader.Get(ctx, key)
		require.NoError(t, getErr)
		require.Len(t, cached.Kvs, 1)
	}
	_, err = client.Put(ctx, prefix+"k0", "delete-me")
	require.NoError(t, err)
	deleted, err := boundsDeleter.Delete(ctx, prefix+"k", clientv3.WithPrefix())
	require.NoError(t, err)
	require.Equal(t, int64(1), deleted.Deleted)
	for _, key := range []string{prefix + "j", prefix + "m"} {
		cached, getErr := boundsReader.Get(ctx, key)
		require.NoError(t, getErr)
		require.Len(t, cached.Kvs, 1)
		require.Equal(t, []byte("bound"), cached.Kvs[0].Value)
		owner, getErr := client.Get(ctx, ownerPrefix+key)
		require.NoError(t, getErr)
		require.Len(t, owner.Kvs, 1)
	}

	modes := []struct {
		name string
		op   func(string) clientv3.Op
	}{
		{
			name: "delete",
			op: func(dataPrefix string) clientv3.Op {
				return clientv3.OpDelete(dataPrefix, clientv3.WithPrefix())
			},
		},
		{
			name: "nested-txn-delete",
			op: func(dataPrefix string) clientv3.Op {
				return clientv3.OpTxn(
					nil,
					[]clientv3.Op{clientv3.OpDelete(dataPrefix, clientv3.WithPrefix())},
					nil,
				)
			},
		},
	}
	for modeIndex, mode := range modes {
		modePrefix := fmt.Sprintf("%scontend/%d/", prefix, modeIndex)
		deleter, closeDeleter, newErr := leasing.NewKV(client, ownerPrefix)
		require.NoError(t, newErr)
		t.Cleanup(closeDeleter)
		writer, closeWriter, newErr := leasing.NewKV(client, ownerPrefix)
		require.NoError(t, newErr)
		t.Cleanup(closeWriter)

		const keys = 4
		for index := 0; index < keys; index++ {
			key := fmt.Sprintf("%s%02d", modePrefix, index)
			_, err = client.Put(ctx, key, "initial")
			require.NoError(t, err)
			_, err = writer.Get(ctx, key)
			require.NoError(t, err)
		}

		stopWriter := make(chan struct{})
		writerDone := make(chan error, 1)
		var writerOperations atomic.Int64
		go func() {
			for iteration := 0; ; iteration++ {
				select {
				case <-stopWriter:
					writerDone <- nil
					return
				default:
				}
				key := fmt.Sprintf("%s%02d", modePrefix, iteration%keys)
				if _, putErr := writer.Put(ctx, key, fmt.Sprintf("writer-%d", iteration)); putErr != nil {
					writerDone <- putErr
					return
				}
				if _, getErr := writer.Get(ctx, key); getErr != nil {
					writerDone <- getErr
					return
				}
				writerOperations.Add(1)
				select {
				case <-stopWriter:
					writerDone <- nil
					return
				case <-ctx.Done():
					writerDone <- ctx.Err()
					return
				case <-time.After(20 * time.Millisecond):
				}
			}
		}()
		require.Eventually(t, func() bool {
			return writerOperations.Load() > 0
		}, 5*time.Second, time.Millisecond, mode.name)

		response, deleteErr := deleter.Do(ctx, mode.op(modePrefix))
		close(stopWriter)
		writerErr := <-writerDone
		require.NoError(t, deleteErr, mode.name)
		require.NoError(t, writerErr, mode.name)
		if mode.name == "delete" {
			require.NotNil(t, response.Del())
		} else {
			require.NotNil(t, response.Txn())
		}

		for index := 0; index < keys; index++ {
			key := fmt.Sprintf("%s%02d", modePrefix, index)
			cached, cachedErr := writer.Get(ctx, key)
			direct, directErr := client.Get(ctx, key)
			require.NoError(t, cachedErr)
			require.NoError(t, directErr)
			require.Equal(t, direct.Kvs, cached.Kvs, "%s key %q differs", mode.name, key)
			require.Equal(t, direct.Count, cached.Count, "%s key %q count differs", mode.name, key)
		}
	}
}

func TestClientLeasingSessionExpiryRefreshesOwnerCache(t *testing.T) {
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

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	prefix := fmt.Sprintf("/a984/leasing-session/%d/", time.Now().UnixNano())
	key := prefix + "data"
	ownerPrefix := prefix + "owners/"
	firstKV, closeFirst, err := leasing.NewKV(first, ownerPrefix, concurrency.WithTTL(2))
	require.NoError(t, err)
	t.Cleanup(closeFirst)
	secondKV, closeSecond, err := leasing.NewKV(second, ownerPrefix, concurrency.WithTTL(2))
	require.NoError(t, err)
	t.Cleanup(closeSecond)

	_, err = first.Put(ctx, key, "old")
	require.NoError(t, err)
	initial, err := firstKV.Get(ctx, key)
	require.NoError(t, err)
	require.Len(t, initial.Kvs, 1)
	require.Equal(t, []byte("old"), initial.Kvs[0].Value)

	owners, err := first.Get(ctx, ownerPrefix, clientv3.WithPrefix())
	require.NoError(t, err)
	require.Len(t, owners.Kvs, 1)
	oldLease := clientv3.LeaseID(owners.Kvs[0].Lease)
	require.NotZero(t, oldLease)
	_, err = second.Revoke(ctx, oldLease)
	require.NoError(t, err)
	require.Eventually(t, func() bool {
		response, getErr := first.Get(ctx, ownerPrefix, clientv3.WithPrefix())
		return getErr == nil && len(response.Kvs) == 0
	}, 5*time.Second, 20*time.Millisecond)

	_, err = secondKV.Put(ctx, key, "new")
	require.NoError(t, err)
	require.Eventually(t, func() bool {
		response, getErr := firstKV.Get(ctx, key)
		return getErr == nil && len(response.Kvs) == 1 && string(response.Kvs[0].Value) == "new"
	}, 10*time.Second, 20*time.Millisecond)
	require.Eventually(t, func() bool {
		_, getErr := firstKV.Get(ctx, key)
		if getErr != nil {
			return false
		}
		newOwners, getErr := first.Get(ctx, ownerPrefix, clientv3.WithPrefix())
		if getErr != nil {
			return false
		}
		for _, owner := range newOwners.Kvs {
			leaseID := clientv3.LeaseID(owner.Lease)
			if leaseID != 0 && leaseID != oldLease {
				return true
			}
		}
		return false
	}, 5*time.Second, 20*time.Millisecond)
}

func TestClientLeasingPutGetDeleteConcurrentProgress(t *testing.T) {
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

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	prefix := fmt.Sprintf("/a985/leasing-put-get-delete/%d/", time.Now().UnixNano())
	key := prefix + "data"
	ownerPrefix := prefix + "owners/"

	const (
		clients = 6
		workers = 6
	)
	leased := make([]clientv3.KV, clients)
	for index := range leased {
		kv, closeKV, newErr := leasing.NewKV(client, ownerPrefix)
		require.NoError(t, newErr)
		t.Cleanup(closeKV)
		leased[index] = kv
	}

	start := make(chan struct{})
	errs := make(chan error, workers)
	var completed atomic.Int64
	var wait sync.WaitGroup
	for worker := 0; worker < workers; worker++ {
		wait.Add(1)
		go func(worker int) {
			defer wait.Done()
			<-start
			for clientIndex, kv := range leased {
				value := fmt.Sprintf("worker-%02d-client-%02d", worker, clientIndex)
				if _, putErr := kv.Put(ctx, key, value); putErr != nil {
					errs <- putErr
					return
				}
				if _, getErr := kv.Get(ctx, key); getErr != nil {
					errs <- getErr
					return
				}
				if _, deleteErr := kv.Delete(ctx, key); deleteErr != nil {
					errs <- deleteErr
					return
				}
				completed.Add(1)
			}
		}(worker)
	}
	close(start)
	wait.Wait()
	close(errs)
	for operationErr := range errs {
		require.NoError(t, operationErr)
	}
	require.Equal(t, int64(clients*workers), completed.Load())

	leasedFinal, err := leased[0].Get(ctx, key)
	require.NoError(t, err)
	directFinal, err := client.Get(ctx, key)
	require.NoError(t, err)
	require.Empty(t, leasedFinal.Kvs)
	require.Empty(t, directFinal.Kvs)
}

func TestClientLeasingAtomicTxnCacheStaysConsistent(t *testing.T) {
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

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	prefix := fmt.Sprintf("/a986/leasing-atomic-cache/%d/", time.Now().UnixNano())
	dataPrefix := prefix + "data/"
	leased, closeLeased, err := leasing.NewKV(client, prefix+"owners/")
	require.NoError(t, err)
	t.Cleanup(closeLeased)

	const (
		keyCount    = 4
		writerCount = 2
		readerCount = 2
		iterations  = 4
	)
	keys := make([]string, keyCount)
	initialPuts := make([]clientv3.Op, keyCount)
	gets := make([]clientv3.Op, keyCount)
	for index := range keys {
		keys[index] = fmt.Sprintf("%s%02d", dataPrefix, index)
		initialPuts[index] = clientv3.OpPut(keys[index], "generation-0")
		gets[index] = clientv3.OpGet(keys[index])
	}
	_, err = client.Txn(ctx).Then(initialPuts...).Commit()
	require.NoError(t, err)
	for _, get := range gets {
		_, err = leased.Do(ctx, get)
		require.NoError(t, err)
	}

	start := make(chan struct{})
	writersDone := make(chan struct{})
	errs := make(chan error, writerCount+readerCount)
	var writerTransactions atomic.Int64
	var readerTransactions atomic.Int64
	var mixedRevisionReads atomic.Int64
	var writers sync.WaitGroup
	var readers sync.WaitGroup
	for writer := 0; writer < writerCount; writer++ {
		writers.Add(1)
		go func(writer int) {
			defer writers.Done()
			<-start
			for iteration := 0; iteration < iterations; iteration++ {
				generation := fmt.Sprintf("writer-%d-generation-%d", writer, iteration)
				puts := make([]clientv3.Op, keyCount)
				for index := range keys {
					puts[index] = clientv3.OpPut(keys[index], generation)
				}
				if _, commitErr := leased.Txn(ctx).Then(puts...).Commit(); commitErr != nil {
					errs <- commitErr
					return
				}
				writerTransactions.Add(1)
			}
		}(writer)
	}
	for reader := 0; reader < readerCount; reader++ {
		readers.Add(1)
		go func() {
			defer readers.Done()
			<-start
			for {
				response, commitErr := leased.Txn(ctx).Then(gets...).Commit()
				if commitErr != nil {
					errs <- commitErr
					return
				}
				readerTransactions.Add(1)
				if !clientLeasingTxnResponseHasSingleRevision(response, keyCount) {
					mixedRevisionReads.Add(1)
				}
				select {
				case <-writersDone:
					return
				default:
				}
			}
		}()
	}
	close(start)
	writers.Wait()
	close(writersDone)
	readers.Wait()
	close(errs)
	for runErr := range errs {
		require.NoError(t, runErr)
	}
	require.Equal(t, int64(writerCount*iterations), writerTransactions.Load())
	require.Positive(t, readerTransactions.Load())
	require.Zero(t, mixedRevisionReads.Load())

	final, err := leased.Txn(ctx).Then(gets...).Commit()
	require.NoError(t, err)
	require.True(t, clientLeasingTxnResponseHasSingleRevision(final, keyCount))
	var finalValue string
	for index, response := range final.Responses {
		kvs := response.GetResponseRange().Kvs
		require.Len(t, kvs, 1)
		if index == 0 {
			finalValue = string(kvs[0].Value)
			continue
		}
		require.Equal(t, finalValue, string(kvs[0].Value))
	}
}

func TestClientLeasingMutationFormsRefreshOwnerCache(t *testing.T) {
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

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	prefix := fmt.Sprintf("/a987/leasing-mutations/%d/", time.Now().UnixNano())
	leased, closeLeased, err := leasing.NewKV(client, prefix+"owners/")
	require.NoError(t, err)
	t.Cleanup(closeLeased)

	cases := []struct {
		name     string
		expected string
		apply    func(context.Context, clientv3.KV, string, string)
	}{
		{
			name: "delete",
			apply: func(callCtx context.Context, kv clientv3.KV, key, _ string) {
				response, callErr := kv.Delete(callCtx, key)
				require.NoError(t, callErr)
				require.Equal(t, int64(1), response.Deleted)
			},
		},
		{
			name:     "txn-put",
			expected: "txn-put-applied",
			apply: func(callCtx context.Context, kv clientv3.KV, key, expected string) {
				response, callErr := kv.Txn(callCtx).Then(
					clientv3.OpGet(key),
					clientv3.OpPut(key, expected),
				).Commit()
				require.NoError(t, callErr)
				require.True(t, response.Succeeded)
				require.Len(t, response.Responses, 2)
			},
		},
		{
			name: "txn-delete",
			apply: func(callCtx context.Context, kv clientv3.KV, key, _ string) {
				response, callErr := kv.Txn(callCtx).Then(
					clientv3.OpGet(key),
					clientv3.OpDelete(key),
				).Commit()
				require.NoError(t, callErr)
				require.True(t, response.Succeeded)
				require.Len(t, response.Responses, 2)
				require.Equal(t, int64(1), response.Responses[1].GetResponseDeleteRange().Deleted)
			},
		},
		{
			name:     "do-put",
			expected: "do-put-applied",
			apply: func(callCtx context.Context, kv clientv3.KV, key, expected string) {
				response, callErr := kv.Do(callCtx, clientv3.OpPut(key, expected))
				require.NoError(t, callErr)
				require.NotNil(t, response.Put())
			},
		},
		{
			name: "do-delete",
			apply: func(callCtx context.Context, kv clientv3.KV, key, _ string) {
				response, callErr := kv.Do(callCtx, clientv3.OpDelete(key))
				require.NoError(t, callErr)
				require.NotNil(t, response.Del())
				require.Equal(t, int64(1), response.Del().Deleted)
			},
		},
	}
	for index, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			key := fmt.Sprintf("%sdata/%02d", prefix, index)
			_, err = leased.Put(ctx, key, "initial")
			require.NoError(t, err)
			initial, err := leased.Get(ctx, key)
			require.NoError(t, err)
			require.Len(t, initial.Kvs, 1)

			testCase.apply(ctx, leased, key, testCase.expected)
			direct, err := client.Get(ctx, key)
			require.NoError(t, err)
			cached, err := leased.Get(ctx, key)
			require.NoError(t, err)
			require.True(t, clientLeasingRangeResponsesEqual(cached, direct), testCase.name)
			if testCase.expected == "" {
				require.Empty(t, cached.Kvs)
				return
			}
			require.Len(t, cached.Kvs, 1)
			require.Equal(t, testCase.expected, string(cached.Kvs[0].Value))
		})
	}
}

func TestClientLeasingAmbiguousMutationsConvergeAfterResponseLoss(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()

	grpcServer := grpc.NewServer(server.ClientServerOptions()...)
	etcdserverpb.RegisterKVServer(grpcServer, server)
	etcdserverpb.RegisterWatchServer(grpcServer, server)
	etcdserverpb.RegisterLeaseServer(grpcServer, server)
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	go func() { _ = grpcServer.Serve(listener) }()
	t.Cleanup(func() {
		grpcServer.Stop()
		_ = listener.Close()
	})

	directEndpoint := listener.Addr().String()
	bridge := newClientLeasingTCPBridge(t, directEndpoint)
	newClient := func(endpoint string) *clientv3.Client {
		client, newErr := clientv3.New(clientv3.Config{
			Endpoints:   []string{endpoint},
			DialTimeout: time.Second,
		})
		require.NoError(t, newErr)
		t.Cleanup(func() { require.NoError(t, client.Close()) })
		return client
	}
	owner := newClient(bridge.Endpoint())
	direct := newClient(directEndpoint)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	prefix := fmt.Sprintf("/a1073/leasing-ambiguous-mutations/%d/", time.Now().UnixNano())
	leased, closeLeased, err := leasing.NewKV(owner, prefix+"owners/")
	require.NoError(t, err)
	t.Cleanup(closeLeased)

	cases := []struct {
		name     string
		expected string
		apply    func(context.Context, clientv3.KV, string, string) error
	}{
		{
			name: "delete",
			apply: func(callCtx context.Context, kv clientv3.KV, key, _ string) error {
				_, callErr := kv.Delete(callCtx, key)
				return callErr
			},
		},
		{
			name:     "txn-put",
			expected: "txn-put-applied",
			apply: func(callCtx context.Context, kv clientv3.KV, key, expected string) error {
				_, callErr := kv.Txn(callCtx).Then(
					clientv3.OpGet(key),
					clientv3.OpPut(key, expected),
				).Commit()
				return callErr
			},
		},
		{
			name: "txn-delete",
			apply: func(callCtx context.Context, kv clientv3.KV, key, _ string) error {
				_, callErr := kv.Txn(callCtx).Then(
					clientv3.OpGet(key),
					clientv3.OpDelete(key),
				).Commit()
				return callErr
			},
		},
		{
			name:     "do-put",
			expected: "do-put-applied",
			apply: func(callCtx context.Context, kv clientv3.KV, key, expected string) error {
				_, callErr := kv.Do(callCtx, clientv3.OpPut(key, expected))
				return callErr
			},
		},
		{
			name: "do-delete",
			apply: func(callCtx context.Context, kv clientv3.KV, key, _ string) error {
				_, callErr := kv.Do(callCtx, clientv3.OpDelete(key))
				return callErr
			},
		},
	}
	for index, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			key := fmt.Sprintf("%sdata/%02d", prefix, index)
			_, err = leased.Put(ctx, key, "initial")
			require.NoError(t, err)
			_, err = leased.Get(ctx, key)
			require.NoError(t, err)

			droppedBefore := bridge.DroppedBytes()
			bridge.BlackholeResponses()
			callCtx, callCancel := context.WithTimeout(ctx, 750*time.Millisecond)
			callDone := make(chan error, 1)
			go func() {
				callDone <- testCase.apply(callCtx, leased, key, testCase.expected)
			}()

			require.Eventually(t, func() bool {
				response, getErr := direct.Get(ctx, key)
				if getErr != nil {
					return false
				}
				if testCase.expected == "" {
					return len(response.Kvs) == 0
				}
				return len(response.Kvs) == 1 && string(response.Kvs[0].Value) == testCase.expected
			}, 5*time.Second, 10*time.Millisecond)
			require.Eventually(t, func() bool {
				return bridge.DroppedBytes() > droppedBefore
			}, 5*time.Second, 10*time.Millisecond)

			var callErr error
			select {
			case callErr = <-callDone:
			case <-ctx.Done():
				t.Fatalf("ambiguous leasing mutation did not observe caller deadline: %v", ctx.Err())
			}
			callCancel()
			require.True(t,
				errors.Is(callErr, context.DeadlineExceeded) ||
					status.Code(callErr) == codes.DeadlineExceeded,
				"unexpected ambiguous mutation error: %v", callErr)
			bridge.Unblackhole()

			require.Eventually(t, func() bool {
				cached, cacheErr := leased.Get(ctx, key)
				directResponse, directErr := direct.Get(ctx, key)
				return cacheErr == nil &&
					directErr == nil &&
					clientLeasingRangeResponsesEqual(cached, directResponse)
			}, 10*time.Second, 20*time.Millisecond)
		})
	}
}

func TestClientLeasingReconnectOperationsMatchDirectKV(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()

	grpcServer := grpc.NewServer(server.ClientServerOptions()...)
	etcdserverpb.RegisterKVServer(grpcServer, server)
	etcdserverpb.RegisterWatchServer(grpcServer, server)
	etcdserverpb.RegisterLeaseServer(grpcServer, server)
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	go func() { _ = grpcServer.Serve(listener) }()
	t.Cleanup(func() {
		grpcServer.Stop()
		_ = listener.Close()
	})

	directEndpoint := listener.Addr().String()
	bridge := newClientLeasingTCPBridge(t, directEndpoint)
	newClient := func(endpoint string) *clientv3.Client {
		client, newErr := clientv3.New(clientv3.Config{
			Endpoints:   []string{endpoint},
			DialTimeout: time.Second,
		})
		require.NoError(t, newErr)
		t.Cleanup(func() { require.NoError(t, client.Close()) })
		return client
	}
	throughBridge := newClient(bridge.Endpoint())
	direct := newClient(directEndpoint)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	prefix := fmt.Sprintf("/a1074/leasing-reconnect-operations/%d/", time.Now().UnixNano())
	dataPrefix := prefix + "data/"
	leased, closeLeased, err := leasing.NewKV(throughBridge, prefix+"owners/")
	require.NoError(t, err)
	t.Cleanup(closeLeased)

	missingKey := dataPrefix + "txn-missing"
	_, err = leased.Get(ctx, missingKey)
	require.NoError(t, err)
	txnCtx, txnCancel := context.WithTimeout(ctx, 5*time.Second)
	txnDropsBefore := bridge.DroppedConnections()
	txnChurn := clientLeasingChurnTCPBridge(bridge, 5, 10*time.Millisecond)
	<-txnChurn.started
	txn, err := leased.Txn(txnCtx).
		If(clientv3.Compare(clientv3.Version(missingKey), "=", 0)).
		Then(clientv3.OpGet(missingKey)).
		Commit()
	txnCancel()
	<-txnChurn.done
	require.Greater(t, bridge.DroppedConnections(), txnDropsBefore)
	require.NoError(t, err)
	require.True(t, txn.Succeeded)
	require.Len(t, txn.Responses, 1)
	require.Empty(t, txn.Responses[0].GetResponseRange().Kvs)

	const keys = 8
	for index := 0; index < keys; index += 2 {
		key := fmt.Sprintf("%s%02d", dataPrefix, index)
		_, err = leased.Put(ctx, key, fmt.Sprintf("value-%02d", index))
		require.NoError(t, err)
	}
	for index := 0; index < keys; index++ {
		key := fmt.Sprintf("%s%02d", dataPrefix, index)
		getCtx, getCancel := context.WithTimeout(ctx, 5*time.Second)
		getDropsBefore := bridge.DroppedConnections()
		getChurn := clientLeasingChurnTCPBridge(bridge, 3, 10*time.Millisecond)
		<-getChurn.started
		leasedResponse, leasedErr := leased.Get(getCtx, key)
		getCancel()
		<-getChurn.done
		require.Greater(t, bridge.DroppedConnections(), getDropsBefore)
		require.NoError(t, leasedErr)
		directResponse, directErr := direct.Get(ctx, key)
		require.NoError(t, directErr)
		require.True(t,
			clientLeasingRangeResponsesEqual(leasedResponse, directResponse),
			"key %q differs after reconnect", key)
	}
}

func TestClientLeasingCachedComparisonsWorkOffline(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()

	grpcServer := grpc.NewServer(server.ClientServerOptions()...)
	etcdserverpb.RegisterKVServer(grpcServer, server)
	etcdserverpb.RegisterWatchServer(grpcServer, server)
	etcdserverpb.RegisterLeaseServer(grpcServer, server)
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	go func() { _ = grpcServer.Serve(listener) }()
	t.Cleanup(func() {
		grpcServer.Stop()
		_ = listener.Close()
	})

	directEndpoint := listener.Addr().String()
	bridge := newClientLeasingTCPBridge(t, directEndpoint)
	owner, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{bridge.Endpoint()},
		DialTimeout: time.Second,
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, owner.Close()) })
	direct, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{directEndpoint},
		DialTimeout: time.Second,
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, direct.Close()) })

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	prefix := fmt.Sprintf("/a1076/leasing-offline-compare/%d/", time.Now().UnixNano())
	leased, closeLeased, err := leasing.NewKV(owner, prefix+"owners/")
	require.NoError(t, err)
	t.Cleanup(closeLeased)

	compareKey := prefix + "compare"
	_, err = direct.Put(ctx, compareKey, "abc")
	require.NoError(t, err)
	cached, err := leased.Get(ctx, compareKey)
	require.NoError(t, err)
	require.Len(t, cached.Kvs, 1)
	cachedKV := cached.Kvs[0]

	bridge.Blackhole()
	defer bridge.Unblackhole()
	for index, testCase := range []struct {
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
	} {
		compareCtx, compareCancel := context.WithTimeout(ctx, time.Second)
		response, compareErr := leased.Txn(compareCtx).
			If(testCase.compare).
			Then(clientv3.OpGet(compareKey)).
			Commit()
		compareCancel()
		require.NoError(t, compareErr, "comparison %d", index)
		require.Equal(t, testCase.want, response.Succeeded, "comparison %d", index)
		expectedResponses := 0
		if testCase.want {
			expectedResponses = 1
		}
		require.Len(t, response.Responses, expectedResponses, "comparison %d", index)
	}
}

func clientLeasingRangeResponsesEqual(left, right *clientv3.GetResponse) bool {
	if len(left.Kvs) != len(right.Kvs) {
		return false
	}
	for index := range left.Kvs {
		leftKV := left.Kvs[index]
		rightKV := right.Kvs[index]
		if string(leftKV.Key) != string(rightKV.Key) ||
			string(leftKV.Value) != string(rightKV.Value) ||
			leftKV.CreateRevision != rightKV.CreateRevision ||
			leftKV.ModRevision != rightKV.ModRevision ||
			leftKV.Version != rightKV.Version ||
			leftKV.Lease != rightKV.Lease {
			return false
		}
	}
	return true
}

func clientLeasingTxnResponseHasSingleRevision(response *clientv3.TxnResponse, expected int) bool {
	if len(response.Responses) != expected {
		return false
	}
	var revision int64
	for index, operation := range response.Responses {
		kvs := operation.GetResponseRange().Kvs
		if len(kvs) != 1 {
			return false
		}
		if index == 0 {
			revision = kvs[0].ModRevision
			continue
		}
		if kvs[0].ModRevision != revision {
			return false
		}
	}
	return revision > 0
}

type clientLeasingTCPBridge struct {
	listener     net.Listener
	target       string
	blackhole    atomic.Int32
	droppedBytes atomic.Int64
	droppedConns atomic.Int64
	closed       atomic.Bool
	mu           sync.Mutex
	conns        map[net.Conn]struct{}
	acceptWG     sync.WaitGroup
	connWG       sync.WaitGroup
}

func newClientLeasingTCPBridge(t *testing.T, target string) *clientLeasingTCPBridge {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	bridge := &clientLeasingTCPBridge{
		listener: listener,
		target:   target,
		conns:    make(map[net.Conn]struct{}),
	}
	bridge.acceptWG.Add(1)
	go bridge.accept()
	t.Cleanup(bridge.Close)
	return bridge
}

func (b *clientLeasingTCPBridge) Endpoint() string {
	return b.listener.Addr().String()
}

func (b *clientLeasingTCPBridge) BlackholeResponses() {
	b.blackhole.Store(1)
}

func (b *clientLeasingTCPBridge) Blackhole() {
	b.blackhole.Store(2)
}

func (b *clientLeasingTCPBridge) Unblackhole() {
	b.blackhole.Store(0)
	b.dropConnections()
}

func (b *clientLeasingTCPBridge) DroppedBytes() int64 {
	return b.droppedBytes.Load()
}

func (b *clientLeasingTCPBridge) DropConnections() {
	b.dropConnections()
}

func (b *clientLeasingTCPBridge) DroppedConnections() int64 {
	return b.droppedConns.Load()
}

func (b *clientLeasingTCPBridge) Close() {
	if !b.closed.CompareAndSwap(false, true) {
		return
	}
	_ = b.listener.Close()
	b.acceptWG.Wait()
	b.dropConnections()
	b.connWG.Wait()
}

func (b *clientLeasingTCPBridge) accept() {
	defer b.acceptWG.Done()
	for {
		inbound, err := b.listener.Accept()
		if err != nil {
			return
		}
		outbound, err := net.DialTimeout("tcp", b.target, 3*time.Second)
		if err != nil {
			_ = inbound.Close()
			continue
		}
		if b.closed.Load() {
			_ = inbound.Close()
			_ = outbound.Close()
			return
		}
		b.track(inbound)
		b.track(outbound)
		b.connWG.Add(1)
		go b.forwardPair(inbound, outbound)
	}
}

func (b *clientLeasingTCPBridge) forwardPair(inbound, outbound net.Conn) {
	defer b.connWG.Done()
	var copies sync.WaitGroup
	copies.Add(2)
	go func() {
		defer copies.Done()
		b.copy(outbound, inbound, false)
	}()
	go func() {
		defer copies.Done()
		b.copy(inbound, outbound, true)
	}()
	copies.Wait()
	_ = inbound.Close()
	_ = outbound.Close()
	b.untrack(inbound)
	b.untrack(outbound)
}

func (b *clientLeasingTCPBridge) copy(destination, source net.Conn, response bool) {
	buffer := make([]byte, 32*1024)
	for {
		read, err := source.Read(buffer)
		if read > 0 {
			mode := b.blackhole.Load()
			if mode == 2 || (mode == 1 && response) {
				b.droppedBytes.Add(int64(read))
			} else if _, writeErr := destination.Write(buffer[:read]); writeErr != nil {
				return
			}
		}
		if err != nil {
			return
		}
	}
}

func (b *clientLeasingTCPBridge) track(connection net.Conn) {
	b.mu.Lock()
	b.conns[connection] = struct{}{}
	b.mu.Unlock()
}

func (b *clientLeasingTCPBridge) untrack(connection net.Conn) {
	b.mu.Lock()
	delete(b.conns, connection)
	b.mu.Unlock()
}

func (b *clientLeasingTCPBridge) dropConnections() {
	b.mu.Lock()
	connections := make([]net.Conn, 0, len(b.conns))
	for connection := range b.conns {
		connections = append(connections, connection)
	}
	b.mu.Unlock()
	b.droppedConns.Add(int64(len(connections)))
	for _, connection := range connections {
		_ = connection.Close()
	}
}

type clientLeasingTCPBridgeChurn struct {
	started <-chan struct{}
	done    <-chan struct{}
}

func clientLeasingChurnTCPBridge(
	bridge *clientLeasingTCPBridge,
	drops int,
	interval time.Duration,
) clientLeasingTCPBridgeChurn {
	started := make(chan struct{})
	done := make(chan struct{})
	go func() {
		defer close(done)
		for drop := 0; drop < drops; drop++ {
			bridge.DropConnections()
			if drop == 0 {
				close(started)
			}
			time.Sleep(interval)
		}
	}()
	return clientLeasingTCPBridgeChurn{started: started, done: done}
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
