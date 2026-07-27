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
	"math"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/mvccpb"
	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/namespace"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
)

func TestClientGetCanceledAfterResponseLossKeepsConnectionUsable(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()

	grpcServer := grpc.NewServer(server.ClientServerOptions()...)
	server.Register(grpcServer)
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	go func() { _ = grpcServer.Serve(listener) }()
	t.Cleanup(func() {
		grpcServer.Stop()
		_ = listener.Close()
	})

	bridge := newClientLeasingTCPBridge(t, listener.Addr().String())
	client, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{bridge.Endpoint()},
		DialTimeout: time.Second,
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, client.Close()) })

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	key := fmt.Sprintf("/a1080/get-cancel-connection/%d/key", time.Now().UnixNano())
	_, err = client.Put(ctx, key, "seed")
	require.NoError(t, err)
	activeConnection := client.ActiveConnection()

	const attempts = 3
	for attempt := 0; attempt < attempts; attempt++ {
		droppedBytesBefore := bridge.DroppedBytes()
		droppedConnsBefore := bridge.DroppedConnections()
		bridge.BlackholeResponses()

		callCtx, callCancel := context.WithCancel(ctx)
		result := make(chan error, 1)
		go func() {
			_, getErr := client.Get(callCtx, key)
			result <- getErr
		}()
		require.Eventually(t, func() bool {
			return bridge.DroppedBytes() > droppedBytesBefore
		}, 2*time.Second, 10*time.Millisecond)
		callCancel()
		getErr := <-result
		require.ErrorIs(t, getErr, context.Canceled)

		bridge.Resume()
		require.True(t, client.ActiveConnection() == activeConnection,
			"attempt %d replaced the active gRPC connection", attempt)
		require.Equal(t, droppedConnsBefore, bridge.DroppedConnections(),
			"attempt %d dropped the TCP transport", attempt)

		value := fmt.Sprintf("after-cancel-%d", attempt)
		followUpCtx, followUpCancel := context.WithTimeout(ctx, 5*time.Second)
		_, putErr := client.Put(followUpCtx, key, value)
		read, readErr := client.Get(followUpCtx, key)
		followUpCancel()
		require.NoError(t, putErr, "attempt %d follow-up Put failed", attempt)
		require.NoError(t, readErr, "attempt %d follow-up Get failed", attempt)
		require.Len(t, read.Kvs, 1)
		require.Equal(t, value, string(read.Kvs[0].Value))
	}
}

func TestClientGetValidationErrorsMatchEtcd(t *testing.T) {
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
	_, err = client.Get(ctx, "/a1446/range/invalid-sort",
		clientv3.WithSort(clientv3.SortTarget(99), clientv3.SortOrder(99)))
	requireClientKVError(t, err, rpctypes.ErrInvalidSortOption, codes.Unknown, "etcdserver: invalid sort option")

	_, err = client.Get(ctx, "",
		clientv3.WithSort(clientv3.SortTarget(99), clientv3.SortOrder(99)))
	requireClientKVError(t, err, rpctypes.ErrInvalidSortOption, codes.Unknown, "etcdserver: invalid sort option")
}

func TestClientNamespaceGetValidationErrorsMatchEtcd(t *testing.T) {
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
	namespacedKV := namespace.NewKV(client.KV, "/a1448/namespace/")
	invalidSort := []clientv3.OpOption{clientv3.WithSort(clientv3.SortTarget(99), clientv3.SortOrder(99))}

	_, err = namespacedKV.Get(ctx, "", invalidSort...)
	requireClientKVError(t, err, rpctypes.ErrEmptyKey, codes.Unknown, "etcdserver: key is not provided")
	_, err = namespacedKV.Get(ctx, "", append(invalidSort, clientv3.WithPrefix())...)
	requireClientKVError(t, err, rpctypes.ErrInvalidSortOption, codes.Unknown, "etcdserver: invalid sort option")
	_, err = namespacedKV.Get(ctx, "key", invalidSort...)
	requireClientKVError(t, err, rpctypes.ErrInvalidSortOption, codes.Unknown, "etcdserver: invalid sort option")
}

func TestClientNamespaceGetCountOnlyKeysOnlyCountsTenantKeys(t *testing.T) {
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
	tenantPrefix := "/a1461/namespace-count/tenant/"
	namespacedKV := namespace.NewKV(client.KV, tenantPrefix)
	for _, key := range []string{"a", "b", "c", "c"} {
		_, err = namespacedKV.Put(ctx, key, "value-"+key)
		require.NoError(t, err)
	}
	_, err = client.Put(ctx, "/a1461/namespace-count/tenant0/outside", "outside")
	require.NoError(t, err)

	resp, err := namespacedKV.Get(ctx, "", clientv3.WithFromKey(), clientv3.WithCountOnly(), clientv3.WithKeysOnly())
	require.NoError(t, err)
	require.NotNil(t, resp.Header)
	require.Equal(t, int64(3), resp.Count)
	require.Empty(t, resp.Kvs)
	require.False(t, resp.More)

	doResp, err := namespacedKV.Do(ctx, clientv3.OpGet("", clientv3.WithFromKey(), clientv3.WithCountOnly(), clientv3.WithKeysOnly()))
	require.NoError(t, err)
	doGet := doResp.Get()
	require.NotNil(t, doGet)
	require.NotNil(t, doGet.Header)
	require.Equal(t, int64(3), doGet.Count)
	require.Empty(t, doGet.Kvs)
	require.False(t, doGet.More)

	txnResp, err := namespacedKV.Txn(ctx).
		Then(clientv3.OpGet("", clientv3.WithFromKey(), clientv3.WithCountOnly(), clientv3.WithKeysOnly())).
		Commit()
	require.NoError(t, err)
	require.True(t, txnResp.Succeeded)
	require.Len(t, txnResp.Responses, 1)
	txnGet := txnResp.Responses[0].GetResponseRange()
	require.NotNil(t, txnGet)
	require.NotNil(t, txnGet.Header)
	require.Equal(t, int64(3), txnGet.Count)
	require.Empty(t, txnGet.Kvs)
	require.False(t, txnGet.More)

	visible, err := namespacedKV.Get(ctx, "", clientv3.WithFromKey(), clientv3.WithSort(clientv3.SortByKey, clientv3.SortAscend))
	require.NoError(t, err)
	require.Equal(t, int64(3), visible.Count)
	require.Equal(t, [][]byte{[]byte("a"), []byte("b"), []byte("c")}, [][]byte{
		visible.Kvs[0].Key,
		visible.Kvs[1].Key,
		visible.Kvs[2].Key,
	})
}

func TestClientNamespaceGetKeysOnlyLimitReturnsLogicalKeys(t *testing.T) {
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
	tenantPrefix := "/a1464/namespace-keys-only/tenant/"
	namespacedKV := namespace.NewKV(client.KV, tenantPrefix)
	for _, key := range []string{"a", "b", "c"} {
		_, err = namespacedKV.Put(ctx, key, "value-"+key)
		require.NoError(t, err)
	}
	_, err = client.Put(ctx, "/a1464/namespace-keys-only/tenant0/outside", "outside")
	require.NoError(t, err)

	resp, err := namespacedKV.Get(ctx, "",
		clientv3.WithFromKey(), clientv3.WithKeysOnly(), clientv3.WithLimit(2),
		clientv3.WithSort(clientv3.SortByKey, clientv3.SortAscend))
	require.NoError(t, err)
	require.NotNil(t, resp.Header)
	require.Equal(t, int64(3), resp.Count)
	require.True(t, resp.More)
	require.Len(t, resp.Kvs, 2)
	require.Equal(t, [][]byte{[]byte("a"), []byte("b")}, [][]byte{resp.Kvs[0].Key, resp.Kvs[1].Key})
	require.Empty(t, resp.Kvs[0].Value)
	require.Empty(t, resp.Kvs[1].Value)

	doResp, err := namespacedKV.Do(ctx, clientv3.OpGet("",
		clientv3.WithFromKey(), clientv3.WithKeysOnly(), clientv3.WithLimit(2),
		clientv3.WithSort(clientv3.SortByKey, clientv3.SortAscend)))
	require.NoError(t, err)
	doGet := doResp.Get()
	require.NotNil(t, doGet)
	require.NotNil(t, doGet.Header)
	require.Equal(t, int64(3), doGet.Count)
	require.True(t, doGet.More)
	require.Len(t, doGet.Kvs, 2)
	require.Equal(t, [][]byte{[]byte("a"), []byte("b")}, [][]byte{doGet.Kvs[0].Key, doGet.Kvs[1].Key})
	require.Empty(t, doGet.Kvs[0].Value)
	require.Empty(t, doGet.Kvs[1].Value)

	txnResp, err := namespacedKV.Txn(ctx).
		Then(clientv3.OpGet("",
			clientv3.WithFromKey(), clientv3.WithKeysOnly(), clientv3.WithLimit(2),
			clientv3.WithSort(clientv3.SortByKey, clientv3.SortAscend))).
		Commit()
	require.NoError(t, err)
	require.True(t, txnResp.Succeeded)
	require.Len(t, txnResp.Responses, 1)
	txnGet := txnResp.Responses[0].GetResponseRange()
	require.NotNil(t, txnGet)
	require.NotNil(t, txnGet.Header)
	require.Equal(t, int64(3), txnGet.Count)
	require.True(t, txnGet.More)
	require.Len(t, txnGet.Kvs, 2)
	require.Equal(t, [][]byte{[]byte("a"), []byte("b")}, [][]byte{txnGet.Kvs[0].Key, txnGet.Kvs[1].Key})
	require.Empty(t, txnGet.Kvs[0].Value)
	require.Empty(t, txnGet.Kvs[1].Value)
}

func TestClientNamespaceGetModRevisionFilterReturnsLogicalKeys(t *testing.T) {
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
	tenantPrefix := "/a1467/namespace-mod-filter/tenant/"
	namespacedKV := namespace.NewKV(client.KV, tenantPrefix)
	for _, key := range []string{"a", "b", "c"} {
		_, err = namespacedKV.Put(ctx, key, "old-"+key)
		require.NoError(t, err)
	}
	updateB, err := namespacedKV.Put(ctx, "b", "new-b")
	require.NoError(t, err)
	_, err = client.Put(ctx, "/a1467/namespace-mod-filter/tenant0/outside", "outside")
	require.NoError(t, err)

	resp, err := namespacedKV.Get(ctx, "", clientv3.WithFromKey(),
		clientv3.WithMinModRev(updateB.Header.Revision),
		clientv3.WithSort(clientv3.SortByKey, clientv3.SortAscend))
	require.NoError(t, err)
	require.NotNil(t, resp.Header)
	require.Equal(t, int64(3), resp.Count)
	require.False(t, resp.More)
	require.Len(t, resp.Kvs, 1)
	require.Equal(t, []byte("b"), resp.Kvs[0].Key)
	require.Equal(t, []byte("new-b"), resp.Kvs[0].Value)

	doResp, err := namespacedKV.Do(ctx, clientv3.OpGet("", clientv3.WithFromKey(),
		clientv3.WithMinModRev(updateB.Header.Revision),
		clientv3.WithSort(clientv3.SortByKey, clientv3.SortAscend)))
	require.NoError(t, err)
	doGet := doResp.Get()
	require.NotNil(t, doGet)
	require.NotNil(t, doGet.Header)
	require.Equal(t, int64(3), doGet.Count)
	require.False(t, doGet.More)
	require.Len(t, doGet.Kvs, 1)
	require.Equal(t, []byte("b"), doGet.Kvs[0].Key)
	require.Equal(t, []byte("new-b"), doGet.Kvs[0].Value)

	txnResp, err := namespacedKV.Txn(ctx).
		Then(clientv3.OpGet("", clientv3.WithFromKey(),
			clientv3.WithMinModRev(updateB.Header.Revision),
			clientv3.WithSort(clientv3.SortByKey, clientv3.SortAscend))).
		Commit()
	require.NoError(t, err)
	require.True(t, txnResp.Succeeded)
	require.Len(t, txnResp.Responses, 1)
	txnGet := txnResp.Responses[0].GetResponseRange()
	require.NotNil(t, txnGet)
	require.NotNil(t, txnGet.Header)
	require.Equal(t, int64(3), txnGet.Count)
	require.False(t, txnGet.More)
	require.Len(t, txnGet.Kvs, 1)
	require.Equal(t, []byte("b"), txnGet.Kvs[0].Key)
	require.Equal(t, []byte("new-b"), txnGet.Kvs[0].Value)
}

func TestClientNamespaceGetMaxModRevisionFilterReturnsLogicalKeys(t *testing.T) {
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
	tenantPrefix := "/a1470/namespace-max-mod-filter/tenant/"
	namespacedKV := namespace.NewKV(client.KV, tenantPrefix)
	for _, key := range []string{"a", "b", "c"} {
		_, err = namespacedKV.Put(ctx, key, "old-"+key)
		require.NoError(t, err)
	}
	updateB, err := namespacedKV.Put(ctx, "b", "new-b")
	require.NoError(t, err)
	_, err = client.Put(ctx, "/a1470/namespace-max-mod-filter/tenant0/outside", "outside")
	require.NoError(t, err)

	resp, err := namespacedKV.Get(ctx, "", clientv3.WithFromKey(),
		clientv3.WithMaxModRev(updateB.Header.Revision-1),
		clientv3.WithSort(clientv3.SortByKey, clientv3.SortAscend))
	require.NoError(t, err)
	require.NotNil(t, resp.Header)
	require.Equal(t, int64(3), resp.Count)
	require.False(t, resp.More)
	require.Len(t, resp.Kvs, 2)
	require.Equal(t, [][]byte{[]byte("a"), []byte("c")}, [][]byte{resp.Kvs[0].Key, resp.Kvs[1].Key})
	require.Equal(t, [][]byte{[]byte("old-a"), []byte("old-c")}, [][]byte{resp.Kvs[0].Value, resp.Kvs[1].Value})

	doResp, err := namespacedKV.Do(ctx, clientv3.OpGet("", clientv3.WithFromKey(),
		clientv3.WithMaxModRev(updateB.Header.Revision-1),
		clientv3.WithSort(clientv3.SortByKey, clientv3.SortAscend)))
	require.NoError(t, err)
	doGet := doResp.Get()
	require.NotNil(t, doGet)
	require.NotNil(t, doGet.Header)
	require.Equal(t, int64(3), doGet.Count)
	require.False(t, doGet.More)
	require.Len(t, doGet.Kvs, 2)
	require.Equal(t, [][]byte{[]byte("a"), []byte("c")}, [][]byte{doGet.Kvs[0].Key, doGet.Kvs[1].Key})
	require.Equal(t, [][]byte{[]byte("old-a"), []byte("old-c")}, [][]byte{doGet.Kvs[0].Value, doGet.Kvs[1].Value})

	txnResp, err := namespacedKV.Txn(ctx).
		Then(clientv3.OpGet("", clientv3.WithFromKey(),
			clientv3.WithMaxModRev(updateB.Header.Revision-1),
			clientv3.WithSort(clientv3.SortByKey, clientv3.SortAscend))).
		Commit()
	require.NoError(t, err)
	require.True(t, txnResp.Succeeded)
	require.Len(t, txnResp.Responses, 1)
	txnGet := txnResp.Responses[0].GetResponseRange()
	require.NotNil(t, txnGet)
	require.NotNil(t, txnGet.Header)
	require.Equal(t, int64(3), txnGet.Count)
	require.False(t, txnGet.More)
	require.Len(t, txnGet.Kvs, 2)
	require.Equal(t, [][]byte{[]byte("a"), []byte("c")}, [][]byte{txnGet.Kvs[0].Key, txnGet.Kvs[1].Key})
	require.Equal(t, [][]byte{[]byte("old-a"), []byte("old-c")}, [][]byte{txnGet.Kvs[0].Value, txnGet.Kvs[1].Value})
}

func TestClientNamespaceGetCreateRevisionFilterReturnsLogicalKeys(t *testing.T) {
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
	tenantPrefix := "/a1473/namespace-create-filter/tenant/"
	namespacedKV := namespace.NewKV(client.KV, tenantPrefix)
	for _, key := range []string{"a", "b"} {
		_, err = namespacedKV.Put(ctx, key, "value-"+key)
		require.NoError(t, err)
	}
	createC, err := namespacedKV.Put(ctx, "c", "value-c")
	require.NoError(t, err)
	_, err = client.Put(ctx, "/a1473/namespace-create-filter/tenant0/outside", "outside")
	require.NoError(t, err)

	resp, err := namespacedKV.Get(ctx, "", clientv3.WithFromKey(),
		clientv3.WithMinCreateRev(createC.Header.Revision),
		clientv3.WithSort(clientv3.SortByKey, clientv3.SortAscend))
	require.NoError(t, err)
	require.NotNil(t, resp.Header)
	require.Equal(t, int64(3), resp.Count)
	require.False(t, resp.More)
	require.Len(t, resp.Kvs, 1)
	require.Equal(t, []byte("c"), resp.Kvs[0].Key)
	require.Equal(t, []byte("value-c"), resp.Kvs[0].Value)

	doResp, err := namespacedKV.Do(ctx, clientv3.OpGet("", clientv3.WithFromKey(),
		clientv3.WithMinCreateRev(createC.Header.Revision),
		clientv3.WithSort(clientv3.SortByKey, clientv3.SortAscend)))
	require.NoError(t, err)
	doGet := doResp.Get()
	require.NotNil(t, doGet)
	require.NotNil(t, doGet.Header)
	require.Equal(t, int64(3), doGet.Count)
	require.False(t, doGet.More)
	require.Len(t, doGet.Kvs, 1)
	require.Equal(t, []byte("c"), doGet.Kvs[0].Key)
	require.Equal(t, []byte("value-c"), doGet.Kvs[0].Value)

	txnResp, err := namespacedKV.Txn(ctx).
		Then(clientv3.OpGet("", clientv3.WithFromKey(),
			clientv3.WithMinCreateRev(createC.Header.Revision),
			clientv3.WithSort(clientv3.SortByKey, clientv3.SortAscend))).
		Commit()
	require.NoError(t, err)
	require.True(t, txnResp.Succeeded)
	require.Len(t, txnResp.Responses, 1)
	txnGet := txnResp.Responses[0].GetResponseRange()
	require.NotNil(t, txnGet)
	require.NotNil(t, txnGet.Header)
	require.Equal(t, int64(3), txnGet.Count)
	require.False(t, txnGet.More)
	require.Len(t, txnGet.Kvs, 1)
	require.Equal(t, []byte("c"), txnGet.Kvs[0].Key)
	require.Equal(t, []byte("value-c"), txnGet.Kvs[0].Value)
}

func TestClientNamespaceGetMaxCreateRevisionFilterReturnsLogicalKeys(t *testing.T) {
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
	tenantPrefix := "/a1476/namespace-max-create-filter/tenant/"
	namespacedKV := namespace.NewKV(client.KV, tenantPrefix)
	for _, key := range []string{"a", "b"} {
		_, err = namespacedKV.Put(ctx, key, "value-"+key)
		require.NoError(t, err)
	}
	createC, err := namespacedKV.Put(ctx, "c", "value-c")
	require.NoError(t, err)
	_, err = client.Put(ctx, "/a1476/namespace-max-create-filter/tenant0/outside", "outside")
	require.NoError(t, err)

	resp, err := namespacedKV.Get(ctx, "", clientv3.WithFromKey(),
		clientv3.WithMaxCreateRev(createC.Header.Revision-1),
		clientv3.WithSort(clientv3.SortByKey, clientv3.SortAscend))
	require.NoError(t, err)
	require.NotNil(t, resp.Header)
	require.Equal(t, int64(3), resp.Count)
	require.False(t, resp.More)
	require.Len(t, resp.Kvs, 2)
	require.Equal(t, [][]byte{[]byte("a"), []byte("b")}, [][]byte{resp.Kvs[0].Key, resp.Kvs[1].Key})
	require.Equal(t, [][]byte{[]byte("value-a"), []byte("value-b")}, [][]byte{resp.Kvs[0].Value, resp.Kvs[1].Value})

	doResp, err := namespacedKV.Do(ctx, clientv3.OpGet("", clientv3.WithFromKey(),
		clientv3.WithMaxCreateRev(createC.Header.Revision-1),
		clientv3.WithSort(clientv3.SortByKey, clientv3.SortAscend)))
	require.NoError(t, err)
	doGet := doResp.Get()
	require.NotNil(t, doGet)
	require.NotNil(t, doGet.Header)
	require.Equal(t, int64(3), doGet.Count)
	require.False(t, doGet.More)
	require.Len(t, doGet.Kvs, 2)
	require.Equal(t, [][]byte{[]byte("a"), []byte("b")}, [][]byte{doGet.Kvs[0].Key, doGet.Kvs[1].Key})
	require.Equal(t, [][]byte{[]byte("value-a"), []byte("value-b")}, [][]byte{doGet.Kvs[0].Value, doGet.Kvs[1].Value})

	txnResp, err := namespacedKV.Txn(ctx).
		Then(clientv3.OpGet("", clientv3.WithFromKey(),
			clientv3.WithMaxCreateRev(createC.Header.Revision-1),
			clientv3.WithSort(clientv3.SortByKey, clientv3.SortAscend))).
		Commit()
	require.NoError(t, err)
	require.True(t, txnResp.Succeeded)
	require.Len(t, txnResp.Responses, 1)
	txnGet := txnResp.Responses[0].GetResponseRange()
	require.NotNil(t, txnGet)
	require.NotNil(t, txnGet.Header)
	require.Equal(t, int64(3), txnGet.Count)
	require.False(t, txnGet.More)
	require.Len(t, txnGet.Kvs, 2)
	require.Equal(t, [][]byte{[]byte("a"), []byte("b")}, [][]byte{txnGet.Kvs[0].Key, txnGet.Kvs[1].Key})
	require.Equal(t, [][]byte{[]byte("value-a"), []byte("value-b")}, [][]byte{txnGet.Kvs[0].Value, txnGet.Kvs[1].Value})
}

func TestClientNamespaceGetFirstCreateReturnsLogicalKey(t *testing.T) {
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
	tenantPrefix := "/a1479/namespace-first-create/tenant/"
	namespacedKV := namespace.NewKV(client.KV, tenantPrefix)
	for _, key := range []string{"b", "a", "c"} {
		_, err = namespacedKV.Put(ctx, key, "value-"+key)
		require.NoError(t, err)
	}
	_, err = client.Put(ctx, "/a1479/namespace-first-create/tenant0/outside", "outside")
	require.NoError(t, err)

	resp, err := namespacedKV.Get(ctx, "", clientv3.WithFirstCreate()...)
	require.NoError(t, err)
	require.NotNil(t, resp.Header)
	require.Equal(t, int64(3), resp.Count)
	require.True(t, resp.More)
	require.Len(t, resp.Kvs, 1)
	require.Equal(t, []byte("b"), resp.Kvs[0].Key)
	require.Equal(t, []byte("value-b"), resp.Kvs[0].Value)
}

func TestClientNamespaceGetFirstCreateWithLogicalPrefixReturnsLogicalKey(t *testing.T) {
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
	tenantPrefix := "/a1480/namespace-first-create-prefix/tenant/"
	namespacedKV := namespace.NewKV(client.KV, tenantPrefix)
	_, err = namespacedKV.Put(ctx, "other/oldest", "other")
	require.NoError(t, err)
	for _, key := range []string{"waiters/b", "waiters/a", "waiters/c"} {
		_, err = namespacedKV.Put(ctx, key, "value-"+key)
		require.NoError(t, err)
	}
	_, err = client.Put(ctx, "/a1480/namespace-first-create-prefix/tenant/waiters0/outside", "same-tenant-outside-prefix")
	require.NoError(t, err)
	_, err = client.Put(ctx, "/a1480/namespace-first-create-prefix/tenant0/waiters/outside", "outside-tenant")
	require.NoError(t, err)

	resp, err := namespacedKV.Get(ctx, "waiters/", clientv3.WithFirstCreate()...)
	require.NoError(t, err)
	require.NotNil(t, resp.Header)
	require.Equal(t, int64(3), resp.Count)
	require.True(t, resp.More)
	require.Len(t, resp.Kvs, 1)
	require.Equal(t, []byte("waiters/b"), resp.Kvs[0].Key)
	require.Equal(t, []byte("value-waiters/b"), resp.Kvs[0].Value)

	doResp, err := namespacedKV.Do(ctx, clientv3.OpGet("waiters/", clientv3.WithFirstCreate()...))
	require.NoError(t, err)
	doGet := doResp.Get()
	require.NotNil(t, doGet)
	require.NotNil(t, doGet.Header)
	require.Equal(t, int64(3), doGet.Count)
	require.True(t, doGet.More)
	require.Len(t, doGet.Kvs, 1)
	require.Equal(t, []byte("waiters/b"), doGet.Kvs[0].Key)
	require.Equal(t, []byte("value-waiters/b"), doGet.Kvs[0].Value)

	txnResp, err := namespacedKV.Txn(ctx).
		Then(clientv3.OpGet("waiters/", clientv3.WithFirstCreate()...)).
		Commit()
	require.NoError(t, err)
	require.True(t, txnResp.Succeeded)
	require.Len(t, txnResp.Responses, 1)
	txnGet := txnResp.Responses[0].GetResponseRange()
	require.NotNil(t, txnGet)
	require.NotNil(t, txnGet.Header)
	require.Equal(t, int64(3), txnGet.Count)
	require.True(t, txnGet.More)
	require.Len(t, txnGet.Kvs, 1)
	require.Equal(t, []byte("waiters/b"), txnGet.Kvs[0].Key)
	require.Equal(t, []byte("value-waiters/b"), txnGet.Kvs[0].Value)
}

func TestClientNamespaceGetLastCreateWithMaxCreateRevisionReturnsLogicalKey(t *testing.T) {
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
	tenantPrefix := "/a1483/namespace-last-create-max-create/tenant/"
	namespacedKV := namespace.NewKV(client.KV, tenantPrefix)
	for _, key := range []string{"waiters/a", "waiters/b"} {
		_, err = namespacedKV.Put(ctx, key, "value-"+key)
		require.NoError(t, err)
	}
	createC, err := namespacedKV.Put(ctx, "waiters/c", "value-waiters/c")
	require.NoError(t, err)
	_, err = namespacedKV.Put(ctx, "other/newer", "other")
	require.NoError(t, err)
	_, err = client.Put(ctx, "/a1483/namespace-last-create-max-create/tenant/waiters0/outside", "same-tenant-outside-prefix")
	require.NoError(t, err)
	_, err = client.Put(ctx, "/a1483/namespace-last-create-max-create/tenant0/waiters/outside", "outside-tenant")
	require.NoError(t, err)

	getOpts := append(clientv3.WithLastCreate(), clientv3.WithMaxCreateRev(createC.Header.Revision-1))
	resp, err := namespacedKV.Get(ctx, "waiters/", getOpts...)
	require.NoError(t, err)
	require.NotNil(t, resp.Header)
	require.Equal(t, int64(3), resp.Count)
	require.True(t, resp.More)
	require.Len(t, resp.Kvs, 1)
	require.Equal(t, []byte("waiters/b"), resp.Kvs[0].Key)
	require.Equal(t, []byte("value-waiters/b"), resp.Kvs[0].Value)

	doResp, err := namespacedKV.Do(ctx, clientv3.OpGet("waiters/", getOpts...))
	require.NoError(t, err)
	doGet := doResp.Get()
	require.NotNil(t, doGet)
	require.NotNil(t, doGet.Header)
	require.Equal(t, int64(3), doGet.Count)
	require.True(t, doGet.More)
	require.Len(t, doGet.Kvs, 1)
	require.Equal(t, []byte("waiters/b"), doGet.Kvs[0].Key)
	require.Equal(t, []byte("value-waiters/b"), doGet.Kvs[0].Value)

	txnResp, err := namespacedKV.Txn(ctx).
		Then(clientv3.OpGet("waiters/", getOpts...)).
		Commit()
	require.NoError(t, err)
	require.True(t, txnResp.Succeeded)
	require.Len(t, txnResp.Responses, 1)
	txnGet := txnResp.Responses[0].GetResponseRange()
	require.NotNil(t, txnGet)
	require.NotNil(t, txnGet.Header)
	require.Equal(t, int64(3), txnGet.Count)
	require.True(t, txnGet.More)
	require.Len(t, txnGet.Kvs, 1)
	require.Equal(t, []byte("waiters/b"), txnGet.Kvs[0].Key)
	require.Equal(t, []byte("value-waiters/b"), txnGet.Kvs[0].Value)
}

func TestClientNamespaceGetFirstKeyWithLogicalPrefixReturnsLogicalKey(t *testing.T) {
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
	tenantPrefix := "/a1486/namespace-first-key-prefix/tenant/"
	namespacedKV := namespace.NewKV(client.KV, tenantPrefix)
	_, err = namespacedKV.Put(ctx, "other/a", "other")
	require.NoError(t, err)
	for _, key := range []string{"items/b", "items/a", "items/c"} {
		_, err = namespacedKV.Put(ctx, key, "value-"+key)
		require.NoError(t, err)
	}
	_, err = client.Put(ctx, "/a1486/namespace-first-key-prefix/tenant/items0/outside", "same-tenant-outside-prefix")
	require.NoError(t, err)
	_, err = client.Put(ctx, "/a1486/namespace-first-key-prefix/tenant0/items/a", "outside-tenant")
	require.NoError(t, err)

	resp, err := namespacedKV.Get(ctx, "items/", clientv3.WithFirstKey()...)
	require.NoError(t, err)
	require.NotNil(t, resp.Header)
	require.Equal(t, int64(3), resp.Count)
	require.True(t, resp.More)
	require.Len(t, resp.Kvs, 1)
	require.Equal(t, []byte("items/a"), resp.Kvs[0].Key)
	require.Equal(t, []byte("value-items/a"), resp.Kvs[0].Value)

	doResp, err := namespacedKV.Do(ctx, clientv3.OpGet("items/", clientv3.WithFirstKey()...))
	require.NoError(t, err)
	doGet := doResp.Get()
	require.NotNil(t, doGet)
	require.NotNil(t, doGet.Header)
	require.Equal(t, int64(3), doGet.Count)
	require.True(t, doGet.More)
	require.Len(t, doGet.Kvs, 1)
	require.Equal(t, []byte("items/a"), doGet.Kvs[0].Key)
	require.Equal(t, []byte("value-items/a"), doGet.Kvs[0].Value)

	txnResp, err := namespacedKV.Txn(ctx).
		Then(clientv3.OpGet("items/", clientv3.WithFirstKey()...)).
		Commit()
	require.NoError(t, err)
	require.True(t, txnResp.Succeeded)
	require.Len(t, txnResp.Responses, 1)
	txnGet := txnResp.Responses[0].GetResponseRange()
	require.NotNil(t, txnGet)
	require.NotNil(t, txnGet.Header)
	require.Equal(t, int64(3), txnGet.Count)
	require.True(t, txnGet.More)
	require.Len(t, txnGet.Kvs, 1)
	require.Equal(t, []byte("items/a"), txnGet.Kvs[0].Key)
	require.Equal(t, []byte("value-items/a"), txnGet.Kvs[0].Value)
}

func TestClientNamespaceGetLastKeyWithLogicalPrefixReturnsLogicalKey(t *testing.T) {
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
	tenantPrefix := "/a1489/namespace-last-key-prefix/tenant/"
	namespacedKV := namespace.NewKV(client.KV, tenantPrefix)
	_, err = namespacedKV.Put(ctx, "other/z", "other")
	require.NoError(t, err)
	for _, key := range []string{"items/a", "items/c", "items/b"} {
		_, err = namespacedKV.Put(ctx, key, "value-"+key)
		require.NoError(t, err)
	}
	_, err = client.Put(ctx, "/a1489/namespace-last-key-prefix/tenant/items0/z", "same-tenant-outside-prefix")
	require.NoError(t, err)
	_, err = client.Put(ctx, "/a1489/namespace-last-key-prefix/tenant0/items/z", "outside-tenant")
	require.NoError(t, err)

	resp, err := namespacedKV.Get(ctx, "items/", clientv3.WithLastKey()...)
	require.NoError(t, err)
	require.NotNil(t, resp.Header)
	require.Equal(t, int64(3), resp.Count)
	require.True(t, resp.More)
	require.Len(t, resp.Kvs, 1)
	require.Equal(t, []byte("items/c"), resp.Kvs[0].Key)
	require.Equal(t, []byte("value-items/c"), resp.Kvs[0].Value)

	doResp, err := namespacedKV.Do(ctx, clientv3.OpGet("items/", clientv3.WithLastKey()...))
	require.NoError(t, err)
	doGet := doResp.Get()
	require.NotNil(t, doGet)
	require.NotNil(t, doGet.Header)
	require.Equal(t, int64(3), doGet.Count)
	require.True(t, doGet.More)
	require.Len(t, doGet.Kvs, 1)
	require.Equal(t, []byte("items/c"), doGet.Kvs[0].Key)
	require.Equal(t, []byte("value-items/c"), doGet.Kvs[0].Value)

	txnResp, err := namespacedKV.Txn(ctx).
		Then(clientv3.OpGet("items/", clientv3.WithLastKey()...)).
		Commit()
	require.NoError(t, err)
	require.True(t, txnResp.Succeeded)
	require.Len(t, txnResp.Responses, 1)
	txnGet := txnResp.Responses[0].GetResponseRange()
	require.NotNil(t, txnGet)
	require.NotNil(t, txnGet.Header)
	require.Equal(t, int64(3), txnGet.Count)
	require.True(t, txnGet.More)
	require.Len(t, txnGet.Kvs, 1)
	require.Equal(t, []byte("items/c"), txnGet.Kvs[0].Key)
	require.Equal(t, []byte("value-items/c"), txnGet.Kvs[0].Value)
}

func TestClientNamespaceGetFirstRevWithLogicalPrefixReturnsLogicalKey(t *testing.T) {
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
	tenantPrefix := "/a1492/namespace-first-rev-prefix/tenant/"
	namespacedKV := namespace.NewKV(client.KV, tenantPrefix)
	_, err = namespacedKV.Put(ctx, "other/oldest", "other")
	require.NoError(t, err)
	for _, key := range []string{"queue/b", "queue/a", "queue/c"} {
		_, err = namespacedKV.Put(ctx, key, "value-"+key)
		require.NoError(t, err)
	}
	_, err = namespacedKV.Put(ctx, "queue/b", "updated-queue/b")
	require.NoError(t, err)
	_, err = client.Put(ctx, "/a1492/namespace-first-rev-prefix/tenant/queue0/outside", "same-tenant-outside-prefix")
	require.NoError(t, err)
	_, err = client.Put(ctx, "/a1492/namespace-first-rev-prefix/tenant0/queue/a", "outside-tenant")
	require.NoError(t, err)

	resp, err := namespacedKV.Get(ctx, "queue/", clientv3.WithFirstRev()...)
	require.NoError(t, err)
	require.NotNil(t, resp.Header)
	require.Equal(t, int64(3), resp.Count)
	require.True(t, resp.More)
	require.Len(t, resp.Kvs, 1)
	require.Equal(t, []byte("queue/a"), resp.Kvs[0].Key)
	require.Equal(t, []byte("value-queue/a"), resp.Kvs[0].Value)

	doResp, err := namespacedKV.Do(ctx, clientv3.OpGet("queue/", clientv3.WithFirstRev()...))
	require.NoError(t, err)
	doGet := doResp.Get()
	require.NotNil(t, doGet)
	require.NotNil(t, doGet.Header)
	require.Equal(t, int64(3), doGet.Count)
	require.True(t, doGet.More)
	require.Len(t, doGet.Kvs, 1)
	require.Equal(t, []byte("queue/a"), doGet.Kvs[0].Key)
	require.Equal(t, []byte("value-queue/a"), doGet.Kvs[0].Value)

	txnResp, err := namespacedKV.Txn(ctx).
		Then(clientv3.OpGet("queue/", clientv3.WithFirstRev()...)).
		Commit()
	require.NoError(t, err)
	require.True(t, txnResp.Succeeded)
	require.Len(t, txnResp.Responses, 1)
	txnGet := txnResp.Responses[0].GetResponseRange()
	require.NotNil(t, txnGet)
	require.NotNil(t, txnGet.Header)
	require.Equal(t, int64(3), txnGet.Count)
	require.True(t, txnGet.More)
	require.Len(t, txnGet.Kvs, 1)
	require.Equal(t, []byte("queue/a"), txnGet.Kvs[0].Key)
	require.Equal(t, []byte("value-queue/a"), txnGet.Kvs[0].Value)
}

func TestClientNamespaceGetLastRevWithMaxModRevisionReturnsLogicalKey(t *testing.T) {
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
	tenantPrefix := "/a1495/namespace-last-rev-max-mod/tenant/"
	namespacedKV := namespace.NewKV(client.KV, tenantPrefix)
	for _, key := range []string{"locks/a", "locks/b", "locks/c"} {
		_, err = namespacedKV.Put(ctx, key, "value-"+key)
		require.NoError(t, err)
	}
	updateB, err := namespacedKV.Put(ctx, "locks/b", "updated-locks/b")
	require.NoError(t, err)
	_, err = namespacedKV.Put(ctx, "locks/c", "updated-locks/c")
	require.NoError(t, err)
	_, err = namespacedKV.Put(ctx, "other/newer", "other")
	require.NoError(t, err)
	_, err = client.Put(ctx, "/a1495/namespace-last-rev-max-mod/tenant/locks0/outside", "same-tenant-outside-prefix")
	require.NoError(t, err)
	_, err = client.Put(ctx, "/a1495/namespace-last-rev-max-mod/tenant0/locks/outside", "outside-tenant")
	require.NoError(t, err)

	getOpts := append(clientv3.WithLastRev(), clientv3.WithMaxModRev(updateB.Header.Revision))
	resp, err := namespacedKV.Get(ctx, "locks/", getOpts...)
	require.NoError(t, err)
	require.NotNil(t, resp.Header)
	require.Equal(t, int64(3), resp.Count)
	require.True(t, resp.More)
	require.Len(t, resp.Kvs, 1)
	require.Equal(t, []byte("locks/b"), resp.Kvs[0].Key)
	require.Equal(t, []byte("updated-locks/b"), resp.Kvs[0].Value)

	doResp, err := namespacedKV.Do(ctx, clientv3.OpGet("locks/", getOpts...))
	require.NoError(t, err)
	doGet := doResp.Get()
	require.NotNil(t, doGet)
	require.NotNil(t, doGet.Header)
	require.Equal(t, int64(3), doGet.Count)
	require.True(t, doGet.More)
	require.Len(t, doGet.Kvs, 1)
	require.Equal(t, []byte("locks/b"), doGet.Kvs[0].Key)
	require.Equal(t, []byte("updated-locks/b"), doGet.Kvs[0].Value)

	txnResp, err := namespacedKV.Txn(ctx).
		Then(clientv3.OpGet("locks/", getOpts...)).
		Commit()
	require.NoError(t, err)
	require.True(t, txnResp.Succeeded)
	require.Len(t, txnResp.Responses, 1)
	txnGet := txnResp.Responses[0].GetResponseRange()
	require.NotNil(t, txnGet)
	require.NotNil(t, txnGet.Header)
	require.Equal(t, int64(3), txnGet.Count)
	require.True(t, txnGet.More)
	require.Len(t, txnGet.Kvs, 1)
	require.Equal(t, []byte("locks/b"), txnGet.Kvs[0].Key)
	require.Equal(t, []byte("updated-locks/b"), txnGet.Kvs[0].Value)
}

func TestClientNamespaceGetLastRevWithLogicalPrefixReturnsLogicalKey(t *testing.T) {
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
	tenantPrefix := "/a1498/namespace-last-rev-prefix/tenant/"
	namespacedKV := namespace.NewKV(client.KV, tenantPrefix)
	for _, key := range []string{"queue/a", "queue/b", "queue/c"} {
		_, err = namespacedKV.Put(ctx, key, "value-"+key)
		require.NoError(t, err)
	}
	_, err = namespacedKV.Put(ctx, "queue/b", "updated-queue/b")
	require.NoError(t, err)
	_, err = namespacedKV.Put(ctx, "other/newer", "other")
	require.NoError(t, err)
	_, err = client.Put(ctx, "/a1498/namespace-last-rev-prefix/tenant/queue0/outside", "same-tenant-outside-prefix")
	require.NoError(t, err)
	_, err = client.Put(ctx, "/a1498/namespace-last-rev-prefix/tenant0/queue/outside", "outside-tenant")
	require.NoError(t, err)

	resp, err := namespacedKV.Get(ctx, "queue/", clientv3.WithLastRev()...)
	require.NoError(t, err)
	require.NotNil(t, resp.Header)
	require.Equal(t, int64(3), resp.Count)
	require.True(t, resp.More)
	require.Len(t, resp.Kvs, 1)
	require.Equal(t, []byte("queue/b"), resp.Kvs[0].Key)
	require.Equal(t, []byte("updated-queue/b"), resp.Kvs[0].Value)

	doResp, err := namespacedKV.Do(ctx, clientv3.OpGet("queue/", clientv3.WithLastRev()...))
	require.NoError(t, err)
	doGet := doResp.Get()
	require.NotNil(t, doGet)
	require.NotNil(t, doGet.Header)
	require.Equal(t, int64(3), doGet.Count)
	require.True(t, doGet.More)
	require.Len(t, doGet.Kvs, 1)
	require.Equal(t, []byte("queue/b"), doGet.Kvs[0].Key)
	require.Equal(t, []byte("updated-queue/b"), doGet.Kvs[0].Value)

	txnResp, err := namespacedKV.Txn(ctx).
		Then(clientv3.OpGet("queue/", clientv3.WithLastRev()...)).
		Commit()
	require.NoError(t, err)
	require.True(t, txnResp.Succeeded)
	require.Len(t, txnResp.Responses, 1)
	txnGet := txnResp.Responses[0].GetResponseRange()
	require.NotNil(t, txnGet)
	require.NotNil(t, txnGet.Header)
	require.Equal(t, int64(3), txnGet.Count)
	require.True(t, txnGet.More)
	require.Len(t, txnGet.Kvs, 1)
	require.Equal(t, []byte("queue/b"), txnGet.Kvs[0].Key)
	require.Equal(t, []byte("updated-queue/b"), txnGet.Kvs[0].Value)
}

func TestClientNamespaceGetLastRevWithLogicalRangeReturnsLogicalKey(t *testing.T) {
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
	tenantPrefix := "/a1501/namespace-last-rev-range/tenant/"
	namespacedKV := namespace.NewKV(client.KV, tenantPrefix)
	for _, key := range []string{"range/a", "range/b", "range/c"} {
		_, err = namespacedKV.Put(ctx, key, "value-"+key)
		require.NoError(t, err)
	}
	_, err = namespacedKV.Put(ctx, "range/b", "updated-range/b")
	require.NoError(t, err)
	_, err = namespacedKV.Put(ctx, "range/d", "outside-upper-bound")
	require.NoError(t, err)
	_, err = namespacedKV.Put(ctx, "other/newer", "other")
	require.NoError(t, err)
	_, err = client.Put(ctx, "/a1501/namespace-last-rev-range/tenant/range0/outside", "same-tenant-outside-range")
	require.NoError(t, err)
	_, err = client.Put(ctx, "/a1501/namespace-last-rev-range/tenant0/range/b", "outside-tenant")
	require.NoError(t, err)

	getOpts := append(clientv3.WithLastRev(), clientv3.WithRange("range/d"))
	resp, err := namespacedKV.Get(ctx, "range/a", getOpts...)
	require.NoError(t, err)
	require.NotNil(t, resp.Header)
	require.Equal(t, int64(3), resp.Count)
	require.True(t, resp.More)
	require.Len(t, resp.Kvs, 1)
	require.Equal(t, []byte("range/b"), resp.Kvs[0].Key)
	require.Equal(t, []byte("updated-range/b"), resp.Kvs[0].Value)

	doResp, err := namespacedKV.Do(ctx, clientv3.OpGet("range/a", getOpts...))
	require.NoError(t, err)
	doGet := doResp.Get()
	require.NotNil(t, doGet)
	require.NotNil(t, doGet.Header)
	require.Equal(t, int64(3), doGet.Count)
	require.True(t, doGet.More)
	require.Len(t, doGet.Kvs, 1)
	require.Equal(t, []byte("range/b"), doGet.Kvs[0].Key)
	require.Equal(t, []byte("updated-range/b"), doGet.Kvs[0].Value)

	txnResp, err := namespacedKV.Txn(ctx).
		Then(clientv3.OpGet("range/a", getOpts...)).
		Commit()
	require.NoError(t, err)
	require.True(t, txnResp.Succeeded)
	require.Len(t, txnResp.Responses, 1)
	txnGet := txnResp.Responses[0].GetResponseRange()
	require.NotNil(t, txnGet)
	require.NotNil(t, txnGet.Header)
	require.Equal(t, int64(3), txnGet.Count)
	require.True(t, txnGet.More)
	require.Len(t, txnGet.Kvs, 1)
	require.Equal(t, []byte("range/b"), txnGet.Kvs[0].Key)
	require.Equal(t, []byte("updated-range/b"), txnGet.Kvs[0].Value)
}

func TestClientNamespaceTxnGetKeysOnlyWithLogicalRangeReturnsLogicalKeys(t *testing.T) {
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
	tenantPrefix := "/a1504/namespace-txn-keys-only-range/tenant/"
	namespacedKV := namespace.NewKV(client.KV, tenantPrefix)
	for _, key := range []string{"range/a", "range/b", "range/c"} {
		_, err = namespacedKV.Put(ctx, key, "value-"+key)
		require.NoError(t, err)
	}
	_, err = namespacedKV.Put(ctx, "range/d", "outside-upper-bound")
	require.NoError(t, err)
	_, err = namespacedKV.Put(ctx, "other/in-tenant", "other")
	require.NoError(t, err)
	_, err = client.Put(ctx, "/a1504/namespace-txn-keys-only-range/tenant/range0/outside", "same-tenant-outside-range")
	require.NoError(t, err)
	_, err = client.Put(ctx, "/a1504/namespace-txn-keys-only-range/tenant0/range/b", "outside-tenant")
	require.NoError(t, err)

	txnResp, err := namespacedKV.Txn(ctx).
		Then(clientv3.OpGet("range/a",
			clientv3.WithRange("range/d"),
			clientv3.WithKeysOnly())).
		Commit()
	require.NoError(t, err)
	require.True(t, txnResp.Succeeded)
	require.Len(t, txnResp.Responses, 1)
	txnGet := txnResp.Responses[0].GetResponseRange()
	require.NotNil(t, txnGet)
	require.NotNil(t, txnGet.Header)
	require.Equal(t, int64(3), txnGet.Count)
	require.False(t, txnGet.More)
	require.Len(t, txnGet.Kvs, 3)
	require.Equal(t, [][]byte{[]byte("range/a"), []byte("range/b"), []byte("range/c")},
		[][]byte{txnGet.Kvs[0].Key, txnGet.Kvs[1].Key, txnGet.Kvs[2].Key})
	for _, kv := range txnGet.Kvs {
		require.Empty(t, kv.Value)
	}
}

func TestClientNamespaceTxnGetKeysOnlyAndDeleteWithLogicalRange(t *testing.T) {
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
	tenantPrefix := "/a1505/namespace-txn-get-delete-range/tenant/"
	namespacedKV := namespace.NewKV(client.KV, tenantPrefix)
	for _, key := range []string{"range/a", "range/b", "range/c"} {
		_, err = namespacedKV.Put(ctx, key, "value-"+key)
		require.NoError(t, err)
	}
	_, err = namespacedKV.Put(ctx, "range/d", "outside-upper-bound")
	require.NoError(t, err)
	_, err = namespacedKV.Put(ctx, "other/in-tenant", "other")
	require.NoError(t, err)
	_, err = client.Put(ctx, "/a1505/namespace-txn-get-delete-range/tenant/range0/outside", "same-tenant-outside-range")
	require.NoError(t, err)
	_, err = client.Put(ctx, "/a1505/namespace-txn-get-delete-range/tenant0/range/b", "outside-tenant")
	require.NoError(t, err)

	txnResp, err := namespacedKV.Txn(ctx).
		Then(
			clientv3.OpGet("range/a", clientv3.WithRange("range/d"), clientv3.WithKeysOnly()),
			clientv3.OpDelete("range/a", clientv3.WithRange("range/d")),
		).
		Commit()
	require.NoError(t, err)
	require.True(t, txnResp.Succeeded)
	require.Len(t, txnResp.Responses, 2)

	txnGet := txnResp.Responses[0].GetResponseRange()
	require.NotNil(t, txnGet)
	require.NotNil(t, txnGet.Header)
	require.Equal(t, int64(3), txnGet.Count)
	require.False(t, txnGet.More)
	require.Len(t, txnGet.Kvs, 3)
	require.Equal(t, [][]byte{[]byte("range/a"), []byte("range/b"), []byte("range/c")},
		[][]byte{txnGet.Kvs[0].Key, txnGet.Kvs[1].Key, txnGet.Kvs[2].Key})
	for _, kv := range txnGet.Kvs {
		require.Empty(t, kv.Value)
	}

	txnDelete := txnResp.Responses[1].GetResponseDeleteRange()
	require.NotNil(t, txnDelete)
	require.NotNil(t, txnDelete.Header)
	require.Equal(t, int64(3), txnDelete.Deleted)

	remaining, err := namespacedKV.Get(ctx, "range/", clientv3.WithPrefix())
	require.NoError(t, err)
	require.Equal(t, int64(1), remaining.Count)
	require.Len(t, remaining.Kvs, 1)
	require.Equal(t, []byte("range/d"), remaining.Kvs[0].Key)
	require.Equal(t, []byte("outside-upper-bound"), remaining.Kvs[0].Value)
}

func TestClientNamespaceTxnDeleteWithPrevKVLogicalRangeReturnsLogicalPrevKVs(t *testing.T) {
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
	tenantPrefix := "/a1506/namespace-txn-delete-prevkv-range/tenant/"
	namespacedKV := namespace.NewKV(client.KV, tenantPrefix)
	for _, key := range []string{"range/a", "range/b", "range/c"} {
		_, err = namespacedKV.Put(ctx, key, "value-"+key)
		require.NoError(t, err)
	}
	_, err = namespacedKV.Put(ctx, "range/d", "outside-upper-bound")
	require.NoError(t, err)
	_, err = namespacedKV.Put(ctx, "other/in-tenant", "other")
	require.NoError(t, err)
	_, err = client.Put(ctx, "/a1506/namespace-txn-delete-prevkv-range/tenant/range0/outside", "same-tenant-outside-range")
	require.NoError(t, err)
	_, err = client.Put(ctx, "/a1506/namespace-txn-delete-prevkv-range/tenant0/range/b", "outside-tenant")
	require.NoError(t, err)

	txnResp, err := namespacedKV.Txn(ctx).
		Then(clientv3.OpDelete("range/a",
			clientv3.WithRange("range/d"),
			clientv3.WithPrevKV())).
		Commit()
	require.NoError(t, err)
	require.True(t, txnResp.Succeeded)
	require.Len(t, txnResp.Responses, 1)
	txnDelete := txnResp.Responses[0].GetResponseDeleteRange()
	require.NotNil(t, txnDelete)
	require.NotNil(t, txnDelete.Header)
	require.Equal(t, int64(3), txnDelete.Deleted)
	require.Len(t, txnDelete.PrevKvs, 3)
	require.Equal(t, [][]byte{[]byte("range/a"), []byte("range/b"), []byte("range/c")},
		[][]byte{txnDelete.PrevKvs[0].Key, txnDelete.PrevKvs[1].Key, txnDelete.PrevKvs[2].Key})
	require.Equal(t, [][]byte{[]byte("value-range/a"), []byte("value-range/b"), []byte("value-range/c")},
		[][]byte{txnDelete.PrevKvs[0].Value, txnDelete.PrevKvs[1].Value, txnDelete.PrevKvs[2].Value})

	remaining, err := namespacedKV.Get(ctx, "range/", clientv3.WithPrefix())
	require.NoError(t, err)
	require.Equal(t, int64(1), remaining.Count)
	require.Len(t, remaining.Kvs, 1)
	require.Equal(t, []byte("range/d"), remaining.Kvs[0].Key)
	require.Equal(t, []byte("outside-upper-bound"), remaining.Kvs[0].Value)
}

func TestClientNamespaceTxnRangeCompareWithLogicalRange(t *testing.T) {
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
	tenantPrefix := "/a1507/namespace-txn-range-compare/tenant/"
	namespacedKV := namespace.NewKV(client.KV, tenantPrefix)
	for _, key := range []string{"range/a", "range/b", "range/c"} {
		_, err = namespacedKV.Put(ctx, key, "value-"+key)
		require.NoError(t, err)
	}
	_, err = namespacedKV.Put(ctx, "range/d", "outside-upper-bound")
	require.NoError(t, err)
	_, err = namespacedKV.Put(ctx, "other/in-tenant", "other")
	require.NoError(t, err)
	_, err = client.Put(ctx, "/a1507/namespace-txn-range-compare/tenant/range0/outside", "same-tenant-outside-range")
	require.NoError(t, err)
	_, err = client.Put(ctx, "/a1507/namespace-txn-range-compare/tenant0/range/b", "outside-tenant")
	require.NoError(t, err)

	txnResp, err := namespacedKV.Txn(ctx).
		If(clientv3.Compare(clientv3.Version("range/a").WithRange("range/d"), ">", 0)).
		Then(clientv3.OpDelete("range/a", clientv3.WithRange("range/d"), clientv3.WithPrevKV())).
		Else(clientv3.OpGet("range/a", clientv3.WithRange("range/d"))).
		Commit()
	require.NoError(t, err)
	require.True(t, txnResp.Succeeded)
	require.Len(t, txnResp.Responses, 1)
	txnDelete := txnResp.Responses[0].GetResponseDeleteRange()
	require.NotNil(t, txnDelete)
	require.NotNil(t, txnDelete.Header)
	require.Equal(t, int64(3), txnDelete.Deleted)
	require.Len(t, txnDelete.PrevKvs, 3)
	require.Equal(t, [][]byte{[]byte("range/a"), []byte("range/b"), []byte("range/c")},
		[][]byte{txnDelete.PrevKvs[0].Key, txnDelete.PrevKvs[1].Key, txnDelete.PrevKvs[2].Key})

	remaining, err := namespacedKV.Get(ctx, "range/", clientv3.WithPrefix())
	require.NoError(t, err)
	require.Equal(t, int64(1), remaining.Count)
	require.Len(t, remaining.Kvs, 1)
	require.Equal(t, []byte("range/d"), remaining.Kvs[0].Key)
	require.Equal(t, []byte("outside-upper-bound"), remaining.Kvs[0].Value)
}

func TestClientNamespaceTxnRangeCompareIgnoresAdjacentNamespaces(t *testing.T) {
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
	tenantPrefix := "/a1508/namespace-txn-range-compare-empty/tenant/"
	namespacedKV := namespace.NewKV(client.KV, tenantPrefix)
	_, err = namespacedKV.Put(ctx, "range/d", "outside-upper-bound")
	require.NoError(t, err)
	_, err = namespacedKV.Put(ctx, "range0/in-tenant", "same-tenant-outside-range")
	require.NoError(t, err)
	_, err = client.Put(ctx, "/a1508/namespace-txn-range-compare-empty/tenant0/range/b", "outside-tenant")
	require.NoError(t, err)

	txnResp, err := namespacedKV.Txn(ctx).
		If(clientv3.Compare(clientv3.Version("range/a").WithRange("range/d"), ">", 0)).
		Then(clientv3.OpDelete("range/a", clientv3.WithRange("range/d"), clientv3.WithPrevKV())).
		Else(clientv3.OpGet("range/a", clientv3.WithRange("range/d"))).
		Commit()
	require.NoError(t, err)
	require.False(t, txnResp.Succeeded)
	require.Len(t, txnResp.Responses, 1)
	txnGet := txnResp.Responses[0].GetResponseRange()
	require.NotNil(t, txnGet)
	require.NotNil(t, txnGet.Header)
	require.Equal(t, int64(0), txnGet.Count)
	require.False(t, txnGet.More)
	require.Empty(t, txnGet.Kvs)

	remaining, err := namespacedKV.Get(ctx, "range/", clientv3.WithPrefix())
	require.NoError(t, err)
	require.Equal(t, int64(1), remaining.Count)
	require.Len(t, remaining.Kvs, 1)
	require.Equal(t, []byte("range/d"), remaining.Kvs[0].Key)
	require.Equal(t, []byte("outside-upper-bound"), remaining.Kvs[0].Value)
}

func TestClientNamespaceNestedTxnGetWithLogicalRangeReturnsLogicalKeys(t *testing.T) {
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
	tenantPrefix := "/a1509/namespace-nested-txn-get-range/tenant/"
	namespacedKV := namespace.NewKV(client.KV, tenantPrefix)
	for _, key := range []string{"range/a", "range/b", "range/c"} {
		_, err = namespacedKV.Put(ctx, key, "value-"+key)
		require.NoError(t, err)
	}
	_, err = namespacedKV.Put(ctx, "range/d", "outside-upper-bound")
	require.NoError(t, err)
	_, err = namespacedKV.Put(ctx, "other/in-tenant", "other")
	require.NoError(t, err)
	_, err = client.Put(ctx, "/a1509/namespace-nested-txn-get-range/tenant/range0/outside", "same-tenant-outside-range")
	require.NoError(t, err)
	_, err = client.Put(ctx, "/a1509/namespace-nested-txn-get-range/tenant0/range/b", "outside-tenant")
	require.NoError(t, err)

	txnResp, err := namespacedKV.Txn(ctx).
		Then(clientv3.OpTxn(nil,
			[]clientv3.Op{clientv3.OpGet("range/a", clientv3.WithRange("range/d"), clientv3.WithKeysOnly())},
			nil)).
		Commit()
	require.NoError(t, err)
	require.True(t, txnResp.Succeeded)
	require.Len(t, txnResp.Responses, 1)
	nestedTxn := txnResp.Responses[0].GetResponseTxn()
	require.NotNil(t, nestedTxn)
	require.True(t, nestedTxn.Succeeded)
	require.Len(t, nestedTxn.Responses, 1)
	nestedGet := nestedTxn.Responses[0].GetResponseRange()
	require.NotNil(t, nestedGet)
	require.NotNil(t, nestedGet.Header)
	require.Equal(t, int64(3), nestedGet.Count)
	require.False(t, nestedGet.More)
	require.Len(t, nestedGet.Kvs, 3)
	require.Equal(t, [][]byte{[]byte("range/a"), []byte("range/b"), []byte("range/c")},
		[][]byte{nestedGet.Kvs[0].Key, nestedGet.Kvs[1].Key, nestedGet.Kvs[2].Key})
	for _, kv := range nestedGet.Kvs {
		require.Empty(t, kv.Value)
	}
}

func TestClientNamespaceNestedTxnDeleteWithPrevKVLogicalRange(t *testing.T) {
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
	tenantPrefix := "/a1510/namespace-nested-txn-delete-range/tenant/"
	namespacedKV := namespace.NewKV(client.KV, tenantPrefix)
	for _, key := range []string{"range/a", "range/b", "range/c"} {
		_, err = namespacedKV.Put(ctx, key, "value-"+key)
		require.NoError(t, err)
	}
	_, err = namespacedKV.Put(ctx, "range/d", "outside-upper-bound")
	require.NoError(t, err)
	_, err = namespacedKV.Put(ctx, "other/in-tenant", "other")
	require.NoError(t, err)
	_, err = client.Put(ctx, "/a1510/namespace-nested-txn-delete-range/tenant/range0/outside", "same-tenant-outside-range")
	require.NoError(t, err)
	_, err = client.Put(ctx, "/a1510/namespace-nested-txn-delete-range/tenant0/range/b", "outside-tenant")
	require.NoError(t, err)

	txnResp, err := namespacedKV.Txn(ctx).
		Then(clientv3.OpTxn(nil,
			[]clientv3.Op{clientv3.OpDelete("range/a", clientv3.WithRange("range/d"), clientv3.WithPrevKV())},
			nil)).
		Commit()
	require.NoError(t, err)
	require.True(t, txnResp.Succeeded)
	require.Len(t, txnResp.Responses, 1)
	nestedTxn := txnResp.Responses[0].GetResponseTxn()
	require.NotNil(t, nestedTxn)
	require.True(t, nestedTxn.Succeeded)
	require.Len(t, nestedTxn.Responses, 1)
	nestedDelete := nestedTxn.Responses[0].GetResponseDeleteRange()
	require.NotNil(t, nestedDelete)
	require.NotNil(t, nestedDelete.Header)
	require.Equal(t, int64(3), nestedDelete.Deleted)
	require.Len(t, nestedDelete.PrevKvs, 3)
	require.Equal(t, [][]byte{[]byte("range/a"), []byte("range/b"), []byte("range/c")},
		[][]byte{nestedDelete.PrevKvs[0].Key, nestedDelete.PrevKvs[1].Key, nestedDelete.PrevKvs[2].Key})
	require.Equal(t, [][]byte{[]byte("value-range/a"), []byte("value-range/b"), []byte("value-range/c")},
		[][]byte{nestedDelete.PrevKvs[0].Value, nestedDelete.PrevKvs[1].Value, nestedDelete.PrevKvs[2].Value})

	remaining, err := namespacedKV.Get(ctx, "range/", clientv3.WithPrefix())
	require.NoError(t, err)
	require.Equal(t, int64(1), remaining.Count)
	require.Len(t, remaining.Kvs, 1)
	require.Equal(t, []byte("range/d"), remaining.Kvs[0].Key)
	require.Equal(t, []byte("outside-upper-bound"), remaining.Kvs[0].Value)
}

func TestClientNamespaceNestedTxnPutWithPrevKVReturnsLogicalKey(t *testing.T) {
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
	tenantPrefix := "/a1511/namespace-nested-txn-put-prevkv/tenant/"
	namespacedKV := namespace.NewKV(client.KV, tenantPrefix)
	_, err = namespacedKV.Put(ctx, "items/a", "old-a")
	require.NoError(t, err)
	_, err = client.Put(ctx, "/a1511/namespace-nested-txn-put-prevkv/tenant0/items/a", "outside-tenant")
	require.NoError(t, err)

	txnResp, err := namespacedKV.Txn(ctx).
		Then(clientv3.OpTxn(nil,
			[]clientv3.Op{clientv3.OpPut("items/a", "new-a", clientv3.WithPrevKV())},
			nil)).
		Commit()
	require.NoError(t, err)
	require.True(t, txnResp.Succeeded)
	require.Len(t, txnResp.Responses, 1)
	nestedTxn := txnResp.Responses[0].GetResponseTxn()
	require.NotNil(t, nestedTxn)
	require.True(t, nestedTxn.Succeeded)
	require.Len(t, nestedTxn.Responses, 1)
	nestedPut := nestedTxn.Responses[0].GetResponsePut()
	require.NotNil(t, nestedPut)
	require.NotNil(t, nestedPut.Header)
	require.NotNil(t, nestedPut.PrevKv)
	require.Equal(t, []byte("items/a"), nestedPut.PrevKv.Key)
	require.Equal(t, []byte("old-a"), nestedPut.PrevKv.Value)

	updated, err := namespacedKV.Get(ctx, "items/a")
	require.NoError(t, err)
	require.Len(t, updated.Kvs, 1)
	require.Equal(t, []byte("items/a"), updated.Kvs[0].Key)
	require.Equal(t, []byte("new-a"), updated.Kvs[0].Value)
}

func TestClientNamespaceNestedTxnRangeCompareWithLogicalRange(t *testing.T) {
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
	tenantPrefix := "/a1512/namespace-nested-txn-compare-range/tenant/"
	namespacedKV := namespace.NewKV(client.KV, tenantPrefix)
	for _, key := range []string{"range/a", "range/b", "range/c"} {
		_, err = namespacedKV.Put(ctx, key, "value-"+key)
		require.NoError(t, err)
	}
	_, err = namespacedKV.Put(ctx, "range/d", "outside-upper-bound")
	require.NoError(t, err)
	_, err = client.Put(ctx, "/a1512/namespace-nested-txn-compare-range/tenant/range0/outside", "same-tenant-outside-range")
	require.NoError(t, err)
	_, err = client.Put(ctx, "/a1512/namespace-nested-txn-compare-range/tenant0/range/b", "outside-tenant")
	require.NoError(t, err)

	txnResp, err := namespacedKV.Txn(ctx).
		Then(clientv3.OpTxn(
			[]clientv3.Cmp{clientv3.Compare(clientv3.Version("range/a").WithRange("range/d"), ">", 0)},
			[]clientv3.Op{clientv3.OpGet("range/a", clientv3.WithRange("range/d"), clientv3.WithKeysOnly())},
			[]clientv3.Op{clientv3.OpGet("range/a", clientv3.WithRange("range/d"))},
		)).
		Commit()
	require.NoError(t, err)
	require.True(t, txnResp.Succeeded)
	require.Len(t, txnResp.Responses, 1)
	nestedTxn := txnResp.Responses[0].GetResponseTxn()
	require.NotNil(t, nestedTxn)
	require.True(t, nestedTxn.Succeeded)
	require.Len(t, nestedTxn.Responses, 1)
	nestedGet := nestedTxn.Responses[0].GetResponseRange()
	require.NotNil(t, nestedGet)
	require.NotNil(t, nestedGet.Header)
	require.Equal(t, int64(3), nestedGet.Count)
	require.False(t, nestedGet.More)
	require.Len(t, nestedGet.Kvs, 3)
	require.Equal(t, [][]byte{[]byte("range/a"), []byte("range/b"), []byte("range/c")},
		[][]byte{nestedGet.Kvs[0].Key, nestedGet.Kvs[1].Key, nestedGet.Kvs[2].Key})
	require.Equal(t, []byte(nil), nestedGet.Kvs[0].Value)
	require.Equal(t, []byte(nil), nestedGet.Kvs[1].Value)
	require.Equal(t, []byte(nil), nestedGet.Kvs[2].Value)
}

func TestClientNamespaceNestedTxnRangeCompareFalseBranchIgnoresAdjacentRanges(t *testing.T) {
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
	tenantPrefix := "/a1513/namespace-nested-txn-compare-false-range/tenant/"
	namespacedKV := namespace.NewKV(client.KV, tenantPrefix)
	_, err = namespacedKV.Put(ctx, "range/d", "outside-upper-bound")
	require.NoError(t, err)
	_, err = client.Put(ctx, "/a1513/namespace-nested-txn-compare-false-range/tenant/range0/outside", "same-tenant-outside-range")
	require.NoError(t, err)
	_, err = client.Put(ctx, "/a1513/namespace-nested-txn-compare-false-range/tenant0/range/b", "outside-tenant")
	require.NoError(t, err)

	txnResp, err := namespacedKV.Txn(ctx).
		Then(clientv3.OpTxn(
			[]clientv3.Cmp{clientv3.Compare(clientv3.Version("range/a").WithRange("range/d"), ">", 0)},
			[]clientv3.Op{clientv3.OpGet("range/a", clientv3.WithRange("range/d"), clientv3.WithKeysOnly())},
			[]clientv3.Op{clientv3.OpGet("range/a", clientv3.WithRange("range/d"))},
		)).
		Commit()
	require.NoError(t, err)
	require.True(t, txnResp.Succeeded)
	require.Len(t, txnResp.Responses, 1)
	nestedTxn := txnResp.Responses[0].GetResponseTxn()
	require.NotNil(t, nestedTxn)
	require.False(t, nestedTxn.Succeeded)
	require.Len(t, nestedTxn.Responses, 1)
	nestedGet := nestedTxn.Responses[0].GetResponseRange()
	require.NotNil(t, nestedGet)
	require.NotNil(t, nestedGet.Header)
	require.Equal(t, int64(0), nestedGet.Count)
	require.False(t, nestedGet.More)
	require.Empty(t, nestedGet.Kvs)
}

func TestClientNamespaceNestedTxnElseBranchGetReturnsLogicalKeys(t *testing.T) {
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
	tenantPrefix := "/a1514/namespace-nested-txn-else-get/tenant/"
	namespacedKV := namespace.NewKV(client.KV, tenantPrefix)
	_, err = namespacedKV.Put(ctx, "items/a", "value-a")
	require.NoError(t, err)
	_, err = namespacedKV.Put(ctx, "items/b", "value-b")
	require.NoError(t, err)
	_, err = client.Put(ctx, "/a1514/namespace-nested-txn-else-get/tenant0/items/a", "outside-tenant")
	require.NoError(t, err)

	txnResp, err := namespacedKV.Txn(ctx).
		Then(clientv3.OpTxn(
			[]clientv3.Cmp{clientv3.Compare(clientv3.Version("missing"), "=", 1)},
			[]clientv3.Op{clientv3.OpGet("items/", clientv3.WithPrefix(), clientv3.WithKeysOnly())},
			[]clientv3.Op{clientv3.OpGet("items/", clientv3.WithPrefix())},
		)).
		Commit()
	require.NoError(t, err)
	require.True(t, txnResp.Succeeded)
	require.Len(t, txnResp.Responses, 1)
	nestedTxn := txnResp.Responses[0].GetResponseTxn()
	require.NotNil(t, nestedTxn)
	require.False(t, nestedTxn.Succeeded)
	require.Len(t, nestedTxn.Responses, 1)
	nestedGet := nestedTxn.Responses[0].GetResponseRange()
	require.NotNil(t, nestedGet)
	require.NotNil(t, nestedGet.Header)
	require.Equal(t, int64(2), nestedGet.Count)
	require.False(t, nestedGet.More)
	require.Len(t, nestedGet.Kvs, 2)
	require.Equal(t, [][]byte{[]byte("items/a"), []byte("items/b")},
		[][]byte{nestedGet.Kvs[0].Key, nestedGet.Kvs[1].Key})
	require.Equal(t, [][]byte{[]byte("value-a"), []byte("value-b")},
		[][]byte{nestedGet.Kvs[0].Value, nestedGet.Kvs[1].Value})
}

func TestClientNamespaceDoubleNestedTxnGetReturnsLogicalKeys(t *testing.T) {
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
	tenantPrefix := "/a1515/namespace-double-nested-txn-get/tenant/"
	namespacedKV := namespace.NewKV(client.KV, tenantPrefix)
	for _, key := range []string{"range/a", "range/b", "range/c"} {
		_, err = namespacedKV.Put(ctx, key, "value-"+key)
		require.NoError(t, err)
	}
	_, err = namespacedKV.Put(ctx, "range/d", "outside-upper-bound")
	require.NoError(t, err)
	_, err = client.Put(ctx, "/a1515/namespace-double-nested-txn-get/tenant0/range/b", "outside-tenant")
	require.NoError(t, err)

	txnResp, err := namespacedKV.Txn(ctx).
		Then(clientv3.OpTxn(nil,
			[]clientv3.Op{clientv3.OpTxn(nil,
				[]clientv3.Op{clientv3.OpGet("range/a", clientv3.WithRange("range/d"))},
				nil,
			)},
			nil)).
		Commit()
	require.NoError(t, err)
	require.True(t, txnResp.Succeeded)
	require.Len(t, txnResp.Responses, 1)
	firstNestedTxn := txnResp.Responses[0].GetResponseTxn()
	require.NotNil(t, firstNestedTxn)
	require.True(t, firstNestedTxn.Succeeded)
	require.Len(t, firstNestedTxn.Responses, 1)
	secondNestedTxn := firstNestedTxn.Responses[0].GetResponseTxn()
	require.NotNil(t, secondNestedTxn)
	require.True(t, secondNestedTxn.Succeeded)
	require.Len(t, secondNestedTxn.Responses, 1)
	nestedGet := secondNestedTxn.Responses[0].GetResponseRange()
	require.NotNil(t, nestedGet)
	require.NotNil(t, nestedGet.Header)
	require.Equal(t, int64(3), nestedGet.Count)
	require.False(t, nestedGet.More)
	require.Len(t, nestedGet.Kvs, 3)
	require.Equal(t, [][]byte{[]byte("range/a"), []byte("range/b"), []byte("range/c")},
		[][]byte{nestedGet.Kvs[0].Key, nestedGet.Kvs[1].Key, nestedGet.Kvs[2].Key})
	require.Equal(t, [][]byte{[]byte("value-range/a"), []byte("value-range/b"), []byte("value-range/c")},
		[][]byte{nestedGet.Kvs[0].Value, nestedGet.Kvs[1].Value, nestedGet.Kvs[2].Value})
}

func TestClientNamespaceNestedTxnGetFromKeyStaysWithinNamespace(t *testing.T) {
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
	tenantPrefix := "/a1516/namespace-nested-txn-get-from-key/tenant/"
	namespacedKV := namespace.NewKV(client.KV, tenantPrefix)
	_, err = namespacedKV.Put(ctx, "range/a", "before-start")
	require.NoError(t, err)
	_, err = namespacedKV.Put(ctx, "range/b", "value-b")
	require.NoError(t, err)
	_, err = namespacedKV.Put(ctx, "z/final", "value-z")
	require.NoError(t, err)
	_, err = client.Put(ctx, "/a1516/namespace-nested-txn-get-from-key/tenant0/range/b", "outside-tenant")
	require.NoError(t, err)
	_, err = client.Put(ctx, "/a1516/namespace-nested-txn-get-from-key/tenant0/z/final", "outside-tenant-z")
	require.NoError(t, err)

	txnResp, err := namespacedKV.Txn(ctx).
		Then(clientv3.OpTxn(nil,
			[]clientv3.Op{clientv3.OpGet("range/b", clientv3.WithFromKey())},
			nil)).
		Commit()
	require.NoError(t, err)
	require.True(t, txnResp.Succeeded)
	require.Len(t, txnResp.Responses, 1)
	nestedTxn := txnResp.Responses[0].GetResponseTxn()
	require.NotNil(t, nestedTxn)
	require.True(t, nestedTxn.Succeeded)
	require.Len(t, nestedTxn.Responses, 1)
	nestedGet := nestedTxn.Responses[0].GetResponseRange()
	require.NotNil(t, nestedGet)
	require.NotNil(t, nestedGet.Header)
	require.Equal(t, int64(2), nestedGet.Count)
	require.False(t, nestedGet.More)
	require.Len(t, nestedGet.Kvs, 2)
	require.Equal(t, [][]byte{[]byte("range/b"), []byte("z/final")},
		[][]byte{nestedGet.Kvs[0].Key, nestedGet.Kvs[1].Key})
	require.Equal(t, [][]byte{[]byte("value-b"), []byte("value-z")},
		[][]byte{nestedGet.Kvs[0].Value, nestedGet.Kvs[1].Value})
}

func TestClientNamespaceNestedTxnDeleteFromKeyStaysWithinNamespace(t *testing.T) {
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
	tenantPrefix := "/a1517/namespace-nested-txn-delete-from-key/tenant/"
	namespacedKV := namespace.NewKV(client.KV, tenantPrefix)
	_, err = namespacedKV.Put(ctx, "range/a", "before-start")
	require.NoError(t, err)
	_, err = namespacedKV.Put(ctx, "range/b", "value-b")
	require.NoError(t, err)
	_, err = namespacedKV.Put(ctx, "z/final", "value-z")
	require.NoError(t, err)
	_, err = client.Put(ctx, "/a1517/namespace-nested-txn-delete-from-key/tenant0/range/b", "outside-tenant")
	require.NoError(t, err)
	_, err = client.Put(ctx, "/a1517/namespace-nested-txn-delete-from-key/tenant0/z/final", "outside-tenant-z")
	require.NoError(t, err)

	txnResp, err := namespacedKV.Txn(ctx).
		Then(clientv3.OpTxn(nil,
			[]clientv3.Op{clientv3.OpDelete("range/b", clientv3.WithFromKey(), clientv3.WithPrevKV())},
			nil)).
		Commit()
	require.NoError(t, err)
	require.True(t, txnResp.Succeeded)
	require.Len(t, txnResp.Responses, 1)
	nestedTxn := txnResp.Responses[0].GetResponseTxn()
	require.NotNil(t, nestedTxn)
	require.True(t, nestedTxn.Succeeded)
	require.Len(t, nestedTxn.Responses, 1)
	nestedDelete := nestedTxn.Responses[0].GetResponseDeleteRange()
	require.NotNil(t, nestedDelete)
	require.NotNil(t, nestedDelete.Header)
	require.Equal(t, int64(2), nestedDelete.Deleted)
	require.Len(t, nestedDelete.PrevKvs, 2)
	require.Equal(t, [][]byte{[]byte("range/b"), []byte("z/final")},
		[][]byte{nestedDelete.PrevKvs[0].Key, nestedDelete.PrevKvs[1].Key})
	require.Equal(t, [][]byte{[]byte("value-b"), []byte("value-z")},
		[][]byte{nestedDelete.PrevKvs[0].Value, nestedDelete.PrevKvs[1].Value})

	remaining, err := namespacedKV.Get(ctx, "", clientv3.WithFromKey())
	require.NoError(t, err)
	require.Equal(t, int64(1), remaining.Count)
	require.Len(t, remaining.Kvs, 1)
	require.Equal(t, []byte("range/a"), remaining.Kvs[0].Key)
	require.Equal(t, []byte("before-start"), remaining.Kvs[0].Value)
	outsideTenant, err := client.Get(ctx, "/a1517/namespace-nested-txn-delete-from-key/tenant0/", clientv3.WithPrefix())
	require.NoError(t, err)
	require.Equal(t, int64(2), outsideTenant.Count)
}

func TestClientNamespaceNestedTxnFromKeyCompareStaysWithinNamespace(t *testing.T) {
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
	tenantPrefix := "/a1518/namespace-nested-txn-compare-from-key/tenant/"
	namespacedKV := namespace.NewKV(client.KV, tenantPrefix)
	_, err = client.Put(ctx, "/a1518/namespace-nested-txn-compare-from-key/tenant0/range/b", "outside-tenant")
	require.NoError(t, err)
	_, err = client.Put(ctx, "/a1518/namespace-nested-txn-compare-from-key/tenant0/z/final", "outside-tenant-z")
	require.NoError(t, err)

	txnResp, err := namespacedKV.Txn(ctx).
		Then(clientv3.OpTxn(
			[]clientv3.Cmp{clientv3.Compare(clientv3.Version("range/b").WithRange("\x00"), ">", 0)},
			[]clientv3.Op{clientv3.OpPut("compare/result", "then")},
			[]clientv3.Op{clientv3.OpPut("compare/result", "else")},
		)).
		Commit()
	require.NoError(t, err)
	require.True(t, txnResp.Succeeded)
	require.Len(t, txnResp.Responses, 1)
	nestedTxn := txnResp.Responses[0].GetResponseTxn()
	require.NotNil(t, nestedTxn)
	require.False(t, nestedTxn.Succeeded)
	require.Len(t, nestedTxn.Responses, 1)
	require.NotNil(t, nestedTxn.Responses[0].GetResponsePut())

	result, err := namespacedKV.Get(ctx, "compare/result")
	require.NoError(t, err)
	require.Len(t, result.Kvs, 1)
	require.Equal(t, []byte("compare/result"), result.Kvs[0].Key)
	require.Equal(t, []byte("else"), result.Kvs[0].Value)
	outsideTenant, err := client.Get(ctx, "/a1518/namespace-nested-txn-compare-from-key/tenant0/", clientv3.WithPrefix())
	require.NoError(t, err)
	require.Equal(t, int64(2), outsideTenant.Count)
}

func TestClientNamespaceNestedTxnGetWithMinModRevisionReturnsLogicalKeys(t *testing.T) {
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
	tenantPrefix := "/a1519/namespace-nested-txn-min-mod-rev/tenant/"
	namespacedKV := namespace.NewKV(client.KV, tenantPrefix)
	_, err = namespacedKV.Put(ctx, "items/a", "old-a")
	require.NoError(t, err)
	_, err = namespacedKV.Put(ctx, "items/b", "old-b")
	require.NoError(t, err)
	updateB, err := namespacedKV.Put(ctx, "items/b", "new-b")
	require.NoError(t, err)
	_, err = client.Put(ctx, "/a1519/namespace-nested-txn-min-mod-rev/tenant/items0/outside", "same-tenant-outside-prefix")
	require.NoError(t, err)
	_, err = client.Put(ctx, "/a1519/namespace-nested-txn-min-mod-rev/tenant0/items/b", "outside-tenant")
	require.NoError(t, err)

	txnResp, err := namespacedKV.Txn(ctx).
		Then(clientv3.OpTxn(nil,
			[]clientv3.Op{clientv3.OpGet("items/",
				clientv3.WithPrefix(),
				clientv3.WithMinModRev(updateB.Header.Revision),
			)},
			nil)).
		Commit()
	require.NoError(t, err)
	require.True(t, txnResp.Succeeded)
	require.Len(t, txnResp.Responses, 1)
	nestedTxn := txnResp.Responses[0].GetResponseTxn()
	require.NotNil(t, nestedTxn)
	require.True(t, nestedTxn.Succeeded)
	require.Len(t, nestedTxn.Responses, 1)
	nestedGet := nestedTxn.Responses[0].GetResponseRange()
	require.NotNil(t, nestedGet)
	require.NotNil(t, nestedGet.Header)
	require.Equal(t, int64(2), nestedGet.Count)
	require.False(t, nestedGet.More)
	require.Len(t, nestedGet.Kvs, 1)
	require.Equal(t, []byte("items/b"), nestedGet.Kvs[0].Key)
	require.Equal(t, []byte("new-b"), nestedGet.Kvs[0].Value)
}

func TestClientNamespaceNestedTxnGetWithMaxModRevisionReturnsLogicalKeys(t *testing.T) {
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
	tenantPrefix := "/a1520/namespace-nested-txn-max-mod-rev/tenant/"
	namespacedKV := namespace.NewKV(client.KV, tenantPrefix)
	putA, err := namespacedKV.Put(ctx, "items/a", "old-a")
	require.NoError(t, err)
	_, err = namespacedKV.Put(ctx, "items/b", "old-b")
	require.NoError(t, err)
	_, err = namespacedKV.Put(ctx, "items/b", "new-b")
	require.NoError(t, err)
	_, err = client.Put(ctx, "/a1520/namespace-nested-txn-max-mod-rev/tenant/items0/outside", "same-tenant-outside-prefix")
	require.NoError(t, err)
	_, err = client.Put(ctx, "/a1520/namespace-nested-txn-max-mod-rev/tenant0/items/a", "outside-tenant")
	require.NoError(t, err)

	txnResp, err := namespacedKV.Txn(ctx).
		Then(clientv3.OpTxn(nil,
			[]clientv3.Op{clientv3.OpGet("items/",
				clientv3.WithPrefix(),
				clientv3.WithMaxModRev(putA.Header.Revision),
			)},
			nil)).
		Commit()
	require.NoError(t, err)
	require.True(t, txnResp.Succeeded)
	require.Len(t, txnResp.Responses, 1)
	nestedTxn := txnResp.Responses[0].GetResponseTxn()
	require.NotNil(t, nestedTxn)
	require.True(t, nestedTxn.Succeeded)
	require.Len(t, nestedTxn.Responses, 1)
	nestedGet := nestedTxn.Responses[0].GetResponseRange()
	require.NotNil(t, nestedGet)
	require.NotNil(t, nestedGet.Header)
	require.Equal(t, int64(2), nestedGet.Count)
	require.False(t, nestedGet.More)
	require.Len(t, nestedGet.Kvs, 1)
	require.Equal(t, []byte("items/a"), nestedGet.Kvs[0].Key)
	require.Equal(t, []byte("old-a"), nestedGet.Kvs[0].Value)
}

func TestClientNamespaceNestedTxnGetWithMinCreateRevisionReturnsLogicalKeys(t *testing.T) {
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
	tenantPrefix := "/a1521/namespace-nested-txn-min-create-rev/tenant/"
	namespacedKV := namespace.NewKV(client.KV, tenantPrefix)
	for _, key := range []string{"items/a", "items/b"} {
		_, err = namespacedKV.Put(ctx, key, "value-"+key)
		require.NoError(t, err)
	}
	createC, err := namespacedKV.Put(ctx, "items/c", "value-items/c")
	require.NoError(t, err)
	_, err = client.Put(ctx, "/a1521/namespace-nested-txn-min-create-rev/tenant/items0/outside", "same-tenant-outside-prefix")
	require.NoError(t, err)
	_, err = client.Put(ctx, "/a1521/namespace-nested-txn-min-create-rev/tenant0/items/c", "outside-tenant")
	require.NoError(t, err)

	txnResp, err := namespacedKV.Txn(ctx).
		Then(clientv3.OpTxn(nil,
			[]clientv3.Op{clientv3.OpGet("items/",
				clientv3.WithPrefix(),
				clientv3.WithMinCreateRev(createC.Header.Revision),
				clientv3.WithSort(clientv3.SortByKey, clientv3.SortAscend),
			)},
			nil)).
		Commit()
	require.NoError(t, err)
	require.True(t, txnResp.Succeeded)
	require.Len(t, txnResp.Responses, 1)
	nestedTxn := txnResp.Responses[0].GetResponseTxn()
	require.NotNil(t, nestedTxn)
	require.True(t, nestedTxn.Succeeded)
	require.Len(t, nestedTxn.Responses, 1)
	nestedGet := nestedTxn.Responses[0].GetResponseRange()
	require.NotNil(t, nestedGet)
	require.NotNil(t, nestedGet.Header)
	require.Equal(t, int64(3), nestedGet.Count)
	require.False(t, nestedGet.More)
	require.Len(t, nestedGet.Kvs, 1)
	require.Equal(t, []byte("items/c"), nestedGet.Kvs[0].Key)
	require.Equal(t, []byte("value-items/c"), nestedGet.Kvs[0].Value)
}

func TestClientNamespaceNestedTxnGetWithMaxCreateRevisionReturnsLogicalKeys(t *testing.T) {
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
	tenantPrefix := "/a1522/namespace-nested-txn-max-create-rev/tenant/"
	namespacedKV := namespace.NewKV(client.KV, tenantPrefix)
	for _, key := range []string{"items/a", "items/b"} {
		_, err = namespacedKV.Put(ctx, key, "value-"+key)
		require.NoError(t, err)
	}
	createC, err := namespacedKV.Put(ctx, "items/c", "value-items/c")
	require.NoError(t, err)
	_, err = client.Put(ctx, "/a1522/namespace-nested-txn-max-create-rev/tenant/items0/outside", "same-tenant-outside-prefix")
	require.NoError(t, err)
	_, err = client.Put(ctx, "/a1522/namespace-nested-txn-max-create-rev/tenant0/items/a", "outside-tenant")
	require.NoError(t, err)

	txnResp, err := namespacedKV.Txn(ctx).
		Then(clientv3.OpTxn(nil,
			[]clientv3.Op{clientv3.OpGet("items/",
				clientv3.WithPrefix(),
				clientv3.WithMaxCreateRev(createC.Header.Revision-1),
				clientv3.WithSort(clientv3.SortByKey, clientv3.SortAscend),
			)},
			nil)).
		Commit()
	require.NoError(t, err)
	require.True(t, txnResp.Succeeded)
	require.Len(t, txnResp.Responses, 1)
	nestedTxn := txnResp.Responses[0].GetResponseTxn()
	require.NotNil(t, nestedTxn)
	require.True(t, nestedTxn.Succeeded)
	require.Len(t, nestedTxn.Responses, 1)
	nestedGet := nestedTxn.Responses[0].GetResponseRange()
	require.NotNil(t, nestedGet)
	require.NotNil(t, nestedGet.Header)
	require.Equal(t, int64(3), nestedGet.Count)
	require.False(t, nestedGet.More)
	require.Len(t, nestedGet.Kvs, 2)
	require.Equal(t, [][]byte{[]byte("items/a"), []byte("items/b")},
		[][]byte{nestedGet.Kvs[0].Key, nestedGet.Kvs[1].Key})
	require.Equal(t, [][]byte{[]byte("value-items/a"), []byte("value-items/b")},
		[][]byte{nestedGet.Kvs[0].Value, nestedGet.Kvs[1].Value})
}

func TestClientNamespaceNestedTxnGetWithFirstCreateReturnsLogicalKey(t *testing.T) {
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
	tenantPrefix := "/a1523/namespace-nested-txn-first-create/tenant/"
	namespacedKV := namespace.NewKV(client.KV, tenantPrefix)
	_, err = namespacedKV.Put(ctx, "other/oldest", "other")
	require.NoError(t, err)
	for _, key := range []string{"waiters/b", "waiters/a", "waiters/c"} {
		_, err = namespacedKV.Put(ctx, key, "value-"+key)
		require.NoError(t, err)
	}
	_, err = client.Put(ctx, "/a1523/namespace-nested-txn-first-create/tenant/waiters0/outside", "same-tenant-outside-prefix")
	require.NoError(t, err)
	_, err = client.Put(ctx, "/a1523/namespace-nested-txn-first-create/tenant0/waiters/outside", "outside-tenant")
	require.NoError(t, err)

	txnResp, err := namespacedKV.Txn(ctx).
		Then(clientv3.OpTxn(nil,
			[]clientv3.Op{clientv3.OpGet("waiters/", clientv3.WithFirstCreate()...)},
			nil)).
		Commit()
	require.NoError(t, err)
	require.True(t, txnResp.Succeeded)
	require.Len(t, txnResp.Responses, 1)
	nestedTxn := txnResp.Responses[0].GetResponseTxn()
	require.NotNil(t, nestedTxn)
	require.True(t, nestedTxn.Succeeded)
	require.Len(t, nestedTxn.Responses, 1)
	nestedGet := nestedTxn.Responses[0].GetResponseRange()
	require.NotNil(t, nestedGet)
	require.NotNil(t, nestedGet.Header)
	require.Equal(t, int64(3), nestedGet.Count)
	require.True(t, nestedGet.More)
	require.Len(t, nestedGet.Kvs, 1)
	require.Equal(t, []byte("waiters/b"), nestedGet.Kvs[0].Key)
	require.Equal(t, []byte("value-waiters/b"), nestedGet.Kvs[0].Value)
}

func TestClientNamespaceNestedTxnGetWithFirstRevReturnsLogicalKey(t *testing.T) {
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
	tenantPrefix := "/a1524/namespace-nested-txn-first-rev/tenant/"
	namespacedKV := namespace.NewKV(client.KV, tenantPrefix)
	_, err = namespacedKV.Put(ctx, "other/oldest", "other")
	require.NoError(t, err)
	for _, key := range []string{"queue/b", "queue/a", "queue/c"} {
		_, err = namespacedKV.Put(ctx, key, "value-"+key)
		require.NoError(t, err)
	}
	_, err = namespacedKV.Put(ctx, "queue/b", "updated-queue/b")
	require.NoError(t, err)
	_, err = client.Put(ctx, "/a1524/namespace-nested-txn-first-rev/tenant/queue0/outside", "same-tenant-outside-prefix")
	require.NoError(t, err)
	_, err = client.Put(ctx, "/a1524/namespace-nested-txn-first-rev/tenant0/queue/a", "outside-tenant")
	require.NoError(t, err)

	txnResp, err := namespacedKV.Txn(ctx).
		Then(clientv3.OpTxn(nil,
			[]clientv3.Op{clientv3.OpGet("queue/", clientv3.WithFirstRev()...)},
			nil)).
		Commit()
	require.NoError(t, err)
	require.True(t, txnResp.Succeeded)
	require.Len(t, txnResp.Responses, 1)
	nestedTxn := txnResp.Responses[0].GetResponseTxn()
	require.NotNil(t, nestedTxn)
	require.True(t, nestedTxn.Succeeded)
	require.Len(t, nestedTxn.Responses, 1)
	nestedGet := nestedTxn.Responses[0].GetResponseRange()
	require.NotNil(t, nestedGet)
	require.NotNil(t, nestedGet.Header)
	require.Equal(t, int64(3), nestedGet.Count)
	require.True(t, nestedGet.More)
	require.Len(t, nestedGet.Kvs, 1)
	require.Equal(t, []byte("queue/a"), nestedGet.Kvs[0].Key)
	require.Equal(t, []byte("value-queue/a"), nestedGet.Kvs[0].Value)
}

func TestClientNamespaceNestedTxnGetWithLastRevReturnsLogicalKey(t *testing.T) {
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
	tenantPrefix := "/a1525/namespace-nested-txn-last-rev/tenant/"
	namespacedKV := namespace.NewKV(client.KV, tenantPrefix)
	for _, key := range []string{"queue/b", "queue/a", "queue/c"} {
		_, err = namespacedKV.Put(ctx, key, "value-"+key)
		require.NoError(t, err)
	}
	_, err = namespacedKV.Put(ctx, "queue/b", "updated-queue/b")
	require.NoError(t, err)
	_, err = namespacedKV.Put(ctx, "other/newer", "other")
	require.NoError(t, err)
	_, err = client.Put(ctx, "/a1525/namespace-nested-txn-last-rev/tenant/queue0/outside", "same-tenant-outside-prefix")
	require.NoError(t, err)
	_, err = client.Put(ctx, "/a1525/namespace-nested-txn-last-rev/tenant0/queue/outside", "outside-tenant")
	require.NoError(t, err)

	txnResp, err := namespacedKV.Txn(ctx).
		Then(clientv3.OpTxn(nil,
			[]clientv3.Op{clientv3.OpGet("queue/", clientv3.WithLastRev()...)},
			nil)).
		Commit()
	require.NoError(t, err)
	require.True(t, txnResp.Succeeded)
	require.Len(t, txnResp.Responses, 1)
	nestedTxn := txnResp.Responses[0].GetResponseTxn()
	require.NotNil(t, nestedTxn)
	require.True(t, nestedTxn.Succeeded)
	require.Len(t, nestedTxn.Responses, 1)
	nestedGet := nestedTxn.Responses[0].GetResponseRange()
	require.NotNil(t, nestedGet)
	require.NotNil(t, nestedGet.Header)
	require.Equal(t, int64(3), nestedGet.Count)
	require.True(t, nestedGet.More)
	require.Len(t, nestedGet.Kvs, 1)
	require.Equal(t, []byte("queue/b"), nestedGet.Kvs[0].Key)
	require.Equal(t, []byte("updated-queue/b"), nestedGet.Kvs[0].Value)
}

func TestClientNamespaceNestedTxnGetWithLastCreateAndMaxCreateRevisionReturnsLogicalKey(t *testing.T) {
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
	tenantPrefix := "/a1526/namespace-nested-txn-last-create-max-create/tenant/"
	namespacedKV := namespace.NewKV(client.KV, tenantPrefix)
	for _, key := range []string{"waiters/a", "waiters/b"} {
		_, err = namespacedKV.Put(ctx, key, "value-"+key)
		require.NoError(t, err)
	}
	createC, err := namespacedKV.Put(ctx, "waiters/c", "value-waiters/c")
	require.NoError(t, err)
	_, err = namespacedKV.Put(ctx, "other/newer", "other")
	require.NoError(t, err)
	_, err = client.Put(ctx, "/a1526/namespace-nested-txn-last-create-max-create/tenant/waiters0/outside", "same-tenant-outside-prefix")
	require.NoError(t, err)
	_, err = client.Put(ctx, "/a1526/namespace-nested-txn-last-create-max-create/tenant0/waiters/outside", "outside-tenant")
	require.NoError(t, err)

	getOpts := append(clientv3.WithLastCreate(), clientv3.WithMaxCreateRev(createC.Header.Revision-1))
	txnResp, err := namespacedKV.Txn(ctx).
		Then(clientv3.OpTxn(nil,
			[]clientv3.Op{clientv3.OpGet("waiters/", getOpts...)},
			nil)).
		Commit()
	require.NoError(t, err)
	require.True(t, txnResp.Succeeded)
	require.Len(t, txnResp.Responses, 1)
	nestedTxn := txnResp.Responses[0].GetResponseTxn()
	require.NotNil(t, nestedTxn)
	require.True(t, nestedTxn.Succeeded)
	require.Len(t, nestedTxn.Responses, 1)
	nestedGet := nestedTxn.Responses[0].GetResponseRange()
	require.NotNil(t, nestedGet)
	require.NotNil(t, nestedGet.Header)
	require.Equal(t, int64(3), nestedGet.Count)
	require.True(t, nestedGet.More)
	require.Len(t, nestedGet.Kvs, 1)
	require.Equal(t, []byte("waiters/b"), nestedGet.Kvs[0].Key)
	require.Equal(t, []byte("value-waiters/b"), nestedGet.Kvs[0].Value)
}

func TestClientNamespaceNestedTxnGetWithLastRevAndMaxModRevisionReturnsLogicalKey(t *testing.T) {
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
	tenantPrefix := "/a1527/namespace-nested-txn-last-rev-max-mod/tenant/"
	namespacedKV := namespace.NewKV(client.KV, tenantPrefix)
	for _, key := range []string{"locks/a", "locks/b", "locks/c"} {
		_, err = namespacedKV.Put(ctx, key, "value-"+key)
		require.NoError(t, err)
	}
	updateB, err := namespacedKV.Put(ctx, "locks/b", "updated-locks/b")
	require.NoError(t, err)
	_, err = namespacedKV.Put(ctx, "locks/c", "updated-locks/c")
	require.NoError(t, err)
	_, err = namespacedKV.Put(ctx, "other/newer", "other")
	require.NoError(t, err)
	_, err = client.Put(ctx, "/a1527/namespace-nested-txn-last-rev-max-mod/tenant/locks0/outside", "same-tenant-outside-prefix")
	require.NoError(t, err)
	_, err = client.Put(ctx, "/a1527/namespace-nested-txn-last-rev-max-mod/tenant0/locks/outside", "outside-tenant")
	require.NoError(t, err)

	getOpts := append(clientv3.WithLastRev(), clientv3.WithMaxModRev(updateB.Header.Revision))
	txnResp, err := namespacedKV.Txn(ctx).
		Then(clientv3.OpTxn(nil,
			[]clientv3.Op{clientv3.OpGet("locks/", getOpts...)},
			nil)).
		Commit()
	require.NoError(t, err)
	require.True(t, txnResp.Succeeded)
	require.Len(t, txnResp.Responses, 1)
	nestedTxn := txnResp.Responses[0].GetResponseTxn()
	require.NotNil(t, nestedTxn)
	require.True(t, nestedTxn.Succeeded)
	require.Len(t, nestedTxn.Responses, 1)
	nestedGet := nestedTxn.Responses[0].GetResponseRange()
	require.NotNil(t, nestedGet)
	require.NotNil(t, nestedGet.Header)
	require.Equal(t, int64(3), nestedGet.Count)
	require.True(t, nestedGet.More)
	require.Len(t, nestedGet.Kvs, 1)
	require.Equal(t, []byte("locks/b"), nestedGet.Kvs[0].Key)
	require.Equal(t, []byte("updated-locks/b"), nestedGet.Kvs[0].Value)
}

func TestClientNamespaceNestedTxnGetWithLastRevAndLogicalRangeReturnsLogicalKey(t *testing.T) {
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
	tenantPrefix := "/a1528/namespace-nested-txn-last-rev-range/tenant/"
	namespacedKV := namespace.NewKV(client.KV, tenantPrefix)
	for _, key := range []string{"range/a", "range/b", "range/c"} {
		_, err = namespacedKV.Put(ctx, key, "value-"+key)
		require.NoError(t, err)
	}
	_, err = namespacedKV.Put(ctx, "range/b", "updated-range/b")
	require.NoError(t, err)
	_, err = namespacedKV.Put(ctx, "range/d", "outside-upper-bound")
	require.NoError(t, err)
	_, err = namespacedKV.Put(ctx, "other/newer", "other")
	require.NoError(t, err)
	_, err = client.Put(ctx, "/a1528/namespace-nested-txn-last-rev-range/tenant/range0/outside", "same-tenant-outside-range")
	require.NoError(t, err)
	_, err = client.Put(ctx, "/a1528/namespace-nested-txn-last-rev-range/tenant0/range/b", "outside-tenant")
	require.NoError(t, err)

	getOpts := append(clientv3.WithLastRev(), clientv3.WithRange("range/d"))
	txnResp, err := namespacedKV.Txn(ctx).
		Then(clientv3.OpTxn(nil,
			[]clientv3.Op{clientv3.OpGet("range/a", getOpts...)},
			nil)).
		Commit()
	require.NoError(t, err)
	require.True(t, txnResp.Succeeded)
	require.Len(t, txnResp.Responses, 1)
	nestedTxn := txnResp.Responses[0].GetResponseTxn()
	require.NotNil(t, nestedTxn)
	require.True(t, nestedTxn.Succeeded)
	require.Len(t, nestedTxn.Responses, 1)
	nestedGet := nestedTxn.Responses[0].GetResponseRange()
	require.NotNil(t, nestedGet)
	require.NotNil(t, nestedGet.Header)
	require.Equal(t, int64(3), nestedGet.Count)
	require.True(t, nestedGet.More)
	require.Len(t, nestedGet.Kvs, 1)
	require.Equal(t, []byte("range/b"), nestedGet.Kvs[0].Key)
	require.Equal(t, []byte("updated-range/b"), nestedGet.Kvs[0].Value)
}

func TestClientNamespaceNestedTxnGetCountOnlyIgnoresLimitWithinNamespace(t *testing.T) {
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
	tenantPrefix := "/a1529/namespace-nested-txn-count-only/tenant/"
	namespacedKV := namespace.NewKV(client.KV, tenantPrefix)
	for _, key := range []string{"items/a", "items/b", "items/c"} {
		_, err = namespacedKV.Put(ctx, key, "value-"+key)
		require.NoError(t, err)
	}
	_, err = client.Put(ctx, "/a1529/namespace-nested-txn-count-only/tenant/items0/outside", "same-tenant-outside-prefix")
	require.NoError(t, err)
	_, err = client.Put(ctx, "/a1529/namespace-nested-txn-count-only/tenant0/items/a", "outside-tenant")
	require.NoError(t, err)

	txnResp, err := namespacedKV.Txn(ctx).
		Then(clientv3.OpTxn(nil,
			[]clientv3.Op{clientv3.OpGet("items/",
				clientv3.WithPrefix(),
				clientv3.WithKeysOnly(),
				clientv3.WithCountOnly(),
				clientv3.WithLimit(1),
			)},
			nil)).
		Commit()
	require.NoError(t, err)
	require.True(t, txnResp.Succeeded)
	require.Len(t, txnResp.Responses, 1)
	nestedTxn := txnResp.Responses[0].GetResponseTxn()
	require.NotNil(t, nestedTxn)
	require.True(t, nestedTxn.Succeeded)
	require.Len(t, nestedTxn.Responses, 1)
	nestedGet := nestedTxn.Responses[0].GetResponseRange()
	require.NotNil(t, nestedGet)
	require.NotNil(t, nestedGet.Header)
	require.Equal(t, int64(3), nestedGet.Count)
	require.False(t, nestedGet.More)
	require.Empty(t, nestedGet.Kvs)
}

func TestClientNamespaceNestedTxnGetSortByValueWithLimitReturnsLogicalKeys(t *testing.T) {
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
	tenantPrefix := "/a1530/namespace-nested-txn-sort-value-limit/tenant/"
	namespacedKV := namespace.NewKV(client.KV, tenantPrefix)
	for _, seed := range []struct {
		key   string
		value string
	}{
		{key: "items/d", value: "n"},
		{key: "items/c", value: "a"},
		{key: "items/b", value: "m"},
		{key: "items/a", value: "z"},
	} {
		_, err = namespacedKV.Put(ctx, seed.key, seed.value)
		require.NoError(t, err)
	}
	_, err = client.Put(ctx, "/a1530/namespace-nested-txn-sort-value-limit/tenant/items0/outside", "0")
	require.NoError(t, err)
	_, err = client.Put(ctx, "/a1530/namespace-nested-txn-sort-value-limit/tenant0/items/e", "0")
	require.NoError(t, err)

	txnResp, err := namespacedKV.Txn(ctx).
		Then(clientv3.OpTxn(nil,
			[]clientv3.Op{clientv3.OpGet("items/",
				clientv3.WithPrefix(),
				clientv3.WithLimit(2),
				clientv3.WithSort(clientv3.SortByValue, clientv3.SortNone),
			)},
			nil)).
		Commit()
	require.NoError(t, err)
	require.True(t, txnResp.Succeeded)
	require.Len(t, txnResp.Responses, 1)
	nestedTxn := txnResp.Responses[0].GetResponseTxn()
	require.NotNil(t, nestedTxn)
	require.True(t, nestedTxn.Succeeded)
	require.Len(t, nestedTxn.Responses, 1)
	nestedGet := nestedTxn.Responses[0].GetResponseRange()
	require.NotNil(t, nestedGet)
	require.NotNil(t, nestedGet.Header)
	require.Equal(t, int64(4), nestedGet.Count)
	require.True(t, nestedGet.More)
	require.Len(t, nestedGet.Kvs, 2)
	require.Equal(t, [][]byte{[]byte("items/c"), []byte("items/b")},
		[][]byte{nestedGet.Kvs[0].Key, nestedGet.Kvs[1].Key})
	require.Equal(t, [][]byte{[]byte("a"), []byte("m")},
		[][]byte{nestedGet.Kvs[0].Value, nestedGet.Kvs[1].Value})
}

func TestClientNamespaceNestedTxnGetKeysOnlySortByValueDescWithLimitReturnsLogicalKeys(t *testing.T) {
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
	tenantPrefix := "/a1531/namespace-nested-txn-keysonly-sort-value/tenant/"
	namespacedKV := namespace.NewKV(client.KV, tenantPrefix)
	for _, seed := range []struct {
		key   string
		value string
	}{
		{key: "items/d", value: "n"},
		{key: "items/c", value: "a"},
		{key: "items/b", value: "m"},
		{key: "items/a", value: "z"},
	} {
		_, err = namespacedKV.Put(ctx, seed.key, seed.value)
		require.NoError(t, err)
	}
	_, err = client.Put(ctx, "/a1531/namespace-nested-txn-keysonly-sort-value/tenant/items0/outside", "zz")
	require.NoError(t, err)
	_, err = client.Put(ctx, "/a1531/namespace-nested-txn-keysonly-sort-value/tenant0/items/e", "zz")
	require.NoError(t, err)

	txnResp, err := namespacedKV.Txn(ctx).
		Then(clientv3.OpTxn(nil,
			[]clientv3.Op{clientv3.OpGet("items/",
				clientv3.WithPrefix(),
				clientv3.WithLimit(2),
				clientv3.WithKeysOnly(),
				clientv3.WithSort(clientv3.SortByValue, clientv3.SortDescend),
			)},
			nil)).
		Commit()
	require.NoError(t, err)
	require.True(t, txnResp.Succeeded)
	require.Len(t, txnResp.Responses, 1)
	nestedTxn := txnResp.Responses[0].GetResponseTxn()
	require.NotNil(t, nestedTxn)
	require.True(t, nestedTxn.Succeeded)
	require.Len(t, nestedTxn.Responses, 1)
	nestedGet := nestedTxn.Responses[0].GetResponseRange()
	require.NotNil(t, nestedGet)
	require.NotNil(t, nestedGet.Header)
	require.Equal(t, int64(4), nestedGet.Count)
	require.True(t, nestedGet.More)
	require.Len(t, nestedGet.Kvs, 2)
	require.Equal(t, [][]byte{[]byte("items/a"), []byte("items/d")},
		[][]byte{nestedGet.Kvs[0].Key, nestedGet.Kvs[1].Key})
	require.Equal(t, []byte(nil), nestedGet.Kvs[0].Value)
	require.Equal(t, []byte(nil), nestedGet.Kvs[1].Value)
}

func TestClientNamespaceNestedTxnGetContradictoryModRevisionFilterReturnsEmptyLogicalRange(t *testing.T) {
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
	tenantPrefix := "/a1532/namespace-nested-txn-contradictory-mod-rev/tenant/"
	namespacedKV := namespace.NewKV(client.KV, tenantPrefix)
	_, err = namespacedKV.Put(ctx, "items/a", "value-a")
	require.NoError(t, err)
	updateB, err := namespacedKV.Put(ctx, "items/b", "value-b")
	require.NoError(t, err)
	_, err = namespacedKV.Put(ctx, "items/c", "value-c")
	require.NoError(t, err)
	_, err = client.Put(ctx, "/a1532/namespace-nested-txn-contradictory-mod-rev/tenant/items0/outside", "same-tenant-outside-prefix")
	require.NoError(t, err)
	_, err = client.Put(ctx, "/a1532/namespace-nested-txn-contradictory-mod-rev/tenant0/items/b", "outside-tenant")
	require.NoError(t, err)

	txnResp, err := namespacedKV.Txn(ctx).
		Then(clientv3.OpTxn(nil,
			[]clientv3.Op{clientv3.OpGet("items/",
				clientv3.WithPrefix(),
				clientv3.WithMinModRev(updateB.Header.Revision+1),
				clientv3.WithMaxModRev(updateB.Header.Revision),
				clientv3.WithLimit(1),
			)},
			nil)).
		Commit()
	require.NoError(t, err)
	require.True(t, txnResp.Succeeded)
	require.Len(t, txnResp.Responses, 1)
	nestedTxn := txnResp.Responses[0].GetResponseTxn()
	require.NotNil(t, nestedTxn)
	require.True(t, nestedTxn.Succeeded)
	require.Len(t, nestedTxn.Responses, 1)
	nestedGet := nestedTxn.Responses[0].GetResponseRange()
	require.NotNil(t, nestedGet)
	require.NotNil(t, nestedGet.Header)
	require.Equal(t, int64(3), nestedGet.Count)
	require.False(t, nestedGet.More)
	require.Empty(t, nestedGet.Kvs)
}

func TestClientNamespaceNestedTxnGetHistoricalRevisionReturnsLogicalKeys(t *testing.T) {
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
	tenantPrefix := "/a1533/namespace-nested-txn-historical-rev/tenant/"
	namespacedKV := namespace.NewKV(client.KV, tenantPrefix)
	_, err = client.Put(ctx, "/a1533/namespace-nested-txn-historical-rev/tenant0/items/a", "outside-tenant")
	require.NoError(t, err)
	first, err := namespacedKV.Put(ctx, "items/a", "old-a")
	require.NoError(t, err)
	_, err = namespacedKV.Put(ctx, "items/a", "new-a")
	require.NoError(t, err)
	_, err = namespacedKV.Put(ctx, "items/b", "new-b")
	require.NoError(t, err)
	_, err = client.Put(ctx, "/a1533/namespace-nested-txn-historical-rev/tenant/items0/outside", "same-tenant-outside-prefix")
	require.NoError(t, err)

	txnResp, err := namespacedKV.Txn(ctx).
		Then(clientv3.OpTxn(nil,
			[]clientv3.Op{clientv3.OpGet("items/",
				clientv3.WithPrefix(),
				clientv3.WithRev(first.Header.Revision),
			)},
			nil)).
		Commit()
	require.NoError(t, err)
	require.True(t, txnResp.Succeeded)
	require.Len(t, txnResp.Responses, 1)
	nestedTxn := txnResp.Responses[0].GetResponseTxn()
	require.NotNil(t, nestedTxn)
	require.True(t, nestedTxn.Succeeded)
	require.Len(t, nestedTxn.Responses, 1)
	nestedGet := nestedTxn.Responses[0].GetResponseRange()
	require.NotNil(t, nestedGet)
	require.NotNil(t, nestedGet.Header)
	require.Equal(t, int64(1), nestedGet.Count)
	require.False(t, nestedGet.More)
	require.Len(t, nestedGet.Kvs, 1)
	require.Equal(t, []byte("items/a"), nestedGet.Kvs[0].Key)
	require.Equal(t, []byte("old-a"), nestedGet.Kvs[0].Value)
	require.Equal(t, first.Header.Revision, nestedGet.Kvs[0].ModRevision)
}

func TestClientNamespaceNestedTxnGetHistoricalKeysOnlyLimitReturnsLogicalKeys(t *testing.T) {
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
	tenantPrefix := "/a1534/namespace-nested-txn-historical-keysonly/tenant/"
	namespacedKV := namespace.NewKV(client.KV, tenantPrefix)
	for _, key := range []string{"items/00", "items/01", "items/02"} {
		_, err = namespacedKV.Put(ctx, key, "value-"+key)
		require.NoError(t, err)
	}
	historical, err := namespacedKV.Put(ctx, "items/03", "value-items/03")
	require.NoError(t, err)
	_, err = namespacedKV.Put(ctx, "items/02", "updated-items/02")
	require.NoError(t, err)
	_, err = namespacedKV.Put(ctx, "items/04", "value-items/04")
	require.NoError(t, err)
	_, err = client.Put(ctx, "/a1534/namespace-nested-txn-historical-keysonly/tenant/items0/outside", "same-tenant-outside-prefix")
	require.NoError(t, err)
	_, err = client.Put(ctx, "/a1534/namespace-nested-txn-historical-keysonly/tenant0/items/00", "outside-tenant")
	require.NoError(t, err)

	txnResp, err := namespacedKV.Txn(ctx).
		Then(clientv3.OpTxn(nil,
			[]clientv3.Op{clientv3.OpGet("items/",
				clientv3.WithPrefix(),
				clientv3.WithKeysOnly(),
				clientv3.WithLimit(2),
				clientv3.WithRev(historical.Header.Revision),
			)},
			nil)).
		Commit()
	require.NoError(t, err)
	require.True(t, txnResp.Succeeded)
	require.Len(t, txnResp.Responses, 1)
	nestedTxn := txnResp.Responses[0].GetResponseTxn()
	require.NotNil(t, nestedTxn)
	require.True(t, nestedTxn.Succeeded)
	require.Len(t, nestedTxn.Responses, 1)
	nestedGet := nestedTxn.Responses[0].GetResponseRange()
	require.NotNil(t, nestedGet)
	require.NotNil(t, nestedGet.Header)
	require.Equal(t, int64(4), nestedGet.Count)
	require.True(t, nestedGet.More)
	require.Len(t, nestedGet.Kvs, 2)
	require.Equal(t, [][]byte{[]byte("items/00"), []byte("items/01")},
		[][]byte{nestedGet.Kvs[0].Key, nestedGet.Kvs[1].Key})
	require.Equal(t, []byte(nil), nestedGet.Kvs[0].Value)
	require.Equal(t, []byte(nil), nestedGet.Kvs[1].Value)
}

func TestClientNamespaceNestedTxnGetHistoricalSerializableReturnsLogicalKey(t *testing.T) {
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
	tenantPrefix := "/a1535/namespace-nested-txn-historical-serializable/tenant/"
	namespacedKV := namespace.NewKV(client.KV, tenantPrefix)
	_, err = client.Put(ctx, "/a1535/namespace-nested-txn-historical-serializable/tenant/items0/outside", "same-tenant-outside-prefix")
	require.NoError(t, err)
	_, err = client.Put(ctx, "/a1535/namespace-nested-txn-historical-serializable/tenant0/items/a", "outside-tenant")
	require.NoError(t, err)
	first, err := namespacedKV.Put(ctx, "items/a", "old-a")
	require.NoError(t, err)
	latest, err := namespacedKV.Put(ctx, "items/a", "new-a")
	require.NoError(t, err)

	txnResp, err := namespacedKV.Txn(ctx).
		Then(clientv3.OpTxn(nil,
			[]clientv3.Op{clientv3.OpGet("items/a",
				clientv3.WithRev(first.Header.Revision),
				clientv3.WithSerializable(),
			)},
			nil)).
		Commit()
	require.NoError(t, err)
	require.True(t, txnResp.Succeeded)
	require.Equal(t, latest.Header.Revision, txnResp.Header.Revision)
	require.Len(t, txnResp.Responses, 1)
	nestedTxn := txnResp.Responses[0].GetResponseTxn()
	require.NotNil(t, nestedTxn)
	require.True(t, nestedTxn.Succeeded)
	require.Len(t, nestedTxn.Responses, 1)
	nestedGet := nestedTxn.Responses[0].GetResponseRange()
	require.NotNil(t, nestedGet)
	require.NotNil(t, nestedGet.Header)
	require.Equal(t, latest.Header.Revision, nestedGet.Header.Revision)
	require.Equal(t, int64(1), nestedGet.Count)
	require.False(t, nestedGet.More)
	require.Len(t, nestedGet.Kvs, 1)
	require.Equal(t, []byte("items/a"), nestedGet.Kvs[0].Key)
	require.Equal(t, []byte("old-a"), nestedGet.Kvs[0].Value)
	require.Equal(t, first.Header.Revision, nestedGet.Kvs[0].ModRevision)
}

func TestClientNamespaceNestedTxnGetSerializablePrefixReturnsLogicalKeys(t *testing.T) {
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
	tenantPrefix := "/a1536/namespace-nested-txn-serializable-prefix/tenant/"
	namespacedKV := namespace.NewKV(client.KV, tenantPrefix)
	for _, key := range []string{"items/a", "items/b"} {
		_, err = namespacedKV.Put(ctx, key, "value-"+key)
		require.NoError(t, err)
	}
	_, err = client.Put(ctx, "/a1536/namespace-nested-txn-serializable-prefix/tenant/items0/outside", "same-tenant-outside-prefix")
	require.NoError(t, err)
	_, err = client.Put(ctx, "/a1536/namespace-nested-txn-serializable-prefix/tenant0/items/a", "outside-tenant")
	require.NoError(t, err)
	latest, err := namespacedKV.Put(ctx, "items/c", "value-items/c")
	require.NoError(t, err)

	txnResp, err := namespacedKV.Txn(ctx).
		Then(clientv3.OpTxn(nil,
			[]clientv3.Op{clientv3.OpGet("items/",
				clientv3.WithPrefix(),
				clientv3.WithSerializable(),
			)},
			nil)).
		Commit()
	require.NoError(t, err)
	require.True(t, txnResp.Succeeded)
	require.Len(t, txnResp.Responses, 1)
	nestedTxn := txnResp.Responses[0].GetResponseTxn()
	require.NotNil(t, nestedTxn)
	require.True(t, nestedTxn.Succeeded)
	require.Len(t, nestedTxn.Responses, 1)
	nestedGet := nestedTxn.Responses[0].GetResponseRange()
	require.NotNil(t, nestedGet)
	require.NotNil(t, nestedGet.Header)
	require.Equal(t, latest.Header.Revision, nestedGet.Header.Revision)
	require.Equal(t, int64(3), nestedGet.Count)
	require.False(t, nestedGet.More)
	require.Len(t, nestedGet.Kvs, 3)
	require.Equal(t, [][]byte{[]byte("items/a"), []byte("items/b"), []byte("items/c")},
		[][]byte{nestedGet.Kvs[0].Key, nestedGet.Kvs[1].Key, nestedGet.Kvs[2].Key})
	require.Equal(t, [][]byte{[]byte("value-items/a"), []byte("value-items/b"), []byte("value-items/c")},
		[][]byte{nestedGet.Kvs[0].Value, nestedGet.Kvs[1].Value, nestedGet.Kvs[2].Value})
}

func TestClientNamespaceNestedTxnFutureRevisionRangeReturnsTypedErrorAndDoesNotCommit(t *testing.T) {
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
	tenantPrefix := "/a1537/namespace-nested-txn-future-rev/tenant/"
	namespacedKV := namespace.NewKV(client.KV, tenantPrefix)
	_, err = namespacedKV.Put(ctx, "items/a", "value-a")
	require.NoError(t, err)
	_, err = client.Put(ctx, "/a1537/namespace-nested-txn-future-rev/tenant0/items/a", "outside-tenant")
	require.NoError(t, err)

	_, err = namespacedKV.Txn(ctx).
		Then(clientv3.OpTxn(nil,
			[]clientv3.Op{
				clientv3.OpGet("items/a", clientv3.WithRev(math.MaxInt64)),
				clientv3.OpPut("items/after-future", "must-not-commit"),
			},
			nil)).
		Commit()
	requireClientTxnError(t, err, codes.Unknown, "etcdserver: mvcc: required revision is a future revision", rpctypes.ErrFutureRev)

	afterFuture, err := namespacedKV.Get(ctx, "items/after-future")
	require.NoError(t, err)
	require.Empty(t, afterFuture.Kvs)
	outsideTenant, err := client.Get(ctx, "/a1537/namespace-nested-txn-future-rev/tenant0/items/a")
	require.NoError(t, err)
	require.Len(t, outsideTenant.Kvs, 1)
	require.Equal(t, []byte("outside-tenant"), outsideTenant.Kvs[0].Value)
}

func TestClientNamespaceNestedTxnCompactedRevisionRangeReturnsTypedErrorAndDoesNotCommit(t *testing.T) {
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
	tenantPrefix := "/a1538/namespace-nested-txn-compacted-rev/tenant/"
	namespacedKV := namespace.NewKV(client.KV, tenantPrefix)
	seed, err := namespacedKV.Put(ctx, "items/a", "old-a")
	require.NoError(t, err)
	latest, err := namespacedKV.Put(ctx, "items/a", "new-a")
	require.NoError(t, err)
	_, err = client.Compact(ctx, latest.Header.Revision)
	require.NoError(t, err)
	_, err = client.Put(ctx, "/a1538/namespace-nested-txn-compacted-rev/tenant0/items/a", "outside-tenant")
	require.NoError(t, err)

	_, err = namespacedKV.Txn(ctx).
		Then(clientv3.OpTxn(nil,
			[]clientv3.Op{
				clientv3.OpGet("items/a", clientv3.WithRev(seed.Header.Revision)),
				clientv3.OpPut("items/after-compacted", "must-not-commit"),
			},
			nil)).
		Commit()
	requireClientTxnError(t, err, codes.Unknown, "etcdserver: mvcc: required revision has been compacted", rpctypes.ErrCompacted)

	afterCompacted, err := namespacedKV.Get(ctx, "items/after-compacted")
	require.NoError(t, err)
	require.Empty(t, afterCompacted.Kvs)
	current, err := namespacedKV.Get(ctx, "items/a")
	require.NoError(t, err)
	require.Len(t, current.Kvs, 1)
	require.Equal(t, []byte("new-a"), current.Kvs[0].Value)
}

func TestClientNamespaceNestedTxnUnselectedFutureRevisionRangeIsNotValidated(t *testing.T) {
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
	tenantPrefix := "/a1539/namespace-nested-txn-unselected-future/tenant/"
	namespacedKV := namespace.NewKV(client.KV, tenantPrefix)
	_, err = namespacedKV.Put(ctx, "items/a", "value-a")
	require.NoError(t, err)
	_, err = client.Put(ctx, "/a1539/namespace-nested-txn-unselected-future/tenant0/items/a", "outside-tenant")
	require.NoError(t, err)

	txnResp, err := namespacedKV.Txn(ctx).
		Then(clientv3.OpTxn(
			[]clientv3.Cmp{clientv3.Compare(clientv3.Version("missing"), "=", 1)},
			[]clientv3.Op{
				clientv3.OpGet("items/a", clientv3.WithRev(math.MaxInt64)),
				clientv3.OpPut("items/unselected-put", "must-not-commit"),
			},
			[]clientv3.Op{clientv3.OpGet("items/a")},
		)).
		Commit()
	require.NoError(t, err)
	require.True(t, txnResp.Succeeded)
	require.Len(t, txnResp.Responses, 1)
	nestedTxn := txnResp.Responses[0].GetResponseTxn()
	require.NotNil(t, nestedTxn)
	require.False(t, nestedTxn.Succeeded)
	require.Len(t, nestedTxn.Responses, 1)
	nestedGet := nestedTxn.Responses[0].GetResponseRange()
	require.NotNil(t, nestedGet)
	require.NotNil(t, nestedGet.Header)
	require.Equal(t, int64(1), nestedGet.Count)
	require.False(t, nestedGet.More)
	require.Len(t, nestedGet.Kvs, 1)
	require.Equal(t, []byte("items/a"), nestedGet.Kvs[0].Key)
	require.Equal(t, []byte("value-a"), nestedGet.Kvs[0].Value)

	unselectedPut, err := namespacedKV.Get(ctx, "items/unselected-put")
	require.NoError(t, err)
	require.Empty(t, unselectedPut.Kvs)
}

func TestClientNamespaceNestedTxnUnselectedCompactedRevisionRangeIsNotValidated(t *testing.T) {
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
	tenantPrefix := "/a1540/namespace-nested-txn-unselected-compacted/tenant/"
	namespacedKV := namespace.NewKV(client.KV, tenantPrefix)
	seed, err := namespacedKV.Put(ctx, "items/a", "old-a")
	require.NoError(t, err)
	latest, err := namespacedKV.Put(ctx, "items/a", "new-a")
	require.NoError(t, err)
	_, err = client.Compact(ctx, latest.Header.Revision)
	require.NoError(t, err)
	_, err = client.Put(ctx, "/a1540/namespace-nested-txn-unselected-compacted/tenant0/items/a", "outside-tenant")
	require.NoError(t, err)

	txnResp, err := namespacedKV.Txn(ctx).
		Then(clientv3.OpTxn(
			[]clientv3.Cmp{clientv3.Compare(clientv3.Version("missing"), "=", 1)},
			[]clientv3.Op{
				clientv3.OpGet("items/a", clientv3.WithRev(seed.Header.Revision)),
				clientv3.OpPut("items/unselected-put", "must-not-commit"),
			},
			[]clientv3.Op{clientv3.OpGet("items/a")},
		)).
		Commit()
	require.NoError(t, err)
	require.True(t, txnResp.Succeeded)
	require.Len(t, txnResp.Responses, 1)
	nestedTxn := txnResp.Responses[0].GetResponseTxn()
	require.NotNil(t, nestedTxn)
	require.False(t, nestedTxn.Succeeded)
	require.Len(t, nestedTxn.Responses, 1)
	nestedGet := nestedTxn.Responses[0].GetResponseRange()
	require.NotNil(t, nestedGet)
	require.NotNil(t, nestedGet.Header)
	require.Equal(t, int64(1), nestedGet.Count)
	require.False(t, nestedGet.More)
	require.Len(t, nestedGet.Kvs, 1)
	require.Equal(t, []byte("items/a"), nestedGet.Kvs[0].Key)
	require.Equal(t, []byte("new-a"), nestedGet.Kvs[0].Value)

	unselectedPut, err := namespacedKV.Get(ctx, "items/unselected-put")
	require.NoError(t, err)
	require.Empty(t, unselectedPut.Kvs)
}

func TestClientNamespaceNestedTxnUnselectedDuplicateWriteBranchReturnsTypedErrorAndDoesNotCommit(t *testing.T) {
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
	tenantPrefix := "/a1541/namespace-nested-txn-unselected-duplicate/tenant/"
	namespacedKV := namespace.NewKV(client.KV, tenantPrefix)
	_, err = namespacedKV.Put(ctx, "items/a", "value-a")
	require.NoError(t, err)
	_, err = client.Put(ctx, "/a1541/namespace-nested-txn-unselected-duplicate/tenant0/items/a", "outside-tenant")
	require.NoError(t, err)

	_, err = namespacedKV.Txn(ctx).
		Then(clientv3.OpTxn(
			[]clientv3.Cmp{clientv3.Compare(clientv3.Version("missing"), "=", 1)},
			[]clientv3.Op{
				clientv3.OpPut("items/duplicate", "one"),
				clientv3.OpPut("items/duplicate", "two"),
			},
			[]clientv3.Op{clientv3.OpGet("items/a")},
		)).
		Commit()
	requireClientTxnError(t, err, codes.Unknown, "etcdserver: duplicate key given in txn request", rpctypes.ErrDuplicateKey)

	duplicate, err := namespacedKV.Get(ctx, "items/duplicate")
	require.NoError(t, err)
	require.Empty(t, duplicate.Kvs)
	current, err := namespacedKV.Get(ctx, "items/a")
	require.NoError(t, err)
	require.Len(t, current.Kvs, 1)
	require.Equal(t, []byte("value-a"), current.Kvs[0].Value)
	outsideTenant, err := client.Get(ctx, "/a1541/namespace-nested-txn-unselected-duplicate/tenant0/items/a")
	require.NoError(t, err)
	require.Len(t, outsideTenant.Kvs, 1)
	require.Equal(t, []byte("outside-tenant"), outsideTenant.Kvs[0].Value)
}

func TestClientNamespaceNestedTxnMissingLeaseBranchSelectionMatchesEtcd(t *testing.T) {
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
	tenantPrefix := "/a1542/namespace-nested-txn-missing-lease/tenant/"
	namespacedKV := namespace.NewKV(client.KV, tenantPrefix)
	missingLease := clientv3.LeaseID(math.MaxInt64)
	_, err = namespacedKV.Put(ctx, "items/a", "value-a")
	require.NoError(t, err)
	_, err = client.Put(ctx, "/a1542/namespace-nested-txn-missing-lease/tenant0/items/a", "outside-tenant")
	require.NoError(t, err)

	unselectedBadLease, err := namespacedKV.Txn(ctx).
		Then(clientv3.OpTxn(
			[]clientv3.Cmp{clientv3.Compare(clientv3.Version("missing"), "=", 1)},
			[]clientv3.Op{clientv3.OpPut("items/bad-lease", "must-not-commit", clientv3.WithLease(missingLease))},
			[]clientv3.Op{clientv3.OpGet("items/a")},
		)).
		Commit()
	require.NoError(t, err)
	require.True(t, unselectedBadLease.Succeeded)
	require.Len(t, unselectedBadLease.Responses, 1)
	nestedTxn := unselectedBadLease.Responses[0].GetResponseTxn()
	require.NotNil(t, nestedTxn)
	require.False(t, nestedTxn.Succeeded)
	require.Len(t, nestedTxn.Responses, 1)
	nestedGet := nestedTxn.Responses[0].GetResponseRange()
	require.NotNil(t, nestedGet)
	require.Equal(t, int64(1), nestedGet.Count)
	require.Len(t, nestedGet.Kvs, 1)
	require.Equal(t, []byte("items/a"), nestedGet.Kvs[0].Key)
	require.Equal(t, []byte("value-a"), nestedGet.Kvs[0].Value)

	_, err = namespacedKV.Txn(ctx).
		Then(clientv3.OpTxn(
			nil,
			[]clientv3.Op{clientv3.OpPut("items/bad-lease", "must-not-commit", clientv3.WithLease(missingLease))},
			nil,
		)).
		Commit()
	requireClientTxnError(t, err, codes.Unknown, "etcdserver: requested lease not found", rpctypes.ErrLeaseNotFound)

	badLease, err := namespacedKV.Get(ctx, "items/bad-lease")
	require.NoError(t, err)
	require.Empty(t, badLease.Kvs)
	current, err := namespacedKV.Get(ctx, "items/a")
	require.NoError(t, err)
	require.Len(t, current.Kvs, 1)
	require.Equal(t, []byte("value-a"), current.Kvs[0].Value)
	outsideTenant, err := client.Get(ctx, "/a1542/namespace-nested-txn-missing-lease/tenant0/items/a")
	require.NoError(t, err)
	require.Len(t, outsideTenant.Kvs, 1)
	require.Equal(t, []byte("outside-tenant"), outsideTenant.Kvs[0].Value)
}

func TestClientNamespaceNestedTxnUnselectedTooManyOpsBranchReturnsTypedErrorAndDoesNotCommit(t *testing.T) {
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
	tenantPrefix := "/a1543/namespace-nested-txn-unselected-too-many/tenant/"
	namespacedKV := namespace.NewKV(client.KV, tenantPrefix)
	_, err = namespacedKV.Put(ctx, "items/a", "value-a")
	require.NoError(t, err)
	_, err = client.Put(ctx, "/a1543/namespace-nested-txn-unselected-too-many/tenant0/items/a", "outside-tenant")
	require.NoError(t, err)

	tooManyOps := make([]clientv3.Op, defaultMaxTxnOps+1)
	for i := range tooManyOps {
		tooManyOps[i] = clientv3.OpGet(fmt.Sprintf("items/over-budget/%03d", i))
	}
	_, err = namespacedKV.Txn(ctx).
		Then(clientv3.OpTxn(
			[]clientv3.Cmp{clientv3.Compare(clientv3.Version("missing"), "=", 1)},
			tooManyOps,
			[]clientv3.Op{clientv3.OpPut("items/else", "must-not-commit")},
		)).
		Commit()
	requireClientTxnError(t, err, codes.Unknown, "etcdserver: too many operations in txn request", rpctypes.ErrTooManyOps)

	elsePut, err := namespacedKV.Get(ctx, "items/else")
	require.NoError(t, err)
	require.Empty(t, elsePut.Kvs)
	current, err := namespacedKV.Get(ctx, "items/a")
	require.NoError(t, err)
	require.Len(t, current.Kvs, 1)
	require.Equal(t, []byte("value-a"), current.Kvs[0].Value)
	outsideTenant, err := client.Get(ctx, "/a1543/namespace-nested-txn-unselected-too-many/tenant0/items/a")
	require.NoError(t, err)
	require.Len(t, outsideTenant.Kvs, 1)
	require.Equal(t, []byte("outside-tenant"), outsideTenant.Kvs[0].Value)
}

func TestClientNamespaceNestedTxnUnselectedInvalidSortBranchReturnsTypedErrorAndDoesNotCommit(t *testing.T) {
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
	tenantPrefix := "/a1544/namespace-nested-txn-unselected-invalid-sort/tenant/"
	namespacedKV := namespace.NewKV(client.KV, tenantPrefix)
	_, err = namespacedKV.Put(ctx, "items/a", "value-a")
	require.NoError(t, err)
	_, err = client.Put(ctx, "/a1544/namespace-nested-txn-unselected-invalid-sort/tenant0/items/a", "outside-tenant")
	require.NoError(t, err)

	_, err = namespacedKV.Txn(ctx).
		Then(clientv3.OpTxn(
			[]clientv3.Cmp{clientv3.Compare(clientv3.Version("missing"), "=", 1)},
			[]clientv3.Op{clientv3.OpGet("items/a",
				clientv3.WithSort(clientv3.SortTarget(99), clientv3.SortOrder(99)))},
			[]clientv3.Op{clientv3.OpPut("items/else", "must-not-commit")},
		)).
		Commit()
	requireClientTxnError(t, err, codes.Unknown, "etcdserver: invalid sort option", rpctypes.ErrInvalidSortOption)

	elsePut, err := namespacedKV.Get(ctx, "items/else")
	require.NoError(t, err)
	require.Empty(t, elsePut.Kvs)
	current, err := namespacedKV.Get(ctx, "items/a")
	require.NoError(t, err)
	require.Len(t, current.Kvs, 1)
	require.Equal(t, []byte("value-a"), current.Kvs[0].Value)
	outsideTenant, err := client.Get(ctx, "/a1544/namespace-nested-txn-unselected-invalid-sort/tenant0/items/a")
	require.NoError(t, err)
	require.Len(t, outsideTenant.Kvs, 1)
	require.Equal(t, []byte("outside-tenant"), outsideTenant.Kvs[0].Value)
}

func TestClientNamespaceNestedTxnUnselectedBranchAtRemainingOpBudgetSucceeds(t *testing.T) {
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
	tenantPrefix := "/a1545/namespace-nested-txn-unselected-budget-limit/tenant/"
	namespacedKV := namespace.NewKV(client.KV, tenantPrefix)
	_, err = namespacedKV.Put(ctx, "items/a", "value-a")
	require.NoError(t, err)
	_, err = client.Put(ctx, "/a1545/namespace-nested-txn-unselected-budget-limit/tenant0/items/a", "outside-tenant")
	require.NoError(t, err)

	exactRemainingOps := make([]clientv3.Op, defaultMaxTxnOps-1)
	for i := range exactRemainingOps {
		exactRemainingOps[i] = clientv3.OpGet(fmt.Sprintf("items/at-budget/%03d", i))
	}
	txnResp, err := namespacedKV.Txn(ctx).
		Then(clientv3.OpTxn(
			[]clientv3.Cmp{clientv3.Compare(clientv3.Version("missing"), "=", 1)},
			exactRemainingOps,
			[]clientv3.Op{clientv3.OpGet("items/a")},
		)).
		Commit()
	require.NoError(t, err)
	require.True(t, txnResp.Succeeded)
	require.Len(t, txnResp.Responses, 1)
	nestedTxn := txnResp.Responses[0].GetResponseTxn()
	require.NotNil(t, nestedTxn)
	require.False(t, nestedTxn.Succeeded)
	require.Len(t, nestedTxn.Responses, 1)
	nestedGet := nestedTxn.Responses[0].GetResponseRange()
	require.NotNil(t, nestedGet)
	require.Equal(t, int64(1), nestedGet.Count)
	require.Len(t, nestedGet.Kvs, 1)
	require.Equal(t, []byte("items/a"), nestedGet.Kvs[0].Key)
	require.Equal(t, []byte("value-a"), nestedGet.Kvs[0].Value)

	outsideTenant, err := client.Get(ctx, "/a1545/namespace-nested-txn-unselected-budget-limit/tenant0/items/a")
	require.NoError(t, err)
	require.Len(t, outsideTenant.Kvs, 1)
	require.Equal(t, []byte("outside-tenant"), outsideTenant.Kvs[0].Value)
}

func TestClientNamespaceNestedTxnPutIgnoreValueAndIgnoreLeaseKeepsLogicalPrevKV(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()

	grpcServer := grpc.NewServer(server.ClientServerOptions()...)
	server.Register(grpcServer)
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
	tenantPrefix := "/a1546/namespace-nested-txn-put-ignore/tenant/"
	namespacedKV := namespace.NewKV(client.KV, tenantPrefix)
	leaseA, err := client.Grant(ctx, 60)
	require.NoError(t, err)
	leaseB, err := client.Grant(ctx, 60)
	require.NoError(t, err)
	_, err = namespacedKV.Put(ctx, "items/a", "old-a", clientv3.WithLease(leaseA.ID))
	require.NoError(t, err)
	_, err = client.Put(ctx, "/a1546/namespace-nested-txn-put-ignore/tenant0/items/a", "outside-tenant")
	require.NoError(t, err)

	ignoreValue, err := namespacedKV.Txn(ctx).
		Then(clientv3.OpTxn(
			nil,
			[]clientv3.Op{clientv3.OpPut("items/a", "", clientv3.WithIgnoreValue(), clientv3.WithLease(leaseB.ID), clientv3.WithPrevKV())},
			nil,
		)).
		Commit()
	require.NoError(t, err)
	require.True(t, ignoreValue.Succeeded)
	require.Len(t, ignoreValue.Responses, 1)
	nestedIgnoreValue := ignoreValue.Responses[0].GetResponseTxn()
	require.NotNil(t, nestedIgnoreValue)
	require.True(t, nestedIgnoreValue.Succeeded)
	require.Len(t, nestedIgnoreValue.Responses, 1)
	ignoreValuePrev := nestedIgnoreValue.Responses[0].GetResponsePut().PrevKv
	require.NotNil(t, ignoreValuePrev)
	require.Equal(t, []byte("items/a"), ignoreValuePrev.Key)
	require.Equal(t, []byte("old-a"), ignoreValuePrev.Value)
	require.Equal(t, int64(leaseA.ID), ignoreValuePrev.Lease)

	afterIgnoreValue, err := namespacedKV.Get(ctx, "items/a")
	require.NoError(t, err)
	require.Len(t, afterIgnoreValue.Kvs, 1)
	require.Equal(t, []byte("old-a"), afterIgnoreValue.Kvs[0].Value)
	require.Equal(t, int64(leaseB.ID), afterIgnoreValue.Kvs[0].Lease)

	ignoreLease, err := namespacedKV.Txn(ctx).
		Then(clientv3.OpTxn(
			nil,
			[]clientv3.Op{clientv3.OpPut("items/a", "new-a", clientv3.WithIgnoreLease(), clientv3.WithPrevKV())},
			nil,
		)).
		Commit()
	require.NoError(t, err)
	require.True(t, ignoreLease.Succeeded)
	require.Len(t, ignoreLease.Responses, 1)
	nestedIgnoreLease := ignoreLease.Responses[0].GetResponseTxn()
	require.NotNil(t, nestedIgnoreLease)
	require.True(t, nestedIgnoreLease.Succeeded)
	require.Len(t, nestedIgnoreLease.Responses, 1)
	ignoreLeasePrev := nestedIgnoreLease.Responses[0].GetResponsePut().PrevKv
	require.NotNil(t, ignoreLeasePrev)
	require.Equal(t, []byte("items/a"), ignoreLeasePrev.Key)
	require.Equal(t, []byte("old-a"), ignoreLeasePrev.Value)
	require.Equal(t, int64(leaseB.ID), ignoreLeasePrev.Lease)

	final, err := namespacedKV.Get(ctx, "items/a")
	require.NoError(t, err)
	require.Len(t, final.Kvs, 1)
	require.Equal(t, []byte("new-a"), final.Kvs[0].Value)
	require.Equal(t, int64(leaseB.ID), final.Kvs[0].Lease)
	outsideTenant, err := client.Get(ctx, "/a1546/namespace-nested-txn-put-ignore/tenant0/items/a")
	require.NoError(t, err)
	require.Len(t, outsideTenant.Kvs, 1)
	require.Equal(t, []byte("outside-tenant"), outsideTenant.Kvs[0].Value)
}

func TestClientNamespaceNestedTxnUnselectedPutIgnoreOptionErrorsAreTypedAndDoNotCommit(t *testing.T) {
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
	tenantPrefix := "/a1547/namespace-nested-txn-put-ignore-errors/tenant/"
	namespacedKV := namespace.NewKV(client.KV, tenantPrefix)
	_, err = namespacedKV.Put(ctx, "items/a", "value-a")
	require.NoError(t, err)
	_, err = client.Put(ctx, "/a1547/namespace-nested-txn-put-ignore-errors/tenant0/items/a", "outside-tenant")
	require.NoError(t, err)

	tests := []struct {
		name        string
		key         string
		op          clientv3.Op
		wantMessage string
		wantErr     error
	}{
		{
			name:        "value-with-ignore-value",
			key:         "items/bad-ignore-value",
			op:          clientv3.OpPut("items/bad-ignore-value", "bad", clientv3.WithIgnoreValue()),
			wantMessage: "etcdserver: value is provided",
			wantErr:     rpctypes.ErrValueProvided,
		},
		{
			name:        "lease-with-ignore-lease",
			key:         "items/bad-ignore-lease",
			op:          clientv3.OpPut("items/bad-ignore-lease", "bad", clientv3.WithIgnoreLease(), clientv3.WithLease(clientv3.LeaseID(12345))),
			wantMessage: "etcdserver: lease is provided",
			wantErr:     rpctypes.ErrLeaseProvided,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := namespacedKV.Txn(ctx).
				Then(clientv3.OpTxn(
					[]clientv3.Cmp{clientv3.Compare(clientv3.Version("missing"), "=", 1)},
					[]clientv3.Op{tt.op},
					[]clientv3.Op{clientv3.OpPut("items/else-"+tt.name, "must-not-commit")},
				)).
				Commit()
			requireClientTxnError(t, err, codes.Unknown, tt.wantMessage, tt.wantErr)

			badKey, err := namespacedKV.Get(ctx, tt.key)
			require.NoError(t, err)
			require.Empty(t, badKey.Kvs)
			elsePut, err := namespacedKV.Get(ctx, "items/else-"+tt.name)
			require.NoError(t, err)
			require.Empty(t, elsePut.Kvs)
			current, err := namespacedKV.Get(ctx, "items/a")
			require.NoError(t, err)
			require.Len(t, current.Kvs, 1)
			require.Equal(t, []byte("value-a"), current.Kvs[0].Value)
			outsideTenant, err := client.Get(ctx, "/a1547/namespace-nested-txn-put-ignore-errors/tenant0/items/a")
			require.NoError(t, err)
			require.Len(t, outsideTenant.Kvs, 1)
			require.Equal(t, []byte("outside-tenant"), outsideTenant.Kvs[0].Value)
		})
	}
}

func TestClientNamespaceNestedTxnDeleteEmptyPrefixWithPrevKVStaysWithinNamespace(t *testing.T) {
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
	tenantPrefix := "/a1548/namespace-nested-txn-delete-empty-prefix/tenant/"
	namespacedKV := namespace.NewKV(client.KV, tenantPrefix)
	for _, key := range []string{"alpha/a", "items/a", "z/final"} {
		_, err = namespacedKV.Put(ctx, key, "value-"+key)
		require.NoError(t, err)
	}
	_, err = client.Put(ctx, "/a1548/namespace-nested-txn-delete-empty-prefix/tenant0/items/a", "outside-tenant")
	require.NoError(t, err)
	_, err = client.Put(ctx, "/a1548/namespace-nested-txn-delete-empty-prefix/tenant0/z/final", "outside-tenant-z")
	require.NoError(t, err)

	txnResp, err := namespacedKV.Txn(ctx).
		Then(clientv3.OpTxn(nil,
			[]clientv3.Op{clientv3.OpDelete("", clientv3.WithPrefix(), clientv3.WithPrevKV())},
			nil)).
		Commit()
	require.NoError(t, err)
	require.True(t, txnResp.Succeeded)
	require.Len(t, txnResp.Responses, 1)
	nestedTxn := txnResp.Responses[0].GetResponseTxn()
	require.NotNil(t, nestedTxn)
	require.True(t, nestedTxn.Succeeded)
	require.Len(t, nestedTxn.Responses, 1)
	nestedDelete := nestedTxn.Responses[0].GetResponseDeleteRange()
	require.NotNil(t, nestedDelete)
	require.Equal(t, int64(3), nestedDelete.Deleted)
	require.Len(t, nestedDelete.PrevKvs, 3)
	require.Equal(t, [][]byte{[]byte("alpha/a"), []byte("items/a"), []byte("z/final")},
		[][]byte{nestedDelete.PrevKvs[0].Key, nestedDelete.PrevKvs[1].Key, nestedDelete.PrevKvs[2].Key})
	require.Equal(t, [][]byte{[]byte("value-alpha/a"), []byte("value-items/a"), []byte("value-z/final")},
		[][]byte{nestedDelete.PrevKvs[0].Value, nestedDelete.PrevKvs[1].Value, nestedDelete.PrevKvs[2].Value})

	remaining, err := namespacedKV.Get(ctx, "", clientv3.WithPrefix())
	require.NoError(t, err)
	require.Empty(t, remaining.Kvs)
	require.Equal(t, int64(0), remaining.Count)
	outsideTenant, err := client.Get(ctx, "/a1548/namespace-nested-txn-delete-empty-prefix/tenant0/", clientv3.WithPrefix())
	require.NoError(t, err)
	require.Equal(t, int64(2), outsideTenant.Count)
	require.Equal(t, [][]byte{
		[]byte("/a1548/namespace-nested-txn-delete-empty-prefix/tenant0/items/a"),
		[]byte("/a1548/namespace-nested-txn-delete-empty-prefix/tenant0/z/final"),
	}, [][]byte{outsideTenant.Kvs[0].Key, outsideTenant.Kvs[1].Key})
}

func TestClientNamespaceNestedTxnGetEmptyPrefixStaysWithinNamespace(t *testing.T) {
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
	tenantPrefix := "/a1549/namespace-nested-txn-get-empty-prefix/tenant/"
	namespacedKV := namespace.NewKV(client.KV, tenantPrefix)
	for _, key := range []string{"alpha/a", "items/a", "z/final"} {
		_, err = namespacedKV.Put(ctx, key, "value-"+key)
		require.NoError(t, err)
	}
	_, err = client.Put(ctx, "/a1549/namespace-nested-txn-get-empty-prefix/tenant0/items/a", "outside-tenant")
	require.NoError(t, err)
	_, err = client.Put(ctx, "/a1549/namespace-nested-txn-get-empty-prefix/tenant0/z/final", "outside-tenant-z")
	require.NoError(t, err)

	txnResp, err := namespacedKV.Txn(ctx).
		Then(clientv3.OpTxn(nil,
			[]clientv3.Op{clientv3.OpGet("", clientv3.WithPrefix())},
			nil)).
		Commit()
	require.NoError(t, err)
	require.True(t, txnResp.Succeeded)
	require.Len(t, txnResp.Responses, 1)
	nestedTxn := txnResp.Responses[0].GetResponseTxn()
	require.NotNil(t, nestedTxn)
	require.True(t, nestedTxn.Succeeded)
	require.Len(t, nestedTxn.Responses, 1)
	nestedGet := nestedTxn.Responses[0].GetResponseRange()
	require.NotNil(t, nestedGet)
	require.Equal(t, int64(3), nestedGet.Count)
	require.False(t, nestedGet.More)
	require.Len(t, nestedGet.Kvs, 3)
	require.Equal(t, [][]byte{[]byte("alpha/a"), []byte("items/a"), []byte("z/final")},
		[][]byte{nestedGet.Kvs[0].Key, nestedGet.Kvs[1].Key, nestedGet.Kvs[2].Key})
	require.Equal(t, [][]byte{[]byte("value-alpha/a"), []byte("value-items/a"), []byte("value-z/final")},
		[][]byte{nestedGet.Kvs[0].Value, nestedGet.Kvs[1].Value, nestedGet.Kvs[2].Value})

	outsideTenant, err := client.Get(ctx, "/a1549/namespace-nested-txn-get-empty-prefix/tenant0/", clientv3.WithPrefix())
	require.NoError(t, err)
	require.Equal(t, int64(2), outsideTenant.Count)
}

func TestClientNamespaceNestedTxnGetEmptyPrefixCountOnlyIgnoresLimitWithinNamespace(t *testing.T) {
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
	tenantPrefix := "/a1550/namespace-nested-txn-empty-prefix-count-only/tenant/"
	namespacedKV := namespace.NewKV(client.KV, tenantPrefix)
	for _, key := range []string{"alpha/a", "items/a", "z/final"} {
		_, err = namespacedKV.Put(ctx, key, "value-"+key)
		require.NoError(t, err)
	}
	_, err = client.Put(ctx, "/a1550/namespace-nested-txn-empty-prefix-count-only/tenant0/items/a", "outside-tenant")
	require.NoError(t, err)

	txnResp, err := namespacedKV.Txn(ctx).
		Then(clientv3.OpTxn(nil,
			[]clientv3.Op{clientv3.OpGet("", clientv3.WithPrefix(), clientv3.WithCountOnly(), clientv3.WithLimit(1))},
			nil)).
		Commit()
	require.NoError(t, err)
	require.True(t, txnResp.Succeeded)
	require.Len(t, txnResp.Responses, 1)
	nestedTxn := txnResp.Responses[0].GetResponseTxn()
	require.NotNil(t, nestedTxn)
	require.True(t, nestedTxn.Succeeded)
	require.Len(t, nestedTxn.Responses, 1)
	nestedGet := nestedTxn.Responses[0].GetResponseRange()
	require.NotNil(t, nestedGet)
	require.Equal(t, int64(3), nestedGet.Count)
	require.False(t, nestedGet.More)
	require.Empty(t, nestedGet.Kvs)

	outsideTenant, err := client.Get(ctx, "/a1550/namespace-nested-txn-empty-prefix-count-only/tenant0/", clientv3.WithPrefix())
	require.NoError(t, err)
	require.Equal(t, int64(1), outsideTenant.Count)
}

func TestClientNamespaceNestedTxnGetEmptyPrefixKeysOnlyLimitReturnsLogicalPage(t *testing.T) {
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
	tenantPrefix := "/a1551/namespace-nested-txn-empty-prefix-keysonly/tenant/"
	namespacedKV := namespace.NewKV(client.KV, tenantPrefix)
	for _, key := range []string{"alpha/a", "items/a", "z/final"} {
		_, err = namespacedKV.Put(ctx, key, "value-"+key)
		require.NoError(t, err)
	}
	_, err = client.Put(ctx, "/a1551/namespace-nested-txn-empty-prefix-keysonly/tenant0/items/a", "outside-tenant")
	require.NoError(t, err)

	txnResp, err := namespacedKV.Txn(ctx).
		Then(clientv3.OpTxn(nil,
			[]clientv3.Op{clientv3.OpGet("", clientv3.WithPrefix(), clientv3.WithKeysOnly(), clientv3.WithLimit(2))},
			nil)).
		Commit()
	require.NoError(t, err)
	require.True(t, txnResp.Succeeded)
	require.Len(t, txnResp.Responses, 1)
	nestedTxn := txnResp.Responses[0].GetResponseTxn()
	require.NotNil(t, nestedTxn)
	require.True(t, nestedTxn.Succeeded)
	require.Len(t, nestedTxn.Responses, 1)
	nestedGet := nestedTxn.Responses[0].GetResponseRange()
	require.NotNil(t, nestedGet)
	require.Equal(t, int64(3), nestedGet.Count)
	require.True(t, nestedGet.More)
	require.Len(t, nestedGet.Kvs, 2)
	require.Equal(t, [][]byte{[]byte("alpha/a"), []byte("items/a")},
		[][]byte{nestedGet.Kvs[0].Key, nestedGet.Kvs[1].Key})
	require.Empty(t, nestedGet.Kvs[0].Value)
	require.Empty(t, nestedGet.Kvs[1].Value)

	outsideTenant, err := client.Get(ctx, "/a1551/namespace-nested-txn-empty-prefix-keysonly/tenant0/", clientv3.WithPrefix())
	require.NoError(t, err)
	require.Equal(t, int64(1), outsideTenant.Count)
}

func TestClientNamespaceNestedTxnGetEmptyPrefixSortByValueDescReturnsLogicalPage(t *testing.T) {
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
	tenantPrefix := "/a1552/namespace-nested-txn-empty-prefix-sort-value/tenant/"
	namespacedKV := namespace.NewKV(client.KV, tenantPrefix)
	for _, key := range []string{"alpha/a", "items/a", "z/final"} {
		_, err = namespacedKV.Put(ctx, key, "value-"+key)
		require.NoError(t, err)
	}
	_, err = client.Put(ctx, "/a1552/namespace-nested-txn-empty-prefix-sort-value/tenant0/z/final", "outside-tenant-z")
	require.NoError(t, err)

	txnResp, err := namespacedKV.Txn(ctx).
		Then(clientv3.OpTxn(nil,
			[]clientv3.Op{clientv3.OpGet("", clientv3.WithPrefix(), clientv3.WithLimit(2),
				clientv3.WithSort(clientv3.SortByValue, clientv3.SortDescend))},
			nil)).
		Commit()
	require.NoError(t, err)
	require.True(t, txnResp.Succeeded)
	require.Len(t, txnResp.Responses, 1)
	nestedTxn := txnResp.Responses[0].GetResponseTxn()
	require.NotNil(t, nestedTxn)
	require.True(t, nestedTxn.Succeeded)
	require.Len(t, nestedTxn.Responses, 1)
	nestedGet := nestedTxn.Responses[0].GetResponseRange()
	require.NotNil(t, nestedGet)
	require.Equal(t, int64(3), nestedGet.Count)
	require.True(t, nestedGet.More)
	require.Len(t, nestedGet.Kvs, 2)
	require.Equal(t, [][]byte{[]byte("z/final"), []byte("items/a")},
		[][]byte{nestedGet.Kvs[0].Key, nestedGet.Kvs[1].Key})
	require.Equal(t, [][]byte{[]byte("value-z/final"), []byte("value-items/a")},
		[][]byte{nestedGet.Kvs[0].Value, nestedGet.Kvs[1].Value})

	outsideTenant, err := client.Get(ctx, "/a1552/namespace-nested-txn-empty-prefix-sort-value/tenant0/", clientv3.WithPrefix())
	require.NoError(t, err)
	require.Equal(t, int64(1), outsideTenant.Count)
}

func TestClientNamespaceNestedTxnGetEmptyPrefixHistoricalRevisionReturnsLogicalKeys(t *testing.T) {
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
	tenantPrefix := "/a1553/namespace-nested-txn-empty-prefix-historical/tenant/"
	namespacedKV := namespace.NewKV(client.KV, tenantPrefix)
	_, err = client.Put(ctx, "/a1553/namespace-nested-txn-empty-prefix-historical/tenant0/items/a", "outside-tenant")
	require.NoError(t, err)
	_, err = namespacedKV.Put(ctx, "alpha/a", "old-alpha")
	require.NoError(t, err)
	_, err = namespacedKV.Put(ctx, "items/a", "old-items")
	require.NoError(t, err)
	historical, err := namespacedKV.Put(ctx, "z/final", "old-z")
	require.NoError(t, err)
	_, err = namespacedKV.Put(ctx, "items/a", "new-items")
	require.NoError(t, err)
	_, err = namespacedKV.Delete(ctx, "alpha/a")
	require.NoError(t, err)
	_, err = namespacedKV.Put(ctx, "later/new", "new-later")
	require.NoError(t, err)

	txnResp, err := namespacedKV.Txn(ctx).
		Then(clientv3.OpTxn(nil,
			[]clientv3.Op{clientv3.OpGet("", clientv3.WithPrefix(), clientv3.WithRev(historical.Header.Revision))},
			nil)).
		Commit()
	require.NoError(t, err)
	require.True(t, txnResp.Succeeded)
	require.Len(t, txnResp.Responses, 1)
	nestedTxn := txnResp.Responses[0].GetResponseTxn()
	require.NotNil(t, nestedTxn)
	require.True(t, nestedTxn.Succeeded)
	require.Len(t, nestedTxn.Responses, 1)
	nestedGet := nestedTxn.Responses[0].GetResponseRange()
	require.NotNil(t, nestedGet)
	require.Equal(t, int64(3), nestedGet.Count)
	require.False(t, nestedGet.More)
	require.Len(t, nestedGet.Kvs, 3)
	require.Equal(t, [][]byte{[]byte("alpha/a"), []byte("items/a"), []byte("z/final")},
		[][]byte{nestedGet.Kvs[0].Key, nestedGet.Kvs[1].Key, nestedGet.Kvs[2].Key})
	require.Equal(t, [][]byte{[]byte("old-alpha"), []byte("old-items"), []byte("old-z")},
		[][]byte{nestedGet.Kvs[0].Value, nestedGet.Kvs[1].Value, nestedGet.Kvs[2].Value})

	current, err := namespacedKV.Get(ctx, "", clientv3.WithPrefix())
	require.NoError(t, err)
	require.Equal(t, int64(3), current.Count)
	require.Equal(t, [][]byte{[]byte("items/a"), []byte("later/new"), []byte("z/final")},
		[][]byte{current.Kvs[0].Key, current.Kvs[1].Key, current.Kvs[2].Key})
	outsideTenant, err := client.Get(ctx, "/a1553/namespace-nested-txn-empty-prefix-historical/tenant0/", clientv3.WithPrefix())
	require.NoError(t, err)
	require.Equal(t, int64(1), outsideTenant.Count)
}

func TestClientNamespaceNestedTxnGetEmptyPrefixSerializableReturnsLogicalKeys(t *testing.T) {
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
	tenantPrefix := "/a1554/namespace-nested-txn-empty-prefix-serializable/tenant/"
	namespacedKV := namespace.NewKV(client.KV, tenantPrefix)
	for _, key := range []string{"alpha/a", "items/a"} {
		_, err = namespacedKV.Put(ctx, key, "value-"+key)
		require.NoError(t, err)
	}
	_, err = client.Put(ctx, "/a1554/namespace-nested-txn-empty-prefix-serializable/tenant0/items/a", "outside-tenant")
	require.NoError(t, err)
	latest, err := namespacedKV.Put(ctx, "z/final", "value-z/final")
	require.NoError(t, err)

	txnResp, err := namespacedKV.Txn(ctx).
		Then(clientv3.OpTxn(nil,
			[]clientv3.Op{clientv3.OpGet("", clientv3.WithPrefix(), clientv3.WithSerializable())},
			nil)).
		Commit()
	require.NoError(t, err)
	require.True(t, txnResp.Succeeded)
	require.Len(t, txnResp.Responses, 1)
	nestedTxn := txnResp.Responses[0].GetResponseTxn()
	require.NotNil(t, nestedTxn)
	require.True(t, nestedTxn.Succeeded)
	require.Len(t, nestedTxn.Responses, 1)
	nestedGet := nestedTxn.Responses[0].GetResponseRange()
	require.NotNil(t, nestedGet)
	require.NotNil(t, nestedGet.Header)
	require.Equal(t, latest.Header.Revision, nestedGet.Header.Revision)
	require.Equal(t, int64(3), nestedGet.Count)
	require.False(t, nestedGet.More)
	require.Len(t, nestedGet.Kvs, 3)
	require.Equal(t, [][]byte{[]byte("alpha/a"), []byte("items/a"), []byte("z/final")},
		[][]byte{nestedGet.Kvs[0].Key, nestedGet.Kvs[1].Key, nestedGet.Kvs[2].Key})
	require.Equal(t, [][]byte{[]byte("value-alpha/a"), []byte("value-items/a"), []byte("value-z/final")},
		[][]byte{nestedGet.Kvs[0].Value, nestedGet.Kvs[1].Value, nestedGet.Kvs[2].Value})

	outsideTenant, err := client.Get(ctx, "/a1554/namespace-nested-txn-empty-prefix-serializable/tenant0/", clientv3.WithPrefix())
	require.NoError(t, err)
	require.Equal(t, int64(1), outsideTenant.Count)
}

func TestClientNamespaceNestedTxnGetEmptyPrefixWithMinModRevisionReturnsLogicalKeys(t *testing.T) {
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
	tenantPrefix := "/a1555/namespace-nested-txn-empty-prefix-min-mod-rev/tenant/"
	namespacedKV := namespace.NewKV(client.KV, tenantPrefix)
	for _, key := range []string{"alpha/a", "items/a", "items/b"} {
		_, err = namespacedKV.Put(ctx, key, "old-"+key)
		require.NoError(t, err)
	}
	updateB, err := namespacedKV.Put(ctx, "items/b", "new-items/b")
	require.NoError(t, err)
	_, err = client.Put(ctx, "/a1555/namespace-nested-txn-empty-prefix-min-mod-rev/tenant0/items/b", "outside-tenant")
	require.NoError(t, err)
	_, err = namespacedKV.Put(ctx, "z/final", "value-z/final")
	require.NoError(t, err)

	txnResp, err := namespacedKV.Txn(ctx).
		Then(clientv3.OpTxn(nil,
			[]clientv3.Op{clientv3.OpGet("",
				clientv3.WithPrefix(),
				clientv3.WithMinModRev(updateB.Header.Revision),
				clientv3.WithSort(clientv3.SortByKey, clientv3.SortAscend),
			)},
			nil)).
		Commit()
	require.NoError(t, err)
	require.True(t, txnResp.Succeeded)
	require.Len(t, txnResp.Responses, 1)
	nestedTxn := txnResp.Responses[0].GetResponseTxn()
	require.NotNil(t, nestedTxn)
	require.True(t, nestedTxn.Succeeded)
	require.Len(t, nestedTxn.Responses, 1)
	nestedGet := nestedTxn.Responses[0].GetResponseRange()
	require.NotNil(t, nestedGet)
	require.Equal(t, int64(4), nestedGet.Count)
	require.False(t, nestedGet.More)
	require.Len(t, nestedGet.Kvs, 2)
	require.Equal(t, [][]byte{[]byte("items/b"), []byte("z/final")},
		[][]byte{nestedGet.Kvs[0].Key, nestedGet.Kvs[1].Key})
	require.Equal(t, [][]byte{[]byte("new-items/b"), []byte("value-z/final")},
		[][]byte{nestedGet.Kvs[0].Value, nestedGet.Kvs[1].Value})

	outsideTenant, err := client.Get(ctx, "/a1555/namespace-nested-txn-empty-prefix-min-mod-rev/tenant0/", clientv3.WithPrefix())
	require.NoError(t, err)
	require.Equal(t, int64(1), outsideTenant.Count)
}

func TestClientNamespaceNestedTxnGetEmptyPrefixWithMaxModRevisionReturnsLogicalKeys(t *testing.T) {
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
	tenantPrefix := "/a1556/namespace-nested-txn-empty-prefix-max-mod-rev/tenant/"
	namespacedKV := namespace.NewKV(client.KV, tenantPrefix)
	for _, key := range []string{"alpha/a", "items/a", "items/b"} {
		_, err = namespacedKV.Put(ctx, key, "old-"+key)
		require.NoError(t, err)
	}
	updateB, err := namespacedKV.Put(ctx, "items/b", "new-items/b")
	require.NoError(t, err)
	_, err = client.Put(ctx, "/a1556/namespace-nested-txn-empty-prefix-max-mod-rev/tenant0/items/a", "outside-tenant")
	require.NoError(t, err)
	_, err = namespacedKV.Put(ctx, "z/final", "value-z/final")
	require.NoError(t, err)

	txnResp, err := namespacedKV.Txn(ctx).
		Then(clientv3.OpTxn(nil,
			[]clientv3.Op{clientv3.OpGet("",
				clientv3.WithPrefix(),
				clientv3.WithMaxModRev(updateB.Header.Revision-1),
				clientv3.WithSort(clientv3.SortByKey, clientv3.SortAscend),
			)},
			nil)).
		Commit()
	require.NoError(t, err)
	require.True(t, txnResp.Succeeded)
	require.Len(t, txnResp.Responses, 1)
	nestedTxn := txnResp.Responses[0].GetResponseTxn()
	require.NotNil(t, nestedTxn)
	require.True(t, nestedTxn.Succeeded)
	require.Len(t, nestedTxn.Responses, 1)
	nestedGet := nestedTxn.Responses[0].GetResponseRange()
	require.NotNil(t, nestedGet)
	require.Equal(t, int64(4), nestedGet.Count)
	require.False(t, nestedGet.More)
	require.Len(t, nestedGet.Kvs, 2)
	require.Equal(t, [][]byte{[]byte("alpha/a"), []byte("items/a")},
		[][]byte{nestedGet.Kvs[0].Key, nestedGet.Kvs[1].Key})
	require.Equal(t, [][]byte{[]byte("old-alpha/a"), []byte("old-items/a")},
		[][]byte{nestedGet.Kvs[0].Value, nestedGet.Kvs[1].Value})

	outsideTenant, err := client.Get(ctx, "/a1556/namespace-nested-txn-empty-prefix-max-mod-rev/tenant0/", clientv3.WithPrefix())
	require.NoError(t, err)
	require.Equal(t, int64(1), outsideTenant.Count)
}

func TestClientNamespaceNestedTxnGetEmptyPrefixWithMinCreateRevisionReturnsLogicalKeys(t *testing.T) {
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
	tenantPrefix := "/a1557/namespace-nested-txn-empty-prefix-min-create-rev/tenant/"
	namespacedKV := namespace.NewKV(client.KV, tenantPrefix)
	for _, key := range []string{"alpha/a", "items/a"} {
		_, err = namespacedKV.Put(ctx, key, "value-"+key)
		require.NoError(t, err)
	}
	createB, err := namespacedKV.Put(ctx, "items/b", "value-items/b")
	require.NoError(t, err)
	_, err = namespacedKV.Put(ctx, "alpha/a", "updated-alpha/a")
	require.NoError(t, err)
	_, err = client.Put(ctx, "/a1557/namespace-nested-txn-empty-prefix-min-create-rev/tenant0/items/b", "outside-tenant")
	require.NoError(t, err)
	_, err = namespacedKV.Put(ctx, "z/final", "value-z/final")
	require.NoError(t, err)

	txnResp, err := namespacedKV.Txn(ctx).
		Then(clientv3.OpTxn(nil,
			[]clientv3.Op{clientv3.OpGet("",
				clientv3.WithPrefix(),
				clientv3.WithMinCreateRev(createB.Header.Revision),
				clientv3.WithSort(clientv3.SortByKey, clientv3.SortAscend),
			)},
			nil)).
		Commit()
	require.NoError(t, err)
	require.True(t, txnResp.Succeeded)
	require.Len(t, txnResp.Responses, 1)
	nestedTxn := txnResp.Responses[0].GetResponseTxn()
	require.NotNil(t, nestedTxn)
	require.True(t, nestedTxn.Succeeded)
	require.Len(t, nestedTxn.Responses, 1)
	nestedGet := nestedTxn.Responses[0].GetResponseRange()
	require.NotNil(t, nestedGet)
	require.Equal(t, int64(4), nestedGet.Count)
	require.False(t, nestedGet.More)
	require.Len(t, nestedGet.Kvs, 2)
	require.Equal(t, [][]byte{[]byte("items/b"), []byte("z/final")},
		[][]byte{nestedGet.Kvs[0].Key, nestedGet.Kvs[1].Key})
	require.Equal(t, [][]byte{[]byte("value-items/b"), []byte("value-z/final")},
		[][]byte{nestedGet.Kvs[0].Value, nestedGet.Kvs[1].Value})

	outsideTenant, err := client.Get(ctx, "/a1557/namespace-nested-txn-empty-prefix-min-create-rev/tenant0/", clientv3.WithPrefix())
	require.NoError(t, err)
	require.Equal(t, int64(1), outsideTenant.Count)
}

func TestClientNamespaceNestedTxnGetEmptyPrefixWithMaxCreateRevisionReturnsLogicalKeys(t *testing.T) {
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
	tenantPrefix := "/a1558/namespace-nested-txn-empty-prefix-max-create-rev/tenant/"
	namespacedKV := namespace.NewKV(client.KV, tenantPrefix)
	for _, key := range []string{"alpha/a", "items/a"} {
		_, err = namespacedKV.Put(ctx, key, "value-"+key)
		require.NoError(t, err)
	}
	createB, err := namespacedKV.Put(ctx, "items/b", "value-items/b")
	require.NoError(t, err)
	_, err = namespacedKV.Put(ctx, "alpha/a", "updated-alpha/a")
	require.NoError(t, err)
	_, err = client.Put(ctx, "/a1558/namespace-nested-txn-empty-prefix-max-create-rev/tenant0/items/a", "outside-tenant")
	require.NoError(t, err)
	_, err = namespacedKV.Put(ctx, "z/final", "value-z/final")
	require.NoError(t, err)

	txnResp, err := namespacedKV.Txn(ctx).
		Then(clientv3.OpTxn(nil,
			[]clientv3.Op{clientv3.OpGet("",
				clientv3.WithPrefix(),
				clientv3.WithMaxCreateRev(createB.Header.Revision-1),
				clientv3.WithSort(clientv3.SortByKey, clientv3.SortAscend),
			)},
			nil)).
		Commit()
	require.NoError(t, err)
	require.True(t, txnResp.Succeeded)
	require.Len(t, txnResp.Responses, 1)
	nestedTxn := txnResp.Responses[0].GetResponseTxn()
	require.NotNil(t, nestedTxn)
	require.True(t, nestedTxn.Succeeded)
	require.Len(t, nestedTxn.Responses, 1)
	nestedGet := nestedTxn.Responses[0].GetResponseRange()
	require.NotNil(t, nestedGet)
	require.Equal(t, int64(4), nestedGet.Count)
	require.False(t, nestedGet.More)
	require.Len(t, nestedGet.Kvs, 2)
	require.Equal(t, [][]byte{[]byte("alpha/a"), []byte("items/a")},
		[][]byte{nestedGet.Kvs[0].Key, nestedGet.Kvs[1].Key})
	require.Equal(t, [][]byte{[]byte("updated-alpha/a"), []byte("value-items/a")},
		[][]byte{nestedGet.Kvs[0].Value, nestedGet.Kvs[1].Value})

	outsideTenant, err := client.Get(ctx, "/a1558/namespace-nested-txn-empty-prefix-max-create-rev/tenant0/", clientv3.WithPrefix())
	require.NoError(t, err)
	require.Equal(t, int64(1), outsideTenant.Count)
}

func TestClientNamespaceNestedTxnGetEmptyPrefixWithFirstCreateReturnsLogicalKey(t *testing.T) {
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
	tenantPrefix := "/a1559/namespace-nested-txn-empty-prefix-first-create/tenant/"
	namespacedKV := namespace.NewKV(client.KV, tenantPrefix)
	_, err = client.Put(ctx, "/a1559/namespace-nested-txn-empty-prefix-first-create/tenant0/oldest", "outside-tenant")
	require.NoError(t, err)
	for _, key := range []string{"alpha/oldest", "items/a", "z/final"} {
		_, err = namespacedKV.Put(ctx, key, "value-"+key)
		require.NoError(t, err)
	}

	txnResp, err := namespacedKV.Txn(ctx).
		Then(clientv3.OpTxn(nil,
			[]clientv3.Op{clientv3.OpGet("", clientv3.WithFirstCreate()...)},
			nil)).
		Commit()
	require.NoError(t, err)
	require.True(t, txnResp.Succeeded)
	require.Len(t, txnResp.Responses, 1)
	nestedTxn := txnResp.Responses[0].GetResponseTxn()
	require.NotNil(t, nestedTxn)
	require.True(t, nestedTxn.Succeeded)
	require.Len(t, nestedTxn.Responses, 1)
	nestedGet := nestedTxn.Responses[0].GetResponseRange()
	require.NotNil(t, nestedGet)
	require.Equal(t, int64(3), nestedGet.Count)
	require.True(t, nestedGet.More)
	require.Len(t, nestedGet.Kvs, 1)
	require.Equal(t, []byte("alpha/oldest"), nestedGet.Kvs[0].Key)
	require.Equal(t, []byte("value-alpha/oldest"), nestedGet.Kvs[0].Value)

	outsideTenant, err := client.Get(ctx, "/a1559/namespace-nested-txn-empty-prefix-first-create/tenant0/", clientv3.WithPrefix())
	require.NoError(t, err)
	require.Equal(t, int64(1), outsideTenant.Count)
}

func TestClientNamespaceNestedTxnGetEmptyPrefixWithFirstRevReturnsLogicalKey(t *testing.T) {
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
	tenantPrefix := "/a1560/namespace-nested-txn-empty-prefix-first-rev/tenant/"
	namespacedKV := namespace.NewKV(client.KV, tenantPrefix)
	_, err = client.Put(ctx, "/a1560/namespace-nested-txn-empty-prefix-first-rev/tenant0/oldest", "outside-tenant")
	require.NoError(t, err)
	for _, key := range []string{"alpha/oldest", "items/a", "z/final"} {
		_, err = namespacedKV.Put(ctx, key, "value-"+key)
		require.NoError(t, err)
	}
	_, err = namespacedKV.Put(ctx, "alpha/oldest", "updated-alpha/oldest")
	require.NoError(t, err)

	txnResp, err := namespacedKV.Txn(ctx).
		Then(clientv3.OpTxn(nil,
			[]clientv3.Op{clientv3.OpGet("", clientv3.WithFirstRev()...)},
			nil)).
		Commit()
	require.NoError(t, err)
	require.True(t, txnResp.Succeeded)
	require.Len(t, txnResp.Responses, 1)
	nestedTxn := txnResp.Responses[0].GetResponseTxn()
	require.NotNil(t, nestedTxn)
	require.True(t, nestedTxn.Succeeded)
	require.Len(t, nestedTxn.Responses, 1)
	nestedGet := nestedTxn.Responses[0].GetResponseRange()
	require.NotNil(t, nestedGet)
	require.Equal(t, int64(3), nestedGet.Count)
	require.True(t, nestedGet.More)
	require.Len(t, nestedGet.Kvs, 1)
	require.Equal(t, []byte("items/a"), nestedGet.Kvs[0].Key)
	require.Equal(t, []byte("value-items/a"), nestedGet.Kvs[0].Value)

	outsideTenant, err := client.Get(ctx, "/a1560/namespace-nested-txn-empty-prefix-first-rev/tenant0/", clientv3.WithPrefix())
	require.NoError(t, err)
	require.Equal(t, int64(1), outsideTenant.Count)
}

func TestClientNamespaceNestedTxnGetEmptyPrefixWithLastRevReturnsLogicalKey(t *testing.T) {
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
	tenantPrefix := "/a1561/namespace-nested-txn-empty-prefix-last-rev/tenant/"
	namespacedKV := namespace.NewKV(client.KV, tenantPrefix)
	for _, key := range []string{"alpha/oldest", "items/a", "z/final"} {
		_, err = namespacedKV.Put(ctx, key, "value-"+key)
		require.NoError(t, err)
	}
	_, err = namespacedKV.Put(ctx, "items/a", "updated-items/a")
	require.NoError(t, err)
	_, err = client.Put(ctx, "/a1561/namespace-nested-txn-empty-prefix-last-rev/tenant0/newest", "outside-tenant")
	require.NoError(t, err)
	_, err = namespacedKV.Put(ctx, "later/new", "value-later/new")
	require.NoError(t, err)

	txnResp, err := namespacedKV.Txn(ctx).
		Then(clientv3.OpTxn(nil,
			[]clientv3.Op{clientv3.OpGet("", clientv3.WithLastRev()...)},
			nil)).
		Commit()
	require.NoError(t, err)
	require.True(t, txnResp.Succeeded)
	require.Len(t, txnResp.Responses, 1)
	nestedTxn := txnResp.Responses[0].GetResponseTxn()
	require.NotNil(t, nestedTxn)
	require.True(t, nestedTxn.Succeeded)
	require.Len(t, nestedTxn.Responses, 1)
	nestedGet := nestedTxn.Responses[0].GetResponseRange()
	require.NotNil(t, nestedGet)
	require.Equal(t, int64(4), nestedGet.Count)
	require.True(t, nestedGet.More)
	require.Len(t, nestedGet.Kvs, 1)
	require.Equal(t, []byte("later/new"), nestedGet.Kvs[0].Key)
	require.Equal(t, []byte("value-later/new"), nestedGet.Kvs[0].Value)

	outsideTenant, err := client.Get(ctx, "/a1561/namespace-nested-txn-empty-prefix-last-rev/tenant0/", clientv3.WithPrefix())
	require.NoError(t, err)
	require.Equal(t, int64(1), outsideTenant.Count)
}

func TestClientNamespaceNestedTxnGetEmptyPrefixWithLastCreateAndMaxCreateRevisionReturnsLogicalKey(t *testing.T) {
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
	tenantPrefix := "/a1562/namespace-nested-txn-empty-prefix-last-create-max-create/tenant/"
	namespacedKV := namespace.NewKV(client.KV, tenantPrefix)
	for _, key := range []string{"alpha/a", "items/a"} {
		_, err = namespacedKV.Put(ctx, key, "value-"+key)
		require.NoError(t, err)
	}
	createB, err := namespacedKV.Put(ctx, "items/b", "value-items/b")
	require.NoError(t, err)
	_, err = namespacedKV.Put(ctx, "alpha/a", "updated-alpha/a")
	require.NoError(t, err)
	_, err = client.Put(ctx, "/a1562/namespace-nested-txn-empty-prefix-last-create-max-create/tenant0/items/a", "outside-tenant")
	require.NoError(t, err)
	_, err = namespacedKV.Put(ctx, "z/final", "value-z/final")
	require.NoError(t, err)

	getOpts := append(clientv3.WithLastCreate(), clientv3.WithMaxCreateRev(createB.Header.Revision-1))
	txnResp, err := namespacedKV.Txn(ctx).
		Then(clientv3.OpTxn(nil,
			[]clientv3.Op{clientv3.OpGet("", getOpts...)},
			nil)).
		Commit()
	require.NoError(t, err)
	require.True(t, txnResp.Succeeded)
	require.Len(t, txnResp.Responses, 1)
	nestedTxn := txnResp.Responses[0].GetResponseTxn()
	require.NotNil(t, nestedTxn)
	require.True(t, nestedTxn.Succeeded)
	require.Len(t, nestedTxn.Responses, 1)
	nestedGet := nestedTxn.Responses[0].GetResponseRange()
	require.NotNil(t, nestedGet)
	require.Equal(t, int64(4), nestedGet.Count)
	require.True(t, nestedGet.More)
	require.Len(t, nestedGet.Kvs, 1)
	require.Equal(t, []byte("items/a"), nestedGet.Kvs[0].Key)
	require.Equal(t, []byte("value-items/a"), nestedGet.Kvs[0].Value)

	outsideTenant, err := client.Get(ctx, "/a1562/namespace-nested-txn-empty-prefix-last-create-max-create/tenant0/", clientv3.WithPrefix())
	require.NoError(t, err)
	require.Equal(t, int64(1), outsideTenant.Count)
}

func TestClientNamespaceNestedTxnGetEmptyPrefixWithLastRevAndMaxModRevisionReturnsLogicalKey(t *testing.T) {
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
	tenantPrefix := "/a1563/namespace-nested-txn-empty-prefix-last-rev-max-mod/tenant/"
	namespacedKV := namespace.NewKV(client.KV, tenantPrefix)
	for _, key := range []string{"alpha/a", "items/a", "z/final"} {
		_, err = namespacedKV.Put(ctx, key, "value-"+key)
		require.NoError(t, err)
	}
	updateItems, err := namespacedKV.Put(ctx, "items/a", "updated-items/a")
	require.NoError(t, err)
	_, err = namespacedKV.Put(ctx, "z/final", "updated-z/final")
	require.NoError(t, err)
	_, err = client.Put(ctx, "/a1563/namespace-nested-txn-empty-prefix-last-rev-max-mod/tenant0/newest", "outside-tenant")
	require.NoError(t, err)
	_, err = namespacedKV.Put(ctx, "later/new", "value-later/new")
	require.NoError(t, err)

	getOpts := append(clientv3.WithLastRev(), clientv3.WithMaxModRev(updateItems.Header.Revision))
	txnResp, err := namespacedKV.Txn(ctx).
		Then(clientv3.OpTxn(nil,
			[]clientv3.Op{clientv3.OpGet("", getOpts...)},
			nil)).
		Commit()
	require.NoError(t, err)
	require.True(t, txnResp.Succeeded)
	require.Len(t, txnResp.Responses, 1)
	nestedTxn := txnResp.Responses[0].GetResponseTxn()
	require.NotNil(t, nestedTxn)
	require.True(t, nestedTxn.Succeeded)
	require.Len(t, nestedTxn.Responses, 1)
	nestedGet := nestedTxn.Responses[0].GetResponseRange()
	require.NotNil(t, nestedGet)
	require.Equal(t, int64(4), nestedGet.Count)
	require.True(t, nestedGet.More)
	require.Len(t, nestedGet.Kvs, 1)
	require.Equal(t, []byte("items/a"), nestedGet.Kvs[0].Key)
	require.Equal(t, []byte("updated-items/a"), nestedGet.Kvs[0].Value)

	outsideTenant, err := client.Get(ctx, "/a1563/namespace-nested-txn-empty-prefix-last-rev-max-mod/tenant0/", clientv3.WithPrefix())
	require.NoError(t, err)
	require.Equal(t, int64(1), outsideTenant.Count)
}

func TestClientNamespaceNestedTxnGetEmptyStartWithLastRevAndLogicalRangeReturnsLogicalKey(t *testing.T) {
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
	tenantPrefix := "/a1564/namespace-nested-txn-empty-start-last-rev-range/tenant/"
	namespacedKV := namespace.NewKV(client.KV, tenantPrefix)
	for _, key := range []string{"alpha/a", "items/a", "z/final"} {
		_, err = namespacedKV.Put(ctx, key, "value-"+key)
		require.NoError(t, err)
	}
	_, err = namespacedKV.Put(ctx, "items/a", "updated-items/a")
	require.NoError(t, err)
	_, err = client.Put(ctx, "/a1564/namespace-nested-txn-empty-start-last-rev-range/tenant0/items/newer", "outside-tenant")
	require.NoError(t, err)
	_, err = namespacedKV.Put(ctx, "z/final", "updated-z/final")
	require.NoError(t, err)

	getOpts := append(clientv3.WithLastRev(), clientv3.WithRange("z"))
	txnResp, err := namespacedKV.Txn(ctx).
		Then(clientv3.OpTxn(nil,
			[]clientv3.Op{clientv3.OpGet("", getOpts...)},
			nil)).
		Commit()
	require.NoError(t, err)
	require.True(t, txnResp.Succeeded)
	require.Len(t, txnResp.Responses, 1)
	nestedTxn := txnResp.Responses[0].GetResponseTxn()
	require.NotNil(t, nestedTxn)
	require.True(t, nestedTxn.Succeeded)
	require.Len(t, nestedTxn.Responses, 1)
	nestedGet := nestedTxn.Responses[0].GetResponseRange()
	require.NotNil(t, nestedGet)
	require.Equal(t, int64(2), nestedGet.Count)
	require.True(t, nestedGet.More)
	require.Len(t, nestedGet.Kvs, 1)
	require.Equal(t, []byte("items/a"), nestedGet.Kvs[0].Key)
	require.Equal(t, []byte("updated-items/a"), nestedGet.Kvs[0].Value)

	outsideTenant, err := client.Get(ctx, "/a1564/namespace-nested-txn-empty-start-last-rev-range/tenant0/", clientv3.WithPrefix())
	require.NoError(t, err)
	require.Equal(t, int64(1), outsideTenant.Count)
}

func TestClientNamespaceNestedTxnDeleteEmptyStartWithLogicalRangePrevKVStaysWithinNamespace(t *testing.T) {
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
	tenantPrefix := "/a1565/namespace-nested-txn-empty-start-delete-range/tenant/"
	namespacedKV := namespace.NewKV(client.KV, tenantPrefix)
	for _, key := range []string{"alpha/a", "items/a", "z/final"} {
		_, err = namespacedKV.Put(ctx, key, "value-"+key)
		require.NoError(t, err)
	}
	_, err = client.Put(ctx, "/a1565/namespace-nested-txn-empty-start-delete-range/tenant0/items/a", "outside-tenant")
	require.NoError(t, err)
	_, err = client.Put(ctx, "/a1565/namespace-nested-txn-empty-start-delete-range/tenant0/z/final", "outside-tenant-z")
	require.NoError(t, err)

	txnResp, err := namespacedKV.Txn(ctx).
		Then(clientv3.OpTxn(nil,
			[]clientv3.Op{clientv3.OpDelete("", clientv3.WithRange("z"), clientv3.WithPrevKV())},
			nil)).
		Commit()
	require.NoError(t, err)
	require.True(t, txnResp.Succeeded)
	require.Len(t, txnResp.Responses, 1)
	nestedTxn := txnResp.Responses[0].GetResponseTxn()
	require.NotNil(t, nestedTxn)
	require.True(t, nestedTxn.Succeeded)
	require.Len(t, nestedTxn.Responses, 1)
	nestedDelete := nestedTxn.Responses[0].GetResponseDeleteRange()
	require.NotNil(t, nestedDelete)
	require.Equal(t, int64(2), nestedDelete.Deleted)
	require.Len(t, nestedDelete.PrevKvs, 2)
	require.Equal(t, [][]byte{[]byte("alpha/a"), []byte("items/a")},
		[][]byte{nestedDelete.PrevKvs[0].Key, nestedDelete.PrevKvs[1].Key})
	require.Equal(t, [][]byte{[]byte("value-alpha/a"), []byte("value-items/a")},
		[][]byte{nestedDelete.PrevKvs[0].Value, nestedDelete.PrevKvs[1].Value})

	remaining, err := namespacedKV.Get(ctx, "", clientv3.WithPrefix())
	require.NoError(t, err)
	require.Equal(t, int64(1), remaining.Count)
	require.Len(t, remaining.Kvs, 1)
	require.Equal(t, []byte("z/final"), remaining.Kvs[0].Key)
	require.Equal(t, []byte("value-z/final"), remaining.Kvs[0].Value)
	outsideTenant, err := client.Get(ctx, "/a1565/namespace-nested-txn-empty-start-delete-range/tenant0/", clientv3.WithPrefix())
	require.NoError(t, err)
	require.Equal(t, int64(2), outsideTenant.Count)
}

func TestClientNamespaceNestedTxnGetEmptyStartRangeCountOnlyTakesPrecedence(t *testing.T) {
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
	tenantPrefix := "/a1566/namespace-nested-txn-empty-start-range-count-only/tenant/"
	namespacedKV := namespace.NewKV(client.KV, tenantPrefix)
	for _, key := range []string{"alpha/a", "items/a", "z/final"} {
		_, err = namespacedKV.Put(ctx, key, "value-"+key)
		require.NoError(t, err)
	}
	_, err = client.Put(ctx, "/a1566/namespace-nested-txn-empty-start-range-count-only/tenant0/items/a", "outside-tenant")
	require.NoError(t, err)
	_, err = client.Put(ctx, "/a1566/namespace-nested-txn-empty-start-range-count-only/tenant0/z/final", "outside-tenant-z")
	require.NoError(t, err)

	txnResp, err := namespacedKV.Txn(ctx).
		Then(clientv3.OpTxn(nil,
			[]clientv3.Op{clientv3.OpGet("",
				clientv3.WithRange("z"),
				clientv3.WithKeysOnly(),
				clientv3.WithCountOnly(),
				clientv3.WithLimit(1),
			)},
			nil)).
		Commit()
	require.NoError(t, err)
	require.True(t, txnResp.Succeeded)
	require.Len(t, txnResp.Responses, 1)
	nestedTxn := txnResp.Responses[0].GetResponseTxn()
	require.NotNil(t, nestedTxn)
	require.True(t, nestedTxn.Succeeded)
	require.Len(t, nestedTxn.Responses, 1)
	nestedGet := nestedTxn.Responses[0].GetResponseRange()
	require.NotNil(t, nestedGet)
	require.Equal(t, int64(2), nestedGet.Count)
	require.False(t, nestedGet.More)
	require.Empty(t, nestedGet.Kvs)

	outsideTenant, err := client.Get(ctx, "/a1566/namespace-nested-txn-empty-start-range-count-only/tenant0/", clientv3.WithPrefix())
	require.NoError(t, err)
	require.Equal(t, int64(2), outsideTenant.Count)
}

func TestClientNamespaceNestedTxnGetEmptyStartRangeKeysOnlyLimitReturnsLogicalPage(t *testing.T) {
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
	tenantPrefix := "/a1567/namespace-nested-txn-empty-start-range-keysonly/tenant/"
	namespacedKV := namespace.NewKV(client.KV, tenantPrefix)
	for _, key := range []string{"alpha/a", "items/a", "items/b", "z/final"} {
		_, err = namespacedKV.Put(ctx, key, "value-"+key)
		require.NoError(t, err)
	}
	_, err = client.Put(ctx, "/a1567/namespace-nested-txn-empty-start-range-keysonly/tenant0/items/a", "outside-tenant")
	require.NoError(t, err)

	txnResp, err := namespacedKV.Txn(ctx).
		Then(clientv3.OpTxn(nil,
			[]clientv3.Op{clientv3.OpGet("",
				clientv3.WithRange("z"),
				clientv3.WithKeysOnly(),
				clientv3.WithLimit(2),
			)},
			nil)).
		Commit()
	require.NoError(t, err)
	require.True(t, txnResp.Succeeded)
	require.Len(t, txnResp.Responses, 1)
	nestedTxn := txnResp.Responses[0].GetResponseTxn()
	require.NotNil(t, nestedTxn)
	require.True(t, nestedTxn.Succeeded)
	require.Len(t, nestedTxn.Responses, 1)
	nestedGet := nestedTxn.Responses[0].GetResponseRange()
	require.NotNil(t, nestedGet)
	require.Equal(t, int64(3), nestedGet.Count)
	require.True(t, nestedGet.More)
	require.Len(t, nestedGet.Kvs, 2)
	require.Equal(t, [][]byte{[]byte("alpha/a"), []byte("items/a")},
		[][]byte{nestedGet.Kvs[0].Key, nestedGet.Kvs[1].Key})
	require.Empty(t, nestedGet.Kvs[0].Value)
	require.Empty(t, nestedGet.Kvs[1].Value)

	outsideTenant, err := client.Get(ctx, "/a1567/namespace-nested-txn-empty-start-range-keysonly/tenant0/", clientv3.WithPrefix())
	require.NoError(t, err)
	require.Equal(t, int64(1), outsideTenant.Count)
}

func TestClientNamespaceNestedTxnGetEmptyStartRangeLimitZeroReturnsFullLogicalRange(t *testing.T) {
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
	tenantPrefix := "/a1583/namespace-nested-txn-empty-start-range-limit-zero/tenant/"
	namespacedKV := namespace.NewKV(client.KV, tenantPrefix)
	for _, key := range []string{"alpha/a", "items/a", "items/b", "z/final"} {
		_, err = namespacedKV.Put(ctx, key, "value-"+key)
		require.NoError(t, err)
	}
	_, err = client.Put(ctx, "/a1583/namespace-nested-txn-empty-start-range-limit-zero/tenant0/items/a", "outside-tenant")
	require.NoError(t, err)

	txnResp, err := namespacedKV.Txn(ctx).
		Then(clientv3.OpTxn(nil,
			[]clientv3.Op{clientv3.OpGet("",
				clientv3.WithRange("z"),
				clientv3.WithLimit(0),
				clientv3.WithSort(clientv3.SortByKey, clientv3.SortAscend),
			)},
			nil)).
		Commit()
	require.NoError(t, err)
	require.True(t, txnResp.Succeeded)
	require.Len(t, txnResp.Responses, 1)
	nestedTxn := txnResp.Responses[0].GetResponseTxn()
	require.NotNil(t, nestedTxn)
	require.True(t, nestedTxn.Succeeded)
	require.Len(t, nestedTxn.Responses, 1)
	nestedGet := nestedTxn.Responses[0].GetResponseRange()
	require.NotNil(t, nestedGet)
	require.Equal(t, int64(3), nestedGet.Count)
	require.False(t, nestedGet.More)
	require.Len(t, nestedGet.Kvs, 3)
	require.Equal(t, [][]byte{[]byte("alpha/a"), []byte("items/a"), []byte("items/b")},
		[][]byte{nestedGet.Kvs[0].Key, nestedGet.Kvs[1].Key, nestedGet.Kvs[2].Key})
	require.Equal(t, [][]byte{[]byte("value-alpha/a"), []byte("value-items/a"), []byte("value-items/b")},
		[][]byte{nestedGet.Kvs[0].Value, nestedGet.Kvs[1].Value, nestedGet.Kvs[2].Value})

	outsideTenant, err := client.Get(ctx, "/a1583/namespace-nested-txn-empty-start-range-limit-zero/tenant0/", clientv3.WithPrefix())
	require.NoError(t, err)
	require.Equal(t, int64(1), outsideTenant.Count)
}

func TestClientNamespaceNestedTxnGetEmptyStartRangeNegativeLimitReturnsFullLogicalRange(t *testing.T) {
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
	tenantPrefix := "/a1584/namespace-nested-txn-empty-start-range-negative-limit/tenant/"
	namespacedKV := namespace.NewKV(client.KV, tenantPrefix)
	for _, key := range []string{"alpha/a", "items/a", "items/b", "z/final"} {
		_, err = namespacedKV.Put(ctx, key, "value-"+key)
		require.NoError(t, err)
	}
	_, err = client.Put(ctx, "/a1584/namespace-nested-txn-empty-start-range-negative-limit/tenant0/items/a", "outside-tenant")
	require.NoError(t, err)

	txnResp, err := namespacedKV.Txn(ctx).
		Then(clientv3.OpTxn(nil,
			[]clientv3.Op{clientv3.OpGet("",
				clientv3.WithRange("z"),
				clientv3.WithLimit(-1),
				clientv3.WithSort(clientv3.SortByKey, clientv3.SortAscend),
			)},
			nil)).
		Commit()
	require.NoError(t, err)
	require.True(t, txnResp.Succeeded)
	require.Len(t, txnResp.Responses, 1)
	nestedTxn := txnResp.Responses[0].GetResponseTxn()
	require.NotNil(t, nestedTxn)
	require.True(t, nestedTxn.Succeeded)
	require.Len(t, nestedTxn.Responses, 1)
	nestedGet := nestedTxn.Responses[0].GetResponseRange()
	require.NotNil(t, nestedGet)
	require.Equal(t, int64(3), nestedGet.Count)
	require.False(t, nestedGet.More)
	require.Len(t, nestedGet.Kvs, 3)
	require.Equal(t, [][]byte{[]byte("alpha/a"), []byte("items/a"), []byte("items/b")},
		[][]byte{nestedGet.Kvs[0].Key, nestedGet.Kvs[1].Key, nestedGet.Kvs[2].Key})
	require.Equal(t, [][]byte{[]byte("value-alpha/a"), []byte("value-items/a"), []byte("value-items/b")},
		[][]byte{nestedGet.Kvs[0].Value, nestedGet.Kvs[1].Value, nestedGet.Kvs[2].Value})

	outsideTenant, err := client.Get(ctx, "/a1584/namespace-nested-txn-empty-start-range-negative-limit/tenant0/", clientv3.WithPrefix())
	require.NoError(t, err)
	require.Equal(t, int64(1), outsideTenant.Count)
}

func TestClientNamespaceNestedTxnGetEmptyStartRangeMaxInt64LimitReturnsFullLogicalRange(t *testing.T) {
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
	tenantPrefix := "/a1585/namespace-nested-txn-empty-start-range-max-limit/tenant/"
	namespacedKV := namespace.NewKV(client.KV, tenantPrefix)
	for _, kv := range []struct {
		key   string
		value string
	}{
		{key: "alpha/a", value: "value-c"},
		{key: "items/a", value: "value-a"},
		{key: "items/b", value: "value-b"},
		{key: "z/final", value: "value-z"},
	} {
		_, err = namespacedKV.Put(ctx, kv.key, kv.value)
		require.NoError(t, err)
	}
	_, err = client.Put(ctx, "/a1585/namespace-nested-txn-empty-start-range-max-limit/tenant0/items/a", "outside-tenant")
	require.NoError(t, err)

	txnResp, err := namespacedKV.Txn(ctx).
		Then(clientv3.OpTxn(nil,
			[]clientv3.Op{clientv3.OpGet("",
				clientv3.WithRange("z"),
				clientv3.WithLimit(math.MaxInt64),
				clientv3.WithSort(clientv3.SortByValue, clientv3.SortNone),
			)},
			nil)).
		Commit()
	require.NoError(t, err)
	require.True(t, txnResp.Succeeded)
	require.Len(t, txnResp.Responses, 1)
	nestedTxn := txnResp.Responses[0].GetResponseTxn()
	require.NotNil(t, nestedTxn)
	require.True(t, nestedTxn.Succeeded)
	require.Len(t, nestedTxn.Responses, 1)
	nestedGet := nestedTxn.Responses[0].GetResponseRange()
	require.NotNil(t, nestedGet)
	require.Equal(t, int64(3), nestedGet.Count)
	require.False(t, nestedGet.More)
	require.Len(t, nestedGet.Kvs, 3)
	require.Equal(t, [][]byte{[]byte("items/a"), []byte("items/b"), []byte("alpha/a")},
		[][]byte{nestedGet.Kvs[0].Key, nestedGet.Kvs[1].Key, nestedGet.Kvs[2].Key})
	require.Equal(t, [][]byte{[]byte("value-a"), []byte("value-b"), []byte("value-c")},
		[][]byte{nestedGet.Kvs[0].Value, nestedGet.Kvs[1].Value, nestedGet.Kvs[2].Value})

	outsideTenant, err := client.Get(ctx, "/a1585/namespace-nested-txn-empty-start-range-max-limit/tenant0/", clientv3.WithPrefix())
	require.NoError(t, err)
	require.Equal(t, int64(1), outsideTenant.Count)
}

func TestClientNamespaceNestedTxnGetEmptyStartRangeFutureRevisionReturnsTypedErrorAndDoesNotCommit(t *testing.T) {
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
	tenantPrefix := "/a1586/namespace-nested-txn-empty-start-range-future-rev/tenant/"
	namespacedKV := namespace.NewKV(client.KV, tenantPrefix)
	for _, key := range []string{"alpha/a", "items/a", "z/final"} {
		_, err = namespacedKV.Put(ctx, key, "value-"+key)
		require.NoError(t, err)
	}
	_, err = client.Put(ctx, "/a1586/namespace-nested-txn-empty-start-range-future-rev/tenant0/items/a", "outside-tenant")
	require.NoError(t, err)

	_, err = namespacedKV.Txn(ctx).
		Then(clientv3.OpTxn(nil,
			[]clientv3.Op{
				clientv3.OpGet("", clientv3.WithRange("z"), clientv3.WithRev(math.MaxInt64)),
				clientv3.OpPut("items/after-future", "must-not-commit"),
			},
			nil)).
		Commit()
	requireClientTxnError(t, err, codes.Unknown, "etcdserver: mvcc: required revision is a future revision", rpctypes.ErrFutureRev)

	afterFuture, err := namespacedKV.Get(ctx, "items/after-future")
	require.NoError(t, err)
	require.Empty(t, afterFuture.Kvs)
	outsideTenant, err := client.Get(ctx, "/a1586/namespace-nested-txn-empty-start-range-future-rev/tenant0/", clientv3.WithPrefix())
	require.NoError(t, err)
	require.Equal(t, int64(1), outsideTenant.Count)
}

func TestClientNamespaceNestedTxnGetEmptyStartRangeCompactedRevisionReturnsTypedErrorAndDoesNotCommit(t *testing.T) {
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
	tenantPrefix := "/a1587/namespace-nested-txn-empty-start-range-compacted-rev/tenant/"
	namespacedKV := namespace.NewKV(client.KV, tenantPrefix)
	seed, err := namespacedKV.Put(ctx, "alpha/a", "old-alpha/a")
	require.NoError(t, err)
	_, err = namespacedKV.Put(ctx, "items/a", "value-items/a")
	require.NoError(t, err)
	latest, err := namespacedKV.Put(ctx, "alpha/a", "new-alpha/a")
	require.NoError(t, err)
	_, err = namespacedKV.Put(ctx, "z/final", "value-z/final")
	require.NoError(t, err)
	_, err = client.Compact(ctx, latest.Header.Revision)
	require.NoError(t, err)
	_, err = client.Put(ctx, "/a1587/namespace-nested-txn-empty-start-range-compacted-rev/tenant0/items/a", "outside-tenant")
	require.NoError(t, err)

	_, err = namespacedKV.Txn(ctx).
		Then(clientv3.OpTxn(nil,
			[]clientv3.Op{
				clientv3.OpGet("", clientv3.WithRange("z"), clientv3.WithRev(seed.Header.Revision)),
				clientv3.OpPut("items/after-compacted", "must-not-commit"),
			},
			nil)).
		Commit()
	requireClientTxnError(t, err, codes.Unknown, "etcdserver: mvcc: required revision has been compacted", rpctypes.ErrCompacted)

	afterCompacted, err := namespacedKV.Get(ctx, "items/after-compacted")
	require.NoError(t, err)
	require.Empty(t, afterCompacted.Kvs)
	current, err := namespacedKV.Get(ctx, "alpha/a")
	require.NoError(t, err)
	require.Len(t, current.Kvs, 1)
	require.Equal(t, []byte("new-alpha/a"), current.Kvs[0].Value)
	outsideTenant, err := client.Get(ctx, "/a1587/namespace-nested-txn-empty-start-range-compacted-rev/tenant0/", clientv3.WithPrefix())
	require.NoError(t, err)
	require.Equal(t, int64(1), outsideTenant.Count)
}

func TestClientNamespaceNestedTxnUnselectedEmptyStartRangeFutureRevisionIsNotValidated(t *testing.T) {
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
	tenantPrefix := "/a1588/namespace-nested-txn-unselected-empty-start-range-future-rev/tenant/"
	namespacedKV := namespace.NewKV(client.KV, tenantPrefix)
	for _, key := range []string{"alpha/a", "items/a", "z/final"} {
		_, err = namespacedKV.Put(ctx, key, "value-"+key)
		require.NoError(t, err)
	}
	_, err = client.Put(ctx, "/a1588/namespace-nested-txn-unselected-empty-start-range-future-rev/tenant0/items/a", "outside-tenant")
	require.NoError(t, err)

	txnResp, err := namespacedKV.Txn(ctx).
		Then(clientv3.OpTxn(
			[]clientv3.Cmp{clientv3.Compare(clientv3.Version("missing"), "=", 1)},
			[]clientv3.Op{
				clientv3.OpGet("", clientv3.WithRange("z"), clientv3.WithRev(math.MaxInt64)),
				clientv3.OpPut("items/unselected-put", "must-not-commit"),
			},
			[]clientv3.Op{clientv3.OpGet("", clientv3.WithRange("z"))},
		)).
		Commit()
	require.NoError(t, err)
	require.True(t, txnResp.Succeeded)
	require.Len(t, txnResp.Responses, 1)
	nestedTxn := txnResp.Responses[0].GetResponseTxn()
	require.NotNil(t, nestedTxn)
	require.False(t, nestedTxn.Succeeded)
	require.Len(t, nestedTxn.Responses, 1)
	nestedGet := nestedTxn.Responses[0].GetResponseRange()
	require.NotNil(t, nestedGet)
	require.Equal(t, int64(2), nestedGet.Count)
	require.False(t, nestedGet.More)
	require.Len(t, nestedGet.Kvs, 2)
	require.Equal(t, [][]byte{[]byte("alpha/a"), []byte("items/a")},
		[][]byte{nestedGet.Kvs[0].Key, nestedGet.Kvs[1].Key})
	require.Equal(t, [][]byte{[]byte("value-alpha/a"), []byte("value-items/a")},
		[][]byte{nestedGet.Kvs[0].Value, nestedGet.Kvs[1].Value})

	unselectedPut, err := namespacedKV.Get(ctx, "items/unselected-put")
	require.NoError(t, err)
	require.Empty(t, unselectedPut.Kvs)
	outsideTenant, err := client.Get(ctx, "/a1588/namespace-nested-txn-unselected-empty-start-range-future-rev/tenant0/", clientv3.WithPrefix())
	require.NoError(t, err)
	require.Equal(t, int64(1), outsideTenant.Count)
}

func TestClientNamespaceNestedTxnUnselectedEmptyStartRangeCompactedRevisionIsNotValidated(t *testing.T) {
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
	tenantPrefix := "/a1589/namespace-nested-txn-unselected-empty-start-range-compacted-rev/tenant/"
	namespacedKV := namespace.NewKV(client.KV, tenantPrefix)
	seed, err := namespacedKV.Put(ctx, "alpha/a", "old-alpha/a")
	require.NoError(t, err)
	_, err = namespacedKV.Put(ctx, "items/a", "value-items/a")
	require.NoError(t, err)
	latest, err := namespacedKV.Put(ctx, "alpha/a", "new-alpha/a")
	require.NoError(t, err)
	_, err = namespacedKV.Put(ctx, "z/final", "value-z/final")
	require.NoError(t, err)
	_, err = client.Compact(ctx, latest.Header.Revision)
	require.NoError(t, err)
	_, err = client.Put(ctx, "/a1589/namespace-nested-txn-unselected-empty-start-range-compacted-rev/tenant0/items/a", "outside-tenant")
	require.NoError(t, err)

	txnResp, err := namespacedKV.Txn(ctx).
		Then(clientv3.OpTxn(
			[]clientv3.Cmp{clientv3.Compare(clientv3.Version("missing"), "=", 1)},
			[]clientv3.Op{
				clientv3.OpGet("", clientv3.WithRange("z"), clientv3.WithRev(seed.Header.Revision)),
				clientv3.OpPut("items/unselected-put", "must-not-commit"),
			},
			[]clientv3.Op{clientv3.OpGet("", clientv3.WithRange("z"))},
		)).
		Commit()
	require.NoError(t, err)
	require.True(t, txnResp.Succeeded)
	require.Len(t, txnResp.Responses, 1)
	nestedTxn := txnResp.Responses[0].GetResponseTxn()
	require.NotNil(t, nestedTxn)
	require.False(t, nestedTxn.Succeeded)
	require.Len(t, nestedTxn.Responses, 1)
	nestedGet := nestedTxn.Responses[0].GetResponseRange()
	require.NotNil(t, nestedGet)
	require.Equal(t, int64(2), nestedGet.Count)
	require.False(t, nestedGet.More)
	require.Len(t, nestedGet.Kvs, 2)
	require.Equal(t, [][]byte{[]byte("alpha/a"), []byte("items/a")},
		[][]byte{nestedGet.Kvs[0].Key, nestedGet.Kvs[1].Key})
	require.Equal(t, [][]byte{[]byte("new-alpha/a"), []byte("value-items/a")},
		[][]byte{nestedGet.Kvs[0].Value, nestedGet.Kvs[1].Value})

	unselectedPut, err := namespacedKV.Get(ctx, "items/unselected-put")
	require.NoError(t, err)
	require.Empty(t, unselectedPut.Kvs)
	outsideTenant, err := client.Get(ctx, "/a1589/namespace-nested-txn-unselected-empty-start-range-compacted-rev/tenant0/", clientv3.WithPrefix())
	require.NoError(t, err)
	require.Equal(t, int64(1), outsideTenant.Count)
}

func TestClientNamespaceNestedTxnGetEmptyStartRangeInvalidSortReturnsTypedErrorAndDoesNotCommit(t *testing.T) {
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
	tenantPrefix := "/a1590/namespace-nested-txn-empty-start-range-invalid-sort/tenant/"
	namespacedKV := namespace.NewKV(client.KV, tenantPrefix)
	for _, key := range []string{"alpha/a", "items/a", "z/final"} {
		_, err = namespacedKV.Put(ctx, key, "value-"+key)
		require.NoError(t, err)
	}
	_, err = client.Put(ctx, "/a1590/namespace-nested-txn-empty-start-range-invalid-sort/tenant0/items/a", "outside-tenant")
	require.NoError(t, err)

	_, err = namespacedKV.Txn(ctx).
		Then(clientv3.OpTxn(nil,
			[]clientv3.Op{
				clientv3.OpGet("",
					clientv3.WithRange("z"),
					clientv3.WithSort(clientv3.SortTarget(99), clientv3.SortOrder(99))),
				clientv3.OpPut("items/after-invalid-sort", "must-not-commit"),
			},
			nil)).
		Commit()
	requireClientTxnError(t, err, codes.Unknown, "etcdserver: invalid sort option", rpctypes.ErrInvalidSortOption)

	afterInvalidSort, err := namespacedKV.Get(ctx, "items/after-invalid-sort")
	require.NoError(t, err)
	require.Empty(t, afterInvalidSort.Kvs)
	outsideTenant, err := client.Get(ctx, "/a1590/namespace-nested-txn-empty-start-range-invalid-sort/tenant0/", clientv3.WithPrefix())
	require.NoError(t, err)
	require.Equal(t, int64(1), outsideTenant.Count)
}

func TestClientNamespaceNestedTxnUnselectedEmptyStartRangeInvalidSortReturnsTypedErrorAndDoesNotCommit(t *testing.T) {
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
	tenantPrefix := "/a1591/namespace-nested-txn-unselected-empty-start-range-invalid-sort/tenant/"
	namespacedKV := namespace.NewKV(client.KV, tenantPrefix)
	for _, key := range []string{"alpha/a", "items/a", "z/final"} {
		_, err = namespacedKV.Put(ctx, key, "value-"+key)
		require.NoError(t, err)
	}
	_, err = client.Put(ctx, "/a1591/namespace-nested-txn-unselected-empty-start-range-invalid-sort/tenant0/items/a", "outside-tenant")
	require.NoError(t, err)

	_, err = namespacedKV.Txn(ctx).
		Then(clientv3.OpTxn(
			[]clientv3.Cmp{clientv3.Compare(clientv3.Version("missing"), "=", 1)},
			[]clientv3.Op{
				clientv3.OpGet("",
					clientv3.WithRange("z"),
					clientv3.WithSort(clientv3.SortTarget(99), clientv3.SortOrder(99))),
				clientv3.OpPut("items/unselected-put", "must-not-commit"),
			},
			[]clientv3.Op{clientv3.OpGet("", clientv3.WithRange("z"))},
		)).
		Commit()
	requireClientTxnError(t, err, codes.Unknown, "etcdserver: invalid sort option", rpctypes.ErrInvalidSortOption)

	unselectedPut, err := namespacedKV.Get(ctx, "items/unselected-put")
	require.NoError(t, err)
	require.Empty(t, unselectedPut.Kvs)
	currentTxn, err := namespacedKV.Txn(ctx).
		Then(clientv3.OpGet("", clientv3.WithRange("z"))).
		Commit()
	require.NoError(t, err)
	require.True(t, currentTxn.Succeeded)
	require.Len(t, currentTxn.Responses, 1)
	current := currentTxn.Responses[0].GetResponseRange()
	require.NotNil(t, current)
	require.Equal(t, int64(2), current.Count)
	require.Len(t, current.Kvs, 2)
	require.Equal(t, [][]byte{[]byte("alpha/a"), []byte("items/a")},
		[][]byte{current.Kvs[0].Key, current.Kvs[1].Key})
	outsideTenant, err := client.Get(ctx, "/a1591/namespace-nested-txn-unselected-empty-start-range-invalid-sort/tenant0/", clientv3.WithPrefix())
	require.NoError(t, err)
	require.Equal(t, int64(1), outsideTenant.Count)
}

func TestClientNamespaceNestedTxnGetEmptyStartRangeContradictoryModRevisionFiltersReturnCountOnly(t *testing.T) {
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
	tenantPrefix := "/a1592/namespace-nested-txn-empty-start-range-contradictory-mod-rev/tenant/"
	namespacedKV := namespace.NewKV(client.KV, tenantPrefix)
	_, err = namespacedKV.Put(ctx, "alpha/a", "value-alpha/a")
	require.NoError(t, err)
	updateItems, err := namespacedKV.Put(ctx, "items/a", "value-items/a")
	require.NoError(t, err)
	_, err = namespacedKV.Put(ctx, "items/b", "value-items/b")
	require.NoError(t, err)
	_, err = namespacedKV.Put(ctx, "z/final", "value-z/final")
	require.NoError(t, err)
	_, err = client.Put(ctx, "/a1592/namespace-nested-txn-empty-start-range-contradictory-mod-rev/tenant0/items/a", "outside-tenant")
	require.NoError(t, err)

	txnResp, err := namespacedKV.Txn(ctx).
		Then(clientv3.OpTxn(nil,
			[]clientv3.Op{clientv3.OpGet("",
				clientv3.WithRange("z"),
				clientv3.WithMinModRev(updateItems.Header.Revision+1),
				clientv3.WithMaxModRev(updateItems.Header.Revision),
				clientv3.WithLimit(1),
			)},
			nil)).
		Commit()
	require.NoError(t, err)
	require.True(t, txnResp.Succeeded)
	require.Len(t, txnResp.Responses, 1)
	nestedTxn := txnResp.Responses[0].GetResponseTxn()
	require.NotNil(t, nestedTxn)
	require.True(t, nestedTxn.Succeeded)
	require.Len(t, nestedTxn.Responses, 1)
	nestedGet := nestedTxn.Responses[0].GetResponseRange()
	require.NotNil(t, nestedGet)
	require.Equal(t, int64(3), nestedGet.Count)
	require.False(t, nestedGet.More)
	require.Empty(t, nestedGet.Kvs)

	outsideTenant, err := client.Get(ctx, "/a1592/namespace-nested-txn-empty-start-range-contradictory-mod-rev/tenant0/", clientv3.WithPrefix())
	require.NoError(t, err)
	require.Equal(t, int64(1), outsideTenant.Count)
}

func TestClientNamespaceNestedTxnGetEmptyStartRangeContradictoryCreateRevisionFiltersReturnCountOnly(t *testing.T) {
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
	tenantPrefix := "/a1593/namespace-nested-txn-empty-start-range-contradictory-create-rev/tenant/"
	namespacedKV := namespace.NewKV(client.KV, tenantPrefix)
	_, err = namespacedKV.Put(ctx, "alpha/a", "value-alpha/a")
	require.NoError(t, err)
	createItems, err := namespacedKV.Put(ctx, "items/a", "value-items/a")
	require.NoError(t, err)
	_, err = namespacedKV.Put(ctx, "items/b", "value-items/b")
	require.NoError(t, err)
	_, err = namespacedKV.Put(ctx, "z/final", "value-z/final")
	require.NoError(t, err)
	_, err = namespacedKV.Put(ctx, "alpha/a", "updated-alpha/a")
	require.NoError(t, err)
	_, err = client.Put(ctx, "/a1593/namespace-nested-txn-empty-start-range-contradictory-create-rev/tenant0/items/a", "outside-tenant")
	require.NoError(t, err)

	txnResp, err := namespacedKV.Txn(ctx).
		Then(clientv3.OpTxn(nil,
			[]clientv3.Op{clientv3.OpGet("",
				clientv3.WithRange("z"),
				clientv3.WithMinCreateRev(createItems.Header.Revision+1),
				clientv3.WithMaxCreateRev(createItems.Header.Revision),
				clientv3.WithLimit(1),
			)},
			nil)).
		Commit()
	require.NoError(t, err)
	require.True(t, txnResp.Succeeded)
	require.Len(t, txnResp.Responses, 1)
	nestedTxn := txnResp.Responses[0].GetResponseTxn()
	require.NotNil(t, nestedTxn)
	require.True(t, nestedTxn.Succeeded)
	require.Len(t, nestedTxn.Responses, 1)
	nestedGet := nestedTxn.Responses[0].GetResponseRange()
	require.NotNil(t, nestedGet)
	require.Equal(t, int64(3), nestedGet.Count)
	require.False(t, nestedGet.More)
	require.Empty(t, nestedGet.Kvs)

	outsideTenant, err := client.Get(ctx, "/a1593/namespace-nested-txn-empty-start-range-contradictory-create-rev/tenant0/", clientv3.WithPrefix())
	require.NoError(t, err)
	require.Equal(t, int64(1), outsideTenant.Count)
}

func TestClientNamespaceNestedTxnGetEmptyStartRangeSortByValueDescReturnsLogicalPage(t *testing.T) {
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
	tenantPrefix := "/a1568/namespace-nested-txn-empty-start-range-sort-value/tenant/"
	namespacedKV := namespace.NewKV(client.KV, tenantPrefix)
	for _, key := range []string{"alpha/a", "items/a", "items/b", "z/final"} {
		_, err = namespacedKV.Put(ctx, key, "value-"+key)
		require.NoError(t, err)
	}
	_, err = client.Put(ctx, "/a1568/namespace-nested-txn-empty-start-range-sort-value/tenant0/items/b", "outside-tenant")
	require.NoError(t, err)

	txnResp, err := namespacedKV.Txn(ctx).
		Then(clientv3.OpTxn(nil,
			[]clientv3.Op{clientv3.OpGet("",
				clientv3.WithRange("z"),
				clientv3.WithLimit(2),
				clientv3.WithSort(clientv3.SortByValue, clientv3.SortDescend),
			)},
			nil)).
		Commit()
	require.NoError(t, err)
	require.True(t, txnResp.Succeeded)
	require.Len(t, txnResp.Responses, 1)
	nestedTxn := txnResp.Responses[0].GetResponseTxn()
	require.NotNil(t, nestedTxn)
	require.True(t, nestedTxn.Succeeded)
	require.Len(t, nestedTxn.Responses, 1)
	nestedGet := nestedTxn.Responses[0].GetResponseRange()
	require.NotNil(t, nestedGet)
	require.Equal(t, int64(3), nestedGet.Count)
	require.True(t, nestedGet.More)
	require.Len(t, nestedGet.Kvs, 2)
	require.Equal(t, [][]byte{[]byte("items/b"), []byte("items/a")},
		[][]byte{nestedGet.Kvs[0].Key, nestedGet.Kvs[1].Key})
	require.Equal(t, [][]byte{[]byte("value-items/b"), []byte("value-items/a")},
		[][]byte{nestedGet.Kvs[0].Value, nestedGet.Kvs[1].Value})

	outsideTenant, err := client.Get(ctx, "/a1568/namespace-nested-txn-empty-start-range-sort-value/tenant0/", clientv3.WithPrefix())
	require.NoError(t, err)
	require.Equal(t, int64(1), outsideTenant.Count)
}

func TestClientNamespaceNestedTxnGetEmptyStartRangeHistoricalRevisionReturnsLogicalKeys(t *testing.T) {
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
	tenantPrefix := "/a1569/namespace-nested-txn-empty-start-range-historical/tenant/"
	namespacedKV := namespace.NewKV(client.KV, tenantPrefix)
	_, err = client.Put(ctx, "/a1569/namespace-nested-txn-empty-start-range-historical/tenant0/items/a", "outside-tenant")
	require.NoError(t, err)
	_, err = namespacedKV.Put(ctx, "alpha/a", "old-alpha")
	require.NoError(t, err)
	historical, err := namespacedKV.Put(ctx, "items/a", "old-items")
	require.NoError(t, err)
	_, err = namespacedKV.Put(ctx, "z/final", "old-z")
	require.NoError(t, err)
	_, err = namespacedKV.Put(ctx, "items/a", "new-items")
	require.NoError(t, err)
	_, err = namespacedKV.Delete(ctx, "alpha/a")
	require.NoError(t, err)
	_, err = namespacedKV.Put(ctx, "later/new", "new-later")
	require.NoError(t, err)

	txnResp, err := namespacedKV.Txn(ctx).
		Then(clientv3.OpTxn(nil,
			[]clientv3.Op{clientv3.OpGet("",
				clientv3.WithRange("z"),
				clientv3.WithRev(historical.Header.Revision),
			)},
			nil)).
		Commit()
	require.NoError(t, err)
	require.True(t, txnResp.Succeeded)
	require.Len(t, txnResp.Responses, 1)
	nestedTxn := txnResp.Responses[0].GetResponseTxn()
	require.NotNil(t, nestedTxn)
	require.True(t, nestedTxn.Succeeded)
	require.Len(t, nestedTxn.Responses, 1)
	nestedGet := nestedTxn.Responses[0].GetResponseRange()
	require.NotNil(t, nestedGet)
	require.Equal(t, int64(2), nestedGet.Count)
	require.False(t, nestedGet.More)
	require.Len(t, nestedGet.Kvs, 2)
	require.Equal(t, [][]byte{[]byte("alpha/a"), []byte("items/a")},
		[][]byte{nestedGet.Kvs[0].Key, nestedGet.Kvs[1].Key})
	require.Equal(t, [][]byte{[]byte("old-alpha"), []byte("old-items")},
		[][]byte{nestedGet.Kvs[0].Value, nestedGet.Kvs[1].Value})

	current, err := namespacedKV.Get(ctx, "", clientv3.WithPrefix())
	require.NoError(t, err)
	require.Equal(t, int64(3), current.Count)
	require.Equal(t, [][]byte{[]byte("items/a"), []byte("later/new"), []byte("z/final")},
		[][]byte{current.Kvs[0].Key, current.Kvs[1].Key, current.Kvs[2].Key})
	outsideTenant, err := client.Get(ctx, "/a1569/namespace-nested-txn-empty-start-range-historical/tenant0/", clientv3.WithPrefix())
	require.NoError(t, err)
	require.Equal(t, int64(1), outsideTenant.Count)
}

func TestClientNamespaceNestedTxnGetEmptyStartRangeSerializableReturnsLogicalKeys(t *testing.T) {
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
	tenantPrefix := "/a1570/namespace-nested-txn-empty-start-range-serializable/tenant/"
	namespacedKV := namespace.NewKV(client.KV, tenantPrefix)
	for _, key := range []string{"alpha/a", "items/a"} {
		_, err = namespacedKV.Put(ctx, key, "value-"+key)
		require.NoError(t, err)
	}
	_, err = client.Put(ctx, "/a1570/namespace-nested-txn-empty-start-range-serializable/tenant0/items/a", "outside-tenant")
	require.NoError(t, err)
	latest, err := namespacedKV.Put(ctx, "z/final", "value-z/final")
	require.NoError(t, err)

	txnResp, err := namespacedKV.Txn(ctx).
		Then(clientv3.OpTxn(nil,
			[]clientv3.Op{clientv3.OpGet("",
				clientv3.WithRange("z"),
				clientv3.WithSerializable(),
			)},
			nil)).
		Commit()
	require.NoError(t, err)
	require.True(t, txnResp.Succeeded)
	require.Len(t, txnResp.Responses, 1)
	nestedTxn := txnResp.Responses[0].GetResponseTxn()
	require.NotNil(t, nestedTxn)
	require.True(t, nestedTxn.Succeeded)
	require.Len(t, nestedTxn.Responses, 1)
	nestedGet := nestedTxn.Responses[0].GetResponseRange()
	require.NotNil(t, nestedGet)
	require.NotNil(t, nestedGet.Header)
	require.Equal(t, latest.Header.Revision, nestedGet.Header.Revision)
	require.Equal(t, int64(2), nestedGet.Count)
	require.False(t, nestedGet.More)
	require.Len(t, nestedGet.Kvs, 2)
	require.Equal(t, [][]byte{[]byte("alpha/a"), []byte("items/a")},
		[][]byte{nestedGet.Kvs[0].Key, nestedGet.Kvs[1].Key})
	require.Equal(t, [][]byte{[]byte("value-alpha/a"), []byte("value-items/a")},
		[][]byte{nestedGet.Kvs[0].Value, nestedGet.Kvs[1].Value})

	outsideTenant, err := client.Get(ctx, "/a1570/namespace-nested-txn-empty-start-range-serializable/tenant0/", clientv3.WithPrefix())
	require.NoError(t, err)
	require.Equal(t, int64(1), outsideTenant.Count)
}

func TestClientNamespaceNestedTxnGetEmptyStartRangeHistoricalSerializableReturnsLogicalKeys(t *testing.T) {
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
	tenantPrefix := "/a1594/namespace-nested-txn-empty-start-range-historical-serializable/tenant/"
	namespacedKV := namespace.NewKV(client.KV, tenantPrefix)
	_, err = client.Put(ctx, "/a1594/namespace-nested-txn-empty-start-range-historical-serializable/tenant0/items/a", "outside-tenant")
	require.NoError(t, err)
	_, err = namespacedKV.Put(ctx, "alpha/a", "old-alpha")
	require.NoError(t, err)
	historical, err := namespacedKV.Put(ctx, "items/a", "old-items")
	require.NoError(t, err)
	_, err = namespacedKV.Put(ctx, "z/final", "value-z/final")
	require.NoError(t, err)
	_, err = namespacedKV.Put(ctx, "items/a", "new-items")
	require.NoError(t, err)
	_, err = namespacedKV.Delete(ctx, "alpha/a")
	require.NoError(t, err)
	_, err = namespacedKV.Put(ctx, "later/new", "new-later")
	require.NoError(t, err)

	txnResp, err := namespacedKV.Txn(ctx).
		Then(clientv3.OpTxn(nil,
			[]clientv3.Op{clientv3.OpGet("",
				clientv3.WithRange("z"),
				clientv3.WithRev(historical.Header.Revision),
				clientv3.WithSerializable(),
			)},
			nil)).
		Commit()
	require.NoError(t, err)
	require.True(t, txnResp.Succeeded)
	require.Len(t, txnResp.Responses, 1)
	nestedTxn := txnResp.Responses[0].GetResponseTxn()
	require.NotNil(t, nestedTxn)
	require.True(t, nestedTxn.Succeeded)
	require.Len(t, nestedTxn.Responses, 1)
	nestedGet := nestedTxn.Responses[0].GetResponseRange()
	require.NotNil(t, nestedGet)
	require.Equal(t, int64(2), nestedGet.Count)
	require.False(t, nestedGet.More)
	require.Len(t, nestedGet.Kvs, 2)
	require.Equal(t, [][]byte{[]byte("alpha/a"), []byte("items/a")},
		[][]byte{nestedGet.Kvs[0].Key, nestedGet.Kvs[1].Key})
	require.Equal(t, [][]byte{[]byte("old-alpha"), []byte("old-items")},
		[][]byte{nestedGet.Kvs[0].Value, nestedGet.Kvs[1].Value})

	current, err := namespacedKV.Get(ctx, "", clientv3.WithPrefix())
	require.NoError(t, err)
	require.Equal(t, int64(3), current.Count)
	require.Equal(t, [][]byte{[]byte("items/a"), []byte("later/new"), []byte("z/final")},
		[][]byte{current.Kvs[0].Key, current.Kvs[1].Key, current.Kvs[2].Key})
	outsideTenant, err := client.Get(ctx, "/a1594/namespace-nested-txn-empty-start-range-historical-serializable/tenant0/", clientv3.WithPrefix())
	require.NoError(t, err)
	require.Equal(t, int64(1), outsideTenant.Count)
}

func TestClientNamespaceNestedTxnGetEmptyStartRangeHistoricalKeysOnlyLimitReturnsLogicalPage(t *testing.T) {
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
	tenantPrefix := "/a1595/namespace-nested-txn-empty-start-range-historical-keysonly/tenant/"
	namespacedKV := namespace.NewKV(client.KV, tenantPrefix)
	_, err = client.Put(ctx, "/a1595/namespace-nested-txn-empty-start-range-historical-keysonly/tenant0/items/a", "outside-tenant")
	require.NoError(t, err)
	_, err = namespacedKV.Put(ctx, "alpha/a", "old-alpha")
	require.NoError(t, err)
	_, err = namespacedKV.Put(ctx, "items/a", "old-items/a")
	require.NoError(t, err)
	_, err = namespacedKV.Put(ctx, "z/final", "value-z/final")
	require.NoError(t, err)
	historical, err := namespacedKV.Put(ctx, "items/b", "old-items/b")
	require.NoError(t, err)
	_, err = namespacedKV.Put(ctx, "items/a", "new-items/a")
	require.NoError(t, err)
	_, err = namespacedKV.Delete(ctx, "alpha/a")
	require.NoError(t, err)
	_, err = namespacedKV.Put(ctx, "later/new", "new-later")
	require.NoError(t, err)

	txnResp, err := namespacedKV.Txn(ctx).
		Then(clientv3.OpTxn(nil,
			[]clientv3.Op{clientv3.OpGet("",
				clientv3.WithRange("z"),
				clientv3.WithRev(historical.Header.Revision),
				clientv3.WithKeysOnly(),
				clientv3.WithLimit(2),
			)},
			nil)).
		Commit()
	require.NoError(t, err)
	require.True(t, txnResp.Succeeded)
	require.Len(t, txnResp.Responses, 1)
	nestedTxn := txnResp.Responses[0].GetResponseTxn()
	require.NotNil(t, nestedTxn)
	require.True(t, nestedTxn.Succeeded)
	require.Len(t, nestedTxn.Responses, 1)
	nestedGet := nestedTxn.Responses[0].GetResponseRange()
	require.NotNil(t, nestedGet)
	require.Equal(t, int64(3), nestedGet.Count)
	require.True(t, nestedGet.More)
	require.Len(t, nestedGet.Kvs, 2)
	require.Equal(t, [][]byte{[]byte("alpha/a"), []byte("items/a")},
		[][]byte{nestedGet.Kvs[0].Key, nestedGet.Kvs[1].Key})
	require.Empty(t, nestedGet.Kvs[0].Value)
	require.Empty(t, nestedGet.Kvs[1].Value)

	current, err := namespacedKV.Get(ctx, "", clientv3.WithPrefix())
	require.NoError(t, err)
	require.Equal(t, int64(4), current.Count)
	require.Equal(t, [][]byte{[]byte("items/a"), []byte("items/b"), []byte("later/new"), []byte("z/final")},
		[][]byte{current.Kvs[0].Key, current.Kvs[1].Key, current.Kvs[2].Key, current.Kvs[3].Key})
	outsideTenant, err := client.Get(ctx, "/a1595/namespace-nested-txn-empty-start-range-historical-keysonly/tenant0/", clientv3.WithPrefix())
	require.NoError(t, err)
	require.Equal(t, int64(1), outsideTenant.Count)
}

func TestClientNamespaceNestedTxnGetEmptyStartRangeHistoricalCountOnlyTakesPrecedence(t *testing.T) {
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
	tenantPrefix := "/a1596/namespace-nested-txn-empty-start-range-historical-countonly/tenant/"
	namespacedKV := namespace.NewKV(client.KV, tenantPrefix)
	_, err = client.Put(ctx, "/a1596/namespace-nested-txn-empty-start-range-historical-countonly/tenant0/items/a", "outside-tenant")
	require.NoError(t, err)
	_, err = namespacedKV.Put(ctx, "alpha/a", "old-alpha")
	require.NoError(t, err)
	_, err = namespacedKV.Put(ctx, "items/a", "old-items/a")
	require.NoError(t, err)
	_, err = namespacedKV.Put(ctx, "z/final", "value-z/final")
	require.NoError(t, err)
	historical, err := namespacedKV.Put(ctx, "items/b", "old-items/b")
	require.NoError(t, err)
	_, err = namespacedKV.Put(ctx, "items/a", "new-items/a")
	require.NoError(t, err)
	_, err = namespacedKV.Delete(ctx, "alpha/a")
	require.NoError(t, err)
	_, err = namespacedKV.Put(ctx, "later/new", "new-later")
	require.NoError(t, err)

	txnResp, err := namespacedKV.Txn(ctx).
		Then(clientv3.OpTxn(nil,
			[]clientv3.Op{clientv3.OpGet("",
				clientv3.WithRange("z"),
				clientv3.WithRev(historical.Header.Revision),
				clientv3.WithKeysOnly(),
				clientv3.WithCountOnly(),
				clientv3.WithLimit(1),
			)},
			nil)).
		Commit()
	require.NoError(t, err)
	require.True(t, txnResp.Succeeded)
	require.Len(t, txnResp.Responses, 1)
	nestedTxn := txnResp.Responses[0].GetResponseTxn()
	require.NotNil(t, nestedTxn)
	require.True(t, nestedTxn.Succeeded)
	require.Len(t, nestedTxn.Responses, 1)
	nestedGet := nestedTxn.Responses[0].GetResponseRange()
	require.NotNil(t, nestedGet)
	require.Equal(t, int64(3), nestedGet.Count)
	require.False(t, nestedGet.More)
	require.Empty(t, nestedGet.Kvs)

	current, err := namespacedKV.Get(ctx, "", clientv3.WithPrefix())
	require.NoError(t, err)
	require.Equal(t, int64(4), current.Count)
	require.Equal(t, [][]byte{[]byte("items/a"), []byte("items/b"), []byte("later/new"), []byte("z/final")},
		[][]byte{current.Kvs[0].Key, current.Kvs[1].Key, current.Kvs[2].Key, current.Kvs[3].Key})
	outsideTenant, err := client.Get(ctx, "/a1596/namespace-nested-txn-empty-start-range-historical-countonly/tenant0/", clientv3.WithPrefix())
	require.NoError(t, err)
	require.Equal(t, int64(1), outsideTenant.Count)
}

func TestClientNamespaceNestedTxnGetEmptyStartRangeHistoricalSortByModRevisionDescReturnsLogicalPage(t *testing.T) {
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
	tenantPrefix := "/a1597/namespace-nested-txn-empty-start-range-historical-mod-sort/tenant/"
	namespacedKV := namespace.NewKV(client.KV, tenantPrefix)
	_, err = client.Put(ctx, "/a1597/namespace-nested-txn-empty-start-range-historical-mod-sort/tenant0/items/newer", "outside-tenant")
	require.NoError(t, err)
	_, err = namespacedKV.Put(ctx, "alpha/a", "old-alpha")
	require.NoError(t, err)
	_, err = namespacedKV.Put(ctx, "items/a", "old-items/a")
	require.NoError(t, err)
	_, err = namespacedKV.Put(ctx, "z/final", "value-z/final")
	require.NoError(t, err)
	historical, err := namespacedKV.Put(ctx, "items/b", "old-items/b")
	require.NoError(t, err)
	_, err = namespacedKV.Put(ctx, "alpha/a", "new-alpha")
	require.NoError(t, err)
	_, err = namespacedKV.Put(ctx, "items/a", "new-items/a")
	require.NoError(t, err)
	_, err = namespacedKV.Put(ctx, "later/new", "new-later")
	require.NoError(t, err)

	txnResp, err := namespacedKV.Txn(ctx).
		Then(clientv3.OpTxn(nil,
			[]clientv3.Op{clientv3.OpGet("",
				clientv3.WithRange("z"),
				clientv3.WithRev(historical.Header.Revision),
				clientv3.WithLimit(2),
				clientv3.WithSort(clientv3.SortByModRevision, clientv3.SortDescend),
			)},
			nil)).
		Commit()
	require.NoError(t, err)
	require.True(t, txnResp.Succeeded)
	require.Len(t, txnResp.Responses, 1)
	nestedTxn := txnResp.Responses[0].GetResponseTxn()
	require.NotNil(t, nestedTxn)
	require.True(t, nestedTxn.Succeeded)
	require.Len(t, nestedTxn.Responses, 1)
	nestedGet := nestedTxn.Responses[0].GetResponseRange()
	require.NotNil(t, nestedGet)
	require.Equal(t, int64(3), nestedGet.Count)
	require.True(t, nestedGet.More)
	require.Len(t, nestedGet.Kvs, 2)
	require.Equal(t, [][]byte{[]byte("items/b"), []byte("items/a")},
		[][]byte{nestedGet.Kvs[0].Key, nestedGet.Kvs[1].Key})
	require.Equal(t, [][]byte{[]byte("old-items/b"), []byte("old-items/a")},
		[][]byte{nestedGet.Kvs[0].Value, nestedGet.Kvs[1].Value})

	current, err := namespacedKV.Get(ctx, "", clientv3.WithPrefix())
	require.NoError(t, err)
	require.Equal(t, int64(5), current.Count)
	require.Equal(t, [][]byte{[]byte("alpha/a"), []byte("items/a"), []byte("items/b"), []byte("later/new"), []byte("z/final")},
		[][]byte{current.Kvs[0].Key, current.Kvs[1].Key, current.Kvs[2].Key, current.Kvs[3].Key, current.Kvs[4].Key})
	outsideTenant, err := client.Get(ctx, "/a1597/namespace-nested-txn-empty-start-range-historical-mod-sort/tenant0/", clientv3.WithPrefix())
	require.NoError(t, err)
	require.Equal(t, int64(1), outsideTenant.Count)
}

func TestClientNamespaceNestedTxnGetEmptyStartRangeHistoricalSortByCreateRevisionDescReturnsLogicalPage(t *testing.T) {
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
	tenantPrefix := "/a1598/namespace-nested-txn-empty-start-range-historical-create-sort/tenant/"
	namespacedKV := namespace.NewKV(client.KV, tenantPrefix)
	_, err = client.Put(ctx, "/a1598/namespace-nested-txn-empty-start-range-historical-create-sort/tenant0/items/newer", "outside-tenant")
	require.NoError(t, err)
	_, err = namespacedKV.Put(ctx, "alpha/a", "old-alpha")
	require.NoError(t, err)
	_, err = namespacedKV.Put(ctx, "items/a", "old-items/a")
	require.NoError(t, err)
	_, err = namespacedKV.Put(ctx, "z/final", "value-z/final")
	require.NoError(t, err)
	historical, err := namespacedKV.Put(ctx, "items/b", "old-items/b")
	require.NoError(t, err)
	_, err = namespacedKV.Delete(ctx, "items/b")
	require.NoError(t, err)
	_, err = namespacedKV.Put(ctx, "alpha/a", "new-alpha")
	require.NoError(t, err)
	_, err = namespacedKV.Put(ctx, "later/new", "new-later")
	require.NoError(t, err)

	txnResp, err := namespacedKV.Txn(ctx).
		Then(clientv3.OpTxn(nil,
			[]clientv3.Op{clientv3.OpGet("",
				clientv3.WithRange("z"),
				clientv3.WithRev(historical.Header.Revision),
				clientv3.WithLimit(2),
				clientv3.WithSort(clientv3.SortByCreateRevision, clientv3.SortDescend),
			)},
			nil)).
		Commit()
	require.NoError(t, err)
	require.True(t, txnResp.Succeeded)
	require.Len(t, txnResp.Responses, 1)
	nestedTxn := txnResp.Responses[0].GetResponseTxn()
	require.NotNil(t, nestedTxn)
	require.True(t, nestedTxn.Succeeded)
	require.Len(t, nestedTxn.Responses, 1)
	nestedGet := nestedTxn.Responses[0].GetResponseRange()
	require.NotNil(t, nestedGet)
	require.Equal(t, int64(3), nestedGet.Count)
	require.True(t, nestedGet.More)
	require.Len(t, nestedGet.Kvs, 2)
	require.Equal(t, [][]byte{[]byte("items/b"), []byte("items/a")},
		[][]byte{nestedGet.Kvs[0].Key, nestedGet.Kvs[1].Key})
	require.Equal(t, [][]byte{[]byte("old-items/b"), []byte("old-items/a")},
		[][]byte{nestedGet.Kvs[0].Value, nestedGet.Kvs[1].Value})

	current, err := namespacedKV.Get(ctx, "", clientv3.WithPrefix())
	require.NoError(t, err)
	require.Equal(t, int64(4), current.Count)
	require.Equal(t, [][]byte{[]byte("alpha/a"), []byte("items/a"), []byte("later/new"), []byte("z/final")},
		[][]byte{current.Kvs[0].Key, current.Kvs[1].Key, current.Kvs[2].Key, current.Kvs[3].Key})
	outsideTenant, err := client.Get(ctx, "/a1598/namespace-nested-txn-empty-start-range-historical-create-sort/tenant0/", clientv3.WithPrefix())
	require.NoError(t, err)
	require.Equal(t, int64(1), outsideTenant.Count)
}

func TestClientNamespaceNestedTxnGetEmptyStartRangeHistoricalSortByVersionDescReturnsLogicalPage(t *testing.T) {
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
	tenantPrefix := "/a1599/namespace-nested-txn-empty-start-range-historical-version-sort/tenant/"
	namespacedKV := namespace.NewKV(client.KV, tenantPrefix)
	_, err = client.Put(ctx, "/a1599/namespace-nested-txn-empty-start-range-historical-version-sort/tenant0/items/newer", "outside-tenant")
	require.NoError(t, err)
	_, err = namespacedKV.Put(ctx, "alpha/a", "old-alpha")
	require.NoError(t, err)
	_, err = namespacedKV.Put(ctx, "items/a", "old-items/a")
	require.NoError(t, err)
	_, err = namespacedKV.Put(ctx, "items/b", "old-items/b")
	require.NoError(t, err)
	_, err = namespacedKV.Put(ctx, "z/final", "value-z/final")
	require.NoError(t, err)
	_, err = namespacedKV.Put(ctx, "alpha/a", "new-alpha")
	require.NoError(t, err)
	_, err = namespacedKV.Put(ctx, "items/b", "new-items/b")
	require.NoError(t, err)
	historical, err := namespacedKV.Put(ctx, "items/b", "historical-items/b")
	require.NoError(t, err)
	_, err = namespacedKV.Delete(ctx, "items/b")
	require.NoError(t, err)
	_, err = namespacedKV.Put(ctx, "items/a", "new-items/a")
	require.NoError(t, err)
	_, err = namespacedKV.Put(ctx, "items/a", "final-items/a")
	require.NoError(t, err)
	_, err = namespacedKV.Put(ctx, "later/new", "new-later")
	require.NoError(t, err)

	txnResp, err := namespacedKV.Txn(ctx).
		Then(clientv3.OpTxn(nil,
			[]clientv3.Op{clientv3.OpGet("",
				clientv3.WithRange("z"),
				clientv3.WithRev(historical.Header.Revision),
				clientv3.WithLimit(2),
				clientv3.WithSort(clientv3.SortByVersion, clientv3.SortDescend),
			)},
			nil)).
		Commit()
	require.NoError(t, err)
	require.True(t, txnResp.Succeeded)
	require.Len(t, txnResp.Responses, 1)
	nestedTxn := txnResp.Responses[0].GetResponseTxn()
	require.NotNil(t, nestedTxn)
	require.True(t, nestedTxn.Succeeded)
	require.Len(t, nestedTxn.Responses, 1)
	nestedGet := nestedTxn.Responses[0].GetResponseRange()
	require.NotNil(t, nestedGet)
	require.Equal(t, int64(3), nestedGet.Count)
	require.True(t, nestedGet.More)
	require.Len(t, nestedGet.Kvs, 2)
	require.Equal(t, [][]byte{[]byte("items/b"), []byte("alpha/a")},
		[][]byte{nestedGet.Kvs[0].Key, nestedGet.Kvs[1].Key})
	require.Equal(t, [][]byte{[]byte("historical-items/b"), []byte("new-alpha")},
		[][]byte{nestedGet.Kvs[0].Value, nestedGet.Kvs[1].Value})
	require.Equal(t, []int64{3, 2}, []int64{nestedGet.Kvs[0].Version, nestedGet.Kvs[1].Version})

	current, err := namespacedKV.Get(ctx, "", clientv3.WithPrefix())
	require.NoError(t, err)
	require.Equal(t, int64(4), current.Count)
	require.Equal(t, [][]byte{[]byte("alpha/a"), []byte("items/a"), []byte("later/new"), []byte("z/final")},
		[][]byte{current.Kvs[0].Key, current.Kvs[1].Key, current.Kvs[2].Key, current.Kvs[3].Key})
	outsideTenant, err := client.Get(ctx, "/a1599/namespace-nested-txn-empty-start-range-historical-version-sort/tenant0/", clientv3.WithPrefix())
	require.NoError(t, err)
	require.Equal(t, int64(1), outsideTenant.Count)
}

func TestClientNamespaceNestedTxnGetEmptyStartRangeHistoricalMinModRevisionReturnsLogicalKeys(t *testing.T) {
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
	tenantPrefix := "/a1600/namespace-nested-txn-empty-start-range-historical-min-mod-rev/tenant/"
	namespacedKV := namespace.NewKV(client.KV, tenantPrefix)
	_, err = client.Put(ctx, "/a1600/namespace-nested-txn-empty-start-range-historical-min-mod-rev/tenant0/items/b", "outside-tenant")
	require.NoError(t, err)
	_, err = namespacedKV.Put(ctx, "alpha/a", "old-alpha")
	require.NoError(t, err)
	_, err = namespacedKV.Put(ctx, "items/a", "old-items/a")
	require.NoError(t, err)
	_, err = namespacedKV.Put(ctx, "z/final", "value-z/final")
	require.NoError(t, err)
	historical, err := namespacedKV.Put(ctx, "items/b", "old-items/b")
	require.NoError(t, err)
	_, err = namespacedKV.Delete(ctx, "items/b")
	require.NoError(t, err)
	_, err = namespacedKV.Put(ctx, "items/a", "new-items/a")
	require.NoError(t, err)
	_, err = namespacedKV.Put(ctx, "later/new", "new-later")
	require.NoError(t, err)

	txnResp, err := namespacedKV.Txn(ctx).
		Then(clientv3.OpTxn(nil,
			[]clientv3.Op{clientv3.OpGet("",
				clientv3.WithRange("z"),
				clientv3.WithRev(historical.Header.Revision),
				clientv3.WithMinModRev(historical.Header.Revision),
				clientv3.WithSort(clientv3.SortByKey, clientv3.SortAscend),
			)},
			nil)).
		Commit()
	require.NoError(t, err)
	require.True(t, txnResp.Succeeded)
	require.Len(t, txnResp.Responses, 1)
	nestedTxn := txnResp.Responses[0].GetResponseTxn()
	require.NotNil(t, nestedTxn)
	require.True(t, nestedTxn.Succeeded)
	require.Len(t, nestedTxn.Responses, 1)
	nestedGet := nestedTxn.Responses[0].GetResponseRange()
	require.NotNil(t, nestedGet)
	require.Equal(t, int64(3), nestedGet.Count)
	require.False(t, nestedGet.More)
	require.Len(t, nestedGet.Kvs, 1)
	require.Equal(t, []byte("items/b"), nestedGet.Kvs[0].Key)
	require.Equal(t, []byte("old-items/b"), nestedGet.Kvs[0].Value)
	require.Equal(t, historical.Header.Revision, nestedGet.Kvs[0].ModRevision)

	current, err := namespacedKV.Get(ctx, "", clientv3.WithPrefix())
	require.NoError(t, err)
	require.Equal(t, int64(4), current.Count)
	require.Equal(t, [][]byte{[]byte("alpha/a"), []byte("items/a"), []byte("later/new"), []byte("z/final")},
		[][]byte{current.Kvs[0].Key, current.Kvs[1].Key, current.Kvs[2].Key, current.Kvs[3].Key})
	outsideTenant, err := client.Get(ctx, "/a1600/namespace-nested-txn-empty-start-range-historical-min-mod-rev/tenant0/", clientv3.WithPrefix())
	require.NoError(t, err)
	require.Equal(t, int64(1), outsideTenant.Count)
}

func TestClientNamespaceNestedTxnGetEmptyStartRangeHistoricalMaxModRevisionReturnsLogicalKeys(t *testing.T) {
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
	tenantPrefix := "/a1601/namespace-nested-txn-empty-start-range-historical-max-mod-rev/tenant/"
	namespacedKV := namespace.NewKV(client.KV, tenantPrefix)
	_, err = client.Put(ctx, "/a1601/namespace-nested-txn-empty-start-range-historical-max-mod-rev/tenant0/items/a", "outside-tenant")
	require.NoError(t, err)
	_, err = namespacedKV.Put(ctx, "alpha/a", "old-alpha")
	require.NoError(t, err)
	_, err = namespacedKV.Put(ctx, "items/a", "old-items/a")
	require.NoError(t, err)
	_, err = namespacedKV.Put(ctx, "z/final", "value-z/final")
	require.NoError(t, err)
	historical, err := namespacedKV.Put(ctx, "items/b", "old-items/b")
	require.NoError(t, err)
	_, err = namespacedKV.Delete(ctx, "items/b")
	require.NoError(t, err)
	_, err = namespacedKV.Put(ctx, "items/a", "new-items/a")
	require.NoError(t, err)
	_, err = namespacedKV.Put(ctx, "later/new", "new-later")
	require.NoError(t, err)

	txnResp, err := namespacedKV.Txn(ctx).
		Then(clientv3.OpTxn(nil,
			[]clientv3.Op{clientv3.OpGet("",
				clientv3.WithRange("z"),
				clientv3.WithRev(historical.Header.Revision),
				clientv3.WithMaxModRev(historical.Header.Revision-1),
				clientv3.WithSort(clientv3.SortByKey, clientv3.SortAscend),
			)},
			nil)).
		Commit()
	require.NoError(t, err)
	require.True(t, txnResp.Succeeded)
	require.Len(t, txnResp.Responses, 1)
	nestedTxn := txnResp.Responses[0].GetResponseTxn()
	require.NotNil(t, nestedTxn)
	require.True(t, nestedTxn.Succeeded)
	require.Len(t, nestedTxn.Responses, 1)
	nestedGet := nestedTxn.Responses[0].GetResponseRange()
	require.NotNil(t, nestedGet)
	require.Equal(t, int64(3), nestedGet.Count)
	require.False(t, nestedGet.More)
	require.Len(t, nestedGet.Kvs, 2)
	require.Equal(t, [][]byte{[]byte("alpha/a"), []byte("items/a")},
		[][]byte{nestedGet.Kvs[0].Key, nestedGet.Kvs[1].Key})
	require.Equal(t, [][]byte{[]byte("old-alpha"), []byte("old-items/a")},
		[][]byte{nestedGet.Kvs[0].Value, nestedGet.Kvs[1].Value})

	current, err := namespacedKV.Get(ctx, "", clientv3.WithPrefix())
	require.NoError(t, err)
	require.Equal(t, int64(4), current.Count)
	require.Equal(t, [][]byte{[]byte("alpha/a"), []byte("items/a"), []byte("later/new"), []byte("z/final")},
		[][]byte{current.Kvs[0].Key, current.Kvs[1].Key, current.Kvs[2].Key, current.Kvs[3].Key})
	outsideTenant, err := client.Get(ctx, "/a1601/namespace-nested-txn-empty-start-range-historical-max-mod-rev/tenant0/", clientv3.WithPrefix())
	require.NoError(t, err)
	require.Equal(t, int64(1), outsideTenant.Count)
}

func TestClientNamespaceNestedTxnGetEmptyStartRangeHistoricalMinCreateRevisionReturnsLogicalKeys(t *testing.T) {
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
	tenantPrefix := "/a1602/namespace-nested-txn-empty-start-range-historical-min-create-rev/tenant/"
	namespacedKV := namespace.NewKV(client.KV, tenantPrefix)
	_, err = client.Put(ctx, "/a1602/namespace-nested-txn-empty-start-range-historical-min-create-rev/tenant0/items/b", "outside-tenant")
	require.NoError(t, err)
	_, err = namespacedKV.Put(ctx, "alpha/a", "old-alpha")
	require.NoError(t, err)
	_, err = namespacedKV.Put(ctx, "items/a", "old-items/a")
	require.NoError(t, err)
	_, err = namespacedKV.Put(ctx, "z/final", "value-z/final")
	require.NoError(t, err)
	historical, err := namespacedKV.Put(ctx, "items/b", "old-items/b")
	require.NoError(t, err)
	_, err = namespacedKV.Delete(ctx, "items/b")
	require.NoError(t, err)
	_, err = namespacedKV.Put(ctx, "alpha/a", "new-alpha")
	require.NoError(t, err)
	_, err = namespacedKV.Put(ctx, "later/new", "new-later")
	require.NoError(t, err)

	txnResp, err := namespacedKV.Txn(ctx).
		Then(clientv3.OpTxn(nil,
			[]clientv3.Op{clientv3.OpGet("",
				clientv3.WithRange("z"),
				clientv3.WithRev(historical.Header.Revision),
				clientv3.WithMinCreateRev(historical.Header.Revision),
				clientv3.WithSort(clientv3.SortByKey, clientv3.SortAscend),
			)},
			nil)).
		Commit()
	require.NoError(t, err)
	require.True(t, txnResp.Succeeded)
	require.Len(t, txnResp.Responses, 1)
	nestedTxn := txnResp.Responses[0].GetResponseTxn()
	require.NotNil(t, nestedTxn)
	require.True(t, nestedTxn.Succeeded)
	require.Len(t, nestedTxn.Responses, 1)
	nestedGet := nestedTxn.Responses[0].GetResponseRange()
	require.NotNil(t, nestedGet)
	require.Equal(t, int64(3), nestedGet.Count)
	require.False(t, nestedGet.More)
	require.Len(t, nestedGet.Kvs, 1)
	require.Equal(t, []byte("items/b"), nestedGet.Kvs[0].Key)
	require.Equal(t, []byte("old-items/b"), nestedGet.Kvs[0].Value)
	require.Equal(t, historical.Header.Revision, nestedGet.Kvs[0].CreateRevision)

	current, err := namespacedKV.Get(ctx, "", clientv3.WithPrefix())
	require.NoError(t, err)
	require.Equal(t, int64(4), current.Count)
	require.Equal(t, [][]byte{[]byte("alpha/a"), []byte("items/a"), []byte("later/new"), []byte("z/final")},
		[][]byte{current.Kvs[0].Key, current.Kvs[1].Key, current.Kvs[2].Key, current.Kvs[3].Key})
	outsideTenant, err := client.Get(ctx, "/a1602/namespace-nested-txn-empty-start-range-historical-min-create-rev/tenant0/", clientv3.WithPrefix())
	require.NoError(t, err)
	require.Equal(t, int64(1), outsideTenant.Count)
}

func TestClientNamespaceNestedTxnGetEmptyStartRangeHistoricalMaxCreateRevisionReturnsLogicalKeys(t *testing.T) {
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
	tenantPrefix := "/a1603/namespace-nested-txn-empty-start-range-historical-max-create-rev/tenant/"
	namespacedKV := namespace.NewKV(client.KV, tenantPrefix)
	_, err = client.Put(ctx, "/a1603/namespace-nested-txn-empty-start-range-historical-max-create-rev/tenant0/items/a", "outside-tenant")
	require.NoError(t, err)
	_, err = namespacedKV.Put(ctx, "alpha/a", "old-alpha")
	require.NoError(t, err)
	_, err = namespacedKV.Put(ctx, "items/a", "old-items/a")
	require.NoError(t, err)
	_, err = namespacedKV.Put(ctx, "z/final", "value-z/final")
	require.NoError(t, err)
	historical, err := namespacedKV.Put(ctx, "items/b", "old-items/b")
	require.NoError(t, err)
	_, err = namespacedKV.Delete(ctx, "items/b")
	require.NoError(t, err)
	_, err = namespacedKV.Put(ctx, "alpha/a", "new-alpha")
	require.NoError(t, err)
	_, err = namespacedKV.Put(ctx, "later/new", "new-later")
	require.NoError(t, err)

	txnResp, err := namespacedKV.Txn(ctx).
		Then(clientv3.OpTxn(nil,
			[]clientv3.Op{clientv3.OpGet("",
				clientv3.WithRange("z"),
				clientv3.WithRev(historical.Header.Revision),
				clientv3.WithMaxCreateRev(historical.Header.Revision-1),
				clientv3.WithSort(clientv3.SortByKey, clientv3.SortAscend),
			)},
			nil)).
		Commit()
	require.NoError(t, err)
	require.True(t, txnResp.Succeeded)
	require.Len(t, txnResp.Responses, 1)
	nestedTxn := txnResp.Responses[0].GetResponseTxn()
	require.NotNil(t, nestedTxn)
	require.True(t, nestedTxn.Succeeded)
	require.Len(t, nestedTxn.Responses, 1)
	nestedGet := nestedTxn.Responses[0].GetResponseRange()
	require.NotNil(t, nestedGet)
	require.Equal(t, int64(3), nestedGet.Count)
	require.False(t, nestedGet.More)
	require.Len(t, nestedGet.Kvs, 2)
	require.Equal(t, [][]byte{[]byte("alpha/a"), []byte("items/a")},
		[][]byte{nestedGet.Kvs[0].Key, nestedGet.Kvs[1].Key})
	require.Equal(t, [][]byte{[]byte("old-alpha"), []byte("old-items/a")},
		[][]byte{nestedGet.Kvs[0].Value, nestedGet.Kvs[1].Value})

	current, err := namespacedKV.Get(ctx, "", clientv3.WithPrefix())
	require.NoError(t, err)
	require.Equal(t, int64(4), current.Count)
	require.Equal(t, [][]byte{[]byte("alpha/a"), []byte("items/a"), []byte("later/new"), []byte("z/final")},
		[][]byte{current.Kvs[0].Key, current.Kvs[1].Key, current.Kvs[2].Key, current.Kvs[3].Key})
	outsideTenant, err := client.Get(ctx, "/a1603/namespace-nested-txn-empty-start-range-historical-max-create-rev/tenant0/", clientv3.WithPrefix())
	require.NoError(t, err)
	require.Equal(t, int64(1), outsideTenant.Count)
}

func TestClientNamespaceNestedTxnGetEmptyStartRangeHistoricalContradictoryModRevisionFiltersReturnCountOnly(t *testing.T) {
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
	tenantPrefix := "/a1604/namespace-nested-txn-empty-start-range-historical-contradictory-mod-rev/tenant/"
	namespacedKV := namespace.NewKV(client.KV, tenantPrefix)
	_, err = client.Put(ctx, "/a1604/namespace-nested-txn-empty-start-range-historical-contradictory-mod-rev/tenant0/items/a", "outside-tenant")
	require.NoError(t, err)
	_, err = namespacedKV.Put(ctx, "alpha/a", "old-alpha")
	require.NoError(t, err)
	_, err = namespacedKV.Put(ctx, "items/a", "old-items/a")
	require.NoError(t, err)
	_, err = namespacedKV.Put(ctx, "z/final", "value-z/final")
	require.NoError(t, err)
	historical, err := namespacedKV.Put(ctx, "items/b", "old-items/b")
	require.NoError(t, err)
	_, err = namespacedKV.Delete(ctx, "items/b")
	require.NoError(t, err)
	_, err = namespacedKV.Put(ctx, "items/a", "new-items/a")
	require.NoError(t, err)
	_, err = namespacedKV.Put(ctx, "later/new", "new-later")
	require.NoError(t, err)

	txnResp, err := namespacedKV.Txn(ctx).
		Then(clientv3.OpTxn(nil,
			[]clientv3.Op{clientv3.OpGet("",
				clientv3.WithRange("z"),
				clientv3.WithRev(historical.Header.Revision),
				clientv3.WithMinModRev(historical.Header.Revision+1),
				clientv3.WithMaxModRev(historical.Header.Revision),
				clientv3.WithLimit(1),
			)},
			nil)).
		Commit()
	require.NoError(t, err)
	require.True(t, txnResp.Succeeded)
	require.Len(t, txnResp.Responses, 1)
	nestedTxn := txnResp.Responses[0].GetResponseTxn()
	require.NotNil(t, nestedTxn)
	require.True(t, nestedTxn.Succeeded)
	require.Len(t, nestedTxn.Responses, 1)
	nestedGet := nestedTxn.Responses[0].GetResponseRange()
	require.NotNil(t, nestedGet)
	require.Equal(t, int64(3), nestedGet.Count)
	require.False(t, nestedGet.More)
	require.Empty(t, nestedGet.Kvs)

	current, err := namespacedKV.Get(ctx, "", clientv3.WithPrefix())
	require.NoError(t, err)
	require.Equal(t, int64(4), current.Count)
	require.Equal(t, [][]byte{[]byte("alpha/a"), []byte("items/a"), []byte("later/new"), []byte("z/final")},
		[][]byte{current.Kvs[0].Key, current.Kvs[1].Key, current.Kvs[2].Key, current.Kvs[3].Key})
	outsideTenant, err := client.Get(ctx, "/a1604/namespace-nested-txn-empty-start-range-historical-contradictory-mod-rev/tenant0/", clientv3.WithPrefix())
	require.NoError(t, err)
	require.Equal(t, int64(1), outsideTenant.Count)
}

func TestClientNamespaceNestedTxnGetEmptyStartRangeHistoricalContradictoryCreateRevisionFiltersReturnCountOnly(t *testing.T) {
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
	tenantPrefix := "/a1605/namespace-nested-txn-empty-start-range-historical-contradictory-create-rev/tenant/"
	namespacedKV := namespace.NewKV(client.KV, tenantPrefix)
	_, err = client.Put(ctx, "/a1605/namespace-nested-txn-empty-start-range-historical-contradictory-create-rev/tenant0/items/a", "outside-tenant")
	require.NoError(t, err)
	_, err = namespacedKV.Put(ctx, "alpha/a", "old-alpha")
	require.NoError(t, err)
	_, err = namespacedKV.Put(ctx, "items/a", "old-items/a")
	require.NoError(t, err)
	_, err = namespacedKV.Put(ctx, "z/final", "value-z/final")
	require.NoError(t, err)
	historical, err := namespacedKV.Put(ctx, "items/b", "old-items/b")
	require.NoError(t, err)
	_, err = namespacedKV.Delete(ctx, "items/b")
	require.NoError(t, err)
	_, err = namespacedKV.Put(ctx, "items/a", "new-items/a")
	require.NoError(t, err)
	_, err = namespacedKV.Put(ctx, "later/new", "new-later")
	require.NoError(t, err)

	txnResp, err := namespacedKV.Txn(ctx).
		Then(clientv3.OpTxn(nil,
			[]clientv3.Op{clientv3.OpGet("",
				clientv3.WithRange("z"),
				clientv3.WithRev(historical.Header.Revision),
				clientv3.WithMinCreateRev(historical.Header.Revision+1),
				clientv3.WithMaxCreateRev(historical.Header.Revision),
				clientv3.WithLimit(1),
			)},
			nil)).
		Commit()
	require.NoError(t, err)
	require.True(t, txnResp.Succeeded)
	require.Len(t, txnResp.Responses, 1)
	nestedTxn := txnResp.Responses[0].GetResponseTxn()
	require.NotNil(t, nestedTxn)
	require.True(t, nestedTxn.Succeeded)
	require.Len(t, nestedTxn.Responses, 1)
	nestedGet := nestedTxn.Responses[0].GetResponseRange()
	require.NotNil(t, nestedGet)
	require.Equal(t, int64(3), nestedGet.Count)
	require.False(t, nestedGet.More)
	require.Empty(t, nestedGet.Kvs)

	current, err := namespacedKV.Get(ctx, "", clientv3.WithPrefix())
	require.NoError(t, err)
	require.Equal(t, int64(4), current.Count)
	require.Equal(t, [][]byte{[]byte("alpha/a"), []byte("items/a"), []byte("later/new"), []byte("z/final")},
		[][]byte{current.Kvs[0].Key, current.Kvs[1].Key, current.Kvs[2].Key, current.Kvs[3].Key})
	outsideTenant, err := client.Get(ctx, "/a1605/namespace-nested-txn-empty-start-range-historical-contradictory-create-rev/tenant0/", clientv3.WithPrefix())
	require.NoError(t, err)
	require.Equal(t, int64(1), outsideTenant.Count)
}

func TestClientNamespaceNestedTxnGetEmptyStartRangeHistoricalWithFirstKeyReturnsLogicalKey(t *testing.T) {
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
	tenantPrefix := "/a1606/namespace-nested-txn-empty-start-range-historical-first-key/tenant/"
	namespacedKV := namespace.NewKV(client.KV, tenantPrefix)
	_, err = client.Put(ctx, "/a1606/namespace-nested-txn-empty-start-range-historical-first-key/tenant0/alpha/a", "outside-tenant")
	require.NoError(t, err)
	_, err = namespacedKV.Put(ctx, "alpha/a", "old-alpha")
	require.NoError(t, err)
	_, err = namespacedKV.Put(ctx, "items/a", "old-items/a")
	require.NoError(t, err)
	_, err = namespacedKV.Put(ctx, "z/final", "value-z/final")
	require.NoError(t, err)
	historical, err := namespacedKV.Put(ctx, "items/b", "old-items/b")
	require.NoError(t, err)
	_, err = namespacedKV.Delete(ctx, "alpha/a")
	require.NoError(t, err)
	_, err = namespacedKV.Put(ctx, "items/a", "new-items/a")
	require.NoError(t, err)
	_, err = namespacedKV.Put(ctx, "later/new", "new-later")
	require.NoError(t, err)

	getOpts := append(clientv3.WithFirstKey(), clientv3.WithRange("z"), clientv3.WithRev(historical.Header.Revision))
	txnResp, err := namespacedKV.Txn(ctx).
		Then(clientv3.OpTxn(nil,
			[]clientv3.Op{clientv3.OpGet("", getOpts...)},
			nil)).
		Commit()
	require.NoError(t, err)
	require.True(t, txnResp.Succeeded)
	require.Len(t, txnResp.Responses, 1)
	nestedTxn := txnResp.Responses[0].GetResponseTxn()
	require.NotNil(t, nestedTxn)
	require.True(t, nestedTxn.Succeeded)
	require.Len(t, nestedTxn.Responses, 1)
	nestedGet := nestedTxn.Responses[0].GetResponseRange()
	require.NotNil(t, nestedGet)
	require.Equal(t, int64(3), nestedGet.Count)
	require.True(t, nestedGet.More)
	require.Len(t, nestedGet.Kvs, 1)
	require.Equal(t, []byte("alpha/a"), nestedGet.Kvs[0].Key)
	require.Equal(t, []byte("old-alpha"), nestedGet.Kvs[0].Value)

	current, err := namespacedKV.Get(ctx, "", clientv3.WithPrefix())
	require.NoError(t, err)
	require.Equal(t, int64(4), current.Count)
	require.Equal(t, [][]byte{[]byte("items/a"), []byte("items/b"), []byte("later/new"), []byte("z/final")},
		[][]byte{current.Kvs[0].Key, current.Kvs[1].Key, current.Kvs[2].Key, current.Kvs[3].Key})
	outsideTenant, err := client.Get(ctx, "/a1606/namespace-nested-txn-empty-start-range-historical-first-key/tenant0/", clientv3.WithPrefix())
	require.NoError(t, err)
	require.Equal(t, int64(1), outsideTenant.Count)
}

func TestClientNamespaceNestedTxnGetEmptyStartRangeHistoricalWithLastKeyReturnsLogicalKey(t *testing.T) {
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
	tenantPrefix := "/a1607/namespace-nested-txn-empty-start-range-historical-last-key/tenant/"
	namespacedKV := namespace.NewKV(client.KV, tenantPrefix)
	_, err = client.Put(ctx, "/a1607/namespace-nested-txn-empty-start-range-historical-last-key/tenant0/items/z", "outside-tenant")
	require.NoError(t, err)
	_, err = namespacedKV.Put(ctx, "alpha/a", "old-alpha")
	require.NoError(t, err)
	_, err = namespacedKV.Put(ctx, "items/a", "old-items/a")
	require.NoError(t, err)
	_, err = namespacedKV.Put(ctx, "z/final", "value-z/final")
	require.NoError(t, err)
	historical, err := namespacedKV.Put(ctx, "items/b", "old-items/b")
	require.NoError(t, err)
	_, err = namespacedKV.Delete(ctx, "items/b")
	require.NoError(t, err)
	_, err = namespacedKV.Put(ctx, "items/a", "new-items/a")
	require.NoError(t, err)
	_, err = namespacedKV.Put(ctx, "later/new", "new-later")
	require.NoError(t, err)

	getOpts := append(clientv3.WithLastKey(), clientv3.WithRange("z"), clientv3.WithRev(historical.Header.Revision))
	txnResp, err := namespacedKV.Txn(ctx).
		Then(clientv3.OpTxn(nil,
			[]clientv3.Op{clientv3.OpGet("", getOpts...)},
			nil)).
		Commit()
	require.NoError(t, err)
	require.True(t, txnResp.Succeeded)
	require.Len(t, txnResp.Responses, 1)
	nestedTxn := txnResp.Responses[0].GetResponseTxn()
	require.NotNil(t, nestedTxn)
	require.True(t, nestedTxn.Succeeded)
	require.Len(t, nestedTxn.Responses, 1)
	nestedGet := nestedTxn.Responses[0].GetResponseRange()
	require.NotNil(t, nestedGet)
	require.Equal(t, int64(3), nestedGet.Count)
	require.True(t, nestedGet.More)
	require.Len(t, nestedGet.Kvs, 1)
	require.Equal(t, []byte("items/b"), nestedGet.Kvs[0].Key)
	require.Equal(t, []byte("old-items/b"), nestedGet.Kvs[0].Value)

	current, err := namespacedKV.Get(ctx, "", clientv3.WithPrefix())
	require.NoError(t, err)
	require.Equal(t, int64(4), current.Count)
	require.Equal(t, [][]byte{[]byte("alpha/a"), []byte("items/a"), []byte("later/new"), []byte("z/final")},
		[][]byte{current.Kvs[0].Key, current.Kvs[1].Key, current.Kvs[2].Key, current.Kvs[3].Key})
	outsideTenant, err := client.Get(ctx, "/a1607/namespace-nested-txn-empty-start-range-historical-last-key/tenant0/", clientv3.WithPrefix())
	require.NoError(t, err)
	require.Equal(t, int64(1), outsideTenant.Count)
}

func TestClientNamespaceNestedTxnGetEmptyStartRangeHistoricalWithFirstCreateReturnsLogicalKey(t *testing.T) {
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
	tenantPrefix := "/a1608/namespace-nested-txn-empty-start-range-historical-first-create/tenant/"
	namespacedKV := namespace.NewKV(client.KV, tenantPrefix)
	_, err = client.Put(ctx, "/a1608/namespace-nested-txn-empty-start-range-historical-first-create/tenant0/alpha/older", "outside-tenant")
	require.NoError(t, err)
	_, err = namespacedKV.Put(ctx, "items/a", "old-items/a")
	require.NoError(t, err)
	_, err = namespacedKV.Put(ctx, "alpha/a", "old-alpha")
	require.NoError(t, err)
	_, err = namespacedKV.Put(ctx, "z/final", "value-z/final")
	require.NoError(t, err)
	historical, err := namespacedKV.Put(ctx, "items/b", "old-items/b")
	require.NoError(t, err)
	_, err = namespacedKV.Delete(ctx, "items/a")
	require.NoError(t, err)
	_, err = namespacedKV.Put(ctx, "alpha/a", "new-alpha")
	require.NoError(t, err)
	_, err = namespacedKV.Put(ctx, "later/new", "new-later")
	require.NoError(t, err)

	getOpts := append(clientv3.WithFirstCreate(), clientv3.WithRange("z"), clientv3.WithRev(historical.Header.Revision))
	txnResp, err := namespacedKV.Txn(ctx).
		Then(clientv3.OpTxn(nil,
			[]clientv3.Op{clientv3.OpGet("", getOpts...)},
			nil)).
		Commit()
	require.NoError(t, err)
	require.True(t, txnResp.Succeeded)
	require.Len(t, txnResp.Responses, 1)
	nestedTxn := txnResp.Responses[0].GetResponseTxn()
	require.NotNil(t, nestedTxn)
	require.True(t, nestedTxn.Succeeded)
	require.Len(t, nestedTxn.Responses, 1)
	nestedGet := nestedTxn.Responses[0].GetResponseRange()
	require.NotNil(t, nestedGet)
	require.Equal(t, int64(3), nestedGet.Count)
	require.True(t, nestedGet.More)
	require.Len(t, nestedGet.Kvs, 1)
	require.Equal(t, []byte("items/a"), nestedGet.Kvs[0].Key)
	require.Equal(t, []byte("old-items/a"), nestedGet.Kvs[0].Value)

	current, err := namespacedKV.Get(ctx, "", clientv3.WithPrefix())
	require.NoError(t, err)
	require.Equal(t, int64(4), current.Count)
	require.Equal(t, [][]byte{[]byte("alpha/a"), []byte("items/b"), []byte("later/new"), []byte("z/final")},
		[][]byte{current.Kvs[0].Key, current.Kvs[1].Key, current.Kvs[2].Key, current.Kvs[3].Key})
	outsideTenant, err := client.Get(ctx, "/a1608/namespace-nested-txn-empty-start-range-historical-first-create/tenant0/", clientv3.WithPrefix())
	require.NoError(t, err)
	require.Equal(t, int64(1), outsideTenant.Count)
}

func TestClientNamespaceNestedTxnGetEmptyStartRangeHistoricalWithLastCreateReturnsLogicalKey(t *testing.T) {
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
	tenantPrefix := "/a1609/namespace-nested-txn-empty-start-range-historical-last-create/tenant/"
	namespacedKV := namespace.NewKV(client.KV, tenantPrefix)
	_, err = client.Put(ctx, "/a1609/namespace-nested-txn-empty-start-range-historical-last-create/tenant0/items/newer", "outside-tenant")
	require.NoError(t, err)
	_, err = namespacedKV.Put(ctx, "items/a", "old-items/a")
	require.NoError(t, err)
	_, err = namespacedKV.Put(ctx, "alpha/a", "old-alpha")
	require.NoError(t, err)
	_, err = namespacedKV.Put(ctx, "z/final", "value-z/final")
	require.NoError(t, err)
	historical, err := namespacedKV.Put(ctx, "items/b", "old-items/b")
	require.NoError(t, err)
	_, err = namespacedKV.Delete(ctx, "items/b")
	require.NoError(t, err)
	_, err = namespacedKV.Put(ctx, "alpha/a", "new-alpha")
	require.NoError(t, err)
	_, err = namespacedKV.Put(ctx, "later/new", "new-later")
	require.NoError(t, err)

	getOpts := append(clientv3.WithLastCreate(), clientv3.WithRange("z"), clientv3.WithRev(historical.Header.Revision))
	txnResp, err := namespacedKV.Txn(ctx).
		Then(clientv3.OpTxn(nil,
			[]clientv3.Op{clientv3.OpGet("", getOpts...)},
			nil)).
		Commit()
	require.NoError(t, err)
	require.True(t, txnResp.Succeeded)
	require.Len(t, txnResp.Responses, 1)
	nestedTxn := txnResp.Responses[0].GetResponseTxn()
	require.NotNil(t, nestedTxn)
	require.True(t, nestedTxn.Succeeded)
	require.Len(t, nestedTxn.Responses, 1)
	nestedGet := nestedTxn.Responses[0].GetResponseRange()
	require.NotNil(t, nestedGet)
	require.Equal(t, int64(3), nestedGet.Count)
	require.True(t, nestedGet.More)
	require.Len(t, nestedGet.Kvs, 1)
	require.Equal(t, []byte("items/b"), nestedGet.Kvs[0].Key)
	require.Equal(t, []byte("old-items/b"), nestedGet.Kvs[0].Value)

	current, err := namespacedKV.Get(ctx, "", clientv3.WithPrefix())
	require.NoError(t, err)
	require.Equal(t, int64(4), current.Count)
	require.Equal(t, [][]byte{[]byte("alpha/a"), []byte("items/a"), []byte("later/new"), []byte("z/final")},
		[][]byte{current.Kvs[0].Key, current.Kvs[1].Key, current.Kvs[2].Key, current.Kvs[3].Key})
	outsideTenant, err := client.Get(ctx, "/a1609/namespace-nested-txn-empty-start-range-historical-last-create/tenant0/", clientv3.WithPrefix())
	require.NoError(t, err)
	require.Equal(t, int64(1), outsideTenant.Count)
}

func TestClientNamespaceNestedTxnGetEmptyStartRangeHistoricalWithFirstRevReturnsLogicalKey(t *testing.T) {
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
	tenantPrefix := "/a1610/namespace-nested-txn-empty-start-range-historical-first-rev/tenant/"
	namespacedKV := namespace.NewKV(client.KV, tenantPrefix)
	_, err = client.Put(ctx, "/a1610/namespace-nested-txn-empty-start-range-historical-first-rev/tenant0/items/older", "outside-tenant")
	require.NoError(t, err)
	_, err = namespacedKV.Put(ctx, "items/a", "old-items/a")
	require.NoError(t, err)
	_, err = namespacedKV.Put(ctx, "alpha/a", "old-alpha")
	require.NoError(t, err)
	_, err = namespacedKV.Put(ctx, "z/final", "value-z/final")
	require.NoError(t, err)
	historical, err := namespacedKV.Put(ctx, "items/b", "old-items/b")
	require.NoError(t, err)
	_, err = namespacedKV.Put(ctx, "items/a", "new-items/a")
	require.NoError(t, err)
	_, err = namespacedKV.Delete(ctx, "items/b")
	require.NoError(t, err)
	_, err = namespacedKV.Put(ctx, "later/new", "new-later")
	require.NoError(t, err)

	getOpts := append(clientv3.WithFirstRev(), clientv3.WithRange("z"), clientv3.WithRev(historical.Header.Revision))
	txnResp, err := namespacedKV.Txn(ctx).
		Then(clientv3.OpTxn(nil,
			[]clientv3.Op{clientv3.OpGet("", getOpts...)},
			nil)).
		Commit()
	require.NoError(t, err)
	require.True(t, txnResp.Succeeded)
	require.Len(t, txnResp.Responses, 1)
	nestedTxn := txnResp.Responses[0].GetResponseTxn()
	require.NotNil(t, nestedTxn)
	require.True(t, nestedTxn.Succeeded)
	require.Len(t, nestedTxn.Responses, 1)
	nestedGet := nestedTxn.Responses[0].GetResponseRange()
	require.NotNil(t, nestedGet)
	require.Equal(t, int64(3), nestedGet.Count)
	require.True(t, nestedGet.More)
	require.Len(t, nestedGet.Kvs, 1)
	require.Equal(t, []byte("items/a"), nestedGet.Kvs[0].Key)
	require.Equal(t, []byte("old-items/a"), nestedGet.Kvs[0].Value)

	current, err := namespacedKV.Get(ctx, "", clientv3.WithPrefix())
	require.NoError(t, err)
	require.Equal(t, int64(4), current.Count)
	require.Equal(t, [][]byte{[]byte("alpha/a"), []byte("items/a"), []byte("later/new"), []byte("z/final")},
		[][]byte{current.Kvs[0].Key, current.Kvs[1].Key, current.Kvs[2].Key, current.Kvs[3].Key})
	outsideTenant, err := client.Get(ctx, "/a1610/namespace-nested-txn-empty-start-range-historical-first-rev/tenant0/", clientv3.WithPrefix())
	require.NoError(t, err)
	require.Equal(t, int64(1), outsideTenant.Count)
}

func TestClientNamespaceNestedTxnGetEmptyStartRangeHistoricalLimitZeroReturnsFullLogicalRange(t *testing.T) {
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
	tenantPrefix := "/a1611/namespace-nested-txn-empty-start-range-historical-limit-zero/tenant/"
	namespacedKV := namespace.NewKV(client.KV, tenantPrefix)
	_, err = client.Put(ctx, "/a1611/namespace-nested-txn-empty-start-range-historical-limit-zero/tenant0/items/a", "outside-tenant")
	require.NoError(t, err)
	_, err = namespacedKV.Put(ctx, "alpha/a", "old-alpha")
	require.NoError(t, err)
	_, err = namespacedKV.Put(ctx, "items/a", "old-items/a")
	require.NoError(t, err)
	_, err = namespacedKV.Put(ctx, "z/final", "value-z/final")
	require.NoError(t, err)
	historical, err := namespacedKV.Put(ctx, "items/b", "old-items/b")
	require.NoError(t, err)
	_, err = namespacedKV.Delete(ctx, "items/b")
	require.NoError(t, err)
	_, err = namespacedKV.Put(ctx, "items/a", "new-items/a")
	require.NoError(t, err)
	_, err = namespacedKV.Put(ctx, "later/new", "new-later")
	require.NoError(t, err)

	txnResp, err := namespacedKV.Txn(ctx).
		Then(clientv3.OpTxn(nil,
			[]clientv3.Op{clientv3.OpGet("",
				clientv3.WithRange("z"),
				clientv3.WithRev(historical.Header.Revision),
				clientv3.WithLimit(0),
				clientv3.WithSort(clientv3.SortByKey, clientv3.SortAscend),
			)},
			nil)).
		Commit()
	require.NoError(t, err)
	require.True(t, txnResp.Succeeded)
	require.Len(t, txnResp.Responses, 1)
	nestedTxn := txnResp.Responses[0].GetResponseTxn()
	require.NotNil(t, nestedTxn)
	require.True(t, nestedTxn.Succeeded)
	require.Len(t, nestedTxn.Responses, 1)
	nestedGet := nestedTxn.Responses[0].GetResponseRange()
	require.NotNil(t, nestedGet)
	require.Equal(t, int64(3), nestedGet.Count)
	require.False(t, nestedGet.More)
	require.Len(t, nestedGet.Kvs, 3)
	require.Equal(t, [][]byte{[]byte("alpha/a"), []byte("items/a"), []byte("items/b")},
		[][]byte{nestedGet.Kvs[0].Key, nestedGet.Kvs[1].Key, nestedGet.Kvs[2].Key})
	require.Equal(t, [][]byte{[]byte("old-alpha"), []byte("old-items/a"), []byte("old-items/b")},
		[][]byte{nestedGet.Kvs[0].Value, nestedGet.Kvs[1].Value, nestedGet.Kvs[2].Value})

	current, err := namespacedKV.Get(ctx, "", clientv3.WithPrefix())
	require.NoError(t, err)
	require.Equal(t, int64(4), current.Count)
	require.Equal(t, [][]byte{[]byte("alpha/a"), []byte("items/a"), []byte("later/new"), []byte("z/final")},
		[][]byte{current.Kvs[0].Key, current.Kvs[1].Key, current.Kvs[2].Key, current.Kvs[3].Key})
	outsideTenant, err := client.Get(ctx, "/a1611/namespace-nested-txn-empty-start-range-historical-limit-zero/tenant0/", clientv3.WithPrefix())
	require.NoError(t, err)
	require.Equal(t, int64(1), outsideTenant.Count)
}

func TestClientNamespaceNestedTxnGetEmptyStartRangeHistoricalNegativeLimitReturnsFullLogicalRange(t *testing.T) {
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
	tenantPrefix := "/a1612/namespace-nested-txn-empty-start-range-historical-negative-limit/tenant/"
	namespacedKV := namespace.NewKV(client.KV, tenantPrefix)
	_, err = client.Put(ctx, "/a1612/namespace-nested-txn-empty-start-range-historical-negative-limit/tenant0/items/a", "outside-tenant")
	require.NoError(t, err)
	_, err = namespacedKV.Put(ctx, "alpha/a", "old-alpha")
	require.NoError(t, err)
	_, err = namespacedKV.Put(ctx, "items/a", "old-items/a")
	require.NoError(t, err)
	_, err = namespacedKV.Put(ctx, "z/final", "value-z/final")
	require.NoError(t, err)
	historical, err := namespacedKV.Put(ctx, "items/b", "old-items/b")
	require.NoError(t, err)
	_, err = namespacedKV.Delete(ctx, "items/b")
	require.NoError(t, err)
	_, err = namespacedKV.Put(ctx, "items/a", "new-items/a")
	require.NoError(t, err)
	_, err = namespacedKV.Put(ctx, "later/new", "new-later")
	require.NoError(t, err)

	txnResp, err := namespacedKV.Txn(ctx).
		Then(clientv3.OpTxn(nil,
			[]clientv3.Op{clientv3.OpGet("",
				clientv3.WithRange("z"),
				clientv3.WithRev(historical.Header.Revision),
				clientv3.WithLimit(-1),
				clientv3.WithSort(clientv3.SortByKey, clientv3.SortAscend),
			)},
			nil)).
		Commit()
	require.NoError(t, err)
	require.True(t, txnResp.Succeeded)
	require.Len(t, txnResp.Responses, 1)
	nestedTxn := txnResp.Responses[0].GetResponseTxn()
	require.NotNil(t, nestedTxn)
	require.True(t, nestedTxn.Succeeded)
	require.Len(t, nestedTxn.Responses, 1)
	nestedGet := nestedTxn.Responses[0].GetResponseRange()
	require.NotNil(t, nestedGet)
	require.Equal(t, int64(3), nestedGet.Count)
	require.False(t, nestedGet.More)
	require.Len(t, nestedGet.Kvs, 3)
	require.Equal(t, [][]byte{[]byte("alpha/a"), []byte("items/a"), []byte("items/b")},
		[][]byte{nestedGet.Kvs[0].Key, nestedGet.Kvs[1].Key, nestedGet.Kvs[2].Key})
	require.Equal(t, [][]byte{[]byte("old-alpha"), []byte("old-items/a"), []byte("old-items/b")},
		[][]byte{nestedGet.Kvs[0].Value, nestedGet.Kvs[1].Value, nestedGet.Kvs[2].Value})

	current, err := namespacedKV.Get(ctx, "", clientv3.WithPrefix())
	require.NoError(t, err)
	require.Equal(t, int64(4), current.Count)
	require.Equal(t, [][]byte{[]byte("alpha/a"), []byte("items/a"), []byte("later/new"), []byte("z/final")},
		[][]byte{current.Kvs[0].Key, current.Kvs[1].Key, current.Kvs[2].Key, current.Kvs[3].Key})
	outsideTenant, err := client.Get(ctx, "/a1612/namespace-nested-txn-empty-start-range-historical-negative-limit/tenant0/", clientv3.WithPrefix())
	require.NoError(t, err)
	require.Equal(t, int64(1), outsideTenant.Count)
}

func TestClientNamespaceNestedTxnGetEmptyStartRangeHistoricalMaxInt64LimitReturnsFullLogicalRange(t *testing.T) {
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
	tenantPrefix := "/a1613/namespace-nested-txn-empty-start-range-historical-max-limit/tenant/"
	namespacedKV := namespace.NewKV(client.KV, tenantPrefix)
	_, err = client.Put(ctx, "/a1613/namespace-nested-txn-empty-start-range-historical-max-limit/tenant0/items/a", "outside-tenant")
	require.NoError(t, err)
	_, err = namespacedKV.Put(ctx, "alpha/a", "old-c")
	require.NoError(t, err)
	_, err = namespacedKV.Put(ctx, "items/a", "old-a")
	require.NoError(t, err)
	_, err = namespacedKV.Put(ctx, "z/final", "old-z")
	require.NoError(t, err)
	historical, err := namespacedKV.Put(ctx, "items/b", "old-b")
	require.NoError(t, err)
	_, err = namespacedKV.Delete(ctx, "items/b")
	require.NoError(t, err)
	_, err = namespacedKV.Put(ctx, "items/a", "new-items/a")
	require.NoError(t, err)
	_, err = namespacedKV.Put(ctx, "later/new", "new-later")
	require.NoError(t, err)

	txnResp, err := namespacedKV.Txn(ctx).
		Then(clientv3.OpTxn(nil,
			[]clientv3.Op{clientv3.OpGet("",
				clientv3.WithRange("z"),
				clientv3.WithRev(historical.Header.Revision),
				clientv3.WithLimit(math.MaxInt64),
				clientv3.WithSort(clientv3.SortByValue, clientv3.SortNone),
			)},
			nil)).
		Commit()
	require.NoError(t, err)
	require.True(t, txnResp.Succeeded)
	require.Len(t, txnResp.Responses, 1)
	nestedTxn := txnResp.Responses[0].GetResponseTxn()
	require.NotNil(t, nestedTxn)
	require.True(t, nestedTxn.Succeeded)
	require.Len(t, nestedTxn.Responses, 1)
	nestedGet := nestedTxn.Responses[0].GetResponseRange()
	require.NotNil(t, nestedGet)
	require.Equal(t, int64(3), nestedGet.Count)
	require.False(t, nestedGet.More)
	require.Len(t, nestedGet.Kvs, 3)
	require.Equal(t, [][]byte{[]byte("items/a"), []byte("items/b"), []byte("alpha/a")},
		[][]byte{nestedGet.Kvs[0].Key, nestedGet.Kvs[1].Key, nestedGet.Kvs[2].Key})
	require.Equal(t, [][]byte{[]byte("old-a"), []byte("old-b"), []byte("old-c")},
		[][]byte{nestedGet.Kvs[0].Value, nestedGet.Kvs[1].Value, nestedGet.Kvs[2].Value})

	current, err := namespacedKV.Get(ctx, "", clientv3.WithPrefix())
	require.NoError(t, err)
	require.Equal(t, int64(4), current.Count)
	require.Equal(t, [][]byte{[]byte("alpha/a"), []byte("items/a"), []byte("later/new"), []byte("z/final")},
		[][]byte{current.Kvs[0].Key, current.Kvs[1].Key, current.Kvs[2].Key, current.Kvs[3].Key})
	outsideTenant, err := client.Get(ctx, "/a1613/namespace-nested-txn-empty-start-range-historical-max-limit/tenant0/", clientv3.WithPrefix())
	require.NoError(t, err)
	require.Equal(t, int64(1), outsideTenant.Count)
}

func TestClientNamespaceNestedTxnGetEmptyStartRangeHistoricalSerializableKeysOnlyLimitReturnsLogicalPage(t *testing.T) {
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
	tenantPrefix := "/a1614/namespace-nested-txn-empty-start-range-historical-serializable-keysonly/tenant/"
	namespacedKV := namespace.NewKV(client.KV, tenantPrefix)
	_, err = client.Put(ctx, "/a1614/namespace-nested-txn-empty-start-range-historical-serializable-keysonly/tenant0/items/a", "outside-tenant")
	require.NoError(t, err)
	_, err = namespacedKV.Put(ctx, "alpha/a", "old-alpha")
	require.NoError(t, err)
	_, err = namespacedKV.Put(ctx, "items/a", "old-items/a")
	require.NoError(t, err)
	_, err = namespacedKV.Put(ctx, "z/final", "value-z/final")
	require.NoError(t, err)
	historical, err := namespacedKV.Put(ctx, "items/b", "old-items/b")
	require.NoError(t, err)
	_, err = namespacedKV.Put(ctx, "items/a", "new-items/a")
	require.NoError(t, err)
	_, err = namespacedKV.Delete(ctx, "alpha/a")
	require.NoError(t, err)
	_, err = namespacedKV.Put(ctx, "later/new", "new-later")
	require.NoError(t, err)

	txnResp, err := namespacedKV.Txn(ctx).
		Then(clientv3.OpTxn(nil,
			[]clientv3.Op{clientv3.OpGet("",
				clientv3.WithRange("z"),
				clientv3.WithRev(historical.Header.Revision),
				clientv3.WithSerializable(),
				clientv3.WithKeysOnly(),
				clientv3.WithLimit(2),
			)},
			nil)).
		Commit()
	require.NoError(t, err)
	require.True(t, txnResp.Succeeded)
	require.Len(t, txnResp.Responses, 1)
	nestedTxn := txnResp.Responses[0].GetResponseTxn()
	require.NotNil(t, nestedTxn)
	require.True(t, nestedTxn.Succeeded)
	require.Len(t, nestedTxn.Responses, 1)
	nestedGet := nestedTxn.Responses[0].GetResponseRange()
	require.NotNil(t, nestedGet)
	require.Equal(t, int64(3), nestedGet.Count)
	require.True(t, nestedGet.More)
	require.Len(t, nestedGet.Kvs, 2)
	require.Equal(t, [][]byte{[]byte("alpha/a"), []byte("items/a")},
		[][]byte{nestedGet.Kvs[0].Key, nestedGet.Kvs[1].Key})
	require.Empty(t, nestedGet.Kvs[0].Value)
	require.Empty(t, nestedGet.Kvs[1].Value)

	current, err := namespacedKV.Get(ctx, "", clientv3.WithPrefix())
	require.NoError(t, err)
	require.Equal(t, int64(4), current.Count)
	require.Equal(t, [][]byte{[]byte("items/a"), []byte("items/b"), []byte("later/new"), []byte("z/final")},
		[][]byte{current.Kvs[0].Key, current.Kvs[1].Key, current.Kvs[2].Key, current.Kvs[3].Key})
	outsideTenant, err := client.Get(ctx, "/a1614/namespace-nested-txn-empty-start-range-historical-serializable-keysonly/tenant0/", clientv3.WithPrefix())
	require.NoError(t, err)
	require.Equal(t, int64(1), outsideTenant.Count)
}

func TestClientNamespaceNestedTxnGetEmptyStartRangeHistoricalSerializableCountOnlyTakesPrecedence(t *testing.T) {
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
	tenantPrefix := "/a1615/namespace-nested-txn-empty-start-range-historical-serializable-countonly/tenant/"
	namespacedKV := namespace.NewKV(client.KV, tenantPrefix)
	_, err = client.Put(ctx, "/a1615/namespace-nested-txn-empty-start-range-historical-serializable-countonly/tenant0/items/a", "outside-tenant")
	require.NoError(t, err)
	_, err = namespacedKV.Put(ctx, "alpha/a", "old-alpha")
	require.NoError(t, err)
	_, err = namespacedKV.Put(ctx, "items/a", "old-items/a")
	require.NoError(t, err)
	_, err = namespacedKV.Put(ctx, "z/final", "value-z/final")
	require.NoError(t, err)
	historical, err := namespacedKV.Put(ctx, "items/b", "old-items/b")
	require.NoError(t, err)
	_, err = namespacedKV.Put(ctx, "items/a", "new-items/a")
	require.NoError(t, err)
	_, err = namespacedKV.Delete(ctx, "alpha/a")
	require.NoError(t, err)
	_, err = namespacedKV.Put(ctx, "later/new", "new-later")
	require.NoError(t, err)

	txnResp, err := namespacedKV.Txn(ctx).
		Then(clientv3.OpTxn(nil,
			[]clientv3.Op{clientv3.OpGet("",
				clientv3.WithRange("z"),
				clientv3.WithRev(historical.Header.Revision),
				clientv3.WithSerializable(),
				clientv3.WithKeysOnly(),
				clientv3.WithCountOnly(),
				clientv3.WithLimit(1),
			)},
			nil)).
		Commit()
	require.NoError(t, err)
	require.True(t, txnResp.Succeeded)
	require.Len(t, txnResp.Responses, 1)
	nestedTxn := txnResp.Responses[0].GetResponseTxn()
	require.NotNil(t, nestedTxn)
	require.True(t, nestedTxn.Succeeded)
	require.Len(t, nestedTxn.Responses, 1)
	nestedGet := nestedTxn.Responses[0].GetResponseRange()
	require.NotNil(t, nestedGet)
	require.Equal(t, int64(3), nestedGet.Count)
	require.False(t, nestedGet.More)
	require.Empty(t, nestedGet.Kvs)

	current, err := namespacedKV.Get(ctx, "", clientv3.WithPrefix())
	require.NoError(t, err)
	require.Equal(t, int64(4), current.Count)
	require.Equal(t, [][]byte{[]byte("items/a"), []byte("items/b"), []byte("later/new"), []byte("z/final")},
		[][]byte{current.Kvs[0].Key, current.Kvs[1].Key, current.Kvs[2].Key, current.Kvs[3].Key})
	outsideTenant, err := client.Get(ctx, "/a1615/namespace-nested-txn-empty-start-range-historical-serializable-countonly/tenant0/", clientv3.WithPrefix())
	require.NoError(t, err)
	require.Equal(t, int64(1), outsideTenant.Count)
}

func TestClientNamespaceNestedTxnGetEmptyStartRangeHistoricalSerializableKeysOnlySortByValueReturnsLogicalPage(t *testing.T) {
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
	tenantPrefix := "/a1616/namespace-nested-txn-empty-start-range-historical-serializable-keysonly-value-sort/tenant/"
	namespacedKV := namespace.NewKV(client.KV, tenantPrefix)
	_, err = client.Put(ctx, "/a1616/namespace-nested-txn-empty-start-range-historical-serializable-keysonly-value-sort/tenant0/items/a", "zz-outside")
	require.NoError(t, err)
	_, err = namespacedKV.Put(ctx, "alpha/a", "m-alpha")
	require.NoError(t, err)
	_, err = namespacedKV.Put(ctx, "items/a", "z-items/a")
	require.NoError(t, err)
	_, err = namespacedKV.Put(ctx, "z/final", "zz-upper")
	require.NoError(t, err)
	historical, err := namespacedKV.Put(ctx, "items/b", "n-items/b")
	require.NoError(t, err)
	_, err = namespacedKV.Put(ctx, "items/a", "a-current-items/a")
	require.NoError(t, err)
	_, err = namespacedKV.Delete(ctx, "alpha/a")
	require.NoError(t, err)
	_, err = namespacedKV.Put(ctx, "later/new", "zz-current-later")
	require.NoError(t, err)

	txnResp, err := namespacedKV.Txn(ctx).
		Then(clientv3.OpTxn(nil,
			[]clientv3.Op{clientv3.OpGet("",
				clientv3.WithRange("z"),
				clientv3.WithRev(historical.Header.Revision),
				clientv3.WithSerializable(),
				clientv3.WithKeysOnly(),
				clientv3.WithLimit(2),
				clientv3.WithSort(clientv3.SortByValue, clientv3.SortDescend),
			)},
			nil)).
		Commit()
	require.NoError(t, err)
	require.True(t, txnResp.Succeeded)
	require.Len(t, txnResp.Responses, 1)
	nestedTxn := txnResp.Responses[0].GetResponseTxn()
	require.NotNil(t, nestedTxn)
	require.True(t, nestedTxn.Succeeded)
	require.Len(t, nestedTxn.Responses, 1)
	nestedGet := nestedTxn.Responses[0].GetResponseRange()
	require.NotNil(t, nestedGet)
	require.Equal(t, int64(3), nestedGet.Count)
	require.True(t, nestedGet.More)
	require.Len(t, nestedGet.Kvs, 2)
	require.Equal(t, [][]byte{[]byte("items/a"), []byte("items/b")},
		[][]byte{nestedGet.Kvs[0].Key, nestedGet.Kvs[1].Key})
	require.Empty(t, nestedGet.Kvs[0].Value)
	require.Empty(t, nestedGet.Kvs[1].Value)

	current, err := namespacedKV.Get(ctx, "", clientv3.WithPrefix())
	require.NoError(t, err)
	require.Equal(t, int64(4), current.Count)
	require.Equal(t, [][]byte{[]byte("items/a"), []byte("items/b"), []byte("later/new"), []byte("z/final")},
		[][]byte{current.Kvs[0].Key, current.Kvs[1].Key, current.Kvs[2].Key, current.Kvs[3].Key})
	outsideTenant, err := client.Get(ctx, "/a1616/namespace-nested-txn-empty-start-range-historical-serializable-keysonly-value-sort/tenant0/", clientv3.WithPrefix())
	require.NoError(t, err)
	require.Equal(t, int64(1), outsideTenant.Count)
}

func TestClientNamespaceNestedTxnGetEmptyStartRangeHistoricalSerializableSortByValueReturnsLogicalPage(t *testing.T) {
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
	tenantPrefix := "/a1617/namespace-nested-txn-empty-start-range-historical-serializable-value-sort/tenant/"
	namespacedKV := namespace.NewKV(client.KV, tenantPrefix)
	_, err = client.Put(ctx, "/a1617/namespace-nested-txn-empty-start-range-historical-serializable-value-sort/tenant0/items/a", "old-0")
	require.NoError(t, err)
	_, err = namespacedKV.Put(ctx, "alpha/a", "old-c")
	require.NoError(t, err)
	_, err = namespacedKV.Put(ctx, "items/a", "old-a")
	require.NoError(t, err)
	_, err = namespacedKV.Put(ctx, "z/final", "old-z")
	require.NoError(t, err)
	historical, err := namespacedKV.Put(ctx, "items/b", "old-b")
	require.NoError(t, err)
	_, err = namespacedKV.Put(ctx, "items/a", "new-z")
	require.NoError(t, err)
	_, err = namespacedKV.Delete(ctx, "alpha/a")
	require.NoError(t, err)
	_, err = namespacedKV.Put(ctx, "later/new", "new-0")
	require.NoError(t, err)

	txnResp, err := namespacedKV.Txn(ctx).
		Then(clientv3.OpTxn(nil,
			[]clientv3.Op{clientv3.OpGet("",
				clientv3.WithRange("z"),
				clientv3.WithRev(historical.Header.Revision),
				clientv3.WithSerializable(),
				clientv3.WithLimit(2),
				clientv3.WithSort(clientv3.SortByValue, clientv3.SortNone),
			)},
			nil)).
		Commit()
	require.NoError(t, err)
	require.True(t, txnResp.Succeeded)
	require.Len(t, txnResp.Responses, 1)
	nestedTxn := txnResp.Responses[0].GetResponseTxn()
	require.NotNil(t, nestedTxn)
	require.True(t, nestedTxn.Succeeded)
	require.Len(t, nestedTxn.Responses, 1)
	nestedGet := nestedTxn.Responses[0].GetResponseRange()
	require.NotNil(t, nestedGet)
	require.Equal(t, int64(3), nestedGet.Count)
	require.True(t, nestedGet.More)
	require.Len(t, nestedGet.Kvs, 2)
	require.Equal(t, [][]byte{[]byte("items/a"), []byte("items/b")},
		[][]byte{nestedGet.Kvs[0].Key, nestedGet.Kvs[1].Key})
	require.Equal(t, [][]byte{[]byte("old-a"), []byte("old-b")},
		[][]byte{nestedGet.Kvs[0].Value, nestedGet.Kvs[1].Value})

	current, err := namespacedKV.Get(ctx, "", clientv3.WithPrefix())
	require.NoError(t, err)
	require.Equal(t, int64(4), current.Count)
	require.Equal(t, [][]byte{[]byte("items/a"), []byte("items/b"), []byte("later/new"), []byte("z/final")},
		[][]byte{current.Kvs[0].Key, current.Kvs[1].Key, current.Kvs[2].Key, current.Kvs[3].Key})
	outsideTenant, err := client.Get(ctx, "/a1617/namespace-nested-txn-empty-start-range-historical-serializable-value-sort/tenant0/", clientv3.WithPrefix())
	require.NoError(t, err)
	require.Equal(t, int64(1), outsideTenant.Count)
}

func TestClientNamespaceNestedTxnGetEmptyStartRangeHistoricalSerializableSortByKeyDescReturnsLogicalPage(t *testing.T) {
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
	tenantPrefix := "/a1618/namespace-nested-txn-empty-start-range-historical-serializable-key-desc/tenant/"
	namespacedKV := namespace.NewKV(client.KV, tenantPrefix)
	_, err = client.Put(ctx, "/a1618/namespace-nested-txn-empty-start-range-historical-serializable-key-desc/tenant0/items/z", "outside-tenant")
	require.NoError(t, err)
	_, err = namespacedKV.Put(ctx, "alpha/a", "old-alpha")
	require.NoError(t, err)
	_, err = namespacedKV.Put(ctx, "items/a", "old-items/a")
	require.NoError(t, err)
	_, err = namespacedKV.Put(ctx, "z/final", "value-z/final")
	require.NoError(t, err)
	historical, err := namespacedKV.Put(ctx, "items/b", "old-items/b")
	require.NoError(t, err)
	_, err = namespacedKV.Put(ctx, "items/a", "new-items/a")
	require.NoError(t, err)
	_, err = namespacedKV.Delete(ctx, "items/b")
	require.NoError(t, err)
	_, err = namespacedKV.Put(ctx, "later/new", "new-later")
	require.NoError(t, err)

	txnResp, err := namespacedKV.Txn(ctx).
		Then(clientv3.OpTxn(nil,
			[]clientv3.Op{clientv3.OpGet("",
				clientv3.WithRange("z"),
				clientv3.WithRev(historical.Header.Revision),
				clientv3.WithSerializable(),
				clientv3.WithLimit(2),
				clientv3.WithSort(clientv3.SortByKey, clientv3.SortDescend),
			)},
			nil)).
		Commit()
	require.NoError(t, err)
	require.True(t, txnResp.Succeeded)
	require.Len(t, txnResp.Responses, 1)
	nestedTxn := txnResp.Responses[0].GetResponseTxn()
	require.NotNil(t, nestedTxn)
	require.True(t, nestedTxn.Succeeded)
	require.Len(t, nestedTxn.Responses, 1)
	nestedGet := nestedTxn.Responses[0].GetResponseRange()
	require.NotNil(t, nestedGet)
	require.Equal(t, int64(3), nestedGet.Count)
	require.True(t, nestedGet.More)
	require.Len(t, nestedGet.Kvs, 2)
	require.Equal(t, [][]byte{[]byte("items/b"), []byte("items/a")},
		[][]byte{nestedGet.Kvs[0].Key, nestedGet.Kvs[1].Key})
	require.Equal(t, [][]byte{[]byte("old-items/b"), []byte("old-items/a")},
		[][]byte{nestedGet.Kvs[0].Value, nestedGet.Kvs[1].Value})

	current, err := namespacedKV.Get(ctx, "", clientv3.WithPrefix())
	require.NoError(t, err)
	require.Equal(t, int64(4), current.Count)
	require.Equal(t, [][]byte{[]byte("alpha/a"), []byte("items/a"), []byte("later/new"), []byte("z/final")},
		[][]byte{current.Kvs[0].Key, current.Kvs[1].Key, current.Kvs[2].Key, current.Kvs[3].Key})
	outsideTenant, err := client.Get(ctx, "/a1618/namespace-nested-txn-empty-start-range-historical-serializable-key-desc/tenant0/", clientv3.WithPrefix())
	require.NoError(t, err)
	require.Equal(t, int64(1), outsideTenant.Count)
}

func TestClientNamespaceNestedTxnGetEmptyStartRangeHistoricalSerializableSortByKeyAscReturnsLogicalPage(t *testing.T) {
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
	tenantPrefix := "/a1619/namespace-nested-txn-empty-start-range-historical-serializable-key-asc/tenant/"
	namespacedKV := namespace.NewKV(client.KV, tenantPrefix)
	_, err = client.Put(ctx, "/a1619/namespace-nested-txn-empty-start-range-historical-serializable-key-asc/tenant0/items/z", "outside-tenant")
	require.NoError(t, err)
	_, err = namespacedKV.Put(ctx, "alpha/a", "old-alpha")
	require.NoError(t, err)
	_, err = namespacedKV.Put(ctx, "items/a", "old-items/a")
	require.NoError(t, err)
	_, err = namespacedKV.Put(ctx, "z/final", "value-z/final")
	require.NoError(t, err)
	historical, err := namespacedKV.Put(ctx, "items/b", "old-items/b")
	require.NoError(t, err)
	_, err = namespacedKV.Delete(ctx, "alpha/a")
	require.NoError(t, err)
	_, err = namespacedKV.Put(ctx, "items/a", "new-items/a")
	require.NoError(t, err)
	_, err = namespacedKV.Put(ctx, "later/new", "new-later")
	require.NoError(t, err)

	txnResp, err := namespacedKV.Txn(ctx).
		Then(clientv3.OpTxn(nil,
			[]clientv3.Op{clientv3.OpGet("",
				clientv3.WithRange("z"),
				clientv3.WithRev(historical.Header.Revision),
				clientv3.WithSerializable(),
				clientv3.WithLimit(2),
				clientv3.WithSort(clientv3.SortByKey, clientv3.SortAscend),
			)},
			nil)).
		Commit()
	require.NoError(t, err)
	require.True(t, txnResp.Succeeded)
	require.Len(t, txnResp.Responses, 1)
	nestedTxn := txnResp.Responses[0].GetResponseTxn()
	require.NotNil(t, nestedTxn)
	require.True(t, nestedTxn.Succeeded)
	require.Len(t, nestedTxn.Responses, 1)
	nestedGet := nestedTxn.Responses[0].GetResponseRange()
	require.NotNil(t, nestedGet)
	require.Equal(t, int64(3), nestedGet.Count)
	require.True(t, nestedGet.More)
	require.Len(t, nestedGet.Kvs, 2)
	require.Equal(t, [][]byte{[]byte("alpha/a"), []byte("items/a")},
		[][]byte{nestedGet.Kvs[0].Key, nestedGet.Kvs[1].Key})
	require.Equal(t, [][]byte{[]byte("old-alpha"), []byte("old-items/a")},
		[][]byte{nestedGet.Kvs[0].Value, nestedGet.Kvs[1].Value})

	current, err := namespacedKV.Get(ctx, "", clientv3.WithPrefix())
	require.NoError(t, err)
	require.Equal(t, int64(4), current.Count)
	require.Equal(t, [][]byte{[]byte("items/a"), []byte("items/b"), []byte("later/new"), []byte("z/final")},
		[][]byte{current.Kvs[0].Key, current.Kvs[1].Key, current.Kvs[2].Key, current.Kvs[3].Key})
	outsideTenant, err := client.Get(ctx, "/a1619/namespace-nested-txn-empty-start-range-historical-serializable-key-asc/tenant0/", clientv3.WithPrefix())
	require.NoError(t, err)
	require.Equal(t, int64(1), outsideTenant.Count)
}

func TestClientNamespaceNestedTxnGetEmptyStartRangeHistoricalSerializableWithFirstKeyReturnsLogicalKey(t *testing.T) {
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
	tenantPrefix := "/a1620/namespace-nested-txn-empty-start-range-historical-serializable-first-key/tenant/"
	namespacedKV := namespace.NewKV(client.KV, tenantPrefix)
	_, err = client.Put(ctx, "/a1620/namespace-nested-txn-empty-start-range-historical-serializable-first-key/tenant0/alpha/a", "outside-tenant")
	require.NoError(t, err)
	_, err = namespacedKV.Put(ctx, "alpha/a", "old-alpha")
	require.NoError(t, err)
	_, err = namespacedKV.Put(ctx, "items/a", "old-items/a")
	require.NoError(t, err)
	_, err = namespacedKV.Put(ctx, "z/final", "value-z/final")
	require.NoError(t, err)
	historical, err := namespacedKV.Put(ctx, "items/b", "old-items/b")
	require.NoError(t, err)
	_, err = namespacedKV.Delete(ctx, "alpha/a")
	require.NoError(t, err)
	_, err = namespacedKV.Put(ctx, "items/a", "new-items/a")
	require.NoError(t, err)
	_, err = namespacedKV.Put(ctx, "later/new", "new-later")
	require.NoError(t, err)

	getOpts := append(clientv3.WithFirstKey(),
		clientv3.WithRange("z"),
		clientv3.WithRev(historical.Header.Revision),
		clientv3.WithSerializable())
	txnResp, err := namespacedKV.Txn(ctx).
		Then(clientv3.OpTxn(nil,
			[]clientv3.Op{clientv3.OpGet("", getOpts...)},
			nil)).
		Commit()
	require.NoError(t, err)
	require.True(t, txnResp.Succeeded)
	require.Len(t, txnResp.Responses, 1)
	nestedTxn := txnResp.Responses[0].GetResponseTxn()
	require.NotNil(t, nestedTxn)
	require.True(t, nestedTxn.Succeeded)
	require.Len(t, nestedTxn.Responses, 1)
	nestedGet := nestedTxn.Responses[0].GetResponseRange()
	require.NotNil(t, nestedGet)
	require.Equal(t, int64(3), nestedGet.Count)
	require.True(t, nestedGet.More)
	require.Len(t, nestedGet.Kvs, 1)
	require.Equal(t, []byte("alpha/a"), nestedGet.Kvs[0].Key)
	require.Equal(t, []byte("old-alpha"), nestedGet.Kvs[0].Value)

	current, err := namespacedKV.Get(ctx, "", clientv3.WithPrefix())
	require.NoError(t, err)
	require.Equal(t, int64(4), current.Count)
	require.Equal(t, [][]byte{[]byte("items/a"), []byte("items/b"), []byte("later/new"), []byte("z/final")},
		[][]byte{current.Kvs[0].Key, current.Kvs[1].Key, current.Kvs[2].Key, current.Kvs[3].Key})
	outsideTenant, err := client.Get(ctx, "/a1620/namespace-nested-txn-empty-start-range-historical-serializable-first-key/tenant0/", clientv3.WithPrefix())
	require.NoError(t, err)
	require.Equal(t, int64(1), outsideTenant.Count)
}

func TestClientNamespaceNestedTxnGetEmptyStartRangeHistoricalSerializableWithLastKeyReturnsLogicalKey(t *testing.T) {
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
	tenantPrefix := "/a1621/namespace-nested-txn-empty-start-range-historical-serializable-last-key/tenant/"
	namespacedKV := namespace.NewKV(client.KV, tenantPrefix)
	_, err = client.Put(ctx, "/a1621/namespace-nested-txn-empty-start-range-historical-serializable-last-key/tenant0/items/z", "outside-tenant")
	require.NoError(t, err)
	_, err = namespacedKV.Put(ctx, "alpha/a", "old-alpha")
	require.NoError(t, err)
	_, err = namespacedKV.Put(ctx, "items/a", "old-items/a")
	require.NoError(t, err)
	_, err = namespacedKV.Put(ctx, "z/final", "value-z/final")
	require.NoError(t, err)
	historical, err := namespacedKV.Put(ctx, "items/b", "old-items/b")
	require.NoError(t, err)
	_, err = namespacedKV.Delete(ctx, "items/b")
	require.NoError(t, err)
	_, err = namespacedKV.Put(ctx, "items/a", "new-items/a")
	require.NoError(t, err)
	_, err = namespacedKV.Put(ctx, "later/new", "new-later")
	require.NoError(t, err)

	getOpts := append(clientv3.WithLastKey(),
		clientv3.WithRange("z"),
		clientv3.WithRev(historical.Header.Revision),
		clientv3.WithSerializable())
	txnResp, err := namespacedKV.Txn(ctx).
		Then(clientv3.OpTxn(nil,
			[]clientv3.Op{clientv3.OpGet("", getOpts...)},
			nil)).
		Commit()
	require.NoError(t, err)
	require.True(t, txnResp.Succeeded)
	require.Len(t, txnResp.Responses, 1)
	nestedTxn := txnResp.Responses[0].GetResponseTxn()
	require.NotNil(t, nestedTxn)
	require.True(t, nestedTxn.Succeeded)
	require.Len(t, nestedTxn.Responses, 1)
	nestedGet := nestedTxn.Responses[0].GetResponseRange()
	require.NotNil(t, nestedGet)
	require.Equal(t, int64(3), nestedGet.Count)
	require.True(t, nestedGet.More)
	require.Len(t, nestedGet.Kvs, 1)
	require.Equal(t, []byte("items/b"), nestedGet.Kvs[0].Key)
	require.Equal(t, []byte("old-items/b"), nestedGet.Kvs[0].Value)

	current, err := namespacedKV.Get(ctx, "", clientv3.WithPrefix())
	require.NoError(t, err)
	require.Equal(t, int64(4), current.Count)
	require.Equal(t, [][]byte{[]byte("alpha/a"), []byte("items/a"), []byte("later/new"), []byte("z/final")},
		[][]byte{current.Kvs[0].Key, current.Kvs[1].Key, current.Kvs[2].Key, current.Kvs[3].Key})
	outsideTenant, err := client.Get(ctx, "/a1621/namespace-nested-txn-empty-start-range-historical-serializable-last-key/tenant0/", clientv3.WithPrefix())
	require.NoError(t, err)
	require.Equal(t, int64(1), outsideTenant.Count)
}

func TestClientNamespaceNestedTxnGetEmptyStartRangeHistoricalSerializableWithFirstCreateReturnsLogicalKey(t *testing.T) {
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
	tenantPrefix := "/a1622/namespace-nested-txn-empty-start-range-historical-serializable-first-create/tenant/"
	namespacedKV := namespace.NewKV(client.KV, tenantPrefix)
	_, err = client.Put(ctx, "/a1622/namespace-nested-txn-empty-start-range-historical-serializable-first-create/tenant0/alpha/older", "outside-tenant")
	require.NoError(t, err)
	_, err = namespacedKV.Put(ctx, "items/a", "old-items/a")
	require.NoError(t, err)
	_, err = namespacedKV.Put(ctx, "alpha/a", "old-alpha")
	require.NoError(t, err)
	_, err = namespacedKV.Put(ctx, "z/final", "value-z/final")
	require.NoError(t, err)
	historical, err := namespacedKV.Put(ctx, "items/b", "old-items/b")
	require.NoError(t, err)
	_, err = namespacedKV.Delete(ctx, "items/a")
	require.NoError(t, err)
	_, err = namespacedKV.Put(ctx, "alpha/a", "new-alpha")
	require.NoError(t, err)
	_, err = namespacedKV.Put(ctx, "later/new", "new-later")
	require.NoError(t, err)

	getOpts := append(clientv3.WithFirstCreate(),
		clientv3.WithRange("z"),
		clientv3.WithRev(historical.Header.Revision),
		clientv3.WithSerializable())
	txnResp, err := namespacedKV.Txn(ctx).
		Then(clientv3.OpTxn(nil,
			[]clientv3.Op{clientv3.OpGet("", getOpts...)},
			nil)).
		Commit()
	require.NoError(t, err)
	require.True(t, txnResp.Succeeded)
	require.Len(t, txnResp.Responses, 1)
	nestedTxn := txnResp.Responses[0].GetResponseTxn()
	require.NotNil(t, nestedTxn)
	require.True(t, nestedTxn.Succeeded)
	require.Len(t, nestedTxn.Responses, 1)
	nestedGet := nestedTxn.Responses[0].GetResponseRange()
	require.NotNil(t, nestedGet)
	require.Equal(t, int64(3), nestedGet.Count)
	require.True(t, nestedGet.More)
	require.Len(t, nestedGet.Kvs, 1)
	require.Equal(t, []byte("items/a"), nestedGet.Kvs[0].Key)
	require.Equal(t, []byte("old-items/a"), nestedGet.Kvs[0].Value)

	current, err := namespacedKV.Get(ctx, "", clientv3.WithPrefix())
	require.NoError(t, err)
	require.Equal(t, int64(4), current.Count)
	require.Equal(t, [][]byte{[]byte("alpha/a"), []byte("items/b"), []byte("later/new"), []byte("z/final")},
		[][]byte{current.Kvs[0].Key, current.Kvs[1].Key, current.Kvs[2].Key, current.Kvs[3].Key})
	outsideTenant, err := client.Get(ctx, "/a1622/namespace-nested-txn-empty-start-range-historical-serializable-first-create/tenant0/", clientv3.WithPrefix())
	require.NoError(t, err)
	require.Equal(t, int64(1), outsideTenant.Count)
}

func TestClientNamespaceNestedTxnGetEmptyStartRangeHistoricalSerializableWithLastCreateReturnsLogicalKey(t *testing.T) {
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
	tenantPrefix := "/a1623/namespace-nested-txn-empty-start-range-historical-serializable-last-create/tenant/"
	namespacedKV := namespace.NewKV(client.KV, tenantPrefix)
	_, err = client.Put(ctx, "/a1623/namespace-nested-txn-empty-start-range-historical-serializable-last-create/tenant0/items/newer", "outside-tenant")
	require.NoError(t, err)
	_, err = namespacedKV.Put(ctx, "items/a", "old-items/a")
	require.NoError(t, err)
	_, err = namespacedKV.Put(ctx, "alpha/a", "old-alpha")
	require.NoError(t, err)
	_, err = namespacedKV.Put(ctx, "z/final", "value-z/final")
	require.NoError(t, err)
	historical, err := namespacedKV.Put(ctx, "items/b", "old-items/b")
	require.NoError(t, err)
	_, err = namespacedKV.Delete(ctx, "items/b")
	require.NoError(t, err)
	_, err = namespacedKV.Put(ctx, "alpha/a", "new-alpha")
	require.NoError(t, err)
	_, err = namespacedKV.Put(ctx, "later/new", "new-later")
	require.NoError(t, err)

	getOpts := append(clientv3.WithLastCreate(),
		clientv3.WithRange("z"),
		clientv3.WithRev(historical.Header.Revision),
		clientv3.WithSerializable())
	txnResp, err := namespacedKV.Txn(ctx).
		Then(clientv3.OpTxn(nil,
			[]clientv3.Op{clientv3.OpGet("", getOpts...)},
			nil)).
		Commit()
	require.NoError(t, err)
	require.True(t, txnResp.Succeeded)
	require.Len(t, txnResp.Responses, 1)
	nestedTxn := txnResp.Responses[0].GetResponseTxn()
	require.NotNil(t, nestedTxn)
	require.True(t, nestedTxn.Succeeded)
	require.Len(t, nestedTxn.Responses, 1)
	nestedGet := nestedTxn.Responses[0].GetResponseRange()
	require.NotNil(t, nestedGet)
	require.Equal(t, int64(3), nestedGet.Count)
	require.True(t, nestedGet.More)
	require.Len(t, nestedGet.Kvs, 1)
	require.Equal(t, []byte("items/b"), nestedGet.Kvs[0].Key)
	require.Equal(t, []byte("old-items/b"), nestedGet.Kvs[0].Value)

	current, err := namespacedKV.Get(ctx, "", clientv3.WithPrefix())
	require.NoError(t, err)
	require.Equal(t, int64(4), current.Count)
	require.Equal(t, [][]byte{[]byte("alpha/a"), []byte("items/a"), []byte("later/new"), []byte("z/final")},
		[][]byte{current.Kvs[0].Key, current.Kvs[1].Key, current.Kvs[2].Key, current.Kvs[3].Key})
	outsideTenant, err := client.Get(ctx, "/a1623/namespace-nested-txn-empty-start-range-historical-serializable-last-create/tenant0/", clientv3.WithPrefix())
	require.NoError(t, err)
	require.Equal(t, int64(1), outsideTenant.Count)
}

func TestClientNamespaceNestedTxnGetEmptyStartRangeHistoricalSerializableWithFirstRevReturnsLogicalKey(t *testing.T) {
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
	tenantPrefix := "/a1624/namespace-nested-txn-empty-start-range-historical-serializable-first-rev/tenant/"
	namespacedKV := namespace.NewKV(client.KV, tenantPrefix)
	_, err = client.Put(ctx, "/a1624/namespace-nested-txn-empty-start-range-historical-serializable-first-rev/tenant0/items/older", "outside-tenant")
	require.NoError(t, err)
	_, err = namespacedKV.Put(ctx, "items/a", "old-items/a")
	require.NoError(t, err)
	_, err = namespacedKV.Put(ctx, "alpha/a", "old-alpha")
	require.NoError(t, err)
	_, err = namespacedKV.Put(ctx, "z/final", "value-z/final")
	require.NoError(t, err)
	historical, err := namespacedKV.Put(ctx, "items/b", "old-items/b")
	require.NoError(t, err)
	_, err = namespacedKV.Put(ctx, "items/a", "new-items/a")
	require.NoError(t, err)
	_, err = namespacedKV.Delete(ctx, "items/b")
	require.NoError(t, err)
	_, err = namespacedKV.Put(ctx, "later/new", "new-later")
	require.NoError(t, err)

	getOpts := append(clientv3.WithFirstRev(),
		clientv3.WithRange("z"),
		clientv3.WithRev(historical.Header.Revision),
		clientv3.WithSerializable())
	txnResp, err := namespacedKV.Txn(ctx).
		Then(clientv3.OpTxn(nil,
			[]clientv3.Op{clientv3.OpGet("", getOpts...)},
			nil)).
		Commit()
	require.NoError(t, err)
	require.True(t, txnResp.Succeeded)
	require.Len(t, txnResp.Responses, 1)
	nestedTxn := txnResp.Responses[0].GetResponseTxn()
	require.NotNil(t, nestedTxn)
	require.True(t, nestedTxn.Succeeded)
	require.Len(t, nestedTxn.Responses, 1)
	nestedGet := nestedTxn.Responses[0].GetResponseRange()
	require.NotNil(t, nestedGet)
	require.Equal(t, int64(3), nestedGet.Count)
	require.True(t, nestedGet.More)
	require.Len(t, nestedGet.Kvs, 1)
	require.Equal(t, []byte("items/a"), nestedGet.Kvs[0].Key)
	require.Equal(t, []byte("old-items/a"), nestedGet.Kvs[0].Value)

	current, err := namespacedKV.Get(ctx, "", clientv3.WithPrefix())
	require.NoError(t, err)
	require.Equal(t, int64(4), current.Count)
	require.Equal(t, [][]byte{[]byte("alpha/a"), []byte("items/a"), []byte("later/new"), []byte("z/final")},
		[][]byte{current.Kvs[0].Key, current.Kvs[1].Key, current.Kvs[2].Key, current.Kvs[3].Key})
	outsideTenant, err := client.Get(ctx, "/a1624/namespace-nested-txn-empty-start-range-historical-serializable-first-rev/tenant0/", clientv3.WithPrefix())
	require.NoError(t, err)
	require.Equal(t, int64(1), outsideTenant.Count)
}

func TestClientNamespaceNestedTxnGetEmptyStartRangeHistoricalSerializableWithLastRevReturnsLogicalKey(t *testing.T) {
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
	tenantPrefix := "/a1625/namespace-nested-txn-empty-start-range-historical-serializable-last-rev/tenant/"
	namespacedKV := namespace.NewKV(client.KV, tenantPrefix)
	_, err = client.Put(ctx, "/a1625/namespace-nested-txn-empty-start-range-historical-serializable-last-rev/tenant0/items/newer", "outside-tenant")
	require.NoError(t, err)
	_, err = namespacedKV.Put(ctx, "items/a", "old-items/a")
	require.NoError(t, err)
	_, err = namespacedKV.Put(ctx, "alpha/a", "old-alpha")
	require.NoError(t, err)
	_, err = namespacedKV.Put(ctx, "z/final", "value-z/final")
	require.NoError(t, err)
	historical, err := namespacedKV.Put(ctx, "items/b", "old-items/b")
	require.NoError(t, err)
	_, err = namespacedKV.Delete(ctx, "items/b")
	require.NoError(t, err)
	_, err = namespacedKV.Put(ctx, "items/a", "new-items/a")
	require.NoError(t, err)
	_, err = namespacedKV.Put(ctx, "later/new", "new-later")
	require.NoError(t, err)

	getOpts := append(clientv3.WithLastRev(),
		clientv3.WithRange("z"),
		clientv3.WithRev(historical.Header.Revision),
		clientv3.WithSerializable())
	txnResp, err := namespacedKV.Txn(ctx).
		Then(clientv3.OpTxn(nil,
			[]clientv3.Op{clientv3.OpGet("", getOpts...)},
			nil)).
		Commit()
	require.NoError(t, err)
	require.True(t, txnResp.Succeeded)
	require.Len(t, txnResp.Responses, 1)
	nestedTxn := txnResp.Responses[0].GetResponseTxn()
	require.NotNil(t, nestedTxn)
	require.True(t, nestedTxn.Succeeded)
	require.Len(t, nestedTxn.Responses, 1)
	nestedGet := nestedTxn.Responses[0].GetResponseRange()
	require.NotNil(t, nestedGet)
	require.Equal(t, int64(3), nestedGet.Count)
	require.True(t, nestedGet.More)
	require.Len(t, nestedGet.Kvs, 1)
	require.Equal(t, []byte("items/b"), nestedGet.Kvs[0].Key)
	require.Equal(t, []byte("old-items/b"), nestedGet.Kvs[0].Value)

	current, err := namespacedKV.Get(ctx, "", clientv3.WithPrefix())
	require.NoError(t, err)
	require.Equal(t, int64(4), current.Count)
	require.Equal(t, [][]byte{[]byte("alpha/a"), []byte("items/a"), []byte("later/new"), []byte("z/final")},
		[][]byte{current.Kvs[0].Key, current.Kvs[1].Key, current.Kvs[2].Key, current.Kvs[3].Key})
	outsideTenant, err := client.Get(ctx, "/a1625/namespace-nested-txn-empty-start-range-historical-serializable-last-rev/tenant0/", clientv3.WithPrefix())
	require.NoError(t, err)
	require.Equal(t, int64(1), outsideTenant.Count)
}

func TestClientNamespaceNestedTxnGetEmptyStartRangeHistoricalSerializableMinModRevisionReturnsLogicalKeys(t *testing.T) {
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
	tenantPrefix := "/a1626/namespace-nested-txn-empty-start-range-historical-serializable-min-mod-rev/tenant/"
	namespacedKV := namespace.NewKV(client.KV, tenantPrefix)
	_, err = client.Put(ctx, "/a1626/namespace-nested-txn-empty-start-range-historical-serializable-min-mod-rev/tenant0/items/b", "outside-tenant")
	require.NoError(t, err)
	_, err = namespacedKV.Put(ctx, "alpha/a", "old-alpha")
	require.NoError(t, err)
	_, err = namespacedKV.Put(ctx, "items/a", "old-items/a")
	require.NoError(t, err)
	_, err = namespacedKV.Put(ctx, "z/final", "value-z/final")
	require.NoError(t, err)
	historical, err := namespacedKV.Put(ctx, "items/b", "old-items/b")
	require.NoError(t, err)
	_, err = namespacedKV.Delete(ctx, "items/b")
	require.NoError(t, err)
	_, err = namespacedKV.Put(ctx, "items/a", "new-items/a")
	require.NoError(t, err)
	_, err = namespacedKV.Put(ctx, "later/new", "new-later")
	require.NoError(t, err)

	txnResp, err := namespacedKV.Txn(ctx).
		Then(clientv3.OpTxn(nil,
			[]clientv3.Op{clientv3.OpGet("",
				clientv3.WithRange("z"),
				clientv3.WithRev(historical.Header.Revision),
				clientv3.WithSerializable(),
				clientv3.WithMinModRev(historical.Header.Revision),
				clientv3.WithSort(clientv3.SortByKey, clientv3.SortAscend),
			)},
			nil)).
		Commit()
	require.NoError(t, err)
	require.True(t, txnResp.Succeeded)
	require.Len(t, txnResp.Responses, 1)
	nestedTxn := txnResp.Responses[0].GetResponseTxn()
	require.NotNil(t, nestedTxn)
	require.True(t, nestedTxn.Succeeded)
	require.Len(t, nestedTxn.Responses, 1)
	nestedGet := nestedTxn.Responses[0].GetResponseRange()
	require.NotNil(t, nestedGet)
	require.Equal(t, int64(3), nestedGet.Count)
	require.False(t, nestedGet.More)
	require.Len(t, nestedGet.Kvs, 1)
	require.Equal(t, []byte("items/b"), nestedGet.Kvs[0].Key)
	require.Equal(t, []byte("old-items/b"), nestedGet.Kvs[0].Value)
	require.Equal(t, historical.Header.Revision, nestedGet.Kvs[0].ModRevision)

	current, err := namespacedKV.Get(ctx, "", clientv3.WithPrefix())
	require.NoError(t, err)
	require.Equal(t, int64(4), current.Count)
	require.Equal(t, [][]byte{[]byte("alpha/a"), []byte("items/a"), []byte("later/new"), []byte("z/final")},
		[][]byte{current.Kvs[0].Key, current.Kvs[1].Key, current.Kvs[2].Key, current.Kvs[3].Key})
	outsideTenant, err := client.Get(ctx, "/a1626/namespace-nested-txn-empty-start-range-historical-serializable-min-mod-rev/tenant0/", clientv3.WithPrefix())
	require.NoError(t, err)
	require.Equal(t, int64(1), outsideTenant.Count)
}

func TestClientNamespaceNestedTxnGetEmptyStartRangeHistoricalSerializableMaxModRevisionReturnsLogicalKeys(t *testing.T) {
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
	tenantPrefix := "/a1627/namespace-nested-txn-empty-start-range-historical-serializable-max-mod-rev/tenant/"
	namespacedKV := namespace.NewKV(client.KV, tenantPrefix)
	_, err = client.Put(ctx, "/a1627/namespace-nested-txn-empty-start-range-historical-serializable-max-mod-rev/tenant0/items/a", "outside-tenant")
	require.NoError(t, err)
	_, err = namespacedKV.Put(ctx, "alpha/a", "old-alpha")
	require.NoError(t, err)
	_, err = namespacedKV.Put(ctx, "items/a", "old-items/a")
	require.NoError(t, err)
	_, err = namespacedKV.Put(ctx, "z/final", "value-z/final")
	require.NoError(t, err)
	historical, err := namespacedKV.Put(ctx, "items/b", "old-items/b")
	require.NoError(t, err)
	_, err = namespacedKV.Delete(ctx, "items/b")
	require.NoError(t, err)
	_, err = namespacedKV.Put(ctx, "items/a", "new-items/a")
	require.NoError(t, err)
	_, err = namespacedKV.Put(ctx, "later/new", "new-later")
	require.NoError(t, err)

	txnResp, err := namespacedKV.Txn(ctx).
		Then(clientv3.OpTxn(nil,
			[]clientv3.Op{clientv3.OpGet("",
				clientv3.WithRange("z"),
				clientv3.WithRev(historical.Header.Revision),
				clientv3.WithSerializable(),
				clientv3.WithMaxModRev(historical.Header.Revision-1),
				clientv3.WithSort(clientv3.SortByKey, clientv3.SortAscend),
			)},
			nil)).
		Commit()
	require.NoError(t, err)
	require.True(t, txnResp.Succeeded)
	require.Len(t, txnResp.Responses, 1)
	nestedTxn := txnResp.Responses[0].GetResponseTxn()
	require.NotNil(t, nestedTxn)
	require.True(t, nestedTxn.Succeeded)
	require.Len(t, nestedTxn.Responses, 1)
	nestedGet := nestedTxn.Responses[0].GetResponseRange()
	require.NotNil(t, nestedGet)
	require.Equal(t, int64(3), nestedGet.Count)
	require.False(t, nestedGet.More)
	require.Len(t, nestedGet.Kvs, 2)
	require.Equal(t, [][]byte{[]byte("alpha/a"), []byte("items/a")},
		[][]byte{nestedGet.Kvs[0].Key, nestedGet.Kvs[1].Key})
	require.Equal(t, [][]byte{[]byte("old-alpha"), []byte("old-items/a")},
		[][]byte{nestedGet.Kvs[0].Value, nestedGet.Kvs[1].Value})

	current, err := namespacedKV.Get(ctx, "", clientv3.WithPrefix())
	require.NoError(t, err)
	require.Equal(t, int64(4), current.Count)
	require.Equal(t, [][]byte{[]byte("alpha/a"), []byte("items/a"), []byte("later/new"), []byte("z/final")},
		[][]byte{current.Kvs[0].Key, current.Kvs[1].Key, current.Kvs[2].Key, current.Kvs[3].Key})
	outsideTenant, err := client.Get(ctx, "/a1627/namespace-nested-txn-empty-start-range-historical-serializable-max-mod-rev/tenant0/", clientv3.WithPrefix())
	require.NoError(t, err)
	require.Equal(t, int64(1), outsideTenant.Count)
}

func TestClientNamespaceNestedTxnGetEmptyStartRangeHistoricalSerializableMinCreateRevisionReturnsLogicalKeys(t *testing.T) {
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
	tenantPrefix := "/a1628/namespace-nested-txn-empty-start-range-historical-serializable-min-create-rev/tenant/"
	namespacedKV := namespace.NewKV(client.KV, tenantPrefix)
	_, err = client.Put(ctx, "/a1628/namespace-nested-txn-empty-start-range-historical-serializable-min-create-rev/tenant0/items/b", "outside-tenant")
	require.NoError(t, err)
	_, err = namespacedKV.Put(ctx, "alpha/a", "old-alpha")
	require.NoError(t, err)
	_, err = namespacedKV.Put(ctx, "items/a", "old-items/a")
	require.NoError(t, err)
	_, err = namespacedKV.Put(ctx, "z/final", "value-z/final")
	require.NoError(t, err)
	historical, err := namespacedKV.Put(ctx, "items/b", "old-items/b")
	require.NoError(t, err)
	_, err = namespacedKV.Delete(ctx, "items/b")
	require.NoError(t, err)
	_, err = namespacedKV.Put(ctx, "alpha/a", "new-alpha")
	require.NoError(t, err)
	_, err = namespacedKV.Put(ctx, "later/new", "new-later")
	require.NoError(t, err)

	txnResp, err := namespacedKV.Txn(ctx).
		Then(clientv3.OpTxn(nil,
			[]clientv3.Op{clientv3.OpGet("",
				clientv3.WithRange("z"),
				clientv3.WithRev(historical.Header.Revision),
				clientv3.WithSerializable(),
				clientv3.WithMinCreateRev(historical.Header.Revision),
				clientv3.WithSort(clientv3.SortByKey, clientv3.SortAscend),
			)},
			nil)).
		Commit()
	require.NoError(t, err)
	require.True(t, txnResp.Succeeded)
	require.Len(t, txnResp.Responses, 1)
	nestedTxn := txnResp.Responses[0].GetResponseTxn()
	require.NotNil(t, nestedTxn)
	require.True(t, nestedTxn.Succeeded)
	require.Len(t, nestedTxn.Responses, 1)
	nestedGet := nestedTxn.Responses[0].GetResponseRange()
	require.NotNil(t, nestedGet)
	require.Equal(t, int64(3), nestedGet.Count)
	require.False(t, nestedGet.More)
	require.Len(t, nestedGet.Kvs, 1)
	require.Equal(t, []byte("items/b"), nestedGet.Kvs[0].Key)
	require.Equal(t, []byte("old-items/b"), nestedGet.Kvs[0].Value)
	require.Equal(t, historical.Header.Revision, nestedGet.Kvs[0].CreateRevision)

	current, err := namespacedKV.Get(ctx, "", clientv3.WithPrefix())
	require.NoError(t, err)
	require.Equal(t, int64(4), current.Count)
	require.Equal(t, [][]byte{[]byte("alpha/a"), []byte("items/a"), []byte("later/new"), []byte("z/final")},
		[][]byte{current.Kvs[0].Key, current.Kvs[1].Key, current.Kvs[2].Key, current.Kvs[3].Key})
	outsideTenant, err := client.Get(ctx, "/a1628/namespace-nested-txn-empty-start-range-historical-serializable-min-create-rev/tenant0/", clientv3.WithPrefix())
	require.NoError(t, err)
	require.Equal(t, int64(1), outsideTenant.Count)
}

func TestClientNamespaceNestedTxnGetEmptyStartRangeHistoricalSerializableMaxCreateRevisionReturnsLogicalKeys(t *testing.T) {
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
	tenantPrefix := "/a1629/namespace-nested-txn-empty-start-range-historical-serializable-max-create-rev/tenant/"
	namespacedKV := namespace.NewKV(client.KV, tenantPrefix)
	_, err = client.Put(ctx, "/a1629/namespace-nested-txn-empty-start-range-historical-serializable-max-create-rev/tenant0/items/a", "outside-tenant")
	require.NoError(t, err)
	_, err = namespacedKV.Put(ctx, "alpha/a", "old-alpha")
	require.NoError(t, err)
	_, err = namespacedKV.Put(ctx, "items/a", "old-items/a")
	require.NoError(t, err)
	_, err = namespacedKV.Put(ctx, "z/final", "value-z/final")
	require.NoError(t, err)
	historical, err := namespacedKV.Put(ctx, "items/b", "old-items/b")
	require.NoError(t, err)
	_, err = namespacedKV.Delete(ctx, "items/b")
	require.NoError(t, err)
	_, err = namespacedKV.Put(ctx, "alpha/a", "new-alpha")
	require.NoError(t, err)
	_, err = namespacedKV.Put(ctx, "later/new", "new-later")
	require.NoError(t, err)

	txnResp, err := namespacedKV.Txn(ctx).
		Then(clientv3.OpTxn(nil,
			[]clientv3.Op{clientv3.OpGet("",
				clientv3.WithRange("z"),
				clientv3.WithRev(historical.Header.Revision),
				clientv3.WithSerializable(),
				clientv3.WithMaxCreateRev(historical.Header.Revision-1),
				clientv3.WithSort(clientv3.SortByKey, clientv3.SortAscend),
			)},
			nil)).
		Commit()
	require.NoError(t, err)
	require.True(t, txnResp.Succeeded)
	require.Len(t, txnResp.Responses, 1)
	nestedTxn := txnResp.Responses[0].GetResponseTxn()
	require.NotNil(t, nestedTxn)
	require.True(t, nestedTxn.Succeeded)
	require.Len(t, nestedTxn.Responses, 1)
	nestedGet := nestedTxn.Responses[0].GetResponseRange()
	require.NotNil(t, nestedGet)
	require.Equal(t, int64(3), nestedGet.Count)
	require.False(t, nestedGet.More)
	require.Len(t, nestedGet.Kvs, 2)
	require.Equal(t, [][]byte{[]byte("alpha/a"), []byte("items/a")},
		[][]byte{nestedGet.Kvs[0].Key, nestedGet.Kvs[1].Key})
	require.Equal(t, [][]byte{[]byte("old-alpha"), []byte("old-items/a")},
		[][]byte{nestedGet.Kvs[0].Value, nestedGet.Kvs[1].Value})

	current, err := namespacedKV.Get(ctx, "", clientv3.WithPrefix())
	require.NoError(t, err)
	require.Equal(t, int64(4), current.Count)
	require.Equal(t, [][]byte{[]byte("alpha/a"), []byte("items/a"), []byte("later/new"), []byte("z/final")},
		[][]byte{current.Kvs[0].Key, current.Kvs[1].Key, current.Kvs[2].Key, current.Kvs[3].Key})
	outsideTenant, err := client.Get(ctx, "/a1629/namespace-nested-txn-empty-start-range-historical-serializable-max-create-rev/tenant0/", clientv3.WithPrefix())
	require.NoError(t, err)
	require.Equal(t, int64(1), outsideTenant.Count)
}

func TestClientNamespaceNestedTxnGetEmptyStartRangeHistoricalSerializableContradictoryModRevisionFiltersReturnCountOnly(t *testing.T) {
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
	tenantPrefix := "/a1630/namespace-nested-txn-empty-start-range-historical-serializable-contradictory-mod-rev/tenant/"
	namespacedKV := namespace.NewKV(client.KV, tenantPrefix)
	_, err = client.Put(ctx, "/a1630/namespace-nested-txn-empty-start-range-historical-serializable-contradictory-mod-rev/tenant0/items/a", "outside-tenant")
	require.NoError(t, err)
	_, err = namespacedKV.Put(ctx, "alpha/a", "old-alpha")
	require.NoError(t, err)
	_, err = namespacedKV.Put(ctx, "items/a", "old-items/a")
	require.NoError(t, err)
	_, err = namespacedKV.Put(ctx, "z/final", "value-z/final")
	require.NoError(t, err)
	historical, err := namespacedKV.Put(ctx, "items/b", "old-items/b")
	require.NoError(t, err)
	_, err = namespacedKV.Delete(ctx, "items/b")
	require.NoError(t, err)
	_, err = namespacedKV.Put(ctx, "items/a", "new-items/a")
	require.NoError(t, err)
	_, err = namespacedKV.Put(ctx, "later/new", "new-later")
	require.NoError(t, err)

	txnResp, err := namespacedKV.Txn(ctx).
		Then(clientv3.OpTxn(nil,
			[]clientv3.Op{clientv3.OpGet("",
				clientv3.WithRange("z"),
				clientv3.WithRev(historical.Header.Revision),
				clientv3.WithSerializable(),
				clientv3.WithMinModRev(historical.Header.Revision+1),
				clientv3.WithMaxModRev(historical.Header.Revision),
				clientv3.WithLimit(1),
			)},
			nil)).
		Commit()
	require.NoError(t, err)
	require.True(t, txnResp.Succeeded)
	require.Len(t, txnResp.Responses, 1)
	nestedTxn := txnResp.Responses[0].GetResponseTxn()
	require.NotNil(t, nestedTxn)
	require.True(t, nestedTxn.Succeeded)
	require.Len(t, nestedTxn.Responses, 1)
	nestedGet := nestedTxn.Responses[0].GetResponseRange()
	require.NotNil(t, nestedGet)
	require.Equal(t, int64(3), nestedGet.Count)
	require.False(t, nestedGet.More)
	require.Empty(t, nestedGet.Kvs)

	current, err := namespacedKV.Get(ctx, "", clientv3.WithPrefix())
	require.NoError(t, err)
	require.Equal(t, int64(4), current.Count)
	require.Equal(t, [][]byte{[]byte("alpha/a"), []byte("items/a"), []byte("later/new"), []byte("z/final")},
		[][]byte{current.Kvs[0].Key, current.Kvs[1].Key, current.Kvs[2].Key, current.Kvs[3].Key})
	outsideTenant, err := client.Get(ctx, "/a1630/namespace-nested-txn-empty-start-range-historical-serializable-contradictory-mod-rev/tenant0/", clientv3.WithPrefix())
	require.NoError(t, err)
	require.Equal(t, int64(1), outsideTenant.Count)
}

func TestClientNamespaceNestedTxnGetEmptyStartRangeHistoricalSerializableContradictoryCreateRevisionFiltersReturnCountOnly(t *testing.T) {
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
	tenantPrefix := "/a1631/namespace-nested-txn-empty-start-range-historical-serializable-contradictory-create-rev/tenant/"
	namespacedKV := namespace.NewKV(client.KV, tenantPrefix)
	_, err = client.Put(ctx, "/a1631/namespace-nested-txn-empty-start-range-historical-serializable-contradictory-create-rev/tenant0/items/a", "outside-tenant")
	require.NoError(t, err)
	_, err = namespacedKV.Put(ctx, "alpha/a", "old-alpha")
	require.NoError(t, err)
	_, err = namespacedKV.Put(ctx, "items/a", "old-items/a")
	require.NoError(t, err)
	_, err = namespacedKV.Put(ctx, "z/final", "value-z/final")
	require.NoError(t, err)
	historical, err := namespacedKV.Put(ctx, "items/b", "old-items/b")
	require.NoError(t, err)
	_, err = namespacedKV.Delete(ctx, "items/b")
	require.NoError(t, err)
	_, err = namespacedKV.Put(ctx, "items/a", "new-items/a")
	require.NoError(t, err)
	_, err = namespacedKV.Put(ctx, "later/new", "new-later")
	require.NoError(t, err)

	txnResp, err := namespacedKV.Txn(ctx).
		Then(clientv3.OpTxn(nil,
			[]clientv3.Op{clientv3.OpGet("",
				clientv3.WithRange("z"),
				clientv3.WithRev(historical.Header.Revision),
				clientv3.WithSerializable(),
				clientv3.WithMinCreateRev(historical.Header.Revision+1),
				clientv3.WithMaxCreateRev(historical.Header.Revision),
				clientv3.WithLimit(1),
			)},
			nil)).
		Commit()
	require.NoError(t, err)
	require.True(t, txnResp.Succeeded)
	require.Len(t, txnResp.Responses, 1)
	nestedTxn := txnResp.Responses[0].GetResponseTxn()
	require.NotNil(t, nestedTxn)
	require.True(t, nestedTxn.Succeeded)
	require.Len(t, nestedTxn.Responses, 1)
	nestedGet := nestedTxn.Responses[0].GetResponseRange()
	require.NotNil(t, nestedGet)
	require.Equal(t, int64(3), nestedGet.Count)
	require.False(t, nestedGet.More)
	require.Empty(t, nestedGet.Kvs)

	current, err := namespacedKV.Get(ctx, "", clientv3.WithPrefix())
	require.NoError(t, err)
	require.Equal(t, int64(4), current.Count)
	require.Equal(t, [][]byte{[]byte("alpha/a"), []byte("items/a"), []byte("later/new"), []byte("z/final")},
		[][]byte{current.Kvs[0].Key, current.Kvs[1].Key, current.Kvs[2].Key, current.Kvs[3].Key})
	outsideTenant, err := client.Get(ctx, "/a1631/namespace-nested-txn-empty-start-range-historical-serializable-contradictory-create-rev/tenant0/", clientv3.WithPrefix())
	require.NoError(t, err)
	require.Equal(t, int64(1), outsideTenant.Count)
}

func TestClientNamespaceNestedTxnGetEmptyStartRangeSerializableWithMinModRevisionReturnsLogicalKeys(t *testing.T) {
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
	tenantPrefix := "/a1632/namespace-nested-txn-empty-start-range-serializable-min-mod-rev/tenant/"
	namespacedKV := namespace.NewKV(client.KV, tenantPrefix)
	for _, key := range []string{"alpha/a", "items/a", "items/b"} {
		_, err = namespacedKV.Put(ctx, key, "old-"+key)
		require.NoError(t, err)
	}
	updateB, err := namespacedKV.Put(ctx, "items/b", "new-items/b")
	require.NoError(t, err)
	_, err = client.Put(ctx, "/a1632/namespace-nested-txn-empty-start-range-serializable-min-mod-rev/tenant0/items/b", "outside-tenant")
	require.NoError(t, err)
	latest, err := namespacedKV.Put(ctx, "z/final", "value-z/final")
	require.NoError(t, err)

	txnResp, err := namespacedKV.Txn(ctx).
		Then(clientv3.OpTxn(nil,
			[]clientv3.Op{clientv3.OpGet("",
				clientv3.WithRange("z"),
				clientv3.WithSerializable(),
				clientv3.WithMinModRev(updateB.Header.Revision),
				clientv3.WithSort(clientv3.SortByKey, clientv3.SortAscend),
			)},
			nil)).
		Commit()
	require.NoError(t, err)
	require.True(t, txnResp.Succeeded)
	require.Len(t, txnResp.Responses, 1)
	nestedTxn := txnResp.Responses[0].GetResponseTxn()
	require.NotNil(t, nestedTxn)
	require.True(t, nestedTxn.Succeeded)
	require.Len(t, nestedTxn.Responses, 1)
	nestedGet := nestedTxn.Responses[0].GetResponseRange()
	require.NotNil(t, nestedGet)
	require.NotNil(t, nestedGet.Header)
	require.Equal(t, latest.Header.Revision, nestedGet.Header.Revision)
	require.Equal(t, int64(3), nestedGet.Count)
	require.False(t, nestedGet.More)
	require.Len(t, nestedGet.Kvs, 1)
	require.Equal(t, []byte("items/b"), nestedGet.Kvs[0].Key)
	require.Equal(t, []byte("new-items/b"), nestedGet.Kvs[0].Value)
	require.Equal(t, updateB.Header.Revision, nestedGet.Kvs[0].ModRevision)

	current, err := namespacedKV.Get(ctx, "", clientv3.WithPrefix())
	require.NoError(t, err)
	require.Equal(t, int64(4), current.Count)
	require.Equal(t, [][]byte{[]byte("alpha/a"), []byte("items/a"), []byte("items/b"), []byte("z/final")},
		[][]byte{current.Kvs[0].Key, current.Kvs[1].Key, current.Kvs[2].Key, current.Kvs[3].Key})
	outsideTenant, err := client.Get(ctx, "/a1632/namespace-nested-txn-empty-start-range-serializable-min-mod-rev/tenant0/", clientv3.WithPrefix())
	require.NoError(t, err)
	require.Equal(t, int64(1), outsideTenant.Count)
}

func TestClientNamespaceNestedTxnGetEmptyStartRangeSerializableWithMaxModRevisionReturnsLogicalKeys(t *testing.T) {
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
	tenantPrefix := "/a1633/namespace-nested-txn-empty-start-range-serializable-max-mod-rev/tenant/"
	namespacedKV := namespace.NewKV(client.KV, tenantPrefix)
	for _, key := range []string{"alpha/a", "items/a", "items/b"} {
		_, err = namespacedKV.Put(ctx, key, "old-"+key)
		require.NoError(t, err)
	}
	updateB, err := namespacedKV.Put(ctx, "items/b", "new-items/b")
	require.NoError(t, err)
	_, err = client.Put(ctx, "/a1633/namespace-nested-txn-empty-start-range-serializable-max-mod-rev/tenant0/items/a", "outside-tenant")
	require.NoError(t, err)
	latest, err := namespacedKV.Put(ctx, "z/final", "value-z/final")
	require.NoError(t, err)

	txnResp, err := namespacedKV.Txn(ctx).
		Then(clientv3.OpTxn(nil,
			[]clientv3.Op{clientv3.OpGet("",
				clientv3.WithRange("z"),
				clientv3.WithSerializable(),
				clientv3.WithMaxModRev(updateB.Header.Revision-1),
				clientv3.WithSort(clientv3.SortByKey, clientv3.SortAscend),
			)},
			nil)).
		Commit()
	require.NoError(t, err)
	require.True(t, txnResp.Succeeded)
	require.Len(t, txnResp.Responses, 1)
	nestedTxn := txnResp.Responses[0].GetResponseTxn()
	require.NotNil(t, nestedTxn)
	require.True(t, nestedTxn.Succeeded)
	require.Len(t, nestedTxn.Responses, 1)
	nestedGet := nestedTxn.Responses[0].GetResponseRange()
	require.NotNil(t, nestedGet)
	require.NotNil(t, nestedGet.Header)
	require.Equal(t, latest.Header.Revision, nestedGet.Header.Revision)
	require.Equal(t, int64(3), nestedGet.Count)
	require.False(t, nestedGet.More)
	require.Len(t, nestedGet.Kvs, 2)
	require.Equal(t, [][]byte{[]byte("alpha/a"), []byte("items/a")},
		[][]byte{nestedGet.Kvs[0].Key, nestedGet.Kvs[1].Key})
	require.Equal(t, [][]byte{[]byte("old-alpha/a"), []byte("old-items/a")},
		[][]byte{nestedGet.Kvs[0].Value, nestedGet.Kvs[1].Value})

	current, err := namespacedKV.Get(ctx, "", clientv3.WithPrefix())
	require.NoError(t, err)
	require.Equal(t, int64(4), current.Count)
	require.Equal(t, [][]byte{[]byte("alpha/a"), []byte("items/a"), []byte("items/b"), []byte("z/final")},
		[][]byte{current.Kvs[0].Key, current.Kvs[1].Key, current.Kvs[2].Key, current.Kvs[3].Key})
	outsideTenant, err := client.Get(ctx, "/a1633/namespace-nested-txn-empty-start-range-serializable-max-mod-rev/tenant0/", clientv3.WithPrefix())
	require.NoError(t, err)
	require.Equal(t, int64(1), outsideTenant.Count)
}

func TestClientNamespaceNestedTxnGetEmptyStartRangeSerializableWithMinCreateRevisionReturnsLogicalKeys(t *testing.T) {
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
	tenantPrefix := "/a1634/namespace-nested-txn-empty-start-range-serializable-min-create-rev/tenant/"
	namespacedKV := namespace.NewKV(client.KV, tenantPrefix)
	for _, key := range []string{"alpha/a", "items/a"} {
		_, err = namespacedKV.Put(ctx, key, "value-"+key)
		require.NoError(t, err)
	}
	createB, err := namespacedKV.Put(ctx, "items/b", "value-items/b")
	require.NoError(t, err)
	_, err = namespacedKV.Put(ctx, "alpha/a", "updated-alpha/a")
	require.NoError(t, err)
	_, err = client.Put(ctx, "/a1634/namespace-nested-txn-empty-start-range-serializable-min-create-rev/tenant0/items/b", "outside-tenant")
	require.NoError(t, err)
	latest, err := namespacedKV.Put(ctx, "z/final", "value-z/final")
	require.NoError(t, err)

	txnResp, err := namespacedKV.Txn(ctx).
		Then(clientv3.OpTxn(nil,
			[]clientv3.Op{clientv3.OpGet("",
				clientv3.WithRange("z"),
				clientv3.WithSerializable(),
				clientv3.WithMinCreateRev(createB.Header.Revision),
				clientv3.WithSort(clientv3.SortByKey, clientv3.SortAscend),
			)},
			nil)).
		Commit()
	require.NoError(t, err)
	require.True(t, txnResp.Succeeded)
	require.Len(t, txnResp.Responses, 1)
	nestedTxn := txnResp.Responses[0].GetResponseTxn()
	require.NotNil(t, nestedTxn)
	require.True(t, nestedTxn.Succeeded)
	require.Len(t, nestedTxn.Responses, 1)
	nestedGet := nestedTxn.Responses[0].GetResponseRange()
	require.NotNil(t, nestedGet)
	require.NotNil(t, nestedGet.Header)
	require.Equal(t, latest.Header.Revision, nestedGet.Header.Revision)
	require.Equal(t, int64(3), nestedGet.Count)
	require.False(t, nestedGet.More)
	require.Len(t, nestedGet.Kvs, 1)
	require.Equal(t, []byte("items/b"), nestedGet.Kvs[0].Key)
	require.Equal(t, []byte("value-items/b"), nestedGet.Kvs[0].Value)
	require.Equal(t, createB.Header.Revision, nestedGet.Kvs[0].CreateRevision)

	current, err := namespacedKV.Get(ctx, "", clientv3.WithPrefix())
	require.NoError(t, err)
	require.Equal(t, int64(4), current.Count)
	require.Equal(t, [][]byte{[]byte("alpha/a"), []byte("items/a"), []byte("items/b"), []byte("z/final")},
		[][]byte{current.Kvs[0].Key, current.Kvs[1].Key, current.Kvs[2].Key, current.Kvs[3].Key})
	outsideTenant, err := client.Get(ctx, "/a1634/namespace-nested-txn-empty-start-range-serializable-min-create-rev/tenant0/", clientv3.WithPrefix())
	require.NoError(t, err)
	require.Equal(t, int64(1), outsideTenant.Count)
}

func TestClientNamespaceNestedTxnGetEmptyStartRangeSerializableWithMaxCreateRevisionReturnsLogicalKeys(t *testing.T) {
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
	tenantPrefix := "/a1635/namespace-nested-txn-empty-start-range-serializable-max-create-rev/tenant/"
	namespacedKV := namespace.NewKV(client.KV, tenantPrefix)
	for _, key := range []string{"alpha/a", "items/a"} {
		_, err = namespacedKV.Put(ctx, key, "value-"+key)
		require.NoError(t, err)
	}
	createB, err := namespacedKV.Put(ctx, "items/b", "value-items/b")
	require.NoError(t, err)
	_, err = namespacedKV.Put(ctx, "alpha/a", "updated-alpha/a")
	require.NoError(t, err)
	_, err = client.Put(ctx, "/a1635/namespace-nested-txn-empty-start-range-serializable-max-create-rev/tenant0/items/a", "outside-tenant")
	require.NoError(t, err)
	latest, err := namespacedKV.Put(ctx, "z/final", "value-z/final")
	require.NoError(t, err)

	txnResp, err := namespacedKV.Txn(ctx).
		Then(clientv3.OpTxn(nil,
			[]clientv3.Op{clientv3.OpGet("",
				clientv3.WithRange("z"),
				clientv3.WithSerializable(),
				clientv3.WithMaxCreateRev(createB.Header.Revision-1),
				clientv3.WithSort(clientv3.SortByKey, clientv3.SortAscend),
			)},
			nil)).
		Commit()
	require.NoError(t, err)
	require.True(t, txnResp.Succeeded)
	require.Len(t, txnResp.Responses, 1)
	nestedTxn := txnResp.Responses[0].GetResponseTxn()
	require.NotNil(t, nestedTxn)
	require.True(t, nestedTxn.Succeeded)
	require.Len(t, nestedTxn.Responses, 1)
	nestedGet := nestedTxn.Responses[0].GetResponseRange()
	require.NotNil(t, nestedGet)
	require.NotNil(t, nestedGet.Header)
	require.Equal(t, latest.Header.Revision, nestedGet.Header.Revision)
	require.Equal(t, int64(3), nestedGet.Count)
	require.False(t, nestedGet.More)
	require.Len(t, nestedGet.Kvs, 2)
	require.Equal(t, [][]byte{[]byte("alpha/a"), []byte("items/a")},
		[][]byte{nestedGet.Kvs[0].Key, nestedGet.Kvs[1].Key})
	require.Equal(t, [][]byte{[]byte("updated-alpha/a"), []byte("value-items/a")},
		[][]byte{nestedGet.Kvs[0].Value, nestedGet.Kvs[1].Value})

	current, err := namespacedKV.Get(ctx, "", clientv3.WithPrefix())
	require.NoError(t, err)
	require.Equal(t, int64(4), current.Count)
	require.Equal(t, [][]byte{[]byte("alpha/a"), []byte("items/a"), []byte("items/b"), []byte("z/final")},
		[][]byte{current.Kvs[0].Key, current.Kvs[1].Key, current.Kvs[2].Key, current.Kvs[3].Key})
	outsideTenant, err := client.Get(ctx, "/a1635/namespace-nested-txn-empty-start-range-serializable-max-create-rev/tenant0/", clientv3.WithPrefix())
	require.NoError(t, err)
	require.Equal(t, int64(1), outsideTenant.Count)
}

func TestClientNamespaceNestedTxnGetEmptyStartRangeSerializableContradictoryModRevisionFiltersReturnCountOnly(t *testing.T) {
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
	tenantPrefix := "/a1636/namespace-nested-txn-empty-start-range-serializable-contradictory-mod-rev/tenant/"
	namespacedKV := namespace.NewKV(client.KV, tenantPrefix)
	_, err = namespacedKV.Put(ctx, "alpha/a", "value-alpha/a")
	require.NoError(t, err)
	updateItems, err := namespacedKV.Put(ctx, "items/a", "value-items/a")
	require.NoError(t, err)
	_, err = namespacedKV.Put(ctx, "items/b", "value-items/b")
	require.NoError(t, err)
	_, err = client.Put(ctx, "/a1636/namespace-nested-txn-empty-start-range-serializable-contradictory-mod-rev/tenant0/items/a", "outside-tenant")
	require.NoError(t, err)
	latest, err := namespacedKV.Put(ctx, "z/final", "value-z/final")
	require.NoError(t, err)

	txnResp, err := namespacedKV.Txn(ctx).
		Then(clientv3.OpTxn(nil,
			[]clientv3.Op{clientv3.OpGet("",
				clientv3.WithRange("z"),
				clientv3.WithSerializable(),
				clientv3.WithMinModRev(updateItems.Header.Revision+1),
				clientv3.WithMaxModRev(updateItems.Header.Revision),
				clientv3.WithLimit(1),
			)},
			nil)).
		Commit()
	require.NoError(t, err)
	require.True(t, txnResp.Succeeded)
	require.Len(t, txnResp.Responses, 1)
	nestedTxn := txnResp.Responses[0].GetResponseTxn()
	require.NotNil(t, nestedTxn)
	require.True(t, nestedTxn.Succeeded)
	require.Len(t, nestedTxn.Responses, 1)
	nestedGet := nestedTxn.Responses[0].GetResponseRange()
	require.NotNil(t, nestedGet)
	require.NotNil(t, nestedGet.Header)
	require.Equal(t, latest.Header.Revision, nestedGet.Header.Revision)
	require.Equal(t, int64(3), nestedGet.Count)
	require.False(t, nestedGet.More)
	require.Empty(t, nestedGet.Kvs)

	current, err := namespacedKV.Get(ctx, "", clientv3.WithPrefix())
	require.NoError(t, err)
	require.Equal(t, int64(4), current.Count)
	require.Equal(t, [][]byte{[]byte("alpha/a"), []byte("items/a"), []byte("items/b"), []byte("z/final")},
		[][]byte{current.Kvs[0].Key, current.Kvs[1].Key, current.Kvs[2].Key, current.Kvs[3].Key})
	outsideTenant, err := client.Get(ctx, "/a1636/namespace-nested-txn-empty-start-range-serializable-contradictory-mod-rev/tenant0/", clientv3.WithPrefix())
	require.NoError(t, err)
	require.Equal(t, int64(1), outsideTenant.Count)
}

func TestClientNamespaceNestedTxnGetEmptyStartRangeSerializableContradictoryCreateRevisionFiltersReturnCountOnly(t *testing.T) {
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
	tenantPrefix := "/a1637/namespace-nested-txn-empty-start-range-serializable-contradictory-create-rev/tenant/"
	namespacedKV := namespace.NewKV(client.KV, tenantPrefix)
	_, err = namespacedKV.Put(ctx, "alpha/a", "value-alpha/a")
	require.NoError(t, err)
	createItems, err := namespacedKV.Put(ctx, "items/a", "value-items/a")
	require.NoError(t, err)
	_, err = namespacedKV.Put(ctx, "items/b", "value-items/b")
	require.NoError(t, err)
	_, err = namespacedKV.Put(ctx, "alpha/a", "updated-alpha/a")
	require.NoError(t, err)
	_, err = client.Put(ctx, "/a1637/namespace-nested-txn-empty-start-range-serializable-contradictory-create-rev/tenant0/items/a", "outside-tenant")
	require.NoError(t, err)
	latest, err := namespacedKV.Put(ctx, "z/final", "value-z/final")
	require.NoError(t, err)

	txnResp, err := namespacedKV.Txn(ctx).
		Then(clientv3.OpTxn(nil,
			[]clientv3.Op{clientv3.OpGet("",
				clientv3.WithRange("z"),
				clientv3.WithSerializable(),
				clientv3.WithMinCreateRev(createItems.Header.Revision+1),
				clientv3.WithMaxCreateRev(createItems.Header.Revision),
				clientv3.WithLimit(1),
			)},
			nil)).
		Commit()
	require.NoError(t, err)
	require.True(t, txnResp.Succeeded)
	require.Len(t, txnResp.Responses, 1)
	nestedTxn := txnResp.Responses[0].GetResponseTxn()
	require.NotNil(t, nestedTxn)
	require.True(t, nestedTxn.Succeeded)
	require.Len(t, nestedTxn.Responses, 1)
	nestedGet := nestedTxn.Responses[0].GetResponseRange()
	require.NotNil(t, nestedGet)
	require.NotNil(t, nestedGet.Header)
	require.Equal(t, latest.Header.Revision, nestedGet.Header.Revision)
	require.Equal(t, int64(3), nestedGet.Count)
	require.False(t, nestedGet.More)
	require.Empty(t, nestedGet.Kvs)

	current, err := namespacedKV.Get(ctx, "", clientv3.WithPrefix())
	require.NoError(t, err)
	require.Equal(t, int64(4), current.Count)
	require.Equal(t, [][]byte{[]byte("alpha/a"), []byte("items/a"), []byte("items/b"), []byte("z/final")},
		[][]byte{current.Kvs[0].Key, current.Kvs[1].Key, current.Kvs[2].Key, current.Kvs[3].Key})
	outsideTenant, err := client.Get(ctx, "/a1637/namespace-nested-txn-empty-start-range-serializable-contradictory-create-rev/tenant0/", clientv3.WithPrefix())
	require.NoError(t, err)
	require.Equal(t, int64(1), outsideTenant.Count)
}

func TestClientNamespaceNestedTxnGetEmptyStartRangeSerializableWithFirstKeyReturnsLogicalKey(t *testing.T) {
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
	tenantPrefix := "/a1638/namespace-nested-txn-empty-start-range-serializable-first-key/tenant/"
	namespacedKV := namespace.NewKV(client.KV, tenantPrefix)
	for _, key := range []string{"alpha/a", "items/a"} {
		_, err = namespacedKV.Put(ctx, key, "value-"+key)
		require.NoError(t, err)
	}
	_, err = client.Put(ctx, "/a1638/namespace-nested-txn-empty-start-range-serializable-first-key/tenant0/alpha/a", "outside-tenant")
	require.NoError(t, err)
	latest, err := namespacedKV.Put(ctx, "z/final", "value-z/final")
	require.NoError(t, err)

	getOpts := append(clientv3.WithFirstKey(),
		clientv3.WithRange("z"),
		clientv3.WithSerializable())
	txnResp, err := namespacedKV.Txn(ctx).
		Then(clientv3.OpTxn(nil,
			[]clientv3.Op{clientv3.OpGet("", getOpts...)},
			nil)).
		Commit()
	require.NoError(t, err)
	require.True(t, txnResp.Succeeded)
	require.Len(t, txnResp.Responses, 1)
	nestedTxn := txnResp.Responses[0].GetResponseTxn()
	require.NotNil(t, nestedTxn)
	require.True(t, nestedTxn.Succeeded)
	require.Len(t, nestedTxn.Responses, 1)
	nestedGet := nestedTxn.Responses[0].GetResponseRange()
	require.NotNil(t, nestedGet)
	require.NotNil(t, nestedGet.Header)
	require.Equal(t, latest.Header.Revision, nestedGet.Header.Revision)
	require.Equal(t, int64(2), nestedGet.Count)
	require.True(t, nestedGet.More)
	require.Len(t, nestedGet.Kvs, 1)
	require.Equal(t, []byte("alpha/a"), nestedGet.Kvs[0].Key)
	require.Equal(t, []byte("value-alpha/a"), nestedGet.Kvs[0].Value)

	current, err := namespacedKV.Get(ctx, "", clientv3.WithPrefix())
	require.NoError(t, err)
	require.Equal(t, int64(3), current.Count)
	require.Equal(t, [][]byte{[]byte("alpha/a"), []byte("items/a"), []byte("z/final")},
		[][]byte{current.Kvs[0].Key, current.Kvs[1].Key, current.Kvs[2].Key})
	outsideTenant, err := client.Get(ctx, "/a1638/namespace-nested-txn-empty-start-range-serializable-first-key/tenant0/", clientv3.WithPrefix())
	require.NoError(t, err)
	require.Equal(t, int64(1), outsideTenant.Count)
}

func TestClientNamespaceNestedTxnGetEmptyStartRangeSerializableWithLastKeyReturnsLogicalKey(t *testing.T) {
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
	tenantPrefix := "/a1639/namespace-nested-txn-empty-start-range-serializable-last-key/tenant/"
	namespacedKV := namespace.NewKV(client.KV, tenantPrefix)
	for _, key := range []string{"alpha/a", "items/a"} {
		_, err = namespacedKV.Put(ctx, key, "value-"+key)
		require.NoError(t, err)
	}
	_, err = client.Put(ctx, "/a1639/namespace-nested-txn-empty-start-range-serializable-last-key/tenant0/items/z", "outside-tenant")
	require.NoError(t, err)
	latest, err := namespacedKV.Put(ctx, "z/final", "value-z/final")
	require.NoError(t, err)

	getOpts := append(clientv3.WithLastKey(),
		clientv3.WithRange("z"),
		clientv3.WithSerializable())
	txnResp, err := namespacedKV.Txn(ctx).
		Then(clientv3.OpTxn(nil,
			[]clientv3.Op{clientv3.OpGet("", getOpts...)},
			nil)).
		Commit()
	require.NoError(t, err)
	require.True(t, txnResp.Succeeded)
	require.Len(t, txnResp.Responses, 1)
	nestedTxn := txnResp.Responses[0].GetResponseTxn()
	require.NotNil(t, nestedTxn)
	require.True(t, nestedTxn.Succeeded)
	require.Len(t, nestedTxn.Responses, 1)
	nestedGet := nestedTxn.Responses[0].GetResponseRange()
	require.NotNil(t, nestedGet)
	require.NotNil(t, nestedGet.Header)
	require.Equal(t, latest.Header.Revision, nestedGet.Header.Revision)
	require.Equal(t, int64(2), nestedGet.Count)
	require.True(t, nestedGet.More)
	require.Len(t, nestedGet.Kvs, 1)
	require.Equal(t, []byte("items/a"), nestedGet.Kvs[0].Key)
	require.Equal(t, []byte("value-items/a"), nestedGet.Kvs[0].Value)

	current, err := namespacedKV.Get(ctx, "", clientv3.WithPrefix())
	require.NoError(t, err)
	require.Equal(t, int64(3), current.Count)
	require.Equal(t, [][]byte{[]byte("alpha/a"), []byte("items/a"), []byte("z/final")},
		[][]byte{current.Kvs[0].Key, current.Kvs[1].Key, current.Kvs[2].Key})
	outsideTenant, err := client.Get(ctx, "/a1639/namespace-nested-txn-empty-start-range-serializable-last-key/tenant0/", clientv3.WithPrefix())
	require.NoError(t, err)
	require.Equal(t, int64(1), outsideTenant.Count)
}

func TestClientNamespaceNestedTxnGetEmptyStartRangeSerializableWithFirstCreateReturnsLogicalKey(t *testing.T) {
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
	tenantPrefix := "/a1640/namespace-nested-txn-empty-start-range-serializable-first-create/tenant/"
	namespacedKV := namespace.NewKV(client.KV, tenantPrefix)
	for _, key := range []string{"items/a", "alpha/a"} {
		_, err = namespacedKV.Put(ctx, key, "value-"+key)
		require.NoError(t, err)
	}
	_, err = client.Put(ctx, "/a1640/namespace-nested-txn-empty-start-range-serializable-first-create/tenant0/alpha/older", "outside-tenant")
	require.NoError(t, err)
	latest, err := namespacedKV.Put(ctx, "z/final", "value-z/final")
	require.NoError(t, err)

	getOpts := append(clientv3.WithFirstCreate(),
		clientv3.WithRange("z"),
		clientv3.WithSerializable())
	txnResp, err := namespacedKV.Txn(ctx).
		Then(clientv3.OpTxn(nil,
			[]clientv3.Op{clientv3.OpGet("", getOpts...)},
			nil)).
		Commit()
	require.NoError(t, err)
	require.True(t, txnResp.Succeeded)
	require.Len(t, txnResp.Responses, 1)
	nestedTxn := txnResp.Responses[0].GetResponseTxn()
	require.NotNil(t, nestedTxn)
	require.True(t, nestedTxn.Succeeded)
	require.Len(t, nestedTxn.Responses, 1)
	nestedGet := nestedTxn.Responses[0].GetResponseRange()
	require.NotNil(t, nestedGet)
	require.NotNil(t, nestedGet.Header)
	require.Equal(t, latest.Header.Revision, nestedGet.Header.Revision)
	require.Equal(t, int64(2), nestedGet.Count)
	require.True(t, nestedGet.More)
	require.Len(t, nestedGet.Kvs, 1)
	require.Equal(t, []byte("items/a"), nestedGet.Kvs[0].Key)
	require.Equal(t, []byte("value-items/a"), nestedGet.Kvs[0].Value)

	current, err := namespacedKV.Get(ctx, "", clientv3.WithPrefix())
	require.NoError(t, err)
	require.Equal(t, int64(3), current.Count)
	require.Equal(t, [][]byte{[]byte("alpha/a"), []byte("items/a"), []byte("z/final")},
		[][]byte{current.Kvs[0].Key, current.Kvs[1].Key, current.Kvs[2].Key})
	outsideTenant, err := client.Get(ctx, "/a1640/namespace-nested-txn-empty-start-range-serializable-first-create/tenant0/", clientv3.WithPrefix())
	require.NoError(t, err)
	require.Equal(t, int64(1), outsideTenant.Count)
}

func TestClientNamespaceNestedTxnGetEmptyStartRangeSerializableWithLastCreateReturnsLogicalKey(t *testing.T) {
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
	tenantPrefix := "/a1641/namespace-nested-txn-empty-start-range-serializable-last-create/tenant/"
	namespacedKV := namespace.NewKV(client.KV, tenantPrefix)
	for _, key := range []string{"items/a", "alpha/a"} {
		_, err = namespacedKV.Put(ctx, key, "value-"+key)
		require.NoError(t, err)
	}
	_, err = client.Put(ctx, "/a1641/namespace-nested-txn-empty-start-range-serializable-last-create/tenant0/items/newer", "outside-tenant")
	require.NoError(t, err)
	latest, err := namespacedKV.Put(ctx, "z/final", "value-z/final")
	require.NoError(t, err)

	getOpts := append(clientv3.WithLastCreate(),
		clientv3.WithRange("z"),
		clientv3.WithSerializable())
	txnResp, err := namespacedKV.Txn(ctx).
		Then(clientv3.OpTxn(nil,
			[]clientv3.Op{clientv3.OpGet("", getOpts...)},
			nil)).
		Commit()
	require.NoError(t, err)
	require.True(t, txnResp.Succeeded)
	require.Len(t, txnResp.Responses, 1)
	nestedTxn := txnResp.Responses[0].GetResponseTxn()
	require.NotNil(t, nestedTxn)
	require.True(t, nestedTxn.Succeeded)
	require.Len(t, nestedTxn.Responses, 1)
	nestedGet := nestedTxn.Responses[0].GetResponseRange()
	require.NotNil(t, nestedGet)
	require.NotNil(t, nestedGet.Header)
	require.Equal(t, latest.Header.Revision, nestedGet.Header.Revision)
	require.Equal(t, int64(2), nestedGet.Count)
	require.True(t, nestedGet.More)
	require.Len(t, nestedGet.Kvs, 1)
	require.Equal(t, []byte("alpha/a"), nestedGet.Kvs[0].Key)
	require.Equal(t, []byte("value-alpha/a"), nestedGet.Kvs[0].Value)

	current, err := namespacedKV.Get(ctx, "", clientv3.WithPrefix())
	require.NoError(t, err)
	require.Equal(t, int64(3), current.Count)
	require.Equal(t, [][]byte{[]byte("alpha/a"), []byte("items/a"), []byte("z/final")},
		[][]byte{current.Kvs[0].Key, current.Kvs[1].Key, current.Kvs[2].Key})
	outsideTenant, err := client.Get(ctx, "/a1641/namespace-nested-txn-empty-start-range-serializable-last-create/tenant0/", clientv3.WithPrefix())
	require.NoError(t, err)
	require.Equal(t, int64(1), outsideTenant.Count)
}

func TestClientNamespaceNestedTxnGetEmptyStartRangeSerializableWithFirstRevReturnsLogicalKey(t *testing.T) {
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
	tenantPrefix := "/a1642/namespace-nested-txn-empty-start-range-serializable-first-rev/tenant/"
	namespacedKV := namespace.NewKV(client.KV, tenantPrefix)
	_, err = namespacedKV.Put(ctx, "items/a", "value-items/a")
	require.NoError(t, err)
	_, err = namespacedKV.Put(ctx, "alpha/a", "value-alpha/a")
	require.NoError(t, err)
	_, err = namespacedKV.Put(ctx, "alpha/a", "updated-alpha/a")
	require.NoError(t, err)
	_, err = client.Put(ctx, "/a1642/namespace-nested-txn-empty-start-range-serializable-first-rev/tenant0/items/older", "outside-tenant")
	require.NoError(t, err)
	latest, err := namespacedKV.Put(ctx, "z/final", "value-z/final")
	require.NoError(t, err)

	getOpts := append(clientv3.WithFirstRev(),
		clientv3.WithRange("z"),
		clientv3.WithSerializable())
	txnResp, err := namespacedKV.Txn(ctx).
		Then(clientv3.OpTxn(nil,
			[]clientv3.Op{clientv3.OpGet("", getOpts...)},
			nil)).
		Commit()
	require.NoError(t, err)
	require.True(t, txnResp.Succeeded)
	require.Len(t, txnResp.Responses, 1)
	nestedTxn := txnResp.Responses[0].GetResponseTxn()
	require.NotNil(t, nestedTxn)
	require.True(t, nestedTxn.Succeeded)
	require.Len(t, nestedTxn.Responses, 1)
	nestedGet := nestedTxn.Responses[0].GetResponseRange()
	require.NotNil(t, nestedGet)
	require.NotNil(t, nestedGet.Header)
	require.Equal(t, latest.Header.Revision, nestedGet.Header.Revision)
	require.Equal(t, int64(2), nestedGet.Count)
	require.True(t, nestedGet.More)
	require.Len(t, nestedGet.Kvs, 1)
	require.Equal(t, []byte("items/a"), nestedGet.Kvs[0].Key)
	require.Equal(t, []byte("value-items/a"), nestedGet.Kvs[0].Value)

	current, err := namespacedKV.Get(ctx, "", clientv3.WithPrefix())
	require.NoError(t, err)
	require.Equal(t, int64(3), current.Count)
	require.Equal(t, [][]byte{[]byte("alpha/a"), []byte("items/a"), []byte("z/final")},
		[][]byte{current.Kvs[0].Key, current.Kvs[1].Key, current.Kvs[2].Key})
	outsideTenant, err := client.Get(ctx, "/a1642/namespace-nested-txn-empty-start-range-serializable-first-rev/tenant0/", clientv3.WithPrefix())
	require.NoError(t, err)
	require.Equal(t, int64(1), outsideTenant.Count)
}

func TestClientNamespaceNestedTxnGetEmptyStartRangeSerializableWithLastRevReturnsLogicalKey(t *testing.T) {
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
	tenantPrefix := "/a1643/namespace-nested-txn-empty-start-range-serializable-last-rev/tenant/"
	namespacedKV := namespace.NewKV(client.KV, tenantPrefix)
	_, err = namespacedKV.Put(ctx, "items/a", "value-items/a")
	require.NoError(t, err)
	_, err = namespacedKV.Put(ctx, "alpha/a", "value-alpha/a")
	require.NoError(t, err)
	_, err = namespacedKV.Put(ctx, "items/a", "updated-items/a")
	require.NoError(t, err)
	_, err = client.Put(ctx, "/a1643/namespace-nested-txn-empty-start-range-serializable-last-rev/tenant0/items/newer", "outside-tenant")
	require.NoError(t, err)
	latest, err := namespacedKV.Put(ctx, "z/final", "value-z/final")
	require.NoError(t, err)

	getOpts := append(clientv3.WithLastRev(),
		clientv3.WithRange("z"),
		clientv3.WithSerializable())
	txnResp, err := namespacedKV.Txn(ctx).
		Then(clientv3.OpTxn(nil,
			[]clientv3.Op{clientv3.OpGet("", getOpts...)},
			nil)).
		Commit()
	require.NoError(t, err)
	require.True(t, txnResp.Succeeded)
	require.Len(t, txnResp.Responses, 1)
	nestedTxn := txnResp.Responses[0].GetResponseTxn()
	require.NotNil(t, nestedTxn)
	require.True(t, nestedTxn.Succeeded)
	require.Len(t, nestedTxn.Responses, 1)
	nestedGet := nestedTxn.Responses[0].GetResponseRange()
	require.NotNil(t, nestedGet)
	require.NotNil(t, nestedGet.Header)
	require.Equal(t, latest.Header.Revision, nestedGet.Header.Revision)
	require.Equal(t, int64(2), nestedGet.Count)
	require.True(t, nestedGet.More)
	require.Len(t, nestedGet.Kvs, 1)
	require.Equal(t, []byte("items/a"), nestedGet.Kvs[0].Key)
	require.Equal(t, []byte("updated-items/a"), nestedGet.Kvs[0].Value)

	current, err := namespacedKV.Get(ctx, "", clientv3.WithPrefix())
	require.NoError(t, err)
	require.Equal(t, int64(3), current.Count)
	require.Equal(t, [][]byte{[]byte("alpha/a"), []byte("items/a"), []byte("z/final")},
		[][]byte{current.Kvs[0].Key, current.Kvs[1].Key, current.Kvs[2].Key})
	outsideTenant, err := client.Get(ctx, "/a1643/namespace-nested-txn-empty-start-range-serializable-last-rev/tenant0/", clientv3.WithPrefix())
	require.NoError(t, err)
	require.Equal(t, int64(1), outsideTenant.Count)
}

func TestClientNamespaceNestedTxnGetEmptyStartRangeSerializableSortByCreateRevisionDescReturnsLogicalPage(t *testing.T) {
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
	tenantPrefix := "/a1644/namespace-nested-txn-empty-start-range-serializable-create-sort/tenant/"
	namespacedKV := namespace.NewKV(client.KV, tenantPrefix)
	for _, key := range []string{"items/a", "alpha/a", "items/b"} {
		_, err = namespacedKV.Put(ctx, key, "value-"+key)
		require.NoError(t, err)
	}
	_, err = client.Put(ctx, "/a1644/namespace-nested-txn-empty-start-range-serializable-create-sort/tenant0/items/newer", "outside-tenant")
	require.NoError(t, err)
	latest, err := namespacedKV.Put(ctx, "z/final", "value-z/final")
	require.NoError(t, err)

	txnResp, err := namespacedKV.Txn(ctx).
		Then(clientv3.OpTxn(nil,
			[]clientv3.Op{clientv3.OpGet("",
				clientv3.WithRange("z"),
				clientv3.WithSerializable(),
				clientv3.WithLimit(2),
				clientv3.WithSort(clientv3.SortByCreateRevision, clientv3.SortDescend),
			)},
			nil)).
		Commit()
	require.NoError(t, err)
	require.True(t, txnResp.Succeeded)
	require.Len(t, txnResp.Responses, 1)
	nestedTxn := txnResp.Responses[0].GetResponseTxn()
	require.NotNil(t, nestedTxn)
	require.True(t, nestedTxn.Succeeded)
	require.Len(t, nestedTxn.Responses, 1)
	nestedGet := nestedTxn.Responses[0].GetResponseRange()
	require.NotNil(t, nestedGet)
	require.NotNil(t, nestedGet.Header)
	require.Equal(t, latest.Header.Revision, nestedGet.Header.Revision)
	require.Equal(t, int64(3), nestedGet.Count)
	require.True(t, nestedGet.More)
	require.Len(t, nestedGet.Kvs, 2)
	require.Equal(t, [][]byte{[]byte("items/b"), []byte("alpha/a")},
		[][]byte{nestedGet.Kvs[0].Key, nestedGet.Kvs[1].Key})
	require.Equal(t, [][]byte{[]byte("value-items/b"), []byte("value-alpha/a")},
		[][]byte{nestedGet.Kvs[0].Value, nestedGet.Kvs[1].Value})

	current, err := namespacedKV.Get(ctx, "", clientv3.WithPrefix())
	require.NoError(t, err)
	require.Equal(t, int64(4), current.Count)
	require.Equal(t, [][]byte{[]byte("alpha/a"), []byte("items/a"), []byte("items/b"), []byte("z/final")},
		[][]byte{current.Kvs[0].Key, current.Kvs[1].Key, current.Kvs[2].Key, current.Kvs[3].Key})
	outsideTenant, err := client.Get(ctx, "/a1644/namespace-nested-txn-empty-start-range-serializable-create-sort/tenant0/", clientv3.WithPrefix())
	require.NoError(t, err)
	require.Equal(t, int64(1), outsideTenant.Count)
}

func TestClientNamespaceNestedTxnGetEmptyStartRangeSerializableSortByCreateRevisionReturnsLogicalPage(t *testing.T) {
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
	tenantPrefix := "/a1651/namespace-nested-txn-empty-start-range-serializable-create-sort-none/tenant/"
	namespacedKV := namespace.NewKV(client.KV, tenantPrefix)
	for _, key := range []string{"items/a", "alpha/a", "items/b"} {
		_, err = namespacedKV.Put(ctx, key, "value-"+key)
		require.NoError(t, err)
	}
	_, err = client.Put(ctx, "/a1651/namespace-nested-txn-empty-start-range-serializable-create-sort-none/tenant0/alpha/a", "outside-tenant")
	require.NoError(t, err)
	latest, err := namespacedKV.Put(ctx, "z/final", "value-z/final")
	require.NoError(t, err)

	txnResp, err := namespacedKV.Txn(ctx).
		Then(clientv3.OpTxn(nil,
			[]clientv3.Op{clientv3.OpGet("",
				clientv3.WithRange("z"),
				clientv3.WithSerializable(),
				clientv3.WithLimit(2),
				clientv3.WithSort(clientv3.SortByCreateRevision, clientv3.SortNone),
			)},
			nil)).
		Commit()
	require.NoError(t, err)
	require.True(t, txnResp.Succeeded)
	require.Len(t, txnResp.Responses, 1)
	nestedTxn := txnResp.Responses[0].GetResponseTxn()
	require.NotNil(t, nestedTxn)
	require.True(t, nestedTxn.Succeeded)
	require.Len(t, nestedTxn.Responses, 1)
	nestedGet := nestedTxn.Responses[0].GetResponseRange()
	require.NotNil(t, nestedGet)
	require.NotNil(t, nestedGet.Header)
	require.Equal(t, latest.Header.Revision, nestedGet.Header.Revision)
	require.Equal(t, int64(3), nestedGet.Count)
	require.True(t, nestedGet.More)
	require.Len(t, nestedGet.Kvs, 2)
	require.Equal(t, [][]byte{[]byte("items/a"), []byte("alpha/a")},
		[][]byte{nestedGet.Kvs[0].Key, nestedGet.Kvs[1].Key})
	require.Equal(t, [][]byte{[]byte("value-items/a"), []byte("value-alpha/a")},
		[][]byte{nestedGet.Kvs[0].Value, nestedGet.Kvs[1].Value})
	require.Less(t, nestedGet.Kvs[0].CreateRevision, nestedGet.Kvs[1].CreateRevision)

	current, err := namespacedKV.Get(ctx, "", clientv3.WithPrefix())
	require.NoError(t, err)
	require.Equal(t, int64(4), current.Count)
	require.Equal(t, [][]byte{[]byte("alpha/a"), []byte("items/a"), []byte("items/b"), []byte("z/final")},
		[][]byte{current.Kvs[0].Key, current.Kvs[1].Key, current.Kvs[2].Key, current.Kvs[3].Key})
	outsideTenant, err := client.Get(ctx, "/a1651/namespace-nested-txn-empty-start-range-serializable-create-sort-none/tenant0/", clientv3.WithPrefix())
	require.NoError(t, err)
	require.Equal(t, int64(1), outsideTenant.Count)
}

func TestClientNamespaceNestedTxnGetEmptyStartRangeSerializableSortByModRevisionDescReturnsLogicalPage(t *testing.T) {
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
	tenantPrefix := "/a1645/namespace-nested-txn-empty-start-range-serializable-mod-sort/tenant/"
	namespacedKV := namespace.NewKV(client.KV, tenantPrefix)
	for _, key := range []string{"items/a", "alpha/a", "items/b"} {
		_, err = namespacedKV.Put(ctx, key, "value-"+key)
		require.NoError(t, err)
	}
	_, err = namespacedKV.Put(ctx, "items/a", "updated-items/a")
	require.NoError(t, err)
	_, err = namespacedKV.Put(ctx, "alpha/a", "updated-alpha/a")
	require.NoError(t, err)
	_, err = client.Put(ctx, "/a1645/namespace-nested-txn-empty-start-range-serializable-mod-sort/tenant0/items/newer", "outside-tenant")
	require.NoError(t, err)
	latest, err := namespacedKV.Put(ctx, "z/final", "value-z/final")
	require.NoError(t, err)

	txnResp, err := namespacedKV.Txn(ctx).
		Then(clientv3.OpTxn(nil,
			[]clientv3.Op{clientv3.OpGet("",
				clientv3.WithRange("z"),
				clientv3.WithSerializable(),
				clientv3.WithLimit(2),
				clientv3.WithSort(clientv3.SortByModRevision, clientv3.SortDescend),
			)},
			nil)).
		Commit()
	require.NoError(t, err)
	require.True(t, txnResp.Succeeded)
	require.Len(t, txnResp.Responses, 1)
	nestedTxn := txnResp.Responses[0].GetResponseTxn()
	require.NotNil(t, nestedTxn)
	require.True(t, nestedTxn.Succeeded)
	require.Len(t, nestedTxn.Responses, 1)
	nestedGet := nestedTxn.Responses[0].GetResponseRange()
	require.NotNil(t, nestedGet)
	require.NotNil(t, nestedGet.Header)
	require.Equal(t, latest.Header.Revision, nestedGet.Header.Revision)
	require.Equal(t, int64(3), nestedGet.Count)
	require.True(t, nestedGet.More)
	require.Len(t, nestedGet.Kvs, 2)
	require.Equal(t, [][]byte{[]byte("alpha/a"), []byte("items/a")},
		[][]byte{nestedGet.Kvs[0].Key, nestedGet.Kvs[1].Key})
	require.Equal(t, [][]byte{[]byte("updated-alpha/a"), []byte("updated-items/a")},
		[][]byte{nestedGet.Kvs[0].Value, nestedGet.Kvs[1].Value})

	current, err := namespacedKV.Get(ctx, "", clientv3.WithPrefix())
	require.NoError(t, err)
	require.Equal(t, int64(4), current.Count)
	require.Equal(t, [][]byte{[]byte("alpha/a"), []byte("items/a"), []byte("items/b"), []byte("z/final")},
		[][]byte{current.Kvs[0].Key, current.Kvs[1].Key, current.Kvs[2].Key, current.Kvs[3].Key})
	outsideTenant, err := client.Get(ctx, "/a1645/namespace-nested-txn-empty-start-range-serializable-mod-sort/tenant0/", clientv3.WithPrefix())
	require.NoError(t, err)
	require.Equal(t, int64(1), outsideTenant.Count)
}

func TestClientNamespaceNestedTxnGetEmptyStartRangeSerializableSortByModRevisionReturnsLogicalPage(t *testing.T) {
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
	tenantPrefix := "/a1652/namespace-nested-txn-empty-start-range-serializable-mod-sort-none/tenant/"
	namespacedKV := namespace.NewKV(client.KV, tenantPrefix)
	for _, key := range []string{"items/b", "items/a", "alpha/a"} {
		_, err = namespacedKV.Put(ctx, key, "value-"+key)
		require.NoError(t, err)
	}
	_, err = namespacedKV.Put(ctx, "alpha/a", "updated-alpha/a")
	require.NoError(t, err)
	_, err = namespacedKV.Put(ctx, "items/a", "updated-items/a")
	require.NoError(t, err)
	_, err = client.Put(ctx, "/a1652/namespace-nested-txn-empty-start-range-serializable-mod-sort-none/tenant0/items/newer", "outside-tenant")
	require.NoError(t, err)
	latest, err := namespacedKV.Put(ctx, "z/final", "value-z/final")
	require.NoError(t, err)

	txnResp, err := namespacedKV.Txn(ctx).
		Then(clientv3.OpTxn(nil,
			[]clientv3.Op{clientv3.OpGet("",
				clientv3.WithRange("z"),
				clientv3.WithSerializable(),
				clientv3.WithLimit(2),
				clientv3.WithSort(clientv3.SortByModRevision, clientv3.SortNone),
			)},
			nil)).
		Commit()
	require.NoError(t, err)
	require.True(t, txnResp.Succeeded)
	require.Len(t, txnResp.Responses, 1)
	nestedTxn := txnResp.Responses[0].GetResponseTxn()
	require.NotNil(t, nestedTxn)
	require.True(t, nestedTxn.Succeeded)
	require.Len(t, nestedTxn.Responses, 1)
	nestedGet := nestedTxn.Responses[0].GetResponseRange()
	require.NotNil(t, nestedGet)
	require.NotNil(t, nestedGet.Header)
	require.Equal(t, latest.Header.Revision, nestedGet.Header.Revision)
	require.Equal(t, int64(3), nestedGet.Count)
	require.True(t, nestedGet.More)
	require.Len(t, nestedGet.Kvs, 2)
	require.Equal(t, [][]byte{[]byte("items/b"), []byte("alpha/a")},
		[][]byte{nestedGet.Kvs[0].Key, nestedGet.Kvs[1].Key})
	require.Equal(t, [][]byte{[]byte("value-items/b"), []byte("updated-alpha/a")},
		[][]byte{nestedGet.Kvs[0].Value, nestedGet.Kvs[1].Value})
	require.Less(t, nestedGet.Kvs[0].ModRevision, nestedGet.Kvs[1].ModRevision)

	current, err := namespacedKV.Get(ctx, "", clientv3.WithPrefix())
	require.NoError(t, err)
	require.Equal(t, int64(4), current.Count)
	require.Equal(t, [][]byte{[]byte("alpha/a"), []byte("items/a"), []byte("items/b"), []byte("z/final")},
		[][]byte{current.Kvs[0].Key, current.Kvs[1].Key, current.Kvs[2].Key, current.Kvs[3].Key})
	outsideTenant, err := client.Get(ctx, "/a1652/namespace-nested-txn-empty-start-range-serializable-mod-sort-none/tenant0/", clientv3.WithPrefix())
	require.NoError(t, err)
	require.Equal(t, int64(1), outsideTenant.Count)
}

func TestClientNamespaceNestedTxnGetEmptyStartRangeSerializableSortByVersionDescReturnsLogicalPage(t *testing.T) {
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
	tenantPrefix := "/a1646/namespace-nested-txn-empty-start-range-serializable-version-sort/tenant/"
	namespacedKV := namespace.NewKV(client.KV, tenantPrefix)
	for _, key := range []string{"items/a", "alpha/a", "items/b"} {
		_, err = namespacedKV.Put(ctx, key, "value-"+key)
		require.NoError(t, err)
	}
	_, err = namespacedKV.Put(ctx, "alpha/a", "updated-alpha/a")
	require.NoError(t, err)
	_, err = namespacedKV.Put(ctx, "items/b", "updated-items/b")
	require.NoError(t, err)
	_, err = namespacedKV.Put(ctx, "items/b", "final-items/b")
	require.NoError(t, err)
	_, err = client.Put(ctx, "/a1646/namespace-nested-txn-empty-start-range-serializable-version-sort/tenant0/items/newer", "outside-tenant")
	require.NoError(t, err)
	latest, err := namespacedKV.Put(ctx, "z/final", "value-z/final")
	require.NoError(t, err)

	txnResp, err := namespacedKV.Txn(ctx).
		Then(clientv3.OpTxn(nil,
			[]clientv3.Op{clientv3.OpGet("",
				clientv3.WithRange("z"),
				clientv3.WithSerializable(),
				clientv3.WithLimit(2),
				clientv3.WithSort(clientv3.SortByVersion, clientv3.SortDescend),
			)},
			nil)).
		Commit()
	require.NoError(t, err)
	require.True(t, txnResp.Succeeded)
	require.Len(t, txnResp.Responses, 1)
	nestedTxn := txnResp.Responses[0].GetResponseTxn()
	require.NotNil(t, nestedTxn)
	require.True(t, nestedTxn.Succeeded)
	require.Len(t, nestedTxn.Responses, 1)
	nestedGet := nestedTxn.Responses[0].GetResponseRange()
	require.NotNil(t, nestedGet)
	require.NotNil(t, nestedGet.Header)
	require.Equal(t, latest.Header.Revision, nestedGet.Header.Revision)
	require.Equal(t, int64(3), nestedGet.Count)
	require.True(t, nestedGet.More)
	require.Len(t, nestedGet.Kvs, 2)
	require.Equal(t, [][]byte{[]byte("items/b"), []byte("alpha/a")},
		[][]byte{nestedGet.Kvs[0].Key, nestedGet.Kvs[1].Key})
	require.Equal(t, [][]byte{[]byte("final-items/b"), []byte("updated-alpha/a")},
		[][]byte{nestedGet.Kvs[0].Value, nestedGet.Kvs[1].Value})
	require.Equal(t, []int64{3, 2}, []int64{nestedGet.Kvs[0].Version, nestedGet.Kvs[1].Version})

	current, err := namespacedKV.Get(ctx, "", clientv3.WithPrefix())
	require.NoError(t, err)
	require.Equal(t, int64(4), current.Count)
	require.Equal(t, [][]byte{[]byte("alpha/a"), []byte("items/a"), []byte("items/b"), []byte("z/final")},
		[][]byte{current.Kvs[0].Key, current.Kvs[1].Key, current.Kvs[2].Key, current.Kvs[3].Key})
	outsideTenant, err := client.Get(ctx, "/a1646/namespace-nested-txn-empty-start-range-serializable-version-sort/tenant0/", clientv3.WithPrefix())
	require.NoError(t, err)
	require.Equal(t, int64(1), outsideTenant.Count)
}

func TestClientNamespaceNestedTxnGetEmptyStartRangeSerializableSortByVersionReturnsLogicalPage(t *testing.T) {
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
	tenantPrefix := "/a1653/namespace-nested-txn-empty-start-range-serializable-version-sort-none/tenant/"
	namespacedKV := namespace.NewKV(client.KV, tenantPrefix)
	_, err = namespacedKV.Put(ctx, "items/a", "value-items/a")
	require.NoError(t, err)
	_, err = namespacedKV.Put(ctx, "alpha/a", "value-alpha/a")
	require.NoError(t, err)
	_, err = namespacedKV.Put(ctx, "alpha/a", "updated-alpha/a")
	require.NoError(t, err)
	_, err = namespacedKV.Put(ctx, "items/b", "value-items/b")
	require.NoError(t, err)
	_, err = namespacedKV.Put(ctx, "items/b", "updated-items/b")
	require.NoError(t, err)
	_, err = namespacedKV.Put(ctx, "items/b", "final-items/b")
	require.NoError(t, err)
	_, err = client.Put(ctx, "/a1653/namespace-nested-txn-empty-start-range-serializable-version-sort-none/tenant0/items/newer", "outside-tenant")
	require.NoError(t, err)
	latest, err := namespacedKV.Put(ctx, "z/final", "value-z/final")
	require.NoError(t, err)

	txnResp, err := namespacedKV.Txn(ctx).
		Then(clientv3.OpTxn(nil,
			[]clientv3.Op{clientv3.OpGet("",
				clientv3.WithRange("z"),
				clientv3.WithSerializable(),
				clientv3.WithLimit(2),
				clientv3.WithSort(clientv3.SortByVersion, clientv3.SortNone),
			)},
			nil)).
		Commit()
	require.NoError(t, err)
	require.True(t, txnResp.Succeeded)
	require.Len(t, txnResp.Responses, 1)
	nestedTxn := txnResp.Responses[0].GetResponseTxn()
	require.NotNil(t, nestedTxn)
	require.True(t, nestedTxn.Succeeded)
	require.Len(t, nestedTxn.Responses, 1)
	nestedGet := nestedTxn.Responses[0].GetResponseRange()
	require.NotNil(t, nestedGet)
	require.NotNil(t, nestedGet.Header)
	require.Equal(t, latest.Header.Revision, nestedGet.Header.Revision)
	require.Equal(t, int64(3), nestedGet.Count)
	require.True(t, nestedGet.More)
	require.Len(t, nestedGet.Kvs, 2)
	require.Equal(t, [][]byte{[]byte("items/a"), []byte("alpha/a")},
		[][]byte{nestedGet.Kvs[0].Key, nestedGet.Kvs[1].Key})
	require.Equal(t, [][]byte{[]byte("value-items/a"), []byte("updated-alpha/a")},
		[][]byte{nestedGet.Kvs[0].Value, nestedGet.Kvs[1].Value})
	require.Equal(t, []int64{1, 2}, []int64{nestedGet.Kvs[0].Version, nestedGet.Kvs[1].Version})

	current, err := namespacedKV.Get(ctx, "", clientv3.WithPrefix())
	require.NoError(t, err)
	require.Equal(t, int64(4), current.Count)
	require.Equal(t, [][]byte{[]byte("alpha/a"), []byte("items/a"), []byte("items/b"), []byte("z/final")},
		[][]byte{current.Kvs[0].Key, current.Kvs[1].Key, current.Kvs[2].Key, current.Kvs[3].Key})
	outsideTenant, err := client.Get(ctx, "/a1653/namespace-nested-txn-empty-start-range-serializable-version-sort-none/tenant0/", clientv3.WithPrefix())
	require.NoError(t, err)
	require.Equal(t, int64(1), outsideTenant.Count)
}

func TestClientNamespaceNestedTxnGetEmptyStartRangeSerializableSortByValueDescReturnsLogicalPage(t *testing.T) {
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
	tenantPrefix := "/a1647/namespace-nested-txn-empty-start-range-serializable-value-sort/tenant/"
	namespacedKV := namespace.NewKV(client.KV, tenantPrefix)
	for _, key := range []string{"alpha/a", "items/a", "items/b"} {
		_, err = namespacedKV.Put(ctx, key, "value-"+key)
		require.NoError(t, err)
	}
	_, err = client.Put(ctx, "/a1647/namespace-nested-txn-empty-start-range-serializable-value-sort/tenant0/items/b", "zz-outside")
	require.NoError(t, err)
	latest, err := namespacedKV.Put(ctx, "z/final", "value-z/final")
	require.NoError(t, err)

	txnResp, err := namespacedKV.Txn(ctx).
		Then(clientv3.OpTxn(nil,
			[]clientv3.Op{clientv3.OpGet("",
				clientv3.WithRange("z"),
				clientv3.WithSerializable(),
				clientv3.WithLimit(2),
				clientv3.WithSort(clientv3.SortByValue, clientv3.SortDescend),
			)},
			nil)).
		Commit()
	require.NoError(t, err)
	require.True(t, txnResp.Succeeded)
	require.Len(t, txnResp.Responses, 1)
	nestedTxn := txnResp.Responses[0].GetResponseTxn()
	require.NotNil(t, nestedTxn)
	require.True(t, nestedTxn.Succeeded)
	require.Len(t, nestedTxn.Responses, 1)
	nestedGet := nestedTxn.Responses[0].GetResponseRange()
	require.NotNil(t, nestedGet)
	require.NotNil(t, nestedGet.Header)
	require.Equal(t, latest.Header.Revision, nestedGet.Header.Revision)
	require.Equal(t, int64(3), nestedGet.Count)
	require.True(t, nestedGet.More)
	require.Len(t, nestedGet.Kvs, 2)
	require.Equal(t, [][]byte{[]byte("items/b"), []byte("items/a")},
		[][]byte{nestedGet.Kvs[0].Key, nestedGet.Kvs[1].Key})
	require.Equal(t, [][]byte{[]byte("value-items/b"), []byte("value-items/a")},
		[][]byte{nestedGet.Kvs[0].Value, nestedGet.Kvs[1].Value})

	current, err := namespacedKV.Get(ctx, "", clientv3.WithPrefix())
	require.NoError(t, err)
	require.Equal(t, int64(4), current.Count)
	require.Equal(t, [][]byte{[]byte("alpha/a"), []byte("items/a"), []byte("items/b"), []byte("z/final")},
		[][]byte{current.Kvs[0].Key, current.Kvs[1].Key, current.Kvs[2].Key, current.Kvs[3].Key})
	outsideTenant, err := client.Get(ctx, "/a1647/namespace-nested-txn-empty-start-range-serializable-value-sort/tenant0/", clientv3.WithPrefix())
	require.NoError(t, err)
	require.Equal(t, int64(1), outsideTenant.Count)
}

func TestClientNamespaceNestedTxnGetEmptyStartRangeSerializableSortByKeyDescReturnsLogicalPage(t *testing.T) {
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
	tenantPrefix := "/a1648/namespace-nested-txn-empty-start-range-serializable-key-desc/tenant/"
	namespacedKV := namespace.NewKV(client.KV, tenantPrefix)
	for _, key := range []string{"alpha/a", "items/a", "items/b"} {
		_, err = namespacedKV.Put(ctx, key, "value-"+key)
		require.NoError(t, err)
	}
	_, err = client.Put(ctx, "/a1648/namespace-nested-txn-empty-start-range-serializable-key-desc/tenant0/items/newer", "outside-tenant")
	require.NoError(t, err)
	latest, err := namespacedKV.Put(ctx, "z/final", "value-z/final")
	require.NoError(t, err)

	txnResp, err := namespacedKV.Txn(ctx).
		Then(clientv3.OpTxn(nil,
			[]clientv3.Op{clientv3.OpGet("",
				clientv3.WithRange("z"),
				clientv3.WithSerializable(),
				clientv3.WithLimit(2),
				clientv3.WithSort(clientv3.SortByKey, clientv3.SortDescend),
			)},
			nil)).
		Commit()
	require.NoError(t, err)
	require.True(t, txnResp.Succeeded)
	require.Len(t, txnResp.Responses, 1)
	nestedTxn := txnResp.Responses[0].GetResponseTxn()
	require.NotNil(t, nestedTxn)
	require.True(t, nestedTxn.Succeeded)
	require.Len(t, nestedTxn.Responses, 1)
	nestedGet := nestedTxn.Responses[0].GetResponseRange()
	require.NotNil(t, nestedGet)
	require.NotNil(t, nestedGet.Header)
	require.Equal(t, latest.Header.Revision, nestedGet.Header.Revision)
	require.Equal(t, int64(3), nestedGet.Count)
	require.True(t, nestedGet.More)
	require.Len(t, nestedGet.Kvs, 2)
	require.Equal(t, [][]byte{[]byte("items/b"), []byte("items/a")},
		[][]byte{nestedGet.Kvs[0].Key, nestedGet.Kvs[1].Key})
	require.Equal(t, [][]byte{[]byte("value-items/b"), []byte("value-items/a")},
		[][]byte{nestedGet.Kvs[0].Value, nestedGet.Kvs[1].Value})

	current, err := namespacedKV.Get(ctx, "", clientv3.WithPrefix())
	require.NoError(t, err)
	require.Equal(t, int64(4), current.Count)
	require.Equal(t, [][]byte{[]byte("alpha/a"), []byte("items/a"), []byte("items/b"), []byte("z/final")},
		[][]byte{current.Kvs[0].Key, current.Kvs[1].Key, current.Kvs[2].Key, current.Kvs[3].Key})
	outsideTenant, err := client.Get(ctx, "/a1648/namespace-nested-txn-empty-start-range-serializable-key-desc/tenant0/", clientv3.WithPrefix())
	require.NoError(t, err)
	require.Equal(t, int64(1), outsideTenant.Count)
}

func TestClientNamespaceNestedTxnGetEmptyStartRangeSerializableSortByKeyAscReturnsLogicalPage(t *testing.T) {
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
	tenantPrefix := "/a1649/namespace-nested-txn-empty-start-range-serializable-key-asc/tenant/"
	namespacedKV := namespace.NewKV(client.KV, tenantPrefix)
	for _, key := range []string{"alpha/a", "items/a", "items/b"} {
		_, err = namespacedKV.Put(ctx, key, "value-"+key)
		require.NoError(t, err)
	}
	_, err = client.Put(ctx, "/a1649/namespace-nested-txn-empty-start-range-serializable-key-asc/tenant0/alpha/a", "outside-tenant")
	require.NoError(t, err)
	latest, err := namespacedKV.Put(ctx, "z/final", "value-z/final")
	require.NoError(t, err)

	txnResp, err := namespacedKV.Txn(ctx).
		Then(clientv3.OpTxn(nil,
			[]clientv3.Op{clientv3.OpGet("",
				clientv3.WithRange("z"),
				clientv3.WithSerializable(),
				clientv3.WithLimit(2),
				clientv3.WithSort(clientv3.SortByKey, clientv3.SortAscend),
			)},
			nil)).
		Commit()
	require.NoError(t, err)
	require.True(t, txnResp.Succeeded)
	require.Len(t, txnResp.Responses, 1)
	nestedTxn := txnResp.Responses[0].GetResponseTxn()
	require.NotNil(t, nestedTxn)
	require.True(t, nestedTxn.Succeeded)
	require.Len(t, nestedTxn.Responses, 1)
	nestedGet := nestedTxn.Responses[0].GetResponseRange()
	require.NotNil(t, nestedGet)
	require.NotNil(t, nestedGet.Header)
	require.Equal(t, latest.Header.Revision, nestedGet.Header.Revision)
	require.Equal(t, int64(3), nestedGet.Count)
	require.True(t, nestedGet.More)
	require.Len(t, nestedGet.Kvs, 2)
	require.Equal(t, [][]byte{[]byte("alpha/a"), []byte("items/a")},
		[][]byte{nestedGet.Kvs[0].Key, nestedGet.Kvs[1].Key})
	require.Equal(t, [][]byte{[]byte("value-alpha/a"), []byte("value-items/a")},
		[][]byte{nestedGet.Kvs[0].Value, nestedGet.Kvs[1].Value})

	current, err := namespacedKV.Get(ctx, "", clientv3.WithPrefix())
	require.NoError(t, err)
	require.Equal(t, int64(4), current.Count)
	require.Equal(t, [][]byte{[]byte("alpha/a"), []byte("items/a"), []byte("items/b"), []byte("z/final")},
		[][]byte{current.Kvs[0].Key, current.Kvs[1].Key, current.Kvs[2].Key, current.Kvs[3].Key})
	outsideTenant, err := client.Get(ctx, "/a1649/namespace-nested-txn-empty-start-range-serializable-key-asc/tenant0/", clientv3.WithPrefix())
	require.NoError(t, err)
	require.Equal(t, int64(1), outsideTenant.Count)
}

func TestClientNamespaceNestedTxnGetEmptyStartRangeSerializableSortByValueReturnsLogicalPage(t *testing.T) {
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
	tenantPrefix := "/a1650/namespace-nested-txn-empty-start-range-serializable-value-sort-none/tenant/"
	namespacedKV := namespace.NewKV(client.KV, tenantPrefix)
	for _, put := range []struct {
		key   string
		value string
	}{
		{key: "alpha/a", value: "value-z-alpha"},
		{key: "items/a", value: "value-a-items"},
		{key: "items/b", value: "value-b-items"},
	} {
		_, err = namespacedKV.Put(ctx, put.key, put.value)
		require.NoError(t, err)
	}
	_, err = client.Put(ctx, "/a1650/namespace-nested-txn-empty-start-range-serializable-value-sort-none/tenant0/items/a", "value-0-outside")
	require.NoError(t, err)
	latest, err := namespacedKV.Put(ctx, "z/final", "value-z-final")
	require.NoError(t, err)

	txnResp, err := namespacedKV.Txn(ctx).
		Then(clientv3.OpTxn(nil,
			[]clientv3.Op{clientv3.OpGet("",
				clientv3.WithRange("z"),
				clientv3.WithSerializable(),
				clientv3.WithLimit(2),
				clientv3.WithSort(clientv3.SortByValue, clientv3.SortNone),
			)},
			nil)).
		Commit()
	require.NoError(t, err)
	require.True(t, txnResp.Succeeded)
	require.Len(t, txnResp.Responses, 1)
	nestedTxn := txnResp.Responses[0].GetResponseTxn()
	require.NotNil(t, nestedTxn)
	require.True(t, nestedTxn.Succeeded)
	require.Len(t, nestedTxn.Responses, 1)
	nestedGet := nestedTxn.Responses[0].GetResponseRange()
	require.NotNil(t, nestedGet)
	require.NotNil(t, nestedGet.Header)
	require.Equal(t, latest.Header.Revision, nestedGet.Header.Revision)
	require.Equal(t, int64(3), nestedGet.Count)
	require.True(t, nestedGet.More)
	require.Len(t, nestedGet.Kvs, 2)
	require.Equal(t, [][]byte{[]byte("items/a"), []byte("items/b")},
		[][]byte{nestedGet.Kvs[0].Key, nestedGet.Kvs[1].Key})
	require.Equal(t, [][]byte{[]byte("value-a-items"), []byte("value-b-items")},
		[][]byte{nestedGet.Kvs[0].Value, nestedGet.Kvs[1].Value})

	current, err := namespacedKV.Get(ctx, "", clientv3.WithPrefix())
	require.NoError(t, err)
	require.Equal(t, int64(4), current.Count)
	require.Equal(t, [][]byte{[]byte("alpha/a"), []byte("items/a"), []byte("items/b"), []byte("z/final")},
		[][]byte{current.Kvs[0].Key, current.Kvs[1].Key, current.Kvs[2].Key, current.Kvs[3].Key})
	outsideTenant, err := client.Get(ctx, "/a1650/namespace-nested-txn-empty-start-range-serializable-value-sort-none/tenant0/", clientv3.WithPrefix())
	require.NoError(t, err)
	require.Equal(t, int64(1), outsideTenant.Count)
}

func TestClientNamespaceNestedTxnGetEmptyStartRangeSerializableKeysOnlySortByValueDescReturnsLogicalPage(t *testing.T) {
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
	tenantPrefix := "/a1654/namespace-nested-txn-empty-start-range-serializable-keysonly-value-sort/tenant/"
	namespacedKV := namespace.NewKV(client.KV, tenantPrefix)
	for _, put := range []struct {
		key   string
		value string
	}{
		{key: "alpha/a", value: "value-alpha/a"},
		{key: "items/a", value: "value-items/a"},
		{key: "items/b", value: "value-items/b"},
	} {
		_, err = namespacedKV.Put(ctx, put.key, put.value)
		require.NoError(t, err)
	}
	_, err = client.Put(ctx, "/a1654/namespace-nested-txn-empty-start-range-serializable-keysonly-value-sort/tenant0/items/b", "zz-outside")
	require.NoError(t, err)
	latest, err := namespacedKV.Put(ctx, "z/final", "zz-upper")
	require.NoError(t, err)

	txnResp, err := namespacedKV.Txn(ctx).
		Then(clientv3.OpTxn(nil,
			[]clientv3.Op{clientv3.OpGet("",
				clientv3.WithRange("z"),
				clientv3.WithSerializable(),
				clientv3.WithKeysOnly(),
				clientv3.WithLimit(2),
				clientv3.WithSort(clientv3.SortByValue, clientv3.SortDescend),
			)},
			nil)).
		Commit()
	require.NoError(t, err)
	require.True(t, txnResp.Succeeded)
	require.Len(t, txnResp.Responses, 1)
	nestedTxn := txnResp.Responses[0].GetResponseTxn()
	require.NotNil(t, nestedTxn)
	require.True(t, nestedTxn.Succeeded)
	require.Len(t, nestedTxn.Responses, 1)
	nestedGet := nestedTxn.Responses[0].GetResponseRange()
	require.NotNil(t, nestedGet)
	require.NotNil(t, nestedGet.Header)
	require.Equal(t, latest.Header.Revision, nestedGet.Header.Revision)
	require.Equal(t, int64(3), nestedGet.Count)
	require.True(t, nestedGet.More)
	require.Len(t, nestedGet.Kvs, 2)
	require.Equal(t, [][]byte{[]byte("items/b"), []byte("items/a")},
		[][]byte{nestedGet.Kvs[0].Key, nestedGet.Kvs[1].Key})
	require.Empty(t, nestedGet.Kvs[0].Value)
	require.Empty(t, nestedGet.Kvs[1].Value)

	current, err := namespacedKV.Get(ctx, "", clientv3.WithPrefix())
	require.NoError(t, err)
	require.Equal(t, int64(4), current.Count)
	require.Equal(t, [][]byte{[]byte("alpha/a"), []byte("items/a"), []byte("items/b"), []byte("z/final")},
		[][]byte{current.Kvs[0].Key, current.Kvs[1].Key, current.Kvs[2].Key, current.Kvs[3].Key})
	outsideTenant, err := client.Get(ctx, "/a1654/namespace-nested-txn-empty-start-range-serializable-keysonly-value-sort/tenant0/", clientv3.WithPrefix())
	require.NoError(t, err)
	require.Equal(t, int64(1), outsideTenant.Count)
}

func TestClientNamespaceNestedTxnGetEmptyStartRangeSerializableCountOnlyTakesPrecedence(t *testing.T) {
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
	tenantPrefix := "/a1655/namespace-nested-txn-empty-start-range-serializable-countonly/tenant/"
	namespacedKV := namespace.NewKV(client.KV, tenantPrefix)
	for _, key := range []string{"alpha/a", "items/a", "items/b"} {
		_, err = namespacedKV.Put(ctx, key, "value-"+key)
		require.NoError(t, err)
	}
	_, err = client.Put(ctx, "/a1655/namespace-nested-txn-empty-start-range-serializable-countonly/tenant0/items/a", "outside-tenant")
	require.NoError(t, err)
	latest, err := namespacedKV.Put(ctx, "z/final", "value-z/final")
	require.NoError(t, err)

	txnResp, err := namespacedKV.Txn(ctx).
		Then(clientv3.OpTxn(nil,
			[]clientv3.Op{clientv3.OpGet("",
				clientv3.WithRange("z"),
				clientv3.WithSerializable(),
				clientv3.WithKeysOnly(),
				clientv3.WithCountOnly(),
				clientv3.WithLimit(1),
			)},
			nil)).
		Commit()
	require.NoError(t, err)
	require.True(t, txnResp.Succeeded)
	require.Len(t, txnResp.Responses, 1)
	nestedTxn := txnResp.Responses[0].GetResponseTxn()
	require.NotNil(t, nestedTxn)
	require.True(t, nestedTxn.Succeeded)
	require.Len(t, nestedTxn.Responses, 1)
	nestedGet := nestedTxn.Responses[0].GetResponseRange()
	require.NotNil(t, nestedGet)
	require.NotNil(t, nestedGet.Header)
	require.Equal(t, latest.Header.Revision, nestedGet.Header.Revision)
	require.Equal(t, int64(3), nestedGet.Count)
	require.False(t, nestedGet.More)
	require.Empty(t, nestedGet.Kvs)

	current, err := namespacedKV.Get(ctx, "", clientv3.WithPrefix())
	require.NoError(t, err)
	require.Equal(t, int64(4), current.Count)
	require.Equal(t, [][]byte{[]byte("alpha/a"), []byte("items/a"), []byte("items/b"), []byte("z/final")},
		[][]byte{current.Kvs[0].Key, current.Kvs[1].Key, current.Kvs[2].Key, current.Kvs[3].Key})
	outsideTenant, err := client.Get(ctx, "/a1655/namespace-nested-txn-empty-start-range-serializable-countonly/tenant0/", clientv3.WithPrefix())
	require.NoError(t, err)
	require.Equal(t, int64(1), outsideTenant.Count)
}

func TestClientNamespaceNestedTxnGetEmptyStartRangeSerializableLimitZeroReturnsFullLogicalRange(t *testing.T) {
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
	tenantPrefix := "/a1656/namespace-nested-txn-empty-start-range-serializable-limit-zero/tenant/"
	namespacedKV := namespace.NewKV(client.KV, tenantPrefix)
	for _, key := range []string{"alpha/a", "items/a", "items/b"} {
		_, err = namespacedKV.Put(ctx, key, "value-"+key)
		require.NoError(t, err)
	}
	_, err = client.Put(ctx, "/a1656/namespace-nested-txn-empty-start-range-serializable-limit-zero/tenant0/items/a", "outside-tenant")
	require.NoError(t, err)
	latest, err := namespacedKV.Put(ctx, "z/final", "value-z/final")
	require.NoError(t, err)

	txnResp, err := namespacedKV.Txn(ctx).
		Then(clientv3.OpTxn(nil,
			[]clientv3.Op{clientv3.OpGet("",
				clientv3.WithRange("z"),
				clientv3.WithSerializable(),
				clientv3.WithLimit(0),
				clientv3.WithSort(clientv3.SortByKey, clientv3.SortAscend),
			)},
			nil)).
		Commit()
	require.NoError(t, err)
	require.True(t, txnResp.Succeeded)
	require.Len(t, txnResp.Responses, 1)
	nestedTxn := txnResp.Responses[0].GetResponseTxn()
	require.NotNil(t, nestedTxn)
	require.True(t, nestedTxn.Succeeded)
	require.Len(t, nestedTxn.Responses, 1)
	nestedGet := nestedTxn.Responses[0].GetResponseRange()
	require.NotNil(t, nestedGet)
	require.NotNil(t, nestedGet.Header)
	require.Equal(t, latest.Header.Revision, nestedGet.Header.Revision)
	require.Equal(t, int64(3), nestedGet.Count)
	require.False(t, nestedGet.More)
	require.Len(t, nestedGet.Kvs, 3)
	require.Equal(t, [][]byte{[]byte("alpha/a"), []byte("items/a"), []byte("items/b")},
		[][]byte{nestedGet.Kvs[0].Key, nestedGet.Kvs[1].Key, nestedGet.Kvs[2].Key})
	require.Equal(t, [][]byte{[]byte("value-alpha/a"), []byte("value-items/a"), []byte("value-items/b")},
		[][]byte{nestedGet.Kvs[0].Value, nestedGet.Kvs[1].Value, nestedGet.Kvs[2].Value})

	current, err := namespacedKV.Get(ctx, "", clientv3.WithPrefix())
	require.NoError(t, err)
	require.Equal(t, int64(4), current.Count)
	require.Equal(t, [][]byte{[]byte("alpha/a"), []byte("items/a"), []byte("items/b"), []byte("z/final")},
		[][]byte{current.Kvs[0].Key, current.Kvs[1].Key, current.Kvs[2].Key, current.Kvs[3].Key})
	outsideTenant, err := client.Get(ctx, "/a1656/namespace-nested-txn-empty-start-range-serializable-limit-zero/tenant0/", clientv3.WithPrefix())
	require.NoError(t, err)
	require.Equal(t, int64(1), outsideTenant.Count)
}

func TestClientNamespaceNestedTxnGetEmptyStartRangeSerializableNegativeLimitReturnsFullLogicalRange(t *testing.T) {
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
	tenantPrefix := "/a1657/namespace-nested-txn-empty-start-range-serializable-negative-limit/tenant/"
	namespacedKV := namespace.NewKV(client.KV, tenantPrefix)
	for _, key := range []string{"alpha/a", "items/a", "items/b"} {
		_, err = namespacedKV.Put(ctx, key, "value-"+key)
		require.NoError(t, err)
	}
	_, err = client.Put(ctx, "/a1657/namespace-nested-txn-empty-start-range-serializable-negative-limit/tenant0/items/a", "outside-tenant")
	require.NoError(t, err)
	latest, err := namespacedKV.Put(ctx, "z/final", "value-z/final")
	require.NoError(t, err)

	txnResp, err := namespacedKV.Txn(ctx).
		Then(clientv3.OpTxn(nil,
			[]clientv3.Op{clientv3.OpGet("",
				clientv3.WithRange("z"),
				clientv3.WithSerializable(),
				clientv3.WithLimit(-1),
				clientv3.WithSort(clientv3.SortByKey, clientv3.SortAscend),
			)},
			nil)).
		Commit()
	require.NoError(t, err)
	require.True(t, txnResp.Succeeded)
	require.Len(t, txnResp.Responses, 1)
	nestedTxn := txnResp.Responses[0].GetResponseTxn()
	require.NotNil(t, nestedTxn)
	require.True(t, nestedTxn.Succeeded)
	require.Len(t, nestedTxn.Responses, 1)
	nestedGet := nestedTxn.Responses[0].GetResponseRange()
	require.NotNil(t, nestedGet)
	require.NotNil(t, nestedGet.Header)
	require.Equal(t, latest.Header.Revision, nestedGet.Header.Revision)
	require.Equal(t, int64(3), nestedGet.Count)
	require.False(t, nestedGet.More)
	require.Len(t, nestedGet.Kvs, 3)
	require.Equal(t, [][]byte{[]byte("alpha/a"), []byte("items/a"), []byte("items/b")},
		[][]byte{nestedGet.Kvs[0].Key, nestedGet.Kvs[1].Key, nestedGet.Kvs[2].Key})
	require.Equal(t, [][]byte{[]byte("value-alpha/a"), []byte("value-items/a"), []byte("value-items/b")},
		[][]byte{nestedGet.Kvs[0].Value, nestedGet.Kvs[1].Value, nestedGet.Kvs[2].Value})

	current, err := namespacedKV.Get(ctx, "", clientv3.WithPrefix())
	require.NoError(t, err)
	require.Equal(t, int64(4), current.Count)
	require.Equal(t, [][]byte{[]byte("alpha/a"), []byte("items/a"), []byte("items/b"), []byte("z/final")},
		[][]byte{current.Kvs[0].Key, current.Kvs[1].Key, current.Kvs[2].Key, current.Kvs[3].Key})
	outsideTenant, err := client.Get(ctx, "/a1657/namespace-nested-txn-empty-start-range-serializable-negative-limit/tenant0/", clientv3.WithPrefix())
	require.NoError(t, err)
	require.Equal(t, int64(1), outsideTenant.Count)
}

func TestClientNamespaceNestedTxnGetEmptyStartRangeSerializableMaxInt64LimitReturnsFullLogicalRange(t *testing.T) {
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
	tenantPrefix := "/a1658/namespace-nested-txn-empty-start-range-serializable-max-limit/tenant/"
	namespacedKV := namespace.NewKV(client.KV, tenantPrefix)
	for _, kv := range []struct {
		key   string
		value string
	}{
		{key: "alpha/a", value: "value-c"},
		{key: "items/a", value: "value-a"},
		{key: "items/b", value: "value-b"},
	} {
		_, err = namespacedKV.Put(ctx, kv.key, kv.value)
		require.NoError(t, err)
	}
	_, err = client.Put(ctx, "/a1658/namespace-nested-txn-empty-start-range-serializable-max-limit/tenant0/items/a", "outside-tenant")
	require.NoError(t, err)
	latest, err := namespacedKV.Put(ctx, "z/final", "value-z")
	require.NoError(t, err)

	txnResp, err := namespacedKV.Txn(ctx).
		Then(clientv3.OpTxn(nil,
			[]clientv3.Op{clientv3.OpGet("",
				clientv3.WithRange("z"),
				clientv3.WithSerializable(),
				clientv3.WithLimit(math.MaxInt64),
				clientv3.WithSort(clientv3.SortByValue, clientv3.SortNone),
			)},
			nil)).
		Commit()
	require.NoError(t, err)
	require.True(t, txnResp.Succeeded)
	require.Len(t, txnResp.Responses, 1)
	nestedTxn := txnResp.Responses[0].GetResponseTxn()
	require.NotNil(t, nestedTxn)
	require.True(t, nestedTxn.Succeeded)
	require.Len(t, nestedTxn.Responses, 1)
	nestedGet := nestedTxn.Responses[0].GetResponseRange()
	require.NotNil(t, nestedGet)
	require.NotNil(t, nestedGet.Header)
	require.Equal(t, latest.Header.Revision, nestedGet.Header.Revision)
	require.Equal(t, int64(3), nestedGet.Count)
	require.False(t, nestedGet.More)
	require.Len(t, nestedGet.Kvs, 3)
	require.Equal(t, [][]byte{[]byte("items/a"), []byte("items/b"), []byte("alpha/a")},
		[][]byte{nestedGet.Kvs[0].Key, nestedGet.Kvs[1].Key, nestedGet.Kvs[2].Key})
	require.Equal(t, [][]byte{[]byte("value-a"), []byte("value-b"), []byte("value-c")},
		[][]byte{nestedGet.Kvs[0].Value, nestedGet.Kvs[1].Value, nestedGet.Kvs[2].Value})

	current, err := namespacedKV.Get(ctx, "", clientv3.WithPrefix())
	require.NoError(t, err)
	require.Equal(t, int64(4), current.Count)
	require.Equal(t, [][]byte{[]byte("alpha/a"), []byte("items/a"), []byte("items/b"), []byte("z/final")},
		[][]byte{current.Kvs[0].Key, current.Kvs[1].Key, current.Kvs[2].Key, current.Kvs[3].Key})
	outsideTenant, err := client.Get(ctx, "/a1658/namespace-nested-txn-empty-start-range-serializable-max-limit/tenant0/", clientv3.WithPrefix())
	require.NoError(t, err)
	require.Equal(t, int64(1), outsideTenant.Count)
}

func TestClientNamespaceNestedTxnGetEmptyPrefixSerializableSortByValueDescReturnsLogicalPage(t *testing.T) {
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
	tenantPrefix := "/a1659/namespace-nested-txn-empty-prefix-serializable-sort-value/tenant/"
	namespacedKV := namespace.NewKV(client.KV, tenantPrefix)
	for _, key := range []string{"alpha/a", "items/a"} {
		_, err = namespacedKV.Put(ctx, key, "value-"+key)
		require.NoError(t, err)
	}
	_, err = client.Put(ctx, "/a1659/namespace-nested-txn-empty-prefix-serializable-sort-value/tenant0/z/final", "zz-outside")
	require.NoError(t, err)
	latest, err := namespacedKV.Put(ctx, "z/final", "value-z/final")
	require.NoError(t, err)

	txnResp, err := namespacedKV.Txn(ctx).
		Then(clientv3.OpTxn(nil,
			[]clientv3.Op{clientv3.OpGet("",
				clientv3.WithPrefix(),
				clientv3.WithSerializable(),
				clientv3.WithLimit(2),
				clientv3.WithSort(clientv3.SortByValue, clientv3.SortDescend),
			)},
			nil)).
		Commit()
	require.NoError(t, err)
	require.True(t, txnResp.Succeeded)
	require.Len(t, txnResp.Responses, 1)
	nestedTxn := txnResp.Responses[0].GetResponseTxn()
	require.NotNil(t, nestedTxn)
	require.True(t, nestedTxn.Succeeded)
	require.Len(t, nestedTxn.Responses, 1)
	nestedGet := nestedTxn.Responses[0].GetResponseRange()
	require.NotNil(t, nestedGet)
	require.NotNil(t, nestedGet.Header)
	require.Equal(t, latest.Header.Revision, nestedGet.Header.Revision)
	require.Equal(t, int64(3), nestedGet.Count)
	require.True(t, nestedGet.More)
	require.Len(t, nestedGet.Kvs, 2)
	require.Equal(t, [][]byte{[]byte("z/final"), []byte("items/a")},
		[][]byte{nestedGet.Kvs[0].Key, nestedGet.Kvs[1].Key})
	require.Equal(t, [][]byte{[]byte("value-z/final"), []byte("value-items/a")},
		[][]byte{nestedGet.Kvs[0].Value, nestedGet.Kvs[1].Value})

	current, err := namespacedKV.Get(ctx, "", clientv3.WithPrefix())
	require.NoError(t, err)
	require.Equal(t, int64(3), current.Count)
	require.Equal(t, [][]byte{[]byte("alpha/a"), []byte("items/a"), []byte("z/final")},
		[][]byte{current.Kvs[0].Key, current.Kvs[1].Key, current.Kvs[2].Key})
	outsideTenant, err := client.Get(ctx, "/a1659/namespace-nested-txn-empty-prefix-serializable-sort-value/tenant0/", clientv3.WithPrefix())
	require.NoError(t, err)
	require.Equal(t, int64(1), outsideTenant.Count)
}

func TestClientNamespaceNestedTxnGetEmptyPrefixSerializableKeysOnlyLimitReturnsLogicalPage(t *testing.T) {
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
	tenantPrefix := "/a1660/namespace-nested-txn-empty-prefix-serializable-keysonly/tenant/"
	namespacedKV := namespace.NewKV(client.KV, tenantPrefix)
	for _, key := range []string{"alpha/a", "items/a"} {
		_, err = namespacedKV.Put(ctx, key, "value-"+key)
		require.NoError(t, err)
	}
	_, err = client.Put(ctx, "/a1660/namespace-nested-txn-empty-prefix-serializable-keysonly/tenant0/items/a", "outside-tenant")
	require.NoError(t, err)
	latest, err := namespacedKV.Put(ctx, "z/final", "value-z/final")
	require.NoError(t, err)

	txnResp, err := namespacedKV.Txn(ctx).
		Then(clientv3.OpTxn(nil,
			[]clientv3.Op{clientv3.OpGet("",
				clientv3.WithPrefix(),
				clientv3.WithSerializable(),
				clientv3.WithKeysOnly(),
				clientv3.WithLimit(2),
			)},
			nil)).
		Commit()
	require.NoError(t, err)
	require.True(t, txnResp.Succeeded)
	require.Len(t, txnResp.Responses, 1)
	nestedTxn := txnResp.Responses[0].GetResponseTxn()
	require.NotNil(t, nestedTxn)
	require.True(t, nestedTxn.Succeeded)
	require.Len(t, nestedTxn.Responses, 1)
	nestedGet := nestedTxn.Responses[0].GetResponseRange()
	require.NotNil(t, nestedGet)
	require.NotNil(t, nestedGet.Header)
	require.Equal(t, latest.Header.Revision, nestedGet.Header.Revision)
	require.Equal(t, int64(3), nestedGet.Count)
	require.True(t, nestedGet.More)
	require.Len(t, nestedGet.Kvs, 2)
	require.Equal(t, [][]byte{[]byte("alpha/a"), []byte("items/a")},
		[][]byte{nestedGet.Kvs[0].Key, nestedGet.Kvs[1].Key})
	require.Empty(t, nestedGet.Kvs[0].Value)
	require.Empty(t, nestedGet.Kvs[1].Value)

	current, err := namespacedKV.Get(ctx, "", clientv3.WithPrefix())
	require.NoError(t, err)
	require.Equal(t, int64(3), current.Count)
	require.Equal(t, [][]byte{[]byte("alpha/a"), []byte("items/a"), []byte("z/final")},
		[][]byte{current.Kvs[0].Key, current.Kvs[1].Key, current.Kvs[2].Key})
	outsideTenant, err := client.Get(ctx, "/a1660/namespace-nested-txn-empty-prefix-serializable-keysonly/tenant0/", clientv3.WithPrefix())
	require.NoError(t, err)
	require.Equal(t, int64(1), outsideTenant.Count)
}

func TestClientNamespaceNestedTxnGetEmptyPrefixSerializableCountOnlyTakesPrecedence(t *testing.T) {
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
	tenantPrefix := "/a1661/namespace-nested-txn-empty-prefix-serializable-countonly/tenant/"
	namespacedKV := namespace.NewKV(client.KV, tenantPrefix)
	for _, key := range []string{"alpha/a", "items/a"} {
		_, err = namespacedKV.Put(ctx, key, "value-"+key)
		require.NoError(t, err)
	}
	_, err = client.Put(ctx, "/a1661/namespace-nested-txn-empty-prefix-serializable-countonly/tenant0/items/a", "outside-tenant")
	require.NoError(t, err)
	latest, err := namespacedKV.Put(ctx, "z/final", "value-z/final")
	require.NoError(t, err)

	txnResp, err := namespacedKV.Txn(ctx).
		Then(clientv3.OpTxn(nil,
			[]clientv3.Op{clientv3.OpGet("",
				clientv3.WithPrefix(),
				clientv3.WithSerializable(),
				clientv3.WithKeysOnly(),
				clientv3.WithCountOnly(),
				clientv3.WithLimit(1),
			)},
			nil)).
		Commit()
	require.NoError(t, err)
	require.True(t, txnResp.Succeeded)
	require.Len(t, txnResp.Responses, 1)
	nestedTxn := txnResp.Responses[0].GetResponseTxn()
	require.NotNil(t, nestedTxn)
	require.True(t, nestedTxn.Succeeded)
	require.Len(t, nestedTxn.Responses, 1)
	nestedGet := nestedTxn.Responses[0].GetResponseRange()
	require.NotNil(t, nestedGet)
	require.NotNil(t, nestedGet.Header)
	require.Equal(t, latest.Header.Revision, nestedGet.Header.Revision)
	require.Equal(t, int64(3), nestedGet.Count)
	require.False(t, nestedGet.More)
	require.Empty(t, nestedGet.Kvs)

	current, err := namespacedKV.Get(ctx, "", clientv3.WithPrefix())
	require.NoError(t, err)
	require.Equal(t, int64(3), current.Count)
	require.Equal(t, [][]byte{[]byte("alpha/a"), []byte("items/a"), []byte("z/final")},
		[][]byte{current.Kvs[0].Key, current.Kvs[1].Key, current.Kvs[2].Key})
	outsideTenant, err := client.Get(ctx, "/a1661/namespace-nested-txn-empty-prefix-serializable-countonly/tenant0/", clientv3.WithPrefix())
	require.NoError(t, err)
	require.Equal(t, int64(1), outsideTenant.Count)
}

func TestClientNamespaceNestedTxnGetSerializablePrefixSortByValueWithLimitReturnsLogicalKeys(t *testing.T) {
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
	tenantPrefix := "/a1662/namespace-nested-txn-serializable-prefix-sort-value/tenant/"
	namespacedKV := namespace.NewKV(client.KV, tenantPrefix)
	for _, seed := range []struct {
		key   string
		value string
	}{
		{key: "items/d", value: "n"},
		{key: "items/c", value: "a"},
		{key: "items/b", value: "m"},
		{key: "items/a", value: "z"},
	} {
		_, err = namespacedKV.Put(ctx, seed.key, seed.value)
		require.NoError(t, err)
	}
	_, err = client.Put(ctx, "/a1662/namespace-nested-txn-serializable-prefix-sort-value/tenant/items0/outside", "0")
	require.NoError(t, err)
	latest, err := client.Put(ctx, "/a1662/namespace-nested-txn-serializable-prefix-sort-value/tenant0/items/e", "0")
	require.NoError(t, err)

	txnResp, err := namespacedKV.Txn(ctx).
		Then(clientv3.OpTxn(nil,
			[]clientv3.Op{clientv3.OpGet("items/",
				clientv3.WithPrefix(),
				clientv3.WithSerializable(),
				clientv3.WithLimit(2),
				clientv3.WithSort(clientv3.SortByValue, clientv3.SortNone),
			)},
			nil)).
		Commit()
	require.NoError(t, err)
	require.True(t, txnResp.Succeeded)
	require.Len(t, txnResp.Responses, 1)
	nestedTxn := txnResp.Responses[0].GetResponseTxn()
	require.NotNil(t, nestedTxn)
	require.True(t, nestedTxn.Succeeded)
	require.Len(t, nestedTxn.Responses, 1)
	nestedGet := nestedTxn.Responses[0].GetResponseRange()
	require.NotNil(t, nestedGet)
	require.NotNil(t, nestedGet.Header)
	require.Equal(t, latest.Header.Revision, nestedGet.Header.Revision)
	require.Equal(t, int64(4), nestedGet.Count)
	require.True(t, nestedGet.More)
	require.Len(t, nestedGet.Kvs, 2)
	require.Equal(t, [][]byte{[]byte("items/c"), []byte("items/b")},
		[][]byte{nestedGet.Kvs[0].Key, nestedGet.Kvs[1].Key})
	require.Equal(t, [][]byte{[]byte("a"), []byte("m")},
		[][]byte{nestedGet.Kvs[0].Value, nestedGet.Kvs[1].Value})

	current, err := namespacedKV.Get(ctx, "items/", clientv3.WithPrefix())
	require.NoError(t, err)
	require.Equal(t, int64(4), current.Count)
	outsideSameTenantPrefix, err := client.Get(ctx, "/a1662/namespace-nested-txn-serializable-prefix-sort-value/tenant/items0/", clientv3.WithPrefix())
	require.NoError(t, err)
	require.Equal(t, int64(1), outsideSameTenantPrefix.Count)
	outsideTenant, err := client.Get(ctx, "/a1662/namespace-nested-txn-serializable-prefix-sort-value/tenant0/", clientv3.WithPrefix())
	require.NoError(t, err)
	require.Equal(t, int64(1), outsideTenant.Count)
}

func TestClientNamespaceNestedTxnGetSerializablePrefixKeysOnlySortByValueDescWithLimitReturnsLogicalKeys(t *testing.T) {
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
	tenantPrefix := "/a1663/namespace-nested-txn-serializable-prefix-keysonly-sort-value/tenant/"
	namespacedKV := namespace.NewKV(client.KV, tenantPrefix)
	for _, seed := range []struct {
		key   string
		value string
	}{
		{key: "items/d", value: "n"},
		{key: "items/c", value: "a"},
		{key: "items/b", value: "m"},
		{key: "items/a", value: "z"},
	} {
		_, err = namespacedKV.Put(ctx, seed.key, seed.value)
		require.NoError(t, err)
	}
	_, err = client.Put(ctx, "/a1663/namespace-nested-txn-serializable-prefix-keysonly-sort-value/tenant/items0/outside", "zz")
	require.NoError(t, err)
	latest, err := client.Put(ctx, "/a1663/namespace-nested-txn-serializable-prefix-keysonly-sort-value/tenant0/items/e", "zz")
	require.NoError(t, err)

	txnResp, err := namespacedKV.Txn(ctx).
		Then(clientv3.OpTxn(nil,
			[]clientv3.Op{clientv3.OpGet("items/",
				clientv3.WithPrefix(),
				clientv3.WithSerializable(),
				clientv3.WithLimit(2),
				clientv3.WithKeysOnly(),
				clientv3.WithSort(clientv3.SortByValue, clientv3.SortDescend),
			)},
			nil)).
		Commit()
	require.NoError(t, err)
	require.True(t, txnResp.Succeeded)
	require.Len(t, txnResp.Responses, 1)
	nestedTxn := txnResp.Responses[0].GetResponseTxn()
	require.NotNil(t, nestedTxn)
	require.True(t, nestedTxn.Succeeded)
	require.Len(t, nestedTxn.Responses, 1)
	nestedGet := nestedTxn.Responses[0].GetResponseRange()
	require.NotNil(t, nestedGet)
	require.NotNil(t, nestedGet.Header)
	require.Equal(t, latest.Header.Revision, nestedGet.Header.Revision)
	require.Equal(t, int64(4), nestedGet.Count)
	require.True(t, nestedGet.More)
	require.Len(t, nestedGet.Kvs, 2)
	require.Equal(t, [][]byte{[]byte("items/a"), []byte("items/d")},
		[][]byte{nestedGet.Kvs[0].Key, nestedGet.Kvs[1].Key})
	require.Empty(t, nestedGet.Kvs[0].Value)
	require.Empty(t, nestedGet.Kvs[1].Value)

	current, err := namespacedKV.Get(ctx, "items/", clientv3.WithPrefix())
	require.NoError(t, err)
	require.Equal(t, int64(4), current.Count)
	outsideSameTenantPrefix, err := client.Get(ctx, "/a1663/namespace-nested-txn-serializable-prefix-keysonly-sort-value/tenant/items0/", clientv3.WithPrefix())
	require.NoError(t, err)
	require.Equal(t, int64(1), outsideSameTenantPrefix.Count)
	outsideTenant, err := client.Get(ctx, "/a1663/namespace-nested-txn-serializable-prefix-keysonly-sort-value/tenant0/", clientv3.WithPrefix())
	require.NoError(t, err)
	require.Equal(t, int64(1), outsideTenant.Count)
}

func TestClientNamespaceNestedTxnGetSerializablePrefixCountOnlyTakesPrecedence(t *testing.T) {
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
	tenantPrefix := "/a1664/namespace-nested-txn-serializable-prefix-countonly/tenant/"
	namespacedKV := namespace.NewKV(client.KV, tenantPrefix)
	for _, key := range []string{"items/a", "items/b", "items/c"} {
		_, err = namespacedKV.Put(ctx, key, "value-"+key)
		require.NoError(t, err)
	}
	_, err = client.Put(ctx, "/a1664/namespace-nested-txn-serializable-prefix-countonly/tenant/items0/outside", "same-tenant-outside-prefix")
	require.NoError(t, err)
	latest, err := client.Put(ctx, "/a1664/namespace-nested-txn-serializable-prefix-countonly/tenant0/items/a", "outside-tenant")
	require.NoError(t, err)

	txnResp, err := namespacedKV.Txn(ctx).
		Then(clientv3.OpTxn(nil,
			[]clientv3.Op{clientv3.OpGet("items/",
				clientv3.WithPrefix(),
				clientv3.WithSerializable(),
				clientv3.WithKeysOnly(),
				clientv3.WithCountOnly(),
				clientv3.WithLimit(1),
			)},
			nil)).
		Commit()
	require.NoError(t, err)
	require.True(t, txnResp.Succeeded)
	require.Len(t, txnResp.Responses, 1)
	nestedTxn := txnResp.Responses[0].GetResponseTxn()
	require.NotNil(t, nestedTxn)
	require.True(t, nestedTxn.Succeeded)
	require.Len(t, nestedTxn.Responses, 1)
	nestedGet := nestedTxn.Responses[0].GetResponseRange()
	require.NotNil(t, nestedGet)
	require.NotNil(t, nestedGet.Header)
	require.Equal(t, latest.Header.Revision, nestedGet.Header.Revision)
	require.Equal(t, int64(3), nestedGet.Count)
	require.False(t, nestedGet.More)
	require.Empty(t, nestedGet.Kvs)

	current, err := namespacedKV.Get(ctx, "items/", clientv3.WithPrefix())
	require.NoError(t, err)
	require.Equal(t, int64(3), current.Count)
	outsideSameTenantPrefix, err := client.Get(ctx, "/a1664/namespace-nested-txn-serializable-prefix-countonly/tenant/items0/", clientv3.WithPrefix())
	require.NoError(t, err)
	require.Equal(t, int64(1), outsideSameTenantPrefix.Count)
	outsideTenant, err := client.Get(ctx, "/a1664/namespace-nested-txn-serializable-prefix-countonly/tenant0/", clientv3.WithPrefix())
	require.NoError(t, err)
	require.Equal(t, int64(1), outsideTenant.Count)
}

func TestClientNamespaceNestedTxnGetHistoricalSerializablePrefixReturnsLogicalKeys(t *testing.T) {
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
	tenantPrefix := "/a1665/namespace-nested-txn-historical-serializable-prefix/tenant/"
	namespacedKV := namespace.NewKV(client.KV, tenantPrefix)
	_, err = client.Put(ctx, "/a1665/namespace-nested-txn-historical-serializable-prefix/tenant/items0/outside", "same-tenant-outside-prefix")
	require.NoError(t, err)
	_, err = client.Put(ctx, "/a1665/namespace-nested-txn-historical-serializable-prefix/tenant0/items/a", "outside-tenant")
	require.NoError(t, err)
	_, err = namespacedKV.Put(ctx, "items/a", "old-a")
	require.NoError(t, err)
	_, err = namespacedKV.Put(ctx, "items/b", "old-b")
	require.NoError(t, err)
	historical, err := namespacedKV.Put(ctx, "items/c", "old-c")
	require.NoError(t, err)
	_, err = namespacedKV.Put(ctx, "items/a", "new-a")
	require.NoError(t, err)
	_, err = namespacedKV.Delete(ctx, "items/b")
	require.NoError(t, err)
	latest, err := namespacedKV.Put(ctx, "items/d", "new-d")
	require.NoError(t, err)

	txnResp, err := namespacedKV.Txn(ctx).
		Then(clientv3.OpTxn(nil,
			[]clientv3.Op{clientv3.OpGet("items/",
				clientv3.WithPrefix(),
				clientv3.WithRev(historical.Header.Revision),
				clientv3.WithSerializable(),
			)},
			nil)).
		Commit()
	require.NoError(t, err)
	require.True(t, txnResp.Succeeded)
	require.Equal(t, latest.Header.Revision, txnResp.Header.Revision)
	require.Len(t, txnResp.Responses, 1)
	nestedTxn := txnResp.Responses[0].GetResponseTxn()
	require.NotNil(t, nestedTxn)
	require.True(t, nestedTxn.Succeeded)
	require.Len(t, nestedTxn.Responses, 1)
	nestedGet := nestedTxn.Responses[0].GetResponseRange()
	require.NotNil(t, nestedGet)
	require.NotNil(t, nestedGet.Header)
	require.Equal(t, latest.Header.Revision, nestedGet.Header.Revision)
	require.Equal(t, int64(3), nestedGet.Count)
	require.False(t, nestedGet.More)
	require.Len(t, nestedGet.Kvs, 3)
	require.Equal(t, [][]byte{[]byte("items/a"), []byte("items/b"), []byte("items/c")},
		[][]byte{nestedGet.Kvs[0].Key, nestedGet.Kvs[1].Key, nestedGet.Kvs[2].Key})
	require.Equal(t, [][]byte{[]byte("old-a"), []byte("old-b"), []byte("old-c")},
		[][]byte{nestedGet.Kvs[0].Value, nestedGet.Kvs[1].Value, nestedGet.Kvs[2].Value})

	current, err := namespacedKV.Get(ctx, "items/", clientv3.WithPrefix())
	require.NoError(t, err)
	require.Equal(t, int64(3), current.Count)
	require.Equal(t, [][]byte{[]byte("items/a"), []byte("items/c"), []byte("items/d")},
		[][]byte{current.Kvs[0].Key, current.Kvs[1].Key, current.Kvs[2].Key})
	outsideSameTenantPrefix, err := client.Get(ctx, "/a1665/namespace-nested-txn-historical-serializable-prefix/tenant/items0/", clientv3.WithPrefix())
	require.NoError(t, err)
	require.Equal(t, int64(1), outsideSameTenantPrefix.Count)
	outsideTenant, err := client.Get(ctx, "/a1665/namespace-nested-txn-historical-serializable-prefix/tenant0/", clientv3.WithPrefix())
	require.NoError(t, err)
	require.Equal(t, int64(1), outsideTenant.Count)
}

func TestClientNamespaceNestedTxnGetHistoricalSerializablePrefixKeysOnlyLimitReturnsLogicalPage(t *testing.T) {
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
	tenantPrefix := "/a1666/namespace-nested-txn-historical-serializable-prefix-keysonly/tenant/"
	namespacedKV := namespace.NewKV(client.KV, tenantPrefix)
	_, err = client.Put(ctx, "/a1666/namespace-nested-txn-historical-serializable-prefix-keysonly/tenant/items0/outside", "same-tenant-outside-prefix")
	require.NoError(t, err)
	_, err = client.Put(ctx, "/a1666/namespace-nested-txn-historical-serializable-prefix-keysonly/tenant0/items/00", "outside-tenant")
	require.NoError(t, err)
	for _, key := range []string{"items/00", "items/01", "items/02"} {
		_, err = namespacedKV.Put(ctx, key, "value-"+key)
		require.NoError(t, err)
	}
	historical, err := namespacedKV.Put(ctx, "items/03", "value-items/03")
	require.NoError(t, err)
	_, err = namespacedKV.Put(ctx, "items/02", "updated-items/02")
	require.NoError(t, err)
	_, err = namespacedKV.Delete(ctx, "items/00")
	require.NoError(t, err)
	latest, err := namespacedKV.Put(ctx, "items/04", "value-items/04")
	require.NoError(t, err)

	txnResp, err := namespacedKV.Txn(ctx).
		Then(clientv3.OpTxn(nil,
			[]clientv3.Op{clientv3.OpGet("items/",
				clientv3.WithPrefix(),
				clientv3.WithRev(historical.Header.Revision),
				clientv3.WithSerializable(),
				clientv3.WithKeysOnly(),
				clientv3.WithLimit(2),
			)},
			nil)).
		Commit()
	require.NoError(t, err)
	require.True(t, txnResp.Succeeded)
	require.Equal(t, latest.Header.Revision, txnResp.Header.Revision)
	require.Len(t, txnResp.Responses, 1)
	nestedTxn := txnResp.Responses[0].GetResponseTxn()
	require.NotNil(t, nestedTxn)
	require.True(t, nestedTxn.Succeeded)
	require.Len(t, nestedTxn.Responses, 1)
	nestedGet := nestedTxn.Responses[0].GetResponseRange()
	require.NotNil(t, nestedGet)
	require.NotNil(t, nestedGet.Header)
	require.Equal(t, latest.Header.Revision, nestedGet.Header.Revision)
	require.Equal(t, int64(4), nestedGet.Count)
	require.True(t, nestedGet.More)
	require.Len(t, nestedGet.Kvs, 2)
	require.Equal(t, [][]byte{[]byte("items/00"), []byte("items/01")},
		[][]byte{nestedGet.Kvs[0].Key, nestedGet.Kvs[1].Key})
	require.Empty(t, nestedGet.Kvs[0].Value)
	require.Empty(t, nestedGet.Kvs[1].Value)

	current, err := namespacedKV.Get(ctx, "items/", clientv3.WithPrefix())
	require.NoError(t, err)
	require.Equal(t, int64(4), current.Count)
	require.Equal(t, [][]byte{[]byte("items/01"), []byte("items/02"), []byte("items/03"), []byte("items/04")},
		[][]byte{current.Kvs[0].Key, current.Kvs[1].Key, current.Kvs[2].Key, current.Kvs[3].Key})
	outsideSameTenantPrefix, err := client.Get(ctx, "/a1666/namespace-nested-txn-historical-serializable-prefix-keysonly/tenant/items0/", clientv3.WithPrefix())
	require.NoError(t, err)
	require.Equal(t, int64(1), outsideSameTenantPrefix.Count)
	outsideTenant, err := client.Get(ctx, "/a1666/namespace-nested-txn-historical-serializable-prefix-keysonly/tenant0/", clientv3.WithPrefix())
	require.NoError(t, err)
	require.Equal(t, int64(1), outsideTenant.Count)
}

func TestClientNamespaceNestedTxnGetHistoricalSerializablePrefixSortByValueWithLimitReturnsLogicalKeys(t *testing.T) {
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
	tenantPrefix := "/a1667/namespace-nested-txn-historical-serializable-prefix-sort-value/tenant/"
	namespacedKV := namespace.NewKV(client.KV, tenantPrefix)
	_, err = client.Put(ctx, "/a1667/namespace-nested-txn-historical-serializable-prefix-sort-value/tenant/items0/outside", "same-tenant-outside-prefix")
	require.NoError(t, err)
	_, err = client.Put(ctx, "/a1667/namespace-nested-txn-historical-serializable-prefix-sort-value/tenant0/items/a", "outside-tenant")
	require.NoError(t, err)
	for _, seed := range []struct {
		key   string
		value string
	}{
		{key: "items/d", value: "n"},
		{key: "items/c", value: "a"},
		{key: "items/b", value: "m"},
	} {
		_, err = namespacedKV.Put(ctx, seed.key, seed.value)
		require.NoError(t, err)
	}
	historical, err := namespacedKV.Put(ctx, "items/a", "z")
	require.NoError(t, err)
	_, err = namespacedKV.Put(ctx, "items/c", "zz-current")
	require.NoError(t, err)
	_, err = namespacedKV.Delete(ctx, "items/b")
	require.NoError(t, err)
	latest, err := namespacedKV.Put(ctx, "items/e", "aa-current")
	require.NoError(t, err)

	txnResp, err := namespacedKV.Txn(ctx).
		Then(clientv3.OpTxn(nil,
			[]clientv3.Op{clientv3.OpGet("items/",
				clientv3.WithPrefix(),
				clientv3.WithRev(historical.Header.Revision),
				clientv3.WithSerializable(),
				clientv3.WithLimit(2),
				clientv3.WithSort(clientv3.SortByValue, clientv3.SortNone),
			)},
			nil)).
		Commit()
	require.NoError(t, err)
	require.True(t, txnResp.Succeeded)
	require.Equal(t, latest.Header.Revision, txnResp.Header.Revision)
	require.Len(t, txnResp.Responses, 1)
	nestedTxn := txnResp.Responses[0].GetResponseTxn()
	require.NotNil(t, nestedTxn)
	require.True(t, nestedTxn.Succeeded)
	require.Len(t, nestedTxn.Responses, 1)
	nestedGet := nestedTxn.Responses[0].GetResponseRange()
	require.NotNil(t, nestedGet)
	require.NotNil(t, nestedGet.Header)
	require.Equal(t, latest.Header.Revision, nestedGet.Header.Revision)
	require.Equal(t, int64(4), nestedGet.Count)
	require.True(t, nestedGet.More)
	require.Len(t, nestedGet.Kvs, 2)
	require.Equal(t, [][]byte{[]byte("items/c"), []byte("items/b")},
		[][]byte{nestedGet.Kvs[0].Key, nestedGet.Kvs[1].Key})
	require.Equal(t, [][]byte{[]byte("a"), []byte("m")},
		[][]byte{nestedGet.Kvs[0].Value, nestedGet.Kvs[1].Value})

	current, err := namespacedKV.Get(ctx, "items/", clientv3.WithPrefix())
	require.NoError(t, err)
	require.Equal(t, int64(4), current.Count)
	require.Equal(t, [][]byte{[]byte("items/a"), []byte("items/c"), []byte("items/d"), []byte("items/e")},
		[][]byte{current.Kvs[0].Key, current.Kvs[1].Key, current.Kvs[2].Key, current.Kvs[3].Key})
	outsideSameTenantPrefix, err := client.Get(ctx, "/a1667/namespace-nested-txn-historical-serializable-prefix-sort-value/tenant/items0/", clientv3.WithPrefix())
	require.NoError(t, err)
	require.Equal(t, int64(1), outsideSameTenantPrefix.Count)
	outsideTenant, err := client.Get(ctx, "/a1667/namespace-nested-txn-historical-serializable-prefix-sort-value/tenant0/", clientv3.WithPrefix())
	require.NoError(t, err)
	require.Equal(t, int64(1), outsideTenant.Count)
}

func TestClientNamespaceNestedTxnGetHistoricalSerializablePrefixSortByValueDescReturnsLogicalPage(t *testing.T) {
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
	tenantPrefix := "/a1677/namespace-nested-txn-historical-serializable-prefix-sort-value-desc/tenant/"
	namespacedKV := namespace.NewKV(client.KV, tenantPrefix)
	_, err = client.Put(ctx, "/a1677/namespace-nested-txn-historical-serializable-prefix-sort-value-desc/tenant/items0/outside", "same-tenant-outside-prefix")
	require.NoError(t, err)
	_, err = client.Put(ctx, "/a1677/namespace-nested-txn-historical-serializable-prefix-sort-value-desc/tenant0/items/a", "outside-tenant")
	require.NoError(t, err)
	for _, seed := range []struct {
		key   string
		value string
	}{
		{key: "items/d", value: "n"},
		{key: "items/c", value: "a"},
		{key: "items/b", value: "m"},
	} {
		_, err = namespacedKV.Put(ctx, seed.key, seed.value)
		require.NoError(t, err)
	}
	historical, err := namespacedKV.Put(ctx, "items/a", "z")
	require.NoError(t, err)
	_, err = namespacedKV.Put(ctx, "items/c", "zz-current")
	require.NoError(t, err)
	_, err = namespacedKV.Delete(ctx, "items/b")
	require.NoError(t, err)
	latest, err := namespacedKV.Put(ctx, "items/e", "aa-current")
	require.NoError(t, err)

	txnResp, err := namespacedKV.Txn(ctx).
		Then(clientv3.OpTxn(nil,
			[]clientv3.Op{clientv3.OpGet("items/",
				clientv3.WithPrefix(),
				clientv3.WithRev(historical.Header.Revision),
				clientv3.WithSerializable(),
				clientv3.WithLimit(2),
				clientv3.WithSort(clientv3.SortByValue, clientv3.SortDescend),
			)},
			nil)).
		Commit()
	require.NoError(t, err)
	require.True(t, txnResp.Succeeded)
	require.Equal(t, latest.Header.Revision, txnResp.Header.Revision)
	require.Len(t, txnResp.Responses, 1)
	nestedTxn := txnResp.Responses[0].GetResponseTxn()
	require.NotNil(t, nestedTxn)
	require.True(t, nestedTxn.Succeeded)
	require.Len(t, nestedTxn.Responses, 1)
	nestedGet := nestedTxn.Responses[0].GetResponseRange()
	require.NotNil(t, nestedGet)
	require.NotNil(t, nestedGet.Header)
	require.Equal(t, latest.Header.Revision, nestedGet.Header.Revision)
	require.Equal(t, int64(4), nestedGet.Count)
	require.True(t, nestedGet.More)
	require.Len(t, nestedGet.Kvs, 2)
	require.Equal(t, [][]byte{[]byte("items/a"), []byte("items/d")},
		[][]byte{nestedGet.Kvs[0].Key, nestedGet.Kvs[1].Key})
	require.Equal(t, [][]byte{[]byte("z"), []byte("n")},
		[][]byte{nestedGet.Kvs[0].Value, nestedGet.Kvs[1].Value})

	current, err := namespacedKV.Get(ctx, "items/", clientv3.WithPrefix())
	require.NoError(t, err)
	require.Equal(t, int64(4), current.Count)
	require.Equal(t, [][]byte{[]byte("items/a"), []byte("items/c"), []byte("items/d"), []byte("items/e")},
		[][]byte{current.Kvs[0].Key, current.Kvs[1].Key, current.Kvs[2].Key, current.Kvs[3].Key})
	outsideSameTenantPrefix, err := client.Get(ctx, "/a1677/namespace-nested-txn-historical-serializable-prefix-sort-value-desc/tenant/items0/", clientv3.WithPrefix())
	require.NoError(t, err)
	require.Equal(t, int64(1), outsideSameTenantPrefix.Count)
	outsideTenant, err := client.Get(ctx, "/a1677/namespace-nested-txn-historical-serializable-prefix-sort-value-desc/tenant0/", clientv3.WithPrefix())
	require.NoError(t, err)
	require.Equal(t, int64(1), outsideTenant.Count)
}

func TestClientNamespaceNestedTxnGetHistoricalSerializablePrefixCountOnlyTakesPrecedence(t *testing.T) {
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
	tenantPrefix := "/a1668/namespace-nested-txn-historical-serializable-prefix-countonly/tenant/"
	namespacedKV := namespace.NewKV(client.KV, tenantPrefix)
	_, err = client.Put(ctx, "/a1668/namespace-nested-txn-historical-serializable-prefix-countonly/tenant/items0/outside", "same-tenant-outside-prefix")
	require.NoError(t, err)
	_, err = client.Put(ctx, "/a1668/namespace-nested-txn-historical-serializable-prefix-countonly/tenant0/items/a", "outside-tenant")
	require.NoError(t, err)
	_, err = namespacedKV.Put(ctx, "items/a", "old-a")
	require.NoError(t, err)
	_, err = namespacedKV.Put(ctx, "items/b", "old-b")
	require.NoError(t, err)
	historical, err := namespacedKV.Put(ctx, "items/c", "old-c")
	require.NoError(t, err)
	_, err = namespacedKV.Put(ctx, "items/a", "new-a")
	require.NoError(t, err)
	_, err = namespacedKV.Delete(ctx, "items/b")
	require.NoError(t, err)
	latest, err := namespacedKV.Put(ctx, "items/d", "new-d")
	require.NoError(t, err)

	txnResp, err := namespacedKV.Txn(ctx).
		Then(clientv3.OpTxn(nil,
			[]clientv3.Op{clientv3.OpGet("items/",
				clientv3.WithPrefix(),
				clientv3.WithRev(historical.Header.Revision),
				clientv3.WithSerializable(),
				clientv3.WithKeysOnly(),
				clientv3.WithCountOnly(),
				clientv3.WithLimit(1),
			)},
			nil)).
		Commit()
	require.NoError(t, err)
	require.True(t, txnResp.Succeeded)
	require.Equal(t, latest.Header.Revision, txnResp.Header.Revision)
	require.Len(t, txnResp.Responses, 1)
	nestedTxn := txnResp.Responses[0].GetResponseTxn()
	require.NotNil(t, nestedTxn)
	require.True(t, nestedTxn.Succeeded)
	require.Len(t, nestedTxn.Responses, 1)
	nestedGet := nestedTxn.Responses[0].GetResponseRange()
	require.NotNil(t, nestedGet)
	require.NotNil(t, nestedGet.Header)
	require.Equal(t, latest.Header.Revision, nestedGet.Header.Revision)
	require.Equal(t, int64(3), nestedGet.Count)
	require.False(t, nestedGet.More)
	require.Empty(t, nestedGet.Kvs)

	current, err := namespacedKV.Get(ctx, "items/", clientv3.WithPrefix())
	require.NoError(t, err)
	require.Equal(t, int64(3), current.Count)
	require.Equal(t, [][]byte{[]byte("items/a"), []byte("items/c"), []byte("items/d")},
		[][]byte{current.Kvs[0].Key, current.Kvs[1].Key, current.Kvs[2].Key})
	outsideSameTenantPrefix, err := client.Get(ctx, "/a1668/namespace-nested-txn-historical-serializable-prefix-countonly/tenant/items0/", clientv3.WithPrefix())
	require.NoError(t, err)
	require.Equal(t, int64(1), outsideSameTenantPrefix.Count)
	outsideTenant, err := client.Get(ctx, "/a1668/namespace-nested-txn-historical-serializable-prefix-countonly/tenant0/", clientv3.WithPrefix())
	require.NoError(t, err)
	require.Equal(t, int64(1), outsideTenant.Count)
}

func TestClientNamespaceNestedTxnGetHistoricalSerializablePrefixSortByKeyDescReturnsLogicalPage(t *testing.T) {
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
	tenantPrefix := "/a1669/namespace-nested-txn-historical-serializable-prefix-key-desc/tenant/"
	namespacedKV := namespace.NewKV(client.KV, tenantPrefix)
	_, err = client.Put(ctx, "/a1669/namespace-nested-txn-historical-serializable-prefix-key-desc/tenant/items0/outside", "same-tenant-outside-prefix")
	require.NoError(t, err)
	_, err = client.Put(ctx, "/a1669/namespace-nested-txn-historical-serializable-prefix-key-desc/tenant0/items/d", "outside-tenant")
	require.NoError(t, err)
	for _, key := range []string{"items/a", "items/b", "items/c"} {
		_, err = namespacedKV.Put(ctx, key, "value-"+key)
		require.NoError(t, err)
	}
	historical, err := namespacedKV.Put(ctx, "items/d", "value-items/d")
	require.NoError(t, err)
	_, err = namespacedKV.Delete(ctx, "items/d")
	require.NoError(t, err)
	latest, err := namespacedKV.Put(ctx, "items/e", "value-items/e")
	require.NoError(t, err)

	txnResp, err := namespacedKV.Txn(ctx).
		Then(clientv3.OpTxn(nil,
			[]clientv3.Op{clientv3.OpGet("items/",
				clientv3.WithPrefix(),
				clientv3.WithRev(historical.Header.Revision),
				clientv3.WithSerializable(),
				clientv3.WithLimit(2),
				clientv3.WithSort(clientv3.SortByKey, clientv3.SortDescend),
			)},
			nil)).
		Commit()
	require.NoError(t, err)
	require.True(t, txnResp.Succeeded)
	require.Equal(t, latest.Header.Revision, txnResp.Header.Revision)
	require.Len(t, txnResp.Responses, 1)
	nestedTxn := txnResp.Responses[0].GetResponseTxn()
	require.NotNil(t, nestedTxn)
	require.True(t, nestedTxn.Succeeded)
	require.Len(t, nestedTxn.Responses, 1)
	nestedGet := nestedTxn.Responses[0].GetResponseRange()
	require.NotNil(t, nestedGet)
	require.NotNil(t, nestedGet.Header)
	require.Equal(t, latest.Header.Revision, nestedGet.Header.Revision)
	require.Equal(t, int64(4), nestedGet.Count)
	require.True(t, nestedGet.More)
	require.Len(t, nestedGet.Kvs, 2)
	require.Equal(t, [][]byte{[]byte("items/d"), []byte("items/c")},
		[][]byte{nestedGet.Kvs[0].Key, nestedGet.Kvs[1].Key})
	require.Equal(t, [][]byte{[]byte("value-items/d"), []byte("value-items/c")},
		[][]byte{nestedGet.Kvs[0].Value, nestedGet.Kvs[1].Value})

	current, err := namespacedKV.Get(ctx, "items/", clientv3.WithPrefix())
	require.NoError(t, err)
	require.Equal(t, int64(4), current.Count)
	require.Equal(t, [][]byte{[]byte("items/a"), []byte("items/b"), []byte("items/c"), []byte("items/e")},
		[][]byte{current.Kvs[0].Key, current.Kvs[1].Key, current.Kvs[2].Key, current.Kvs[3].Key})
	outsideSameTenantPrefix, err := client.Get(ctx, "/a1669/namespace-nested-txn-historical-serializable-prefix-key-desc/tenant/items0/", clientv3.WithPrefix())
	require.NoError(t, err)
	require.Equal(t, int64(1), outsideSameTenantPrefix.Count)
	outsideTenant, err := client.Get(ctx, "/a1669/namespace-nested-txn-historical-serializable-prefix-key-desc/tenant0/", clientv3.WithPrefix())
	require.NoError(t, err)
	require.Equal(t, int64(1), outsideTenant.Count)
}

func TestClientNamespaceNestedTxnGetHistoricalSerializablePrefixSortByKeyAscReturnsLogicalPage(t *testing.T) {
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
	tenantPrefix := "/a1670/namespace-nested-txn-historical-serializable-prefix-key-asc/tenant/"
	namespacedKV := namespace.NewKV(client.KV, tenantPrefix)
	_, err = client.Put(ctx, "/a1670/namespace-nested-txn-historical-serializable-prefix-key-asc/tenant/items0/outside", "same-tenant-outside-prefix")
	require.NoError(t, err)
	_, err = client.Put(ctx, "/a1670/namespace-nested-txn-historical-serializable-prefix-key-asc/tenant0/items/a", "outside-tenant")
	require.NoError(t, err)
	for _, key := range []string{"items/a", "items/b", "items/c"} {
		_, err = namespacedKV.Put(ctx, key, "value-"+key)
		require.NoError(t, err)
	}
	historical, err := namespacedKV.Put(ctx, "items/d", "value-items/d")
	require.NoError(t, err)
	_, err = namespacedKV.Delete(ctx, "items/a")
	require.NoError(t, err)
	latest, err := namespacedKV.Put(ctx, "items/0", "value-items/0")
	require.NoError(t, err)

	txnResp, err := namespacedKV.Txn(ctx).
		Then(clientv3.OpTxn(nil,
			[]clientv3.Op{clientv3.OpGet("items/",
				clientv3.WithPrefix(),
				clientv3.WithRev(historical.Header.Revision),
				clientv3.WithSerializable(),
				clientv3.WithLimit(2),
				clientv3.WithSort(clientv3.SortByKey, clientv3.SortAscend),
			)},
			nil)).
		Commit()
	require.NoError(t, err)
	require.True(t, txnResp.Succeeded)
	require.Equal(t, latest.Header.Revision, txnResp.Header.Revision)
	require.Len(t, txnResp.Responses, 1)
	nestedTxn := txnResp.Responses[0].GetResponseTxn()
	require.NotNil(t, nestedTxn)
	require.True(t, nestedTxn.Succeeded)
	require.Len(t, nestedTxn.Responses, 1)
	nestedGet := nestedTxn.Responses[0].GetResponseRange()
	require.NotNil(t, nestedGet)
	require.NotNil(t, nestedGet.Header)
	require.Equal(t, latest.Header.Revision, nestedGet.Header.Revision)
	require.Equal(t, int64(4), nestedGet.Count)
	require.True(t, nestedGet.More)
	require.Len(t, nestedGet.Kvs, 2)
	require.Equal(t, [][]byte{[]byte("items/a"), []byte("items/b")},
		[][]byte{nestedGet.Kvs[0].Key, nestedGet.Kvs[1].Key})
	require.Equal(t, [][]byte{[]byte("value-items/a"), []byte("value-items/b")},
		[][]byte{nestedGet.Kvs[0].Value, nestedGet.Kvs[1].Value})

	current, err := namespacedKV.Get(ctx, "items/", clientv3.WithPrefix())
	require.NoError(t, err)
	require.Equal(t, int64(4), current.Count)
	require.Equal(t, [][]byte{[]byte("items/0"), []byte("items/b"), []byte("items/c"), []byte("items/d")},
		[][]byte{current.Kvs[0].Key, current.Kvs[1].Key, current.Kvs[2].Key, current.Kvs[3].Key})
	outsideSameTenantPrefix, err := client.Get(ctx, "/a1670/namespace-nested-txn-historical-serializable-prefix-key-asc/tenant/items0/", clientv3.WithPrefix())
	require.NoError(t, err)
	require.Equal(t, int64(1), outsideSameTenantPrefix.Count)
	outsideTenant, err := client.Get(ctx, "/a1670/namespace-nested-txn-historical-serializable-prefix-key-asc/tenant0/", clientv3.WithPrefix())
	require.NoError(t, err)
	require.Equal(t, int64(1), outsideTenant.Count)
}

func TestClientNamespaceNestedTxnGetHistoricalSerializablePrefixSortByKeyReturnsLogicalPage(t *testing.T) {
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
	tenantPrefix := "/a1678/namespace-nested-txn-historical-serializable-prefix-key-sort-none/tenant/"
	namespacedKV := namespace.NewKV(client.KV, tenantPrefix)
	_, err = client.Put(ctx, "/a1678/namespace-nested-txn-historical-serializable-prefix-key-sort-none/tenant/items0/outside", "same-tenant-outside-prefix")
	require.NoError(t, err)
	_, err = client.Put(ctx, "/a1678/namespace-nested-txn-historical-serializable-prefix-key-sort-none/tenant0/items/a", "outside-tenant")
	require.NoError(t, err)
	for _, key := range []string{"items/b", "items/a", "items/c"} {
		_, err = namespacedKV.Put(ctx, key, "value-"+key)
		require.NoError(t, err)
	}
	historical, err := namespacedKV.Put(ctx, "items/d", "value-items/d")
	require.NoError(t, err)
	_, err = namespacedKV.Delete(ctx, "items/a")
	require.NoError(t, err)
	latest, err := namespacedKV.Put(ctx, "items/0", "value-items/0")
	require.NoError(t, err)

	txnResp, err := namespacedKV.Txn(ctx).
		Then(clientv3.OpTxn(nil,
			[]clientv3.Op{clientv3.OpGet("items/",
				clientv3.WithPrefix(),
				clientv3.WithRev(historical.Header.Revision),
				clientv3.WithSerializable(),
				clientv3.WithLimit(2),
				clientv3.WithSort(clientv3.SortByKey, clientv3.SortNone),
			)},
			nil)).
		Commit()
	require.NoError(t, err)
	require.True(t, txnResp.Succeeded)
	require.Equal(t, latest.Header.Revision, txnResp.Header.Revision)
	require.Len(t, txnResp.Responses, 1)
	nestedTxn := txnResp.Responses[0].GetResponseTxn()
	require.NotNil(t, nestedTxn)
	require.True(t, nestedTxn.Succeeded)
	require.Len(t, nestedTxn.Responses, 1)
	nestedGet := nestedTxn.Responses[0].GetResponseRange()
	require.NotNil(t, nestedGet)
	require.NotNil(t, nestedGet.Header)
	require.Equal(t, latest.Header.Revision, nestedGet.Header.Revision)
	require.Equal(t, int64(4), nestedGet.Count)
	require.True(t, nestedGet.More)
	require.Len(t, nestedGet.Kvs, 2)
	require.Equal(t, [][]byte{[]byte("items/a"), []byte("items/b")},
		[][]byte{nestedGet.Kvs[0].Key, nestedGet.Kvs[1].Key})
	require.Equal(t, [][]byte{[]byte("value-items/a"), []byte("value-items/b")},
		[][]byte{nestedGet.Kvs[0].Value, nestedGet.Kvs[1].Value})

	current, err := namespacedKV.Get(ctx, "items/", clientv3.WithPrefix())
	require.NoError(t, err)
	require.Equal(t, int64(4), current.Count)
	require.Equal(t, [][]byte{[]byte("items/0"), []byte("items/b"), []byte("items/c"), []byte("items/d")},
		[][]byte{current.Kvs[0].Key, current.Kvs[1].Key, current.Kvs[2].Key, current.Kvs[3].Key})
	outsideSameTenantPrefix, err := client.Get(ctx, "/a1678/namespace-nested-txn-historical-serializable-prefix-key-sort-none/tenant/items0/", clientv3.WithPrefix())
	require.NoError(t, err)
	require.Equal(t, int64(1), outsideSameTenantPrefix.Count)
	outsideTenant, err := client.Get(ctx, "/a1678/namespace-nested-txn-historical-serializable-prefix-key-sort-none/tenant0/", clientv3.WithPrefix())
	require.NoError(t, err)
	require.Equal(t, int64(1), outsideTenant.Count)
}

func TestClientNamespaceNestedTxnGetHistoricalSerializablePrefixMinModRevisionReturnsLogicalKeys(t *testing.T) {
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
	tenantPrefix := "/a1679/namespace-nested-txn-historical-serializable-prefix-min-mod-rev/tenant/"
	namespacedKV := namespace.NewKV(client.KV, tenantPrefix)
	_, err = client.Put(ctx, "/a1679/namespace-nested-txn-historical-serializable-prefix-min-mod-rev/tenant/items0/outside", "same-tenant-outside-prefix")
	require.NoError(t, err)
	_, err = client.Put(ctx, "/a1679/namespace-nested-txn-historical-serializable-prefix-min-mod-rev/tenant0/items/b", "outside-tenant")
	require.NoError(t, err)
	_, err = namespacedKV.Put(ctx, "items/a", "old-items/a")
	require.NoError(t, err)
	_, err = namespacedKV.Put(ctx, "items/c", "old-items/c")
	require.NoError(t, err)
	historical, err := namespacedKV.Put(ctx, "items/b", "old-items/b")
	require.NoError(t, err)
	_, err = namespacedKV.Delete(ctx, "items/b")
	require.NoError(t, err)
	_, err = namespacedKV.Put(ctx, "items/a", "new-items/a")
	require.NoError(t, err)
	latest, err := namespacedKV.Put(ctx, "items/d", "new-items/d")
	require.NoError(t, err)

	txnResp, err := namespacedKV.Txn(ctx).
		Then(clientv3.OpTxn(nil,
			[]clientv3.Op{clientv3.OpGet("items/",
				clientv3.WithPrefix(),
				clientv3.WithRev(historical.Header.Revision),
				clientv3.WithSerializable(),
				clientv3.WithMinModRev(historical.Header.Revision),
				clientv3.WithSort(clientv3.SortByKey, clientv3.SortAscend),
			)},
			nil)).
		Commit()
	require.NoError(t, err)
	require.True(t, txnResp.Succeeded)
	require.Equal(t, latest.Header.Revision, txnResp.Header.Revision)
	require.Len(t, txnResp.Responses, 1)
	nestedTxn := txnResp.Responses[0].GetResponseTxn()
	require.NotNil(t, nestedTxn)
	require.True(t, nestedTxn.Succeeded)
	require.Len(t, nestedTxn.Responses, 1)
	nestedGet := nestedTxn.Responses[0].GetResponseRange()
	require.NotNil(t, nestedGet)
	require.NotNil(t, nestedGet.Header)
	require.Equal(t, latest.Header.Revision, nestedGet.Header.Revision)
	require.Equal(t, int64(3), nestedGet.Count)
	require.False(t, nestedGet.More)
	require.Len(t, nestedGet.Kvs, 1)
	require.Equal(t, []byte("items/b"), nestedGet.Kvs[0].Key)
	require.Equal(t, []byte("old-items/b"), nestedGet.Kvs[0].Value)
	require.Equal(t, historical.Header.Revision, nestedGet.Kvs[0].ModRevision)

	current, err := namespacedKV.Get(ctx, "items/", clientv3.WithPrefix())
	require.NoError(t, err)
	require.Equal(t, int64(3), current.Count)
	require.Equal(t, [][]byte{[]byte("items/a"), []byte("items/c"), []byte("items/d")},
		[][]byte{current.Kvs[0].Key, current.Kvs[1].Key, current.Kvs[2].Key})
	outsideSameTenantPrefix, err := client.Get(ctx, "/a1679/namespace-nested-txn-historical-serializable-prefix-min-mod-rev/tenant/items0/", clientv3.WithPrefix())
	require.NoError(t, err)
	require.Equal(t, int64(1), outsideSameTenantPrefix.Count)
	outsideTenant, err := client.Get(ctx, "/a1679/namespace-nested-txn-historical-serializable-prefix-min-mod-rev/tenant0/", clientv3.WithPrefix())
	require.NoError(t, err)
	require.Equal(t, int64(1), outsideTenant.Count)
}

func TestClientNamespaceNestedTxnGetHistoricalSerializablePrefixMaxModRevisionReturnsLogicalKeys(t *testing.T) {
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
	tenantPrefix := "/a1680/namespace-nested-txn-historical-serializable-prefix-max-mod-rev/tenant/"
	namespacedKV := namespace.NewKV(client.KV, tenantPrefix)
	_, err = client.Put(ctx, "/a1680/namespace-nested-txn-historical-serializable-prefix-max-mod-rev/tenant/items0/outside", "same-tenant-outside-prefix")
	require.NoError(t, err)
	_, err = client.Put(ctx, "/a1680/namespace-nested-txn-historical-serializable-prefix-max-mod-rev/tenant0/items/a", "outside-tenant")
	require.NoError(t, err)
	_, err = namespacedKV.Put(ctx, "items/a", "old-items/a")
	require.NoError(t, err)
	_, err = namespacedKV.Put(ctx, "items/c", "old-items/c")
	require.NoError(t, err)
	historical, err := namespacedKV.Put(ctx, "items/b", "old-items/b")
	require.NoError(t, err)
	_, err = namespacedKV.Delete(ctx, "items/b")
	require.NoError(t, err)
	_, err = namespacedKV.Put(ctx, "items/a", "new-items/a")
	require.NoError(t, err)
	latest, err := namespacedKV.Put(ctx, "items/d", "new-items/d")
	require.NoError(t, err)

	txnResp, err := namespacedKV.Txn(ctx).
		Then(clientv3.OpTxn(nil,
			[]clientv3.Op{clientv3.OpGet("items/",
				clientv3.WithPrefix(),
				clientv3.WithRev(historical.Header.Revision),
				clientv3.WithSerializable(),
				clientv3.WithMaxModRev(historical.Header.Revision-1),
				clientv3.WithSort(clientv3.SortByKey, clientv3.SortAscend),
			)},
			nil)).
		Commit()
	require.NoError(t, err)
	require.True(t, txnResp.Succeeded)
	require.Equal(t, latest.Header.Revision, txnResp.Header.Revision)
	require.Len(t, txnResp.Responses, 1)
	nestedTxn := txnResp.Responses[0].GetResponseTxn()
	require.NotNil(t, nestedTxn)
	require.True(t, nestedTxn.Succeeded)
	require.Len(t, nestedTxn.Responses, 1)
	nestedGet := nestedTxn.Responses[0].GetResponseRange()
	require.NotNil(t, nestedGet)
	require.NotNil(t, nestedGet.Header)
	require.Equal(t, latest.Header.Revision, nestedGet.Header.Revision)
	require.Equal(t, int64(3), nestedGet.Count)
	require.False(t, nestedGet.More)
	require.Len(t, nestedGet.Kvs, 2)
	require.Equal(t, [][]byte{[]byte("items/a"), []byte("items/c")},
		[][]byte{nestedGet.Kvs[0].Key, nestedGet.Kvs[1].Key})
	require.Equal(t, [][]byte{[]byte("old-items/a"), []byte("old-items/c")},
		[][]byte{nestedGet.Kvs[0].Value, nestedGet.Kvs[1].Value})

	current, err := namespacedKV.Get(ctx, "items/", clientv3.WithPrefix())
	require.NoError(t, err)
	require.Equal(t, int64(3), current.Count)
	require.Equal(t, [][]byte{[]byte("items/a"), []byte("items/c"), []byte("items/d")},
		[][]byte{current.Kvs[0].Key, current.Kvs[1].Key, current.Kvs[2].Key})
	outsideSameTenantPrefix, err := client.Get(ctx, "/a1680/namespace-nested-txn-historical-serializable-prefix-max-mod-rev/tenant/items0/", clientv3.WithPrefix())
	require.NoError(t, err)
	require.Equal(t, int64(1), outsideSameTenantPrefix.Count)
	outsideTenant, err := client.Get(ctx, "/a1680/namespace-nested-txn-historical-serializable-prefix-max-mod-rev/tenant0/", clientv3.WithPrefix())
	require.NoError(t, err)
	require.Equal(t, int64(1), outsideTenant.Count)
}

func TestClientNamespaceNestedTxnGetHistoricalSerializablePrefixMinCreateRevisionReturnsLogicalKeys(t *testing.T) {
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
	tenantPrefix := "/a1681/namespace-nested-txn-historical-serializable-prefix-min-create-rev/tenant/"
	namespacedKV := namespace.NewKV(client.KV, tenantPrefix)
	_, err = client.Put(ctx, "/a1681/namespace-nested-txn-historical-serializable-prefix-min-create-rev/tenant/items0/outside", "same-tenant-outside-prefix")
	require.NoError(t, err)
	_, err = client.Put(ctx, "/a1681/namespace-nested-txn-historical-serializable-prefix-min-create-rev/tenant0/items/b", "outside-tenant")
	require.NoError(t, err)
	_, err = namespacedKV.Put(ctx, "items/a", "old-items/a")
	require.NoError(t, err)
	_, err = namespacedKV.Put(ctx, "items/c", "old-items/c")
	require.NoError(t, err)
	historical, err := namespacedKV.Put(ctx, "items/b", "old-items/b")
	require.NoError(t, err)
	_, err = namespacedKV.Delete(ctx, "items/b")
	require.NoError(t, err)
	_, err = namespacedKV.Put(ctx, "items/c", "new-items/c")
	require.NoError(t, err)
	latest, err := namespacedKV.Put(ctx, "items/d", "new-items/d")
	require.NoError(t, err)

	txnResp, err := namespacedKV.Txn(ctx).
		Then(clientv3.OpTxn(nil,
			[]clientv3.Op{clientv3.OpGet("items/",
				clientv3.WithPrefix(),
				clientv3.WithRev(historical.Header.Revision),
				clientv3.WithSerializable(),
				clientv3.WithMinCreateRev(historical.Header.Revision),
				clientv3.WithSort(clientv3.SortByKey, clientv3.SortAscend),
			)},
			nil)).
		Commit()
	require.NoError(t, err)
	require.True(t, txnResp.Succeeded)
	require.Equal(t, latest.Header.Revision, txnResp.Header.Revision)
	require.Len(t, txnResp.Responses, 1)
	nestedTxn := txnResp.Responses[0].GetResponseTxn()
	require.NotNil(t, nestedTxn)
	require.True(t, nestedTxn.Succeeded)
	require.Len(t, nestedTxn.Responses, 1)
	nestedGet := nestedTxn.Responses[0].GetResponseRange()
	require.NotNil(t, nestedGet)
	require.NotNil(t, nestedGet.Header)
	require.Equal(t, latest.Header.Revision, nestedGet.Header.Revision)
	require.Equal(t, int64(3), nestedGet.Count)
	require.False(t, nestedGet.More)
	require.Len(t, nestedGet.Kvs, 1)
	require.Equal(t, []byte("items/b"), nestedGet.Kvs[0].Key)
	require.Equal(t, []byte("old-items/b"), nestedGet.Kvs[0].Value)
	require.Equal(t, historical.Header.Revision, nestedGet.Kvs[0].CreateRevision)

	current, err := namespacedKV.Get(ctx, "items/", clientv3.WithPrefix())
	require.NoError(t, err)
	require.Equal(t, int64(3), current.Count)
	require.Equal(t, [][]byte{[]byte("items/a"), []byte("items/c"), []byte("items/d")},
		[][]byte{current.Kvs[0].Key, current.Kvs[1].Key, current.Kvs[2].Key})
	outsideSameTenantPrefix, err := client.Get(ctx, "/a1681/namespace-nested-txn-historical-serializable-prefix-min-create-rev/tenant/items0/", clientv3.WithPrefix())
	require.NoError(t, err)
	require.Equal(t, int64(1), outsideSameTenantPrefix.Count)
	outsideTenant, err := client.Get(ctx, "/a1681/namespace-nested-txn-historical-serializable-prefix-min-create-rev/tenant0/", clientv3.WithPrefix())
	require.NoError(t, err)
	require.Equal(t, int64(1), outsideTenant.Count)
}

func TestClientNamespaceNestedTxnGetHistoricalSerializablePrefixMaxCreateRevisionReturnsLogicalKeys(t *testing.T) {
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
	tenantPrefix := "/a1682/namespace-nested-txn-historical-serializable-prefix-max-create-rev/tenant/"
	namespacedKV := namespace.NewKV(client.KV, tenantPrefix)
	_, err = client.Put(ctx, "/a1682/namespace-nested-txn-historical-serializable-prefix-max-create-rev/tenant/items0/outside", "same-tenant-outside-prefix")
	require.NoError(t, err)
	_, err = client.Put(ctx, "/a1682/namespace-nested-txn-historical-serializable-prefix-max-create-rev/tenant0/items/a", "outside-tenant")
	require.NoError(t, err)
	_, err = namespacedKV.Put(ctx, "items/a", "old-items/a")
	require.NoError(t, err)
	_, err = namespacedKV.Put(ctx, "items/c", "old-items/c")
	require.NoError(t, err)
	historical, err := namespacedKV.Put(ctx, "items/b", "old-items/b")
	require.NoError(t, err)
	_, err = namespacedKV.Delete(ctx, "items/b")
	require.NoError(t, err)
	_, err = namespacedKV.Put(ctx, "items/c", "new-items/c")
	require.NoError(t, err)
	latest, err := namespacedKV.Put(ctx, "items/d", "new-items/d")
	require.NoError(t, err)

	txnResp, err := namespacedKV.Txn(ctx).
		Then(clientv3.OpTxn(nil,
			[]clientv3.Op{clientv3.OpGet("items/",
				clientv3.WithPrefix(),
				clientv3.WithRev(historical.Header.Revision),
				clientv3.WithSerializable(),
				clientv3.WithMaxCreateRev(historical.Header.Revision-1),
				clientv3.WithSort(clientv3.SortByKey, clientv3.SortAscend),
			)},
			nil)).
		Commit()
	require.NoError(t, err)
	require.True(t, txnResp.Succeeded)
	require.Equal(t, latest.Header.Revision, txnResp.Header.Revision)
	require.Len(t, txnResp.Responses, 1)
	nestedTxn := txnResp.Responses[0].GetResponseTxn()
	require.NotNil(t, nestedTxn)
	require.True(t, nestedTxn.Succeeded)
	require.Len(t, nestedTxn.Responses, 1)
	nestedGet := nestedTxn.Responses[0].GetResponseRange()
	require.NotNil(t, nestedGet)
	require.NotNil(t, nestedGet.Header)
	require.Equal(t, latest.Header.Revision, nestedGet.Header.Revision)
	require.Equal(t, int64(3), nestedGet.Count)
	require.False(t, nestedGet.More)
	require.Len(t, nestedGet.Kvs, 2)
	require.Equal(t, [][]byte{[]byte("items/a"), []byte("items/c")},
		[][]byte{nestedGet.Kvs[0].Key, nestedGet.Kvs[1].Key})
	require.Equal(t, [][]byte{[]byte("old-items/a"), []byte("old-items/c")},
		[][]byte{nestedGet.Kvs[0].Value, nestedGet.Kvs[1].Value})

	current, err := namespacedKV.Get(ctx, "items/", clientv3.WithPrefix())
	require.NoError(t, err)
	require.Equal(t, int64(3), current.Count)
	require.Equal(t, [][]byte{[]byte("items/a"), []byte("items/c"), []byte("items/d")},
		[][]byte{current.Kvs[0].Key, current.Kvs[1].Key, current.Kvs[2].Key})
	outsideSameTenantPrefix, err := client.Get(ctx, "/a1682/namespace-nested-txn-historical-serializable-prefix-max-create-rev/tenant/items0/", clientv3.WithPrefix())
	require.NoError(t, err)
	require.Equal(t, int64(1), outsideSameTenantPrefix.Count)
	outsideTenant, err := client.Get(ctx, "/a1682/namespace-nested-txn-historical-serializable-prefix-max-create-rev/tenant0/", clientv3.WithPrefix())
	require.NoError(t, err)
	require.Equal(t, int64(1), outsideTenant.Count)
}

func TestClientNamespaceNestedTxnGetHistoricalSerializablePrefixContradictoryModRevisionFiltersReturnCountOnly(t *testing.T) {
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
	tenantPrefix := "/a1683/namespace-nested-txn-historical-serializable-prefix-contradictory-mod-rev/tenant/"
	namespacedKV := namespace.NewKV(client.KV, tenantPrefix)
	_, err = client.Put(ctx, "/a1683/namespace-nested-txn-historical-serializable-prefix-contradictory-mod-rev/tenant/items0/outside", "same-tenant-outside-prefix")
	require.NoError(t, err)
	_, err = client.Put(ctx, "/a1683/namespace-nested-txn-historical-serializable-prefix-contradictory-mod-rev/tenant0/items/a", "outside-tenant")
	require.NoError(t, err)
	_, err = namespacedKV.Put(ctx, "items/a", "old-items/a")
	require.NoError(t, err)
	_, err = namespacedKV.Put(ctx, "items/c", "old-items/c")
	require.NoError(t, err)
	historical, err := namespacedKV.Put(ctx, "items/b", "old-items/b")
	require.NoError(t, err)
	_, err = namespacedKV.Delete(ctx, "items/b")
	require.NoError(t, err)
	_, err = namespacedKV.Put(ctx, "items/a", "new-items/a")
	require.NoError(t, err)
	latest, err := namespacedKV.Put(ctx, "items/d", "new-items/d")
	require.NoError(t, err)

	txnResp, err := namespacedKV.Txn(ctx).
		Then(clientv3.OpTxn(nil,
			[]clientv3.Op{clientv3.OpGet("items/",
				clientv3.WithPrefix(),
				clientv3.WithRev(historical.Header.Revision),
				clientv3.WithSerializable(),
				clientv3.WithMinModRev(historical.Header.Revision+1),
				clientv3.WithMaxModRev(historical.Header.Revision),
				clientv3.WithLimit(1),
			)},
			nil)).
		Commit()
	require.NoError(t, err)
	require.True(t, txnResp.Succeeded)
	require.Equal(t, latest.Header.Revision, txnResp.Header.Revision)
	require.Len(t, txnResp.Responses, 1)
	nestedTxn := txnResp.Responses[0].GetResponseTxn()
	require.NotNil(t, nestedTxn)
	require.True(t, nestedTxn.Succeeded)
	require.Len(t, nestedTxn.Responses, 1)
	nestedGet := nestedTxn.Responses[0].GetResponseRange()
	require.NotNil(t, nestedGet)
	require.NotNil(t, nestedGet.Header)
	require.Equal(t, latest.Header.Revision, nestedGet.Header.Revision)
	require.Equal(t, int64(3), nestedGet.Count)
	require.False(t, nestedGet.More)
	require.Empty(t, nestedGet.Kvs)

	current, err := namespacedKV.Get(ctx, "items/", clientv3.WithPrefix())
	require.NoError(t, err)
	require.Equal(t, int64(3), current.Count)
	require.Equal(t, [][]byte{[]byte("items/a"), []byte("items/c"), []byte("items/d")},
		[][]byte{current.Kvs[0].Key, current.Kvs[1].Key, current.Kvs[2].Key})
	outsideSameTenantPrefix, err := client.Get(ctx, "/a1683/namespace-nested-txn-historical-serializable-prefix-contradictory-mod-rev/tenant/items0/", clientv3.WithPrefix())
	require.NoError(t, err)
	require.Equal(t, int64(1), outsideSameTenantPrefix.Count)
	outsideTenant, err := client.Get(ctx, "/a1683/namespace-nested-txn-historical-serializable-prefix-contradictory-mod-rev/tenant0/", clientv3.WithPrefix())
	require.NoError(t, err)
	require.Equal(t, int64(1), outsideTenant.Count)
}

func TestClientNamespaceNestedTxnGetHistoricalSerializablePrefixContradictoryCreateRevisionFiltersReturnCountOnly(t *testing.T) {
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
	tenantPrefix := "/a1684/namespace-nested-txn-historical-serializable-prefix-contradictory-create-rev/tenant/"
	namespacedKV := namespace.NewKV(client.KV, tenantPrefix)
	_, err = client.Put(ctx, "/a1684/namespace-nested-txn-historical-serializable-prefix-contradictory-create-rev/tenant/items0/outside", "same-tenant-outside-prefix")
	require.NoError(t, err)
	_, err = client.Put(ctx, "/a1684/namespace-nested-txn-historical-serializable-prefix-contradictory-create-rev/tenant0/items/a", "outside-tenant")
	require.NoError(t, err)
	_, err = namespacedKV.Put(ctx, "items/a", "old-items/a")
	require.NoError(t, err)
	_, err = namespacedKV.Put(ctx, "items/c", "old-items/c")
	require.NoError(t, err)
	historical, err := namespacedKV.Put(ctx, "items/b", "old-items/b")
	require.NoError(t, err)
	_, err = namespacedKV.Delete(ctx, "items/b")
	require.NoError(t, err)
	_, err = namespacedKV.Put(ctx, "items/c", "new-items/c")
	require.NoError(t, err)
	latest, err := namespacedKV.Put(ctx, "items/d", "new-items/d")
	require.NoError(t, err)

	txnResp, err := namespacedKV.Txn(ctx).
		Then(clientv3.OpTxn(nil,
			[]clientv3.Op{clientv3.OpGet("items/",
				clientv3.WithPrefix(),
				clientv3.WithRev(historical.Header.Revision),
				clientv3.WithSerializable(),
				clientv3.WithMinCreateRev(historical.Header.Revision+1),
				clientv3.WithMaxCreateRev(historical.Header.Revision),
				clientv3.WithLimit(1),
			)},
			nil)).
		Commit()
	require.NoError(t, err)
	require.True(t, txnResp.Succeeded)
	require.Equal(t, latest.Header.Revision, txnResp.Header.Revision)
	require.Len(t, txnResp.Responses, 1)
	nestedTxn := txnResp.Responses[0].GetResponseTxn()
	require.NotNil(t, nestedTxn)
	require.True(t, nestedTxn.Succeeded)
	require.Len(t, nestedTxn.Responses, 1)
	nestedGet := nestedTxn.Responses[0].GetResponseRange()
	require.NotNil(t, nestedGet)
	require.NotNil(t, nestedGet.Header)
	require.Equal(t, latest.Header.Revision, nestedGet.Header.Revision)
	require.Equal(t, int64(3), nestedGet.Count)
	require.False(t, nestedGet.More)
	require.Empty(t, nestedGet.Kvs)

	current, err := namespacedKV.Get(ctx, "items/", clientv3.WithPrefix())
	require.NoError(t, err)
	require.Equal(t, int64(3), current.Count)
	require.Equal(t, [][]byte{[]byte("items/a"), []byte("items/c"), []byte("items/d")},
		[][]byte{current.Kvs[0].Key, current.Kvs[1].Key, current.Kvs[2].Key})
	outsideSameTenantPrefix, err := client.Get(ctx, "/a1684/namespace-nested-txn-historical-serializable-prefix-contradictory-create-rev/tenant/items0/", clientv3.WithPrefix())
	require.NoError(t, err)
	require.Equal(t, int64(1), outsideSameTenantPrefix.Count)
	outsideTenant, err := client.Get(ctx, "/a1684/namespace-nested-txn-historical-serializable-prefix-contradictory-create-rev/tenant0/", clientv3.WithPrefix())
	require.NoError(t, err)
	require.Equal(t, int64(1), outsideTenant.Count)
}

func TestClientNamespaceNestedTxnGetHistoricalSerializablePrefixWithFirstCreateReturnsLogicalKey(t *testing.T) {
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
	tenantPrefix := "/a1685/namespace-nested-txn-historical-serializable-prefix-first-create/tenant/"
	namespacedKV := namespace.NewKV(client.KV, tenantPrefix)
	_, err = client.Put(ctx, "/a1685/namespace-nested-txn-historical-serializable-prefix-first-create/tenant/items0/outside", "same-tenant-outside-prefix")
	require.NoError(t, err)
	_, err = client.Put(ctx, "/a1685/namespace-nested-txn-historical-serializable-prefix-first-create/tenant0/items/older", "outside-tenant")
	require.NoError(t, err)
	_, err = namespacedKV.Put(ctx, "items/b", "old-items/b")
	require.NoError(t, err)
	_, err = namespacedKV.Put(ctx, "items/a", "old-items/a")
	require.NoError(t, err)
	historical, err := namespacedKV.Put(ctx, "items/c", "old-items/c")
	require.NoError(t, err)
	_, err = namespacedKV.Delete(ctx, "items/b")
	require.NoError(t, err)
	_, err = namespacedKV.Put(ctx, "items/a", "new-items/a")
	require.NoError(t, err)
	latest, err := namespacedKV.Put(ctx, "items/d", "new-items/d")
	require.NoError(t, err)

	getOpts := append(clientv3.WithFirstCreate(),
		clientv3.WithRev(historical.Header.Revision),
		clientv3.WithSerializable())
	txnResp, err := namespacedKV.Txn(ctx).
		Then(clientv3.OpTxn(nil,
			[]clientv3.Op{clientv3.OpGet("items/", getOpts...)},
			nil)).
		Commit()
	require.NoError(t, err)
	require.True(t, txnResp.Succeeded)
	require.Equal(t, latest.Header.Revision, txnResp.Header.Revision)
	require.Len(t, txnResp.Responses, 1)
	nestedTxn := txnResp.Responses[0].GetResponseTxn()
	require.NotNil(t, nestedTxn)
	require.True(t, nestedTxn.Succeeded)
	require.Len(t, nestedTxn.Responses, 1)
	nestedGet := nestedTxn.Responses[0].GetResponseRange()
	require.NotNil(t, nestedGet)
	require.NotNil(t, nestedGet.Header)
	require.Equal(t, latest.Header.Revision, nestedGet.Header.Revision)
	require.Equal(t, int64(3), nestedGet.Count)
	require.True(t, nestedGet.More)
	require.Len(t, nestedGet.Kvs, 1)
	require.Equal(t, []byte("items/b"), nestedGet.Kvs[0].Key)
	require.Equal(t, []byte("old-items/b"), nestedGet.Kvs[0].Value)

	current, err := namespacedKV.Get(ctx, "items/", clientv3.WithPrefix())
	require.NoError(t, err)
	require.Equal(t, int64(3), current.Count)
	require.Equal(t, [][]byte{[]byte("items/a"), []byte("items/c"), []byte("items/d")},
		[][]byte{current.Kvs[0].Key, current.Kvs[1].Key, current.Kvs[2].Key})
	outsideSameTenantPrefix, err := client.Get(ctx, "/a1685/namespace-nested-txn-historical-serializable-prefix-first-create/tenant/items0/", clientv3.WithPrefix())
	require.NoError(t, err)
	require.Equal(t, int64(1), outsideSameTenantPrefix.Count)
	outsideTenant, err := client.Get(ctx, "/a1685/namespace-nested-txn-historical-serializable-prefix-first-create/tenant0/", clientv3.WithPrefix())
	require.NoError(t, err)
	require.Equal(t, int64(1), outsideTenant.Count)
}

func TestClientNamespaceNestedTxnGetHistoricalSerializablePrefixWithLastCreateReturnsLogicalKey(t *testing.T) {
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
	tenantPrefix := "/a1686/namespace-nested-txn-historical-serializable-prefix-last-create/tenant/"
	namespacedKV := namespace.NewKV(client.KV, tenantPrefix)
	_, err = client.Put(ctx, "/a1686/namespace-nested-txn-historical-serializable-prefix-last-create/tenant/items0/outside", "same-tenant-outside-prefix")
	require.NoError(t, err)
	_, err = client.Put(ctx, "/a1686/namespace-nested-txn-historical-serializable-prefix-last-create/tenant0/items/newer", "outside-tenant")
	require.NoError(t, err)
	_, err = namespacedKV.Put(ctx, "items/b", "old-items/b")
	require.NoError(t, err)
	_, err = namespacedKV.Put(ctx, "items/a", "old-items/a")
	require.NoError(t, err)
	historical, err := namespacedKV.Put(ctx, "items/c", "old-items/c")
	require.NoError(t, err)
	_, err = namespacedKV.Delete(ctx, "items/c")
	require.NoError(t, err)
	_, err = namespacedKV.Put(ctx, "items/a", "new-items/a")
	require.NoError(t, err)
	latest, err := namespacedKV.Put(ctx, "items/d", "new-items/d")
	require.NoError(t, err)

	getOpts := append(clientv3.WithLastCreate(),
		clientv3.WithRev(historical.Header.Revision),
		clientv3.WithSerializable())
	txnResp, err := namespacedKV.Txn(ctx).
		Then(clientv3.OpTxn(nil,
			[]clientv3.Op{clientv3.OpGet("items/", getOpts...)},
			nil)).
		Commit()
	require.NoError(t, err)
	require.True(t, txnResp.Succeeded)
	require.Equal(t, latest.Header.Revision, txnResp.Header.Revision)
	require.Len(t, txnResp.Responses, 1)
	nestedTxn := txnResp.Responses[0].GetResponseTxn()
	require.NotNil(t, nestedTxn)
	require.True(t, nestedTxn.Succeeded)
	require.Len(t, nestedTxn.Responses, 1)
	nestedGet := nestedTxn.Responses[0].GetResponseRange()
	require.NotNil(t, nestedGet)
	require.NotNil(t, nestedGet.Header)
	require.Equal(t, latest.Header.Revision, nestedGet.Header.Revision)
	require.Equal(t, int64(3), nestedGet.Count)
	require.True(t, nestedGet.More)
	require.Len(t, nestedGet.Kvs, 1)
	require.Equal(t, []byte("items/c"), nestedGet.Kvs[0].Key)
	require.Equal(t, []byte("old-items/c"), nestedGet.Kvs[0].Value)

	current, err := namespacedKV.Get(ctx, "items/", clientv3.WithPrefix())
	require.NoError(t, err)
	require.Equal(t, int64(3), current.Count)
	require.Equal(t, [][]byte{[]byte("items/a"), []byte("items/b"), []byte("items/d")},
		[][]byte{current.Kvs[0].Key, current.Kvs[1].Key, current.Kvs[2].Key})
	outsideSameTenantPrefix, err := client.Get(ctx, "/a1686/namespace-nested-txn-historical-serializable-prefix-last-create/tenant/items0/", clientv3.WithPrefix())
	require.NoError(t, err)
	require.Equal(t, int64(1), outsideSameTenantPrefix.Count)
	outsideTenant, err := client.Get(ctx, "/a1686/namespace-nested-txn-historical-serializable-prefix-last-create/tenant0/", clientv3.WithPrefix())
	require.NoError(t, err)
	require.Equal(t, int64(1), outsideTenant.Count)
}

func TestClientNamespaceNestedTxnGetHistoricalSerializablePrefixWithFirstRevReturnsLogicalKey(t *testing.T) {
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
	tenantPrefix := "/a1687/namespace-nested-txn-historical-serializable-prefix-first-rev/tenant/"
	namespacedKV := namespace.NewKV(client.KV, tenantPrefix)
	_, err = client.Put(ctx, "/a1687/namespace-nested-txn-historical-serializable-prefix-first-rev/tenant/items0/outside", "same-tenant-outside-prefix")
	require.NoError(t, err)
	_, err = client.Put(ctx, "/a1687/namespace-nested-txn-historical-serializable-prefix-first-rev/tenant0/items/older", "outside-tenant")
	require.NoError(t, err)
	_, err = namespacedKV.Put(ctx, "items/b", "old-items/b")
	require.NoError(t, err)
	_, err = namespacedKV.Put(ctx, "items/a", "old-items/a")
	require.NoError(t, err)
	_, err = namespacedKV.Put(ctx, "items/b", "updated-items/b")
	require.NoError(t, err)
	historical, err := namespacedKV.Put(ctx, "items/c", "old-items/c")
	require.NoError(t, err)
	_, err = namespacedKV.Delete(ctx, "items/a")
	require.NoError(t, err)
	_, err = namespacedKV.Put(ctx, "items/c", "new-items/c")
	require.NoError(t, err)
	latest, err := namespacedKV.Put(ctx, "items/d", "new-items/d")
	require.NoError(t, err)

	getOpts := append(clientv3.WithFirstRev(),
		clientv3.WithRev(historical.Header.Revision),
		clientv3.WithSerializable())
	txnResp, err := namespacedKV.Txn(ctx).
		Then(clientv3.OpTxn(nil,
			[]clientv3.Op{clientv3.OpGet("items/", getOpts...)},
			nil)).
		Commit()
	require.NoError(t, err)
	require.True(t, txnResp.Succeeded)
	require.Equal(t, latest.Header.Revision, txnResp.Header.Revision)
	require.Len(t, txnResp.Responses, 1)
	nestedTxn := txnResp.Responses[0].GetResponseTxn()
	require.NotNil(t, nestedTxn)
	require.True(t, nestedTxn.Succeeded)
	require.Len(t, nestedTxn.Responses, 1)
	nestedGet := nestedTxn.Responses[0].GetResponseRange()
	require.NotNil(t, nestedGet)
	require.NotNil(t, nestedGet.Header)
	require.Equal(t, latest.Header.Revision, nestedGet.Header.Revision)
	require.Equal(t, int64(3), nestedGet.Count)
	require.True(t, nestedGet.More)
	require.Len(t, nestedGet.Kvs, 1)
	require.Equal(t, []byte("items/a"), nestedGet.Kvs[0].Key)
	require.Equal(t, []byte("old-items/a"), nestedGet.Kvs[0].Value)

	current, err := namespacedKV.Get(ctx, "items/", clientv3.WithPrefix())
	require.NoError(t, err)
	require.Equal(t, int64(3), current.Count)
	require.Equal(t, [][]byte{[]byte("items/b"), []byte("items/c"), []byte("items/d")},
		[][]byte{current.Kvs[0].Key, current.Kvs[1].Key, current.Kvs[2].Key})
	outsideSameTenantPrefix, err := client.Get(ctx, "/a1687/namespace-nested-txn-historical-serializable-prefix-first-rev/tenant/items0/", clientv3.WithPrefix())
	require.NoError(t, err)
	require.Equal(t, int64(1), outsideSameTenantPrefix.Count)
	outsideTenant, err := client.Get(ctx, "/a1687/namespace-nested-txn-historical-serializable-prefix-first-rev/tenant0/", clientv3.WithPrefix())
	require.NoError(t, err)
	require.Equal(t, int64(1), outsideTenant.Count)
}

func TestClientNamespaceNestedTxnGetHistoricalSerializablePrefixWithLastRevReturnsLogicalKey(t *testing.T) {
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
	tenantPrefix := "/a1688/namespace-nested-txn-historical-serializable-prefix-last-rev/tenant/"
	namespacedKV := namespace.NewKV(client.KV, tenantPrefix)
	_, err = client.Put(ctx, "/a1688/namespace-nested-txn-historical-serializable-prefix-last-rev/tenant/items0/outside", "same-tenant-outside-prefix")
	require.NoError(t, err)
	_, err = client.Put(ctx, "/a1688/namespace-nested-txn-historical-serializable-prefix-last-rev/tenant0/items/newer", "outside-tenant")
	require.NoError(t, err)
	_, err = namespacedKV.Put(ctx, "items/b", "old-items/b")
	require.NoError(t, err)
	_, err = namespacedKV.Put(ctx, "items/a", "old-items/a")
	require.NoError(t, err)
	_, err = namespacedKV.Put(ctx, "items/b", "updated-items/b")
	require.NoError(t, err)
	historical, err := namespacedKV.Put(ctx, "items/c", "old-items/c")
	require.NoError(t, err)
	_, err = namespacedKV.Delete(ctx, "items/c")
	require.NoError(t, err)
	_, err = namespacedKV.Put(ctx, "items/a", "new-items/a")
	require.NoError(t, err)
	latest, err := namespacedKV.Put(ctx, "items/d", "new-items/d")
	require.NoError(t, err)

	getOpts := append(clientv3.WithLastRev(),
		clientv3.WithRev(historical.Header.Revision),
		clientv3.WithSerializable())
	txnResp, err := namespacedKV.Txn(ctx).
		Then(clientv3.OpTxn(nil,
			[]clientv3.Op{clientv3.OpGet("items/", getOpts...)},
			nil)).
		Commit()
	require.NoError(t, err)
	require.True(t, txnResp.Succeeded)
	require.Equal(t, latest.Header.Revision, txnResp.Header.Revision)
	require.Len(t, txnResp.Responses, 1)
	nestedTxn := txnResp.Responses[0].GetResponseTxn()
	require.NotNil(t, nestedTxn)
	require.True(t, nestedTxn.Succeeded)
	require.Len(t, nestedTxn.Responses, 1)
	nestedGet := nestedTxn.Responses[0].GetResponseRange()
	require.NotNil(t, nestedGet)
	require.NotNil(t, nestedGet.Header)
	require.Equal(t, latest.Header.Revision, nestedGet.Header.Revision)
	require.Equal(t, int64(3), nestedGet.Count)
	require.True(t, nestedGet.More)
	require.Len(t, nestedGet.Kvs, 1)
	require.Equal(t, []byte("items/c"), nestedGet.Kvs[0].Key)
	require.Equal(t, []byte("old-items/c"), nestedGet.Kvs[0].Value)

	current, err := namespacedKV.Get(ctx, "items/", clientv3.WithPrefix())
	require.NoError(t, err)
	require.Equal(t, int64(3), current.Count)
	require.Equal(t, [][]byte{[]byte("items/a"), []byte("items/b"), []byte("items/d")},
		[][]byte{current.Kvs[0].Key, current.Kvs[1].Key, current.Kvs[2].Key})
	outsideSameTenantPrefix, err := client.Get(ctx, "/a1688/namespace-nested-txn-historical-serializable-prefix-last-rev/tenant/items0/", clientv3.WithPrefix())
	require.NoError(t, err)
	require.Equal(t, int64(1), outsideSameTenantPrefix.Count)
	outsideTenant, err := client.Get(ctx, "/a1688/namespace-nested-txn-historical-serializable-prefix-last-rev/tenant0/", clientv3.WithPrefix())
	require.NoError(t, err)
	require.Equal(t, int64(1), outsideTenant.Count)
}

func TestClientNamespaceNestedTxnGetHistoricalSerializablePrefixWithFirstKeyReturnsLogicalKey(t *testing.T) {
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
	tenantPrefix := "/a1689/namespace-nested-txn-historical-serializable-prefix-first-key/tenant/"
	namespacedKV := namespace.NewKV(client.KV, tenantPrefix)
	_, err = client.Put(ctx, "/a1689/namespace-nested-txn-historical-serializable-prefix-first-key/tenant/items0/outside", "same-tenant-outside-prefix")
	require.NoError(t, err)
	_, err = client.Put(ctx, "/a1689/namespace-nested-txn-historical-serializable-prefix-first-key/tenant0/items/0", "outside-tenant")
	require.NoError(t, err)
	_, err = namespacedKV.Put(ctx, "items/b", "old-items/b")
	require.NoError(t, err)
	_, err = namespacedKV.Put(ctx, "items/a", "old-items/a")
	require.NoError(t, err)
	historical, err := namespacedKV.Put(ctx, "items/c", "old-items/c")
	require.NoError(t, err)
	_, err = namespacedKV.Delete(ctx, "items/a")
	require.NoError(t, err)
	_, err = namespacedKV.Put(ctx, "items/b", "new-items/b")
	require.NoError(t, err)
	latest, err := namespacedKV.Put(ctx, "items/0", "new-items/0")
	require.NoError(t, err)

	getOpts := append(clientv3.WithFirstKey(),
		clientv3.WithRev(historical.Header.Revision),
		clientv3.WithSerializable())
	txnResp, err := namespacedKV.Txn(ctx).
		Then(clientv3.OpTxn(nil,
			[]clientv3.Op{clientv3.OpGet("items/", getOpts...)},
			nil)).
		Commit()
	require.NoError(t, err)
	require.True(t, txnResp.Succeeded)
	require.Equal(t, latest.Header.Revision, txnResp.Header.Revision)
	require.Len(t, txnResp.Responses, 1)
	nestedTxn := txnResp.Responses[0].GetResponseTxn()
	require.NotNil(t, nestedTxn)
	require.True(t, nestedTxn.Succeeded)
	require.Len(t, nestedTxn.Responses, 1)
	nestedGet := nestedTxn.Responses[0].GetResponseRange()
	require.NotNil(t, nestedGet)
	require.NotNil(t, nestedGet.Header)
	require.Equal(t, latest.Header.Revision, nestedGet.Header.Revision)
	require.Equal(t, int64(3), nestedGet.Count)
	require.True(t, nestedGet.More)
	require.Len(t, nestedGet.Kvs, 1)
	require.Equal(t, []byte("items/a"), nestedGet.Kvs[0].Key)
	require.Equal(t, []byte("old-items/a"), nestedGet.Kvs[0].Value)

	current, err := namespacedKV.Get(ctx, "items/", clientv3.WithPrefix())
	require.NoError(t, err)
	require.Equal(t, int64(3), current.Count)
	require.Equal(t, [][]byte{[]byte("items/0"), []byte("items/b"), []byte("items/c")},
		[][]byte{current.Kvs[0].Key, current.Kvs[1].Key, current.Kvs[2].Key})
	outsideSameTenantPrefix, err := client.Get(ctx, "/a1689/namespace-nested-txn-historical-serializable-prefix-first-key/tenant/items0/", clientv3.WithPrefix())
	require.NoError(t, err)
	require.Equal(t, int64(1), outsideSameTenantPrefix.Count)
	outsideTenant, err := client.Get(ctx, "/a1689/namespace-nested-txn-historical-serializable-prefix-first-key/tenant0/", clientv3.WithPrefix())
	require.NoError(t, err)
	require.Equal(t, int64(1), outsideTenant.Count)
}

func TestClientNamespaceNestedTxnGetHistoricalSerializablePrefixWithLastKeyReturnsLogicalKey(t *testing.T) {
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
	tenantPrefix := "/a1690/namespace-nested-txn-historical-serializable-prefix-last-key/tenant/"
	namespacedKV := namespace.NewKV(client.KV, tenantPrefix)
	_, err = client.Put(ctx, "/a1690/namespace-nested-txn-historical-serializable-prefix-last-key/tenant/items0/outside", "same-tenant-outside-prefix")
	require.NoError(t, err)
	_, err = client.Put(ctx, "/a1690/namespace-nested-txn-historical-serializable-prefix-last-key/tenant0/items/z", "outside-tenant")
	require.NoError(t, err)
	_, err = namespacedKV.Put(ctx, "items/b", "old-items/b")
	require.NoError(t, err)
	_, err = namespacedKV.Put(ctx, "items/a", "old-items/a")
	require.NoError(t, err)
	historical, err := namespacedKV.Put(ctx, "items/c", "old-items/c")
	require.NoError(t, err)
	_, err = namespacedKV.Delete(ctx, "items/c")
	require.NoError(t, err)
	_, err = namespacedKV.Put(ctx, "items/b", "new-items/b")
	require.NoError(t, err)
	latest, err := namespacedKV.Put(ctx, "items/d", "new-items/d")
	require.NoError(t, err)

	getOpts := append(clientv3.WithLastKey(),
		clientv3.WithRev(historical.Header.Revision),
		clientv3.WithSerializable())
	txnResp, err := namespacedKV.Txn(ctx).
		Then(clientv3.OpTxn(nil,
			[]clientv3.Op{clientv3.OpGet("items/", getOpts...)},
			nil)).
		Commit()
	require.NoError(t, err)
	require.True(t, txnResp.Succeeded)
	require.Equal(t, latest.Header.Revision, txnResp.Header.Revision)
	require.Len(t, txnResp.Responses, 1)
	nestedTxn := txnResp.Responses[0].GetResponseTxn()
	require.NotNil(t, nestedTxn)
	require.True(t, nestedTxn.Succeeded)
	require.Len(t, nestedTxn.Responses, 1)
	nestedGet := nestedTxn.Responses[0].GetResponseRange()
	require.NotNil(t, nestedGet)
	require.NotNil(t, nestedGet.Header)
	require.Equal(t, latest.Header.Revision, nestedGet.Header.Revision)
	require.Equal(t, int64(3), nestedGet.Count)
	require.True(t, nestedGet.More)
	require.Len(t, nestedGet.Kvs, 1)
	require.Equal(t, []byte("items/c"), nestedGet.Kvs[0].Key)
	require.Equal(t, []byte("old-items/c"), nestedGet.Kvs[0].Value)

	current, err := namespacedKV.Get(ctx, "items/", clientv3.WithPrefix())
	require.NoError(t, err)
	require.Equal(t, int64(3), current.Count)
	require.Equal(t, [][]byte{[]byte("items/a"), []byte("items/b"), []byte("items/d")},
		[][]byte{current.Kvs[0].Key, current.Kvs[1].Key, current.Kvs[2].Key})
	outsideSameTenantPrefix, err := client.Get(ctx, "/a1690/namespace-nested-txn-historical-serializable-prefix-last-key/tenant/items0/", clientv3.WithPrefix())
	require.NoError(t, err)
	require.Equal(t, int64(1), outsideSameTenantPrefix.Count)
	outsideTenant, err := client.Get(ctx, "/a1690/namespace-nested-txn-historical-serializable-prefix-last-key/tenant0/", clientv3.WithPrefix())
	require.NoError(t, err)
	require.Equal(t, int64(1), outsideTenant.Count)
}

func TestClientNamespaceNestedTxnGetHistoricalSerializablePrefixWithLastCreateAndMaxCreateRevisionReturnsLogicalKey(t *testing.T) {
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
	tenantPrefix := "/a1691/namespace-nested-txn-historical-serializable-prefix-last-create-max-create/tenant/"
	namespacedKV := namespace.NewKV(client.KV, tenantPrefix)
	_, err = client.Put(ctx, "/a1691/namespace-nested-txn-historical-serializable-prefix-last-create-max-create/tenant/waiters0/outside", "same-tenant-outside-prefix")
	require.NoError(t, err)
	_, err = client.Put(ctx, "/a1691/namespace-nested-txn-historical-serializable-prefix-last-create-max-create/tenant0/waiters/outside", "outside-tenant")
	require.NoError(t, err)
	_, err = namespacedKV.Put(ctx, "waiters/a", "value-waiters/a")
	require.NoError(t, err)
	_, err = namespacedKV.Put(ctx, "waiters/b", "value-waiters/b")
	require.NoError(t, err)
	historical, err := namespacedKV.Put(ctx, "waiters/c", "value-waiters/c")
	require.NoError(t, err)
	_, err = namespacedKV.Delete(ctx, "waiters/b")
	require.NoError(t, err)
	_, err = namespacedKV.Put(ctx, "waiters/a", "updated-waiters/a")
	require.NoError(t, err)
	latest, err := namespacedKV.Put(ctx, "waiters/d", "value-waiters/d")
	require.NoError(t, err)

	getOpts := append(clientv3.WithLastCreate(),
		clientv3.WithMaxCreateRev(historical.Header.Revision-1),
		clientv3.WithRev(historical.Header.Revision),
		clientv3.WithSerializable())
	txnResp, err := namespacedKV.Txn(ctx).
		Then(clientv3.OpTxn(nil,
			[]clientv3.Op{clientv3.OpGet("waiters/", getOpts...)},
			nil)).
		Commit()
	require.NoError(t, err)
	require.True(t, txnResp.Succeeded)
	require.Equal(t, latest.Header.Revision, txnResp.Header.Revision)
	require.Len(t, txnResp.Responses, 1)
	nestedTxn := txnResp.Responses[0].GetResponseTxn()
	require.NotNil(t, nestedTxn)
	require.True(t, nestedTxn.Succeeded)
	require.Len(t, nestedTxn.Responses, 1)
	nestedGet := nestedTxn.Responses[0].GetResponseRange()
	require.NotNil(t, nestedGet)
	require.NotNil(t, nestedGet.Header)
	require.Equal(t, latest.Header.Revision, nestedGet.Header.Revision)
	require.Equal(t, int64(3), nestedGet.Count)
	require.True(t, nestedGet.More)
	require.Len(t, nestedGet.Kvs, 1)
	require.Equal(t, []byte("waiters/b"), nestedGet.Kvs[0].Key)
	require.Equal(t, []byte("value-waiters/b"), nestedGet.Kvs[0].Value)

	current, err := namespacedKV.Get(ctx, "waiters/", clientv3.WithPrefix())
	require.NoError(t, err)
	require.Equal(t, int64(3), current.Count)
	require.Equal(t, [][]byte{[]byte("waiters/a"), []byte("waiters/c"), []byte("waiters/d")},
		[][]byte{current.Kvs[0].Key, current.Kvs[1].Key, current.Kvs[2].Key})
	outsideSameTenantPrefix, err := client.Get(ctx, "/a1691/namespace-nested-txn-historical-serializable-prefix-last-create-max-create/tenant/waiters0/", clientv3.WithPrefix())
	require.NoError(t, err)
	require.Equal(t, int64(1), outsideSameTenantPrefix.Count)
	outsideTenant, err := client.Get(ctx, "/a1691/namespace-nested-txn-historical-serializable-prefix-last-create-max-create/tenant0/", clientv3.WithPrefix())
	require.NoError(t, err)
	require.Equal(t, int64(1), outsideTenant.Count)
}

func TestClientNamespaceNestedTxnGetHistoricalSerializablePrefixWithLastRevAndMaxModRevisionReturnsLogicalKey(t *testing.T) {
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
	tenantPrefix := "/a1692/namespace-nested-txn-historical-serializable-prefix-last-rev-max-mod/tenant/"
	namespacedKV := namespace.NewKV(client.KV, tenantPrefix)
	_, err = client.Put(ctx, "/a1692/namespace-nested-txn-historical-serializable-prefix-last-rev-max-mod/tenant/locks0/outside", "same-tenant-outside-prefix")
	require.NoError(t, err)
	_, err = client.Put(ctx, "/a1692/namespace-nested-txn-historical-serializable-prefix-last-rev-max-mod/tenant0/locks/outside", "outside-tenant")
	require.NoError(t, err)
	for _, key := range []string{"locks/a", "locks/b", "locks/c"} {
		_, err = namespacedKV.Put(ctx, key, "value-"+key)
		require.NoError(t, err)
	}
	updateB, err := namespacedKV.Put(ctx, "locks/b", "updated-locks/b")
	require.NoError(t, err)
	historical, err := namespacedKV.Put(ctx, "locks/c", "historical-locks/c")
	require.NoError(t, err)
	_, err = namespacedKV.Delete(ctx, "locks/b")
	require.NoError(t, err)
	_, err = namespacedKV.Put(ctx, "locks/c", "current-locks/c")
	require.NoError(t, err)
	latest, err := namespacedKV.Put(ctx, "locks/d", "value-locks/d")
	require.NoError(t, err)

	getOpts := append(clientv3.WithLastRev(),
		clientv3.WithMaxModRev(updateB.Header.Revision),
		clientv3.WithRev(historical.Header.Revision),
		clientv3.WithSerializable())
	txnResp, err := namespacedKV.Txn(ctx).
		Then(clientv3.OpTxn(nil,
			[]clientv3.Op{clientv3.OpGet("locks/", getOpts...)},
			nil)).
		Commit()
	require.NoError(t, err)
	require.True(t, txnResp.Succeeded)
	require.Equal(t, latest.Header.Revision, txnResp.Header.Revision)
	require.Len(t, txnResp.Responses, 1)
	nestedTxn := txnResp.Responses[0].GetResponseTxn()
	require.NotNil(t, nestedTxn)
	require.True(t, nestedTxn.Succeeded)
	require.Len(t, nestedTxn.Responses, 1)
	nestedGet := nestedTxn.Responses[0].GetResponseRange()
	require.NotNil(t, nestedGet)
	require.NotNil(t, nestedGet.Header)
	require.Equal(t, latest.Header.Revision, nestedGet.Header.Revision)
	require.Equal(t, int64(3), nestedGet.Count)
	require.True(t, nestedGet.More)
	require.Len(t, nestedGet.Kvs, 1)
	require.Equal(t, []byte("locks/b"), nestedGet.Kvs[0].Key)
	require.Equal(t, []byte("updated-locks/b"), nestedGet.Kvs[0].Value)

	current, err := namespacedKV.Get(ctx, "locks/", clientv3.WithPrefix())
	require.NoError(t, err)
	require.Equal(t, int64(3), current.Count)
	require.Equal(t, [][]byte{[]byte("locks/a"), []byte("locks/c"), []byte("locks/d")},
		[][]byte{current.Kvs[0].Key, current.Kvs[1].Key, current.Kvs[2].Key})
	outsideSameTenantPrefix, err := client.Get(ctx, "/a1692/namespace-nested-txn-historical-serializable-prefix-last-rev-max-mod/tenant/locks0/", clientv3.WithPrefix())
	require.NoError(t, err)
	require.Equal(t, int64(1), outsideSameTenantPrefix.Count)
	outsideTenant, err := client.Get(ctx, "/a1692/namespace-nested-txn-historical-serializable-prefix-last-rev-max-mod/tenant0/", clientv3.WithPrefix())
	require.NoError(t, err)
	require.Equal(t, int64(1), outsideTenant.Count)
}

func TestClientNamespaceNestedTxnGetHistoricalSerializableLogicalRangeWithLastRevReturnsLogicalKey(t *testing.T) {
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
	tenantPrefix := "/a1693/namespace-nested-txn-historical-serializable-logical-range-last-rev/tenant/"
	namespacedKV := namespace.NewKV(client.KV, tenantPrefix)
	_, err = client.Put(ctx, "/a1693/namespace-nested-txn-historical-serializable-logical-range-last-rev/tenant/range0/outside", "same-tenant-outside-range")
	require.NoError(t, err)
	_, err = client.Put(ctx, "/a1693/namespace-nested-txn-historical-serializable-logical-range-last-rev/tenant0/range/c", "outside-tenant")
	require.NoError(t, err)
	_, err = namespacedKV.Put(ctx, "range/a", "old-range/a")
	require.NoError(t, err)
	_, err = namespacedKV.Put(ctx, "range/b", "old-range/b")
	require.NoError(t, err)
	_, err = namespacedKV.Put(ctx, "range/b", "updated-range/b")
	require.NoError(t, err)
	historical, err := namespacedKV.Put(ctx, "range/c", "old-range/c")
	require.NoError(t, err)
	_, err = namespacedKV.Delete(ctx, "range/c")
	require.NoError(t, err)
	_, err = namespacedKV.Put(ctx, "range/a", "new-range/a")
	require.NoError(t, err)
	_, err = namespacedKV.Put(ctx, "range/d", "outside-upper-bound")
	require.NoError(t, err)
	latest, err := namespacedKV.Put(ctx, "other/newer", "other")
	require.NoError(t, err)

	getOpts := append(clientv3.WithLastRev(),
		clientv3.WithRange("range/d"),
		clientv3.WithRev(historical.Header.Revision),
		clientv3.WithSerializable())
	txnResp, err := namespacedKV.Txn(ctx).
		Then(clientv3.OpTxn(nil,
			[]clientv3.Op{clientv3.OpGet("range/a", getOpts...)},
			nil)).
		Commit()
	require.NoError(t, err)
	require.True(t, txnResp.Succeeded)
	require.Equal(t, latest.Header.Revision, txnResp.Header.Revision)
	require.Len(t, txnResp.Responses, 1)
	nestedTxn := txnResp.Responses[0].GetResponseTxn()
	require.NotNil(t, nestedTxn)
	require.True(t, nestedTxn.Succeeded)
	require.Len(t, nestedTxn.Responses, 1)
	nestedGet := nestedTxn.Responses[0].GetResponseRange()
	require.NotNil(t, nestedGet)
	require.NotNil(t, nestedGet.Header)
	require.Equal(t, latest.Header.Revision, nestedGet.Header.Revision)
	require.Equal(t, int64(3), nestedGet.Count)
	require.True(t, nestedGet.More)
	require.Len(t, nestedGet.Kvs, 1)
	require.Equal(t, []byte("range/c"), nestedGet.Kvs[0].Key)
	require.Equal(t, []byte("old-range/c"), nestedGet.Kvs[0].Value)

	current, err := namespacedKV.Get(ctx, "range/a", clientv3.WithRange("range/d"))
	require.NoError(t, err)
	require.Equal(t, int64(2), current.Count)
	require.Equal(t, [][]byte{[]byte("range/a"), []byte("range/b")},
		[][]byte{current.Kvs[0].Key, current.Kvs[1].Key})
	outsideSameTenantRange, err := client.Get(ctx, "/a1693/namespace-nested-txn-historical-serializable-logical-range-last-rev/tenant/range0/", clientv3.WithPrefix())
	require.NoError(t, err)
	require.Equal(t, int64(1), outsideSameTenantRange.Count)
	outsideTenant, err := client.Get(ctx, "/a1693/namespace-nested-txn-historical-serializable-logical-range-last-rev/tenant0/", clientv3.WithPrefix())
	require.NoError(t, err)
	require.Equal(t, int64(1), outsideTenant.Count)
}

func TestClientNamespaceNestedTxnGetHistoricalSerializableLogicalRangeWithFirstRevReturnsLogicalKey(t *testing.T) {
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
	tenantPrefix := "/a1694/namespace-nested-txn-historical-serializable-logical-range-first-rev/tenant/"
	namespacedKV := namespace.NewKV(client.KV, tenantPrefix)
	_, err = client.Put(ctx, "/a1694/namespace-nested-txn-historical-serializable-logical-range-first-rev/tenant/range0/outside", "same-tenant-outside-range")
	require.NoError(t, err)
	_, err = client.Put(ctx, "/a1694/namespace-nested-txn-historical-serializable-logical-range-first-rev/tenant0/range/a", "outside-tenant")
	require.NoError(t, err)
	_, err = namespacedKV.Put(ctx, "range/a", "old-range/a")
	require.NoError(t, err)
	_, err = namespacedKV.Put(ctx, "range/b", "old-range/b")
	require.NoError(t, err)
	_, err = namespacedKV.Put(ctx, "range/b", "updated-range/b")
	require.NoError(t, err)
	historical, err := namespacedKV.Put(ctx, "range/c", "old-range/c")
	require.NoError(t, err)
	_, err = namespacedKV.Delete(ctx, "range/a")
	require.NoError(t, err)
	_, err = namespacedKV.Put(ctx, "range/b", "new-range/b")
	require.NoError(t, err)
	_, err = namespacedKV.Put(ctx, "range/d", "outside-upper-bound")
	require.NoError(t, err)
	latest, err := namespacedKV.Put(ctx, "other/newer", "other")
	require.NoError(t, err)

	getOpts := append(clientv3.WithFirstRev(),
		clientv3.WithRange("range/d"),
		clientv3.WithRev(historical.Header.Revision),
		clientv3.WithSerializable())
	txnResp, err := namespacedKV.Txn(ctx).
		Then(clientv3.OpTxn(nil,
			[]clientv3.Op{clientv3.OpGet("range/a", getOpts...)},
			nil)).
		Commit()
	require.NoError(t, err)
	require.True(t, txnResp.Succeeded)
	require.Equal(t, latest.Header.Revision, txnResp.Header.Revision)
	require.Len(t, txnResp.Responses, 1)
	nestedTxn := txnResp.Responses[0].GetResponseTxn()
	require.NotNil(t, nestedTxn)
	require.True(t, nestedTxn.Succeeded)
	require.Len(t, nestedTxn.Responses, 1)
	nestedGet := nestedTxn.Responses[0].GetResponseRange()
	require.NotNil(t, nestedGet)
	require.NotNil(t, nestedGet.Header)
	require.Equal(t, latest.Header.Revision, nestedGet.Header.Revision)
	require.Equal(t, int64(3), nestedGet.Count)
	require.True(t, nestedGet.More)
	require.Len(t, nestedGet.Kvs, 1)
	require.Equal(t, []byte("range/a"), nestedGet.Kvs[0].Key)
	require.Equal(t, []byte("old-range/a"), nestedGet.Kvs[0].Value)

	current, err := namespacedKV.Get(ctx, "range/a", clientv3.WithRange("range/d"))
	require.NoError(t, err)
	require.Equal(t, int64(2), current.Count)
	require.Equal(t, [][]byte{[]byte("range/b"), []byte("range/c")},
		[][]byte{current.Kvs[0].Key, current.Kvs[1].Key})
	outsideSameTenantRange, err := client.Get(ctx, "/a1694/namespace-nested-txn-historical-serializable-logical-range-first-rev/tenant/range0/", clientv3.WithPrefix())
	require.NoError(t, err)
	require.Equal(t, int64(1), outsideSameTenantRange.Count)
	outsideTenant, err := client.Get(ctx, "/a1694/namespace-nested-txn-historical-serializable-logical-range-first-rev/tenant0/", clientv3.WithPrefix())
	require.NoError(t, err)
	require.Equal(t, int64(1), outsideTenant.Count)
}

func TestClientNamespaceNestedTxnGetHistoricalSerializableLogicalRangeWithFirstKeyReturnsLogicalKey(t *testing.T) {
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
	tenantPrefix := "/a1695/namespace-nested-txn-historical-serializable-logical-range-first-key/tenant/"
	namespacedKV := namespace.NewKV(client.KV, tenantPrefix)
	_, err = client.Put(ctx, "/a1695/namespace-nested-txn-historical-serializable-logical-range-first-key/tenant/range0/outside", "same-tenant-outside-range")
	require.NoError(t, err)
	_, err = client.Put(ctx, "/a1695/namespace-nested-txn-historical-serializable-logical-range-first-key/tenant0/range/a", "outside-tenant")
	require.NoError(t, err)
	_, err = namespacedKV.Put(ctx, "range/b", "old-range/b")
	require.NoError(t, err)
	_, err = namespacedKV.Put(ctx, "range/a", "old-range/a")
	require.NoError(t, err)
	historical, err := namespacedKV.Put(ctx, "range/c", "old-range/c")
	require.NoError(t, err)
	_, err = namespacedKV.Delete(ctx, "range/a")
	require.NoError(t, err)
	_, err = namespacedKV.Put(ctx, "range/b", "new-range/b")
	require.NoError(t, err)
	_, err = namespacedKV.Put(ctx, "range/d", "outside-upper-bound")
	require.NoError(t, err)
	latest, err := namespacedKV.Put(ctx, "other/newer", "other")
	require.NoError(t, err)

	getOpts := append(clientv3.WithFirstKey(),
		clientv3.WithRange("range/d"),
		clientv3.WithRev(historical.Header.Revision),
		clientv3.WithSerializable())
	txnResp, err := namespacedKV.Txn(ctx).
		Then(clientv3.OpTxn(nil,
			[]clientv3.Op{clientv3.OpGet("range/a", getOpts...)},
			nil)).
		Commit()
	require.NoError(t, err)
	require.True(t, txnResp.Succeeded)
	require.Equal(t, latest.Header.Revision, txnResp.Header.Revision)
	require.Len(t, txnResp.Responses, 1)
	nestedTxn := txnResp.Responses[0].GetResponseTxn()
	require.NotNil(t, nestedTxn)
	require.True(t, nestedTxn.Succeeded)
	require.Len(t, nestedTxn.Responses, 1)
	nestedGet := nestedTxn.Responses[0].GetResponseRange()
	require.NotNil(t, nestedGet)
	require.NotNil(t, nestedGet.Header)
	require.Equal(t, latest.Header.Revision, nestedGet.Header.Revision)
	require.Equal(t, int64(3), nestedGet.Count)
	require.True(t, nestedGet.More)
	require.Len(t, nestedGet.Kvs, 1)
	require.Equal(t, []byte("range/a"), nestedGet.Kvs[0].Key)
	require.Equal(t, []byte("old-range/a"), nestedGet.Kvs[0].Value)

	current, err := namespacedKV.Get(ctx, "range/a", clientv3.WithRange("range/d"))
	require.NoError(t, err)
	require.Equal(t, int64(2), current.Count)
	require.Equal(t, [][]byte{[]byte("range/b"), []byte("range/c")},
		[][]byte{current.Kvs[0].Key, current.Kvs[1].Key})
	outsideSameTenantRange, err := client.Get(ctx, "/a1695/namespace-nested-txn-historical-serializable-logical-range-first-key/tenant/range0/", clientv3.WithPrefix())
	require.NoError(t, err)
	require.Equal(t, int64(1), outsideSameTenantRange.Count)
	outsideTenant, err := client.Get(ctx, "/a1695/namespace-nested-txn-historical-serializable-logical-range-first-key/tenant0/", clientv3.WithPrefix())
	require.NoError(t, err)
	require.Equal(t, int64(1), outsideTenant.Count)
}

func TestClientNamespaceNestedTxnGetHistoricalSerializableLogicalRangeWithLastKeyReturnsLogicalKey(t *testing.T) {
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
	tenantPrefix := "/a1696/namespace-nested-txn-historical-serializable-logical-range-last-key/tenant/"
	namespacedKV := namespace.NewKV(client.KV, tenantPrefix)
	_, err = client.Put(ctx, "/a1696/namespace-nested-txn-historical-serializable-logical-range-last-key/tenant/range0/outside", "same-tenant-outside-range")
	require.NoError(t, err)
	_, err = client.Put(ctx, "/a1696/namespace-nested-txn-historical-serializable-logical-range-last-key/tenant0/range/z", "outside-tenant")
	require.NoError(t, err)
	_, err = namespacedKV.Put(ctx, "range/b", "old-range/b")
	require.NoError(t, err)
	_, err = namespacedKV.Put(ctx, "range/a", "old-range/a")
	require.NoError(t, err)
	historical, err := namespacedKV.Put(ctx, "range/c", "old-range/c")
	require.NoError(t, err)
	_, err = namespacedKV.Delete(ctx, "range/c")
	require.NoError(t, err)
	_, err = namespacedKV.Put(ctx, "range/b", "new-range/b")
	require.NoError(t, err)
	_, err = namespacedKV.Put(ctx, "range/d", "outside-upper-bound")
	require.NoError(t, err)
	latest, err := namespacedKV.Put(ctx, "other/newer", "other")
	require.NoError(t, err)

	getOpts := append(clientv3.WithLastKey(),
		clientv3.WithRange("range/d"),
		clientv3.WithRev(historical.Header.Revision),
		clientv3.WithSerializable())
	txnResp, err := namespacedKV.Txn(ctx).
		Then(clientv3.OpTxn(nil,
			[]clientv3.Op{clientv3.OpGet("range/a", getOpts...)},
			nil)).
		Commit()
	require.NoError(t, err)
	require.True(t, txnResp.Succeeded)
	require.Equal(t, latest.Header.Revision, txnResp.Header.Revision)
	require.Len(t, txnResp.Responses, 1)
	nestedTxn := txnResp.Responses[0].GetResponseTxn()
	require.NotNil(t, nestedTxn)
	require.True(t, nestedTxn.Succeeded)
	require.Len(t, nestedTxn.Responses, 1)
	nestedGet := nestedTxn.Responses[0].GetResponseRange()
	require.NotNil(t, nestedGet)
	require.NotNil(t, nestedGet.Header)
	require.Equal(t, latest.Header.Revision, nestedGet.Header.Revision)
	require.Equal(t, int64(3), nestedGet.Count)
	require.True(t, nestedGet.More)
	require.Len(t, nestedGet.Kvs, 1)
	require.Equal(t, []byte("range/c"), nestedGet.Kvs[0].Key)
	require.Equal(t, []byte("old-range/c"), nestedGet.Kvs[0].Value)

	current, err := namespacedKV.Get(ctx, "range/a", clientv3.WithRange("range/d"))
	require.NoError(t, err)
	require.Equal(t, int64(2), current.Count)
	require.Equal(t, [][]byte{[]byte("range/a"), []byte("range/b")},
		[][]byte{current.Kvs[0].Key, current.Kvs[1].Key})
	outsideSameTenantRange, err := client.Get(ctx, "/a1696/namespace-nested-txn-historical-serializable-logical-range-last-key/tenant/range0/", clientv3.WithPrefix())
	require.NoError(t, err)
	require.Equal(t, int64(1), outsideSameTenantRange.Count)
	outsideTenant, err := client.Get(ctx, "/a1696/namespace-nested-txn-historical-serializable-logical-range-last-key/tenant0/", clientv3.WithPrefix())
	require.NoError(t, err)
	require.Equal(t, int64(1), outsideTenant.Count)
}

func TestClientNamespaceNestedTxnGetHistoricalSerializableLogicalRangeWithFirstCreateReturnsLogicalKey(t *testing.T) {
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
	tenantPrefix := "/a1697/namespace-nested-txn-historical-serializable-logical-range-first-create/tenant/"
	namespacedKV := namespace.NewKV(client.KV, tenantPrefix)
	_, err = client.Put(ctx, "/a1697/namespace-nested-txn-historical-serializable-logical-range-first-create/tenant/range0/outside", "same-tenant-outside-range")
	require.NoError(t, err)
	_, err = client.Put(ctx, "/a1697/namespace-nested-txn-historical-serializable-logical-range-first-create/tenant0/range/a", "outside-tenant")
	require.NoError(t, err)
	_, err = namespacedKV.Put(ctx, "range/b", "old-range/b")
	require.NoError(t, err)
	_, err = namespacedKV.Put(ctx, "range/a", "old-range/a")
	require.NoError(t, err)
	historical, err := namespacedKV.Put(ctx, "range/c", "old-range/c")
	require.NoError(t, err)
	_, err = namespacedKV.Delete(ctx, "range/b")
	require.NoError(t, err)
	_, err = namespacedKV.Put(ctx, "range/a", "new-range/a")
	require.NoError(t, err)
	_, err = namespacedKV.Put(ctx, "range/d", "outside-upper-bound")
	require.NoError(t, err)
	latest, err := namespacedKV.Put(ctx, "other/newer", "other")
	require.NoError(t, err)

	getOpts := append(clientv3.WithFirstCreate(),
		clientv3.WithRange("range/d"),
		clientv3.WithRev(historical.Header.Revision),
		clientv3.WithSerializable())
	txnResp, err := namespacedKV.Txn(ctx).
		Then(clientv3.OpTxn(nil,
			[]clientv3.Op{clientv3.OpGet("range/a", getOpts...)},
			nil)).
		Commit()
	require.NoError(t, err)
	require.True(t, txnResp.Succeeded)
	require.Equal(t, latest.Header.Revision, txnResp.Header.Revision)
	require.Len(t, txnResp.Responses, 1)
	nestedTxn := txnResp.Responses[0].GetResponseTxn()
	require.NotNil(t, nestedTxn)
	require.True(t, nestedTxn.Succeeded)
	require.Len(t, nestedTxn.Responses, 1)
	nestedGet := nestedTxn.Responses[0].GetResponseRange()
	require.NotNil(t, nestedGet)
	require.NotNil(t, nestedGet.Header)
	require.Equal(t, latest.Header.Revision, nestedGet.Header.Revision)
	require.Equal(t, int64(3), nestedGet.Count)
	require.True(t, nestedGet.More)
	require.Len(t, nestedGet.Kvs, 1)
	require.Equal(t, []byte("range/b"), nestedGet.Kvs[0].Key)
	require.Equal(t, []byte("old-range/b"), nestedGet.Kvs[0].Value)

	current, err := namespacedKV.Get(ctx, "range/a", clientv3.WithRange("range/d"))
	require.NoError(t, err)
	require.Equal(t, int64(2), current.Count)
	require.Equal(t, [][]byte{[]byte("range/a"), []byte("range/c")},
		[][]byte{current.Kvs[0].Key, current.Kvs[1].Key})
	outsideSameTenantRange, err := client.Get(ctx, "/a1697/namespace-nested-txn-historical-serializable-logical-range-first-create/tenant/range0/", clientv3.WithPrefix())
	require.NoError(t, err)
	require.Equal(t, int64(1), outsideSameTenantRange.Count)
	outsideTenant, err := client.Get(ctx, "/a1697/namespace-nested-txn-historical-serializable-logical-range-first-create/tenant0/", clientv3.WithPrefix())
	require.NoError(t, err)
	require.Equal(t, int64(1), outsideTenant.Count)
}

func TestClientNamespaceNestedTxnGetHistoricalSerializableLogicalRangeWithLastCreateReturnsLogicalKey(t *testing.T) {
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
	tenantPrefix := "/a1698/namespace-nested-txn-historical-serializable-logical-range-last-create/tenant/"
	namespacedKV := namespace.NewKV(client.KV, tenantPrefix)
	_, err = client.Put(ctx, "/a1698/namespace-nested-txn-historical-serializable-logical-range-last-create/tenant/range0/outside", "same-tenant-outside-range")
	require.NoError(t, err)
	_, err = client.Put(ctx, "/a1698/namespace-nested-txn-historical-serializable-logical-range-last-create/tenant0/range/z", "outside-tenant")
	require.NoError(t, err)
	_, err = namespacedKV.Put(ctx, "range/b", "old-range/b")
	require.NoError(t, err)
	_, err = namespacedKV.Put(ctx, "range/a", "old-range/a")
	require.NoError(t, err)
	historical, err := namespacedKV.Put(ctx, "range/c", "old-range/c")
	require.NoError(t, err)
	_, err = namespacedKV.Delete(ctx, "range/c")
	require.NoError(t, err)
	_, err = namespacedKV.Put(ctx, "range/a", "new-range/a")
	require.NoError(t, err)
	_, err = namespacedKV.Put(ctx, "range/d", "outside-upper-bound")
	require.NoError(t, err)
	latest, err := namespacedKV.Put(ctx, "other/newer", "other")
	require.NoError(t, err)

	getOpts := append(clientv3.WithLastCreate(),
		clientv3.WithRange("range/d"),
		clientv3.WithRev(historical.Header.Revision),
		clientv3.WithSerializable())
	txnResp, err := namespacedKV.Txn(ctx).
		Then(clientv3.OpTxn(nil,
			[]clientv3.Op{clientv3.OpGet("range/a", getOpts...)},
			nil)).
		Commit()
	require.NoError(t, err)
	require.True(t, txnResp.Succeeded)
	require.Equal(t, latest.Header.Revision, txnResp.Header.Revision)
	require.Len(t, txnResp.Responses, 1)
	nestedTxn := txnResp.Responses[0].GetResponseTxn()
	require.NotNil(t, nestedTxn)
	require.True(t, nestedTxn.Succeeded)
	require.Len(t, nestedTxn.Responses, 1)
	nestedGet := nestedTxn.Responses[0].GetResponseRange()
	require.NotNil(t, nestedGet)
	require.NotNil(t, nestedGet.Header)
	require.Equal(t, latest.Header.Revision, nestedGet.Header.Revision)
	require.Equal(t, int64(3), nestedGet.Count)
	require.True(t, nestedGet.More)
	require.Len(t, nestedGet.Kvs, 1)
	require.Equal(t, []byte("range/c"), nestedGet.Kvs[0].Key)
	require.Equal(t, []byte("old-range/c"), nestedGet.Kvs[0].Value)

	current, err := namespacedKV.Get(ctx, "range/a", clientv3.WithRange("range/d"))
	require.NoError(t, err)
	require.Equal(t, int64(2), current.Count)
	require.Equal(t, [][]byte{[]byte("range/a"), []byte("range/b")},
		[][]byte{current.Kvs[0].Key, current.Kvs[1].Key})
	outsideSameTenantRange, err := client.Get(ctx, "/a1698/namespace-nested-txn-historical-serializable-logical-range-last-create/tenant/range0/", clientv3.WithPrefix())
	require.NoError(t, err)
	require.Equal(t, int64(1), outsideSameTenantRange.Count)
	outsideTenant, err := client.Get(ctx, "/a1698/namespace-nested-txn-historical-serializable-logical-range-last-create/tenant0/", clientv3.WithPrefix())
	require.NoError(t, err)
	require.Equal(t, int64(1), outsideTenant.Count)
}

func TestClientNamespaceNestedTxnGetHistoricalSerializableLogicalRangeMinModRevisionReturnsLogicalKeys(t *testing.T) {
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
	tenantPrefix := "/a1699/namespace-nested-txn-historical-serializable-logical-range-min-mod-rev/tenant/"
	namespacedKV := namespace.NewKV(client.KV, tenantPrefix)
	_, err = client.Put(ctx, "/a1699/namespace-nested-txn-historical-serializable-logical-range-min-mod-rev/tenant/range0/outside", "same-tenant-outside-range")
	require.NoError(t, err)
	_, err = client.Put(ctx, "/a1699/namespace-nested-txn-historical-serializable-logical-range-min-mod-rev/tenant0/range/c", "outside-tenant")
	require.NoError(t, err)
	_, err = namespacedKV.Put(ctx, "range/a", "old-range/a")
	require.NoError(t, err)
	_, err = namespacedKV.Put(ctx, "range/b", "old-range/b")
	require.NoError(t, err)
	historical, err := namespacedKV.Put(ctx, "range/c", "old-range/c")
	require.NoError(t, err)
	_, err = namespacedKV.Delete(ctx, "range/c")
	require.NoError(t, err)
	_, err = namespacedKV.Put(ctx, "range/a", "new-range/a")
	require.NoError(t, err)
	_, err = namespacedKV.Put(ctx, "range/d", "outside-upper-bound")
	require.NoError(t, err)
	latest, err := namespacedKV.Put(ctx, "other/newer", "other")
	require.NoError(t, err)

	txnResp, err := namespacedKV.Txn(ctx).
		Then(clientv3.OpTxn(nil,
			[]clientv3.Op{clientv3.OpGet("range/a",
				clientv3.WithRange("range/d"),
				clientv3.WithRev(historical.Header.Revision),
				clientv3.WithSerializable(),
				clientv3.WithMinModRev(historical.Header.Revision),
				clientv3.WithSort(clientv3.SortByKey, clientv3.SortAscend))},
			nil)).
		Commit()
	require.NoError(t, err)
	require.True(t, txnResp.Succeeded)
	require.Equal(t, latest.Header.Revision, txnResp.Header.Revision)
	require.Len(t, txnResp.Responses, 1)
	nestedTxn := txnResp.Responses[0].GetResponseTxn()
	require.NotNil(t, nestedTxn)
	require.True(t, nestedTxn.Succeeded)
	require.Len(t, nestedTxn.Responses, 1)
	nestedGet := nestedTxn.Responses[0].GetResponseRange()
	require.NotNil(t, nestedGet)
	require.NotNil(t, nestedGet.Header)
	require.Equal(t, latest.Header.Revision, nestedGet.Header.Revision)
	require.Equal(t, int64(3), nestedGet.Count)
	require.False(t, nestedGet.More)
	require.Len(t, nestedGet.Kvs, 1)
	require.Equal(t, []byte("range/c"), nestedGet.Kvs[0].Key)
	require.Equal(t, []byte("old-range/c"), nestedGet.Kvs[0].Value)
	require.Equal(t, historical.Header.Revision, nestedGet.Kvs[0].ModRevision)

	current, err := namespacedKV.Get(ctx, "range/a", clientv3.WithRange("range/d"))
	require.NoError(t, err)
	require.Equal(t, int64(2), current.Count)
	require.Equal(t, [][]byte{[]byte("range/a"), []byte("range/b")},
		[][]byte{current.Kvs[0].Key, current.Kvs[1].Key})
	outsideSameTenantRange, err := client.Get(ctx, "/a1699/namespace-nested-txn-historical-serializable-logical-range-min-mod-rev/tenant/range0/", clientv3.WithPrefix())
	require.NoError(t, err)
	require.Equal(t, int64(1), outsideSameTenantRange.Count)
	outsideTenant, err := client.Get(ctx, "/a1699/namespace-nested-txn-historical-serializable-logical-range-min-mod-rev/tenant0/", clientv3.WithPrefix())
	require.NoError(t, err)
	require.Equal(t, int64(1), outsideTenant.Count)
}

func TestClientNamespaceNestedTxnGetHistoricalSerializableLogicalRangeMaxModRevisionReturnsLogicalKeys(t *testing.T) {
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
	tenantPrefix := "/a1700/namespace-nested-txn-historical-serializable-logical-range-max-mod-rev/tenant/"
	namespacedKV := namespace.NewKV(client.KV, tenantPrefix)
	_, err = client.Put(ctx, "/a1700/namespace-nested-txn-historical-serializable-logical-range-max-mod-rev/tenant/range0/outside", "same-tenant-outside-range")
	require.NoError(t, err)
	_, err = client.Put(ctx, "/a1700/namespace-nested-txn-historical-serializable-logical-range-max-mod-rev/tenant0/range/a", "outside-tenant")
	require.NoError(t, err)
	putA, err := namespacedKV.Put(ctx, "range/a", "old-range/a")
	require.NoError(t, err)
	putB, err := namespacedKV.Put(ctx, "range/b", "old-range/b")
	require.NoError(t, err)
	historical, err := namespacedKV.Put(ctx, "range/c", "old-range/c")
	require.NoError(t, err)
	_, err = namespacedKV.Delete(ctx, "range/c")
	require.NoError(t, err)
	_, err = namespacedKV.Put(ctx, "range/a", "new-range/a")
	require.NoError(t, err)
	_, err = namespacedKV.Put(ctx, "range/d", "outside-upper-bound")
	require.NoError(t, err)
	latest, err := namespacedKV.Put(ctx, "other/newer", "other")
	require.NoError(t, err)

	txnResp, err := namespacedKV.Txn(ctx).
		Then(clientv3.OpTxn(nil,
			[]clientv3.Op{clientv3.OpGet("range/a",
				clientv3.WithRange("range/d"),
				clientv3.WithRev(historical.Header.Revision),
				clientv3.WithSerializable(),
				clientv3.WithMaxModRev(historical.Header.Revision-1),
				clientv3.WithSort(clientv3.SortByKey, clientv3.SortAscend))},
			nil)).
		Commit()
	require.NoError(t, err)
	require.True(t, txnResp.Succeeded)
	require.Equal(t, latest.Header.Revision, txnResp.Header.Revision)
	require.Len(t, txnResp.Responses, 1)
	nestedTxn := txnResp.Responses[0].GetResponseTxn()
	require.NotNil(t, nestedTxn)
	require.True(t, nestedTxn.Succeeded)
	require.Len(t, nestedTxn.Responses, 1)
	nestedGet := nestedTxn.Responses[0].GetResponseRange()
	require.NotNil(t, nestedGet)
	require.NotNil(t, nestedGet.Header)
	require.Equal(t, latest.Header.Revision, nestedGet.Header.Revision)
	require.Equal(t, int64(3), nestedGet.Count)
	require.False(t, nestedGet.More)
	require.Len(t, nestedGet.Kvs, 2)
	require.Equal(t, [][]byte{[]byte("range/a"), []byte("range/b")},
		[][]byte{nestedGet.Kvs[0].Key, nestedGet.Kvs[1].Key})
	require.Equal(t, [][]byte{[]byte("old-range/a"), []byte("old-range/b")},
		[][]byte{nestedGet.Kvs[0].Value, nestedGet.Kvs[1].Value})
	require.Equal(t, []int64{putA.Header.Revision, putB.Header.Revision},
		[]int64{nestedGet.Kvs[0].ModRevision, nestedGet.Kvs[1].ModRevision})

	current, err := namespacedKV.Get(ctx, "range/a", clientv3.WithRange("range/d"))
	require.NoError(t, err)
	require.Equal(t, int64(2), current.Count)
	require.Equal(t, [][]byte{[]byte("range/a"), []byte("range/b")},
		[][]byte{current.Kvs[0].Key, current.Kvs[1].Key})
	outsideSameTenantRange, err := client.Get(ctx, "/a1700/namespace-nested-txn-historical-serializable-logical-range-max-mod-rev/tenant/range0/", clientv3.WithPrefix())
	require.NoError(t, err)
	require.Equal(t, int64(1), outsideSameTenantRange.Count)
	outsideTenant, err := client.Get(ctx, "/a1700/namespace-nested-txn-historical-serializable-logical-range-max-mod-rev/tenant0/", clientv3.WithPrefix())
	require.NoError(t, err)
	require.Equal(t, int64(1), outsideTenant.Count)
}

func TestClientNamespaceNestedTxnGetHistoricalSerializableLogicalRangeMinCreateRevisionReturnsLogicalKeys(t *testing.T) {
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
	tenantPrefix := "/a1701/namespace-nested-txn-historical-serializable-logical-range-min-create-rev/tenant/"
	namespacedKV := namespace.NewKV(client.KV, tenantPrefix)
	_, err = client.Put(ctx, "/a1701/namespace-nested-txn-historical-serializable-logical-range-min-create-rev/tenant/range0/outside", "same-tenant-outside-range")
	require.NoError(t, err)
	_, err = client.Put(ctx, "/a1701/namespace-nested-txn-historical-serializable-logical-range-min-create-rev/tenant0/range/c", "outside-tenant")
	require.NoError(t, err)
	_, err = namespacedKV.Put(ctx, "range/a", "old-range/a")
	require.NoError(t, err)
	_, err = namespacedKV.Put(ctx, "range/b", "old-range/b")
	require.NoError(t, err)
	historical, err := namespacedKV.Put(ctx, "range/c", "old-range/c")
	require.NoError(t, err)
	_, err = namespacedKV.Delete(ctx, "range/c")
	require.NoError(t, err)
	_, err = namespacedKV.Put(ctx, "range/a", "new-range/a")
	require.NoError(t, err)
	_, err = namespacedKV.Put(ctx, "range/d", "outside-upper-bound")
	require.NoError(t, err)
	latest, err := namespacedKV.Put(ctx, "other/newer", "other")
	require.NoError(t, err)

	txnResp, err := namespacedKV.Txn(ctx).
		Then(clientv3.OpTxn(nil,
			[]clientv3.Op{clientv3.OpGet("range/a",
				clientv3.WithRange("range/d"),
				clientv3.WithRev(historical.Header.Revision),
				clientv3.WithSerializable(),
				clientv3.WithMinCreateRev(historical.Header.Revision),
				clientv3.WithSort(clientv3.SortByKey, clientv3.SortAscend))},
			nil)).
		Commit()
	require.NoError(t, err)
	require.True(t, txnResp.Succeeded)
	require.Equal(t, latest.Header.Revision, txnResp.Header.Revision)
	require.Len(t, txnResp.Responses, 1)
	nestedTxn := txnResp.Responses[0].GetResponseTxn()
	require.NotNil(t, nestedTxn)
	require.True(t, nestedTxn.Succeeded)
	require.Len(t, nestedTxn.Responses, 1)
	nestedGet := nestedTxn.Responses[0].GetResponseRange()
	require.NotNil(t, nestedGet)
	require.NotNil(t, nestedGet.Header)
	require.Equal(t, latest.Header.Revision, nestedGet.Header.Revision)
	require.Equal(t, int64(3), nestedGet.Count)
	require.False(t, nestedGet.More)
	require.Len(t, nestedGet.Kvs, 1)
	require.Equal(t, []byte("range/c"), nestedGet.Kvs[0].Key)
	require.Equal(t, []byte("old-range/c"), nestedGet.Kvs[0].Value)
	require.Equal(t, historical.Header.Revision, nestedGet.Kvs[0].CreateRevision)

	current, err := namespacedKV.Get(ctx, "range/a", clientv3.WithRange("range/d"))
	require.NoError(t, err)
	require.Equal(t, int64(2), current.Count)
	require.Equal(t, [][]byte{[]byte("range/a"), []byte("range/b")},
		[][]byte{current.Kvs[0].Key, current.Kvs[1].Key})
	outsideSameTenantRange, err := client.Get(ctx, "/a1701/namespace-nested-txn-historical-serializable-logical-range-min-create-rev/tenant/range0/", clientv3.WithPrefix())
	require.NoError(t, err)
	require.Equal(t, int64(1), outsideSameTenantRange.Count)
	outsideTenant, err := client.Get(ctx, "/a1701/namespace-nested-txn-historical-serializable-logical-range-min-create-rev/tenant0/", clientv3.WithPrefix())
	require.NoError(t, err)
	require.Equal(t, int64(1), outsideTenant.Count)
}

func TestClientNamespaceNestedTxnGetHistoricalSerializableLogicalRangeMaxCreateRevisionReturnsLogicalKeys(t *testing.T) {
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
	tenantPrefix := "/a1702/namespace-nested-txn-historical-serializable-logical-range-max-create-rev/tenant/"
	namespacedKV := namespace.NewKV(client.KV, tenantPrefix)
	_, err = client.Put(ctx, "/a1702/namespace-nested-txn-historical-serializable-logical-range-max-create-rev/tenant/range0/outside", "same-tenant-outside-range")
	require.NoError(t, err)
	_, err = client.Put(ctx, "/a1702/namespace-nested-txn-historical-serializable-logical-range-max-create-rev/tenant0/range/a", "outside-tenant")
	require.NoError(t, err)
	putA, err := namespacedKV.Put(ctx, "range/a", "old-range/a")
	require.NoError(t, err)
	putB, err := namespacedKV.Put(ctx, "range/b", "old-range/b")
	require.NoError(t, err)
	historical, err := namespacedKV.Put(ctx, "range/c", "old-range/c")
	require.NoError(t, err)
	_, err = namespacedKV.Delete(ctx, "range/c")
	require.NoError(t, err)
	_, err = namespacedKV.Put(ctx, "range/a", "new-range/a")
	require.NoError(t, err)
	_, err = namespacedKV.Put(ctx, "range/d", "outside-upper-bound")
	require.NoError(t, err)
	latest, err := namespacedKV.Put(ctx, "other/newer", "other")
	require.NoError(t, err)

	txnResp, err := namespacedKV.Txn(ctx).
		Then(clientv3.OpTxn(nil,
			[]clientv3.Op{clientv3.OpGet("range/a",
				clientv3.WithRange("range/d"),
				clientv3.WithRev(historical.Header.Revision),
				clientv3.WithSerializable(),
				clientv3.WithMaxCreateRev(historical.Header.Revision-1),
				clientv3.WithSort(clientv3.SortByKey, clientv3.SortAscend))},
			nil)).
		Commit()
	require.NoError(t, err)
	require.True(t, txnResp.Succeeded)
	require.Equal(t, latest.Header.Revision, txnResp.Header.Revision)
	require.Len(t, txnResp.Responses, 1)
	nestedTxn := txnResp.Responses[0].GetResponseTxn()
	require.NotNil(t, nestedTxn)
	require.True(t, nestedTxn.Succeeded)
	require.Len(t, nestedTxn.Responses, 1)
	nestedGet := nestedTxn.Responses[0].GetResponseRange()
	require.NotNil(t, nestedGet)
	require.NotNil(t, nestedGet.Header)
	require.Equal(t, latest.Header.Revision, nestedGet.Header.Revision)
	require.Equal(t, int64(3), nestedGet.Count)
	require.False(t, nestedGet.More)
	require.Len(t, nestedGet.Kvs, 2)
	require.Equal(t, [][]byte{[]byte("range/a"), []byte("range/b")},
		[][]byte{nestedGet.Kvs[0].Key, nestedGet.Kvs[1].Key})
	require.Equal(t, [][]byte{[]byte("old-range/a"), []byte("old-range/b")},
		[][]byte{nestedGet.Kvs[0].Value, nestedGet.Kvs[1].Value})
	require.Equal(t, []int64{putA.Header.Revision, putB.Header.Revision},
		[]int64{nestedGet.Kvs[0].CreateRevision, nestedGet.Kvs[1].CreateRevision})

	current, err := namespacedKV.Get(ctx, "range/a", clientv3.WithRange("range/d"))
	require.NoError(t, err)
	require.Equal(t, int64(2), current.Count)
	require.Equal(t, [][]byte{[]byte("range/a"), []byte("range/b")},
		[][]byte{current.Kvs[0].Key, current.Kvs[1].Key})
	outsideSameTenantRange, err := client.Get(ctx, "/a1702/namespace-nested-txn-historical-serializable-logical-range-max-create-rev/tenant/range0/", clientv3.WithPrefix())
	require.NoError(t, err)
	require.Equal(t, int64(1), outsideSameTenantRange.Count)
	outsideTenant, err := client.Get(ctx, "/a1702/namespace-nested-txn-historical-serializable-logical-range-max-create-rev/tenant0/", clientv3.WithPrefix())
	require.NoError(t, err)
	require.Equal(t, int64(1), outsideTenant.Count)
}

func TestClientNamespaceNestedTxnGetHistoricalSerializablePrefixSortByCreateRevisionDescReturnsLogicalPage(t *testing.T) {
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
	tenantPrefix := "/a1671/namespace-nested-txn-historical-serializable-prefix-create-sort/tenant/"
	namespacedKV := namespace.NewKV(client.KV, tenantPrefix)
	_, err = client.Put(ctx, "/a1671/namespace-nested-txn-historical-serializable-prefix-create-sort/tenant/items0/outside", "same-tenant-outside-prefix")
	require.NoError(t, err)
	_, err = client.Put(ctx, "/a1671/namespace-nested-txn-historical-serializable-prefix-create-sort/tenant0/items/b", "outside-tenant")
	require.NoError(t, err)
	for _, key := range []string{"items/a", "items/c"} {
		_, err = namespacedKV.Put(ctx, key, "value-"+key)
		require.NoError(t, err)
	}
	historical, err := namespacedKV.Put(ctx, "items/b", "value-items/b")
	require.NoError(t, err)
	_, err = namespacedKV.Delete(ctx, "items/b")
	require.NoError(t, err)
	latest, err := namespacedKV.Put(ctx, "items/d", "value-items/d")
	require.NoError(t, err)

	txnResp, err := namespacedKV.Txn(ctx).
		Then(clientv3.OpTxn(nil,
			[]clientv3.Op{clientv3.OpGet("items/",
				clientv3.WithPrefix(),
				clientv3.WithRev(historical.Header.Revision),
				clientv3.WithSerializable(),
				clientv3.WithLimit(2),
				clientv3.WithSort(clientv3.SortByCreateRevision, clientv3.SortDescend),
			)},
			nil)).
		Commit()
	require.NoError(t, err)
	require.True(t, txnResp.Succeeded)
	require.Equal(t, latest.Header.Revision, txnResp.Header.Revision)
	require.Len(t, txnResp.Responses, 1)
	nestedTxn := txnResp.Responses[0].GetResponseTxn()
	require.NotNil(t, nestedTxn)
	require.True(t, nestedTxn.Succeeded)
	require.Len(t, nestedTxn.Responses, 1)
	nestedGet := nestedTxn.Responses[0].GetResponseRange()
	require.NotNil(t, nestedGet)
	require.NotNil(t, nestedGet.Header)
	require.Equal(t, latest.Header.Revision, nestedGet.Header.Revision)
	require.Equal(t, int64(3), nestedGet.Count)
	require.True(t, nestedGet.More)
	require.Len(t, nestedGet.Kvs, 2)
	require.Equal(t, [][]byte{[]byte("items/b"), []byte("items/c")},
		[][]byte{nestedGet.Kvs[0].Key, nestedGet.Kvs[1].Key})
	require.Equal(t, [][]byte{[]byte("value-items/b"), []byte("value-items/c")},
		[][]byte{nestedGet.Kvs[0].Value, nestedGet.Kvs[1].Value})
	require.Greater(t, nestedGet.Kvs[0].CreateRevision, nestedGet.Kvs[1].CreateRevision)

	current, err := namespacedKV.Get(ctx, "items/", clientv3.WithPrefix())
	require.NoError(t, err)
	require.Equal(t, int64(3), current.Count)
	require.Equal(t, [][]byte{[]byte("items/a"), []byte("items/c"), []byte("items/d")},
		[][]byte{current.Kvs[0].Key, current.Kvs[1].Key, current.Kvs[2].Key})
	outsideSameTenantPrefix, err := client.Get(ctx, "/a1671/namespace-nested-txn-historical-serializable-prefix-create-sort/tenant/items0/", clientv3.WithPrefix())
	require.NoError(t, err)
	require.Equal(t, int64(1), outsideSameTenantPrefix.Count)
	outsideTenant, err := client.Get(ctx, "/a1671/namespace-nested-txn-historical-serializable-prefix-create-sort/tenant0/", clientv3.WithPrefix())
	require.NoError(t, err)
	require.Equal(t, int64(1), outsideTenant.Count)
}

func TestClientNamespaceNestedTxnGetHistoricalSerializablePrefixSortByModRevisionDescReturnsLogicalPage(t *testing.T) {
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
	tenantPrefix := "/a1672/namespace-nested-txn-historical-serializable-prefix-mod-sort/tenant/"
	namespacedKV := namespace.NewKV(client.KV, tenantPrefix)
	_, err = client.Put(ctx, "/a1672/namespace-nested-txn-historical-serializable-prefix-mod-sort/tenant/items0/outside", "same-tenant-outside-prefix")
	require.NoError(t, err)
	_, err = client.Put(ctx, "/a1672/namespace-nested-txn-historical-serializable-prefix-mod-sort/tenant0/items/c", "outside-tenant")
	require.NoError(t, err)
	for _, key := range []string{"items/a", "items/b", "items/c"} {
		_, err = namespacedKV.Put(ctx, key, "old-"+key)
		require.NoError(t, err)
	}
	_, err = namespacedKV.Put(ctx, "items/a", "updated-items/a")
	require.NoError(t, err)
	historical, err := namespacedKV.Put(ctx, "items/c", "historical-items/c")
	require.NoError(t, err)
	_, err = namespacedKV.Put(ctx, "items/a", "current-items/a")
	require.NoError(t, err)
	_, err = namespacedKV.Delete(ctx, "items/c")
	require.NoError(t, err)
	latest, err := namespacedKV.Put(ctx, "items/d", "value-items/d")
	require.NoError(t, err)

	txnResp, err := namespacedKV.Txn(ctx).
		Then(clientv3.OpTxn(nil,
			[]clientv3.Op{clientv3.OpGet("items/",
				clientv3.WithPrefix(),
				clientv3.WithRev(historical.Header.Revision),
				clientv3.WithSerializable(),
				clientv3.WithLimit(2),
				clientv3.WithSort(clientv3.SortByModRevision, clientv3.SortDescend),
			)},
			nil)).
		Commit()
	require.NoError(t, err)
	require.True(t, txnResp.Succeeded)
	require.Equal(t, latest.Header.Revision, txnResp.Header.Revision)
	require.Len(t, txnResp.Responses, 1)
	nestedTxn := txnResp.Responses[0].GetResponseTxn()
	require.NotNil(t, nestedTxn)
	require.True(t, nestedTxn.Succeeded)
	require.Len(t, nestedTxn.Responses, 1)
	nestedGet := nestedTxn.Responses[0].GetResponseRange()
	require.NotNil(t, nestedGet)
	require.NotNil(t, nestedGet.Header)
	require.Equal(t, latest.Header.Revision, nestedGet.Header.Revision)
	require.Equal(t, int64(3), nestedGet.Count)
	require.True(t, nestedGet.More)
	require.Len(t, nestedGet.Kvs, 2)
	require.Equal(t, [][]byte{[]byte("items/c"), []byte("items/a")},
		[][]byte{nestedGet.Kvs[0].Key, nestedGet.Kvs[1].Key})
	require.Equal(t, [][]byte{[]byte("historical-items/c"), []byte("updated-items/a")},
		[][]byte{nestedGet.Kvs[0].Value, nestedGet.Kvs[1].Value})
	require.Greater(t, nestedGet.Kvs[0].ModRevision, nestedGet.Kvs[1].ModRevision)

	current, err := namespacedKV.Get(ctx, "items/", clientv3.WithPrefix())
	require.NoError(t, err)
	require.Equal(t, int64(3), current.Count)
	require.Equal(t, [][]byte{[]byte("items/a"), []byte("items/b"), []byte("items/d")},
		[][]byte{current.Kvs[0].Key, current.Kvs[1].Key, current.Kvs[2].Key})
	outsideSameTenantPrefix, err := client.Get(ctx, "/a1672/namespace-nested-txn-historical-serializable-prefix-mod-sort/tenant/items0/", clientv3.WithPrefix())
	require.NoError(t, err)
	require.Equal(t, int64(1), outsideSameTenantPrefix.Count)
	outsideTenant, err := client.Get(ctx, "/a1672/namespace-nested-txn-historical-serializable-prefix-mod-sort/tenant0/", clientv3.WithPrefix())
	require.NoError(t, err)
	require.Equal(t, int64(1), outsideTenant.Count)
}

func TestClientNamespaceNestedTxnGetHistoricalSerializablePrefixSortByVersionDescReturnsLogicalPage(t *testing.T) {
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
	tenantPrefix := "/a1673/namespace-nested-txn-historical-serializable-prefix-version-sort/tenant/"
	namespacedKV := namespace.NewKV(client.KV, tenantPrefix)
	_, err = client.Put(ctx, "/a1673/namespace-nested-txn-historical-serializable-prefix-version-sort/tenant/items0/outside", "same-tenant-outside-prefix")
	require.NoError(t, err)
	_, err = client.Put(ctx, "/a1673/namespace-nested-txn-historical-serializable-prefix-version-sort/tenant0/items/c", "outside-tenant")
	require.NoError(t, err)
	for _, key := range []string{"items/a", "items/b", "items/c"} {
		_, err = namespacedKV.Put(ctx, key, "value-"+key)
		require.NoError(t, err)
	}
	_, err = namespacedKV.Put(ctx, "items/a", "updated-items/a")
	require.NoError(t, err)
	_, err = namespacedKV.Put(ctx, "items/c", "updated-items/c")
	require.NoError(t, err)
	historical, err := namespacedKV.Put(ctx, "items/c", "historical-items/c")
	require.NoError(t, err)
	_, err = namespacedKV.Put(ctx, "items/a", "current-items/a")
	require.NoError(t, err)
	_, err = namespacedKV.Delete(ctx, "items/c")
	require.NoError(t, err)
	latest, err := namespacedKV.Put(ctx, "items/d", "value-items/d")
	require.NoError(t, err)

	txnResp, err := namespacedKV.Txn(ctx).
		Then(clientv3.OpTxn(nil,
			[]clientv3.Op{clientv3.OpGet("items/",
				clientv3.WithPrefix(),
				clientv3.WithRev(historical.Header.Revision),
				clientv3.WithSerializable(),
				clientv3.WithLimit(2),
				clientv3.WithSort(clientv3.SortByVersion, clientv3.SortDescend),
			)},
			nil)).
		Commit()
	require.NoError(t, err)
	require.True(t, txnResp.Succeeded)
	require.Equal(t, latest.Header.Revision, txnResp.Header.Revision)
	require.Len(t, txnResp.Responses, 1)
	nestedTxn := txnResp.Responses[0].GetResponseTxn()
	require.NotNil(t, nestedTxn)
	require.True(t, nestedTxn.Succeeded)
	require.Len(t, nestedTxn.Responses, 1)
	nestedGet := nestedTxn.Responses[0].GetResponseRange()
	require.NotNil(t, nestedGet)
	require.NotNil(t, nestedGet.Header)
	require.Equal(t, latest.Header.Revision, nestedGet.Header.Revision)
	require.Equal(t, int64(3), nestedGet.Count)
	require.True(t, nestedGet.More)
	require.Len(t, nestedGet.Kvs, 2)
	require.Equal(t, [][]byte{[]byte("items/c"), []byte("items/a")},
		[][]byte{nestedGet.Kvs[0].Key, nestedGet.Kvs[1].Key})
	require.Equal(t, [][]byte{[]byte("historical-items/c"), []byte("updated-items/a")},
		[][]byte{nestedGet.Kvs[0].Value, nestedGet.Kvs[1].Value})
	require.Equal(t, []int64{3, 2}, []int64{nestedGet.Kvs[0].Version, nestedGet.Kvs[1].Version})

	current, err := namespacedKV.Get(ctx, "items/", clientv3.WithPrefix())
	require.NoError(t, err)
	require.Equal(t, int64(3), current.Count)
	require.Equal(t, [][]byte{[]byte("items/a"), []byte("items/b"), []byte("items/d")},
		[][]byte{current.Kvs[0].Key, current.Kvs[1].Key, current.Kvs[2].Key})
	outsideSameTenantPrefix, err := client.Get(ctx, "/a1673/namespace-nested-txn-historical-serializable-prefix-version-sort/tenant/items0/", clientv3.WithPrefix())
	require.NoError(t, err)
	require.Equal(t, int64(1), outsideSameTenantPrefix.Count)
	outsideTenant, err := client.Get(ctx, "/a1673/namespace-nested-txn-historical-serializable-prefix-version-sort/tenant0/", clientv3.WithPrefix())
	require.NoError(t, err)
	require.Equal(t, int64(1), outsideTenant.Count)
}

func TestClientNamespaceNestedTxnGetHistoricalSerializablePrefixSortByCreateRevisionReturnsLogicalPage(t *testing.T) {
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
	tenantPrefix := "/a1674/namespace-nested-txn-historical-serializable-prefix-create-sort-none/tenant/"
	namespacedKV := namespace.NewKV(client.KV, tenantPrefix)
	_, err = client.Put(ctx, "/a1674/namespace-nested-txn-historical-serializable-prefix-create-sort-none/tenant/items0/outside", "same-tenant-outside-prefix")
	require.NoError(t, err)
	_, err = client.Put(ctx, "/a1674/namespace-nested-txn-historical-serializable-prefix-create-sort-none/tenant0/items/a", "outside-tenant")
	require.NoError(t, err)
	for _, key := range []string{"items/b", "items/a"} {
		_, err = namespacedKV.Put(ctx, key, "value-"+key)
		require.NoError(t, err)
	}
	historical, err := namespacedKV.Put(ctx, "items/c", "value-items/c")
	require.NoError(t, err)
	_, err = namespacedKV.Delete(ctx, "items/b")
	require.NoError(t, err)
	latest, err := namespacedKV.Put(ctx, "items/d", "value-items/d")
	require.NoError(t, err)

	txnResp, err := namespacedKV.Txn(ctx).
		Then(clientv3.OpTxn(nil,
			[]clientv3.Op{clientv3.OpGet("items/",
				clientv3.WithPrefix(),
				clientv3.WithRev(historical.Header.Revision),
				clientv3.WithSerializable(),
				clientv3.WithLimit(2),
				clientv3.WithSort(clientv3.SortByCreateRevision, clientv3.SortNone),
			)},
			nil)).
		Commit()
	require.NoError(t, err)
	require.True(t, txnResp.Succeeded)
	require.Equal(t, latest.Header.Revision, txnResp.Header.Revision)
	require.Len(t, txnResp.Responses, 1)
	nestedTxn := txnResp.Responses[0].GetResponseTxn()
	require.NotNil(t, nestedTxn)
	require.True(t, nestedTxn.Succeeded)
	require.Len(t, nestedTxn.Responses, 1)
	nestedGet := nestedTxn.Responses[0].GetResponseRange()
	require.NotNil(t, nestedGet)
	require.NotNil(t, nestedGet.Header)
	require.Equal(t, latest.Header.Revision, nestedGet.Header.Revision)
	require.Equal(t, int64(3), nestedGet.Count)
	require.True(t, nestedGet.More)
	require.Len(t, nestedGet.Kvs, 2)
	require.Equal(t, [][]byte{[]byte("items/b"), []byte("items/a")},
		[][]byte{nestedGet.Kvs[0].Key, nestedGet.Kvs[1].Key})
	require.Equal(t, [][]byte{[]byte("value-items/b"), []byte("value-items/a")},
		[][]byte{nestedGet.Kvs[0].Value, nestedGet.Kvs[1].Value})
	require.Less(t, nestedGet.Kvs[0].CreateRevision, nestedGet.Kvs[1].CreateRevision)

	current, err := namespacedKV.Get(ctx, "items/", clientv3.WithPrefix())
	require.NoError(t, err)
	require.Equal(t, int64(3), current.Count)
	require.Equal(t, [][]byte{[]byte("items/a"), []byte("items/c"), []byte("items/d")},
		[][]byte{current.Kvs[0].Key, current.Kvs[1].Key, current.Kvs[2].Key})
	outsideSameTenantPrefix, err := client.Get(ctx, "/a1674/namespace-nested-txn-historical-serializable-prefix-create-sort-none/tenant/items0/", clientv3.WithPrefix())
	require.NoError(t, err)
	require.Equal(t, int64(1), outsideSameTenantPrefix.Count)
	outsideTenant, err := client.Get(ctx, "/a1674/namespace-nested-txn-historical-serializable-prefix-create-sort-none/tenant0/", clientv3.WithPrefix())
	require.NoError(t, err)
	require.Equal(t, int64(1), outsideTenant.Count)
}

func TestClientNamespaceNestedTxnGetHistoricalSerializablePrefixSortByModRevisionReturnsLogicalPage(t *testing.T) {
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
	tenantPrefix := "/a1675/namespace-nested-txn-historical-serializable-prefix-mod-sort-none/tenant/"
	namespacedKV := namespace.NewKV(client.KV, tenantPrefix)
	_, err = client.Put(ctx, "/a1675/namespace-nested-txn-historical-serializable-prefix-mod-sort-none/tenant/items0/outside", "same-tenant-outside-prefix")
	require.NoError(t, err)
	_, err = client.Put(ctx, "/a1675/namespace-nested-txn-historical-serializable-prefix-mod-sort-none/tenant0/items/a", "outside-tenant")
	require.NoError(t, err)
	for _, key := range []string{"items/c", "items/a", "items/b"} {
		_, err = namespacedKV.Put(ctx, key, "old-"+key)
		require.NoError(t, err)
	}
	_, err = namespacedKV.Put(ctx, "items/a", "updated-items/a")
	require.NoError(t, err)
	historical, err := namespacedKV.Put(ctx, "items/c", "historical-items/c")
	require.NoError(t, err)
	_, err = namespacedKV.Delete(ctx, "items/b")
	require.NoError(t, err)
	_, err = namespacedKV.Put(ctx, "items/c", "current-items/c")
	require.NoError(t, err)
	latest, err := namespacedKV.Put(ctx, "items/d", "value-items/d")
	require.NoError(t, err)

	txnResp, err := namespacedKV.Txn(ctx).
		Then(clientv3.OpTxn(nil,
			[]clientv3.Op{clientv3.OpGet("items/",
				clientv3.WithPrefix(),
				clientv3.WithRev(historical.Header.Revision),
				clientv3.WithSerializable(),
				clientv3.WithLimit(2),
				clientv3.WithSort(clientv3.SortByModRevision, clientv3.SortNone),
			)},
			nil)).
		Commit()
	require.NoError(t, err)
	require.True(t, txnResp.Succeeded)
	require.Equal(t, latest.Header.Revision, txnResp.Header.Revision)
	require.Len(t, txnResp.Responses, 1)
	nestedTxn := txnResp.Responses[0].GetResponseTxn()
	require.NotNil(t, nestedTxn)
	require.True(t, nestedTxn.Succeeded)
	require.Len(t, nestedTxn.Responses, 1)
	nestedGet := nestedTxn.Responses[0].GetResponseRange()
	require.NotNil(t, nestedGet)
	require.NotNil(t, nestedGet.Header)
	require.Equal(t, latest.Header.Revision, nestedGet.Header.Revision)
	require.Equal(t, int64(3), nestedGet.Count)
	require.True(t, nestedGet.More)
	require.Len(t, nestedGet.Kvs, 2)
	require.Equal(t, [][]byte{[]byte("items/b"), []byte("items/a")},
		[][]byte{nestedGet.Kvs[0].Key, nestedGet.Kvs[1].Key})
	require.Equal(t, [][]byte{[]byte("old-items/b"), []byte("updated-items/a")},
		[][]byte{nestedGet.Kvs[0].Value, nestedGet.Kvs[1].Value})
	require.Less(t, nestedGet.Kvs[0].ModRevision, nestedGet.Kvs[1].ModRevision)

	current, err := namespacedKV.Get(ctx, "items/", clientv3.WithPrefix())
	require.NoError(t, err)
	require.Equal(t, int64(3), current.Count)
	require.Equal(t, [][]byte{[]byte("items/a"), []byte("items/c"), []byte("items/d")},
		[][]byte{current.Kvs[0].Key, current.Kvs[1].Key, current.Kvs[2].Key})
	outsideSameTenantPrefix, err := client.Get(ctx, "/a1675/namespace-nested-txn-historical-serializable-prefix-mod-sort-none/tenant/items0/", clientv3.WithPrefix())
	require.NoError(t, err)
	require.Equal(t, int64(1), outsideSameTenantPrefix.Count)
	outsideTenant, err := client.Get(ctx, "/a1675/namespace-nested-txn-historical-serializable-prefix-mod-sort-none/tenant0/", clientv3.WithPrefix())
	require.NoError(t, err)
	require.Equal(t, int64(1), outsideTenant.Count)
}

func TestClientNamespaceNestedTxnGetHistoricalSerializablePrefixSortByVersionReturnsLogicalPage(t *testing.T) {
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
	tenantPrefix := "/a1676/namespace-nested-txn-historical-serializable-prefix-version-sort-none/tenant/"
	namespacedKV := namespace.NewKV(client.KV, tenantPrefix)
	_, err = client.Put(ctx, "/a1676/namespace-nested-txn-historical-serializable-prefix-version-sort-none/tenant/items0/outside", "same-tenant-outside-prefix")
	require.NoError(t, err)
	_, err = client.Put(ctx, "/a1676/namespace-nested-txn-historical-serializable-prefix-version-sort-none/tenant0/items/a", "outside-tenant")
	require.NoError(t, err)
	_, err = namespacedKV.Put(ctx, "items/a", "value-items/a")
	require.NoError(t, err)
	_, err = namespacedKV.Put(ctx, "items/c", "value-items/c")
	require.NoError(t, err)
	_, err = namespacedKV.Put(ctx, "items/c", "updated-items/c")
	require.NoError(t, err)
	_, err = namespacedKV.Put(ctx, "items/b", "value-items/b")
	require.NoError(t, err)
	_, err = namespacedKV.Put(ctx, "items/b", "updated-items/b")
	require.NoError(t, err)
	historical, err := namespacedKV.Put(ctx, "items/b", "historical-items/b")
	require.NoError(t, err)
	_, err = namespacedKV.Put(ctx, "items/a", "current-items/a")
	require.NoError(t, err)
	_, err = namespacedKV.Delete(ctx, "items/c")
	require.NoError(t, err)
	latest, err := namespacedKV.Put(ctx, "items/d", "value-items/d")
	require.NoError(t, err)

	txnResp, err := namespacedKV.Txn(ctx).
		Then(clientv3.OpTxn(nil,
			[]clientv3.Op{clientv3.OpGet("items/",
				clientv3.WithPrefix(),
				clientv3.WithRev(historical.Header.Revision),
				clientv3.WithSerializable(),
				clientv3.WithLimit(2),
				clientv3.WithSort(clientv3.SortByVersion, clientv3.SortNone),
			)},
			nil)).
		Commit()
	require.NoError(t, err)
	require.True(t, txnResp.Succeeded)
	require.Equal(t, latest.Header.Revision, txnResp.Header.Revision)
	require.Len(t, txnResp.Responses, 1)
	nestedTxn := txnResp.Responses[0].GetResponseTxn()
	require.NotNil(t, nestedTxn)
	require.True(t, nestedTxn.Succeeded)
	require.Len(t, nestedTxn.Responses, 1)
	nestedGet := nestedTxn.Responses[0].GetResponseRange()
	require.NotNil(t, nestedGet)
	require.NotNil(t, nestedGet.Header)
	require.Equal(t, latest.Header.Revision, nestedGet.Header.Revision)
	require.Equal(t, int64(3), nestedGet.Count)
	require.True(t, nestedGet.More)
	require.Len(t, nestedGet.Kvs, 2)
	require.Equal(t, [][]byte{[]byte("items/a"), []byte("items/c")},
		[][]byte{nestedGet.Kvs[0].Key, nestedGet.Kvs[1].Key})
	require.Equal(t, [][]byte{[]byte("value-items/a"), []byte("updated-items/c")},
		[][]byte{nestedGet.Kvs[0].Value, nestedGet.Kvs[1].Value})
	require.Equal(t, []int64{1, 2}, []int64{nestedGet.Kvs[0].Version, nestedGet.Kvs[1].Version})

	current, err := namespacedKV.Get(ctx, "items/", clientv3.WithPrefix())
	require.NoError(t, err)
	require.Equal(t, int64(3), current.Count)
	require.Equal(t, [][]byte{[]byte("items/a"), []byte("items/b"), []byte("items/d")},
		[][]byte{current.Kvs[0].Key, current.Kvs[1].Key, current.Kvs[2].Key})
	outsideSameTenantPrefix, err := client.Get(ctx, "/a1676/namespace-nested-txn-historical-serializable-prefix-version-sort-none/tenant/items0/", clientv3.WithPrefix())
	require.NoError(t, err)
	require.Equal(t, int64(1), outsideSameTenantPrefix.Count)
	outsideTenant, err := client.Get(ctx, "/a1676/namespace-nested-txn-historical-serializable-prefix-version-sort-none/tenant0/", clientv3.WithPrefix())
	require.NoError(t, err)
	require.Equal(t, int64(1), outsideTenant.Count)
}

func TestClientNamespaceNestedTxnGetEmptyStartRangeWithMinModRevisionReturnsLogicalKeys(t *testing.T) {
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
	tenantPrefix := "/a1571/namespace-nested-txn-empty-start-range-min-mod-rev/tenant/"
	namespacedKV := namespace.NewKV(client.KV, tenantPrefix)
	for _, key := range []string{"alpha/a", "items/a", "items/b"} {
		_, err = namespacedKV.Put(ctx, key, "old-"+key)
		require.NoError(t, err)
	}
	updateB, err := namespacedKV.Put(ctx, "items/b", "new-items/b")
	require.NoError(t, err)
	_, err = namespacedKV.Put(ctx, "z/final", "value-z/final")
	require.NoError(t, err)
	_, err = client.Put(ctx, "/a1571/namespace-nested-txn-empty-start-range-min-mod-rev/tenant0/items/b", "outside-tenant")
	require.NoError(t, err)

	txnResp, err := namespacedKV.Txn(ctx).
		Then(clientv3.OpTxn(nil,
			[]clientv3.Op{clientv3.OpGet("",
				clientv3.WithRange("z"),
				clientv3.WithMinModRev(updateB.Header.Revision),
				clientv3.WithSort(clientv3.SortByKey, clientv3.SortAscend),
			)},
			nil)).
		Commit()
	require.NoError(t, err)
	require.True(t, txnResp.Succeeded)
	require.Len(t, txnResp.Responses, 1)
	nestedTxn := txnResp.Responses[0].GetResponseTxn()
	require.NotNil(t, nestedTxn)
	require.True(t, nestedTxn.Succeeded)
	require.Len(t, nestedTxn.Responses, 1)
	nestedGet := nestedTxn.Responses[0].GetResponseRange()
	require.NotNil(t, nestedGet)
	require.Equal(t, int64(3), nestedGet.Count)
	require.False(t, nestedGet.More)
	require.Len(t, nestedGet.Kvs, 1)
	require.Equal(t, []byte("items/b"), nestedGet.Kvs[0].Key)
	require.Equal(t, []byte("new-items/b"), nestedGet.Kvs[0].Value)

	outsideTenant, err := client.Get(ctx, "/a1571/namespace-nested-txn-empty-start-range-min-mod-rev/tenant0/", clientv3.WithPrefix())
	require.NoError(t, err)
	require.Equal(t, int64(1), outsideTenant.Count)
}

func TestClientNamespaceNestedTxnGetEmptyStartRangeWithMaxModRevisionReturnsLogicalKeys(t *testing.T) {
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
	tenantPrefix := "/a1572/namespace-nested-txn-empty-start-range-max-mod-rev/tenant/"
	namespacedKV := namespace.NewKV(client.KV, tenantPrefix)
	for _, key := range []string{"alpha/a", "items/a", "items/b"} {
		_, err = namespacedKV.Put(ctx, key, "old-"+key)
		require.NoError(t, err)
	}
	updateB, err := namespacedKV.Put(ctx, "items/b", "new-items/b")
	require.NoError(t, err)
	_, err = namespacedKV.Put(ctx, "z/final", "value-z/final")
	require.NoError(t, err)
	_, err = client.Put(ctx, "/a1572/namespace-nested-txn-empty-start-range-max-mod-rev/tenant0/items/a", "outside-tenant")
	require.NoError(t, err)

	txnResp, err := namespacedKV.Txn(ctx).
		Then(clientv3.OpTxn(nil,
			[]clientv3.Op{clientv3.OpGet("",
				clientv3.WithRange("z"),
				clientv3.WithMaxModRev(updateB.Header.Revision-1),
				clientv3.WithSort(clientv3.SortByKey, clientv3.SortAscend),
			)},
			nil)).
		Commit()
	require.NoError(t, err)
	require.True(t, txnResp.Succeeded)
	require.Len(t, txnResp.Responses, 1)
	nestedTxn := txnResp.Responses[0].GetResponseTxn()
	require.NotNil(t, nestedTxn)
	require.True(t, nestedTxn.Succeeded)
	require.Len(t, nestedTxn.Responses, 1)
	nestedGet := nestedTxn.Responses[0].GetResponseRange()
	require.NotNil(t, nestedGet)
	require.Equal(t, int64(3), nestedGet.Count)
	require.False(t, nestedGet.More)
	require.Len(t, nestedGet.Kvs, 2)
	require.Equal(t, [][]byte{[]byte("alpha/a"), []byte("items/a")},
		[][]byte{nestedGet.Kvs[0].Key, nestedGet.Kvs[1].Key})
	require.Equal(t, [][]byte{[]byte("old-alpha/a"), []byte("old-items/a")},
		[][]byte{nestedGet.Kvs[0].Value, nestedGet.Kvs[1].Value})

	outsideTenant, err := client.Get(ctx, "/a1572/namespace-nested-txn-empty-start-range-max-mod-rev/tenant0/", clientv3.WithPrefix())
	require.NoError(t, err)
	require.Equal(t, int64(1), outsideTenant.Count)
}

func TestClientNamespaceNestedTxnGetEmptyStartRangeWithMinCreateRevisionReturnsLogicalKeys(t *testing.T) {
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
	tenantPrefix := "/a1573/namespace-nested-txn-empty-start-range-min-create-rev/tenant/"
	namespacedKV := namespace.NewKV(client.KV, tenantPrefix)
	for _, key := range []string{"alpha/a", "items/a"} {
		_, err = namespacedKV.Put(ctx, key, "value-"+key)
		require.NoError(t, err)
	}
	createB, err := namespacedKV.Put(ctx, "items/b", "value-items/b")
	require.NoError(t, err)
	_, err = namespacedKV.Put(ctx, "alpha/a", "updated-alpha/a")
	require.NoError(t, err)
	_, err = namespacedKV.Put(ctx, "z/final", "value-z/final")
	require.NoError(t, err)
	_, err = client.Put(ctx, "/a1573/namespace-nested-txn-empty-start-range-min-create-rev/tenant0/items/b", "outside-tenant")
	require.NoError(t, err)

	txnResp, err := namespacedKV.Txn(ctx).
		Then(clientv3.OpTxn(nil,
			[]clientv3.Op{clientv3.OpGet("",
				clientv3.WithRange("z"),
				clientv3.WithMinCreateRev(createB.Header.Revision),
				clientv3.WithSort(clientv3.SortByKey, clientv3.SortAscend),
			)},
			nil)).
		Commit()
	require.NoError(t, err)
	require.True(t, txnResp.Succeeded)
	require.Len(t, txnResp.Responses, 1)
	nestedTxn := txnResp.Responses[0].GetResponseTxn()
	require.NotNil(t, nestedTxn)
	require.True(t, nestedTxn.Succeeded)
	require.Len(t, nestedTxn.Responses, 1)
	nestedGet := nestedTxn.Responses[0].GetResponseRange()
	require.NotNil(t, nestedGet)
	require.Equal(t, int64(3), nestedGet.Count)
	require.False(t, nestedGet.More)
	require.Len(t, nestedGet.Kvs, 1)
	require.Equal(t, []byte("items/b"), nestedGet.Kvs[0].Key)
	require.Equal(t, []byte("value-items/b"), nestedGet.Kvs[0].Value)

	outsideTenant, err := client.Get(ctx, "/a1573/namespace-nested-txn-empty-start-range-min-create-rev/tenant0/", clientv3.WithPrefix())
	require.NoError(t, err)
	require.Equal(t, int64(1), outsideTenant.Count)
}

func TestClientNamespaceNestedTxnGetEmptyStartRangeWithMaxCreateRevisionReturnsLogicalKeys(t *testing.T) {
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
	tenantPrefix := "/a1574/namespace-nested-txn-empty-start-range-max-create-rev/tenant/"
	namespacedKV := namespace.NewKV(client.KV, tenantPrefix)
	for _, key := range []string{"alpha/a", "items/a"} {
		_, err = namespacedKV.Put(ctx, key, "value-"+key)
		require.NoError(t, err)
	}
	createB, err := namespacedKV.Put(ctx, "items/b", "value-items/b")
	require.NoError(t, err)
	_, err = namespacedKV.Put(ctx, "alpha/a", "updated-alpha/a")
	require.NoError(t, err)
	_, err = namespacedKV.Put(ctx, "z/final", "value-z/final")
	require.NoError(t, err)
	_, err = client.Put(ctx, "/a1574/namespace-nested-txn-empty-start-range-max-create-rev/tenant0/items/a", "outside-tenant")
	require.NoError(t, err)

	txnResp, err := namespacedKV.Txn(ctx).
		Then(clientv3.OpTxn(nil,
			[]clientv3.Op{clientv3.OpGet("",
				clientv3.WithRange("z"),
				clientv3.WithMaxCreateRev(createB.Header.Revision-1),
				clientv3.WithSort(clientv3.SortByKey, clientv3.SortAscend),
			)},
			nil)).
		Commit()
	require.NoError(t, err)
	require.True(t, txnResp.Succeeded)
	require.Len(t, txnResp.Responses, 1)
	nestedTxn := txnResp.Responses[0].GetResponseTxn()
	require.NotNil(t, nestedTxn)
	require.True(t, nestedTxn.Succeeded)
	require.Len(t, nestedTxn.Responses, 1)
	nestedGet := nestedTxn.Responses[0].GetResponseRange()
	require.NotNil(t, nestedGet)
	require.Equal(t, int64(3), nestedGet.Count)
	require.False(t, nestedGet.More)
	require.Len(t, nestedGet.Kvs, 2)
	require.Equal(t, [][]byte{[]byte("alpha/a"), []byte("items/a")},
		[][]byte{nestedGet.Kvs[0].Key, nestedGet.Kvs[1].Key})
	require.Equal(t, [][]byte{[]byte("updated-alpha/a"), []byte("value-items/a")},
		[][]byte{nestedGet.Kvs[0].Value, nestedGet.Kvs[1].Value})

	outsideTenant, err := client.Get(ctx, "/a1574/namespace-nested-txn-empty-start-range-max-create-rev/tenant0/", clientv3.WithPrefix())
	require.NoError(t, err)
	require.Equal(t, int64(1), outsideTenant.Count)
}

func TestClientNamespaceNestedTxnGetEmptyStartRangeWithFirstKeyReturnsLogicalKey(t *testing.T) {
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
	tenantPrefix := "/a1575/namespace-nested-txn-empty-start-range-first-key/tenant/"
	namespacedKV := namespace.NewKV(client.KV, tenantPrefix)
	for _, key := range []string{"alpha/a", "items/a", "z/final"} {
		_, err = namespacedKV.Put(ctx, key, "value-"+key)
		require.NoError(t, err)
	}
	_, err = client.Put(ctx, "/a1575/namespace-nested-txn-empty-start-range-first-key/tenant0/alpha/a", "outside-tenant")
	require.NoError(t, err)

	getOpts := append(clientv3.WithFirstKey(), clientv3.WithRange("z"))
	txnResp, err := namespacedKV.Txn(ctx).
		Then(clientv3.OpTxn(nil,
			[]clientv3.Op{clientv3.OpGet("", getOpts...)},
			nil)).
		Commit()
	require.NoError(t, err)
	require.True(t, txnResp.Succeeded)
	require.Len(t, txnResp.Responses, 1)
	nestedTxn := txnResp.Responses[0].GetResponseTxn()
	require.NotNil(t, nestedTxn)
	require.True(t, nestedTxn.Succeeded)
	require.Len(t, nestedTxn.Responses, 1)
	nestedGet := nestedTxn.Responses[0].GetResponseRange()
	require.NotNil(t, nestedGet)
	require.Equal(t, int64(2), nestedGet.Count)
	require.True(t, nestedGet.More)
	require.Len(t, nestedGet.Kvs, 1)
	require.Equal(t, []byte("alpha/a"), nestedGet.Kvs[0].Key)
	require.Equal(t, []byte("value-alpha/a"), nestedGet.Kvs[0].Value)

	outsideTenant, err := client.Get(ctx, "/a1575/namespace-nested-txn-empty-start-range-first-key/tenant0/", clientv3.WithPrefix())
	require.NoError(t, err)
	require.Equal(t, int64(1), outsideTenant.Count)
}

func TestClientNamespaceNestedTxnGetEmptyStartRangeWithLastKeyReturnsLogicalKey(t *testing.T) {
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
	tenantPrefix := "/a1576/namespace-nested-txn-empty-start-range-last-key/tenant/"
	namespacedKV := namespace.NewKV(client.KV, tenantPrefix)
	for _, key := range []string{"alpha/a", "items/a", "z/final"} {
		_, err = namespacedKV.Put(ctx, key, "value-"+key)
		require.NoError(t, err)
	}
	_, err = client.Put(ctx, "/a1576/namespace-nested-txn-empty-start-range-last-key/tenant0/items/z", "outside-tenant")
	require.NoError(t, err)

	getOpts := append(clientv3.WithLastKey(), clientv3.WithRange("z"))
	txnResp, err := namespacedKV.Txn(ctx).
		Then(clientv3.OpTxn(nil,
			[]clientv3.Op{clientv3.OpGet("", getOpts...)},
			nil)).
		Commit()
	require.NoError(t, err)
	require.True(t, txnResp.Succeeded)
	require.Len(t, txnResp.Responses, 1)
	nestedTxn := txnResp.Responses[0].GetResponseTxn()
	require.NotNil(t, nestedTxn)
	require.True(t, nestedTxn.Succeeded)
	require.Len(t, nestedTxn.Responses, 1)
	nestedGet := nestedTxn.Responses[0].GetResponseRange()
	require.NotNil(t, nestedGet)
	require.Equal(t, int64(2), nestedGet.Count)
	require.True(t, nestedGet.More)
	require.Len(t, nestedGet.Kvs, 1)
	require.Equal(t, []byte("items/a"), nestedGet.Kvs[0].Key)
	require.Equal(t, []byte("value-items/a"), nestedGet.Kvs[0].Value)

	outsideTenant, err := client.Get(ctx, "/a1576/namespace-nested-txn-empty-start-range-last-key/tenant0/", clientv3.WithPrefix())
	require.NoError(t, err)
	require.Equal(t, int64(1), outsideTenant.Count)
}

func TestClientNamespaceNestedTxnGetEmptyStartRangeWithFirstCreateReturnsLogicalKey(t *testing.T) {
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
	tenantPrefix := "/a1577/namespace-nested-txn-empty-start-range-first-create/tenant/"
	namespacedKV := namespace.NewKV(client.KV, tenantPrefix)
	for _, key := range []string{"items/a", "alpha/a", "z/final"} {
		_, err = namespacedKV.Put(ctx, key, "value-"+key)
		require.NoError(t, err)
	}
	_, err = client.Put(ctx, "/a1577/namespace-nested-txn-empty-start-range-first-create/tenant0/alpha/older", "outside-tenant")
	require.NoError(t, err)

	getOpts := append(clientv3.WithFirstCreate(), clientv3.WithRange("z"))
	txnResp, err := namespacedKV.Txn(ctx).
		Then(clientv3.OpTxn(nil,
			[]clientv3.Op{clientv3.OpGet("", getOpts...)},
			nil)).
		Commit()
	require.NoError(t, err)
	require.True(t, txnResp.Succeeded)
	require.Len(t, txnResp.Responses, 1)
	nestedTxn := txnResp.Responses[0].GetResponseTxn()
	require.NotNil(t, nestedTxn)
	require.True(t, nestedTxn.Succeeded)
	require.Len(t, nestedTxn.Responses, 1)
	nestedGet := nestedTxn.Responses[0].GetResponseRange()
	require.NotNil(t, nestedGet)
	require.Equal(t, int64(2), nestedGet.Count)
	require.True(t, nestedGet.More)
	require.Len(t, nestedGet.Kvs, 1)
	require.Equal(t, []byte("items/a"), nestedGet.Kvs[0].Key)
	require.Equal(t, []byte("value-items/a"), nestedGet.Kvs[0].Value)

	outsideTenant, err := client.Get(ctx, "/a1577/namespace-nested-txn-empty-start-range-first-create/tenant0/", clientv3.WithPrefix())
	require.NoError(t, err)
	require.Equal(t, int64(1), outsideTenant.Count)
}

func TestClientNamespaceNestedTxnGetEmptyStartRangeWithLastCreateReturnsLogicalKey(t *testing.T) {
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
	tenantPrefix := "/a1578/namespace-nested-txn-empty-start-range-last-create/tenant/"
	namespacedKV := namespace.NewKV(client.KV, tenantPrefix)
	for _, key := range []string{"items/a", "alpha/a", "z/final"} {
		_, err = namespacedKV.Put(ctx, key, "value-"+key)
		require.NoError(t, err)
	}
	_, err = client.Put(ctx, "/a1578/namespace-nested-txn-empty-start-range-last-create/tenant0/items/newer", "outside-tenant")
	require.NoError(t, err)

	getOpts := append(clientv3.WithLastCreate(), clientv3.WithRange("z"))
	txnResp, err := namespacedKV.Txn(ctx).
		Then(clientv3.OpTxn(nil,
			[]clientv3.Op{clientv3.OpGet("", getOpts...)},
			nil)).
		Commit()
	require.NoError(t, err)
	require.True(t, txnResp.Succeeded)
	require.Len(t, txnResp.Responses, 1)
	nestedTxn := txnResp.Responses[0].GetResponseTxn()
	require.NotNil(t, nestedTxn)
	require.True(t, nestedTxn.Succeeded)
	require.Len(t, nestedTxn.Responses, 1)
	nestedGet := nestedTxn.Responses[0].GetResponseRange()
	require.NotNil(t, nestedGet)
	require.Equal(t, int64(2), nestedGet.Count)
	require.True(t, nestedGet.More)
	require.Len(t, nestedGet.Kvs, 1)
	require.Equal(t, []byte("alpha/a"), nestedGet.Kvs[0].Key)
	require.Equal(t, []byte("value-alpha/a"), nestedGet.Kvs[0].Value)

	outsideTenant, err := client.Get(ctx, "/a1578/namespace-nested-txn-empty-start-range-last-create/tenant0/", clientv3.WithPrefix())
	require.NoError(t, err)
	require.Equal(t, int64(1), outsideTenant.Count)
}

func TestClientNamespaceNestedTxnGetEmptyStartRangeWithFirstRevReturnsLogicalKey(t *testing.T) {
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
	tenantPrefix := "/a1579/namespace-nested-txn-empty-start-range-first-rev/tenant/"
	namespacedKV := namespace.NewKV(client.KV, tenantPrefix)
	for _, key := range []string{"items/a", "alpha/a", "z/final"} {
		_, err = namespacedKV.Put(ctx, key, "value-"+key)
		require.NoError(t, err)
	}
	_, err = namespacedKV.Put(ctx, "alpha/a", "updated-alpha/a")
	require.NoError(t, err)
	_, err = client.Put(ctx, "/a1579/namespace-nested-txn-empty-start-range-first-rev/tenant0/items/older", "outside-tenant")
	require.NoError(t, err)

	getOpts := append(clientv3.WithFirstRev(), clientv3.WithRange("z"))
	txnResp, err := namespacedKV.Txn(ctx).
		Then(clientv3.OpTxn(nil,
			[]clientv3.Op{clientv3.OpGet("", getOpts...)},
			nil)).
		Commit()
	require.NoError(t, err)
	require.True(t, txnResp.Succeeded)
	require.Len(t, txnResp.Responses, 1)
	nestedTxn := txnResp.Responses[0].GetResponseTxn()
	require.NotNil(t, nestedTxn)
	require.True(t, nestedTxn.Succeeded)
	require.Len(t, nestedTxn.Responses, 1)
	nestedGet := nestedTxn.Responses[0].GetResponseRange()
	require.NotNil(t, nestedGet)
	require.Equal(t, int64(2), nestedGet.Count)
	require.True(t, nestedGet.More)
	require.Len(t, nestedGet.Kvs, 1)
	require.Equal(t, []byte("items/a"), nestedGet.Kvs[0].Key)
	require.Equal(t, []byte("value-items/a"), nestedGet.Kvs[0].Value)

	outsideTenant, err := client.Get(ctx, "/a1579/namespace-nested-txn-empty-start-range-first-rev/tenant0/", clientv3.WithPrefix())
	require.NoError(t, err)
	require.Equal(t, int64(1), outsideTenant.Count)
}

func TestClientNamespaceNestedTxnGetEmptyStartRangeSortByCreateRevisionDescReturnsLogicalPage(t *testing.T) {
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
	tenantPrefix := "/a1580/namespace-nested-txn-empty-start-range-create-sort/tenant/"
	namespacedKV := namespace.NewKV(client.KV, tenantPrefix)
	for _, key := range []string{"items/a", "alpha/a", "items/b", "z/final"} {
		_, err = namespacedKV.Put(ctx, key, "value-"+key)
		require.NoError(t, err)
	}
	_, err = client.Put(ctx, "/a1580/namespace-nested-txn-empty-start-range-create-sort/tenant0/items/newer", "outside-tenant")
	require.NoError(t, err)

	txnResp, err := namespacedKV.Txn(ctx).
		Then(clientv3.OpTxn(nil,
			[]clientv3.Op{clientv3.OpGet("",
				clientv3.WithRange("z"),
				clientv3.WithLimit(2),
				clientv3.WithSort(clientv3.SortByCreateRevision, clientv3.SortDescend),
			)},
			nil)).
		Commit()
	require.NoError(t, err)
	require.True(t, txnResp.Succeeded)
	require.Len(t, txnResp.Responses, 1)
	nestedTxn := txnResp.Responses[0].GetResponseTxn()
	require.NotNil(t, nestedTxn)
	require.True(t, nestedTxn.Succeeded)
	require.Len(t, nestedTxn.Responses, 1)
	nestedGet := nestedTxn.Responses[0].GetResponseRange()
	require.NotNil(t, nestedGet)
	require.Equal(t, int64(3), nestedGet.Count)
	require.True(t, nestedGet.More)
	require.Len(t, nestedGet.Kvs, 2)
	require.Equal(t, [][]byte{[]byte("items/b"), []byte("alpha/a")},
		[][]byte{nestedGet.Kvs[0].Key, nestedGet.Kvs[1].Key})
	require.Equal(t, [][]byte{[]byte("value-items/b"), []byte("value-alpha/a")},
		[][]byte{nestedGet.Kvs[0].Value, nestedGet.Kvs[1].Value})

	outsideTenant, err := client.Get(ctx, "/a1580/namespace-nested-txn-empty-start-range-create-sort/tenant0/", clientv3.WithPrefix())
	require.NoError(t, err)
	require.Equal(t, int64(1), outsideTenant.Count)
}

func TestClientNamespaceNestedTxnGetEmptyStartRangeSortByModRevisionDescReturnsLogicalPage(t *testing.T) {
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
	tenantPrefix := "/a1581/namespace-nested-txn-empty-start-range-mod-sort/tenant/"
	namespacedKV := namespace.NewKV(client.KV, tenantPrefix)
	for _, key := range []string{"items/a", "alpha/a", "items/b", "z/final"} {
		_, err = namespacedKV.Put(ctx, key, "value-"+key)
		require.NoError(t, err)
	}
	_, err = namespacedKV.Put(ctx, "items/a", "updated-items/a")
	require.NoError(t, err)
	_, err = namespacedKV.Put(ctx, "alpha/a", "updated-alpha/a")
	require.NoError(t, err)
	_, err = client.Put(ctx, "/a1581/namespace-nested-txn-empty-start-range-mod-sort/tenant0/items/newer", "outside-tenant")
	require.NoError(t, err)

	txnResp, err := namespacedKV.Txn(ctx).
		Then(clientv3.OpTxn(nil,
			[]clientv3.Op{clientv3.OpGet("",
				clientv3.WithRange("z"),
				clientv3.WithLimit(2),
				clientv3.WithSort(clientv3.SortByModRevision, clientv3.SortDescend),
			)},
			nil)).
		Commit()
	require.NoError(t, err)
	require.True(t, txnResp.Succeeded)
	require.Len(t, txnResp.Responses, 1)
	nestedTxn := txnResp.Responses[0].GetResponseTxn()
	require.NotNil(t, nestedTxn)
	require.True(t, nestedTxn.Succeeded)
	require.Len(t, nestedTxn.Responses, 1)
	nestedGet := nestedTxn.Responses[0].GetResponseRange()
	require.NotNil(t, nestedGet)
	require.Equal(t, int64(3), nestedGet.Count)
	require.True(t, nestedGet.More)
	require.Len(t, nestedGet.Kvs, 2)
	require.Equal(t, [][]byte{[]byte("alpha/a"), []byte("items/a")},
		[][]byte{nestedGet.Kvs[0].Key, nestedGet.Kvs[1].Key})
	require.Equal(t, [][]byte{[]byte("updated-alpha/a"), []byte("updated-items/a")},
		[][]byte{nestedGet.Kvs[0].Value, nestedGet.Kvs[1].Value})

	outsideTenant, err := client.Get(ctx, "/a1581/namespace-nested-txn-empty-start-range-mod-sort/tenant0/", clientv3.WithPrefix())
	require.NoError(t, err)
	require.Equal(t, int64(1), outsideTenant.Count)
}

func TestClientNamespaceNestedTxnGetEmptyStartRangeSortByVersionDescReturnsLogicalPage(t *testing.T) {
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
	tenantPrefix := "/a1582/namespace-nested-txn-empty-start-range-version-sort/tenant/"
	namespacedKV := namespace.NewKV(client.KV, tenantPrefix)
	for _, key := range []string{"items/a", "alpha/a", "items/b", "z/final"} {
		_, err = namespacedKV.Put(ctx, key, "value-"+key)
		require.NoError(t, err)
	}
	_, err = namespacedKV.Put(ctx, "alpha/a", "updated-alpha/a")
	require.NoError(t, err)
	_, err = namespacedKV.Put(ctx, "items/b", "updated-items/b")
	require.NoError(t, err)
	_, err = namespacedKV.Put(ctx, "items/b", "final-items/b")
	require.NoError(t, err)
	_, err = client.Put(ctx, "/a1582/namespace-nested-txn-empty-start-range-version-sort/tenant0/items/newer", "outside-tenant")
	require.NoError(t, err)

	txnResp, err := namespacedKV.Txn(ctx).
		Then(clientv3.OpTxn(nil,
			[]clientv3.Op{clientv3.OpGet("",
				clientv3.WithRange("z"),
				clientv3.WithLimit(2),
				clientv3.WithSort(clientv3.SortByVersion, clientv3.SortDescend),
			)},
			nil)).
		Commit()
	require.NoError(t, err)
	require.True(t, txnResp.Succeeded)
	require.Len(t, txnResp.Responses, 1)
	nestedTxn := txnResp.Responses[0].GetResponseTxn()
	require.NotNil(t, nestedTxn)
	require.True(t, nestedTxn.Succeeded)
	require.Len(t, nestedTxn.Responses, 1)
	nestedGet := nestedTxn.Responses[0].GetResponseRange()
	require.NotNil(t, nestedGet)
	require.Equal(t, int64(3), nestedGet.Count)
	require.True(t, nestedGet.More)
	require.Len(t, nestedGet.Kvs, 2)
	require.Equal(t, [][]byte{[]byte("items/b"), []byte("alpha/a")},
		[][]byte{nestedGet.Kvs[0].Key, nestedGet.Kvs[1].Key})
	require.Equal(t, [][]byte{[]byte("final-items/b"), []byte("updated-alpha/a")},
		[][]byte{nestedGet.Kvs[0].Value, nestedGet.Kvs[1].Value})
	require.Equal(t, []int64{3, 2}, []int64{nestedGet.Kvs[0].Version, nestedGet.Kvs[1].Version})

	outsideTenant, err := client.Get(ctx, "/a1582/namespace-nested-txn-empty-start-range-version-sort/tenant0/", clientv3.WithPrefix())
	require.NoError(t, err)
	require.Equal(t, int64(1), outsideTenant.Count)
}

func TestClientNamespaceTxnGetValidationErrorsMatchEtcd(t *testing.T) {
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
	namespacedKV := namespace.NewKV(client.KV, "/a1449/namespace-txn/")
	invalidSort := []clientv3.OpOption{clientv3.WithSort(clientv3.SortTarget(99), clientv3.SortOrder(99))}

	resp, err := namespacedKV.Txn(ctx).Then(clientv3.OpGet("")).Commit()
	require.NoError(t, err)
	require.NotNil(t, resp)
	require.Len(t, resp.Responses, 1)
	require.Empty(t, resp.Responses[0].GetResponseRange().Kvs)

	_, err = namespacedKV.Txn(ctx).Then(clientv3.OpGet("", invalidSort...)).Commit()
	requireClientKVError(t, err, rpctypes.ErrInvalidSortOption, codes.Unknown, "etcdserver: invalid sort option")
}

func TestClientNamespaceDoGetValidationErrorsMatchEtcd(t *testing.T) {
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
	namespacedKV := namespace.NewKV(client.KV, "/a1451/namespace-do/")
	invalidSort := []clientv3.OpOption{clientv3.WithSort(clientv3.SortTarget(99), clientv3.SortOrder(99))}

	_, err = namespacedKV.Do(ctx, clientv3.OpGet("", invalidSort...))
	requireClientKVError(t, err, rpctypes.ErrEmptyKey, codes.Unknown, "etcdserver: key is not provided")
	_, err = namespacedKV.Do(ctx, clientv3.OpGet("", append(invalidSort, clientv3.WithPrefix())...))
	requireClientKVError(t, err, rpctypes.ErrInvalidSortOption, codes.Unknown, "etcdserver: invalid sort option")
}

func TestClientKVGetCanceledContextKeepsConnectionUsable(t *testing.T) {
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
	key := "/a1137/kv/get-cancel/key"
	_, err = client.Put(ctx, key, "value")
	require.NoError(t, err)
	activeConnection := client.ActiveConnection()

	canceledCtx, canceledCancel := context.WithCancel(ctx)
	canceledCancel()
	resp, err := client.Get(canceledCtx, key)
	require.Nil(t, resp)
	require.ErrorIs(t, err, context.Canceled)
	require.True(t, client.ActiveConnection() == activeConnection)

	got, err := client.Get(ctx, key)
	require.NoError(t, err)
	require.Len(t, got.Kvs, 1)
	require.Equal(t, "value", string(got.Kvs[0].Value))
}

func TestClientKVOperationsAfterCloseAreBoundedAndDoNotCommit(t *testing.T) {
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
	require.NoError(t, client.Close())

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	_, err = client.Get(ctx, "/a1125/closed/get")
	requireClientClosedError(t, err, "Get")
	_, err = client.Put(ctx, "/a1125/closed/put", "value")
	requireClientClosedError(t, err, "Put")
	_, err = client.Txn(ctx).Then(clientv3.OpGet("/a1125/closed/txn")).Commit()
	requireClientClosedError(t, err, "Txn")

	committed, err := server.Range(context.Background(), &etcdserverpb.RangeRequest{
		Key: []byte("/a1125/closed/put"),
	})
	require.NoError(t, err)
	require.Empty(t, committed.Kvs)
}

func requireClientClosedError(t *testing.T, err error, operation string) {
	t.Helper()
	require.Error(t, err, "%s after Close unexpectedly succeeded", operation)
	require.True(t,
		clientv3.IsConnCanceled(err) || errors.Is(err, context.DeadlineExceeded),
		"%s after Close error = %v", operation, err)
}

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

func TestClientRangeFromKeyReturnsWholeUserKeyspace(t *testing.T) {
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
	putRevisions := map[string]int64{}
	keySet := []string{"a", "b", "c", "c", "c", "foo", "foo/abc", "fop"}
	for _, key := range keySet {
		put, putErr := client.Put(ctx, key, "")
		require.NoError(t, putErr)
		putRevisions[key] = put.Header.Revision
	}
	single, err := client.Get(ctx, "a")
	require.NoError(t, err)
	require.Len(t, single.Kvs, 1)

	response, err := client.Get(ctx, "\x00",
		clientv3.WithFromKey(),
		clientv3.WithSort(clientv3.SortByKey, clientv3.SortAscend),
	)
	require.NoError(t, err)
	require.Equal(t, single.Header, response.Header)
	require.Equal(t, int64(6), response.Count)
	require.False(t, response.More)
	require.Equal(t, []string{"a", "b", "c", "foo", "foo/abc", "fop"}, rangeClientRelativeKeys(response.Kvs, ""))

	byKey := make(map[string]*mvccpb.KeyValue, len(response.Kvs))
	for _, kv := range response.Kvs {
		byKey[string(kv.Key)] = kv
		require.Empty(t, kv.Value)
	}
	require.Equal(t, putRevisions["a"], byKey["a"].CreateRevision)
	require.Equal(t, putRevisions["a"], byKey["a"].ModRevision)
	require.Equal(t, int64(1), byKey["a"].Version)
	require.Equal(t, putRevisions["c"]-2, byKey["c"].CreateRevision)
	require.Equal(t, putRevisions["c"], byKey["c"].ModRevision)
	require.Equal(t, int64(3), byKey["c"].Version)
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

func TestClientRangeCountOnlyLimitAcrossTombstones(t *testing.T) {
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
	prefix := fmt.Sprintf("/a1046/range-count-tombstone-client/%d/", time.Now().UnixNano())
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

	assertCountOnly := func(stage string, revision, wantCount int64) {
		t.Helper()
		options := []clientv3.OpOption{
			clientv3.WithPrefix(), clientv3.WithCountOnly(), clientv3.WithLimit(1),
		}
		if revision != 0 {
			options = append(options, clientv3.WithRev(revision))
		}
		response, getErr := client.Get(ctx, prefix, options...)
		require.NoError(t, getErr, stage)
		require.Equal(t, wantCount, response.Count, stage)
		require.Empty(t, response.Kvs, stage)
		require.False(t, response.More, stage)
	}
	assertCountOnly("before-deletes", beforeDeletes, 4)
	assertCountOnly("after-deletes", deleted.Header.Revision, 2)
	assertCountOnly("after-recreate", 0, 4)
}

func TestClientRangeCountOnlyTakesPrecedenceOverKeysOnly(t *testing.T) {
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
	prefix := fmt.Sprintf("/a1053/range-count-keys-client/%d/", time.Now().UnixNano())
	for _, key := range []string{"a", "b", "c"} {
		_, err = client.Put(ctx, prefix+key, "")
		require.NoError(t, err)
	}

	response, err := client.Get(
		ctx,
		prefix,
		clientv3.WithPrefix(),
		clientv3.WithKeysOnly(),
		clientv3.WithCountOnly(),
		clientv3.WithLimit(1),
	)
	require.NoError(t, err)
	require.NotNil(t, response.Header)
	require.Equal(t, int64(3), response.Count)
	require.Empty(t, response.Kvs)
	require.False(t, response.More)

	fullKeyspace, err := client.Get(
		ctx,
		"",
		clientv3.WithFromKey(),
		clientv3.WithKeysOnly(),
		clientv3.WithCountOnly(),
	)
	require.NoError(t, err)
	require.Equal(t, response.Header, fullKeyspace.Header)
	require.Equal(t, int64(3), fullKeyspace.Count)
	require.Empty(t, fullKeyspace.Kvs)
	require.False(t, fullKeyspace.More)
}

func TestClientRangeClientSideRecvLimitIsResourceExhausted(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()

	grpcServer := grpc.NewServer(server.ClientServerOptions()...)
	server.Register(grpcServer)
	listener := bufconn.Listen(1 << 20)
	go func() { _ = grpcServer.Serve(listener) }()
	t.Cleanup(grpcServer.Stop)

	newClient := func(maxRecv int) *clientv3.Client {
		t.Helper()
		cfg := clientv3.Config{
			Endpoints:   []string{"bufnet"},
			DialTimeout: time.Second,
			DialOptions: []grpc.DialOption{
				grpc.WithTransportCredentials(insecure.NewCredentials()),
				grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
					return listener.Dial()
				}),
			},
		}
		if maxRecv > 0 {
			cfg.MaxCallRecvMsgSize = maxRecv
		}
		client, err := clientv3.New(cfg)
		require.NoError(t, err)
		t.Cleanup(func() { require.NoError(t, client.Close()) })
		return client
	}

	writer := newClient(0)
	reader := newClient(512)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	key := "/a1165/range-client-side-recv-limit"
	_, err := writer.Put(ctx, key, strings.Repeat("a", 2048))
	require.NoError(t, err)

	resp, err := reader.Get(ctx, key)
	require.Nil(t, resp)
	requireClientRangeTransportLimitError(t, err, "received message larger than max")
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

func TestClientDeleteRangeBoundaryHighPrefixMatchesEtcd(t *testing.T) {
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
	tests := []struct {
		name              string
		deleteKeySuffix   string
		opts              func(prefix string) []clientv3.OpOption
		wantDeleted       int64
		wantAdvanced      bool
		wantPrevKeys      []string
		wantRemainingKeys []string
	}{
		{
			name: "from-key", deleteKeySuffix: "b",
			opts: func(string) []clientv3.OpOption {
				return []clientv3.OpOption{clientv3.WithFromKey(), clientv3.WithPrevKV()}
			},
			wantDeleted: 2, wantAdvanced: true, wantPrevKeys: []string{"b", "c"},
			wantRemainingKeys: []string{"a"},
		},
		{
			name: "equal-empty", deleteKeySuffix: "b",
			opts: func(prefix string) []clientv3.OpOption {
				return []clientv3.OpOption{clientv3.WithRange(prefix + "b"), clientv3.WithPrevKV()}
			},
			wantPrevKeys: []string{}, wantRemainingKeys: []string{"a", "b", "c"},
		},
		{
			name: "reverse-empty", deleteKeySuffix: "c",
			opts: func(prefix string) []clientv3.OpOption {
				return []clientv3.OpOption{clientv3.WithRange(prefix + "b"), clientv3.WithPrevKV()}
			},
			wantPrevKeys: []string{}, wantRemainingKeys: []string{"a", "b", "c"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			prefix := strings.Repeat("\xff", 64) +
				fmt.Sprintf("/a1043/delete-boundary/%d/%s/", time.Now().UnixNano(), tt.name)
			var lastPutRevision int64
			for _, suffix := range []string{"a", "b", "c"} {
				put, putErr := client.Put(ctx, prefix+suffix, "value-"+suffix)
				require.NoError(t, putErr)
				lastPutRevision = put.Header.Revision
			}

			deleted, deleteErr := client.Delete(ctx, prefix+tt.deleteKeySuffix, tt.opts(prefix)...)
			require.NoError(t, deleteErr)
			require.Equal(t, tt.wantDeleted, deleted.Deleted)
			require.Equal(t, tt.wantAdvanced, deleted.Header.Revision > lastPutRevision)
			require.Equal(t, tt.wantPrevKeys, rangeClientRelativeKeys(deleted.PrevKvs, prefix))
			for _, kv := range deleted.PrevKvs {
				require.Less(t, kv.ModRevision, deleted.Header.Revision)
			}

			remaining, getErr := client.Get(ctx, prefix, clientv3.WithPrefix())
			require.NoError(t, getErr)
			require.Equal(t, deleted.Header.Revision, remaining.Header.Revision)
			require.Equal(t, tt.wantRemainingKeys, rangeClientRelativeKeys(remaining.Kvs, prefix))
			for _, kv := range remaining.Kvs {
				require.Equal(t, "value-"+strings.TrimPrefix(string(kv.Key), prefix), string(kv.Value))
			}
		})
	}
}

func TestClientPrefixedNULRangeEndIsReverseEmptyRange(t *testing.T) {
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
	prefix := fmt.Sprintf("/a1102/prefixed-nul-range/%d/", time.Now().UnixNano())
	ownerPrefix := prefix + "owners/"
	dataKey := ownerPrefix + prefix + "data/m"
	rangeEnd := ownerPrefix + "\x00"
	for _, key := range []string{ownerPrefix + "sentinel", dataKey} {
		_, putErr := client.Put(ctx, key, "value")
		require.NoError(t, putErr)
	}

	ranged, err := client.Get(ctx, dataKey, clientv3.WithRange(rangeEnd))
	require.NoError(t, err)
	require.Zero(t, ranged.Count)
	require.Empty(t, ranged.Kvs)

	deleted, err := client.Delete(ctx, dataKey, clientv3.WithRange(rangeEnd), clientv3.WithPrevKV())
	require.NoError(t, err)
	require.Zero(t, deleted.Deleted)
	require.Empty(t, deleted.PrevKvs)

	remaining, err := client.Get(ctx, ownerPrefix, clientv3.WithPrefix())
	require.NoError(t, err)
	require.Equal(t, []string{prefix + "data/m", "sentinel"}, rangeClientRelativeKeys(remaining.Kvs, ownerPrefix))
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
	requireRawGRPCRangeRevisionError(t, err, codes.OutOfRange, "etcdserver: mvcc: required revision is a future revision", rpctypes.ErrGRPCFutureRev)

	selectedNegative := &etcdserverpb.TxnRequest{
		Success: []*etcdserverpb.RequestOp{rawGRPCRangeRequestOp(&etcdserverpb.RangeRequest{
			Key: key, Revision: -1,
		})},
	}
	_, err = kv.Txn(ctx, selectedNegative)
	requireRawGRPCRangeRevisionError(t, err, codes.OutOfRange, "etcdserver: mvcc: required revision has been compacted", rpctypes.ErrGRPCCompacted)

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
	requireRawGRPCRangeRevisionError(t, err, codes.OutOfRange, "etcdserver: mvcc: required revision is a future revision", rpctypes.ErrGRPCFutureRev)
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

	_, err = client.Get(ctx, key, clientv3.WithRev(current.Header.Revision-1))
	requireClientRangeError(t, err, codes.Unknown, "etcdserver: mvcc: required revision has been compacted", rpctypes.ErrCompacted)

	_, err = client.Get(ctx, key, clientv3.WithRev(math.MaxInt64))
	requireClientRangeError(t, err, codes.Unknown, "etcdserver: mvcc: required revision is a future revision", rpctypes.ErrFutureRev)
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

func requireRawGRPCRangeRevisionError(t *testing.T, err error, code codes.Code, message string, wantErrorIs ...error) {
	t.Helper()
	require.EqualError(t, err, status.Error(code, message).Error())
	for _, want := range wantErrorIs {
		require.ErrorIs(t, err, want)
	}
	require.Equal(t, code, status.Code(err))
	require.Equal(t, message, status.Convert(err).Message())
}

func requireClientRangeError(t *testing.T, err error, code codes.Code, message string, wantErrorIs ...error) {
	t.Helper()
	require.EqualError(t, err, message)
	for _, want := range wantErrorIs {
		require.ErrorIs(t, err, want)
	}
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

func requireClientRangeTransportLimitError(t *testing.T, err error, messageFragment string) {
	t.Helper()
	require.ErrorContains(t, err, messageFragment)
	require.Equal(t, codes.ResourceExhausted, status.Code(err))
	require.Contains(t, status.Convert(err).Message(), messageFragment)
	require.False(t, errors.Is(err, rpctypes.ErrRequestTooLarge))
}
