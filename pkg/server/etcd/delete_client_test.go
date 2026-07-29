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
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/api/v3/mvccpb"
	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
)

func TestClientDeleteRangeDifferentialScenario(t *testing.T) {
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

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	prefix := "/a1052/client-delete/"
	base, err := client.Get(ctx, prefix, clientv3.WithPrefix())
	require.NoError(t, err)
	baseRev := base.Header.Revision
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cleanupCancel()
		_, _ = client.Delete(cleanupCtx, prefix, clientv3.WithPrefix())
	})

	putA, err := client.Put(ctx, prefix+"a", "va")
	require.NoError(t, err)
	putB, err := client.Put(ctx, prefix+"b", "vb")
	require.NoError(t, err)
	putC, err := client.Put(ctx, prefix+"c", "vc")
	require.NoError(t, err)
	updateB, err := client.Put(ctx, prefix+"b", "vb2")
	require.NoError(t, err)
	require.Equal(t, []int64{1, 2, 3, 4}, []int64{
		putA.Header.Revision - baseRev,
		putB.Header.Revision - baseRev,
		putC.Header.Revision - baseRev,
		updateB.Header.Revision - baseRev,
	})

	deleted, err := client.Delete(ctx, prefix+"a",
		clientv3.WithRange(prefix+"c"),
		clientv3.WithPrevKV(),
	)
	require.NoError(t, err)
	require.Equal(t, int64(5), deleted.Header.Revision-baseRev)
	require.Equal(t, int64(2), deleted.Deleted)
	require.Equal(t, []deleteClientKV{
		{key: "a", value: "va", createRevision: putA.Header.Revision, modRevision: putA.Header.Revision, version: 1},
		{key: "b", value: "vb2", createRevision: putB.Header.Revision, modRevision: updateB.Header.Revision, version: 2},
	}, deleteClientKVs(deleted.PrevKvs, prefix))

	historical, err := client.Get(ctx, prefix,
		clientv3.WithPrefix(),
		clientv3.WithRev(deleted.Header.Revision-1),
		clientv3.WithSort(clientv3.SortByKey, clientv3.SortAscend),
	)
	require.NoError(t, err)
	require.Equal(t, []deleteClientKV{
		{key: "a", value: "va", createRevision: putA.Header.Revision, modRevision: putA.Header.Revision, version: 1},
		{key: "b", value: "vb2", createRevision: putB.Header.Revision, modRevision: updateB.Header.Revision, version: 2},
		{key: "c", value: "vc", createRevision: putC.Header.Revision, modRevision: putC.Header.Revision, version: 1},
	}, deleteClientKVs(historical.Kvs, prefix))

	current, err := client.Get(ctx, prefix,
		clientv3.WithPrefix(),
		clientv3.WithSort(clientv3.SortByKey, clientv3.SortAscend),
	)
	require.NoError(t, err)
	require.Equal(t, []deleteClientKV{
		{key: "c", value: "vc", createRevision: putC.Header.Revision, modRevision: putC.Header.Revision, version: 1},
	}, deleteClientKVs(current.Kvs, prefix))

	emptyRange, err := client.Delete(ctx, prefix+"c",
		clientv3.WithRange(prefix+"c"),
		clientv3.WithPrevKV(),
	)
	require.NoError(t, err)
	require.Equal(t, current.Header.Revision, emptyRange.Header.Revision)
	require.Zero(t, emptyRange.Deleted)
	require.Empty(t, emptyRange.PrevKvs)

	missing, err := client.Delete(ctx, prefix+"missing", clientv3.WithPrevKV())
	require.NoError(t, err)
	require.Equal(t, emptyRange.Header.Revision, missing.Header.Revision)
	require.Zero(t, missing.Deleted)
	require.Empty(t, missing.PrevKvs)

	final, err := client.Get(ctx, prefix, clientv3.WithPrefix())
	require.NoError(t, err)
	require.Equal(t, missing.Header.Revision, final.Header.Revision)
	require.Equal(t, []deleteClientKV{
		{key: "c", value: "vc", createRevision: putC.Header.Revision, modRevision: putC.Header.Revision, version: 1},
	}, deleteClientKVs(final.Kvs, prefix))
}

func TestClientDeleteFromKeyRemovesAllUserKeys(t *testing.T) {
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
	keys := []string{"a", "b", "c", "c/abc", "d"}
	var lastPutRevision int64
	for _, key := range keys {
		put, err := client.Put(ctx, key, "value-"+key)
		require.NoError(t, err)
		lastPutRevision = put.Header.Revision
	}

	deleted, err := client.Delete(ctx, "\x00", clientv3.WithFromKey(), clientv3.WithPrevKV())
	require.NoError(t, err)
	require.NotNil(t, deleted.Header)
	require.Equal(t, lastPutRevision+1, deleted.Header.Revision)
	require.Equal(t, int64(len(keys)), deleted.Deleted)
	require.Len(t, deleted.PrevKvs, len(keys))
	gotPrevKeys := make([]string, 0, len(deleted.PrevKvs))
	for _, kv := range deleted.PrevKvs {
		gotPrevKeys = append(gotPrevKeys, string(kv.Key))
		require.Positive(t, kv.ModRevision)
		require.Less(t, kv.ModRevision, deleted.Header.Revision)
	}
	require.Equal(t, keys, gotPrevKeys)

	remaining, err := client.Get(ctx, "a", clientv3.WithFromKey())
	require.NoError(t, err)
	require.NotNil(t, remaining.Header)
	require.Equal(t, deleted.Header.Revision, remaining.Header.Revision)
	require.Zero(t, remaining.Count)
	require.Empty(t, remaining.Kvs)
}

func TestClientDeleteNULFromKeyWithoutPrevKVSuppressesPayload(t *testing.T) {
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
	keys := []string{"a", "b", "c", "c/abc", "d"}
	var lastPutRevision int64
	for _, key := range keys {
		put, err := client.Put(ctx, key, "value-"+key)
		require.NoError(t, err)
		lastPutRevision = put.Header.Revision
	}

	deleted, err := client.Delete(ctx, "\x00", clientv3.WithFromKey())
	require.NoError(t, err)
	require.NotNil(t, deleted.Header)
	require.Equal(t, lastPutRevision+1, deleted.Header.Revision)
	require.Equal(t, int64(len(keys)), deleted.Deleted)
	require.Empty(t, deleted.PrevKvs)

	remaining, err := client.Get(ctx, "\x00", clientv3.WithFromKey())
	require.NoError(t, err)
	require.NotNil(t, remaining.Header)
	require.Equal(t, deleted.Header.Revision, remaining.Header.Revision)
	require.Zero(t, remaining.Count)
	require.Empty(t, remaining.Kvs)
}

func TestClientDoOpDeleteNULFromKeySuppressesPrevKV(t *testing.T) {
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
	keys := []string{"a", "b", "c", "c/abc", "d"}
	var lastPutRevision int64
	for _, key := range keys {
		put, err := client.Put(ctx, key, "value-"+key)
		require.NoError(t, err)
		lastPutRevision = put.Header.Revision
	}

	opResponse, err := client.Do(ctx, clientv3.OpDelete("\x00", clientv3.WithFromKey()))
	require.NoError(t, err)
	deleted := opResponse.Del()
	require.NotNil(t, deleted)
	require.NotNil(t, deleted.Header)
	require.Equal(t, lastPutRevision+1, deleted.Header.Revision)
	require.Equal(t, int64(len(keys)), deleted.Deleted)
	require.Empty(t, deleted.PrevKvs)

	remaining, err := client.Get(ctx, "\x00", clientv3.WithFromKey())
	require.NoError(t, err)
	require.NotNil(t, remaining.Header)
	require.Equal(t, deleted.Header.Revision, remaining.Header.Revision)
	require.Zero(t, remaining.Count)
	require.Empty(t, remaining.Kvs)
}

func TestClientDoOpDeletePointKeyMatchesEtcd(t *testing.T) {
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
	prefix := "/a2145/do-opdelete-point/"
	key := prefix + "key"
	neighborKey := prefix + "neighbor"
	put, err := client.Put(ctx, key, "value")
	require.NoError(t, err)
	neighbor, err := client.Put(ctx, neighborKey, "neighbor")
	require.NoError(t, err)

	opResponse, err := client.Do(ctx, clientv3.OpDelete(key))
	require.NoError(t, err)
	deleted := opResponse.Del()
	require.NotNil(t, deleted)
	require.NotNil(t, deleted.Header)
	require.Equal(t, neighbor.Header.Revision+1, deleted.Header.Revision)
	require.Equal(t, int64(1), deleted.Deleted)
	require.Empty(t, deleted.PrevKvs)

	current, err := client.Get(ctx, prefix, clientv3.WithPrefix(), clientv3.WithSort(clientv3.SortByKey, clientv3.SortAscend))
	require.NoError(t, err)
	require.NotNil(t, current.Header)
	require.Equal(t, deleted.Header.Revision, current.Header.Revision)
	require.Equal(t, []deleteClientKV{
		{key: "neighbor", value: "neighbor", createRevision: neighbor.Header.Revision, modRevision: neighbor.Header.Revision, version: 1},
	}, deleteClientKVs(current.Kvs, prefix))

	historical, err := client.Get(ctx, key, clientv3.WithRev(put.Header.Revision))
	require.NoError(t, err)
	require.Len(t, historical.Kvs, 1)
	require.Equal(t, []byte(key), historical.Kvs[0].Key)
	require.Equal(t, []byte("value"), historical.Kvs[0].Value)
	require.Equal(t, put.Header.Revision, historical.Kvs[0].CreateRevision)
	require.Equal(t, put.Header.Revision, historical.Kvs[0].ModRevision)
	require.Equal(t, int64(1), historical.Kvs[0].Version)
}

func TestClientDoOpDeleteRangeWithPrevKVMatchesEtcd(t *testing.T) {
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
	prefix := "/a2141/do-opdelete-range/"
	base, err := client.Get(ctx, prefix, clientv3.WithPrefix())
	require.NoError(t, err)
	baseRev := base.Header.Revision

	putA, err := client.Put(ctx, prefix+"a", "va")
	require.NoError(t, err)
	putB, err := client.Put(ctx, prefix+"b", "vb")
	require.NoError(t, err)
	putC, err := client.Put(ctx, prefix+"c", "vc")
	require.NoError(t, err)
	updateB, err := client.Put(ctx, prefix+"b", "vb2")
	require.NoError(t, err)
	require.Equal(t, []int64{1, 2, 3, 4}, []int64{
		putA.Header.Revision - baseRev,
		putB.Header.Revision - baseRev,
		putC.Header.Revision - baseRev,
		updateB.Header.Revision - baseRev,
	})

	opResponse, err := client.Do(ctx, clientv3.OpDelete(prefix+"a", clientv3.WithRange(prefix+"c"), clientv3.WithPrevKV()))
	require.NoError(t, err)
	deleted := opResponse.Del()
	require.NotNil(t, deleted)
	require.NotNil(t, deleted.Header)
	require.Equal(t, int64(5), deleted.Header.Revision-baseRev)
	require.Equal(t, int64(2), deleted.Deleted)
	require.Equal(t, []deleteClientKV{
		{key: "a", value: "va", createRevision: putA.Header.Revision, modRevision: putA.Header.Revision, version: 1},
		{key: "b", value: "vb2", createRevision: putB.Header.Revision, modRevision: updateB.Header.Revision, version: 2},
	}, deleteClientKVs(deleted.PrevKvs, prefix))

	historical, err := client.Get(ctx, prefix,
		clientv3.WithPrefix(),
		clientv3.WithRev(deleted.Header.Revision-1),
		clientv3.WithSort(clientv3.SortByKey, clientv3.SortAscend),
	)
	require.NoError(t, err)
	require.Equal(t, []deleteClientKV{
		{key: "a", value: "va", createRevision: putA.Header.Revision, modRevision: putA.Header.Revision, version: 1},
		{key: "b", value: "vb2", createRevision: putB.Header.Revision, modRevision: updateB.Header.Revision, version: 2},
		{key: "c", value: "vc", createRevision: putC.Header.Revision, modRevision: putC.Header.Revision, version: 1},
	}, deleteClientKVs(historical.Kvs, prefix))

	current, err := client.Get(ctx, prefix,
		clientv3.WithPrefix(),
		clientv3.WithSort(clientv3.SortByKey, clientv3.SortAscend),
	)
	require.NoError(t, err)
	require.NotNil(t, current.Header)
	require.Equal(t, deleted.Header.Revision, current.Header.Revision)
	require.Equal(t, []deleteClientKV{
		{key: "c", value: "vc", createRevision: putC.Header.Revision, modRevision: putC.Header.Revision, version: 1},
	}, deleteClientKVs(current.Kvs, prefix))
}

func TestClientDoOpDeletePrefixWithPrevKVMatchesEtcd(t *testing.T) {
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
	root := "/a2144/do-opdelete-prefix/"
	prefix := root + "items/"
	base, err := client.Get(ctx, root, clientv3.WithPrefix())
	require.NoError(t, err)
	baseRev := base.Header.Revision

	putA, err := client.Put(ctx, prefix+"a", "va")
	require.NoError(t, err)
	putB, err := client.Put(ctx, prefix+"b", "vb")
	require.NoError(t, err)
	putC, err := client.Put(ctx, prefix+"nested/c", "vc")
	require.NoError(t, err)
	neighborPrefix, err := client.Put(ctx, root+"items0", "neighbor-prefix")
	require.NoError(t, err)
	neighborOther, err := client.Put(ctx, root+"other", "neighbor-other")
	require.NoError(t, err)
	updateB, err := client.Put(ctx, prefix+"b", "vb2")
	require.NoError(t, err)
	require.Equal(t, []int64{1, 2, 3, 4, 5, 6}, []int64{
		putA.Header.Revision - baseRev,
		putB.Header.Revision - baseRev,
		putC.Header.Revision - baseRev,
		neighborPrefix.Header.Revision - baseRev,
		neighborOther.Header.Revision - baseRev,
		updateB.Header.Revision - baseRev,
	})

	opResponse, err := client.Do(ctx, clientv3.OpDelete(prefix, clientv3.WithPrefix(), clientv3.WithPrevKV()))
	require.NoError(t, err)
	deleted := opResponse.Del()
	require.NotNil(t, deleted)
	require.NotNil(t, deleted.Header)
	require.Equal(t, int64(7), deleted.Header.Revision-baseRev)
	require.Equal(t, int64(3), deleted.Deleted)
	require.Equal(t, []deleteClientKV{
		{key: "a", value: "va", createRevision: putA.Header.Revision, modRevision: putA.Header.Revision, version: 1},
		{key: "b", value: "vb2", createRevision: putB.Header.Revision, modRevision: updateB.Header.Revision, version: 2},
		{key: "nested/c", value: "vc", createRevision: putC.Header.Revision, modRevision: putC.Header.Revision, version: 1},
	}, deleteClientKVs(deleted.PrevKvs, prefix))

	current, err := client.Get(ctx, root,
		clientv3.WithPrefix(),
		clientv3.WithSort(clientv3.SortByKey, clientv3.SortAscend),
	)
	require.NoError(t, err)
	require.NotNil(t, current.Header)
	require.Equal(t, deleted.Header.Revision, current.Header.Revision)
	require.Equal(t, []deleteClientKV{
		{key: "items0", value: "neighbor-prefix", createRevision: neighborPrefix.Header.Revision, modRevision: neighborPrefix.Header.Revision, version: 1},
		{key: "other", value: "neighbor-other", createRevision: neighborOther.Header.Revision, modRevision: neighborOther.Header.Revision, version: 1},
	}, deleteClientKVs(current.Kvs, root))
}

func TestClientDoOpDeleteNoOpDoesNotConsumeRevisionMatchesEtcd(t *testing.T) {
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
	prefix := "/a2142/do-opdelete-noop/"
	key := prefix + "key"
	create, err := client.Put(ctx, key, "value")
	require.NoError(t, err)

	missingOp, err := client.Do(ctx, clientv3.OpDelete(prefix+"missing", clientv3.WithPrevKV()))
	require.NoError(t, err)
	missing := missingOp.Del()
	require.NotNil(t, missing)
	require.NotNil(t, missing.Header)
	require.Equal(t, create.Header.Revision, missing.Header.Revision)
	require.Zero(t, missing.Deleted)
	require.Empty(t, missing.PrevKvs)

	emptyRangeOp, err := client.Do(ctx, clientv3.OpDelete(key, clientv3.WithRange(key), clientv3.WithPrevKV()))
	require.NoError(t, err)
	emptyRange := emptyRangeOp.Del()
	require.NotNil(t, emptyRange)
	require.NotNil(t, emptyRange.Header)
	require.Equal(t, create.Header.Revision, emptyRange.Header.Revision)
	require.Zero(t, emptyRange.Deleted)
	require.Empty(t, emptyRange.PrevKvs)

	current, err := client.Get(ctx, key)
	require.NoError(t, err)
	require.NotNil(t, current.Header)
	require.Equal(t, create.Header.Revision, current.Header.Revision)
	require.Len(t, current.Kvs, 1)
	require.Equal(t, []byte(key), current.Kvs[0].Key)
	require.Equal(t, []byte("value"), current.Kvs[0].Value)
	require.Equal(t, create.Header.Revision, current.Kvs[0].CreateRevision)
	require.Equal(t, create.Header.Revision, current.Kvs[0].ModRevision)
	require.Equal(t, int64(1), current.Kvs[0].Version)
}

func TestClientDoOpDeleteEmptyKeyIsTyped(t *testing.T) {
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
	_, err = client.Do(ctx, clientv3.OpDelete(""))
	requireClientDeleteError(t, err, codes.Unknown, "etcdserver: key is not provided", rpctypes.ErrEmptyKey)
}

func TestClientDeleteEmptyKeyIsTyped(t *testing.T) {
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
	_, err = client.Delete(ctx, "")
	requireClientDeleteError(t, err, codes.Unknown, "etcdserver: key is not provided", rpctypes.ErrEmptyKey)
}

type deleteClientKV struct {
	key            string
	value          string
	createRevision int64
	modRevision    int64
	version        int64
}

func deleteClientKVs(kvs []*mvccpb.KeyValue, prefix string) []deleteClientKV {
	out := make([]deleteClientKV, 0, len(kvs))
	for _, kv := range kvs {
		out = append(out, deleteClientKV{
			key:            string(kv.Key[len(prefix):]),
			value:          string(kv.Value),
			createRevision: kv.CreateRevision,
			modRevision:    kv.ModRevision,
			version:        kv.Version,
		})
	}
	return out
}

func requireClientDeleteError(t *testing.T, err error, code codes.Code, message string, wantErrorIs ...error) {
	t.Helper()
	require.EqualError(t, err, message)
	for _, want := range wantErrorIs {
		require.ErrorIs(t, err, want)
	}
	require.Equal(t, code, status.Code(err))
	require.Equal(t, message, status.Convert(err).Message())
}
