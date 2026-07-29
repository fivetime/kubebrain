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

func TestClientDoOpDeletePointKeyWithPrevKVMatchesEtcd(t *testing.T) {
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
	key := "/a2146/do-opdelete-point-prevkv/key"
	create, err := client.Put(ctx, key, "before")
	require.NoError(t, err)
	update, err := client.Put(ctx, key, "after")
	require.NoError(t, err)

	opResponse, err := client.Do(ctx, clientv3.OpDelete(key, clientv3.WithPrevKV()))
	require.NoError(t, err)
	deleted := opResponse.Del()
	require.NotNil(t, deleted)
	require.NotNil(t, deleted.Header)
	require.Equal(t, update.Header.Revision+1, deleted.Header.Revision)
	require.Equal(t, int64(1), deleted.Deleted)
	require.Len(t, deleted.PrevKvs, 1)
	prev := deleted.PrevKvs[0]
	require.Equal(t, []byte(key), prev.Key)
	require.Equal(t, []byte("after"), prev.Value)
	require.Equal(t, create.Header.Revision, prev.CreateRevision)
	require.Equal(t, update.Header.Revision, prev.ModRevision)
	require.Equal(t, int64(2), prev.Version)
	require.Zero(t, prev.Lease)

	current, err := client.Get(ctx, key)
	require.NoError(t, err)
	require.NotNil(t, current.Header)
	require.Equal(t, deleted.Header.Revision, current.Header.Revision)
	require.Empty(t, current.Kvs)

	historical, err := client.Get(ctx, key, clientv3.WithRev(update.Header.Revision))
	require.NoError(t, err)
	require.Len(t, historical.Kvs, 1)
	require.Equal(t, []byte("after"), historical.Kvs[0].Value)
	require.Equal(t, create.Header.Revision, historical.Kvs[0].CreateRevision)
	require.Equal(t, update.Header.Revision, historical.Kvs[0].ModRevision)
	require.Equal(t, int64(2), historical.Kvs[0].Version)
}

func TestClientDoOpDeleteLeasedPointKeyWithPrevKVMatchesEtcd(t *testing.T) {
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
	lease, err := client.Grant(ctx, 300)
	require.NoError(t, err)
	key := "/a2149/do-opdelete-leased-point-prevkv/key"
	put, err := client.Put(ctx, key, "leased", clientv3.WithLease(lease.ID))
	require.NoError(t, err)

	ttlBefore, err := client.TimeToLive(ctx, lease.ID, clientv3.WithAttachedKeys())
	require.NoError(t, err)
	require.Equal(t, []string{key}, leaseClientAttachedKeys(ttlBefore.Keys))

	opResponse, err := client.Do(ctx, clientv3.OpDelete(key, clientv3.WithPrevKV()))
	require.NoError(t, err)
	deleted := opResponse.Del()
	require.NotNil(t, deleted)
	require.NotNil(t, deleted.Header)
	require.Equal(t, put.Header.Revision+1, deleted.Header.Revision)
	require.Equal(t, int64(1), deleted.Deleted)
	require.Len(t, deleted.PrevKvs, 1)
	prev := deleted.PrevKvs[0]
	require.Equal(t, []byte(key), prev.Key)
	require.Equal(t, []byte("leased"), prev.Value)
	require.Equal(t, put.Header.Revision, prev.CreateRevision)
	require.Equal(t, put.Header.Revision, prev.ModRevision)
	require.Equal(t, int64(1), prev.Version)
	require.Equal(t, int64(lease.ID), prev.Lease)

	current, err := client.Get(ctx, key)
	require.NoError(t, err)
	require.NotNil(t, current.Header)
	require.Equal(t, deleted.Header.Revision, current.Header.Revision)
	require.Empty(t, current.Kvs)

	ttlAfter, err := client.TimeToLive(ctx, lease.ID, clientv3.WithAttachedKeys())
	require.NoError(t, err)
	require.Empty(t, ttlAfter.Keys)
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

func TestClientDoOpDeleteRangeWithLeasesPrevKVMatchesEtcd(t *testing.T) {
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
	leaseA, err := client.Grant(ctx, 300)
	require.NoError(t, err)
	leaseB, err := client.Grant(ctx, 300)
	require.NoError(t, err)
	prefix := "/a2150/do-opdelete-range-leased/"
	putA, err := client.Put(ctx, prefix+"a", "va", clientv3.WithLease(leaseA.ID))
	require.NoError(t, err)
	putB, err := client.Put(ctx, prefix+"b", "vb", clientv3.WithLease(leaseB.ID))
	require.NoError(t, err)
	putC, err := client.Put(ctx, prefix+"c", "vc", clientv3.WithLease(leaseA.ID))
	require.NoError(t, err)
	updateB, err := client.Put(ctx, prefix+"b", "vb2", clientv3.WithLease(leaseB.ID))
	require.NoError(t, err)

	ttlABefore, err := client.TimeToLive(ctx, leaseA.ID, clientv3.WithAttachedKeys())
	require.NoError(t, err)
	require.ElementsMatch(t, []string{prefix + "a", prefix + "c"}, leaseClientAttachedKeys(ttlABefore.Keys))
	ttlBBefore, err := client.TimeToLive(ctx, leaseB.ID, clientv3.WithAttachedKeys())
	require.NoError(t, err)
	require.Equal(t, []string{prefix + "b"}, leaseClientAttachedKeys(ttlBBefore.Keys))

	opResponse, err := client.Do(ctx, clientv3.OpDelete(prefix+"a", clientv3.WithRange(prefix+"c"), clientv3.WithPrevKV()))
	require.NoError(t, err)
	deleted := opResponse.Del()
	require.NotNil(t, deleted)
	require.NotNil(t, deleted.Header)
	require.Equal(t, updateB.Header.Revision+1, deleted.Header.Revision)
	require.Equal(t, int64(2), deleted.Deleted)
	require.Len(t, deleted.PrevKvs, 2)
	require.Equal(t, []byte(prefix+"a"), deleted.PrevKvs[0].Key)
	require.Equal(t, []byte("va"), deleted.PrevKvs[0].Value)
	require.Equal(t, putA.Header.Revision, deleted.PrevKvs[0].CreateRevision)
	require.Equal(t, putA.Header.Revision, deleted.PrevKvs[0].ModRevision)
	require.Equal(t, int64(1), deleted.PrevKvs[0].Version)
	require.Equal(t, int64(leaseA.ID), deleted.PrevKvs[0].Lease)
	require.Equal(t, []byte(prefix+"b"), deleted.PrevKvs[1].Key)
	require.Equal(t, []byte("vb2"), deleted.PrevKvs[1].Value)
	require.Equal(t, putB.Header.Revision, deleted.PrevKvs[1].CreateRevision)
	require.Equal(t, updateB.Header.Revision, deleted.PrevKvs[1].ModRevision)
	require.Equal(t, int64(2), deleted.PrevKvs[1].Version)
	require.Equal(t, int64(leaseB.ID), deleted.PrevKvs[1].Lease)

	ttlAAfter, err := client.TimeToLive(ctx, leaseA.ID, clientv3.WithAttachedKeys())
	require.NoError(t, err)
	require.Equal(t, []string{prefix + "c"}, leaseClientAttachedKeys(ttlAAfter.Keys))
	ttlBAfter, err := client.TimeToLive(ctx, leaseB.ID, clientv3.WithAttachedKeys())
	require.NoError(t, err)
	require.Empty(t, ttlBAfter.Keys)

	current, err := client.Get(ctx, prefix, clientv3.WithPrefix(), clientv3.WithSort(clientv3.SortByKey, clientv3.SortAscend))
	require.NoError(t, err)
	require.NotNil(t, current.Header)
	require.Equal(t, deleted.Header.Revision, current.Header.Revision)
	require.Equal(t, []deleteClientKV{
		{key: "c", value: "vc", createRevision: putC.Header.Revision, modRevision: putC.Header.Revision, version: 1},
	}, deleteClientKVs(current.Kvs, prefix))
	require.Equal(t, int64(leaseA.ID), current.Kvs[0].Lease)
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

func TestClientDoOpDeletePrefixWithLeasesPrevKVMatchesEtcd(t *testing.T) {
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
	leaseA, err := client.Grant(ctx, 300)
	require.NoError(t, err)
	leaseB, err := client.Grant(ctx, 300)
	require.NoError(t, err)
	root := "/a2151/do-opdelete-prefix-leased/"
	prefix := root + "items/"
	putA, err := client.Put(ctx, prefix+"a", "va", clientv3.WithLease(leaseA.ID))
	require.NoError(t, err)
	putB, err := client.Put(ctx, prefix+"b", "vb", clientv3.WithLease(leaseB.ID))
	require.NoError(t, err)
	outsideA, err := client.Put(ctx, root+"outside-a", "outside-a", clientv3.WithLease(leaseA.ID))
	require.NoError(t, err)
	outsideB, err := client.Put(ctx, root+"outside-b", "outside-b", clientv3.WithLease(leaseB.ID))
	require.NoError(t, err)
	updateB, err := client.Put(ctx, prefix+"b", "vb2", clientv3.WithLease(leaseB.ID))
	require.NoError(t, err)

	ttlABefore, err := client.TimeToLive(ctx, leaseA.ID, clientv3.WithAttachedKeys())
	require.NoError(t, err)
	require.ElementsMatch(t, []string{prefix + "a", root + "outside-a"}, leaseClientAttachedKeys(ttlABefore.Keys))
	ttlBBefore, err := client.TimeToLive(ctx, leaseB.ID, clientv3.WithAttachedKeys())
	require.NoError(t, err)
	require.ElementsMatch(t, []string{prefix + "b", root + "outside-b"}, leaseClientAttachedKeys(ttlBBefore.Keys))

	opResponse, err := client.Do(ctx, clientv3.OpDelete(prefix, clientv3.WithPrefix(), clientv3.WithPrevKV()))
	require.NoError(t, err)
	deleted := opResponse.Del()
	require.NotNil(t, deleted)
	require.NotNil(t, deleted.Header)
	require.Equal(t, updateB.Header.Revision+1, deleted.Header.Revision)
	require.Equal(t, int64(2), deleted.Deleted)
	require.Len(t, deleted.PrevKvs, 2)
	require.Equal(t, []byte(prefix+"a"), deleted.PrevKvs[0].Key)
	require.Equal(t, []byte("va"), deleted.PrevKvs[0].Value)
	require.Equal(t, putA.Header.Revision, deleted.PrevKvs[0].CreateRevision)
	require.Equal(t, putA.Header.Revision, deleted.PrevKvs[0].ModRevision)
	require.Equal(t, int64(1), deleted.PrevKvs[0].Version)
	require.Equal(t, int64(leaseA.ID), deleted.PrevKvs[0].Lease)
	require.Equal(t, []byte(prefix+"b"), deleted.PrevKvs[1].Key)
	require.Equal(t, []byte("vb2"), deleted.PrevKvs[1].Value)
	require.Equal(t, putB.Header.Revision, deleted.PrevKvs[1].CreateRevision)
	require.Equal(t, updateB.Header.Revision, deleted.PrevKvs[1].ModRevision)
	require.Equal(t, int64(2), deleted.PrevKvs[1].Version)
	require.Equal(t, int64(leaseB.ID), deleted.PrevKvs[1].Lease)

	ttlAAfter, err := client.TimeToLive(ctx, leaseA.ID, clientv3.WithAttachedKeys())
	require.NoError(t, err)
	require.Equal(t, []string{root + "outside-a"}, leaseClientAttachedKeys(ttlAAfter.Keys))
	ttlBAfter, err := client.TimeToLive(ctx, leaseB.ID, clientv3.WithAttachedKeys())
	require.NoError(t, err)
	require.Equal(t, []string{root + "outside-b"}, leaseClientAttachedKeys(ttlBAfter.Keys))

	current, err := client.Get(ctx, root, clientv3.WithPrefix(), clientv3.WithSort(clientv3.SortByKey, clientv3.SortAscend))
	require.NoError(t, err)
	require.NotNil(t, current.Header)
	require.Equal(t, deleted.Header.Revision, current.Header.Revision)
	require.Equal(t, []deleteClientKV{
		{key: "outside-a", value: "outside-a", createRevision: outsideA.Header.Revision, modRevision: outsideA.Header.Revision, version: 1},
		{key: "outside-b", value: "outside-b", createRevision: outsideB.Header.Revision, modRevision: outsideB.Header.Revision, version: 1},
	}, deleteClientKVs(current.Kvs, root))
	require.Equal(t, int64(leaseA.ID), current.Kvs[0].Lease)
	require.Equal(t, int64(leaseB.ID), current.Kvs[1].Lease)
}

func TestClientDoOpDeleteFromKeyWithPrevKVMatchesEtcd(t *testing.T) {
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
	prefix := "/a2147/do-opdelete-fromkey/"
	before, err := client.Put(ctx, prefix+"a", "va")
	require.NoError(t, err)
	putB, err := client.Put(ctx, prefix+"b", "vb")
	require.NoError(t, err)
	putC, err := client.Put(ctx, prefix+"c", "vc")
	require.NoError(t, err)
	putD, err := client.Put(ctx, prefix+"d", "vd")
	require.NoError(t, err)
	updateC, err := client.Put(ctx, prefix+"c", "vc2")
	require.NoError(t, err)

	opResponse, err := client.Do(ctx, clientv3.OpDelete(prefix+"b", clientv3.WithFromKey(), clientv3.WithPrevKV()))
	require.NoError(t, err)
	deleted := opResponse.Del()
	require.NotNil(t, deleted)
	require.NotNil(t, deleted.Header)
	require.Equal(t, updateC.Header.Revision+1, deleted.Header.Revision)
	require.Equal(t, int64(3), deleted.Deleted)
	require.Equal(t, []deleteClientKV{
		{key: "b", value: "vb", createRevision: putB.Header.Revision, modRevision: putB.Header.Revision, version: 1},
		{key: "c", value: "vc2", createRevision: putC.Header.Revision, modRevision: updateC.Header.Revision, version: 2},
		{key: "d", value: "vd", createRevision: putD.Header.Revision, modRevision: putD.Header.Revision, version: 1},
	}, deleteClientKVs(deleted.PrevKvs, prefix))

	current, err := client.Get(ctx, prefix,
		clientv3.WithPrefix(),
		clientv3.WithSort(clientv3.SortByKey, clientv3.SortAscend),
	)
	require.NoError(t, err)
	require.NotNil(t, current.Header)
	require.Equal(t, deleted.Header.Revision, current.Header.Revision)
	require.Equal(t, []deleteClientKV{
		{key: "a", value: "va", createRevision: before.Header.Revision, modRevision: before.Header.Revision, version: 1},
	}, deleteClientKVs(current.Kvs, prefix))

	historical, err := client.Get(ctx, prefix,
		clientv3.WithPrefix(),
		clientv3.WithRev(updateC.Header.Revision),
		clientv3.WithSort(clientv3.SortByKey, clientv3.SortAscend),
	)
	require.NoError(t, err)
	require.Equal(t, []deleteClientKV{
		{key: "a", value: "va", createRevision: before.Header.Revision, modRevision: before.Header.Revision, version: 1},
		{key: "b", value: "vb", createRevision: putB.Header.Revision, modRevision: putB.Header.Revision, version: 1},
		{key: "c", value: "vc2", createRevision: putC.Header.Revision, modRevision: updateC.Header.Revision, version: 2},
		{key: "d", value: "vd", createRevision: putD.Header.Revision, modRevision: putD.Header.Revision, version: 1},
	}, deleteClientKVs(historical.Kvs, prefix))
}

func TestClientDoOpDeleteFromKeyWithLeasesPrevKVMatchesEtcd(t *testing.T) {
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
	leaseA, err := client.Grant(ctx, 300)
	require.NoError(t, err)
	leaseB, err := client.Grant(ctx, 300)
	require.NoError(t, err)
	prefix := "/a2152/do-opdelete-fromkey-leased/"
	before, err := client.Put(ctx, prefix+"a", "va", clientv3.WithLease(leaseA.ID))
	require.NoError(t, err)
	putB, err := client.Put(ctx, prefix+"b", "vb", clientv3.WithLease(leaseA.ID))
	require.NoError(t, err)
	putC, err := client.Put(ctx, prefix+"c", "vc", clientv3.WithLease(leaseB.ID))
	require.NoError(t, err)
	putD, err := client.Put(ctx, prefix+"d", "vd", clientv3.WithLease(leaseA.ID))
	require.NoError(t, err)
	updateC, err := client.Put(ctx, prefix+"c", "vc2", clientv3.WithLease(leaseB.ID))
	require.NoError(t, err)

	ttlABefore, err := client.TimeToLive(ctx, leaseA.ID, clientv3.WithAttachedKeys())
	require.NoError(t, err)
	require.ElementsMatch(t, []string{prefix + "a", prefix + "b", prefix + "d"}, leaseClientAttachedKeys(ttlABefore.Keys))
	ttlBBefore, err := client.TimeToLive(ctx, leaseB.ID, clientv3.WithAttachedKeys())
	require.NoError(t, err)
	require.Equal(t, []string{prefix + "c"}, leaseClientAttachedKeys(ttlBBefore.Keys))

	opResponse, err := client.Do(ctx, clientv3.OpDelete(prefix+"b", clientv3.WithFromKey(), clientv3.WithPrevKV()))
	require.NoError(t, err)
	deleted := opResponse.Del()
	require.NotNil(t, deleted)
	require.NotNil(t, deleted.Header)
	require.Equal(t, updateC.Header.Revision+1, deleted.Header.Revision)
	require.Equal(t, int64(3), deleted.Deleted)
	require.Len(t, deleted.PrevKvs, 3)
	require.Equal(t, []byte(prefix+"b"), deleted.PrevKvs[0].Key)
	require.Equal(t, []byte("vb"), deleted.PrevKvs[0].Value)
	require.Equal(t, putB.Header.Revision, deleted.PrevKvs[0].CreateRevision)
	require.Equal(t, putB.Header.Revision, deleted.PrevKvs[0].ModRevision)
	require.Equal(t, int64(1), deleted.PrevKvs[0].Version)
	require.Equal(t, int64(leaseA.ID), deleted.PrevKvs[0].Lease)
	require.Equal(t, []byte(prefix+"c"), deleted.PrevKvs[1].Key)
	require.Equal(t, []byte("vc2"), deleted.PrevKvs[1].Value)
	require.Equal(t, putC.Header.Revision, deleted.PrevKvs[1].CreateRevision)
	require.Equal(t, updateC.Header.Revision, deleted.PrevKvs[1].ModRevision)
	require.Equal(t, int64(2), deleted.PrevKvs[1].Version)
	require.Equal(t, int64(leaseB.ID), deleted.PrevKvs[1].Lease)
	require.Equal(t, []byte(prefix+"d"), deleted.PrevKvs[2].Key)
	require.Equal(t, []byte("vd"), deleted.PrevKvs[2].Value)
	require.Equal(t, putD.Header.Revision, deleted.PrevKvs[2].CreateRevision)
	require.Equal(t, putD.Header.Revision, deleted.PrevKvs[2].ModRevision)
	require.Equal(t, int64(1), deleted.PrevKvs[2].Version)
	require.Equal(t, int64(leaseA.ID), deleted.PrevKvs[2].Lease)

	ttlAAfter, err := client.TimeToLive(ctx, leaseA.ID, clientv3.WithAttachedKeys())
	require.NoError(t, err)
	require.Equal(t, []string{prefix + "a"}, leaseClientAttachedKeys(ttlAAfter.Keys))
	ttlBAfter, err := client.TimeToLive(ctx, leaseB.ID, clientv3.WithAttachedKeys())
	require.NoError(t, err)
	require.Empty(t, ttlBAfter.Keys)

	current, err := client.Get(ctx, prefix,
		clientv3.WithPrefix(),
		clientv3.WithSort(clientv3.SortByKey, clientv3.SortAscend),
	)
	require.NoError(t, err)
	require.NotNil(t, current.Header)
	require.Equal(t, deleted.Header.Revision, current.Header.Revision)
	require.Equal(t, []deleteClientKV{
		{key: "a", value: "va", createRevision: before.Header.Revision, modRevision: before.Header.Revision, version: 1},
	}, deleteClientKVs(current.Kvs, prefix))
	require.Equal(t, int64(leaseA.ID), current.Kvs[0].Lease)

	historical, err := client.Get(ctx, prefix,
		clientv3.WithPrefix(),
		clientv3.WithRev(updateC.Header.Revision),
		clientv3.WithSort(clientv3.SortByKey, clientv3.SortAscend),
	)
	require.NoError(t, err)
	require.Equal(t, []deleteClientKV{
		{key: "a", value: "va", createRevision: before.Header.Revision, modRevision: before.Header.Revision, version: 1},
		{key: "b", value: "vb", createRevision: putB.Header.Revision, modRevision: putB.Header.Revision, version: 1},
		{key: "c", value: "vc2", createRevision: putC.Header.Revision, modRevision: updateC.Header.Revision, version: 2},
		{key: "d", value: "vd", createRevision: putD.Header.Revision, modRevision: putD.Header.Revision, version: 1},
	}, deleteClientKVs(historical.Kvs, prefix))
	require.Equal(t, int64(leaseA.ID), historical.Kvs[0].Lease)
	require.Equal(t, int64(leaseA.ID), historical.Kvs[1].Lease)
	require.Equal(t, int64(leaseB.ID), historical.Kvs[2].Lease)
	require.Equal(t, int64(leaseA.ID), historical.Kvs[3].Lease)
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
	beforeKey := prefix + "a"
	afterKey := prefix + "z"
	before, err := client.Put(ctx, beforeKey, "before")
	require.NoError(t, err)
	after, err := client.Put(ctx, afterKey, "after")
	require.NoError(t, err)

	missingOp, err := client.Do(ctx, clientv3.OpDelete(prefix+"missing", clientv3.WithPrevKV()))
	require.NoError(t, err)
	missing := missingOp.Del()
	require.NotNil(t, missing)
	require.NotNil(t, missing.Header)
	require.Equal(t, after.Header.Revision, missing.Header.Revision)
	require.Zero(t, missing.Deleted)
	require.Empty(t, missing.PrevKvs)

	emptyRangeOp, err := client.Do(ctx, clientv3.OpDelete(key, clientv3.WithRange(key), clientv3.WithPrevKV()))
	require.NoError(t, err)
	emptyRange := emptyRangeOp.Del()
	require.NotNil(t, emptyRange)
	require.NotNil(t, emptyRange.Header)
	require.Equal(t, after.Header.Revision, emptyRange.Header.Revision)
	require.Zero(t, emptyRange.Deleted)
	require.Empty(t, emptyRange.PrevKvs)

	reversedRangeOp, err := client.Do(ctx, clientv3.OpDelete(afterKey, clientv3.WithRange(beforeKey), clientv3.WithPrevKV()))
	require.NoError(t, err)
	reversedRange := reversedRangeOp.Del()
	require.NotNil(t, reversedRange)
	require.NotNil(t, reversedRange.Header)
	require.Equal(t, after.Header.Revision, reversedRange.Header.Revision)
	require.Zero(t, reversedRange.Deleted)
	require.Empty(t, reversedRange.PrevKvs)

	current, err := client.Get(ctx, prefix, clientv3.WithPrefix(), clientv3.WithSort(clientv3.SortByKey, clientv3.SortAscend))
	require.NoError(t, err)
	require.NotNil(t, current.Header)
	require.Equal(t, after.Header.Revision, current.Header.Revision)
	require.Equal(t, []deleteClientKV{
		{key: "a", value: "before", createRevision: before.Header.Revision, modRevision: before.Header.Revision, version: 1},
		{key: "key", value: "value", createRevision: create.Header.Revision, modRevision: create.Header.Revision, version: 1},
		{key: "z", value: "after", createRevision: after.Header.Revision, modRevision: after.Header.Revision, version: 1},
	}, deleteClientKVs(current.Kvs, prefix))
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
