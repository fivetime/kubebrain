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
	"slices"
	"sync"
	"sync/atomic"
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

func TestClientLeaseReadBoundaryAndAttachedKeys(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()

	grpcServer := grpc.NewServer(server.ClientServerOptions()...)
	etcdserverpb.RegisterKVServer(grpcServer, server)
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

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	zeroWithoutKeys, err := client.TimeToLive(ctx, 0)
	require.NoError(t, err)
	zeroWithKeys, err := client.TimeToLive(ctx, 0, clientv3.WithAttachedKeys())
	require.NoError(t, err)
	require.Equal(t, clientv3.LeaseID(0), zeroWithoutKeys.ID)
	require.Equal(t, int64(-1), zeroWithoutKeys.TTL)
	require.Zero(t, zeroWithoutKeys.GrantedTTL)
	require.Empty(t, zeroWithoutKeys.Keys)
	require.Empty(t, zeroWithKeys.Keys)
	require.Equal(t, zeroWithoutKeys.ID, zeroWithKeys.ID)
	require.Equal(t, zeroWithoutKeys.TTL, zeroWithKeys.TTL)
	require.Equal(t, zeroWithoutKeys.GrantedTTL, zeroWithKeys.GrantedTTL)
	requireClientLeaseHeaderWellFormed(t, zeroWithoutKeys.ResponseHeader)
	requireClientLeaseHeaderWellFormed(t, zeroWithKeys.ResponseHeader)

	grant, err := client.Grant(ctx, 300)
	require.NoError(t, err)
	leaseID := grant.ID
	require.NotZero(t, leaseID)
	prefix := fmt.Sprintf("/a994/lease-client-read/%d/", time.Now().UnixNano())
	keys := []string{prefix + "z", prefix + "a"}
	for _, key := range keys {
		_, err = client.Put(ctx, key, "value", clientv3.WithLease(leaseID))
		require.NoError(t, err)
	}

	liveWithoutKeys, err := client.TimeToLive(ctx, leaseID)
	require.NoError(t, err)
	require.Equal(t, leaseID, liveWithoutKeys.ID)
	require.Positive(t, liveWithoutKeys.TTL)
	require.LessOrEqual(t, liveWithoutKeys.TTL, int64(300))
	require.Equal(t, int64(300), liveWithoutKeys.GrantedTTL)
	require.Empty(t, liveWithoutKeys.Keys)
	requireClientLeaseHeaderWellFormed(t, liveWithoutKeys.ResponseHeader)

	liveWithKeys, err := client.TimeToLive(ctx, leaseID, clientv3.WithAttachedKeys())
	require.NoError(t, err)
	require.Equal(t, leaseID, liveWithKeys.ID)
	require.Positive(t, liveWithKeys.TTL)
	require.LessOrEqual(t, liveWithKeys.TTL, int64(300))
	require.Equal(t, int64(300), liveWithKeys.GrantedTTL)
	gotKeys := make([]string, 0, len(liveWithKeys.Keys))
	for _, key := range liveWithKeys.Keys {
		gotKeys = append(gotKeys, string(key))
	}
	slices.Sort(keys)
	slices.Sort(gotKeys)
	require.Equal(t, keys, gotKeys)
	requireClientLeaseHeaderWellFormed(t, liveWithKeys.ResponseHeader)

	list, err := client.Leases(ctx)
	require.NoError(t, err)
	requireClientLeaseHeaderWellFormed(t, list.ResponseHeader)
	containsLive := false
	for _, status := range list.Leases {
		require.NotZero(t, status.ID)
		containsLive = containsLive || status.ID == leaseID
	}
	require.True(t, containsLive)
}

func TestClientLeaseTimeToLiveRevokedLeaseReturnsMinusOne(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()

	grpcServer := grpc.NewServer(server.ClientServerOptions()...)
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

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	grant, err := client.Grant(ctx, 10)
	require.NoError(t, err)
	_, err = client.Revoke(ctx, grant.ID)
	require.NoError(t, err)

	ttl, err := client.TimeToLive(ctx, grant.ID, clientv3.WithAttachedKeys())
	require.NoError(t, err)
	require.NotNil(t, ttl.ResponseHeader)
	require.Equal(t, grant.ID, ttl.ID)
	require.Equal(t, int64(-1), ttl.TTL)
	require.Zero(t, ttl.GrantedTTL)
	require.Empty(t, ttl.Keys)
}

func TestClientLeaseOperationsAfterCloseAreBoundedAndDoNotCommit(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()

	grpcServer := grpc.NewServer(server.ClientServerOptions()...)
	etcdserverpb.RegisterKVServer(grpcServer, server)
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

	setupCtx, setupCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer setupCancel()
	grant, err := client.Grant(setupCtx, 60)
	require.NoError(t, err)
	key := "/a1127/closed-lease/key"
	_, err = client.Put(setupCtx, key, "value", clientv3.WithLease(grant.ID))
	require.NoError(t, err)
	require.NoError(t, client.Close())

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	_, err = client.Grant(ctx, 5)
	requireClientClosedError(t, err, "Lease.Grant")
	_, err = client.Revoke(ctx, grant.ID)
	requireClientClosedError(t, err, "Lease.Revoke")
	_, err = client.TimeToLive(ctx, grant.ID)
	requireClientClosedError(t, err, "Lease.TimeToLive")
	_, err = client.Leases(ctx)
	requireClientClosedError(t, err, "Lease.Leases")
	_, err = client.KeepAliveOnce(ctx, grant.ID)
	requireClientClosedError(t, err, "Lease.KeepAliveOnce")

	committed, err := server.Range(context.Background(), &etcdserverpb.RangeRequest{Key: []byte(key)})
	require.NoError(t, err)
	require.Len(t, committed.Kvs, 1)
	require.Equal(t, "value", string(committed.Kvs[0].Value))
	ttl, err := server.LeaseTimeToLive(context.Background(), &etcdserverpb.LeaseTimeToLiveRequest{ID: int64(grant.ID)})
	require.NoError(t, err)
	require.Positive(t, ttl.TTL)
}

func TestClientLeaseNotFoundErrorsMatchEtcd(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()

	grpcServer := grpc.NewServer(server.ClientServerOptions()...)
	etcdserverpb.RegisterKVServer(grpcServer, server)
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

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_, err = client.Put(ctx, "/a1130/lease-not-found/missing", "value", clientv3.WithLease(500))
	requireClientLeaseError(t, err, codes.Unknown, "etcdserver: requested lease not found", rpctypes.ErrLeaseNotFound)

	grant, err := client.Grant(ctx, 10)
	require.NoError(t, err)
	_, err = client.Revoke(ctx, grant.ID)
	require.NoError(t, err)
	_, err = client.Put(ctx, "/a1130/lease-not-found/revoked", "value", clientv3.WithLease(grant.ID))
	requireClientLeaseError(t, err, codes.Unknown, "etcdserver: requested lease not found", rpctypes.ErrLeaseNotFound)
	_, err = client.Revoke(ctx, 0)
	requireClientLeaseError(t, err, codes.Unknown, "etcdserver: requested lease not found", rpctypes.ErrLeaseNotFound)
	_, err = client.KeepAliveOnce(ctx, 0)
	requireClientLeaseError(t, err, codes.Unknown, "etcdserver: requested lease not found", rpctypes.ErrLeaseNotFound)

	ttl, err := client.TimeToLive(ctx, grant.ID)
	require.NoError(t, err)
	require.NotNil(t, ttl.ResponseHeader)
	require.Equal(t, grant.ID, ttl.ID)
	require.Equal(t, int64(-1), ttl.TTL)
}

func TestClientLeaseGrantTooLargeTTLIsTyped(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()

	grpcServer := grpc.NewServer(server.ClientServerOptions()...)
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

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	for _, tc := range []struct {
		name string
		ttl  int64
	}{
		{name: "above-maximum", ttl: clientv3.MaxLeaseTTL + 1},
		{name: "maximum-int64", ttl: math.MaxInt64},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err = client.Grant(ctx, tc.ttl)
			requireClientLeaseError(t, err, codes.Unknown, "etcdserver: too large lease TTL", rpctypes.ErrLeaseTTLTooLarge)
		})
	}
}

func TestClientLeaseGrantClampsSmallTTLLikeEtcd(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()

	grpcServer := grpc.NewServer(server.ClientServerOptions()...)
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

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	for _, ttl := range []int64{math.MinInt64, -1, 0, 1, minLeaseTTL} {
		t.Run(fmt.Sprintf("ttl-%d", ttl), func(t *testing.T) {
			grant, err := client.Grant(ctx, ttl)
			require.NoError(t, err)
			t.Cleanup(func() { _, _ = client.Revoke(context.Background(), grant.ID) })
			require.NotZero(t, grant.ID)
			require.Equal(t, minLeaseTTL, grant.TTL)

			ttlResp, err := client.TimeToLive(ctx, grant.ID)
			require.NoError(t, err)
			require.Equal(t, grant.ID, ttlResp.ID)
			require.Equal(t, minLeaseTTL, ttlResp.GrantedTTL)
		})
	}
}

func TestClientLeaseGrantMaximumTTLMatchesEtcd(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()

	grpcServer := grpc.NewServer(server.ClientServerOptions()...)
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

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	grant, err := client.Grant(ctx, clientv3.MaxLeaseTTL)
	require.NoError(t, err)
	require.NotZero(t, grant.ID)
	require.Equal(t, int64(clientv3.MaxLeaseTTL), grant.TTL)

	ttlResp, err := client.TimeToLive(ctx, grant.ID)
	require.NoError(t, err)
	require.Equal(t, grant.ID, ttlResp.ID)
	require.Equal(t, int64(clientv3.MaxLeaseTTL), ttlResp.GrantedTTL)
}

func TestClientLeaseGrantNoSpaceIsTyped(t *testing.T) {
	server := newQuotaRPCServer(t, 6)

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
	_, err = client.Put(ctx, "key", "123")
	require.NoError(t, err)
	_, err = client.Put(ctx, "x", "y")
	requireClientLeaseError(t, err, codes.Unknown, "etcdserver: mvcc: database space exceeded", rpctypes.ErrNoSpace)

	_, err = client.Grant(ctx, 30)
	requireClientLeaseError(t, err, codes.Unknown, "etcdserver: mvcc: database space exceeded", rpctypes.ErrNoSpace)
}

func TestClientLeaseLeasesListsGrantedIDsInOrder(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()

	grpcServer := grpc.NewServer(server.ClientServerOptions()...)
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

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	wantIDs := make([]clientv3.LeaseID, 0, 5)
	for i := 0; i < 5; i++ {
		grant, grantErr := client.Grant(ctx, 30+int64(i))
		require.NoError(t, grantErr)
		wantIDs = append(wantIDs, grant.ID)
	}

	list, err := client.Leases(ctx)
	require.NoError(t, err)
	requireClientLeaseHeaderWellFormed(t, list.ResponseHeader)
	gotIDs := make([]clientv3.LeaseID, 0, len(list.Leases))
	for _, status := range list.Leases {
		gotIDs = append(gotIDs, status.ID)
	}
	require.Equal(t, wantIDs, gotIDs)
}

func TestClientLeaseKeepAliveNotFoundDoesNotCloseOtherLeases(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()

	grpcServer := grpc.NewServer(server.ClientServerOptions()...)
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
	type leaseChannel struct {
		id clientv3.LeaseID
		ch <-chan *clientv3.LeaseKeepAliveResponse
	}
	channels := make([]leaseChannel, 0, 3)
	for i := 0; i < 3; i++ {
		grant, grantErr := client.Grant(ctx, 3)
		require.NoError(t, grantErr)
		keepAlive, keepAliveErr := client.KeepAlive(ctx, grant.ID)
		require.NoError(t, keepAliveErr)
		channels = append(channels, leaseChannel{id: grant.ID, ch: keepAlive})
	}
	for _, lease := range channels {
		response, ok := receiveClientKeepAlive(t, ctx, lease.ch)
		require.True(t, ok)
		require.Equal(t, lease.id, response.ID)
		require.Positive(t, response.TTL)
	}

	_, err = client.Revoke(ctx, channels[1].id)
	require.NoError(t, err)

	response, ok := receiveClientKeepAlive(t, ctx, channels[0].ch)
	require.True(t, ok, "revoking a different lease closed the first keepalive channel")
	require.Equal(t, channels[0].id, response.ID)
	require.Positive(t, response.TTL)
	response, ok = receiveClientKeepAlive(t, ctx, channels[2].ch)
	require.True(t, ok, "revoking a different lease closed the third keepalive channel")
	require.Equal(t, channels[2].id, response.ID)
	require.Positive(t, response.TTL)
	_, ok = receiveClientKeepAlive(t, ctx, channels[1].ch)
	require.False(t, ok, "revoked lease keepalive channel remained open")
}

func receiveClientKeepAlive(
	t *testing.T,
	ctx context.Context,
	ch <-chan *clientv3.LeaseKeepAliveResponse,
) (*clientv3.LeaseKeepAliveResponse, bool) {
	t.Helper()
	select {
	case response, ok := <-ch:
		return response, ok
	case <-ctx.Done():
		t.Fatalf("timed out waiting for lease keepalive response: %v", ctx.Err())
	}
	return nil, false
}

func TestClientNamespaceLeaseTimeToLiveFiltersAttachedKeys(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()

	grpcServer := grpc.NewServer(server.ClientServerOptions()...)
	etcdserverpb.RegisterKVServer(grpcServer, server)
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

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	tenantPrefix := fmt.Sprintf("/a1117/namespace-lease/%d/tenant/", time.Now().UnixNano())
	adjacentKey := tenantPrefix[:len(tenantPrefix)-1] + "0/outside"
	namespacedKV := namespace.NewKV(client.KV, tenantPrefix)
	namespacedLease := namespace.NewLease(client.Lease, tenantPrefix)

	grant, err := client.Grant(ctx, 30)
	require.NoError(t, err)
	for _, key := range []string{"alpha", "beta"} {
		_, err = namespacedKV.Put(ctx, key, "tenant-"+key, clientv3.WithLease(grant.ID))
		require.NoError(t, err)
	}
	_, err = client.Put(ctx, adjacentKey, "outside", clientv3.WithLease(grant.ID))
	require.NoError(t, err)
	_, err = client.Put(ctx, "/a1117/short", "short", clientv3.WithLease(grant.ID))
	require.NoError(t, err)

	raw, err := client.TimeToLive(ctx, grant.ID, clientv3.WithAttachedKeys())
	require.NoError(t, err)
	rawKeys := make([]string, 0, len(raw.Keys))
	for _, key := range raw.Keys {
		rawKeys = append(rawKeys, string(key))
	}
	slices.Sort(rawKeys)
	wantRawKeys := []string{
		"/a1117/short",
		adjacentKey,
		tenantPrefix + "alpha",
		tenantPrefix + "beta",
	}
	slices.Sort(wantRawKeys)
	require.Equal(t, wantRawKeys, rawKeys)

	filtered, err := namespacedLease.TimeToLive(ctx, grant.ID, clientv3.WithAttachedKeys())
	require.NoError(t, err)
	filteredKeys := make([]string, 0, len(filtered.Keys))
	for _, key := range filtered.Keys {
		filteredKeys = append(filteredKeys, string(key))
	}
	slices.Sort(filteredKeys)
	require.Equal(t, grant.ID, filtered.ID)
	require.Equal(t, int64(30), filtered.GrantedTTL)
	require.Positive(t, filtered.TTL)
	require.Equal(t, []string{"alpha", "beta"}, filteredKeys)

	withoutKeys, err := namespacedLease.TimeToLive(ctx, grant.ID)
	require.NoError(t, err)
	require.Equal(t, grant.ID, withoutKeys.ID)
	require.Empty(t, withoutKeys.Keys)
}

func TestClientLeaseConcurrentRenewLifecycleHasNoTransientNotFound(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()

	grpcServer := grpc.NewServer(server.ClientServerOptions()...)
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

	const workers = 12
	const rounds = 5
	var completed, zeroTTL, notFound, otherErrors atomic.Int64
	var wg sync.WaitGroup
	wg.Add(workers)
	for range workers {
		go func() {
			defer wg.Done()
			for range rounds {
				grant, grantErr := client.Grant(ctx, 60)
				if grantErr != nil {
					classifyLeaseRenewStressError(grantErr, &notFound, &otherErrors)
					continue
				}
				keepAlive, keepAliveErr := client.KeepAliveOnce(ctx, grant.ID)
				if keepAliveErr != nil {
					classifyLeaseRenewStressError(keepAliveErr, &notFound, &otherErrors)
					continue
				}
				if keepAlive.TTL == 0 {
					zeroTTL.Add(1)
				}
				if _, ttlErr := client.TimeToLive(ctx, grant.ID); ttlErr != nil {
					classifyLeaseRenewStressError(ttlErr, &notFound, &otherErrors)
					continue
				}
				if _, revokeErr := client.Revoke(ctx, grant.ID); revokeErr != nil {
					classifyLeaseRenewStressError(revokeErr, &notFound, &otherErrors)
					continue
				}
				completed.Add(1)
			}
		}()
	}
	wg.Wait()
	require.NoError(t, ctx.Err())
	require.Equal(t, int64(workers*rounds), completed.Load())
	require.Zero(t, zeroTTL.Load(), "live KeepAliveOnce must not return TTL=0")
	require.Zero(t, notFound.Load(), "freshly granted leases must not disappear during renew lifecycle")
	require.Zero(t, otherErrors.Load())
}

func classifyLeaseRenewStressError(err error, notFound, other *atomic.Int64) {
	if errors.Is(err, rpctypes.ErrLeaseNotFound) {
		notFound.Add(1)
		return
	}
	other.Add(1)
}

func TestClientLeaseSwitchConcurrentOldRevokePreservesNewBinding(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()

	grpcServer := grpc.NewServer(server.ClientServerOptions()...)
	etcdserverpb.RegisterKVServer(grpcServer, server)
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
	prefix := fmt.Sprintf("/a1084/lease-switch/%d/", time.Now().UnixNano())

	const rounds = 8
	for round := 0; round < rounds; round++ {
		leaseA, err := client.Grant(ctx, 300)
		require.NoError(t, err)
		leaseB, err := client.Grant(ctx, 300)
		require.NoError(t, err)
		key := fmt.Sprintf("%s%02d", prefix, round)
		_, err = client.Put(ctx, key, "lease-a", clientv3.WithLease(leaseA.ID))
		require.NoError(t, err)

		start := make(chan struct{})
		results := make(chan error, 2)
		go func() {
			<-start
			_, putErr := client.Put(ctx, key, "lease-b", clientv3.WithLease(leaseB.ID))
			results <- putErr
		}()
		go func() {
			<-start
			_, revokeErr := client.Revoke(ctx, leaseA.ID)
			results <- revokeErr
		}()
		close(start)
		require.NoError(t, <-results)
		require.NoError(t, <-results)

		current, err := client.Get(ctx, key)
		require.NoError(t, err)
		require.Len(t, current.Kvs, 1)
		require.Equal(t, "lease-b", string(current.Kvs[0].Value))
		require.Equal(t, int64(leaseB.ID), current.Kvs[0].Lease)
		oldTTL, err := client.TimeToLive(ctx, leaseA.ID)
		require.NoError(t, err)
		require.Equal(t, int64(-1), oldTTL.TTL)

		_, err = client.Revoke(ctx, leaseB.ID)
		require.NoError(t, err)
		after, err := client.Get(ctx, key)
		require.NoError(t, err)
		require.Empty(t, after.Kvs)
	}
}

func TestClientLeaseNaturalExpiryWatchPrevKVMatchesEtcd(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()

	grpcServer := grpc.NewServer(server.ClientServerOptions()...)
	etcdserverpb.RegisterKVServer(grpcServer, server)
	etcdserverpb.RegisterLeaseServer(grpcServer, server)
	etcdserverpb.RegisterWatchServer(grpcServer, server)
	listener := bufconn.Listen(1 << 20)
	go func() { _ = grpcServer.Serve(listener) }()
	t.Cleanup(grpcServer.Stop)

	client, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{"bufnet"},
		DialTimeout: time.Second,
		DialOptions: []grpc.DialOption{
			grpc.WithTransportCredentials(insecure.NewCredentials()),
			grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
				return listener.Dial()
			}),
		},
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, client.Close()) })

	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()
	prefix := fmt.Sprintf("/a1085/lease-expiry-watch/%d/", time.Now().UnixNano())
	base, err := client.Get(ctx, prefix, clientv3.WithPrefix())
	require.NoError(t, err)
	grant, err := client.Grant(ctx, 2)
	require.NoError(t, err)
	require.Equal(t, int64(2), grant.TTL)

	putB, err := client.Put(ctx, prefix+"b", "value-b", clientv3.WithLease(grant.ID))
	require.NoError(t, err)
	putA, err := client.Put(ctx, prefix+"a", "value-a", clientv3.WithLease(grant.ID))
	require.NoError(t, err)
	require.Equal(t, putB.Header.Revision+1, putA.Header.Revision)

	watchCtx, watchCancel := context.WithCancel(ctx)
	defer watchCancel()
	watch := client.Watch(watchCtx, prefix, clientv3.WithPrefix(), clientv3.WithRev(putA.Header.Revision+1), clientv3.WithPrevKV())

	var events []*clientv3.Event
	var watchRevision int64
	for len(events) < 2 {
		select {
		case response, ok := <-watch:
			require.True(t, ok, "watch closed before natural lease expiry")
			require.NoError(t, response.Err())
			watchRevision = response.Header.Revision
			events = append(events, response.Events...)
		case <-ctx.Done():
			t.Fatalf("lease did not expire before timeout: %v", ctx.Err())
		}
	}
	watchCancel()
	require.Len(t, events, 2)
	require.Equal(t, []string{prefix + "a", prefix + "b"}, []string{string(events[0].Kv.Key), string(events[1].Kv.Key)})
	require.Equal(t, []string{"value-a", "value-b"}, []string{string(events[0].PrevKv.Value), string(events[1].PrevKv.Value)})
	for _, event := range events {
		require.Equal(t, mvccpb.DELETE, event.Type)
		require.NotNil(t, event.PrevKv)
		require.Equal(t, watchRevision, event.Kv.ModRevision)
		require.Zero(t, event.Kv.Lease)
		require.Equal(t, int64(grant.ID), event.PrevKv.Lease)
		require.Equal(t, int64(1), event.PrevKv.Version)
		require.Positive(t, event.PrevKv.CreateRevision)
		require.Greater(t, event.PrevKv.CreateRevision, base.Header.Revision)
	}
	require.Equal(t, watchRevision, events[0].Kv.ModRevision)
	require.Equal(t, watchRevision, events[1].Kv.ModRevision)

	after, err := client.Get(ctx, prefix, clientv3.WithPrefix())
	require.NoError(t, err)
	require.Empty(t, after.Kvs)
	require.Equal(t, watchRevision, after.Header.Revision)
	unknown, err := client.TimeToLive(ctx, grant.ID, clientv3.WithAttachedKeys())
	require.NoError(t, err)
	require.Equal(t, grant.ID, unknown.ID)
	require.Equal(t, int64(-1), unknown.TTL)
	require.Zero(t, unknown.GrantedTTL)
	require.Empty(t, unknown.Keys)
	leases, err := client.Leases(ctx)
	require.NoError(t, err)
	for _, lease := range leases.Leases {
		require.NotEqual(t, grant.ID, lease.ID)
	}
}

func TestClientCorruptAlarmDefersNaturalLeaseExpiryUntilDisarm(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()

	grpcServer := grpc.NewServer(server.ClientServerOptions()...)
	etcdserverpb.RegisterKVServer(grpcServer, server)
	etcdserverpb.RegisterLeaseServer(grpcServer, server)
	etcdserverpb.RegisterMaintenanceServer(grpcServer, server)
	listener := bufconn.Listen(1 << 20)
	go func() { _ = grpcServer.Serve(listener) }()
	t.Cleanup(grpcServer.Stop)

	dialer := func(context.Context, string) (net.Conn, error) { return listener.Dial() }
	client, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{"bufnet"},
		DialTimeout: time.Second,
		DialOptions: []grpc.DialOption{
			grpc.WithTransportCredentials(insecure.NewCredentials()),
			grpc.WithContextDialer(dialer),
		},
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, client.Close()) })
	conn, err := grpc.NewClient("passthrough:///corrupt-lease-expiry",
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithContextDialer(dialer))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, conn.Close()) })
	maintenance := etcdserverpb.NewMaintenanceClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	statusResp, err := maintenance.Status(ctx, &etcdserverpb.StatusRequest{})
	require.NoError(t, err)
	memberID := statusResp.Header.MemberId
	require.NotZero(t, memberID)
	key := fmt.Sprintf("/a1086/corrupt-lease-expiry/%d", time.Now().UnixNano())
	grant, err := client.Grant(ctx, 2)
	require.NoError(t, err)
	_, err = client.Put(ctx, key, "leased", clientv3.WithLease(grant.ID))
	require.NoError(t, err)
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cleanupCancel()
		_, _ = maintenance.Alarm(cleanupCtx, &etcdserverpb.AlarmRequest{
			Action: etcdserverpb.AlarmRequest_DEACTIVATE, Alarm: etcdserverpb.AlarmType_CORRUPT, MemberID: memberID,
		})
		_, _ = client.Delete(cleanupCtx, key)
	})

	activated, err := maintenance.Alarm(ctx, &etcdserverpb.AlarmRequest{
		Action: etcdserverpb.AlarmRequest_ACTIVATE, Alarm: etcdserverpb.AlarmType_CORRUPT, MemberID: memberID,
	})
	require.NoError(t, err)
	require.Len(t, activated.Alarms, 1)
	time.Sleep(3 * time.Second)
	duringAlarm, err := client.Get(ctx, key)
	require.NoError(t, err)
	require.Len(t, duringAlarm.Kvs, 1)
	require.Equal(t, "leased", string(duringAlarm.Kvs[0].Value))
	ttl, err := client.TimeToLive(ctx, grant.ID)
	require.NoError(t, err)
	require.Negative(t, ttl.TTL)

	deactivated, err := maintenance.Alarm(ctx, &etcdserverpb.AlarmRequest{
		Action: etcdserverpb.AlarmRequest_DEACTIVATE, Alarm: etcdserverpb.AlarmType_CORRUPT, MemberID: memberID,
	})
	require.NoError(t, err)
	require.Len(t, deactivated.Alarms, 1)
	require.Eventually(t, func() bool {
		after, getErr := client.Get(ctx, key)
		return getErr == nil && len(after.Kvs) == 0
	}, 5*time.Second, 50*time.Millisecond)
	unknown, err := client.TimeToLive(ctx, grant.ID)
	require.NoError(t, err)
	require.Equal(t, int64(-1), unknown.TTL)
}

func TestRawGRPCCorruptAlarmKeepAliveLiveAndExpiredLeases(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()

	grpcServer := grpc.NewServer(server.ClientServerOptions()...)
	etcdserverpb.RegisterKVServer(grpcServer, server)
	etcdserverpb.RegisterLeaseServer(grpcServer, server)
	etcdserverpb.RegisterMaintenanceServer(grpcServer, server)
	listener := bufconn.Listen(1 << 20)
	go func() { _ = grpcServer.Serve(listener) }()
	t.Cleanup(grpcServer.Stop)

	conn, err := grpc.NewClient("passthrough:///corrupt-lease-keepalive",
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return listener.Dial() }))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, conn.Close()) })
	kv := etcdserverpb.NewKVClient(conn)
	lease := etcdserverpb.NewLeaseClient(conn)
	maintenance := etcdserverpb.NewMaintenanceClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), 12*time.Second)
	defer cancel()
	statusResp, err := maintenance.Status(ctx, &etcdserverpb.StatusRequest{})
	require.NoError(t, err)
	memberID := statusResp.Header.MemberId
	require.NotZero(t, memberID)
	prefix := fmt.Sprintf("/a1087/corrupt-lease-keepalive/%d/", time.Now().UnixNano())
	liveKey := []byte(prefix + "live")
	expiredKey := []byte(prefix + "expired")
	live, err := lease.LeaseGrant(ctx, &etcdserverpb.LeaseGrantRequest{TTL: 3})
	require.NoError(t, err)
	expired, err := lease.LeaseGrant(ctx, &etcdserverpb.LeaseGrantRequest{TTL: 2})
	require.NoError(t, err)
	_, err = kv.Put(ctx, &etcdserverpb.PutRequest{Key: liveKey, Value: []byte("live"), Lease: live.ID})
	require.NoError(t, err)
	_, err = kv.Put(ctx, &etcdserverpb.PutRequest{Key: expiredKey, Value: []byte("expired"), Lease: expired.ID})
	require.NoError(t, err)
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cleanupCancel()
		_, _ = maintenance.Alarm(cleanupCtx, &etcdserverpb.AlarmRequest{
			Action: etcdserverpb.AlarmRequest_DEACTIVATE, Alarm: etcdserverpb.AlarmType_CORRUPT, MemberID: memberID,
		})
		_, _ = kv.DeleteRange(cleanupCtx, &etcdserverpb.DeleteRangeRequest{
			Key: []byte(prefix), RangeEnd: []byte(clientv3.GetPrefixRangeEnd(prefix)),
		})
	})

	_, err = maintenance.Alarm(ctx, &etcdserverpb.AlarmRequest{
		Action: etcdserverpb.AlarmRequest_ACTIVATE, Alarm: etcdserverpb.AlarmType_CORRUPT, MemberID: memberID,
	})
	require.NoError(t, err)
	time.Sleep(2 * time.Second)
	liveStream, err := lease.LeaseKeepAlive(ctx)
	require.NoError(t, err)
	require.NoError(t, liveStream.Send(&etcdserverpb.LeaseKeepAliveRequest{ID: live.ID}))
	liveRenew, err := liveStream.Recv()
	require.NoError(t, err)
	require.Equal(t, live.ID, liveRenew.ID)
	require.Positive(t, liveRenew.TTL)
	require.LessOrEqual(t, liveRenew.TTL, int64(3))
	require.NoError(t, liveStream.CloseSend())

	time.Sleep(2 * time.Second)
	liveRead, err := kv.Range(ctx, &etcdserverpb.RangeRequest{Key: liveKey})
	require.NoError(t, err)
	require.Len(t, liveRead.Kvs, 1)
	expiredRead, err := kv.Range(ctx, &etcdserverpb.RangeRequest{Key: expiredKey})
	require.NoError(t, err)
	require.Len(t, expiredRead.Kvs, 1)

	expiredStream, err := lease.LeaseKeepAlive(ctx)
	require.NoError(t, err)
	require.NoError(t, expiredStream.Send(&etcdserverpb.LeaseKeepAliveRequest{ID: expired.ID}))
	type keepAliveResult struct {
		response *etcdserverpb.LeaseKeepAliveResponse
		err      error
	}
	renewed := make(chan keepAliveResult, 1)
	go func() {
		response, recvErr := expiredStream.Recv()
		renewed <- keepAliveResult{response: response, err: recvErr}
	}()
	select {
	case result := <-renewed:
		t.Fatalf("expired keepalive returned before corrupt alarm was disarmed: response=%v err=%v", result.response, result.err)
	case <-time.After(500 * time.Millisecond):
	}

	_, err = maintenance.Alarm(ctx, &etcdserverpb.AlarmRequest{
		Action: etcdserverpb.AlarmRequest_DEACTIVATE, Alarm: etcdserverpb.AlarmType_CORRUPT, MemberID: memberID,
	})
	require.NoError(t, err)
	select {
	case result := <-renewed:
		require.NoError(t, result.err)
		require.NotNil(t, result.response)
		require.Equal(t, expired.ID, result.response.ID)
		require.Zero(t, result.response.TTL)
	case <-time.After(5 * time.Second):
		t.Fatal("expired keepalive did not finish after corrupt alarm was disarmed")
	}
	require.NoError(t, expiredStream.CloseSend())
	require.Eventually(t, func() bool {
		read, rangeErr := kv.Range(ctx, &etcdserverpb.RangeRequest{Key: expiredKey})
		return rangeErr == nil && len(read.Kvs) == 0
	}, 5*time.Second, 50*time.Millisecond)
}

func TestClientLeaseTimeToLiveReportsZeroBeforeExpiry(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()

	grpcServer := grpc.NewServer(server.ClientServerOptions()...)
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

	ctx, cancel := context.WithTimeout(context.Background(), 7*time.Second)
	defer cancel()
	grant, err := client.Grant(ctx, 2)
	require.NoError(t, err)

	observedZero := false
	for {
		ttl, ttlErr := client.TimeToLive(ctx, grant.ID)
		require.NoError(t, ttlErr)
		require.Equal(t, grant.ID, ttl.ID)
		if ttl.TTL == 0 {
			observedZero = true
		}
		if ttl.TTL == -1 {
			break
		}
		select {
		case <-ctx.Done():
			t.Fatal("lease did not expire before timeout")
		case <-time.After(20 * time.Millisecond):
		}
	}
	require.True(t, observedZero, "live lease must report TTL=0 before TTL=-1")
}

func TestClientLeaseKeepAliveAtZeroTTLSurvivesOriginalDeadline(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()

	grpcServer := grpc.NewServer(server.ClientServerOptions()...)
	etcdserverpb.RegisterKVServer(grpcServer, server)
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
	key := fmt.Sprintf("/a1033/lease-keepalive-zero-ttl/%d", time.Now().UnixNano())
	grant, err := client.Grant(ctx, 2)
	require.NoError(t, err)
	_, err = client.Put(ctx, key, "value", clientv3.WithLease(grant.ID))
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		ttl, ttlErr := client.TimeToLive(ctx, grant.ID)
		return ttlErr == nil && ttl.TTL == 0
	}, 3*time.Second, 20*time.Millisecond, "lease never entered its live final subsecond")

	renewed, err := client.KeepAliveOnce(ctx, grant.ID)
	require.NoError(t, err)
	require.Equal(t, int64(2), renewed.TTL)

	time.Sleep(1200 * time.Millisecond)
	ttl, err := client.TimeToLive(ctx, grant.ID)
	require.NoError(t, err)
	require.NotEqual(t, int64(-1), ttl.TTL)
	got, err := client.Get(ctx, key)
	require.NoError(t, err)
	require.Len(t, got.Kvs, 1)
	require.Equal(t, int64(grant.ID), got.Kvs[0].Lease)
}

func TestClientLeaseRepeatedKeepAliveExtendsDeadlineAcrossWindows(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()

	grpcServer := grpc.NewServer(server.ClientServerOptions()...)
	etcdserverpb.RegisterKVServer(grpcServer, server)
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

	ctx, cancel := context.WithTimeout(context.Background(), 12*time.Second)
	defer cancel()
	key := fmt.Sprintf("/a1106/lease-repeated-renewal/%d", time.Now().UnixNano())
	grant, err := client.Grant(ctx, 2)
	require.NoError(t, err)
	_, err = client.Put(ctx, key, "value", clientv3.WithLease(grant.ID))
	require.NoError(t, err)

	start := time.Now()
	for round := 0; round < 4; round++ {
		select {
		case <-time.After(1200 * time.Millisecond):
		case <-ctx.Done():
			t.Fatalf("timed out before renewal round %d: %v", round+1, ctx.Err())
		}

		beforeRenew, err := client.Get(ctx, key)
		require.NoError(t, err)
		require.Len(t, beforeRenew.Kvs, 1)
		require.Equal(t, int64(grant.ID), beforeRenew.Kvs[0].Lease)

		renewed, err := client.KeepAliveOnce(ctx, grant.ID)
		require.NoError(t, err)
		require.Equal(t, grant.ID, renewed.ID)
		require.Equal(t, int64(2), renewed.TTL)

		ttl, err := client.TimeToLive(ctx, grant.ID)
		require.NoError(t, err)
		require.Equal(t, grant.ID, ttl.ID)
		require.Positive(t, ttl.TTL)
		require.Equal(t, int64(2), ttl.GrantedTTL)
	}
	require.Greater(t, time.Since(start), 4*time.Second)

	select {
	case <-time.After(1200 * time.Millisecond):
	case <-ctx.Done():
		t.Fatalf("timed out after repeated renewals: %v", ctx.Err())
	}
	got, err := client.Get(ctx, key)
	require.NoError(t, err)
	require.Len(t, got.Kvs, 1)
	require.Equal(t, int64(grant.ID), got.Kvs[0].Lease)
}

func TestClientLeaseBatchPartialRenewalIsolatesOriginalDeadlines(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()

	grpcServer := grpc.NewServer(server.ClientServerOptions()...)
	etcdserverpb.RegisterKVServer(grpcServer, server)
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
	type leaseItem struct {
		id      clientv3.LeaseID
		key     string
		value   string
		renewed bool
	}
	const leaseCount = 4
	prefix := fmt.Sprintf("/a1116/lease-batch-renewal/%d/", time.Now().UnixNano())
	items := make([]leaseItem, 0, leaseCount)
	for index := 0; index < leaseCount; index++ {
		grant, grantErr := client.Grant(ctx, 2)
		require.NoError(t, grantErr)
		item := leaseItem{
			id:      grant.ID,
			key:     fmt.Sprintf("%s%02d", prefix, index),
			value:   fmt.Sprintf("value-%d", index),
			renewed: index%2 == 0,
		}
		_, putErr := client.Put(ctx, item.key, item.value, clientv3.WithLease(item.id))
		require.NoError(t, putErr)
		items = append(items, item)
	}

	time.Sleep(1200 * time.Millisecond)
	for _, item := range items {
		if !item.renewed {
			continue
		}
		renewed, renewErr := client.KeepAliveOnce(ctx, item.id)
		require.NoError(t, renewErr)
		require.Equal(t, item.id, renewed.ID)
		require.Equal(t, int64(2), renewed.TTL)
	}
	time.Sleep(1200 * time.Millisecond)

	for _, item := range items {
		got, getErr := client.Get(ctx, item.key)
		require.NoError(t, getErr)
		ttl, ttlErr := client.TimeToLive(ctx, item.id)
		require.NoError(t, ttlErr)
		if item.renewed {
			require.Len(t, got.Kvs, 1, "renewed lease key expired at its original deadline")
			require.Equal(t, item.value, string(got.Kvs[0].Value))
			require.Equal(t, int64(item.id), got.Kvs[0].Lease)
			require.NotEqual(t, int64(-1), ttl.TTL)
			continue
		}
		require.Empty(t, got.Kvs, "unrenewed lease key survived past its original deadline")
		require.Equal(t, int64(-1), ttl.TTL)
	}
}

func TestClientExpiredKeepAliveResponseFollowsKeyDeletion(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()

	grpcServer := grpc.NewServer(server.ClientServerOptions()...)
	etcdserverpb.RegisterKVServer(grpcServer, server)
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
	const leases = 16
	type leasedKey struct {
		id  clientv3.LeaseID
		key string
	}
	items := make([]leasedKey, 0, leases)
	prefix := fmt.Sprintf("/a1034/lease-expired-keepalive-order/%d/", time.Now().UnixNano())
	for index := 0; index < leases; index++ {
		grant, grantErr := client.Grant(ctx, 2)
		require.NoError(t, grantErr)
		key := fmt.Sprintf("%s%02d", prefix, index)
		_, putErr := client.Put(ctx, key, "value", clientv3.WithLease(grant.ID))
		require.NoError(t, putErr)
		items = append(items, leasedKey{id: grant.ID, key: key})
	}

	time.Sleep(2 * time.Second)
	var wg sync.WaitGroup
	errs := make(chan error, leases)
	for _, item := range items {
		item := item
		wg.Add(1)
		go func() {
			defer wg.Done()
			renewed, keepAliveErr := client.KeepAliveOnce(ctx, item.id)
			got, getErr := client.Get(ctx, item.key)
			if getErr != nil {
				errs <- fmt.Errorf("get %q: %w", item.key, getErr)
				return
			}
			switch {
			case keepAliveErr == nil:
				if renewed == nil || renewed.TTL <= 0 || len(got.Kvs) != 1 {
					errs <- fmt.Errorf("successful renewal %d returned %+v with %d keys", item.id, renewed, len(got.Kvs))
					return
				}
				if got.Kvs[0].Lease != int64(item.id) {
					errs <- fmt.Errorf("renewed key %q carries lease %d, want %d", item.key, got.Kvs[0].Lease, item.id)
				}
			case errors.Is(keepAliveErr, rpctypes.ErrLeaseNotFound):
				if len(got.Kvs) != 0 {
					errs <- fmt.Errorf("not-found renewal %d returned before attached key deletion", item.id)
				}
			default:
				errs <- fmt.Errorf("keepalive %d: %w", item.id, keepAliveErr)
			}
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		require.NoError(t, err)
	}
}

func TestClientLeaseSurvivesCompaction(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()

	grpcServer := grpc.NewServer(server.ClientServerOptions()...)
	etcdserverpb.RegisterKVServer(grpcServer, server)
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
	grant, err := client.Grant(ctx, 300)
	require.NoError(t, err)
	key := fmt.Sprintf("/a1035/lease-compaction/%d/leased", time.Now().UnixNano())
	_, err = client.Put(ctx, key, "value", clientv3.WithLease(grant.ID))
	require.NoError(t, err)
	for index := 0; index < 5; index++ {
		keepAlive, keepAliveErr := client.KeepAliveOnce(ctx, grant.ID)
		require.NoError(t, keepAliveErr)
		require.Equal(t, grant.ID, keepAlive.ID)
		require.Positive(t, keepAlive.TTL)
	}

	current, err := client.Get(ctx, key)
	require.NoError(t, err)
	require.Len(t, current.Kvs, 1)
	_, err = client.Compact(ctx, current.Header.Revision, clientv3.WithCompactPhysical())
	require.NoError(t, err)

	got, err := client.Get(ctx, key)
	require.NoError(t, err)
	require.Len(t, got.Kvs, 1, "bound key must survive compaction")
	require.Equal(t, int64(grant.ID), got.Kvs[0].Lease)
	ttl, err := client.TimeToLive(ctx, grant.ID, clientv3.WithAttachedKeys())
	require.NoError(t, err)
	require.Positive(t, ttl.TTL)
	require.Contains(t, byteKeysToStrings(ttl.Keys), key)
	keepAlive, err := client.KeepAliveOnce(ctx, grant.ID)
	require.NoError(t, err)
	require.Positive(t, keepAlive.TTL)
}

func TestRawGRPCLeaseReadBoundaryAndList(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()

	grpcServer := grpc.NewServer(server.ClientServerOptions()...)
	etcdserverpb.RegisterKVServer(grpcServer, server)
	etcdserverpb.RegisterLeaseServer(grpcServer, server)
	listener := bufconn.Listen(1 << 20)
	go func() { _ = grpcServer.Serve(listener) }()
	t.Cleanup(grpcServer.Stop)

	conn, err := grpc.NewClient("passthrough:///lease-read-boundary-client",
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
			return listener.Dial()
		}))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, conn.Close()) })
	kv := etcdserverpb.NewKVClient(conn)
	lease := etcdserverpb.NewLeaseClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	zeroWithoutKeys, err := lease.LeaseTimeToLive(ctx, &etcdserverpb.LeaseTimeToLiveRequest{})
	require.NoError(t, err)
	zeroWithKeys, err := lease.LeaseTimeToLive(ctx, &etcdserverpb.LeaseTimeToLiveRequest{Keys: true})
	require.NoError(t, err)
	require.Equal(t, int64(0), zeroWithoutKeys.ID)
	require.Equal(t, int64(-1), zeroWithoutKeys.TTL)
	require.Zero(t, zeroWithoutKeys.GrantedTTL)
	require.Empty(t, zeroWithoutKeys.Keys)
	require.Empty(t, zeroWithKeys.Keys)
	require.Equal(t, zeroWithoutKeys.ID, zeroWithKeys.ID)
	require.Equal(t, zeroWithoutKeys.TTL, zeroWithKeys.TTL)
	require.Equal(t, zeroWithoutKeys.GrantedTTL, zeroWithKeys.GrantedTTL)
	requireClientLeaseHeaderWellFormed(t, zeroWithoutKeys.Header)
	requireClientLeaseHeaderWellFormed(t, zeroWithKeys.Header)

	leaseID := time.Now().UnixNano() & ((1 << 62) - 1)
	grant, err := lease.LeaseGrant(ctx, &etcdserverpb.LeaseGrantRequest{ID: leaseID, TTL: 300})
	require.NoError(t, err)
	require.Equal(t, leaseID, grant.ID)
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cleanupCancel()
		_, _ = lease.LeaseRevoke(cleanupCtx, &etcdserverpb.LeaseRevokeRequest{ID: leaseID})
	})
	prefix := fmt.Sprintf("/a1017/lease-read-boundary/%d/", time.Now().UnixNano())
	keys := []string{prefix + "z", prefix + "a"}
	for _, key := range keys {
		_, err = kv.Put(ctx, &etcdserverpb.PutRequest{
			Key: []byte(key), Value: []byte("value"), Lease: leaseID,
		})
		require.NoError(t, err)
	}

	liveWithoutKeys, err := lease.LeaseTimeToLive(ctx, &etcdserverpb.LeaseTimeToLiveRequest{ID: leaseID})
	require.NoError(t, err)
	require.Equal(t, leaseID, liveWithoutKeys.ID)
	require.Positive(t, liveWithoutKeys.TTL)
	require.LessOrEqual(t, liveWithoutKeys.TTL, int64(300))
	require.Equal(t, int64(300), liveWithoutKeys.GrantedTTL)
	require.Empty(t, liveWithoutKeys.Keys)
	requireClientLeaseHeaderWellFormed(t, liveWithoutKeys.Header)

	liveWithKeys, err := lease.LeaseTimeToLive(ctx, &etcdserverpb.LeaseTimeToLiveRequest{ID: leaseID, Keys: true})
	require.NoError(t, err)
	require.Equal(t, leaseID, liveWithKeys.ID)
	require.Positive(t, liveWithKeys.TTL)
	require.LessOrEqual(t, liveWithKeys.TTL, int64(300))
	require.Equal(t, int64(300), liveWithKeys.GrantedTTL)
	gotKeys := make([]string, 0, len(liveWithKeys.Keys))
	for _, key := range liveWithKeys.Keys {
		gotKeys = append(gotKeys, string(key))
	}
	slices.Sort(keys)
	slices.Sort(gotKeys)
	require.Equal(t, keys, gotKeys)
	requireClientLeaseHeaderWellFormed(t, liveWithKeys.Header)

	list, err := lease.LeaseLeases(ctx, &etcdserverpb.LeaseLeasesRequest{})
	require.NoError(t, err)
	require.Contains(t, leaseIDsFromList(list), leaseID)
	for _, status := range list.Leases {
		require.NotZero(t, status.ID)
	}
	requireClientLeaseHeaderWellFormed(t, list.Header)
}

func TestRawGRPCLeaseLeasesOrdersByExpiry(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()

	grpcServer := grpc.NewServer(server.ClientServerOptions()...)
	etcdserverpb.RegisterLeaseServer(grpcServer, server)
	listener := bufconn.Listen(1 << 20)
	go func() { _ = grpcServer.Serve(listener) }()
	t.Cleanup(grpcServer.Stop)

	conn, err := grpc.NewClient("passthrough:///lease-list-order-client",
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
			return listener.Dial()
		}))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, conn.Close()) })
	lease := etcdserverpb.NewLeaseClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	for _, id := range []int64{30_003, 30_001, 30_004, 30_002} {
		_, err = lease.LeaseGrant(ctx, &etcdserverpb.LeaseGrantRequest{TTL: 300, ID: id})
		require.NoError(t, err)
		t.Cleanup(func() {
			cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cleanupCancel()
			_, _ = lease.LeaseRevoke(cleanupCtx, &etcdserverpb.LeaseRevokeRequest{ID: id})
		})
	}

	server.leaseMu.Lock()
	now := time.Now()
	server.leases[30_003].deadline = now.Add(30 * time.Second)
	server.leases[30_001].deadline = now.Add(10 * time.Second)
	server.leases[30_004].deadline = now.Add(20 * time.Second)
	server.leases[30_002].deadline = now.Add(20 * time.Second)
	server.leaseMu.Unlock()

	list, err := lease.LeaseLeases(ctx, &etcdserverpb.LeaseLeasesRequest{})
	require.NoError(t, err)
	requireClientLeaseHeaderWellFormed(t, list.Header)
	require.Equal(t, []int64{30_001, 30_002, 30_004, 30_003}, leaseIDsFromList(list))
}

func TestClientLeaseRevokeDeletesAttachedKeysAtOneRevision(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()

	grpcServer := grpc.NewServer(server.ClientServerOptions()...)
	etcdserverpb.RegisterKVServer(grpcServer, server)
	etcdserverpb.RegisterLeaseServer(grpcServer, server)
	etcdserverpb.RegisterWatchServer(grpcServer, server)
	listener := bufconn.Listen(1 << 20)
	go func() { _ = grpcServer.Serve(listener) }()
	t.Cleanup(grpcServer.Stop)

	client, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{"bufnet"},
		DialTimeout: time.Second,
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
	prefix := fmt.Sprintf("/a1013/lease-revoke-atomic/%d/", time.Now().UnixNano())
	grant, err := client.Grant(ctx, 30)
	require.NoError(t, err)
	_, err = client.Put(ctx, prefix+"b", "value-b", clientv3.WithLease(grant.ID))
	require.NoError(t, err)
	lastPut, err := client.Put(ctx, prefix+"a", "value-a", clientv3.WithLease(grant.ID))
	require.NoError(t, err)

	watchCtx, watchCancel := context.WithCancel(ctx)
	defer watchCancel()
	watch := client.Watch(
		watchCtx,
		prefix,
		clientv3.WithPrefix(),
		clientv3.WithRev(lastPut.Header.Revision+1),
		clientv3.WithPrevKV(),
	)
	revoke, err := client.Revoke(ctx, grant.ID)
	require.NoError(t, err)
	require.Greater(t, revoke.Header.Revision, lastPut.Header.Revision)

	events := make([]*clientv3.Event, 0, 2)
	for len(events) < 2 {
		select {
		case response, ok := <-watch:
			require.True(t, ok)
			require.NoError(t, response.Err())
			require.Equal(t, revoke.Header.Revision, response.Header.Revision)
			events = append(events, response.Events...)
		case <-ctx.Done():
			t.Fatalf("lease revoke delete events not received: %v", ctx.Err())
		}
	}
	require.Len(t, events, 2)
	for _, event := range events {
		require.Equal(t, mvccpb.DELETE, event.Type)
		require.Equal(t, revoke.Header.Revision, event.Kv.ModRevision)
		require.NotNil(t, event.PrevKv)
		require.Equal(t, int64(grant.ID), event.PrevKv.Lease)
		require.Contains(t, []string{"value-a", "value-b"}, string(event.PrevKv.Value))
	}
	require.Equal(t, prefix+"a", string(events[0].Kv.Key))
	require.Equal(t, prefix+"b", string(events[1].Kv.Key))

	got, err := client.Get(ctx, prefix, clientv3.WithPrefix())
	require.NoError(t, err)
	require.Empty(t, got.Kvs)
	ttl, err := client.TimeToLive(ctx, grant.ID, clientv3.WithAttachedKeys())
	require.NoError(t, err)
	require.Equal(t, int64(-1), ttl.TTL)
	require.Empty(t, ttl.Keys)
}

func TestClientLeaseKeepAliveClosesAfterRevoke(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()

	grpcServer := grpc.NewServer(server.ClientServerOptions()...)
	etcdserverpb.RegisterKVServer(grpcServer, server)
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
	grant, err := client.Grant(ctx, 10)
	require.NoError(t, err)
	key := fmt.Sprintf("/a1015/lease-keepalive-revoke-buffer/%d", time.Now().UnixNano())
	_, err = client.Put(ctx, key, "value", clientv3.WithLease(grant.ID))
	require.NoError(t, err)

	keepAlive, err := client.KeepAlive(ctx, grant.ID)
	require.NoError(t, err)
	var initial *clientv3.LeaseKeepAliveResponse
	select {
	case initial = <-keepAlive:
	case <-ctx.Done():
		t.Fatalf("initial keepalive response not received: %v", ctx.Err())
	}
	require.NotNil(t, initial)
	require.Equal(t, grant.ID, initial.ID)
	require.Positive(t, initial.TTL)

	_, err = client.Revoke(ctx, grant.ID)
	require.NoError(t, err)
	for {
		select {
		case response, ok := <-keepAlive:
			if !ok {
				got, getErr := client.Get(ctx, key)
				require.NoError(t, getErr)
				require.Empty(t, got.Kvs)
				ttl, ttlErr := client.TimeToLive(ctx, grant.ID)
				require.NoError(t, ttlErr)
				require.Equal(t, int64(-1), ttl.TTL)
				return
			}
			require.NotNil(t, response)
			require.Equal(t, grant.ID, response.ID)
			require.Positive(t, response.TTL)
		case <-ctx.Done():
			t.Fatalf("keepalive channel did not close after revoke: %v", ctx.Err())
		}
	}
}

func TestClientLeaseKeepAliveContextCancelIsolationAndClose(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()

	grpcServer := grpc.NewServer(server.ClientServerOptions()...)
	etcdserverpb.RegisterKVServer(grpcServer, server)
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
	t.Cleanup(func() { _ = client.Close() })

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	grant, err := client.Grant(ctx, 10)
	require.NoError(t, err)

	type uncomparableCtx struct {
		context.Context
		_ func()
	}
	firstCtx, firstCancel := context.WithCancel(ctx)
	defer firstCancel()
	first, err := client.KeepAlive(uncomparableCtx{Context: firstCtx}, grant.ID)
	require.NoError(t, err)
	firstResponse := requireClientLeaseKeepAliveResponse(t, ctx, first)
	require.Equal(t, grant.ID, firstResponse.ID)
	require.Positive(t, firstResponse.TTL)

	secondCtx, secondCancel := context.WithCancel(ctx)
	second, err := client.KeepAlive(uncomparableCtx{Context: secondCtx}, grant.ID)
	require.NoError(t, err)
	secondResponse := requireClientLeaseKeepAliveResponse(t, ctx, second)
	require.Equal(t, grant.ID, secondResponse.ID)
	require.Positive(t, secondResponse.TTL)

	secondCancel()
	requireClientLeaseKeepAliveClosed(t, ctx, second)
	select {
	case response, ok := <-first:
		require.True(t, ok, "canceling second keepalive context must not close the first channel")
		require.NotNil(t, response)
		require.Equal(t, grant.ID, response.ID)
		require.Positive(t, response.TTL)
	default:
	}

	require.NoError(t, client.Close())
	requireClientLeaseKeepAliveClosed(t, ctx, first)
}

func TestRawGRPCLeaseSignedIDBoundaries(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()

	grpcServer := grpc.NewServer(server.ClientServerOptions()...)
	etcdserverpb.RegisterKVServer(grpcServer, server)
	etcdserverpb.RegisterLeaseServer(grpcServer, server)
	listener := bufconn.Listen(1 << 20)
	go func() { _ = grpcServer.Serve(listener) }()
	t.Cleanup(grpcServer.Stop)

	conn, err := grpc.NewClient("passthrough:///lease-signed-id-client",
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
			return listener.Dial()
		}))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, conn.Close()) })
	kv := etcdserverpb.NewKVClient(conn)
	lease := etcdserverpb.NewLeaseClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	for _, tt := range []struct {
		name string
		id   int64
	}{
		{name: "negative-one", id: -1},
		{name: "minimum", id: math.MinInt64},
		{name: "maximum", id: math.MaxInt64},
	} {
		t.Run(tt.name, func(t *testing.T) {
			key := []byte(fmt.Sprintf("/a1012/lease-signed-id/%d/%s", time.Now().UnixNano(), tt.name))
			before, err := kv.Range(ctx, &etcdserverpb.RangeRequest{Key: key})
			require.NoError(t, err)
			grant, err := lease.LeaseGrant(ctx, &etcdserverpb.LeaseGrantRequest{ID: tt.id, TTL: 300})
			require.NoError(t, err)
			require.Equal(t, tt.id, grant.ID)
			require.Equal(t, before.Header.Revision, grant.Header.Revision)
			requireClientLeaseHeaderWellFormed(t, grant.Header)
			t.Cleanup(func() {
				cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cleanupCancel()
				_, _ = lease.LeaseRevoke(cleanupCtx, &etcdserverpb.LeaseRevokeRequest{ID: tt.id})
			})

			put, err := kv.Put(ctx, &etcdserverpb.PutRequest{Key: key, Value: []byte("value"), Lease: tt.id})
			require.NoError(t, err)
			requireClientLeaseHeaderWellFormed(t, put.Header)

			withoutKeys, err := lease.LeaseTimeToLive(ctx, &etcdserverpb.LeaseTimeToLiveRequest{ID: tt.id})
			require.NoError(t, err)
			require.Equal(t, tt.id, withoutKeys.ID)
			require.Positive(t, withoutKeys.TTL)
			require.LessOrEqual(t, withoutKeys.TTL, int64(300))
			require.Equal(t, int64(300), withoutKeys.GrantedTTL)
			require.Empty(t, withoutKeys.Keys)
			requireClientLeaseHeaderWellFormed(t, withoutKeys.Header)

			withKeys, err := lease.LeaseTimeToLive(ctx, &etcdserverpb.LeaseTimeToLiveRequest{ID: tt.id, Keys: true})
			require.NoError(t, err)
			require.Equal(t, tt.id, withKeys.ID)
			require.Positive(t, withKeys.TTL)
			require.LessOrEqual(t, withKeys.TTL, int64(300))
			require.Equal(t, int64(300), withKeys.GrantedTTL)
			require.Equal(t, [][]byte{key}, withKeys.Keys)
			requireClientLeaseHeaderWellFormed(t, withKeys.Header)

			list, err := lease.LeaseLeases(ctx, &etcdserverpb.LeaseLeasesRequest{})
			require.NoError(t, err)
			require.Contains(t, leaseIDsFromList(list), tt.id)
			requireClientLeaseHeaderWellFormed(t, list.Header)

			_, err = lease.LeaseRevoke(ctx, &etcdserverpb.LeaseRevokeRequest{ID: tt.id})
			require.NoError(t, err)
			unknown, err := lease.LeaseTimeToLive(ctx, &etcdserverpb.LeaseTimeToLiveRequest{ID: tt.id, Keys: true})
			require.NoError(t, err)
			require.Equal(t, tt.id, unknown.ID)
			require.Equal(t, int64(-1), unknown.TTL)
			require.Zero(t, unknown.GrantedTTL)
			require.Empty(t, unknown.Keys)
			require.GreaterOrEqual(t, unknown.Header.Revision, put.Header.Revision)
		})
	}
}

func TestRawGRPCLeaseKeepAliveRevokeBoundaries(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()

	grpcServer := grpc.NewServer(server.ClientServerOptions()...)
	etcdserverpb.RegisterLeaseServer(grpcServer, server)
	listener := bufconn.Listen(1 << 20)
	go func() { _ = grpcServer.Serve(listener) }()
	t.Cleanup(grpcServer.Stop)

	conn, err := grpc.NewClient("passthrough:///lease-keepalive-boundary-client",
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
			return listener.Dial()
		}))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, conn.Close()) })
	lease := etcdserverpb.NewLeaseClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	liveIDs := []int64{-1, math.MinInt64, math.MaxInt64}
	for _, id := range liveIDs {
		grant, grantErr := lease.LeaseGrant(ctx, &etcdserverpb.LeaseGrantRequest{ID: id, TTL: 30})
		require.NoError(t, grantErr)
		require.Equal(t, id, grant.ID)
		requireClientLeaseHeaderWellFormed(t, grant.Header)
		t.Cleanup(func() {
			cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cleanupCancel()
			_, _ = lease.LeaseRevoke(cleanupCtx, &etcdserverpb.LeaseRevokeRequest{ID: id})
		})
	}

	stream, err := lease.LeaseKeepAlive(ctx)
	require.NoError(t, err)
	for _, tt := range []struct {
		name string
		id   int64
		live bool
	}{
		{name: "zero", id: 0},
		{name: "unknown", id: 13_400},
		{name: "negative-one", id: -1, live: true},
		{name: "minimum", id: math.MinInt64, live: true},
		{name: "maximum", id: math.MaxInt64, live: true},
	} {
		t.Run("keepalive-"+tt.name, func(t *testing.T) {
			require.NoError(t, stream.Send(&etcdserverpb.LeaseKeepAliveRequest{ID: tt.id}))
			response, recvErr := stream.Recv()
			require.NoError(t, recvErr)
			require.Equal(t, tt.id, response.ID)
			requireClientLeaseHeaderWellFormed(t, response.Header)
			if tt.live {
				require.Positive(t, response.TTL)
				require.LessOrEqual(t, response.TTL, int64(30))
			} else {
				require.Zero(t, response.TTL)
			}
		})
	}
	require.NoError(t, stream.CloseSend())

	for _, id := range liveIDs {
		revoked, err := lease.LeaseRevoke(ctx, &etcdserverpb.LeaseRevokeRequest{ID: id})
		require.NoError(t, err)
		requireClientLeaseHeaderWellFormed(t, revoked.Header)
		_, err = lease.LeaseRevoke(ctx, &etcdserverpb.LeaseRevokeRequest{ID: id})
		requireRawLeaseError(t, err, rpctypes.ErrGRPCLeaseNotFound, codes.NotFound, "etcdserver: requested lease not found")
	}
	for _, id := range []int64{0, 13_400} {
		_, err := lease.LeaseRevoke(ctx, &etcdserverpb.LeaseRevokeRequest{ID: id})
		requireRawLeaseError(t, err, rpctypes.ErrGRPCLeaseNotFound, codes.NotFound, "etcdserver: requested lease not found")
	}

	afterRevoke, err := lease.LeaseKeepAlive(ctx)
	require.NoError(t, err)
	require.NoError(t, afterRevoke.Send(&etcdserverpb.LeaseKeepAliveRequest{ID: -1}))
	missing, err := afterRevoke.Recv()
	require.NoError(t, err)
	require.Equal(t, int64(-1), missing.ID)
	require.Zero(t, missing.TTL)
	requireClientLeaseHeaderWellFormed(t, missing.Header)
	require.NoError(t, afterRevoke.CloseSend())
}

func TestRawGRPCLeaseGrantTTLAndIDBoundaries(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()

	grpcServer := grpc.NewServer(server.ClientServerOptions()...)
	etcdserverpb.RegisterLeaseServer(grpcServer, server)
	listener := bufconn.Listen(1 << 20)
	go func() { _ = grpcServer.Serve(listener) }()
	t.Cleanup(grpcServer.Stop)

	conn, err := grpc.NewClient("passthrough:///lease-grant-boundary-client",
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
			return listener.Dial()
		}))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, conn.Close()) })
	lease := etcdserverpb.NewLeaseClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	granted := make([]int64, 0, 8)
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cleanupCancel()
		for _, id := range granted {
			_, _ = lease.LeaseRevoke(cleanupCtx, &etcdserverpb.LeaseRevokeRequest{ID: id})
		}
	})

	for _, tt := range []struct {
		name    string
		id      int64
		ttl     int64
		wantTTL int64
	}{
		{name: "minimum-int64", id: 13_301, ttl: math.MinInt64, wantTTL: minLeaseTTL},
		{name: "negative-one", id: 13_302, ttl: -1, wantTTL: minLeaseTTL},
		{name: "zero", id: 13_303, ttl: 0, wantTTL: minLeaseTTL},
		{name: "one", id: 13_304, ttl: 1, wantTTL: minLeaseTTL},
		{name: "minimum", id: 13_305, ttl: minLeaseTTL, wantTTL: minLeaseTTL},
		{name: "maximum", id: 13_306, ttl: maxLeaseTTL, wantTTL: maxLeaseTTL},
	} {
		t.Run(tt.name, func(t *testing.T) {
			grant, err := lease.LeaseGrant(ctx, &etcdserverpb.LeaseGrantRequest{ID: tt.id, TTL: tt.ttl})
			require.NoError(t, err)
			require.Equal(t, tt.id, grant.ID)
			require.Equal(t, tt.wantTTL, grant.TTL)
			requireClientLeaseHeaderWellFormed(t, grant.Header)
			granted = append(granted, grant.ID)

			ttl, err := lease.LeaseTimeToLive(ctx, &etcdserverpb.LeaseTimeToLiveRequest{ID: grant.ID})
			require.NoError(t, err)
			require.Equal(t, tt.wantTTL, ttl.GrantedTTL)
		})
	}

	for _, tt := range []struct {
		name string
		id   int64
		ttl  int64
	}{
		{name: "above-maximum", id: 13_307, ttl: maxLeaseTTL + 1},
		{name: "maximum-int64", id: 13_308, ttl: math.MaxInt64},
	} {
		t.Run(tt.name, func(t *testing.T) {
			_, err := lease.LeaseGrant(ctx, &etcdserverpb.LeaseGrantRequest{ID: tt.id, TTL: tt.ttl})
			requireRawGRPCLeaseError(t, err, codes.OutOfRange, "etcdserver: too large lease TTL", rpctypes.ErrGRPCLeaseTTLTooLarge)
		})
	}

	automatic, err := lease.LeaseGrant(ctx, &etcdserverpb.LeaseGrantRequest{TTL: 10})
	require.NoError(t, err)
	require.NotZero(t, automatic.ID)
	require.Equal(t, int64(10), automatic.TTL)
	requireClientLeaseHeaderWellFormed(t, automatic.Header)
	granted = append(granted, automatic.ID)

	const duplicateID = int64(13_309)
	first, err := lease.LeaseGrant(ctx, &etcdserverpb.LeaseGrantRequest{ID: duplicateID, TTL: 10})
	require.NoError(t, err)
	granted = append(granted, first.ID)
	_, err = lease.LeaseGrant(ctx, &etcdserverpb.LeaseGrantRequest{ID: duplicateID, TTL: 20})
	requireRawGRPCLeaseError(t, err, codes.FailedPrecondition, "etcdserver: lease already exists", rpctypes.ErrGRPCLeaseExist)
	_, err = lease.LeaseGrant(ctx, &etcdserverpb.LeaseGrantRequest{ID: duplicateID, TTL: maxLeaseTTL + 1})
	requireRawGRPCLeaseError(t, err, codes.OutOfRange, "etcdserver: too large lease TTL", rpctypes.ErrGRPCLeaseTTLTooLarge)
}

func requireClientLeaseHeaderWellFormed(t *testing.T, header *etcdserverpb.ResponseHeader) {
	t.Helper()
	require.NotNil(t, header)
	require.NotZero(t, header.ClusterId)
	require.NotZero(t, header.MemberId)
	require.Positive(t, header.Revision)
	require.Positive(t, header.RaftTerm)
}

func requireClientLeaseError(t *testing.T, err error, code codes.Code, message string, wantErrorIs ...error) {
	t.Helper()
	require.EqualError(t, err, message)
	for _, want := range wantErrorIs {
		require.ErrorIs(t, err, want)
	}
	require.Equal(t, code, status.Code(err))
	require.Equal(t, message, status.Convert(err).Message())
}

func requireRawGRPCLeaseError(t *testing.T, err error, code codes.Code, message string, wantErrorIs ...error) {
	t.Helper()
	require.EqualError(t, err, status.Error(code, message).Error())
	for _, want := range wantErrorIs {
		require.ErrorIs(t, err, want)
	}
	require.Equal(t, code, status.Code(err))
	require.Equal(t, message, status.Convert(err).Message())
}

func requireRawLeaseError(t *testing.T, err error, want error, code codes.Code, message string) {
	t.Helper()
	require.ErrorIs(t, err, want)
	require.Equal(t, code, status.Code(err))
	require.Equal(t, message, status.Convert(err).Message())
}

func requireClientLeaseKeepAliveResponse(
	t *testing.T,
	ctx context.Context,
	ch <-chan *clientv3.LeaseKeepAliveResponse,
) *clientv3.LeaseKeepAliveResponse {
	t.Helper()
	select {
	case response, ok := <-ch:
		require.True(t, ok, "keepalive channel closed before response")
		require.NotNil(t, response)
		return response
	case <-ctx.Done():
		t.Fatalf("timed out waiting for keepalive response: %v", ctx.Err())
		return nil
	}
}

func requireClientLeaseKeepAliveClosed(
	t *testing.T,
	ctx context.Context,
	ch <-chan *clientv3.LeaseKeepAliveResponse,
) {
	t.Helper()
	select {
	case response, ok := <-ch:
		require.False(t, ok, "expected closed keepalive channel, got response=%+v", response)
	case <-ctx.Done():
		t.Fatalf("timed out waiting for keepalive channel close: %v", ctx.Err())
	}
}

func byteKeysToStrings(keys [][]byte) []string {
	out := make([]string, 0, len(keys))
	for _, key := range keys {
		out = append(out, string(key))
	}
	return out
}
