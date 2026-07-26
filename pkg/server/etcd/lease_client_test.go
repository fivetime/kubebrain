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
	"slices"
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
		require.Equal(t, codes.NotFound, status.Code(err))
		require.Equal(t, "etcdserver: requested lease not found", status.Convert(err).Message())
	}
	for _, id := range []int64{0, 13_400} {
		_, err := lease.LeaseRevoke(ctx, &etcdserverpb.LeaseRevokeRequest{ID: id})
		require.Equal(t, codes.NotFound, status.Code(err))
		require.Equal(t, "etcdserver: requested lease not found", status.Convert(err).Message())
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

func requireClientLeaseHeaderWellFormed(t *testing.T, header *etcdserverpb.ResponseHeader) {
	t.Helper()
	require.NotNil(t, header)
	require.NotZero(t, header.ClusterId)
	require.NotZero(t, header.MemberId)
	require.Positive(t, header.Revision)
	require.Positive(t, header.RaftTerm)
}
