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
	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
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

func requireClientLeaseHeaderWellFormed(t *testing.T, header *etcdserverpb.ResponseHeader) {
	t.Helper()
	require.NotNil(t, header)
	require.NotZero(t, header.ClusterId)
	require.NotZero(t, header.MemberId)
	require.Positive(t, header.Revision)
	require.Positive(t, header.RaftTerm)
}
