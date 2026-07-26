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
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/mvccpb"
	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
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
			require.Equal(t, codes.OutOfRange, status.Code(err))
			require.Equal(t, "etcdserver: too large lease TTL", status.Convert(err).Message())
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
	require.Equal(t, codes.FailedPrecondition, status.Code(err))
	require.Equal(t, "etcdserver: lease already exists", status.Convert(err).Message())
}

func requireClientLeaseHeaderWellFormed(t *testing.T, header *etcdserverpb.ResponseHeader) {
	t.Helper()
	require.NotNil(t, header)
	require.NotZero(t, header.ClusterId)
	require.NotZero(t, header.MemberId)
	require.Positive(t, header.Revision)
	require.Positive(t, header.RaftTerm)
}

func byteKeysToStrings(keys [][]byte) []string {
	out := make([]string, 0, len(keys))
	for _, key := range keys {
		out = append(out, string(key))
	}
	return out
}
