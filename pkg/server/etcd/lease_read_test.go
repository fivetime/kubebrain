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
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
)

// TestReadPopulatesAttachedLease pins the kv.Lease read-populate: Get/Range
// (and the delete prev_kv) must report the lease attached to each key, like
// etcd. Previously kvToEtcdKv left Lease=0 on every read.
func TestReadPopulatesAttachedLease(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()
	ctx := context.Background()

	const leaseID int64 = 24680
	_, err := server.LeaseGrant(ctx, &etcdserverpb.LeaseGrantRequest{TTL: 60, ID: leaseID})
	require.NoError(t, err)

	leased := []byte("/registry/pods/leased")
	plain := []byte("/registry/pods/plain")
	putLeased, err := server.Put(ctx, &etcdserverpb.PutRequest{Key: leased, Value: []byte("v"), Lease: leaseID})
	require.NoError(t, err)
	putPlain, err := server.Put(ctx, &etcdserverpb.PutRequest{Key: plain, Value: []byte("v")})
	require.NoError(t, err)

	// The memKv backend advances the readable revision asynchronously after a Put;
	// wait for it to catch up so the range read below cannot observe a revision
	// before the writes (otherwise it flakily misses the just-written key). This
	// mirrors the wait other tests use (e.g. watch_test.go).
	wantRev := putPlain.Header.Revision
	if putLeased.Header.Revision > wantRev {
		wantRev = putLeased.Header.Revision
	}
	require.Eventually(t, func() bool {
		return server.backend.GetCurrentRevision() >= uint64(wantRev)
	}, 5*time.Second, 2*time.Millisecond)

	// Get on the leased key carries the lease; the plain key carries 0.
	getLeased, err := server.Range(ctx, &etcdserverpb.RangeRequest{Key: leased})
	require.NoError(t, err)
	require.Len(t, getLeased.Kvs, 1)
	require.Equal(t, leaseID, getLeased.Kvs[0].Lease, "Get must report the attached lease")

	getPlain, err := server.Range(ctx, &etcdserverpb.RangeRequest{Key: plain})
	require.NoError(t, err)
	require.Len(t, getPlain.Kvs, 1)
	require.Zero(t, getPlain.Kvs[0].Lease, "a leaseless key must report lease 0")

	// Range over both keys reports each key's own lease.
	listResp, err := server.Range(ctx, &etcdserverpb.RangeRequest{
		Key:      []byte("/registry/pods/"),
		RangeEnd: []byte("/registry/pods0"),
	})
	require.NoError(t, err)
	got := map[string]int64{}
	for _, kv := range listResp.Kvs {
		got[string(kv.Key)] = kv.Lease
	}
	require.Equal(t, leaseID, got[string(leased)], "Range must report the attached lease")
	require.Zero(t, got[string(plain)], "Range must report 0 for the leaseless key")

	// After the lease is revoked the (now-deleted) key is gone; recreate the key
	// without a lease and confirm the read reports 0 (binding cleared).
	putUnbind, err := server.Put(ctx, &etcdserverpb.PutRequest{Key: leased, Value: []byte("v2")})
	require.NoError(t, err) // Put without lease unbinds
	require.Eventually(t, func() bool {
		return server.backend.GetCurrentRevision() >= uint64(putUnbind.Header.Revision)
	}, 5*time.Second, 2*time.Millisecond)
	after, err := server.Range(ctx, &etcdserverpb.RangeRequest{Key: leased})
	require.NoError(t, err)
	require.Len(t, after.Kvs, 1)
	require.Zero(t, after.Kvs[0].Lease, "after unbinding, the read must report lease 0")
}

func TestHistoricalUnleasedVersionDoesNotInheritCurrentLease(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()
	ctx := context.Background()

	const leaseID int64 = 24681
	_, err := server.LeaseGrant(ctx, &etcdserverpb.LeaseGrantRequest{TTL: 60, ID: leaseID})
	require.NoError(t, err)

	key := []byte("/registry/pods/historical-lease")
	unleased, err := server.Put(ctx, &etcdserverpb.PutRequest{Key: key, Value: []byte("unleased")})
	require.NoError(t, err)
	leased, err := server.Put(ctx, &etcdserverpb.PutRequest{Key: key, Value: []byte("leased"), Lease: leaseID})
	require.NoError(t, err)
	require.Eventually(t, func() bool {
		return server.backend.GetCurrentRevision() >= uint64(leased.Header.Revision)
	}, 5*time.Second, 2*time.Millisecond)

	historical, err := server.Range(ctx, &etcdserverpb.RangeRequest{
		Key: key, Revision: unleased.Header.Revision,
	})
	require.NoError(t, err)
	require.Len(t, historical.Kvs, 1)
	require.Equal(t, []byte("unleased"), historical.Kvs[0].Value)
	require.Zero(t, historical.Kvs[0].Lease, "v1 envelope records an authoritative unleased version")

	current, err := server.Range(ctx, &etcdserverpb.RangeRequest{Key: key})
	require.NoError(t, err)
	require.Len(t, current.Kvs, 1)
	require.Equal(t, leaseID, current.Kvs[0].Lease)
}

// TestLeaseIDForKeyFastPath pins the lock-free fast path: leasedKeyCount mirrors
// keyLeaseIndex so a leaseless server resolves lease lookups without taking
// leaseMu, and the count tracks bind/unbind so a bound key still resolves.
func TestLeaseIDForKeyFastPath(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()
	ctx := context.Background()

	require.Zero(t, atomic.LoadInt64(&server.leasedKeyCount), "no keys leased at start")
	require.Zero(t, server.leaseIDForKey("/registry/pods/x"), "leaseless lookup returns 0")

	const leaseID int64 = 13579
	_, err := server.LeaseGrant(ctx, &etcdserverpb.LeaseGrantRequest{TTL: 60, ID: leaseID})
	require.NoError(t, err)
	key := "/registry/pods/x"
	_, err = server.Put(ctx, &etcdserverpb.PutRequest{Key: []byte(key), Value: []byte("v"), Lease: leaseID})
	require.NoError(t, err)

	require.Equal(t, int64(1), atomic.LoadInt64(&server.leasedKeyCount), "one key leased after bind")
	require.Equal(t, leaseID, server.leaseIDForKey(key), "bound key resolves through the slow path")

	// Unbind by overwriting without a lease; count returns to 0 and fast path re-engages.
	_, err = server.Put(ctx, &etcdserverpb.PutRequest{Key: []byte(key), Value: []byte("v2")})
	require.NoError(t, err)
	require.Zero(t, atomic.LoadInt64(&server.leasedKeyCount), "count returns to 0 after unbind")
	require.Zero(t, server.leaseIDForKey(key), "unbound key resolves to 0")
}
