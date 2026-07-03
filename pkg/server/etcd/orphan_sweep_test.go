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
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
)

// TestOrphanLeaseSweepReconciles pins the borrowed-from-kine safety net: a leased
// key whose lease meta record is gone (so its expiry timer was never re-armed on
// reload — applyLeaseRecords drops such an attachment) is eventually collected by
// the leader sweep, while a key bound to a still-live lease is left untouched.
func TestOrphanLeaseSweepReconciles(t *testing.T) {
	server, _, cleanup := newLeaseTestServer(t)
	defer cleanup()
	ctx := context.Background()

	const leaseOrphan int64 = 700001 // will be made defunct
	const leaseLive int64 = 700002   // stays alive
	_, err := server.LeaseGrant(ctx, &etcdserverpb.LeaseGrantRequest{TTL: 3000, ID: leaseOrphan})
	require.NoError(t, err)
	_, err = server.LeaseGrant(ctx, &etcdserverpb.LeaseGrantRequest{TTL: 3000, ID: leaseLive})
	require.NoError(t, err)

	orphanKey := []byte("/registry/events/ns/orphaned")
	liveKey := []byte("/registry/events/ns/live")
	putO, err := server.Put(ctx, &etcdserverpb.PutRequest{Key: orphanKey, Value: []byte("v"), Lease: leaseOrphan})
	require.NoError(t, err)
	putL, err := server.Put(ctx, &etcdserverpb.PutRequest{Key: liveKey, Value: []byte("v"), Lease: leaseLive})
	require.NoError(t, err)
	require.Eventually(t, func() bool {
		return server.backend.GetCurrentRevision() >= uint64(putO.Header.Revision) &&
			server.backend.GetCurrentRevision() >= uint64(putL.Header.Revision)
	}, 5*time.Second, 2*time.Millisecond)

	// Make leaseOrphan defunct exactly the way a reload would leave it: drop the
	// in-memory lease + index entry but leave the durable attachment record (which
	// applyLeaseRecords would have skipped as an orphan). The value keeps its
	// inline lease.
	server.leaseMu.Lock()
	if st := server.leases[leaseOrphan]; st != nil && st.timer != nil {
		st.timer.Stop()
	}
	delete(server.leases, leaseOrphan)
	delete(server.keyLeaseIndex, string(orphanKey))
	server.leaseMu.Unlock()

	// Sanity: the durable attachment record for the orphan still exists.
	rec, err := server.backend.Get(ctx, &etcdserverpb.RangeRequest{Key: leaseAttachKey(string(orphanKey))})
	require.NoError(t, err)
	require.Len(t, rec.Kvs, 1)

	server.sweepOrphanLeasedKeys(ctx)

	// The orphaned key and its stale attachment record are gone.
	gone, err := server.Range(ctx, &etcdserverpb.RangeRequest{Key: orphanKey})
	require.NoError(t, err)
	require.Len(t, gone.Kvs, 0, "orphaned leased key must be collected by the sweep")
	recGone, err := server.backend.Get(ctx, &etcdserverpb.RangeRequest{Key: leaseAttachKey(string(orphanKey))})
	require.NoError(t, err)
	require.Len(t, recGone.Kvs, 0, "stale attachment record must be reclaimed")

	// The key bound to a still-live lease is untouched.
	kept, err := server.Range(ctx, &etcdserverpb.RangeRequest{Key: liveKey})
	require.NoError(t, err)
	require.Len(t, kept.Kvs, 1, "key bound to a live lease must not be swept")
	require.Equal(t, leaseLive, kept.Kvs[0].Lease)
}

// TestOrphanLeaseSweepReclaimsStaleRecordButKeepsRebound pins the conservative
// half: a stale attachment record whose key was rebound/recreated to a different
// (or no) lease is reclaimed, but the live key itself is never deleted.
func TestOrphanLeaseSweepReclaimsStaleRecordButKeepsRebound(t *testing.T) {
	server, _, cleanup := newLeaseTestServer(t)
	defer cleanup()
	ctx := context.Background()

	const defunct int64 = 700010
	_, err := server.LeaseGrant(ctx, &etcdserverpb.LeaseGrantRequest{TTL: 3000, ID: defunct})
	require.NoError(t, err)

	key := []byte("/registry/events/ns/rebound")
	// Bind to the (soon-defunct) lease, then rebind to no lease. putLeasedAtomic
	// removes the attachment atomically on the second put, so re-create the stale
	// record by hand to model a failed detach.
	_, err = server.Put(ctx, &etcdserverpb.PutRequest{Key: key, Value: []byte("v1"), Lease: defunct})
	require.NoError(t, err)
	putNL, err := server.Put(ctx, &etcdserverpb.PutRequest{Key: key, Value: []byte("v2")}) // rebind to no lease
	require.NoError(t, err)
	require.Eventually(t, func() bool { return server.backend.GetCurrentRevision() >= uint64(putNL.Header.Revision) }, 5*time.Second, 2*time.Millisecond)

	// Re-plant a stale attachment record pointing at the now-defunct lease, and drop
	// the lease from memory.
	require.NoError(t, server.attachKeyToStorage(ctx, defunct, string(key)))
	server.leaseMu.Lock()
	if st := server.leases[defunct]; st != nil && st.timer != nil {
		st.timer.Stop()
	}
	delete(server.leases, defunct)
	server.leaseMu.Unlock()

	server.sweepOrphanLeasedKeys(ctx)

	// The live (now leaseless) key is kept; only the stale record is reclaimed.
	kept, err := server.Range(ctx, &etcdserverpb.RangeRequest{Key: key})
	require.NoError(t, err)
	require.Len(t, kept.Kvs, 1, "a rebound/leaseless key must not be deleted by the sweep")
	require.Equal(t, int64(0), kept.Kvs[0].Lease)
	rec, err := server.backend.Get(ctx, &etcdserverpb.RangeRequest{Key: leaseAttachKey(string(key))})
	require.NoError(t, err)
	require.Len(t, rec.Kvs, 0, "stale attachment record must be reclaimed")
}
