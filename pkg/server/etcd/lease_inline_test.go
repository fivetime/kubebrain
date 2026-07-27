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

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/api/v3/etcdserverpb"

	"github.com/kubewharf/kubebrain/pkg/backend"
	"github.com/kubewharf/kubebrain/pkg/metrics/mock"
	"github.com/kubewharf/kubebrain/pkg/storage"
	"github.com/kubewharf/kubebrain/pkg/storage/memkv"
)

func newLeaseTestServer(t *testing.T) (*RPCServer, backend.Backend, func()) {
	t.Helper()
	ctrl := gomock.NewController(t)
	metrics := mock.NewMinimalMetrics(ctrl)
	kv := memkv.NewKvStorage()
	b := backend.NewBackend(kv, backend.Config{
		Identity:                "lease-inline-test-peer",
		EnableEtcdCompatibility: true,
	}, metrics)
	server := New(b, metrics, testPeerService{isLeader: true})
	cleanup := func() {
		server.stopLeases()
		require.NoError(t, kv.Close())
		ctrl.Finish()
	}
	return server, b, cleanup
}

// TestLeaseInlinedPerVersion pins review #9: the lease is recorded per MVCC
// version (in the value envelope), so a historical read reports the lease the key
// held AT that revision — not merely the key's current binding. Before the fix the
// lease came solely from the live key->lease index, so rebinding a key to a new
// lease retroactively changed what every historical read reported.
func TestLeaseInlinedPerVersion(t *testing.T) {
	server, _, cleanup := newLeaseTestServer(t)
	defer cleanup()
	ctx := context.Background()

	const leaseA int64 = 111001
	const leaseB int64 = 222002
	_, err := server.LeaseGrant(ctx, &etcdserverpb.LeaseGrantRequest{TTL: 300, ID: leaseA})
	require.NoError(t, err)
	_, err = server.LeaseGrant(ctx, &etcdserverpb.LeaseGrantRequest{TTL: 300, ID: leaseB})
	require.NoError(t, err)

	key := []byte("/registry/events/ns/e1")

	// v1: bound to leaseA.
	putA, err := server.Put(ctx, &etcdserverpb.PutRequest{Key: key, Value: []byte("v1"), Lease: leaseA})
	require.NoError(t, err)
	revA := putA.Header.Revision

	// v2: rebound to leaseB.
	putB, err := server.Put(ctx, &etcdserverpb.PutRequest{Key: key, Value: []byte("v2"), Lease: leaseB})
	require.NoError(t, err)
	revB := putB.Header.Revision
	require.Greater(t, revB, revA)
	// Wait for the committed revision to catch up so a latest read observes v2
	// (the event collector publishes revisions asynchronously).
	require.Eventually(t, func() bool { return server.backend.GetCurrentRevision() >= uint64(revB) }, 5*time.Second, 2*time.Millisecond)

	// Latest read reports the current lease (leaseB).
	latest, err := server.Range(ctx, &etcdserverpb.RangeRequest{Key: key})
	require.NoError(t, err)
	require.Len(t, latest.Kvs, 1)
	require.Equal(t, leaseB, latest.Kvs[0].Lease)
	require.Equal(t, []byte("v2"), latest.Kvs[0].Value)

	// Historical read at revA reports leaseA — the lease that version actually
	// held, NOT the current binding leaseB. This is the crux of #9.
	hist, err := server.Range(ctx, &etcdserverpb.RangeRequest{Key: key, Revision: revA})
	require.NoError(t, err)
	require.Len(t, hist.Kvs, 1)
	require.Equal(t, []byte("v1"), hist.Kvs[0].Value)
	require.Equal(t, leaseA, hist.Kvs[0].Lease, "historical read must report the per-version lease, not the current binding")

	// A version put with no lease reports lease 0 even though nothing else changed.
	_, err = server.Put(ctx, &etcdserverpb.PutRequest{Key: key, Value: []byte("v3")})
	require.NoError(t, err)
	none, err := server.Range(ctx, &etcdserverpb.RangeRequest{Key: key})
	require.NoError(t, err)
	require.Len(t, none.Kvs, 1)
	require.Equal(t, []byte("v3"), none.Kvs[0].Value)
	require.Equal(t, int64(0), none.Kvs[0].Lease)
}

// TestLeasedPutWritesAttachmentAtomically pins review #2: a leased Put persists the
// value and its lease attachment record in one atomic batch, and clearing the lease
// removes the attachment in the same atomic batch — so a leased key is never
// durably present without its attachment (which would orphan it past lease expiry),
// and a de-leased key never keeps a stale attachment (which could later mis-delete a
// live value). Recovery on a fresh leader reconstructs the binding from the record.
func TestLeasedPutWritesAttachmentAtomically(t *testing.T) {
	server, b, cleanup := newLeaseTestServer(t)
	defer cleanup()
	ctx := context.Background()

	const leaseID int64 = 333003
	baseRevision := int64(server.backend.GetCurrentRevision())
	_, err := server.LeaseGrant(ctx, &etcdserverpb.LeaseGrantRequest{TTL: 300, ID: leaseID})
	require.NoError(t, err)
	require.Equal(t, baseRevision, int64(server.backend.GetCurrentRevision()))

	key := []byte("/registry/masterleases/1.2.3.4")

	// Leased Put: both the value and the attachment record must exist.
	putLeased, err := server.Put(ctx, &etcdserverpb.PutRequest{Key: key, Value: []byte("v"), Lease: leaseID})
	require.NoError(t, err)
	require.Equal(t, baseRevision+1, putLeased.Header.Revision)
	require.Eventually(t, func() bool { return server.backend.GetCurrentRevision() >= uint64(putLeased.Header.Revision) }, 5*time.Second, 2*time.Millisecond)

	valResp, err := server.backend.Get(ctx, &etcdserverpb.RangeRequest{Key: key})
	require.NoError(t, err)
	require.Len(t, valResp.Kvs, 1)

	attachValue, err := server.backend.InternalGet(ctx, leaseAttachKey(string(key)))
	require.NoError(t, err)
	require.Equal(t, []byte("333003"), attachValue, "leased Put must have committed the attachment record atomically with the value")

	// A fresh leader reloads the binding from the durable attachment record.
	reloaded := New(b, server.metricCli, testPeerService{isLeader: true})
	defer reloaded.stopLeases()
	require.NoError(t, reloaded.ReloadLeases(ctx))
	require.Equal(t, leaseID, reloaded.leaseIDForKey(string(key)))

	// Clearing the lease removes the attachment record in the same atomic batch.
	putClear, err := server.Put(ctx, &etcdserverpb.PutRequest{Key: key, Value: []byte("v2")})
	require.NoError(t, err)
	require.Equal(t, putLeased.Header.Revision+1, putClear.Header.Revision)
	require.Eventually(t, func() bool { return server.backend.GetCurrentRevision() >= uint64(putClear.Header.Revision) }, 5*time.Second, 2*time.Millisecond)
	_, err = server.backend.InternalGet(ctx, leaseAttachKey(string(key)))
	require.ErrorIs(t, err, storage.ErrKeyNotFound, "clearing the lease must remove the attachment record")

	latest, err := server.Range(ctx, &etcdserverpb.RangeRequest{Key: key})
	require.NoError(t, err)
	require.Len(t, latest.Kvs, 1)
	require.Equal(t, int64(0), latest.Kvs[0].Lease)
}
