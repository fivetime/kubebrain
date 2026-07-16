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
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/kubewharf/kubebrain/pkg/backend"
	"github.com/kubewharf/kubebrain/pkg/metrics/mock"
	"github.com/kubewharf/kubebrain/pkg/storage/memkv"
)

var errFakeDelete = errors.New("injected delete failure")

// TestLeaseAttachPersistsPerKeyNotWholeList pins #17: attaching a key to a lease
// writes one small per-key attachment record (leasekeys/<key> -> <id>) instead of
// rewriting the whole lease key-list. The meta record is therefore untouched by
// attach (no O(N) rewrite), and every binding is still durably recovered on
// failover from the attachment records.
func TestLeaseAttachPersistsPerKeyNotWholeList(t *testing.T) {
	ctrl := gomock.NewController(t)
	metrics := mock.NewMinimalMetrics(ctrl)
	kv := memkv.NewKvStorage()
	b := backend.NewBackend(kv, backend.Config{
		Identity:                "attach-test-peer",
		EnableEtcdCompatibility: true,
	}, metrics)
	server := New(b, metrics, testPeerService{isLeader: true})
	defer func() {
		server.stopLeases()
		require.NoError(t, kv.Close())
		ctrl.Finish()
	}()
	ctx := context.Background()

	const leaseID int64 = 90017
	_, err := server.LeaseGrant(ctx, &etcdserverpb.LeaseGrantRequest{TTL: 300, ID: leaseID})
	require.NoError(t, err)

	getMeta := func() []byte {
		var value []byte
		require.Eventually(t, func() bool {
			v, err := server.backend.InternalGet(ctx, leaseStorageKey(leaseID))
			if err != nil {
				return false
			}
			value = v
			return true
		}, time.Second, 5*time.Millisecond)
		return value
	}
	metaAfterGrant := getMeta()

	const n = 25
	keys := make([]string, n)
	for i := 0; i < n; i++ {
		k := fmt.Sprintf("/registry/events/default/e-%04d", i)
		keys[i] = k
		_, err := server.Put(ctx, &etcdserverpb.PutRequest{Key: []byte(k), Value: []byte("v"), Lease: leaseID})
		require.NoError(t, err)
	}

	// #17 core: the meta record is NOT rewritten on attach — its revision is
	// unchanged since grant. The old code rewrote the full (growing) key list on
	// every Put, so the meta revision would have advanced n times.
	require.Equal(t, metaAfterGrant, getMeta(),
		"attach must not rewrite the lease meta record (no O(N) key-list rewrite)")

	// Each attach wrote exactly one small per-key attachment record.
	for _, k := range keys {
		var value []byte
		require.Eventually(t, func() bool {
			v, err := server.backend.InternalGet(ctx, leaseAttachKey(k))
			if err != nil {
				return false
			}
			value = v
			return true
		}, time.Second, 5*time.Millisecond)
		require.Equal(t, strconv.FormatInt(leaseID, 10), string(value),
			"attachment record must point at the lease id")
	}

	// Failover: a fresh server recovers every binding from the attachment records.
	server.stopLeases()
	b.SetCurrentRevision(0)
	restored := New(b, metrics, testPeerService{isLeader: true})
	defer restored.stopLeases()

	ttlResp, err := restored.LeaseTimeToLive(ctx, &etcdserverpb.LeaseTimeToLiveRequest{ID: leaseID, Keys: true})
	require.NoError(t, err)
	require.Equal(t, int64(300), ttlResp.GrantedTTL)
	require.Len(t, ttlResp.Keys, n, "all attached keys must be recovered from per-key attachment records")

	// Detach one key (overwrite without a lease): its attachment record is removed
	// and a subsequent restore recovers exactly n-1 bindings.
	_, err = restored.Put(ctx, &etcdserverpb.PutRequest{Key: []byte(keys[0]), Value: []byte("v2")})
	require.NoError(t, err)
	require.Eventually(t, func() bool {
		_, err := restored.backend.InternalGet(ctx, leaseAttachKey(keys[0]))
		return err != nil
	}, time.Second, 5*time.Millisecond)

	restored.stopLeases()
	b.SetCurrentRevision(0)
	restored2 := New(b, metrics, testPeerService{isLeader: true})
	defer restored2.stopLeases()
	ttlResp2, err := restored2.LeaseTimeToLive(ctx, &etcdserverpb.LeaseTimeToLiveRequest{ID: leaseID, Keys: true})
	require.NoError(t, err)
	require.Len(t, ttlResp2.Keys, n-1, "detached key must not be recovered")
}

// TestLegacyLeaseRecordMigratesToAttachments pins the one-time migration path:
// a pre-#17 monolithic record (meta with an inline key list) is converted on
// leadership acquisition into a keyless meta plus one attachment record per key.
func TestLegacyLeaseRecordMigratesToAttachments(t *testing.T) {
	ctrl := gomock.NewController(t)
	metrics := mock.NewMinimalMetrics(ctrl)
	kv := memkv.NewKvStorage()
	b := backend.NewBackend(kv, backend.Config{
		Identity:                "migrate-test-peer",
		EnableEtcdCompatibility: true,
	}, metrics)
	server := New(b, metrics, testPeerService{isLeader: true})
	defer func() {
		server.stopLeases()
		require.NoError(t, kv.Close())
		ctrl.Finish()
	}()
	ctx := context.Background()

	const leaseID int64 = 55123
	legacyKeys := []string{"/registry/events/a", "/registry/events/b", "/registry/events/c"}
	// Seed a legacy monolithic record directly (no attachment records exist).
	data, err := jsonMarshalLeaseRecord(leaseID, 200, legacyKeys)
	require.NoError(t, err)
	_, err = server.backend.Put(ctx, &etcdserverpb.PutRequest{Key: leaseStorageKey(leaseID), Value: data})
	require.NoError(t, err)

	// Leadership acquisition: reload + migrate.
	require.NoError(t, server.ReloadLeases(ctx))

	// Bindings recovered.
	ttlResp, err := server.LeaseTimeToLive(ctx, &etcdserverpb.LeaseTimeToLiveRequest{ID: leaseID, Keys: true})
	require.NoError(t, err)
	require.Len(t, ttlResp.Keys, len(legacyKeys))

	// Each legacy key now has its own attachment record.
	for _, k := range legacyKeys {
		var value []byte
		require.Eventually(t, func() bool {
			v, err := server.backend.InternalGet(ctx, leaseAttachKey(k))
			if err != nil {
				return false
			}
			value = v
			return true
		}, time.Second, 5*time.Millisecond)
		require.Equal(t, strconv.FormatInt(leaseID, 10), string(value))
	}

	// The meta record was rewritten without the inline key list.
	var meta []byte
	require.Eventually(t, func() bool {
		value, err := server.backend.InternalGet(ctx, leaseStorageKey(leaseID))
		if err != nil {
			return false
		}
		meta = value
		return true
	}, time.Second, 5*time.Millisecond)
	rec, err := jsonUnmarshalLeaseRecord(meta)
	require.NoError(t, err)
	require.Empty(t, rec.Keys, "migrated meta record must no longer carry the inline key list")
	require.Equal(t, int64(200), rec.TTL)
}

func jsonMarshalLeaseRecord(id, ttl int64, keys []string) ([]byte, error) {
	return json.Marshal(leaseRecord{ID: id, TTL: ttl, Keys: keys})
}

func jsonUnmarshalLeaseRecord(data []byte) (leaseRecord, error) {
	var r leaseRecord
	err := json.Unmarshal(data, &r)
	return r, err
}

// failDeleteShim wraps a BackendShim and fails Delete for one specific key.
type failDeleteShim struct {
	BackendShim
	failKey string
	fail    bool
}

type demoteBeforeTxnApplyShim struct {
	BackendShim
	demote func()
}

func (s *demoteBeforeTxnApplyShim) TxnApply(ctx context.Context, ops []backend.TxnWriteOp, guards []backend.TxnGuard, prevKV []bool) ([]*etcdserverpb.ResponseOp, uint64, []backend.TxnWriteResult, error) {
	s.demote()
	return s.BackendShim.TxnApply(ctx, ops, guards, prevKV)
}

func TestLeaseRevokeIsFencedAcrossLeadershipChange(t *testing.T) {
	ctrl := gomock.NewController(t)
	metrics := mock.NewMinimalMetrics(ctrl)
	kv := memkv.NewKvStorage()
	b := backend.NewBackend(kv, backend.Config{
		Identity:                "lease-fence-peer",
		EnableEtcdCompatibility: true,
	}, metrics)
	var epoch atomic.Uint64
	var leading atomic.Bool
	leading.Store(true)
	b.SetLeadershipFence(func() (uint64, bool) { return epoch.Load(), leading.Load() })
	server := New(b, metrics, testPeerService{isLeader: true})
	defer func() {
		server.stopLeases()
		require.NoError(t, kv.Close())
		ctrl.Finish()
	}()
	ctx := context.Background()

	const leaseID int64 = 9039
	key := []byte("/registry/lease-fence/key")
	_, err := server.LeaseGrant(ctx, &etcdserverpb.LeaseGrantRequest{TTL: 300, ID: leaseID})
	require.NoError(t, err)
	_, err = server.Put(ctx, &etcdserverpb.PutRequest{Key: key, Value: []byte("value"), Lease: leaseID})
	require.NoError(t, err)

	server.backend = &demoteBeforeTxnApplyShim{
		BackendShim: server.backend,
		demote: func() {
			epoch.Store(1)
			leading.Store(false)
		},
	}
	_, err = server.LeaseRevoke(ctx, &etcdserverpb.LeaseRevokeRequest{ID: leaseID})
	require.Equal(t, codes.Unavailable, status.Code(err))

	stored, err := server.backend.Get(ctx, &etcdserverpb.RangeRequest{Key: key})
	require.NoError(t, err)
	require.Len(t, stored.Kvs, 1, "a deposed leader must not delete the leased key")
	_, err = server.backend.InternalGet(ctx, leaseStorageKey(leaseID))
	require.NoError(t, err, "a fenced revoke must retain durable lease state for the successor")
}

func (f *failDeleteShim) Delete(ctx context.Context, key []byte, revision int64, includeFailureRange bool) (*etcdserverpb.TxnResponse, error) {
	if f.fail && string(key) == f.failKey {
		return nil, errFakeDelete
	}
	return f.BackendShim.Delete(ctx, key, revision, includeFailureRange)
}

func (f *failDeleteShim) TxnApply(ctx context.Context, ops []backend.TxnWriteOp, guards []backend.TxnGuard, prevKV []bool) ([]*etcdserverpb.ResponseOp, uint64, []backend.TxnWriteResult, error) {
	if f.fail {
		for _, op := range ops {
			if op.Delete && !op.Internal && string(op.Key) == f.failKey {
				return nil, 0, nil, errFakeDelete
			}
		}
	}
	return f.BackendShim.TxnApply(ctx, ops, guards, prevKV)
}

// TestExpiryKeepsLeaseAndRecordWhenKeyDeleteFails pins #36: expiry deletes the
// attached keys before the lease record, and a failed key delete must keep the
// lease (and its record) so the surviving keys are never orphaned.
func TestExpiryKeepsLeaseAndRecordWhenKeyDeleteFails(t *testing.T) {
	ctrl := gomock.NewController(t)
	metrics := mock.NewMinimalMetrics(ctrl)
	kv := memkv.NewKvStorage()
	b := backend.NewBackend(kv, backend.Config{
		Identity:                "expire-order-peer",
		EnableEtcdCompatibility: true,
	}, metrics)
	server := New(b, metrics, testPeerService{isLeader: true})
	defer func() {
		server.stopLeases()
		require.NoError(t, kv.Close())
		ctrl.Finish()
	}()
	ctx := context.Background()

	const leaseID int64 = 77036
	_, err := server.LeaseGrant(ctx, &etcdserverpb.LeaseGrantRequest{TTL: 300, ID: leaseID})
	require.NoError(t, err)
	survivor := "/registry/events/keep-me"
	victim := "/registry/events/delete-me"
	for _, k := range []string{survivor, victim} {
		_, err := server.Put(ctx, &etcdserverpb.PutRequest{Key: []byte(k), Value: []byte("v"), Lease: leaseID})
		require.NoError(t, err)
	}

	// Make deletes of `survivor` fail, then run expiry.
	shim := &failDeleteShim{BackendShim: server.backend, failKey: survivor, fail: true}
	server.backend = shim
	server.expireLease(leaseID)

	// The lease must NOT have been removed: its record still exists and it is still
	// tracked, so recovery keeps expiring the keys instead of orphaning them.
	_, err = shim.InternalGet(ctx, leaseStorageKey(leaseID))
	require.NoError(t, err)
	ttlResp, err := server.LeaseTimeToLive(ctx, &etcdserverpb.LeaseTimeToLiveRequest{ID: leaseID, Keys: true})
	require.NoError(t, err)
	require.Greater(t, ttlResp.TTL, int64(0), "lease must still be alive after a failed expiry")
	require.Len(t, ttlResp.Keys, 2, "no key may be unbound when expiry did not complete")

	// Now let deletes succeed; a re-run finishes expiry cleanly.
	shim.fail = false
	server.expireLease(leaseID)
	after, err := server.LeaseTimeToLive(ctx, &etcdserverpb.LeaseTimeToLiveRequest{ID: leaseID})
	require.NoError(t, err)
	require.Equal(t, int64(-1), after.TTL, "lease must be gone once all bound keys are deleted")
	for _, k := range []string{survivor, victim} {
		r, err := server.Range(ctx, &etcdserverpb.RangeRequest{Key: []byte(k)})
		require.NoError(t, err)
		require.Len(t, r.Kvs, 0, "bound key must be deleted after successful expiry")
	}
}

// TestDeleteLeasedKeyCompareDeleteGuardsReassignment pins review finding #1: a
// lease revoke/expire must delete keys STILL bound to that lease, never a value a
// concurrent Put reassigned to another lease (which would delete the new value).
func TestDeleteLeasedKeyCompareDeleteGuardsReassignment(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()
	ctx := context.Background()

	const leaseA int64 = 111
	const leaseB int64 = 222
	_, err := server.LeaseGrant(ctx, &etcdserverpb.LeaseGrantRequest{TTL: 300, ID: leaseA})
	require.NoError(t, err)
	_, err = server.LeaseGrant(ctx, &etcdserverpb.LeaseGrantRequest{TTL: 300, ID: leaseB})
	require.NoError(t, err)

	key := []byte("/registry/leased/reassigned")
	_, err = server.Put(ctx, &etcdserverpb.PutRequest{Key: key, Value: []byte("v1"), Lease: leaseA})
	require.NoError(t, err)
	// reassign the key to lease B with a new value (as a Put mid-revoke would)
	put2, err := server.Put(ctx, &etcdserverpb.PutRequest{Key: key, Value: []byte("v2"), Lease: leaseB})
	require.NoError(t, err)
	require.Eventually(t, func() bool { return server.backend.GetCurrentRevision() >= uint64(put2.Header.Revision) }, 5*time.Second, 2*time.Millisecond)

	// Simulate lease A's revoke/expire processing this key, which is now bound to B.
	require.NoError(t, server.deleteLeasedKey(ctx, leaseA, string(key)))

	// The key must survive with B's value: A's revoke must not clobber B's binding.
	resp, err := server.Range(ctx, &etcdserverpb.RangeRequest{Key: key})
	require.NoError(t, err)
	require.Len(t, resp.Kvs, 1, "reassigned key must not be deleted by the other lease's revoke")
	require.Equal(t, "v2", string(resp.Kvs[0].Value))
	require.Equal(t, leaseB, resp.Kvs[0].Lease)

	// Positive control: deleteLeasedKey DOES delete a key still bound to the lease.
	require.NoError(t, server.deleteLeasedKey(ctx, leaseB, string(key)))
	after, err := server.Range(ctx, &etcdserverpb.RangeRequest{Key: key})
	require.NoError(t, err)
	require.Len(t, after.Kvs, 0, "a key still bound to the lease must be deleted")
}
