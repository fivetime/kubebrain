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
	"sync"
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
	"github.com/kubewharf/kubebrain/pkg/storage"
	"github.com/kubewharf/kubebrain/pkg/storage/memkv"
)

type blockingLegacyMigrationBackend struct {
	BackendShim
	entered chan struct{}
	release chan struct{}
	once    sync.Once
}

type staleUncertainAttachmentReadBackend struct {
	BackendShim
	target  []byte
	entered chan struct{}
	release chan struct{}
	once    sync.Once
}

func (b *staleUncertainAttachmentReadBackend) InternalGet(ctx context.Context, key []byte) ([]byte, error) {
	value, err := b.BackendShim.InternalGet(ctx, key)
	if string(key) == string(b.target) {
		b.once.Do(func() {
			close(b.entered)
			<-b.release
		})
	}
	return value, err
}

func (b *blockingLegacyMigrationBackend) InternalPut(ctx context.Context, key, value []byte) error {
	if len(key) >= len(leaseAttachPrefix) && string(key[:len(leaseAttachPrefix)]) == string(leaseAttachPrefix) {
		b.once.Do(func() { close(b.entered) })
		<-b.release
	}
	return b.BackendShim.InternalPut(ctx, key, value)
}

var errFakeDelete = errors.New("injected delete failure")

type commitThenUncertainLeaseShim struct {
	BackendShim
	trigger               atomic.Bool
	remainingReadFailures atomic.Int32
}

func (s *commitThenUncertainLeaseShim) TxnApply(ctx context.Context, ops []backend.TxnWriteOp, guards []backend.TxnGuard, prevKV []bool) ([]*etcdserverpb.ResponseOp, uint64, []backend.TxnWriteResult, error) {
	responses, revision, results, err := s.BackendShim.TxnApply(ctx, ops, guards, prevKV)
	if err == nil && s.trigger.CompareAndSwap(true, false) {
		s.remainingReadFailures.Store(3)
		return nil, revision, nil, storage.NewErrUncertainResult(context.DeadlineExceeded)
	}
	return responses, revision, results, err
}

func (s *commitThenUncertainLeaseShim) InternalGet(ctx context.Context, key []byte) ([]byte, error) {
	for {
		remaining := s.remainingReadFailures.Load()
		if remaining == 0 {
			return s.BackendShim.InternalGet(ctx, key)
		}
		if s.remainingReadFailures.CompareAndSwap(remaining, remaining-1) {
			return nil, storage.ErrUnavailable
		}
	}
}

func TestCommittedUncertainLeasedPutReconcilesIndexBeforeRevoke(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()
	ctx := context.Background()
	const leaseID int64 = 90018
	key := []byte("/registry/events/ns/uncertain-lease")

	_, err := server.LeaseGrant(ctx, &etcdserverpb.LeaseGrantRequest{TTL: 300, ID: leaseID})
	require.NoError(t, err)
	shim := &commitThenUncertainLeaseShim{BackendShim: server.backend}
	server.backend = shim
	server.leaseManager.srv.backend = shim

	shim.trigger.Store(true)
	_, err = server.Put(ctx, &etcdserverpb.PutRequest{Key: key, Value: []byte("value"), Lease: leaseID})
	require.ErrorIs(t, err, storage.ErrUncertainResult)
	require.Eventually(t, func() bool {
		return server.leaseIDForKey(string(key)) == leaseID
	}, 2*time.Second, 10*time.Millisecond, "committed attachment was not reconciled into the leader index")
	require.Zero(t, shim.remainingReadFailures.Load())

	_, err = server.LeaseRevoke(ctx, &etcdserverpb.LeaseRevokeRequest{ID: leaseID})
	require.NoError(t, err)
	got, err := server.Range(ctx, &etcdserverpb.RangeRequest{Key: key})
	require.NoError(t, err)
	require.Empty(t, got.Kvs, "revoke must delete the committed-uncertain leased key")
	_, err = shim.InternalGet(ctx, leaseAttachKey(string(key)))
	require.Error(t, err, "revoke must remove the durable attachment")
}

func TestLeasedPutReturnsPrevKV(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()
	ctx := context.Background()
	const leaseID int64 = 90016
	_, err := server.LeaseGrant(ctx, &etcdserverpb.LeaseGrantRequest{TTL: 300, ID: leaseID})
	require.NoError(t, err)
	key := []byte("/leased-prev-kv")
	_, err = server.Put(ctx, &etcdserverpb.PutRequest{Key: key, Value: []byte("before"), Lease: leaseID})
	require.NoError(t, err)

	response, err := server.Put(ctx, &etcdserverpb.PutRequest{Key: key, Value: []byte("after"), PrevKv: true})
	require.NoError(t, err)
	require.NotNil(t, response.PrevKv)
	require.Equal(t, key, response.PrevKv.Key)
	require.Equal(t, []byte("before"), response.PrevKv.Value)
	require.Equal(t, leaseID, response.PrevKv.Lease)
	require.Less(t, response.PrevKv.ModRevision, response.Header.Revision)
}

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
	return decodeLeaseRecord(data)
}

// failDeleteShim wraps a BackendShim and fails Delete for one specific key.
type failDeleteShim struct {
	BackendShim
	failKey string
	fail    bool
}

type recordingDeleteRangeListShim struct {
	BackendShim
	limit int64
}

func (s *recordingDeleteRangeListShim) List(ctx context.Context, r *etcdserverpb.RangeRequest) (*etcdserverpb.RangeResponse, error) {
	s.limit = r.Limit
	return s.BackendShim.List(ctx, r)
}

type demoteBeforeTxnApplyShim struct {
	BackendShim
	demote func()
}

type blockLeasedPutShim struct {
	BackendShim
	entered chan struct{}
	release chan struct{}
}

type failLeaseMutationShim struct {
	BackendShim
	key  string
	fail bool
}

func (s *failLeaseMutationShim) TxnApply(ctx context.Context, ops []backend.TxnWriteOp, guards []backend.TxnGuard, prevKV []bool) ([]*etcdserverpb.ResponseOp, uint64, []backend.TxnWriteResult, error) {
	if s.fail {
		for _, op := range ops {
			if !op.Internal && string(op.Key) == s.key {
				return nil, 0, nil, errFakeDelete
			}
		}
	}
	return s.BackendShim.TxnApply(ctx, ops, guards, prevKV)
}

func TestTxnFastShapesMutateLeaseAttachmentsAtomically(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()
	ctx := context.Background()
	const leaseID int64 = 9041
	key := []byte("/registry/lease-fence/fast-shape")
	_, err := server.LeaseGrant(ctx, &etcdserverpb.LeaseGrantRequest{TTL: 300, ID: leaseID})
	require.NoError(t, err)
	shim := &failLeaseMutationShim{BackendShim: server.backend, key: string(key), fail: true}
	server.backend = shim

	create := &etcdserverpb.TxnRequest{
		Compare: []*etcdserverpb.Compare{{
			Key: key, Target: etcdserverpb.Compare_MOD, Result: etcdserverpb.Compare_EQUAL,
			TargetUnion: &etcdserverpb.Compare_ModRevision{ModRevision: 0},
		}},
		Success: []*etcdserverpb.RequestOp{{Request: &etcdserverpb.RequestOp_RequestPut{
			RequestPut: &etcdserverpb.PutRequest{Key: key, Value: []byte("value"), Lease: leaseID},
		}}},
	}
	_, err = server.Txn(ctx, create)
	require.ErrorIs(t, err, errFakeDelete)
	absent, err := shim.Get(ctx, &etcdserverpb.RangeRequest{Key: key})
	require.NoError(t, err)
	require.Empty(t, absent.Kvs)
	_, err = shim.InternalGet(ctx, leaseAttachKey(string(key)))
	require.Error(t, err, "failed create must not leave an attachment")

	shim.fail = false
	created, err := server.Txn(ctx, create)
	require.NoError(t, err)
	require.True(t, created.Succeeded)
	stored, err := shim.Get(ctx, &etcdserverpb.RangeRequest{Key: key})
	require.NoError(t, err)
	require.Len(t, stored.Kvs, 1)
	_, err = shim.InternalGet(ctx, leaseAttachKey(string(key)))
	require.NoError(t, err)

	remove := &etcdserverpb.TxnRequest{
		Compare: []*etcdserverpb.Compare{{
			Key: key, Target: etcdserverpb.Compare_MOD, Result: etcdserverpb.Compare_EQUAL,
			TargetUnion: &etcdserverpb.Compare_ModRevision{ModRevision: stored.Kvs[0].ModRevision},
		}},
		Success: []*etcdserverpb.RequestOp{{Request: &etcdserverpb.RequestOp_RequestDeleteRange{
			RequestDeleteRange: &etcdserverpb.DeleteRangeRequest{Key: key, PrevKv: true},
		}}},
	}
	shim.fail = true
	_, err = server.Txn(ctx, remove)
	require.ErrorIs(t, err, errFakeDelete)
	stillPresent, err := shim.Get(ctx, &etcdserverpb.RangeRequest{Key: key})
	require.NoError(t, err)
	require.Len(t, stillPresent.Kvs, 1)
	_, err = shim.InternalGet(ctx, leaseAttachKey(string(key)))
	require.NoError(t, err, "failed delete must retain its attachment")

	shim.fail = false
	removed, err := server.Txn(ctx, remove)
	require.NoError(t, err)
	require.True(t, removed.Succeeded)
	deleteResponse := removed.Responses[0].GetResponseDeleteRange()
	require.Equal(t, int64(1), deleteResponse.Deleted)
	require.Equal(t, []byte("value"), deleteResponse.PrevKvs[0].Value)
	require.Equal(t, leaseID, deleteResponse.PrevKvs[0].Lease)
	_, err = shim.InternalGet(ctx, leaseAttachKey(string(key)))
	require.Error(t, err, "successful delete must remove its attachment")
}

func (s *blockLeasedPutShim) TxnApply(ctx context.Context, ops []backend.TxnWriteOp, guards []backend.TxnGuard, prevKV []bool) ([]*etcdserverpb.ResponseOp, uint64, []backend.TxnWriteResult, error) {
	for _, op := range ops {
		if !op.Internal && !op.Delete && op.Lease != 0 {
			close(s.entered)
			select {
			case <-s.release:
			case <-ctx.Done():
				return nil, 0, nil, ctx.Err()
			}
			break
		}
	}
	return s.BackendShim.TxnApply(ctx, ops, guards, prevKV)
}

func TestLeaseRevokeWaitsForAdmittedLeasedPut(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()
	ctx := context.Background()

	const leaseID int64 = 9040
	key := []byte("/registry/lease-fence/concurrent-put")
	_, err := server.LeaseGrant(ctx, &etcdserverpb.LeaseGrantRequest{TTL: 300, ID: leaseID})
	require.NoError(t, err)

	shim := &blockLeasedPutShim{
		BackendShim: server.backend,
		entered:     make(chan struct{}),
		release:     make(chan struct{}),
	}
	server.backend = shim
	putDone := make(chan error, 1)
	go func() {
		_, err := server.Put(ctx, &etcdserverpb.PutRequest{Key: key, Value: []byte("value"), Lease: leaseID})
		putDone <- err
	}()
	<-shim.entered

	revokeDone := make(chan error, 1)
	go func() {
		_, err := server.LeaseRevoke(ctx, &etcdserverpb.LeaseRevokeRequest{ID: leaseID})
		revokeDone <- err
	}()
	select {
	case err := <-revokeDone:
		require.Failf(t, "revoke overtook leased put", "revoke returned before admitted put committed: %v", err)
	case <-time.After(50 * time.Millisecond):
	}

	close(shim.release)
	require.NoError(t, <-putDone)
	require.NoError(t, <-revokeDone)
	resp, err := server.Range(ctx, &etcdserverpb.RangeRequest{Key: key})
	require.NoError(t, err)
	require.Empty(t, resp.Kvs, "revoke must delete a leased put admitted before it")
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

func TestDeleteRangeAtomicallyRemovesLeaseAttachments(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()
	ctx := context.Background()
	const leaseID int64 = 77035
	leasedKey := "/registry/events/atomic-delete/a"
	leaselessKey := "/registry/events/atomic-delete/b"
	_, err := server.LeaseGrant(ctx, &etcdserverpb.LeaseGrantRequest{TTL: 300, ID: leaseID})
	require.NoError(t, err)
	_, err = server.Put(ctx, &etcdserverpb.PutRequest{Key: []byte(leasedKey), Value: []byte("leased"), Lease: leaseID})
	require.NoError(t, err)
	_, err = server.Put(ctx, &etcdserverpb.PutRequest{Key: []byte(leaselessKey), Value: []byte("plain")})
	require.NoError(t, err)

	shim := &failDeleteShim{BackendShim: server.backend, failKey: leasedKey, fail: true}
	server.backend = shim
	request := &etcdserverpb.DeleteRangeRequest{
		Key: []byte("/registry/events/atomic-delete/"), RangeEnd: []byte("/registry/events/atomic-delete0"), PrevKv: true,
	}
	_, err = server.DeleteRange(ctx, request)
	require.ErrorIs(t, err, errFakeDelete)
	stillPresent, err := shim.Get(ctx, &etcdserverpb.RangeRequest{Key: []byte(leasedKey)})
	require.NoError(t, err)
	require.Len(t, stillPresent.Kvs, 1, "failed atomic batch must retain the user key")
	_, err = shim.InternalGet(ctx, leaseAttachKey(leasedKey))
	require.NoError(t, err, "failed atomic batch must retain the attachment")

	shim.fail = false
	response, err := server.DeleteRange(ctx, request)
	require.NoError(t, err)
	require.Equal(t, int64(2), response.Deleted)
	require.Len(t, response.PrevKvs, 2)
	require.Equal(t, []byte("leased"), response.PrevKvs[0].Value)
	require.Equal(t, leaseID, response.PrevKvs[0].Lease)
	_, err = shim.InternalGet(ctx, leaseAttachKey(leasedKey))
	require.Error(t, err, "successful batch must remove the attachment")
	ttl, err := server.LeaseTimeToLive(ctx, &etcdserverpb.LeaseTimeToLiveRequest{ID: leaseID, Keys: true})
	require.NoError(t, err)
	require.Empty(t, ttl.Keys)
}

func TestLargeLeasedDeleteRangeUsesOneRevision(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()
	ctx := context.Background()
	const (
		leaseID  = int64(77037)
		keyCount = 129
	)
	prefix := "/registry/events/large-atomic-delete/"
	_, err := server.LeaseGrant(ctx, &etcdserverpb.LeaseGrantRequest{TTL: 300, ID: leaseID})
	require.NoError(t, err)
	for i := 0; i < keyCount; i++ {
		key := fmt.Sprintf("%s%03d", prefix, i)
		_, err = server.Put(ctx, &etcdserverpb.PutRequest{
			Key: []byte(key), Value: []byte("value"), Lease: leaseID,
		})
		require.NoError(t, err)
	}

	before := server.backend.GetCurrentRevision()
	deleted, err := server.DeleteRange(ctx, &etcdserverpb.DeleteRangeRequest{
		Key: []byte(prefix), RangeEnd: prefixEnd([]byte(prefix)), PrevKv: true,
	})
	require.NoError(t, err)
	require.Equal(t, int64(keyCount), deleted.Deleted)
	require.Len(t, deleted.PrevKvs, keyCount)
	require.Equal(t, int64(before+1), deleted.Header.Revision)
	require.Equal(t, before+1, server.backend.GetCurrentRevision(),
		"etcd DeleteRange advances MVCC exactly once regardless of key count")

	ttl, err := server.LeaseTimeToLive(ctx, &etcdserverpb.LeaseTimeToLiveRequest{ID: leaseID, Keys: true})
	require.NoError(t, err)
	require.Empty(t, ttl.Keys)
}

func TestLargeLeasedDeleteRangeFailureIsAtomic(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()
	ctx := context.Background()
	const (
		leaseID  = int64(77038)
		keyCount = 129
	)
	prefix := "/registry/events/large-atomic-delete-failure/"
	keys := make([]string, 0, keyCount)
	_, err := server.LeaseGrant(ctx, &etcdserverpb.LeaseGrantRequest{TTL: 300, ID: leaseID})
	require.NoError(t, err)
	for i := 0; i < keyCount; i++ {
		key := fmt.Sprintf("%s%03d", prefix, i)
		keys = append(keys, key)
		_, err = server.Put(ctx, &etcdserverpb.PutRequest{
			Key: []byte(key), Value: []byte("value"), Lease: leaseID,
		})
		require.NoError(t, err)
	}

	server.backend = &failDeleteShim{
		BackendShim: server.backend,
		failKey:     keys[len(keys)-1],
		fail:        true,
	}
	_, err = server.DeleteRange(ctx, &etcdserverpb.DeleteRangeRequest{
		Key: []byte(prefix), RangeEnd: prefixEnd([]byte(prefix)),
	})
	require.ErrorIs(t, err, errFakeDelete)

	remaining, err := server.Range(ctx, &etcdserverpb.RangeRequest{
		Key: []byte(prefix), RangeEnd: prefixEnd([]byte(prefix)),
	})
	require.NoError(t, err)
	require.Len(t, remaining.Kvs, keyCount,
		"a failed large DeleteRange must not expose a committed first chunk")
	ttl, err := server.LeaseTimeToLive(ctx, &etcdserverpb.LeaseTimeToLiveRequest{ID: leaseID, Keys: true})
	require.NoError(t, err)
	require.Len(t, ttl.Keys, keyCount)
	for _, key := range []string{keys[0], keys[len(keys)-1]} {
		_, err = server.backend.InternalGet(ctx, leaseAttachKey(key))
		require.NoError(t, err)
	}
}

func TestLargeLeaselessDeleteRangeFailureIsAtomic(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()
	ctx := context.Background()
	const keyCount = 129
	prefix := "/registry/events/large-plain-delete-failure/"
	keys := make([]string, 0, keyCount)
	for i := 0; i < keyCount; i++ {
		key := fmt.Sprintf("%s%03d", prefix, i)
		keys = append(keys, key)
		_, err := server.Put(ctx, &etcdserverpb.PutRequest{Key: []byte(key), Value: []byte("value")})
		require.NoError(t, err)
	}

	server.backend = &failDeleteShim{
		BackendShim: server.backend,
		failKey:     keys[len(keys)-1],
		fail:        true,
	}
	before := server.backend.GetCurrentRevision()
	_, err := server.DeleteRange(ctx, &etcdserverpb.DeleteRangeRequest{
		Key: []byte(prefix), RangeEnd: prefixEnd([]byte(prefix)),
	})
	require.ErrorIs(t, err, errFakeDelete)
	require.Equal(t, before, server.backend.GetCurrentRevision())

	remaining, err := server.Range(ctx, &etcdserverpb.RangeRequest{
		Key: []byte(prefix), RangeEnd: prefixEnd([]byte(prefix)),
	})
	require.NoError(t, err)
	require.Len(t, remaining.Kvs, keyCount)
}

func TestDeleteRangeKeyLimitRejectsBeforeMutation(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()
	ctx := context.Background()
	prefix := "/registry/events/delete-limit/"
	for i := 0; i < 4; i++ {
		_, err := server.Put(ctx, &etcdserverpb.PutRequest{
			Key: []byte(fmt.Sprintf("%s%d", prefix, i)), Value: []byte("value"),
		})
		require.NoError(t, err)
	}
	recorder := &recordingDeleteRangeListShim{BackendShim: server.backend}
	server.backend = recorder
	server.SetMaxDeleteRangeKeys(3)
	before := server.backend.GetCurrentRevision()

	_, err := server.DeleteRange(ctx, &etcdserverpb.DeleteRangeRequest{
		Key: []byte(prefix), RangeEnd: prefixEnd([]byte(prefix)),
	})
	require.Equal(t, codes.ResourceExhausted, status.Code(err))
	require.Equal(t, "etcdserver: too many requests", status.Convert(err).Message())
	require.Equal(t, int64(4), recorder.limit, "admission scan must stop at limit+1")
	require.Equal(t, before, server.backend.GetCurrentRevision())
	remaining, err := server.Range(ctx, &etcdserverpb.RangeRequest{
		Key: []byte(prefix), RangeEnd: prefixEnd([]byte(prefix)),
	})
	require.NoError(t, err)
	require.Len(t, remaining.Kvs, 4)
}

func TestTxnDeleteRangeCannotBypassKeyLimit(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()
	ctx := context.Background()
	prefix := "/registry/events/txn-delete-limit/"
	for i := 0; i < 3; i++ {
		_, err := server.Put(ctx, &etcdserverpb.PutRequest{
			Key: []byte(fmt.Sprintf("%s%d", prefix, i)), Value: []byte("value"),
		})
		require.NoError(t, err)
	}
	server.SetMaxDeleteRangeKeys(2)
	before := server.backend.GetCurrentRevision()

	_, err := server.Txn(ctx, &etcdserverpb.TxnRequest{
		Success: []*etcdserverpb.RequestOp{{
			Request: &etcdserverpb.RequestOp_RequestDeleteRange{
				RequestDeleteRange: &etcdserverpb.DeleteRangeRequest{
					Key: []byte(prefix), RangeEnd: prefixEnd([]byte(prefix)),
				},
			},
		}},
	})
	require.Equal(t, codes.ResourceExhausted, status.Code(err))
	require.Equal(t, before, server.backend.GetCurrentRevision())
	remaining, err := server.Range(ctx, &etcdserverpb.RangeRequest{
		Key: []byte(prefix), RangeEnd: prefixEnd([]byte(prefix)),
	})
	require.NoError(t, err)
	require.Len(t, remaining.Kvs, 3)
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
	server.leaseMu.Lock()
	server.leases[leaseID].timer.Stop()
	server.leases[leaseID].deadline = time.Now().Add(-time.Second)
	server.leaseMu.Unlock()
	server.expireLease(leaseID)

	// The lease must NOT have been removed: its record still exists and it is still
	// tracked, so recovery keeps expiring the keys instead of orphaning them.
	_, err = shim.InternalGet(ctx, leaseStorageKey(leaseID))
	require.NoError(t, err)
	ttlResp, err := server.LeaseTimeToLive(ctx, &etcdserverpb.LeaseTimeToLiveRequest{ID: leaseID, Keys: true})
	require.NoError(t, err)
	require.Negative(t, ttlResp.TTL, "failed revoke keeps the expired lease pending for retry")
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

func TestLegacyLeaseMigrationFencedAcrossLeadershipEpoch(t *testing.T) {
	ctrl := gomock.NewController(t)
	metrics := mock.NewMinimalMetrics(ctrl)
	kv := memkv.NewKvStorage()
	b := backend.NewBackend(kv, backend.Config{
		Identity:                "migration-fence-test-peer",
		EnableEtcdCompatibility: true,
	}, metrics)
	var epoch atomic.Uint64
	epoch.Store(1)
	peers := testPeerService{
		isLeaderFn: func() bool { return true },
		epochFn:    func() (uint64, bool) { return epoch.Load(), true },
	}
	server := New(b, metrics, peers)
	defer func() {
		server.stopLeases()
		require.NoError(t, kv.Close())
		ctrl.Finish()
	}()
	b.SetLeadershipFence(peers.EpochAndLeadingFresh)
	ctx := context.Background()

	const leaseID int64 = 55124
	const legacyKey = "/registry/events/migration-fenced"
	data, err := jsonMarshalLeaseRecord(leaseID, 200, []string{legacyKey})
	require.NoError(t, err)
	_, err = server.backend.Put(ctx, &etcdserverpb.PutRequest{Key: leaseStorageKey(leaseID), Value: data})
	require.NoError(t, err)

	blocking := &blockingLegacyMigrationBackend{
		BackendShim: server.backend,
		entered:     make(chan struct{}),
		release:     make(chan struct{}),
	}
	server.backend = blocking
	done := make(chan error, 1)
	go func() { done <- server.ReloadLeases(ctx) }()
	<-blocking.entered
	epoch.Store(2)
	close(blocking.release)
	require.NoError(t, <-done, "migration is best effort; the durable legacy record remains retryable")

	_, err = b.InternalGet(ctx, leaseAttachKey(legacyKey))
	require.Error(t, err, "the old term must not create an attachment")
	_, err = b.InternalGet(ctx, leaseStorageKey(leaseID))
	require.Error(t, err, "the old term must not create replacement lease metadata")
	legacy, err := server.backend.Get(ctx, &etcdserverpb.RangeRequest{Key: leaseStorageKey(leaseID)})
	require.NoError(t, err)
	require.Len(t, legacy.Kvs, 1, "the retryable legacy source must remain durable")
}

func TestReloadLeasesRejectsFollower(t *testing.T) {
	server, _, cleanup := newLeaseTestServer(t)
	defer cleanup()
	server.peers = testPeerService{isLeader: false}

	err := server.ReloadLeases(context.Background())
	require.Error(t, err)
	require.Equal(t, codes.Unavailable, status.Code(err))
}

func TestUncertainLeaseReconcileCannotOverwriteReloadedGeneration(t *testing.T) {
	server, b, cleanup := newLeaseTestServer(t)
	defer cleanup()
	ctx := context.Background()
	const oldLease int64 = 55130
	const newLease int64 = 55131
	const key = "/registry/events/uncertain-generation"

	var epoch atomic.Uint64
	epoch.Store(1)
	peers := testPeerService{
		isLeaderFn: func() bool { return true },
		epochFn:    func() (uint64, bool) { return epoch.Load(), true },
	}
	server.peers = peers
	b.SetLeadershipFence(peers.EpochAndLeadingFresh)
	_, err := server.LeaseGrant(backend.WithLeadershipEpoch(ctx, 1),
		&etcdserverpb.LeaseGrantRequest{TTL: 300, ID: oldLease})
	require.NoError(t, err)
	_, err = server.Put(backend.WithLeadershipEpoch(ctx, 1),
		&etcdserverpb.PutRequest{Key: []byte(key), Value: []byte("old"), Lease: oldLease})
	require.NoError(t, err)

	server.leaseMu.Lock()
	oldGeneration := server.leaseGeneration
	server.leaseMu.Unlock()
	staleRead := &staleUncertainAttachmentReadBackend{
		BackendShim: server.backend,
		target:      leaseAttachKey(key),
		entered:     make(chan struct{}),
		release:     make(chan struct{}),
	}
	server.backend = staleRead
	done := make(chan struct{})
	go func() {
		server.reconcileLeaseIndexesAtRevision(context.Background(), b.GetCurrentRevision(), []string{key}, 1, oldGeneration)
		close(done)
	}()
	<-staleRead.entered

	epoch.Store(2)
	server.StopLeases()
	newCtx := backend.WithLeadershipEpoch(ctx, 2)
	require.NoError(t, b.InternalDelete(newCtx, leaseStorageKey(oldLease)))
	newMeta, err := jsonMarshalLeaseRecord(newLease, 300, nil)
	require.NoError(t, err)
	require.NoError(t, b.InternalPut(newCtx, leaseStorageKey(newLease), newMeta))
	require.NoError(t, b.InternalPut(newCtx, leaseAttachKey(key), []byte(strconv.FormatInt(newLease, 10))))
	require.NoError(t, server.ReloadLeases(ctx))
	require.Equal(t, newLease, server.leaseIDForKey(key))

	close(staleRead.release)
	<-done
	require.Equal(t, newLease, server.leaseIDForKey(key),
		"an old-term uncertain reconciliation must not overwrite the reloaded lease snapshot")
}
