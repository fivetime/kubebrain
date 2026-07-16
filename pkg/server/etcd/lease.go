// Copyright 2022 ByteDance and/or its affiliates
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
	"io"
	"strconv"
	"sync/atomic"
	"time"

	"go.etcd.io/etcd/api/v3/etcdserverpb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"k8s.io/klog/v2"

	"github.com/kubewharf/kubebrain/pkg/backend"
)

// leaseStoragePrefix holds one small meta record per lease: leases/<id> -> {id,ttl}.
// leaseAttachPrefix holds one record per attached key: leasekeys/<userKey> -> <id>.
// Splitting the key list out of the meta record makes attach/detach an O(1) write
// instead of rewriting the whole (potentially thousands-of-keys) list per Put (#17).
// Both live under the reserved \x00kubebrain/ namespace so #15 compaction reclaims
// their superseded versions and tombstones. leasekeys/ sorts before leases/ (k<s),
// so a leases/ prefix scan never picks up attachment records.
var leaseStoragePrefix = []byte("\x00kubebrain/leases/")
var leaseAttachPrefix = []byte("\x00kubebrain/leasekeys/")

const leaseExpiryRetryInterval = time.Second
const latestRestoreRevision = int64(^uint64(0) >> 1)
const maxLeaseTTL = int64(9000000000)

// leaseRecord is the per-lease meta record. Keys is written only by pre-#17
// (legacy monolithic) records and is still read on restore for a one-time
// migration; new meta records carry only ID and TTL.
type leaseRecord struct {
	ID               int64    `json:"id"`
	TTL              int64    `json:"ttl"`
	DeadlineUnixNano int64    `json:"deadlineUnixNano,omitempty"`
	Keys             []string `json:"keys,omitempty"`
	LegacyStorage    bool     `json:"-"`
}

func (m *leaseManager) LeaseGrant(ctx context.Context, req *etcdserverpb.LeaseGrantRequest) (*etcdserverpb.LeaseGrantResponse, error) {
	m.srv.metricCli.EmitCounter("lease.grant", 1)
	if err := m.requireLeaseLeader("lease grant"); err != nil {
		if m.srv.peers.EtcdProxyEnabled() {
			return m.srv.peers.LeaseGrant(ctx, req)
		}
		return nil, err
	}
	if req.TTL <= 0 {
		return nil, leaseNotFound(req.ID)
	}
	if req.TTL > maxLeaseTTL {
		return nil, status.Error(codes.OutOfRange, "etcdserver: too large lease TTL")
	}

	id := req.ID
	if id == 0 {
		id = m.nextLeaseID()
	}

	m.leaseMu.Lock()
	if _, ok := m.leases[id]; ok {
		m.leaseMu.Unlock()
		return nil, status.Error(codes.FailedPrecondition, "etcdserver: lease already exists")
	}

	st := &leaseState{
		id:       id,
		ttl:      req.TTL,
		deadline: time.Now().Add(time.Duration(req.TTL) * time.Second),
		keys:     make(map[string]struct{}),
	}
	m.scheduleLeaseLocked(st)
	m.leases[id] = st
	m.leaseMu.Unlock()

	if err := m.persistLeaseMeta(ctx, st.id, st.ttl); err != nil {
		_, _ = m.revokeLease(context.Background(), id)
		return nil, err
	}

	return &etcdserverpb.LeaseGrantResponse{
		Header: &etcdserverpb.ResponseHeader{},
		ID:     id,
		TTL:    req.TTL,
	}, nil
}

func (m *leaseManager) LeaseRevoke(ctx context.Context, req *etcdserverpb.LeaseRevokeRequest) (*etcdserverpb.LeaseRevokeResponse, error) {
	m.srv.metricCli.EmitCounter("lease.revoke", 1)
	if err := m.requireLeaseLeader("lease revoke"); err != nil {
		if m.srv.peers.EtcdProxyEnabled() {
			return m.srv.peers.LeaseRevoke(ctx, req)
		}
		return nil, err
	}
	rev, err := m.revokeLease(ctx, req.ID)
	if err != nil {
		return nil, err
	}
	return &etcdserverpb.LeaseRevokeResponse{
		Header: txnHeader(int64(rev)),
	}, nil
}

func (m *leaseManager) LeaseKeepAlive(stream etcdserverpb.Lease_LeaseKeepAliveServer) error {
	m.srv.metricCli.EmitCounter("lease.keepalive", 1)
	for {
		req, err := stream.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		if err := m.requireLeaseLeader("lease keepalive"); err != nil {
			if !m.srv.peers.EtcdProxyEnabled() {
				return err
			}
			resp, err := m.srv.peers.LeaseKeepAlive(stream.Context(), req)
			if err != nil {
				return err
			}
			if err := stream.Send(resp); err != nil {
				return err
			}
			continue
		}

		ttl, err := m.refreshLease(req.ID)
		if err != nil {
			ttl = 0
		}
		if err := stream.Send(&etcdserverpb.LeaseKeepAliveResponse{
			Header: &etcdserverpb.ResponseHeader{},
			ID:     req.ID,
			TTL:    ttl,
		}); err != nil {
			return err
		}
	}
}

func (m *leaseManager) LeaseTimeToLive(ctx context.Context, req *etcdserverpb.LeaseTimeToLiveRequest) (*etcdserverpb.LeaseTimeToLiveResponse, error) {
	m.srv.metricCli.EmitCounter("lease.ttl", 1)
	// A follower's lease state is a stale snapshot: the leader advances deadlines
	// via keepalive and grants/revokes leases the follower never observes. Answer
	// only as the leader; otherwise proxy, or fail like the write lease RPCs so the
	// client retries against the leader instead of reading stale local state (#56).
	if err := m.requireLeaseLeader("lease time-to-live"); err != nil {
		if m.srv.peers.EtcdProxyEnabled() {
			return m.srv.peers.LeaseTimeToLive(ctx, req)
		}
		return nil, err
	}

	m.leaseMu.Lock()
	defer m.leaseMu.Unlock()
	st, ok := m.leases[req.ID]
	if !ok {
		return &etcdserverpb.LeaseTimeToLiveResponse{
			Header: &etcdserverpb.ResponseHeader{},
			ID:     req.ID,
			TTL:    -1,
		}, nil
	}

	resp := &etcdserverpb.LeaseTimeToLiveResponse{
		Header:     &etcdserverpb.ResponseHeader{},
		ID:         req.ID,
		TTL:        remainingTTL(st),
		GrantedTTL: st.ttl,
	}
	if req.Keys {
		resp.Keys = leaseKeys(st)
	}
	return resp, nil
}

func (m *leaseManager) LeaseLeases(ctx context.Context, req *etcdserverpb.LeaseLeasesRequest) (*etcdserverpb.LeaseLeasesResponse, error) {
	m.srv.metricCli.EmitCounter("lease.leases", 1)
	// See LeaseTimeToLive: a follower must not enumerate leases from its stale
	// local snapshot; proxy to the leader or fail (#56).
	if err := m.requireLeaseLeader("lease leases"); err != nil {
		if m.srv.peers.EtcdProxyEnabled() {
			return m.srv.peers.LeaseLeases(ctx, req)
		}
		return nil, err
	}

	m.leaseMu.Lock()
	defer m.leaseMu.Unlock()
	resp := &etcdserverpb.LeaseLeasesResponse{
		Header: &etcdserverpb.ResponseHeader{},
		Leases: make([]*etcdserverpb.LeaseStatus, 0, len(m.leases)),
	}
	for id := range m.leases {
		resp.Leases = append(resp.Leases, &etcdserverpb.LeaseStatus{ID: id})
	}
	return resp, nil
}

func (m *leaseManager) ensureLeaseExists(id int64) error {
	if id == 0 {
		return nil
	}
	m.leaseMu.Lock()
	defer m.leaseMu.Unlock()
	if _, ok := m.leases[id]; !ok {
		return leaseNotFound(id)
	}
	return nil
}

// putLeasedAtomic writes a put together with its per-key lease attachment record
// in ONE atomic backend batch, so a leased key is never durably present without
// its attachment record. This closes the write+attach race (review #2): the old
// path did backend.Put and THEN attachKeyToStorage as a second, error-ignored
// write, so if the attach failed the key survived with no durable binding and
// was never expired when its lease lapsed (an orphan/leak). The value op also
// carries the lease inline in its v2 envelope (review #9). prevLease is the key's
// current in-memory binding: when the put clears the lease (put.Lease == 0) on a
// previously-leased key, the now-stale attachment is deleted in the same batch.
//
// Only the common single-Put path (leased Events, masterlease endpoints) routes
// here; the rarer generic-txn paths keep best-effort binding (documented at their
// call sites), and the expiry-time guard in deleteLeasedKey neutralizes any stale
// record either way.
func (m *leaseManager) putLeasedAtomic(ctx context.Context, put *etcdserverpb.PutRequest, prevLease int64) (*etcdserverpb.PutResponse, error) {
	userKey := string(put.Key)
	ops := []backend.TxnWriteOp{{Key: put.Key, Value: put.Value, Lease: put.Lease}}
	if put.Lease != 0 {
		ops = append(ops, backend.TxnWriteOp{
			Internal: true,
			Key:      leaseAttachKey(userKey),
			Value:    []byte(strconv.FormatInt(put.Lease, 10)),
		})
	} else {
		// Rebind to leaseless: drop the stale attachment in the same batch. A
		// delete of an absent attachment record is a no-op.
		ops = append(ops, backend.TxnWriteOp{Delete: true, Internal: true, Key: leaseAttachKey(userKey)})
	}
	_, rev, _, err := m.srv.backend.TxnApply(ctx, ops, nil, make([]bool, len(ops)))
	if err != nil {
		return nil, err
	}
	// The durable attachment committed atomically with the value above; update
	// only the in-memory index here (no separate durable write that could fail).
	m.bindKeyIndexOnly(put.Lease, userKey)
	return &etcdserverpb.PutResponse{Header: txnHeader(int64(rev))}, nil
}

// bindKeyIndexOnly updates the in-memory key->lease index (and per-lease key set)
// without writing the durable attachment record — used after putLeasedAtomic,
// which already persisted the attachment atomically with the value.
func (m *leaseManager) bindKeyIndexOnly(id int64, key string) {
	m.leaseMu.Lock()
	m.bindKeyToLeaseLocked(id, key)
	m.leaseMu.Unlock()
}

func (m *leaseManager) unbindKeyIndexOnly(key string) {
	m.leaseMu.Lock()
	if id, ok := m.keyLeaseIndex[key]; ok {
		if st := m.leases[id]; st != nil {
			delete(st.keys, key)
		}
		delete(m.keyLeaseIndex, key)
	}
	atomic.StoreInt64(&m.leasedKeyCount, int64(len(m.keyLeaseIndex)))
	m.leaseMu.Unlock()
}

// withLeaseAttachmentOps appends internal attachment mutations after the user
// writes. The returned userCount lets response/result handling ignore those
// implementation-only ops.
func (m *leaseManager) withLeaseAttachmentOps(writes []backend.TxnWriteOp) ([]backend.TxnWriteOp, int) {
	userCount := len(writes)
	out := append([]backend.TxnWriteOp(nil), writes...)
	for _, op := range writes {
		key := string(op.Key)
		previous := m.leaseIDForKey(key)
		switch {
		case op.Delete && previous != 0:
			out = append(out, backend.TxnWriteOp{Delete: true, Internal: true, Key: leaseAttachKey(key)})
		case !op.Delete && op.Lease != 0:
			out = append(out, backend.TxnWriteOp{Internal: true, Key: leaseAttachKey(key), Value: []byte(strconv.FormatInt(op.Lease, 10))})
		case !op.Delete && previous != 0:
			out = append(out, backend.TxnWriteOp{Delete: true, Internal: true, Key: leaseAttachKey(key)})
		}
	}
	return out, userCount
}

func (m *leaseManager) applyLeaseIndexes(writes []backend.TxnWriteOp, results []backend.TxnWriteResult, userCount int) {
	for i := 0; i < userCount; i++ {
		if writes[i].Delete {
			if results[i].Deleted {
				m.unbindKeyIndexOnly(string(writes[i].Key))
			}
			continue
		}
		m.bindKeyIndexOnly(writes[i].Lease, string(writes[i].Key))
	}
}

func (m *leaseManager) bindKeyToLease(ctx context.Context, id int64, key string) {
	m.leaseMu.Lock()
	_, hadPrevious := m.keyLeaseIndex[key]
	m.bindKeyToLeaseLocked(id, key)
	// Read back the resulting binding under the lock to decide the durable action.
	boundTo, bound := m.keyLeaseIndex[key]
	m.leaseMu.Unlock()
	// Persist only THIS key's attachment (O(1)), never the whole key-list (#17).
	// A rebind (previous lease -> id) needs no write to the previous lease: the
	// attachment record is keyed by the user key, so the new id overwrites it.
	switch {
	case bound && boundTo == id:
		_ = m.attachKeyToStorage(ctx, id, key)
	case hadPrevious:
		// key had an attachment but is now unbound (id==0 or the lease was absent):
		// clear the stale attachment record.
		_ = m.detachKeyFromStorage(ctx, key)
	}
	// else: a plain Put of a never-leased key — no lease bookkeeping, no write.
}

func (m *leaseManager) bindKeyToLeaseLocked(id int64, key string) []*leaseState {
	// caller holds leaseMu; keep the lock-free lease count in sync (multiple
	// return paths, so update on exit). Closure so len() is read at return, not
	// captured at defer registration.
	defer func() { atomic.StoreInt64(&m.leasedKeyCount, int64(len(m.keyLeaseIndex))) }()
	var changed []*leaseState
	if previousID, ok := m.keyLeaseIndex[key]; ok {
		if previous, exists := m.leases[previousID]; exists {
			delete(previous.keys, key)
			changed = append(changed, previous)
		}
		delete(m.keyLeaseIndex, key)
	}
	if id == 0 {
		return changed
	}
	st, ok := m.leases[id]
	if !ok {
		return changed
	}
	st.keys[key] = struct{}{}
	m.keyLeaseIndex[key] = id
	changed = append(changed, st)
	return changed
}

func (m *leaseManager) unbindKeyFromLease(ctx context.Context, key string) {
	m.leaseMu.Lock()
	_, wasBound := m.keyLeaseIndex[key]
	if id, ok := m.keyLeaseIndex[key]; ok {
		if st, exists := m.leases[id]; exists {
			delete(st.keys, key)
		}
		delete(m.keyLeaseIndex, key)
	}
	atomic.StoreInt64(&m.leasedKeyCount, int64(len(m.keyLeaseIndex)))
	m.leaseMu.Unlock()
	if wasBound {
		// Drop just this key's attachment record (O(1)), not the whole list (#17).
		_ = m.detachKeyFromStorage(ctx, key)
	}
}

func (m *leaseManager) leaseIDForKey(key string) int64 {
	// Fast path: when no key holds a lease, skip the mutex entirely. This is the
	// common case for reads (most ranges cover leaseless keys) and keeps the read
	// hot path from serializing on leaseMu once per returned KeyValue. A key that
	// truly has a committed lease is always counted here, so this never misses one;
	// it only races a concurrent bind, for which returning 0 is acceptable (that
	// write is not ordered before this read).
	if atomic.LoadInt64(&m.leasedKeyCount) == 0 {
		return 0
	}
	m.leaseMu.Lock()
	defer m.leaseMu.Unlock()
	return m.keyLeaseIndex[key]
}

func (m *leaseManager) refreshLease(id int64) (int64, error) {
	m.leaseMu.Lock()
	st, ok := m.leases[id]
	if !ok {
		m.leaseMu.Unlock()
		return 0, leaseNotFound(id)
	}
	st.deadline = time.Now().Add(time.Duration(st.ttl) * time.Second)
	m.scheduleLeaseLocked(st)
	ttl := st.ttl
	m.leaseMu.Unlock()
	// Mirror etcd lessor.Renew -> l.refresh(0): a keepalive only bumps the
	// in-memory deadline and reschedules the expiry timer. It must NOT persist,
	// otherwise every keepalive tick mints a fresh MVCC version, a watch event,
	// and a TSO revision. Durable lease state (grant identity and attached keys)
	// is still persisted on grant/bind/unbind; on restart or leader-change the
	// deadline is reconstructed as now+grantedTTL in applyLeaseRecords.
	return ttl, nil
}

// deleteLeasedKey deletes one key bound to lease id WITHOUT clobbering a value a
// concurrent writer changed out from under us (#1). It compare-deletes at the key's
// current revision, so a Put that reassigned the key to another lease or updated it
// (both mint a new revision) is not wrongly deleted. If the compare fails but the
// key is still bound to THIS same lease (a keepalive re-Put), it retries at the new
// revision; if the key was reassigned to another lease or unbound, it is left
// untouched. A genuine storage error is returned so the caller keeps the lease and
// retries rather than orphaning the surviving keys.
func (m *leaseManager) deleteLeasedKey(ctx context.Context, id int64, key string) error {
	for attempt := 0; attempt < 4; attempt++ {
		// Is the key STILL bound to this lease? A concurrent Put that reassigned it
		// to another lease (or made it leaseless) between the revoke/expire snapshot
		// and now means it is no longer ours to delete. keyLeaseIndex is the
		// authoritative current binding.
		m.leaseMu.Lock()
		boundTo, bound := m.keyLeaseIndex[key]
		m.leaseMu.Unlock()
		if !bound || boundTo != id {
			return nil // reassigned or unbound -> not ours to delete
		}
		resp, err := m.srv.backend.Get(ctx, &etcdserverpb.RangeRequest{Key: []byte(key)})
		if err != nil {
			return err
		}
		if len(resp.Kvs) == 0 {
			return nil // already gone
		}
		// Guard against a stale attachment record (e.g. a best-effort detach that
		// failed, then a leader change reloaded the record) causing us to delete a
		// key that is no longer ours: the current version's inline lease (review #9)
		// is authoritative. If it names a different lease, or none, the key was
		// rebound/recreated — leave it. For legacy v1 values with no inline lease
		// the read falls back to the (rechecked-above) live index, so id matches and
		// this is a no-op — preserving pre-existing behavior for un-upgraded data.
		if resp.Kvs[0].Lease != id {
			return nil
		}
		// Compare-delete at the observed revision so a Put that changes the key
		// between this read and the delete (a keepalive re-Put, or a reassignment
		// whose value write landed but whose index update has not yet) does not get
		// its new value clobbered; a failed compare loops to re-check the binding.
		dresp, err := m.srv.backend.Delete(ctx, []byte(key), resp.Kvs[0].ModRevision, false)
		if err != nil {
			return err
		}
		if dresp.GetSucceeded() {
			return nil // deleted exactly the version bound to this lease
		}
		// changed under us -> loop: re-check binding (retry keepalive, skip reassign)
	}
	return nil // extremely rare: a keepalive kept racing; leave it for the next cycle
}

// deleteLeasedKeysAtomic removes every still-live key bound to id in one user
// MVCC revision. Exact mod-revision guards make the whole batch retry if any
// concurrent Put/rebind races the snapshot; attachment records are internal ops
// in the same TiKV transaction and therefore add no revision of their own.
func (m *leaseManager) deleteLeasedKeysAtomic(ctx context.Context, id int64, keys []string) (uint64, error) {
	for attempt := 0; attempt < 4; attempt++ {
		ops := make([]backend.TxnWriteOp, 0, len(keys)*2)
		guards := make([]backend.TxnGuard, 0, len(keys))
		for _, key := range keys {
			m.leaseMu.Lock()
			boundTo, bound := m.keyLeaseIndex[key]
			m.leaseMu.Unlock()
			if !bound || boundTo != id {
				continue
			}
			resp, err := m.srv.backend.Get(ctx, &etcdserverpb.RangeRequest{Key: []byte(key)})
			if err != nil {
				return 0, err
			}
			if len(resp.Kvs) == 0 || resp.Kvs[0].Lease != id {
				continue
			}
			ops = append(ops,
				backend.TxnWriteOp{Delete: true, Key: []byte(key)},
				backend.TxnWriteOp{Delete: true, Internal: true, Key: leaseAttachKey(key)},
			)
			guards = append(guards, backend.TxnGuard{Key: []byte(key), Revision: uint64(resp.Kvs[0].ModRevision)})
		}
		if len(ops) == 0 {
			return m.srv.backend.GetCurrentRevision(), nil
		}
		prevKV := make([]bool, len(ops))
		_, rev, _, err := m.srv.backend.TxnApply(ctx, ops, guards, prevKV)
		if errors.Is(err, backend.ErrTxnGuardConflict) {
			continue
		}
		return rev, err
	}
	// A persistently racing key remains attached; leave the lease intact so the
	// next expiry/revoke attempt can retry instead of orphaning it.
	return 0, status.Error(codes.Unavailable, "etcdserver: lease keys changed during revoke")
}

func (m *leaseManager) revokeLease(ctx context.Context, id int64) (uint64, error) {
	// Delete the attached keys BEFORE removing the lease state/record (#36): if a
	// key delete fails we must keep the lease so the keys are not orphaned (no
	// lease left to ever expire them). Only once every bound key is gone is it
	// safe to drop the lease record and its attachment records.
	keys, ok := m.leaseKeysSnapshot(id)
	if !ok {
		return 0, leaseNotFound(id)
	}
	rev, err := m.deleteLeasedKeysAtomic(ctx, id, keys)
	if err != nil {
		return 0, err
	}
	_, err = m.removeLease(id)
	return rev, err
}

func (m *leaseManager) expireLease(id int64) {
	if !m.srv.peers.IsLeader() {
		m.retryLeaseExpiry(id)
		return
	}

	ctx := context.Background()
	keys, ok := m.leaseKeysSnapshot(id)
	if !ok {
		return // already removed
	}
	// Delete bound keys first; a missing key returns no error, so a retry after a
	// partial deletion is safe. On a real storage failure keep the lease and retry
	// rather than swallowing the error and orphaning the surviving keys (#36).
	if _, err := m.deleteLeasedKeysAtomic(ctx, id, keys); err != nil {
		m.srv.metricCli.EmitCounter("lease.expire.delete.err", 1)
		klog.ErrorS(err, "lease expiry: atomic delete of bound keys failed; keeping lease for retry", "lease", id, "keys", len(keys))
		m.retryLeaseExpiry(id)
		return
	}
	// Every bound key is gone; now drop the lease record and attachment records.
	_, _ = m.removeLease(id)
}

// leaseKeysSnapshot returns a copy of the keys currently attached to lease id
// without removing the lease, so callers can delete the keys first and only then
// tear the lease down (#36).
func (m *leaseManager) leaseKeysSnapshot(id int64) ([]string, bool) {
	m.leaseMu.Lock()
	defer m.leaseMu.Unlock()
	st, ok := m.leases[id]
	if !ok {
		return nil, false
	}
	keys := make([]string, 0, len(st.keys))
	for key := range st.keys {
		keys = append(keys, key)
	}
	return keys, true
}

func (m *leaseManager) retryLeaseExpiry(id int64) {
	m.leaseMu.Lock()
	defer m.leaseMu.Unlock()
	st, ok := m.leases[id]
	if !ok || st.timer == nil {
		return
	}
	st.timer.Reset(leaseExpiryRetryInterval)
}

func (m *leaseManager) removeLease(id int64) ([]string, error) {
	m.leaseMu.Lock()
	st, ok := m.leases[id]
	if !ok {
		m.leaseMu.Unlock()
		return nil, leaseNotFound(id)
	}
	if st.timer != nil {
		st.timer.Stop()
	}
	keys := make([]string, 0, len(st.keys))
	for key := range st.keys {
		keys = append(keys, key)
		delete(m.keyLeaseIndex, key)
	}
	delete(m.leases, id)
	atomic.StoreInt64(&m.leasedKeyCount, int64(len(m.keyLeaseIndex)))
	m.leaseMu.Unlock()

	ctx := context.Background()
	err := m.deleteLeaseState(ctx, id)
	// Remove each key's attachment record so the leasekeys/ keyspace does not leak
	// live records pointing at a now-deleted lease (recovery skips orphans, but they
	// would never be reclaimed otherwise). Same O(keys) order as the object deletes
	// the caller performs on expiry.
	for _, key := range keys {
		_ = m.detachKeyFromStorage(ctx, key)
	}
	return keys, err
}

func (m *leaseManager) keysInDeleteRange(ctx context.Context, r *etcdserverpb.DeleteRangeRequest) ([]string, error) {
	if len(r.RangeEnd) == 0 {
		return []string{string(r.Key)}, nil
	}
	if isEmptyNonFromKeyRange(r.Key, r.RangeEnd) {
		return nil, nil
	}
	resp, err := m.srv.backend.List(ctx, &etcdserverpb.RangeRequest{
		Key:      r.Key,
		RangeEnd: r.RangeEnd,
	})
	if err != nil {
		return nil, err
	}
	keys := make([]string, 0, len(resp.Kvs))
	for _, kv := range resp.Kvs {
		keys = append(keys, string(kv.Key))
	}
	return keys, nil
}

func remainingTTL(st *leaseState) int64 {
	ttl := int64(time.Until(st.deadline).Seconds())
	if ttl < 0 {
		return 0
	}
	if ttl == 0 && time.Now().Before(st.deadline) {
		return 1
	}
	return ttl
}

func leaseKeys(st *leaseState) [][]byte {
	keys := make([][]byte, 0, len(st.keys))
	for key := range st.keys {
		keys = append(keys, []byte(key))
	}
	return keys
}

func leaseNotFound(id int64) error {
	return status.Error(codes.NotFound, "etcdserver: requested lease not found")
}

func (m *leaseManager) requireLeaseLeader(op string) error {
	if m.srv.peers.IsLeader() {
		return nil
	}
	m.srv.metricCli.EmitCounter("lease.follower", 1)
	return status.Errorf(codes.Unavailable, "%s error addr is %s leader %s", op, m.srv.backend.GetResourceLock().Identity(), m.srv.peers.GetLeaderInfo())
}

func (m *leaseManager) restoreLeases(ctx context.Context) error {
	records, attachments, err := m.loadLeaseRecords(ctx)
	if err != nil {
		return err
	}
	// Load into memory only; migration writes happen on leadership (ReloadLeases),
	// since restore runs in New() before this node has necessarily won election.
	m.applyLeaseRecords(records, attachments)
	return nil
}

// ReloadLeases refreshes in-memory lease state from storage. It MUST be called
// when this node acquires leadership: while a follower the node holds a stale
// snapshot (the real leader advanced deadlines via keepalive and granted leases
// this node never saw). Without a reload the new leader would expire
// still-alive leases — deleting their bound keys (e.g. masterleases) — and
// orphan leases granted after this node started.
func (m *leaseManager) ReloadLeases(ctx context.Context) error {
	records, attachments, err := m.loadLeaseRecords(ctx)
	if err != nil {
		return err
	}
	legacy := m.applyLeaseRecords(records, attachments)
	// This node is now the leader; convert any pre-#17 monolithic records to the
	// per-key attachment format so subsequent detaches are durable and the
	// monolithic key-list is not carried forward. One-time, idempotent.
	m.migrateLegacyLeases(ctx, legacy)
	// Start the safety-net sweeper that reclaims leased keys whose expiry timer was
	// never (re)armed because their attachment outlived its lease meta record.
	m.startOrphanSweeper()
	return nil
}

// loadLeaseRecords reads the per-lease meta records and the per-key attachment
// records from storage at the latest revision.
func (m *leaseManager) loadLeaseRecords(ctx context.Context) ([]leaseRecord, map[string]int64, error) {
	internalRecords, err := m.srv.backend.InternalRange(ctx, leaseStoragePrefix)
	if err != nil {
		return nil, nil, err
	}
	resp, err := m.srv.backend.List(ctx, &etcdserverpb.RangeRequest{
		Key:      leaseStoragePrefix,
		RangeEnd: prefixEnd(leaseStoragePrefix),
		Revision: latestRestoreRevision,
	})
	if err != nil {
		return nil, nil, err
	}
	// Read the legacy user-MVCC keyspace during rolling upgrades. A new internal
	// record wins when both layouts contain the same lease ID.
	recordByID := make(map[int64]leaseRecord, len(resp.Kvs)+len(internalRecords))
	for _, kv := range resp.Kvs {
		var record leaseRecord
		if err := json.Unmarshal(kv.Value, &record); err != nil {
			return nil, nil, err
		}
		record.LegacyStorage = true
		recordByID[record.ID] = record
	}
	for _, value := range internalRecords {
		var record leaseRecord
		if err := json.Unmarshal(value, &record); err != nil {
			return nil, nil, err
		}
		recordByID[record.ID] = record
	}
	records := make([]leaseRecord, 0, len(recordByID))
	for _, record := range recordByID {
		records = append(records, record)
	}

	aresp, err := m.srv.backend.List(ctx, &etcdserverpb.RangeRequest{
		Key:      leaseAttachPrefix,
		RangeEnd: prefixEnd(leaseAttachPrefix),
		Revision: latestRestoreRevision,
	})
	if err != nil {
		return nil, nil, err
	}
	attachments := make(map[string]int64, len(aresp.Kvs))
	for _, kv := range aresp.Kvs {
		userKey := string(kv.Key[len(leaseAttachPrefix):])
		id, perr := strconv.ParseInt(string(kv.Value), 10, 64)
		if perr != nil {
			continue
		}
		attachments[userKey] = id
	}
	internalAttachments, err := m.srv.backend.InternalRange(ctx, leaseAttachPrefix)
	if err != nil {
		return nil, nil, err
	}
	for key, value := range internalAttachments {
		userKey := key[len(leaseAttachPrefix):]
		id, perr := strconv.ParseInt(string(value), 10, 64)
		if perr == nil {
			attachments[userKey] = id
		}
	}
	return records, attachments, nil
}

// migrateLegacyLeases rewrites pre-#17 leases (meta records that still carried an
// inline key list) into the new format: one attachment record per currently-bound
// key plus a keyless meta record. Idempotent; only runs on the leader.
func (m *leaseManager) migrateLegacyLeases(ctx context.Context, ids []int64) {
	for _, id := range ids {
		m.leaseMu.Lock()
		st, ok := m.leases[id]
		var keys []string
		var ttl int64
		if ok {
			ttl = st.ttl
			keys = make([]string, 0, len(st.keys))
			for k := range st.keys {
				keys = append(keys, k)
			}
		}
		m.leaseMu.Unlock()
		if !ok {
			continue
		}
		complete := true
		for _, k := range keys {
			if err := m.attachKeyToStorage(ctx, id, k); err != nil {
				complete = false
				break
			}
		}
		if !complete || m.persistLeaseMeta(ctx, id, ttl) != nil {
			continue
		}
		// Retire the legacy user-MVCC record only after the internal replacement
		// and every attachment are durable. This prevents a revoked lease from
		// being resurrected by the compatibility reader on the next leadership.
		_, _ = m.srv.backend.DeleteRange(ctx, &etcdserverpb.DeleteRangeRequest{Key: leaseStorageKey(id)})
		for _, k := range keys {
			_, _ = m.srv.backend.DeleteRange(ctx, &etcdserverpb.DeleteRangeRequest{Key: leaseAttachKey(k)})
		}
	}
}

// applyLeaseRecords atomically replaces the in-memory lease state from storage:
// it stops existing expiry timers, rebuilds the lease map from the meta records
// and the key->lease bindings from the per-key attachment records, and schedules
// timers from a fresh now+TTL deadline. Any lease whose meta still carried an
// inline key list (a pre-#17 record) is included in the returned slice so the
// leader can migrate it to the new per-key format. Safe on both first restore
// (empty maps) and leadership reload.
func (m *leaseManager) applyLeaseRecords(records []leaseRecord, attachments map[string]int64) (legacy []int64) {
	now := time.Now()
	m.leaseMu.Lock()
	defer m.leaseMu.Unlock()
	for _, st := range m.leases {
		if st.timer != nil {
			st.timer.Stop()
		}
	}
	m.leases = make(map[int64]*leaseState, len(records))
	m.keyLeaseIndex = make(map[string]int64)
	for _, record := range records {
		st := &leaseState{
			id:  record.ID,
			ttl: record.TTL,
			// Mirror etcd initAndRecover + Promote->refresh: recover the deadline
			// as now+grantedTTL rather than the stale persisted absolute deadline.
			// Because keepalive no longer persists the deadline, the persisted
			// DeadlineUnixNano is only ever the grant-time value; trusting it would
			// immediately expire a lease that was kept alive well past grant time
			// (regressing #14/#18). A fresh full-TTL window is the safe, etcd-matching
			// recovery.
			deadline: now.Add(time.Duration(record.TTL) * time.Second),
			keys:     make(map[string]struct{}, len(record.Keys)),
		}
		// Legacy (pre-#17) monolithic key list, if present. New records carry none.
		for _, key := range record.Keys {
			st.keys[key] = struct{}{}
			m.keyLeaseIndex[key] = st.id
		}
		if record.LegacyStorage || len(record.Keys) > 0 {
			legacy = append(legacy, record.ID)
		}
		if st.id > m.leaseID {
			m.leaseID = st.id
		}
		m.leases[st.id] = st
	}
	// Per-key attachment records (#17). Attachments for a lease that no longer has
	// a meta record (revoked, tombstone not yet compacted) are skipped as orphans.
	for key, id := range attachments {
		st, ok := m.leases[id]
		if !ok {
			continue
		}
		st.keys[key] = struct{}{}
		m.keyLeaseIndex[key] = id
	}
	// Schedule timers once all keys are attached.
	for _, st := range m.leases {
		m.scheduleLeaseLocked(st)
		if !st.deadline.After(now) {
			st.timer.Reset(0)
		}
	}
	atomic.StoreInt64(&m.leasedKeyCount, int64(len(m.keyLeaseIndex)))
	return legacy
}

// StopLeases stops every expiry timer and drops the in-memory lease snapshot. It
// MUST be called when this node loses leadership: otherwise a demoted leader
// keeps firing expiry timers (churn, and it would act on a stale snapshot) and
// answers lease reads from state the new leader has since advanced. Leadership
// re-acquisition rebuilds the state from storage via ReloadLeases (#57).
func (m *leaseManager) StopLeases() {
	m.stopLeases()
}

func (m *leaseManager) stopLeases() {
	// Stop the sweeper first (it acquires leaseMu itself, so must run outside the
	// lock below).
	m.stopOrphanSweeper()
	m.leaseMu.Lock()
	defer m.leaseMu.Unlock()
	for _, st := range m.leases {
		if st.timer != nil {
			st.timer.Stop()
		}
	}
	m.leases = make(map[int64]*leaseState)
	m.keyLeaseIndex = make(map[string]int64)
	atomic.StoreInt64(&m.leasedKeyCount, 0)
}

// orphanLeaseSweepInterval is how often the leader reconciles durable lease
// attachment records against live leases. It is a slow safety net (expiry itself
// is timer-driven), so a coarse interval is fine.
const orphanLeaseSweepInterval = 10 * time.Minute

// startOrphanSweeper launches the leader-side orphaned-leased-key sweeper if it is
// not already running. Called on leadership acquisition (after ReloadLeases has
// rebuilt lease state).
func (m *leaseManager) startOrphanSweeper() {
	m.leaseMu.Lock()
	if m.orphanSweepStop != nil {
		m.leaseMu.Unlock()
		return
	}
	stop := make(chan struct{})
	m.orphanSweepStop = stop
	m.leaseMu.Unlock()
	go m.runOrphanSweeper(stop)
}

// stopOrphanSweeper signals the sweeper goroutine to exit (idempotent).
func (m *leaseManager) stopOrphanSweeper() {
	m.leaseMu.Lock()
	if m.orphanSweepStop != nil {
		close(m.orphanSweepStop)
		m.orphanSweepStop = nil
	}
	m.leaseMu.Unlock()
}

func (m *leaseManager) runOrphanSweeper(stop chan struct{}) {
	ticker := time.NewTicker(orphanLeaseSweepInterval)
	defer ticker.Stop()
	for {
		select {
		case <-stop:
			return
		case <-ticker.C:
			if !m.srv.peers.IsLeader() {
				continue
			}
			m.sweepOrphanLeasedKeys(context.Background())
		}
	}
}

// sweepOrphanLeasedKeys is the safety-net reconciliation borrowed from kine's TTL
// poller: it guarantees a leased key is eventually collected even if its in-memory
// expiry timer was never (re)armed. That happens when a durable attachment record
// (leasekeys/<key> -> <id>) outlives its lease's meta record — e.g. a best-effort
// detach failed on revoke, then a leader change reloaded the attachment, which
// applyLeaseRecords drops as an orphan (no timer). The key would otherwise carry
// an inline lease in its value forever with nothing to expire it.
//
// For each durable attachment whose lease id is no longer live and whose key is
// not in the live index, it deletes the key (its lease is gone — etcd deletes a
// key when its lease lapses), or, when the key was rebound/removed, just reclaims
// the stale attachment record.
func (m *leaseManager) sweepOrphanLeasedKeys(ctx context.Context) {
	_, attachments, err := m.loadLeaseRecords(ctx)
	if err != nil {
		m.srv.metricCli.EmitCounter("lease.orphan_sweep.err", 1)
		klog.ErrorS(err, "orphan lease sweep: load attachment records failed")
		return
	}
	for key, id := range attachments {
		m.leaseMu.Lock()
		_, leaseLive := m.leases[id]
		_, indexed := m.keyLeaseIndex[key]
		m.leaseMu.Unlock()
		if leaseLive || indexed {
			continue // healthy binding, or already tracked for expiry
		}
		if !m.srv.peers.IsLeader() {
			return // lost leadership mid-sweep
		}
		m.reconcileOrphanAttachment(ctx, key, id)
	}
}

// reconcileOrphanAttachment handles one attachment record whose lease is no longer
// live. It deletes the key only when the key's authoritative per-version lease
// (inline in the value, review #9) still names the defunct lease; otherwise it
// merely reclaims the stale attachment record, never touching a key that was
// rebound or recreated leaseless.
func (m *leaseManager) reconcileOrphanAttachment(ctx context.Context, key string, id int64) {
	resp, err := m.srv.backend.Get(ctx, &etcdserverpb.RangeRequest{Key: []byte(key)})
	if err != nil {
		return
	}
	if len(resp.Kvs) == 0 || resp.Kvs[0].Lease != id {
		// Key already gone, or rebound/recreated to a different (or no) lease: the
		// attachment record is stale. Reclaim it; leave any live key alone. (A
		// legacy v1 value carries no inline lease and reads back Lease==0 here, so
		// its key is conservatively left in place — only its stale record is
		// cleaned.)
		if err := m.detachKeyFromStorage(ctx, key); err == nil {
			m.srv.metricCli.EmitCounter("lease.orphan_sweep.record_reclaimed", 1)
		}
		return
	}
	// The key is live and still bound (per its inline lease) to a lease that no
	// longer exists. Re-check under the lock that the lease was not just
	// (re)granted, then compare-delete at the observed revision so a concurrent
	// re-Put is not clobbered.
	m.leaseMu.Lock()
	_, leaseLive := m.leases[id]
	m.leaseMu.Unlock()
	if leaseLive {
		return
	}
	dresp, err := m.srv.backend.Delete(ctx, []byte(key), resp.Kvs[0].ModRevision, false)
	if err != nil {
		return
	}
	if dresp.GetSucceeded() {
		_ = m.detachKeyFromStorage(ctx, key)
		m.srv.metricCli.EmitCounter("lease.orphan_sweep.key_deleted", 1)
		klog.InfoS("orphan lease sweep: deleted key bound to a defunct lease", "key", key, "lease", id)
	}
}

func (m *leaseManager) scheduleLeaseLocked(st *leaseState) {
	duration := time.Until(st.deadline)
	if duration < 0 {
		duration = 0
	}
	if st.timer == nil {
		st.timer = time.AfterFunc(duration, func() {
			m.expireLease(st.id)
		})
		return
	}
	st.timer.Reset(duration)
}

// persistLeaseMeta writes the small per-lease meta record {id, ttl}. It carries
// no key list (attachments are separate records, #17) and no deadline (recovery
// resets the deadline to now+TTL, see applyLeaseRecords). Written on grant and on
// legacy-record migration.
func (m *leaseManager) persistLeaseMeta(ctx context.Context, id, ttl int64) error {
	data, err := json.Marshal(leaseRecord{ID: id, TTL: ttl})
	if err != nil {
		return err
	}
	return m.srv.backend.InternalPut(ctx, leaseStorageKey(id), data)
}

// attachKeyToStorage records that userKey is attached to lease id as a single
// small record (leasekeys/<userKey> -> <id>). O(1) per attach, replacing the old
// full-key-list rewrite (#17). The record is keyed by the user key, which has at
// most one lease, so a rebind simply overwrites it.
func (m *leaseManager) attachKeyToStorage(ctx context.Context, id int64, userKey string) error {
	return m.srv.backend.InternalPut(ctx, leaseAttachKey(userKey), []byte(strconv.FormatInt(id, 10)))
}

func (m *leaseManager) detachKeyFromStorage(ctx context.Context, userKey string) error {
	return m.srv.backend.InternalDelete(ctx, leaseAttachKey(userKey))
}

func (m *leaseManager) deleteLeaseState(ctx context.Context, id int64) error {
	return m.srv.backend.InternalDelete(ctx, leaseStorageKey(id))
}

func leaseAttachKey(userKey string) []byte {
	key := make([]byte, 0, len(leaseAttachPrefix)+len(userKey))
	key = append(key, leaseAttachPrefix...)
	key = append(key, userKey...)
	return key
}

func leaseStorageKey(id int64) []byte {
	key := make([]byte, 0, len(leaseStoragePrefix)+20)
	key = append(key, leaseStoragePrefix...)
	key = append(key, []byte(strconv.FormatInt(id, 10))...)
	return key
}

func prefixEnd(prefix []byte) []byte {
	end := append([]byte(nil), prefix...)
	for i := len(end) - 1; i >= 0; i-- {
		if end[i] < 0xff {
			end[i]++
			return end[:i+1]
		}
	}
	return []byte{0}
}
