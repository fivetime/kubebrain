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
	"io"
	"strconv"
	"sync/atomic"
	"time"

	"go.etcd.io/etcd/api/v3/etcdserverpb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"k8s.io/klog/v2"
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
}

func (s *RPCServer) LeaseGrant(ctx context.Context, req *etcdserverpb.LeaseGrantRequest) (*etcdserverpb.LeaseGrantResponse, error) {
	s.metricCli.EmitCounter("lease.grant", 1)
	if err := s.requireLeaseLeader("lease grant"); err != nil {
		if s.peers.EtcdProxyEnabled() {
			return s.peers.LeaseGrant(ctx, req)
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
		id = s.nextLeaseID()
	}

	s.leaseMu.Lock()
	if _, ok := s.leases[id]; ok {
		s.leaseMu.Unlock()
		return nil, status.Error(codes.FailedPrecondition, "etcdserver: lease already exists")
	}

	st := &leaseState{
		id:       id,
		ttl:      req.TTL,
		deadline: time.Now().Add(time.Duration(req.TTL) * time.Second),
		keys:     make(map[string]struct{}),
	}
	s.scheduleLeaseLocked(st)
	s.leases[id] = st
	s.leaseMu.Unlock()

	if err := s.persistLeaseMeta(ctx, st.id, st.ttl); err != nil {
		_ = s.revokeLease(context.Background(), id)
		return nil, err
	}

	return &etcdserverpb.LeaseGrantResponse{
		Header: &etcdserverpb.ResponseHeader{},
		ID:     id,
		TTL:    req.TTL,
	}, nil
}

func (s *RPCServer) LeaseRevoke(ctx context.Context, req *etcdserverpb.LeaseRevokeRequest) (*etcdserverpb.LeaseRevokeResponse, error) {
	s.metricCli.EmitCounter("lease.revoke", 1)
	if err := s.requireLeaseLeader("lease revoke"); err != nil {
		if s.peers.EtcdProxyEnabled() {
			return s.peers.LeaseRevoke(ctx, req)
		}
		return nil, err
	}
	if err := s.revokeLease(ctx, req.ID); err != nil {
		return nil, err
	}
	return &etcdserverpb.LeaseRevokeResponse{
		Header: &etcdserverpb.ResponseHeader{},
	}, nil
}

func (s *RPCServer) LeaseKeepAlive(stream etcdserverpb.Lease_LeaseKeepAliveServer) error {
	s.metricCli.EmitCounter("lease.keepalive", 1)
	for {
		req, err := stream.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		if err := s.requireLeaseLeader("lease keepalive"); err != nil {
			if !s.peers.EtcdProxyEnabled() {
				return err
			}
			resp, err := s.peers.LeaseKeepAlive(stream.Context(), req)
			if err != nil {
				return err
			}
			if err := stream.Send(resp); err != nil {
				return err
			}
			continue
		}

		ttl, err := s.refreshLease(req.ID)
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

func (s *RPCServer) LeaseTimeToLive(ctx context.Context, req *etcdserverpb.LeaseTimeToLiveRequest) (*etcdserverpb.LeaseTimeToLiveResponse, error) {
	s.metricCli.EmitCounter("lease.ttl", 1)
	// A follower's lease state is a stale snapshot: the leader advances deadlines
	// via keepalive and grants/revokes leases the follower never observes. Answer
	// only as the leader; otherwise proxy, or fail like the write lease RPCs so the
	// client retries against the leader instead of reading stale local state (#56).
	if err := s.requireLeaseLeader("lease time-to-live"); err != nil {
		if s.peers.EtcdProxyEnabled() {
			return s.peers.LeaseTimeToLive(ctx, req)
		}
		return nil, err
	}

	s.leaseMu.Lock()
	defer s.leaseMu.Unlock()
	st, ok := s.leases[req.ID]
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

func (s *RPCServer) LeaseLeases(ctx context.Context, req *etcdserverpb.LeaseLeasesRequest) (*etcdserverpb.LeaseLeasesResponse, error) {
	s.metricCli.EmitCounter("lease.leases", 1)
	// See LeaseTimeToLive: a follower must not enumerate leases from its stale
	// local snapshot; proxy to the leader or fail (#56).
	if err := s.requireLeaseLeader("lease leases"); err != nil {
		if s.peers.EtcdProxyEnabled() {
			return s.peers.LeaseLeases(ctx, req)
		}
		return nil, err
	}

	s.leaseMu.Lock()
	defer s.leaseMu.Unlock()
	resp := &etcdserverpb.LeaseLeasesResponse{
		Header: &etcdserverpb.ResponseHeader{},
		Leases: make([]*etcdserverpb.LeaseStatus, 0, len(s.leases)),
	}
	for id := range s.leases {
		resp.Leases = append(resp.Leases, &etcdserverpb.LeaseStatus{ID: id})
	}
	return resp, nil
}

func (s *RPCServer) ensureLeaseExists(id int64) error {
	if id == 0 {
		return nil
	}
	s.leaseMu.Lock()
	defer s.leaseMu.Unlock()
	if _, ok := s.leases[id]; !ok {
		return leaseNotFound(id)
	}
	return nil
}

func (s *RPCServer) bindKeyToLease(id int64, key string) {
	s.leaseMu.Lock()
	_, hadPrevious := s.keyLeaseIndex[key]
	s.bindKeyToLeaseLocked(id, key)
	// Read back the resulting binding under the lock to decide the durable action.
	boundTo, bound := s.keyLeaseIndex[key]
	s.leaseMu.Unlock()
	// Persist only THIS key's attachment (O(1)), never the whole key-list (#17).
	// A rebind (previous lease -> id) needs no write to the previous lease: the
	// attachment record is keyed by the user key, so the new id overwrites it.
	switch {
	case bound && boundTo == id:
		_ = s.attachKeyToStorage(context.Background(), id, key)
	case hadPrevious:
		// key had an attachment but is now unbound (id==0 or the lease was absent):
		// clear the stale attachment record.
		_ = s.detachKeyFromStorage(context.Background(), key)
	}
	// else: a plain Put of a never-leased key — no lease bookkeeping, no write.
}

func (s *RPCServer) bindKeyToLeaseLocked(id int64, key string) []*leaseState {
	// caller holds leaseMu; keep the lock-free lease count in sync (multiple
	// return paths, so update on exit). Closure so len() is read at return, not
	// captured at defer registration.
	defer func() { atomic.StoreInt64(&s.leasedKeyCount, int64(len(s.keyLeaseIndex))) }()
	var changed []*leaseState
	if previousID, ok := s.keyLeaseIndex[key]; ok {
		if previous, exists := s.leases[previousID]; exists {
			delete(previous.keys, key)
			changed = append(changed, previous)
		}
		delete(s.keyLeaseIndex, key)
	}
	if id == 0 {
		return changed
	}
	st, ok := s.leases[id]
	if !ok {
		return changed
	}
	st.keys[key] = struct{}{}
	s.keyLeaseIndex[key] = id
	changed = append(changed, st)
	return changed
}

func (s *RPCServer) unbindKeyFromLease(key string) {
	s.leaseMu.Lock()
	_, wasBound := s.keyLeaseIndex[key]
	if id, ok := s.keyLeaseIndex[key]; ok {
		if st, exists := s.leases[id]; exists {
			delete(st.keys, key)
		}
		delete(s.keyLeaseIndex, key)
	}
	atomic.StoreInt64(&s.leasedKeyCount, int64(len(s.keyLeaseIndex)))
	s.leaseMu.Unlock()
	if wasBound {
		// Drop just this key's attachment record (O(1)), not the whole list (#17).
		_ = s.detachKeyFromStorage(context.Background(), key)
	}
}

func (s *RPCServer) leaseIDForKey(key string) int64 {
	// Fast path: when no key holds a lease, skip the mutex entirely. This is the
	// common case for reads (most ranges cover leaseless keys) and keeps the read
	// hot path from serializing on leaseMu once per returned KeyValue. A key that
	// truly has a committed lease is always counted here, so this never misses one;
	// it only races a concurrent bind, for which returning 0 is acceptable (that
	// write is not ordered before this read).
	if atomic.LoadInt64(&s.leasedKeyCount) == 0 {
		return 0
	}
	s.leaseMu.Lock()
	defer s.leaseMu.Unlock()
	return s.keyLeaseIndex[key]
}

func (s *RPCServer) refreshLease(id int64) (int64, error) {
	s.leaseMu.Lock()
	st, ok := s.leases[id]
	if !ok {
		s.leaseMu.Unlock()
		return 0, leaseNotFound(id)
	}
	st.deadline = time.Now().Add(time.Duration(st.ttl) * time.Second)
	s.scheduleLeaseLocked(st)
	ttl := st.ttl
	s.leaseMu.Unlock()
	// Mirror etcd lessor.Renew -> l.refresh(0): a keepalive only bumps the
	// in-memory deadline and reschedules the expiry timer. It must NOT persist,
	// otherwise every keepalive tick mints a fresh MVCC version, a watch event,
	// and a TSO revision. Durable lease state (grant identity and attached keys)
	// is still persisted on grant/bind/unbind; on restart or leader-change the
	// deadline is reconstructed as now+grantedTTL in applyLeaseRecords.
	return ttl, nil
}

func (s *RPCServer) revokeLease(ctx context.Context, id int64) error {
	// Delete the attached keys BEFORE removing the lease state/record (#36): if a
	// key delete fails we must keep the lease so the keys are not orphaned (no
	// lease left to ever expire them). Only once every bound key is gone is it
	// safe to drop the lease record and its attachment records.
	keys, ok := s.leaseKeysSnapshot(id)
	if !ok {
		return leaseNotFound(id)
	}
	for _, key := range keys {
		if _, err := s.backend.Delete(ctx, []byte(key), 0, false); err != nil {
			return err
		}
	}
	_, err := s.removeLease(id)
	return err
}

func (s *RPCServer) expireLease(id int64) {
	if !s.peers.IsLeader() {
		s.retryLeaseExpiry(id)
		return
	}

	ctx := context.Background()
	keys, ok := s.leaseKeysSnapshot(id)
	if !ok {
		return // already removed
	}
	// Delete bound keys first; a missing key returns no error, so a retry after a
	// partial deletion is safe. On a real storage failure keep the lease and retry
	// rather than swallowing the error and orphaning the surviving keys (#36).
	for _, key := range keys {
		if _, err := s.backend.Delete(ctx, []byte(key), 0, false); err != nil {
			s.metricCli.EmitCounter("lease.expire.delete.err", 1)
			klog.ErrorS(err, "lease expiry: delete of bound key failed; keeping lease for retry", "lease", id, "key", key)
			s.retryLeaseExpiry(id)
			return
		}
	}
	// Every bound key is gone; now drop the lease record and attachment records.
	_, _ = s.removeLease(id)
}

// leaseKeysSnapshot returns a copy of the keys currently attached to lease id
// without removing the lease, so callers can delete the keys first and only then
// tear the lease down (#36).
func (s *RPCServer) leaseKeysSnapshot(id int64) ([]string, bool) {
	s.leaseMu.Lock()
	defer s.leaseMu.Unlock()
	st, ok := s.leases[id]
	if !ok {
		return nil, false
	}
	keys := make([]string, 0, len(st.keys))
	for key := range st.keys {
		keys = append(keys, key)
	}
	return keys, true
}

func (s *RPCServer) retryLeaseExpiry(id int64) {
	s.leaseMu.Lock()
	defer s.leaseMu.Unlock()
	st, ok := s.leases[id]
	if !ok || st.timer == nil {
		return
	}
	st.timer.Reset(leaseExpiryRetryInterval)
}

func (s *RPCServer) removeLease(id int64) ([]string, error) {
	s.leaseMu.Lock()
	st, ok := s.leases[id]
	if !ok {
		s.leaseMu.Unlock()
		return nil, leaseNotFound(id)
	}
	if st.timer != nil {
		st.timer.Stop()
	}
	keys := make([]string, 0, len(st.keys))
	for key := range st.keys {
		keys = append(keys, key)
		delete(s.keyLeaseIndex, key)
	}
	delete(s.leases, id)
	atomic.StoreInt64(&s.leasedKeyCount, int64(len(s.keyLeaseIndex)))
	s.leaseMu.Unlock()

	ctx := context.Background()
	err := s.deleteLeaseState(ctx, id)
	// Remove each key's attachment record so the leasekeys/ keyspace does not leak
	// live records pointing at a now-deleted lease (recovery skips orphans, but they
	// would never be reclaimed otherwise). Same O(keys) order as the object deletes
	// the caller performs on expiry.
	for _, key := range keys {
		_ = s.detachKeyFromStorage(ctx, key)
	}
	return keys, err
}

func (s *RPCServer) keysInDeleteRange(ctx context.Context, r *etcdserverpb.DeleteRangeRequest) ([]string, error) {
	if len(r.RangeEnd) == 0 {
		return []string{string(r.Key)}, nil
	}
	if isEmptyNonFromKeyRange(r.Key, r.RangeEnd) {
		return nil, nil
	}
	resp, err := s.backend.List(ctx, &etcdserverpb.RangeRequest{
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

func (s *RPCServer) requireLeaseLeader(op string) error {
	if s.peers.IsLeader() {
		return nil
	}
	s.metricCli.EmitCounter("lease.follower", 1)
	return status.Errorf(codes.Unavailable, "%s error addr is %s leader %s", op, s.backend.GetResourceLock().Identity(), s.peers.GetLeaderInfo())
}

func (s *RPCServer) restoreLeases(ctx context.Context) error {
	records, attachments, err := s.loadLeaseRecords(ctx)
	if err != nil {
		return err
	}
	// Load into memory only; migration writes happen on leadership (ReloadLeases),
	// since restore runs in New() before this node has necessarily won election.
	s.applyLeaseRecords(records, attachments)
	return nil
}

// ReloadLeases refreshes in-memory lease state from storage. It MUST be called
// when this node acquires leadership: while a follower the node holds a stale
// snapshot (the real leader advanced deadlines via keepalive and granted leases
// this node never saw). Without a reload the new leader would expire
// still-alive leases — deleting their bound keys (e.g. masterleases) — and
// orphan leases granted after this node started.
func (s *RPCServer) ReloadLeases(ctx context.Context) error {
	records, attachments, err := s.loadLeaseRecords(ctx)
	if err != nil {
		return err
	}
	legacy := s.applyLeaseRecords(records, attachments)
	// This node is now the leader; convert any pre-#17 monolithic records to the
	// per-key attachment format so subsequent detaches are durable and the
	// monolithic key-list is not carried forward. One-time, idempotent.
	s.migrateLegacyLeases(ctx, legacy)
	return nil
}

// loadLeaseRecords reads the per-lease meta records and the per-key attachment
// records from storage at the latest revision.
func (s *RPCServer) loadLeaseRecords(ctx context.Context) ([]leaseRecord, map[string]int64, error) {
	resp, err := s.backend.List(ctx, &etcdserverpb.RangeRequest{
		Key:      leaseStoragePrefix,
		RangeEnd: prefixEnd(leaseStoragePrefix),
		Revision: latestRestoreRevision,
	})
	if err != nil {
		return nil, nil, err
	}
	records := make([]leaseRecord, 0, len(resp.Kvs))
	for _, kv := range resp.Kvs {
		var record leaseRecord
		if err := json.Unmarshal(kv.Value, &record); err != nil {
			return nil, nil, err
		}
		records = append(records, record)
	}

	aresp, err := s.backend.List(ctx, &etcdserverpb.RangeRequest{
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
	return records, attachments, nil
}

// migrateLegacyLeases rewrites pre-#17 leases (meta records that still carried an
// inline key list) into the new format: one attachment record per currently-bound
// key plus a keyless meta record. Idempotent; only runs on the leader.
func (s *RPCServer) migrateLegacyLeases(ctx context.Context, ids []int64) {
	for _, id := range ids {
		s.leaseMu.Lock()
		st, ok := s.leases[id]
		var keys []string
		var ttl int64
		if ok {
			ttl = st.ttl
			keys = make([]string, 0, len(st.keys))
			for k := range st.keys {
				keys = append(keys, k)
			}
		}
		s.leaseMu.Unlock()
		if !ok {
			continue
		}
		for _, k := range keys {
			_ = s.attachKeyToStorage(ctx, id, k)
		}
		// Rewrite the meta without the inline key list.
		_ = s.persistLeaseMeta(ctx, id, ttl)
	}
}

// applyLeaseRecords atomically replaces the in-memory lease state from storage:
// it stops existing expiry timers, rebuilds the lease map from the meta records
// and the key->lease bindings from the per-key attachment records, and schedules
// timers from a fresh now+TTL deadline. Any lease whose meta still carried an
// inline key list (a pre-#17 record) is included in the returned slice so the
// leader can migrate it to the new per-key format. Safe on both first restore
// (empty maps) and leadership reload.
func (s *RPCServer) applyLeaseRecords(records []leaseRecord, attachments map[string]int64) (legacy []int64) {
	now := time.Now()
	s.leaseMu.Lock()
	defer s.leaseMu.Unlock()
	for _, st := range s.leases {
		if st.timer != nil {
			st.timer.Stop()
		}
	}
	s.leases = make(map[int64]*leaseState, len(records))
	s.keyLeaseIndex = make(map[string]int64)
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
			s.keyLeaseIndex[key] = st.id
		}
		if len(record.Keys) > 0 {
			legacy = append(legacy, record.ID)
		}
		if st.id > s.leaseID {
			s.leaseID = st.id
		}
		s.leases[st.id] = st
	}
	// Per-key attachment records (#17). Attachments for a lease that no longer has
	// a meta record (revoked, tombstone not yet compacted) are skipped as orphans.
	for key, id := range attachments {
		st, ok := s.leases[id]
		if !ok {
			continue
		}
		st.keys[key] = struct{}{}
		s.keyLeaseIndex[key] = id
	}
	// Schedule timers once all keys are attached.
	for _, st := range s.leases {
		s.scheduleLeaseLocked(st)
		if !st.deadline.After(now) {
			st.timer.Reset(0)
		}
	}
	atomic.StoreInt64(&s.leasedKeyCount, int64(len(s.keyLeaseIndex)))
	return legacy
}

// StopLeases stops every expiry timer and drops the in-memory lease snapshot. It
// MUST be called when this node loses leadership: otherwise a demoted leader
// keeps firing expiry timers (churn, and it would act on a stale snapshot) and
// answers lease reads from state the new leader has since advanced. Leadership
// re-acquisition rebuilds the state from storage via ReloadLeases (#57).
func (s *RPCServer) StopLeases() {
	s.stopLeases()
}

func (s *RPCServer) stopLeases() {
	s.leaseMu.Lock()
	defer s.leaseMu.Unlock()
	for _, st := range s.leases {
		if st.timer != nil {
			st.timer.Stop()
		}
	}
	s.leases = make(map[int64]*leaseState)
	s.keyLeaseIndex = make(map[string]int64)
	atomic.StoreInt64(&s.leasedKeyCount, 0)
}

func (s *RPCServer) scheduleLeaseLocked(st *leaseState) {
	duration := time.Until(st.deadline)
	if duration < 0 {
		duration = 0
	}
	if st.timer == nil {
		st.timer = time.AfterFunc(duration, func() {
			s.expireLease(st.id)
		})
		return
	}
	st.timer.Reset(duration)
}

// persistLeaseMeta writes the small per-lease meta record {id, ttl}. It carries
// no key list (attachments are separate records, #17) and no deadline (recovery
// resets the deadline to now+TTL, see applyLeaseRecords). Written on grant and on
// legacy-record migration.
func (s *RPCServer) persistLeaseMeta(ctx context.Context, id, ttl int64) error {
	data, err := json.Marshal(leaseRecord{ID: id, TTL: ttl})
	if err != nil {
		return err
	}
	_, err = s.backend.Put(ctx, &etcdserverpb.PutRequest{
		Key:   leaseStorageKey(id),
		Value: data,
	})
	return err
}

// attachKeyToStorage records that userKey is attached to lease id as a single
// small record (leasekeys/<userKey> -> <id>). O(1) per attach, replacing the old
// full-key-list rewrite (#17). The record is keyed by the user key, which has at
// most one lease, so a rebind simply overwrites it.
func (s *RPCServer) attachKeyToStorage(ctx context.Context, id int64, userKey string) error {
	_, err := s.backend.Put(ctx, &etcdserverpb.PutRequest{
		Key:   leaseAttachKey(userKey),
		Value: []byte(strconv.FormatInt(id, 10)),
	})
	return err
}

func (s *RPCServer) detachKeyFromStorage(ctx context.Context, userKey string) error {
	_, err := s.backend.DeleteRange(ctx, &etcdserverpb.DeleteRangeRequest{
		Key: leaseAttachKey(userKey),
	})
	return err
}

func (s *RPCServer) deleteLeaseState(ctx context.Context, id int64) error {
	_, err := s.backend.DeleteRange(ctx, &etcdserverpb.DeleteRangeRequest{
		Key: leaseStorageKey(id),
	})
	return err
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
