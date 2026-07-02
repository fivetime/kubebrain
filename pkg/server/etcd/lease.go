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
	"sort"
	"strconv"
	"time"

	"go.etcd.io/etcd/api/v3/etcdserverpb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var leaseStoragePrefix = []byte("\x00kubebrain/leases/")

const leaseExpiryRetryInterval = time.Second
const latestRestoreRevision = int64(^uint64(0) >> 1)
const maxLeaseTTL = int64(9000000000)

type leaseRecord struct {
	ID               int64    `json:"id"`
	TTL              int64    `json:"ttl"`
	DeadlineUnixNano int64    `json:"deadlineUnixNano"`
	Keys             []string `json:"keys"`
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

	if err := s.persistLeaseState(ctx, st); err != nil {
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
	if !s.peers.IsLeader() && s.peers.EtcdProxyEnabled() {
		return s.peers.LeaseTimeToLive(ctx, req)
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
	if !s.peers.IsLeader() && s.peers.EtcdProxyEnabled() {
		return s.peers.LeaseLeases(ctx, req)
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
	states := s.bindKeyToLeaseLocked(id, key)
	s.leaseMu.Unlock()
	for _, st := range states {
		_ = s.persistLeaseState(context.Background(), st)
	}
}

func (s *RPCServer) bindKeyToLeaseLocked(id int64, key string) []*leaseState {
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
	var changed *leaseState
	if id, ok := s.keyLeaseIndex[key]; ok {
		if st, exists := s.leases[id]; exists {
			delete(st.keys, key)
			changed = st
		}
		delete(s.keyLeaseIndex, key)
	}
	s.leaseMu.Unlock()
	if changed != nil {
		_ = s.persistLeaseState(context.Background(), changed)
	}
}

func (s *RPCServer) leaseIDForKey(key string) int64 {
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
	return ttl, s.persistLeaseState(context.Background(), st)
}

func (s *RPCServer) revokeLease(ctx context.Context, id int64) error {
	keys, err := s.removeLease(id)
	if err != nil {
		return err
	}
	for _, key := range keys {
		if _, err := s.backend.Delete(ctx, []byte(key), 0, false); err != nil {
			return err
		}
	}
	return nil
}

func (s *RPCServer) expireLease(id int64) {
	if !s.peers.IsLeader() {
		s.retryLeaseExpiry(id)
		return
	}

	keys, err := s.removeLease(id)
	if err != nil {
		return
	}
	ctx := context.Background()
	for _, key := range keys {
		_, _ = s.backend.Delete(ctx, []byte(key), 0, false)
	}
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
	s.leaseMu.Unlock()
	return keys, s.deleteLeaseState(context.Background(), id)
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
	records, err := s.loadLeaseRecords(ctx)
	if err != nil {
		return err
	}
	s.applyLeaseRecords(records)
	return nil
}

// ReloadLeases refreshes in-memory lease state from storage. It MUST be called
// when this node acquires leadership: while a follower the node holds a stale
// snapshot (the real leader advanced deadlines via keepalive and granted leases
// this node never saw). Without a reload the new leader would expire
// still-alive leases — deleting their bound keys (e.g. masterleases) — and
// orphan leases granted after this node started.
func (s *RPCServer) ReloadLeases(ctx context.Context) error {
	records, err := s.loadLeaseRecords(ctx)
	if err != nil {
		return err
	}
	s.applyLeaseRecords(records)
	return nil
}

func (s *RPCServer) loadLeaseRecords(ctx context.Context) ([]leaseRecord, error) {
	resp, err := s.backend.List(ctx, &etcdserverpb.RangeRequest{
		Key:      leaseStoragePrefix,
		RangeEnd: prefixEnd(leaseStoragePrefix),
		Revision: latestRestoreRevision,
	})
	if err != nil {
		return nil, err
	}
	records := make([]leaseRecord, 0, len(resp.Kvs))
	for _, kv := range resp.Kvs {
		var record leaseRecord
		if err := json.Unmarshal(kv.Value, &record); err != nil {
			return nil, err
		}
		records = append(records, record)
	}
	return records, nil
}

// applyLeaseRecords atomically replaces the in-memory lease state with records
// loaded from storage: it stops any existing expiry timers, rebuilds the maps,
// and re-schedules timers from the freshly-read deadlines. Safe on both first
// restore (empty maps) and leadership reload.
func (s *RPCServer) applyLeaseRecords(records []leaseRecord) {
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
			id:       record.ID,
			ttl:      record.TTL,
			deadline: time.Unix(0, record.DeadlineUnixNano),
			keys:     make(map[string]struct{}, len(record.Keys)),
		}
		for _, key := range record.Keys {
			st.keys[key] = struct{}{}
			s.keyLeaseIndex[key] = st.id
		}
		if st.id > s.leaseID {
			s.leaseID = st.id
		}
		s.scheduleLeaseLocked(st)
		if !st.deadline.After(now) {
			st.timer.Reset(0)
		}
		s.leases[st.id] = st
	}
}

func (s *RPCServer) stopLeases() {
	s.leaseMu.Lock()
	defer s.leaseMu.Unlock()
	for _, st := range s.leases {
		if st.timer != nil {
			st.timer.Stop()
		}
	}
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

func (s *RPCServer) persistLeaseState(ctx context.Context, st *leaseState) error {
	s.leaseMu.Lock()
	record := leaseRecord{
		ID:               st.id,
		TTL:              st.ttl,
		DeadlineUnixNano: st.deadline.UnixNano(),
		Keys:             make([]string, 0, len(st.keys)),
	}
	for key := range st.keys {
		record.Keys = append(record.Keys, key)
	}
	sort.Strings(record.Keys)
	s.leaseMu.Unlock()

	data, err := json.Marshal(record)
	if err != nil {
		return err
	}
	_, err = s.backend.Put(ctx, &etcdserverpb.PutRequest{
		Key:   leaseStorageKey(st.id),
		Value: data,
	})
	return err
}

func (s *RPCServer) deleteLeaseState(ctx context.Context, id int64) error {
	_, err := s.backend.DeleteRange(ctx, &etcdserverpb.DeleteRangeRequest{
		Key: leaseStorageKey(id),
	})
	return err
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
