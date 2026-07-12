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
	"bytes"
	"context"
	"errors"
	"fmt"
	"time"

	"go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/mvccpb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"k8s.io/klog/v2"

	"github.com/kubewharf/kubebrain/pkg/backend"
	"github.com/kubewharf/kubebrain/pkg/metrics"
)

const (
	// GetPartitionMagic is a sentinel ModRevision the range-stream (List) path sets
	// on its EOF event. It is NOT overloaded onto the Range API any more: a Range
	// at revision 1888 now gets normal etcd semantics instead of being hijacked to
	// return partition metadata (#53). Partition discovery uses the brain-protocol
	// ListPartition RPC.
	GetPartitionMagic int64 = 1888
	unaryRpcTimeout         = 10 * time.Second

	compactRevKey    = "compact_rev_key"
	defaultMaxTxnOps = 128
)

func (s *RPCServer) Range(ctx context.Context, r *etcdserverpb.RangeRequest) (*etcdserverpb.RangeResponse, error) {
	startTime := time.Now()
	klog.V(4).InfoS("RANGE", "key", r.Key, "rangeEnd", r.RangeEnd, "countOnly", r.CountOnly)
	if err := validateRangeRequest(r); err != nil {
		return nil, err
	}
	if r.Revision > 0 && !s.peers.IsLeader() && s.peers.EtcdProxyEnabled() {
		s.metricCli.EmitCounter("read.follower.historical_proxy", 1)
		return s.peers.Range(ctx, r)
	}
	if err := s.peers.SyncReadRevision(ctx); err != nil {
		return &etcdserverpb.RangeResponse{}, err
	}
	if err := s.checkRequestedRevision(ctx, r.Revision); err != nil {
		return nil, err
	}
	if isEmptyNonFromKeyRange(r.Key, r.RangeEnd) {
		revision, err := safeBackendRevision(ctx, s.backend)
		if err != nil {
			return nil, err
		}
		return &etcdserverpb.RangeResponse{
			Header: txnHeader(int64(revision)),
		}, nil
	}
	var (
		response              *etcdserverpb.RangeResponse
		err                   error
		methodTag, successTag metrics.T
	)
	if len(r.RangeEnd) == 0 {
		// get method
		response, err = s.backend.Get(ctx, r)
		methodTag = metrics.Tag("method", "get")
	} else if r.CountOnly && !hasRangeRevisionFilters(r) {
		// Count only. Count honors r.Revision: for the current revision (or a
		// historical one the count index can serve) it returns a count with no range
		// materialization; a historical count the index cannot serve falls back
		// inside Count to a revision-honoring range read, so the count reflects that
		// revision, not the current one.
		methodTag = metrics.Tag("method", "count")
		response, err = s.backend.Count(ctx, r)
	} else {
		methodTag = metrics.Tag("method", "range")
		response, err = s.backend.List(ctx, r)
	}
	successTag = getSuccessMetricTagByErr(err)
	s.metricCli.EmitCounter("read", 1, methodTag, successTag, errClassTag(err))
	s.metricCli.EmitHistogram("read.latency", time.Since(startTime).Seconds(), methodTag, successTag)
	if response != nil {
		s.metricCli.EmitHistogram("read.responsesize", response.Size(), methodTag, successTag)
	}
	return response, err
}

func validateRangeRequest(r *etcdserverpb.RangeRequest) error {
	if len(r.Key) == 0 {
		return status.Error(codes.InvalidArgument, "etcdserver: key is not provided")
	}
	if _, ok := etcdserverpb.RangeRequest_SortOrder_name[int32(r.SortOrder)]; !ok {
		return status.Error(codes.InvalidArgument, "etcdserver: invalid sort option")
	}
	if _, ok := etcdserverpb.RangeRequest_SortTarget_name[int32(r.SortTarget)]; !ok {
		return status.Error(codes.InvalidArgument, "etcdserver: invalid sort option")
	}
	return nil
}

func isEmptyNonFromKeyRange(key, rangeEnd []byte) bool {
	return len(rangeEnd) != 0 && !isFromKeyRangeEnd(rangeEnd) && bytes.Compare(key, rangeEnd) >= 0
}

func isFromKeyRangeEnd(rangeEnd []byte) bool {
	return len(rangeEnd) == 1 && rangeEnd[0] == 0
}

func (s *RPCServer) Txn(ctx context.Context, txn *etcdserverpb.TxnRequest) (*etcdserverpb.TxnResponse, error) {
	startTime := time.Now()

	if err := validateTxnRequest(txn); err != nil {
		return nil, err
	}

	deadline, ok := ctx.Deadline()
	if ok && startTime.Sub(deadline) >= 0 {
		return nil, context.DeadlineExceeded
	}
	ctx, cancel := context.WithTimeout(ctx, unaryRpcTimeout)
	defer cancel()

	// only leader can accept and handle write request
	// return error includes current leader, help etcd client send request to right instance
	// Capture the leadership epoch at admission and thread it through the context;
	// the backend re-checks it just before commit so a leadership change mid-write
	// fences the commit instead of losing it silently (FINDING #39).
	epoch, leadingFresh := s.peers.EpochAndLeadingFresh()
	if !leadingFresh {
		s.metricCli.EmitCounter("write.follower", 1)
		if s.peers.EtcdProxyEnabled() {
			return s.peers.Txn(ctx, txn)
		}
		return nil, status.Errorf(codes.Unavailable, "txn error addr is %s leader %s", s.backend.GetResourceLock().Identity(), s.backend.GetResourceLock().Describe())
	}
	ctx = backend.WithLeadershipEpoch(ctx, epoch)
	var (
		err                   error
		response              *etcdserverpb.TxnResponse
		methodTag, successTag metrics.T
		failedKey             string
	)
	if put, includeFailureRange := isCreate(txn); put != nil {
		put, err = s.putWithEffectiveOptions(ctx, put)
		if err != nil {
			return nil, err
		}
		if err = s.ensureLeaseExists(put.Lease); err != nil {
			return nil, err
		}
		response, err = s.backend.Create(ctx, put, includeFailureRange)
		methodTag = metrics.Tag("method", "create")
		if err != nil || !response.Succeeded {
			failedKey = string(put.Key)
		} else {
			s.bindKeyToLease(put.Lease, string(put.Key))
		}
	} else if rev, key, deleteReq, includeFailureRange, ok := isCompareDelete(txn); ok {
		response, err = s.backend.CompareDelete(ctx, deleteReq, rev, includeFailureRange)
		methodTag = metrics.Tag("method", "delete")
		if err != nil || !response.Succeeded {
			failedKey = string(key)
		} else {
			s.unbindKeyFromLease(string(key))
		}
	} else if rev, key, includeFailureRange, ok := isDelete(txn); ok {
		response, err = s.backend.Delete(ctx, key, rev, includeFailureRange)
		methodTag = metrics.Tag("method", "delete")
		if err != nil || !response.Succeeded {
			failedKey = string(key)
		} else {
			s.unbindKeyFromLease(string(key))
		}
	} else if rev, key, put, includeFailureRange, ok := isUpdate(txn); ok {
		put, err = s.putWithEffectiveOptions(ctx, put)
		if err != nil {
			return nil, err
		}
		if err = s.ensureLeaseExists(put.Lease); err != nil {
			return nil, err
		}
		response, err = s.backend.Update(ctx, rev, put, includeFailureRange)
		methodTag = metrics.Tag("method", "update")
		if err != nil || !response.Succeeded {
			failedKey = string(key)
		} else {
			s.bindKeyToLease(put.Lease, string(key))
		}
	} else if ok := isCompact(txn); ok {
		response, err = s.compact(ctx, txn)
		methodTag = metrics.Tag("method", "compact")
	} else if isSimpleSuccessTxn(txn) {
		response, err = s.executeGenericTxn(ctx, txn)
		methodTag = metrics.Tag("method", "txn-simple")
	} else if isComparableTxn(txn) {
		response, err = s.executeGenericTxn(ctx, txn)
		methodTag = metrics.Tag("method", "txn-compare")
	} else {
		response, err = nil, fmt.Errorf("unsupported transaction: %v", txn)
		methodTag = metrics.Tag("method", "invalid")
	}
	// emit metric
	successTag = getSuccessMetricTagByErr(err)
	s.metricCli.EmitCounter("write", 1, methodTag, successTag, errClassTag(err))
	s.metricCli.EmitHistogram("write.latency", time.Since(startTime).Seconds(), methodTag, successTag)
	if response != nil {
		s.metricCli.EmitHistogram("write.responsesize", response.Size(), methodTag, successTag)
		if !response.Succeeded {
			s.metricCli.EmitCounter("write.fail", 1, methodTag)
		}
	}
	if len(failedKey) > 0 {
		if err != nil {
			klog.ErrorS(err, "txn failed", "op", methodTag.Value, "key", failedKey)
		} else {
			klog.V(4).InfoS("txn compare failed", "op", methodTag.Value, "key", failedKey)
		}
	}
	return response, mapFenceErr(err)
}

func validateTxnRequest(txn *etcdserverpb.TxnRequest) error {
	return validateTxnRequestWithMaxOps(txn, defaultMaxTxnOps)
}

func validateTxnRequestWithMaxOps(txn *etcdserverpb.TxnRequest, maxTxnOps int) error {
	opc := len(txn.Compare)
	if opc < len(txn.Success) {
		opc = len(txn.Success)
	}
	if opc < len(txn.Failure) {
		opc = len(txn.Failure)
	}
	if opc > maxTxnOps {
		return tooManyTxnOpsError()
	}

	for _, cmp := range txn.Compare {
		if len(cmp.Key) == 0 {
			return status.Error(codes.InvalidArgument, "etcdserver: key is not provided")
		}
		if _, ok := etcdserverpb.Compare_CompareResult_name[int32(cmp.Result)]; !ok {
			return status.Error(codes.InvalidArgument, "etcdserver: invalid compare result")
		}
		if _, ok := etcdserverpb.Compare_CompareTarget_name[int32(cmp.Target)]; !ok {
			return status.Error(codes.InvalidArgument, "etcdserver: invalid compare target")
		}
	}
	for _, op := range txn.Success {
		if err := validateTxnRequestOp(op, maxTxnOps-opc); err != nil {
			return err
		}
	}
	if err := validateTxnIntervals(txn.Success); err != nil {
		return err
	}
	for _, op := range txn.Failure {
		if err := validateTxnRequestOp(op, maxTxnOps-opc); err != nil {
			return err
		}
	}
	if err := validateTxnIntervals(txn.Failure); err != nil {
		return err
	}
	return nil
}

func validateTxnRequestOp(op *etcdserverpb.RequestOp, maxTxnOps int) error {
	if op == nil {
		return txnKeyNotFoundError()
	}
	switch {
	case op.GetRequestPut() != nil:
		return validatePutRequest(op.GetRequestPut())
	case op.GetRequestRange() != nil:
		return validateRangeRequest(op.GetRequestRange())
	case op.GetRequestDeleteRange() != nil:
		return validateDeleteRangeRequest(op.GetRequestDeleteRange())
	case op.GetRequestTxn() != nil:
		return validateTxnRequestWithMaxOps(op.GetRequestTxn(), maxTxnOps)
	default:
		return txnKeyNotFoundError()
	}
}

type txnDeleteInterval struct {
	start     []byte
	end       []byte
	point     bool
	openEnded bool
}

func validateTxnIntervals(ops []*etcdserverpb.RequestOp) error {
	_, _, err := collectTxnIntervals(ops)
	return err
}

func collectTxnIntervals(ops []*etcdserverpb.RequestOp) (map[string]struct{}, []txnDeleteInterval, error) {
	deleteIntervals := make([]txnDeleteInterval, 0)
	for _, op := range ops {
		if r := op.GetRequestDeleteRange(); r != nil {
			deleteIntervals = append(deleteIntervals, newTxnDeleteInterval(r))
		}
	}

	putKeys := make(map[string]struct{})
	for _, op := range ops {
		if nested := op.GetRequestTxn(); nested != nil {
			thenPuts, thenDeletes, err := collectTxnIntervals(nested.Success)
			if err != nil {
				return nil, deleteIntervals, err
			}
			elsePuts, elseDeletes, err := collectTxnIntervals(nested.Failure)
			if err != nil {
				return nil, deleteIntervals, err
			}
			for key := range thenPuts {
				if _, ok := putKeys[key]; ok {
					return nil, deleteIntervals, duplicateTxnKeyError()
				}
				if txnIntervalsContain(deleteIntervals, []byte(key)) {
					return nil, deleteIntervals, duplicateTxnKeyError()
				}
				putKeys[key] = struct{}{}
			}
			for key := range elsePuts {
				if _, ok := putKeys[key]; ok {
					if _, safe := thenPuts[key]; !safe {
						return nil, deleteIntervals, duplicateTxnKeyError()
					}
				}
				if txnIntervalsContain(deleteIntervals, []byte(key)) {
					return nil, deleteIntervals, duplicateTxnKeyError()
				}
				putKeys[key] = struct{}{}
			}
			deleteIntervals = append(deleteIntervals, thenDeletes...)
			deleteIntervals = append(deleteIntervals, elseDeletes...)
			continue
		}

		r := op.GetRequestPut()
		if r == nil {
			continue
		}
		key := string(r.Key)
		if _, ok := putKeys[key]; ok {
			return nil, deleteIntervals, duplicateTxnKeyError()
		}
		if txnIntervalsContain(deleteIntervals, r.Key) {
			return nil, deleteIntervals, duplicateTxnKeyError()
		}
		putKeys[key] = struct{}{}
	}
	return putKeys, deleteIntervals, nil
}

func txnIntervalsContain(intervals []txnDeleteInterval, key []byte) bool {
	for _, interval := range intervals {
		if interval.contains(key) {
			return true
		}
	}
	return false
}

func newTxnDeleteInterval(r *etcdserverpb.DeleteRangeRequest) txnDeleteInterval {
	if len(r.RangeEnd) == 0 {
		return txnDeleteInterval{start: r.Key, point: true}
	}
	return txnDeleteInterval{
		start:     r.Key,
		end:       r.RangeEnd,
		openEnded: len(r.RangeEnd) == 1 && r.RangeEnd[0] == 0,
	}
}

func (i txnDeleteInterval) contains(key []byte) bool {
	if i.point {
		return bytes.Equal(key, i.start)
	}
	if bytes.Compare(key, i.start) < 0 {
		return false
	}
	return i.openEnded || bytes.Compare(key, i.end) < 0
}

func duplicateTxnKeyError() error {
	return status.Error(codes.InvalidArgument, "etcdserver: duplicate key given in txn request")
}

func tooManyTxnOpsError() error {
	return status.Error(codes.InvalidArgument, "etcdserver: too many operations in txn request")
}

func txnKeyNotFoundError() error {
	return status.Error(codes.InvalidArgument, "etcdserver: key not found")
}

func futureRevisionError() error {
	return status.Error(codes.OutOfRange, "etcdserver: mvcc: required revision is a future revision")
}

func compactedRevisionError() error {
	return status.Error(codes.OutOfRange, "etcdserver: mvcc: required revision has been compacted")
}

func (s *RPCServer) checkRequestedRevision(ctx context.Context, revision int64) error {
	if revision <= 0 {
		return nil
	}
	compactRevision, err := s.backend.GetCompactRevision(ctx)
	if err != nil {
		return err
	}
	if revision < int64(compactRevision) {
		return compactedRevisionError()
	}
	if revision > int64(s.backend.GetCurrentRevision()) {
		return futureRevisionError()
	}
	return nil
}

func validatePutRequest(r *etcdserverpb.PutRequest) error {
	if len(r.Key) == 0 {
		return status.Error(codes.InvalidArgument, "etcdserver: key is not provided")
	}
	if r.IgnoreValue && len(r.Value) != 0 {
		return status.Error(codes.InvalidArgument, "etcdserver: value is provided")
	}
	if r.IgnoreLease && r.Lease != 0 {
		return status.Error(codes.InvalidArgument, "etcdserver: lease is provided")
	}
	return nil
}

func validateDeleteRangeRequest(r *etcdserverpb.DeleteRangeRequest) error {
	if len(r.Key) == 0 {
		return status.Error(codes.InvalidArgument, "etcdserver: key is not provided")
	}
	return nil
}

func (s *RPCServer) Compact(ctx context.Context, r *etcdserverpb.CompactionRequest) (*etcdserverpb.CompactionResponse, error) {
	if !s.peers.IsLeader() {
		s.metricCli.EmitCounter("write.follower", 1)
		if s.peers.EtcdProxyEnabled() {
			return s.peers.Compact(ctx, r)
		}
		return nil, status.Errorf(codes.Unavailable, "compact error addr is %s leader %s", s.backend.GetResourceLock().Identity(), s.backend.GetResourceLock().Describe())
	}
	if err := s.peers.SyncReadRevision(ctx); err != nil {
		return nil, err
	}
	if r.Revision > int64(s.backend.GetCurrentRevision()) {
		return nil, futureRevisionError()
	}
	compactRevision, err := s.backend.GetCompactRevision(ctx)
	if err != nil {
		return nil, err
	}
	if compactRevision > 0 && r.Revision <= int64(compactRevision) {
		return nil, compactedRevisionError()
	}
	// The apiserver's compaction loop sends Physical=false and only needs the
	// logical watermark advanced; run the physical GC in the background so a large
	// backlog cannot exceed the caller's RPC timeout. Physical=true callers
	// (e.g. etcdctl compact --physical) still block until the scan completes.
	var compactResp *etcdserverpb.TxnResponse
	if r.Physical {
		compactResp, err = s.backend.Compact(ctx, uint64(r.Revision))
	} else {
		compactResp, err = s.backend.CompactAsync(ctx, uint64(r.Revision))
	}
	if err != nil {
		return nil, err
	}
	if r.Revision > 0 && compactResp != nil && compactResp.Header != nil && compactResp.Header.Revision < r.Revision {
		return nil, status.Errorf(codes.Unavailable, "etcdserver: mvcc: compact revision %d is pending behind requested revision %d", compactResp.Header.Revision, r.Revision)
	}
	if r.Revision > 0 {
		if err := s.waitCompactRevisionVisible(ctx, r.Revision); err != nil {
			return nil, err
		}
	}
	return &etcdserverpb.CompactionResponse{
		Header: &etcdserverpb.ResponseHeader{
			Revision: int64(s.backend.GetCurrentRevision()),
		},
	}, nil
}

func (s *RPCServer) waitCompactRevisionVisible(ctx context.Context, revision int64) error {
	deadline := time.Now().Add(unaryRpcTimeout)
	for {
		compactRevision, err := s.backend.GetCompactRevision(ctx)
		if err != nil {
			return err
		}
		if int64(compactRevision) >= revision {
			return nil
		}
		if time.Now().After(deadline) {
			return status.Errorf(codes.Unavailable, "etcdserver: mvcc: compact revision %d is not visible at requested revision %d", compactRevision, revision)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(50 * time.Millisecond):
		}
	}
}

func (s *RPCServer) Put(ctx context.Context, r *etcdserverpb.PutRequest) (*etcdserverpb.PutResponse, error) {
	startTime := time.Now()
	if err := validatePutRequest(r); err != nil {
		return nil, err
	}
	epoch, leadingFresh := s.peers.EpochAndLeadingFresh()
	if !leadingFresh {
		s.metricCli.EmitCounter("write.follower", 1)
		if s.peers.EtcdProxyEnabled() {
			return s.peers.Put(ctx, r)
		}
		return nil, status.Errorf(codes.Unavailable, "put error addr is %s leader %s", s.backend.GetResourceLock().Identity(), s.backend.GetResourceLock().Describe())
	}
	ctx = backend.WithLeadershipEpoch(ctx, epoch)
	put, err := s.putWithEffectiveOptions(ctx, r)
	if err != nil {
		return nil, err
	}
	if err := s.ensureLeaseExists(put.Lease); err != nil {
		return nil, err
	}
	// When a lease is involved (the new put binds one, or the key currently holds
	// one), write the value and its lease attachment record atomically so the
	// binding can never be lost independently of the value (review #2). The common
	// leaseless put keeps the cheap single-write path.
	var response *etcdserverpb.PutResponse
	if prevLease := s.leaseIDForKey(string(put.Key)); put.Lease != 0 || prevLease != 0 {
		response, err = s.putLeasedAtomic(ctx, put, prevLease)
	} else {
		response, err = s.backend.Put(ctx, put)
	}
	successTag := getSuccessMetricTagByErr(err)
	s.metricCli.EmitCounter("write", 1, metrics.Tag("method", "put"), successTag, errClassTag(err))
	s.metricCli.EmitHistogram("write.latency", time.Since(startTime).Seconds(), metrics.Tag("method", "put"), successTag)
	if response != nil {
		s.metricCli.EmitHistogram("write.responsesize", response.Size(), metrics.Tag("method", "put"), successTag)
	}
	return response, mapFenceErr(err)
}

func (s *RPCServer) DeleteRange(ctx context.Context, r *etcdserverpb.DeleteRangeRequest) (*etcdserverpb.DeleteRangeResponse, error) {
	startTime := time.Now()
	if err := validateDeleteRangeRequest(r); err != nil {
		return nil, err
	}
	epoch, leadingFresh := s.peers.EpochAndLeadingFresh()
	if !leadingFresh {
		s.metricCli.EmitCounter("write.follower", 1)
		if s.peers.EtcdProxyEnabled() {
			return s.peers.DeleteRange(ctx, r)
		}
		return nil, status.Errorf(codes.Unavailable, "delete range error addr is %s leader %s", s.backend.GetResourceLock().Identity(), s.backend.GetResourceLock().Describe())
	}
	ctx = backend.WithLeadershipEpoch(ctx, epoch)
	if len(r.RangeEnd) != 0 {
		if err := s.peers.SyncReadRevision(ctx); err != nil {
			return nil, err
		}
	}
	if isEmptyNonFromKeyRange(r.Key, r.RangeEnd) {
		return s.emptyDeleteRangeResponse(), nil
	}
	deletedKeys, keyErr := s.keysInDeleteRange(ctx, r)
	if keyErr != nil {
		return nil, keyErr
	}
	response, err := s.backend.DeleteRange(ctx, r)
	if err == nil {
		for _, key := range deletedKeys {
			s.unbindKeyFromLease(key)
		}
	}
	successTag := getSuccessMetricTagByErr(err)
	s.metricCli.EmitCounter("write", 1, metrics.Tag("method", "delete-range"), successTag, errClassTag(err))
	s.metricCli.EmitHistogram("write.latency", time.Since(startTime).Seconds(), metrics.Tag("method", "delete-range"), successTag)
	if response != nil {
		s.metricCli.EmitHistogram("write.responsesize", response.Size(), metrics.Tag("method", "delete-range"), successTag)
	}
	return response, mapFenceErr(err)
}

func (s *RPCServer) putWithEffectiveOptions(ctx context.Context, r *etcdserverpb.PutRequest) (*etcdserverpb.PutRequest, error) {
	if !r.IgnoreLease && !r.IgnoreValue {
		return r, nil
	}
	clone := *r
	if r.IgnoreLease {
		clone.IgnoreLease = false
		clone.Lease = s.leaseIDForKey(string(r.Key))
	}
	if r.IgnoreValue {
		rangeResp, err := s.backend.Get(ctx, &etcdserverpb.RangeRequest{Key: r.Key})
		if err != nil {
			return nil, err
		}
		if len(rangeResp.Kvs) == 0 {
			return nil, status.Errorf(codes.NotFound, "ignore value requires existing key %q", string(r.Key))
		}
		clone.IgnoreValue = false
		clone.Value = rangeResp.Kvs[0].Value
	}
	return &clone, nil
}

func isCreate(txn *etcdserverpb.TxnRequest) (*etcdserverpb.PutRequest, bool) {
	if len(txn.Compare) == 1 &&
		txn.Compare[0].Target == etcdserverpb.Compare_MOD &&
		txn.Compare[0].Result == etcdserverpb.Compare_EQUAL &&
		txn.Compare[0].GetModRevision() == 0 &&
		(len(txn.Failure) == 0 || (len(txn.Failure) == 1 && txn.Failure[0].GetRequestRange() != nil)) &&
		len(txn.Success) == 1 &&
		txn.Success[0].GetRequestPut() != nil {
		return txn.Success[0].GetRequestPut(), len(txn.Failure) == 1
	}
	return nil, false
}

func isDelete(txn *etcdserverpb.TxnRequest) (int64, []byte, bool, bool) {
	if len(txn.Compare) == 0 &&
		len(txn.Failure) == 0 &&
		len(txn.Success) == 2 &&
		txn.Success[0].GetRequestRange() != nil &&
		txn.Success[1].GetRequestDeleteRange() != nil {
		rng := txn.Success[1].GetRequestDeleteRange()
		if len(rng.RangeEnd) == 0 {
			return 0, rng.Key, true, true
		}
	}
	return 0, nil, false, false
}

func isCompareDelete(txn *etcdserverpb.TxnRequest) (int64, []byte, *etcdserverpb.DeleteRangeRequest, bool, bool) {
	if len(txn.Compare) == 1 &&
		txn.Compare[0].Target == etcdserverpb.Compare_MOD &&
		txn.Compare[0].Result == etcdserverpb.Compare_EQUAL &&
		(len(txn.Failure) == 0 || (len(txn.Failure) == 1 && txn.Failure[0].GetRequestRange() != nil)) &&
		len(txn.Success) == 1 &&
		txn.Success[0].GetRequestDeleteRange() != nil {
		deleteReq := txn.Success[0].GetRequestDeleteRange()
		if len(deleteReq.RangeEnd) != 0 {
			return 0, nil, nil, false, false
		}
		return txn.Compare[0].GetModRevision(),
			deleteReq.Key,
			deleteReq,
			len(txn.Failure) == 1,
			true
	}
	return 0, nil, nil, false, false
}

func isUpdate(txn *etcdserverpb.TxnRequest) (int64, []byte, *etcdserverpb.PutRequest, bool, bool) {
	if len(txn.Compare) == 1 &&
		txn.Compare[0].Target == etcdserverpb.Compare_MOD &&
		txn.Compare[0].Result == etcdserverpb.Compare_EQUAL &&
		len(txn.Success) == 1 &&
		txn.Success[0].GetRequestPut() != nil &&
		(len(txn.Failure) == 0 || (len(txn.Failure) == 1 && txn.Failure[0].GetRequestRange() != nil)) {
		return txn.Compare[0].GetModRevision(),
			txn.Compare[0].Key,
			txn.Success[0].GetRequestPut(),
			len(txn.Failure) == 1,
			true
	}
	return 0, nil, nil, false, false
}

func isSimpleSuccessTxn(txn *etcdserverpb.TxnRequest) bool {
	if len(txn.Compare) != 0 || len(txn.Failure) != 0 {
		return false
	}
	return txnOpsSupported(txn.Success)
}

func isComparableTxn(txn *etcdserverpb.TxnRequest) bool {
	return len(txn.Compare) > 0 && txnOpsSupported(txn.Success) && txnOpsSupported(txn.Failure)
}

func txnOpsSupported(ops []*etcdserverpb.RequestOp) bool {
	for _, op := range ops {
		if op.GetRequestPut() == nil &&
			op.GetRequestRange() == nil &&
			op.GetRequestDeleteRange() == nil &&
			op.GetRequestTxn() == nil {
			return false
		}
		if txn := op.GetRequestTxn(); txn != nil && (!txnOpsSupported(txn.Success) || !txnOpsSupported(txn.Failure)) {
			return false
		}
	}
	return true
}

func (s *RPCServer) executeGenericTxn(ctx context.Context, txn *etcdserverpb.TxnRequest) (*etcdserverpb.TxnResponse, error) {
	deadline := time.Now().Add(unaryRpcTimeout)
	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if time.Now().After(deadline) {
			return nil, status.Errorf(codes.Unavailable, "txn still contended after %s", unaryRpcTimeout)
		}
		paths, guards, err := s.txnComparePathsGuarded(ctx, txn)
		if err != nil {
			return nil, err
		}
		compactRevision, err := s.backend.GetCompactRevision(ctx)
		if err != nil {
			return nil, err
		}
		if err := validateTxnRangeRevisions(txn, paths, int64(compactRevision), int64(s.backend.GetCurrentRevision())); err != nil {
			return nil, err
		}
		// Prefer the atomic single-revision path when the chosen branch is a set of
		// distinct-key writes (#4). The compare guards make it serializable: a guard
		// conflict means a compared key changed, so re-evaluate the compares and
		// retry. Ineligible shapes fall back to the (unchanged) sequential path.
		resp, handled, err := s.tryAtomicGenericTxn(ctx, txn, paths[0], guards)
		if errors.Is(err, backend.ErrTxnGuardConflict) {
			continue
		}
		if handled {
			return resp, err
		}
		return s.executeTxnWithCursor(ctx, txn, &txnPathCursor{paths: paths})
	}
}

// tryAtomicGenericTxn applies the chosen path atomically when it consists solely
// of distinct-key Put and single-key DeleteRange ops (>= 2 of them, since a
// single op is already atomic on the sequential path). handled=false means the
// txn shape is ineligible and the caller must use the sequential fallback.
func (s *RPCServer) tryAtomicGenericTxn(ctx context.Context, txn *etcdserverpb.TxnRequest, succeeded bool, guards []backend.TxnGuard) (*etcdserverpb.TxnResponse, bool, error) {
	ops := txn.Success
	if !succeeded {
		ops = txn.Failure
	}
	writeOps := make([]backend.TxnWriteOp, 0, len(ops))
	prevKv := make([]bool, 0, len(ops))
	seen := make(map[string]struct{}, len(ops))
	for _, op := range ops {
		switch {
		case op.GetRequestPut() != nil:
			put := op.GetRequestPut()
			// IgnoreLease/IgnoreValue need a read-modify step the atomic batch
			// does not model; leave those to the sequential path.
			if put.IgnoreLease || put.IgnoreValue {
				return nil, false, nil
			}
			if _, dup := seen[string(put.Key)]; dup {
				return nil, false, nil
			}
			seen[string(put.Key)] = struct{}{}
			// Inline the lease per-version (review #9). The attachment record on this
			// path stays best-effort via bindKeyToLease below — leased keys in a
			// multi-op generic txn (as opposed to a plain Put) are rare; the
			// expiry-time guard in deleteLeasedKey neutralizes a lost attachment.
			writeOps = append(writeOps, backend.TxnWriteOp{Key: put.Key, Value: put.Value, Lease: put.Lease})
			prevKv = append(prevKv, false)
		case op.GetRequestDeleteRange() != nil:
			del := op.GetRequestDeleteRange()
			if len(del.RangeEnd) != 0 { // multi-key range delete
				return nil, false, nil
			}
			if _, dup := seen[string(del.Key)]; dup {
				return nil, false, nil
			}
			seen[string(del.Key)] = struct{}{}
			writeOps = append(writeOps, backend.TxnWriteOp{Delete: true, Key: del.Key})
			prevKv = append(prevKv, del.PrevKv)
		default: // range read, nested txn, or unsupported op
			return nil, false, nil
		}
	}
	if len(writeOps) < 2 {
		return nil, false, nil
	}
	// A compared key that is also written is guarded by its own write CAS, but its
	// guard revision (from the compare read) and its write's expected revision
	// (from TxnApply's pre-read) could diverge; rather than reconcile that, drop
	// to the sequential path when compare and write keys overlap.
	applyGuards := guards[:0:0]
	for _, g := range guards {
		if _, written := seen[string(g.Key)]; written {
			return nil, false, nil
		}
		applyGuards = append(applyGuards, g)
	}
	// Validate all put leases up front: an atomic txn must reject as a whole if a
	// referenced lease is missing, never apply a prefix of its writes.
	for _, op := range ops {
		if put := op.GetRequestPut(); put != nil {
			if err := s.ensureLeaseExists(put.Lease); err != nil {
				return nil, true, err
			}
		}
	}

	responses, rev, results, err := s.backend.TxnApply(ctx, writeOps, applyGuards, prevKv)
	if err != nil {
		return nil, true, err
	}
	// Bind/unbind leases now that the writes committed.
	for i, op := range ops {
		if put := op.GetRequestPut(); put != nil {
			s.bindKeyToLease(put.Lease, string(put.Key))
		} else if del := op.GetRequestDeleteRange(); del != nil {
			if results[i].Deleted {
				s.unbindKeyFromLease(string(del.Key))
			}
		}
	}
	return &etcdserverpb.TxnResponse{
		Succeeded: succeeded,
		Header:    txnHeader(int64(rev)),
		Responses: responses,
	}, true, nil
}

func (s *RPCServer) txnComparePaths(ctx context.Context, txn *etcdserverpb.TxnRequest) ([]bool, error) {
	paths, _, err := s.txnComparePathsGuarded(ctx, txn)
	return paths, err
}

// txnComparePathsGuarded is txnComparePaths that additionally returns OCC guards
// for the top-level compares it evaluated (those that decided this txn's branch).
// The guards are used only by the atomic path; nested compares are not guarded
// (a nested txn is never atomic-eligible and falls back to the sequential path).
func (s *RPCServer) txnComparePathsGuarded(ctx context.Context, txn *etcdserverpb.TxnRequest) ([]bool, []backend.TxnGuard, error) {
	succeeded := true
	var guards []backend.TxnGuard
	for _, cmp := range txn.Compare {
		ok, guard, err := s.evalCompareGuarded(ctx, cmp)
		if err != nil {
			return nil, nil, err
		}
		if guard != nil {
			guards = append(guards, *guard)
		}
		if !ok {
			succeeded = false
			break
		}
	}
	paths := []bool{succeeded}
	ops := txn.Success
	if !succeeded {
		ops = txn.Failure
	}
	for _, op := range ops {
		nested := op.GetRequestTxn()
		if nested == nil {
			continue
		}
		nestedPaths, err := s.txnComparePaths(ctx, nested)
		if err != nil {
			return nil, nil, err
		}
		paths = append(paths, nestedPaths...)
	}
	return paths, guards, nil
}

// txnPathCursor walks the flat pre-order list of per-(sub)txn "succeeded" flags
// that txnComparePaths produced, handing them out one per tree node as the
// validate and execute passes re-walk the SAME tree in the SAME order. Wrapping
// paths+position in a cursor makes the shared position explicit and impossible
// to advance by accident — the old code threaded a raw *int through every call
// (audit A4).
type txnPathCursor struct {
	paths []bool
	pos   int
}

func (c *txnPathCursor) next() (bool, error) {
	if c.pos >= len(c.paths) {
		return false, fmt.Errorf("missing txn compare path")
	}
	v := c.paths[c.pos]
	c.pos++
	return v, nil
}

func validateTxnRangeRevisions(txn *etcdserverpb.TxnRequest, paths []bool, compactRevision, currentRevision int64) error {
	return validateTxnRangeRevisionsCursor(txn, &txnPathCursor{paths: paths}, compactRevision, currentRevision)
}

func validateTxnRangeRevisionsCursor(txn *etcdserverpb.TxnRequest, cur *txnPathCursor, compactRevision, currentRevision int64) error {
	succeeded, err := cur.next()
	if err != nil {
		return err
	}
	ops := txn.Success
	if !succeeded {
		ops = txn.Failure
	}
	for _, op := range ops {
		if r := op.GetRequestRange(); r != nil {
			if r.Revision > 0 && r.Revision < compactRevision {
				return compactedRevisionError()
			}
			if r.Revision > currentRevision {
				return futureRevisionError()
			}
		}
		if nested := op.GetRequestTxn(); nested != nil {
			if err := validateTxnRangeRevisionsCursor(nested, cur, compactRevision, currentRevision); err != nil {
				return err
			}
		}
	}
	return nil
}

func (s *RPCServer) executeTxnWithCursor(ctx context.Context, txn *etcdserverpb.TxnRequest, cur *txnPathCursor) (*etcdserverpb.TxnResponse, error) {
	succeeded, err := cur.next()
	if err != nil {
		return nil, err
	}
	ops := txn.Success
	if !succeeded {
		ops = txn.Failure
	}

	resp := &etcdserverpb.TxnResponse{
		Succeeded: succeeded,
		Responses: make([]*etcdserverpb.ResponseOp, 0, len(ops)),
	}
	for _, op := range ops {
		switch {
		case op.GetRequestPut() != nil:
			put, err := s.putWithEffectiveOptions(ctx, op.GetRequestPut())
			if err != nil {
				return nil, err
			}
			if err := s.ensureLeaseExists(put.Lease); err != nil {
				return nil, err
			}
			putResp, err := s.backend.Put(ctx, put)
			if err != nil {
				return nil, err
			}
			s.bindKeyToLease(put.Lease, string(put.Key))
			resp.Header = putResp.Header
			resp.Responses = append(resp.Responses, &etcdserverpb.ResponseOp{
				Response: &etcdserverpb.ResponseOp_ResponsePut{
					ResponsePut: putResp,
				},
			})
		case op.GetRequestRange() != nil:
			rangeResp, err := s.Range(ctx, op.GetRequestRange())
			if err != nil {
				return nil, err
			}
			resp.Header = rangeResp.Header
			resp.Responses = append(resp.Responses, &etcdserverpb.ResponseOp{
				Response: &etcdserverpb.ResponseOp_ResponseRange{
					ResponseRange: rangeResp,
				},
			})
		case op.GetRequestDeleteRange() != nil:
			del := op.GetRequestDeleteRange()
			if len(del.RangeEnd) != 0 {
				if err := s.peers.SyncReadRevision(ctx); err != nil {
					return nil, err
				}
			}
			deleteResp := s.emptyDeleteRangeResponse()
			if !isEmptyNonFromKeyRange(del.Key, del.RangeEnd) {
				deletedKeys, err := s.keysInDeleteRange(ctx, del)
				if err != nil {
					return nil, err
				}
				deleteResp, err = s.backend.DeleteRange(ctx, del)
				if err != nil {
					return nil, err
				}
				for _, key := range deletedKeys {
					s.unbindKeyFromLease(key)
				}
			}
			resp.Header = deleteResp.Header
			resp.Responses = append(resp.Responses, &etcdserverpb.ResponseOp{
				Response: &etcdserverpb.ResponseOp_ResponseDeleteRange{
					ResponseDeleteRange: deleteResp,
				},
			})
		case op.GetRequestTxn() != nil:
			txnResp, err := s.executeTxnWithCursor(ctx, op.GetRequestTxn(), cur)
			if err != nil {
				return nil, err
			}
			resp.Header = txnResp.Header
			resp.Responses = append(resp.Responses, &etcdserverpb.ResponseOp{
				Response: &etcdserverpb.ResponseOp_ResponseTxn{
					ResponseTxn: txnResp,
				},
			})
		default:
			return nil, fmt.Errorf("unsupported transaction operation: %v", op)
		}
	}
	if resp.Header == nil {
		resp.Header = txnHeader(int64(s.backend.GetCurrentRevision()))
	}
	return resp, nil
}

func (s *RPCServer) emptyDeleteRangeResponse() *etcdserverpb.DeleteRangeResponse {
	return &etcdserverpb.DeleteRangeResponse{
		Header: txnHeader(int64(s.backend.GetCurrentRevision())),
	}
}

func isCompact(txn *etcdserverpb.TxnRequest) bool {
	// See https://github.com/kubernetes/kubernetes/blob/442a69c3bdf6fe8e525b05887e57d89db1e2f3a5/staging/src/k8s.io/apiserver/pkg/storage/etcd3/compact.go#L72
	return len(txn.Compare) == 1 &&
		txn.Compare[0].Target == etcdserverpb.Compare_VERSION &&
		txn.Compare[0].Result == etcdserverpb.Compare_EQUAL &&
		len(txn.Success) == 1 &&
		txn.Success[0].GetRequestPut() != nil &&
		len(txn.Failure) == 1 &&
		txn.Failure[0].GetRequestRange() != nil &&
		string(txn.Compare[0].Key) == compactRevKey
}

// compact emulates the apiserver's compaction CAS
// (If(Version(compact_rev_key)==t) Then(Put) Else(Get)) atomically.
//
// The old emulation compared a SEPARATE version counter and then issued two
// independent Puts, so concurrent HA-apiserver compactors could both pass the
// check and both "win", and the two Puts could desync (#54/#72). It now uses the
// key's own MVCC version and a single atomic Create/Update CAS: the compare
// selects the Then/Else branch, and the CAS resolves any concurrent race so
// exactly one compactor wins — the loser's CAS fails and yields the same
// Else-shaped response (the current key + version) the apiserver reads.
func (s *RPCServer) compact(ctx context.Context, txn *etcdserverpb.TxnRequest) (*etcdserverpb.TxnResponse, error) {
	expectVersion := txn.Compare[0].GetVersion()
	put := txn.Success[0].GetRequestPut()
	key := []byte(compactRevKey)

	getResp, err := s.backend.Get(ctx, &etcdserverpb.RangeRequest{Key: key})
	if err != nil {
		return nil, err
	}
	var curKv *mvccpb.KeyValue
	curVersion := int64(0)
	if len(getResp.Kvs) > 0 {
		curKv = getResp.Kvs[0]
		curVersion = curKv.Version
	}

	// Compare mismatch -> Else branch: return the current key so the loser learns
	// the new version. (compact_rev_key's version is monotonically increasing, so
	// a mismatch here is never a transient the CAS below could recover.)
	if curVersion != expectVersion {
		rangeResp := &etcdserverpb.RangeResponse{Header: getResp.Header}
		if curKv != nil {
			rangeResp.Kvs = []*mvccpb.KeyValue{curKv}
			rangeResp.Count = 1
		}
		return &etcdserverpb.TxnResponse{
			Header:    getResp.Header,
			Succeeded: false,
			Responses: []*etcdserverpb.ResponseOp{{
				Response: &etcdserverpb.ResponseOp_ResponseRange{ResponseRange: rangeResp},
			}},
		}, nil
	}

	// Then branch: one atomic CAS. Create when the key is absent (version 0),
	// otherwise update at its exact current mod revision. On success the backend
	// returns a Put response (Succeeded=true) with the version bumped; on a lost
	// race the CAS fails and the backend returns the current key as a Range
	// response (Succeeded=false) — exactly the Else shape.
	putReq := &etcdserverpb.PutRequest{Key: key, Value: put.Value}
	if curKv == nil {
		return s.backend.Create(ctx, putReq, true)
	}
	return s.backend.Update(ctx, curKv.ModRevision, putReq, true)
}

// mapFenceErr converts a write-fence abort (backend.ErrLeadershipFenced) into a
// codes.Unavailable status so the etcd client retries the write against the
// current leader, exactly like the not-leader gate above (FINDING #39). Other
// errors pass through unchanged.
func mapFenceErr(err error) error {
	if errors.Is(err, backend.ErrLeadershipFenced) {
		return status.Errorf(codes.Unavailable, "write rejected: leadership changed during commit, retry on current leader")
	}
	return err
}

func getSuccessMetricTagByErr(err error) metrics.T {
	if err != nil {
		return metrics.Tag("success", "false")
	}

	return metrics.Tag("success", "true")
}

// errClassTag buckets a read/write error into a bounded, low-cardinality label so
// dashboards can separate benign, client-retriable failures — a compacted/future
// revision (client relists), a leader failover / fence (client retries) — from
// real backend trouble (timeouts/overload, unexpected errors). "none" on success.
// This is the missing signal over plain success=true/false: it distinguishes
// "the client will recover on its own" from "something is actually wrong".
func errClassTag(err error) metrics.T {
	return metrics.Tag("errclass", errClass(err))
}

func errClass(err error) string {
	switch {
	case err == nil:
		return "none"
	case errors.Is(err, context.Canceled):
		return "canceled"
	case errors.Is(err, context.DeadlineExceeded):
		return "deadline"
	case errors.Is(err, backend.ErrLeadershipFenced):
		return "fenced" // leadership changed during commit; client retries the new leader
	}
	switch status.Code(err) {
	case codes.OK:
		return "none"
	case codes.OutOfRange:
		return "revision" // compacted or future revision — benign, client relists/retries
	case codes.Unavailable:
		return "unavailable" // no fresh leader / contended — benign, client retries
	case codes.DeadlineExceeded:
		return "deadline" // timeout — possible overload
	case codes.NotFound:
		return "not_found"
	case codes.InvalidArgument:
		return "invalid"
	default:
		return "other" // unexpected — the one to alert on
	}
}

func txnHeader(rev int64) *etcdserverpb.ResponseHeader {
	return &etcdserverpb.ResponseHeader{
		Revision: rev,
	}
}

func safeBackendRevision(ctx context.Context, backend BackendShim) (uint64, error) {
	currentRevision := backend.GetCurrentRevision()
	compactRevision, err := backend.GetCompactRevision(ctx)
	if err != nil {
		return 0, err
	}
	if compactRevision >= currentRevision {
		currentRevision = compactRevision + 1
		backend.SetCurrentRevision(currentRevision)
	}
	return currentRevision, nil
}
