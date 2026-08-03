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

	"go.etcd.io/etcd/api/v3/authpb"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/mvccpb"
	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"
	"k8s.io/klog/v2"

	"github.com/kubewharf/kubebrain/pkg/backend"
	"github.com/kubewharf/kubebrain/pkg/metrics"
	"github.com/kubewharf/kubebrain/pkg/storage"
)

const (
	unaryRpcTimeout = 10 * time.Second

	compactRevKey          = "compact_rev_key"
	defaultMaxTxnOps       = 128
	defaultMaxRequestBytes = 1572864
	grpcOverheadBytes      = 512 * 1024
)

func (s *RPCServer) Range(ctx context.Context, r *etcdserverpb.RangeRequest) (*etcdserverpb.RangeResponse, error) {
	startTime := time.Now()
	klog.V(4).InfoS("RANGE", "key", r.Key, "rangeEnd", r.RangeEnd, "countOnly", r.CountOnly)
	if err := validateRangeRequest(r); err != nil {
		return nil, err
	}
	// A durable follower snapshot is sufficient only for an explicitly
	// serializable request. A linearizable historical read still has to observe
	// the leader's current revision in its response header, even though its KVs
	// come from the requested older revision.
	durableHistorical := r.Serializable && r.Revision > 0 && s.followerHasDurableRevision(ctx, uint64(r.Revision))
	// A serializable historical read whose revision is not yet covered by this
	// follower's durable watermark must run on the leader. A linearizable read
	// instead establishes SyncReadRevision below and can then read the shared
	// TiKV snapshot locally; proxying it here would incorrectly put auth before
	// etcd's read barrier.
	if r.Serializable && r.Revision > 0 && !s.peers.IsLeader() && !durableHistorical && s.peers.EtcdProxyEnabled() {
		caller, authErr := s.authCallerFromContext(ctx)
		if authErr != nil {
			return nil, authErr
		}
		if authErr = caller.require(r.Key, r.RangeEnd, authpb.READ); authErr != nil {
			return nil, authErr
		}
		s.metricCli.EmitCounter("read.follower.historical_proxy", 1)
		proxyCtx, err := s.forwardAuthToken(ctx, caller)
		if err != nil {
			return nil, err
		}
		response, err := s.peers.Range(proxyCtx, r)
		s.observeForwardedRevision(response.GetHeader(), err)
		return response, err
	}
	if !r.Serializable || (r.Revision > 0 && !s.peers.IsLeader() && !durableHistorical) {
		if err := s.peers.SyncReadRevision(ctx); err != nil {
			return &etcdserverpb.RangeResponse{}, readBarrierStatusErr(err)
		}
	}
	caller, authErr := s.authCallerFromContext(ctx)
	if authErr != nil {
		return nil, authErr
	}
	if authErr = caller.require(r.Key, r.RangeEnd, authpb.READ); authErr != nil {
		return nil, authErr
	}
	if err := s.checkRequestedRevision(ctx, r.Revision); err != nil {
		return nil, err
	}
	if isEmptyNonFromKeyRange(r.Key, r.RangeEnd) {
		revision, err := safeBackendRevision(ctx, s.backend)
		if err != nil {
			return nil, err
		}
		if err = s.ensureAuthRevision(ctx, caller); err != nil {
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
		s.metricCli.EmitHistogram("read.responsesize", proto.Size(response), methodTag, successTag)
	}
	if authErr = s.ensureAuthRevision(ctx, caller); authErr != nil {
		return nil, authErr
	}
	return response, err
}

// RangeStream implements the etcd 3.7 KV.RangeStream server-streaming RPC: it
// streams a recursive List as disjoint chunks at a single pinned revision rather
// than materializing the whole result in one unary RangeResponse. The
// kube-apiserver's watch-cache initialization (EtcdRangeStream feature gate, k8s
// 1.37) uses it to bound memory on large initial LISTs. This overrides the
// RangeStream default promoted from the embedded UnimplementedKVServer.
//
// Wire contract (etcd semantics): Kvs across chunks are disjoint and concatenate
// to the full result; only the final chunk carries Header/More/Count, including
// the pinned revision the apiserver uses as the sync's initial revision; the
// stream then ends with a normal return (io.EOF). A
// backend error aborts the stream with a gRPC status so the apiserver relists
// rather than treating a partial stream as complete.
func (s *RPCServer) RangeStream(r *etcdserverpb.RangeRequest, rs etcdserverpb.KV_RangeStreamServer) error {
	ctx := rs.Context()
	startTime := time.Now()
	klog.V(4).InfoS("RANGE STREAM", "key", r.Key, "rangeEnd", r.RangeEnd, "rev", r.Revision)
	if err := validateRangeRequest(r); err != nil {
		return err
	}
	// Match etcd's checkRangeStreamRequest: NONE means the natural ascending-key
	// order regardless of SortTarget, and explicit ASCEND+KEY is equivalent.
	// Other sort orders and revision filters cannot be streamed incrementally.
	if !isDefaultRangeStreamOrdering(r) {
		return status.Error(codes.Unimplemented, "RangeStream does not support custom sort orders")
	}
	if hasRangeRevisionFilters(r) {
		return status.Error(codes.Unimplemented, "RangeStream does not support revision filters")
	}
	// CountOnly has no KV payload to stream. A point lookup is inherently
	// bounded to one KV, and empty/reversed intervals must not enter the
	// partition scanner: its encoded MVCC borders are meaningful only for a
	// non-empty range. Use the unary path for these shapes, matching etcd's
	// Range result exactly while keeping recursive ranges on the bounded stream.
	if r.CountOnly || len(r.RangeEnd) == 0 || isEmptyNonFromKeyRange(r.Key, r.RangeEnd) {
		resp, err := s.Range(ctx, proto.Clone(r).(*etcdserverpb.RangeRequest))
		if err != nil {
			return rangeStreamStatusErr(err)
		}
		for _, response := range splitRangeStreamResponse(resp, int(s.maxRequestBytes), true) {
			if err := rs.Send(response); err != nil {
				return err
			}
		}
		s.metricCli.EmitCounter("read.range_stream", 1)
		s.metricCli.EmitHistogram("read.range_stream.latency", time.Since(startTime).Seconds())
		return nil
	}
	durableHistorical := r.Serializable && r.Revision > 0 && s.followerHasDurableRevision(ctx, uint64(r.Revision))
	if !r.Serializable || (r.Revision > 0 && !s.peers.IsLeader() && !durableHistorical) {
		if err := s.peers.SyncReadRevision(ctx); err != nil {
			return readBarrierStatusErr(err)
		}
	}
	caller, err := s.authCallerFromContext(ctx)
	if err != nil {
		return err
	}
	if err = caller.require(r.Key, r.RangeEnd, authpb.READ); err != nil {
		return err
	}
	if err := s.checkRequestedRevision(ctx, r.Revision); err != nil {
		return rangeStreamStatusErr(err)
	}
	// The data snapshot is pinned to r.Revision, but etcd's response header is
	// the store revision observed when the stream starts. In particular, a
	// historical RangeStream returns old KVs with a current header revision.
	streamHeaderRevision, err := safeBackendRevision(ctx, s.backend)
	if err != nil {
		return rangeStreamStatusErr(err)
	}
	ch, err := s.backend.RangeStreamChan(ctx, r.Key, r.RangeEnd, uint64(r.Revision))
	if err != nil {
		s.metricCli.EmitCounter("read.range_stream.err", 1)
		return rangeStreamStatusErr(err)
	}
	var (
		terminalSeen bool
		headerRev    int64
		chunks       int
		sentCount    int64
		totalCount   int64
		dataRevision uint64
	)
	for chunk := range ch {
		if chunk.err != nil {
			s.metricCli.EmitCounter("read.range_stream.err", 1)
			// Surface as Unavailable so the apiserver relists instead of trusting a
			// truncated stream.
			return status.Error(codes.Unavailable, chunk.err.Error())
		}
		headerRev = chunk.resp.Header.Revision
		if dataRevision == 0 {
			dataRevision = uint64(headerRev)
		}
		terminal := len(chunk.resp.Kvs) == 0
		if terminal {
			terminalSeen = true
		}
		totalCount += int64(len(chunk.resp.Kvs))
		// Scanner.More is an internal "another scanner chunk follows" marker.
		// etcd's wire More means the client Limit truncated the requested range;
		// an unlimited stream therefore reports false on every chunk. The terminal
		// header-only response carries the merged response's total Count.
		chunk.resp.More = false
		if terminal {
			chunk.resp.Header = txnHeader(int64(streamHeaderRevision))
			chunk.resp.Count = totalCount
			chunk.resp.More = r.Limit > 0 && totalCount > sentCount
		} else if r.Limit > 0 {
			remaining := r.Limit - sentCount
			switch {
			case remaining <= 0:
				chunk.resp.Kvs = nil
			case int64(len(chunk.resp.Kvs)) > remaining:
				chunk.resp.Kvs = chunk.resp.Kvs[:remaining]
			}
		}
		if !terminal && len(chunk.resp.Kvs) == 0 {
			// The limit was already satisfied; drain this scanner chunk only to
			// compute the terminal Count/More without retaining or sending it.
			continue
		}
		sentCount += int64(len(chunk.resp.Kvs))
		if r.KeysOnly {
			for _, kv := range chunk.resp.Kvs {
				kv.Value = nil
			}
		}
		for _, response := range splitRangeStreamResponse(chunk.resp, int(s.maxRequestBytes), terminal) {
			// etcd executes a revisioned Range for every chunk. If compaction
			// advances past the pinned snapshot after a partial response, the
			// next chunk must terminate with ErrCompacted; completing the stream
			// would make already-compacted history appear valid. Check each wire
			// chunk, not just each backend batch, because one batch can split
			// into several gRPC messages.
			if chunks > 0 {
				compactRevision, compactErr := s.backend.GetCompactRevisionFresh(ctx)
				if compactErr != nil {
					s.metricCli.EmitCounter("read.range_stream.err", 1)
					return rangeStreamStatusErr(compactErr)
				}
				if compactRevision > dataRevision {
					s.metricCli.EmitCounter("read.range_stream.err", 1)
					return compactedRevisionError()
				}
			}
			if err := rs.Send(response); err != nil {
				s.metricCli.EmitCounter("read.range_stream.send_err", 1)
				return err
			}
			chunks++
		}
	}
	if !terminalSeen {
		if err := ctx.Err(); err != nil {
			return err
		}
		s.metricCli.EmitCounter("read.range_stream.err", 1)
		return status.Error(codes.Unavailable, "range stream ended without terminal metadata")
	}
	if err := s.ensureAuthRevision(ctx, caller); err != nil {
		return err
	}
	s.metricCli.EmitCounter("read.range_stream", 1)
	s.metricCli.EmitHistogram("read.range_stream.latency", time.Since(startTime).Seconds())
	klog.V(4).InfoS("RANGE STREAM done", "key", r.Key, "chunks", chunks, "rev", headerRev)
	return nil
}

// splitRangeStreamResponse keeps public stream messages near the configured
// request-size target, matching etcd's adaptive RangeStream chunking. Scanner
// batches intentionally use an internal throughput-oriented size independent
// of per-instance admission settings, so a second boundary is required here.
// A single KV is indivisible and is emitted alone even when it exceeds target.
func splitRangeStreamResponse(resp *etcdserverpb.RangeResponse, target int, final bool) []*etcdserverpb.RangeStreamResponse {
	wrapped := func(kvs []*mvccpb.KeyValue, metadata bool) *etcdserverpb.RangeStreamResponse {
		rangeResp := &etcdserverpb.RangeResponse{Kvs: kvs}
		if metadata {
			rangeResp.Header = resp.Header
			rangeResp.More = resp.More
			rangeResp.Count = resp.Count
		}
		return &etcdserverpb.RangeStreamResponse{RangeResponse: rangeResp}
	}
	if target <= 0 || len(resp.Kvs) < 2 {
		return []*etcdserverpb.RangeStreamResponse{wrapped(resp.Kvs, final)}
	}

	// RangeResponse.kvs is field 2 and RangeStreamResponse.range_response is
	// field 1. Prefix sums make candidate sizing O(1), avoiding repeated
	// serialization of an ever-growing slice of large values.
	kvWirePrefix := make([]int, len(resp.Kvs)+1)
	for i, kv := range resp.Kvs {
		kvWirePrefix[i+1] = kvWirePrefix[i] +
			protowire.SizeTag(2) + protowire.SizeBytes(proto.Size(kv))
	}
	metadataSize := proto.Size(&etcdserverpb.RangeResponse{
		Header: resp.Header,
		More:   resp.More,
		Count:  resp.Count,
	})
	wireSize := func(start, end int, metadata bool) int {
		innerSize := kvWirePrefix[end] - kvWirePrefix[start]
		if metadata {
			innerSize += metadataSize
		}
		return protowire.SizeTag(1) + protowire.SizeBytes(innerSize)
	}

	responses := make([]*etcdserverpb.RangeStreamResponse, 0, 1)
	for start := 0; start < len(resp.Kvs); {
		end := start + 1
		for end <= len(resp.Kvs) && wireSize(start, end, final && end == len(resp.Kvs)) <= target {
			end++
		}
		if end == start+1 {
			// The first KV alone exceeds target; it cannot be split further.
			responses = append(responses, wrapped(resp.Kvs[start:end], final && end == len(resp.Kvs)))
			start = end
			continue
		}
		if end > len(resp.Kvs) {
			responses = append(responses, wrapped(resp.Kvs[start:], final))
			break
		}
		responses = append(responses, wrapped(resp.Kvs[start:end-1], false))
		start = end - 1
	}
	return responses
}

func isDefaultRangeStreamOrdering(r *etcdserverpb.RangeRequest) bool {
	return r.SortOrder == etcdserverpb.RangeRequest_NONE ||
		(r.SortOrder == etcdserverpb.RangeRequest_ASCEND && r.SortTarget == etcdserverpb.RangeRequest_KEY)
}

func (s *RPCServer) followerHasDurableRevision(ctx context.Context, requested uint64) bool {
	if requested == 0 || s.peers.IsLeader() {
		return false
	}
	durable, err := s.backend.GetDurableRevision(ctx)
	if err != nil || requested > durable {
		return false
	}
	// The persisted marker is a safe lower bound for this follower's in-memory
	// revision too; advancing it prevents the generic future-revision check from
	// rejecting the historical snapshot we just proved is committed.
	s.backend.SetCurrentRevision(durable)
	return true
}

// rangeStreamStatusErr shapes a pre-stream failure into the RangeStream error
// contract, mirroring the structure of etcd's togRPCError (v3rpc/util.go):
// context errors pass through so gRPC reports Canceled/DeadlineExceeded, and
// proper status errors (rpctypes compacted/future, our own Unimplemented
// rejections) pass through untouched. Where we deliberately diverge is the
// remainder: etcd maps its KNOWN transient errors (request timed out, leader
// changed) to Unavailable and only truly foreign errors to Unknown — our raw
// errors here (a revision sync timeout, a backend construction error) ARE that
// transient class, so wrap them as Unavailable: transient, relist — the same
// contract as a mid-stream error. Left raw they surface as code Unknown (seen
// once in the 1.37-alpha cold-start test).
func rangeStreamStatusErr(err error) error {
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return err
	}
	if _, ok := status.FromError(err); ok {
		return err
	}
	return status.Error(codes.Unavailable, err.Error())
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

	if err := validateTxnRequestWithMaxOps(txn, s.maxTxnOps); err != nil {
		return nil, err
	}
	readOnly := txnIsReadonly(txn)
	// Match EtcdServer.Txn: a read-only transaction containing any
	// non-serializable Range establishes its linearizable read barrier before
	// authorization and execution. Besides preserving etcd's error ordering,
	// this pins the shared-TiKV snapshot after the current leadership has been
	// confirmed. Fully serializable read-only txns intentionally bypass it.
	if readOnly && !txnIsSerializable(txn) {
		if err := s.peers.SyncReadRevision(ctx); err != nil {
			return nil, readBarrierStatusErr(err)
		}
	}
	var epoch uint64
	if !readOnly {
		var leadingFresh bool
		epoch, leadingFresh = s.peers.EpochAndLeadingFresh()
		if !leadingFresh {
			s.metricCli.EmitCounter("write.follower", 1)
			if s.peers.EtcdProxyEnabled() {
				proxyCtx, err := s.forwardWriteAuthContext(ctx)
				if err != nil {
					return nil, err
				}
				response, err := s.peers.Txn(proxyCtx, txn)
				s.observeForwardedRevision(response.GetHeader(), err)
				return response, err
			}
			return nil, s.notLeaderErr("txn")
		}
	}
	caller, authErr := s.authCallerFromContext(ctx)
	if authErr != nil {
		return nil, authErr
	}
	if authErr = s.authorizeTxn(caller, txn); authErr != nil {
		return nil, authErr
	}
	ctx = withAuthWriteGuard(ctx, caller)

	deadline, ok := ctx.Deadline()
	if ok && startTime.Sub(deadline) >= 0 {
		return nil, context.DeadlineExceeded
	}
	ctx, cancel := withUnaryRequestTimeout(ctx)
	defer cancel()
	if readOnly {
		var (
			revision uint64
			err      error
		)
		if txnIsSerializable(txn) {
			revision, err = s.serializableTxnRevision(ctx)
		} else {
			// SyncReadRevision above already established the linearizable
			// barrier. The shared TiKV revision observed now is therefore safe
			// to pin for the whole read-only transaction on any replica.
			revision, err = safeBackendRevision(ctx, s.backend)
		}
		if err != nil {
			if txnIsSerializable(txn) && !s.peers.IsLeader() && s.peers.EtcdProxyEnabled() {
				proxyCtx, proxyErr := s.forwardAuthToken(ctx, caller)
				if proxyErr != nil {
					return nil, proxyErr
				}
				response, proxyErr := s.peers.Txn(proxyCtx, txn)
				s.observeForwardedRevision(response.GetHeader(), proxyErr)
				return response, proxyErr
			}
			return nil, err
		}
		response, err := s.executeReadonlyTxnAtRevision(ctx, txn, int64(revision))
		if err == nil {
			err = s.ensureAuthRevision(ctx, caller)
		}
		return response, err
	}
	if err := s.rejectCorrupt(ctx); err != nil {
		return nil, err
	}

	// only leader can accept and handle write request
	// return error includes current leader, help etcd client send request to right instance
	// Capture the leadership epoch at admission and thread it through the context;
	// the backend re-checks it just before commit so a leadership change mid-write
	// fences the commit instead of losing it silently (FINDING #39).
	ctx = backend.WithLeadershipEpoch(ctx, epoch)
	s.leaseWriteMu.RLock()
	defer s.leaseWriteMu.RUnlock()
	// A privileged writer may attach a protected key to a referenced lease
	// between the admission check above and this lock. Re-check against the
	// locked attachment snapshot so a leased Put nested anywhere in the txn
	// cannot pass RBAC using stale lease membership.
	if authErr = s.authorizeTxn(caller, txn); authErr != nil {
		return nil, authErr
	}
	if txnContainsPut(txn) {
		_, _, noSpace, quotaErr := s.backend.QuotaStatus(ctx)
		if quotaErr != nil {
			return nil, mapFenceErr(quotaErr)
		}
		if noSpace {
			return nil, rpctypes.ErrGRPCNoSpace
		}
	}
	var (
		err                   error
		response              *etcdserverpb.TxnResponse
		methodTag, successTag metrics.T
		failedKey             string
	)
	if sh, ok := isCreate(txn); ok && !s.writeShapeTouchesLease(sh) {
		var put *etcdserverpb.PutRequest
		put, err = s.putWithEffectiveOptions(ctx, sh.put)
		if err != nil {
			return nil, err
		}
		if err = s.ensureLeaseExists(put.Lease); err != nil {
			return nil, err
		}
		response, err = s.backend.Create(ctx, put, sh.includeFailure)
		methodTag = metrics.Tag("method", "create")
		if err != nil || !response.Succeeded {
			failedKey = string(put.Key)
		} else {
			s.bindKeyToLease(ctx, put.Lease, string(put.Key))
		}
	} else if sh, ok := isCompareDelete(txn); ok && !s.writeShapeTouchesLease(sh) {
		response, err = s.backend.CompareDelete(ctx, sh.deleteReq, sh.rev, sh.includeFailure)
		methodTag = metrics.Tag("method", "delete")
		if err != nil || !response.Succeeded {
			failedKey = string(sh.key)
		} else {
			s.unbindKeyFromLease(ctx, string(sh.key))
		}
	} else if sh, ok := isDelete(txn); ok && !s.writeShapeTouchesLease(sh) {
		response, err = s.backend.Delete(ctx, sh.key, sh.rev, sh.includeFailure)
		methodTag = metrics.Tag("method", "delete")
		if err != nil || !response.Succeeded {
			failedKey = string(sh.key)
		} else {
			s.unbindKeyFromLease(ctx, string(sh.key))
		}
	} else if sh, ok := isUpdate(txn); ok && !s.writeShapeTouchesLease(sh) {
		var put *etcdserverpb.PutRequest
		put, err = s.putWithEffectiveOptions(ctx, sh.put)
		if err != nil {
			return nil, err
		}
		if err = s.ensureLeaseExists(put.Lease); err != nil {
			return nil, err
		}
		response, err = s.backend.Update(ctx, sh.rev, put, sh.includeFailure)
		methodTag = metrics.Tag("method", "update")
		if err != nil || !response.Succeeded {
			failedKey = string(sh.key)
		} else {
			s.bindKeyToLease(ctx, put.Lease, string(sh.key))
		}
	} else if ok := isCompact(txn); ok {
		response, err = s.compact(ctx, txn)
		methodTag = metrics.Tag("method", "compact")
	} else if isUnconditionalTxn(txn) {
		response, err = s.executeGenericTxn(ctx, txn)
		methodTag = metrics.Tag("method", "txn-unconditional")
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
		s.metricCli.EmitHistogram("write.responsesize", proto.Size(response), methodTag, successTag)
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

func txnContainsPut(txn *etcdserverpb.TxnRequest) bool {
	if txn == nil {
		return false
	}
	for _, ops := range [][]*etcdserverpb.RequestOp{txn.Success, txn.Failure} {
		for _, op := range ops {
			if op.GetRequestPut() != nil || txnContainsPut(op.GetRequestTxn()) {
				return true
			}
		}
	}
	return false
}

// Match upstream txn.IsTxnReadonly/IsTxnSerializable: nested transactions and
// writes make the request ineligible; every operation in both top-level branches
// must be a serializable Range.
func txnIsReadonly(txn *etcdserverpb.TxnRequest) bool {
	for _, ops := range [][]*etcdserverpb.RequestOp{txn.Success, txn.Failure} {
		for _, op := range ops {
			if op.GetRequestRange() == nil {
				return false
			}
		}
	}
	return true
}

func txnIsSerializable(txn *etcdserverpb.TxnRequest) bool {
	for _, ops := range [][]*etcdserverpb.RequestOp{txn.Success, txn.Failure} {
		for _, op := range ops {
			r := op.GetRequestRange()
			if r == nil || !r.Serializable {
				return false
			}
		}
	}
	return true
}

func (s *RPCServer) serializableTxnRevision(ctx context.Context) (uint64, error) {
	if s.peers.IsLeader() {
		return safeBackendRevision(ctx, s.backend)
	}
	return s.backend.GetDurableRevision(ctx)
}

func (s *RPCServer) executeReadonlyTxnAtRevision(ctx context.Context, txn *etcdserverpb.TxnRequest, revision int64) (*etcdserverpb.TxnResponse, error) {
	// An explicit historical read must observe a Compact that completed on any
	// replica. The ordinary compact-revision accessor has a short TTL cache and
	// can therefore be stale-low immediately after a proxied compaction; using it
	// here would let a linearizable read-only Txn return history that etcd already
	// made inaccessible. Historical reads are the cold path, so read the shared TiKV
	// watermark authoritatively and refresh this replica's cache.
	compactRevision, err := s.backend.GetCompactRevisionFresh(ctx)
	if err != nil {
		return nil, err
	}
	paths, _, err := s.txnComparePathsGuardedAtRevision(ctx, txn, revision)
	if err != nil {
		return nil, err
	}
	if err := validateTxnRangeRevisions(txn, paths, int64(compactRevision), revision); err != nil {
		return nil, err
	}
	return s.executeStagedGenericTxnAtRevision(ctx, txn, paths, nil, revision)
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
	}
	for _, op := range txn.Success {
		if err := validateTxnRequestOp(op, maxTxnOps-opc); err != nil {
			return err
		}
	}
	for _, op := range txn.Failure {
		if err := validateTxnRequestOp(op, maxTxnOps-opc); err != nil {
			return err
		}
	}
	if err := validateTxnIntervals(txn.Success); err != nil {
		return err
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
	start []byte
	end   []byte
	point bool
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
	// Match etcd's checkIntervals ordering: collect every child transaction's
	// puts and delete intervals before checking puts at this level. Otherwise a
	// Put followed by a nested DeleteRange containing that key is accepted
	// merely because the child appears later in the request.
	for _, op := range ops {
		nested := op.GetRequestTxn()
		if nested == nil {
			continue
		}
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
	}

	for _, op := range ops {
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
		start: r.Key,
		end:   r.RangeEnd,
	}
}

func (i txnDeleteInterval) contains(key []byte) bool {
	if i.point {
		return bytes.Equal(key, i.start)
	}
	if bytes.Compare(key, i.start) < 0 {
		return false
	}
	return bytes.Compare(key, i.end) < 0
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

// notLeaderErr is the Unavailable error a write RPC returns when this node is
// not the fresh leader and cannot forward to one; the message carries this
// node's identity and the current leader so the etcd client can redirect. op
// names the rejected write (e.g. "txn", "put") — single-sourced so all four
// write paths report it identically (audit E1).
func (s *RPCServer) notLeaderErr(op string) error {
	lock := s.backend.GetResourceLock()
	return status.Errorf(codes.Unavailable, "%s error addr is %s leader %s", op, lock.Identity(), lock.Describe())
}

func (s *RPCServer) checkRequestedRevision(ctx context.Context, revision int64) error {
	if revision <= 0 {
		return nil
	}
	// A completed Compact is a cluster-wide visibility boundary. Bypass the
	// replica-local TTL cache for explicit historical reads so a Range arriving
	// on another replica cannot briefly return already-compacted history.
	compactRevision, err := s.backend.GetCompactRevisionFresh(ctx)
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
	epoch, leadingFresh := s.peers.EpochAndLeadingFresh()
	if !leadingFresh {
		s.metricCli.EmitCounter("write.follower", 1)
		if s.peers.EtcdProxyEnabled() {
			proxyCtx, err := s.forwardWriteAuthContext(ctx)
			if err != nil {
				return nil, err
			}
			response, err := s.peers.Compact(proxyCtx, r)
			s.observeForwardedRevision(response.GetHeader(), err)
			return response, err
		}
		return nil, s.notLeaderErr("compact")
	}
	// Compact is a cluster-wide destructive history operation, not a key-range
	// write. Upstream etcd protects it with AuthAdmin.isPermitted (root only).
	caller, err := s.authCallerFromContext(ctx)
	if err != nil {
		return nil, err
	}
	if !caller.isRoot() {
		return nil, rpctypes.ErrPermissionDenied
	}
	if err := s.rejectCorrupt(ctx); err != nil {
		return nil, err
	}
	ctx = backend.WithLeadershipEpoch(ctx, epoch)
	if err := s.peers.SyncReadRevision(ctx); err != nil {
		return nil, readBarrierStatusErr(err)
	}
	if r.Revision < 0 {
		return nil, compactedRevisionError()
	}
	if r.Revision > int64(s.backend.GetCurrentRevision()) {
		return nil, futureRevisionError()
	}
	// Compact retries must be classified against the shared durable watermark.
	// After leadership changes this replica's TTL cache may be stale-low; letting
	// an already-compacted request reach the idempotent backend would turn etcd's
	// ErrCompacted into a successful response.
	compactRevision, err := s.backend.GetCompactRevisionFresh(ctx)
	if err != nil {
		return nil, err
	}
	hasCompactRevision, err := s.backend.HasCompactRevision(ctx)
	if err != nil {
		return nil, err
	}
	if hasCompactRevision && r.Revision <= int64(compactRevision) {
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
		return nil, mapFenceErr(err)
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
	ctx, cancel := withUnaryRequestTimeout(ctx)
	defer cancel()
	if err := validatePutRequest(r); err != nil {
		return nil, err
	}
	epoch, leadingFresh := s.peers.EpochAndLeadingFresh()
	if !leadingFresh {
		s.metricCli.EmitCounter("write.follower", 1)
		if s.peers.EtcdProxyEnabled() {
			proxyCtx, err := s.forwardWriteAuthContext(ctx)
			if err != nil {
				return nil, err
			}
			response, err := s.peers.Put(proxyCtx, r)
			s.observeForwardedRevision(response.GetHeader(), err)
			return response, err
		}
		return nil, s.notLeaderErr("put")
	}
	caller, authErr := s.authCallerFromContext(ctx)
	if authErr != nil {
		return nil, authErr
	}
	if authErr = s.authorizePut(caller, r); authErr != nil {
		return nil, authErr
	}
	ctx = withAuthWriteGuard(ctx, caller)
	if err := s.rejectCorrupt(ctx); err != nil {
		return nil, err
	}
	ctx = backend.WithLeadershipEpoch(ctx, epoch)
	s.leaseWriteMu.RLock()
	defer s.leaseWriteMu.RUnlock()
	// Keep the lease key snapshot used by RBAC stable through the durable write.
	// The admission check remains above for follower/error-order compatibility;
	// this second check closes a concurrent attachment TOCTOU on the leader.
	if authErr = s.authorizePut(caller, r); authErr != nil {
		return nil, authErr
	}
	_, _, noSpace, quotaErr := s.backend.QuotaStatus(ctx)
	if quotaErr != nil {
		return nil, mapFenceErr(quotaErr)
	}
	if noSpace {
		return nil, rpctypes.ErrGRPCNoSpace
	}
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
		s.metricCli.EmitHistogram("write.responsesize", proto.Size(response), metrics.Tag("method", "put"), successTag)
	}
	return response, mapFenceErr(err)
}

func (s *RPCServer) DeleteRange(ctx context.Context, r *etcdserverpb.DeleteRangeRequest) (*etcdserverpb.DeleteRangeResponse, error) {
	startTime := time.Now()
	ctx, cancel := withUnaryRequestTimeout(ctx)
	defer cancel()
	if err := validateDeleteRangeRequest(r); err != nil {
		return nil, err
	}
	epoch, leadingFresh := s.peers.EpochAndLeadingFresh()
	if !leadingFresh {
		s.metricCli.EmitCounter("write.follower", 1)
		if s.peers.EtcdProxyEnabled() {
			proxyCtx, err := s.forwardWriteAuthContext(ctx)
			if err != nil {
				return nil, err
			}
			response, err := s.peers.DeleteRange(proxyCtx, r)
			s.observeForwardedRevision(response.GetHeader(), err)
			return response, err
		}
		return nil, s.notLeaderErr("delete range")
	}
	caller, authErr := s.authCallerFromContext(ctx)
	if authErr != nil {
		return nil, authErr
	}
	if authErr = caller.require(r.Key, r.RangeEnd, authpb.WRITE); authErr != nil {
		return nil, authErr
	}
	if r.PrevKv {
		if authErr = caller.require(r.Key, r.RangeEnd, authpb.READ); authErr != nil {
			return nil, authErr
		}
	}
	ctx = withAuthWriteGuard(ctx, caller)
	if err := s.rejectCorrupt(ctx); err != nil {
		return nil, err
	}
	ctx = backend.WithLeadershipEpoch(ctx, epoch)
	if len(r.RangeEnd) != 0 {
		if err := s.peers.SyncReadRevision(ctx); err != nil {
			return nil, readBarrierStatusErr(err)
		}
	}
	if isEmptyNonFromKeyRange(r.Key, r.RangeEnd) {
		return s.emptyDeleteRangeResponse(), nil
	}
	s.leaseWriteMu.Lock()
	defer s.leaseWriteMu.Unlock()
	deletedKeys, keyErr := s.keysInDeleteRange(ctx, r)
	if keyErr != nil {
		return nil, keyErr
	}
	response, err := s.deleteRangeWithAttachments(ctx, r, deletedKeys)
	successTag := getSuccessMetricTagByErr(err)
	s.metricCli.EmitCounter("write", 1, metrics.Tag("method", "delete-range"), successTag, errClassTag(err))
	s.metricCli.EmitHistogram("write.latency", time.Since(startTime).Seconds(), metrics.Tag("method", "delete-range"), successTag)
	if response != nil {
		s.metricCli.EmitHistogram("write.responsesize", proto.Size(response), metrics.Tag("method", "delete-range"), successTag)
	}
	return response, mapFenceErr(err)
}

func (s *RPCServer) putWithEffectiveOptions(ctx context.Context, r *etcdserverpb.PutRequest) (*etcdserverpb.PutRequest, error) {
	// etcd validates the explicitly requested lease before loading the previous
	// KV needed by IgnoreValue/IgnoreLease. Preserve that error precedence when
	// both the key and lease are missing.
	if r.IgnoreValue || r.IgnoreLease {
		if err := s.ensureLeaseExists(r.Lease); err != nil {
			return nil, err
		}
	}
	if !r.IgnoreLease && !r.IgnoreValue {
		return r, nil
	}
	clone := proto.Clone(r).(*etcdserverpb.PutRequest)
	rangeResp, err := s.backend.Get(ctx, &etcdserverpb.RangeRequest{Key: r.Key})
	if err != nil {
		return nil, err
	}
	if len(rangeResp.Kvs) == 0 {
		return nil, txnKeyNotFoundError()
	}
	current := rangeResp.Kvs[0]
	if r.IgnoreLease {
		clone.IgnoreLease = false
		clone.Lease = current.Lease
	}
	if r.IgnoreValue {
		clone.IgnoreValue = false
		clone.Value = current.Value
	}
	return clone, nil
}

// writeShape is a decoded fast-path write. Which fields are set depends on which
// detector matched; includeFailure records whether the txn carried a failure
// range op. Returning a named-field struct (with an ok bool) instead of a
// positional (int64, []byte, ..., bool, bool) tuple removes the transposition
// hazard of the trailing bools (audit E8).
type writeShape struct {
	rev            int64
	key            []byte
	put            *etcdserverpb.PutRequest
	deleteReq      *etcdserverpb.DeleteRangeRequest
	includeFailure bool
}

func (s *RPCServer) writeShapeTouchesLease(shape writeShape) bool {
	if shape.put != nil && shape.put.Lease != 0 {
		return true
	}
	key := shape.key
	if len(key) == 0 && shape.put != nil {
		key = shape.put.Key
	}
	return len(key) != 0 && s.leaseIDForKey(string(key)) != 0
}

func isCreate(txn *etcdserverpb.TxnRequest) (writeShape, bool) {
	if len(txn.Compare) == 1 &&
		txn.Compare[0].Target == etcdserverpb.Compare_MOD &&
		txn.Compare[0].Result == etcdserverpb.Compare_EQUAL &&
		txn.Compare[0].GetModRevision() == 0 &&
		(len(txn.Failure) == 0 || (len(txn.Failure) == 1 && txn.Failure[0].GetRequestRange() != nil)) &&
		len(txn.Success) == 1 &&
		txn.Success[0].GetRequestPut() != nil &&
		bytes.Equal(txn.Compare[0].Key, txn.Success[0].GetRequestPut().Key) {
		return writeShape{put: txn.Success[0].GetRequestPut(), includeFailure: len(txn.Failure) == 1}, true
	}
	return writeShape{}, false
}

func isDelete(txn *etcdserverpb.TxnRequest) (writeShape, bool) {
	if len(txn.Compare) == 0 &&
		len(txn.Failure) == 0 &&
		len(txn.Success) == 2 &&
		txn.Success[0].GetRequestRange() != nil &&
		txn.Success[1].GetRequestDeleteRange() != nil {
		rng := txn.Success[1].GetRequestDeleteRange()
		if len(rng.RangeEnd) == 0 {
			return writeShape{key: rng.Key, includeFailure: true}, true
		}
	}
	return writeShape{}, false
}

func isCompareDelete(txn *etcdserverpb.TxnRequest) (writeShape, bool) {
	if len(txn.Compare) == 1 &&
		txn.Compare[0].Target == etcdserverpb.Compare_MOD &&
		txn.Compare[0].Result == etcdserverpb.Compare_EQUAL &&
		(len(txn.Failure) == 0 || (len(txn.Failure) == 1 && txn.Failure[0].GetRequestRange() != nil)) &&
		len(txn.Success) == 1 &&
		txn.Success[0].GetRequestDeleteRange() != nil {
		deleteReq := txn.Success[0].GetRequestDeleteRange()
		if len(deleteReq.RangeEnd) != 0 {
			return writeShape{}, false
		}
		if !bytes.Equal(txn.Compare[0].Key, deleteReq.Key) {
			return writeShape{}, false
		}
		return writeShape{
			rev:            txn.Compare[0].GetModRevision(),
			key:            deleteReq.Key,
			deleteReq:      deleteReq,
			includeFailure: len(txn.Failure) == 1,
		}, true
	}
	return writeShape{}, false
}

func isUpdate(txn *etcdserverpb.TxnRequest) (writeShape, bool) {
	if len(txn.Compare) == 1 &&
		txn.Compare[0].Target == etcdserverpb.Compare_MOD &&
		txn.Compare[0].Result == etcdserverpb.Compare_EQUAL &&
		len(txn.Success) == 1 &&
		txn.Success[0].GetRequestPut() != nil &&
		bytes.Equal(txn.Compare[0].Key, txn.Success[0].GetRequestPut().Key) &&
		(len(txn.Failure) == 0 || (len(txn.Failure) == 1 && txn.Failure[0].GetRequestRange() != nil)) {
		return writeShape{
			rev:            txn.Compare[0].GetModRevision(),
			key:            txn.Compare[0].Key,
			put:            txn.Success[0].GetRequestPut(),
			includeFailure: len(txn.Failure) == 1,
		}, true
	}
	return writeShape{}, false
}

// With no compares etcd unconditionally selects Success. Failure still has to
// pass request validation and authorization, but its presence does not make the
// transaction shape unsupported.
func isUnconditionalTxn(txn *etcdserverpb.TxnRequest) bool {
	if len(txn.Compare) != 0 {
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
	needsStaged := txnNeedsStagedExecution(txn)
	if needsStaged {
		var unlock func()
		ctx, unlock = s.backend.BeginRangeTxn(ctx)
		defer unlock()
	}
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
		// A write Txn may run on a newly elected leader whose compact watermark
		// cache predates a Compact completed by the previous leader. Validate the
		// selected branch against shared durable state before any staged/atomic
		// mutation, otherwise a historical Range can be accepted and its sibling
		// writes committed after that history became inaccessible.
		compactRevision, err := s.backend.GetCompactRevisionFresh(ctx)
		if err != nil {
			return nil, err
		}
		if err := s.validateTxnExecutionOrder(
			ctx, txn, paths, int64(compactRevision), int64(s.backend.GetCurrentRevision()),
		); err != nil {
			return nil, err
		}
		// Prefer the atomic single-revision path when the chosen branch is a set of
		// distinct-key writes (#4). The compare guards make it serializable: a guard
		// conflict means a compared key changed, so re-evaluate the compares and
		// retry. Ineligible shapes fall back to the (unchanged) sequential path.
		resp, handled, err := s.tryAtomicGenericTxn(ctx, txn, paths, guards)
		if errors.Is(err, backend.ErrTxnGuardConflict) {
			continue
		}
		if handled {
			return resp, err
		}
		if needsStaged {
			resp, err = s.executeStagedGenericTxn(ctx, txn, paths, guards)
			if errors.Is(err, backend.ErrTxnGuardConflict) {
				continue
			}
			return resp, err
		}
		// A valid shape can still be ineligible for the atomic flattening fast
		// path, for example sibling nested Put(b) followed by point Delete(b).
		// Falling back to the legacy sequential executor would expose nested txn
		// headers and multiple write revisions; staged execution preserves etcd's
		// single MVCC write transaction semantics.
		resp, err = s.executeStagedGenericTxn(ctx, txn, paths, guards)
		if errors.Is(err, backend.ErrTxnGuardConflict) {
			continue
		}
		return resp, err
	}
}

func txnNeedsStagedExecution(txn *etcdserverpb.TxnRequest) bool {
	deleteOps := 0
	var walk func(*etcdserverpb.TxnRequest) bool
	walk = func(cur *etcdserverpb.TxnRequest) bool {
		for _, cmp := range cur.Compare {
			if len(cmp.RangeEnd) != 0 {
				return true
			}
		}
		for _, branches := range [][]*etcdserverpb.RequestOp{cur.Success, cur.Failure} {
			for _, op := range branches {
				switch {
				case op.GetRequestRange() != nil:
					return true
				case op.GetRequestPut() != nil:
					put := op.GetRequestPut()
					if put.IgnoreLease || put.IgnoreValue {
						return true
					}
				case op.GetRequestDeleteRange() != nil:
					deleteOps++
					if len(op.GetRequestDeleteRange().RangeEnd) != 0 || deleteOps > 1 {
						return true
					}
				case op.GetRequestTxn() != nil:
					if walk(op.GetRequestTxn()) {
						return true
					}
				}
			}
		}
		return false
	}
	return walk(txn)
}

func txnHasRangeCompare(txn *etcdserverpb.TxnRequest) bool {
	for _, cmp := range txn.Compare {
		if len(cmp.RangeEnd) != 0 {
			return true
		}
	}
	for _, branches := range [][]*etcdserverpb.RequestOp{txn.Success, txn.Failure} {
		for _, op := range branches {
			if nested := op.GetRequestTxn(); nested != nil && txnHasRangeCompare(nested) {
				return true
			}
		}
	}
	return false
}

// tryAtomicGenericTxn recursively flattens the chosen nested path when it
// consists solely of distinct-key Put and single-key DeleteRange ops. Every
// write then shares one backend batch/revision and the flat results are rebuilt
// into the original nested response tree. handled=false means the path needs
// staged range/Ignore* semantics and must use the fallback for now.

type atomicTxnPlan struct {
	writes    []backend.TxnWriteOp
	prevKvs   []bool
	requests  []*etcdserverpb.RequestOp
	responses []*etcdserverpb.ResponseOp
	root      *etcdserverpb.TxnResponse
	seen      map[string]struct{}
}

func (s *RPCServer) tryAtomicGenericTxn(ctx context.Context, txn *etcdserverpb.TxnRequest, paths []bool, guards []backend.TxnGuard) (*etcdserverpb.TxnResponse, bool, error) {
	cur := &txnPathCursor{paths: paths}
	plan := &atomicTxnPlan{seen: make(map[string]struct{})}
	root, eligible, err := buildAtomicTxnPlan(txn, cur, plan)
	if err != nil {
		return nil, true, err
	}
	if !eligible || len(plan.writes) == 0 {
		return nil, false, nil
	}
	plan.root = root
	// Validate all put leases up front: an atomic txn must reject as a whole if a
	// referenced lease is missing, never apply a prefix of its writes.
	for _, op := range plan.requests {
		if put := op.GetRequestPut(); put != nil {
			if err := s.ensureLeaseExists(put.Lease); err != nil {
				return nil, true, err
			}
		}
	}

	writes, userCount := s.withLeaseAttachmentOps(plan.writes)
	prevKVs := append([]bool(nil), plan.prevKvs...)
	prevKVs = append(prevKVs, make([]bool, len(writes)-userCount)...)
	responses, rev, results, err := s.backend.TxnApply(ctx, writes, guards, prevKVs)
	if err != nil {
		s.reconcileLeaseIndexesAfterUncertain(err, rev, writes, userCount)
		return nil, true, err
	}
	for i := 0; i < userCount; i++ {
		plan.responses[i].Response = responses[i].Response
	}
	stampTxnResponseHeaders(plan.root, int64(rev))
	s.applyLeaseIndexes(writes, results, userCount)
	return plan.root, true, nil
}

func buildAtomicTxnPlan(txn *etcdserverpb.TxnRequest, cur *txnPathCursor, plan *atomicTxnPlan) (*etcdserverpb.TxnResponse, bool, error) {
	succeeded, err := cur.next()
	if err != nil {
		return nil, false, err
	}
	ops := txn.Success
	if !succeeded {
		ops = txn.Failure
	}
	resp := &etcdserverpb.TxnResponse{
		Header:    &etcdserverpb.ResponseHeader{},
		Succeeded: succeeded,
		Responses: make([]*etcdserverpb.ResponseOp, 0, len(ops)),
	}
	for _, op := range ops {
		switch {
		case op.GetRequestPut() != nil:
			put := op.GetRequestPut()
			if put.IgnoreLease || put.IgnoreValue {
				return nil, false, nil
			}
			if _, duplicate := plan.seen[string(put.Key)]; duplicate {
				return nil, false, nil
			}
			plan.seen[string(put.Key)] = struct{}{}
			writeResp := &etcdserverpb.ResponseOp{}
			resp.Responses = append(resp.Responses, writeResp)
			plan.responses = append(plan.responses, writeResp)
			plan.requests = append(plan.requests, op)
			plan.writes = append(plan.writes, backend.TxnWriteOp{Key: put.Key, Value: put.Value, Lease: put.Lease})
			plan.prevKvs = append(plan.prevKvs, put.PrevKv)
		case op.GetRequestDeleteRange() != nil:
			del := op.GetRequestDeleteRange()
			if len(del.RangeEnd) != 0 {
				return nil, false, nil
			}
			if _, duplicate := plan.seen[string(del.Key)]; duplicate {
				return nil, false, nil
			}
			plan.seen[string(del.Key)] = struct{}{}
			writeResp := &etcdserverpb.ResponseOp{}
			resp.Responses = append(resp.Responses, writeResp)
			plan.responses = append(plan.responses, writeResp)
			plan.requests = append(plan.requests, op)
			plan.writes = append(plan.writes, backend.TxnWriteOp{Delete: true, Key: del.Key})
			plan.prevKvs = append(plan.prevKvs, del.PrevKv)
		case op.GetRequestTxn() != nil:
			nested, ok, err := buildAtomicTxnPlan(op.GetRequestTxn(), cur, plan)
			if err != nil || !ok {
				return nil, ok, err
			}
			resp.Responses = append(resp.Responses, &etcdserverpb.ResponseOp{Response: &etcdserverpb.ResponseOp_ResponseTxn{ResponseTxn: nested}})
		default:
			return nil, false, nil
		}
	}
	return resp, true, nil
}

func stampTxnResponseHeaders(resp *etcdserverpb.TxnResponse, revision int64) {
	resp.Header = txnHeader(revision)
}

// txnComparePathsGuarded additionally returns OCC guards for every compare that
// selected the top-level and recursively selected nested branches. The atomic
// path submits all of them with the flattened writes in one storage batch.
func (s *RPCServer) txnComparePathsGuarded(ctx context.Context, txn *etcdserverpb.TxnRequest) ([]bool, []backend.TxnGuard, error) {
	return s.txnComparePathsGuardedAtRevision(ctx, txn, 0)
}

func (s *RPCServer) txnComparePathsGuardedAtRevision(ctx context.Context, txn *etcdserverpb.TxnRequest, revision int64) ([]bool, []backend.TxnGuard, error) {
	succeeded := true
	var guards []backend.TxnGuard
	for _, cmp := range txn.Compare {
		ok, guard, err := s.evalCompareGuardedAtRevision(ctx, cmp, revision)
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
		nestedPaths, nestedGuards, err := s.txnComparePathsGuardedAtRevision(ctx, nested, revision)
		if err != nil {
			return nil, nil, err
		}
		paths = append(paths, nestedPaths...)
		guards = append(guards, nestedGuards...)
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
			if err := validateTxnRangeRevision(r, compactRevision, currentRevision); err != nil {
				return err
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

func validateTxnRangeRevision(r *etcdserverpb.RangeRequest, compactRevision, currentRevision int64) error {
	if r.Revision < -1 || (r.Revision < 0 && compactRevision > 0) {
		return compactedRevisionError()
	}
	if r.Revision > 0 && r.Revision < compactRevision {
		return compactedRevisionError()
	}
	if r.Revision > currentRevision {
		return futureRevisionError()
	}
	return nil
}

func (s *RPCServer) validateTxnExecutionOrder(
	ctx context.Context,
	txn *etcdserverpb.TxnRequest,
	paths []bool,
	compactRevision, currentRevision int64,
) error {
	return s.validateTxnExecutionOrderCursor(
		ctx, txn, &txnPathCursor{paths: paths}, compactRevision, currentRevision,
	)
}

func (s *RPCServer) validateTxnExecutionOrderCursor(
	ctx context.Context,
	txn *etcdserverpb.TxnRequest,
	cur *txnPathCursor,
	compactRevision, currentRevision int64,
) error {
	succeeded, err := cur.next()
	if err != nil {
		return err
	}
	ops := txn.Success
	if !succeeded {
		ops = txn.Failure
	}
	for _, op := range ops {
		switch {
		case op.GetRequestRange() != nil:
			if err := validateTxnRangeRevision(op.GetRequestRange(), compactRevision, currentRevision); err != nil {
				return err
			}
		case op.GetRequestPut() != nil:
			put := op.GetRequestPut()
			if err := s.ensureLeaseExists(put.Lease); err != nil {
				return err
			}
			if put.IgnoreValue || put.IgnoreLease {
				resp, err := s.backend.Get(ctx, &etcdserverpb.RangeRequest{Key: put.Key})
				if err != nil {
					return err
				}
				if len(resp.Kvs) == 0 {
					return txnKeyNotFoundError()
				}
			}
		case op.GetRequestTxn() != nil:
			if err := s.validateTxnExecutionOrderCursor(
				ctx, op.GetRequestTxn(), cur, compactRevision, currentRevision,
			); err != nil {
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
	// etcd commits every op of a txn at ONE revision and stamps that revision on
	// the txn header. KubeBrain applies ops sequentially, so the closest correct
	// header is the revision of the LAST WRITE — not whatever the final op's
	// header happens to say. Letting a trailing read overwrite the header broke
	// etcd's concurrency.Mutex (#78): its acquire txn is Then(OpPut(lockKey),
	// OpGet(owner-prefix)); the read's current-revision header made
	// myRev > lockKey.CreateRevision whenever ANY write interleaved (lease
	// keepalives suffice), so the mutex mistook its own key for a predecessor
	// and waited on it forever.
	var lastWriteRev int64
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
			s.bindKeyToLease(ctx, put.Lease, string(put.Key))
			resp.Header = putResp.Header
			if putResp.Header != nil && putResp.Header.Revision > lastWriteRev {
				lastWriteRev = putResp.Header.Revision
			}
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
					return nil, readBarrierStatusErr(err)
				}
			}
			deleteResp := s.emptyDeleteRangeResponse()
			if !isEmptyNonFromKeyRange(del.Key, del.RangeEnd) {
				deletedKeys, err := s.keysInDeleteRange(ctx, del)
				if err != nil {
					return nil, err
				}
				deleteResp, err = s.deleteRangeWithAttachments(ctx, del, deletedKeys)
				if err != nil {
					return nil, err
				}
			}
			resp.Header = deleteResp.Header
			if deleteResp.Header != nil && deleteResp.Header.Revision > lastWriteRev {
				lastWriteRev = deleteResp.Header.Revision
			}
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
	if lastWriteRev > 0 {
		resp.Header = txnHeader(lastWriteRev)
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
	if errors.Is(err, backend.ErrNoSpace) {
		return rpctypes.ErrGRPCNoSpace
	}
	if errors.Is(err, backend.ErrQuotaUninitialized) {
		return status.Error(codes.Unavailable, "quota usage is not initialized")
	}
	if errors.Is(err, backend.ErrLeadershipFenced) {
		return status.Errorf(codes.Unavailable, "write rejected: leadership changed during commit, retry on current leader")
	}
	if errors.Is(err, backend.ErrInternalWriteGuardConflict) {
		return rpctypes.ErrAuthOldRevision
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

// observeForwardedRevision keeps a serving follower's committed watermark at
// least as fresh as a successful response it returned to the same client. It
// deliberately does not advance the published watch watermark: event delivery
// remains responsible for proving that a revision is safe for watch progress.
func (s *RPCServer) observeForwardedRevision(header *etcdserverpb.ResponseHeader, err error) {
	if err == nil && header != nil && header.Revision > 0 {
		s.backend.SetCurrentRevision(uint64(header.Revision))
	}
}

func safeBackendRevision(ctx context.Context, backend BackendShim) (uint64, error) {
	currentRevision := backend.GetCurrentRevision()
	// Latest serializable reads may execute directly on a cold follower without
	// a leader barrier. Recover the shared user watermark before consulting the
	// compact lower bound; compaction can legitimately lag far behind the latest
	// snapshot and therefore cannot identify the response header on its own.
	if currentRevision == 0 {
		durableRevision, err := backend.GetDurableRevision(ctx)
		if err != nil && !errors.Is(err, storage.ErrKeyNotFound) {
			return 0, err
		}
		if err == nil && durableRevision > currentRevision {
			currentRevision = durableRevision
			backend.SetCurrentRevision(currentRevision)
		}
	}
	compactRevision, err := backend.GetCompactRevision(ctx)
	if err != nil {
		return 0, err
	}
	// Match backend.safeCurrentRevision: compaction itself never creates an
	// MVCC revision. Catch a cold cache up to a proven compact watermark, while
	// preserving compact==current exactly; only an uninitialized empty store is
	// normalized to etcd's initial revision 1.
	if currentRevision == 0 && compactRevision == 0 {
		currentRevision = 1
		backend.SetCurrentRevision(currentRevision)
	} else if compactRevision > currentRevision {
		currentRevision = compactRevision
		backend.SetCurrentRevision(currentRevision)
	}
	return currentRevision, nil
}
