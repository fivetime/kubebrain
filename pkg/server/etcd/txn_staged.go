// Copyright 2026 ByteDance and/or its affiliates
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
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
	"math"
	"sort"

	"go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/mvccpb"
	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
	"google.golang.org/protobuf/proto"

	"github.com/kubewharf/kubebrain/pkg/backend"
)

type stagedMutation struct {
	op backend.TxnWriteOp
}

type stagedTxnExecutor struct {
	srv        *RPCServer
	ctx        context.Context
	baseRev    int64
	pendingRev int64
	paths      *txnPathCursor
	guards     []backend.TxnGuard

	mutations map[string]*stagedMutation
	order     []string
	changed   bool
}

func (s *RPCServer) executeStagedGenericTxn(ctx context.Context, txn *etcdserverpb.TxnRequest, paths []bool, guards []backend.TxnGuard) (*etcdserverpb.TxnResponse, error) {
	base := int64(s.backend.GetCurrentRevision())
	return s.executeStagedGenericTxnAtRevision(ctx, txn, paths, guards, base)
}

func (s *RPCServer) executeStagedGenericTxnAtRevision(ctx context.Context, txn *etcdserverpb.TxnRequest, paths []bool, guards []backend.TxnGuard, base int64) (*etcdserverpb.TxnResponse, error) {
	e := &stagedTxnExecutor{
		srv:        s,
		ctx:        ctx,
		baseRev:    base,
		pendingRev: base + 1,
		paths:      &txnPathCursor{paths: paths},
		guards:     guards,
		mutations:  make(map[string]*stagedMutation),
	}
	resp, err := e.execute(txn)
	if err != nil {
		return nil, err
	}
	if len(e.order) == 0 {
		stampTxnResponseHeaders(resp, e.baseRev)
		return resp, nil
	}

	writes := make([]backend.TxnWriteOp, 0, len(e.order))
	for _, key := range e.order {
		writes = append(writes, e.mutations[key].op)
	}
	writes, userCount := s.withLeaseAttachmentOps(writes)
	_, revision, results, err := s.backend.TxnApply(ctx, writes, guards, make([]bool, len(writes)))
	if err != nil {
		s.reconcileLeaseIndexesAfterUncertain(err, revision, writes, userCount)
		return nil, err
	}
	rewriteTxnRevision(resp, e.pendingRev, int64(revision))
	stampTxnResponseHeaders(resp, int64(revision))
	s.applyLeaseIndexes(writes, results, userCount)
	return resp, nil
}

func (e *stagedTxnExecutor) execute(txn *etcdserverpb.TxnRequest) (*etcdserverpb.TxnResponse, error) {
	succeeded, err := e.paths.next()
	if err != nil {
		return nil, err
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
		case op.GetRequestRange() != nil:
			rangeResp, err := e.rangeResponse(op.GetRequestRange())
			if err != nil {
				return nil, err
			}
			resp.Responses = append(resp.Responses, &etcdserverpb.ResponseOp{Response: &etcdserverpb.ResponseOp_ResponseRange{ResponseRange: rangeResp}})
		case op.GetRequestPut() != nil:
			putResp, err := e.put(op.GetRequestPut())
			if err != nil {
				return nil, err
			}
			resp.Responses = append(resp.Responses, &etcdserverpb.ResponseOp{Response: &etcdserverpb.ResponseOp_ResponsePut{ResponsePut: putResp}})
		case op.GetRequestDeleteRange() != nil:
			deleteResp, err := e.deleteRange(op.GetRequestDeleteRange())
			if err != nil {
				return nil, err
			}
			resp.Responses = append(resp.Responses, &etcdserverpb.ResponseOp{Response: &etcdserverpb.ResponseOp_ResponseDeleteRange{ResponseDeleteRange: deleteResp}})
		case op.GetRequestTxn() != nil:
			nested, err := e.execute(op.GetRequestTxn())
			if err != nil {
				return nil, err
			}
			resp.Responses = append(resp.Responses, &etcdserverpb.ResponseOp{Response: &etcdserverpb.ResponseOp_ResponseTxn{ResponseTxn: nested}})
		default:
			return nil, txnKeyNotFoundError()
		}
	}
	return resp, nil
}

func (e *stagedTxnExecutor) visibleRevision() int64 {
	if e.changed {
		return e.pendingRev
	}
	return e.baseRev
}

func (e *stagedTxnExecutor) currentPoint(key []byte) (*mvccpb.KeyValue, error) {
	if mutation, ok := e.mutations[string(key)]; ok {
		if mutation.op.Delete {
			return nil, nil
		}
		return e.stagedPutKV(mutation.op)
	}
	resp, err := e.srv.backend.Get(e.ctx, &etcdserverpb.RangeRequest{Key: key, Revision: e.baseRev})
	if err != nil || len(resp.Kvs) == 0 {
		return nil, err
	}
	return proto.Clone(resp.Kvs[0]).(*mvccpb.KeyValue), nil
}

func (e *stagedTxnExecutor) stagedPutKV(op backend.TxnWriteOp) (*mvccpb.KeyValue, error) {
	// Validation rejects multiple puts to the same key, so the base state is the
	// state this one staged put updates.
	baseResp, err := e.srv.backend.Get(e.ctx, &etcdserverpb.RangeRequest{Key: op.Key, Revision: e.baseRev})
	if err != nil {
		return nil, err
	}
	if len(baseResp.Kvs) != 0 {
		kv := proto.Clone(baseResp.Kvs[0]).(*mvccpb.KeyValue)
		kv.Value = append([]byte(nil), op.Value...)
		kv.ModRevision = e.pendingRev
		kv.Version++
		kv.Lease = op.Lease
		return kv, nil
	}
	return &mvccpb.KeyValue{
		Key:            append([]byte(nil), op.Key...),
		Value:          append([]byte(nil), op.Value...),
		CreateRevision: e.pendingRev,
		ModRevision:    e.pendingRev,
		Version:        1,
		Lease:          op.Lease,
	}, nil
}

func (e *stagedTxnExecutor) put(r *etcdserverpb.PutRequest) (*etcdserverpb.PutResponse, error) {
	if r.IgnoreValue || r.IgnoreLease {
		if err := e.srv.ensureLeaseExists(r.Lease); err != nil {
			return nil, err
		}
	}
	current, err := e.currentPoint(r.Key)
	if err != nil {
		return nil, err
	}
	if (r.IgnoreValue || r.IgnoreLease) && current == nil {
		return nil, txnKeyNotFoundError()
	}
	value := r.Value
	lease := r.Lease
	if r.IgnoreValue {
		value = current.Value
	}
	if r.IgnoreLease {
		lease = current.Lease
	}
	if err := e.srv.ensureLeaseExists(lease); err != nil {
		return nil, err
	}
	resp := &etcdserverpb.PutResponse{Header: txnHeader(e.pendingRev)}
	if r.PrevKv && current != nil {
		resp.PrevKv = proto.Clone(current).(*mvccpb.KeyValue)
	}
	e.stage(backend.TxnWriteOp{Key: append([]byte(nil), r.Key...), Value: append([]byte(nil), value...), Lease: lease})
	return resp, nil
}

func (e *stagedTxnExecutor) deleteRange(r *etcdserverpb.DeleteRangeRequest) (*etcdserverpb.DeleteRangeResponse, error) {
	kvs, err := e.currentRangeLimited(r.Key, r.RangeEnd, e.srv.maxDeleteRangeKeys)
	if err != nil {
		return nil, err
	}
	if e.srv.maxDeleteRangeKeys > 0 && len(kvs) > int(e.srv.maxDeleteRangeKeys) {
		e.srv.metricCli.EmitCounter("delete_range.admission.rejected", 1)
		return nil, rpctypes.ErrGRPCRequestTooManyRequests
	}
	resp := &etcdserverpb.DeleteRangeResponse{Header: txnHeader(e.visibleRevision())}
	for _, kv := range kvs {
		if _, alreadyDeleted := e.mutations[string(kv.Key)]; alreadyDeleted && e.mutations[string(kv.Key)].op.Delete {
			continue
		}
		resp.Deleted++
		if r.PrevKv {
			resp.PrevKvs = append(resp.PrevKvs, proto.Clone(kv).(*mvccpb.KeyValue))
		}
		e.stage(backend.TxnWriteOp{Delete: true, Key: append([]byte(nil), kv.Key...)})
	}
	resp.Header = txnHeader(e.visibleRevision())
	return resp, nil
}

func (e *stagedTxnExecutor) stage(op backend.TxnWriteOp) {
	key := string(op.Key)
	if _, exists := e.mutations[key]; !exists {
		e.order = append(e.order, key)
	}
	e.mutations[key] = &stagedMutation{op: op}
	e.changed = true
}

func (e *stagedTxnExecutor) currentRange(start, end []byte) ([]*mvccpb.KeyValue, error) {
	return e.currentRangeLimited(start, end, 0)
}

func (e *stagedTxnExecutor) currentRangeLimited(start, end []byte, maxKeys uint32) ([]*mvccpb.KeyValue, error) {
	if isEmptyNonFromKeyRange(start, end) {
		return nil, nil
	}
	var resp *etcdserverpb.RangeResponse
	var err error
	request := &etcdserverpb.RangeRequest{Key: start, RangeEnd: end, Revision: e.baseRev}
	if maxKeys > 0 && len(end) != 0 {
		// A prior staged delete may remove a base key from this transaction's
		// logical view. Read one replacement for each such key plus the overflow
		// sentinel, keeping the scan bounded without rejecting a valid txn.
		limit := int64(maxKeys) + 1
		for key, mutation := range e.mutations {
			if mutation.op.Delete && txnKeyInRange([]byte(key), start, end) {
				limit++
			}
		}
		request.Limit = limit
	}
	if len(end) == 0 {
		resp, err = e.srv.backend.Get(e.ctx, request)
	} else {
		resp, err = e.srv.backend.List(e.ctx, request)
	}
	if err != nil {
		return nil, err
	}
	byKey := make(map[string]*mvccpb.KeyValue, len(resp.Kvs)+len(e.mutations))
	for _, kv := range resp.Kvs {
		byKey[string(kv.Key)] = proto.Clone(kv).(*mvccpb.KeyValue)
	}
	for key, mutation := range e.mutations {
		keyBytes := []byte(key)
		if !txnKeyInRange(keyBytes, start, end) {
			continue
		}
		if mutation.op.Delete {
			delete(byKey, key)
			continue
		}
		kv, err := e.stagedPutKV(mutation.op)
		if err != nil {
			return nil, err
		}
		byKey[key] = kv
	}
	kvs := make([]*mvccpb.KeyValue, 0, len(byKey))
	for _, kv := range byKey {
		kvs = append(kvs, kv)
	}
	sort.Slice(kvs, func(i, j int) bool { return bytes.Compare(kvs[i].Key, kvs[j].Key) < 0 })
	return kvs, nil
}

func (e *stagedTxnExecutor) rangeResponse(r *etcdserverpb.RangeRequest) (*etcdserverpb.RangeResponse, error) {
	if r.Revision > 0 {
		var resp *etcdserverpb.RangeResponse
		var err error
		if len(r.RangeEnd) == 0 {
			resp, err = e.srv.backend.Get(e.ctx, r)
		} else {
			resp, err = e.srv.backend.List(e.ctx, r)
		}
		if err == nil && resp.Header != nil {
			resp.Header.Revision = e.visibleRevision()
		}
		return resp, err
	}
	kvs, err := e.currentRange(r.Key, r.RangeEnd)
	if err != nil {
		return nil, err
	}
	resp := &etcdserverpb.RangeResponse{Header: txnHeader(e.visibleRevision()), Kvs: kvs, Count: int64(len(kvs))}
	if !needsFullRangeMaterialization(r) && needsNonKeyNoneLookahead(r) && r.Limit < math.MaxInt64 {
		candidateLimit := r.Limit + 1
		if int64(len(resp.Kvs)) > candidateLimit {
			resp.Kvs = resp.Kvs[:int(candidateLimit)]
		}
	}
	filterRangeKvs(resp, r)
	if r.CountOnly {
		resp.Kvs = nil
		return resp, nil
	}
	sortRangeKvs(resp.Kvs, r)
	if r.Limit > 0 && int64(len(resp.Kvs)) > r.Limit {
		resp.More = true
		resp.Kvs = resp.Kvs[:int(r.Limit)]
	}
	if r.KeysOnly {
		for _, kv := range resp.Kvs {
			kv.Value = nil
		}
	}
	return resp, nil
}

func txnKeyInRange(key, start, end []byte) bool {
	if bytes.Compare(key, start) < 0 {
		return false
	}
	if len(end) == 0 {
		return bytes.Equal(key, start)
	}
	return isFromKeyRangeEnd(end) || bytes.Compare(key, end) < 0
}

func rewriteTxnRevision(resp *etcdserverpb.TxnResponse, from, to int64) {
	if resp.Header != nil && resp.Header.Revision == from {
		resp.Header.Revision = to
	}
	for _, op := range resp.Responses {
		switch {
		case op.GetResponseRange() != nil:
			r := op.GetResponseRange()
			if r.Header != nil && r.Header.Revision == from {
				r.Header.Revision = to
			}
			for _, kv := range r.Kvs {
				if kv.CreateRevision == from {
					kv.CreateRevision = to
				}
				if kv.ModRevision == from {
					kv.ModRevision = to
				}
			}
		case op.GetResponsePut() != nil:
			if h := op.GetResponsePut().Header; h != nil && h.Revision == from {
				h.Revision = to
			}
		case op.GetResponseDeleteRange() != nil:
			if h := op.GetResponseDeleteRange().Header; h != nil && h.Revision == from {
				h.Revision = to
			}
		case op.GetResponseTxn() != nil:
			rewriteTxnRevision(op.GetResponseTxn(), from, to)
		}
	}
}
