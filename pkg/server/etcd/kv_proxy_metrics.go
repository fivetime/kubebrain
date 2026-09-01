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
	"bytes"
	"fmt"

	"go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/mvccpb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/kubewharf/kubebrain/pkg/backend"
	"github.com/kubewharf/kubebrain/pkg/metrics"
)

func validateRangeProxyPayload(metricCli metrics.Metrics, request *etcdserverpb.RangeRequest, response *etcdserverpb.RangeResponse, err error) (*etcdserverpb.RangeResponse, error) {
	if err != nil {
		return response, err
	}
	fail := func(message string) (*etcdserverpb.RangeResponse, error) {
		emitKVProxyIntegrityFailure(metricCli, kvProxyRPCRange)
		return nil, status.Error(codes.DataLoss, message)
	}
	if response.GetCount() < 0 || response.GetCount() < int64(len(response.GetKvs())) {
		return fail(fmt.Sprintf("leader range proxy returned count %d for %d key-values", response.GetCount(), len(response.GetKvs())))
	}
	if request.GetRevision() > 0 && response.GetHeader().GetRevision() < request.GetRevision() {
		return fail(fmt.Sprintf("leader range proxy returned header revision %d below requested revision %d", response.GetHeader().GetRevision(), request.GetRevision()))
	}
	if request.GetCountOnly() {
		if len(response.GetKvs()) != 0 || response.GetMore() {
			return fail("leader range proxy returned key-values or more=true for a count-only request")
		}
		return response, nil
	}
	if request.GetLimit() <= 0 && response.GetMore() {
		return fail("leader range proxy returned more=true for an unlimited request")
	}
	if request.GetLimit() > 0 && int64(len(response.GetKvs())) > request.GetLimit() {
		return fail("leader range proxy returned more key-values than the requested limit")
	}
	if response.GetMore() && int64(len(response.GetKvs())) != request.GetLimit() {
		return fail("leader range proxy returned a non-full page with more=true")
	}
	if request.GetMinModRevision() == 0 && request.GetMaxModRevision() == 0 &&
		request.GetMinCreateRevision() == 0 && request.GetMaxCreateRevision() == 0 &&
		response.GetMore() != (response.GetCount() > int64(len(response.GetKvs()))) {
		return fail("leader range proxy returned inconsistent count and more metadata")
	}
	seen := make(map[string]struct{}, len(response.GetKvs()))
	for index, kv := range response.GetKvs() {
		if kv == nil {
			return fail("leader range proxy returned a nil key-value")
		}
		if !rangeProxyContainsKey(request.GetKey(), request.GetRangeEnd(), kv.GetKey()) {
			return fail("leader range proxy returned a key outside the requested range")
		}
		if _, exists := seen[string(kv.GetKey())]; exists {
			return fail("leader range proxy returned a duplicate key")
		}
		seen[string(kv.GetKey())] = struct{}{}
		if request.GetKeysOnly() && len(kv.GetValue()) != 0 {
			return fail("leader range proxy returned a value for a keys-only request")
		}
		if validateProxyKeyValueLifecycle(kv) != nil {
			return fail("leader range proxy returned invalid key-value revision metadata")
		}
		snapshotRevision := response.GetHeader().GetRevision()
		if request.GetRevision() > 0 {
			snapshotRevision = request.GetRevision()
		}
		if kv.GetModRevision() > snapshotRevision {
			return fail("leader range proxy returned a key-value newer than the requested snapshot")
		}
		if (request.GetMinModRevision() > 0 && kv.GetModRevision() < request.GetMinModRevision()) ||
			(request.GetMaxModRevision() > 0 && kv.GetModRevision() > request.GetMaxModRevision()) ||
			(request.GetMinCreateRevision() > 0 && kv.GetCreateRevision() < request.GetMinCreateRevision()) ||
			(request.GetMaxCreateRevision() > 0 && kv.GetCreateRevision() > request.GetMaxCreateRevision()) {
			return fail("leader range proxy returned a key-value outside the requested revision filters")
		}
		if index > 0 && !rangeProxyOrderValid(request, response.GetKvs()[index-1], kv) {
			return fail("leader range proxy returned key-values outside the requested sort order")
		}
	}
	return response, nil
}

func validatePutProxyPayload(metricCli metrics.Metrics, request *etcdserverpb.PutRequest, response *etcdserverpb.PutResponse, err error) (*etcdserverpb.PutResponse, error) {
	if err != nil {
		return response, err
	}
	fail := func(message string) (*etcdserverpb.PutResponse, error) {
		emitKVProxyIntegrityFailure(metricCli, kvProxyRPCPut)
		return nil, status.Error(codes.DataLoss, message)
	}
	if response.GetHeader().GetRevision() <= 0 {
		return fail("leader put proxy returned a non-positive write revision")
	}
	previous := response.GetPrevKv()
	if !request.GetPrevKv() {
		if previous != nil {
			return fail("leader put proxy returned a previous key-value when none was requested")
		}
		return response, nil
	}
	if previous == nil {
		if request.GetIgnoreValue() || request.GetIgnoreLease() {
			return fail("leader put proxy omitted the required previous key-value for an ignore request")
		}
		return response, nil
	}
	if !bytes.Equal(previous.GetKey(), request.GetKey()) {
		return fail("leader put proxy returned a previous key-value for a different key")
	}
	if validateProxyKeyValueLifecycle(previous) != nil {
		return fail("leader put proxy returned invalid previous key-value revision metadata")
	}
	if previous.GetModRevision() >= response.GetHeader().GetRevision() {
		return fail("leader put proxy returned a previous key-value not older than the write revision")
	}
	return response, nil
}

func validateDeleteRangeProxyPayload(metricCli metrics.Metrics, request *etcdserverpb.DeleteRangeRequest, response *etcdserverpb.DeleteRangeResponse, err error) (*etcdserverpb.DeleteRangeResponse, error) {
	if err != nil {
		return response, err
	}
	fail := func(message string) (*etcdserverpb.DeleteRangeResponse, error) {
		emitKVProxyIntegrityFailure(metricCli, kvProxyRPCDeleteRange)
		return nil, status.Error(codes.DataLoss, message)
	}
	if response.GetHeader().GetRevision() <= 0 {
		return fail("leader delete_range proxy returned a non-positive write revision")
	}
	if response.GetDeleted() < 0 {
		return fail("leader delete_range proxy returned a negative deleted count")
	}
	if !request.GetPrevKv() {
		if len(response.GetPrevKvs()) != 0 {
			return fail("leader delete_range proxy returned previous key-values when none were requested")
		}
		return response, nil
	}
	if int64(len(response.GetPrevKvs())) != response.GetDeleted() {
		return fail("leader delete_range proxy returned a previous key-value count different from deleted")
	}
	for index, previous := range response.GetPrevKvs() {
		if previous == nil {
			return fail("leader delete_range proxy returned a nil previous key-value")
		}
		if !rangeProxyContainsKey(request.GetKey(), request.GetRangeEnd(), previous.GetKey()) {
			return fail("leader delete_range proxy returned a previous key-value outside the requested range")
		}
		if validateProxyKeyValueLifecycle(previous) != nil {
			return fail("leader delete_range proxy returned invalid previous key-value revision metadata")
		}
		if previous.GetModRevision() >= response.GetHeader().GetRevision() {
			return fail("leader delete_range proxy returned a previous key-value not older than the delete revision")
		}
		if index > 0 && bytes.Compare(response.GetPrevKvs()[index-1].GetKey(), previous.GetKey()) >= 0 {
			return fail("leader delete_range proxy returned duplicate or unsorted previous keys")
		}
	}
	return response, nil
}

func validateCompactProxyPayload(metricCli metrics.Metrics, request *etcdserverpb.CompactionRequest, response *etcdserverpb.CompactionResponse, err error) (*etcdserverpb.CompactionResponse, error) {
	if err != nil {
		return response, err
	}
	fail := func(message string) (*etcdserverpb.CompactionResponse, error) {
		emitKVProxyIntegrityFailure(metricCli, kvProxyRPCCompact)
		return nil, status.Error(codes.DataLoss, message)
	}
	revision := response.GetHeader().GetRevision()
	if revision <= 0 {
		return fail("leader compact proxy returned a non-positive current revision")
	}
	if revision < request.GetRevision() {
		return fail(fmt.Sprintf("leader compact proxy returned current revision %d below compacted revision %d", revision, request.GetRevision()))
	}
	return response, nil
}

func validateTxnProxyPayload(metricCli metrics.Metrics, request *etcdserverpb.TxnRequest, response *etcdserverpb.TxnResponse, err error) (*etcdserverpb.TxnResponse, error) {
	if err != nil {
		return response, err
	}
	if response.GetHeader().GetRevision() <= 0 {
		emitKVProxyIntegrityFailure(metricCli, kvProxyRPCTxn)
		return nil, status.Error(codes.DataLoss, "leader txn proxy returned a non-positive response revision")
	}
	if validationErr := validateTxnProxyResponseTree(request, response); validationErr != nil {
		emitKVProxyIntegrityFailure(metricCli, kvProxyRPCTxn)
		return nil, status.Error(codes.DataLoss, validationErr.Error())
	}
	if validationErr := validateTxnProxyResponseHeaders(response, response.GetHeader().GetRevision(), true); validationErr != nil {
		emitKVProxyIntegrityFailure(metricCli, kvProxyRPCTxn)
		return nil, status.Error(codes.DataLoss, validationErr.Error())
	}
	if validationErr := validateTxnProxyOperationPayloads(request, response, response.GetHeader().GetRevision()); validationErr != nil {
		emitKVProxyIntegrityFailure(metricCli, kvProxyRPCTxn)
		return nil, status.Error(codes.DataLoss, validationErr.Error())
	}
	return response, nil
}

func validateTxnProxyResponseTree(request *etcdserverpb.TxnRequest, response *etcdserverpb.TxnResponse) error {
	// Upstream applyCompares returns true for an empty compare list, and
	// compareToPath applies that rule independently to every nested transaction.
	if len(request.GetCompare()) == 0 && !response.GetSucceeded() {
		return fmt.Errorf("leader txn proxy selected the failure branch without compares")
	}
	requests := request.GetFailure()
	if response.GetSucceeded() {
		requests = request.GetSuccess()
	}
	if len(response.GetResponses()) != len(requests) {
		return fmt.Errorf("leader txn proxy returned %d response operations for selected branch with %d requests", len(response.GetResponses()), len(requests))
	}
	for index, requestOp := range requests {
		responseOp := response.GetResponses()[index]
		if requestOp == nil || responseOp == nil {
			return fmt.Errorf("leader txn proxy returned a nil request/response operation at index %d", index)
		}
		switch requestUnion := requestOp.GetRequest().(type) {
		case *etcdserverpb.RequestOp_RequestRange:
			responseUnion, ok := responseOp.GetResponse().(*etcdserverpb.ResponseOp_ResponseRange)
			if !ok || responseUnion.ResponseRange == nil {
				return fmt.Errorf("leader txn proxy returned a non-range response for range request at index %d", index)
			}
		case *etcdserverpb.RequestOp_RequestPut:
			responseUnion, ok := responseOp.GetResponse().(*etcdserverpb.ResponseOp_ResponsePut)
			if !ok || responseUnion.ResponsePut == nil {
				return fmt.Errorf("leader txn proxy returned a non-put response for put request at index %d", index)
			}
		case *etcdserverpb.RequestOp_RequestDeleteRange:
			responseUnion, ok := responseOp.GetResponse().(*etcdserverpb.ResponseOp_ResponseDeleteRange)
			if !ok || responseUnion.ResponseDeleteRange == nil {
				return fmt.Errorf("leader txn proxy returned a non-delete response for delete request at index %d", index)
			}
		case *etcdserverpb.RequestOp_RequestTxn:
			responseUnion, ok := responseOp.GetResponse().(*etcdserverpb.ResponseOp_ResponseTxn)
			if !ok || requestUnion.RequestTxn == nil || responseUnion.ResponseTxn == nil {
				return fmt.Errorf("leader txn proxy returned a non-txn response for txn request at index %d", index)
			}
			if err := validateTxnProxyResponseTree(requestUnion.RequestTxn, responseUnion.ResponseTxn); err != nil {
				return err
			}
		default:
			return fmt.Errorf("leader txn proxy selected an unknown request operation at index %d", index)
		}
	}
	return nil
}

func validateTxnProxyResponseHeaders(response *etcdserverpb.TxnResponse, outerRevision int64, root bool) error {
	if response.GetHeader() == nil {
		return fmt.Errorf("leader txn proxy returned a txn response without a header")
	}
	if !root && response.GetHeader().GetRevision() != 0 {
		return fmt.Errorf("leader txn proxy returned nested txn revision %d instead of zero", response.GetHeader().GetRevision())
	}
	for index, responseOp := range response.GetResponses() {
		var header *etcdserverpb.ResponseHeader
		switch {
		case responseOp.GetResponseRange() != nil:
			header = responseOp.GetResponseRange().GetHeader()
		case responseOp.GetResponsePut() != nil:
			header = responseOp.GetResponsePut().GetHeader()
		case responseOp.GetResponseDeleteRange() != nil:
			header = responseOp.GetResponseDeleteRange().GetHeader()
		case responseOp.GetResponseTxn() != nil:
			if err := validateTxnProxyResponseHeaders(responseOp.GetResponseTxn(), outerRevision, false); err != nil {
				return err
			}
			continue
		}
		if header == nil {
			return fmt.Errorf("leader txn proxy returned response operation without a header at index %d", index)
		}
		revision := header.GetRevision()
		if revision <= 0 || revision > outerRevision || revision < outerRevision-1 {
			return fmt.Errorf("leader txn proxy returned response operation revision %d outside outer revision window [%d,%d] at index %d", revision, outerRevision-1, outerRevision, index)
		}
	}
	return nil
}

func validateTxnProxyOperationPayloads(request *etcdserverpb.TxnRequest, response *etcdserverpb.TxnResponse, outerRevision int64) error {
	requests := request.GetFailure()
	if response.GetSucceeded() {
		requests = request.GetSuccess()
	}
	for index, requestOp := range requests {
		responseOp := response.GetResponses()[index]
		switch {
		case requestOp.GetRequestRange() != nil:
			if _, err := validateRangeProxyPayload(nil, requestOp.GetRequestRange(), responseOp.GetResponseRange(), nil); err != nil {
				return fmt.Errorf("leader txn proxy returned invalid range payload at index %d: %s", index, status.Convert(err).Message())
			}
		case requestOp.GetRequestPut() != nil:
			if err := validateTxnProxyPutPayload(requestOp.GetRequestPut(), responseOp.GetResponsePut(), outerRevision); err != nil {
				return fmt.Errorf("leader txn proxy returned invalid put payload at index %d: %w", index, err)
			}
		case requestOp.GetRequestDeleteRange() != nil:
			if err := validateTxnProxyDeletePayload(requestOp.GetRequestDeleteRange(), responseOp.GetResponseDeleteRange(), outerRevision); err != nil {
				return fmt.Errorf("leader txn proxy returned invalid delete payload at index %d: %w", index, err)
			}
		case requestOp.GetRequestTxn() != nil:
			if err := validateTxnProxyOperationPayloads(requestOp.GetRequestTxn(), responseOp.GetResponseTxn(), outerRevision); err != nil {
				return err
			}
		}
	}
	return nil
}

func validateTxnProxyPutPayload(request *etcdserverpb.PutRequest, response *etcdserverpb.PutResponse, outerRevision int64) error {
	if response.GetHeader().GetRevision() != outerRevision {
		return fmt.Errorf("put revision %d differs from outer revision %d", response.GetHeader().GetRevision(), outerRevision)
	}
	previous := response.GetPrevKv()
	if !request.GetPrevKv() {
		if previous != nil {
			return fmt.Errorf("put returned an unrequested previous key-value")
		}
		return nil
	}
	if previous == nil {
		if request.GetIgnoreValue() || request.GetIgnoreLease() {
			return fmt.Errorf("put omitted the required previous key-value for an ignore request")
		}
		return nil
	}
	if !bytes.Equal(previous.GetKey(), request.GetKey()) {
		return fmt.Errorf("put returned a previous key-value for a different key")
	}
	if validateProxyKeyValueLifecycle(previous) != nil {
		return fmt.Errorf("put returned invalid previous key-value revision metadata")
	}
	// etcd reads PrevKV from the transaction's pre-write snapshot and rejects
	// overlapping writes, while any effective Put advances the outer revision.
	if previous.GetModRevision() >= outerRevision {
		return fmt.Errorf("put returned a previous key-value not older than the transaction revision")
	}
	return nil
}

func validateTxnProxyDeletePayload(request *etcdserverpb.DeleteRangeRequest, response *etcdserverpb.DeleteRangeResponse, outerRevision int64) error {
	if response.GetDeleted() < 0 {
		return fmt.Errorf("delete returned a negative deleted count")
	}
	if response.GetDeleted() > 0 && response.GetHeader().GetRevision() != outerRevision {
		return fmt.Errorf("effective delete revision %d differs from outer revision %d", response.GetHeader().GetRevision(), outerRevision)
	}
	if !request.GetPrevKv() {
		if len(response.GetPrevKvs()) != 0 {
			return fmt.Errorf("delete returned unrequested previous key-values")
		}
		return nil
	}
	if int64(len(response.GetPrevKvs())) != response.GetDeleted() {
		return fmt.Errorf("delete previous key-value count differs from deleted")
	}
	for index, previous := range response.GetPrevKvs() {
		if previous == nil {
			return fmt.Errorf("delete returned a nil previous key-value")
		}
		if !rangeProxyContainsKey(request.GetKey(), request.GetRangeEnd(), previous.GetKey()) {
			return fmt.Errorf("delete returned a previous key-value outside the requested range")
		}
		if validateProxyKeyValueLifecycle(previous) != nil {
			return fmt.Errorf("delete returned invalid previous key-value revision metadata")
		}
		// An effective delete advances the outer revision after collecting its
		// previous values from the same pre-write transaction snapshot.
		if previous.GetModRevision() >= outerRevision {
			return fmt.Errorf("delete returned a previous key-value not older than the transaction revision")
		}
		if index > 0 && bytes.Compare(response.GetPrevKvs()[index-1].GetKey(), previous.GetKey()) >= 0 {
			return fmt.Errorf("delete returned duplicate or unsorted previous keys")
		}
	}
	return nil
}

func validateProxyKeyValueLifecycle(kv *mvccpb.KeyValue) error {
	if kv.GetCreateRevision() <= 0 || kv.GetModRevision() <= 0 || kv.GetVersion() <= 0 {
		return backend.ErrInvalidMVCCMetadata
	}
	return backend.ValidateEtcdMetadataAtRevision(backend.EtcdMetadata{
		CreateRevision: uint64(kv.GetCreateRevision()), Version: uint64(kv.GetVersion()), Lease: kv.GetLease(),
	}, uint64(kv.GetModRevision()), "leader proxy key-value")
}

func rangeProxyOrderValid(request *etcdserverpb.RangeRequest, previous, current *mvccpb.KeyValue) bool {
	// Upstream sorts by the full value before KeysOnly projection. That ordering
	// cannot be reconstructed from the intentionally elided response values.
	if request.GetKeysOnly() && request.GetSortTarget() == etcdserverpb.RangeRequest_VALUE {
		return true
	}
	comparison := 0
	switch request.GetSortTarget() {
	case etcdserverpb.RangeRequest_KEY:
		comparison = bytes.Compare(previous.GetKey(), current.GetKey())
	case etcdserverpb.RangeRequest_VERSION:
		comparison = compareOrderedInt64(previous.GetVersion(), current.GetVersion())
	case etcdserverpb.RangeRequest_CREATE:
		comparison = compareOrderedInt64(previous.GetCreateRevision(), current.GetCreateRevision())
	case etcdserverpb.RangeRequest_MOD:
		comparison = compareOrderedInt64(previous.GetModRevision(), current.GetModRevision())
	case etcdserverpb.RangeRequest_VALUE:
		comparison = bytes.Compare(previous.GetValue(), current.GetValue())
	}
	order := request.GetSortOrder()
	if order == etcdserverpb.RangeRequest_NONE {
		order = etcdserverpb.RangeRequest_ASCEND
	}
	if order == etcdserverpb.RangeRequest_DESCEND {
		return comparison >= 0
	}
	return comparison <= 0
}

func compareOrderedInt64(left, right int64) int {
	if left < right {
		return -1
	}
	if left > right {
		return 1
	}
	return 0
}

func rangeProxyContainsKey(start, end, key []byte) bool {
	if len(key) == 0 || bytes.Compare(key, start) < 0 {
		return false
	}
	if len(end) == 0 {
		return bytes.Equal(key, start)
	}
	if len(end) == 1 && end[0] == 0 {
		return true
	}
	return bytes.Compare(key, end) < 0
}

const (
	kvProxyRPCRange       = "range"
	kvProxyRPCTxn         = "txn"
	kvProxyRPCPut         = "put"
	kvProxyRPCDeleteRange = "delete_range"
	kvProxyRPCCompact     = "compact"
)

var kvProxyRPCs = []string{
	kvProxyRPCRange,
	kvProxyRPCTxn,
	kvProxyRPCPut,
	kvProxyRPCDeleteRange,
	kvProxyRPCCompact,
}

func initKVProxyIntegrityMetrics(metricCli metrics.Metrics) {
	if metricCli == nil {
		return
	}
	for _, rpc := range kvProxyRPCs {
		_ = metricCli.EmitCounter("kv.proxy.integrity_failure", int64(0), metrics.Tag("rpc", rpc))
	}
}

func validateKVProxyResult[T any](metricCli metrics.Metrics, identity proxyResponseIdentity, rpc string, response *T, err error) (*T, error) {
	if (response == nil) != (err == nil) {
		if response != nil {
			if issue := validateProxyResponseHeader(response, identity, proxyResponseRevisionPositive); issue != "" {
				emitKVProxyIntegrityFailure(metricCli, rpc)
				return nil, status.Error(codes.DataLoss, fmt.Sprintf("leader %s proxy returned a response %s", rpc, issue))
			}
		}
		return response, err
	}
	emitKVProxyIntegrityFailure(metricCli, rpc)
	if response == nil {
		return nil, status.Error(codes.DataLoss, fmt.Sprintf("leader %s proxy returned neither response nor error", rpc))
	}
	return nil, status.Error(codes.DataLoss, fmt.Sprintf("leader %s proxy returned both response and error", rpc))
}

func emitKVProxyIntegrityFailure(metricCli metrics.Metrics, rpc string) {
	if metricCli != nil {
		_ = metricCli.EmitCounter("kv.proxy.integrity_failure", 1, metrics.Tag("rpc", rpc))
	}
}
