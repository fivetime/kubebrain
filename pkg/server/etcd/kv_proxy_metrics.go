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
	if violation := rangeIntervalCardinalityViolation(
		request, response.GetCount(), int64(len(response.GetKvs())), response.GetMore(),
	); violation != "" {
		return fail("leader range proxy returned " + violation)
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

func rangeIntervalCardinalityViolation(request *etcdserverpb.RangeRequest, count, kvCount int64, more bool) string {
	if len(request.GetRangeEnd()) == 0 {
		if count > 1 {
			return fmt.Sprintf("count %d above exact-key cardinality", count)
		}
		if more {
			return "more=true for an exact-key request"
		}
		return ""
	}
	if isEmptyNonFromKeyRange(request.GetKey(), request.GetRangeEnd()) &&
		(count != 0 || kvCount != 0 || more) {
		return "non-empty metadata for an empty requested range"
	}
	return ""
}

func deleteRangeIntervalCardinalityViolation(request *etcdserverpb.DeleteRangeRequest, deleted int64) string {
	if len(request.GetRangeEnd()) == 0 && deleted > 1 {
		return fmt.Sprintf("deleted count %d above exact-key cardinality", deleted)
	}
	if isEmptyNonFromKeyRange(request.GetKey(), request.GetRangeEnd()) && deleted != 0 {
		return fmt.Sprintf("nonzero deleted count %d for an empty requested range", deleted)
	}
	return ""
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
	if violation := deleteRangeIntervalCardinalityViolation(request, response.GetDeleted()); violation != "" {
		return fail("leader delete_range proxy returned " + violation)
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
	outerRevision := response.GetHeader().GetRevision()
	baseRevision := outerRevision
	if txnProxyResponseHasEffectiveWrite(request, response) {
		if outerRevision <= 1 {
			emitKVProxyIntegrityFailure(metricCli, kvProxyRPCTxn)
			return nil, status.Error(codes.DataLoss, "leader txn proxy returned an effective write without a positive pre-write revision")
		}
		baseRevision--
	}
	if validationErr := validateTxnProxyRevisionBoundCompareBranches(request, response, baseRevision); validationErr != nil {
		emitKVProxyIntegrityFailure(metricCli, kvProxyRPCTxn)
		return nil, status.Error(codes.DataLoss, validationErr.Error())
	}
	changed := false
	if validationErr := validateTxnProxyResponseHeaders(request, response, outerRevision, baseRevision, true, &changed); validationErr != nil {
		emitKVProxyIntegrityFailure(metricCli, kvProxyRPCTxn)
		return nil, status.Error(codes.DataLoss, validationErr.Error())
	}
	if validationErr := validateTxnProxyOperationPayloads(request, response, response.GetHeader().GetRevision()); validationErr != nil {
		emitKVProxyIntegrityFailure(metricCli, kvProxyRPCTxn)
		return nil, status.Error(codes.DataLoss, validationErr.Error())
	}
	var mutations []txnProxyMutationInterval
	var compareEvidence []txnProxyCompareEvidence
	collectTxnProxyCompareEvidence(request, response, &mutations, &compareEvidence)
	if validationErr := validateTxnProxyCompareEvidenceBranches(request, response, compareEvidence); validationErr != nil {
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
	if len(request.GetCompare()) != 0 {
		expected, deterministic := txnProxyDeterministicCompareBranch(request.GetCompare())
		if deterministic && response.GetSucceeded() != expected {
			return txnProxyCompareBranchError(response.GetSucceeded(), expected)
		}
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

func validateTxnProxyRevisionBoundCompareBranches(
	request *etcdserverpb.TxnRequest,
	response *etcdserverpb.TxnResponse,
	maxRevision int64,
) error {
	if len(request.GetCompare()) != 0 {
		expected, deterministic := txnProxyDeterministicCompareBranchAtRevision(request.GetCompare(), maxRevision)
		if deterministic && response.GetSucceeded() != expected {
			return txnProxyCompareBranchError(response.GetSucceeded(), expected)
		}
	}
	requests := request.GetFailure()
	if response.GetSucceeded() {
		requests = request.GetSuccess()
	}
	for index, requestOp := range requests {
		if requestOp.GetRequestTxn() == nil {
			continue
		}
		if err := validateTxnProxyRevisionBoundCompareBranches(
			requestOp.GetRequestTxn(), response.GetResponses()[index].GetResponseTxn(), maxRevision,
		); err != nil {
			return err
		}
	}
	return nil
}

func txnProxyCompareBranchError(selected, expected bool) error {
	selectedBranch := "failure"
	expectedBranch := "success"
	if selected {
		selectedBranch = "success"
		expectedBranch = "failure"
	}
	return fmt.Errorf(
		"leader txn proxy selected the %s branch for compares that deterministically select the %s branch",
		selectedBranch, expectedBranch,
	)
}

type txnProxyCompareEvidence struct {
	key, rangeEnd []byte
	kvs           []*mvccpb.KeyValue
}

type txnProxyMutationInterval struct {
	key, rangeEnd []byte
}

// collectTxnProxyCompareEvidence walks the selected operations in their
// execution order. Upstream compareToPath evaluates the entire selected txn
// tree before executing any operation. A complete current Range, requested Put
// PrevKv, complete Delete PrevKvs, or no-op Delete therefore observes the same
// compare snapshot whenever no earlier effective mutation intersects that
// interval. Disjoint mutations do not invalidate the evidence.
func collectTxnProxyCompareEvidence(
	request *etcdserverpb.TxnRequest,
	response *etcdserverpb.TxnResponse,
	mutations *[]txnProxyMutationInterval,
	evidence *[]txnProxyCompareEvidence,
) {
	requests := request.GetFailure()
	if response.GetSucceeded() {
		requests = request.GetSuccess()
	}
	for index, requestOp := range requests {
		responseOp := response.GetResponses()[index]
		switch {
		case requestOp.GetRequestRange() != nil:
			rangeRequest := requestOp.GetRequestRange()
			interval := txnProxyMutationInterval{key: rangeRequest.GetKey(), rangeEnd: rangeRequest.GetRangeEnd()}
			if txnProxyRangeProvidesCompareEvidence(rangeRequest) && !txnProxyIntervalWasMutated(interval, *mutations) {
				*evidence = append(*evidence, txnProxyCompareEvidence{
					key: rangeRequest.GetKey(), rangeEnd: rangeRequest.GetRangeEnd(),
					kvs: responseOp.GetResponseRange().GetKvs(),
				})
			}
		case requestOp.GetRequestPut() != nil:
			putRequest := requestOp.GetRequestPut()
			interval := txnProxyMutationInterval{key: putRequest.GetKey()}
			if putRequest.GetPrevKv() && !txnProxyIntervalWasMutated(interval, *mutations) {
				var kvs []*mvccpb.KeyValue
				if previous := responseOp.GetResponsePut().GetPrevKv(); previous != nil {
					kvs = []*mvccpb.KeyValue{previous}
				}
				*evidence = append(*evidence, txnProxyCompareEvidence{key: putRequest.GetKey(), kvs: kvs})
			}
			*mutations = append(*mutations, interval)
		case requestOp.GetRequestDeleteRange() != nil:
			deleteRequest := requestOp.GetRequestDeleteRange()
			deleteResponse := responseOp.GetResponseDeleteRange()
			interval := txnProxyMutationInterval{key: deleteRequest.GetKey(), rangeEnd: deleteRequest.GetRangeEnd()}
			if !txnProxyIntervalWasMutated(interval, *mutations) &&
				(deleteRequest.GetPrevKv() || deleteResponse.GetDeleted() == 0) {
				*evidence = append(*evidence, txnProxyCompareEvidence{
					key: deleteRequest.GetKey(), rangeEnd: deleteRequest.GetRangeEnd(), kvs: deleteResponse.GetPrevKvs(),
				})
			}
			if deleteResponse.GetDeleted() > 0 {
				if deleteRequest.GetPrevKv() {
					for _, previous := range deleteResponse.GetPrevKvs() {
						*mutations = append(*mutations, txnProxyMutationInterval{key: previous.GetKey()})
					}
				} else {
					*mutations = append(*mutations, interval)
				}
			}
		case requestOp.GetRequestTxn() != nil:
			collectTxnProxyCompareEvidence(
				requestOp.GetRequestTxn(), responseOp.GetResponseTxn(), mutations, evidence,
			)
		}
	}
}

func txnProxyIntervalWasMutated(interval txnProxyMutationInterval, mutations []txnProxyMutationInterval) bool {
	for _, mutation := range mutations {
		if txnProxyIntervalsIntersect(interval, mutation) {
			return true
		}
	}
	return false
}

func txnProxyIntervalsIntersect(left, right txnProxyMutationInterval) bool {
	if txnProxyIntervalIsEmpty(left) || txnProxyIntervalIsEmpty(right) {
		return false
	}
	leftPoint := len(left.rangeEnd) == 0
	rightPoint := len(right.rangeEnd) == 0
	if leftPoint {
		return txnProxyIntervalContains(right, left.key)
	}
	if rightPoint {
		return txnProxyIntervalContains(left, right.key)
	}
	leftFromKey := isFromKeyRangeEnd(left.rangeEnd)
	rightFromKey := isFromKeyRangeEnd(right.rangeEnd)
	if leftFromKey && rightFromKey {
		return true
	}
	if leftFromKey {
		return bytes.Compare(left.key, right.rangeEnd) < 0
	}
	if rightFromKey {
		return bytes.Compare(right.key, left.rangeEnd) < 0
	}
	return bytes.Compare(left.key, right.rangeEnd) < 0 && bytes.Compare(right.key, left.rangeEnd) < 0
}

func txnProxyIntervalIsEmpty(interval txnProxyMutationInterval) bool {
	return len(interval.rangeEnd) != 0 && !isFromKeyRangeEnd(interval.rangeEnd) &&
		bytes.Compare(interval.key, interval.rangeEnd) >= 0
}

func txnProxyIntervalContains(interval txnProxyMutationInterval, key []byte) bool {
	if txnProxyIntervalIsEmpty(interval) {
		return false
	}
	if len(interval.rangeEnd) == 0 {
		return bytes.Equal(interval.key, key)
	}
	if bytes.Compare(key, interval.key) < 0 {
		return false
	}
	return isFromKeyRangeEnd(interval.rangeEnd) || bytes.Compare(key, interval.rangeEnd) < 0
}

func txnProxyRangeProvidesCompareEvidence(request *etcdserverpb.RangeRequest) bool {
	// A limited, projected, historical, or revision-filtered response is not a
	// complete description of its requested interval. Sort and Serializable do
	// not change the represented key-values and therefore need no exclusion.
	return request.GetRevision() == 0 && request.GetLimit() <= 0 &&
		!request.GetKeysOnly() && !request.GetCountOnly() &&
		request.GetMinModRevision() == 0 && request.GetMaxModRevision() == 0 &&
		request.GetMinCreateRevision() == 0 && request.GetMaxCreateRevision() == 0
}

func validateTxnProxyCompareEvidenceBranches(
	request *etcdserverpb.TxnRequest,
	response *etcdserverpb.TxnResponse,
	evidence []txnProxyCompareEvidence,
) error {
	if len(request.GetCompare()) != 0 {
		expected, deterministic, err := txnProxyDeterministicCompareBranchFromEvidence(request.GetCompare(), evidence)
		if err != nil {
			return err
		}
		if deterministic && response.GetSucceeded() != expected {
			return txnProxyCompareBranchError(response.GetSucceeded(), expected)
		}
	}
	requests := request.GetFailure()
	if response.GetSucceeded() {
		requests = request.GetSuccess()
	}
	for index, requestOp := range requests {
		if requestOp.GetRequestTxn() == nil {
			continue
		}
		if err := validateTxnProxyCompareEvidenceBranches(
			requestOp.GetRequestTxn(), response.GetResponses()[index].GetResponseTxn(), evidence,
		); err != nil {
			return err
		}
	}
	return nil
}

func txnProxyDeterministicCompareBranchFromEvidence(
	compares []*etcdserverpb.Compare,
	evidence []txnProxyCompareEvidence,
) (bool, bool, error) {
	allDeterministic := true
	for _, compare := range compares {
		result, deterministic, err := txnProxyCompareResultFromEvidence(compare, evidence)
		if err != nil {
			return false, false, err
		}
		if !deterministic {
			allDeterministic = false
			continue
		}
		if !result {
			return false, true, nil
		}
	}
	if allDeterministic {
		return true, true, nil
	}
	return false, false, nil
}

func txnProxyCompareResultFromEvidence(
	compare *etcdserverpb.Compare,
	evidence []txnProxyCompareEvidence,
) (bool, bool, error) {
	var result bool
	found := false
	for _, candidate := range evidence {
		// Only an exact interval match is a complete proof. Inferring a compare
		// from a containing Range would require independently proving that the
		// returned payload has been sliced without omission.
		if !bytes.Equal(compare.GetKey(), candidate.key) || !bytes.Equal(compare.GetRangeEnd(), candidate.rangeEnd) {
			continue
		}
		candidateResult := true
		if len(candidate.kvs) == 0 {
			candidateResult = compareKeyValue(compare, nil)
		} else {
			for _, kv := range candidate.kvs {
				if !compareKeyValue(compare, kv) {
					candidateResult = false
					break
				}
			}
		}
		if found && result != candidateResult {
			return false, false, fmt.Errorf("leader txn proxy returned inconsistent pre-write evidence for a compare interval")
		}
		result = candidateResult
		found = true
	}
	return result, found, nil
}

func txnProxyDeterministicCompareBranch(compares []*etcdserverpb.Compare) (bool, bool) {
	allDeterministic := true
	for _, compare := range compares {
		result, deterministic := txnProxyDeterministicCompare(compare)
		if !deterministic {
			allDeterministic = false
			continue
		}
		if !result {
			return false, true
		}
	}
	if allDeterministic {
		return true, true
	}
	return false, false
}

func txnProxyDeterministicCompareBranchAtRevision(compares []*etcdserverpb.Compare, maxRevision int64) (bool, bool) {
	allDeterministic := true
	for _, compare := range compares {
		result, deterministic := txnProxyDeterministicCompareAtRevision(compare, maxRevision)
		if !deterministic {
			allDeterministic = false
			continue
		}
		if !result {
			return false, true
		}
	}
	if allDeterministic {
		return true, true
	}
	return false, false
}

func txnProxyDeterministicCompareAtRevision(compare *etcdserverpb.Compare, maxRevision int64) (bool, bool) {
	if result, deterministic := txnProxyDeterministicCompare(compare); deterministic {
		return result, true
	}
	var expected int64
	switch compare.GetTarget() {
	case etcdserverpb.Compare_VERSION:
		expected = compare.GetVersion()
	case etcdserverpb.Compare_CREATE:
		expected = compare.GetCreateRevision()
	case etcdserverpb.Compare_MOD:
		expected = compare.GetModRevision()
	default:
		return false, false
	}
	// compareToPath evaluates every nested transaction against the same
	// pre-write read view. Existing metadata cannot exceed that revision;
	// missing keys contribute zero.
	if expected > maxRevision {
		return compareOrder(-1, compare.GetResult()), true
	}
	if expected == maxRevision && compare.GetResult() == etcdserverpb.Compare_GREATER {
		return false, true
	}
	return false, false
}

func txnProxyDeterministicCompare(compare *etcdserverpb.Compare) (bool, bool) {
	if isEmptyNonFromKeyRange(compare.GetKey(), compare.GetRangeEnd()) {
		// Upstream applyCompare evaluates an empty numeric interval against a
		// zero KeyValue and always fails VALUE compares. compareKeyValue shares
		// that exact truth table, including unknown enum fallthrough behavior.
		return compareKeyValue(compare, nil), true
	}

	// Unknown results fall through to true in upstream compareKV. VALUE is the
	// exception for an interval whose runtime result is empty: applyCompare
	// returns false before compareKV, so its branch still depends on storage.
	switch compare.GetResult() {
	case etcdserverpb.Compare_EQUAL, etcdserverpb.Compare_GREATER,
		etcdserverpb.Compare_LESS, etcdserverpb.Compare_NOT_EQUAL:
	default:
		if compare.GetTarget() == etcdserverpb.Compare_VALUE {
			return false, false
		}
		return true, true
	}

	// Upstream leaves the comparison order at zero for unknown targets.
	switch compare.GetTarget() {
	case etcdserverpb.Compare_VERSION:
		return txnProxyNonNegativeCompareResult(compare.GetVersion(), compare.GetResult())
	case etcdserverpb.Compare_CREATE:
		return txnProxyNonNegativeCompareResult(compare.GetCreateRevision(), compare.GetResult())
	case etcdserverpb.Compare_MOD:
		return txnProxyNonNegativeCompareResult(compare.GetModRevision(), compare.GetResult())
	case etcdserverpb.Compare_VALUE, etcdserverpb.Compare_LEASE:
		return false, false
	default:
		return compareOrder(0, compare.GetResult()), true
	}
}

func txnProxyNonNegativeCompareResult(expected int64, result etcdserverpb.Compare_CompareResult) (bool, bool) {
	// Missing keys contribute zero; existing VERSION/CREATE/MOD metadata is
	// positive. Therefore the complete observable domain is non-negative.
	if expected < 0 {
		return compareOrder(1, result), true
	}
	if expected == 0 && result == etcdserverpb.Compare_LESS {
		return false, true
	}
	return false, false
}

func txnProxyResponseHasEffectiveWrite(request *etcdserverpb.TxnRequest, response *etcdserverpb.TxnResponse) bool {
	requests := request.GetFailure()
	if response.GetSucceeded() {
		requests = request.GetSuccess()
	}
	for index, requestOp := range requests {
		switch {
		case requestOp.GetRequestPut() != nil:
			return true
		case requestOp.GetRequestDeleteRange() != nil:
			if response.GetResponses()[index].GetResponseDeleteRange().GetDeleted() > 0 {
				return true
			}
		case requestOp.GetRequestTxn() != nil:
			if txnProxyResponseHasEffectiveWrite(requestOp.GetRequestTxn(), response.GetResponses()[index].GetResponseTxn()) {
				return true
			}
		}
	}
	return false
}

func validateTxnProxyResponseHeaders(
	request *etcdserverpb.TxnRequest,
	response *etcdserverpb.TxnResponse,
	outerRevision, baseRevision int64,
	root bool,
	changed *bool,
) error {
	if response.GetHeader() == nil {
		return fmt.Errorf("leader txn proxy returned a txn response without a header")
	}
	if !root {
		if txnProxyHeaderHasIdentity(response.GetHeader()) {
			return fmt.Errorf("leader txn proxy returned nested txn header with nonzero identity")
		}
		if response.GetHeader().GetRevision() != 0 {
			return fmt.Errorf("leader txn proxy returned nested txn revision %d instead of zero", response.GetHeader().GetRevision())
		}
	}
	requests := request.GetFailure()
	if response.GetSucceeded() {
		requests = request.GetSuccess()
	}
	for index, responseOp := range response.GetResponses() {
		var header *etcdserverpb.ResponseHeader
		switch requestOp := requests[index]; {
		case requestOp.GetRequestRange() != nil:
			header = responseOp.GetResponseRange().GetHeader()
		case requestOp.GetRequestPut() != nil:
			*changed = true
			header = responseOp.GetResponsePut().GetHeader()
		case requestOp.GetRequestDeleteRange() != nil:
			if responseOp.GetResponseDeleteRange().GetDeleted() > 0 {
				*changed = true
			}
			header = responseOp.GetResponseDeleteRange().GetHeader()
		case requestOp.GetRequestTxn() != nil:
			if err := validateTxnProxyResponseHeaders(
				requestOp.GetRequestTxn(), responseOp.GetResponseTxn(), outerRevision, baseRevision, false, changed,
			); err != nil {
				return err
			}
			continue
		}
		if header == nil {
			return fmt.Errorf("leader txn proxy returned response operation without a header at index %d", index)
		}
		if txnProxyHeaderHasIdentity(header) {
			return fmt.Errorf("leader txn proxy returned operation header with nonzero identity at index %d", index)
		}
		visibleRevision := baseRevision
		if *changed {
			visibleRevision = outerRevision
		}
		if header.GetRevision() != visibleRevision {
			return fmt.Errorf(
				"leader txn proxy returned operation revision %d differs from visible revision %d at index %d",
				header.GetRevision(), visibleRevision, index,
			)
		}
	}
	return nil
}

func txnProxyHeaderHasIdentity(header *etcdserverpb.ResponseHeader) bool {
	return header.GetClusterId() != 0 || header.GetMemberId() != 0 || header.GetRaftTerm() != 0
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
	if violation := deleteRangeIntervalCardinalityViolation(request, response.GetDeleted()); violation != "" {
		return fmt.Errorf("delete returned %s", violation)
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
