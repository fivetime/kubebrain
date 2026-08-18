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
		if kv.GetCreateRevision() <= 0 || kv.GetModRevision() <= 0 || kv.GetVersion() <= 0 {
			return fail("leader range proxy returned incomplete key-value revision metadata")
		}
		if validationErr := backend.ValidateEtcdMetadataAtRevision(backend.EtcdMetadata{
			CreateRevision: uint64(kv.GetCreateRevision()), Version: uint64(kv.GetVersion()), Lease: kv.GetLease(),
		}, uint64(kv.GetModRevision()), "leader range proxy key-value"); validationErr != nil {
			return fail("leader range proxy returned impossible key-value revision metadata")
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

func validateKVProxyResult[T any](metricCli metrics.Metrics, rpc string, response *T, err error) (*T, error) {
	if (response == nil) != (err == nil) {
		if response != nil {
			headerResponse, ok := any(response).(interface {
				GetHeader() *etcdserverpb.ResponseHeader
			})
			if !ok || headerResponse.GetHeader() == nil {
				emitKVProxyIntegrityFailure(metricCli, rpc)
				return nil, status.Error(codes.DataLoss, fmt.Sprintf("leader %s proxy returned a response without a header", rpc))
			}
			if headerResponse.GetHeader().GetRevision() < 0 {
				emitKVProxyIntegrityFailure(metricCli, rpc)
				return nil, status.Error(codes.DataLoss, fmt.Sprintf("leader %s proxy returned a response with a negative header revision", rpc))
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
