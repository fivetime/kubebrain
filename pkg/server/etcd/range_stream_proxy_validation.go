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
	"go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/mvccpb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

type rangeStreamProxyPayloadValidator struct {
	request    *etcdserverpb.RangeRequest
	sentCount  int64
	previousKV *mvccpb.KeyValue
	seenKeys   map[string]struct{}
	maxModRev  int64
}

func newRangeStreamProxyPayloadValidator(request *etcdserverpb.RangeRequest) *rangeStreamProxyPayloadValidator {
	return &rangeStreamProxyPayloadValidator{request: request, seenKeys: make(map[string]struct{})}
}

func (v *rangeStreamProxyPayloadValidator) validate(response *etcdserverpb.RangeResponse) error {
	fail := func(message string) error {
		return status.Error(codes.DataLoss, "forwarded range stream "+message)
	}
	terminal := response.GetHeader() != nil
	if !terminal && (response.GetCount() != 0 || response.GetMore()) {
		return fail("returned aggregate metadata before the terminal frame")
	}
	for _, kv := range response.GetKvs() {
		if v.request.GetCountOnly() {
			return fail("returned key-values for a count-only request")
		}
		if kv == nil {
			return fail("returned a nil key-value")
		}
		if !rangeProxyContainsKey(v.request.GetKey(), v.request.GetRangeEnd(), kv.GetKey()) {
			return fail("returned a key outside the requested range")
		}
		if _, ok := v.seenKeys[string(kv.GetKey())]; ok {
			return fail("returned a duplicate key")
		}
		if v.previousKV != nil && !rangeProxyOrderValid(v.request, v.previousKV, kv) {
			return fail("returned key-values outside the requested sort order")
		}
		if v.request.GetKeysOnly() && len(kv.GetValue()) != 0 {
			return fail("returned a value for a keys-only request")
		}
		if v.request.GetKeysOnly() && v.request.GetSortTarget() != etcdserverpb.RangeRequest_VALUE && kv.GetLease() != 0 {
			return fail("returned a lease for a fast keys-only request")
		}
		if validateProxyKeyValueLifecycle(kv) != nil {
			return fail("returned invalid key-value revision metadata")
		}
		v.seenKeys[string(kv.GetKey())] = struct{}{}
		v.previousKV = proto.Clone(kv).(*mvccpb.KeyValue)
		if kv.GetModRevision() > v.maxModRev {
			v.maxModRev = kv.GetModRevision()
		}
		v.sentCount++
		if v.request.GetLimit() > 0 && v.sentCount > v.request.GetLimit() {
			return fail("returned more key-values than the requested limit")
		}
	}
	if !terminal {
		return nil
	}
	headerRevision := response.GetHeader().GetRevision()
	if headerRevision <= 0 {
		return fail("returned a non-positive terminal revision")
	}
	if v.request.GetRevision() > 0 && headerRevision < v.request.GetRevision() {
		return fail("returned a terminal revision below the requested revision")
	}
	snapshotRevision := headerRevision
	if v.request.GetRevision() > 0 {
		snapshotRevision = v.request.GetRevision()
	}
	if v.maxModRev > snapshotRevision {
		return fail("returned a key-value newer than the requested snapshot")
	}
	if response.GetCount() < 0 || response.GetCount() < v.sentCount {
		return fail("returned an invalid terminal count")
	}
	if violation := rangeIntervalCardinalityViolation(
		v.request, response.GetCount(), v.sentCount, response.GetMore(),
	); violation != "" {
		return fail("returned " + violation)
	}
	if v.request.GetCountOnly() {
		if v.sentCount != 0 || response.GetMore() {
			return fail("returned key-values or more=true for a count-only request")
		}
		return nil
	}
	wantMore := response.GetCount() > v.sentCount
	if response.GetMore() != wantMore {
		return fail("returned inconsistent terminal count and more metadata")
	}
	if v.request.GetLimit() <= 0 && response.GetMore() {
		return fail("returned more=true for an unlimited request")
	}
	if response.GetMore() && v.sentCount != v.request.GetLimit() {
		return fail("returned a non-full limited stream with more=true")
	}
	return nil
}
