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

	"go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/mvccpb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

type rangeStreamProxyPayloadValidator struct {
	request        *etcdserverpb.RangeRequest
	sentCount      int64
	previousKV     *mvccpb.KeyValue
	seenKeys       map[string]struct{}
	maxModRev      int64
	strictKeyOrder bool
}

func newRangeStreamProxyPayloadValidator(request *etcdserverpb.RangeRequest) *rangeStreamProxyPayloadValidator {
	validator := &rangeStreamProxyPayloadValidator{
		request: request,
		strictKeyOrder: request.GetSortTarget() == etcdserverpb.RangeRequest_KEY &&
			(request.GetSortOrder() == etcdserverpb.RangeRequest_NONE ||
				request.GetSortOrder() == etcdserverpb.RangeRequest_ASCEND),
	}
	if !validator.strictKeyOrder {
		validator.seenKeys = make(map[string]struct{})
	}
	return validator
}

func (v *rangeStreamProxyPayloadValidator) validate(response *etcdserverpb.RangeResponse) error {
	fail := func(message string) error {
		return status.Error(codes.DataLoss, "forwarded range stream "+message)
	}
	terminal := response.GetHeader() != nil
	if !terminal && (response.GetCount() != 0 || response.GetMore()) {
		return fail("returned aggregate metadata before the terminal frame")
	}
	// Upstream advances the next key only after a Range response with KVs and
	// attaches the envelope to the Range response that finishes the loop. Thus
	// an empty frame is valid only as the sole terminal frame for an empty range
	// (or for CountOnly). Rejecting the other shapes also prevents a bad peer
	// from keeping a follower stream alive with unbounded empty messages.
	if !terminal && len(response.GetKvs()) == 0 {
		return fail("returned an empty non-terminal frame")
	}
	if terminal && !v.request.GetCountOnly() && v.sentCount > 0 && len(response.GetKvs()) == 0 {
		return fail("returned terminal metadata without final key-values")
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
		if v.seenKeys != nil {
			if _, ok := v.seenKeys[string(kv.GetKey())]; ok {
				return fail("returned a duplicate key")
			}
		}
		if v.previousKV != nil {
			if v.strictKeyOrder {
				comparison := bytes.Compare(v.previousKV.GetKey(), kv.GetKey())
				if comparison == 0 {
					return fail("returned a duplicate key")
				}
				if comparison > 0 {
					return fail("returned key-values outside the requested sort order")
				}
			} else if !rangeProxyOrderValid(v.request, v.previousKV, kv) {
				return fail("returned key-values outside the requested sort order")
			}
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
		if v.seenKeys != nil {
			v.seenKeys[string(kv.GetKey())] = struct{}{}
			v.previousKV = proto.Clone(kv).(*mvccpb.KeyValue)
		} else {
			// Default RangeStream ordering is strictly ascending by key. Retain
			// only the adjacent key so follower validation stays bounded even
			// when an unlimited stream contains millions of key-values.
			v.previousKV = &mvccpb.KeyValue{Key: bytes.Clone(kv.GetKey())}
		}
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
