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
	"fmt"

	"go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/mvccpb"

	"github.com/kubewharf/kubebrain/pkg/backend"
)

// This file holds the etcd Txn COMPARE evaluation extracted from kv.go (audit
// A4): evaluating a single Compare against the stored key (point or range), and
// the per-target truth tables (mod/value/version/create/lease).

// evalCompareGuarded evaluates a single compare and, for a single-key compare on
// an existing key, also returns an optimistic-concurrency guard (the key's
// current revision) that the atomic txn path can assert at commit time (#4 Tier
// 2). No guard is returned for range compares or absent keys.
func (s *RPCServer) evalCompareGuarded(ctx context.Context, cmp *etcdserverpb.Compare) (bool, *backend.TxnGuard, error) {
	return s.evalCompareGuardedAtRevision(ctx, cmp, 0)
}

func (s *RPCServer) evalCompareGuardedAtRevision(ctx context.Context, cmp *etcdserverpb.Compare, revision int64) (bool, *backend.TxnGuard, error) {
	if len(cmp.RangeEnd) > 0 {
		ok, err := s.evalRangeCompareAtRevision(ctx, cmp, revision)
		return ok, nil, err
	}
	rangeResp, err := s.backend.Get(ctx, &etcdserverpb.RangeRequest{Key: cmp.Key, Revision: revision})
	if err != nil {
		return false, nil, err
	}
	var kv *mvccpb.KeyValue
	if len(rangeResp.Kvs) > 0 {
		kv = rangeResp.Kvs[0]
	}
	ok, err := s.compareSingleKey(cmp, kv)
	if err != nil {
		return false, nil, err
	}
	guard := &backend.TxnGuard{Key: cmp.Key, Absent: kv == nil}
	if kv != nil {
		guard.Revision = uint64(kv.ModRevision)
	}
	return ok, guard, nil
}

func (s *RPCServer) compareSingleKey(cmp *etcdserverpb.Compare, kv *mvccpb.KeyValue) (bool, error) {
	switch cmp.Target {
	case etcdserverpb.Compare_MOD:
		var actual int64
		if kv != nil {
			actual = kv.ModRevision
		}
		return compareInt64(actual, cmp.GetModRevision(), cmp.Result), nil
	case etcdserverpb.Compare_VALUE:
		var actual []byte
		if kv != nil {
			actual = kv.Value
		}
		return compareBytes(actual, cmp.GetValue(), cmp.Result), nil
	case etcdserverpb.Compare_VERSION:
		actual := int64(0)
		if kv != nil {
			actual = kv.Version
		}
		return compareInt64(actual, cmp.GetVersion(), cmp.Result), nil
	case etcdserverpb.Compare_CREATE:
		actual := int64(0)
		if kv != nil {
			actual = kv.CreateRevision
		}
		return compareInt64(actual, cmp.GetCreateRevision(), cmp.Result), nil
	case etcdserverpb.Compare_LEASE:
		var actual int64
		if kv != nil {
			actual = kv.Lease
		}
		return compareInt64(actual, cmp.GetLease(), cmp.Result), nil
	default:
		return false, unsupported(fmt.Sprintf("compare target %s", cmp.Target))
	}
}

func (s *RPCServer) evalRangeCompare(ctx context.Context, cmp *etcdserverpb.Compare) (bool, error) {
	return s.evalRangeCompareAtRevision(ctx, cmp, 0)
}

func (s *RPCServer) evalRangeCompareAtRevision(ctx context.Context, cmp *etcdserverpb.Compare, revision int64) (bool, error) {
	switch cmp.Target {
	case etcdserverpb.Compare_MOD, etcdserverpb.Compare_VALUE, etcdserverpb.Compare_VERSION, etcdserverpb.Compare_CREATE, etcdserverpb.Compare_LEASE:
	default:
		return false, unsupported(fmt.Sprintf("compare target %s", cmp.Target))
	}
	rangeResp, err := s.backend.List(ctx, &etcdserverpb.RangeRequest{
		Key:      cmp.Key,
		RangeEnd: cmp.RangeEnd,
		Revision: revision,
	})
	if err != nil {
		return false, err
	}
	if len(rangeResp.Kvs) == 0 {
		if cmp.Target == etcdserverpb.Compare_VALUE {
			return false, nil
		}
		return s.compareKeyValue(cmp, nil), nil
	}
	for _, kv := range rangeResp.Kvs {
		if !s.compareKeyValue(cmp, kv) {
			return false, nil
		}
	}
	return true, nil
}

func (s *RPCServer) compareKeyValue(cmp *etcdserverpb.Compare, kv *mvccpb.KeyValue) bool {
	switch cmp.Target {
	case etcdserverpb.Compare_MOD:
		var actual int64
		if kv != nil {
			actual = kv.ModRevision
		}
		return compareInt64(actual, cmp.GetModRevision(), cmp.Result)
	case etcdserverpb.Compare_VALUE:
		if kv == nil {
			return false
		}
		return compareBytes(kv.Value, cmp.GetValue(), cmp.Result)
	case etcdserverpb.Compare_VERSION:
		var actual int64
		if kv != nil {
			actual = kv.Version
		}
		return compareInt64(actual, cmp.GetVersion(), cmp.Result)
	case etcdserverpb.Compare_CREATE:
		var actual int64
		if kv != nil {
			actual = kv.CreateRevision
		}
		return compareInt64(actual, cmp.GetCreateRevision(), cmp.Result)
	case etcdserverpb.Compare_LEASE:
		var actual int64
		if kv != nil {
			actual = kv.Lease
		}
		return compareInt64(actual, cmp.GetLease(), cmp.Result)
	default:
		return false
	}
}

func compareInt64(actual, expected int64, result etcdserverpb.Compare_CompareResult) bool {
	switch result {
	case etcdserverpb.Compare_EQUAL:
		return actual == expected
	case etcdserverpb.Compare_GREATER:
		return actual > expected
	case etcdserverpb.Compare_LESS:
		return actual < expected
	case etcdserverpb.Compare_NOT_EQUAL:
		return actual != expected
	default:
		return false
	}
}

func compareBytes(actual, expected []byte, result etcdserverpb.Compare_CompareResult) bool {
	cmp := bytes.Compare(actual, expected)
	switch result {
	case etcdserverpb.Compare_EQUAL:
		return cmp == 0
	case etcdserverpb.Compare_GREATER:
		return cmp > 0
	case etcdserverpb.Compare_LESS:
		return cmp < 0
	case etcdserverpb.Compare_NOT_EQUAL:
		return cmp != 0
	default:
		return false
	}
}
