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
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
)

// A failed optimistic update returns the current Range, including its count.
// This is also checked by the follower's existing payload integrity validator.
func TestTxnFailedUpdateRangeCount(t *testing.T) {
	for _, present := range []bool{false, true} {
		name := "missing"
		if present {
			name = "stale"
		}
		t.Run(name, func(t *testing.T) {
			s, closeFn := newTestRPCServer(t)
			defer closeFn()
			ctx := context.Background()
			key := []byte("/registry/count-conflict/key")
			if present {
				_, err := s.Put(ctx, &etcdserverpb.PutRequest{Key: key, Value: []byte("current")})
				require.NoError(t, err)
			}
			before, err := s.Range(ctx, &etcdserverpb.RangeRequest{Key: key})
			require.NoError(t, err)
			rangeReq := &etcdserverpb.RangeRequest{Key: key}
			req := &etcdserverpb.TxnRequest{
				Compare: []*etcdserverpb.Compare{{Key: key, Target: etcdserverpb.Compare_MOD, Result: etcdserverpb.Compare_EQUAL, TargetUnion: &etcdserverpb.Compare_ModRevision{ModRevision: before.Header.Revision + 100}}},
				Success: []*etcdserverpb.RequestOp{{Request: &etcdserverpb.RequestOp_RequestPut{RequestPut: &etcdserverpb.PutRequest{Key: key, Value: []byte("must-not-write")}}}},
				Failure: []*etcdserverpb.RequestOp{{Request: &etcdserverpb.RequestOp_RequestRange{RequestRange: rangeReq}}},
			}
			_, fast := isUpdate(req)
			require.True(t, fast)
			resp, err := s.Txn(ctx, req)
			require.NoError(t, err)
			require.False(t, resp.Succeeded)
			require.Len(t, resp.Responses, 1)
			got := resp.Responses[0].GetResponseRange()
			require.NotNil(t, got)
			require.Equal(t, before.Count, got.Count)
			require.Equal(t, before.Kvs, got.Kvs)
			require.Equal(t, before.Header.Revision, resp.Header.Revision)
			_, err = validateRangeProxyPayload(s.metricCli, rangeReq, got, nil)
			require.NoError(t, err)
			after, err := s.Range(ctx, rangeReq)
			require.NoError(t, err)
			require.Equal(t, before.Kvs, after.Kvs)
			require.Equal(t, before.Header.Revision, after.Header.Revision)
		})
	}
}

func TestBackendShimDeleteRangeCount(t *testing.T) {
	for _, match := range []bool{false, true} {
		name := "conflict"
		if match {
			name = "success"
		}
		t.Run(name, func(t *testing.T) {
			s, closeFn := newTestRPCServer(t)
			defer closeFn()
			b := s.backend.(*backendShim)
			ctx := context.Background()
			key := []byte("/registry/delete-count/key")
			put, err := b.Put(ctx, &etcdserverpb.PutRequest{Key: key, Value: []byte("value")})
			require.NoError(t, err)
			rev := put.Header.Revision
			if !match {
				rev++
			}
			resp, err := b.Delete(ctx, key, rev, true)
			require.NoError(t, err)
			require.Equal(t, match, resp.Succeeded)
			require.Len(t, resp.Responses, 1)
			r := resp.Responses[0].GetResponseRange()
			require.NotNil(t, r)
			require.Len(t, r.Kvs, 1)
			require.Equal(t, int64(1), r.Count)
		})
	}
}
