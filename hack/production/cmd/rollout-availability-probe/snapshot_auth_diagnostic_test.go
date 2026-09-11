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

package main

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
	clientv3 "go.etcd.io/etcd/client/v3"
)

type restoredAuthDiagnosticKV struct{ clientv3.KV }

func (restoredAuthDiagnosticKV) Get(context.Context, string, ...clientv3.OpOption) (*clientv3.GetResponse, error) {
	return &clientv3.GetResponse{}, nil
}

type restoredAuthDiagnosticWatcher struct {
	clientv3.Watcher
	ctx      context.Context
	response clientv3.WatchResponse
}

func (w *restoredAuthDiagnosticWatcher) Watch(ctx context.Context, _ string, _ ...clientv3.OpOption) clientv3.WatchChan {
	w.ctx = ctx
	ch := make(chan clientv3.WatchResponse, 1)
	ch <- w.response
	close(ch)
	return ch
}

func TestRestoredAuthWatchFailureIncludesEvidenceBeforeOwnCancel(t *testing.T) {
	w := &restoredAuthDiagnosticWatcher{response: clientv3.WatchResponse{
		Header:   &etcdserverpb.ResponseHeader{ClusterId: 11, MemberId: 12, Revision: 13, RaftTerm: 14},
		Canceled: true, CancelReason: rpctypes.ErrGRPCInvalidAuthToken.Error(),
	}}
	client := &clientv3.Client{KV: restoredAuthDiagnosticKV{}, Watcher: w}
	err := verifyRestoredAuthAccess(t.Context(), client, restoredSnapshotAuthAccessExpectation{
		username: "fixture-reader", key: "/fixture/key", read: true,
	}, 0, nil)
	require.ErrorContains(t, err, "restored auth Watch was denied")
	require.ErrorContains(t, err, "invalid auth token")
	require.ErrorContains(t, err, "header_present=true cluster_id=11 member_id=12 revision=13 raft_term=14")
	require.ErrorContains(t, err, "context_done=false")
	require.ErrorContains(t, err, `auth_stage="Watch"`)
	require.ErrorIs(t, w.ctx.Err(), context.Canceled, "watch must still be canceled after collecting evidence")
}

func TestRestoredAuthAccessFailurePreservesOutcome(t *testing.T) {
	original := errors.New("original authentication failure")
	for _, tc := range []struct {
		name       string
		contextErr error
		flags      string
	}{
		{"active", nil, "context_done=false deadline_exceeded=false"},
		{"canceled", context.Canceled, "context_done=true deadline_exceeded=false"},
		{"deadline", context.DeadlineExceeded, "context_done=true deadline_exceeded=true"},
		{"custom", errors.New("private cancellation detail"), "context_done=true deadline_exceeded=false"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := restoredAuthAccessFailure(original, "LeaseKeepAliveOnce", 123*time.Millisecond, tc.contextErr)
			require.ErrorIs(t, err, original)
			require.ErrorContains(t, err, `auth_stage="LeaseKeepAliveOnce" access_elapsed_ms=123 `+tc.flags)
			require.NotContains(t, err.Error(), "private cancellation detail")
			require.NoError(t, restoredAuthAccessFailure(nil, "LeaseKeepAliveOnce", time.Second, tc.contextErr),
				"diagnostics must not turn success into failure")
		})
	}
}
