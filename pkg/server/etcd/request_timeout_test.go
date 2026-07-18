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
	"time"

	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/api/v3/etcdserverpb"

	"github.com/kubewharf/kubebrain/pkg/backend"
)

type deadlineRecordingShim struct {
	BackendShim
	remaining time.Duration
}

func (s *deadlineRecordingShim) TxnApply(ctx context.Context, ops []backend.TxnWriteOp, guards []backend.TxnGuard, prevKV []bool) ([]*etcdserverpb.ResponseOp, uint64, []backend.TxnWriteResult, error) {
	deadline, ok := ctx.Deadline()
	if ok {
		s.remaining = time.Until(deadline)
	}
	return s.BackendShim.TxnApply(ctx, ops, guards, prevKV)
}

func TestDeleteRangePropagatesServerAndClientDeadline(t *testing.T) {
	for _, tc := range []struct {
		name          string
		clientTimeout time.Duration
		minRemaining  time.Duration
		maxRemaining  time.Duration
	}{
		{
			name: "server default", minRemaining: unaryRpcTimeout - time.Second,
			maxRemaining: unaryRpcTimeout,
		},
		{
			name: "shorter client", clientTimeout: 500 * time.Millisecond,
			minRemaining: 100 * time.Millisecond, maxRemaining: 500 * time.Millisecond,
		},
		{
			name: "longer client", clientTimeout: 30 * time.Second,
			minRemaining: 29 * time.Second, maxRemaining: 30 * time.Second,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			server, closeFn := newTestRPCServer(t)
			defer closeFn()
			key := []byte("/registry/request-timeout/" + tc.name)
			_, err := server.Put(context.Background(), &etcdserverpb.PutRequest{Key: key, Value: []byte("value")})
			require.NoError(t, err)
			recorder := &deadlineRecordingShim{BackendShim: server.backend}
			server.backend = recorder

			ctx := context.Background()
			if tc.clientTimeout > 0 {
				var cancel context.CancelFunc
				ctx, cancel = context.WithTimeout(ctx, tc.clientTimeout)
				defer cancel()
			}
			_, err = server.DeleteRange(ctx, &etcdserverpb.DeleteRangeRequest{Key: key})
			require.NoError(t, err)
			require.GreaterOrEqual(t, recorder.remaining, tc.minRemaining)
			require.LessOrEqual(t, recorder.remaining, tc.maxRemaining)
		})
	}
}
