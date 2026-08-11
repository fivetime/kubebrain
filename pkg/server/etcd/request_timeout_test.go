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

func TestWaitLeaderReadyParksUntilStartupCompletes(t *testing.T) {
	server := &RPCServer{}
	server.SetLeaderReady(false)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- server.waitLeaderReady(ctx) }()

	select {
	case err := <-done:
		t.Fatalf("leader startup wait returned early: %v", err)
	case <-time.After(50 * time.Millisecond):
	}
	server.SetLeaderReady(true)
	require.NoError(t, <-done)
}

func TestLeaseRevokeWaitsForLeaderStartup(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()
	grant, err := server.LeaseGrant(context.Background(), &etcdserverpb.LeaseGrantRequest{ID: 9821, TTL: 300})
	require.NoError(t, err)
	server.SetLeaderReady(false)
	done := make(chan error, 1)
	go func() {
		_, revokeErr := server.LeaseRevoke(context.Background(), &etcdserverpb.LeaseRevokeRequest{ID: grant.ID})
		done <- revokeErr
	}()
	select {
	case err := <-done:
		t.Fatalf("LeaseRevoke returned before leader startup completed: %v", err)
	case <-time.After(50 * time.Millisecond):
	}
	server.SetLeaderReady(true)
	require.NoError(t, <-done)
	ttl, err := server.LeaseTimeToLive(context.Background(), &etcdserverpb.LeaseTimeToLiveRequest{ID: grant.ID})
	require.NoError(t, err)
	require.Equal(t, int64(-1), ttl.TTL)
}

func TestLeaseKeepAliveWaitsForLeaderStartup(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()
	grant, err := server.LeaseGrant(context.Background(), &etcdserverpb.LeaseGrantRequest{ID: 9822, TTL: 300})
	require.NoError(t, err)
	server.SetLeaderReady(false)
	stream := &fakeLeaseKeepAliveServer{
		requests: []*etcdserverpb.LeaseKeepAliveRequest{{ID: grant.ID}},
	}
	done := make(chan error, 1)
	go func() { done <- server.LeaseKeepAlive(stream) }()
	select {
	case err := <-done:
		t.Fatalf("LeaseKeepAlive returned before leader startup completed: %v", err)
	case <-time.After(50 * time.Millisecond):
	}
	server.SetLeaderReady(true)
	require.NoError(t, <-done)
	require.Len(t, stream.sent, 1)
	require.Equal(t, grant.ID, stream.sent[0].ID)
	require.Positive(t, stream.sent[0].TTL)
}

func TestKVWritesWaitForLeaderStartup(t *testing.T) {
	for _, tc := range []struct {
		name    string
		prepare func(*RPCServer)
		apply   func(*RPCServer) error
	}{
		{
			name: "put",
			apply: func(server *RPCServer) error {
				_, err := server.Put(context.Background(), &etcdserverpb.PutRequest{Key: []byte("/startup/put"), Value: []byte("value")})
				return err
			},
		},
		{
			name: "delete range",
			prepare: func(server *RPCServer) {
				_, err := server.Put(context.Background(), &etcdserverpb.PutRequest{Key: []byte("/startup/delete"), Value: []byte("value")})
				require.NoError(t, err)
			},
			apply: func(server *RPCServer) error {
				_, err := server.DeleteRange(context.Background(), &etcdserverpb.DeleteRangeRequest{Key: []byte("/startup/delete")})
				return err
			},
		},
		{
			name: "txn",
			apply: func(server *RPCServer) error {
				_, err := server.Txn(context.Background(), &etcdserverpb.TxnRequest{Success: []*etcdserverpb.RequestOp{{Request: &etcdserverpb.RequestOp_RequestPut{RequestPut: &etcdserverpb.PutRequest{Key: []byte("/startup/txn"), Value: []byte("value")}}}}})
				return err
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			server, closeFn := newTestRPCServer(t)
			defer closeFn()
			if tc.prepare != nil {
				tc.prepare(server)
			}
			server.SetLeaderReady(false)
			done := make(chan error, 1)
			go func() { done <- tc.apply(server) }()
			select {
			case err := <-done:
				t.Fatalf("%s returned before leader startup completed: %v", tc.name, err)
			case <-time.After(50 * time.Millisecond):
			}
			server.SetLeaderReady(true)
			require.NoError(t, <-done)
		})
	}
}
