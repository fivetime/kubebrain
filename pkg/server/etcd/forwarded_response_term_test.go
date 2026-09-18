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
	"errors"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
	"google.golang.org/grpc"
)

func TestForwardedResponseTermIsReportingOnly(t *testing.T) {
	s, closeFn := newTestRPCServer(t)
	defer closeFn()
	localTerm := uint64(124)
	s.peers = testPeerService{
		currentTermFn: func() uint64 { return localTerm },
		leadershipTermFn: func(context.Context) (uint64, error) {
			t.Fatal("validated response term must not require backend access")
			return 0, nil
		},
	}
	header := proxiedResponseHeader(s, 1)
	header.RaftTerm = 125
	s.observeForwardedRevision(header, nil)
	header.RaftTerm = 123
	s.observeForwardedRevision(header, nil)
	for _, cached := range []uint64{124, 0, 126} {
		localTerm = cached
		term, err := s.responseRaftTerm(context.Background())
		require.NoError(t, err)
		require.Equal(t, max(cached, uint64(125)), term)
		require.Equal(t, cached, s.peers.CurrentLeadershipTerm())
	}
	localTerm = 124
	reply, err := s.stampUnary(context.Background(), &etcdserverpb.StatusRequest{},
		&grpc.UnaryServerInfo{FullMethod: etcdserverpb.Maintenance_Status_FullMethodName},
		func(ctx context.Context, req any) (any, error) {
			return s.Status(ctx, req.(*etcdserverpb.StatusRequest))
		})
	require.NoError(t, err)
	response := reply.(*etcdserverpb.StatusResponse)
	require.Equal(t, uint64(125), response.RaftTerm)
	require.Equal(t, response.RaftTerm, response.Header.RaftTerm)
}

func TestForwardedResponseTermRejectsUntrustedHeaders(t *testing.T) {
	for _, name := range []string{"nil", "error", "foreign", "zero_cluster", "unknown_member", "zero_member", "zero_term", "negative_revision"} {
		t.Run(name, func(t *testing.T) {
			s, closeFn := newTestRPCServer(t)
			defer closeFn()
			header := proxiedResponseHeader(s, 1)
			s.staticMembers = []*etcdserverpb.Member{{ID: header.MemberId}}
			header.RaftTerm = 999
			var responseErr error
			switch name {
			case "nil":
				header = nil
			case "error":
				responseErr = errors.New("forward failed")
			case "foreign":
				header.ClusterId++
			case "zero_cluster":
				header.ClusterId = 0
			case "unknown_member":
				header.MemberId++
			case "zero_member":
				header.MemberId = 0
			case "zero_term":
				header.RaftTerm = 0
			case "negative_revision":
				header.Revision = -1
			}
			s.observeForwardedRevision(header, responseErr)
			require.Zero(t, s.forwardedResponseTerm.Load())
		})
	}
}

func TestForwardedResponseTermConcurrentMonotonicFloor(t *testing.T) {
	s, closeFn := newTestRPCServer(t)
	defer closeFn()
	s.peers = testPeerService{currentTermFn: func() uint64 { return 1 }}
	var wg sync.WaitGroup
	for i := uint64(1); i <= 100; i++ {
		header := proxiedResponseHeader(s, 0)
		header.RaftTerm = i
		wg.Add(1)
		go func() {
			defer wg.Done()
			s.observeForwardedRevision(header, nil)
			term, err := s.responseRaftTerm(context.Background())
			require.NoError(t, err)
			require.GreaterOrEqual(t, term, header.RaftTerm)
		}()
	}
	wg.Wait()
	require.Equal(t, uint64(100), s.forwardedResponseTerm.Load())
	require.Equal(t, uint64(1), s.peers.CurrentLeadershipTerm())
}
