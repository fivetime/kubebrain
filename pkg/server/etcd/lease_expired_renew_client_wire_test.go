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
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
)

func TestClientExpiredLeaseKeepAliveOnceRoutesAfterTermEnds(t *testing.T) {
	s, closeFn := newTestRPCServer(t)
	defer closeFn()
	var leading atomic.Bool
	leading.Store(true)
	var forwarded atomic.Int32
	var keepAliveStreams atomic.Int32
	header := proxiedResponseHeader(s, int64(s.backend.GetCurrentRevision()))
	s.peers = testPeerService{isLeaderFn: leading.Load, proxyEnabled: true,
		leaseKeepAliveFn: func(_ context.Context, req *etcdserverpb.LeaseKeepAliveRequest) (*etcdserverpb.LeaseKeepAliveResponse, error) {
			forwarded.Add(1)
			return &etcdserverpb.LeaseKeepAliveResponse{Header: header, ID: req.ID, TTL: 37}, nil
		},
	}
	options := append(s.ClientServerOptions(), grpc.ChainStreamInterceptor(
		func(srv any, stream grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
			if info.FullMethod == "/etcdserverpb.Lease/LeaseKeepAlive" {
				keepAliveStreams.Add(1)
			}
			return handler(srv, stream)
		}))
	gs := grpc.NewServer(options...)
	etcdserverpb.RegisterLeaseServer(gs, s)
	listener := bufconn.Listen(1 << 20)
	defer listener.Close()
	go func() { _ = gs.Serve(listener) }()
	defer gs.Stop()
	client, err := clientv3.New(clientv3.Config{
		Endpoints: []string{"bufnet"}, DialTimeout: time.Second,
		DialOptions: []grpc.DialOption{
			grpc.WithTransportCredentials(insecure.NewCredentials()),
			grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) { return listener.DialContext(ctx) }),
		},
	})
	require.NoError(t, err)
	defer client.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	grant, err := client.Grant(ctx, 300)
	require.NoError(t, err)
	term, endTerm := context.WithCancel(context.Background())
	defer endTerm()
	observed := &leaseRenewStateWaitContext{Context: term, waiting: make(chan struct{})}
	s.leaseMu.Lock()
	st := s.leases[int64(grant.ID)]
	st.timer.Stop()
	st.deadline = time.Now().Add(-time.Second)
	s.leaseTermCtx = observed
	s.leaseMu.Unlock()
	type result struct {
		response *clientv3.LeaseKeepAliveResponse
		err      error
	}
	done := make(chan result, 1)
	joined := make(chan struct{})
	go func() {
		defer close(joined)
		response, err := client.KeepAliveOnce(ctx, grant.ID)
		done <- result{response, err}
	}()
	defer func() { cancel(); <-joined }()
	select {
	case <-observed.waiting:
	case <-time.After(time.Second):
		t.Fatal("client renewal did not enter expired wait")
	}
	leading.Store(false)
	endTerm()
	select {
	case got := <-done:
		require.NoError(t, got.err)
		require.NotNil(t, got.response)
		require.Equal(t, grant.ID, got.response.ID)
		require.Equal(t, int64(37), got.response.TTL)
		require.Equal(t, int32(1), forwarded.Load())
		// KeepAliveOnce retries failed streams internally. Success must come
		// from the original pending stream, not a fresh RPC after demotion.
		require.Equal(t, int32(1), keepAliveStreams.Load(), "renewal must survive on its original RPC stream")
		requireClientLeaseHeaderWellFormed(t, got.response.ResponseHeader)
	case <-time.After(time.Second):
		t.Fatal("official client renewal remained blocked after term ended")
	}
	select {
	case <-st.revoked:
		t.Fatal("term exit must not report durable revocation")
	default:
	}
}
