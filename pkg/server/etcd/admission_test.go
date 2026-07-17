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
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
)

type blockingHealthServer struct {
	healthpb.UnimplementedHealthServer
	entered chan struct{}
	release chan struct{}
}

type streamingHealthServer struct {
	healthpb.UnimplementedHealthServer
	entered chan struct{}
}

func (s *streamingHealthServer) Watch(_ *healthpb.HealthCheckRequest, stream grpc.ServerStreamingServer[healthpb.HealthCheckResponse]) error {
	close(s.entered)
	<-stream.Context().Done()
	return stream.Context().Err()
}

func (s *blockingHealthServer) Check(context.Context, *healthpb.HealthCheckRequest) (*healthpb.HealthCheckResponse, error) {
	select {
	case s.entered <- struct{}{}:
	default:
	}
	<-s.release
	return &healthpb.HealthCheckResponse{Status: healthpb.HealthCheckResponse_SERVING}, nil
}

func startAdmissionServer(t *testing.T, options []grpc.ServerOption, service healthpb.HealthServer) *bufconn.Listener {
	t.Helper()
	listener := bufconn.Listen(1 << 20)
	server := grpc.NewServer(options...)
	healthpb.RegisterHealthServer(server, service)
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(server.Stop)
	return listener
}

func admissionClient(t *testing.T, listener *bufconn.Listener) healthpb.HealthClient {
	t.Helper()
	conn, err := grpc.NewClient("passthrough:///admission",
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
			return listener.Dial()
		}))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, conn.Close()) })
	return healthpb.NewHealthClient(conn)
}

func TestClientAdmissionLimitsAcrossConnectionsAndReservesPeer(t *testing.T) {
	rpc, closeFn := newTestRPCServer(t)
	defer closeFn()
	rpc.SetMaxRequestsInFlight(1)

	blocking := &blockingHealthServer{entered: make(chan struct{}, 1), release: make(chan struct{})}
	clientListener := startAdmissionServer(t, rpc.ClientServerOptions(), blocking)
	first := admissionClient(t, clientListener)
	second := admissionClient(t, clientListener)

	firstDone := make(chan error, 1)
	go func() {
		_, err := first.Check(context.Background(), &healthpb.HealthCheckRequest{})
		firstDone <- err
	}()
	select {
	case <-blocking.entered:
	case <-time.After(2 * time.Second):
		t.Fatal("first request did not occupy the admission slot")
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	_, err := second.Check(ctx, &healthpb.HealthCheckRequest{})
	cancel()
	require.Equal(t, codes.ResourceExhausted, status.Code(err))
	require.Equal(t, "etcdserver: too many requests", status.Convert(err).Message())

	peerHealth := health.NewServer()
	peerHealth.SetServingStatus("", healthpb.HealthCheckResponse_SERVING)
	peerListener := startAdmissionServer(t, rpc.PeerServerOptions(), peerHealth)
	peer := admissionClient(t, peerListener)
	ctx, cancel = context.WithTimeout(context.Background(), time.Second)
	response, err := peer.Check(ctx, &healthpb.HealthCheckRequest{})
	cancel()
	require.NoError(t, err, "peer listener must retain capacity during client overload")
	require.Equal(t, healthpb.HealthCheckResponse_SERVING, response.Status)

	close(blocking.release)
	require.NoError(t, <-firstDone)
	require.Zero(t, rpc.requestsInFlight)
}

func TestClientAdmissionCountsStreamLifetime(t *testing.T) {
	rpc, closeFn := newTestRPCServer(t)
	defer closeFn()
	rpc.SetMaxRequestsInFlight(1)

	streaming := &streamingHealthServer{entered: make(chan struct{})}
	listener := startAdmissionServer(t, rpc.ClientServerOptions(), streaming)
	watchClient := admissionClient(t, listener)
	unaryClient := admissionClient(t, listener)

	watchCtx, cancelWatch := context.WithCancel(context.Background())
	watch, err := watchClient.Watch(watchCtx, &healthpb.HealthCheckRequest{})
	require.NoError(t, err)
	recvDone := make(chan error, 1)
	go func() {
		_, recvErr := watch.Recv()
		recvDone <- recvErr
	}()
	select {
	case <-streaming.entered:
	case <-time.After(2 * time.Second):
		t.Fatal("watch did not occupy the admission slot")
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	_, err = unaryClient.Check(ctx, &healthpb.HealthCheckRequest{})
	cancel()
	require.Equal(t, codes.ResourceExhausted, status.Code(err))

	cancelWatch()
	require.Error(t, <-recvDone)
	require.Eventually(t, func() bool {
		rpc.admissionMu.Lock()
		defer rpc.admissionMu.Unlock()
		return rpc.requestsInFlight == 0
	}, time.Second, time.Millisecond)
}

func TestClientAdmissionDisabledDoesNotTrackRequests(t *testing.T) {
	rpc, closeFn := newTestRPCServer(t)
	defer closeFn()
	require.True(t, rpc.acquireRequest("method", "unary"))
	require.Zero(t, rpc.requestsInFlight)
}

func TestClientRequestRateLimitsUnaryAndReservesPeer(t *testing.T) {
	rpc, closeFn := newTestRPCServer(t)
	defer closeFn()
	rpc.SetRequestRateLimit(1, 2)

	clientHealth := health.NewServer()
	clientHealth.SetServingStatus("", healthpb.HealthCheckResponse_SERVING)
	clientListener := startAdmissionServer(t, rpc.ClientServerOptions(), clientHealth)
	client := admissionClient(t, clientListener)
	for i := 0; i < 2; i++ {
		response, err := client.Check(context.Background(), &healthpb.HealthCheckRequest{})
		require.NoError(t, err)
		require.Equal(t, healthpb.HealthCheckResponse_SERVING, response.Status)
	}
	_, err := client.Check(context.Background(), &healthpb.HealthCheckRequest{})
	require.Equal(t, codes.ResourceExhausted, status.Code(err))
	require.Equal(t, "etcdserver: too many requests", status.Convert(err).Message())

	peerHealth := health.NewServer()
	peerHealth.SetServingStatus("", healthpb.HealthCheckResponse_SERVING)
	peerListener := startAdmissionServer(t, rpc.PeerServerOptions(), peerHealth)
	peer := admissionClient(t, peerListener)
	response, err := peer.Check(context.Background(), &healthpb.HealthCheckRequest{})
	require.NoError(t, err, "peer listener must not consume the public rate budget")
	require.Equal(t, healthpb.HealthCheckResponse_SERVING, response.Status)
}

func TestClientRequestRateCountsEveryWatchStreamMessage(t *testing.T) {
	rpc, closeFn := newTestRPCServer(t)
	defer closeFn()
	rpc.SetRequestRateLimit(1, 1)

	listener := bufconn.Listen(1 << 20)
	server := grpc.NewServer(rpc.ClientServerOptions()...)
	etcdserverpb.RegisterWatchServer(server, rpc)
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(server.Stop)
	conn, err := grpc.NewClient("passthrough:///rate-limited-watch",
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
			return listener.Dial()
		}))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, conn.Close()) })

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	stream, err := etcdserverpb.NewWatchClient(conn).Watch(ctx)
	require.NoError(t, err)
	require.NoError(t, stream.Send(&etcdserverpb.WatchRequest{
		RequestUnion: &etcdserverpb.WatchRequest_CreateRequest{
			CreateRequest: &etcdserverpb.WatchCreateRequest{Key: []byte("/rate/first"), WatchId: 1},
		},
	}))
	created, err := stream.Recv()
	require.NoError(t, err)
	require.True(t, created.Created)
	require.Equal(t, int64(1), created.WatchId)

	// The server may close as soon as it receives this message, so Send can race
	// with the terminal status and return EOF. Recv is the authoritative status.
	_ = stream.Send(&etcdserverpb.WatchRequest{
		RequestUnion: &etcdserverpb.WatchRequest_CreateRequest{
			CreateRequest: &etcdserverpb.WatchCreateRequest{Key: []byte("/rate/second"), WatchId: 2},
		},
	})
	_, err = stream.Recv()
	require.Equal(t, codes.ResourceExhausted, status.Code(err))
	require.Equal(t, "etcdserver: too many requests", status.Convert(err).Message())
}

func TestClientRequestRateDisabledDoesNotReject(t *testing.T) {
	rpc, closeFn := newTestRPCServer(t)
	defer closeFn()
	for i := 0; i < 100; i++ {
		require.True(t, rpc.allowRequestRate("method", "unary"))
	}
}

func TestClientRequestRateBurstIsAtomicAcrossConnections(t *testing.T) {
	rpc, closeFn := newTestRPCServer(t)
	defer closeFn()
	const (
		burst      = 7
		contenders = 64
	)
	rpc.SetRequestRateLimit(1, burst)

	start := make(chan struct{})
	results := make(chan bool, contenders)
	var wg sync.WaitGroup
	wg.Add(contenders)
	for i := 0; i < contenders; i++ {
		go func() {
			defer wg.Done()
			<-start
			results <- rpc.allowRequestRate("method", "unary")
		}()
	}
	close(start)
	wg.Wait()
	close(results)

	admitted := 0
	for allowed := range results {
		if allowed {
			admitted++
		}
	}
	require.Equal(t, burst, admitted)
}
