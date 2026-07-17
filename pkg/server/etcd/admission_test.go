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
	"testing"
	"time"

	"github.com/stretchr/testify/require"
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
