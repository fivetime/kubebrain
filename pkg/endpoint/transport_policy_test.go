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

package endpoint

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

func TestMaxConcurrentStreamsLimitsOneConnection(t *testing.T) {
	grpcServer := grpc.NewServer(grpcTransportOptions(&Config{MaxConcurrentStreams: 1})...)
	healthServer := health.NewServer()
	healthServer.SetServingStatus("", healthpb.HealthCheckResponse_SERVING)
	healthpb.RegisterHealthServer(grpcServer, healthServer)
	listener := bufconn.Listen(1 << 20)
	go func() { _ = grpcServer.Serve(listener) }()
	t.Cleanup(grpcServer.Stop)

	conn, err := grpc.NewClient("passthrough:///bufnet",
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return listener.Dial() }))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, conn.Close()) })
	client := healthpb.NewHealthClient(conn)

	watchCtx, cancelWatch := context.WithCancel(context.Background())
	watch, err := client.Watch(watchCtx, &healthpb.HealthCheckRequest{})
	require.NoError(t, err)
	_, err = watch.Recv()
	require.NoError(t, err)

	blockedCtx, cancelBlocked := context.WithTimeout(context.Background(), 150*time.Millisecond)
	_, err = client.Check(blockedCtx, &healthpb.HealthCheckRequest{})
	cancelBlocked()
	require.Equal(t, codes.DeadlineExceeded, status.Code(err), "the held watch must consume the only HTTP/2 stream slot")

	cancelWatch()
	_, _ = watch.Recv()
	readyCtx, cancelReady := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancelReady()
	response, err := client.Check(readyCtx, &healthpb.HealthCheckRequest{})
	require.NoError(t, err)
	require.Equal(t, healthpb.HealthCheckResponse_SERVING, response.Status)
}

func TestKeepaliveOptionEnableConditionsMatchEtcd(t *testing.T) {
	// MaxConcurrentStreams is always present.
	require.Len(t, grpcTransportOptions(&Config{}), 1)
	require.Len(t, grpcTransportOptions(&Config{GRPCKeepAliveMinTime: time.Second}), 2)
	require.Len(t, grpcTransportOptions(&Config{GRPCKeepAliveInterval: time.Second}), 1)
	require.Len(t, grpcTransportOptions(&Config{GRPCKeepAliveTimeout: time.Second}), 1)
	require.Len(t, grpcTransportOptions(&Config{
		GRPCKeepAliveInterval: time.Second, GRPCKeepAliveTimeout: time.Second,
	}), 2)
	require.Len(t, grpcTransportOptions(&Config{
		GRPCMaxConnectionAge: time.Hour, GRPCMaxConnectionAgeGrace: time.Minute,
	}), 2)
	require.Len(t, grpcTransportOptions(&Config{
		GRPCKeepAliveInterval: time.Second, GRPCKeepAliveTimeout: time.Second,
		GRPCMaxConnectionAge: time.Hour, GRPCMaxConnectionAgeGrace: time.Minute,
	}), 2, "server ping and connection aging must share one keepalive parameter option")
}
