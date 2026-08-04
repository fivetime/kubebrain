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
	"go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"

	brainpb "github.com/kubewharf/kubebrain-client/api/v2rpc"
)

type nativeAuthBoundaryReadServer struct {
	*brainpb.UnimplementedReadServer
}

func (nativeAuthBoundaryReadServer) Get(context.Context, *brainpb.GetRequest) (*brainpb.GetResponse, error) {
	return &brainpb.GetResponse{}, nil
}

func (nativeAuthBoundaryReadServer) RangeStream(_ *brainpb.RangeRequest, stream brainpb.Read_RangeStreamServer) error {
	<-stream.Context().Done()
	return context.Cause(stream.Context())
}

func startNativeAuthBoundaryReadClient(t *testing.T, options ...grpc.ServerOption) brainpb.ReadClient {
	t.Helper()
	listener := bufconn.Listen(1024 * 1024)
	grpcServer := grpc.NewServer(options...)
	brainpb.RegisterReadServer(grpcServer, nativeAuthBoundaryReadServer{UnimplementedReadServer: &brainpb.UnimplementedReadServer{}})
	go func() {
		_ = grpcServer.Serve(listener)
	}()
	t.Cleanup(func() {
		grpcServer.Stop()
		require.NoError(t, listener.Close())
	})
	connection, err := grpc.NewClient("passthrough:///bufconn",
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return listener.Dial() }))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, connection.Close()) })
	return brainpb.NewReadClient(connection)
}

func TestNativeBrainClientRPCsFailClosedWhileEtcdAuthIsEnabled(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()

	ctx := context.Background()
	unaryCalled := false
	unaryHandler := func(context.Context, any) (any, error) {
		unaryCalled = true
		return "served", nil
	}
	streamCalled := false
	streamHandler := func(any, grpc.ServerStream) error {
		streamCalled = true
		return nil
	}
	stream := &serverStreamWithContext{ctx: ctx}

	response, err := server.rejectNativeClientAuthBypassUnary(ctx, nil,
		&grpc.UnaryServerInfo{FullMethod: "/Write/Create"}, unaryHandler)
	require.NoError(t, err)
	require.Equal(t, "served", response)
	require.True(t, unaryCalled, "legacy native clients must keep working while auth is disabled")

	_ = setupAuthKVUser(t, server)
	unaryCalled = false
	_, err = server.rejectNativeClientAuthBypassUnary(ctx, nil,
		&grpc.UnaryServerInfo{FullMethod: "/Read/Get"}, unaryHandler)
	require.ErrorIs(t, err, rpctypes.ErrGRPCPermissionDenied)
	require.Equal(t, codes.PermissionDenied, status.Code(err))
	require.False(t, unaryCalled, "native read handler would bypass etcd key authorization")

	err = server.rejectNativeClientAuthBypassStream(nil, stream,
		&grpc.StreamServerInfo{FullMethod: "/Watch/Watch"}, streamHandler)
	require.ErrorIs(t, err, rpctypes.ErrGRPCPermissionDenied)
	require.False(t, streamCalled, "native watch handler would bypass etcd key authorization")

	// The client-only boundary must not accidentally reject an etcd service or
	// an unrelated gRPC service just because auth is enabled.
	unaryCalled = false
	response, err = server.rejectNativeClientAuthBypassUnary(ctx, nil,
		&grpc.UnaryServerInfo{FullMethod: "/etcdserverpb.KV/Range"}, unaryHandler)
	require.NoError(t, err)
	require.Equal(t, "served", response)
	require.True(t, unaryCalled)
}

func TestNativeBrainAuthBoundaryIsAppliedOnlyToClientListener(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()

	client := startNativeAuthBoundaryReadClient(t, server.ClientServerOptions()...)
	peer := startNativeAuthBoundaryReadClient(t, server.PeerServerOptions()...)
	ctx := context.Background()
	_, err := client.Get(ctx, &brainpb.GetRequest{Key: []byte("key")})
	require.NoError(t, err)
	stream, err := client.RangeStream(ctx, &brainpb.RangeRequest{Key: []byte("a"), End: []byte("z")})
	require.NoError(t, err)
	streamResult := make(chan error, 1)
	go func() {
		_, receiveErr := stream.Recv()
		streamResult <- receiveErr
	}()

	_ = setupAuthKVUser(t, server)
	_, err = client.Get(ctx, &brainpb.GetRequest{Key: []byte("key")})
	require.ErrorIs(t, err, rpctypes.ErrGRPCPermissionDenied)
	require.Equal(t, codes.PermissionDenied, status.Code(err))

	_, err = peer.Get(ctx, &brainpb.GetRequest{Key: []byte("key")})
	require.NoError(t, err, "internal peer coordination must not be disabled by public etcd auth")

	select {
	case err = <-streamResult:
		require.ErrorIs(t, err, rpctypes.ErrGRPCPermissionDenied)
		require.Equal(t, codes.PermissionDenied, status.Code(err))
	case <-time.After(3 * time.Second):
		t.Fatal("native stream opened before AuthEnable remained readable")
	}
}

func TestAuthEnableDrainsAdmittedNativeUnaryBeforeCommitting(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()
	ctx := context.Background()

	_, err := server.UserAdd(ctx, &etcdserverpb.AuthUserAddRequest{Name: "root", Password: "secret"})
	require.NoError(t, err)
	_, err = server.RoleAdd(ctx, &etcdserverpb.AuthRoleAddRequest{Name: "root"})
	require.NoError(t, err)
	_, err = server.UserGrantRole(ctx, &etcdserverpb.AuthUserGrantRoleRequest{User: "root", Role: "root"})
	require.NoError(t, err)

	entered := make(chan struct{})
	release := make(chan struct{})
	nativeDone := make(chan error, 1)
	go func() {
		_, callErr := server.rejectNativeClientAuthBypassUnary(ctx, nil,
			&grpc.UnaryServerInfo{FullMethod: "/Write/Create"},
			func(context.Context, any) (any, error) {
				close(entered)
				<-release
				return nil, nil
			})
		nativeDone <- callErr
	}()
	<-entered

	enableDone := make(chan error, 1)
	go func() {
		_, enableErr := server.AuthEnable(ctx, &etcdserverpb.AuthEnableRequest{})
		enableDone <- enableErr
	}()
	select {
	case err = <-enableDone:
		t.Fatalf("AuthEnable crossed an admitted native unary: %v", err)
	case <-time.After(100 * time.Millisecond):
	}

	close(release)
	require.NoError(t, <-nativeDone)
	require.NoError(t, <-enableDone)

	_, err = server.rejectNativeClientAuthBypassUnary(ctx, nil,
		&grpc.UnaryServerInfo{FullMethod: "/Write/Create"},
		func(context.Context, any) (any, error) { return nil, nil })
	require.ErrorIs(t, err, rpctypes.ErrGRPCPermissionDenied)
}

func TestNativeBrainMethodClassificationIsExact(t *testing.T) {
	for _, method := range []string{
		"/Read/Get", "/Read/Range", "/Read/Count", "/Read/ListPartition", "/Read/RangeStream",
		"/Write/Create", "/Write/Update", "/Write/Delete", "/Write/Compact",
		"/Watch/Watch",
	} {
		require.Truef(t, isNativeBrainMethod(method), "%s must remain behind the auth boundary", method)
	}
	for _, method := range []string{
		"", "/Read", "/Reader/Get", "/Write", "/Writer/Create", "/Watch",
		"/etcdserverpb.Watch/Watch", "/grpc.health.v1.Health/Watch",
	} {
		require.Falsef(t, isNativeBrainMethod(method), "%s is not a native Brain RPC", method)
	}
}
