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
	"net"
	"testing"

	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/proto"
)

func TestUnaryRequestLimitUsesEtcdPayloadBoundaryAndError(t *testing.T) {
	request := &etcdserverpb.PutRequest{Key: []byte("key"), Value: []byte("value")}
	size := uint(proto.Size(request))
	server := &RPCServer{maxRequestBytes: size - 1}
	called := false
	handler := func(context.Context, any) (any, error) {
		called = true
		return nil, rpctypes.ErrGRPCKeyNotFound
	}

	_, err := server.stampUnary(context.Background(), request, &grpc.UnaryServerInfo{}, handler)
	require.False(t, called, "an oversized request must not reach storage handlers")
	require.Equal(t, status.Code(rpctypes.ErrGRPCRequestTooLarge), status.Code(err))
	require.Equal(t, status.Convert(rpctypes.ErrGRPCRequestTooLarge).Message(), status.Convert(err).Message())

	server.maxRequestBytes = size
	_, err = server.stampUnary(context.Background(), request, &grpc.UnaryServerInfo{}, handler)
	require.True(t, called, "a request exactly at the limit must be admitted")
	require.Error(t, err)
}

func TestResponseHeadersIncludeSharedRaftTerm(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()

	handler := func(context.Context, any) (any, error) {
		return &etcdserverpb.RangeResponse{Header: &etcdserverpb.ResponseHeader{}}, nil
	}
	reply, err := server.stampUnary(context.Background(), &etcdserverpb.RangeRequest{}, &grpc.UnaryServerInfo{}, handler)
	require.NoError(t, err)
	require.Equal(t, uint64(1), reply.(*etcdserverpb.RangeResponse).Header.RaftTerm)

	watch := &etcdserverpb.WatchResponse{Header: &etcdserverpb.ResponseHeader{}}
	stampHeader(watch, 2, 3, 4)
	require.Equal(t, uint64(2), watch.Header.ClusterId)
	require.Equal(t, uint64(3), watch.Header.MemberId)
	require.Equal(t, uint64(4), watch.Header.RaftTerm)
}

func TestResponseRaftTermUsesCacheAndClassifiesInitialReadFailure(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()

	server.peers = testPeerService{
		currentTermFn: func() uint64 { return 7 },
		leadershipTermFn: func(context.Context) (uint64, error) {
			t.Fatal("a populated term cache must avoid the TiKV-backed election record")
			return 0, nil
		},
	}
	handler := func(context.Context, any) (any, error) {
		return &etcdserverpb.RangeResponse{Header: &etcdserverpb.ResponseHeader{}}, nil
	}
	reply, err := server.stampUnary(context.Background(), &etcdserverpb.RangeRequest{}, &grpc.UnaryServerInfo{}, handler)
	require.NoError(t, err)
	require.Equal(t, uint64(7), reply.(*etcdserverpb.RangeResponse).Header.RaftTerm)

	termErr := errors.New("election record unavailable")
	server.peers = testPeerService{
		currentTermFn:    func() uint64 { return 0 },
		leadershipTermFn: func(context.Context) (uint64, error) { return 0, termErr },
	}
	reply, err = server.stampUnary(context.Background(), &etcdserverpb.RangeRequest{}, &grpc.UnaryServerInfo{}, handler)
	require.Nil(t, reply)
	require.Equal(t, codes.Unavailable, status.Code(err))
	require.Equal(t, termErr.Error(), status.Convert(err).Message())
}

func TestRequestLimitReturnsEtcdErrorOverGRPC(t *testing.T) {
	server := &RPCServer{maxTxnOps: defaultMaxTxnOps, maxRequestBytes: 32}
	grpcServer := grpc.NewServer(server.ClientServerOptions()...)
	etcdserverpb.RegisterKVServer(grpcServer, server)
	listener := bufconn.Listen(1 << 20)
	go func() { _ = grpcServer.Serve(listener) }()
	t.Cleanup(grpcServer.Stop)

	conn, err := grpc.NewClient("passthrough:///bufnet",
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return listener.Dial() }))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, conn.Close()) })

	request := &etcdserverpb.PutRequest{Key: []byte("key"), Value: make([]byte, 64)}
	_, err = etcdserverpb.NewKVClient(conn).Put(context.Background(), request)
	require.Equal(t, status.Code(rpctypes.ErrGRPCRequestTooLarge), status.Code(err))
	require.Equal(t, status.Convert(rpctypes.ErrGRPCRequestTooLarge).Message(), status.Convert(err).Message())
}

func TestRequestLimitTransportAllowanceMatchesEtcd(t *testing.T) {
	server := &RPCServer{maxTxnOps: defaultMaxTxnOps, maxRequestBytes: 1024}
	grpcServer := grpc.NewServer(server.ClientServerOptions()...)
	etcdserverpb.RegisterKVServer(grpcServer, server)
	listener := bufconn.Listen(1 << 20)
	go func() { _ = grpcServer.Serve(listener) }()
	t.Cleanup(grpcServer.Stop)

	conn, err := grpc.NewClient("passthrough:///bufnet",
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return listener.Dial() }))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, conn.Close()) })

	// This exceeds both the payload limit and KubeBrain's old max+512-byte
	// transport window, but remains within etcd's max+512-KiB allowance.
	request := &etcdserverpb.PutRequest{Key: []byte("key"), Value: make([]byte, 256*1024)}
	_, err = etcdserverpb.NewKVClient(conn).Put(context.Background(), request)
	require.Equal(t, status.Code(rpctypes.ErrGRPCRequestTooLarge), status.Code(err))
	require.Equal(t, status.Convert(rpctypes.ErrGRPCRequestTooLarge).Message(), status.Convert(err).Message())
}

func TestStreamRequestLimitUsesEtcdPayloadBoundaryAndError(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()
	server.SetRequestLimits(defaultMaxTxnOps, 64)

	grpcServer := grpc.NewServer(server.ClientServerOptions()...)
	etcdserverpb.RegisterWatchServer(grpcServer, server)
	listener := bufconn.Listen(1 << 20)
	go func() { _ = grpcServer.Serve(listener) }()
	t.Cleanup(grpcServer.Stop)

	conn, err := grpc.NewClient("passthrough:///stream-request-limit",
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return listener.Dial() }))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, conn.Close()) })

	stream, err := etcdserverpb.NewWatchClient(conn).Watch(context.Background())
	require.NoError(t, err)
	request := &etcdserverpb.WatchRequest{
		RequestUnion: &etcdserverpb.WatchRequest_CreateRequest{
			CreateRequest: &etcdserverpb.WatchCreateRequest{
				Key:     make([]byte, 128),
				WatchId: 1,
			},
		},
	}
	require.Greater(t, proto.Size(request), 64)
	require.NoError(t, stream.Send(request),
		"the message fits etcd's transport allowance and must reach the logical payload check")
	_, err = stream.Recv()
	require.Equal(t, status.Code(rpctypes.ErrGRPCRequestTooLarge), status.Code(err))
	require.Equal(t, status.Convert(rpctypes.ErrGRPCRequestTooLarge).Message(), status.Convert(err).Message())
}
