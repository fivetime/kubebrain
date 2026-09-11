package main

import (
	"context"
	"encoding/json"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"
)

type authTraceAuthServer struct {
	etcdserverpb.UnimplementedAuthServer
}

func (authTraceAuthServer) Authenticate(context.Context, *etcdserverpb.AuthenticateRequest) (*etcdserverpb.AuthenticateResponse, error) {
	return &etcdserverpb.AuthenticateResponse{Token: "secret-server-token"}, nil
}

type authTraceWatchServer struct {
	etcdserverpb.UnimplementedWatchServer
}

func (authTraceWatchServer) Watch(stream etcdserverpb.Watch_WatchServer) error {
	if _, err := stream.Recv(); err != nil {
		return err
	}
	return status.Error(codes.Unauthenticated, "secret-rejection-detail")
}

func TestRestoredAuthRPCTraceCapturesRealGRPCPeer(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	server := grpc.NewServer()
	etcdserverpb.RegisterAuthServer(server, authTraceAuthServer{})
	etcdserverpb.RegisterWatchServer(server, authTraceWatchServer{})
	done := make(chan struct{})
	go func() { defer close(done); _ = server.Serve(listener) }()
	t.Cleanup(func() { server.Stop(); <-done })
	trace := &restoredAuthRPCTrace{}
	cfg := trace.config(clientv3.Config{DialOptions: []grpc.DialOption{
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	}})
	conn, err := grpc.NewClient(listener.Addr().String(), cfg.DialOptions...)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, conn.Close()) })
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	_, err = etcdserverpb.NewAuthClient(conn).Authenticate(ctx, &etcdserverpb.AuthenticateRequest{
		Name: "secret-user", Password: "secret-password",
	})
	require.NoError(t, err)
	stream, err := etcdserverpb.NewWatchClient(conn).Watch(ctx)
	require.NoError(t, err)
	require.NoError(t, stream.Send(&etcdserverpb.WatchRequest{RequestUnion: &etcdserverpb.WatchRequest_CreateRequest{
		CreateRequest: &etcdserverpb.WatchCreateRequest{Key: []byte("secret-key")},
	}}))
	_, err = stream.Recv()
	require.Equal(t, codes.Unauthenticated, status.Code(err))
	var result struct {
		Recent []restoredAuthRPCEvent `json:"recent"`
	}
	require.NoError(t, json.Unmarshal([]byte(trace.summary()), &result))
	require.Len(t, result.Recent, 2)
	for _, event := range result.Recent {
		require.Equal(t, listener.Addr().String(), event.Peer)
	}
	require.Equal(t, "OK", result.Recent[0].Code)
	require.Equal(t, "Unauthenticated", result.Recent[1].Code)
	require.NotContains(t, trace.summary(), "secret-")
}

func TestRestoredAuthRPCTracePreservesUnaryErrorWithoutPayloads(t *testing.T) {
	trace := &restoredAuthRPCTrace{}
	want := status.Error(codes.Unauthenticated, "secret-token-error")
	ctx := metadata.NewOutgoingContext(t.Context(), metadata.Pairs("token", "secret-token-metadata"))
	calls := 0
	err := trace.unary(ctx, "/etcdserverpb.Auth/Authenticate", "secret-password", "secret-response", nil,
		func(_ context.Context, _ string, _, _ any, _ *grpc.ClientConn, opts ...grpc.CallOption) error {
			calls++
			for _, opt := range opts {
				if option, ok := opt.(grpc.PeerCallOption); ok {
					option.PeerAddr.Addr = &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 2379}
				}
			}
			return want
		})
	require.Same(t, want, err)
	require.Equal(t, 1, calls, "diagnostics must not retry")
	summary := trace.summary()
	require.Contains(t, summary, "127.0.0.1:2379")
	require.Contains(t, summary, "Unauthenticated")
	require.NotContains(t, summary, "secret-")
}

type authTraceTestStream struct {
	grpc.ClientStream
	ctx      context.Context
	err      error
	receives int
}

func (s *authTraceTestStream) Context() context.Context { return s.ctx }
func (s *authTraceTestStream) RecvMsg(any) error        { s.receives++; return s.err }

func TestRestoredAuthRPCTracePreservesStreamReceiveAndOpenErrors(t *testing.T) {
	for _, openFails := range []bool{false, true} {
		trace := &restoredAuthRPCTrace{}
		want := status.Error(codes.Unauthenticated, "secret-token")
		underlying := &authTraceTestStream{ctx: peer.NewContext(t.Context(), &peer.Peer{
			Addr: &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 2381},
		}), err: want}
		stream, err := trace.stream(t.Context(), &grpc.StreamDesc{}, nil, "/etcdserverpb.Watch/Watch",
			func(context.Context, *grpc.StreamDesc, *grpc.ClientConn, string, ...grpc.CallOption) (grpc.ClientStream, error) {
				if openFails {
					return nil, want
				}
				return underlying, nil
			})
		if openFails {
			require.Same(t, want, err)
			require.Nil(t, stream)
		} else {
			require.NoError(t, err)
			require.Same(t, want, stream.RecvMsg("secret-message"))
			require.Equal(t, 1, underlying.receives)
			require.Contains(t, trace.summary(), "127.0.0.1:2381")
		}
		require.Contains(t, trace.summary(), "Unauthenticated")
		require.NotContains(t, trace.summary(), "secret-")
	}
}

func TestRestoredAuthRPCTraceIsBoundedAndConfigDoesNotAlias(t *testing.T) {
	trace := &restoredAuthRPCTrace{}
	trace.record("/etcdserverpb.Auth/Authenticate", "unary", nil, nil)
	var wg sync.WaitGroup
	for range 100 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			trace.record("/etcdserverpb.KV/Range", "unary", nil, nil)
			_ = trace.summary()
		}()
	}
	wg.Wait()
	var result struct {
		Auth   *restoredAuthRPCEvent  `json:"last_auth"`
		Recent []restoredAuthRPCEvent `json:"recent"`
	}
	require.NoError(t, json.Unmarshal([]byte(trace.summary()), &result))
	require.Len(t, result.Recent, 16)
	require.NotNil(t, result.Auth)
	options := make([]grpc.DialOption, 3)
	options[0] = grpc.WithUserAgent("test")
	cfg := trace.config(clientv3.Config{DialOptions: options[:1]})
	require.Len(t, cfg.DialOptions, 3)
	require.Nil(t, options[1])
	require.Nil(t, options[2])
}
