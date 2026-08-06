package compat

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest/observer"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type retryPeerLogKVServer struct {
	etcdserverpb.UnimplementedKVServer
}

func (retryPeerLogKVServer) Range(context.Context, *etcdserverpb.RangeRequest) (*etcdserverpb.RangeResponse, error) {
	return nil, status.Error(codes.Unavailable, "injected retryable range failure")
}

// TestClientV3UnaryRetryLogsConnectedPeer pins upstream etcd 1fd87206f. Unary
// retry warnings must include the concrete peer chosen by grpc, not just the
// resolver target; DBaaS multi-endpoint operators depend on this to identify
// the replica that produced a retryable error.
func TestClientV3UnaryRetryLogsConnectedPeer(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	server := grpc.NewServer()
	etcdserverpb.RegisterKVServer(server, retryPeerLogKVServer{})
	serveDone := make(chan error, 1)
	go func() {
		serveDone <- server.Serve(listener)
	}()
	t.Cleanup(func() {
		server.Stop()
		require.NoError(t, <-serveDone)
	})

	core, observed := observer.New(zap.WarnLevel)
	client, err := clientv3.New(clientv3.Config{
		Endpoints:          []string{"http://" + listener.Addr().String()},
		DialTimeout:        3 * time.Second,
		MaxUnaryRetries:    1,
		BackoffWaitBetween: time.Millisecond,
		Logger:             zap.New(core),
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, client.Close()) })

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	_, err = client.Get(ctx, "retry-peer-log")
	require.Error(t, err)

	var retryLog observer.LoggedEntry
	for _, entry := range observed.All() {
		if entry.Message == "retrying of unary invoker failed" {
			retryLog = entry
			break
		}
	}
	require.NotZero(t, retryLog.Message, "expected unary retry failure log")
	peer := retryLog.ContextMap()["peer"]
	require.Contains(t, peer, listener.Addr().String())
	require.Contains(t, peer, "AuthInfo: 'insecure'")
	require.Equal(t, "/etcdserverpb.KV/Range", retryLog.ContextMap()["method"])
}
