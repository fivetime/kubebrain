package compat

import (
	"context"
	"errors"
	"net"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

// TestClientStaticTokenRetryDifferentialAgainstReferenceEtcd pins upstream
// etcd 572ac40db. A caller-supplied token is not refreshable: when the server
// rejects it, clientv3 must return the authentication error without replaying
// the request up to MaxUnaryRetries. The second half proves that the same
// client build can still complete real Range RPCs against both dataplanes.
func TestClientStaticTokenRetryDifferentialAgainstReferenceEtcd(t *testing.T) {
	reference := os.Getenv("REFERENCE_ETCD_ENDPOINT")
	if reference == "" {
		t.Skip("set REFERENCE_ETCD_ENDPOINT to run differential compatibility tests")
	}

	runClientStaticTokenRetryOracle(t)
	for name, endpoint := range map[string]string{
		"reference": reference,
		"kubebrain": compatEndpoint(t),
	} {
		t.Run(name, func(t *testing.T) {
			client, err := clientv3.New(clientv3.Config{
				Endpoints:   []string{endpoint},
				DialTimeout: 5 * time.Second,
			})
			require.NoError(t, err)
			defer func() { require.NoError(t, client.Close()) }()
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			response, err := client.Get(ctx, "/a3748/client-static-token/missing")
			require.NoError(t, err)
			require.Empty(t, response.Kvs)
			require.NotNil(t, response.Header)
			require.Positive(t, response.Header.Revision)
		})
	}
}

type rejectingStaticTokenKVServer struct {
	etcdserverpb.UnimplementedKVServer
	calls  atomic.Int32
	mu     sync.Mutex
	tokens []string
}

func (server *rejectingStaticTokenKVServer) Range(ctx context.Context, _ *etcdserverpb.RangeRequest) (*etcdserverpb.RangeResponse, error) {
	server.calls.Add(1)
	md, _ := metadata.FromIncomingContext(ctx)
	server.mu.Lock()
	server.tokens = append(server.tokens, md.Get(rpctypes.TokenFieldNameGRPC)...)
	server.mu.Unlock()
	return nil, rpctypes.ErrGRPCInvalidAuthToken
}

func runClientStaticTokenRetryOracle(t *testing.T) {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	server := grpc.NewServer()
	rejecting := &rejectingStaticTokenKVServer{}
	etcdserverpb.RegisterKVServer(server, rejecting)
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(func() {
		require.NoError(t, listener.Close())
		server.Stop()
	})

	const token = "caller-supplied-static-token"
	client, err := clientv3.New(clientv3.Config{
		Endpoints:          []string{listener.Addr().String()},
		DialTimeout:        5 * time.Second,
		Token:              token,
		MaxUnaryRetries:    7,
		BackoffWaitBetween: time.Millisecond,
		Logger:             zap.NewNop(),
	})
	require.NoError(t, err)
	defer func() { require.NoError(t, client.Close()) }()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err = client.Get(ctx, "/rejected")
	require.Error(t, err)
	require.ErrorIs(t, err, rpctypes.ErrInvalidAuthToken)
	require.True(t, errors.Is(err, rpctypes.ErrInvalidAuthToken))
	require.Equal(t, rpctypes.ErrInvalidAuthToken.Error(), err.Error())
	require.EqualValues(t, 1, rejecting.calls.Load(), "static-token request must not be replayed")
	rejecting.mu.Lock()
	defer rejecting.mu.Unlock()
	require.Equal(t, []string{token}, rejecting.tokens)
}
