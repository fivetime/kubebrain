package compat

import (
	"context"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

// TestBearerPrefixedAuthTokenLive requires a disposable endpoint with auth
// already enabled. It sends token metadata directly so clientv3 cannot
// normalize the credential before it reaches the server.
func TestBearerPrefixedAuthTokenLive(t *testing.T) {
	endpoint := os.Getenv("KUBEBRAIN_AUTH_ENDPOINT")
	if endpoint == "" {
		t.Skip("set KUBEBRAIN_AUTH_ENDPOINT to an auth-enabled disposable endpoint")
	}
	username := os.Getenv("KUBEBRAIN_AUTH_USERNAME")
	password := os.Getenv("KUBEBRAIN_AUTH_PASSWORD")
	require.NotEmpty(t, username)
	require.NotEmpty(t, password)

	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{endpoint},
		DialTimeout: 5 * time.Second,
	})
	require.NoError(t, err)
	defer cli.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	auth, err := cli.Authenticate(ctx, username, password)
	require.NoError(t, err)
	conn, err := grpc.NewClient(
		strings.TrimPrefix(strings.TrimPrefix(endpoint, "http://"), "https://"),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	require.NoError(t, err)
	defer conn.Close()
	kv := etcdserverpb.NewKVClient(conn)

	for _, credential := range []string{auth.Token, "Bearer " + auth.Token} {
		callCtx := metadata.NewOutgoingContext(
			ctx, metadata.Pairs(rpctypes.TokenFieldNameGRPC, credential),
		)
		_, err = kv.Range(callCtx, &etcdserverpb.RangeRequest{Key: []byte("/a248/probe")})
		require.NoError(t, err)
	}

	callCtx := metadata.NewOutgoingContext(
		ctx, metadata.Pairs(rpctypes.TokenFieldNameGRPC, "bearer "+auth.Token),
	)
	_, err = kv.Range(callCtx, &etcdserverpb.RangeRequest{Key: []byte("/a248/probe")})
	require.Equal(t, codes.Unauthenticated, status.Code(err))
	require.Equal(t, status.Convert(rpctypes.ErrGRPCInvalidAuthToken).Message(), status.Convert(err).Message())
}
