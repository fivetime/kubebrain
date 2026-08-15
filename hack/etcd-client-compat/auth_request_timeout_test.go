package compat

import (
	"context"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

// TestAuthStatusServerAttemptTimeout is an opt-in black-box fault probe. The
// caller keeps every TiKV endpoint unavailable for longer than the server's
// unary attempt budget. A raw Auth client deliberately bypasses clientv3's
// retry interceptor so the observed deadline belongs to exactly one server
// attempt.
func TestAuthStatusServerAttemptTimeout(t *testing.T) {
	endpoint := os.Getenv("KUBEBRAIN_AUTH_TIMEOUT_PROBE_ENDPOINT")
	if endpoint == "" {
		t.Skip("set KUBEBRAIN_AUTH_TIMEOUT_PROBE_ENDPOINT during a TiKV outage longer than 10 seconds")
	}
	endpoint = strings.TrimPrefix(strings.TrimPrefix(endpoint, "http://"), "https://")
	conn, err := grpc.NewClient(endpoint, grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, conn.Close()) })

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	started := time.Now()
	_, err = etcdserverpb.NewAuthClient(conn).AuthStatus(ctx, &etcdserverpb.AuthStatusRequest{})
	elapsed := time.Since(started)

	require.Equal(t, codes.DeadlineExceeded, status.Code(err), "response after %s: %v", elapsed, err)
	require.GreaterOrEqual(t, elapsed, 8*time.Second)
	require.Less(t, elapsed, 13*time.Second)
}
