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
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// TestLeaseKeepAliveRequireLeaderAcrossBackendFailover mirrors upstream's
// TestLeaseWithRequireLeader against an external PD quorum loss. The high-level
// lessor must close only the require-leader channel and reconnect the ordinary
// channel; an independent raw stream pins the wire error to Unavailable /
// ErrNoLeader while its server handler is idle in RecvMsg.
func TestLeaseKeepAliveRequireLeaderAcrossBackendFailover(t *testing.T) {
	failoverCommand := os.Getenv("KUBEBRAIN_LEASE_BACKEND_FAILOVER_COMMAND")
	if failoverCommand == "" {
		t.Skip("set KUBEBRAIN_LEASE_BACKEND_FAILOVER_COMMAND to run destructive backend failover")
	}
	endpoint := os.Getenv("KUBEBRAIN_ETCD_ENDPOINT")
	if endpoint == "" {
		t.Fatal("set KUBEBRAIN_ETCD_ENDPOINT explicitly for lease backend failover")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	cli, err := clientv3.New(clientv3.Config{Endpoints: []string{endpoint}, DialTimeout: 5 * time.Second})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, cli.Close()) })

	requireLease, err := cli.Grant(ctx, 600)
	require.NoError(t, err)
	ordinaryLease, err := cli.Grant(ctx, 600)
	require.NoError(t, err)
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cleanupCancel()
		_, _ = cli.Revoke(cleanupCtx, requireLease.ID)
		_, _ = cli.Revoke(cleanupCtx, ordinaryLease.ID)
	})

	requireKeepAlive, err := cli.KeepAlive(clientv3.WithRequireLeader(ctx), requireLease.ID)
	require.NoError(t, err)
	ordinaryKeepAlive, err := cli.KeepAlive(ctx, ordinaryLease.ID)
	require.NoError(t, err)
	firstRequire := receiveKeepAlive(t, ctx, requireKeepAlive, "require-leader")
	require.Equal(t, requireLease.ID, firstRequire.ID)
	firstOrdinary := receiveKeepAlive(t, ctx, ordinaryKeepAlive, "ordinary")
	require.Equal(t, ordinaryLease.ID, firstOrdinary.ID)

	rawLease := etcdserverpb.NewLeaseClient(cli.ActiveConnection())
	rawRequire, err := rawLease.LeaseKeepAlive(clientv3.WithRequireLeader(ctx))
	require.NoError(t, err)
	require.NoError(t, rawRequire.Send(&etcdserverpb.LeaseKeepAliveRequest{ID: int64(requireLease.ID)}))
	rawResponse, err := rawRequire.Recv()
	require.NoError(t, err)
	require.Equal(t, int64(requireLease.ID), rawResponse.ID)
	require.Positive(t, rawResponse.TTL)
	rawRequireErr := make(chan error, 1)
	go func() {
		_, recvErr := rawRequire.Recv()
		rawRequireErr <- recvErr
	}()

	output, err := runCompatShellCommandContext(t, ctx, failoverCommand)
	require.NoErrorf(t, err, "backend failover command: %s", strings.TrimSpace(string(output)))
	t.Logf("backend failover command: %s", strings.TrimSpace(string(output)))

	select {
	case requireErr := <-rawRequireErr:
		require.Equal(t, codes.Unavailable, status.Code(requireErr))
		require.Equal(t, rpctypes.ErrNoLeader.Error(), status.Convert(requireErr).Message())
	case <-time.After(15 * time.Second):
		t.Fatal("idle raw require-leader LeaseKeepAlive did not return ErrNoLeader")
	}

	for {
		select {
		case response, ok := <-requireKeepAlive:
			if !ok {
				goto requireClosed
			}
			require.NotNil(t, response)
		case <-time.After(15 * time.Second):
			t.Fatal("high-level require-leader KeepAlive channel did not close")
		}
	}

requireClosed:
	select {
	case response, ok := <-ordinaryKeepAlive:
		require.True(t, ok, "ordinary KeepAlive channel closed after require-leader stream failure")
		if response != nil {
			require.Positive(t, response.TTL)
		}
	case <-time.After(500 * time.Millisecond):
	}

	var recovered *clientv3.LeaseKeepAliveResponse
	require.Eventually(t, func() bool {
		callCtx, callCancel := context.WithTimeout(ctx, 3*time.Second)
		defer callCancel()
		var keepAliveErr error
		recovered, keepAliveErr = cli.KeepAliveOnce(callCtx, ordinaryLease.ID)
		return keepAliveErr == nil && recovered != nil && recovered.TTL > 0
	}, 45*time.Second, 100*time.Millisecond, "ordinary KeepAliveOnce must recover after backend quorum loss")
	require.Equal(t, ordinaryLease.ID, recovered.ID)
}

func receiveKeepAlive(
	t *testing.T,
	ctx context.Context,
	responses <-chan *clientv3.LeaseKeepAliveResponse,
	label string,
) *clientv3.LeaseKeepAliveResponse {
	t.Helper()
	select {
	case response, ok := <-responses:
		require.Truef(t, ok, "%s KeepAlive channel closed before first response", label)
		require.NotNil(t, response)
		require.Positive(t, response.TTL)
		return response
	case <-ctx.Done():
		require.NoErrorf(t, ctx.Err(), "%s KeepAlive did not return first response", label)
		return nil
	}
}
