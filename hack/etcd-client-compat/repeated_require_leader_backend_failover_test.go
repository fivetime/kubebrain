package compat

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

// TestRequireLeaderStreamsAcrossRepeatedBackendFailover recreates both public
// require-leader stream families before every independent backend quorum loss.
// This catches monitor state or transport reuse that works once but fails to
// re-arm after the backend recovers.
func TestRequireLeaderStreamsAcrossRepeatedBackendFailover(t *testing.T) {
	command := os.Getenv("KUBEBRAIN_REPEATED_REQUIRE_LEADER_COMMAND")
	if command == "" {
		t.Skip("set KUBEBRAIN_REPEATED_REQUIRE_LEADER_COMMAND to run destructive backend failover")
	}
	endpoint := os.Getenv("KUBEBRAIN_ETCD_ENDPOINT")
	if endpoint == "" {
		t.Fatal("set KUBEBRAIN_ETCD_ENDPOINT explicitly for repeated require-leader failover")
	}
	cycles, err := strconv.Atoi(os.Getenv("KUBEBRAIN_REPEATED_REQUIRE_LEADER_CYCLES"))
	require.NoError(t, err, "KUBEBRAIN_REPEATED_REQUIRE_LEADER_CYCLES must be an integer")
	require.GreaterOrEqual(t, cycles, 1)
	require.LessOrEqual(t, cycles, 20)

	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(cycles)*2*time.Minute)
	defer cancel()
	cli, err := clientv3.New(clientv3.Config{Endpoints: []string{endpoint}, DialTimeout: 5 * time.Second})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, cli.Close()) })
	prefix := testPrefix(t) + "/"

	for cycle := 1; cycle <= cycles; cycle++ {
		lease, grantErr := cli.Grant(ctx, 600)
		require.NoError(t, grantErr, "cycle %d grant", cycle)

		requireCtx := metadata.NewOutgoingContext(ctx, metadata.Pairs(
			rpctypes.MetadataRequireLeaderKey, rpctypes.MetadataHasLeader,
		))
		watchStream, watchErr := etcdserverpb.NewWatchClient(cli.ActiveConnection()).Watch(requireCtx)
		require.NoError(t, watchErr, "cycle %d open Watch", cycle)
		watchErr = watchStream.Send(&etcdserverpb.WatchRequest{
			RequestUnion: &etcdserverpb.WatchRequest_CreateRequest{
				CreateRequest: &etcdserverpb.WatchCreateRequest{Key: []byte(prefix + fmt.Sprintf("cycle-%d", cycle))},
			},
		})
		require.NoError(t, watchErr, "cycle %d create Watch", cycle)
		created, watchErr := watchStream.Recv()
		require.NoError(t, watchErr, "cycle %d receive Watch creation", cycle)
		require.True(t, created.Created, "cycle %d Watch creation response", cycle)

		leaseStream, leaseErr := etcdserverpb.NewLeaseClient(cli.ActiveConnection()).LeaseKeepAlive(requireCtx)
		require.NoError(t, leaseErr, "cycle %d open LeaseKeepAlive", cycle)
		require.NoError(t, leaseStream.Send(&etcdserverpb.LeaseKeepAliveRequest{ID: int64(lease.ID)}),
			"cycle %d send LeaseKeepAlive", cycle)
		firstLease, leaseErr := leaseStream.Recv()
		require.NoError(t, leaseErr, "cycle %d receive LeaseKeepAlive", cycle)
		require.Equal(t, int64(lease.ID), firstLease.ID)
		require.Positive(t, firstLease.TTL)

		watchDone := make(chan error, 1)
		leaseDone := make(chan error, 1)
		go func() {
			_, recvErr := watchStream.Recv()
			watchDone <- recvErr
		}()
		go func() {
			_, recvErr := leaseStream.Recv()
			leaseDone <- recvErr
		}()

		output, commandErr := runCompatShellCommandContext(t, ctx, command)
		require.NoErrorf(t, commandErr, "cycle %d backend failover command: %s",
			cycle, strings.TrimSpace(string(output)))
		t.Logf("cycle %d backend failover command: %s", cycle, strings.TrimSpace(string(output)))
		requireRepeatedNoLeader(t, cycle, "Watch", watchDone)
		requireRepeatedNoLeader(t, cycle, "LeaseKeepAlive", leaseDone)

		var recovered *clientv3.LeaseKeepAliveResponse
		require.Eventually(t, func() bool {
			callCtx, callCancel := context.WithTimeout(ctx, 3*time.Second)
			defer callCancel()
			var keepAliveErr error
			recovered, keepAliveErr = cli.KeepAliveOnce(callCtx, lease.ID)
			return keepAliveErr == nil && recovered != nil && recovered.TTL > 0
		}, 45*time.Second, 100*time.Millisecond, "cycle %d ordinary lease must recover", cycle)
		key := prefix + fmt.Sprintf("recovered-%d", cycle)
		value := fmt.Sprintf("value-%d", cycle)
		require.Eventually(t, func() bool {
			callCtx, callCancel := context.WithTimeout(ctx, 3*time.Second)
			defer callCancel()
			_, putErr := cli.Put(callCtx, key, value)
			return putErr == nil
		}, 45*time.Second, 100*time.Millisecond,
			"cycle %d Put must recover after ordinary lease path", cycle)
		var got *clientv3.GetResponse
		require.Eventually(t, func() bool {
			callCtx, callCancel := context.WithTimeout(ctx, 3*time.Second)
			defer callCancel()
			var getErr error
			got, getErr = cli.Get(callCtx, key)
			return getErr == nil && len(got.Kvs) == 1
		}, 45*time.Second, 100*time.Millisecond,
			"cycle %d Range must recover after Put", cycle)
		require.Len(t, got.Kvs, 1)
		require.Equal(t, value, string(got.Kvs[0].Value))
		_, _ = cli.Revoke(ctx, lease.ID)
	}

	_, err = cli.Delete(ctx, prefix, clientv3.WithPrefix())
	require.NoError(t, err)
}

func requireRepeatedNoLeader(t *testing.T, cycle int, stream string, done <-chan error) {
	t.Helper()
	select {
	case err := <-done:
		require.Equal(t, codes.Unavailable, status.Code(err), "cycle %d %s code", cycle, stream)
		require.Equal(t, rpctypes.ErrNoLeader.Error(), status.Convert(err).Message(),
			"cycle %d %s message", cycle, stream)
	case <-time.After(15 * time.Second):
		t.Fatalf("cycle %d %s did not return ErrNoLeader", cycle, stream)
	}
}
