package main

import (
	"context"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/server/v3/embed"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

func verifyRestoredAuthDelayedApply(t *testing.T, ctx context.Context, adminConfig clientv3.Config,
	servers []*embed.Etcd, connections []*grpc.ClientConn, indexes []*restoredAuthRaftIndexCore, fixture *snapshotAuthFixture,
) {
	t.Helper()
	admin, err := clientv3.New(adminConfig)
	require.NoError(t, err)
	defer func() { require.NoError(t, admin.Close()) }()
	lease, err := admin.Grant(ctx, 30)
	require.NoError(t, err)
	defer func() { _, err := admin.Revoke(ctx, lease.ID); require.NoError(t, err) }()
	key := fixture.expected.access[3].key
	put, err := admin.Put(ctx, key, "delayed-auth", clientv3.WithLease(lease.ID))
	require.NoError(t, err)
	require.NoError(t, waitForRestoredAuthLeaseReplication(ctx, adminConfig, key, "delayed-auth", lease.ID, put.Header.Revision))
	source, target := -1, -1
	leader := servers[0].Server.Leader()
	for i, server := range servers {
		if server.Server.MemberID() == leader {
			source = i
		} else {
			target = i
		}
	}
	require.GreaterOrEqual(t, source, 0)
	require.GreaterOrEqual(t, target, 0)
	opCtx, cancel := context.WithTimeout(ctx, 8*time.Second)
	defer cancel()
	gate := &restoredAuthApplyGate{ctx: opCtx, reached: make(chan struct{}), release: make(chan struct{})}
	defer gate.open() // Release before client/member cleanup even on assertion failure.
	indexes[target].mu.Lock()
	indexes[target].gate = gate
	indexes[target].mu.Unlock()
	user := fixture.expected.users[2]
	authenticated, err := etcdserverpb.NewAuthClient(connections[source]).Authenticate(opCtx,
		&etcdserverpb.AuthenticateRequest{Name: user.name, Password: user.password})
	require.NoError(t, err)
	select {
	case <-gate.reached:
	case <-opCtx.Done():
		t.Fatal("follower did not reach controlled Authenticate application")
	}
	separator := strings.LastIndexByte(authenticated.Token, '.')
	require.Greater(t, separator, 0)
	tokenIndex, err := strconv.ParseUint(authenticated.Token[separator+1:], 10, 64)
	require.True(t, err == nil, "invalid numeric token index")
	entryIndex := indexes[target].authIndex()
	require.Less(t, tokenIndex, entryIndex)
	require.GreaterOrEqual(t, servers[target].Server.AppliedIndex(), tokenIndex)
	tokenCtx := metadata.AppendToOutgoingContext(opCtx, "token", authenticated.Token)
	watchOnce := func() *etcdserverpb.WatchResponse {
		watchCtx, stop := context.WithCancel(tokenCtx)
		defer stop()
		stream, err := etcdserverpb.NewWatchClient(connections[target]).Watch(watchCtx)
		require.NoError(t, err)
		require.NoError(t, stream.Send(&etcdserverpb.WatchRequest{RequestUnion: &etcdserverpb.WatchRequest_CreateRequest{
			CreateRequest: &etcdserverpb.WatchCreateRequest{Key: []byte(key)},
		}}))
		response, err := stream.Recv()
		require.NoError(t, err)
		return response
	}
	failed := watchOnce()
	require.True(t, failed.Canceled)
	require.Contains(t, failed.CancelReason, "invalid auth token")
	keepAlive := func() (*etcdserverpb.LeaseKeepAliveResponse, error) {
		leaseCtx, stop := context.WithCancel(tokenCtx)
		defer stop()
		stream, err := etcdserverpb.NewLeaseClient(connections[target]).LeaseKeepAlive(leaseCtx)
		if err != nil {
			return nil, err
		}
		if err := stream.Send(&etcdserverpb.LeaseKeepAliveRequest{ID: int64(lease.ID)}); err != nil {
			return nil, err
		}
		return stream.Recv()
	}
	_, err = keepAlive()
	require.ErrorIs(t, rpctypes.Error(err), rpctypes.ErrInvalidAuthToken)
	require.NoError(t, opCtx.Err(), "failure must precede pause timeout")
	gate.open()
	require.Eventually(t, func() bool { return servers[target].Server.AppliedIndex() >= entryIndex }, 3*time.Second, time.Millisecond)
	created := watchOnce()
	require.True(t, created.Created && !created.Canceled)
	alive, err := keepAlive()
	require.NoError(t, err)
	require.Equal(t, int64(lease.ID), alive.ID)
	require.Positive(t, alive.TTL)
	t.Logf("same token rejected during follower apply pause, accepted after release: token_index=%d entry_index=%d", tokenIndex, entryIndex)
}
