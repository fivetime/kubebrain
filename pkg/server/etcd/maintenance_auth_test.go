package etcd

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

type maintenanceSnapshotServer struct {
	etcdserverpb.Maintenance_SnapshotServer
	ctx context.Context
}

func (s *maintenanceSnapshotServer) Context() context.Context { return s.ctx }

func TestMaintenanceAuthorizationMatchesEtcd(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()
	aliceCtx := setupAuthKVUser(t, server)
	plain := context.Background()

	rootToken, err := server.tokens.authenticate(plain, "root", "root-secret")
	require.NoError(t, err)
	rootCtx := metadata.NewIncomingContext(plain, metadata.Pairs(rpctypes.TokenFieldNameGRPC, rootToken))

	_, err = server.Status(plain, &etcdserverpb.StatusRequest{})
	require.ErrorIs(t, err, rpctypes.ErrUserEmpty)
	_, err = server.Status(aliceCtx, &etcdserverpb.StatusRequest{})
	require.NoError(t, err)

	_, err = server.Alarm(aliceCtx, &etcdserverpb.AlarmRequest{Action: etcdserverpb.AlarmRequest_GET})
	require.NoError(t, err)
	_, err = server.Alarm(aliceCtx, &etcdserverpb.AlarmRequest{Action: etcdserverpb.AlarmRequest_ACTIVATE})
	require.ErrorIs(t, err, rpctypes.ErrPermissionDenied)

	_, err = server.HashKV(aliceCtx, &etcdserverpb.HashKVRequest{})
	require.ErrorIs(t, err, rpctypes.ErrPermissionDenied)
	_, err = server.HashKV(rootCtx, &etcdserverpb.HashKVRequest{})
	require.NoError(t, err)
	_, err = server.Hash(aliceCtx, &etcdserverpb.HashRequest{})
	require.ErrorIs(t, err, rpctypes.ErrPermissionDenied)

	_, err = server.Defragment(aliceCtx, &etcdserverpb.DefragmentRequest{})
	require.ErrorIs(t, err, rpctypes.ErrPermissionDenied)
	_, err = server.Defragment(rootCtx, &etcdserverpb.DefragmentRequest{})
	require.NoError(t, err)

	err = server.Snapshot(&etcdserverpb.SnapshotRequest{}, &maintenanceSnapshotServer{ctx: aliceCtx})
	require.ErrorIs(t, err, rpctypes.ErrPermissionDenied)
	err = server.Snapshot(&etcdserverpb.SnapshotRequest{}, &maintenanceSnapshotServer{ctx: rootCtx})
	require.Equal(t, codes.Unimplemented, status.Code(err))

	_, err = server.MoveLeader(aliceCtx, &etcdserverpb.MoveLeaderRequest{})
	require.ErrorIs(t, err, rpctypes.ErrPermissionDenied)
	_, err = server.MoveLeader(rootCtx, &etcdserverpb.MoveLeaderRequest{})
	require.Equal(t, codes.Unimplemented, status.Code(err))
	_, err = server.Downgrade(aliceCtx, &etcdserverpb.DowngradeRequest{})
	require.ErrorIs(t, err, rpctypes.ErrPermissionDenied)
	_, err = server.Downgrade(rootCtx, &etcdserverpb.DowngradeRequest{})
	require.Equal(t, codes.Unimplemented, status.Code(err))
}
