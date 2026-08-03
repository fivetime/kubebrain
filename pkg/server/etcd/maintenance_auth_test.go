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
	server.SetStaticMembers([]*etcdserverpb.Member{{ID: 1, Name: "voter"}})
	aliceCtx := setupAuthKVUser(t, server)
	plain := context.Background()

	rootToken, err := server.tokens.authenticate(plain, "root", "root-secret")
	require.NoError(t, err)
	rootCtx := metadata.NewIncomingContext(plain, metadata.Pairs(rpctypes.TokenFieldNameGRPC, rootToken))

	_, err = server.Status(plain, &etcdserverpb.StatusRequest{})
	requireMaintenanceAuthError(t, err, rpctypes.ErrUserEmpty, codes.Unknown, "etcdserver: user name is empty")
	_, err = server.Status(aliceCtx, &etcdserverpb.StatusRequest{})
	require.NoError(t, err)

	_, err = server.Alarm(aliceCtx, &etcdserverpb.AlarmRequest{Action: etcdserverpb.AlarmRequest_GET})
	require.NoError(t, err)
	_, err = server.Alarm(aliceCtx, &etcdserverpb.AlarmRequest{Action: etcdserverpb.AlarmRequest_ACTIVATE})
	requireMaintenanceAuthError(t, err, rpctypes.ErrPermissionDenied, codes.Unknown, "etcdserver: permission denied")
	_, err = server.Alarm(plain, &etcdserverpb.AlarmRequest{
		Action: etcdserverpb.AlarmRequest_GET, Alarm: etcdserverpb.AlarmType(127),
	})
	requireMaintenanceAuthError(t, err, rpctypes.ErrUserEmpty, codes.Unknown, "etcdserver: user name is empty")
	_, err = server.Alarm(aliceCtx, &etcdserverpb.AlarmRequest{
		Action: etcdserverpb.AlarmRequest_AlarmAction(127), Alarm: etcdserverpb.AlarmType(127),
	})
	requireMaintenanceAuthError(t, err, rpctypes.ErrPermissionDenied, codes.Unknown, "etcdserver: permission denied")
	_, err = server.Alarm(rootCtx, &etcdserverpb.AlarmRequest{
		Action: etcdserverpb.AlarmRequest_AlarmAction(127), Alarm: etcdserverpb.AlarmType(127),
	})
	require.Equal(t, codes.InvalidArgument, status.Code(err))
	require.Equal(t, "etcdserver: invalid alarm action", status.Convert(err).Message())
	alarm, err := server.Alarm(rootCtx, &etcdserverpb.AlarmRequest{Action: etcdserverpb.AlarmRequest_ACTIVATE})
	require.NoError(t, err)
	require.Empty(t, alarm.Alarms)

	const (
		genericAlarmMember = uint64(0xa342501)
		genericAlarm       = etcdserverpb.AlarmType(127)
	)
	_, err = server.Alarm(aliceCtx, &etcdserverpb.AlarmRequest{
		Action: etcdserverpb.AlarmRequest_ACTIVATE, MemberID: genericAlarmMember, Alarm: genericAlarm,
	})
	requireMaintenanceAuthError(t, err, rpctypes.ErrPermissionDenied, codes.Unknown, "etcdserver: permission denied")
	generic, err := server.Alarm(rootCtx, &etcdserverpb.AlarmRequest{
		Action: etcdserverpb.AlarmRequest_GET, Alarm: genericAlarm,
	})
	require.NoError(t, err)
	require.Empty(t, generic.Alarms, "a rejected non-root mutation must not alter generic alarm metadata")

	generic, err = server.Alarm(rootCtx, &etcdserverpb.AlarmRequest{
		Action: etcdserverpb.AlarmRequest_ACTIVATE, MemberID: genericAlarmMember, Alarm: genericAlarm,
	})
	require.NoError(t, err)
	require.Equal(t, []*etcdserverpb.AlarmMember{{MemberID: genericAlarmMember, Alarm: genericAlarm}}, generic.Alarms)
	generic, err = server.Alarm(aliceCtx, &etcdserverpb.AlarmRequest{
		Action: etcdserverpb.AlarmRequest_GET, Alarm: genericAlarm,
	})
	require.NoError(t, err)
	require.Equal(t, []*etcdserverpb.AlarmMember{{MemberID: genericAlarmMember, Alarm: genericAlarm}}, generic.Alarms)
	_, err = server.Alarm(aliceCtx, &etcdserverpb.AlarmRequest{
		Action: etcdserverpb.AlarmRequest_DEACTIVATE, MemberID: genericAlarmMember, Alarm: genericAlarm,
	})
	requireMaintenanceAuthError(t, err, rpctypes.ErrPermissionDenied, codes.Unknown, "etcdserver: permission denied")
	generic, err = server.Alarm(rootCtx, &etcdserverpb.AlarmRequest{
		Action: etcdserverpb.AlarmRequest_GET, Alarm: genericAlarm,
	})
	require.NoError(t, err)
	require.Equal(t, []*etcdserverpb.AlarmMember{{MemberID: genericAlarmMember, Alarm: genericAlarm}}, generic.Alarms,
		"a rejected non-root disarm must preserve generic alarm metadata")
	generic, err = server.Alarm(rootCtx, &etcdserverpb.AlarmRequest{
		Action: etcdserverpb.AlarmRequest_DEACTIVATE, MemberID: genericAlarmMember, Alarm: genericAlarm,
	})
	require.NoError(t, err)
	require.Equal(t, []*etcdserverpb.AlarmMember{{MemberID: genericAlarmMember, Alarm: genericAlarm}}, generic.Alarms)

	_, err = server.HashKV(aliceCtx, &etcdserverpb.HashKVRequest{})
	requireMaintenanceAuthError(t, err, rpctypes.ErrPermissionDenied, codes.Unknown, "etcdserver: permission denied")
	_, err = server.HashKV(rootCtx, &etcdserverpb.HashKVRequest{})
	require.NoError(t, err)
	_, err = server.Hash(aliceCtx, &etcdserverpb.HashRequest{})
	requireMaintenanceAuthError(t, err, rpctypes.ErrPermissionDenied, codes.Unknown, "etcdserver: permission denied")

	currentRevision := int64(server.backend.GetCurrentRevision())
	_, err = server.Compact(plain, &etcdserverpb.CompactionRequest{Revision: currentRevision})
	requireMaintenanceAuthError(t, err, rpctypes.ErrUserEmpty, codes.Unknown, "etcdserver: user name is empty")
	_, err = server.Compact(aliceCtx, &etcdserverpb.CompactionRequest{Revision: currentRevision})
	requireMaintenanceAuthError(t, err, rpctypes.ErrPermissionDenied, codes.Unknown, "etcdserver: permission denied")
	_, err = server.Compact(rootCtx, &etcdserverpb.CompactionRequest{Revision: currentRevision})
	require.NoError(t, err)

	_, err = server.Defragment(aliceCtx, &etcdserverpb.DefragmentRequest{})
	requireMaintenanceAuthError(t, err, rpctypes.ErrPermissionDenied, codes.Unknown, "etcdserver: permission denied")
	_, err = server.Defragment(rootCtx, &etcdserverpb.DefragmentRequest{})
	require.NoError(t, err)

	err = server.Snapshot(&etcdserverpb.SnapshotRequest{}, &maintenanceSnapshotServer{ctx: aliceCtx})
	requireMaintenanceAuthError(t, err, rpctypes.ErrPermissionDenied, codes.Unknown, "etcdserver: permission denied")
	err = server.Snapshot(&etcdserverpb.SnapshotRequest{}, &maintenanceSnapshotServer{ctx: rootCtx})
	requireMaintenancePlatformReplacementError(t, err, snapshotUnsupportedMessage)

	_, err = server.MoveLeader(aliceCtx, &etcdserverpb.MoveLeaderRequest{TargetID: 1})
	requireMaintenanceAuthError(t, err, rpctypes.ErrPermissionDenied, codes.Unknown, "etcdserver: permission denied")
	_, err = server.MoveLeader(rootCtx, &etcdserverpb.MoveLeaderRequest{TargetID: 1})
	requireMaintenancePlatformReplacementError(t, err, moveLeaderUnsupportedMessage)
	validDowngrade := &etcdserverpb.DowngradeRequest{
		Action:  etcdserverpb.DowngradeRequest_VALIDATE,
		Version: "3.6.0",
	}
	_, err = server.Downgrade(aliceCtx, validDowngrade)
	requireMaintenanceAuthError(t, err, rpctypes.ErrPermissionDenied, codes.Unknown, "etcdserver: permission denied")
	downgradeResponse, err := server.Downgrade(rootCtx, validDowngrade)
	require.NoError(t, err)
	require.Equal(t, ClusterVersion, downgradeResponse.GetVersion())
	require.NotNil(t, downgradeResponse.GetHeader())
}

func TestMaintenanceRootAuthorizationClientCertificateErrorsMatchEtcd(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()
	setupAuthKVUser(t, server)
	server.SetClientCertAuth(true)
	ctx := context.Background()

	_, err := server.Defragment(verifiedTLSContext(ctx, ""), &etcdserverpb.DefragmentRequest{})
	requireMaintenanceAuthError(t, err, rpctypes.ErrUserEmpty, codes.Unknown, "etcdserver: user name is empty")
	_, err = server.Defragment(verifiedTLSContext(ctx, "external-cn"), &etcdserverpb.DefragmentRequest{})
	requireMaintenanceAuthError(t, err, rpctypes.ErrUserNotFound, codes.Unknown, "etcdserver: user name not found")
	_, err = server.Defragment(verifiedTLSContext(ctx, "alice"), &etcdserverpb.DefragmentRequest{})
	requireMaintenanceAuthError(t, err, rpctypes.ErrPermissionDenied, codes.Unknown, "etcdserver: permission denied")
	_, err = server.Defragment(verifiedTLSContext(ctx, "root"), &etcdserverpb.DefragmentRequest{})
	require.NoError(t, err)
}

func requireMaintenanceAuthError(t *testing.T, err error, want error, code codes.Code, message string) {
	t.Helper()
	require.EqualError(t, err, message)
	require.ErrorIs(t, err, want)
	require.Equal(t, code, status.Code(err))
	require.Equal(t, message, status.Convert(err).Message())
}

func requireMaintenancePlatformReplacementError(t *testing.T, err error, message string) {
	t.Helper()
	require.EqualError(t, err, status.Error(codes.Unimplemented, message).Error())
	require.Equal(t, codes.Unimplemented, status.Code(err))
	require.Equal(t, message, status.Convert(err).Message())
}
