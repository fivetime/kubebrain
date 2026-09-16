package etcd

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

type maintenanceSnapshotServer struct {
	etcdserverpb.Maintenance_SnapshotServer
	ctx       context.Context
	responses []*etcdserverpb.SnapshotResponse
}

func (s *maintenanceSnapshotServer) Context() context.Context { return s.ctx }
func (s *maintenanceSnapshotServer) Send(response *etcdserverpb.SnapshotResponse) error {
	s.responses = append(s.responses, response)
	return nil
}

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

	require.NoError(t, server.snapshotAdmission.semaphore().Acquire(context.Background(), 1))
	err = server.Snapshot(&etcdserverpb.SnapshotRequest{}, &maintenanceSnapshotServer{ctx: plain})
	requireMaintenanceAuthError(t, err, rpctypes.ErrUserEmpty, codes.Unknown, "etcdserver: user name is empty")
	err = server.Snapshot(&etcdserverpb.SnapshotRequest{}, &maintenanceSnapshotServer{ctx: aliceCtx})
	requireMaintenanceAuthError(t, err, rpctypes.ErrPermissionDenied, codes.Unknown, "etcdserver: permission denied")
	waitCtx, cancelWait := context.WithTimeout(rootCtx, 50*time.Millisecond)
	err = server.Snapshot(&etcdserverpb.SnapshotRequest{}, &maintenanceSnapshotServer{ctx: waitCtx})
	cancelWait()
	require.Equal(t, codes.DeadlineExceeded, status.Code(err))
	server.snapshotAdmission.release()

	err = server.Snapshot(&etcdserverpb.SnapshotRequest{}, &maintenanceSnapshotServer{ctx: aliceCtx})
	requireMaintenanceAuthError(t, err, rpctypes.ErrPermissionDenied, codes.Unknown, "etcdserver: permission denied")
	rootSnapshot := &maintenanceSnapshotServer{ctx: rootCtx}
	err = server.Snapshot(&etcdserverpb.SnapshotRequest{}, rootSnapshot)
	require.NoError(t, err)
	require.NotEmpty(t, rootSnapshot.responses)

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

func TestMoveLeaderFollowerUsesAppliedAuthStateWithoutStorage(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()
	aliceCtx := setupAuthKVUser(t, server)
	rootToken, err := server.tokens.authenticate(context.Background(), "root", "root-secret")
	require.NoError(t, err)
	rootCtx := metadata.NewIncomingContext(context.Background(), metadata.Pairs(
		rpctypes.TokenFieldNameGRPC, rootToken,
	))

	server.peers = testPeerService{leaderInfo: "leader.test:3380", proxyEnabled: true}
	server.SetStaticMembers([]*etcdserverpb.Member{
		{ID: 7, Name: "local", PeerURLs: []string{"http://" + server.backend.GetResourceLock().Identity()}},
		{ID: 8, Name: "leader", PeerURLs: []string{"http://leader.test:3380"}},
	})
	shim := &blockingAuthConfigReadShim{
		BackendShim:  server.backend,
		blockAt:      1,
		entered:      make(chan struct{}),
		release:      make(chan struct{}),
		putCommitted: make(chan struct{}),
	}
	server.backend = shim
	server.tokens.snapshots.repo.backend = shim
	t.Cleanup(func() { close(shim.release) })

	response, err := server.MoveLeader(rootCtx, &etcdserverpb.MoveLeaderRequest{TargetID: 8})
	require.Nil(t, response)
	require.ErrorIs(t, err, rpctypes.ErrGRPCNotLeader)
	require.Equal(t, codes.FailedPrecondition, status.Code(err))

	response, err = server.MoveLeader(aliceCtx, &etcdserverpb.MoveLeaderRequest{TargetID: 8})
	require.Nil(t, response)
	requireMaintenanceAuthError(t, err, rpctypes.ErrPermissionDenied, codes.Unknown, "etcdserver: permission denied")
	require.Equal(t, int32(0), shim.reads.Load(), "follower MoveLeader must not read auth state from storage")
	select {
	case <-shim.entered:
		t.Fatal("follower MoveLeader reached the storage-backed auth path")
	default:
	}
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
