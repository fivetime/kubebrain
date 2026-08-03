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
	"google.golang.org/grpc/credentials/insecure"
)

func TestAlarmUnknownTypePersistsAcrossKubeBrainRestart(t *testing.T) {
	restartCommand := os.Getenv("KUBEBRAIN_GENERIC_ALARM_RESTART_COMMAND")
	if restartCommand == "" {
		t.Skip("set KUBEBRAIN_GENERIC_ALARM_RESTART_COMMAND to restart the KubeBrain data plane")
	}
	conn, err := grpc.NewClient(grpcTarget(compatEndpoint(t)), grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, conn.Close()) })
	maintenance := etcdserverpb.NewMaintenanceClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	memberID := uint64(time.Now().UnixNano())
	const alarm = etcdserverpb.AlarmType(126)
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cleanupCancel()
		_, _ = maintenance.Alarm(cleanupCtx, &etcdserverpb.AlarmRequest{
			Action: etcdserverpb.AlarmRequest_DEACTIVATE, MemberID: memberID, Alarm: alarm,
		})
	})
	activated, err := maintenance.Alarm(ctx, &etcdserverpb.AlarmRequest{
		Action: etcdserverpb.AlarmRequest_ACTIVATE, MemberID: memberID, Alarm: alarm,
	})
	require.NoError(t, err)
	require.Equal(t, []*etcdserverpb.AlarmMember{{MemberID: memberID, Alarm: alarm}}, activated.Alarms)

	output, err := runCompatShellCommandContext(t, ctx, restartCommand)
	require.NoErrorf(t, err, "restart command: %s", strings.TrimSpace(string(output)))
	var recovered *etcdserverpb.AlarmResponse
	require.Eventually(t, func() bool {
		callCtx, callCancel := context.WithTimeout(ctx, 3*time.Second)
		defer callCancel()
		recovered, err = maintenance.Alarm(callCtx, &etcdserverpb.AlarmRequest{
			Action: etcdserverpb.AlarmRequest_GET, Alarm: alarm,
		})
		return err == nil && len(recovered.Alarms) == 1
	}, 60*time.Second, 500*time.Millisecond)
	require.Equal(t, []*etcdserverpb.AlarmMember{{MemberID: memberID, Alarm: alarm}}, recovered.Alarms)

	disarmed, err := maintenance.Alarm(ctx, &etcdserverpb.AlarmRequest{
		Action: etcdserverpb.AlarmRequest_DEACTIVATE, MemberID: memberID, Alarm: alarm,
	})
	require.NoError(t, err)
	require.Equal(t, []*etcdserverpb.AlarmMember{{MemberID: memberID, Alarm: alarm}}, disarmed.Alarms)
	final, err := maintenance.Alarm(ctx, &etcdserverpb.AlarmRequest{
		Action: etcdserverpb.AlarmRequest_GET, Alarm: alarm,
	})
	require.NoError(t, err)
	require.Empty(t, final.Alarms)
}
