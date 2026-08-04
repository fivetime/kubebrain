package compat

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

func TestReferenceEtcdUnknownAlarmSurvivesRestart(t *testing.T) {
	binary := os.Getenv("REFERENCE_ETCD_BINARY")
	if binary == "" {
		t.Skip("set REFERENCE_ETCD_BINARY to run the alarm restart oracle")
	}
	start := newReferenceAlarmRestartServer(t, binary, "unknown-alarm-restart-oracle")
	stop, conn := start()
	maintenance := etcdserverpb.NewMaintenanceClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	const (
		memberID = uint64(0xa352601)
		alarm    = etcdserverpb.AlarmType(127)
	)
	activated, err := maintenance.Alarm(ctx, &etcdserverpb.AlarmRequest{
		Action: etcdserverpb.AlarmRequest_ACTIVATE, MemberID: memberID, Alarm: alarm,
	})
	require.NoError(t, err)
	require.Equal(t, []*etcdserverpb.AlarmMember{{MemberID: memberID, Alarm: alarm}}, activated.Alarms)
	require.NoError(t, conn.Close())
	stop()

	_, restartedConn := start()
	defer restartedConn.Close()
	restartedMaintenance := etcdserverpb.NewMaintenanceClient(restartedConn)
	recovered, err := restartedMaintenance.Alarm(ctx, &etcdserverpb.AlarmRequest{
		Action: etcdserverpb.AlarmRequest_GET, Alarm: alarm,
	})
	require.NoError(t, err)
	require.Equal(t, []*etcdserverpb.AlarmMember{{MemberID: memberID, Alarm: alarm}}, recovered.Alarms)
	disarmed, err := restartedMaintenance.Alarm(ctx, &etcdserverpb.AlarmRequest{
		Action: etcdserverpb.AlarmRequest_DEACTIVATE, MemberID: memberID, Alarm: alarm,
	})
	require.NoError(t, err)
	require.Equal(t, recovered.Alarms, disarmed.Alarms)
}

func TestReferenceEtcdCorruptAlarmSurvivesRestart(t *testing.T) {
	binary := os.Getenv("REFERENCE_ETCD_BINARY")
	if binary == "" {
		t.Skip("set REFERENCE_ETCD_BINARY to run the alarm restart oracle")
	}
	start := newReferenceAlarmRestartServer(t, binary, "corrupt-alarm-restart-oracle")
	stop, conn := start()
	maintenance := etcdserverpb.NewMaintenanceClient(conn)
	kv := etcdserverpb.NewKVClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	key := []byte("/a3526/reference-corrupt-restart")
	_, err := kv.Put(ctx, &etcdserverpb.PutRequest{Key: key, Value: []byte("before")})
	require.NoError(t, err)
	statusResponse, err := maintenance.Status(ctx, &etcdserverpb.StatusRequest{})
	require.NoError(t, err)
	memberID := statusResponse.GetHeader().GetMemberId()
	require.NotZero(t, memberID)
	activated, err := maintenance.Alarm(ctx, &etcdserverpb.AlarmRequest{
		Action:   etcdserverpb.AlarmRequest_ACTIVATE,
		MemberID: memberID,
		Alarm:    etcdserverpb.AlarmType_CORRUPT,
	})
	require.NoError(t, err)
	require.Equal(t, []*etcdserverpb.AlarmMember{{
		MemberID: memberID, Alarm: etcdserverpb.AlarmType_CORRUPT,
	}}, activated.Alarms)
	_, err = kv.Put(ctx, &etcdserverpb.PutRequest{Key: key, Value: []byte("blocked")})
	require.Equal(t, codes.DataLoss, status.Code(err))
	require.NoError(t, conn.Close())
	stop()

	_, restartedConn := start()
	defer restartedConn.Close()
	restartedMaintenance := etcdserverpb.NewMaintenanceClient(restartedConn)
	restartedKV := etcdserverpb.NewKVClient(restartedConn)
	recovered, err := restartedMaintenance.Alarm(ctx, &etcdserverpb.AlarmRequest{
		Action: etcdserverpb.AlarmRequest_GET, Alarm: etcdserverpb.AlarmType_CORRUPT,
	})
	require.NoError(t, err)
	require.Equal(t, activated.Alarms, recovered.Alarms)
	read, err := restartedKV.Range(ctx, &etcdserverpb.RangeRequest{Key: key})
	require.NoError(t, err)
	require.Len(t, read.Kvs, 1)
	require.Equal(t, []byte("before"), read.Kvs[0].Value)
	_, err = restartedKV.Put(ctx, &etcdserverpb.PutRequest{Key: key, Value: []byte("still-blocked")})
	require.Equal(t, codes.DataLoss, status.Code(err))
	disarmed, err := restartedMaintenance.Alarm(ctx, &etcdserverpb.AlarmRequest{
		Action:   etcdserverpb.AlarmRequest_DEACTIVATE,
		MemberID: memberID,
		Alarm:    etcdserverpb.AlarmType_CORRUPT,
	})
	require.NoError(t, err)
	require.Equal(t, recovered.Alarms, disarmed.Alarms)
	_, err = restartedKV.Put(ctx, &etcdserverpb.PutRequest{Key: key, Value: []byte("restored")})
	require.NoError(t, err)
}

func newReferenceAlarmRestartServer(t *testing.T, binary, name string) func() (func(), *grpc.ClientConn) {
	t.Helper()
	dataDir := t.TempDir()
	args := []string{
		"--name", name,
		"--data-dir", dataDir,
		"--listen-client-urls", "http://127.0.0.1:42379",
		"--advertise-client-urls", "http://127.0.0.1:42379",
		"--listen-peer-urls", "http://127.0.0.1:42380",
		"--initial-advertise-peer-urls", "http://127.0.0.1:42380",
		"--initial-cluster", name + "=http://127.0.0.1:42380",
	}
	return func() (func(), *grpc.ClientConn) {
		t.Helper()
		stop := startCompatCommand(t, binary, args...)
		conn, err := grpc.NewClient("127.0.0.1:42379", grpc.WithTransportCredentials(insecure.NewCredentials()))
		require.NoError(t, err)
		kv := etcdserverpb.NewKVClient(conn)
		require.Eventually(t, func() bool {
			ctx, cancel := context.WithTimeout(context.Background(), 250*time.Millisecond)
			defer cancel()
			_, rangeErr := kv.Range(ctx, &etcdserverpb.RangeRequest{Key: []byte("/a3526/health")})
			return rangeErr == nil
		}, 10*time.Second, 50*time.Millisecond)
		return stop, conn
	}
}
