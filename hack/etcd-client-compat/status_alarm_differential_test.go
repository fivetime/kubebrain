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

type statusAlarmOutcome struct {
	ActiveErrors   []string
	DisarmedErrors []string
}

func TestStatusAlarmDifferentialAgainstReferenceEtcd(t *testing.T) {
	reference := os.Getenv("REFERENCE_QUOTA_ETCD_ENDPOINT")
	kubebrain := os.Getenv("KUBEBRAIN_QUOTA_ENDPOINT")
	if reference == "" || kubebrain == "" {
		t.Skip("set REFERENCE_QUOTA_ETCD_ENDPOINT and KUBEBRAIN_QUOTA_ENDPOINT")
	}

	require.Equal(t,
		runStatusAlarmScenario(t, reference),
		runStatusAlarmScenario(t, kubebrain),
	)
}

func TestStatusAlarmCrossEndpointVisibility(t *testing.T) {
	rawEndpoints := os.Getenv("KUBEBRAIN_MULTI_QUOTA_ENDPOINTS")
	if rawEndpoints == "" {
		t.Skip("set KUBEBRAIN_MULTI_QUOTA_ENDPOINTS to three comma-separated replica endpoints")
	}
	endpoints := strings.Split(rawEndpoints, ",")
	require.Len(t, endpoints, 3)

	connections := make([]*grpc.ClientConn, 0, len(endpoints))
	clients := make([]etcdserverpb.MaintenanceClient, 0, len(endpoints))
	for _, endpoint := range endpoints {
		endpoint = strings.TrimSpace(strings.TrimPrefix(strings.TrimPrefix(endpoint, "http://"), "https://"))
		connection, err := grpc.NewClient(endpoint, grpc.WithTransportCredentials(insecure.NewCredentials()))
		require.NoError(t, err)
		connections = append(connections, connection)
		clients = append(clients, etcdserverpb.NewMaintenanceClient(connection))
	}
	t.Cleanup(func() {
		for _, connection := range connections {
			require.NoError(t, connection.Close())
		}
	})

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	const memberID uint64 = 515151
	activated, err := clients[0].Alarm(ctx, &etcdserverpb.AlarmRequest{
		Action:   etcdserverpb.AlarmRequest_ACTIVATE,
		MemberID: memberID,
		Alarm:    etcdserverpb.AlarmType_NOSPACE,
	})
	require.NoError(t, err)
	require.Len(t, activated.Alarms, 1)
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cleanupCancel()
		_, _ = clients[0].Alarm(cleanupCtx, &etcdserverpb.AlarmRequest{
			Action:   etcdserverpb.AlarmRequest_DEACTIVATE,
			MemberID: memberID,
			Alarm:    etcdserverpb.AlarmType_NOSPACE,
		})
	})

	firstStatus, err := clients[0].Status(ctx, &etcdserverpb.StatusRequest{})
	require.NoError(t, err)
	require.Len(t, firstStatus.Errors, 1)
	require.Contains(t, firstStatus.Errors[0], "memberID:515151")
	require.Contains(t, firstStatus.Errors[0], "alarm:NOSPACE")
	for i, client := range clients[1:] {
		statusResponse, statusErr := client.Status(ctx, &etcdserverpb.StatusRequest{})
		require.NoError(t, statusErr, "endpoint %d", i+1)
		require.Equal(t, firstStatus.Errors, statusResponse.Errors, "endpoint %d", i+1)
	}
	_, err = clients[2].Alarm(ctx, &etcdserverpb.AlarmRequest{
		Action:   etcdserverpb.AlarmRequest_DEACTIVATE,
		MemberID: memberID,
		Alarm:    etcdserverpb.AlarmType_NOSPACE,
	})
	require.NoError(t, err)
	for i, client := range clients {
		statusResponse, statusErr := client.Status(ctx, &etcdserverpb.StatusRequest{})
		require.NoError(t, statusErr, "endpoint %d", i)
		require.Empty(t, statusResponse.Errors, "endpoint %d", i)
	}
}

func runStatusAlarmScenario(t *testing.T, endpoint string) statusAlarmOutcome {
	t.Helper()
	endpoint = strings.TrimPrefix(strings.TrimPrefix(endpoint, "http://"), "https://")
	conn, err := grpc.NewClient(endpoint, grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, conn.Close()) })
	maintenance := etcdserverpb.NewMaintenanceClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	const memberID uint64 = 424242
	activated, err := maintenance.Alarm(ctx, &etcdserverpb.AlarmRequest{
		Action:   etcdserverpb.AlarmRequest_ACTIVATE,
		MemberID: memberID,
		Alarm:    etcdserverpb.AlarmType_NOSPACE,
	})
	require.NoError(t, err)
	require.Equal(t, []*etcdserverpb.AlarmMember{{
		MemberID: memberID,
		Alarm:    etcdserverpb.AlarmType_NOSPACE,
	}}, activated.Alarms)
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cleanupCancel()
		_, _ = maintenance.Alarm(cleanupCtx, &etcdserverpb.AlarmRequest{
			Action:   etcdserverpb.AlarmRequest_DEACTIVATE,
			MemberID: memberID,
			Alarm:    etcdserverpb.AlarmType_NOSPACE,
		})
	})

	active, err := maintenance.Status(ctx, &etcdserverpb.StatusRequest{})
	require.NoError(t, err)
	_, err = maintenance.Alarm(ctx, &etcdserverpb.AlarmRequest{
		Action:   etcdserverpb.AlarmRequest_DEACTIVATE,
		MemberID: memberID,
		Alarm:    etcdserverpb.AlarmType_NOSPACE,
	})
	require.NoError(t, err)
	disarmed, err := maintenance.Status(ctx, &etcdserverpb.StatusRequest{})
	require.NoError(t, err)

	return statusAlarmOutcome{
		ActiveErrors:   active.Errors,
		DisarmedErrors: disarmed.Errors,
	}
}
