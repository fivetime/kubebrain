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
