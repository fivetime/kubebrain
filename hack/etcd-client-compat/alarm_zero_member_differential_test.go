package compat

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type zeroMemberAlarmOutcome struct {
	Activate []uint64
	List     []uint64
	Disarm   []uint64
	Final    []uint64
}

func TestAlarmZeroMemberDifferentialAgainstReferenceEtcd(t *testing.T) {
	reference := os.Getenv("REFERENCE_ETCD_ENDPOINT")
	if reference == "" {
		t.Skip("set REFERENCE_ETCD_ENDPOINT to run differential compatibility tests")
	}
	require.Equal(t, runZeroMemberAlarmScenario(t, reference), runZeroMemberAlarmScenario(t, compatEndpoint()))
}

func runZeroMemberAlarmScenario(t *testing.T, endpoint string) zeroMemberAlarmOutcome {
	t.Helper()
	conn, err := grpc.NewClient(grpcTarget(endpoint), grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, conn.Close()) })
	maintenance := etcdserverpb.NewMaintenanceClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	t.Cleanup(cancel)

	call := func(action etcdserverpb.AlarmRequest_AlarmAction) []uint64 {
		response, callErr := maintenance.Alarm(ctx, &etcdserverpb.AlarmRequest{
			Action: action,
			Alarm:  etcdserverpb.AlarmType_NOSPACE,
		})
		require.NoError(t, callErr)
		members := make([]uint64, 0, len(response.Alarms))
		for _, alarm := range response.Alarms {
			require.Equal(t, etcdserverpb.AlarmType_NOSPACE, alarm.Alarm)
			members = append(members, alarm.MemberID)
		}
		return members
	}

	_ = call(etcdserverpb.AlarmRequest_DEACTIVATE)
	outcome := zeroMemberAlarmOutcome{
		Activate: call(etcdserverpb.AlarmRequest_ACTIVATE),
		List:     call(etcdserverpb.AlarmRequest_GET),
		Disarm:   call(etcdserverpb.AlarmRequest_DEACTIVATE),
		Final:    call(etcdserverpb.AlarmRequest_GET),
	}
	require.Equal(t, zeroMemberAlarmOutcome{
		Activate: []uint64{0},
		List:     []uint64{0},
		Disarm:   []uint64{0},
		Final:    []uint64{},
	}, outcome)
	return outcome
}
