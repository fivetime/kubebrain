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
	"google.golang.org/grpc/status"
)

type unknownAlarmOutcome struct {
	Name                string
	Code                string
	Message             string
	Members             []uint64
	HeaderRevisionDelta int64
}

func TestAlarmUnknownTypeDifferentialAgainstReferenceEtcd(t *testing.T) {
	reference := os.Getenv("REFERENCE_ETCD_ENDPOINT")
	if reference == "" {
		t.Skip("set REFERENCE_ETCD_ENDPOINT to run differential compatibility tests")
	}

	referenceOutcomes := runUnknownAlarmScenario(t, reference)
	want := []unknownAlarmOutcome{
		{Name: "activate", Code: "OK", Members: []uint64{0xa342001}},
		{Name: "activate-idempotent", Code: "OK", Members: []uint64{0xa342001}},
		{Name: "get-active", Code: "OK", Members: []uint64{0xa342001}},
		{Name: "get-all-active", Code: "OK", Members: []uint64{0xa342001}},
		{Name: "deactivate-wrong-member", Code: "OK", Members: []uint64{}},
		{Name: "get-after-wrong-member", Code: "OK", Members: []uint64{0xa342001}},
		{Name: "deactivate", Code: "OK", Members: []uint64{0xa342001}},
		{Name: "get-empty", Code: "OK", Members: []uint64{}},
	}
	require.Equal(t, want, referenceOutcomes)
	require.Equal(t, referenceOutcomes, runUnknownAlarmScenario(t, compatEndpoint()))
}

func runUnknownAlarmScenario(t *testing.T, endpoint string) []unknownAlarmOutcome {
	t.Helper()
	conn, err := grpc.NewClient(grpcTarget(endpoint), grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, conn.Close()) })
	kv := etcdserverpb.NewKVClient(conn)
	maintenance := etcdserverpb.NewMaintenanceClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	t.Cleanup(cancel)

	seedKey := testPrefix(t) + "/alarm-unknown-type-seed"
	seed, err := kv.Put(ctx, &etcdserverpb.PutRequest{Key: []byte(seedKey), Value: []byte("value")})
	require.NoError(t, err)
	const (
		memberID = uint64(0xa342001)
		alarm    = etcdserverpb.AlarmType(127)
	)
	disarm := func(cleanupCtx context.Context) {
		_, _ = maintenance.Alarm(cleanupCtx, &etcdserverpb.AlarmRequest{
			Action: etcdserverpb.AlarmRequest_DEACTIVATE, MemberID: memberID, Alarm: alarm,
		})
	}
	disarm(ctx)
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cleanupCancel()
		disarm(cleanupCtx)
		_, _ = kv.DeleteRange(cleanupCtx, &etcdserverpb.DeleteRangeRequest{Key: []byte(seedKey)})
	})

	outcomes := make([]unknownAlarmOutcome, 0, 8)
	call := func(
		name string,
		action etcdserverpb.AlarmRequest_AlarmAction,
		requestedMember uint64,
		requestedAlarm etcdserverpb.AlarmType,
	) {
		response, callErr := maintenance.Alarm(ctx, &etcdserverpb.AlarmRequest{
			Action: action, MemberID: requestedMember, Alarm: requestedAlarm,
		})
		outcome := unknownAlarmOutcome{
			Name: name, Code: status.Code(callErr).String(), Message: status.Convert(callErr).Message(),
			Members: []uint64{},
		}
		if response != nil {
			require.NotNil(t, response.Header)
			outcome.HeaderRevisionDelta = response.Header.Revision - seed.Header.Revision
			for _, active := range response.Alarms {
				require.Equal(t, alarm, active.Alarm)
				outcome.Members = append(outcome.Members, active.MemberID)
			}
		}
		outcomes = append(outcomes, outcome)
	}

	call("activate", etcdserverpb.AlarmRequest_ACTIVATE, memberID, alarm)
	call("activate-idempotent", etcdserverpb.AlarmRequest_ACTIVATE, memberID, alarm)
	call("get-active", etcdserverpb.AlarmRequest_GET, 0, alarm)
	call("get-all-active", etcdserverpb.AlarmRequest_GET, 0, etcdserverpb.AlarmType_NONE)
	call("deactivate-wrong-member", etcdserverpb.AlarmRequest_DEACTIVATE, memberID+1, alarm)
	call("get-after-wrong-member", etcdserverpb.AlarmRequest_GET, 0, alarm)
	call("deactivate", etcdserverpb.AlarmRequest_DEACTIVATE, memberID, alarm)
	call("get-empty", etcdserverpb.AlarmRequest_GET, 0, alarm)
	return outcomes
}
