package compat

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

type unknownAlarmStatusOutcome struct {
	Name                string
	Code                string
	Message             string
	Errors              []string
	HeaderRevisionDelta int64
}

func TestAlarmUnknownTypeStatusDifferentialAgainstReferenceEtcd(t *testing.T) {
	reference := os.Getenv("REFERENCE_ETCD_ENDPOINT")
	if reference == "" {
		t.Skip("set REFERENCE_ETCD_ENDPOINT to run differential compatibility tests")
	}
	const (
		memberID = uint64(0xa342002)
		alarm    = etcdserverpb.AlarmType(127)
	)
	// StatusResponse.Errors uses etcd server formatting, which is deliberately
	// distinct from the protobuf AlarmMember.String representation.
	alarmError := fmt.Sprintf("memberID:%d  alarm:%s", memberID, alarm.String())
	want := []unknownAlarmStatusOutcome{
		{Name: "active", Code: "OK", Errors: []string{alarmError}},
		{Name: "after-wrong-member", Code: "OK", Errors: []string{alarmError}},
		{Name: "disarmed", Code: "OK", Errors: []string{}},
	}
	referenceOutcomes := runUnknownAlarmStatusScenario(t, reference, memberID, alarm)
	require.Equal(t, want, referenceOutcomes)
	require.Equal(t, referenceOutcomes, runUnknownAlarmStatusScenario(t, compatEndpoint(), memberID, alarm))
}

func runUnknownAlarmStatusScenario(
	t *testing.T,
	endpoint string,
	memberID uint64,
	alarm etcdserverpb.AlarmType,
) []unknownAlarmStatusOutcome {
	t.Helper()
	conn, err := grpc.NewClient(grpcTarget(endpoint), grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, conn.Close()) })
	kv := etcdserverpb.NewKVClient(conn)
	maintenance := etcdserverpb.NewMaintenanceClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	t.Cleanup(cancel)
	disarm := func(callCtx context.Context, requestedMember uint64) {
		_, _ = maintenance.Alarm(callCtx, &etcdserverpb.AlarmRequest{
			Action: etcdserverpb.AlarmRequest_DEACTIVATE, MemberID: requestedMember, Alarm: alarm,
		})
	}
	disarm(ctx, memberID)
	seedKey := testPrefix(t) + "/alarm-unknown-status-seed"
	seed, err := kv.Put(ctx, &etcdserverpb.PutRequest{Key: []byte(seedKey), Value: []byte("value")})
	require.NoError(t, err)
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cleanupCancel()
		disarm(cleanupCtx, memberID)
		_, _ = kv.DeleteRange(cleanupCtx, &etcdserverpb.DeleteRangeRequest{Key: []byte(seedKey)})
	})
	_, err = maintenance.Alarm(ctx, &etcdserverpb.AlarmRequest{
		Action: etcdserverpb.AlarmRequest_ACTIVATE, MemberID: memberID, Alarm: alarm,
	})
	require.NoError(t, err)

	outcomes := make([]unknownAlarmStatusOutcome, 0, 3)
	recordStatus := func(name string) {
		response, callErr := maintenance.Status(ctx, &etcdserverpb.StatusRequest{})
		outcome := unknownAlarmStatusOutcome{
			Name: name, Code: status.Code(callErr).String(), Message: status.Convert(callErr).Message(),
			Errors: []string{},
		}
		if response != nil {
			require.NotNil(t, response.Header)
			outcome.Errors = append(outcome.Errors, response.Errors...)
			outcome.HeaderRevisionDelta = response.Header.Revision - seed.Header.Revision
		}
		outcomes = append(outcomes, outcome)
	}
	recordStatus("active")
	disarm(ctx, memberID+1)
	recordStatus("after-wrong-member")
	disarm(ctx, memberID)
	recordStatus("disarmed")
	return outcomes
}
