package compat

import (
	"context"
	"math"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

type alarmGetOutcome struct {
	Name                   string
	Code                   string
	Message                string
	AlarmCount             int
	HeaderRevisionDelta    int64
	HeaderIdentitySet      bool
	HeaderRaftTermPositive bool
}

func TestAlarmGetDifferentialAgainstReferenceEtcd(t *testing.T) {
	reference := os.Getenv("REFERENCE_ETCD_ENDPOINT")
	if reference == "" {
		t.Skip("set REFERENCE_ETCD_ENDPOINT to run differential compatibility tests")
	}

	referenceOutcomes := runAlarmGetScenario(t, reference)
	require.Equal(t, []alarmGetOutcome{
		{Name: "all", Code: "OK", HeaderIdentitySet: true, HeaderRaftTermPositive: true},
		{Name: "nospace", Code: "OK", HeaderIdentitySet: true, HeaderRaftTermPositive: true},
		{Name: "corrupt", Code: "OK", HeaderIdentitySet: true, HeaderRaftTermPositive: true},
		{Name: "unknown-alarm", Code: "OK", HeaderIdentitySet: true, HeaderRaftTermPositive: true},
		{Name: "max-member", Code: "OK", HeaderIdentitySet: true, HeaderRaftTermPositive: true},
	}, referenceOutcomes)
	require.Equal(t, referenceOutcomes, runAlarmGetScenario(t, compatEndpoint(t)))
}

func runAlarmGetScenario(t *testing.T, endpoint string) []alarmGetOutcome {
	t.Helper()
	endpoint = strings.TrimPrefix(strings.TrimPrefix(endpoint, "http://"), "https://")
	conn, err := grpc.NewClient(endpoint, grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, conn.Close()) })
	kv := etcdserverpb.NewKVClient(conn)
	maintenance := etcdserverpb.NewMaintenanceClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	key := testPrefix(t) + "/alarm-get"
	put, err := kv.Put(ctx, &etcdserverpb.PutRequest{
		Key:   []byte(key),
		Value: []byte("value"),
	})
	require.NoError(t, err)
	putRevision := put.Header.Revision
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cleanupCancel()
		_, _ = kv.DeleteRange(cleanupCtx, &etcdserverpb.DeleteRangeRequest{Key: []byte(key)})
	})

	tests := []struct {
		name     string
		memberID uint64
		alarm    etcdserverpb.AlarmType
	}{
		{name: "all"},
		{name: "nospace", alarm: etcdserverpb.AlarmType_NOSPACE},
		{name: "corrupt", alarm: etcdserverpb.AlarmType_CORRUPT},
		{name: "unknown-alarm", alarm: etcdserverpb.AlarmType(127)},
		{name: "max-member", memberID: math.MaxUint64},
	}
	outcomes := make([]alarmGetOutcome, 0, len(tests))
	for _, test := range tests {
		resp, callErr := maintenance.Alarm(ctx, &etcdserverpb.AlarmRequest{
			Action:   etcdserverpb.AlarmRequest_GET,
			MemberID: test.memberID,
			Alarm:    test.alarm,
		})
		outcome := alarmGetOutcome{
			Name:    test.name,
			Code:    status.Code(callErr).String(),
			Message: status.Convert(callErr).Message(),
		}
		if resp != nil {
			require.NotNil(t, resp.Header)
			outcome.AlarmCount = len(resp.Alarms)
			outcome.HeaderRevisionDelta = resp.Header.Revision - putRevision
			outcome.HeaderIdentitySet = resp.Header.ClusterId != 0 && resp.Header.MemberId != 0
			outcome.HeaderRaftTermPositive = resp.Header.RaftTerm > 0
		}
		outcomes = append(outcomes, outcome)
	}
	return outcomes
}
