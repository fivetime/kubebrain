package compat

import (
	"context"
	"os"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type alarmMemberSetOutcome struct {
	Name                   string
	Members                []uint64
	HeaderRevisionDelta    int64
	HeaderIdentitySet      bool
	HeaderRaftTermPositive bool
}

func TestAlarmMemberSetDifferentialAgainstReferenceEtcd(t *testing.T) {
	reference := os.Getenv("REFERENCE_ETCD_ENDPOINT")
	if reference == "" {
		t.Skip("set REFERENCE_ETCD_ENDPOINT to run differential compatibility tests")
	}

	referenceOutcomes := runAlarmMemberSetScenario(t, reference)
	want := []alarmMemberSetOutcome{
		{Name: "activate-first", Members: []uint64{0xa37001}},
		{Name: "activate-first-idempotent", Members: []uint64{0xa37001}},
		{Name: "activate-second", Members: []uint64{0xa37002}},
		{Name: "list-two", Members: []uint64{0xa37001, 0xa37002}},
		{Name: "deactivate-first", Members: []uint64{0xa37001}},
		{Name: "list-second", Members: []uint64{0xa37002}},
		{Name: "deactivate-first-idempotent", Members: []uint64{}},
		{Name: "deactivate-second", Members: []uint64{0xa37002}},
		{Name: "list-empty", Members: []uint64{}},
	}
	for i := range want {
		want[i].HeaderIdentitySet = true
		want[i].HeaderRaftTermPositive = true
	}
	require.Equal(t, want, referenceOutcomes)
	require.Equal(t, referenceOutcomes, runAlarmMemberSetScenario(t, compatEndpoint(t)))
}

func runAlarmMemberSetScenario(t *testing.T, endpoint string) []alarmMemberSetOutcome {
	t.Helper()
	endpoint = strings.TrimPrefix(strings.TrimPrefix(endpoint, "http://"), "https://")
	conn, err := grpc.NewClient(endpoint, grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, conn.Close()) })
	maintenance := etcdserverpb.NewMaintenanceClient(conn)
	kv := etcdserverpb.NewKVClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	t.Cleanup(cancel)
	seedKey := testPrefix(t) + "/alarm-member-set-seed"
	seed, err := kv.Put(ctx, &etcdserverpb.PutRequest{Key: []byte(seedKey), Value: []byte("value")})
	require.NoError(t, err)
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cleanupCancel()
		_, _ = kv.DeleteRange(cleanupCtx, &etcdserverpb.DeleteRangeRequest{Key: []byte(seedKey)})
	})
	const first, second = uint64(0xa37001), uint64(0xa37002)
	for _, memberID := range []uint64{first, second} {
		_, _ = maintenance.Alarm(ctx, &etcdserverpb.AlarmRequest{
			Action: etcdserverpb.AlarmRequest_DEACTIVATE, MemberID: memberID, Alarm: etcdserverpb.AlarmType_NOSPACE,
		})
	}
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cleanupCancel()
		for _, memberID := range []uint64{first, second} {
			_, _ = maintenance.Alarm(cleanupCtx, &etcdserverpb.AlarmRequest{
				Action: etcdserverpb.AlarmRequest_DEACTIVATE, MemberID: memberID, Alarm: etcdserverpb.AlarmType_NOSPACE,
			})
		}
	})

	outcomes := make([]alarmMemberSetOutcome, 0, 9)
	record := func(name string, response *etcdserverpb.AlarmResponse) {
		require.NotNil(t, response.Header)
		members := make([]uint64, 0, len(response.Alarms))
		for _, alarm := range response.Alarms {
			require.Equal(t, etcdserverpb.AlarmType_NOSPACE, alarm.Alarm)
			members = append(members, alarm.MemberID)
		}
		sort.Slice(members, func(i, j int) bool { return members[i] < members[j] })
		outcomes = append(outcomes, alarmMemberSetOutcome{
			Name:                   name,
			Members:                members,
			HeaderRevisionDelta:    response.Header.Revision - seed.Header.Revision,
			HeaderIdentitySet:      response.Header.ClusterId != 0 && response.Header.MemberId != 0,
			HeaderRaftTermPositive: response.Header.RaftTerm > 0,
		})
	}
	call := func(name string, action etcdserverpb.AlarmRequest_AlarmAction, memberID uint64) {
		response, callErr := maintenance.Alarm(ctx, &etcdserverpb.AlarmRequest{
			Action: action, MemberID: memberID, Alarm: etcdserverpb.AlarmType_NOSPACE,
		})
		require.NoError(t, callErr, name)
		record(name, response)
	}
	list := func(name string) {
		response, callErr := maintenance.Alarm(ctx, &etcdserverpb.AlarmRequest{
			Action: etcdserverpb.AlarmRequest_GET, Alarm: etcdserverpb.AlarmType_NOSPACE,
		})
		require.NoError(t, callErr, name)
		record(name, response)
	}

	call("activate-first", etcdserverpb.AlarmRequest_ACTIVATE, first)
	call("activate-first-idempotent", etcdserverpb.AlarmRequest_ACTIVATE, first)
	call("activate-second", etcdserverpb.AlarmRequest_ACTIVATE, second)
	list("list-two")
	call("deactivate-first", etcdserverpb.AlarmRequest_DEACTIVATE, first)
	list("list-second")
	call("deactivate-first-idempotent", etcdserverpb.AlarmRequest_DEACTIVATE, first)
	call("deactivate-second", etcdserverpb.AlarmRequest_DEACTIVATE, second)
	list("list-empty")
	return outcomes
}
