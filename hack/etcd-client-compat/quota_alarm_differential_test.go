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
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

type quotaAlarmOutcome struct {
	Name       string
	Code       codes.Code
	AlarmCount int
}

func TestQuotaAlarmCappedStateDifferentialAgainstReferenceEtcd(t *testing.T) {
	reference := os.Getenv("REFERENCE_QUOTA_ETCD_ENDPOINT")
	kubebrain := os.Getenv("KUBEBRAIN_QUOTA_ENDPOINT")
	if reference == "" || kubebrain == "" {
		t.Skip("set REFERENCE_QUOTA_ETCD_ENDPOINT and KUBEBRAIN_QUOTA_ENDPOINT")
	}

	want := runQuotaAlarmCappedScenario(t, reference)
	require.Equal(t, []quotaAlarmOutcome{
		{Name: "activate-none", Code: codes.OK},
		{Name: "deactivate-none", Code: codes.OK},
		{Name: "activate", Code: codes.OK, AlarmCount: 1},
		{Name: "deactivate-wrong-member", Code: codes.OK},
		{Name: "put-after-wrong-member", Code: codes.ResourceExhausted},
		{Name: "shrinking-put", Code: codes.ResourceExhausted},
		{Name: "put-in-unchosen-branch", Code: codes.ResourceExhausted},
		{Name: "delete-only-txn", Code: codes.OK},
		{Name: "read-only-txn", Code: codes.OK},
		{Name: "lease-grant", Code: codes.ResourceExhausted},
		{Name: "deactivate", Code: codes.OK, AlarmCount: 1},
		{Name: "deactivate-again", Code: codes.OK},
		{Name: "put-after-disarm", Code: codes.OK},
	}, want)
	require.Equal(t, want, runQuotaAlarmCappedScenario(t, kubebrain))
}

func TestQuotaAlarmCrossEndpointDisarm(t *testing.T) {
	rawEndpoints := os.Getenv("KUBEBRAIN_MULTI_QUOTA_ENDPOINTS")
	if rawEndpoints == "" {
		t.Skip("set KUBEBRAIN_MULTI_QUOTA_ENDPOINTS to three comma-separated replica endpoints")
	}
	endpoints := strings.Split(rawEndpoints, ",")
	require.Len(t, endpoints, 3)

	connections := make([]*grpc.ClientConn, 0, len(endpoints))
	for _, endpoint := range endpoints {
		endpoint = strings.TrimSpace(strings.TrimPrefix(strings.TrimPrefix(endpoint, "http://"), "https://"))
		connection, err := grpc.NewClient(endpoint, grpc.WithTransportCredentials(insecure.NewCredentials()))
		require.NoError(t, err)
		connections = append(connections, connection)
	}
	t.Cleanup(func() {
		for _, connection := range connections {
			require.NoError(t, connection.Close())
		}
	})

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	kv := etcdserverpb.NewKVClient(connections[0])
	first := etcdserverpb.NewMaintenanceClient(connections[0])
	second := etcdserverpb.NewMaintenanceClient(connections[1])
	third := etcdserverpb.NewMaintenanceClient(connections[2])
	key := []byte(testPrefix(t) + "/cross-endpoint-alarm")
	_, err := kv.Put(ctx, &etcdserverpb.PutRequest{Key: key, Value: []byte("value")})
	require.NoError(t, err)
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cleanupCancel()
		if alarms, getErr := first.Alarm(cleanupCtx, &etcdserverpb.AlarmRequest{
			Action: etcdserverpb.AlarmRequest_GET,
			Alarm:  etcdserverpb.AlarmType_NOSPACE,
		}); getErr == nil {
			for _, alarm := range alarms.Alarms {
				_, _ = first.Alarm(cleanupCtx, &etcdserverpb.AlarmRequest{
					Action:   etcdserverpb.AlarmRequest_DEACTIVATE,
					MemberID: alarm.MemberID,
					Alarm:    etcdserverpb.AlarmType_NOSPACE,
				})
			}
		}
		_, _ = kv.DeleteRange(cleanupCtx, &etcdserverpb.DeleteRangeRequest{Key: key})
	})

	activated, err := first.Alarm(ctx, &etcdserverpb.AlarmRequest{
		Action: etcdserverpb.AlarmRequest_ACTIVATE,
		Alarm:  etcdserverpb.AlarmType_NOSPACE,
	})
	require.NoError(t, err)
	require.Len(t, activated.Alarms, 1)
	listed, err := second.Alarm(ctx, &etcdserverpb.AlarmRequest{
		Action: etcdserverpb.AlarmRequest_GET,
		Alarm:  etcdserverpb.AlarmType_NOSPACE,
	})
	require.NoError(t, err)
	require.Len(t, listed.Alarms, 1)
	require.NotZero(t, activated.Alarms[0].MemberID)
	require.NotZero(t, listed.Alarms[0].MemberID)
	require.Equal(t, activated.Alarms, listed.Alarms, "alarm owner must remain stable across serving replicas")

	wrong, err := third.Alarm(ctx, &etcdserverpb.AlarmRequest{
		Action:   etcdserverpb.AlarmRequest_DEACTIVATE,
		MemberID: ^uint64(0),
		Alarm:    etcdserverpb.AlarmType_NOSPACE,
	})
	require.NoError(t, err)
	require.Empty(t, wrong.Alarms)
	_, err = kv.Put(ctx, &etcdserverpb.PutRequest{Key: key, Value: []byte("blocked")})
	require.Equal(t, codes.ResourceExhausted, status.Code(err))

	disarmed, err := third.Alarm(ctx, &etcdserverpb.AlarmRequest{
		Action:   etcdserverpb.AlarmRequest_DEACTIVATE,
		MemberID: listed.Alarms[0].MemberID,
		Alarm:    etcdserverpb.AlarmType_NOSPACE,
	})
	require.NoError(t, err)
	require.Equal(t, listed.Alarms, disarmed.Alarms)
	_, err = kv.Put(ctx, &etcdserverpb.PutRequest{Key: key, Value: []byte("restored")})
	require.NoError(t, err)
}

func runQuotaAlarmCappedScenario(t *testing.T, endpoint string) []quotaAlarmOutcome {
	t.Helper()
	endpoint = strings.TrimPrefix(strings.TrimPrefix(endpoint, "http://"), "https://")
	conn, err := grpc.NewClient(endpoint, grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, conn.Close()) })
	kv := etcdserverpb.NewKVClient(conn)
	lease := etcdserverpb.NewLeaseClient(conn)
	maintenance := etcdserverpb.NewMaintenanceClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	key := []byte(testPrefix(t) + "/quota-capped")
	_, err = kv.Put(ctx, &etcdserverpb.PutRequest{Key: key, Value: []byte("long-value")})
	require.NoError(t, err)
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cleanupCancel()
		if alarms, getErr := maintenance.Alarm(cleanupCtx, &etcdserverpb.AlarmRequest{
			Action: etcdserverpb.AlarmRequest_GET,
			Alarm:  etcdserverpb.AlarmType_NOSPACE,
		}); getErr == nil {
			for _, alarm := range alarms.Alarms {
				_, _ = maintenance.Alarm(cleanupCtx, &etcdserverpb.AlarmRequest{
					Action:   etcdserverpb.AlarmRequest_DEACTIVATE,
					MemberID: alarm.MemberID,
					Alarm:    etcdserverpb.AlarmType_NOSPACE,
				})
			}
		}
		_, _ = kv.DeleteRange(cleanupCtx, &etcdserverpb.DeleteRangeRequest{Key: key})
	})

	outcomes := make([]quotaAlarmOutcome, 0, 13)
	record := func(name string, response *etcdserverpb.AlarmResponse, callErr error) {
		outcome := quotaAlarmOutcome{Name: name, Code: status.Code(callErr)}
		if response != nil {
			outcome.AlarmCount = len(response.Alarms)
		}
		outcomes = append(outcomes, outcome)
	}

	response, err := maintenance.Alarm(ctx, &etcdserverpb.AlarmRequest{
		Action:   etcdserverpb.AlarmRequest_ACTIVATE,
		MemberID: 123,
		Alarm:    etcdserverpb.AlarmType_NONE,
	})
	record("activate-none", response, err)
	response, err = maintenance.Alarm(ctx, &etcdserverpb.AlarmRequest{
		Action:   etcdserverpb.AlarmRequest_DEACTIVATE,
		MemberID: 123,
		Alarm:    etcdserverpb.AlarmType_NONE,
	})
	record("deactivate-none", response, err)

	activate, err := maintenance.Alarm(ctx, &etcdserverpb.AlarmRequest{
		Action: etcdserverpb.AlarmRequest_ACTIVATE,
		Alarm:  etcdserverpb.AlarmType_NOSPACE,
	})
	record("activate", activate, err)
	require.Len(t, activate.GetAlarms(), 1)
	alarmMemberID := activate.Alarms[0].MemberID

	response, err = maintenance.Alarm(ctx, &etcdserverpb.AlarmRequest{
		Action:   etcdserverpb.AlarmRequest_DEACTIVATE,
		MemberID: alarmMemberID + 1,
		Alarm:    etcdserverpb.AlarmType_NOSPACE,
	})
	record("deactivate-wrong-member", response, err)
	_, err = kv.Put(ctx, &etcdserverpb.PutRequest{Key: key, Value: []byte("x")})
	record("put-after-wrong-member", nil, err)

	_, err = kv.Put(ctx, &etcdserverpb.PutRequest{Key: key, Value: []byte("x")})
	record("shrinking-put", nil, err)

	_, err = kv.Txn(ctx, &etcdserverpb.TxnRequest{
		Compare: []*etcdserverpb.Compare{{
			Key:         key,
			Result:      etcdserverpb.Compare_NOT_EQUAL,
			Target:      etcdserverpb.Compare_VERSION,
			TargetUnion: &etcdserverpb.Compare_Version{Version: 0},
		}},
		Success: []*etcdserverpb.RequestOp{{
			Request: &etcdserverpb.RequestOp_RequestRange{
				RequestRange: &etcdserverpb.RangeRequest{Key: key},
			},
		}},
		Failure: []*etcdserverpb.RequestOp{{
			Request: &etcdserverpb.RequestOp_RequestPut{
				RequestPut: &etcdserverpb.PutRequest{Key: key, Value: []byte("x")},
			},
		}},
	})
	record("put-in-unchosen-branch", nil, err)

	_, err = kv.Txn(ctx, &etcdserverpb.TxnRequest{Success: []*etcdserverpb.RequestOp{{
		Request: &etcdserverpb.RequestOp_RequestDeleteRange{
			RequestDeleteRange: &etcdserverpb.DeleteRangeRequest{Key: key},
		},
	}}})
	record("delete-only-txn", nil, err)

	_, err = kv.Txn(ctx, &etcdserverpb.TxnRequest{Success: []*etcdserverpb.RequestOp{{
		Request: &etcdserverpb.RequestOp_RequestRange{
			RequestRange: &etcdserverpb.RangeRequest{Key: key},
		},
	}}})
	record("read-only-txn", nil, err)

	_, err = lease.LeaseGrant(ctx, &etcdserverpb.LeaseGrantRequest{TTL: 30})
	record("lease-grant", nil, err)

	response, err = maintenance.Alarm(ctx, &etcdserverpb.AlarmRequest{
		Action:   etcdserverpb.AlarmRequest_DEACTIVATE,
		MemberID: alarmMemberID,
		Alarm:    etcdserverpb.AlarmType_NOSPACE,
	})
	record("deactivate", response, err)
	response, err = maintenance.Alarm(ctx, &etcdserverpb.AlarmRequest{
		Action:   etcdserverpb.AlarmRequest_DEACTIVATE,
		MemberID: alarmMemberID,
		Alarm:    etcdserverpb.AlarmType_NOSPACE,
	})
	record("deactivate-again", response, err)

	_, err = kv.Put(ctx, &etcdserverpb.PutRequest{Key: key, Value: []byte("restored")})
	record("put-after-disarm", nil, err)
	return outcomes
}
