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
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

type combinedAlarmOutcome struct {
	Name         string
	Code         codes.Code
	AlarmTypes   []string
	StatusErrors []string
}

func TestCombinedAlarmDifferentialAgainstReferenceEtcd(t *testing.T) {
	reference := os.Getenv("REFERENCE_ETCD_ENDPOINT")
	kubebrain := os.Getenv("KUBEBRAIN_ETCD_ENDPOINT")
	if reference == "" || kubebrain == "" {
		t.Skip("set REFERENCE_ETCD_ENDPOINT and KUBEBRAIN_ETCD_ENDPOINT")
	}

	want := runCombinedAlarmScenario(t, reference)
	require.Equal(t, []combinedAlarmOutcome{
		{Name: "activate-nospace", Code: codes.OK, AlarmTypes: []string{"NOSPACE"}},
		{Name: "activate-nospace-again", Code: codes.OK, AlarmTypes: []string{"NOSPACE"}},
		{Name: "activate-corrupt", Code: codes.OK, AlarmTypes: []string{"CORRUPT"}},
		{Name: "get-all", Code: codes.OK, AlarmTypes: []string{"CORRUPT", "NOSPACE"}},
		{Name: "status", Code: codes.OK, StatusErrors: []string{"CORRUPT", "NOSPACE"}},
		{Name: "range", Code: codes.OK},
		{Name: "read-only-txn", Code: codes.OK},
		{Name: "put-both", Code: codes.DataLoss},
		{Name: "delete-txn-both", Code: codes.DataLoss},
		{Name: "lease-grant-both", Code: codes.DataLoss},
		{Name: "deactivate-corrupt", Code: codes.OK, AlarmTypes: []string{"CORRUPT"}},
		{Name: "get-nospace", Code: codes.OK, AlarmTypes: []string{"NOSPACE"}},
		{Name: "put-nospace", Code: codes.ResourceExhausted},
		{Name: "delete-txn-nospace", Code: codes.OK},
		{Name: "lease-grant-nospace", Code: codes.ResourceExhausted},
		{Name: "deactivate-nospace", Code: codes.OK, AlarmTypes: []string{"NOSPACE"}},
		{Name: "get-empty", Code: codes.OK},
		{Name: "put-restored", Code: codes.OK},
	}, want)
	require.Equal(t, want, runCombinedAlarmScenario(t, kubebrain))
}

func TestCombinedAlarmCrossEndpointStateTransition(t *testing.T) {
	rawEndpoints := os.Getenv("KUBEBRAIN_MULTI_ENDPOINTS")
	if rawEndpoints == "" {
		t.Skip("set KUBEBRAIN_MULTI_ENDPOINTS to three comma-separated replica endpoints")
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
	maintenances := []etcdserverpb.MaintenanceClient{
		etcdserverpb.NewMaintenanceClient(connections[0]),
		etcdserverpb.NewMaintenanceClient(connections[1]),
		etcdserverpb.NewMaintenanceClient(connections[2]),
	}
	kvs := []etcdserverpb.KVClient{
		etcdserverpb.NewKVClient(connections[0]),
		etcdserverpb.NewKVClient(connections[1]),
		etcdserverpb.NewKVClient(connections[2]),
	}
	const (
		noSpaceOwner = uint64(400001)
		corruptOwner = uint64(400002)
	)
	key := []byte(testPrefix(t) + "/combined-alarm-cross-endpoint")
	_, err := kvs[0].Put(ctx, &etcdserverpb.PutRequest{Key: key, Value: []byte("before")})
	require.NoError(t, err)
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cleanupCancel()
		_, _ = maintenances[0].Alarm(cleanupCtx, &etcdserverpb.AlarmRequest{
			Action: etcdserverpb.AlarmRequest_DEACTIVATE, Alarm: etcdserverpb.AlarmType_CORRUPT, MemberID: corruptOwner,
		})
		_, _ = maintenances[0].Alarm(cleanupCtx, &etcdserverpb.AlarmRequest{
			Action: etcdserverpb.AlarmRequest_DEACTIVATE, Alarm: etcdserverpb.AlarmType_NOSPACE, MemberID: noSpaceOwner,
		})
		_, _ = kvs[0].DeleteRange(cleanupCtx, &etcdserverpb.DeleteRangeRequest{Key: key})
	})

	_, err = maintenances[0].Alarm(ctx, &etcdserverpb.AlarmRequest{
		Action: etcdserverpb.AlarmRequest_ACTIVATE, Alarm: etcdserverpb.AlarmType_NOSPACE, MemberID: noSpaceOwner,
	})
	require.NoError(t, err)
	_, err = maintenances[1].Alarm(ctx, &etcdserverpb.AlarmRequest{
		Action: etcdserverpb.AlarmRequest_ACTIVATE, Alarm: etcdserverpb.AlarmType_CORRUPT, MemberID: corruptOwner,
	})
	require.NoError(t, err)
	listed, err := maintenances[2].Alarm(ctx, &etcdserverpb.AlarmRequest{
		Action: etcdserverpb.AlarmRequest_GET, Alarm: etcdserverpb.AlarmType_NONE,
	})
	require.NoError(t, err)
	require.ElementsMatch(t, []*etcdserverpb.AlarmMember{
		{MemberID: noSpaceOwner, Alarm: etcdserverpb.AlarmType_NOSPACE},
		{MemberID: corruptOwner, Alarm: etcdserverpb.AlarmType_CORRUPT},
	}, listed.Alarms)
	_, err = kvs[2].Put(ctx, &etcdserverpb.PutRequest{Key: key, Value: []byte("blocked")})
	require.Equal(t, codes.DataLoss, status.Code(err))

	_, err = maintenances[0].Alarm(ctx, &etcdserverpb.AlarmRequest{
		Action: etcdserverpb.AlarmRequest_DEACTIVATE, Alarm: etcdserverpb.AlarmType_CORRUPT, MemberID: corruptOwner,
	})
	require.NoError(t, err)
	_, err = kvs[1].Put(ctx, &etcdserverpb.PutRequest{Key: key, Value: []byte("still-blocked")})
	require.Equal(t, codes.ResourceExhausted, status.Code(err))
	_, err = kvs[2].DeleteRange(ctx, &etcdserverpb.DeleteRangeRequest{Key: key})
	require.NoError(t, err)

	_, err = maintenances[1].Alarm(ctx, &etcdserverpb.AlarmRequest{
		Action: etcdserverpb.AlarmRequest_DEACTIVATE, Alarm: etcdserverpb.AlarmType_NOSPACE, MemberID: noSpaceOwner,
	})
	require.NoError(t, err)
	_, err = kvs[0].Put(ctx, &etcdserverpb.PutRequest{Key: key, Value: []byte("restored")})
	require.NoError(t, err)
}

func runCombinedAlarmScenario(t *testing.T, endpoint string) []combinedAlarmOutcome {
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
	statusResponse, err := maintenance.Status(ctx, &etcdserverpb.StatusRequest{})
	require.NoError(t, err)
	memberID := statusResponse.Header.MemberId
	require.NotZero(t, memberID)
	key := []byte(testPrefix(t) + "/combined-alarm")
	_, err = kv.Put(ctx, &etcdserverpb.PutRequest{Key: key, Value: []byte("before")})
	require.NoError(t, err)
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cleanupCancel()
		for _, alarm := range []etcdserverpb.AlarmType{etcdserverpb.AlarmType_CORRUPT, etcdserverpb.AlarmType_NOSPACE} {
			_, _ = maintenance.Alarm(cleanupCtx, &etcdserverpb.AlarmRequest{
				Action: etcdserverpb.AlarmRequest_DEACTIVATE, Alarm: alarm, MemberID: memberID,
			})
		}
		_, _ = kv.DeleteRange(cleanupCtx, &etcdserverpb.DeleteRangeRequest{Key: key})
	})

	outcomes := make([]combinedAlarmOutcome, 0, 18)
	record := func(name string, alarmResponse *etcdserverpb.AlarmResponse, statusResponse *etcdserverpb.StatusResponse, callErr error) {
		outcome := combinedAlarmOutcome{Name: name, Code: status.Code(callErr)}
		if alarmResponse != nil {
			for _, alarm := range alarmResponse.Alarms {
				outcome.AlarmTypes = append(outcome.AlarmTypes, alarm.Alarm.String())
			}
			sort.Strings(outcome.AlarmTypes)
		}
		if statusResponse != nil {
			for _, statusError := range statusResponse.Errors {
				switch {
				case strings.Contains(statusError, "CORRUPT"):
					outcome.StatusErrors = append(outcome.StatusErrors, "CORRUPT")
				case strings.Contains(statusError, "NOSPACE"):
					outcome.StatusErrors = append(outcome.StatusErrors, "NOSPACE")
				}
			}
			sort.Strings(outcome.StatusErrors)
		}
		outcomes = append(outcomes, outcome)
	}
	alarmCall := func(name string, action etcdserverpb.AlarmRequest_AlarmAction, alarm etcdserverpb.AlarmType) {
		response, callErr := maintenance.Alarm(ctx, &etcdserverpb.AlarmRequest{
			Action: action, Alarm: alarm, MemberID: memberID,
		})
		record(name, response, nil, callErr)
	}

	alarmCall("activate-nospace", etcdserverpb.AlarmRequest_ACTIVATE, etcdserverpb.AlarmType_NOSPACE)
	alarmCall("activate-nospace-again", etcdserverpb.AlarmRequest_ACTIVATE, etcdserverpb.AlarmType_NOSPACE)
	alarmCall("activate-corrupt", etcdserverpb.AlarmRequest_ACTIVATE, etcdserverpb.AlarmType_CORRUPT)
	alarmCall("get-all", etcdserverpb.AlarmRequest_GET, etcdserverpb.AlarmType_NONE)
	statusResponse, err = maintenance.Status(ctx, &etcdserverpb.StatusRequest{})
	record("status", nil, statusResponse, err)
	_, err = kv.Range(ctx, &etcdserverpb.RangeRequest{Key: key})
	record("range", nil, nil, err)
	_, err = kv.Txn(ctx, readOnlyTxn(key, false))
	record("read-only-txn", nil, nil, err)
	_, err = kv.Put(ctx, &etcdserverpb.PutRequest{Key: key, Value: []byte("blocked")})
	record("put-both", nil, nil, err)
	_, err = kv.Txn(ctx, deleteOnlyTxn(key))
	record("delete-txn-both", nil, nil, err)
	_, err = lease.LeaseGrant(ctx, &etcdserverpb.LeaseGrantRequest{TTL: 30})
	record("lease-grant-both", nil, nil, err)
	alarmCall("deactivate-corrupt", etcdserverpb.AlarmRequest_DEACTIVATE, etcdserverpb.AlarmType_CORRUPT)
	alarmCall("get-nospace", etcdserverpb.AlarmRequest_GET, etcdserverpb.AlarmType_NONE)
	_, err = kv.Put(ctx, &etcdserverpb.PutRequest{Key: key, Value: []byte("blocked")})
	record("put-nospace", nil, nil, err)
	_, err = kv.Txn(ctx, deleteOnlyTxn(key))
	record("delete-txn-nospace", nil, nil, err)
	_, err = lease.LeaseGrant(ctx, &etcdserverpb.LeaseGrantRequest{TTL: 30})
	record("lease-grant-nospace", nil, nil, err)
	alarmCall("deactivate-nospace", etcdserverpb.AlarmRequest_DEACTIVATE, etcdserverpb.AlarmType_NOSPACE)
	alarmCall("get-empty", etcdserverpb.AlarmRequest_GET, etcdserverpb.AlarmType_NONE)
	_, err = kv.Put(ctx, &etcdserverpb.PutRequest{Key: key, Value: []byte("restored")})
	record("put-restored", nil, nil, err)
	return outcomes
}

func deleteOnlyTxn(key []byte) *etcdserverpb.TxnRequest {
	return &etcdserverpb.TxnRequest{Success: []*etcdserverpb.RequestOp{{
		Request: &etcdserverpb.RequestOp_RequestDeleteRange{
			RequestDeleteRange: &etcdserverpb.DeleteRangeRequest{Key: key},
		},
	}}}
}
