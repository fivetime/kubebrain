package compat

import (
	"context"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

type corruptAlarmOutcome struct {
	Name       string
	Code       codes.Code
	Message    string
	AlarmCount int
}

type corruptAlarmLeaseExpiryOutcome struct {
	PresentWhileAlarmed bool
	DeletedAfterDisarm  bool
}

func TestCorruptAlarmDefersLeaseExpiryDifferentialAgainstReferenceEtcd(t *testing.T) {
	reference := os.Getenv("REFERENCE_ETCD_ENDPOINT")
	kubebrain := os.Getenv("KUBEBRAIN_ETCD_ENDPOINT")
	if reference == "" || kubebrain == "" {
		t.Skip("set REFERENCE_ETCD_ENDPOINT and KUBEBRAIN_ETCD_ENDPOINT")
	}

	want := runCorruptAlarmLeaseExpiryScenario(t, reference)
	require.Equal(t, corruptAlarmLeaseExpiryOutcome{
		PresentWhileAlarmed: true,
		DeletedAfterDisarm:  true,
	}, want)
	require.Equal(t, want, runCorruptAlarmLeaseExpiryScenario(t, kubebrain))
}

func TestCorruptAlarmDifferentialAgainstReferenceEtcd(t *testing.T) {
	reference := os.Getenv("REFERENCE_ETCD_ENDPOINT")
	kubebrain := os.Getenv("KUBEBRAIN_ETCD_ENDPOINT")
	if reference == "" || kubebrain == "" {
		t.Skip("set REFERENCE_ETCD_ENDPOINT and KUBEBRAIN_ETCD_ENDPOINT")
	}

	want := runCorruptAlarmScenario(t, reference)
	require.Equal(t, []corruptAlarmOutcome{
		{Name: "activate", Code: codes.OK, AlarmCount: 1},
		{Name: "get", Code: codes.OK, AlarmCount: 1},
		{Name: "range", Code: codes.OK},
		{Name: "empty-txn", Code: codes.OK},
		{Name: "linearizable-read-txn", Code: codes.OK},
		{Name: "serializable-read-txn", Code: codes.OK},
		{Name: "write-in-unchosen-branch", Code: codes.DataLoss, Message: "etcdserver: corrupt cluster"},
		{Name: "put", Code: codes.DataLoss, Message: "etcdserver: corrupt cluster"},
		{Name: "delete", Code: codes.DataLoss, Message: "etcdserver: corrupt cluster"},
		{Name: "txn", Code: codes.DataLoss, Message: "etcdserver: corrupt cluster"},
		{Name: "compact", Code: codes.DataLoss, Message: "etcdserver: corrupt cluster"},
		{Name: "lease-grant", Code: codes.DataLoss, Message: "etcdserver: corrupt cluster"},
		{Name: "lease-revoke", Code: codes.DataLoss, Message: "etcdserver: corrupt cluster"},
		{Name: "deactivate-wrong-member", Code: codes.OK},
		{Name: "put-still-blocked", Code: codes.DataLoss, Message: "etcdserver: corrupt cluster"},
		{Name: "deactivate", Code: codes.OK, AlarmCount: 1},
		{Name: "put-after-disarm", Code: codes.OK},
	}, want)
	require.Equal(t, want, runCorruptAlarmScenario(t, kubebrain))
}

func TestCorruptAlarmCrossEndpoint(t *testing.T) {
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
	firstMaintenance := etcdserverpb.NewMaintenanceClient(connections[0])
	thirdMaintenance := etcdserverpb.NewMaintenanceClient(connections[2])
	firstKV := etcdserverpb.NewKVClient(connections[0])
	secondKV := etcdserverpb.NewKVClient(connections[1])
	statusResponse, err := firstMaintenance.Status(ctx, &etcdserverpb.StatusRequest{})
	require.NoError(t, err)
	memberID := statusResponse.Header.MemberId
	require.NotZero(t, memberID)
	key := []byte(testPrefix(t) + "/corrupt-alarm-cross-endpoint")
	_, err = firstKV.Put(ctx, &etcdserverpb.PutRequest{Key: key, Value: []byte("before")})
	require.NoError(t, err)
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cleanupCancel()
		_, _ = firstMaintenance.Alarm(cleanupCtx, &etcdserverpb.AlarmRequest{
			Action: etcdserverpb.AlarmRequest_DEACTIVATE, Alarm: etcdserverpb.AlarmType_CORRUPT, MemberID: memberID,
		})
		_, _ = firstKV.DeleteRange(cleanupCtx, &etcdserverpb.DeleteRangeRequest{Key: key})
	})

	_, err = firstMaintenance.Alarm(ctx, &etcdserverpb.AlarmRequest{
		Action: etcdserverpb.AlarmRequest_ACTIVATE, Alarm: etcdserverpb.AlarmType_CORRUPT, MemberID: memberID,
	})
	require.NoError(t, err)
	read, err := secondKV.Range(ctx, &etcdserverpb.RangeRequest{Key: key})
	require.NoError(t, err)
	require.Len(t, read.Kvs, 1)
	_, err = secondKV.Put(ctx, &etcdserverpb.PutRequest{Key: key, Value: []byte("blocked")})
	require.ErrorIs(t, err, rpctypes.ErrGRPCCorrupt)
	listed, err := thirdMaintenance.Alarm(ctx, &etcdserverpb.AlarmRequest{
		Action: etcdserverpb.AlarmRequest_GET, Alarm: etcdserverpb.AlarmType_CORRUPT,
	})
	require.NoError(t, err)
	require.Len(t, listed.Alarms, 1)
	require.Equal(t, memberID, listed.Alarms[0].MemberID)
	_, err = thirdMaintenance.Alarm(ctx, &etcdserverpb.AlarmRequest{
		Action: etcdserverpb.AlarmRequest_DEACTIVATE, Alarm: etcdserverpb.AlarmType_CORRUPT, MemberID: memberID,
	})
	require.NoError(t, err)
	_, err = firstKV.Put(ctx, &etcdserverpb.PutRequest{Key: key, Value: []byte("restored")})
	require.NoError(t, err)
}

func runCorruptAlarmScenario(t *testing.T, endpoint string) []corruptAlarmOutcome {
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
	key := []byte(testPrefix(t) + "/corrupt-alarm")
	_, err = kv.Put(ctx, &etcdserverpb.PutRequest{Key: key, Value: []byte("before")})
	require.NoError(t, err)
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cleanupCancel()
		_, _ = maintenance.Alarm(cleanupCtx, &etcdserverpb.AlarmRequest{
			Action: etcdserverpb.AlarmRequest_DEACTIVATE, Alarm: etcdserverpb.AlarmType_CORRUPT, MemberID: memberID,
		})
		_, _ = kv.DeleteRange(cleanupCtx, &etcdserverpb.DeleteRangeRequest{Key: key})
	})

	outcomes := make([]corruptAlarmOutcome, 0, 13)
	record := func(name string, response *etcdserverpb.AlarmResponse, callErr error) {
		outcome := corruptAlarmOutcome{Name: name, Code: status.Code(callErr)}
		if callErr != nil {
			outcome.Message = status.Convert(callErr).Message()
		}
		if response != nil {
			outcome.AlarmCount = len(response.Alarms)
		}
		outcomes = append(outcomes, outcome)
	}
	response, err := maintenance.Alarm(ctx, &etcdserverpb.AlarmRequest{
		Action: etcdserverpb.AlarmRequest_ACTIVATE, Alarm: etcdserverpb.AlarmType_CORRUPT, MemberID: memberID,
	})
	record("activate", response, err)
	response, err = maintenance.Alarm(ctx, &etcdserverpb.AlarmRequest{
		Action: etcdserverpb.AlarmRequest_GET, Alarm: etcdserverpb.AlarmType_CORRUPT,
	})
	record("get", response, err)
	_, err = kv.Range(ctx, &etcdserverpb.RangeRequest{Key: key})
	record("range", nil, err)
	_, err = kv.Txn(ctx, &etcdserverpb.TxnRequest{})
	record("empty-txn", nil, err)
	_, err = kv.Txn(ctx, readOnlyTxn(key, false))
	record("linearizable-read-txn", nil, err)
	_, err = kv.Txn(ctx, readOnlyTxn(key, true))
	record("serializable-read-txn", nil, err)
	_, err = kv.Txn(ctx, &etcdserverpb.TxnRequest{
		Compare: []*etcdserverpb.Compare{{
			Key: key, Result: etcdserverpb.Compare_GREATER,
			Target:      etcdserverpb.Compare_CREATE,
			TargetUnion: &etcdserverpb.Compare_CreateRevision{CreateRevision: 0},
		}},
		Success: readOnlyTxn(key, false).Success,
		Failure: []*etcdserverpb.RequestOp{{
			Request: &etcdserverpb.RequestOp_RequestPut{RequestPut: &etcdserverpb.PutRequest{Key: key, Value: []byte("unchosen")}},
		}},
	})
	record("write-in-unchosen-branch", nil, err)
	_, err = kv.Put(ctx, &etcdserverpb.PutRequest{Key: key, Value: []byte("value")})
	record("put", nil, err)
	_, err = kv.DeleteRange(ctx, &etcdserverpb.DeleteRangeRequest{Key: key})
	record("delete", nil, err)
	_, err = kv.Txn(ctx, &etcdserverpb.TxnRequest{Success: []*etcdserverpb.RequestOp{{
		Request: &etcdserverpb.RequestOp_RequestPut{RequestPut: &etcdserverpb.PutRequest{Key: key, Value: []byte("txn")}},
	}}})
	record("txn", nil, err)
	_, err = kv.Compact(ctx, &etcdserverpb.CompactionRequest{Revision: 1})
	record("compact", nil, err)
	_, err = lease.LeaseGrant(ctx, &etcdserverpb.LeaseGrantRequest{TTL: 10})
	record("lease-grant", nil, err)
	_, err = lease.LeaseRevoke(ctx, &etcdserverpb.LeaseRevokeRequest{ID: 1})
	record("lease-revoke", nil, err)
	response, err = maintenance.Alarm(ctx, &etcdserverpb.AlarmRequest{
		Action: etcdserverpb.AlarmRequest_DEACTIVATE, Alarm: etcdserverpb.AlarmType_CORRUPT, MemberID: memberID + 1,
	})
	record("deactivate-wrong-member", response, err)
	_, err = kv.Put(ctx, &etcdserverpb.PutRequest{Key: key, Value: []byte("value")})
	record("put-still-blocked", nil, err)
	response, err = maintenance.Alarm(ctx, &etcdserverpb.AlarmRequest{
		Action: etcdserverpb.AlarmRequest_DEACTIVATE, Alarm: etcdserverpb.AlarmType_CORRUPT, MemberID: memberID,
	})
	record("deactivate", response, err)
	_, err = kv.Put(ctx, &etcdserverpb.PutRequest{Key: key, Value: []byte("value")})
	record("put-after-disarm", nil, err)
	return outcomes
}

func readOnlyTxn(key []byte, serializable bool) *etcdserverpb.TxnRequest {
	return &etcdserverpb.TxnRequest{Success: []*etcdserverpb.RequestOp{{
		Request: &etcdserverpb.RequestOp_RequestRange{RequestRange: &etcdserverpb.RangeRequest{
			Key: key, Serializable: serializable,
		}},
	}}}
}

func runCorruptAlarmLeaseExpiryScenario(t *testing.T, endpoint string) corruptAlarmLeaseExpiryOutcome {
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
	key := []byte(testPrefix(t) + "/corrupt-expiry")
	grant, err := lease.LeaseGrant(ctx, &etcdserverpb.LeaseGrantRequest{TTL: 2})
	require.NoError(t, err)
	_, err = kv.Put(ctx, &etcdserverpb.PutRequest{Key: key, Value: []byte("leased"), Lease: grant.ID})
	require.NoError(t, err)
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cleanupCancel()
		_, _ = maintenance.Alarm(cleanupCtx, &etcdserverpb.AlarmRequest{
			Action: etcdserverpb.AlarmRequest_DEACTIVATE, Alarm: etcdserverpb.AlarmType_CORRUPT, MemberID: memberID,
		})
		_, _ = kv.DeleteRange(cleanupCtx, &etcdserverpb.DeleteRangeRequest{Key: key})
	})
	_, err = maintenance.Alarm(ctx, &etcdserverpb.AlarmRequest{
		Action: etcdserverpb.AlarmRequest_ACTIVATE, Alarm: etcdserverpb.AlarmType_CORRUPT, MemberID: memberID,
	})
	require.NoError(t, err)

	time.Sleep(4 * time.Second)
	duringAlarm, err := kv.Range(ctx, &etcdserverpb.RangeRequest{Key: key})
	require.NoError(t, err)
	outcome := corruptAlarmLeaseExpiryOutcome{PresentWhileAlarmed: len(duringAlarm.Kvs) == 1}
	_, err = maintenance.Alarm(ctx, &etcdserverpb.AlarmRequest{
		Action: etcdserverpb.AlarmRequest_DEACTIVATE, Alarm: etcdserverpb.AlarmType_CORRUPT, MemberID: memberID,
	})
	require.NoError(t, err)
	require.Eventually(t, func() bool {
		response, rangeErr := kv.Range(ctx, &etcdserverpb.RangeRequest{Key: key})
		return rangeErr == nil && len(response.Kvs) == 0
	}, 15*time.Second, 100*time.Millisecond)
	outcome.DeletedAfterDisarm = true
	return outcome
}
