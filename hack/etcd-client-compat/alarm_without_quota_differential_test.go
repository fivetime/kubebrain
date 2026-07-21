package compat

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/concurrency"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

type manualAlarmWithoutQuotaOutcome struct {
	Name string
	Code codes.Code
}

func TestManualAlarmWithoutQuotaDifferentialAgainstReferenceEtcd(t *testing.T) {
	reference := os.Getenv("REFERENCE_ETCD_ENDPOINT")
	candidate := os.Getenv("KUBEBRAIN_NO_QUOTA_ENDPOINT")
	if reference == "" || candidate == "" {
		t.Skip("set REFERENCE_ETCD_ENDPOINT and KUBEBRAIN_NO_QUOTA_ENDPOINT")
	}
	want := runManualAlarmWithoutQuotaScenario(t, reference)
	require.Equal(t, []manualAlarmWithoutQuotaOutcome{
		{Name: "put-before-alarm", Code: codes.OK},
		{Name: "lease-grant-before-alarm", Code: codes.OK},
		{Name: "campaign-lease-grant-before-alarm", Code: codes.OK},
		{Name: "activate", Code: codes.OK},
		{Name: "range", Code: codes.OK},
		{Name: "put-capped", Code: codes.ResourceExhausted},
		{Name: "readonly-txn", Code: codes.OK},
		{Name: "write-txn-capped", Code: codes.ResourceExhausted},
		{Name: "failure-branch-put-capped", Code: codes.ResourceExhausted},
		{Name: "lease-grant-capped", Code: codes.ResourceExhausted},
		{Name: "lease-ttl", Code: codes.OK},
		{Name: "lock-capped", Code: codes.Unknown},
		{Name: "campaign-capped", Code: codes.Unknown},
		{Name: "lease-revoke", Code: codes.OK},
		{Name: "campaign-lease-revoke", Code: codes.OK},
		{Name: "delete", Code: codes.OK},
		{Name: "delete-txn", Code: codes.OK},
		{Name: "deactivate", Code: codes.OK},
		{Name: "put-after-disarm", Code: codes.OK},
	}, want)
	require.Equal(t, want, runManualAlarmWithoutQuotaScenario(t, candidate))
}

func runManualAlarmWithoutQuotaScenario(t *testing.T, endpoint string) []manualAlarmWithoutQuotaOutcome {
	t.Helper()
	conn, err := grpc.NewClient(grpcTarget(endpoint), grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, conn.Close()) })
	kv := etcdserverpb.NewKVClient(conn)
	lease := etcdserverpb.NewLeaseClient(conn)
	maintenance := etcdserverpb.NewMaintenanceClient(conn)
	client, err := clientv3.New(clientv3.Config{Endpoints: []string{endpoint}, DialTimeout: 5 * time.Second})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, client.Close()) })
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	t.Cleanup(cancel)
	key := []byte(testPrefix(t) + "/manual-alarm-without-quota")
	const memberID uint64 = 0xa372

	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cleanupCancel()
		_, _ = maintenance.Alarm(cleanupCtx, &etcdserverpb.AlarmRequest{
			Action: etcdserverpb.AlarmRequest_DEACTIVATE, MemberID: memberID, Alarm: etcdserverpb.AlarmType_NOSPACE,
		})
		_, _ = kv.DeleteRange(cleanupCtx, &etcdserverpb.DeleteRangeRequest{Key: key})
	})
	_, _ = maintenance.Alarm(ctx, &etcdserverpb.AlarmRequest{
		Action: etcdserverpb.AlarmRequest_DEACTIVATE, MemberID: memberID, Alarm: etcdserverpb.AlarmType_NOSPACE,
	})

	outcomes := make([]manualAlarmWithoutQuotaOutcome, 0, 19)
	record := func(name string, err error) {
		outcomes = append(outcomes, manualAlarmWithoutQuotaOutcome{Name: name, Code: status.Code(err)})
	}
	_, err = kv.Put(ctx, &etcdserverpb.PutRequest{Key: key, Value: []byte("before")})
	record("put-before-alarm", err)
	grant, err := lease.LeaseGrant(ctx, &etcdserverpb.LeaseGrantRequest{TTL: 30})
	record("lease-grant-before-alarm", err)
	require.NoError(t, err)
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cleanupCancel()
		_, _ = lease.LeaseRevoke(cleanupCtx, &etcdserverpb.LeaseRevokeRequest{ID: grant.ID})
	})
	campaignGrant, err := lease.LeaseGrant(ctx, &etcdserverpb.LeaseGrantRequest{TTL: 30})
	record("campaign-lease-grant-before-alarm", err)
	require.NoError(t, err)
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cleanupCancel()
		_, _ = lease.LeaseRevoke(cleanupCtx, &etcdserverpb.LeaseRevokeRequest{ID: campaignGrant.ID})
	})
	_, err = maintenance.Alarm(ctx, &etcdserverpb.AlarmRequest{
		Action: etcdserverpb.AlarmRequest_ACTIVATE, MemberID: memberID, Alarm: etcdserverpb.AlarmType_NOSPACE,
	})
	record("activate", err)
	_, err = kv.Range(ctx, &etcdserverpb.RangeRequest{Key: key})
	record("range", err)
	_, err = kv.Put(ctx, &etcdserverpb.PutRequest{Key: key, Value: []byte("blocked")})
	record("put-capped", err)
	_, err = kv.Txn(ctx, &etcdserverpb.TxnRequest{Success: []*etcdserverpb.RequestOp{{
		Request: &etcdserverpb.RequestOp_RequestRange{RequestRange: &etcdserverpb.RangeRequest{Key: key}},
	}}})
	record("readonly-txn", err)
	_, err = kv.Txn(ctx, &etcdserverpb.TxnRequest{Success: []*etcdserverpb.RequestOp{{
		Request: &etcdserverpb.RequestOp_RequestPut{RequestPut: &etcdserverpb.PutRequest{Key: key, Value: []byte("blocked-txn")}},
	}}})
	record("write-txn-capped", err)
	_, err = kv.Txn(ctx, &etcdserverpb.TxnRequest{
		Compare: []*etcdserverpb.Compare{{Key: key, Result: etcdserverpb.Compare_EQUAL}},
		Failure: []*etcdserverpb.RequestOp{{Request: &etcdserverpb.RequestOp_RequestPut{
			RequestPut: &etcdserverpb.PutRequest{Key: key, Value: []byte("blocked-failure")},
		}}},
	})
	record("failure-branch-put-capped", err)
	_, err = lease.LeaseGrant(ctx, &etcdserverpb.LeaseGrantRequest{TTL: 30})
	record("lease-grant-capped", err)
	_, err = lease.LeaseTimeToLive(ctx, &etcdserverpb.LeaseTimeToLiveRequest{ID: grant.ID})
	record("lease-ttl", err)
	lockSession, err := concurrency.NewSession(client, concurrency.WithLease(clientv3.LeaseID(grant.ID)))
	require.NoError(t, err)
	mutex := concurrency.NewMutex(lockSession, string(key)+"/lock")
	err = mutex.Lock(ctx)
	record("lock-capped", err)
	campaignSession, err := concurrency.NewSession(client, concurrency.WithLease(clientv3.LeaseID(campaignGrant.ID)))
	require.NoError(t, err)
	election := concurrency.NewElection(campaignSession, string(key)+"/election")
	err = election.Campaign(ctx, "candidate")
	record("campaign-capped", err)
	_, err = lease.LeaseRevoke(ctx, &etcdserverpb.LeaseRevokeRequest{ID: grant.ID})
	record("lease-revoke", err)
	_, err = lease.LeaseRevoke(ctx, &etcdserverpb.LeaseRevokeRequest{ID: campaignGrant.ID})
	record("campaign-lease-revoke", err)
	_, err = kv.DeleteRange(ctx, &etcdserverpb.DeleteRangeRequest{Key: key})
	record("delete", err)
	_, err = kv.Txn(ctx, &etcdserverpb.TxnRequest{Success: []*etcdserverpb.RequestOp{{
		Request: &etcdserverpb.RequestOp_RequestDeleteRange{
			RequestDeleteRange: &etcdserverpb.DeleteRangeRequest{Key: key},
		},
	}}})
	record("delete-txn", err)
	_, err = maintenance.Alarm(ctx, &etcdserverpb.AlarmRequest{
		Action: etcdserverpb.AlarmRequest_DEACTIVATE, MemberID: memberID, Alarm: etcdserverpb.AlarmType_NOSPACE,
	})
	record("deactivate", err)
	_, err = kv.Put(ctx, &etcdserverpb.PutRequest{Key: key, Value: []byte("after")})
	record("put-after-disarm", err)
	return outcomes
}
