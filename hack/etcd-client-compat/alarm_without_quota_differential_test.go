package compat

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
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
		{Name: "activate", Code: codes.OK},
		{Name: "range", Code: codes.OK},
		{Name: "put-capped", Code: codes.ResourceExhausted},
		{Name: "readonly-txn", Code: codes.OK},
		{Name: "write-txn-capped", Code: codes.ResourceExhausted},
		{Name: "lease-grant-capped", Code: codes.ResourceExhausted},
		{Name: "delete", Code: codes.OK},
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

	outcomes := make([]manualAlarmWithoutQuotaOutcome, 0, 10)
	record := func(name string, err error) {
		outcomes = append(outcomes, manualAlarmWithoutQuotaOutcome{Name: name, Code: status.Code(err)})
	}
	_, err = kv.Put(ctx, &etcdserverpb.PutRequest{Key: key, Value: []byte("before")})
	record("put-before-alarm", err)
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
	_, err = lease.LeaseGrant(ctx, &etcdserverpb.LeaseGrantRequest{TTL: 30})
	record("lease-grant-capped", err)
	_, err = kv.DeleteRange(ctx, &etcdserverpb.DeleteRangeRequest{Key: key})
	record("delete", err)
	_, err = maintenance.Alarm(ctx, &etcdserverpb.AlarmRequest{
		Action: etcdserverpb.AlarmRequest_DEACTIVATE, MemberID: memberID, Alarm: etcdserverpb.AlarmType_NOSPACE,
	})
	record("deactivate", err)
	_, err = kv.Put(ctx, &etcdserverpb.PutRequest{Key: key, Value: []byte("after")})
	record("put-after-disarm", err)
	return outcomes
}
