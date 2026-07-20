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
	Name string
	Code codes.Code
}

func TestQuotaAlarmCappedStateDifferentialAgainstReferenceEtcd(t *testing.T) {
	reference := os.Getenv("REFERENCE_QUOTA_ETCD_ENDPOINT")
	kubebrain := os.Getenv("KUBEBRAIN_QUOTA_ENDPOINT")
	if reference == "" || kubebrain == "" {
		t.Skip("set REFERENCE_QUOTA_ETCD_ENDPOINT and KUBEBRAIN_QUOTA_ENDPOINT")
	}

	want := runQuotaAlarmCappedScenario(t, reference)
	require.Equal(t, []quotaAlarmOutcome{
		{Name: "activate", Code: codes.OK},
		{Name: "shrinking-put", Code: codes.ResourceExhausted},
		{Name: "put-in-unchosen-branch", Code: codes.ResourceExhausted},
		{Name: "delete-only-txn", Code: codes.OK},
		{Name: "read-only-txn", Code: codes.OK},
		{Name: "lease-grant", Code: codes.ResourceExhausted},
		{Name: "deactivate", Code: codes.OK},
		{Name: "put-after-disarm", Code: codes.OK},
	}, want)
	require.Equal(t, want, runQuotaAlarmCappedScenario(t, kubebrain))
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
		_, _ = maintenance.Alarm(cleanupCtx, &etcdserverpb.AlarmRequest{
			Action: etcdserverpb.AlarmRequest_DEACTIVATE,
			Alarm:  etcdserverpb.AlarmType_NOSPACE,
		})
		_, _ = kv.DeleteRange(cleanupCtx, &etcdserverpb.DeleteRangeRequest{Key: key})
	})

	outcomes := make([]quotaAlarmOutcome, 0, 8)
	record := func(name string, callErr error) {
		outcomes = append(outcomes, quotaAlarmOutcome{Name: name, Code: status.Code(callErr)})
	}

	_, err = maintenance.Alarm(ctx, &etcdserverpb.AlarmRequest{
		Action: etcdserverpb.AlarmRequest_ACTIVATE,
		Alarm:  etcdserverpb.AlarmType_NOSPACE,
	})
	record("activate", err)

	_, err = kv.Put(ctx, &etcdserverpb.PutRequest{Key: key, Value: []byte("x")})
	record("shrinking-put", err)

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
	record("put-in-unchosen-branch", err)

	_, err = kv.Txn(ctx, &etcdserverpb.TxnRequest{Success: []*etcdserverpb.RequestOp{{
		Request: &etcdserverpb.RequestOp_RequestDeleteRange{
			RequestDeleteRange: &etcdserverpb.DeleteRangeRequest{Key: key},
		},
	}}})
	record("delete-only-txn", err)

	_, err = kv.Txn(ctx, &etcdserverpb.TxnRequest{Success: []*etcdserverpb.RequestOp{{
		Request: &etcdserverpb.RequestOp_RequestRange{
			RequestRange: &etcdserverpb.RangeRequest{Key: key},
		},
	}}})
	record("read-only-txn", err)

	_, err = lease.LeaseGrant(ctx, &etcdserverpb.LeaseGrantRequest{TTL: 30})
	record("lease-grant", err)

	_, err = maintenance.Alarm(ctx, &etcdserverpb.AlarmRequest{
		Action: etcdserverpb.AlarmRequest_DEACTIVATE,
		Alarm:  etcdserverpb.AlarmType_NOSPACE,
	})
	record("deactivate", err)

	_, err = kv.Put(ctx, &etcdserverpb.PutRequest{Key: key, Value: []byte("restored")})
	record("put-after-disarm", err)
	return outcomes
}
