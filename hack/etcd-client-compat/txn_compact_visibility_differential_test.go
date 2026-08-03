package compat

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

type writeTxnCompactedRangeOutcome struct {
	Code          string
	Message       string
	HasResponse   bool
	MarkerPresent bool
}

func TestWriteTxnCompactedRangeDifferentialAgainstReferenceEtcd(t *testing.T) {
	reference := os.Getenv("REFERENCE_ETCD_ENDPOINT")
	if reference == "" {
		t.Skip("set REFERENCE_ETCD_ENDPOINT to run differential compatibility tests")
	}
	want := writeTxnCompactedRangeOutcome{
		Code: "OutOfRange", Message: "etcdserver: mvcc: required revision has been compacted",
	}
	referenceOutcome := runWriteTxnCompactedRangeScenario(t, reference, "etcd")
	require.Equal(t, want, referenceOutcome)
	require.Equal(t, referenceOutcome, runWriteTxnCompactedRangeScenario(t, compatEndpoint(), "kubebrain"))
}

func runWriteTxnCompactedRangeScenario(t *testing.T, endpoint, instance string) writeTxnCompactedRangeOutcome {
	t.Helper()
	conn, err := grpc.NewClient(grpcTarget(endpoint), grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, conn.Close()) })
	kv := etcdserverpb.NewKVClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	t.Cleanup(cancel)
	prefix := fmt.Sprintf("/dbaas-txn-compact-visibility/%s/%d/", instance, time.Now().UnixNano())
	historyKey := []byte(prefix + "history")
	markerKey := []byte(prefix + "marker")
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cleanupCancel()
		_, _ = kv.DeleteRange(cleanupCtx, &etcdserverpb.DeleteRangeRequest{
			Key: []byte(prefix), RangeEnd: []byte(clientv3.GetPrefixRangeEnd(prefix)),
		})
	})

	first, err := kv.Put(ctx, &etcdserverpb.PutRequest{Key: historyKey, Value: []byte("v1")})
	require.NoError(t, err)
	second, err := kv.Put(ctx, &etcdserverpb.PutRequest{Key: historyKey, Value: []byte("v2")})
	require.NoError(t, err)
	_, err = kv.Compact(ctx, &etcdserverpb.CompactionRequest{Revision: second.Header.Revision})
	require.NoError(t, err)

	response, callErr := kv.Txn(ctx, &etcdserverpb.TxnRequest{Success: []*etcdserverpb.RequestOp{
		{Request: &etcdserverpb.RequestOp_RequestPut{RequestPut: &etcdserverpb.PutRequest{
			Key: markerKey, Value: []byte("must-not-commit"),
		}}},
		{Request: &etcdserverpb.RequestOp_RequestRange{RequestRange: &etcdserverpb.RangeRequest{
			Key: historyKey, Revision: first.Header.Revision,
		}}},
	}})
	marker, markerErr := kv.Range(ctx, &etcdserverpb.RangeRequest{Key: markerKey})
	require.NoError(t, markerErr)
	return writeTxnCompactedRangeOutcome{
		Code:          status.Code(callErr).String(),
		Message:       status.Convert(callErr).Message(),
		HasResponse:   response != nil,
		MarkerPresent: len(marker.Kvs) != 0,
	}
}
