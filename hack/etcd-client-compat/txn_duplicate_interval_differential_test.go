package compat

import (
	"context"
	"fmt"
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

type txnDuplicateIntervalOutcome struct {
	Name    string
	Code    string
	Message string
}

func TestTxnDuplicateIntervalDifferentialAgainstReferenceEtcd(t *testing.T) {
	reference := os.Getenv("REFERENCE_ETCD_ENDPOINT")
	if reference == "" {
		t.Skip("set REFERENCE_ETCD_ENDPOINT to run differential compatibility tests")
	}

	require.Equal(t,
		runTxnDuplicateIntervalScenario(t, reference, "etcd"),
		runTxnDuplicateIntervalScenario(t, compatEndpoint(), "kubebrain"),
	)
}

func runTxnDuplicateIntervalScenario(t *testing.T, endpoint, instance string) []txnDuplicateIntervalOutcome {
	t.Helper()
	endpoint = strings.TrimPrefix(strings.TrimPrefix(endpoint, "http://"), "https://")
	conn, err := grpc.NewClient(endpoint, grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, conn.Close()) })

	client := etcdserverpb.NewKVClient(conn)
	prefix := fmt.Sprintf("/dbaas-txn-duplicate-interval/%s/%d/", instance, time.Now().UnixNano())
	key := []byte(prefix + "abc")
	put := putRequestOp(key, "value")
	deleteKey := deleteRequestOp(key, nil)
	deleteContaining := deleteRequestOp([]byte(prefix+"a"), []byte(prefix+"b"))
	deleteBefore := deleteRequestOp([]byte(prefix+"abb"), key)
	nestedDelete := txnRequestOp([]*etcdserverpb.RequestOp{deleteContaining}, nil)
	nestedDeleteBoth := txnRequestOp(
		[]*etcdserverpb.RequestOp{deleteContaining},
		[]*etcdserverpb.RequestOp{deleteContaining},
	)
	nestedPut := txnRequestOp([]*etcdserverpb.RequestOp{put}, nil)
	nestedPutBoth := txnRequestOp(
		[]*etcdserverpb.RequestOp{put},
		[]*etcdserverpb.RequestOp{put},
	)

	tests := []struct {
		name string
		ops  []*etcdserverpb.RequestOp
	}{
		{"duplicate-put", []*etcdserverpb.RequestOp{put, put}},
		{"put-and-point-delete", []*etcdserverpb.RequestOp{put, deleteKey}},
		{"put-and-containing-delete", []*etcdserverpb.RequestOp{put, deleteContaining}},
		{"put-and-nested-containing-delete", []*etcdserverpb.RequestOp{put, nestedDelete}},
		{"containing-delete-and-nested-put", []*etcdserverpb.RequestOp{deleteContaining, nestedPut}},
		{"duplicate-sibling-nested-put", []*etcdserverpb.RequestOp{nestedPutBoth, nestedPutBoth}},
		{"disjoint-delete-and-mutually-exclusive-put", []*etcdserverpb.RequestOp{deleteBefore, nestedPutBoth}},
		{"nested-overlapping-deletes", []*etcdserverpb.RequestOp{nestedDelete, nestedDeleteBoth}},
		{"repeated-overlapping-deletes", []*etcdserverpb.RequestOp{deleteKey, deleteContaining, deleteKey, deleteContaining}},
		{"put-and-disjoint-delete", []*etcdserverpb.RequestOp{put, deleteBefore}},
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	outcomes := make([]txnDuplicateIntervalOutcome, 0, len(tests))
	for _, test := range tests {
		_, callErr := client.Txn(ctx, &etcdserverpb.TxnRequest{Success: test.ops})
		outcomes = append(outcomes, txnDuplicateIntervalOutcome{
			Name:    test.name,
			Code:    status.Code(callErr).String(),
			Message: status.Convert(callErr).Message(),
		})
	}
	return outcomes
}

func txnRequestOp(success, failure []*etcdserverpb.RequestOp) *etcdserverpb.RequestOp {
	return &etcdserverpb.RequestOp{Request: &etcdserverpb.RequestOp_RequestTxn{
		RequestTxn: &etcdserverpb.TxnRequest{Success: success, Failure: failure},
	}}
}
