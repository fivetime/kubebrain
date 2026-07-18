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
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

type txnOperationValidationOutcome struct {
	Name    string
	Code    string
	Message string
}

func TestTxnOperationValidationDifferentialAgainstReferenceEtcd(t *testing.T) {
	reference := os.Getenv("REFERENCE_ETCD_ENDPOINT")
	if reference == "" {
		t.Skip("set REFERENCE_ETCD_ENDPOINT to run differential compatibility tests")
	}

	require.Equal(t,
		runTxnOperationValidationScenario(t, reference),
		runTxnOperationValidationScenario(t, compatEndpoint()),
	)
}

func runTxnOperationValidationScenario(t *testing.T, endpoint string) []txnOperationValidationOutcome {
	t.Helper()
	endpoint = strings.TrimPrefix(strings.TrimPrefix(endpoint, "http://"), "https://")
	conn, err := grpc.NewClient(endpoint, grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, conn.Close()) })
	client := etcdserverpb.NewKVClient(conn)

	key := []byte("/dbaas-txn-operation-validation")
	tests := []struct {
		name string
		op   *etcdserverpb.RequestOp
	}{
		{
			name: "put-empty-key-precedes-ignore-value",
			op: &etcdserverpb.RequestOp{Request: &etcdserverpb.RequestOp_RequestPut{
				RequestPut: &etcdserverpb.PutRequest{Value: []byte("value"), IgnoreValue: true},
			}},
		},
		{
			name: "put-ignore-value-precedes-ignore-lease",
			op: &etcdserverpb.RequestOp{Request: &etcdserverpb.RequestOp_RequestPut{
				RequestPut: &etcdserverpb.PutRequest{
					Key: key, Value: []byte("value"), Lease: 1, IgnoreValue: true, IgnoreLease: true,
				},
			}},
		},
		{
			name: "put-ignore-lease-with-lease",
			op: &etcdserverpb.RequestOp{Request: &etcdserverpb.RequestOp_RequestPut{
				RequestPut: &etcdserverpb.PutRequest{Key: key, Lease: 1, IgnoreLease: true},
			}},
		},
		{
			name: "put-missing-lease-precedes-missing-ignore-value-key",
			op: &etcdserverpb.RequestOp{Request: &etcdserverpb.RequestOp_RequestPut{
				RequestPut: &etcdserverpb.PutRequest{Key: key, Lease: 987654321, IgnoreValue: true},
			}},
		},
		{
			name: "range-empty-key-precedes-invalid-sort",
			op: &etcdserverpb.RequestOp{Request: &etcdserverpb.RequestOp_RequestRange{
				RequestRange: &etcdserverpb.RangeRequest{SortOrder: 99, SortTarget: 99},
			}},
		},
		{
			name: "range-invalid-order-precedes-target",
			op: &etcdserverpb.RequestOp{Request: &etcdserverpb.RequestOp_RequestRange{
				RequestRange: &etcdserverpb.RangeRequest{Key: key, SortOrder: 99, SortTarget: 99},
			}},
		},
		{
			name: "range-invalid-target",
			op: &etcdserverpb.RequestOp{Request: &etcdserverpb.RequestOp_RequestRange{
				RequestRange: &etcdserverpb.RangeRequest{Key: key, SortTarget: 99},
			}},
		},
		{
			name: "delete-empty-key",
			op: &etcdserverpb.RequestOp{Request: &etcdserverpb.RequestOp_RequestDeleteRange{
				RequestDeleteRange: &etcdserverpb.DeleteRangeRequest{RangeEnd: []byte{0}},
			}},
		},
		{
			name: "empty-operation",
			op:   &etcdserverpb.RequestOp{},
		},
	}

	outcomes := make([]txnOperationValidationOutcome, 0, len(tests))
	for _, test := range tests {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		_, callErr := client.Txn(ctx, &etcdserverpb.TxnRequest{
			Success: []*etcdserverpb.RequestOp{test.op},
		})
		cancel()
		require.Error(t, callErr, test.name)
		outcomes = append(outcomes, txnOperationValidationOutcome{
			Name:    test.name,
			Code:    status.Code(callErr).String(),
			Message: status.Convert(callErr).Message(),
		})
	}
	return outcomes
}
