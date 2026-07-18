package compat

import (
	"context"
	"math"
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

type txnValidationOrderOutcome struct {
	Code    string
	Message string
}

func TestTxnValidationOrderDifferentialAgainstReferenceEtcd(t *testing.T) {
	reference := os.Getenv("REFERENCE_ETCD_ENDPOINT")
	if reference == "" {
		t.Skip("set REFERENCE_ETCD_ENDPOINT to run differential compatibility tests")
	}

	require.Equal(t,
		runTxnValidationOrderScenario(t, reference),
		runTxnValidationOrderScenario(t, compatEndpoint()),
	)
}

func TestTxnExecutionValidationOrderDifferentialAgainstReferenceEtcd(t *testing.T) {
	reference := os.Getenv("REFERENCE_ETCD_ENDPOINT")
	if reference == "" {
		t.Skip("set REFERENCE_ETCD_ENDPOINT to run differential compatibility tests")
	}

	require.Equal(t,
		runTxnExecutionValidationOrderScenario(t, reference),
		runTxnExecutionValidationOrderScenario(t, compatEndpoint()),
	)
}

func runTxnExecutionValidationOrderScenario(t *testing.T, endpoint string) []txnOperationValidationOutcome {
	t.Helper()
	endpoint = strings.TrimPrefix(strings.TrimPrefix(endpoint, "http://"), "https://")
	conn, err := grpc.NewClient(endpoint, grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, conn.Close()) })
	client := etcdserverpb.NewKVClient(conn)

	put := &etcdserverpb.RequestOp{Request: &etcdserverpb.RequestOp_RequestPut{
		RequestPut: &etcdserverpb.PutRequest{
			Key: []byte("/dbaas-txn-validation-order/missing-lease"), Value: []byte("value"), Lease: 987654321,
		},
	}}
	read := &etcdserverpb.RequestOp{Request: &etcdserverpb.RequestOp_RequestRange{
		RequestRange: &etcdserverpb.RangeRequest{
			Key: []byte("/dbaas-txn-validation-order/future"), Revision: math.MaxInt64,
		},
	}}
	tests := []struct {
		name string
		ops  []*etcdserverpb.RequestOp
	}{
		{name: "lease-before-future-range", ops: []*etcdserverpb.RequestOp{put, read}},
		{name: "future-range-before-lease", ops: []*etcdserverpb.RequestOp{read, put}},
	}

	outcomes := make([]txnOperationValidationOutcome, 0, len(tests))
	for _, test := range tests {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		_, callErr := client.Txn(ctx, &etcdserverpb.TxnRequest{Success: test.ops})
		cancel()
		require.Error(t, callErr, test.name)
		outcomes = append(outcomes, txnOperationValidationOutcome{
			Name: test.name, Code: status.Code(callErr).String(), Message: status.Convert(callErr).Message(),
		})
	}
	return outcomes
}

func runTxnValidationOrderScenario(t *testing.T, endpoint string) txnValidationOrderOutcome {
	t.Helper()
	endpoint = strings.TrimPrefix(strings.TrimPrefix(endpoint, "http://"), "https://")
	conn, err := grpc.NewClient(endpoint, grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, conn.Close()) })

	key := []byte("/dbaas-txn-validation-order")
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, callErr := etcdserverpb.NewKVClient(conn).Txn(ctx, &etcdserverpb.TxnRequest{
		Success: []*etcdserverpb.RequestOp{
			putRequestOp(key, "v1"),
			putRequestOp(key, "v2"),
		},
		Failure: []*etcdserverpb.RequestOp{
			putRequestOp(nil, "invalid"),
		},
	})
	require.Error(t, callErr)
	return txnValidationOrderOutcome{
		Code:    status.Code(callErr).String(),
		Message: status.Convert(callErr).Message(),
	}
}
