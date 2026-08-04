package compat

import (
	"context"
	"fmt"
	"math"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

type txnValidationOrderOutcome struct {
	Code        string
	Message     string
	RevisionGap int64
	FinalKVs    []string
}

func TestTxnValidationOrderDifferentialAgainstReferenceEtcd(t *testing.T) {
	reference := os.Getenv("REFERENCE_ETCD_ENDPOINT")
	if reference == "" {
		t.Skip("set REFERENCE_ETCD_ENDPOINT to run differential compatibility tests")
	}

	prefix := fmt.Sprintf("/dbaas-txn-static-validation-order/%d/", time.Now().UnixNano())
	require.Equal(t,
		runTxnValidationOrderScenario(t, reference, prefix),
		runTxnValidationOrderScenario(t, compatEndpoint(t), prefix),
	)
}

func TestTxnExecutionValidationOrderDifferentialAgainstReferenceEtcd(t *testing.T) {
	reference := os.Getenv("REFERENCE_ETCD_ENDPOINT")
	if reference == "" {
		t.Skip("set REFERENCE_ETCD_ENDPOINT to run differential compatibility tests")
	}

	prefix := fmt.Sprintf("/dbaas-txn-execution-validation-order/%d/", time.Now().UnixNano())
	require.Equal(t,
		runTxnExecutionValidationOrderScenario(t, reference, prefix),
		runTxnExecutionValidationOrderScenario(t, compatEndpoint(t), prefix),
	)
}

func runTxnExecutionValidationOrderScenario(t *testing.T, endpoint, prefix string) []txnOperationValidationOutcome {
	t.Helper()
	endpoint = strings.TrimPrefix(strings.TrimPrefix(endpoint, "http://"), "https://")
	conn, err := grpc.NewClient(endpoint, grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, conn.Close()) })
	client := etcdserverpb.NewKVClient(conn)
	registerRawPrefixCleanup(t, client, prefix)
	seedCtx, seedCancel := context.WithTimeout(context.Background(), 5*time.Second)
	seed, err := client.Put(seedCtx, &etcdserverpb.PutRequest{
		Key: []byte(prefix + "seed"), Value: []byte("seed"),
	})
	seedCancel()
	require.NoError(t, err)
	require.NotNil(t, seed.Header)

	put := &etcdserverpb.RequestOp{Request: &etcdserverpb.RequestOp_RequestPut{
		RequestPut: &etcdserverpb.PutRequest{
			Key: []byte(prefix + "missing-lease"), Value: []byte("value"), Lease: 987654321,
		},
	}}
	read := &etcdserverpb.RequestOp{Request: &etcdserverpb.RequestOp_RequestRange{
		RequestRange: &etcdserverpb.RangeRequest{
			Key: []byte(prefix + "future"), Revision: math.MaxInt64,
		},
	}}
	candidatePut := putRequestOp([]byte(prefix+"mutation"), "unexpected")
	tests := []struct {
		name string
		ops  []*etcdserverpb.RequestOp
	}{
		{name: "lease-before-future-range", ops: []*etcdserverpb.RequestOp{put, read, candidatePut}},
		{name: "future-range-before-lease", ops: []*etcdserverpb.RequestOp{read, put, candidatePut}},
	}

	outcomes := make([]txnOperationValidationOutcome, 0, len(tests))
	for _, test := range tests {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		resp, callErr := client.Txn(ctx, &etcdserverpb.TxnRequest{Success: test.ops})
		cancel()
		require.Error(t, callErr, test.name)
		require.Nil(t, resp, test.name)
		gap, finalKVs := txnValidationOrderState(t, client, prefix, seed.Header.Revision, test.name)
		require.Zero(t, gap, test.name)
		require.Equal(t, []string{"seed=seed"}, finalKVs, test.name)
		outcomes = append(outcomes, txnOperationValidationOutcome{
			Name: test.name, Code: status.Code(callErr).String(), Message: status.Convert(callErr).Message(),
			RevisionGap: gap, FinalKVs: finalKVs,
		})
	}
	return outcomes
}

func runTxnValidationOrderScenario(t *testing.T, endpoint, prefix string) txnValidationOrderOutcome {
	t.Helper()
	endpoint = strings.TrimPrefix(strings.TrimPrefix(endpoint, "http://"), "https://")
	conn, err := grpc.NewClient(endpoint, grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, conn.Close()) })

	client := etcdserverpb.NewKVClient(conn)
	registerRawPrefixCleanup(t, client, prefix)
	seedCtx, seedCancel := context.WithTimeout(context.Background(), 5*time.Second)
	seed, err := client.Put(seedCtx, &etcdserverpb.PutRequest{
		Key: []byte(prefix + "seed"), Value: []byte("seed"),
	})
	seedCancel()
	require.NoError(t, err)
	require.NotNil(t, seed.Header)
	key := []byte(prefix + "mutation")
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	resp, callErr := client.Txn(ctx, &etcdserverpb.TxnRequest{
		Success: []*etcdserverpb.RequestOp{
			putRequestOp(key, "v1"),
			putRequestOp(key, "v2"),
		},
		Failure: []*etcdserverpb.RequestOp{
			putRequestOp(nil, "invalid"),
		},
	})
	cancel()
	require.Error(t, callErr)
	require.Nil(t, resp)
	gap, finalKVs := txnValidationOrderState(t, client, prefix, seed.Header.Revision, "static-validation-order")
	require.Zero(t, gap)
	require.Equal(t, []string{"seed=seed"}, finalKVs)
	return txnValidationOrderOutcome{
		Code: status.Code(callErr).String(), Message: status.Convert(callErr).Message(),
		RevisionGap: gap, FinalKVs: finalKVs,
	}
}

func txnValidationOrderState(t *testing.T, client etcdserverpb.KVClient, prefix string, baseRevision int64, name string) (int64, []string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	final, err := client.Range(ctx, &etcdserverpb.RangeRequest{
		Key: []byte(prefix), RangeEnd: []byte(clientv3.GetPrefixRangeEnd(prefix)),
	})
	require.NoError(t, err, name)
	require.NotNil(t, final.Header, name)
	finalKVs := make([]string, 0, len(final.Kvs))
	for _, kv := range final.Kvs {
		finalKVs = append(finalKVs, fmt.Sprintf("%s=%s", strings.TrimPrefix(string(kv.Key), prefix), kv.Value))
	}
	return final.Header.Revision - baseRevision, finalKVs
}
