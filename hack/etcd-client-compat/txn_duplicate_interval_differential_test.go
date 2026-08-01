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
	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

type txnDuplicateIntervalOutcome struct {
	Name        string
	Code        string
	Message     string
	HasResponse bool
	Succeeded   bool
	RevisionGap int64
	FinalKVs    []string
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
	seedCtx, seedCancel := context.WithTimeout(context.Background(), 5*time.Second)
	seed, err := client.Put(seedCtx, &etcdserverpb.PutRequest{
		Key: []byte(prefix + "seed"), Value: []byte("seed"),
	})
	seedCancel()
	require.NoError(t, err)
	require.NotNil(t, seed.Header)
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
		name    string
		ops     []*etcdserverpb.RequestOp
		wantErr bool
	}{
		{"duplicate-put", []*etcdserverpb.RequestOp{put, put}, true},
		{"put-and-point-delete", []*etcdserverpb.RequestOp{put, deleteKey}, true},
		{"put-and-containing-delete", []*etcdserverpb.RequestOp{put, deleteContaining}, true},
		{"put-and-nested-containing-delete", []*etcdserverpb.RequestOp{put, nestedDelete}, true},
		{"containing-delete-and-nested-put", []*etcdserverpb.RequestOp{deleteContaining, nestedPut}, true},
		{"duplicate-sibling-nested-put", []*etcdserverpb.RequestOp{nestedPutBoth, nestedPutBoth}, true},
		{"disjoint-delete-and-mutually-exclusive-put", []*etcdserverpb.RequestOp{deleteBefore, nestedPutBoth}, false},
		{"nested-overlapping-deletes", []*etcdserverpb.RequestOp{nestedDelete, nestedDeleteBoth}, false},
		{"repeated-overlapping-deletes", []*etcdserverpb.RequestOp{deleteKey, deleteContaining, deleteKey, deleteContaining}, false},
		{"put-and-disjoint-delete", []*etcdserverpb.RequestOp{put, deleteBefore}, false},
	}

	outcomes := make([]txnDuplicateIntervalOutcome, 0, len(tests))
	for _, test := range tests {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		resp, callErr := client.Txn(ctx, &etcdserverpb.TxnRequest{Success: test.ops})
		cancel()
		outcome := txnDuplicateIntervalOutcome{
			Name:    test.name,
			Code:    status.Code(callErr).String(),
			Message: status.Convert(callErr).Message(),
		}
		if test.wantErr {
			require.Error(t, callErr, test.name)
			require.Nil(t, resp, test.name)
			rangeCtx, rangeCancel := context.WithTimeout(context.Background(), 5*time.Second)
			final, rangeErr := client.Range(rangeCtx, &etcdserverpb.RangeRequest{
				Key: []byte(prefix), RangeEnd: []byte(clientv3.GetPrefixRangeEnd(prefix)),
			})
			rangeCancel()
			require.NoError(t, rangeErr, test.name)
			require.NotNil(t, final.Header, test.name)
			outcome.RevisionGap = final.Header.Revision - seed.Header.Revision
			for _, kv := range final.Kvs {
				outcome.FinalKVs = append(outcome.FinalKVs,
					fmt.Sprintf("%s=%s", strings.TrimPrefix(string(kv.Key), prefix), kv.Value))
			}
			require.Zero(t, outcome.RevisionGap, test.name)
			require.Equal(t, []string{"seed=seed"}, outcome.FinalKVs, test.name)
		} else {
			require.NoError(t, callErr, test.name)
			require.NotNil(t, resp, test.name)
			require.True(t, resp.Succeeded, test.name)
			outcome.HasResponse = true
			outcome.Succeeded = resp.Succeeded
		}
		outcomes = append(outcomes, outcome)
	}
	return outcomes
}

func txnRequestOp(success, failure []*etcdserverpb.RequestOp) *etcdserverpb.RequestOp {
	return &etcdserverpb.RequestOp{Request: &etcdserverpb.RequestOp_RequestTxn{
		RequestTxn: &etcdserverpb.TxnRequest{Success: success, Failure: failure},
	}}
}
