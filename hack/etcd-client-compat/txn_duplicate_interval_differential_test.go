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
		runTxnDuplicateIntervalScenario(t, compatEndpoint(t), "kubebrain"),
	)
}

func runTxnDuplicateIntervalScenario(t *testing.T, endpoint, instance string) []txnDuplicateIntervalOutcome {
	t.Helper()
	endpoint = strings.TrimPrefix(strings.TrimPrefix(endpoint, "http://"), "https://")
	conn, err := grpc.NewClient(endpoint, grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, conn.Close()) })

	client := etcdserverpb.NewKVClient(conn)
	tests := []struct {
		name       string
		wantErr    bool
		seedTarget bool
		wantGap    int64
		wantKVs    []string
		selectOps  func(txnDuplicateIntervalOps) []*etcdserverpb.RequestOp
	}{
		{name: "duplicate-put", wantErr: true, wantKVs: []string{"seed=seed"}, selectOps: func(o txnDuplicateIntervalOps) []*etcdserverpb.RequestOp {
			return []*etcdserverpb.RequestOp{o.put, o.put}
		}},
		{name: "put-and-point-delete", wantErr: true, wantKVs: []string{"seed=seed"}, selectOps: func(o txnDuplicateIntervalOps) []*etcdserverpb.RequestOp {
			return []*etcdserverpb.RequestOp{o.put, o.deleteKey}
		}},
		{name: "put-and-containing-delete", wantErr: true, wantKVs: []string{"seed=seed"}, selectOps: func(o txnDuplicateIntervalOps) []*etcdserverpb.RequestOp {
			return []*etcdserverpb.RequestOp{o.put, o.deleteContaining}
		}},
		{name: "put-and-nested-containing-delete", wantErr: true, wantKVs: []string{"seed=seed"}, selectOps: func(o txnDuplicateIntervalOps) []*etcdserverpb.RequestOp {
			return []*etcdserverpb.RequestOp{o.put, o.nestedDelete}
		}},
		{name: "containing-delete-and-nested-put", wantErr: true, wantKVs: []string{"seed=seed"}, selectOps: func(o txnDuplicateIntervalOps) []*etcdserverpb.RequestOp {
			return []*etcdserverpb.RequestOp{o.deleteContaining, o.nestedPut}
		}},
		{name: "duplicate-sibling-nested-put", wantErr: true, wantKVs: []string{"seed=seed"}, selectOps: func(o txnDuplicateIntervalOps) []*etcdserverpb.RequestOp {
			return []*etcdserverpb.RequestOp{o.nestedPutBoth, o.nestedPutBoth}
		}},
		{name: "disjoint-delete-and-mutually-exclusive-put", wantGap: 1, wantKVs: []string{"abc=value", "seed=seed"}, selectOps: func(o txnDuplicateIntervalOps) []*etcdserverpb.RequestOp {
			return []*etcdserverpb.RequestOp{o.deleteBefore, o.nestedPutBoth}
		}},
		{name: "nested-overlapping-deletes", seedTarget: true, wantGap: 1, wantKVs: []string{"seed=seed"}, selectOps: func(o txnDuplicateIntervalOps) []*etcdserverpb.RequestOp {
			return []*etcdserverpb.RequestOp{o.nestedDelete, o.nestedDeleteBoth}
		}},
		{name: "repeated-overlapping-deletes", seedTarget: true, wantGap: 1, wantKVs: []string{"seed=seed"}, selectOps: func(o txnDuplicateIntervalOps) []*etcdserverpb.RequestOp {
			return []*etcdserverpb.RequestOp{o.deleteKey, o.deleteContaining, o.deleteKey, o.deleteContaining}
		}},
		{name: "put-and-disjoint-delete", wantGap: 1, wantKVs: []string{"abc=value", "seed=seed"}, selectOps: func(o txnDuplicateIntervalOps) []*etcdserverpb.RequestOp {
			return []*etcdserverpb.RequestOp{o.put, o.deleteBefore}
		}},
	}

	outcomes := make([]txnDuplicateIntervalOutcome, 0, len(tests))
	for i, test := range tests {
		prefix := fmt.Sprintf("/dbaas-txn-duplicate-interval/%s/%d/%02d/", instance, time.Now().UnixNano(), i)
		ops := newTxnDuplicateIntervalOps(prefix)
		seedCtx, seedCancel := context.WithTimeout(context.Background(), 5*time.Second)
		seed, seedErr := client.Put(seedCtx, &etcdserverpb.PutRequest{
			Key: []byte(prefix + "seed"), Value: []byte("seed"),
		})
		seedCancel()
		require.NoError(t, seedErr, test.name)
		require.NotNil(t, seed.Header, test.name)
		baseRevision := seed.Header.Revision
		if test.seedTarget {
			seedTargetCtx, seedTargetCancel := context.WithTimeout(context.Background(), 5*time.Second)
			seedTarget, seedTargetErr := client.Put(seedTargetCtx, &etcdserverpb.PutRequest{
				Key: []byte(prefix + "abc"), Value: []byte("before"),
			})
			seedTargetCancel()
			require.NoError(t, seedTargetErr, test.name)
			require.NotNil(t, seedTarget.Header, test.name)
			baseRevision = seedTarget.Header.Revision
		}
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		resp, callErr := client.Txn(ctx, &etcdserverpb.TxnRequest{Success: test.selectOps(ops)})
		cancel()
		outcome := txnDuplicateIntervalOutcome{
			Name:    test.name,
			Code:    status.Code(callErr).String(),
			Message: status.Convert(callErr).Message(),
		}
		if test.wantErr {
			require.Error(t, callErr, test.name)
			require.Nil(t, resp, test.name)
		} else {
			require.NoError(t, callErr, test.name)
			require.NotNil(t, resp, test.name)
			require.True(t, resp.Succeeded, test.name)
			outcome.HasResponse = true
			outcome.Succeeded = resp.Succeeded
		}
		rangeCtx, rangeCancel := context.WithTimeout(context.Background(), 5*time.Second)
		final, rangeErr := client.Range(rangeCtx, &etcdserverpb.RangeRequest{
			Key: []byte(prefix), RangeEnd: []byte(clientv3.GetPrefixRangeEnd(prefix)),
		})
		rangeCancel()
		require.NoError(t, rangeErr, test.name)
		require.NotNil(t, final.Header, test.name)
		outcome.RevisionGap = final.Header.Revision - baseRevision
		for _, kv := range final.Kvs {
			outcome.FinalKVs = append(outcome.FinalKVs,
				fmt.Sprintf("%s=%s", strings.TrimPrefix(string(kv.Key), prefix), kv.Value))
		}
		require.Equal(t, test.wantGap, outcome.RevisionGap, test.name)
		require.Equal(t, test.wantKVs, outcome.FinalKVs, test.name)
		outcomes = append(outcomes, outcome)
	}
	return outcomes
}

type txnDuplicateIntervalOps struct {
	put              *etcdserverpb.RequestOp
	deleteKey        *etcdserverpb.RequestOp
	deleteContaining *etcdserverpb.RequestOp
	deleteBefore     *etcdserverpb.RequestOp
	nestedDelete     *etcdserverpb.RequestOp
	nestedDeleteBoth *etcdserverpb.RequestOp
	nestedPut        *etcdserverpb.RequestOp
	nestedPutBoth    *etcdserverpb.RequestOp
}

func newTxnDuplicateIntervalOps(prefix string) txnDuplicateIntervalOps {
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
	return txnDuplicateIntervalOps{
		put: put, deleteKey: deleteKey, deleteContaining: deleteContaining, deleteBefore: deleteBefore,
		nestedDelete: nestedDelete, nestedDeleteBoth: nestedDeleteBoth,
		nestedPut: nestedPut, nestedPutBoth: nestedPutBoth,
	}
}

func txnRequestOp(success, failure []*etcdserverpb.RequestOp) *etcdserverpb.RequestOp {
	return &etcdserverpb.RequestOp{Request: &etcdserverpb.RequestOp_RequestTxn{
		RequestTxn: &etcdserverpb.TxnRequest{Success: success, Failure: failure},
	}}
}
