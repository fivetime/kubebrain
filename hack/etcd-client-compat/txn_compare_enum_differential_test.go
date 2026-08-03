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

type compareEnumOutcome struct {
	Code              string
	Message           string
	Succeeded         bool
	Value             string
	TxnRevisionGap    int64
	RangeRevisionGap  int64
	CreateRevisionGap int64
	ModRevisionGap    int64
	Version           int64
	FinalKVs          []string
}

func TestTxnCompareEnumDifferentialAgainstReferenceEtcd(t *testing.T) {
	reference := os.Getenv("REFERENCE_ETCD_ENDPOINT")
	if reference == "" {
		t.Skip("set REFERENCE_ETCD_ENDPOINT to run differential compatibility tests")
	}
	require.Equal(t,
		runCompareEnumScenario(t, reference, "etcd"),
		runCompareEnumScenario(t, compatEndpoint(t), "kubebrain"),
	)
}

func runCompareEnumScenario(t *testing.T, endpoint, instance string) []compareEnumOutcome {
	t.Helper()
	endpoint = strings.TrimPrefix(strings.TrimPrefix(endpoint, "http://"), "https://")
	conn, err := grpc.NewClient(endpoint, grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, conn.Close()) })
	client := etcdserverpb.NewKVClient(conn)

	tests := []struct {
		name          string
		target        etcdserverpb.Compare_CompareTarget
		result        etcdserverpb.Compare_CompareResult
		compareValue  []byte
		wantSucceeded bool
		wantValue     string
	}{
		{name: "unknown-result", target: etcdserverpb.Compare_MOD, result: 99, wantSucceeded: true, wantValue: "success"},
		{name: "unknown-target-equal", target: 99, result: etcdserverpb.Compare_EQUAL, wantSucceeded: true, wantValue: "success"},
		{name: "unknown-target-not-equal", target: 99, result: etcdserverpb.Compare_NOT_EQUAL, wantValue: "failure"},
		{name: "absent-value-equal-empty", target: etcdserverpb.Compare_VALUE, result: etcdserverpb.Compare_EQUAL, wantValue: "failure"},
		{name: "absent-value-not-equal", target: etcdserverpb.Compare_VALUE, result: etcdserverpb.Compare_NOT_EQUAL, compareValue: []byte("value"), wantValue: "failure"},
		{name: "absent-value-unknown-result", target: etcdserverpb.Compare_VALUE, result: 99, wantValue: "failure"},
	}
	outcomes := make([]compareEnumOutcome, 0, len(tests))
	for _, test := range tests {
		prefix := fmt.Sprintf("/dbaas-compare-enum/%s/%s/%d/", instance, test.name, time.Now().UnixNano())
		seedCtx, seedCancel := context.WithTimeout(context.Background(), 5*time.Second)
		seed, seedErr := client.Put(seedCtx, &etcdserverpb.PutRequest{
			Key: []byte(prefix + "seed"), Value: []byte("seed"),
		})
		seedCancel()
		require.NoError(t, seedErr, test.name)
		require.NotNil(t, seed.Header, test.name)
		key := []byte(prefix + "key")
		compare := &etcdserverpb.Compare{
			Key:         key,
			Target:      test.target,
			Result:      test.result,
			TargetUnion: &etcdserverpb.Compare_ModRevision{ModRevision: 0},
		}
		if test.target == etcdserverpb.Compare_VALUE {
			compare.TargetUnion = &etcdserverpb.Compare_Value{Value: test.compareValue}
		}
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		resp, callErr := client.Txn(ctx, &etcdserverpb.TxnRequest{
			Compare: []*etcdserverpb.Compare{compare},
			Success: []*etcdserverpb.RequestOp{putRequestOp(key, "success")},
			Failure: []*etcdserverpb.RequestOp{putRequestOp(key, "failure")},
		})
		require.NoError(t, callErr)
		cancel()
		require.NotNil(t, resp, test.name)
		require.NotNil(t, resp.Header, test.name)
		require.Len(t, resp.Responses, 1, test.name)
		putResp := resp.Responses[0].GetResponsePut()
		require.NotNil(t, putResp, test.name)
		require.NotNil(t, putResp.Header, test.name)
		require.Equal(t, resp.Header.Revision, putResp.Header.Revision, test.name)
		rangeCtx, rangeCancel := context.WithTimeout(context.Background(), 5*time.Second)
		ranged, rangeErr := client.Range(rangeCtx, &etcdserverpb.RangeRequest{
			Key: []byte(prefix), RangeEnd: []byte(clientv3.GetPrefixRangeEnd(prefix)),
		})
		rangeCancel()
		require.NoError(t, rangeErr)
		require.NotNil(t, ranged.Header, test.name)
		require.Len(t, ranged.Kvs, 2, test.name)
		require.Equal(t, key, ranged.Kvs[0].Key, test.name)
		require.Equal(t, []byte(prefix+"seed"), ranged.Kvs[1].Key, test.name)
		require.Equal(t, []byte("seed"), ranged.Kvs[1].Value, test.name)
		require.Equal(t, int64(1), resp.Header.Revision-seed.Header.Revision, test.name)
		require.Equal(t, resp.Header.Revision, ranged.Header.Revision, test.name)
		require.Equal(t, resp.Header.Revision, ranged.Kvs[0].CreateRevision, test.name)
		require.Equal(t, resp.Header.Revision, ranged.Kvs[0].ModRevision, test.name)
		require.Equal(t, int64(1), ranged.Kvs[0].Version, test.name)
		require.Equal(t, test.wantSucceeded, resp.Succeeded, test.name)
		require.Equal(t, test.wantValue, string(ranged.Kvs[0].Value), test.name)
		finalKVs := []string{
			fmt.Sprintf("key=%s", ranged.Kvs[0].Value),
			fmt.Sprintf("seed=%s", ranged.Kvs[1].Value),
		}
		outcomes = append(outcomes, compareEnumOutcome{
			Code:              status.Code(callErr).String(),
			Message:           status.Convert(callErr).Message(),
			Succeeded:         resp.Succeeded,
			Value:             string(ranged.Kvs[0].Value),
			TxnRevisionGap:    resp.Header.Revision - seed.Header.Revision,
			RangeRevisionGap:  ranged.Header.Revision - seed.Header.Revision,
			CreateRevisionGap: ranged.Kvs[0].CreateRevision - seed.Header.Revision,
			ModRevisionGap:    ranged.Kvs[0].ModRevision - seed.Header.Revision,
			Version:           ranged.Kvs[0].Version,
			FinalKVs:          finalKVs,
		})
	}
	return outcomes
}

func putRequestOp(key []byte, value string) *etcdserverpb.RequestOp {
	return &etcdserverpb.RequestOp{Request: &etcdserverpb.RequestOp_RequestPut{
		RequestPut: &etcdserverpb.PutRequest{Key: key, Value: []byte(value)},
	}}
}
