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

type compareEnumOutcome struct {
	Code      string
	Message   string
	Succeeded bool
	Value     string
}

func TestTxnCompareEnumDifferentialAgainstReferenceEtcd(t *testing.T) {
	reference := os.Getenv("REFERENCE_ETCD_ENDPOINT")
	if reference == "" {
		t.Skip("set REFERENCE_ETCD_ENDPOINT to run differential compatibility tests")
	}
	require.Equal(t,
		runCompareEnumScenario(t, reference, "etcd"),
		runCompareEnumScenario(t, compatEndpoint(), "kubebrain"),
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
		name         string
		target       etcdserverpb.Compare_CompareTarget
		result       etcdserverpb.Compare_CompareResult
		compareValue []byte
	}{
		{name: "unknown-result", target: etcdserverpb.Compare_MOD, result: 99},
		{name: "unknown-target-equal", target: 99, result: etcdserverpb.Compare_EQUAL},
		{name: "unknown-target-not-equal", target: 99, result: etcdserverpb.Compare_NOT_EQUAL},
		{name: "absent-value-equal-empty", target: etcdserverpb.Compare_VALUE, result: etcdserverpb.Compare_EQUAL},
		{name: "absent-value-not-equal", target: etcdserverpb.Compare_VALUE, result: etcdserverpb.Compare_NOT_EQUAL, compareValue: []byte("value")},
		{name: "absent-value-unknown-result", target: etcdserverpb.Compare_VALUE, result: 99},
	}
	outcomes := make([]compareEnumOutcome, 0, len(tests))
	for _, test := range tests {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		key := []byte(fmt.Sprintf("/dbaas-compare-enum/%s/%s/%d", instance, test.name, time.Now().UnixNano()))
		compare := &etcdserverpb.Compare{
			Key:         key,
			Target:      test.target,
			Result:      test.result,
			TargetUnion: &etcdserverpb.Compare_ModRevision{ModRevision: 0},
		}
		if test.target == etcdserverpb.Compare_VALUE {
			compare.TargetUnion = &etcdserverpb.Compare_Value{Value: test.compareValue}
		}
		resp, callErr := client.Txn(ctx, &etcdserverpb.TxnRequest{
			Compare: []*etcdserverpb.Compare{compare},
			Success: []*etcdserverpb.RequestOp{putRequestOp(key, "success")},
			Failure: []*etcdserverpb.RequestOp{putRequestOp(key, "failure")},
		})
		require.NoError(t, callErr)
		ranged, rangeErr := client.Range(ctx, &etcdserverpb.RangeRequest{Key: key})
		cancel()
		require.NoError(t, rangeErr)
		require.Len(t, ranged.Kvs, 1)
		outcomes = append(outcomes, compareEnumOutcome{
			Code:      status.Code(callErr).String(),
			Message:   status.Convert(callErr).Message(),
			Succeeded: resp.Succeeded,
			Value:     string(ranged.Kvs[0].Value),
		})
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 5*time.Second)
		_, cleanupErr := client.DeleteRange(cleanupCtx, &etcdserverpb.DeleteRangeRequest{Key: key})
		cleanupCancel()
		require.NoError(t, cleanupErr)
	}
	return outcomes
}

func putRequestOp(key []byte, value string) *etcdserverpb.RequestOp {
	return &etcdserverpb.RequestOp{Request: &etcdserverpb.RequestOp_RequestPut{
		RequestPut: &etcdserverpb.PutRequest{Key: key, Value: []byte(value)},
	}}
}
