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
)

type multiCompareOutcome struct {
	Name            string
	TopSucceeded    bool
	TopMarker       string
	NestedSucceeded bool
	NestedMarker    string
	WantSucceeded   bool
}

func TestTxnMultiCompareDifferentialAgainstReferenceEtcd(t *testing.T) {
	reference := os.Getenv("REFERENCE_ETCD_ENDPOINT")
	if reference == "" {
		t.Skip("set REFERENCE_ETCD_ENDPOINT to run differential compatibility tests")
	}

	referenceOutcomes := runMultiCompareScenario(t, reference, "etcd")
	for _, outcome := range referenceOutcomes {
		wantMarker := map[bool]string{true: "success", false: "failure"}[outcome.WantSucceeded]
		require.Equal(t, outcome.WantSucceeded, outcome.TopSucceeded, "official top txn for %s", outcome.Name)
		require.Equal(t, wantMarker, outcome.TopMarker, "official top branch for %s", outcome.Name)
		require.Equal(t, outcome.WantSucceeded, outcome.NestedSucceeded, "official nested txn for %s", outcome.Name)
		require.Equal(t, wantMarker, outcome.NestedMarker, "official nested branch for %s", outcome.Name)
	}
	require.Equal(t, referenceOutcomes, runMultiCompareScenario(t, compatEndpoint(t), "kubebrain"))
}

func runMultiCompareScenario(t *testing.T, endpoint, instance string) []multiCompareOutcome {
	t.Helper()
	endpoint = strings.TrimPrefix(strings.TrimPrefix(endpoint, "http://"), "https://")
	conn, err := grpc.NewClient(endpoint, grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, conn.Close()) })

	kv := etcdserverpb.NewKVClient(conn)
	lease := etcdserverpb.NewLeaseClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	prefix := fmt.Sprintf("/dbaas-multi-compare/%s/%d/", instance, time.Now().UnixNano())
	dataPrefix := prefix + "data/"
	dataEnd := []byte(clientv3.GetPrefixRangeEnd(dataPrefix))
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cleanupCancel()
		_, _ = kv.DeleteRange(cleanupCtx, &etcdserverpb.DeleteRangeRequest{
			Key: []byte(prefix), RangeEnd: []byte(clientv3.GetPrefixRangeEnd(prefix)),
		})
	})

	grant, err := lease.LeaseGrant(ctx, &etcdserverpb.LeaseGrantRequest{TTL: 300})
	require.NoError(t, err)
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cleanupCancel()
		_, _ = lease.LeaseRevoke(cleanupCtx, &etcdserverpb.LeaseRevokeRequest{ID: grant.ID})
	})

	keyA, keyB := []byte(dataPrefix+"a"), []byte(dataPrefix+"b")
	seed, err := kv.Txn(ctx, &etcdserverpb.TxnRequest{Success: []*etcdserverpb.RequestOp{
		putRequestOpWithLease(keyA, "alpha", grant.ID),
		putRequestOpWithLease(keyB, "beta", grant.ID),
	}})
	require.NoError(t, err)
	require.True(t, seed.Succeeded)
	seedRevision := seed.Header.Revision

	trueValue := valueCompare(keyA, nil, etcdserverpb.Compare_EQUAL, "alpha")
	falseValue := valueCompare(keyA, nil, etcdserverpb.Compare_EQUAL, "wrong")
	trueVersion := intCompare(keyB, nil, etcdserverpb.Compare_VERSION, etcdserverpb.Compare_EQUAL, 1)
	falseVersion := intCompare(keyB, nil, etcdserverpb.Compare_VERSION, etcdserverpb.Compare_GREATER, 1)
	trueCreate := intCompare(keyA, nil, etcdserverpb.Compare_CREATE, etcdserverpb.Compare_EQUAL, seedRevision)
	trueMod := intCompare(keyB, nil, etcdserverpb.Compare_MOD, etcdserverpb.Compare_EQUAL, seedRevision)
	trueLease := intCompare(keyA, nil, etcdserverpb.Compare_LEASE, etcdserverpb.Compare_EQUAL, grant.ID)
	trueRange := intCompare([]byte(dataPrefix), dataEnd, etcdserverpb.Compare_VERSION, etcdserverpb.Compare_EQUAL, 1)
	falseRange := valueCompare([]byte(dataPrefix), dataEnd, etcdserverpb.Compare_EQUAL, "alpha")
	missingValue := valueCompare([]byte(dataPrefix+"missing"), nil, etcdserverpb.Compare_NOT_EQUAL, "anything")

	tests := []struct {
		name      string
		compares  []*etcdserverpb.Compare
		succeeded bool
	}{
		{name: "empty", succeeded: true},
		{name: "all-targets-true", compares: []*etcdserverpb.Compare{trueValue, trueVersion, trueCreate, trueMod, trueLease}, succeeded: true},
		{name: "false-first", compares: []*etcdserverpb.Compare{falseValue, trueVersion, trueLease}},
		{name: "false-middle", compares: []*etcdserverpb.Compare{trueValue, falseVersion, trueLease}},
		{name: "false-last", compares: []*etcdserverpb.Compare{trueValue, trueVersion, falseValue}},
		{name: "same-key-conflict", compares: []*etcdserverpb.Compare{trueVersion, intCompare(keyB, nil, etcdserverpb.Compare_VERSION, etcdserverpb.Compare_LESS, 1)}},
		{name: "point-range-true", compares: []*etcdserverpb.Compare{trueValue, trueRange}, succeeded: true},
		{name: "point-range-false", compares: []*etcdserverpb.Compare{trueValue, falseRange}},
		{name: "range-point-false-first", compares: []*etcdserverpb.Compare{falseRange, trueLease}},
		{name: "missing-value-after-true", compares: []*etcdserverpb.Compare{trueCreate, missingValue}},
	}

	outcomes := make([]multiCompareOutcome, 0, len(tests))
	for index, test := range tests {
		topMarker := []byte(fmt.Sprintf("%smarkers/%02d-top", prefix, index))
		topResponse, topErr := kv.Txn(ctx, branchTxn(test.compares, topMarker))
		require.NoError(t, topErr, test.name)
		topValue := readSingleValue(t, ctx, kv, topMarker, test.name+"-top")

		nestedMarker := []byte(fmt.Sprintf("%smarkers/%02d-nested", prefix, index))
		nestedResponse, nestedErr := kv.Txn(ctx, &etcdserverpb.TxnRequest{Success: []*etcdserverpb.RequestOp{{
			Request: &etcdserverpb.RequestOp_RequestTxn{RequestTxn: branchTxn(test.compares, nestedMarker)},
		}}})
		require.NoError(t, nestedErr, test.name)
		require.Len(t, nestedResponse.Responses, 1, test.name)
		inner := nestedResponse.Responses[0].GetResponseTxn()
		require.NotNil(t, inner, test.name)
		nestedValue := readSingleValue(t, ctx, kv, nestedMarker, test.name+"-nested")

		outcomes = append(outcomes, multiCompareOutcome{
			Name: test.name, TopSucceeded: topResponse.Succeeded, TopMarker: topValue,
			NestedSucceeded: inner.Succeeded, NestedMarker: nestedValue, WantSucceeded: test.succeeded,
		})
	}
	return outcomes
}

func branchTxn(compares []*etcdserverpb.Compare, marker []byte) *etcdserverpb.TxnRequest {
	return &etcdserverpb.TxnRequest{
		Compare: compares,
		Success: []*etcdserverpb.RequestOp{putRequestOp(marker, "success")},
		Failure: []*etcdserverpb.RequestOp{putRequestOp(marker, "failure")},
	}
}

func putRequestOpWithLease(key []byte, value string, leaseID int64) *etcdserverpb.RequestOp {
	return &etcdserverpb.RequestOp{Request: &etcdserverpb.RequestOp_RequestPut{RequestPut: &etcdserverpb.PutRequest{
		Key: key, Value: []byte(value), Lease: leaseID,
	}}}
}

func readSingleValue(t *testing.T, ctx context.Context, kv etcdserverpb.KVClient, key []byte, name string) string {
	t.Helper()
	response, err := kv.Range(ctx, &etcdserverpb.RangeRequest{Key: key})
	require.NoError(t, err, name)
	require.Len(t, response.Kvs, 1, name)
	return string(response.Kvs[0].Value)
}
