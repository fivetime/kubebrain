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

type multiRangeCompareOutcome struct {
	Name            string
	TopSucceeded    bool
	TopMarker       string
	NestedSucceeded bool
	NestedMarker    string
	WantSucceeded   bool
}

// TestTxnMultiRangeCompareDifferentialAgainstReferenceEtcd covers conjunction
// across multiple range comparisons. The single-range generated matrix and the
// point-plus-range matrix do not exercise aggregation when two range scans
// overlap, are disjoint, or fail at different positions in Compare.
func TestTxnMultiRangeCompareDifferentialAgainstReferenceEtcd(t *testing.T) {
	reference := os.Getenv("REFERENCE_ETCD_ENDPOINT")
	if reference == "" {
		t.Skip("set REFERENCE_ETCD_ENDPOINT to run differential compatibility tests")
	}

	referenceOutcomes := runMultiRangeCompareScenario(t, reference, "etcd")
	for _, outcome := range referenceOutcomes {
		wantMarker := map[bool]string{true: "success", false: "failure"}[outcome.WantSucceeded]
		require.Equal(t, outcome.WantSucceeded, outcome.TopSucceeded, "official top txn for %s", outcome.Name)
		require.Equal(t, wantMarker, outcome.TopMarker, "official top branch for %s", outcome.Name)
		require.Equal(t, outcome.WantSucceeded, outcome.NestedSucceeded, "official nested txn for %s", outcome.Name)
		require.Equal(t, wantMarker, outcome.NestedMarker, "official nested branch for %s", outcome.Name)
	}
	require.Equal(t, referenceOutcomes, runMultiRangeCompareScenario(t, compatEndpoint(t), "kubebrain"))
}

func runMultiRangeCompareScenario(t *testing.T, endpoint, instance string) []multiRangeCompareOutcome {
	t.Helper()
	endpoint = strings.TrimPrefix(strings.TrimPrefix(endpoint, "http://"), "https://")
	conn, err := grpc.NewClient(endpoint, grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, conn.Close()) })

	kv := etcdserverpb.NewKVClient(conn)
	lease := etcdserverpb.NewLeaseClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	prefix := fmt.Sprintf("/dbaas-multi-range-compare/%s/%d/", instance, time.Now().UnixNano())
	dataPrefix := prefix + "data/"
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cleanupCancel()
		_, _ = kv.DeleteRange(cleanupCtx, &etcdserverpb.DeleteRangeRequest{
			Key: []byte(prefix), RangeEnd: []byte(clientv3.GetPrefixRangeEnd(prefix)),
		})
	})

	grantA, err := lease.LeaseGrant(ctx, &etcdserverpb.LeaseGrantRequest{TTL: 300})
	require.NoError(t, err)
	grantB, err := lease.LeaseGrant(ctx, &etcdserverpb.LeaseGrantRequest{TTL: 300})
	require.NoError(t, err)
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cleanupCancel()
		_, _ = lease.LeaseRevoke(cleanupCtx, &etcdserverpb.LeaseRevokeRequest{ID: grantA.ID})
		_, _ = lease.LeaseRevoke(cleanupCtx, &etcdserverpb.LeaseRevokeRequest{ID: grantB.ID})
	})

	keys := [][]byte{[]byte(dataPrefix + "a"), []byte(dataPrefix + "b"), []byte(dataPrefix + "c"), []byte(dataPrefix + "d")}
	rangeEnd := []byte(dataPrefix + "e")
	seedOps := make([]*etcdserverpb.RequestOp, 0, len(keys))
	for index, key := range keys {
		leaseID := grantA.ID
		if index >= 2 {
			leaseID = grantB.ID
		}
		seedOps = append(seedOps, putRequestOpWithLease(key, fmt.Sprintf("value-%d", index), leaseID))
	}
	seed, err := kv.Txn(ctx, &etcdserverpb.TxnRequest{Success: seedOps})
	require.NoError(t, err)
	require.True(t, seed.Succeeded)
	updated, err := kv.Txn(ctx, &etcdserverpb.TxnRequest{Success: []*etcdserverpb.RequestOp{
		putRequestOpWithLease(keys[1], "value-1-updated", grantA.ID),
		putRequestOpWithLease(keys[3], "value-3-updated", grantB.ID),
	}})
	require.NoError(t, err)
	require.True(t, updated.Succeeded)

	rangeCompare := func(begin, end int, result etcdserverpb.Compare_CompareResult, version int64) *etcdserverpb.Compare {
		return intCompare(keys[begin], keys[end], etcdserverpb.Compare_VERSION, result, version)
	}
	trueAB := rangeCompare(0, 2, etcdserverpb.Compare_GREATER, 0)
	trueBC := rangeCompare(1, 3, etcdserverpb.Compare_GREATER, 0)
	trueCD := rangeCompare(2, 3, etcdserverpb.Compare_GREATER, 0)
	falseAB := rangeCompare(0, 2, etcdserverpb.Compare_GREATER, 2)
	falseBC := rangeCompare(1, 3, etcdserverpb.Compare_LESS, 1)
	allValueTrue := valueCompare(keys[0], rangeEnd, etcdserverpb.Compare_LESS, "zz")
	allVersionTrue := intCompare(keys[0], rangeEnd, etcdserverpb.Compare_VERSION, etcdserverpb.Compare_GREATER, 0)
	allCreateTrue := intCompare(keys[0], rangeEnd, etcdserverpb.Compare_CREATE, etcdserverpb.Compare_EQUAL, seed.Header.Revision)
	allModTrue := intCompare(keys[0], rangeEnd, etcdserverpb.Compare_MOD, etcdserverpb.Compare_GREATER, 0)
	leaseATrue := intCompare(keys[0], keys[2], etcdserverpb.Compare_LEASE, etcdserverpb.Compare_EQUAL, grantA.ID)
	leaseBTrue := intCompare(keys[2], rangeEnd, etcdserverpb.Compare_LEASE, etcdserverpb.Compare_EQUAL, grantB.ID)
	allValueFalse := valueCompare(keys[0], rangeEnd, etcdserverpb.Compare_EQUAL, "value-0")
	mixedVersionFalse := intCompare(keys[0], rangeEnd, etcdserverpb.Compare_VERSION, etcdserverpb.Compare_EQUAL, 1)
	allLeaseFalse := intCompare(keys[0], rangeEnd, etcdserverpb.Compare_LEASE, etcdserverpb.Compare_EQUAL, grantA.ID)
	// etcd evaluates an empty range against the target's missing-key zero
	// value; VERSION == 0 is therefore the true empty-range comparison.
	empty := intCompare([]byte(dataPrefix+"x"), []byte(dataPrefix+"z"), etcdserverpb.Compare_VERSION, etcdserverpb.Compare_EQUAL, 0)

	tests := []struct {
		name      string
		compares  []*etcdserverpb.Compare
		succeeded bool
	}{
		{name: "overlap-all-true", compares: []*etcdserverpb.Compare{trueAB, trueBC}, succeeded: true},
		{name: "disjoint-all-true", compares: []*etcdserverpb.Compare{trueAB, trueCD}, succeeded: true},
		{name: "false-first-overlap", compares: []*etcdserverpb.Compare{falseAB, trueBC}},
		{name: "false-last-overlap", compares: []*etcdserverpb.Compare{trueAB, falseBC}},
		{name: "same-range-conflict", compares: []*etcdserverpb.Compare{trueAB, falseAB}},
		{name: "empty-then-nonempty", compares: []*etcdserverpb.Compare{empty, trueBC}, succeeded: true},
		{name: "all-targets-all-true", compares: []*etcdserverpb.Compare{
			allValueTrue, allVersionTrue, allCreateTrue, allModTrue, leaseATrue, leaseBTrue,
		}, succeeded: true},
		{name: "mixed-target-false-first", compares: []*etcdserverpb.Compare{
			allValueFalse, allVersionTrue, allCreateTrue, allModTrue, leaseATrue, leaseBTrue,
		}},
		{name: "mixed-target-false-middle", compares: []*etcdserverpb.Compare{
			allValueTrue, allVersionTrue, mixedVersionFalse, allCreateTrue, allModTrue, leaseATrue, leaseBTrue,
		}},
		{name: "mixed-target-false-last", compares: []*etcdserverpb.Compare{
			allValueTrue, allVersionTrue, allCreateTrue, allModTrue, leaseATrue, leaseBTrue, allLeaseFalse,
		}},
	}

	outcomes := make([]multiRangeCompareOutcome, 0, len(tests))
	for index, test := range tests {
		topMarker := []byte(fmt.Sprintf("%smarkers/%02d-top", prefix, index))
		topResponse, topErr := kv.Txn(ctx, branchTxn(test.compares, topMarker))
		require.NoError(t, topErr, test.name)

		nestedMarker := []byte(fmt.Sprintf("%smarkers/%02d-nested", prefix, index))
		nestedResponse, nestedErr := kv.Txn(ctx, &etcdserverpb.TxnRequest{Success: []*etcdserverpb.RequestOp{{
			Request: &etcdserverpb.RequestOp_RequestTxn{RequestTxn: branchTxn(test.compares, nestedMarker)},
		}}})
		require.NoError(t, nestedErr, test.name)
		require.Len(t, nestedResponse.Responses, 1, test.name)
		inner := nestedResponse.Responses[0].GetResponseTxn()
		require.NotNil(t, inner, test.name)

		outcomes = append(outcomes, multiRangeCompareOutcome{
			Name: test.name, TopSucceeded: topResponse.Succeeded,
			TopMarker:       readSingleValue(t, ctx, kv, topMarker, test.name+"-top"),
			NestedSucceeded: inner.Succeeded,
			NestedMarker:    readSingleValue(t, ctx, kv, nestedMarker, test.name+"-nested"),
			WantSucceeded:   test.succeeded,
		})
	}
	return outcomes
}
