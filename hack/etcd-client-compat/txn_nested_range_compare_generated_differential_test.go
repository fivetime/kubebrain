package compat

import (
	"bytes"
	"context"
	"fmt"
	"math"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"
)

type nestedRangeCompareOutcome struct {
	Name                      string
	NestedSucceeded           bool
	Marker                    string
	MarkerSharesOuterRevision bool
	InnerHeaderZero           bool
}

type nestedRangeCompareSpec struct {
	Target   etcdserverpb.Compare_CompareTarget
	Result   etcdserverpb.Compare_CompareResult
	AllMatch bool
}

func TestGeneratedTxnNestedRangeCompareDifferentialAgainstReferenceEtcd(t *testing.T) {
	reference := os.Getenv("REFERENCE_ETCD_ENDPOINT")
	if reference == "" {
		t.Skip("set REFERENCE_ETCD_ENDPOINT to run nested range-compare differential tests")
	}

	want := runGeneratedTxnNestedRangeCompareScenario(t, reference, "reference")
	for _, outcome := range want {
		require.Equal(t, map[bool]string{true: "success", false: "failure"}[outcome.NestedSucceeded], outcome.Marker)
		require.True(t, outcome.MarkerSharesOuterRevision, outcome.Name)
		require.True(t, outcome.InnerHeaderZero, outcome.Name)
	}
	require.Equal(t, want, runGeneratedTxnNestedRangeCompareScenario(t, compatEndpoint(t), "kubebrain"))
}

func TestGeneratedTxnNestedRangeCompareCoversTargetResultAndQuantificationMatrix(t *testing.T) {
	specs := generatedTxnNestedRangeCompareSpecs()
	require.Len(t, specs, 40)
	coverage := make(map[[3]int32]bool, len(specs))
	for _, spec := range specs {
		allMatch := int32(0)
		if spec.AllMatch {
			allMatch = 1
		}
		coverage[[3]int32{int32(spec.Target), int32(spec.Result), allMatch}] = true
	}
	require.Len(t, coverage, 40)
	for _, target := range []etcdserverpb.Compare_CompareTarget{
		etcdserverpb.Compare_VALUE, etcdserverpb.Compare_VERSION, etcdserverpb.Compare_CREATE,
		etcdserverpb.Compare_MOD, etcdserverpb.Compare_LEASE,
	} {
		for _, result := range []etcdserverpb.Compare_CompareResult{
			etcdserverpb.Compare_EQUAL, etcdserverpb.Compare_NOT_EQUAL,
			etcdserverpb.Compare_LESS, etcdserverpb.Compare_GREATER,
		} {
			for _, allMatch := range []int32{0, 1} {
				require.True(t, coverage[[3]int32{int32(target), int32(result), allMatch}])
			}
		}
	}
}

func generatedTxnNestedRangeCompareSpecs() []nestedRangeCompareSpec {
	targets := []etcdserverpb.Compare_CompareTarget{
		etcdserverpb.Compare_VALUE,
		etcdserverpb.Compare_VERSION,
		etcdserverpb.Compare_CREATE,
		etcdserverpb.Compare_MOD,
		etcdserverpb.Compare_LEASE,
	}
	results := []etcdserverpb.Compare_CompareResult{
		etcdserverpb.Compare_EQUAL,
		etcdserverpb.Compare_NOT_EQUAL,
		etcdserverpb.Compare_LESS,
		etcdserverpb.Compare_GREATER,
	}
	specs := make([]nestedRangeCompareSpec, 0, len(targets)*len(results)*2)
	for _, target := range targets {
		for _, result := range results {
			for _, allMatch := range []bool{true, false} {
				specs = append(specs, nestedRangeCompareSpec{
					Target: target, Result: result, AllMatch: allMatch,
				})
			}
		}
	}
	return specs
}

func runGeneratedTxnNestedRangeCompareScenario(
	t *testing.T,
	endpoint, instance string,
) []nestedRangeCompareOutcome {
	t.Helper()
	cli, err := clientv3.New(clientv3.Config{Endpoints: []string{endpoint}, DialTimeout: 3 * time.Second})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, cli.Close()) })
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	t.Cleanup(cancel)

	prefix := fmt.Sprintf("/a3727/txn-nested-range-compare/%s/%d/", instance, time.Now().UnixNano())
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cleanupCancel()
		_, _ = cli.Delete(cleanupCtx, prefix, clientv3.WithPrefix())
	})
	leaseA, err := cli.Grant(ctx, 300)
	require.NoError(t, err)
	leaseB, err := cli.Grant(ctx, 300)
	require.NoError(t, err)
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cleanupCancel()
		_, _ = cli.Revoke(cleanupCtx, leaseA.ID)
		_, _ = cli.Revoke(cleanupCtx, leaseB.ID)
	})
	lowLease, highLease := leaseA.ID, leaseB.ID
	if lowLease > highLease {
		lowLease, highLease = highLease, lowLease
	}
	require.Less(t, lowLease, highLease)

	specs := generatedTxnNestedRangeCompareSpecs()
	outcomes := make([]nestedRangeCompareOutcome, 0, len(specs))
	kv := etcdserverpb.NewKVClient(cli.ActiveConnection())
	for _, spec := range specs {
		name := fmt.Sprintf("%s-%s-%s", spec.Target, spec.Result, map[bool]string{true: "all", false: "mixed"}[spec.AllMatch])
		casePrefix := fmt.Sprintf("%s%s/", prefix, name)
		items := seedNestedRangeCompareFixture(
			t, ctx, cli, casePrefix, spec.Result, spec.AllMatch, lowLease, highLease,
		)
		compare := nestedRangeCompareForFixture(t, []byte(casePrefix+"data/"), items, spec.Target, spec.Result, spec.AllMatch)
		marker := []byte(casePrefix + "marker")
		before, beforeErr := cli.Get(ctx, casePrefix, clientv3.WithPrefix())
		require.NoError(t, beforeErr, name)

		nested := &etcdserverpb.TxnRequest{
			Compare: []*etcdserverpb.Compare{compare},
			Success: []*etcdserverpb.RequestOp{putRequestOp(marker, "success")},
			Failure: []*etcdserverpb.RequestOp{putRequestOp(marker, "failure")},
		}
		response, txnErr := kv.Txn(ctx, &etcdserverpb.TxnRequest{
			Success: []*etcdserverpb.RequestOp{{
				Request: &etcdserverpb.RequestOp_RequestTxn{RequestTxn: nested},
			}},
		})
		require.NoError(t, txnErr, name)
		require.True(t, response.Succeeded, name)
		require.Len(t, response.Responses, 1, name)
		inner := response.Responses[0].GetResponseTxn()
		require.NotNil(t, inner, name)
		require.Equal(t, spec.AllMatch, inner.Succeeded, name)
		require.Len(t, inner.Responses, 1, name)
		require.NotNil(t, inner.Responses[0].GetResponsePut(), name)

		markerResponse, markerErr := cli.Get(ctx, string(marker))
		require.NoError(t, markerErr, name)
		require.Len(t, markerResponse.Kvs, 1, name)
		outcomes = append(outcomes, nestedRangeCompareOutcome{
			Name: name, NestedSucceeded: inner.Succeeded, Marker: string(markerResponse.Kvs[0].Value),
			MarkerSharesOuterRevision: response.Header.Revision > before.Header.Revision &&
				response.Header.Revision == markerResponse.Kvs[0].ModRevision,
			InnerHeaderZero: inner.Header != nil && inner.Header.Revision == 0,
		})
	}
	require.Len(t, outcomes, 40)
	return outcomes
}

func seedNestedRangeCompareFixture(
	t *testing.T,
	ctx context.Context,
	cli *clientv3.Client,
	prefix string,
	result etcdserverpb.Compare_CompareResult,
	allMatch bool,
	lowLease, highLease clientv3.LeaseID,
) []*mvccpb.KeyValue {
	t.Helper()
	keys := []string{prefix + "data/a", prefix + "data/b", prefix + "data/c"}
	putTxn := func(lease clientv3.LeaseID, value string, selected ...int) {
		ops := make([]clientv3.Op, 0, len(selected))
		for _, index := range selected {
			ops = append(ops, clientv3.OpPut(keys[index], value, clientv3.WithLease(lease)))
		}
		response, err := cli.Txn(ctx).Then(ops...).Commit()
		require.NoError(t, err)
		require.True(t, response.Succeeded)
	}

	switch {
	case allMatch:
		putTxn(lowLease, "m", 0, 1, 2)
	case result == etcdserverpb.Compare_GREATER:
		// The third key is strictly lower for every target: value, version,
		// create/mod revision and lease ID.
		putTxn(lowLease, "a", 2)
		putTxn(highLease, "m", 0, 1)
		putTxn(highLease, "m", 0, 1)
	default:
		// The third key is strictly higher for every target.
		putTxn(lowLease, "m", 0, 1)
		putTxn(highLease, "z", 2)
		putTxn(highLease, "z", 2)
	}
	response, err := cli.Get(ctx, prefix+"data/", clientv3.WithPrefix())
	require.NoError(t, err)
	require.Len(t, response.Kvs, 3)
	return response.Kvs
}

func nestedRangeCompareForFixture(
	t *testing.T,
	key []byte,
	items []*mvccpb.KeyValue,
	target etcdserverpb.Compare_CompareTarget,
	result etcdserverpb.Compare_CompareResult,
	allMatch bool,
) *etcdserverpb.Compare {
	t.Helper()
	end := []byte(clientv3.GetPrefixRangeEnd(string(key)))
	if target == etcdserverpb.Compare_VALUE {
		value := items[0].Value
		switch result {
		case etcdserverpb.Compare_NOT_EQUAL:
			if allMatch {
				value = []byte("not-present")
			} else {
				value = items[2].Value
			}
		case etcdserverpb.Compare_LESS, etcdserverpb.Compare_GREATER:
			if allMatch {
				value = map[etcdserverpb.Compare_CompareResult][]byte{
					etcdserverpb.Compare_LESS:    []byte("z"),
					etcdserverpb.Compare_GREATER: []byte("a"),
				}[result]
			} else {
				value = items[2].Value
			}
		}
		matched := true
		for _, item := range items {
			matched = matched && compareNestedRangeBytes(item.Value, value, result)
		}
		require.Equal(t, allMatch, matched)
		return valueCompare(key, end, result, string(value))
	}

	values := make([]int64, len(items))
	for index, item := range items {
		switch target {
		case etcdserverpb.Compare_VERSION:
			values[index] = item.Version
		case etcdserverpb.Compare_CREATE:
			values[index] = item.CreateRevision
		case etcdserverpb.Compare_MOD:
			values[index] = item.ModRevision
		case etcdserverpb.Compare_LEASE:
			values[index] = item.Lease
		}
	}
	expected := values[0]
	switch result {
	case etcdserverpb.Compare_NOT_EQUAL:
		if allMatch {
			expected = distinctNestedRangeCompareInt(values[0])
		} else {
			expected = values[2]
		}
	case etcdserverpb.Compare_LESS:
		if allMatch {
			maximum := maxNestedRangeCompareInt(values)
			require.Less(t, maximum, int64(math.MaxInt64))
			expected = maximum + 1
		} else {
			expected = values[2]
		}
	case etcdserverpb.Compare_GREATER:
		if allMatch {
			minimum := minNestedRangeCompareInt(values)
			require.Greater(t, minimum, int64(math.MinInt64))
			expected = minimum - 1
		} else {
			expected = values[2]
		}
	}
	matched := true
	for _, value := range values {
		matched = matched && compareNestedRangeInts(value, expected, result)
	}
	require.Equal(t, allMatch, matched)
	return intCompare(key, end, target, result, expected)
}

func compareNestedRangeBytes(actual, expected []byte, result etcdserverpb.Compare_CompareResult) bool {
	comparison := bytes.Compare(actual, expected)
	return compareNestedRangeOrder(comparison, result)
}

func compareNestedRangeInts(actual, expected int64, result etcdserverpb.Compare_CompareResult) bool {
	comparison := 0
	if actual < expected {
		comparison = -1
	} else if actual > expected {
		comparison = 1
	}
	return compareNestedRangeOrder(comparison, result)
}

func compareNestedRangeOrder(comparison int, result etcdserverpb.Compare_CompareResult) bool {
	switch result {
	case etcdserverpb.Compare_EQUAL:
		return comparison == 0
	case etcdserverpb.Compare_NOT_EQUAL:
		return comparison != 0
	case etcdserverpb.Compare_LESS:
		return comparison < 0
	case etcdserverpb.Compare_GREATER:
		return comparison > 0
	default:
		return false
	}
}

func distinctNestedRangeCompareInt(value int64) int64 {
	if value == math.MaxInt64 {
		return value - 1
	}
	return value + 1
}

func minNestedRangeCompareInt(values []int64) int64 {
	minimum := values[0]
	for _, value := range values[1:] {
		minimum = min(minimum, value)
	}
	return minimum
}

func maxNestedRangeCompareInt(values []int64) int64 {
	maximum := values[0]
	for _, value := range values[1:] {
		maximum = max(maximum, value)
	}
	return maximum
}
