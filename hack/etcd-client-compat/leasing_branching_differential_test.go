package compat

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/leasing"
)

type leasingBranchingOutcome struct {
	OfflineComparisons     int
	OfflineResultsCorrect  bool
	TypedResponses         int
	NestedTreeKeys         int
	NestedSelectedKeys     int
	NestedSingleRevision   bool
	NestedCacheMatchesData bool
}

func TestLeasingBranchingDifferentialAgainstReferenceEtcd(t *testing.T) {
	reference := os.Getenv("REFERENCE_ETCD_ENDPOINT")
	if reference == "" {
		t.Skip("set REFERENCE_ETCD_ENDPOINT to run leasing branching differential tests")
	}

	require.Equal(t,
		runLeasingBranchingScenario(t, reference, "etcd"),
		runLeasingBranchingScenario(t, compatEndpoint(), "kubebrain"),
	)
}

func runLeasingBranchingScenario(t *testing.T, endpoint, instance string) leasingBranchingOutcome {
	t.Helper()
	bridge := newTCPBridge(t, endpoint)
	throughBridge, err := clientv3.New(clientv3.Config{
		Endpoints: []string{bridge.Endpoint()}, DialTimeout: 3 * time.Second,
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, throughBridge.Close()) })
	direct, err := clientv3.New(clientv3.Config{
		Endpoints: []string{endpoint}, DialTimeout: 3 * time.Second,
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, direct.Close()) })

	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	t.Cleanup(cancel)
	prefix := fmt.Sprintf("/dbaas-leasing-branching/%s/%d/", instance, time.Now().UnixNano())
	leased, closeLeased, err := leasing.NewKV(throughBridge, prefix+"owners/")
	require.NoError(t, err)
	t.Cleanup(closeLeased)
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cleanupCancel()
		_, _ = direct.Delete(cleanupCtx, prefix, clientv3.WithPrefix())
	})

	compareKey := prefix + "compare"
	_, err = direct.Put(ctx, compareKey, "abc")
	require.NoError(t, err)
	cached, err := leased.Get(ctx, compareKey)
	require.NoError(t, err)
	require.Len(t, cached.Kvs, 1)
	cachedKV := cached.Kvs[0]
	comparisons := []struct {
		compare clientv3.Cmp
		want    bool
	}{
		{clientv3.Compare(clientv3.Value(compareKey), "=", "abc"), true},
		{clientv3.Compare(clientv3.CreateRevision(compareKey), "=", cachedKV.CreateRevision), true},
		{clientv3.Compare(clientv3.ModRevision(compareKey), "=", cachedKV.ModRevision), true},
		{clientv3.Compare(clientv3.Version(compareKey), "=", cachedKV.Version), true},
		{clientv3.Compare(clientv3.Value(compareKey), ">", "abc"), false},
		{clientv3.Compare(clientv3.CreateRevision(compareKey), ">", cachedKV.CreateRevision), false},
		{clientv3.Compare(clientv3.ModRevision(compareKey), "<", cachedKV.ModRevision), false},
		{clientv3.Compare(clientv3.Version(compareKey), "<", cachedKV.Version), false},
	}
	bridge.Blackhole()
	offlineResultsCorrect := true
	for index, comparison := range comparisons {
		compareCtx, compareCancel := context.WithTimeout(ctx, time.Second)
		response, compareErr := leased.Txn(compareCtx).
			If(comparison.compare).
			Then(clientv3.OpGet(compareKey)).
			Commit()
		compareCancel()
		require.NoError(t, compareErr, "comparison %d", index)
		require.Equal(t, comparison.want, response.Succeeded, "comparison %d", index)
		expectedResponses := 0
		if comparison.want {
			expectedResponses = 1
		}
		require.Len(t, response.Responses, expectedResponses, "comparison %d", index)
		offlineResultsCorrect = offlineResultsCorrect &&
			response.Succeeded == comparison.want &&
			len(response.Responses) == expectedResponses
	}
	bridge.Unblackhole()

	typedResponses := 0
	typedKey := prefix + "typed/value"
	typedOps := []clientv3.Op{
		clientv3.OpTxn(nil, nil, nil),
		clientv3.OpGet(typedKey),
		clientv3.OpPut(typedKey, "typed"),
		clientv3.OpDelete(prefix+"typed/", clientv3.WithPrefix()),
		clientv3.OpTxn(nil, nil, nil),
	}
	for index, operation := range typedOps {
		response, doErr := leased.Do(ctx, operation)
		require.NoError(t, doErr, "typed operation %d", index)
		switch {
		case operation.IsTxn():
			require.NotNil(t, response.Txn(), "typed operation %d", index)
		case operation.IsGet():
			require.NotNil(t, response.Get(), "typed operation %d", index)
		case operation.IsPut():
			require.NotNil(t, response.Put(), "typed operation %d", index)
		case operation.IsDelete():
			require.NotNil(t, response.Del(), "typed operation %d", index)
		}
		typedResponses++
	}

	treePrefix := prefix + "tree/"
	next := 0
	expected := make(map[string]string)
	treeOperation := makeDeterministicLeasingTree(treePrefix, 3, &next, expected)
	require.Equal(t, 15, next)
	require.Len(t, expected, 4)
	for index := 0; index < next; index++ {
		key := fmt.Sprintf("%s%02d", treePrefix, index)
		_, err = direct.Put(ctx, key, "initial")
		require.NoError(t, err)
		_, err = leased.Get(ctx, key)
		require.NoError(t, err)
	}
	treeResponse, err := leased.Do(ctx, treeOperation)
	require.NoError(t, err)
	require.NotNil(t, treeResponse.Txn())
	treeRevision := treeResponse.Txn().Header.Revision
	require.Positive(t, treeRevision)

	nestedSingleRevision := true
	nestedCacheMatchesData := true
	for index := 0; index < next; index++ {
		key := fmt.Sprintf("%s%02d", treePrefix, index)
		leasedResponse, leasedErr := leased.Get(ctx, key)
		directResponse, directErr := direct.Get(ctx, key)
		require.NoError(t, leasedErr)
		require.NoError(t, directErr)
		require.Len(t, leasedResponse.Kvs, 1)
		require.Len(t, directResponse.Kvs, 1)
		caseMatches := leasingRangeResponsesEqual(leasedResponse, directResponse)
		require.True(t, caseMatches, "tree key %q differs", key)
		nestedCacheMatchesData = nestedCacheMatchesData && caseMatches
		expectedValue, selected := expected[key]
		if selected {
			require.Equal(t, expectedValue, string(directResponse.Kvs[0].Value))
			if directResponse.Kvs[0].ModRevision != treeRevision {
				nestedSingleRevision = false
			}
		} else {
			require.Equal(t, "initial", string(directResponse.Kvs[0].Value))
		}
	}

	return leasingBranchingOutcome{
		OfflineComparisons:     len(comparisons),
		OfflineResultsCorrect:  offlineResultsCorrect,
		TypedResponses:         typedResponses,
		NestedTreeKeys:         next,
		NestedSelectedKeys:     len(expected),
		NestedSingleRevision:   nestedSingleRevision,
		NestedCacheMatchesData: nestedCacheMatchesData,
	}
}

func makeDeterministicLeasingTree(
	prefix string,
	depth int,
	next *int,
	expected map[string]string,
) clientv3.Op {
	index := *next
	*next = *next + 1
	key := fmt.Sprintf("%s%02d", prefix, index)
	if depth == 0 {
		expected[key] = "leaf"
		return clientv3.OpPut(key, "leaf")
	}

	thenExpected := make(map[string]string)
	thenOperation := makeDeterministicLeasingTree(prefix, depth-1, next, thenExpected)
	elseExpected := make(map[string]string)
	elseOperation := makeDeterministicLeasingTree(prefix, depth-1, next, elseExpected)
	if index%2 == 0 {
		for selectedKey, value := range thenExpected {
			expected[selectedKey] = value
		}
		expected[key] = "then"
		return clientv3.OpTxn(
			[]clientv3.Cmp{clientv3.Compare(clientv3.Version(prefix+"missing"), "=", 0)},
			[]clientv3.Op{thenOperation, clientv3.OpPut(key, "then")},
			[]clientv3.Op{elseOperation, clientv3.OpPut(key, "else")},
		)
	}
	for selectedKey, value := range elseExpected {
		expected[selectedKey] = value
	}
	expected[key] = "else"
	return clientv3.OpTxn(
		[]clientv3.Cmp{clientv3.Compare(clientv3.Version(prefix+"missing"), ">", 0)},
		[]clientv3.Op{thenOperation, clientv3.OpPut(key, "then")},
		[]clientv3.Op{elseOperation, clientv3.OpPut(key, "else")},
	)
}
