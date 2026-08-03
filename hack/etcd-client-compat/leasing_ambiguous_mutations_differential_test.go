package compat

import (
	"context"
	"errors"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/leasing"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type leasingAmbiguousMutationsOutcome struct {
	Cases              int
	Applied            bool
	ResponsesDiscarded bool
	CallsTimedOut      bool
	CachesConsistent   bool
}

type leasingAmbiguousMutationCase struct {
	name     string
	expected string
	apply    func(context.Context, clientv3.KV, string, string) error
}

func TestLeasingAmbiguousMutationsDifferentialAgainstReferenceEtcd(t *testing.T) {
	reference := os.Getenv("REFERENCE_ETCD_ENDPOINT")
	if reference == "" {
		t.Skip("set REFERENCE_ETCD_ENDPOINT to run leasing ambiguous mutation differential tests")
	}

	require.Equal(t,
		runLeasingAmbiguousMutationsScenario(t, reference, "etcd"),
		runLeasingAmbiguousMutationsScenario(t, compatEndpoint(t), "kubebrain"),
	)
}

func runLeasingAmbiguousMutationsScenario(
	t *testing.T,
	endpoint string,
	instance string,
) leasingAmbiguousMutationsOutcome {
	t.Helper()
	bridge := newTCPBridge(t, endpoint)
	owner, err := clientv3.New(clientv3.Config{
		Endpoints: []string{bridge.Endpoint()}, DialTimeout: 3 * time.Second,
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, owner.Close()) })
	observer, err := clientv3.New(clientv3.Config{
		Endpoints: []string{endpoint}, DialTimeout: 3 * time.Second,
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, observer.Close()) })

	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	t.Cleanup(cancel)
	prefix := fmt.Sprintf("/dbaas-leasing-ambiguous-mutations/%s/%d/", instance, time.Now().UnixNano())
	leased, closeLeased, err := leasing.NewKV(owner, prefix+"owners/")
	require.NoError(t, err)
	t.Cleanup(closeLeased)
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cleanupCancel()
		_, _ = observer.Delete(cleanupCtx, prefix, clientv3.WithPrefix())
	})

	cases := []leasingAmbiguousMutationCase{
		{
			name: "delete",
			apply: func(callCtx context.Context, kv clientv3.KV, key, _ string) error {
				_, callErr := kv.Delete(callCtx, key)
				return callErr
			},
		},
		{
			name:     "txn-put",
			expected: "txn-put-applied",
			apply: func(callCtx context.Context, kv clientv3.KV, key, expected string) error {
				_, callErr := kv.Txn(callCtx).Then(
					clientv3.OpGet(key),
					clientv3.OpPut(key, expected),
				).Commit()
				return callErr
			},
		},
		{
			name: "txn-delete",
			apply: func(callCtx context.Context, kv clientv3.KV, key, _ string) error {
				_, callErr := kv.Txn(callCtx).Then(
					clientv3.OpGet(key),
					clientv3.OpDelete(key),
				).Commit()
				return callErr
			},
		},
		{
			name:     "do-put",
			expected: "do-put-applied",
			apply: func(callCtx context.Context, kv clientv3.KV, key, expected string) error {
				_, callErr := kv.Do(callCtx, clientv3.OpPut(key, expected))
				return callErr
			},
		},
		{
			name: "do-delete",
			apply: func(callCtx context.Context, kv clientv3.KV, key, _ string) error {
				_, callErr := kv.Do(callCtx, clientv3.OpDelete(key))
				return callErr
			},
		},
	}

	applied := true
	responsesDiscarded := true
	callsTimedOut := true
	cachesConsistent := true
	for index, testCase := range cases {
		key := fmt.Sprintf("%sdata/%02d", prefix, index)
		_, err = leased.Put(ctx, key, "initial")
		require.NoError(t, err)
		_, err = leased.Get(ctx, key)
		require.NoError(t, err)

		droppedBefore := bridge.DroppedBytes()
		bridge.BlackholeResponses()
		callCtx, callCancel := context.WithTimeout(ctx, 750*time.Millisecond)
		callDone := make(chan error, 1)
		go func() {
			callDone <- testCase.apply(callCtx, leased, key, testCase.expected)
		}()

		caseApplied := false
		require.Eventually(t, func() bool {
			response, getErr := observer.Get(ctx, key)
			if getErr != nil {
				return false
			}
			if testCase.expected == "" {
				caseApplied = len(response.Kvs) == 0
			} else {
				caseApplied = len(response.Kvs) == 1 &&
					string(response.Kvs[0].Value) == testCase.expected
			}
			return caseApplied
		}, 5*time.Second, 10*time.Millisecond, testCase.name)
		require.Eventually(t, func() bool {
			return bridge.DroppedBytes() > droppedBefore
		}, 5*time.Second, 10*time.Millisecond, testCase.name)
		caseResponseDiscarded := bridge.DroppedBytes() > droppedBefore
		callErr := <-callDone
		callCancel()
		caseTimedOut := errors.Is(callErr, context.DeadlineExceeded) ||
			status.Code(callErr) == codes.DeadlineExceeded
		require.True(t, caseTimedOut, "%s returned %v", testCase.name, callErr)
		bridge.Unblackhole()

		caseConsistent := false
		require.Eventually(t, func() bool {
			leasedResponse, leasedErr := leased.Get(ctx, key)
			directResponse, directErr := observer.Get(ctx, key)
			if leasedErr != nil || directErr != nil {
				return false
			}
			caseConsistent = leasingRangeResponsesEqual(leasedResponse, directResponse)
			return caseConsistent
		}, 10*time.Second, 20*time.Millisecond, testCase.name)

		applied = applied && caseApplied
		responsesDiscarded = responsesDiscarded && caseResponseDiscarded
		callsTimedOut = callsTimedOut && caseTimedOut
		cachesConsistent = cachesConsistent && caseConsistent
	}

	return leasingAmbiguousMutationsOutcome{
		Cases:              len(cases),
		Applied:            applied,
		ResponsesDiscarded: responsesDiscarded,
		CallsTimedOut:      callsTimedOut,
		CachesConsistent:   cachesConsistent,
	}
}

func leasingRangeResponsesEqual(left, right *clientv3.GetResponse) bool {
	if len(left.Kvs) != len(right.Kvs) {
		return false
	}
	for index := range left.Kvs {
		leftKV := left.Kvs[index]
		rightKV := right.Kvs[index]
		if string(leftKV.Key) != string(rightKV.Key) ||
			string(leftKV.Value) != string(rightKV.Value) ||
			leftKV.CreateRevision != rightKV.CreateRevision ||
			leftKV.ModRevision != rightKV.ModRevision ||
			leftKV.Version != rightKV.Version ||
			leftKV.Lease != rightKV.Lease {
			return false
		}
	}
	return true
}
