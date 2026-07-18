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
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type txnAtMostOnceOutcome struct {
	Attempts               int
	AmbiguousTimeouts      int
	TrafficDiscarded       bool
	EveryTxnVisible        bool
	EveryVersionExactOne   bool
	EveryTxnSingleRevision bool
	FinalVersionDelta      int64
}

func TestTxnAtMostOnceDifferentialAgainstReferenceEtcd(t *testing.T) {
	reference := os.Getenv("REFERENCE_ETCD_ENDPOINT")
	if reference == "" {
		t.Skip("set REFERENCE_ETCD_ENDPOINT to run Txn at-most-once differential tests")
	}

	require.Equal(t,
		runTxnAtMostOnceScenario(t, reference, "etcd"),
		runTxnAtMostOnceScenario(t, compatEndpoint(), "kubebrain"),
	)
}

func runTxnAtMostOnceScenario(t *testing.T, endpoint, instance string) txnAtMostOnceOutcome {
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

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	t.Cleanup(cancel)
	prefix := fmt.Sprintf("/dbaas-txn-at-most-once/%s/%d/", instance, time.Now().UnixNano())
	keys := []string{prefix + "a", prefix + "b", prefix + "c"}
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cleanupCancel()
		_, _ = direct.Delete(cleanupCtx, prefix, clientv3.WithPrefix())
	})

	seed, err := direct.Txn(ctx).Then(
		clientv3.OpPut(keys[0], "seed"),
		clientv3.OpPut(keys[1], "seed"),
		clientv3.OpPut(keys[2], "seed"),
	).Commit()
	require.NoError(t, err)
	require.True(t, seed.Succeeded)
	previousVersions := []int64{1, 1, 1}

	const attempts = 4
	ambiguousTimeouts := 0
	trafficDiscarded := true
	everyTxnVisible := true
	everyVersionExactOne := true
	everyTxnSingleRevision := true
	for attempt := 0; attempt < attempts; attempt++ {
		warm, warmErr := throughBridge.Get(ctx, prefix, clientv3.WithPrefix())
		require.NoError(t, warmErr)
		require.Len(t, warm.Kvs, len(keys))

		value := fmt.Sprintf("attempt-%d", attempt)
		droppedBefore := bridge.DroppedBytes()
		bridge.BlackholeResponses()
		callCtx, callCancel := context.WithTimeout(ctx, 750*time.Millisecond)
		_, txnErr := throughBridge.Txn(callCtx).Then(
			clientv3.OpTxn(nil, []clientv3.Op{
				clientv3.OpPut(keys[0], value),
				clientv3.OpPut(keys[1], value),
			}, nil),
			clientv3.OpPut(keys[2], value),
		).Commit()
		callCancel()
		timedOut := errors.Is(txnErr, context.DeadlineExceeded) ||
			status.Code(txnErr) == codes.DeadlineExceeded
		require.True(t, timedOut, "attempt %d returned unexpected error: %v", attempt, txnErr)
		ambiguousTimeouts++
		require.Eventually(t, func() bool {
			return bridge.DroppedBytes() > droppedBefore
		}, 2*time.Second, 10*time.Millisecond)
		trafficDiscarded = trafficDiscarded && bridge.DroppedBytes() > droppedBefore
		bridge.Unblackhole()
		bridge.DropConnections()

		var observed []*clientv3.GetResponse
		visible := false
		require.Eventually(t, func() bool {
			current := make([]*clientv3.GetResponse, 0, len(keys))
			for _, key := range keys {
				response, getErr := direct.Get(ctx, key)
				if getErr != nil || len(response.Kvs) != 1 || string(response.Kvs[0].Value) != value {
					return false
				}
				current = append(current, response)
			}
			observed = current
			visible = true
			return true
		}, 5*time.Second, 20*time.Millisecond)
		everyTxnVisible = everyTxnVisible && visible

		revision := observed[0].Kvs[0].ModRevision
		singleRevision := true
		for index, response := range observed {
			version := response.Kvs[0].Version
			exactOne := version == previousVersions[index]+1
			require.True(t, exactOne,
				"attempt %d key %d advanced version from %d to %d",
				attempt, index, previousVersions[index], version,
			)
			everyVersionExactOne = everyVersionExactOne && exactOne
			previousVersions[index] = version
			singleRevision = singleRevision && response.Kvs[0].ModRevision == revision
		}
		require.True(t, singleRevision, "attempt %d committed keys at different revisions", attempt)
		everyTxnSingleRevision = everyTxnSingleRevision && singleRevision
	}

	return txnAtMostOnceOutcome{
		Attempts:               attempts,
		AmbiguousTimeouts:      ambiguousTimeouts,
		TrafficDiscarded:       trafficDiscarded,
		EveryTxnVisible:        everyTxnVisible,
		EveryVersionExactOne:   everyVersionExactOne,
		EveryTxnSingleRevision: everyTxnSingleRevision,
		FinalVersionDelta:      previousVersions[0] - 1,
	}
}
