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

type putAtMostOnceOutcome struct {
	Attempts             int
	AmbiguousTimeouts    int
	TrafficDiscarded     bool
	EveryAttemptVisible  bool
	EveryVersionExactOne bool
	FinalVersionDelta    int64
}

func TestPutAtMostOnceDifferentialAgainstReferenceEtcd(t *testing.T) {
	reference := os.Getenv("REFERENCE_ETCD_ENDPOINT")
	if reference == "" {
		t.Skip("set REFERENCE_ETCD_ENDPOINT to run Put at-most-once differential tests")
	}

	require.Equal(t,
		runPutAtMostOnceScenario(t, reference, "etcd"),
		runPutAtMostOnceScenario(t, compatEndpoint(), "kubebrain"),
	)
}

func runPutAtMostOnceScenario(t *testing.T, endpoint, instance string) putAtMostOnceOutcome {
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
	prefix := fmt.Sprintf("/dbaas-put-at-most-once/%s/%d/", instance, time.Now().UnixNano())
	key := prefix + "key"
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cleanupCancel()
		_, _ = direct.Delete(cleanupCtx, prefix, clientv3.WithPrefix())
	})

	_, err = direct.Put(ctx, key, "seed")
	require.NoError(t, err)
	seed, err := direct.Get(ctx, key)
	require.NoError(t, err)
	require.Len(t, seed.Kvs, 1)
	previousVersion := seed.Kvs[0].Version

	const attempts = 6
	ambiguousTimeouts := 0
	trafficDiscarded := true
	everyAttemptVisible := true
	everyVersionExactOne := true
	for attempt := 0; attempt < attempts; attempt++ {
		value := fmt.Sprintf("attempt-%d", attempt)
		warm, warmErr := throughBridge.Get(ctx, key)
		require.NoError(t, warmErr)
		require.Len(t, warm.Kvs, 1)
		droppedBefore := bridge.DroppedBytes()
		bridge.BlackholeResponses()
		callCtx, callCancel := context.WithTimeout(ctx, 750*time.Millisecond)
		_, putErr := throughBridge.Put(callCtx, key, value)
		callCancel()
		timedOut := errors.Is(putErr, context.DeadlineExceeded) ||
			status.Code(putErr) == codes.DeadlineExceeded
		require.True(t, timedOut, "attempt %d returned unexpected error: %v", attempt, putErr)
		ambiguousTimeouts++
		require.Eventually(t, func() bool {
			return bridge.DroppedBytes() > droppedBefore
		}, 2*time.Second, 10*time.Millisecond)
		trafficDiscarded = trafficDiscarded && bridge.DroppedBytes() > droppedBefore
		bridge.Unblackhole()
		bridge.DropConnections()

		var observedVersion int64
		visible := false
		require.Eventually(t, func() bool {
			response, getErr := direct.Get(ctx, key)
			if getErr != nil || len(response.Kvs) != 1 || string(response.Kvs[0].Value) != value {
				return false
			}
			observedVersion = response.Kvs[0].Version
			visible = true
			return true
		}, 5*time.Second, 20*time.Millisecond)
		everyAttemptVisible = everyAttemptVisible && visible
		exactOne := observedVersion == previousVersion+1
		require.True(t, exactOne, "attempt %d advanced version from %d to %d", attempt, previousVersion, observedVersion)
		everyVersionExactOne = everyVersionExactOne && exactOne
		previousVersion = observedVersion
	}

	return putAtMostOnceOutcome{
		Attempts:             attempts,
		AmbiguousTimeouts:    ambiguousTimeouts,
		TrafficDiscarded:     trafficDiscarded,
		EveryAttemptVisible:  everyAttemptVisible,
		EveryVersionExactOne: everyVersionExactOne,
		FinalVersionDelta:    previousVersion - seed.Kvs[0].Version,
	}
}
