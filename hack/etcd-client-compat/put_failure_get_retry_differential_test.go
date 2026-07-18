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

type putFailureGetRetryOutcome struct {
	Attempts             int
	FailedPuts           int
	RequestBytesDropped  bool
	EveryDirectReadEmpty bool
	EveryRetryReadEmpty  bool
}

func TestPutFailureGetRetryDifferentialAgainstReferenceEtcd(t *testing.T) {
	reference := os.Getenv("REFERENCE_ETCD_ENDPOINT")
	if reference == "" {
		t.Skip("set REFERENCE_ETCD_ENDPOINT to run failed Put/Get retry differential tests")
	}

	require.Equal(t,
		runPutFailureGetRetryScenario(t, reference, "etcd"),
		runPutFailureGetRetryScenario(t, compatEndpoint(), "kubebrain"),
	)
}

func runPutFailureGetRetryScenario(t *testing.T, endpoint, instance string) putFailureGetRetryOutcome {
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
	prefix := fmt.Sprintf("/dbaas-put-failure-get-retry/%s/%d/", instance, time.Now().UnixNano())
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cleanupCancel()
		_, _ = direct.Delete(cleanupCtx, prefix, clientv3.WithPrefix())
	})

	const attempts = 4
	failedPuts := 0
	requestBytesDropped := true
	everyDirectReadEmpty := true
	everyRetryReadEmpty := true
	for attempt := 0; attempt < attempts; attempt++ {
		key := fmt.Sprintf("%skey-%d", prefix, attempt)
		warm, warmErr := throughBridge.Get(ctx, key)
		require.NoError(t, warmErr)
		require.Empty(t, warm.Kvs)

		droppedBefore := bridge.DroppedBytes()
		bridge.Blackhole()
		callCtx, callCancel := context.WithTimeout(ctx, 750*time.Millisecond)
		_, putErr := throughBridge.Put(callCtx, key, "must-not-commit")
		callCancel()
		timedOut := errors.Is(putErr, context.DeadlineExceeded) ||
			status.Code(putErr) == codes.DeadlineExceeded
		require.True(t, timedOut, "attempt %d returned unexpected error: %v", attempt, putErr)
		failedPuts++
		require.Eventually(t, func() bool {
			return bridge.DroppedBytes() > droppedBefore
		}, 2*time.Second, 10*time.Millisecond)
		requestBytesDropped = requestBytesDropped && bridge.DroppedBytes() > droppedBefore

		directRead, directErr := direct.Get(ctx, key)
		require.NoError(t, directErr)
		require.Empty(t, directRead.Kvs, "attempt %d committed despite its request being dropped", attempt)
		everyDirectReadEmpty = everyDirectReadEmpty && len(directRead.Kvs) == 0

		bridge.Unblackhole()
		retryCtx, retryCancel := context.WithTimeout(ctx, 5*time.Second)
		retryRead, retryErr := throughBridge.Get(retryCtx, key)
		retryCancel()
		require.NoError(t, retryErr, "attempt %d failed to reconnect for Get", attempt)
		require.Empty(t, retryRead.Kvs)
		everyRetryReadEmpty = everyRetryReadEmpty && len(retryRead.Kvs) == 0
	}

	return putFailureGetRetryOutcome{
		Attempts:             attempts,
		FailedPuts:           failedPuts,
		RequestBytesDropped:  requestBytesDropped,
		EveryDirectReadEmpty: everyDirectReadEmpty,
		EveryRetryReadEmpty:  everyRetryReadEmpty,
	}
}
