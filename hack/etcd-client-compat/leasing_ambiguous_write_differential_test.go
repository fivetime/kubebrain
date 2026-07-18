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

type leasingAmbiguousWriteOutcome struct {
	WriteApplied      bool
	ResponseDiscarded bool
	WriteTimedOut     bool
	LeasedValue       string
	DirectValue       string
	CacheConsistent   bool
}

func TestLeasingAmbiguousWriteDifferentialAgainstReferenceEtcd(t *testing.T) {
	reference := os.Getenv("REFERENCE_ETCD_ENDPOINT")
	if reference == "" {
		t.Skip("set REFERENCE_ETCD_ENDPOINT to run leasing ambiguous write differential tests")
	}

	require.Equal(t,
		runLeasingAmbiguousWriteScenario(t, reference, "etcd"),
		runLeasingAmbiguousWriteScenario(t, compatEndpoint(), "kubebrain"),
	)
}

func runLeasingAmbiguousWriteScenario(t *testing.T, endpoint, instance string) leasingAmbiguousWriteOutcome {
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

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	t.Cleanup(cancel)
	prefix := fmt.Sprintf("/dbaas-leasing-ambiguous/%s/%d/", instance, time.Now().UnixNano())
	key := prefix + "data"
	leased, closeLeased, err := leasing.NewKV(owner, prefix+"owners/")
	require.NoError(t, err)
	t.Cleanup(closeLeased)
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cleanupCancel()
		_, _ = observer.Delete(cleanupCtx, prefix, clientv3.WithPrefix())
	})

	_, err = leased.Put(ctx, key, "initial")
	require.NoError(t, err)
	_, err = leased.Get(ctx, key)
	require.NoError(t, err)

	bridge.BlackholeResponses()
	writeCtx, writeCancel := context.WithTimeout(ctx, 750*time.Millisecond)
	writeDone := make(chan error, 1)
	go func() {
		_, writeErr := leased.Put(writeCtx, key, "committed-without-response")
		writeDone <- writeErr
	}()

	writeApplied := false
	require.Eventually(t, func() bool {
		response, getErr := observer.Get(ctx, key)
		writeApplied = getErr == nil &&
			len(response.Kvs) == 1 &&
			string(response.Kvs[0].Value) == "committed-without-response"
		return writeApplied
	}, 5*time.Second, 10*time.Millisecond)
	require.Eventually(t, func() bool {
		return bridge.DroppedBytes() > 0
	}, 5*time.Second, 10*time.Millisecond)
	responseDiscarded := bridge.DroppedBytes() > 0
	writeErr := <-writeDone
	writeCancel()
	require.Error(t, writeErr)
	writeTimedOut := errors.Is(writeErr, context.DeadlineExceeded) ||
		status.Code(writeErr) == codes.DeadlineExceeded
	require.True(t, writeTimedOut, "unexpected ambiguous write error: %v", writeErr)

	bridge.Unblackhole()
	var leasedValue string
	var directValue string
	require.Eventually(t, func() bool {
		leasedResponse, leasedErr := leased.Get(ctx, key)
		directResponse, directErr := observer.Get(ctx, key)
		if leasedErr != nil || directErr != nil ||
			len(leasedResponse.Kvs) != 1 || len(directResponse.Kvs) != 1 {
			return false
		}
		leasedValue = string(leasedResponse.Kvs[0].Value)
		directValue = string(directResponse.Kvs[0].Value)
		return leasedValue == directValue && leasedValue == "committed-without-response"
	}, 10*time.Second, 20*time.Millisecond)

	return leasingAmbiguousWriteOutcome{
		WriteApplied:      writeApplied,
		ResponseDiscarded: responseDiscarded,
		WriteTimedOut:     writeTimedOut,
		LeasedValue:       leasedValue,
		DirectValue:       directValue,
		CacheConsistent:   leasedValue == directValue,
	}
}
