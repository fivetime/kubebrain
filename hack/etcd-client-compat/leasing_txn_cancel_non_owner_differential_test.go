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
)

type leasingTxnCancelNonOwnerOutcome struct {
	Canceled            bool
	ValueAfterCancel    string
	TrafficDiscarded    bool
	TxnSucceeded        bool
	ResponseCount       int
	AllEventsAtTxnRev   bool
	OwnerCacheValues    []string
	DirectValues        []string
	FinalRevisionShared bool
}

func TestLeasingTxnCancelNonOwnerDifferentialAgainstReferenceEtcd(t *testing.T) {
	reference := os.Getenv("REFERENCE_ETCD_ENDPOINT")
	if reference == "" {
		t.Skip("set REFERENCE_ETCD_ENDPOINT to run leasing Txn cancel/non-owner differential tests")
	}

	require.Equal(t,
		runLeasingTxnCancelNonOwnerScenario(t, reference, "etcd"),
		runLeasingTxnCancelNonOwnerScenario(t, compatEndpoint(t), "kubebrain"),
	)
}

func runLeasingTxnCancelNonOwnerScenario(t *testing.T, endpoint, instance string) leasingTxnCancelNonOwnerOutcome {
	t.Helper()
	ownerBridge := newTCPBridge(t, endpoint)
	ownerClient, err := clientv3.New(clientv3.Config{
		Endpoints: []string{ownerBridge.Endpoint()}, DialTimeout: 3 * time.Second,
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, ownerClient.Close()) })
	writerClient, err := clientv3.New(clientv3.Config{
		Endpoints: []string{endpoint}, DialTimeout: 3 * time.Second,
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, writerClient.Close()) })

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	t.Cleanup(cancel)
	prefix := fmt.Sprintf("/dbaas-leasing-txn-cancel/%s/%d/", instance, time.Now().UnixNano())
	ownerPrefix := prefix + "owners/"
	keys := []string{prefix + "a", prefix + "b", prefix + "c"}
	ownerKV, closeOwner, err := leasing.NewKV(ownerClient, ownerPrefix)
	require.NoError(t, err)
	t.Cleanup(closeOwner)
	writerKV, closeWriter, err := leasing.NewKV(writerClient, ownerPrefix)
	require.NoError(t, err)
	t.Cleanup(closeWriter)
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cleanupCancel()
		_, _ = writerClient.Delete(cleanupCtx, prefix, clientv3.WithPrefix())
	})

	for index, key := range keys {
		_, err = writerClient.Put(ctx, key, fmt.Sprintf("initial-%d", index))
		require.NoError(t, err)
		cached, getErr := ownerKV.Get(ctx, key)
		require.NoError(t, getErr)
		require.Len(t, cached.Kvs, 1)
	}

	droppedBefore := ownerBridge.DroppedBytes()
	ownerBridge.Blackhole()
	cancelCtx, cancelTxn := context.WithCancel(ctx)
	cancelTimer := time.AfterFunc(250*time.Millisecond, cancelTxn)
	_, cancelErr := writerKV.Txn(cancelCtx).Then(clientv3.OpPut(keys[0], "must-not-commit")).Commit()
	cancelTimer.Stop()
	cancelTxn()
	require.True(t, errors.Is(cancelErr, context.Canceled), "unexpected canceled Txn error: %v", cancelErr)
	require.Eventually(t, func() bool {
		return ownerBridge.DroppedBytes() > droppedBefore
	}, 2*time.Second, 10*time.Millisecond)
	trafficDiscarded := ownerBridge.DroppedBytes() > droppedBefore
	afterCancel, err := writerClient.Get(ctx, keys[0])
	require.NoError(t, err)
	require.Len(t, afterCancel.Kvs, 1)
	require.Equal(t, "initial-0", string(afterCancel.Kvs[0].Value))

	ownerBridge.Unblackhole()
	require.Eventually(t, func() bool {
		response, getErr := ownerKV.Get(ctx, keys[0])
		return getErr == nil && len(response.Kvs) == 1 && string(response.Kvs[0].Value) == "initial-0"
	}, 5*time.Second, 20*time.Millisecond)

	watchCtx, watchCancel := context.WithTimeout(ctx, 5*time.Second)
	defer watchCancel()
	watch := writerClient.Watch(watchCtx, prefix, clientv3.WithPrefix())
	txn, err := writerKV.Txn(ctx).Then(
		clientv3.OpTxn(nil, []clientv3.Op{
			clientv3.OpPut(keys[1], "updated-1"),
		}, nil),
		clientv3.OpPut(keys[0], "updated-0"),
		clientv3.OpPut(keys[2], "updated-2"),
	).Commit()
	require.NoError(t, err)
	require.True(t, txn.Succeeded)
	require.Len(t, txn.Responses, 3)

	eventsAtRevision := 0
	for eventsAtRevision < len(keys) {
		select {
		case response := <-watch:
			require.NoError(t, response.Err())
			for _, event := range response.Events {
				if event.Kv.ModRevision == txn.Header.Revision {
					eventsAtRevision++
				}
			}
		case <-watchCtx.Done():
			t.Fatalf("timed out after %d/%d events at revision %d", eventsAtRevision, len(keys), txn.Header.Revision)
		}
	}

	ownerValues := make([]string, 0, len(keys))
	directValues := make([]string, 0, len(keys))
	var finalRevision int64
	finalRevisionShared := true
	for _, key := range keys {
		owner, getErr := ownerKV.Get(ctx, key)
		require.NoError(t, getErr)
		require.Len(t, owner.Kvs, 1)
		direct, getErr := writerClient.Get(ctx, key)
		require.NoError(t, getErr)
		require.Len(t, direct.Kvs, 1)
		ownerValues = append(ownerValues, string(owner.Kvs[0].Value))
		directValues = append(directValues, string(direct.Kvs[0].Value))
		if finalRevision == 0 {
			finalRevision = direct.Kvs[0].ModRevision
		}
		if owner.Kvs[0].ModRevision != txn.Header.Revision ||
			direct.Kvs[0].ModRevision != txn.Header.Revision {
			finalRevisionShared = false
		}
	}

	return leasingTxnCancelNonOwnerOutcome{
		Canceled:            errors.Is(cancelErr, context.Canceled),
		ValueAfterCancel:    string(afterCancel.Kvs[0].Value),
		TrafficDiscarded:    trafficDiscarded,
		TxnSucceeded:        txn.Succeeded,
		ResponseCount:       len(txn.Responses),
		AllEventsAtTxnRev:   eventsAtRevision == len(keys),
		OwnerCacheValues:    ownerValues,
		DirectValues:        directValues,
		FinalRevisionShared: finalRevisionShared && finalRevision == txn.Header.Revision,
	}
}
