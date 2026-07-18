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

type leasingReconnectOperationsOutcome struct {
	TxnSucceeded       bool
	TxnReadMissing     bool
	GetsCompleted      int
	ExistingKeys       int
	MissingKeys        int
	CacheMatchesDirect bool
}

func TestLeasingReconnectOperationsDifferentialAgainstReferenceEtcd(t *testing.T) {
	reference := os.Getenv("REFERENCE_ETCD_ENDPOINT")
	if reference == "" {
		t.Skip("set REFERENCE_ETCD_ENDPOINT to run leasing reconnect operation differential tests")
	}

	require.Equal(t,
		runLeasingReconnectOperationsScenario(t, reference, "etcd"),
		runLeasingReconnectOperationsScenario(t, compatEndpoint(), "kubebrain"),
	)
}

func runLeasingReconnectOperationsScenario(
	t *testing.T,
	endpoint string,
	instance string,
) leasingReconnectOperationsOutcome {
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
	prefix := fmt.Sprintf("/dbaas-leasing-reconnect-operations/%s/%d/", instance, time.Now().UnixNano())
	dataPrefix := prefix + "data/"
	leased, closeLeased, err := leasing.NewKV(throughBridge, prefix+"owners/")
	require.NoError(t, err)
	t.Cleanup(closeLeased)
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cleanupCancel()
		_, _ = direct.Delete(cleanupCtx, prefix, clientv3.WithPrefix())
	})

	missingKey := dataPrefix + "txn-missing"
	_, err = leased.Get(ctx, missingKey)
	require.NoError(t, err)
	txnCtx, txnCancel := context.WithTimeout(ctx, 5*time.Second)
	txnDropsBefore := bridge.DroppedConnections()
	txnChurn := churnTCPBridge(bridge, 5, 10*time.Millisecond)
	<-txnChurn.started
	txn, err := leased.Txn(txnCtx).
		If(clientv3.Compare(clientv3.Version(missingKey), "=", 0)).
		Then(clientv3.OpGet(missingKey)).
		Commit()
	txnCancel()
	<-txnChurn.done
	require.Greater(t, bridge.DroppedConnections(), txnDropsBefore)
	require.NoError(t, err)
	require.True(t, txn.Succeeded)
	require.Len(t, txn.Responses, 1)
	require.Empty(t, txn.Responses[0].GetResponseRange().Kvs)

	const keys = 10
	for index := 0; index < keys; index += 2 {
		key := fmt.Sprintf("%s%02d", dataPrefix, index)
		_, err = leased.Put(ctx, key, fmt.Sprintf("value-%02d", index))
		require.NoError(t, err)
	}

	getsCompleted := 0
	existingKeys := 0
	missingKeys := 0
	cacheMatchesDirect := true
	for index := 0; index < keys; index++ {
		key := fmt.Sprintf("%s%02d", dataPrefix, index)
		getCtx, getCancel := context.WithTimeout(ctx, 5*time.Second)
		getDropsBefore := bridge.DroppedConnections()
		getChurn := churnTCPBridge(bridge, 3, 10*time.Millisecond)
		<-getChurn.started
		leasedResponse, leasedErr := leased.Get(getCtx, key)
		getCancel()
		<-getChurn.done
		require.Greater(t, bridge.DroppedConnections(), getDropsBefore)
		require.NoError(t, leasedErr)
		directResponse, directErr := direct.Get(ctx, key)
		require.NoError(t, directErr)
		caseMatches := leasingRangeResponsesEqual(leasedResponse, directResponse)
		require.True(t, caseMatches, "key %q differs after reconnect", key)
		cacheMatchesDirect = cacheMatchesDirect && caseMatches
		getsCompleted++
		if len(leasedResponse.Kvs) == 0 {
			missingKeys++
		} else {
			existingKeys++
		}
	}

	return leasingReconnectOperationsOutcome{
		TxnSucceeded:       txn.Succeeded,
		TxnReadMissing:     len(txn.Responses[0].GetResponseRange().Kvs) == 0,
		GetsCompleted:      getsCompleted,
		ExistingKeys:       existingKeys,
		MissingKeys:        missingKeys,
		CacheMatchesDirect: cacheMatchesDirect,
	}
}

type tcpBridgeChurn struct {
	started <-chan struct{}
	done    <-chan struct{}
}

func churnTCPBridge(bridge *tcpBridge, drops int, interval time.Duration) tcpBridgeChurn {
	started := make(chan struct{})
	done := make(chan struct{})
	go func() {
		defer close(done)
		for drop := 0; drop < drops; drop++ {
			bridge.DropConnections()
			if drop == 0 {
				close(started)
			}
			time.Sleep(interval)
		}
	}()
	return tcpBridgeChurn{started: started, done: done}
}
