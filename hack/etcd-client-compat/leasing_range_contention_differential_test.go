package compat

import (
	"context"
	"fmt"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/leasing"
)

type leasingRangeContentionOutcome struct {
	BoundsPreserved        bool
	BoundOwnersPreserved   bool
	ContentionModes        int
	WritersMadeProgress    bool
	DeleteResponsesTyped   bool
	ContendedCachesMatched bool
}

func TestLeasingRangeContentionDifferentialAgainstReferenceEtcd(t *testing.T) {
	reference := os.Getenv("REFERENCE_ETCD_ENDPOINT")
	if reference == "" {
		t.Skip("set REFERENCE_ETCD_ENDPOINT to run leasing range contention differential tests")
	}

	require.Equal(t,
		runLeasingRangeContentionScenario(t, reference, "etcd"),
		runLeasingRangeContentionScenario(t, compatEndpoint(), "kubebrain"),
	)
}

func runLeasingRangeContentionScenario(
	t *testing.T,
	endpoint string,
	instance string,
) leasingRangeContentionOutcome {
	t.Helper()
	client, err := clientv3.New(clientv3.Config{
		Endpoints: []string{endpoint}, DialTimeout: 3 * time.Second,
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, client.Close()) })

	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	t.Cleanup(cancel)
	prefix := fmt.Sprintf("/dbaas-leasing-range-contention/%s/%d/", instance, time.Now().UnixNano())
	ownerPrefix := prefix + "owners/"
	boundsDeleter, closeBoundsDeleter, err := leasing.NewKV(client, ownerPrefix)
	require.NoError(t, err)
	t.Cleanup(closeBoundsDeleter)
	boundsReader, closeBoundsReader, err := leasing.NewKV(client, ownerPrefix)
	require.NoError(t, err)
	t.Cleanup(closeBoundsReader)
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cleanupCancel()
		_, _ = client.Delete(cleanupCtx, prefix, clientv3.WithPrefix())
	})

	boundKeys := []string{prefix + "j", prefix + "m"}
	for _, key := range boundKeys {
		_, err = client.Put(ctx, key, "bound")
		require.NoError(t, err)
		_, err = boundsReader.Get(ctx, key)
		require.NoError(t, err)
	}
	_, err = boundsDeleter.Delete(ctx, prefix+"k", clientv3.WithPrefix())
	require.NoError(t, err)
	boundsPreserved := true
	boundOwnersPreserved := true
	for _, key := range boundKeys {
		response, getErr := boundsReader.Get(ctx, key)
		require.NoError(t, getErr)
		require.Len(t, response.Kvs, 1)
		boundsPreserved = boundsPreserved && string(response.Kvs[0].Value) == "bound"
		owners, ownerErr := client.Get(ctx, ownerPrefix+key, clientv3.WithPrefix())
		require.NoError(t, ownerErr)
		require.Len(t, owners.Kvs, 1)
		boundOwnersPreserved = boundOwnersPreserved && len(owners.Kvs) == 1
	}

	modes := []struct {
		name string
		op   func(string) clientv3.Op
	}{
		{
			name: "delete",
			op: func(dataPrefix string) clientv3.Op {
				return clientv3.OpDelete(dataPrefix, clientv3.WithPrefix())
			},
		},
		{
			name: "nested-txn-delete",
			op: func(dataPrefix string) clientv3.Op {
				return clientv3.OpTxn(
					nil,
					[]clientv3.Op{clientv3.OpDelete(dataPrefix, clientv3.WithPrefix())},
					nil,
				)
			},
		},
	}

	writersMadeProgress := true
	deleteResponsesTyped := true
	contendedCachesMatched := true
	for modeIndex, mode := range modes {
		modePrefix := fmt.Sprintf("%scontend/%d/", prefix, modeIndex)
		deleter, closeDeleter, newErr := leasing.NewKV(client, ownerPrefix)
		require.NoError(t, newErr)
		defer closeDeleter()
		writer, closeWriter, newErr := leasing.NewKV(client, ownerPrefix)
		require.NoError(t, newErr)
		defer closeWriter()

		const keys = 8
		for index := 0; index < keys; index++ {
			key := fmt.Sprintf("%s%02d", modePrefix, index)
			_, err = client.Put(ctx, key, "initial")
			require.NoError(t, err)
			_, err = writer.Get(ctx, key)
			require.NoError(t, err)
		}

		stopWriter := make(chan struct{})
		started := make(chan struct{})
		writerDone := make(chan error, 1)
		var writerOperations atomic.Int64
		go func() {
			close(started)
			for iteration := 0; ; iteration++ {
				select {
				case <-stopWriter:
					writerDone <- nil
					return
				default:
				}
				key := fmt.Sprintf("%s%02d", modePrefix, iteration%keys)
				if _, putErr := writer.Put(ctx, key, fmt.Sprintf("writer-%d", iteration)); putErr != nil {
					writerDone <- putErr
					return
				}
				if _, getErr := writer.Get(ctx, key); getErr != nil {
					writerDone <- getErr
					return
				}
				writerOperations.Add(1)
			}
		}()
		<-started
		require.Eventually(t, func() bool {
			return writerOperations.Load() > 0
		}, 5*time.Second, time.Millisecond, mode.name)

		deleteResponse, deleteErr := deleter.Do(ctx, mode.op(modePrefix))
		require.NoError(t, deleteErr, mode.name)
		close(stopWriter)
		require.NoError(t, <-writerDone, mode.name)
		modeProgress := writerOperations.Load() > 0
		writersMadeProgress = writersMadeProgress && modeProgress
		if mode.name == "delete" {
			require.NotNil(t, deleteResponse.Del())
			deleteResponsesTyped = deleteResponsesTyped && deleteResponse.Del() != nil
		} else {
			require.NotNil(t, deleteResponse.Txn())
			deleteResponsesTyped = deleteResponsesTyped && deleteResponse.Txn() != nil
		}

		for index := 0; index < keys; index++ {
			key := fmt.Sprintf("%s%02d", modePrefix, index)
			cached, cachedErr := writer.Get(ctx, key)
			direct, directErr := client.Get(ctx, key)
			require.NoError(t, cachedErr)
			require.NoError(t, directErr)
			matches := leasingRangeResponsesEqual(cached, direct)
			require.True(t, matches, "%s key %q differs", mode.name, key)
			contendedCachesMatched = contendedCachesMatched && matches
		}
	}

	return leasingRangeContentionOutcome{
		BoundsPreserved:        boundsPreserved,
		BoundOwnersPreserved:   boundOwnersPreserved,
		ContentionModes:        len(modes),
		WritersMadeProgress:    writersMadeProgress,
		DeleteResponsesTyped:   deleteResponsesTyped,
		ContendedCachesMatched: contendedCachesMatched,
	}
}
