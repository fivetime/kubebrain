package compat

import (
	"context"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/leasing"
)

type leasingPutGetDeleteConcurrencyOutcome struct {
	Clients            int
	Workers            int
	CompletedSequences int64
	LeasingFinalEmpty  bool
	DirectFinalEmpty   bool
}

func TestLeasingPutGetDeleteConcurrencyDifferentialAgainstReferenceEtcd(t *testing.T) {
	reference := os.Getenv("REFERENCE_ETCD_ENDPOINT")
	if reference == "" {
		t.Skip("set REFERENCE_ETCD_ENDPOINT to run leasing Put/Get/Delete concurrency differential tests")
	}

	require.Equal(t,
		runLeasingPutGetDeleteConcurrencyScenario(t, reference, "etcd"),
		runLeasingPutGetDeleteConcurrencyScenario(t, compatEndpoint(), "kubebrain"),
	)
}

func runLeasingPutGetDeleteConcurrencyScenario(
	t *testing.T,
	endpoint string,
	instance string,
) leasingPutGetDeleteConcurrencyOutcome {
	t.Helper()
	client, err := clientv3.New(clientv3.Config{
		Endpoints: []string{endpoint}, DialTimeout: 3 * time.Second,
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, client.Close()) })

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	t.Cleanup(cancel)
	prefix := fmt.Sprintf("/dbaas-leasing-put-get-delete/%s/%d/", instance, time.Now().UnixNano())
	key := prefix + "data"
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cleanupCancel()
		_, _ = client.Delete(cleanupCtx, prefix, clientv3.WithPrefix())
	})

	const (
		clients = 16
		workers = 16
	)
	leased := make([]clientv3.KV, clients)
	for index := range leased {
		kv, closeKV, newErr := leasing.NewKV(client, prefix+"owners/")
		require.NoError(t, newErr)
		t.Cleanup(closeKV)
		leased[index] = kv
	}

	start := make(chan struct{})
	errors := make(chan error, workers)
	var completed atomic.Int64
	var wait sync.WaitGroup
	for worker := 0; worker < workers; worker++ {
		wait.Add(1)
		go func(worker int) {
			defer wait.Done()
			<-start
			for clientIndex, kv := range leased {
				value := fmt.Sprintf("worker-%02d-client-%02d", worker, clientIndex)
				if _, putErr := kv.Put(ctx, key, value); putErr != nil {
					errors <- putErr
					return
				}
				time.Sleep(time.Millisecond)
				if _, getErr := kv.Get(ctx, key); getErr != nil {
					errors <- getErr
					return
				}
				if _, deleteErr := kv.Delete(ctx, key); deleteErr != nil {
					errors <- deleteErr
					return
				}
				time.Sleep(2 * time.Millisecond)
				completed.Add(1)
			}
		}(worker)
	}
	close(start)
	wait.Wait()
	close(errors)
	for operationErr := range errors {
		require.NoError(t, operationErr)
	}
	require.Equal(t, int64(clients*workers), completed.Load())

	leasedFinal, err := leased[0].Get(ctx, key)
	require.NoError(t, err)
	directFinal, err := client.Get(ctx, key)
	require.NoError(t, err)
	require.Empty(t, leasedFinal.Kvs)
	require.Empty(t, directFinal.Kvs)

	return leasingPutGetDeleteConcurrencyOutcome{
		Clients:            clients,
		Workers:            workers,
		CompletedSequences: completed.Load(),
		LeasingFinalEmpty:  len(leasedFinal.Kvs) == 0,
		DirectFinalEmpty:   len(directFinal.Kvs) == 0,
	}
}
