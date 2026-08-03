package compat

import (
	"context"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/leasing"
)

type leasingClientOutcome struct {
	InitialMissing     bool
	RemoteValue        string
	InvalidatedValue   string
	PreviousValue      string
	HistoricalValue    string
	CurrentIsWorker    bool
	ConcurrentVersion  int64
	ConcurrentAtMaxRev bool
	Deleted            bool
}

func TestLeasingClientDifferentialAgainstReferenceEtcd(t *testing.T) {
	reference := os.Getenv("REFERENCE_ETCD_ENDPOINT")
	if reference == "" {
		t.Skip("set REFERENCE_ETCD_ENDPOINT to run leasing client differential tests")
	}

	require.Equal(t,
		runLeasingClientScenario(t, reference, "etcd"),
		runLeasingClientScenario(t, compatEndpoint(t), "kubebrain"),
	)
}

func runLeasingClientScenario(t *testing.T, endpoint, instance string) leasingClientOutcome {
	t.Helper()
	first, err := clientv3.New(clientv3.Config{
		Endpoints: []string{endpoint}, DialTimeout: 3 * time.Second,
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, first.Close()) })
	second, err := clientv3.New(clientv3.Config{
		Endpoints: []string{endpoint}, DialTimeout: 3 * time.Second,
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, second.Close()) })

	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	t.Cleanup(cancel)
	prefix := fmt.Sprintf("/dbaas-leasing-client/%s/%d/", instance, time.Now().UnixNano())
	key := prefix + "data"
	firstKV, closeFirst, err := leasing.NewKV(first, prefix+"owners/")
	require.NoError(t, err)
	t.Cleanup(closeFirst)
	secondKV, closeSecond, err := leasing.NewKV(second, prefix+"owners/")
	require.NoError(t, err)
	t.Cleanup(closeSecond)
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cleanupCancel()
		_, _ = first.Delete(cleanupCtx, prefix, clientv3.WithPrefix())
	})

	missing, err := firstKV.Get(ctx, key)
	require.NoError(t, err)
	_, err = firstKV.Put(ctx, key, "one")
	require.NoError(t, err)
	remote, err := secondKV.Get(ctx, key)
	require.NoError(t, err)
	require.Len(t, remote.Kvs, 1)

	_, err = firstKV.Get(ctx, key)
	require.NoError(t, err)
	_, err = secondKV.Put(ctx, key, "two")
	require.NoError(t, err)
	invalidated, err := firstKV.Get(ctx, key)
	require.NoError(t, err)
	require.Len(t, invalidated.Kvs, 1)

	previous, err := firstKV.Put(ctx, key, "three", clientv3.WithPrevKV())
	require.NoError(t, err)
	require.NotNil(t, previous.PrevKv)
	historical, err := firstKV.Get(ctx, key, clientv3.WithRev(previous.PrevKv.ModRevision))
	require.NoError(t, err)
	require.Len(t, historical.Kvs, 1)

	const workers = 8
	contentionCtx, contentionCancel := context.WithTimeout(ctx, 5*time.Second)
	defer contentionCancel()
	responses := make(chan *clientv3.PutResponse, workers)
	errors := make(chan error, workers)
	var wait sync.WaitGroup
	for worker := 0; worker < workers; worker++ {
		wait.Add(1)
		go func(worker int) {
			defer wait.Done()
			response, putErr := firstKV.Put(contentionCtx, key, fmt.Sprintf("worker-%d", worker))
			responses <- response
			errors <- putErr
		}(worker)
	}
	wait.Wait()
	close(responses)
	close(errors)
	for putErr := range errors {
		require.NoError(t, putErr)
	}
	var maxRevision int64
	for response := range responses {
		require.NotNil(t, response)
		if response.Header.Revision > maxRevision {
			maxRevision = response.Header.Revision
		}
	}
	current, err := firstKV.Get(ctx, key)
	require.NoError(t, err)
	require.Len(t, current.Kvs, 1)
	_, err = secondKV.Delete(ctx, key)
	require.NoError(t, err)
	deleted, err := firstKV.Get(ctx, key)
	require.NoError(t, err)

	return leasingClientOutcome{
		InitialMissing:     len(missing.Kvs) == 0,
		RemoteValue:        string(remote.Kvs[0].Value),
		InvalidatedValue:   string(invalidated.Kvs[0].Value),
		PreviousValue:      string(previous.PrevKv.Value),
		HistoricalValue:    string(historical.Kvs[0].Value),
		CurrentIsWorker:    len(current.Kvs[0].Value) > 7 && string(current.Kvs[0].Value[:7]) == "worker-",
		ConcurrentVersion:  current.Kvs[0].Version,
		ConcurrentAtMaxRev: current.Kvs[0].ModRevision == maxRevision,
		Deleted:            len(deleted.Kvs) == 0,
	}
}
