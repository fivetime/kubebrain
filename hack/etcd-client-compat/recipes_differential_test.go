package compat

import (
	"context"
	"fmt"
	"os"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	clientv3 "go.etcd.io/etcd/client/v3"
	recipe "go.etcd.io/etcd/client/v3/experimental/recipes"
)

type recipesOutcome struct {
	BarrierDoubleHoldRejected   bool
	BarrierBlockedBeforeRelease bool
	BarrierWaitersReleased      int
	NeighborWaitImmediate       bool
	FIFOValues                  []string
	PriorityValues              []string
	ConcurrentValues            []string
}

func TestRecipesDifferentialAgainstReferenceEtcd(t *testing.T) {
	reference := os.Getenv("REFERENCE_ETCD_ENDPOINT")
	if reference == "" {
		t.Skip("set REFERENCE_ETCD_ENDPOINT to run recipes differential tests")
	}

	referenceOutcome := runRecipesScenario(t, reference, "etcd")
	kubeBrainOutcome := runRecipesScenario(t, compatEndpoint(t), "kubebrain")
	require.Equal(t, referenceOutcome, kubeBrainOutcome)
	require.Equal(t, recipesOutcome{
		BarrierDoubleHoldRejected:   true,
		BarrierBlockedBeforeRelease: true,
		BarrierWaitersReleased:      5,
		NeighborWaitImmediate:       true,
		FIFOValues:                  []string{"zero", "one", "two", "three", "four"},
		PriorityValues:              []string{"zero-a", "zero-b", "one", "two-a", "two-b"},
		ConcurrentValues: []string{
			"writer-0-item-0", "writer-0-item-1", "writer-0-item-2",
			"writer-1-item-0", "writer-1-item-1", "writer-1-item-2",
			"writer-2-item-0", "writer-2-item-1", "writer-2-item-2",
		},
	}, kubeBrainOutcome)
}

func runRecipesScenario(t *testing.T, endpoint, instance string) recipesOutcome {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	client, err := clientv3.New(clientv3.Config{
		Endpoints: []string{endpoint}, DialTimeout: 3 * time.Second, Context: ctx,
	})
	require.NoError(t, err)
	defer func() { require.NoError(t, client.Close()) }()

	base := fmt.Sprintf("/dbaas-recipes/%s/%d/", instance, time.Now().UnixNano())
	defer func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cleanupCancel()
		_, _ = client.Delete(cleanupCtx, base, clientv3.WithPrefix())
	}()

	barrierName := base + "barrier"
	barrier := recipe.NewBarrier(client, barrierName)
	require.NoError(t, barrier.Hold())
	doubleHoldRejected := barrier.Hold() != nil

	const waiters = 5
	waitResults := make(chan error, waiters)
	for range waiters {
		go func() { waitResults <- recipe.NewBarrier(client, barrierName).Wait() }()
	}
	time.Sleep(150 * time.Millisecond)
	blocked := len(waitResults) == 0
	require.NoError(t, barrier.Release())
	released := 0
	for range waiters {
		select {
		case waitErr := <-waitResults:
			require.NoError(t, waitErr)
			released++
		case <-ctx.Done():
			t.Fatal("barrier waiters did not release")
		}
	}

	_, err = client.Put(ctx, base+"nonexistent-neighbor", "value")
	require.NoError(t, err)
	neighborWait := make(chan error, 1)
	go func() { neighborWait <- recipe.NewBarrier(client, base+"nonexistent").Wait() }()
	neighborImmediate := false
	select {
	case waitErr := <-neighborWait:
		require.NoError(t, waitErr)
		neighborImmediate = true
	case <-time.After(2 * time.Second):
		t.Fatal("barrier wait on a nonexistent exact key was blocked by its neighbor")
	}

	fifo := recipe.NewQueue(client, base+"fifo")
	for _, value := range []string{"zero", "one", "two", "three", "four"} {
		require.NoError(t, fifo.Enqueue(value))
	}
	fifoValues := make([]string, 0, 5)
	for range 5 {
		value, dequeueErr := fifo.Dequeue()
		require.NoError(t, dequeueErr)
		fifoValues = append(fifoValues, value)
	}

	priority := recipe.NewPriorityQueue(client, base+"priority")
	for _, item := range []struct {
		value    string
		priority uint16
	}{{"two-a", 2}, {"zero-a", 0}, {"one", 1}, {"zero-b", 0}, {"two-b", 2}} {
		require.NoError(t, priority.Enqueue(item.value, item.priority))
	}
	priorityValues := make([]string, 0, 5)
	for range 5 {
		value, dequeueErr := priority.Dequeue()
		require.NoError(t, dequeueErr)
		priorityValues = append(priorityValues, value)
	}

	concurrent := recipe.NewQueue(client, base+"concurrent")
	const writerCount = 3
	const itemsPerWriter = 3
	var writers sync.WaitGroup
	writerErrors := make(chan error, writerCount)
	for writer := range writerCount {
		writers.Add(1)
		go func(writer int) {
			defer writers.Done()
			for item := range itemsPerWriter {
				if enqueueErr := concurrent.Enqueue(fmt.Sprintf("writer-%d-item-%d", writer, item)); enqueueErr != nil {
					writerErrors <- enqueueErr
					return
				}
			}
		}(writer)
	}
	writers.Wait()
	close(writerErrors)
	for writerErr := range writerErrors {
		require.NoError(t, writerErr)
	}

	const readerCount = 3
	values := make(chan string, writerCount*itemsPerWriter)
	readerErrors := make(chan error, readerCount)
	var readers sync.WaitGroup
	for range readerCount {
		readers.Add(1)
		go func() {
			defer readers.Done()
			for range itemsPerWriter {
				value, dequeueErr := concurrent.Dequeue()
				if dequeueErr != nil {
					readerErrors <- dequeueErr
					return
				}
				values <- value
			}
		}()
	}
	readers.Wait()
	close(readerErrors)
	close(values)
	for readerErr := range readerErrors {
		require.NoError(t, readerErr)
	}
	concurrentValues := make([]string, 0, writerCount*itemsPerWriter)
	for value := range values {
		concurrentValues = append(concurrentValues, value)
	}
	sort.Strings(concurrentValues)

	return recipesOutcome{
		BarrierDoubleHoldRejected:   doubleHoldRejected,
		BarrierBlockedBeforeRelease: blocked,
		BarrierWaitersReleased:      released,
		NeighborWaitImmediate:       neighborImmediate,
		FIFOValues:                  fifoValues,
		PriorityValues:              priorityValues,
		ConcurrentValues:            concurrentValues,
	}
}
