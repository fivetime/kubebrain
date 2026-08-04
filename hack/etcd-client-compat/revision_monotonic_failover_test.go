package compat

import (
	"context"
	"fmt"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	clientv3 "go.etcd.io/etcd/client/v3"
)

// TestRevisionRemainsMonotonicAcrossLeaderFailover mirrors upstream etcd's
// TestRevisionMonotonicWithLeaderRestarts against a deployed KubeBrain data
// plane. The supplied command must delete the current mutation leader and
// return after issuing the disruption.
func TestRevisionRemainsMonotonicAcrossLeaderFailover(t *testing.T) {
	failoverCommand := os.Getenv("KUBEBRAIN_REVISION_FAILOVER_COMMAND")
	if failoverCommand == "" {
		t.Skip("set KUBEBRAIN_REVISION_FAILOVER_COMMAND to delete the current live leader")
	}
	endpoint := os.Getenv("KUBEBRAIN_ETCD_ENDPOINT")
	if endpoint == "" {
		t.Fatal("set KUBEBRAIN_ETCD_ENDPOINT explicitly for revision failover")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	prefix := testPrefix(t)
	seed, err := clientv3.New(clientv3.Config{
		Endpoints: []string{endpoint}, DialTimeout: 5 * time.Second,
	})
	require.NoError(t, err)
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cleanupCancel()
		_, _ = seed.Delete(cleanupCtx, prefix, clientv3.WithPrefix())
		require.NoError(t, seed.Close())
	})
	seedResponse, err := seed.Put(ctx, prefix+"/seed", "seed")
	require.NoError(t, err)
	require.Positive(t, seedResponse.Header.Revision)

	const writerCount, readerCount = 4, 6
	start := make(chan struct{})
	stop := make(chan struct{})
	var stopOnce sync.Once
	stopWorkers := func() { stopOnce.Do(func() { close(stop) }) }
	errCh := make(chan error, writerCount+readerCount)
	var ready, successfulReads, successfulWrites, transientErrors atomic.Int64
	var maximumRevision atomic.Int64
	maximumRevision.Store(seedResponse.Header.Revision)
	updateMaximum := func(revision int64) {
		for current := maximumRevision.Load(); revision > current; current = maximumRevision.Load() {
			if maximumRevision.CompareAndSwap(current, revision) {
				return
			}
		}
	}

	var workers sync.WaitGroup
	for worker := 0; worker < writerCount; worker++ {
		workers.Add(1)
		go func(worker int) {
			defer workers.Done()
			client, clientErr := clientv3.New(clientv3.Config{
				Endpoints: []string{endpoint}, DialTimeout: 5 * time.Second,
			})
			if clientErr != nil {
				errCh <- clientErr
				return
			}
			defer client.Close()
			ready.Add(1)
			<-start
			for operation := 0; ; operation++ {
				select {
				case <-stop:
					return
				default:
				}
				callCtx, callCancel := context.WithTimeout(ctx, 3*time.Second)
				response, putErr := client.Put(callCtx, fmt.Sprintf("%s/writer-%d", prefix, worker), fmt.Sprintf("%d", operation))
				callCancel()
				if putErr != nil {
					if isMutationFailoverAmbiguous(putErr) {
						transientErrors.Add(1)
						continue
					}
					errCh <- fmt.Errorf("writer %d operation %d: %w", worker, operation, putErr)
					return
				}
				successfulWrites.Add(1)
				updateMaximum(response.Header.Revision)
			}
		}(worker)
	}
	for worker := 0; worker < readerCount; worker++ {
		workers.Add(1)
		go func(worker int) {
			defer workers.Done()
			client, clientErr := clientv3.New(clientv3.Config{
				Endpoints: []string{endpoint}, DialTimeout: 5 * time.Second,
			})
			if clientErr != nil {
				errCh <- clientErr
				return
			}
			defer client.Close()
			var previousRevision int64
			ready.Add(1)
			<-start
			for {
				select {
				case <-stop:
					return
				default:
				}
				callCtx, callCancel := context.WithTimeout(ctx, 3*time.Second)
				response, getErr := client.Get(callCtx, prefix, clientv3.WithPrefix())
				callCancel()
				if getErr != nil {
					if isMutationFailoverAmbiguous(getErr) {
						transientErrors.Add(1)
						continue
					}
					errCh <- fmt.Errorf("reader %d: %w", worker, getErr)
					return
				}
				if response.Header.Revision < previousRevision {
					errCh <- fmt.Errorf("reader %d observed revision regression: previous=%d current=%d",
						worker, previousRevision, response.Header.Revision)
					return
				}
				previousRevision = response.Header.Revision
				successfulReads.Add(1)
				updateMaximum(response.Header.Revision)
			}
		}(worker)
	}
	defer func() {
		stopWorkers()
		workers.Wait()
	}()
	close(start)
	require.Eventually(t, func() bool { return ready.Load() == writerCount+readerCount },
		10*time.Second, 10*time.Millisecond, "all revision workers must initialize")
	require.Eventually(t, func() bool { return successfulReads.Load() > 0 && successfulWrites.Load() > 0 },
		10*time.Second, 10*time.Millisecond, "workload must begin before leader failover")

	output, err := runCompatShellCommandContext(t, ctx, failoverCommand)
	require.NoErrorf(t, err, "leader failover command: %s", strings.TrimSpace(string(output)))
	require.Eventually(t, func() bool { return transientErrors.Load() > 0 },
		10*time.Second, 10*time.Millisecond, "leader restart must overlap the revision workload")
	require.Eventually(t, func() bool {
		probeCtx, probeCancel := context.WithTimeout(ctx, 3*time.Second)
		defer probeCancel()
		_, probeErr := seed.Get(probeCtx, prefix, clientv3.WithPrefix())
		return probeErr == nil
	}, 45*time.Second, 500*time.Millisecond, "linearizable reads must recover after leader restart")
	time.Sleep(2 * time.Second)
	stopWorkers()
	workers.Wait()
	close(errCh)
	for workerErr := range errCh {
		require.NoError(t, workerErr)
	}

	var (
		finalPut        *clientv3.PutResponse
		finalPutError   error
		unexpectedError error
	)
	require.Eventually(t, func() bool {
		callCtx, callCancel := context.WithTimeout(ctx, 3*time.Second)
		defer callCancel()
		finalPut, finalPutError = seed.Put(callCtx, prefix+"/final", "recovered")
		if finalPutError == nil {
			return true
		}
		if !isMutationFailoverAmbiguous(finalPutError) {
			unexpectedError = finalPutError
			return true
		}
		return false
	}, 30*time.Second, 500*time.Millisecond, "writes must recover after leader restart")
	require.NoError(t, unexpectedError)
	require.NoError(t, finalPutError)
	var finalGet *clientv3.GetResponse
	require.Eventually(t, func() bool {
		callCtx, callCancel := context.WithTimeout(ctx, 3*time.Second)
		defer callCancel()
		finalGet, err = seed.Get(callCtx, prefix, clientv3.WithPrefix())
		if err == nil {
			return true
		}
		if !isMutationFailoverAmbiguous(err) {
			unexpectedError = err
			return true
		}
		return false
	}, 30*time.Second, 500*time.Millisecond, "reads must recover after leader restart")
	require.NoError(t, unexpectedError)
	require.NoError(t, err)
	require.Greater(t, finalPut.Header.Revision, maximumRevision.Load())
	require.GreaterOrEqual(t, finalGet.Header.Revision, finalPut.Header.Revision)
	require.Positive(t, successfulReads.Load())
	require.Positive(t, successfulWrites.Load())
	t.Logf("successful reads=%d writes=%d transient errors=%d final revision=%d",
		successfulReads.Load(), successfulWrites.Load(), transientErrors.Load(), finalGet.Header.Revision)
}
