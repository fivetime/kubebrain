package compat

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	clientv3 "go.etcd.io/etcd/client/v3"
)

// TestBackendQuorumFailoverKeepsServing is opt-in because the supplied command
// disrupts the external TiKV/PD cluster. Unlike the total-backend-outage test,
// this test targets a replicated backend and requires useful client progress
// while one PD or TiKV member is being replaced.
func TestBackendQuorumFailoverKeepsServing(t *testing.T) {
	failoverCommand := os.Getenv("KUBEBRAIN_QUORUM_FAILOVER_COMMAND")
	if failoverCommand == "" {
		t.Skip("set KUBEBRAIN_QUORUM_FAILOVER_COMMAND to run replicated-backend failover")
	}
	endpoint := os.Getenv("KUBEBRAIN_ETCD_ENDPOINT")
	if endpoint == "" {
		t.Fatal("set KUBEBRAIN_ETCD_ENDPOINT explicitly for backend quorum failover")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{endpoint},
		DialTimeout: 3 * time.Second,
	})
	require.NoError(t, err)
	prefix := testPrefix(t) + "/"
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cleanupCancel()
		_, _ = cli.Delete(cleanupCtx, prefix, clientv3.WithPrefix())
		require.NoError(t, cli.Close())
	})

	const workers = 8
	for worker := 0; worker < workers; worker++ {
		_, err = cli.Put(ctx, fmt.Sprintf("%skey-%d", prefix, worker), "seed")
		require.NoError(t, err)
	}

	var active atomic.Bool
	var successfulDuringFailover atomic.Int64
	errCh := make(chan error, workers+1)
	stop := make(chan struct{})
	var wg sync.WaitGroup
	for worker := 0; worker < workers; worker++ {
		wg.Add(1)
		go func(worker int) {
			defer wg.Done()
			workerClient, clientErr := clientv3.New(clientv3.Config{
				Endpoints:   []string{endpoint},
				DialTimeout: 3 * time.Second,
			})
			if clientErr != nil {
				errCh <- clientErr
				return
			}
			defer workerClient.Close()
			key := fmt.Sprintf("%skey-%d", prefix, worker)
			for operation := 0; ; operation++ {
				select {
				case <-stop:
					return
				default:
					callCtx, callCancel := context.WithTimeout(ctx, 4*time.Second)
					startedDuringFailover := active.Load()
					switch operation % 3 {
					case 0:
						_, clientErr = workerClient.Put(callCtx, key, fmt.Sprintf("put-%d", operation))
					case 1:
						var response *clientv3.TxnResponse
						response, clientErr = workerClient.Txn(callCtx).
							Then(clientv3.OpPut(key, fmt.Sprintf("txn-%d", operation))).
							Commit()
						if clientErr == nil && !response.Succeeded {
							clientErr = fmt.Errorf("unconditional txn selected failure")
						}
					default:
						_, clientErr = workerClient.Get(callCtx, key)
					}
					callCancel()
					if clientErr != nil {
						if !isMutationFailoverAmbiguous(clientErr) {
							errCh <- fmt.Errorf("worker %d operation %d: %w", worker, operation, clientErr)
							return
						}
					} else if startedDuringFailover && active.Load() {
						successfulDuringFailover.Add(1)
					}
					time.Sleep(5 * time.Millisecond)
				}
			}
		}(worker)
	}

	time.Sleep(500 * time.Millisecond)
	active.Store(true)
	output, err := exec.CommandContext(ctx, "bash", "-c", failoverCommand).CombinedOutput()
	active.Store(false)
	close(stop)
	if err != nil {
		errCh <- fmt.Errorf("failover command: %w: %s", err, strings.TrimSpace(string(output)))
	}
	wg.Wait()
	close(errCh)
	for workerErr := range errCh {
		require.NoError(t, workerErr)
	}
	successes := successfulDuringFailover.Load()
	t.Logf("completed %d data operations while one backend member was unavailable", successes)
	require.GreaterOrEqual(t, successes, int64(workers),
		"replicated backend must complete at least one data operation per worker during failover")

	for worker := 0; worker < workers; worker++ {
		key := fmt.Sprintf("%skey-%d", prefix, worker)
		require.Eventually(t, func() bool {
			probeCtx, probeCancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer probeCancel()
			if _, probeErr := cli.Put(probeCtx, key, "recovered"); probeErr != nil {
				return false
			}
			response, probeErr := cli.Get(probeCtx, key)
			return probeErr == nil &&
				len(response.Kvs) == 1 &&
				string(response.Kvs[0].Value) == "recovered"
		}, 45*time.Second, 500*time.Millisecond,
			"key %q must become writable and readable after backend member recovery", key)
	}
}
