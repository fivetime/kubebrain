package compat

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// TestMutationCoordinatorRecoversAfterBackendFailover is opt-in because its
// command disrupts the external TiKV/PD cluster. It keeps overlapping point
// mutations queued while leadership is lost, then proves a same-process
// re-election does not retain a stale mutation stripe or admit an old epoch.
func TestMutationCoordinatorRecoversAfterBackendFailover(t *testing.T) {
	failoverCommand := os.Getenv("KUBEBRAIN_MUTATION_FAILOVER_COMMAND")
	if failoverCommand == "" {
		t.Skip("set KUBEBRAIN_MUTATION_FAILOVER_COMMAND to run destructive backend failover")
	}
	endpoint := os.Getenv("KUBEBRAIN_ETCD_ENDPOINT")
	if endpoint == "" {
		t.Fatal("set KUBEBRAIN_ETCD_ENDPOINT explicitly for mutation failover")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	key := testPrefix(t) + "/hot"
	seed, err := clientv3.New(clientv3.Config{Endpoints: []string{endpoint}, DialTimeout: 3 * time.Second})
	require.NoError(t, err)
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cleanupCancel()
		_, _ = seed.Delete(cleanupCtx, key)
		require.NoError(t, seed.Close())
	})
	_, err = seed.Put(ctx, key, "seed")
	require.NoError(t, err)

	const workers, operations = 6, 30
	start := make(chan struct{})
	errCh := make(chan error, workers+1)
	var ambiguous atomic.Int64
	var wg sync.WaitGroup
	for worker := 0; worker < workers; worker++ {
		wg.Add(1)
		go func(worker int) {
			defer wg.Done()
			cli, err := clientv3.New(clientv3.Config{Endpoints: []string{endpoint}, DialTimeout: 3 * time.Second})
			if err != nil {
				errCh <- err
				return
			}
			defer cli.Close()
			<-start
			for i := 0; i < operations; i++ {
				callCtx, callCancel := context.WithTimeout(ctx, 4*time.Second)
				switch (worker + i) % 3 {
				case 0:
					_, err = cli.Put(callCtx, key, fmt.Sprintf("put-%d-%d", worker, i))
				case 1:
					_, err = cli.Delete(callCtx, key)
				default:
					var resp *clientv3.TxnResponse
					resp, err = cli.Txn(callCtx).
						Then(clientv3.OpPut(key, fmt.Sprintf("txn-%d-%d", worker, i))).
						Commit()
					if err == nil && !resp.Succeeded {
						err = fmt.Errorf("unconditional txn selected failure")
					}
				}
				callCancel()
				if err != nil {
					if !isMutationFailoverAmbiguous(err) {
						errCh <- fmt.Errorf("worker %d operation %d: %w", worker, i, err)
						return
					}
					ambiguous.Add(1)
				}
				time.Sleep(10 * time.Millisecond)
			}
		}(worker)
	}
	close(start)
	time.Sleep(200 * time.Millisecond)
	output, err := runCompatShellCommandContext(t, ctx, failoverCommand)
	if err != nil {
		errCh <- fmt.Errorf("failover command: %w: %s", err, strings.TrimSpace(string(output)))
	}
	wg.Wait()
	close(errCh)
	for workerErr := range errCh {
		require.NoError(t, workerErr)
	}
	require.Positive(t, ambiguous.Load(), "fault must overlap at least one ambiguous mutation")

	require.Eventually(t, func() bool {
		probeCtx, probeCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer probeCancel()
		if _, probeErr := seed.Put(probeCtx, key, "recovered-put"); probeErr != nil {
			return false
		}
		txn, probeErr := seed.Txn(probeCtx).Then(clientv3.OpPut(key, "recovered-txn")).Commit()
		if probeErr != nil || !txn.Succeeded {
			return false
		}
		deleted, probeErr := seed.Delete(probeCtx, key)
		return probeErr == nil && deleted.Deleted == 1
	}, 45*time.Second, 500*time.Millisecond, "Put/Txn/Delete must recover without a leaked mutation stripe")
}

func isMutationFailoverAmbiguous(err error) bool {
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return true
	}
	// clientv3 maps recognized server errors to rpctypes.EtcdError. It exposes
	// Code() but intentionally does not implement gRPCStatus(), so status.Code
	// would report Unknown for etcd's retryable timeout contract.
	var etcdError interface{ Code() codes.Code }
	if errors.As(err, &etcdError) {
		switch etcdError.Code() {
		case codes.Canceled, codes.DeadlineExceeded, codes.Unavailable:
			return true
		}
	}
	switch status.Code(err) {
	case codes.Canceled, codes.DeadlineExceeded, codes.Unavailable:
		return true
	default:
		return false
	}
}

func TestMutationFailoverAmbiguousClassifiesClientv3EtcdTimeout(t *testing.T) {
	require.True(t, isMutationFailoverAmbiguous(rpctypes.ErrTimeout))
	require.True(t, isMutationFailoverAmbiguous(fmt.Errorf("wrapped: %w", rpctypes.ErrTimeoutDueToLeaderFail)))
	require.False(t, isMutationFailoverAmbiguous(rpctypes.ErrLeaseNotFound))
}
