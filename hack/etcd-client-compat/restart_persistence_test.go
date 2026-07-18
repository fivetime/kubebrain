package compat

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// TestReplicatedRestartPreservesState is opt-in because the supplied command
// restarts every KubeBrain, PD, and TiKV member in the external test cluster.
func TestReplicatedRestartPreservesState(t *testing.T) {
	restartCommand := os.Getenv("KUBEBRAIN_RESTART_PERSISTENCE_COMMAND")
	if restartCommand == "" {
		t.Skip("set KUBEBRAIN_RESTART_PERSISTENCE_COMMAND to run replicated restart persistence")
	}
	endpoint := os.Getenv("KUBEBRAIN_ETCD_ENDPOINT")
	if endpoint == "" {
		t.Fatal("set KUBEBRAIN_ETCD_ENDPOINT explicitly for replicated restart persistence")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{endpoint},
		DialTimeout: 5 * time.Second,
	})
	require.NoError(t, err)
	prefix := testPrefix(t) + "/"
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cleanupCancel()
		_, _ = cli.Delete(cleanupCtx, prefix, clientv3.WithPrefix())
		require.NoError(t, cli.Close())
	})

	durableKey := prefix + "durable"
	deletedKey := prefix + "deleted"
	leasedKey := prefix + "leased"
	historyKey := prefix + "history"

	durablePut, err := cli.Put(ctx, durableKey, "before-restart")
	require.NoError(t, err)
	deletedPut, err := cli.Put(ctx, deletedKey, "must-not-return")
	require.NoError(t, err)
	deleted, err := cli.Delete(ctx, deletedKey)
	require.NoError(t, err)
	require.Equal(t, int64(1), deleted.Deleted)

	lease, err := cli.Grant(ctx, 900)
	require.NoError(t, err)
	leasedPut, err := cli.Put(ctx, leasedKey, "leased-before-restart", clientv3.WithLease(lease.ID))
	require.NoError(t, err)
	historyPut, err := cli.Put(ctx, historyKey, "replay-after-restart")
	require.NoError(t, err)
	beforeRevision := historyPut.Header.Revision

	errCh := make(chan error, 1)
	stop := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		var previousRevision = beforeRevision
		for {
			select {
			case <-stop:
				return
			default:
			}
			callCtx, callCancel := context.WithTimeout(ctx, 5*time.Second)
			response, getErr := cli.Get(callCtx, durableKey)
			callCancel()
			if getErr != nil {
				if !isRestartTransient(getErr) {
					select {
					case errCh <- getErr:
					default:
					}
					return
				}
			} else {
				if response.Header.Revision < previousRevision {
					select {
					case errCh <- fmt.Errorf("revision regressed from %d to %d", previousRevision, response.Header.Revision):
					default:
					}
					return
				}
				previousRevision = response.Header.Revision
			}
			time.Sleep(25 * time.Millisecond)
		}
	}()

	output, commandErr := exec.CommandContext(ctx, "bash", "-c", restartCommand).CombinedOutput()
	close(stop)
	wg.Wait()
	select {
	case pollErr := <-errCh:
		require.NoError(t, pollErr)
	default:
	}
	require.NoErrorf(t, commandErr, "restart command output:\n%s", strings.TrimSpace(string(output)))
	t.Logf("restart command output:\n%s", strings.TrimSpace(string(output)))

	durable, err := cli.Get(ctx, durableKey)
	require.NoError(t, err)
	require.Len(t, durable.Kvs, 1)
	require.Equal(t, "before-restart", string(durable.Kvs[0].Value))
	require.GreaterOrEqual(t, durable.Header.Revision, beforeRevision)

	tombstone, err := cli.Get(ctx, deletedKey)
	require.NoError(t, err)
	require.Empty(t, tombstone.Kvs)
	historicalDeleted, err := cli.Get(ctx, deletedKey, clientv3.WithRev(deletedPut.Header.Revision))
	require.NoError(t, err)
	require.Len(t, historicalDeleted.Kvs, 1)
	require.Equal(t, "must-not-return", string(historicalDeleted.Kvs[0].Value))

	leased, err := cli.Get(ctx, leasedKey)
	require.NoError(t, err)
	require.Len(t, leased.Kvs, 1)
	require.Equal(t, leasedPut.Header.Revision, leased.Kvs[0].CreateRevision)
	require.Equal(t, int64(lease.ID), leased.Kvs[0].Lease)
	ttl, err := cli.TimeToLive(ctx, lease.ID, clientv3.WithAttachedKeys())
	require.NoError(t, err)
	require.Positive(t, ttl.TTL)
	require.Contains(t, ttl.Keys, []byte(leasedKey))

	watchCtx, watchCancel := context.WithTimeout(ctx, 15*time.Second)
	defer watchCancel()
	watch := cli.Watch(watchCtx, historyKey, clientv3.WithRev(historyPut.Header.Revision))
	select {
	case response := <-watch:
		require.NoError(t, response.Err())
		require.Len(t, response.Events, 1)
		require.Equal(t, "replay-after-restart", string(response.Events[0].Kv.Value))
	case <-watchCtx.Done():
		t.Fatal("timed out replaying persisted watch history")
	}

	after, err := cli.Put(ctx, durableKey, "after-restart")
	require.NoError(t, err)
	require.Greater(t, after.Header.Revision, beforeRevision)
	require.Greater(t, after.Header.Revision, durablePut.Header.Revision)
	require.Greater(t, after.Header.Revision, deleted.Header.Revision)

	_, err = cli.Revoke(ctx, lease.ID)
	require.NoError(t, err)
}

func isRestartTransient(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return true
	}
	switch status.Code(err) {
	case codes.Canceled, codes.DeadlineExceeded, codes.Unavailable:
		return true
	default:
		return false
	}
}
