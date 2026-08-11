package compat

import (
	"context"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"
)

// TestLeaseExpiresAfterPDTotalLossOutlastsTTL covers the time boundary omitted
// by short quorum-loss tests. A keepalive attempted while every PD endpoint is
// unreachable must not be acknowledged or applied later, and the successor
// elected after recovery must revoke the now-expired durable lease atomically.
func TestLeaseExpiresAfterPDTotalLossOutlastsTTL(t *testing.T) {
	command := os.Getenv("KUBEBRAIN_PD_LONG_LOSS_LEASE_COMMAND")
	if command == "" {
		t.Skip("set KUBEBRAIN_PD_LONG_LOSS_LEASE_COMMAND to run destructive long PD total loss")
	}
	endpoint := os.Getenv("KUBEBRAIN_ETCD_ENDPOINT")
	if endpoint == "" {
		t.Fatal("set KUBEBRAIN_ETCD_ENDPOINT explicitly for long PD total loss")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	cli, err := clientv3.New(clientv3.Config{Endpoints: []string{endpoint}, DialTimeout: 5 * time.Second})
	require.NoError(t, err)
	prefix := testPrefix(t) + "/"
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cleanupCancel()
		_, _ = cli.Delete(cleanupCtx, prefix, clientv3.WithPrefix())
		require.NoError(t, cli.Close())
	})

	grant, err := cli.Grant(ctx, 10)
	require.NoError(t, err)
	require.Equal(t, int64(10), grant.TTL)
	key := prefix + "leased"
	put, err := cli.Put(ctx, key, "expires-across-long-loss", clientv3.WithLease(grant.ID))
	require.NoError(t, err)
	require.Positive(t, put.Header.Revision)
	watch := cli.Watch(ctx, key, clientv3.WithRev(put.Header.Revision+1), clientv3.WithPrevKV())

	type commandResult struct {
		output []byte
		err    error
	}
	commandDone := make(chan commandResult, 1)
	go func() {
		output, commandErr := runCompatShellCommandContext(t, ctx, command)
		commandDone <- commandResult{output: output, err: commandErr}
	}()
	commandWaited := false
	defer func() {
		if commandWaited {
			return
		}
		result := <-commandDone
		require.NoErrorf(t, result.err, "long PD total-loss command cleanup: %s",
			strings.TrimSpace(string(result.output)))
	}()

	require.Eventually(t, func() bool { return reachablePDCount(t, ctx) == 0 },
		30*time.Second, 200*time.Millisecond, "all three PD endpoints must become unreachable")
	keepAliveCtx, keepAliveCancel := context.WithTimeout(ctx, time.Second)
	keepAliveResponse, keepAliveErr := cli.KeepAliveOnce(keepAliveCtx, grant.ID)
	keepAliveCancel()
	require.Nil(t, keepAliveResponse, "PD total loss must not acknowledge a lease renewal")
	require.Error(t, keepAliveErr)

	// Stay beyond the granted TTL while all endpoints are still isolated. The
	// helper's live mode holds the partition for 45s, leaving ample margin for
	// observer probes and this TTL-relative assertion.
	time.Sleep(13 * time.Second)
	require.Equal(t, 0, reachablePDCount(t, ctx), "PD total loss must outlast the lease TTL")

	result := <-commandDone
	commandWaited = true
	require.NoErrorf(t, result.err, "long PD total-loss command: %s", strings.TrimSpace(string(result.output)))
	t.Logf("long PD total-loss command: %s", strings.TrimSpace(string(result.output)))

	fresh, err := clientv3.New(clientv3.Config{Endpoints: []string{endpoint}, DialTimeout: 5 * time.Second})
	require.NoError(t, err)
	defer func() { require.NoError(t, fresh.Close()) }()
	require.Eventually(t, func() bool {
		callCtx, callCancel := context.WithTimeout(ctx, 3*time.Second)
		defer callCancel()
		response, getErr := fresh.Get(callCtx, key)
		return getErr == nil && len(response.Kvs) == 0
	}, 60*time.Second, 100*time.Millisecond, "expired lease key must be deleted after PD recovery")
	require.Eventually(t, func() bool {
		callCtx, callCancel := context.WithTimeout(ctx, 3*time.Second)
		defer callCancel()
		response, ttlErr := fresh.TimeToLive(callCtx, grant.ID)
		return ttlErr == nil && response != nil && response.TTL == -1
	}, 30*time.Second, 100*time.Millisecond, "expired lease must not survive or be resurrected after PD recovery")

	var deleteEvent *mvccpb.Event
	deadline := time.NewTimer(150 * time.Second)
	defer deadline.Stop()
	for deleteEvent == nil {
		select {
		case response, ok := <-watch:
			require.True(t, ok, "watch closed before expired lease deletion")
			require.NoError(t, response.Err())
			for _, event := range response.Events {
				if event.Type == mvccpb.DELETE && string(event.Kv.Key) == key {
					require.Nil(t, deleteEvent, "lease expiry must emit exactly one delete event")
					deleteEvent = event
				}
			}
		case <-deadline.C:
			t.Fatal("timed out waiting for lease-expiry delete event after PD recovery")
		case <-ctx.Done():
			require.NoError(t, ctx.Err())
		}
	}
	require.Greater(t, deleteEvent.Kv.ModRevision, put.Header.Revision)
	require.NotNil(t, deleteEvent.PrevKv)
	require.Equal(t, "expires-across-long-loss", string(deleteEvent.PrevKv.Value))
	require.Equal(t, int64(grant.ID), deleteEvent.PrevKv.Lease)
}
