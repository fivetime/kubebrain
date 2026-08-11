package compat

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"
)

// TestLeaseRevokesWithLongDeadlineSurvivePDTotalLoss extends upstream
// TestLeaseRevoke across a complete PD outage. Every Revoke is issued once by
// the application and must survive the blackout through clientv3's internal
// retry path, atomically delete its attached key, publish PrevKV watch history,
// and leave no live/listed lease behind.
func TestLeaseRevokesWithLongDeadlineSurvivePDTotalLoss(t *testing.T) {
	command := os.Getenv("KUBEBRAIN_PD_LEASE_REVOKE_LONG_DEADLINE_COMMAND")
	if command == "" {
		t.Skip("set KUBEBRAIN_PD_LEASE_REVOKE_LONG_DEADLINE_COMMAND to run destructive revoke overlap")
	}
	endpoint := os.Getenv("KUBEBRAIN_ETCD_ENDPOINT")
	if endpoint == "" {
		t.Fatal("set KUBEBRAIN_ETCD_ENDPOINT explicitly for revoke overlap")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 6*time.Minute)
	defer cancel()
	client, err := clientv3.New(clientv3.Config{Endpoints: []string{endpoint}, DialTimeout: 5 * time.Second})
	require.NoError(t, err)
	defer func() { require.NoError(t, client.Close()) }()

	prefix := fmt.Sprintf("/compat/pd-revoke-long/%d/", time.Now().UnixNano())
	prepared := make([]preparedLease, 0, 16)
	var watchRevision int64
	for index := range 16 {
		grant, grantErr := client.Grant(ctx, 300)
		require.NoError(t, grantErr)
		key := fmt.Sprintf("%s%02d", prefix, index)
		value := fmt.Sprintf("value-%02d", index)
		put, putErr := client.Put(ctx, key, value, clientv3.WithLease(grant.ID))
		require.NoError(t, putErr)
		watchRevision = put.Header.Revision + 1
		prepared = append(prepared, preparedLease{id: grant.ID, key: key, value: value})
	}
	watchCtx, watchCancel := context.WithCancel(ctx)
	defer watchCancel()
	watch := client.Watch(watchCtx, prefix, clientv3.WithPrefix(), clientv3.WithRev(watchRevision), clientv3.WithPrevKV())

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
		require.NoErrorf(t, result.err, "long-deadline revoke cleanup: %s", strings.TrimSpace(string(result.output)))
	}()

	require.Eventually(t, func() bool { return reachablePDCount(t, ctx) == 0 },
		30*time.Second, 200*time.Millisecond, "all PD endpoints must become unreachable")
	during := startLeaseRevokeWave(ctx, client, prepared[:8], 180*time.Second)
	assertNoLeaseRevokeCompletion(t, during, 2*time.Second)

	time.Sleep(25 * time.Second)
	require.Equal(t, 0, reachablePDCount(t, ctx), "boundary revokes must start during PD total loss")
	boundary := startLeaseRevokeWave(ctx, client, prepared[8:], 180*time.Second)

	result := <-commandDone
	commandWaited = true
	require.NoErrorf(t, result.err, "long-deadline revoke command: %s", strings.TrimSpace(string(result.output)))
	t.Logf("long-deadline revoke command: %s", strings.TrimSpace(string(result.output)))
	collectLeaseRevokeWave(t, ctx, during, 8)
	collectLeaseRevokeWave(t, ctx, boundary, 8)

	rangeResp, err := client.Get(ctx, prefix, clientv3.WithPrefix())
	require.NoError(t, err)
	require.Empty(t, rangeResp.Kvs)
	wantLeases := make(map[clientv3.LeaseID]struct{}, len(prepared))
	wantDeletes := make(map[string]preparedLease, len(prepared))
	for _, lease := range prepared {
		wantLeases[lease.id] = struct{}{}
		wantDeletes[lease.key] = lease
		ttl, ttlErr := client.TimeToLive(ctx, lease.id)
		require.NoError(t, ttlErr)
		require.Equal(t, int64(-1), ttl.TTL)
	}
	listed, err := client.Leases(ctx)
	require.NoError(t, err)
	for _, lease := range listed.Leases {
		_, leaked := wantLeases[lease.ID]
		require.Falsef(t, leaked, "revoked lease %d remained listed", lease.ID)
	}
	assertLeaseRevokeWatch(t, ctx, watch, wantDeletes)
}

type preparedLease struct {
	id    clientv3.LeaseID
	key   string
	value string
}

type leaseRevokeOutcome struct{ err error }

func startLeaseRevokeWave(ctx context.Context, client *clientv3.Client, leases []preparedLease, timeout time.Duration) <-chan leaseRevokeOutcome {
	outcomes := make(chan leaseRevokeOutcome, len(leases))
	for _, lease := range leases {
		go func(id clientv3.LeaseID) {
			requestCtx, cancel := context.WithTimeout(ctx, timeout)
			defer cancel()
			_, err := client.Revoke(requestCtx, id)
			outcomes <- leaseRevokeOutcome{err: err}
		}(lease.id)
	}
	return outcomes
}

func assertNoLeaseRevokeCompletion(t *testing.T, outcomes <-chan leaseRevokeOutcome, duration time.Duration) {
	t.Helper()
	select {
	case outcome := <-outcomes:
		require.Failf(t, "lease revoke completed during PD total loss", "err=%v", outcome.err)
	case <-time.After(duration):
	}
}

func collectLeaseRevokeWave(t *testing.T, ctx context.Context, outcomes <-chan leaseRevokeOutcome, count int) {
	t.Helper()
	for range count {
		select {
		case outcome := <-outcomes:
			require.NoError(t, outcome.err)
		case <-ctx.Done():
			require.NoError(t, ctx.Err())
		}
	}
}

func assertLeaseRevokeWatch(t *testing.T, ctx context.Context, watch clientv3.WatchChan, want map[string]preparedLease) {
	t.Helper()
	seen := make(map[string]struct{}, len(want))
	for len(seen) < len(want) {
		select {
		case response, ok := <-watch:
			require.True(t, ok, "revoke watch closed before all deletes")
			require.NoError(t, response.Err())
			for _, event := range response.Events {
				require.Equal(t, mvccpb.DELETE, event.Type)
				expected, exists := want[string(event.Kv.Key)]
				require.Truef(t, exists, "unexpected revoke delete %q", event.Kv.Key)
				require.NotNil(t, event.PrevKv)
				require.Equal(t, expected.value, string(event.PrevKv.Value))
				require.Equal(t, int64(expected.id), event.PrevKv.Lease)
				_, duplicate := seen[expected.key]
				require.Falsef(t, duplicate, "duplicate revoke delete %q", expected.key)
				seen[expected.key] = struct{}{}
			}
		case <-ctx.Done():
			require.NoError(t, ctx.Err())
		}
	}
}
