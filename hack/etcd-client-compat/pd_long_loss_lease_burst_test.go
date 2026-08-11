package compat

import (
	"context"
	"fmt"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"
)

// TestLeaseExpiryBurstAfterPDTotalLoss exceeds KubeBrain's default 1000/s
// recovered-lease revoke budget. It verifies that promotion spreading drains
// every expired attachment without losing, duplicating, or reordering the
// externally visible delete history.
func TestLeaseExpiryBurstAfterPDTotalLoss(t *testing.T) {
	command := os.Getenv("KUBEBRAIN_PD_LONG_LOSS_LEASE_BURST_COMMAND")
	if command == "" {
		t.Skip("set KUBEBRAIN_PD_LONG_LOSS_LEASE_BURST_COMMAND to run destructive burst expiry")
	}
	endpoint := os.Getenv("KUBEBRAIN_ETCD_ENDPOINT")
	if endpoint == "" {
		t.Fatal("set KUBEBRAIN_ETCD_ENDPOINT explicitly for burst expiry")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	cli, err := clientv3.New(clientv3.Config{Endpoints: []string{endpoint}, DialTimeout: 5 * time.Second})
	require.NoError(t, err)
	prefix := testPrefix(t) + "/"
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cleanupCancel()
		_, _ = cli.Delete(cleanupCtx, prefix, clientv3.WithPrefix())
		require.NoError(t, cli.Close())
	})

	const leaseCount = 1200
	renewCtx, stopRenewals := context.WithCancel(ctx)
	leaseIDs := make([]clientv3.LeaseID, leaseCount)
	keys := make([]string, leaseCount)
	var preparedMu sync.RWMutex
	prepared := make([]clientv3.LeaseID, 0, leaseCount)
	renewalsDone := make(chan struct{})
	go func() {
		defer close(renewalsDone)
		ticker := time.NewTicker(10 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				preparedMu.RLock()
				ids := append([]clientv3.LeaseID(nil), prepared...)
				preparedMu.RUnlock()
				renewLeaseBatch(renewCtx, cli, ids)
			case <-renewCtx.Done():
				return
			}
		}
	}()
	defer func() {
		stopRenewals()
		<-renewalsDone
	}()
	type setupFailure struct {
		index int
		err   error
	}
	failures := make(chan setupFailure, leaseCount)
	work := make(chan int)
	var workers sync.WaitGroup
	for range 32 {
		workers.Add(1)
		go func() {
			defer workers.Done()
			for index := range work {
				grant, grantErr := cli.Grant(ctx, 60)
				if grantErr != nil {
					failures <- setupFailure{index: index, err: fmt.Errorf("grant: %w", grantErr)}
					continue
				}
				key := fmt.Sprintf("%s%04d", prefix, index)
				if _, putErr := cli.Put(ctx, key, fmt.Sprintf("value-%04d", index), clientv3.WithLease(grant.ID)); putErr != nil {
					failures <- setupFailure{index: index, err: fmt.Errorf("put: %w", putErr)}
					continue
				}
				leaseIDs[index] = grant.ID
				keys[index] = key
				preparedMu.Lock()
				prepared = append(prepared, grant.ID)
				preparedMu.Unlock()
			}
		}()
	}
	for index := range leaseCount {
		work <- index
	}
	close(work)
	workers.Wait()
	close(failures)
	for failure := range failures {
		require.NoErrorf(t, failure.err, "prepare lease %d", failure.index)
	}
	for index := range leaseCount {
		require.NotZero(t, leaseIDs[index])
		require.NotEmpty(t, keys[index])
	}
	seeded, err := cli.Get(ctx, prefix, clientv3.WithPrefix())
	require.NoError(t, err)
	require.Len(t, seeded.Kvs, leaseCount, "all leases must remain attached before PD total loss")
	firstTTL, err := cli.TimeToLive(ctx, leaseIDs[0])
	require.NoError(t, err)
	lastTTL, err := cli.TimeToLive(ctx, leaseIDs[len(leaseIDs)-1])
	require.NoError(t, err)
	require.Positive(t, firstTTL.TTL)
	require.Positive(t, lastTTL.TTL)
	stopRenewals()
	<-renewalsDone
	watch := cli.Watch(ctx, prefix, clientv3.WithPrefix(), clientv3.WithRev(seeded.Header.Revision+1),
		clientv3.WithPrevKV(), clientv3.WithCreatedNotify())
	created := <-watch
	require.NoError(t, created.Err())
	require.True(t, created.Created)
	require.Empty(t, created.Events)

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
		require.NoErrorf(t, result.err, "lease burst PD total-loss cleanup: %s",
			strings.TrimSpace(string(result.output)))
	}()

	require.Eventually(t, func() bool { return reachablePDCount(t, ctx) == 0 },
		30*time.Second, 200*time.Millisecond, "all PD endpoints must become unreachable")
	time.Sleep(63 * time.Second)
	require.Equal(t, 0, reachablePDCount(t, ctx), "PD total loss must outlast every burst lease TTL")
	result := <-commandDone
	commandWaited = true
	require.NoErrorf(t, result.err, "lease burst PD total-loss command: %s", strings.TrimSpace(string(result.output)))
	t.Logf("lease burst PD total-loss command: %s", strings.TrimSpace(string(result.output)))

	wantLeaseByKey := make(map[string]int64, leaseCount)
	for index, key := range keys {
		wantLeaseByKey[key] = int64(leaseIDs[index])
	}
	seen := make(map[string]int64, leaseCount)
	var previousRevision int64
	deadline := time.NewTimer(2 * time.Minute)
	defer deadline.Stop()
	for len(seen) < leaseCount {
		select {
		case response, ok := <-watch:
			require.True(t, ok, "watch closed after %d/%d burst deletes", len(seen), leaseCount)
			require.NoError(t, response.Err())
			for _, event := range response.Events {
				require.Equal(t, mvccpb.DELETE, event.Type)
				key := string(event.Kv.Key)
				wantLease, exists := wantLeaseByKey[key]
				require.Truef(t, exists, "unexpected burst delete key %q", key)
				_, duplicate := seen[key]
				require.Falsef(t, duplicate, "duplicate burst delete for %q", key)
				require.Greater(t, event.Kv.ModRevision, previousRevision,
					"one-key lease revokes must advance revision in delivery order")
				require.NotNil(t, event.PrevKv)
				require.Equal(t, wantLease, event.PrevKv.Lease)
				seen[key] = event.Kv.ModRevision
				previousRevision = event.Kv.ModRevision
			}
		case <-deadline.C:
			t.Fatalf("timed out after %d/%d burst lease deletes", len(seen), leaseCount)
		case <-ctx.Done():
			require.NoError(t, ctx.Err())
		}
	}

	fresh, err := clientv3.New(clientv3.Config{Endpoints: []string{endpoint}, DialTimeout: 5 * time.Second})
	require.NoError(t, err)
	defer func() { require.NoError(t, fresh.Close()) }()
	remaining, err := fresh.Get(ctx, prefix, clientv3.WithPrefix())
	require.NoError(t, err)
	require.Empty(t, remaining.Kvs, "burst expiry must delete every attached key")
	listed, err := fresh.Leases(ctx)
	require.NoError(t, err)
	testLeases := make(map[clientv3.LeaseID]struct{}, leaseCount)
	for _, id := range leaseIDs {
		testLeases[id] = struct{}{}
	}
	for _, lease := range listed.Leases {
		_, leaked := testLeases[lease.ID]
		require.Falsef(t, leaked, "expired burst lease %d remained listed", lease.ID)
	}
}

func renewLeaseBatch(ctx context.Context, client *clientv3.Client, ids []clientv3.LeaseID) {
	work := make(chan clientv3.LeaseID)
	var workers sync.WaitGroup
	for range 32 {
		workers.Add(1)
		go func() {
			defer workers.Done()
			for id := range work {
				_, _ = client.KeepAliveOnce(ctx, id)
			}
		}()
	}
	for _, id := range ids {
		select {
		case work <- id:
		case <-ctx.Done():
			close(work)
			workers.Wait()
			return
		}
	}
	close(work)
	workers.Wait()
}
