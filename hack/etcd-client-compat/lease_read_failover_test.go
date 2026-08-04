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

func TestLeaseReadsStayAuthoritativeDuringLeaderFailover(t *testing.T) {
	failoverCommand := os.Getenv("KUBEBRAIN_LEASE_READ_FAILOVER_COMMAND")
	if failoverCommand == "" {
		t.Skip("set KUBEBRAIN_LEASE_READ_FAILOVER_COMMAND to delete the current active leader")
	}

	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{compatEndpoint(t)},
		DialTimeout: 3 * time.Second,
	})
	require.NoError(t, err)
	defer func() { require.NoError(t, cli.Close()) }()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	lease, err := cli.Grant(ctx, 300)
	require.NoError(t, err)
	defer func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cleanupCancel()
		_, _ = cli.Revoke(cleanupCtx, lease.ID)
	}()

	stop := make(chan struct{})
	var stopOnce sync.Once
	stopWorkers := func() { stopOnce.Do(func() { close(stop) }) }
	errs := make(chan error, 64)
	var successes, transientErrors atomic.Int64
	var wg sync.WaitGroup
	for range 16 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				ttlCtx, ttlCancel := context.WithTimeout(ctx, time.Second)
				ttl, ttlErr := cli.TimeToLive(ttlCtx, lease.ID)
				ttlCancel()
				if ttlErr == nil {
					if ttl.ID != lease.ID || ttl.TTL <= 0 || ttl.GrantedTTL != 300 {
						errs <- fmt.Errorf("stale TTL response: id=%x ttl=%d granted=%d", ttl.ID, ttl.TTL, ttl.GrantedTTL)
						return
					}
					successes.Add(1)
				} else if isMutationFailoverAmbiguous(ttlErr) {
					transientErrors.Add(1)
				} else {
					errs <- fmt.Errorf("LeaseTimeToLive returned unexpected error: %w", ttlErr)
					return
				}
				listCtx, listCancel := context.WithTimeout(ctx, time.Second)
				leases, listErr := cli.Leases(listCtx)
				listCancel()
				if listErr == nil {
					found := false
					for _, listed := range leases.Leases {
						found = found || listed.ID == lease.ID
					}
					if !found {
						errs <- fmt.Errorf("successful LeaseLeases omitted live lease %x", lease.ID)
						return
					}
					successes.Add(1)
				} else if isMutationFailoverAmbiguous(listErr) {
					transientErrors.Add(1)
				} else {
					errs <- fmt.Errorf("LeaseLeases returned unexpected error: %w", listErr)
					return
				}
				// Two RPCs per iteration across 16 workers stay below the
				// production 2,000 request/s admission limit. The test targets
				// authoritative failover reads, not rate-limit saturation.
				time.Sleep(20 * time.Millisecond)
			}
		}()
	}
	defer func() {
		stopWorkers()
		wg.Wait()
	}()
	require.Eventually(t, func() bool { return successes.Load() > 0 },
		10*time.Second, 10*time.Millisecond, "lease read workload must begin before leader failover")

	output, err := runCompatShellCommandContext(t, ctx, failoverCommand)
	require.NoErrorf(t, err, "failover command: %s", strings.TrimSpace(string(output)))
	require.Eventually(t, func() bool { return transientErrors.Load() > 0 },
		10*time.Second, 10*time.Millisecond, "leader restart must overlap the lease read workload")
	var (
		recoveredTTL    *clientv3.LeaseTimeToLiveResponse
		recoveredLeases *clientv3.LeaseLeasesResponse
		recoveryErr     error
		unexpectedErr   error
	)
	require.Eventually(t, func() bool {
		probeCtx, probeCancel := context.WithTimeout(ctx, 3*time.Second)
		defer probeCancel()
		recoveredTTL, recoveryErr = cli.TimeToLive(probeCtx, lease.ID)
		if recoveryErr != nil {
			if !isMutationFailoverAmbiguous(recoveryErr) {
				unexpectedErr = recoveryErr
				return true
			}
			return false
		}
		recoveredLeases, recoveryErr = cli.Leases(probeCtx)
		if recoveryErr != nil && !isMutationFailoverAmbiguous(recoveryErr) {
			unexpectedErr = recoveryErr
			return true
		}
		return recoveryErr == nil
	}, 45*time.Second, 500*time.Millisecond, "linearizable lease reads must recover after leader restart")
	require.NoError(t, unexpectedErr)
	require.NoError(t, recoveryErr)
	require.Equal(t, lease.ID, recoveredTTL.ID)
	require.Positive(t, recoveredTTL.TTL)
	require.Equal(t, int64(300), recoveredTTL.GrantedTTL)
	recoveredLeaseFound := false
	for _, listed := range recoveredLeases.Leases {
		recoveredLeaseFound = recoveredLeaseFound || listed.ID == lease.ID
	}
	require.True(t, recoveredLeaseFound, "recovered LeaseLeases must include live lease %x", lease.ID)
	stopWorkers()
	wg.Wait()
	close(errs)
	for readErr := range errs {
		require.NoError(t, readErr)
	}
	require.Positive(t, successes.Load())
	require.Positive(t, transientErrors.Load())
	t.Logf("successful lease reads=%d transient errors=%d recovered ttl=%d",
		successes.Load(), transientErrors.Load(), recoveredTTL.TTL)
}
