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
	namespace := os.Getenv("KUBEBRAIN_FAILOVER_NAMESPACE")
	if namespace == "" {
		namespace = "kubebrain-dev"
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
	errs := make(chan error, 64)
	var successes atomic.Int64
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
				readCtx, readCancel := context.WithTimeout(ctx, time.Second)
				ttl, ttlErr := cli.TimeToLive(readCtx, lease.ID)
				if ttlErr == nil {
					if ttl.ID != lease.ID || ttl.TTL <= 0 || ttl.GrantedTTL != 300 {
						errs <- fmt.Errorf("stale TTL response: id=%x ttl=%d granted=%d", ttl.ID, ttl.TTL, ttl.GrantedTTL)
						readCancel()
						return
					}
					successes.Add(1)
				}
				leases, listErr := cli.Leases(readCtx)
				if listErr == nil {
					found := false
					for _, listed := range leases.Leases {
						found = found || listed.ID == lease.ID
					}
					if !found {
						errs <- fmt.Errorf("successful LeaseLeases omitted live lease %x", lease.ID)
						readCancel()
						return
					}
					successes.Add(1)
				}
				readCancel()
				// Two RPCs per iteration across 16 workers stay below the
				// production 2,000 request/s admission limit. The test targets
				// authoritative failover reads, not rate-limit saturation.
				time.Sleep(20 * time.Millisecond)
			}
		}()
	}

	output, err := runCompatShellCommandContext(t, ctx, failoverCommand)
	require.NoErrorf(t, err, "failover command: %s", strings.TrimSpace(string(output)))
	output, err = waitForKubeBrainRollout(t, ctx, namespace)
	require.NoErrorf(t, err, "wait for KubeBrain recovery: %s", strings.TrimSpace(string(output)))
	close(stop)
	wg.Wait()
	close(errs)
	for readErr := range errs {
		require.NoError(t, readErr)
	}
	require.Positive(t, successes.Load())

	require.Eventually(t, func() bool {
		ttl, ttlErr := cli.TimeToLive(ctx, lease.ID)
		return ttlErr == nil && ttl.ID == lease.ID && ttl.TTL > 0 && ttl.GrantedTTL == 300
	}, 30*time.Second, 250*time.Millisecond)
}
