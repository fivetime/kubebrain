package compat

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestLeaseFailoverSpreadsRecoveredExpiryPileup(t *testing.T) {
	failoverCommand := os.Getenv("KUBEBRAIN_LEASE_EXPIRY_SPREAD_FAILOVER_COMMAND")
	if failoverCommand == "" {
		t.Skip("set KUBEBRAIN_LEASE_EXPIRY_SPREAD_FAILOVER_COMMAND to delete the current live leader")
	}
	namespace := os.Getenv("KUBEBRAIN_FAILOVER_NAMESPACE")
	if namespace == "" {
		namespace = "kubebrain-dev"
	}

	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{compatEndpoint()},
		DialTimeout: 3 * time.Second,
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, cli.Close()) })
	rawLease := etcdserverpb.NewLeaseClient(cli.ActiveConnection())

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	const leaseCount = 1200
	baseID := time.Now().UnixNano() & 0x3fffffffffffffff
	ids := make([]int64, leaseCount)
	for i := range ids {
		ids[i] = baseID + int64(i)
		_, err = rawLease.LeaseGrant(ctx, &etcdserverpb.LeaseGrantRequest{ID: ids[i], TTL: 600})
		require.NoError(t, err)
	}
	t.Cleanup(func() {
		cleanupCli, cleanupErr := clientv3.New(clientv3.Config{
			Endpoints:   []string{compatEndpoint()},
			DialTimeout: 3 * time.Second,
		})
		require.NoError(t, cleanupErr)
		defer func() { require.NoError(t, cleanupCli.Close()) }()
		cleanupLease := etcdserverpb.NewLeaseClient(cleanupCli.ActiveConnection())
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 2*time.Minute)
		defer cleanupCancel()
		var wg sync.WaitGroup
		work := make(chan int64)
		errs := make(chan error, len(ids))
		for range 8 {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for id := range work {
					for cleanupCtx.Err() == nil {
						_, revokeErr := cleanupLease.LeaseRevoke(cleanupCtx, &etcdserverpb.LeaseRevokeRequest{ID: id})
						if revokeErr == nil || status.Code(revokeErr) == codes.NotFound {
							break
						}
						time.Sleep(100 * time.Millisecond)
					}
					if cleanupCtx.Err() != nil {
						errs <- fmt.Errorf("revoke lease %d: %w", id, cleanupCtx.Err())
					}
				}
			}()
		}
		for _, id := range ids {
			work <- id
		}
		close(work)
		wg.Wait()
		close(errs)
		for cleanupErr := range errs {
			require.NoError(t, cleanupErr)
		}
		listed, listErr := cleanupLease.LeaseLeases(cleanupCtx, &etcdserverpb.LeaseLeasesRequest{})
		require.NoError(t, listErr)
		for _, lease := range listed.Leases {
			require.Falsef(t, lease.ID >= ids[0] && lease.ID <= ids[len(ids)-1],
				"test lease %d remained after cleanup", lease.ID)
		}
	})

	output, err := exec.CommandContext(ctx, "bash", "-c", failoverCommand).CombinedOutput()
	require.NoErrorf(t, err, "failover command: %s", strings.TrimSpace(string(output)))
	output, err = waitForKubeBrainRollout(ctx, namespace)
	require.NoErrorf(t, err, "wait for KubeBrain recovery: %s", strings.TrimSpace(string(output)))

	require.Eventually(t, func() bool {
		type result struct {
			ttl int64
			err error
		}
		firstC := make(chan result, 1)
		lastC := make(chan result, 1)
		go func() {
			resp, ttlErr := rawLease.LeaseTimeToLive(ctx, &etcdserverpb.LeaseTimeToLiveRequest{ID: ids[0]})
			if ttlErr != nil {
				firstC <- result{err: ttlErr}
				return
			}
			firstC <- result{ttl: resp.TTL}
		}()
		go func() {
			resp, ttlErr := rawLease.LeaseTimeToLive(ctx, &etcdserverpb.LeaseTimeToLiveRequest{ID: ids[len(ids)-1]})
			if ttlErr != nil {
				lastC <- result{err: ttlErr}
				return
			}
			lastC <- result{ttl: resp.TTL}
		}()
		first, last := <-firstC, <-lastC
		return first.err == nil && last.err == nil && last.ttl >= first.ttl+1
	}, 30*time.Second, 250*time.Millisecond,
		fmt.Sprintf("lease %d should expire later than lease %d after promotion spreading", ids[len(ids)-1], ids[0]))
}

func TestEmptyLeaseRevokeRemainsDeletedAfterFailover(t *testing.T) {
	failoverCommand := os.Getenv("KUBEBRAIN_LEASE_EXPIRY_SPREAD_FAILOVER_COMMAND")
	if failoverCommand == "" {
		t.Skip("set KUBEBRAIN_LEASE_EXPIRY_SPREAD_FAILOVER_COMMAND to delete the current live leader")
	}
	namespace := os.Getenv("KUBEBRAIN_FAILOVER_NAMESPACE")
	if namespace == "" {
		namespace = "kubebrain-dev"
	}

	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{compatEndpoint()},
		DialTimeout: 3 * time.Second,
	})
	require.NoError(t, err)
	defer func() { require.NoError(t, cli.Close()) }()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	lease, err := cli.Grant(ctx, 300)
	require.NoError(t, err)
	_, err = cli.Revoke(ctx, lease.ID)
	require.NoError(t, err)
	ttl, err := cli.TimeToLive(ctx, lease.ID)
	require.NoError(t, err)
	require.Equal(t, int64(-1), ttl.TTL)

	output, err := exec.CommandContext(ctx, "bash", "-c", failoverCommand).CombinedOutput()
	require.NoErrorf(t, err, "failover command: %s", strings.TrimSpace(string(output)))
	output, err = waitForKubeBrainRollout(ctx, namespace)
	require.NoErrorf(t, err, "wait for KubeBrain recovery: %s", strings.TrimSpace(string(output)))

	require.Eventually(t, func() bool {
		ttl, ttlErr := cli.TimeToLive(ctx, lease.ID)
		if ttlErr != nil || ttl.TTL != -1 {
			return false
		}
		leases, listErr := cli.Leases(ctx)
		if listErr != nil {
			return false
		}
		for _, listed := range leases.Leases {
			if listed.ID == lease.ID {
				return false
			}
		}
		return true
	}, 30*time.Second, 250*time.Millisecond)
}
