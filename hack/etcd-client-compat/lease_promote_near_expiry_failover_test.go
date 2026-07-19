package compat

import (
	"context"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	clientv3 "go.etcd.io/etcd/client/v3"
)

func TestNearExpiryLeaseIsRefreshedOnLeaderPromotion(t *testing.T) {
	endpoint := os.Getenv("KUBEBRAIN_ETCD_ENDPOINT")
	pod := linearizabilityDeletePod()
	if endpoint == "" || pod == "" {
		t.Skip("set KUBEBRAIN_ETCD_ENDPOINT and LINEARIZABILITY_DELETE_POD to run lease promotion failover")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	t.Cleanup(cancel)
	cli, err := clientv3.New(clientv3.Config{
		Endpoints: []string{endpoint}, DialTimeout: 3 * time.Second,
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, cli.Close()) })

	key := fmt.Sprintf("/dbaas-lease-promote/%d", time.Now().UnixNano())
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cleanupCancel()
		_, _ = cli.Delete(cleanupCtx, key)
	})
	lease, err := cli.Grant(ctx, 3)
	require.NoError(t, err)
	require.Equal(t, int64(3), lease.TTL)
	_, err = cli.Put(ctx, key, "value", clientv3.WithLease(lease.ID))
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		ttl, ttlErr := cli.TimeToLive(ctx, lease.ID)
		return ttlErr == nil && ttl.TTL == 1
	}, 3*time.Second, 50*time.Millisecond)

	var operationClock atomic.Int64
	operationClock.Store(20)
	errCh := make(chan error, 1)
	var fault sync.WaitGroup
	startLinearizabilityPodDeletion(ctx, &operationClock, pod, errCh, &fault)
	fault.Wait()
	close(errCh)
	for faultErr := range errCh {
		require.NoError(t, faultErr)
	}

	require.Eventually(t, func() bool {
		attemptCtx, attemptCancel := context.WithTimeout(ctx, 300*time.Millisecond)
		defer attemptCancel()
		ttl, ttlErr := cli.TimeToLive(attemptCtx, lease.ID)
		if ttlErr != nil || ttl.TTL <= 0 {
			return false
		}
		got, getErr := cli.Get(attemptCtx, key)
		return getErr == nil && len(got.Kvs) == 1 && got.Kvs[0].Lease == int64(lease.ID)
	}, 30*time.Second, 25*time.Millisecond)

	require.Eventually(t, func() bool {
		attemptCtx, attemptCancel := context.WithTimeout(ctx, 300*time.Millisecond)
		defer attemptCancel()
		ttl, ttlErr := cli.TimeToLive(attemptCtx, lease.ID)
		if ttlErr != nil || ttl.TTL != -1 {
			return false
		}
		resp, getErr := cli.Get(attemptCtx, key)
		return getErr == nil && len(resp.Kvs) == 0
	}, 15*time.Second, 25*time.Millisecond)
}
