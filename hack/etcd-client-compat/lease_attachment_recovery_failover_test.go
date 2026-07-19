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

func TestLeaseAttachmentRecoveryAcrossLeaderReplacement(t *testing.T) {
	endpoint := os.Getenv("KUBEBRAIN_ETCD_ENDPOINT")
	pod := linearizabilityDeletePod()
	if endpoint == "" || pod == "" {
		t.Skip("set KUBEBRAIN_ETCD_ENDPOINT and LINEARIZABILITY_DELETE_POD to run lease recovery failover")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	t.Cleanup(cancel)
	cli, err := clientv3.New(clientv3.Config{
		Endpoints: []string{endpoint}, DialTimeout: 3 * time.Second,
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, cli.Close()) })

	prefix := fmt.Sprintf("/dbaas-lease-recovery/%d/", time.Now().UnixNano())
	detachedKey := prefix + "detached"
	reboundKey := prefix + "rebound"
	revokedKey := prefix + "revoked"
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cleanupCancel()
		_, _ = cli.Delete(cleanupCtx, prefix, clientv3.WithPrefix())
	})

	detached := grantLeaseForRecovery(t, ctx, cli)
	_, err = cli.Put(ctx, detachedKey, "leased", clientv3.WithLease(detached.ID))
	require.NoError(t, err)
	_, err = cli.Put(ctx, detachedKey, "detached")
	require.NoError(t, err)

	oldLease := grantLeaseForRecovery(t, ctx, cli)
	currentLease := grantLeaseForRecovery(t, ctx, cli)
	_, err = cli.Put(ctx, reboundKey, "old", clientv3.WithLease(oldLease.ID))
	require.NoError(t, err)
	_, err = cli.Put(ctx, reboundKey, "current", clientv3.WithLease(currentLease.ID))
	require.NoError(t, err)

	revoked := grantLeaseForRecovery(t, ctx, cli)
	_, err = cli.Put(ctx, revokedKey, "revoked", clientv3.WithLease(revoked.ID))
	require.NoError(t, err)
	_, err = cli.Revoke(ctx, revoked.ID)
	require.NoError(t, err)

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
		for _, id := range []clientv3.LeaseID{detached.ID, oldLease.ID, currentLease.ID} {
			ttl, ttlErr := cli.TimeToLive(ctx, id)
			if ttlErr != nil || ttl.TTL <= 0 {
				return false
			}
		}
		ttl, ttlErr := cli.TimeToLive(ctx, revoked.ID)
		return ttlErr == nil && ttl.TTL == -1
	}, 30*time.Second, 100*time.Millisecond)

	_, err = cli.Revoke(ctx, detached.ID)
	require.NoError(t, err)
	assertRecoveryKey(t, ctx, cli, detachedKey, "detached", 0)

	_, err = cli.Revoke(ctx, oldLease.ID)
	require.NoError(t, err)
	assertRecoveryKey(t, ctx, cli, reboundKey, "current", int64(currentLease.ID))

	assertRecoveryKeyAbsent(t, ctx, cli, revokedKey)

	_, err = cli.Revoke(ctx, currentLease.ID)
	require.NoError(t, err)
	assertRecoveryKeyAbsent(t, ctx, cli, reboundKey)
}

func grantLeaseForRecovery(t *testing.T, ctx context.Context, cli *clientv3.Client) *clientv3.LeaseGrantResponse {
	t.Helper()
	lease, err := cli.Grant(ctx, 300)
	require.NoError(t, err)
	return lease
}

func assertRecoveryKey(t *testing.T, ctx context.Context, cli *clientv3.Client, key, value string, lease int64) {
	t.Helper()
	resp, err := cli.Get(ctx, key)
	require.NoError(t, err)
	require.Len(t, resp.Kvs, 1)
	require.Equal(t, value, string(resp.Kvs[0].Value))
	require.Equal(t, lease, resp.Kvs[0].Lease)
}

func assertRecoveryKeyAbsent(t *testing.T, ctx context.Context, cli *clientv3.Client, key string) {
	t.Helper()
	resp, err := cli.Get(ctx, key)
	require.NoError(t, err)
	require.Empty(t, resp.Kvs)
}
