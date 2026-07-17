package compat

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	clientv3 "go.etcd.io/etcd/client/v3"
)

func TestLeaseKeepAliveAtZeroTTLSurvivesOriginalDeadline(t *testing.T) {
	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{compatEndpoint()},
		DialTimeout: 3 * time.Second,
	})
	require.NoError(t, err)
	defer func() { require.NoError(t, cli.Close()) }()

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	key := fmt.Sprintf("/dbaas-lease-keepalive-expiry-race/%d", time.Now().UnixNano())
	grant, err := cli.Grant(ctx, 2)
	require.NoError(t, err)
	defer func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cleanupCancel()
		_, _ = cli.Revoke(cleanupCtx, grant.ID)
	}()
	_, err = cli.Put(ctx, key, "value", clientv3.WithLease(grant.ID))
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		ttl, ttlErr := cli.TimeToLive(ctx, grant.ID)
		return ttlErr == nil && ttl.TTL == 0
	}, 2*time.Second, 25*time.Millisecond, "lease never entered its live final subsecond")

	renewed, err := cli.KeepAliveOnce(ctx, grant.ID)
	require.NoError(t, err)
	require.Equal(t, int64(2), renewed.TTL)

	// The pre-renewal deadline is less than one second away. Waiting longer than
	// that proves an already-queued expiry callback cannot revoke the renewal.
	time.Sleep(1200 * time.Millisecond)
	ttl, err := cli.TimeToLive(ctx, grant.ID)
	require.NoError(t, err)
	require.NotEqual(t, int64(-1), ttl.TTL)
	got, err := cli.Get(ctx, key)
	require.NoError(t, err)
	require.Len(t, got.Kvs, 1)
	require.Equal(t, int64(grant.ID), got.Kvs[0].Lease)
}
