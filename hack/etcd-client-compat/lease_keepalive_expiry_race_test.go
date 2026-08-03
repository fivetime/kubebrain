package compat

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
	clientv3 "go.etcd.io/etcd/client/v3"
)

func TestLeaseKeepAliveAtZeroTTLSurvivesOriginalDeadline(t *testing.T) {
	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{compatEndpoint(t)},
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

func TestExpiredKeepAliveResponseFollowsKeyDeletion(t *testing.T) {
	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{compatEndpoint(t)},
		DialTimeout: 3 * time.Second,
	})
	require.NoError(t, err)
	defer func() { require.NoError(t, cli.Close()) }()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	const leases = 64
	type leaseKey struct {
		id  clientv3.LeaseID
		key string
	}
	items := make([]leaseKey, 0, leases)
	prefix := fmt.Sprintf("/dbaas-expired-keepalive-order/%d/", time.Now().UnixNano())
	for i := 0; i < leases; i++ {
		grant, grantErr := cli.Grant(ctx, 2)
		require.NoError(t, grantErr)
		key := fmt.Sprintf("%s%02d", prefix, i)
		_, putErr := cli.Put(ctx, key, "value", clientv3.WithLease(grant.ID))
		require.NoError(t, putErr)
		items = append(items, leaseKey{id: grant.ID, key: key})
	}

	time.Sleep(2 * time.Second)
	var wg sync.WaitGroup
	errs := make(chan error, leases)
	for _, item := range items {
		item := item
		wg.Add(1)
		go func() {
			defer wg.Done()
			renewed, keepAliveErr := cli.KeepAliveOnce(ctx, item.id)
			got, getErr := cli.Get(ctx, item.key)
			if getErr != nil {
				errs <- fmt.Errorf("get %q: %w", item.key, getErr)
				return
			}
			switch {
			case keepAliveErr == nil:
				if renewed == nil || renewed.TTL <= 0 || len(got.Kvs) != 1 {
					errs <- fmt.Errorf("successful renewal %d returned %+v with %d keys", item.id, renewed, len(got.Kvs))
					return
				}
				if got.Kvs[0].Lease != int64(item.id) {
					errs <- fmt.Errorf("renewed key %q carries lease %d, want %d", item.key, got.Kvs[0].Lease, item.id)
				}
			case errors.Is(keepAliveErr, rpctypes.ErrLeaseNotFound):
				if len(got.Kvs) != 0 {
					errs <- fmt.Errorf("not-found renewal %d returned before attached key deletion", item.id)
				}
			default:
				errs <- fmt.Errorf("keepalive %d: %w", item.id, keepAliveErr)
			}
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		require.NoError(t, err)
	}
}
