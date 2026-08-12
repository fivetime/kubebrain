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

// TestShortLeaseKeepAliveSurvivesExpiryBurst proves that TiKV deletion of
// unrelated expired leases cannot consume a live lease's complete TTL=3
// keepalive budget. This is the black-box counterpart of
// TestSlowLeaseExpiryDoesNotBlockUnrelatedRenewal.
func TestShortLeaseKeepAliveSurvivesExpiryBurst(t *testing.T) {
	endpoint := os.Getenv("KUBEBRAIN_LEASE_EXPIRY_ISOLATION_ENDPOINT")
	if endpoint == "" {
		t.Skip("set KUBEBRAIN_LEASE_EXPIRY_ISOLATION_ENDPOINT to run the real TiKV expiry-isolation gate")
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	t.Cleanup(cancel)
	client, err := clientv3.New(clientv3.Config{
		Endpoints: []string{endpoint}, DialTimeout: 5 * time.Second,
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, client.Close()) })

	prefix := fmt.Sprintf("/compat/lease-expiry-isolation/%d/", time.Now().UnixNano())
	live, err := client.Grant(ctx, 3)
	require.NoError(t, err)
	liveKey := prefix + "live"
	_, err = client.Put(ctx, liveKey, "live", clientv3.WithLease(live.ID))
	require.NoError(t, err)
	keepalives, err := client.KeepAlive(ctx, live.ID)
	require.NoError(t, err)
	require.Positive(t, receivePositiveKeepAlive(t, ctx, keepalives, live.ID).TTL)

	keepaliveErr := make(chan error, 1)
	var keepaliveResponses atomic.Int64
	go func() {
		for response := range keepalives {
			if response == nil || response.ID != live.ID || response.TTL <= 0 {
				keepaliveErr <- fmt.Errorf("invalid live keepalive response: %#v", response)
				return
			}
			keepaliveResponses.Add(1)
		}
		select {
		case <-ctx.Done():
		default:
			keepaliveErr <- fmt.Errorf("live TTL=3 keepalive channel closed during expiry burst")
		}
	}()

	const burstLeases = 64
	var createWG sync.WaitGroup
	createErrs := make(chan error, burstLeases)
	for index := range burstLeases {
		createWG.Add(1)
		go func() {
			defer createWG.Done()
			grant, grantErr := client.Grant(ctx, 3)
			if grantErr != nil {
				createErrs <- grantErr
				return
			}
			key := fmt.Sprintf("%sburst-%02d", prefix, index)
			if _, putErr := client.Put(ctx, key, "expire", clientv3.WithLease(grant.ID)); putErr != nil {
				createErrs <- putErr
			}
		}()
	}
	createWG.Wait()
	close(createErrs)
	for createErr := range createErrs {
		require.NoError(t, createErr)
	}

	require.Eventually(t, func() bool {
		response, getErr := client.Get(ctx, prefix+"burst-", clientv3.WithPrefix(), clientv3.WithCountOnly())
		return getErr == nil && response.Count == 0
	}, 20*time.Second, 200*time.Millisecond, "all burst leases must expire through real TiKV")
	require.GreaterOrEqual(t, keepaliveResponses.Load(), int64(2),
		"live lease must receive repeated keepalive responses while burst leases expire")
	select {
	case streamErr := <-keepaliveErr:
		require.NoError(t, streamErr)
	default:
	}
	liveTTL, err := client.TimeToLive(ctx, live.ID)
	require.NoError(t, err)
	require.Positive(t, liveTTL.TTL)
	liveGet, err := client.Get(ctx, liveKey)
	require.NoError(t, err)
	require.Len(t, liveGet.Kvs, 1)
	require.Equal(t, int64(live.ID), liveGet.Kvs[0].Lease)
	require.NoError(t, revokeLease(ctx, client, live.ID))
}
