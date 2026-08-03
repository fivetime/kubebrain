package compat

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	clientv3 "go.etcd.io/etcd/client/v3"
)

// TestLeaseTimeToLiveReportsZeroBeforeExpiry pins etcd's duration truncation:
// during the final live sub-second TimeToLive returns TTL=0, then TTL=-1 only
// after the lessor removes the lease.
func TestLeaseTimeToLiveReportsZeroBeforeExpiry(t *testing.T) {
	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{compatEndpoint(t)},
		DialTimeout: 3 * time.Second,
	})
	require.NoError(t, err)
	defer func() { require.NoError(t, cli.Close()) }()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	grant, err := cli.Grant(ctx, 2)
	require.NoError(t, err)

	observedZero := false
	for {
		ttl, ttlErr := cli.TimeToLive(ctx, grant.ID)
		require.NoError(t, ttlErr)
		if ttl.TTL == 0 {
			observedZero = true
		}
		if ttl.TTL == -1 {
			break
		}
		select {
		case <-ctx.Done():
			t.Fatal("lease did not expire before timeout")
		case <-time.After(20 * time.Millisecond):
		}
	}
	require.True(t, observedZero, "live lease must report TTL=0 before TTL=-1")
}
