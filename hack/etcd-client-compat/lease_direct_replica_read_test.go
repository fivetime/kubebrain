package compat

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	clientv3 "go.etcd.io/etcd/client/v3"
)

func TestLeaseReadAndRevokeAcrossDirectReplicas(t *testing.T) {
	rawEndpoints := os.Getenv("KUBEBRAIN_DIRECT_ENDPOINTS")
	if rawEndpoints == "" {
		t.Skip("set KUBEBRAIN_DIRECT_ENDPOINTS to comma-separated direct replica endpoints")
	}
	endpoints := strings.Split(rawEndpoints, ",")
	require.GreaterOrEqual(t, len(endpoints), 3)

	service, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{compatEndpoint()},
		DialTimeout: 5 * time.Second,
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, service.Close()) })

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	grant, err := service.Grant(ctx, 300)
	require.NoError(t, err)
	key := fmt.Sprintf("/dbaas-direct-replica-lease/%d", time.Now().UnixNano())
	_, err = service.Put(ctx, key, "value", clientv3.WithLease(grant.ID))
	require.NoError(t, err)

	replicas := make([]*clientv3.Client, 0, len(endpoints))
	for _, endpoint := range endpoints {
		replica, clientErr := clientv3.New(clientv3.Config{
			Endpoints:   []string{strings.TrimSpace(endpoint)},
			DialTimeout: 5 * time.Second,
		})
		require.NoError(t, clientErr)
		replicas = append(replicas, replica)
		t.Cleanup(func() { require.NoError(t, replica.Close()) })

		got, getErr := replica.Get(ctx, key)
		require.NoError(t, getErr)
		require.Len(t, got.Kvs, 1)
		require.Equal(t, int64(grant.ID), got.Kvs[0].Lease)
		ttl, ttlErr := replica.TimeToLive(ctx, grant.ID, clientv3.WithAttachedKeys())
		require.NoError(t, ttlErr)
		require.Positive(t, ttl.TTL)
		require.Equal(t, [][]byte{[]byte(key)}, ttl.Keys)
	}

	_, err = service.Revoke(ctx, grant.ID)
	require.NoError(t, err)
	for _, replica := range replicas {
		// A linearizable read establishes that this direct member has observed the
		// revoke before the nonexistent-lease checks, matching upstream's barrier.
		got, getErr := replica.Get(ctx, key)
		require.NoError(t, getErr)
		require.Empty(t, got.Kvs)
		ttl, ttlErr := replica.TimeToLive(ctx, grant.ID, clientv3.WithAttachedKeys())
		require.NoError(t, ttlErr)
		require.Equal(t, int64(-1), ttl.TTL)
		require.Empty(t, ttl.Keys)
	}
}
