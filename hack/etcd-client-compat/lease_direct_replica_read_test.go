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
	requireDistinctDirectReplicaTopology(t, endpoints)

	service, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{compatEndpoint(t)},
		DialTimeout: 5 * time.Second,
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, service.Close()) })

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	key := fmt.Sprintf("/dbaas-direct-replica-lease/%d", time.Now().UnixNano())
	grant, err := service.Grant(ctx, 300)
	require.NoError(t, err)
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cleanupCancel()
		beforeRevoke, ttlErr := service.TimeToLive(cleanupCtx, grant.ID)
		require.NoError(t, ttlErr)
		if beforeRevoke.TTL != -1 {
			_, revokeErr := service.Revoke(cleanupCtx, grant.ID)
			require.NoError(t, revokeErr)
		}
		_, deleteErr := service.Delete(cleanupCtx, key)
		require.NoError(t, deleteErr)
		remaining, getErr := service.Get(cleanupCtx, key)
		require.NoError(t, getErr)
		require.Zero(t, remaining.Count)
		ttl, ttlErr := service.TimeToLive(cleanupCtx, grant.ID, clientv3.WithAttachedKeys())
		require.NoError(t, ttlErr)
		require.Equal(t, int64(-1), ttl.TTL)
		require.Empty(t, ttl.Keys)
	})
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
		listed, listErr := replica.Leases(ctx)
		require.NoError(t, listErr)
		require.True(t, leaseListContains(listed, grant.ID),
			"direct replica %s omitted live lease %x", endpoint, grant.ID)
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
		listed, listErr := replica.Leases(ctx)
		require.NoError(t, listErr)
		require.False(t, leaseListContains(listed, grant.ID),
			"direct replica still listed revoked lease %x", grant.ID)
	}
}

func leaseListContains(response *clientv3.LeaseLeasesResponse, id clientv3.LeaseID) bool {
	for _, lease := range response.Leases {
		if lease.ID == id {
			return true
		}
	}
	return false
}
