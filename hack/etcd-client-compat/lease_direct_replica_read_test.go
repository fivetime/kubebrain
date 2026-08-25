package compat

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
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
	identity := newLiveResponseIdentityAdmission(t)
	key := fmt.Sprintf("/dbaas-direct-replica-lease/%d", time.Now().UnixNano())
	grant, err := service.Grant(ctx, 300)
	require.NoError(t, err)
	require.NoError(t, identity.admitHeader(0, grant.ResponseHeader, 1))
	minimumRevision := grant.ResponseHeader.Revision
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cleanupCancel()
		beforeRevoke, ttlErr := service.TimeToLive(cleanupCtx, grant.ID)
		require.NoError(t, ttlErr)
		require.NoError(t, identity.admitHeader(0, beforeRevoke.ResponseHeader, minimumRevision))
		if beforeRevoke.TTL != -1 {
			revoked, revokeErr := service.Revoke(cleanupCtx, grant.ID)
			require.NoError(t, revokeErr)
			require.NoError(t, identity.admitHeader(0, revoked.Header, minimumRevision))
		}
		deleted, deleteErr := service.Delete(cleanupCtx, key)
		require.NoError(t, deleteErr)
		require.NoError(t, identity.admitHeader(0, deleted.Header, minimumRevision))
		remaining, getErr := service.Get(cleanupCtx, key)
		require.NoError(t, getErr)
		require.NoError(t, identity.admitHeader(0, remaining.Header, minimumRevision))
		require.Zero(t, remaining.Count)
		ttl, ttlErr := service.TimeToLive(cleanupCtx, grant.ID, clientv3.WithAttachedKeys())
		require.NoError(t, ttlErr)
		require.NoError(t, identity.admitHeader(0, ttl.ResponseHeader, minimumRevision))
		require.Equal(t, int64(-1), ttl.TTL)
		require.Empty(t, ttl.Keys)
	})
	put, err := service.Put(ctx, key, "value", clientv3.WithLease(grant.ID))
	require.NoError(t, err)
	require.NoError(t, identity.admitHeader(0, put.Header, minimumRevision+1))
	minimumRevision = put.Header.Revision

	replicas := make([]*clientv3.Client, 0, len(endpoints))
	for index, endpoint := range endpoints {
		replica, clientErr := clientv3.New(clientv3.Config{
			Endpoints:   []string{strings.TrimSpace(endpoint)},
			DialTimeout: 5 * time.Second,
		})
		require.NoError(t, clientErr)
		replicas = append(replicas, replica)
		t.Cleanup(func() { require.NoError(t, replica.Close()) })

		got, getErr := replica.Get(ctx, key)
		require.NoError(t, getErr)
		require.NoError(t, identity.admitHeader(index, got.Header, minimumRevision))
		require.Len(t, got.Kvs, 1)
		require.Equal(t, int64(grant.ID), got.Kvs[0].Lease)
		ttl, ttlErr := replica.TimeToLive(ctx, grant.ID, clientv3.WithAttachedKeys())
		require.NoError(t, ttlErr)
		require.NoError(t, identity.admitHeader(index, ttl.ResponseHeader, minimumRevision))
		require.Positive(t, ttl.TTL)
		require.Equal(t, [][]byte{[]byte(key)}, ttl.Keys)
		listed, listErr := replica.Leases(ctx)
		require.NoError(t, listErr)
		require.NoError(t, identity.admitHeader(index, listed.ResponseHeader, minimumRevision))
		require.True(t, leaseListContains(listed, grant.ID),
			"direct replica %s omitted live lease %x", endpoint, grant.ID)

		maintenance := etcdserverpb.NewMaintenanceClient(replica.ActiveConnection())
		statusResponse, statusErr := maintenance.Status(ctx, &etcdserverpb.StatusRequest{})
		require.NoError(t, statusErr)
		require.NotNil(t, statusResponse.Header)
		require.NoError(t, identity.admitHeader(index, statusResponse.Header, minimumRevision))
		keepAlive, keepAliveErr := etcdserverpb.NewLeaseClient(replica.ActiveConnection()).LeaseKeepAlive(ctx)
		require.NoError(t, keepAliveErr)
		require.NoError(t, keepAlive.Send(&etcdserverpb.LeaseKeepAliveRequest{ID: int64(grant.ID)}))
		kept, keepAliveErr := keepAlive.Recv()
		require.NoError(t, keepAliveErr)
		require.NotNil(t, kept.Header)
		require.NoError(t, identity.admitHeader(index, kept.Header, minimumRevision))
		require.Equal(t, int64(grant.ID), kept.ID)
		require.Positive(t, kept.TTL)
		require.Equal(t, statusResponse.Header.ClusterId, kept.Header.ClusterId,
			"keepalive cluster id must belong to direct replica %s", endpoint)
		require.Equal(t, statusResponse.Header.MemberId, kept.Header.MemberId,
			"keepalive member id must identify direct replica %s", endpoint)
		require.Positive(t, kept.Header.RaftTerm)
		require.NoError(t, keepAlive.CloseSend())
	}

	revoked, err := service.Revoke(ctx, grant.ID)
	require.NoError(t, err)
	require.NoError(t, identity.admitHeader(0, revoked.Header, minimumRevision+1))
	minimumRevision = revoked.Header.Revision
	for index, replica := range replicas {
		// A linearizable read establishes that this direct member has observed the
		// revoke before the nonexistent-lease checks, matching upstream's barrier.
		got, getErr := replica.Get(ctx, key)
		require.NoError(t, getErr)
		require.NoError(t, identity.admitHeader(index, got.Header, minimumRevision))
		require.Empty(t, got.Kvs)
		ttl, ttlErr := replica.TimeToLive(ctx, grant.ID, clientv3.WithAttachedKeys())
		require.NoError(t, ttlErr)
		require.NoError(t, identity.admitHeader(index, ttl.ResponseHeader, minimumRevision))
		require.Equal(t, int64(-1), ttl.TTL)
		require.Empty(t, ttl.Keys)
		listed, listErr := replica.Leases(ctx)
		require.NoError(t, listErr)
		require.NoError(t, identity.admitHeader(index, listed.ResponseHeader, minimumRevision))
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
