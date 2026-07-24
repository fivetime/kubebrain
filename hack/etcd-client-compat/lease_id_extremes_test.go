package compat

import (
	"context"
	"fmt"
	"math"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
	clientv3 "go.etcd.io/etcd/client/v3"
)

// TestExplicitSignedLeaseIDLifecycle mirrors etcd's negative-ID recovery
// contract at the public API boundary. Explicit IDs use the full int64 range;
// automatic IDs remain positive and independent.
func TestExplicitSignedLeaseIDLifecycle(t *testing.T) {
	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{compatEndpoint()},
		DialTimeout: 3 * time.Second,
	})
	require.NoError(t, err)
	defer func() { require.NoError(t, cli.Close()) }()

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	rawLease := etcdserverpb.NewLeaseClient(cli.ActiveConnection())
	for i, id := range []int64{-1, math.MaxInt64, math.MinInt64} {
		key := fmt.Sprintf("/dbaas-lease-id-extremes/%d/%d", time.Now().UnixNano(), i)
		grant, grantErr := rawLease.LeaseGrant(ctx, &etcdserverpb.LeaseGrantRequest{ID: id, TTL: 30})
		require.NoError(t, grantErr)
		require.Equal(t, id, grant.ID)

		_, err = cli.Put(ctx, key, "value", clientv3.WithLease(clientv3.LeaseID(id)))
		require.NoError(t, err)
		ttl, ttlErr := cli.TimeToLive(ctx, clientv3.LeaseID(id), clientv3.WithAttachedKeys())
		require.NoError(t, ttlErr)
		require.Equal(t, int64(30), ttl.GrantedTTL)
		require.Equal(t, [][]byte{[]byte(key)}, ttl.Keys)

		_, revokeErr := rawLease.LeaseRevoke(ctx, &etcdserverpb.LeaseRevokeRequest{ID: id})
		require.NoError(t, revokeErr)
		got, getErr := cli.Get(ctx, key)
		require.NoError(t, getErr)
		require.Empty(t, got.Kvs)
	}

	auto, err := cli.Grant(ctx, 30)
	require.NoError(t, err)
	require.Positive(t, int64(auto.ID))
	_, err = cli.Revoke(ctx, auto.ID)
	require.NoError(t, err)
}

// TestMaxLeaseIDFailoverKeepsAutomaticIDsPositive is opt-in because it deletes
// the named live leader pod. It proves that reloading an explicit MaxInt64 lease
// does not reseed or overflow the independent automatic-ID generator.
func TestMaxLeaseIDFailoverKeepsAutomaticIDsPositive(t *testing.T) {
	pod := os.Getenv("KUBEBRAIN_LEASE_ID_FAILOVER_POD")
	if pod == "" {
		t.Skip("set KUBEBRAIN_LEASE_ID_FAILOVER_POD to delete a live leader pod")
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

	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	rawLease := etcdserverpb.NewLeaseClient(cli.ActiveConnection())
	grant, err := rawLease.LeaseGrant(ctx, &etcdserverpb.LeaseGrantRequest{ID: math.MaxInt64, TTL: 300})
	require.NoError(t, err)
	require.Equal(t, int64(math.MaxInt64), grant.ID)
	key := fmt.Sprintf("/dbaas-lease-id-failover/%d", time.Now().UnixNano())
	_, err = cli.Put(ctx, key, "survivor", clientv3.WithLease(clientv3.LeaseID(math.MaxInt64)))
	require.NoError(t, err)

	output, err := exec.CommandContext(ctx, "kubectl", "-n", namespace, "delete", "pod", pod, "--wait=false").CombinedOutput()
	require.NoErrorf(t, err, "delete leader pod: %s", strings.TrimSpace(string(output)))
	output, err = waitForKubeBrainRollout(t, ctx, namespace)
	require.NoErrorf(t, err, "wait for KubeBrain recovery: %s", strings.TrimSpace(string(output)))

	ttl, err := cli.TimeToLive(ctx, clientv3.LeaseID(math.MaxInt64), clientv3.WithAttachedKeys())
	require.NoError(t, err)
	require.Positive(t, ttl.TTL)
	require.Equal(t, [][]byte{[]byte(key)}, ttl.Keys)
	got, err := cli.Get(ctx, key)
	require.NoError(t, err)
	require.Len(t, got.Kvs, 1)

	auto, err := cli.Grant(ctx, 30)
	require.NoError(t, err)
	require.Positive(t, int64(auto.ID))
	require.NotEqual(t, clientv3.LeaseID(math.MaxInt64), auto.ID)
	_, err = cli.Revoke(ctx, auto.ID)
	require.NoError(t, err)
	_, err = cli.Revoke(ctx, clientv3.LeaseID(math.MaxInt64))
	require.NoError(t, err)
}
