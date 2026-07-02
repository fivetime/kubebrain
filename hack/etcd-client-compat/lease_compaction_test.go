package compat

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	clientv3 "go.etcd.io/etcd/client/v3"
)

// TestLeaseSurvivesCompaction verifies the lease half of #15 does not regress
// lease correctness: lease records now live in the \x00kubebrain/ namespace that
// compaction sweeps, so a compaction cycle must retire only superseded record
// versions — never the live lease. It grants a lease, binds a key, keepalives
// (each a new record version), forces a compaction at the current revision, and
// asserts the lease is still alive, its bound key still present, its TTL still
// reported, and keepalive still works afterward.
func TestLeaseSurvivesCompaction(t *testing.T) {
	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{compatEndpoint()},
		DialTimeout: 3 * time.Second,
	})
	require.NoError(t, err)
	defer cli.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 40*time.Second)
	defer cancel()

	prefix := testPrefix(t)
	cleanupPrefix(t, newKubernetesClient(t), prefix)

	grant, err := cli.Grant(ctx, 300)
	require.NoError(t, err)
	require.NotZero(t, grant.ID)

	key := prefix + "/leased"
	_, err = cli.Put(ctx, key, "v", clientv3.WithLease(grant.ID))
	require.NoError(t, err)

	// Several keepalives, each rewriting the lease record (a new MVCC version).
	for i := 0; i < 5; i++ {
		ka, err := cli.KeepAliveOnce(ctx, grant.ID)
		require.NoError(t, err)
		require.EqualValues(t, grant.ID, ka.ID)
		require.Positive(t, ka.TTL)
	}

	// Force a compaction at the current revision — this now sweeps the lease
	// keyspace and must retire only the superseded record versions.
	cur, err := cli.Get(ctx, key)
	require.NoError(t, err)
	_, err = cli.Compact(ctx, cur.Header.Revision, clientv3.WithCompactPhysical())
	require.NoError(t, err)

	// The lease and its bound key must both survive.
	g, err := cli.Get(ctx, key)
	require.NoError(t, err)
	require.Len(t, g.Kvs, 1, "bound key must survive compaction")

	// TimeToLive is the authoritative check: the lease is still alive and its
	// key binding survived compaction. (KubeBrain does not populate the Lease
	// field on read responses — a separate compat gap, not exercised here.)
	ttl, err := cli.TimeToLive(ctx, grant.ID, clientv3.WithAttachedKeys())
	require.NoError(t, err)
	require.Positive(t, ttl.TTL, "lease must still be alive after compaction")
	attached := make([]string, 0, len(ttl.Keys))
	for _, k := range ttl.Keys {
		attached = append(attached, string(k))
	}
	require.Contains(t, attached, key, "lease->key binding must survive compaction")

	// Keepalive must still succeed post-compaction (record still readable/writable).
	ka, err := cli.KeepAliveOnce(ctx, grant.ID)
	require.NoError(t, err)
	require.Positive(t, ka.TTL)

	_, err = cli.Revoke(ctx, grant.ID)
	require.NoError(t, err)
}
