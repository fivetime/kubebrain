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
		Endpoints:   []string{compatEndpoint(t)},
		DialTimeout: 3 * time.Second,
	})
	require.NoError(t, err)
	defer cli.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 40*time.Second)
	defer cancel()
	identity := &liveResponseIdentityAdmission{}

	prefix := testPrefix(t)
	cleanupPrefix(t, newKubernetesClient(t), prefix)

	grant, err := cli.Grant(ctx, 300)
	require.NoError(t, err)
	require.NoError(t, identity.admitHeader(0, grant.ResponseHeader, 1))
	require.NotZero(t, grant.ID)

	key := prefix + "/leased"
	put, err := cli.Put(ctx, key, "v", clientv3.WithLease(grant.ID))
	require.NoError(t, err)
	require.NoError(t, identity.admitHeader(0, put.Header, grant.ResponseHeader.Revision))

	// Several keepalives, each rewriting the lease record (a new MVCC version).
	for i := 0; i < 5; i++ {
		ka, err := cli.KeepAliveOnce(ctx, grant.ID)
		require.NoError(t, err)
		require.NoError(t, identity.admitHeader(0, ka.ResponseHeader, put.Header.Revision))
		require.EqualValues(t, grant.ID, ka.ID)
		require.Positive(t, ka.TTL)
	}

	// Force a compaction at the current revision — this now sweeps the lease
	// keyspace and must retire only the superseded record versions.
	cur, err := cli.Get(ctx, key)
	require.NoError(t, err)
	require.NoError(t, identity.admitHeader(0, cur.Header, put.Header.Revision))
	require.Len(t, cur.Kvs, 1)
	require.Equal(t, int64(grant.ID), cur.Kvs[0].Lease,
		"bound key must report its lease before compaction")
	compacted, err := cli.Compact(ctx, cur.Header.Revision, clientv3.WithCompactPhysical())
	require.NoError(t, err)
	require.NoError(t, identity.admitHeader(0, compacted.Header, cur.Header.Revision))

	// The lease and its bound key must both survive.
	g, err := cli.Get(ctx, key)
	require.NoError(t, err)
	require.NoError(t, identity.admitHeader(0, g.Header, cur.Header.Revision))
	require.Len(t, g.Kvs, 1, "bound key must survive compaction")
	require.Equal(t, int64(grant.ID), g.Kvs[0].Lease,
		"bound key must preserve its lease metadata after compaction")

	// TimeToLive is the authoritative check: the lease is still alive and its
	// reverse key binding survived compaction. Range above independently verifies
	// that the forward KeyValue lease metadata remains visible to etcd clients.
	ttl, err := cli.TimeToLive(ctx, grant.ID, clientv3.WithAttachedKeys())
	require.NoError(t, err)
	require.NoError(t, identity.admitHeader(0, ttl.ResponseHeader, cur.Header.Revision))
	require.Positive(t, ttl.TTL, "lease must still be alive after compaction")
	attached := make([]string, 0, len(ttl.Keys))
	for _, k := range ttl.Keys {
		attached = append(attached, string(k))
	}
	require.Contains(t, attached, key, "lease->key binding must survive compaction")

	// Keepalive must still succeed post-compaction (record still readable/writable).
	ka, err := cli.KeepAliveOnce(ctx, grant.ID)
	require.NoError(t, err)
	require.NoError(t, identity.admitHeader(0, ka.ResponseHeader, cur.Header.Revision))
	require.Positive(t, ka.TTL)

	revoked, err := cli.Revoke(ctx, grant.ID)
	require.NoError(t, err)
	require.NoError(t, identity.admitHeader(0, revoked.Header, cur.Header.Revision))
}
