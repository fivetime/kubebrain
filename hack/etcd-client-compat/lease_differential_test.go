package compat

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	clientv3 "go.etcd.io/etcd/client/v3"
)

type leaseDifferentialResult struct {
	GrantRevision     int64
	PutRevision       int64
	TTLRevision       int64
	GrantedTTL        int64
	TTLKeys           int
	KeepAliveRevision int64
	KeepAliveTTL      int64
	ListRevision      int64
	ListContainsLease bool
	RevokeRevision    int64
	KeyAfterRevoke    int
	UnknownRevision   int64
	UnknownTTL        int64
	MinimumGrantedTTL int64
}

func TestLeaseDifferentialAgainstReferenceEtcd(t *testing.T) {
	reference := os.Getenv("REFERENCE_ETCD_ENDPOINT")
	if reference == "" {
		t.Skip("set REFERENCE_ETCD_ENDPOINT to run differential compatibility tests")
	}
	kubebrain := os.Getenv("KUBEBRAIN_ETCD_ENDPOINT")
	if kubebrain == "" {
		t.Fatal("set KUBEBRAIN_ETCD_ENDPOINT explicitly for differential tests")
	}
	require.Equal(t,
		runLeaseDifferentialScenario(t, reference, "etcd"),
		runLeaseDifferentialScenario(t, kubebrain, "kubebrain"),
	)
}

func runLeaseDifferentialScenario(t *testing.T, endpoint, instance string) leaseDifferentialResult {
	t.Helper()
	cli, err := clientv3.New(clientv3.Config{Endpoints: []string{endpoint}, DialTimeout: 3 * time.Second})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, cli.Close()) })
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	t.Cleanup(cancel)

	key := fmt.Sprintf("/dbaas-lease-differential/%s/%d", instance, time.Now().UnixNano())
	base, err := cli.Get(ctx, key)
	require.NoError(t, err)
	baseRev := base.Header.Revision
	grant, err := cli.Grant(ctx, 300)
	require.NoError(t, err)
	put, err := cli.Put(ctx, key, "value", clientv3.WithLease(grant.ID))
	require.NoError(t, err)
	ttl, err := cli.TimeToLive(ctx, grant.ID, clientv3.WithAttachedKeys())
	require.NoError(t, err)
	keepAlive, err := cli.KeepAliveOnce(ctx, grant.ID)
	require.NoError(t, err)
	leases, err := cli.Leases(ctx)
	require.NoError(t, err)
	contains := false
	for _, lease := range leases.Leases {
		contains = contains || lease.ID == grant.ID
	}
	revoke, err := cli.Revoke(ctx, grant.ID)
	require.NoError(t, err)
	after, err := cli.Get(ctx, key)
	require.NoError(t, err)
	unknown, err := cli.TimeToLive(ctx, grant.ID)
	require.NoError(t, err)
	minimum, err := cli.Grant(ctx, 0)
	require.NoError(t, err)
	_, err = cli.Revoke(ctx, minimum.ID)
	require.NoError(t, err)

	return leaseDifferentialResult{
		GrantRevision:     grant.ResponseHeader.Revision - baseRev,
		PutRevision:       put.Header.Revision - baseRev,
		TTLRevision:       ttl.ResponseHeader.Revision - baseRev,
		GrantedTTL:        ttl.GrantedTTL,
		TTLKeys:           len(ttl.Keys),
		KeepAliveRevision: keepAlive.ResponseHeader.Revision - baseRev,
		KeepAliveTTL:      keepAlive.TTL,
		ListRevision:      leases.ResponseHeader.Revision - baseRev,
		ListContainsLease: contains,
		RevokeRevision:    revoke.Header.Revision - baseRev,
		KeyAfterRevoke:    len(after.Kvs),
		UnknownRevision:   unknown.ResponseHeader.Revision - baseRev,
		UnknownTTL:        unknown.TTL,
		MinimumGrantedTTL: minimum.TTL,
	}
}
