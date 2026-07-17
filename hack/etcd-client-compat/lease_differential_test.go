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
	DeleteRevision    int64
	DeleteCount       int64
	DeletePrev        []normalizedKV
	KeysAfterDelete   int
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

func TestLeaseListExpiryOrderAgainstReferenceEtcd(t *testing.T) {
	reference := os.Getenv("REFERENCE_ETCD_ENDPOINT")
	if reference == "" {
		t.Skip("set REFERENCE_ETCD_ENDPOINT to run differential compatibility tests")
	}
	kubebrain := os.Getenv("KUBEBRAIN_ETCD_ENDPOINT")
	if kubebrain == "" {
		t.Fatal("set KUBEBRAIN_ETCD_ENDPOINT explicitly for differential tests")
	}

	requireLeaseListExpiryOrder(t, reference)
	requireLeaseListExpiryOrder(t, kubebrain)
}

func requireLeaseListExpiryOrder(t *testing.T, endpoint string) {
	t.Helper()
	cli, err := clientv3.New(clientv3.Config{Endpoints: []string{endpoint}, DialTimeout: 3 * time.Second})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, cli.Close()) })
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	var grants []*clientv3.LeaseGrantResponse
	for _, ttl := range []int64{303, 301, 302} {
		grant, err := cli.Grant(ctx, ttl)
		require.NoError(t, err)
		grants = append(grants, grant)
		t.Cleanup(func() {
			cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cleanupCancel()
			_, _ = cli.Revoke(cleanupCtx, grant.ID)
		})
	}

	resp, err := cli.Leases(ctx)
	require.NoError(t, err)
	positions := make(map[clientv3.LeaseID]int, len(resp.Leases))
	for i, lease := range resp.Leases {
		positions[lease.ID] = i
	}
	for _, grant := range grants {
		require.Contains(t, positions, grant.ID)
	}
	require.Less(t, positions[grants[1].ID], positions[grants[2].ID])
	require.Less(t, positions[grants[2].ID], positions[grants[0].ID])
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

	deletePrefix := key + "/delete/"
	deleteLease, err := cli.Grant(ctx, 300)
	require.NoError(t, err)
	_, err = cli.Put(ctx, deletePrefix+"leased", "leased-value", clientv3.WithLease(deleteLease.ID))
	require.NoError(t, err)
	_, err = cli.Put(ctx, deletePrefix+"plain", "plain-value")
	require.NoError(t, err)
	deleted, err := cli.Delete(ctx, deletePrefix, clientv3.WithPrefix(), clientv3.WithPrevKV())
	require.NoError(t, err)
	afterDeleteTTL, err := cli.TimeToLive(ctx, deleteLease.ID, clientv3.WithAttachedKeys())
	require.NoError(t, err)
	_, err = cli.Revoke(ctx, deleteLease.ID)
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
		DeleteRevision:    deleted.Header.Revision - baseRev,
		DeleteCount:       deleted.Deleted,
		DeletePrev:        normalizeKVs(deleted.PrevKvs, deletePrefix, baseRev),
		KeysAfterDelete:   len(afterDeleteTTL.Keys),
	}
}
