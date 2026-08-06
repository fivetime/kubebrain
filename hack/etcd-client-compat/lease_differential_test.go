package compat

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
	clientv3 "go.etcd.io/etcd/client/v3"
)

type leaseDifferentialResult struct {
	GrantRevision          int64
	PutRevision            int64
	TTLRevision            int64
	GrantedTTL             int64
	TTLKeys                int
	KeepAliveRevision      int64
	KeepAliveTTL           int64
	ListRevision           int64
	ListContainsLease      bool
	RevokeRevision         int64
	KeyAfterRevoke         int
	ListedAfterRevoke      bool
	UnknownRevision        int64
	UnknownTTL             int64
	MinimumGrantedTTL      int64
	MinimumGrantRevision   int64
	MinimumRevokeRevision  int64
	DeleteRevision         int64
	DeleteCount            int64
	DeletePrev             []normalizedKV
	KeysAfterDelete        int
	DetachedRevokeRevision int64
	ExplicitGrantRevision  int64
	ExplicitRevokeRevision int64
	ReusedGrantRevision    int64
	ReusedRevokeRevision   int64
	ReusedGrantedTTL       int64
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
	referenceOutcome := runLeaseDifferentialScenario(t, reference, "etcd")
	want := leaseDifferentialResult{
		PutRevision:           1,
		TTLRevision:           1,
		GrantedTTL:            300,
		TTLKeys:               1,
		KeepAliveRevision:     1,
		KeepAliveTTL:          300,
		ListRevision:          1,
		ListContainsLease:     true,
		RevokeRevision:        2,
		UnknownRevision:       2,
		UnknownTTL:            -1,
		MinimumGrantedTTL:     2,
		MinimumGrantRevision:  2,
		MinimumRevokeRevision: 2,
		DeleteRevision:        5,
		DeleteCount:           2,
		DeletePrev: []normalizedKV{
			{Key: "leased", Value: "leased-value", CreateRev: 3, ModRev: 3, Version: 1, HasLease: true},
			{Key: "plain", Value: "plain-value", CreateRev: 4, ModRev: 4, Version: 1},
		},
		DetachedRevokeRevision: 5,
		ExplicitGrantRevision:  5,
		ExplicitRevokeRevision: 5,
		ReusedGrantRevision:    5,
		ReusedRevokeRevision:   5,
		ReusedGrantedTTL:       301,
	}
	require.Equal(t, want, referenceOutcome)
	require.Equal(t, referenceOutcome, runLeaseDifferentialScenario(t, kubebrain, "kubebrain"))
}

func TestLeaseListExpiryOrderDifferentialAgainstReferenceEtcd(t *testing.T) {
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
	afterRevokeList, err := cli.Leases(ctx)
	require.NoError(t, err)
	listedAfterRevoke := false
	for _, lease := range afterRevokeList.Leases {
		listedAfterRevoke = listedAfterRevoke || lease.ID == grant.ID
	}
	minimum, err := cli.Grant(ctx, 0)
	require.NoError(t, err)
	minimumRevoke, err := cli.Revoke(ctx, minimum.ID)
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
	detachedRevoke, err := cli.Revoke(ctx, deleteLease.ID)
	require.NoError(t, err)

	explicitID := clientv3.LeaseID(time.Now().UnixNano() & ((1 << 62) - 1))
	rawLease := etcdserverpb.NewLeaseClient(cli.ActiveConnection())
	explicitGrant, err := rawLease.LeaseGrant(ctx, &etcdserverpb.LeaseGrantRequest{ID: int64(explicitID), TTL: 300})
	require.NoError(t, err)
	explicitRevoke, err := rawLease.LeaseRevoke(ctx, &etcdserverpb.LeaseRevokeRequest{ID: int64(explicitID)})
	require.NoError(t, err)
	reusedGrant, err := rawLease.LeaseGrant(ctx, &etcdserverpb.LeaseGrantRequest{ID: int64(explicitID), TTL: 301})
	require.NoError(t, err)
	reused, err := cli.TimeToLive(ctx, explicitID)
	require.NoError(t, err)
	reusedRevoke, err := cli.Revoke(ctx, explicitID)
	require.NoError(t, err)

	return leaseDifferentialResult{
		GrantRevision:          grant.ResponseHeader.Revision - baseRev,
		PutRevision:            put.Header.Revision - baseRev,
		TTLRevision:            ttl.ResponseHeader.Revision - baseRev,
		GrantedTTL:             ttl.GrantedTTL,
		TTLKeys:                len(ttl.Keys),
		KeepAliveRevision:      keepAlive.ResponseHeader.Revision - baseRev,
		KeepAliveTTL:           keepAlive.TTL,
		ListRevision:           leases.ResponseHeader.Revision - baseRev,
		ListContainsLease:      contains,
		RevokeRevision:         revoke.Header.Revision - baseRev,
		KeyAfterRevoke:         len(after.Kvs),
		ListedAfterRevoke:      listedAfterRevoke,
		UnknownRevision:        unknown.ResponseHeader.Revision - baseRev,
		UnknownTTL:             unknown.TTL,
		MinimumGrantedTTL:      minimum.TTL,
		MinimumGrantRevision:   minimum.ResponseHeader.Revision - baseRev,
		MinimumRevokeRevision:  minimumRevoke.Header.Revision - baseRev,
		DeleteRevision:         deleted.Header.Revision - baseRev,
		DeleteCount:            deleted.Deleted,
		DeletePrev:             normalizeKVs(deleted.PrevKvs, deletePrefix, baseRev),
		KeysAfterDelete:        len(afterDeleteTTL.Keys),
		DetachedRevokeRevision: detachedRevoke.Header.Revision - baseRev,
		ExplicitGrantRevision:  explicitGrant.Header.Revision - baseRev,
		ExplicitRevokeRevision: explicitRevoke.Header.Revision - baseRev,
		ReusedGrantRevision:    reusedGrant.Header.Revision - baseRev,
		ReusedRevokeRevision:   reusedRevoke.Header.Revision - baseRev,
		ReusedGrantedTTL:       reused.GrantedTTL,
	}
}
