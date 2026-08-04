package compat

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	clientv3 "go.etcd.io/etcd/client/v3"
)

type leaseListRenewalOrderOutcome struct {
	InitialOrder      []string
	RenewedOrder      []string
	AfterRevokeOrder  []string
	AfterCleanupOrder []string
	GrantRevisionGap  int64
	RenewRevisionGap  int64
	RevokeRevisionGap int64
}

func TestLeaseListRenewalOrderDifferentialAgainstReferenceEtcd(t *testing.T) {
	reference := os.Getenv("REFERENCE_ETCD_ENDPOINT")
	if reference == "" {
		t.Skip("set REFERENCE_ETCD_ENDPOINT to run differential compatibility tests")
	}
	want := leaseListRenewalOrderOutcome{
		InitialOrder:      []string{"a", "b", "c"},
		RenewedOrder:      []string{"b", "c", "a"},
		AfterRevokeOrder:  []string{"c", "a"},
		AfterCleanupOrder: []string{},
	}
	referenceOutcome := runLeaseListRenewalOrderScenario(t, reference)
	require.Equal(t, want, referenceOutcome)
	require.Equal(t, referenceOutcome, runLeaseListRenewalOrderScenario(t, compatEndpoint(t)))
}

func runLeaseListRenewalOrderScenario(t *testing.T, endpoint string) leaseListRenewalOrderOutcome {
	t.Helper()
	cli, err := clientv3.New(clientv3.Config{Endpoints: []string{endpoint}, DialTimeout: 3 * time.Second})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, cli.Close()) })

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	seedKey := "/dbaas-lease-list-renewal-order/" + time.Now().Format("20060102150405.000000000")
	seed, err := cli.Put(ctx, seedKey, "seed")
	require.NoError(t, err)
	baseRevision := seed.Header.Revision

	names := make(map[clientv3.LeaseID]string, 3)
	grants := make(map[string]clientv3.LeaseID, 3)
	defer func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cleanupCancel()
		for _, id := range grants {
			_, _ = cli.Revoke(cleanupCtx, id)
		}
		_, _ = cli.Delete(cleanupCtx, seedKey)
	}()
	for _, name := range []string{"a", "b", "c"} {
		grant, grantErr := cli.Grant(ctx, 300)
		require.NoError(t, grantErr)
		names[grant.ID] = name
		grants[name] = grant.ID
	}
	list, err := cli.Leases(ctx)
	require.NoError(t, err)
	outcome := leaseListRenewalOrderOutcome{
		InitialOrder:     targetLeaseOrder(list, names),
		GrantRevisionGap: list.Revision - baseRevision,
	}

	renew, err := cli.KeepAliveOnce(ctx, grants["a"])
	require.NoError(t, err)
	list, err = cli.Leases(ctx)
	require.NoError(t, err)
	outcome.RenewedOrder = targetLeaseOrder(list, names)
	outcome.RenewRevisionGap = list.Revision - baseRevision
	require.Equal(t, int64(300), renew.TTL)

	revoke, err := cli.Revoke(ctx, grants["b"])
	require.NoError(t, err)
	delete(grants, "b")
	list, err = cli.Leases(ctx)
	require.NoError(t, err)
	outcome.AfterRevokeOrder = targetLeaseOrder(list, names)
	outcome.RevokeRevisionGap = list.Revision - baseRevision
	require.Equal(t, baseRevision, revoke.Header.Revision)

	for _, name := range []string{"c", "a"} {
		_, err = cli.Revoke(ctx, grants[name])
		require.NoError(t, err)
		delete(grants, name)
	}
	list, err = cli.Leases(ctx)
	require.NoError(t, err)
	outcome.AfterCleanupOrder = targetLeaseOrder(list, names)
	require.Equal(t, baseRevision, list.Revision)

	_, err = cli.Delete(ctx, seedKey)
	require.NoError(t, err)
	deleted, err := cli.Get(ctx, seedKey)
	require.NoError(t, err)
	require.Empty(t, deleted.Kvs)
	return outcome
}

func targetLeaseOrder(response *clientv3.LeaseLeasesResponse, names map[clientv3.LeaseID]string) []string {
	order := make([]string, 0, len(names))
	for _, lease := range response.Leases {
		if name, ok := names[lease.ID]; ok {
			order = append(order, name)
		}
	}
	return order
}
