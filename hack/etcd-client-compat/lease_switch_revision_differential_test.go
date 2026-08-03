package compat

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
	clientv3 "go.etcd.io/etcd/client/v3"
)

type leaseSwitchRevisionOutcome struct {
	GrantARevisionGap      int64
	GrantBRevisionGap      int64
	PutARevisionGap        int64
	PutBRevisionGap        int64
	OldLeaseKeysEmpty      bool
	NewLeaseAttached       bool
	OldTTLRevisionGap      int64
	NewTTLRevisionGap      int64
	OldRevokeRevisionGap   int64
	NewBindingPreserved    bool
	RangeAfterOldRevokeGap int64
	NewRevokeRevisionGap   int64
	KeyDeleted             bool
	FinalRangeRevisionGap  int64
	OldMissingRevisionGap  int64
	NewMissingRevisionGap  int64
}

func TestLeaseSwitchRevisionDifferentialAgainstReferenceEtcd(t *testing.T) {
	reference := os.Getenv("REFERENCE_ETCD_ENDPOINT")
	if reference == "" {
		t.Skip("set REFERENCE_ETCD_ENDPOINT to run differential compatibility tests")
	}

	want := leaseSwitchRevisionOutcome{
		PutARevisionGap:        1,
		PutBRevisionGap:        2,
		OldLeaseKeysEmpty:      true,
		NewLeaseAttached:       true,
		OldTTLRevisionGap:      2,
		NewTTLRevisionGap:      2,
		OldRevokeRevisionGap:   2,
		NewBindingPreserved:    true,
		RangeAfterOldRevokeGap: 2,
		NewRevokeRevisionGap:   3,
		KeyDeleted:             true,
		FinalRangeRevisionGap:  3,
		OldMissingRevisionGap:  3,
		NewMissingRevisionGap:  3,
	}
	referenceOutcome := runLeaseSwitchRevisionScenario(t, reference, "reference")
	require.Equal(t, want, referenceOutcome)
	require.Equal(t, referenceOutcome, runLeaseSwitchRevisionScenario(t, compatEndpoint(t), "kubebrain"))
}

func runLeaseSwitchRevisionScenario(t *testing.T, endpoint, instance string) leaseSwitchRevisionOutcome {
	t.Helper()
	cli, err := clientv3.New(clientv3.Config{Endpoints: []string{endpoint}, DialTimeout: 3 * time.Second})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, cli.Close()) })
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	t.Cleanup(cancel)

	prefix := testPrefix(t) + "/lease-switch-revision/" + instance
	seedKey := prefix + "/seed"
	key := prefix + "/key"
	seed, err := cli.Put(ctx, seedKey, "seed")
	require.NoError(t, err)
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cleanupCancel()
		_, _ = cli.Delete(cleanupCtx, prefix, clientv3.WithPrefix())
	})

	leaseA, err := cli.Grant(ctx, 300)
	require.NoError(t, err)
	leaseB, err := cli.Grant(ctx, 300)
	require.NoError(t, err)
	putA, err := cli.Put(ctx, key, "lease-a", clientv3.WithLease(leaseA.ID))
	require.NoError(t, err)
	putB, err := cli.Put(ctx, key, "lease-b", clientv3.WithLease(leaseB.ID))
	require.NoError(t, err)
	oldTTL, err := cli.TimeToLive(ctx, leaseA.ID, clientv3.WithAttachedKeys())
	require.NoError(t, err)
	newTTL, err := cli.TimeToLive(ctx, leaseB.ID, clientv3.WithAttachedKeys())
	require.NoError(t, err)
	oldRevoke, err := cli.Revoke(ctx, leaseA.ID)
	require.NoError(t, err)
	afterOld, err := cli.Get(ctx, key)
	require.NoError(t, err)
	newRevoke, err := cli.Revoke(ctx, leaseB.ID)
	require.NoError(t, err)
	afterNew, err := cli.Get(ctx, key)
	require.NoError(t, err)
	oldMissing, err := cli.TimeToLive(ctx, leaseA.ID)
	require.NoError(t, err)
	newMissing, err := cli.TimeToLive(ctx, leaseB.ID)
	require.NoError(t, err)
	for name, header := range map[string]*etcdserverpb.ResponseHeader{
		"seed": seed.Header, "grant-a": leaseA.ResponseHeader, "grant-b": leaseB.ResponseHeader,
		"put-a": putA.Header, "put-b": putB.Header, "old-ttl": oldTTL.ResponseHeader,
		"new-ttl": newTTL.ResponseHeader, "old-revoke": oldRevoke.Header,
		"range-after-old": afterOld.Header, "new-revoke": newRevoke.Header,
		"range-after-new": afterNew.Header, "old-missing": oldMissing.ResponseHeader,
		"new-missing": newMissing.ResponseHeader,
	} {
		require.NotNil(t, header, name)
	}

	baseRevision := seed.Header.Revision
	return leaseSwitchRevisionOutcome{
		GrantARevisionGap:      leaseA.ResponseHeader.Revision - baseRevision,
		GrantBRevisionGap:      leaseB.ResponseHeader.Revision - baseRevision,
		PutARevisionGap:        putA.Header.Revision - baseRevision,
		PutBRevisionGap:        putB.Header.Revision - baseRevision,
		OldLeaseKeysEmpty:      len(oldTTL.Keys) == 0,
		NewLeaseAttached:       len(newTTL.Keys) == 1 && string(newTTL.Keys[0]) == key,
		OldTTLRevisionGap:      oldTTL.ResponseHeader.Revision - baseRevision,
		NewTTLRevisionGap:      newTTL.ResponseHeader.Revision - baseRevision,
		OldRevokeRevisionGap:   oldRevoke.Header.Revision - baseRevision,
		NewBindingPreserved:    len(afterOld.Kvs) == 1 && string(afterOld.Kvs[0].Value) == "lease-b" && afterOld.Kvs[0].Lease == int64(leaseB.ID),
		RangeAfterOldRevokeGap: afterOld.Header.Revision - baseRevision,
		NewRevokeRevisionGap:   newRevoke.Header.Revision - baseRevision,
		KeyDeleted:             len(afterNew.Kvs) == 0,
		FinalRangeRevisionGap:  afterNew.Header.Revision - baseRevision,
		OldMissingRevisionGap:  oldMissing.ResponseHeader.Revision - baseRevision,
		NewMissingRevisionGap:  newMissing.ResponseHeader.Revision - baseRevision,
	}
}
