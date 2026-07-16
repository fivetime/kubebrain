package compat

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"
)

type putDifferentialResult struct {
	CreateRevision      int64
	CreatePrev          *normalizedKV
	RebindRevision      int64
	RebindPrev          *normalizedKV
	IgnoreValueRevision int64
	IgnoreValuePrev     *normalizedKV
	IgnoreLeaseRevision int64
	IgnoreLeasePrev     *normalizedKV
	Current             normalizedKV
	MissingLease        authErrorOutcome
	MissingIgnoreValue  authErrorOutcome
	EmptyKey            authErrorOutcome
	ValueWithIgnore     authErrorOutcome
	LeaseWithIgnore     authErrorOutcome
}

func TestPutDifferentialAgainstReferenceEtcd(t *testing.T) {
	reference := os.Getenv("REFERENCE_ETCD_ENDPOINT")
	if reference == "" {
		t.Skip("set REFERENCE_ETCD_ENDPOINT to run differential compatibility tests")
	}

	kubebrain := runPutDifferentialScenario(t, compatEndpoint(), "kubebrain")
	etcd := runPutDifferentialScenario(t, reference, "etcd")
	require.Equal(t, etcd, kubebrain)
}

func runPutDifferentialScenario(t *testing.T, endpoint, instance string) putDifferentialResult {
	t.Helper()
	cli, err := clientv3.New(clientv3.Config{Endpoints: []string{endpoint}, DialTimeout: 3 * time.Second})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, cli.Close()) })
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	t.Cleanup(cancel)

	prefix := fmt.Sprintf("/dbaas-put-differential/%s/%d/", instance, time.Now().UnixNano())
	key := prefix + "key"
	missing := prefix + "missing"
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cleanupCancel()
		_, _ = cli.Delete(cleanupCtx, prefix, clientv3.WithPrefix())
	})
	empty, err := cli.Get(ctx, prefix, clientv3.WithPrefix())
	require.NoError(t, err)
	baseRev := empty.Header.Revision
	leaseBase := time.Now().UnixNano() & ((1 << 62) - 1)
	leaseClient := etcdserverpb.NewLeaseClient(cli.ActiveConnection())
	leaseA, err := leaseClient.LeaseGrant(ctx, &etcdserverpb.LeaseGrantRequest{ID: leaseBase, TTL: 300})
	require.NoError(t, err)
	leaseB, err := leaseClient.LeaseGrant(ctx, &etcdserverpb.LeaseGrantRequest{ID: leaseBase + 1, TTL: 300})
	require.NoError(t, err)
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cleanupCancel()
		_, _ = leaseClient.LeaseRevoke(cleanupCtx, &etcdserverpb.LeaseRevokeRequest{ID: leaseA.ID})
		_, _ = leaseClient.LeaseRevoke(cleanupCtx, &etcdserverpb.LeaseRevokeRequest{ID: leaseB.ID})
	})

	create, err := cli.Put(ctx, key, "one", clientv3.WithLease(clientv3.LeaseID(leaseA.ID)), clientv3.WithPrevKV())
	require.NoError(t, err)
	rebind, err := cli.Put(ctx, key, "two", clientv3.WithLease(clientv3.LeaseID(leaseB.ID)), clientv3.WithPrevKV())
	require.NoError(t, err)
	ignoreValue, err := cli.Put(ctx, key, "", clientv3.WithIgnoreValue(), clientv3.WithLease(clientv3.LeaseID(leaseA.ID)), clientv3.WithPrevKV())
	require.NoError(t, err)
	ignoreLease, err := cli.Put(ctx, key, "three", clientv3.WithIgnoreLease(), clientv3.WithPrevKV())
	require.NoError(t, err)
	current, err := cli.Get(ctx, key)
	require.NoError(t, err)
	require.Len(t, current.Kvs, 1)

	_, missingLeaseErr := cli.Put(ctx, key, "bad", clientv3.WithLease(clientv3.LeaseID(leaseBase+100)))
	_, missingIgnoreValueErr := cli.Put(ctx, missing, "", clientv3.WithIgnoreValue())
	raw := etcdserverpb.NewKVClient(cli.ActiveConnection())
	_, emptyKeyErr := raw.Put(ctx, &etcdserverpb.PutRequest{Value: []byte("bad")})
	_, valueWithIgnoreErr := raw.Put(ctx, &etcdserverpb.PutRequest{Key: []byte(key), Value: []byte("bad"), IgnoreValue: true})
	_, leaseWithIgnoreErr := raw.Put(ctx, &etcdserverpb.PutRequest{Key: []byte(key), Lease: int64(leaseA.ID), IgnoreLease: true})

	return putDifferentialResult{
		CreateRevision:      create.Header.Revision - baseRev,
		CreatePrev:          normalizeOptionalKV(create.PrevKv, prefix, baseRev),
		RebindRevision:      rebind.Header.Revision - baseRev,
		RebindPrev:          normalizeOptionalKV(rebind.PrevKv, prefix, baseRev),
		IgnoreValueRevision: ignoreValue.Header.Revision - baseRev,
		IgnoreValuePrev:     normalizeOptionalKV(ignoreValue.PrevKv, prefix, baseRev),
		IgnoreLeaseRevision: ignoreLease.Header.Revision - baseRev,
		IgnoreLeasePrev:     normalizeOptionalKV(ignoreLease.PrevKv, prefix, baseRev),
		Current:             normalizeKV(current.Kvs[0], prefix, baseRev),
		MissingLease:        authError(missingLeaseErr),
		MissingIgnoreValue:  authError(missingIgnoreValueErr),
		EmptyKey:            authError(emptyKeyErr),
		ValueWithIgnore:     authError(valueWithIgnoreErr),
		LeaseWithIgnore:     authError(leaseWithIgnoreErr),
	}
}

func normalizeOptionalKV(kv *mvccpb.KeyValue, prefix string, baseRev int64) *normalizedKV {
	if kv == nil {
		return nil
	}
	value := normalizeKV(kv, prefix, baseRev)
	return &value
}
