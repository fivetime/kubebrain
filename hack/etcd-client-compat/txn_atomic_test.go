package compat

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	clientv3 "go.etcd.io/etcd/client/v3"
)

// TestTxnMultiWriteSingleRevision pins #4 Tier 1: all writes of a multi-op txn
// must land at ONE revision (etcd semantics). Before the fix each op allocated
// its own revision, so a and b below would differ.
func TestTxnMultiWriteSingleRevision(t *testing.T) {
	cli, err := clientv3.New(clientv3.Config{Endpoints: []string{compatEndpoint()}, DialTimeout: 3 * time.Second})
	require.NoError(t, err)
	defer cli.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	pfx := testPrefix(t)
	cleanupPrefix(t, newKubernetesClient(t), pfx)
	a, b, c := pfx+"/a", pfx+"/b", pfx+"/c"

	// seed c so the delete is effective
	_, err = cli.Put(ctx, c, "c0")
	require.NoError(t, err)

	// no-compare multi-op txn: two puts + one delete, all in one revision
	resp, err := cli.Txn(ctx).
		Then(clientv3.OpPut(a, "va"), clientv3.OpPut(b, "vb"), clientv3.OpDelete(c)).
		Commit()
	require.NoError(t, err)
	require.True(t, resp.Succeeded)
	require.Len(t, resp.Responses, 3)
	txnRev := resp.Header.Revision

	ga, err := cli.Get(ctx, a)
	require.NoError(t, err)
	require.Len(t, ga.Kvs, 1)
	gb, err := cli.Get(ctx, b)
	require.NoError(t, err)
	require.Len(t, gb.Kvs, 1)
	gc, err := cli.Get(ctx, c)
	require.NoError(t, err)
	require.Len(t, gc.Kvs, 0, "c must be deleted by the txn")

	require.Equal(t, txnRev, ga.Kvs[0].ModRevision, "put a must be at the txn revision")
	require.Equal(t, txnRev, gb.Kvs[0].ModRevision, "put b must be at the txn revision")
	require.Equal(t, ga.Kvs[0].ModRevision, gb.Kvs[0].ModRevision,
		"all writes of one txn must share a single revision (#4)")
	require.EqualValues(t, txnRev, ga.Kvs[0].CreateRevision, "fresh key created at txn revision")
}

// TestTxnCompareMultiWriteSingleRevision covers the compare + multi-write shape:
// the compare picks the Then branch and both writes land at one revision.
func TestTxnCompareMultiWriteSingleRevision(t *testing.T) {
	cli, err := clientv3.New(clientv3.Config{Endpoints: []string{compatEndpoint()}, DialTimeout: 3 * time.Second})
	require.NoError(t, err)
	defer cli.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	pfx := testPrefix(t)
	cleanupPrefix(t, newKubernetesClient(t), pfx)
	guard, a, b := pfx+"/guard", pfx+"/a", pfx+"/b"

	_, err = cli.Put(ctx, guard, "g")
	require.NoError(t, err)

	resp, err := cli.Txn(ctx).
		If(clientv3.Compare(clientv3.Version(guard), "=", 1)).
		Then(clientv3.OpPut(a, "va"), clientv3.OpPut(b, "vb")).
		Else(clientv3.OpGet(guard)).
		Commit()
	require.NoError(t, err)
	require.True(t, resp.Succeeded, "guard exists at version 1, Then branch taken")

	ga, err := cli.Get(ctx, a)
	require.NoError(t, err)
	gb, err := cli.Get(ctx, b)
	require.NoError(t, err)
	require.Len(t, ga.Kvs, 1)
	require.Len(t, gb.Kvs, 1)
	require.Equal(t, resp.Header.Revision, ga.Kvs[0].ModRevision)
	require.Equal(t, ga.Kvs[0].ModRevision, gb.Kvs[0].ModRevision,
		"compare-then-multi-write must be a single revision (#4)")
}
