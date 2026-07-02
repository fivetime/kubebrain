package compat

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	clientv3 "go.etcd.io/etcd/client/v3"
)

// TestTxnIntraTxnVersionSemantics pins etcd's intra-transaction read-your-write
// version behavior, using the real etcd client as the consumer:
//
//   - The If(Version==1) compare is evaluated against the committed pre-txn state
//     (the seed Put made version 1), so the txn takes the Then branch.
//   - Inside Then, the OpPut is the key's second write, bumping version to 2.
//   - A Range that follows a Put in the same txn observes that write and reports
//     the updated value and version 2 -- not the pre-txn version 1.
//
// Ground truth: etcd server/storage/mvcc/kvstore_txn.go storeTxnWrite.Range reads
// at beginRev+1 once the txn has changes, and put sets Version=ver+1. A regression
// that made the intra-txn Get miss the same-txn write (or report the stale version)
// would break apiserver guaranteed-update/optimistic-concurrency flows.
func TestTxnIntraTxnVersionSemantics(t *testing.T) {
	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{compatEndpoint()},
		DialTimeout: 3 * time.Second,
	})
	require.NoError(t, err)
	defer cli.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	key := testPrefix(t) + "/compare-version"

	_, err = cli.Put(ctx, key, "exists")
	require.NoError(t, err)

	g, err := cli.Get(ctx, key)
	require.NoError(t, err)
	require.Len(t, g.Kvs, 1)
	require.Equal(t, int64(1), g.Kvs[0].Version, "fresh key Version must be 1")
	require.Equal(t, g.Kvs[0].CreateRevision, g.Kvs[0].ModRevision,
		"fresh key CreateRevision must equal ModRevision")

	resp, err := cli.Txn(ctx).
		If(clientv3.Compare(clientv3.Version(key), "=", 1)).
		Then(clientv3.OpPut(key, "version-matched"), clientv3.OpGet(key)).
		Else(clientv3.OpPut(key, "version-not-matched")).
		Commit()
	require.NoError(t, err)

	require.True(t, resp.Succeeded, "Version==1 compare must succeed against committed pre-txn state")
	require.Len(t, resp.Responses, 2)
	rr := resp.Responses[1].GetResponseRange()
	require.NotNil(t, rr)
	require.Len(t, rr.Kvs, 1)
	require.Equal(t, "version-matched", string(rr.Kvs[0].Value),
		"intra-txn OpGet must read the value written by the OpPut in the same Then block")
	require.Equal(t, int64(2), rr.Kvs[0].Version,
		"intra-txn OpGet must report the version bumped by the same-txn OpPut (etcd reads at beginRev+1)")
}
