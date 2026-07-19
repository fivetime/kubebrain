package compat

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
	clientv3 "go.etcd.io/etcd/client/v3"
)

// TestTxnCompactedRangeNeverAppliesFollowingWrite mirrors upstream etcd
// tests/integration/txn_range_consistency_test.go (fbba4f46e). A Range error in
// a write Txn must reject the whole request before any later operation commits.
func TestTxnCompactedRangeNeverAppliesFollowingWrite(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{compatEndpoint()},
		DialTimeout: 3 * time.Second,
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, cli.Close()) })

	prefix := fmt.Sprintf("/dbaas-txn-range-consistency/%d/", time.Now().UnixNano())
	sourceKey := prefix + "source"
	forbiddenKey := prefix + "must-not-exist"
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cleanupCancel()
		_, _ = cli.Delete(cleanupCtx, prefix, clientv3.WithPrefix())
	})

	first, err := cli.Put(ctx, sourceKey, "v1")
	require.NoError(t, err)
	second, err := cli.Put(ctx, sourceKey, "v2")
	require.NoError(t, err)
	require.Greater(t, second.Header.Revision, first.Header.Revision)
	_, err = cli.Compact(ctx, second.Header.Revision)
	require.NoError(t, err)

	_, err = cli.Txn(ctx).Then(
		clientv3.OpGet(sourceKey, clientv3.WithRev(first.Header.Revision)),
		clientv3.OpPut(forbiddenKey, "inconsistent"),
	).Commit()
	require.ErrorIs(t, err, rpctypes.ErrCompacted)

	got, err := cli.Get(ctx, forbiddenKey)
	require.NoError(t, err)
	require.Zero(t, got.Count, "a Txn with a compacted Range must not apply its following Put")
}
