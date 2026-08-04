package compat

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	clientv3 "go.etcd.io/etcd/client/v3"
)

// registerPrefixCleanup keeps endpoint-driven compatibility tests from
// accumulating live user keys in a shared DBaaS data plane. The verification
// makes a silently failed best-effort delete fail the owning test.
func registerPrefixCleanup(t *testing.T, cli *clientv3.Client, prefix string) {
	t.Helper()
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		_, err := cli.Delete(ctx, prefix, clientv3.WithPrefix())
		require.NoError(t, err)
		remaining, err := cli.Get(ctx, prefix, clientv3.WithPrefix(), clientv3.WithLimit(1))
		require.NoError(t, err)
		require.Empty(t, remaining.Kvs, "compatibility test prefix %q was not cleaned", prefix)
	})
}
