package compat

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	clientv3 "go.etcd.io/etcd/client/v3"
)

// TestMaintenanceHashKVSemantics exercises KubeBrain through etcd's official
// client. Hash values are backend-layout-specific, so the compatibility
// contract is stability and data sensitivity rather than numeric equality with
// etcd's bbolt hash.
func TestMaintenanceHashKVSemantics(t *testing.T) {
	endpoint := compatEndpoint()
	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{endpoint},
		DialTimeout: 3 * time.Second,
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, cli.Close()) })

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	key := testPrefix(t) + "/hash-key"
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cleanupCancel()
		_, _ = cli.Delete(cleanupCtx, key)
	})

	put1, err := cli.Put(ctx, key, "v1")
	require.NoError(t, err)
	rev1 := put1.Header.Revision

	hash1, err := cli.HashKV(ctx, endpoint, rev1)
	require.NoError(t, err)
	hash1Again, err := cli.HashKV(ctx, endpoint, rev1)
	require.NoError(t, err)
	require.Equal(t, hash1.Hash, hash1Again.Hash)

	_, err = cli.Put(ctx, key, "v2")
	require.NoError(t, err)
	current, err := cli.HashKV(ctx, endpoint, 0)
	require.NoError(t, err)
	require.NotEqual(t, hash1.Hash, current.Hash)

	historical, err := cli.HashKV(ctx, endpoint, rev1)
	require.NoError(t, err)
	require.Equal(t, hash1.Hash, historical.Hash)
}
