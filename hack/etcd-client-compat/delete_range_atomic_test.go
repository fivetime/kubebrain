package compat

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	clientv3 "go.etcd.io/etcd/client/v3"
)

// TestDeleteRangeAtomicity requires a KubeBrain endpoint configured with
// --max-delete-range-keys matching KUBEBRAIN_DELETE_RANGE_LIMIT.
func TestDeleteRangeAtomicity(t *testing.T) {
	endpoint := os.Getenv("KUBEBRAIN_DELETE_RANGE_ENDPOINT")
	limitText := os.Getenv("KUBEBRAIN_DELETE_RANGE_LIMIT")
	if endpoint == "" || limitText == "" {
		t.Skip("set KUBEBRAIN_DELETE_RANGE_ENDPOINT and KUBEBRAIN_DELETE_RANGE_LIMIT")
	}
	limit, err := strconv.Atoi(limitText)
	require.NoError(t, err)
	require.GreaterOrEqual(t, limit, 129)

	client, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{endpoint},
		DialTimeout: 5 * time.Second,
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, client.Close()) })
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	putRange := func(prefix string, count int) {
		for i := 0; i < count; i++ {
			_, putErr := client.Put(ctx, fmt.Sprintf("%s%04d", prefix, i), "value")
			require.NoError(t, putErr)
		}
	}
	assertCount := func(prefix string, want int64) {
		response, rangeErr := client.Get(ctx, prefix, clientv3.WithPrefix(), clientv3.WithCountOnly())
		require.NoError(t, rangeErr)
		require.Equal(t, want, response.Count)
	}

	atomicPrefix := fmt.Sprintf("/compat/delete-range/atomic/%d/", time.Now().UnixNano())
	putRange(atomicPrefix, 129)
	before, err := client.Get(ctx, atomicPrefix, clientv3.WithPrefix(), clientv3.WithCountOnly())
	require.NoError(t, err)
	deleted, err := client.Delete(ctx, atomicPrefix, clientv3.WithPrefix(), clientv3.WithPrevKV())
	require.NoError(t, err)
	require.Equal(t, int64(129), deleted.Deleted)
	require.Len(t, deleted.PrevKvs, 129)
	require.Equal(t, before.Header.Revision+1, deleted.Header.Revision)
	assertCount(atomicPrefix, 0)

	overflowPrefix := fmt.Sprintf("/compat/delete-range/overflow/%d/", time.Now().UnixNano())
	putRange(overflowPrefix, limit+1)
	before, err = client.Get(ctx, overflowPrefix, clientv3.WithPrefix(), clientv3.WithCountOnly())
	require.NoError(t, err)
	_, err = client.Delete(ctx, overflowPrefix, clientv3.WithPrefix())
	require.ErrorContains(t, err, "etcdserver: too many requests")
	after, err := client.Get(ctx, overflowPrefix, clientv3.WithPrefix(), clientv3.WithCountOnly())
	require.NoError(t, err)
	require.Equal(t, before.Header.Revision, after.Header.Revision)
	require.Equal(t, int64(limit+1), after.Count)

	_, err = client.Txn(ctx).Then(clientv3.OpDelete(overflowPrefix, clientv3.WithPrefix())).Commit()
	require.ErrorContains(t, err, "etcdserver: too many requests")
	assertCount(overflowPrefix, int64(limit+1))
}
