package compat

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	etcdcache "go.etcd.io/etcd/cache/v3"
	clientv3 "go.etcd.io/etcd/client/v3"
)

// TestCacheHistoricalGetDoesNotReturnTooNewRevision pins upstream etcd
// 4f081fb1a. A cache Get at a historical revision between two updates must use
// the snapshot at or before the requested revision, not the latest snapshot.
func TestCacheHistoricalGetDoesNotReturnTooNewRevision(t *testing.T) {
	endpoint := compatEndpoint(t)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	client, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{endpoint},
		DialTimeout: 5 * time.Second,
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, client.Close()) })

	prefix := fmt.Sprintf("/a3757-cache-historical-get/%d/", time.Now().UnixNano())
	advancePrefix := fmt.Sprintf("/a3757-cache-historical-advance/%d/", time.Now().UnixNano())
	key := prefix + "item"
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cleanupCancel()
		_, _ = client.Delete(cleanupCtx, prefix, clientv3.WithPrefix())
		_, _ = client.Delete(cleanupCtx, advancePrefix, clientv3.WithPrefix())
	})

	cache, err := etcdcache.New(
		client,
		prefix,
		etcdcache.WithProgressRequestInterval(100*time.Millisecond),
		etcdcache.WithGetTimeout(5*time.Second),
		etcdcache.WithWaitTimeout(5*time.Second),
	)
	require.NoError(t, err)
	t.Cleanup(cache.Close)

	readyCtx, readyCancel := context.WithTimeout(ctx, 5*time.Second)
	defer readyCancel()
	require.NoError(t, cache.WaitReady(readyCtx))

	first, err := client.Put(ctx, key, "val1")
	require.NoError(t, err)
	for i := 0; i < 3; i++ {
		_, err = client.Put(ctx, fmt.Sprintf("%sadvance-%d", advancePrefix, i), "advance")
		require.NoError(t, err)
	}
	second, err := client.Put(ctx, key, "val2")
	require.NoError(t, err)
	require.Greater(t, second.Header.Revision, first.Header.Revision+1)
	require.NoError(t, client.RequestProgress(ctx))

	latest, err := cache.Get(ctx, key)
	require.NoError(t, err)
	require.Len(t, latest.Kvs, 1)
	require.Equal(t, "val2", string(latest.Kvs[0].Value))
	require.Equal(t, second.Header.Revision, latest.Kvs[0].ModRevision)

	historical, err := cache.Get(ctx, key, clientv3.WithRev(first.Header.Revision+1), clientv3.WithSerializable())
	require.NoError(t, err)
	require.Len(t, historical.Kvs, 1)
	require.Equal(t, "val1", string(historical.Kvs[0].Value))
	require.Equal(t, first.Header.Revision, historical.Kvs[0].ModRevision)
}
