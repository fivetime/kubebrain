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

// TestCacheLinearizableGetCatchesUpOnQuietPrefix pins upstream etcd
// a1cb0a244, 910fdba06, and fa8a5a248. A cache.Get without
// WithSerializable must perform a consistent read: after an unrelated key
// advances the server revision, a quiet watched prefix must catch up via
// cache-driven RequestProgress instead of returning a stale cache snapshot.
func TestCacheLinearizableGetCatchesUpOnQuietPrefix(t *testing.T) {
	endpoint := compatEndpoint(t)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	client, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{endpoint},
		DialTimeout: 5 * time.Second,
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, client.Close()) })

	prefix := fmt.Sprintf("/a3760-cache-consistent-get/%d/", time.Now().UnixNano())
	advancePrefix := fmt.Sprintf("/a3760-cache-consistent-advance/%d/", time.Now().UnixNano())
	key := prefix + "seed"
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cleanupCancel()
		_, _ = client.Delete(cleanupCtx, prefix, clientv3.WithPrefix())
		_, _ = client.Delete(cleanupCtx, advancePrefix, clientv3.WithPrefix())
	})

	seed, err := client.Put(ctx, key, "value")
	require.NoError(t, err)

	cache, err := etcdcache.New(
		client,
		prefix,
		etcdcache.WithProgressRequestInterval(50*time.Millisecond),
		etcdcache.WithGetTimeout(5*time.Second),
		etcdcache.WithWaitTimeout(5*time.Second),
	)
	require.NoError(t, err)
	t.Cleanup(cache.Close)

	readyCtx, readyCancel := context.WithTimeout(ctx, 5*time.Second)
	defer readyCancel()
	require.NoError(t, cache.WaitReady(readyCtx))

	advance, err := client.Put(ctx, advancePrefix+"outside", "advance")
	require.NoError(t, err)
	require.Greater(t, advance.Header.Revision, seed.Header.Revision)

	got, err := cache.Get(ctx, key)
	require.NoError(t, err)
	require.NotNil(t, got.Header)
	require.GreaterOrEqual(t, got.Header.Revision, advance.Header.Revision)
	require.Len(t, got.Kvs, 1)
	require.Equal(t, key, string(got.Kvs[0].Key))
	require.Equal(t, "value", string(got.Kvs[0].Value))
	require.Equal(t, seed.Header.Revision, got.Kvs[0].ModRevision)
}
