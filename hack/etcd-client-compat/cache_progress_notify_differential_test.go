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

// TestCacheWatchProgressNotifyAgainstKubeBrain pins upstream etcd 236179af0.
// cache.Watch must accept clientv3.WithProgressNotify and periodically emit
// progress notifications for local cache-backed watchers.
func TestCacheWatchProgressNotifyAgainstKubeBrain(t *testing.T) {
	endpoint := compatEndpoint(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	client, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{endpoint},
		DialTimeout: 5 * time.Second,
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, client.Close()) })

	prefix := fmt.Sprintf("/a3756-cache-progress/%d/", time.Now().UnixNano())
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cleanupCancel()
		_, _ = client.Delete(cleanupCtx, prefix, clientv3.WithPrefix())
	})

	seed, err := client.Put(ctx, prefix+"seed", "value")
	require.NoError(t, err)
	require.NotNil(t, seed.Header)

	cache, err := etcdcache.New(
		client,
		prefix,
		etcdcache.WithProgressNotifyInterval(100*time.Millisecond),
		etcdcache.WithProgressRequestInterval(100*time.Millisecond),
		etcdcache.WithGetTimeout(5*time.Second),
		etcdcache.WithWaitTimeout(5*time.Second),
	)
	require.NoError(t, err)
	t.Cleanup(cache.Close)

	readyCtx, readyCancel := context.WithTimeout(ctx, 5*time.Second)
	defer readyCancel()
	require.NoError(t, cache.WaitReady(readyCtx))

	watchCtx, watchCancel := context.WithTimeout(ctx, 5*time.Second)
	defer watchCancel()
	watchCh := cache.Watch(watchCtx, prefix, clientv3.WithPrefix(), clientv3.WithProgressNotify())
	eventPut, err := client.Put(ctx, prefix+"event", "value")
	require.NoError(t, err)

	sawEvent := false
	for {
		select {
		case <-watchCtx.Done():
			require.FailNow(t, "timed out waiting for cache progress notification")
		case response, ok := <-watchCh:
			require.True(t, ok, "cache watch channel closed before progress notification")
			require.False(t, response.Canceled, response.Err())
			if len(response.Events) > 0 {
				for _, event := range response.Events {
					if string(event.Kv.Key) == prefix+"event" {
						sawEvent = true
						require.Equal(t, eventPut.Header.Revision, event.Kv.ModRevision)
					}
				}
				continue
			}
			if response.IsProgressNotify() {
				require.True(t, sawEvent, "cache progress should follow a cached event so the demux has a local revision")
				require.NotNil(t, response.Header)
				require.GreaterOrEqual(t, response.Header.Revision, eventPut.Header.Revision)
				return
			}
		}
	}
}
