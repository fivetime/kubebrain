package compat

import (
	"context"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	clientv3 "go.etcd.io/etcd/client/v3"
)

// TestPhysicalCompactionUnderTraffic exercises the real client-facing path:
// physical compaction runs while point reads, range reads, writes, and a watch
// remain active. It is intentionally endpoint-driven so CI can run it against
// either reference etcd or a KubeBrain+TiKV deployment without mocks.
func TestPhysicalCompactionUnderTraffic(t *testing.T) {
	endpoint := os.Getenv("KUBEBRAIN_ETCD_ENDPOINT")
	if endpoint == "" {
		t.Skip("set KUBEBRAIN_ETCD_ENDPOINT to run live physical compaction tests")
	}
	cli, err := clientv3.New(clientv3.Config{Endpoints: []string{endpoint}, DialTimeout: 3 * time.Second})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, cli.Close()) })
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	prefix := fmt.Sprintf("/dbaas-physical-traffic/%d/", time.Now().UnixNano())
	registerPrefixCleanup(t, cli, prefix)
	seed := prefix + "seed"
	var target int64
	for i := 0; i < 20; i++ {
		resp, putErr := cli.Put(ctx, seed, fmt.Sprintf("v-%d", i))
		require.NoError(t, putErr)
		target = resp.Header.Revision
	}
	// Keep a current revision above the compact target and start a watch there.
	tail, err := cli.Put(ctx, prefix+"tail", "tail")
	require.NoError(t, err)
	watchRev := tail.Header.Revision + 1
	watchCtx, watchCancel := context.WithCancel(ctx)
	defer watchCancel()
	watchCh := cli.Watch(watchCtx, prefix+"live/", clientv3.WithPrefix(), clientv3.WithRev(watchRev))

	const writes = 100
	start := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(2)
	var compactErr, trafficErr error
	go func() {
		defer wg.Done()
		<-start
		_, compactErr = cli.Compact(ctx, target, clientv3.WithCompactPhysical())
	}()
	go func() {
		defer wg.Done()
		<-start
		for i := 0; i < writes; i++ {
			key := fmt.Sprintf("%slive/%03d", prefix, i)
			if _, trafficErr = cli.Put(ctx, key, "live"); trafficErr != nil {
				return
			}
			var got *clientv3.GetResponse
			got, trafficErr = cli.Get(ctx, key)
			if trafficErr != nil || len(got.Kvs) != 1 {
				if trafficErr == nil {
					trafficErr = fmt.Errorf("point read for %q returned %d keys", key, len(got.Kvs))
				}
				return
			}
		}
		_, trafficErr = cli.Get(ctx, prefix+"live/", clientv3.WithPrefix())
	}()
	close(start)
	wg.Wait()
	require.NoError(t, compactErr)
	require.NoError(t, trafficErr)

	seen := make(map[string]struct{}, writes)
	for len(seen) < writes {
		select {
		case response := <-watchCh:
			require.NoError(t, response.Err())
			for _, event := range response.Events {
				seen[string(event.Kv.Key)] = struct{}{}
			}
		case <-ctx.Done():
			t.Fatalf("watch received %d/%d concurrent writes: %v", len(seen), writes, ctx.Err())
		}
	}

	boundary, err := cli.Get(ctx, seed, clientv3.WithRev(target))
	require.NoError(t, err)
	require.Len(t, boundary.Kvs, 1)
	require.Equal(t, "v-19", string(boundary.Kvs[0].Value))
	_, err = cli.Get(ctx, seed, clientv3.WithRev(target-1))
	require.Error(t, err, "revision below the compact boundary must be rejected")
	current, err := cli.Get(ctx, prefix, clientv3.WithPrefix())
	require.NoError(t, err)
	require.Len(t, current.Kvs, writes+2)
}
