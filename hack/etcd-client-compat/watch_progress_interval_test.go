package compat

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	clientv3 "go.etcd.io/etcd/client/v3"
)

func TestWatchProgressIntervalClampsToEtcdMinimum(t *testing.T) {
	if os.Getenv("EXPECT_WATCH_PROGRESS_MIN_INTERVAL") == "" {
		t.Skip("set EXPECT_WATCH_PROGRESS_MIN_INTERVAL for a server configured below 100ms")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{compatEndpoint()},
		DialTimeout: time.Second,
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, cli.Close()) })

	key := fmt.Sprintf("/dbaas-watch-progress-min/%d", time.Now().UnixNano())
	_, err = cli.Put(ctx, key+"/seed", "seed")
	require.NoError(t, err)
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cleanupCancel()
		_, _ = cli.Delete(cleanupCtx, key, clientv3.WithPrefix())
	})

	start := time.Now()
	watchCh := cli.Watch(ctx, key, clientv3.WithPrefix(), clientv3.WithProgressNotify())
	resp := requireWatchResponse(t, watchCh)
	require.True(t, resp.IsProgressNotify())
	elapsed := time.Since(start)
	require.GreaterOrEqual(t, elapsed, 75*time.Millisecond,
		"a sub-minimum configuration must not emit at its requested ticker-storm cadence")
	require.Less(t, elapsed, time.Second,
		"the clamped 100ms interval should remain materially below the default cadence")
}
