package compat

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	clientv3 "go.etcd.io/etcd/client/v3"
)

func TestWatchProgressNotifySuppressesTickAfterEvent(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{compatEndpoint()},
		DialTimeout: 3 * time.Second,
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, cli.Close()) })

	key := fmt.Sprintf("/dbaas-watch-progress-cadence/%d", time.Now().UnixNano())
	_, err = cli.Put(ctx, key+"/seed", "seed")
	require.NoError(t, err)
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cleanupCancel()
		_, _ = cli.Delete(cleanupCtx, key, clientv3.WithPrefix())
	})

	watchCtx, watchCancel := context.WithCancel(ctx)
	defer watchCancel()
	watchCh := cli.Watch(watchCtx, key,
		clientv3.WithPrefix(), clientv3.WithCreatedNotify(), clientv3.WithProgressNotify())

	for {
		resp := requireWatchResponse(t, watchCh)
		if resp.IsProgressNotify() {
			break
		}
		require.True(t, resp.Created)
	}

	// Move away from the tick boundary, then deliver an event comfortably
	// before the next one.
	time.Sleep(200 * time.Millisecond)
	_, err = cli.Put(ctx, key+"/event", "value")
	require.NoError(t, err)
	for {
		resp := requireWatchResponse(t, watchCh)
		if len(resp.Events) != 0 {
			break
		}
		require.False(t, resp.IsProgressNotify(), "progress must not overtake the event")
	}

	select {
	case resp := <-watchCh:
		t.Fatalf("next periodic tick must be suppressed after an event, got %+v", resp)
	case <-time.After(1200 * time.Millisecond):
	}

	select {
	case resp := <-watchCh:
		require.True(t, resp.IsProgressNotify(), "the following tick must rearm progress")
	case <-time.After(1500 * time.Millisecond):
		t.Fatal("timed out waiting for rearmed progress notification")
	}
}

func requireWatchResponse(t *testing.T, watchCh clientv3.WatchChan) clientv3.WatchResponse {
	t.Helper()
	select {
	case resp, ok := <-watchCh:
		require.True(t, ok)
		require.NoError(t, resp.Err())
		return resp
	case <-time.After(4 * time.Second):
		t.Fatal("timed out waiting for watch response")
		return clientv3.WatchResponse{}
	}
}
