package compat

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/grpc/metadata"
)

type mixedPrevKVStreamsOutcome struct {
	Watchers            int
	Updates             int
	WithPrevEvents      int
	WithoutPrevEvents   int
	CurrentValuesMatch  bool
	PreviousValuesMatch bool
}

func TestWatchMixedPrevKVStreamsDifferentialAgainstReferenceEtcd(t *testing.T) {
	reference := os.Getenv("REFERENCE_ETCD_ENDPOINT")
	if reference == "" {
		t.Skip("set REFERENCE_ETCD_ENDPOINT to run mixed PrevKV stream differential tests")
	}

	require.Equal(t,
		runWatchMixedPrevKVStreamsScenario(t, reference, "etcd"),
		runWatchMixedPrevKVStreamsScenario(t, compatEndpoint(), "kubebrain"),
	)
}

func runWatchMixedPrevKVStreamsScenario(t *testing.T, endpoint, instance string) mixedPrevKVStreamsOutcome {
	t.Helper()
	client, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{endpoint},
		DialTimeout: 5 * time.Second,
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, client.Close()) })

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	t.Cleanup(cancel)
	key := fmt.Sprintf("/dbaas-watch-mixed-prevkv/%s/%d", instance, time.Now().UnixNano())
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cleanupCancel()
		_, _ = client.Delete(cleanupCtx, key)
	})
	_, err = client.Put(ctx, key, "v0")
	require.NoError(t, err)

	const (
		watcherCount  = 6
		withPrevCount = watcherCount / 2
		updateCount   = 8
	)
	type watchCase struct {
		withPrev bool
		cancel   context.CancelFunc
		channel  clientv3.WatchChan
	}
	watches := make([]watchCase, watcherCount)
	for i := range watches {
		streamContext := metadata.NewOutgoingContext(
			ctx,
			metadata.Pairs("dbaas-watch-stream-id", fmt.Sprintf("%s-%d", instance, i)),
		)
		watchContext, watchCancel := context.WithCancel(streamContext)
		options := []clientv3.OpOption{clientv3.WithCreatedNotify()}
		withPrev := i < withPrevCount
		if withPrev {
			options = append(options, clientv3.WithPrevKV())
		}
		watches[i] = watchCase{
			withPrev: withPrev,
			cancel:   watchCancel,
			channel:  client.Watch(watchContext, key, options...),
		}
	}
	t.Cleanup(func() {
		for _, watch := range watches {
			watch.cancel()
		}
	})
	for index, watch := range watches {
		created := receiveMixedPrevKVWatchResponse(t, ctx, watch.channel, index, 0)
		require.True(t, created.Created)
		require.NoError(t, created.Err())
	}

	outcome := mixedPrevKVStreamsOutcome{
		Watchers:            watcherCount,
		Updates:             updateCount,
		CurrentValuesMatch:  true,
		PreviousValuesMatch: true,
	}
	previous := "v0"
	for update := 1; update <= updateCount; update++ {
		current := fmt.Sprintf("v%d", update)
		_, err = client.Put(ctx, key, current)
		require.NoError(t, err)

		for index, watch := range watches {
			response := receiveMixedPrevKVWatchResponse(t, ctx, watch.channel, index, update)
			require.NoError(t, response.Err())
			require.Len(t, response.Events, 1)
			event := response.Events[0]
			if string(event.Kv.Key) != key || string(event.Kv.Value) != current {
				outcome.CurrentValuesMatch = false
			}
			if watch.withPrev {
				outcome.WithPrevEvents++
				if event.PrevKv == nil || string(event.PrevKv.Key) != key ||
					string(event.PrevKv.Value) != previous {
					outcome.PreviousValuesMatch = false
				}
			} else {
				outcome.WithoutPrevEvents++
				if event.PrevKv != nil {
					outcome.PreviousValuesMatch = false
				}
			}
		}
		previous = current
	}
	return outcome
}

func receiveMixedPrevKVWatchResponse(
	t *testing.T,
	ctx context.Context,
	channel clientv3.WatchChan,
	watcher int,
	update int,
) clientv3.WatchResponse {
	t.Helper()
	select {
	case response, ok := <-channel:
		require.Truef(t, ok, "watcher %d closed at update %d", watcher, update)
		return response
	case <-ctx.Done():
		t.Fatalf("watcher %d timed out at update %d: %v", watcher, update, ctx.Err())
		return clientv3.WatchResponse{}
	}
}
