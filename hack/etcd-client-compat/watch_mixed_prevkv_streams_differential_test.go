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
	CreatedHeadersMatch bool
	PutSequenceMatches  bool
	EventHeadersMatch   bool
	EventMetadataMatch  bool
	PrevMetadataMatch   bool
}

func TestWatchMixedPrevKVStreamsDifferentialAgainstReferenceEtcd(t *testing.T) {
	reference := os.Getenv("REFERENCE_ETCD_ENDPOINT")
	if reference == "" {
		t.Skip("set REFERENCE_ETCD_ENDPOINT to run mixed PrevKV stream differential tests")
	}

	want := mixedPrevKVStreamsOutcome{
		Watchers: 6, Updates: 8, WithPrevEvents: 24, WithoutPrevEvents: 24,
		CurrentValuesMatch: true, PreviousValuesMatch: true, CreatedHeadersMatch: true,
		PutSequenceMatches: true, EventHeadersMatch: true, EventMetadataMatch: true,
		PrevMetadataMatch: true,
	}
	referenceOutcome := runWatchMixedPrevKVStreamsScenario(t, reference, "etcd")
	require.Equal(t, want, referenceOutcome)
	require.Equal(t, referenceOutcome, runWatchMixedPrevKVStreamsScenario(t, compatEndpoint(t), "kubebrain"))
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
	seed, err := client.Put(ctx, key, "v0")
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
	createdHeadersMatch := true
	for index, watch := range watches {
		created := receiveMixedPrevKVWatchResponse(t, ctx, watch.channel, index, 0)
		require.True(t, created.Created)
		require.NoError(t, created.Err())
		createdHeadersMatch = createdHeadersMatch && created.Header.Revision == seed.Header.Revision
	}

	outcome := mixedPrevKVStreamsOutcome{
		Watchers:            watcherCount,
		Updates:             updateCount,
		CurrentValuesMatch:  true,
		PreviousValuesMatch: true,
		CreatedHeadersMatch: createdHeadersMatch,
		PutSequenceMatches:  true,
		EventHeadersMatch:   true,
		EventMetadataMatch:  true,
		PrevMetadataMatch:   true,
	}
	previous := "v0"
	for update := 1; update <= updateCount; update++ {
		current := fmt.Sprintf("v%d", update)
		put, putErr := client.Put(ctx, key, current)
		require.NoError(t, putErr)
		expectedRevision := seed.Header.Revision + int64(update)
		outcome.PutSequenceMatches = outcome.PutSequenceMatches && put.Header.Revision == expectedRevision

		for index, watch := range watches {
			response := receiveMixedPrevKVWatchResponse(t, ctx, watch.channel, index, update)
			require.NoError(t, response.Err())
			require.Len(t, response.Events, 1)
			event := response.Events[0]
			require.NotNil(t, event.Kv)
			outcome.EventHeadersMatch = outcome.EventHeadersMatch && response.Header.Revision == expectedRevision
			outcome.EventMetadataMatch = outcome.EventMetadataMatch &&
				event.Kv.CreateRevision == seed.Header.Revision &&
				event.Kv.ModRevision == expectedRevision && event.Kv.Version == int64(update+1)
			if string(event.Kv.Key) != key || string(event.Kv.Value) != current {
				outcome.CurrentValuesMatch = false
			}
			if watch.withPrev {
				outcome.WithPrevEvents++
				if event.PrevKv == nil || string(event.PrevKv.Key) != key ||
					string(event.PrevKv.Value) != previous {
					outcome.PreviousValuesMatch = false
				}
				outcome.PrevMetadataMatch = outcome.PrevMetadataMatch && event.PrevKv != nil &&
					event.PrevKv.CreateRevision == seed.Header.Revision &&
					event.PrevKv.ModRevision == expectedRevision-1 && event.PrevKv.Version == int64(update)
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
