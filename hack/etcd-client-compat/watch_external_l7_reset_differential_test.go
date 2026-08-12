package compat

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	clientv3 "go.etcd.io/etcd/client/v3"
)

type watchExternalL7ResetOutcome struct {
	Created             bool
	FirstValue          string
	StreamFailed        bool
	SecondReplicaDialed bool
	SecondValue         string
	RevisionDelta       int64
	WatchStayedOpen     bool
}

// TestWatchResumesAcrossExternalL7ResetDifferential fixes clientv3's stream
// recovery contract through a standalone gRPC-aware proxy. The downstream
// HTTP/2 connection remains open while the proxy returns an L7 Unavailable for
// the Watch stream and routes the resumed stream to another replica.
func TestWatchResumesAcrossExternalL7ResetDifferential(t *testing.T) {
	binary := strings.TrimSpace(os.Getenv("EXTERNAL_GRPC_SWITCH_PROXY_BINARY"))
	if binary == "" {
		t.Skip("set EXTERNAL_GRPC_SWITCH_PROXY_BINARY to run the external L7 Watch reset differential")
	}
	referenceEndpoints := splitRequiredDirectEndpoints(t, "REFERENCE_ETCD_DIRECT_ENDPOINTS")
	kubeBrainEndpoints := splitRequiredDirectEndpoints(t, "KUBEBRAIN_DIRECT_ENDPOINTS")
	requireDistinctDirectReplicaTopology(t, referenceEndpoints)
	requireDistinctDirectReplicaTopology(t, kubeBrainEndpoints)

	want := watchExternalL7ResetOutcome{
		Created:             true,
		FirstValue:          "before-reset",
		StreamFailed:        true,
		SecondReplicaDialed: true,
		SecondValue:         "after-reset",
		RevisionDelta:       1,
		WatchStayedOpen:     true,
	}
	reference := runWatchExternalL7ResetScenario(t, binary, referenceEndpoints, "etcd")
	require.Equal(t, want, reference)
	require.Equal(t, reference, runWatchExternalL7ResetScenario(t, binary, kubeBrainEndpoints, "kubebrain"))
}

func runWatchExternalL7ResetScenario(
	t *testing.T,
	binary string,
	endpoints []string,
	instance string,
) watchExternalL7ResetOutcome {
	t.Helper()
	proxy, endpoint := startExternalL7Proxy(t, binary, endpoints[0])
	throughProxy, err := clientv3.New(clientv3.Config{
		Endpoints: []string{endpoint}, DialTimeout: 3 * time.Second,
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, throughProxy.Close()) })
	writer, err := clientv3.New(clientv3.Config{
		Endpoints: []string{endpoints[2]}, DialTimeout: 3 * time.Second,
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, writer.Close()) })

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	t.Cleanup(cancel)
	key := fmt.Sprintf("/dbaas-external-l7-watch-reset/%s/%d", instance, time.Now().UnixNano())
	watch := throughProxy.Watch(ctx, key, clientv3.WithCreatedNotify())
	created := receiveExternalL7WatchResponse(t, ctx, watch)
	require.True(t, created.Created)
	initialStats := proxy.command(t, "stats")
	require.Positive(t, initialStats.Dials[grpcTarget(endpoints[0])])
	require.Equal(t, 1, initialStats.ActiveStreams)

	_, err = writer.Put(ctx, key, "before-reset")
	require.NoError(t, err)
	first := receiveExternalL7WatchEvent(t, ctx, watch)
	require.Len(t, first.Events, 1)
	firstRevision := first.Events[0].Kv.ModRevision

	require.True(t, proxy.command(t, "target "+grpcTarget(endpoints[1])).OK)
	require.True(t, proxy.command(t, "fail-streams").OK)
	require.Eventually(t, func() bool {
		stats := proxy.command(t, "stats")
		return stats.Dials[grpcTarget(endpoints[1])] > 0 && stats.ActiveStreams == 1
	}, 5*time.Second, 10*time.Millisecond)
	stats := proxy.command(t, "stats")

	_, err = writer.Put(ctx, key, "after-reset")
	require.NoError(t, err)
	second := receiveExternalL7WatchEvent(t, ctx, watch)
	require.Len(t, second.Events, 1)
	secondRevision := second.Events[0].Kv.ModRevision
	return watchExternalL7ResetOutcome{
		Created:             created.Created,
		FirstValue:          string(first.Events[0].Kv.Value),
		StreamFailed:        stats.FailedStreams > initialStats.FailedStreams,
		SecondReplicaDialed: stats.Dials[grpcTarget(endpoints[1])] > 0,
		SecondValue:         string(second.Events[0].Kv.Value),
		RevisionDelta:       secondRevision - firstRevision,
		WatchStayedOpen:     second.Err() == nil && !second.Canceled,
	}
}

func receiveExternalL7WatchResponse(
	t *testing.T,
	ctx context.Context,
	watch clientv3.WatchChan,
) clientv3.WatchResponse {
	t.Helper()
	select {
	case response, open := <-watch:
		require.True(t, open, "Watch channel closed")
		require.NoError(t, response.Err())
		return response
	case <-ctx.Done():
		require.NoError(t, ctx.Err())
		return clientv3.WatchResponse{}
	}
}

func receiveExternalL7WatchEvent(
	t *testing.T,
	ctx context.Context,
	watch clientv3.WatchChan,
) clientv3.WatchResponse {
	t.Helper()
	for {
		response := receiveExternalL7WatchResponse(t, ctx, watch)
		if len(response.Events) > 0 {
			return response
		}
	}
}
