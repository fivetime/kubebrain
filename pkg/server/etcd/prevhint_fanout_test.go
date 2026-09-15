package etcd

import (
	"context"
	"sync"
	"testing"

	proto "github.com/kubewharf/kubebrain-client/api/v2rpc"
	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/api/v3/mvccpb"
)

func TestWatchTranslationReusesHintAcrossStreams(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()
	b := server.backend.(*backendShim).backend
	ctx := context.Background()
	key := []byte("/watch/hint-fanout")
	created, err := b.Create(ctx, &proto.CreateRequest{Key: key, Value: []byte("old")})
	require.NoError(t, err)
	require.True(t, created.Succeeded)
	oldValue, err := b.Get(ctx, &proto.GetRequest{Key: key})
	require.NoError(t, err)
	updated, err := b.Update(ctx, &proto.UpdateRequest{Kv: &proto.KeyValue{Key: key, Value: []byte("new"), Revision: oldValue.Kv.Revision}})
	require.NoError(t, err)
	require.True(t, updated.Succeeded)
	newValue, err := b.Get(ctx, &proto.GetRequest{Key: key})
	require.NoError(t, err)

	// Use real stored inline envelopes but deliberately no storage dependency
	// for conversion. An unexpected metadata/PrevKV fallback cannot succeed.
	shim := NewBackendShim(nil, &recordingMetrics{}).(*backendShim)
	t.Cleanup(shim.Close)
	previous, err := shim.watchEventToEtcdEvent(ctx, &proto.Event{Type: proto.Event_CREATE, Revision: created.Header.Revision, Kv: oldValue.Kv})
	require.NoError(t, err)
	input := &proto.Event{Type: proto.Event_PUT, Revision: updated.Header.Revision, Kv: newValue.Kv}
	first, err := shim.watchEventToEtcdEvent(ctx, input)
	require.NoError(t, err)
	require.Equal(t, previous.Kv, first.PrevKv)
	require.Equal(t, []byte("new"), first.Kv.Value)
	require.EqualValues(t, 2, first.Kv.Version)
	_, ok := shim.prevCache.get(revCacheKey(key, input.Revision))
	require.True(t, ok, "conversion must preserve its hint before publishing the current event")
	for range 8 {
		next, err := shim.watchEventToEtcdEvent(ctx, input)
		require.NoError(t, err)
		require.Equal(t, first, next, "all streams must retain the same current and previous values")
	}
}

func TestPrevHintHitWarmsSharedRevisionCache(t *testing.T) {
	// No backend is installed: all follower conversions below must resolve the
	// already-proven predecessor without any storage access.
	shim := &backendShim{metricCli: &recordingMetrics{}}
	shim.prevKvResolver = newPrevKvResolver(shim)
	t.Cleanup(shim.Close)
	ctx := context.Background()
	key := []byte("key")
	previous := &mvccpb.KeyValue{Key: key, Value: []byte("old"), CreateRevision: 4, ModRevision: 10, Version: 2, Lease: 7}
	current := &mvccpb.KeyValue{Key: key, Value: []byte("new"), CreateRevision: 4, ModRevision: 20, Version: 3, Lease: 8}
	shim.noteEvent(key, 10, previous, false)
	require.Same(t, previous, shim.cachedPreviousEtcdKv(ctx, key, 20, 3, 4))
	// The first stream publishes the event it just converted. This invalidates
	// the predecessor hint for every subsequent stream converting revision 20.
	shim.noteEvent(key, 20, current, false)
	_, hinted := shim.hintedPreviousEtcdKv(key, 20, 3, 4)
	require.False(t, hinted)
	cached, ok := shim.prevCache.get(revCacheKey(key, 20))
	require.True(t, ok, "a proven hint must survive publication of the current event for other streams")
	require.Same(t, previous, cached)

	const readers = 32
	var wg sync.WaitGroup
	results := make(chan *mvccpb.KeyValue, readers)
	for range readers {
		wg.Go(func() { results <- shim.cachedPreviousEtcdKv(ctx, key, 20, 3, 4) })
	}
	wg.Wait()
	close(results)
	for result := range results {
		require.Same(t, previous, result)
	}

	// A fast stream can advance again, then delete/recreate the key. A lagging
	// stream must still get revision 20's predecessor, not the latest lifetime.
	require.Same(t, current, shim.cachedPreviousEtcdKv(ctx, key, 30, 4, 4))
	shim.noteEvent(key, 40, nil, true)
	shim.noteEvent(key, 50, &mvccpb.KeyValue{Key: key, CreateRevision: 50, ModRevision: 50, Version: 1}, false)
	require.Same(t, previous, shim.cachedPreviousEtcdKv(ctx, key, 20, 3, 4))
	require.Same(t, current, shim.cachedPreviousEtcdKv(ctx, key, 30, 4, 4))
	_, ok = shim.prevCache.get(revCacheKey(key, 50))
	require.False(t, ok, "a cached predecessor must not spill into another revision")
	_, ok = shim.prevCache.get(revCacheKey([]byte("other-key"), 20))
	require.False(t, ok, "a cached predecessor must not spill into another key")
}
