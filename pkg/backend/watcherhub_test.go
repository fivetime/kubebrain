// Copyright 2026 ByteDance and/or its affiliates
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package backend

import (
	"context"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"

	proto "github.com/kubewharf/kubebrain-client/api/v2rpc"

	"github.com/kubewharf/kubebrain/pkg/metrics/mock"
)

func newTestWatcherHub(t *testing.T, bufSize int) *WatcherHub {
	ctrl := gomock.NewController(t)
	t.Cleanup(ctrl.Finish)
	return &WatcherHub{
		subs:      make(map[chan []*proto.Event][]byte),
		metricCli: mock.NewMinimalMetrics(ctrl),
		bufSize:   bufSize,
	}
}

func batch(rev uint64) []*proto.Event {
	return []*proto.Event{{Revision: rev, Kv: &proto.KeyValue{Revision: rev}}}
}

// keyBatch builds a one-event batch whose key is set, for routing tests.
func keyBatch(rev uint64, key string) []*proto.Event {
	return []*proto.Event{{Revision: rev, Kv: &proto.KeyValue{Revision: rev, Key: []byte(key)}}}
}

// TestBroadcastRoutesByPrefix pins the #69 fan-out routing: a batch is delivered
// only to subscribers whose watched prefix a key in the batch falls under; a
// subscriber watching an unrelated prefix is skipped entirely (no send), so it
// never has to filter the batch away downstream.
func TestBroadcastRoutesByPrefix(t *testing.T) {
	hub := newTestWatcherHub(t, 8)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	subA, err := hub.AddWatcher(ctx, []byte("/registry/a/"))
	require.NoError(t, err)
	subB, err := hub.AddWatcher(ctx, []byte("/registry/b/"))
	require.NoError(t, err)

	// A batch touching only /registry/a/ must reach A and skip B.
	hub.broadcast(keyBatch(10, "/registry/a/pod-1"))
	// A batch touching only /registry/b/ must reach B and skip A.
	hub.broadcast(keyBatch(11, "/registry/b/pod-2"))

	drain := func(sub <-chan []*proto.Event) []uint64 {
		var revs []uint64
		for {
			select {
			case evs := <-sub:
				revs = append(revs, evs[0].Revision)
			default:
				return revs
			}
		}
	}
	require.Equal(t, []uint64{10}, drain(subA), "A must get only its own-prefix batch")
	require.Equal(t, []uint64{11}, drain(subB), "B must get only its own-prefix batch")
}

// TestBroadcastProgressReachesQuietSubBehindEvents pins the safe advancing
// progress fan-out: a quiet subscriber (whose prefix matches no event) receives
// an in-band progress marker carrying the published watermark, while a subscriber
// interleaved on the same batches receives its events *before* the marker (FIFO).
// This is the backend half of the frozen-progress fix.
func TestBroadcastProgressReachesQuietSubBehindEvents(t *testing.T) {
	hub := newTestWatcherHub(t, 8)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	// quiet sub watches b/, active sub watches a/.
	quiet, err := hub.AddWatcher(ctx, []byte("b/"))
	require.NoError(t, err)
	active, err := hub.AddWatcher(ctx, []byte("a/"))
	require.NoError(t, err)

	// Two batches touching only a/: the quiet sub is skipped, publishedRev -> 11.
	hub.broadcast(keyBatch(10, "a/x"))
	hub.broadcast(keyBatch(11, "a/y"))
	require.Equal(t, uint64(11), hub.PublishedRevision())

	// Fan a progress marker at the published watermark.
	hub.broadcastProgress(hub.PublishedRevision())

	// The quiet sub received exactly one batch: a progress marker at rev 11.
	var quietBatches [][]*proto.Event
	for {
		done := false
		select {
		case evs := <-quiet:
			quietBatches = append(quietBatches, evs)
		default:
			done = true
		}
		if done {
			break
		}
	}
	require.Len(t, quietBatches, 1, "quiet sub must receive exactly the progress marker")
	require.True(t, isProgressMarker(quietBatches[0]), "quiet sub batch must be a progress marker")
	require.Equal(t, uint64(11), quietBatches[0][0].Revision)

	// The active sub received event@10, event@11, then the marker@11 — in order.
	var kinds []string
	var revs []uint64
	for {
		done := false
		select {
		case evs := <-active:
			revs = append(revs, evs[0].Revision)
			if isProgressMarker(evs) {
				kinds = append(kinds, "progress")
			} else {
				kinds = append(kinds, "event")
			}
		default:
			done = true
		}
		if done {
			break
		}
	}
	require.Equal(t, []string{"event", "event", "progress"}, kinds,
		"events must be delivered before the progress marker (FIFO)")
	require.Equal(t, []uint64{10, 11, 11}, revs)
}

// TestBroadcastProgressSkipsFullSubWithoutEviction pins that a full subscriber is
// skipped (never evicted) by a progress fan-out: a dropped marker only delays
// progress by a tick, unlike a dropped event which tears a gap.
func TestBroadcastProgressSkipsFullSubWithoutEviction(t *testing.T) {
	hub := newTestWatcherHub(t, 1) // buffer of exactly one batch

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	_, err := hub.AddWatcher(ctx, nil)
	require.NoError(t, err)

	// Fill the one-slot buffer with an event, so the sub is now full.
	hub.broadcast(keyBatch(10, "a/x"))
	require.Equal(t, 1, hub.subCount())

	// A progress fan-out to the full sub must skip it, not evict it.
	hub.broadcastProgress(10)
	require.Equal(t, 1, hub.subCount(), "full sub must survive a dropped progress marker")
}

func (w *WatcherHub) subCount() int {
	w.RLock()
	defer w.RUnlock()
	return len(w.subs)
}

// TestBroadcastEvictsSlowConsumerBeforeNextBatch pins the Critical #3 fix: a
// subscriber that misses one batch must be removed from the hub synchronously,
// before any later batch could be delivered to it. Pre-fix the eviction was
// `go DeleteWatcher(...)` (async), so the sub stayed registered and could
// receive a later batch — delivering a gap (…N-1, N+1) that reflectors never
// recover from.
func TestBroadcastEvictsSlowConsumerBeforeNextBatch(t *testing.T) {
	hub := newTestWatcherHub(t, 1) // buffer of exactly one batch

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	sub, err := hub.AddWatcher(ctx, nil)
	require.NoError(t, err)
	require.Equal(t, 1, hub.subCount())

	// First batch fits in the buffer; the (never-reading) consumer stays alive.
	hub.broadcast(batch(10))
	require.Equal(t, 1, hub.subCount(), "sub should survive a batch that fits the buffer")

	// Second batch overflows. The fix evicts synchronously inside broadcast.
	hub.broadcast(batch(11))
	require.Equal(t, 0, hub.subCount(),
		"slow sub must be evicted synchronously, before any later batch can reach it")

	// A later batch must not be delivered to the evicted sub.
	hub.broadcast(batch(12))

	// The sub delivered a contiguous prefix ([10]) and is now closed — never a
	// gap such as [10, 12].
	var got []uint64
	for evs := range sub {
		got = append(got, evs[0].Revision)
	}
	require.Equal(t, []uint64{10}, got,
		"evicted sub must have received a contiguous prefix with no gap")
}

// TestStreamDeliversGapFreePrefixToSlowConsumer drives the real Stream loop with
// a consumer that drains slowly, and asserts the delivered revisions are a
// strictly increasing contiguous prefix (no skipped batch) up to the point of
// eviction.
func TestStreamDeliversGapFreePrefixToSlowConsumer(t *testing.T) {
	hub := newTestWatcherHub(t, 4)

	input := make(chan []*proto.Event)
	done := make(chan struct{})
	go func() { hub.Stream(input); close(done) }()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	sub, err := hub.AddWatcher(ctx, nil)
	require.NoError(t, err)

	// Consumer that drains one batch every few ms — slow enough to eventually be
	// evicted under a faster producer.
	var got []uint64
	consumerDone := make(chan struct{})
	go func() {
		defer close(consumerDone)
		for evs := range sub {
			got = append(got, evs[0].Revision)
			time.Sleep(2 * time.Millisecond)
		}
	}()

	for rev := uint64(1); rev <= 200; rev++ {
		input <- batch(rev)
	}
	close(input)
	<-done
	<-consumerDone

	require.NotEmpty(t, got)
	for i := 1; i < len(got); i++ {
		require.Equal(t, got[i-1]+1, got[i],
			"delivered revisions must be a contiguous prefix; gap between %d and %d", got[i-1], got[i])
	}
	require.Equal(t, uint64(1), got[0], "delivery must start at the first batch")
}
