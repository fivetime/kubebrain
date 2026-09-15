package backend

import (
	"context"
	"errors"
	"testing"
	"time"

	proto "github.com/kubewharf/kubebrain-client/api/v2rpc"
	"github.com/kubewharf/kubebrain/pkg/metrics"
	"github.com/stretchr/testify/require"
)

type watchDispatchRecorder struct{ compactMetricRecorder }

func (r *watchDispatchRecorder) EmitHistogram(name string, value interface{}, tags ...metrics.T) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.records = append(r.records, compactMetricRecord{kind: "histogram", name: name, value: value, tags: append([]metrics.T(nil), tags...)})
	return errors.New("metrics failure must not affect event delivery")
}

func TestWatchDispatchDurationUnits(t *testing.T) {
	r := &watchDispatchRecorder{}
	emitWatchDispatchDuration(r, "watch.collector.enqueue_duration_seconds", 250*time.Millisecond, "enqueued")
	require.Equal(t, []compactMetricRecord{{kind: "histogram", name: "watch.collector.enqueue_duration_seconds", value: 0.25, tags: []metrics.T{metrics.Tag("outcome", "enqueued")}}}, r.snapshot())
	require.NotPanics(t, func() { emitWatchDispatchDuration(nil, "unused", time.Second, "complete") })
}

func TestWatchCollectorEnqueueMetrics(t *testing.T) {
	for _, canceled := range []bool{false, true} {
		outcome := "enqueued"
		if canceled {
			outcome = "canceled"
		}
		t.Run(outcome, func(t *testing.T) {
			r := &watchDispatchRecorder{}
			b := &backend{metricCli: r, watchChan: make(chan []*proto.Event, 1)}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			first, second := keyBatch(1, "/a"), keyBatch(2, "/b")
			if canceled {
				b.watchChan <- first // full queue: only cancellation can win
				cancel()
			}
			require.Equal(t, !canceled, b.enqueueWatchBatch(ctx, second))
			got := <-b.watchChan
			if canceled {
				require.Equal(t, first, got)
			} else {
				require.Equal(t, second, got)
			}
			require.Empty(t, b.watchChan)
			records := r.snapshot()
			require.Len(t, records, 1)
			require.Equal(t, "watch.collector.enqueue_duration_seconds", records[0].name)
			require.Equal(t, []metrics.T{metrics.Tag("outcome", outcome)}, records[0].tags)
			require.GreaterOrEqual(t, records[0].value.(float64), 0.0)
		})
	}
}

func TestWatchBroadcastDurationPreservesDelivery(t *testing.T) {
	r := &watchDispatchRecorder{}
	sub := make(chan []*proto.Event, 1)
	hub := &WatcherHub{metricCli: r, subs: map[chan []*proto.Event][]byte{sub: []byte("/a")}}
	events := keyBatch(7, "/a/key")
	hub.broadcast(events)
	require.Equal(t, events, <-sub)
	require.Equal(t, uint64(7), hub.PublishedRevision())
	records := r.snapshot()
	require.Len(t, records, 1)
	require.Equal(t, "watcher_hub.broadcast_duration_seconds", records[0].name)
	require.Equal(t, []metrics.T{metrics.Tag("outcome", "complete")}, records[0].tags)
	require.GreaterOrEqual(t, records[0].value.(float64), 0.0)
}
