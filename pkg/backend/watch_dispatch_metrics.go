package backend

import (
	"context"
	"time"

	proto "github.com/kubewharf/kubebrain-client/api/v2rpc"
	"github.com/kubewharf/kubebrain/pkg/metrics"
)

// enqueueWatchBatch measures producer backpressure after collection, not the
// batch's residence time in the buffered channel or the TiKV commit duration.
// Keep the original send/cancellation select: instrumentation must not change
// revision ordering or introduce another asynchronous publisher.
func (b *backend) enqueueWatchBatch(ctx context.Context, events []*proto.Event) bool {
	started := time.Now()
	outcome := "enqueued"
	defer func() {
		emitWatchDispatchDuration(b.metricCli, "watch.collector.enqueue_duration_seconds", time.Since(started), outcome)
	}()
	select {
	case <-ctx.Done():
		outcome = "canceled"
		return false
	case b.watchChan <- events:
		return true
	}
}

// Names and outcomes are internal constants, never keys, revisions or IDs.
// Broadcast duration covers locking, routing and synchronous slow-subscriber
// detachment, but not asynchronous catch-up, downstream delivery or gRPC flush.
func emitWatchDispatchDuration(m metrics.Metrics, name string, elapsed time.Duration, outcome string) {
	if m != nil {
		_ = m.EmitHistogram(name, elapsed.Seconds(), metrics.Tag("outcome", outcome))
	}
}
