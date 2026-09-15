package main

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"sync"
	"time"

	"go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/mvccpb"
	"google.golang.org/grpc/stats"
)

const watchDeliveryCapacity = 256

type watchPayloadObservation struct {
	revision  int64
	at        time.Time
	duplicate bool
}

// A callback timestamp is a client-local decoded gRPC payload observation,
// NOT a server send or packet-arrival timestamp. Keep no values or response
// pointers. Only the dedicated public probe key is eligible, so other streaming
// workers on the same client cannot supply an unrelated revision match.
type watchDeliveryRecorder struct {
	stats.Handler
	key  []byte
	now  func() time.Time
	mu   sync.Mutex
	ring [watchDeliveryCapacity]watchPayloadObservation
	next int
}

func (r *watchDeliveryRecorder) HandleRPC(ctx context.Context, event stats.RPCStats) {
	if payload, ok := event.(*stats.InPayload); ok && payload != nil && payload.Client {
		if response, ok := payload.Payload.(*etcdserverpb.WatchResponse); ok && response != nil {
			at := r.now()
			r.mu.Lock()
			for _, e := range response.Events {
				if e == nil || e.Type != mvccpb.PUT || e.Kv == nil || e.Kv.ModRevision <= 0 || !bytes.Equal(e.Kv.Key, r.key) {
					continue
				}
				found := false
				for i := range r.ring {
					if r.ring[i].revision == e.Kv.ModRevision {
						r.ring[i].duplicate = true
						found = true
						break
					}
				}
				if !found {
					r.ring[r.next] = watchPayloadObservation{revision: e.Kv.ModRevision, at: at}
					r.next = (r.next + 1) % len(r.ring)
				}
			}
			r.mu.Unlock()
		}
	}
	// Preserve the existing unary attempt diagnostics and all its context tags.
	r.Handler.HandleRPC(ctx, event)
}

func (r *watchDeliveryRecorder) take(revision int64) (watchPayloadObservation, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for i := range r.ring {
		if revision > 0 && r.ring[i].revision == revision {
			observation := r.ring[i]
			r.ring[i] = watchPayloadObservation{}
			return observation, true
		}
	}
	return watchPayloadObservation{}, false
}

type watchDeliveryProgress struct {
	matched, missing, ambiguous, invalid, beforePut int
	prePayload, postPayload                         time.Duration
}

func (p *watchDeliveryProgress) record(r *watchDeliveryRecorder, revision int64, putResolved, received time.Time) {
	o, ok := r.take(revision)
	switch {
	case !ok:
		p.missing++
	case o.duplicate:
		// Replay/duplicate delivery cannot be uniquely paired; do not invent a
		// latency by selecting one of several observations.
		p.ambiguous++
	case o.at.IsZero() || received.Before(putResolved) || o.at.After(received):
		p.invalid++
	default:
		p.matched++
		boundary := o.at
		if boundary.Before(putResolved) {
			p.beforePut++
			boundary = putResolved
		}
		// Partition ONLY the post-Put interval for matched observations. The
		// pre-Put payload overlap is not counted twice or called network time.
		p.prePayload += boundary.Sub(putResolved)
		p.postPayload += received.Sub(boundary)
	}
}

func (p *watchDeliveryProgress) write(w io.Writer, final bool) error {
	_, err := fmt.Fprintf(w, "PROBE_WATCH_DELIVERY matched=%d missing=%d ambiguous=%d invalid=%d payload_before_put=%d post_put_to_payload_us=%d payload_to_consume_after_put_us=%d final=%t scope=diagnostic_only\n",
		p.matched, p.missing, p.ambiguous, p.invalid, p.beforePut, p.prePayload.Microseconds(), p.postPayload.Microseconds(), final)
	return err
}
