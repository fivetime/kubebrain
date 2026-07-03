// Copyright 2022 ByteDance and/or its affiliates
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
	"bytes"
	"context"
	"sync"
	"sync/atomic"
	"time"

	"k8s.io/klog/v2"

	proto "github.com/kubewharf/kubebrain-client/api/v2rpc"

	"github.com/kubewharf/kubebrain/pkg/metrics"
)

const (
	// TODO: from start options or self-adaptive
	watchBuffer = 10000
)

// WatcherHub maintain registry of Watcher
type WatcherHub struct {
	sync.RWMutex
	// subs maps each subscriber channel to the key prefix it watches, so the hub
	// can skip fanning a batch to subscribers none of whose keys it contains
	// (#69). An empty prefix matches everything (watch-all). The authoritative
	// per-event filtering (prefix + start revision) still happens downstream in
	// processEvents; this routing only avoids waking subscribers that would
	// filter the whole batch away.
	subs      map[chan []*proto.Event][]byte
	metricCli metrics.Metrics
	// bufSize is the per-subscriber channel buffer; 0 means watchBuffer.
	bufSize int
	// publishedRev is the highest batch-max revision that broadcast has fully
	// fanned out to every matching subscriber (atomic). It is the safe watermark
	// for a quiet watch's progress notification: because it is stamped only after
	// fan-out, every event with revision <= publishedRev has already been enqueued
	// (FIFO) into its subscriber ahead of any progress marker carrying that value,
	// so advertising it can never claim an undelivered event. handleWatchEventOverflow
	// also advances it to the reset target so a post-reset marker never advertises
	// a stale-low revision.
	publishedRev uint64
}

// storeMaxUint64 atomically advances *addr to val, never moving it backwards.
// Duplicated from server/etcd (the two packages share no util); kept in sync.
func storeMaxUint64(addr *uint64, val uint64) {
	for {
		old := atomic.LoadUint64(addr)
		if val <= old {
			return
		}
		if atomic.CompareAndSwapUint64(addr, old, val) {
			return
		}
	}
}

// newProgressMarker builds an in-band progress marker: a one-element batch whose
// single event carries only a revision and a nil Kv. Real events always set Kv,
// so a nil Kv unambiguously distinguishes a marker from an event batch.
func newProgressMarker(rev uint64) []*proto.Event {
	return []*proto.Event{{Revision: rev}}
}

// isProgressMarker reports whether events is an in-band progress marker (see
// newProgressMarker) rather than a real event batch.
func isProgressMarker(events []*proto.Event) bool {
	return len(events) == 1 && events[0] != nil && events[0].Kv == nil
}

// PublishedRevision returns the highest revision fully fanned out to subscribers.
func (w *WatcherHub) PublishedRevision() uint64 {
	return atomic.LoadUint64(&w.publishedRev)
}

// AdvancePublishedRevision advances the published watermark to target (never
// backwards). Called after a watch-overflow reset jumps the current revision so a
// subsequent progress marker does not advertise a stale-low revision.
func (w *WatcherHub) AdvancePublishedRevision(target uint64) {
	storeMaxUint64(&w.publishedRev, target)
}

func (w *WatcherHub) subBufferSize() int {
	if w.bufSize > 0 {
		return w.bufSize
	}
	return watchBuffer
}

// AddWatcher registers a subscriber watching the given key prefix. The prefix is
// used only for coarse fan-out routing (see WatcherHub.subs); the authoritative
// prefix + revision filtering still runs in the upper/processEvents layer.
func (w *WatcherHub) AddWatcher(ctx context.Context, prefix []byte) (<-chan []*proto.Event, error) {
	w.metricCli.EmitCounter("watcher_hub.add_watcher", 1)
	w.Lock()
	defer w.Unlock()

	// set watch buffer
	sub := make(chan []*proto.Event, w.subBufferSize())
	if w.subs == nil {
		w.subs = map[chan []*proto.Event][]byte{}
	}
	// copy the prefix; the caller's backing array may be reused/mutated.
	w.subs[sub] = append([]byte(nil), prefix...)
	go func() {
		<-ctx.Done()
		klog.InfoS("ctx done, delete watcher %v", "chan", sub)
		w.DeleteWatcher(sub, true)
	}()

	return sub, nil
}

// DeleteWatcher delete watcher
func (w *WatcherHub) DeleteWatcher(sub chan []*proto.Event, lock bool) {
	w.metricCli.EmitCounter("watcher_hub.delete_watcher", 1)
	if lock {
		w.Lock()
	}
	if _, ok := w.subs[sub]; ok {
		klog.InfoS("close event chan in watcher hub", "chan", sub)
		close(sub)
		delete(w.subs, sub)
	}
	if lock {
		w.Unlock()
	}
}

// CloseAll closes all watchers and forces clients to re-list before watching again.
func (w *WatcherHub) CloseAll() {
	w.metricCli.EmitCounter("watcher_hub.close_all", 1)
	w.Lock()
	defer w.Unlock()
	for sub := range w.subs {
		w.DeleteWatcher(sub, false)
	}
}

// Stream push events to watchers.
func (w *WatcherHub) Stream(input chan []*proto.Event) {
	// A once-per-second progress tick fans an in-band marker carrying the current
	// published watermark to every subscriber (including quiet ones the event
	// broadcast skips), so a quiet watch's progress notification advances with the
	// cluster's published revision instead of freezing at its start revision. The
	// marker rides the same FIFO subscriber channel as events, so it can never
	// overtake an unsent matching event (see publishedRev).
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		select {
		case item, ok := <-input:
			if !ok {
				w.Lock()
				klog.Info("[watcher hub] input channel from heap closed, delete all watchers")
				for sub := range w.subs {
					w.DeleteWatcher(sub, false)
				}
				w.Unlock()
				return
			}
			w.broadcast(item)
		case <-ticker.C:
			w.broadcastProgress(atomic.LoadUint64(&w.publishedRev))
		}
	}
}

// broadcast delivers one event batch to every subscriber, then synchronously
// evicts any subscriber whose buffer was full.
//
// Eviction must happen here, before the next batch: sending a slow subscriber a
// later batch after it missed one would deliver a gap (…N-1, N+1 with N
// dropped) that a kube-apiserver reflector never recovers from. It also must
// happen outside the RLock — DeleteWatcher takes the write lock, so evicting
// inline would deadlock. Since Stream is the sole sender, a sub removed here
// receives no further batch: the client sees a contiguous prefix then a clean
// cancel and re-watches/re-lists cleanly.
func (w *WatcherHub) broadcast(item []*proto.Event) {
	var slow []chan []*proto.Event
	skipped := 0
	w.RLock()
	for sub, prefix := range w.subs {
		// Coarse routing: if the batch contains no key under this subscriber's
		// prefix, skip it entirely — no send, no downstream goroutine wakeup, no
		// filter allocation. A skipped subscriber misses nothing it watches, so
		// its stream stays contiguous (#69).
		if !batchMatchesPrefix(item, prefix) {
			skipped++
			continue
		}
		select {
		case sub <- item:
		default:
			slow = append(slow, sub)
		}
	}
	w.RUnlock()
	// Stamp the published watermark after fan-out. Batches arrive strictly
	// ascending in revision (the collector emits in order and Stream is FIFO), so
	// the last element is the batch max. Because this happens after every matching
	// sub has been enqueued the batch, a later progress marker carrying this
	// revision provably sits behind every event <= it.
	if len(item) > 0 {
		storeMaxUint64(&w.publishedRev, item[len(item)-1].Revision)
	}
	if skipped > 0 {
		w.metricCli.EmitCounter("watcher_hub.route_skipped", skipped)
	}
	for _, sub := range slow {
		klog.InfoS("drop slow consumer", "chan", sub, "bufSize", w.subBufferSize())
		w.metricCli.EmitCounter("drop.slow.watcher", 1)
		w.DeleteWatcher(sub, true)
	}
}

// broadcastProgress fans an in-band progress marker carrying rev to every
// subscriber, bypassing the prefix routing that broadcast uses — quiet
// subscribers (whose prefix matches no event) are exactly the ones that need it.
// A full subscriber is skipped, never evicted: a dropped marker only delays that
// watch's progress by one tick, whereas a dropped event would tear a gap in the
// stream. Runs on the same goroutine as broadcast, so it never overlaps a
// concurrent event fan-out on the same subscriber.
func (w *WatcherHub) broadcastProgress(rev uint64) {
	if rev == 0 {
		return
	}
	marker := newProgressMarker(rev)
	w.RLock()
	for sub := range w.subs {
		select {
		case sub <- marker:
		default:
			// full sub: skip, never evict — the next tick retries.
		}
	}
	w.RUnlock()
}

// batchMatchesPrefix reports whether any event in the batch has the given prefix.
// An empty prefix (watch-all) matches every batch. It early-exits on the first
// match, so a subscriber whose resource is present in the batch costs O(1) in the
// common single-resource batch.
func batchMatchesPrefix(events []*proto.Event, prefix []byte) bool {
	if len(prefix) == 0 {
		return true
	}
	for _, e := range events {
		if e.Kv != nil && bytes.HasPrefix(e.Kv.Key, prefix) {
			return true
		}
	}
	return false
}
