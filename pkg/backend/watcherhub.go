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
	"github.com/kubewharf/kubebrain/pkg/util"
)

const (
	// watchBuffer is the default per-subscriber channel buffer (batches, not
	// events); override with --watch-fanout-buffer.
	watchBuffer = 10000
	// catchUpChunk bounds the batch size a catch-up replay sends downstream, so
	// a subscriber that fell 100k ring events behind is fed digestible batches
	// instead of one giant slice.
	catchUpChunk = 1024
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
	// catchingUp holds subscribers evicted from live fan-out after a full
	// buffer while a per-subscriber goroutine replays their missed tail from
	// the watch-cache ring and re-attaches them (see beginCatchUp). A
	// subscriber is in subs or catchingUp, never both. Once here, its channel
	// is owned by the catch-up goroutine: only that goroutine may close it
	// (every exit path except a successful re-attach closes it); DeleteWatcher
	// and CloseAll only signal st.stop().
	catchingUp map[chan []*proto.Event]*catchUpState
	// ringLookup reads the watch-cache ring from a revision (inclusive);
	// injected by the backend after construction. nil disables catch-up and
	// restores the old drop-on-full behavior.
	ringLookup func(fromRev uint64) *FindRet
	// bufSize is the per-subscriber channel buffer; 0 means watchBuffer.
	bufSize int
	// progressInterval is the in-band progress-marker fan-out cadence; <=0 uses
	// defaultWatchProgressNotifyInterval.
	progressInterval time.Duration
	// publishedRev is the highest batch-max revision that broadcast has fully
	// fanned out to every matching subscriber (atomic). It is the safe watermark
	// for a quiet watch's progress notification: because it is stamped only after
	// fan-out, every event with revision <= publishedRev has already been enqueued
	// (FIFO) into its subscriber ahead of any progress marker carrying that value,
	// so advertising it can never claim an undelivered event. handleWatchEventOverflow
	// also advances it to the reset target so a post-reset marker never advertises
	// a stale-low revision.
	publishedRev uint64
	// progressKick asks the Stream loop for one immediate progress fan-out ahead
	// of the periodic ticker. It is fed by KickProgress on the watch
	// ProgressRequest path: kube-apiserver's ConsistentListFromCache polls
	// RequestProgress every 100ms with a 3s block timeout, and a watermark that
	// only advances on the ticker cadence makes every consistent read pay up to
	// one full interval — or, with a misconfigured interval > 3s, time out and
	// fall back to an O(all-keys) LIST against storage. Buffered(1) so kicks
	// coalesce; fan-out stays confined to the Stream goroutine (no new races).
	progressKick chan struct{}
}

// KickProgress requests one immediate progress fan-out (see progressKick).
// Non-blocking: concurrent kicks coalesce into the one already pending.
func (w *WatcherHub) KickProgress() {
	if w.progressKick == nil {
		return
	}
	select {
	case w.progressKick <- struct{}{}:
	default:
	}
}

// catchUpState is the hub-side handle of a subscriber in ring catch-up.
type catchUpState struct {
	prefix []byte
	// done asks the catch-up goroutine to stop (watcher ctx gone, or the hub is
	// closing all watchers). Signalled via stop() so double-delete is safe.
	done     chan struct{}
	stopOnce sync.Once
}

func (st *catchUpState) stop() { st.stopOnce.Do(func() { close(st.done) }) }

// NewProgressMarker builds an in-band progress marker: a one-element batch whose
// single event carries only a revision and a nil Kv. Real events always set Kv,
// so a nil Kv unambiguously distinguishes a marker from an event batch.
func NewProgressMarker(rev uint64) []*proto.Event {
	return []*proto.Event{{Revision: rev}}
}

// IsProgressMarker reports whether events is an in-band progress marker (see
// NewProgressMarker) rather than a real event batch.
func IsProgressMarker(events []*proto.Event) bool {
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
	util.StoreMaxUint64(&w.publishedRev, target)
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
		klog.V(4).InfoS("ctx done, deleting watcher", "chan", sub)
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
	} else if st, ok := w.catchingUp[sub]; ok {
		// The catch-up goroutine owns the channel: removing the entry here and
		// signalling stop makes that goroutine close sub and exit; closing it
		// here would race its in-flight replay sends.
		delete(w.catchingUp, sub)
		st.stop()
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
	for sub, st := range w.catchingUp {
		delete(w.catchingUp, sub)
		st.stop()
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
	interval := w.progressInterval
	if interval <= 0 {
		interval = defaultWatchProgressNotifyInterval
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case item, ok := <-input:
			if !ok {
				klog.Info("[watcher hub] input channel from heap closed, delete all watchers")
				w.CloseAll()
				return
			}
			w.broadcast(item)
		case <-ticker.C:
			w.broadcastProgress(atomic.LoadUint64(&w.publishedRev))
		case <-w.progressKick:
			// On-demand fan-out for an in-flight RequestProgress (see progressKick).
			// Same FIFO marker as the ticker path, so it can never overtake an
			// unsent matching event.
			w.broadcastProgress(atomic.LoadUint64(&w.publishedRev))
		}
	}
}

// broadcast delivers one event batch to every subscriber, then synchronously
// detaches any subscriber whose buffer was full.
//
// Detaching must happen here, before the next batch: sending a slow subscriber
// a later batch after it missed one would deliver a gap (…N-1, N+1 with N
// dropped) that a kube-apiserver reflector never recovers from. It also must
// happen outside the RLock — the detach takes the write lock, so doing it
// inline would deadlock. Since Stream is the sole live sender, a sub detached
// here receives no further fan-out batch; its missed tail is instead replayed
// from the watch-cache ring by a catch-up goroutine that re-attaches it once
// level (#34) — dropping it outright would force the consumer to re-list the
// whole keyspace (O(all-keys) at scale), and at 500k+ objects those re-lists
// burn enough CPU to make more watchers slow: a vicious circle. Only a
// subscriber whose backlog has already been evicted from the ring (or a ring
// reset) is dropped, exactly as before.
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
		util.StoreMaxUint64(&w.publishedRev, item[len(item)-1].Revision)
	}
	if skipped > 0 {
		w.metricCli.EmitCounter("watcher_hub.route_skipped", skipped)
	}
	for _, sub := range slow {
		w.beginCatchUp(sub, item)
	}
}

// beginCatchUp moves a full subscriber from live fan-out into ring catch-up:
// a goroutine replays its missed tail from the watch-cache ring and re-attaches
// it once level. missed is the first batch the subscriber failed to accept —
// everything before it is already in its FIFO buffer, so the replay starts at
// the batch's first revision. Falls back to the legacy drop when no ring is
// wired.
func (w *WatcherHub) beginCatchUp(sub chan []*proto.Event, missed []*proto.Event) {
	if w.ringLookup == nil || len(missed) == 0 {
		klog.InfoS("drop slow consumer", "chan", sub, "bufSize", w.subBufferSize())
		w.metricCli.EmitCounter("drop.slow.watcher", 1)
		w.DeleteWatcher(sub, true)
		return
	}
	fromRev := missed[0].Revision
	w.Lock()
	prefix, ok := w.subs[sub]
	if !ok {
		// Already deleted (watcher ctx raced us); nothing to do.
		w.Unlock()
		return
	}
	delete(w.subs, sub)
	st := &catchUpState{prefix: prefix, done: make(chan struct{})}
	if w.catchingUp == nil {
		w.catchingUp = map[chan []*proto.Event]*catchUpState{}
	}
	w.catchingUp[sub] = st
	w.Unlock()
	w.metricCli.EmitCounter("watcher_hub.catch_up.entered", 1)
	klog.InfoS("slow consumer entering ring catch-up", "chan", sub,
		"prefix", string(prefix), "fromRev", fromRev, "bufSize", w.subBufferSize())
	go w.catchUp(sub, st, fromRev)
}

// catchUp replays ring events >= fromRev to sub until it is level with the
// ring, then re-attaches it to live fan-out. Correctness of the re-attach:
// every event is Add()ed to the ring BEFORE it is fanned out (collector
// order), and broadcast holds the hub read lock — so under the hub WRITE lock,
// "the ring holds nothing >= fromRev" proves no event the subscriber missed
// exists anywhere ahead of the fan-out, and re-attaching is gap-free: the next
// broadcast batch it receives carries revisions >= fromRev.
//
// Every exit path except the successful re-attach closes sub (this goroutine
// owns the channel once catch-up begins): the downstream watcher sees a clean
// close and cancels, exactly like the legacy drop.
func (w *WatcherHub) catchUp(sub chan []*proto.Event, st *catchUpState, fromRev uint64) {
	for {
		ret := w.ringLookup(fromRev)
		if ret.empty || ret.low {
			// Ring reset (watch-event overflow) or the backlog was already
			// evicted: too far behind to replay cheaply. Drop, as before #34.
			w.finishCatchUp(sub, st, "backlog beyond ring, dropping", "drop.slow.watcher")
			return
		}
		if !ret.high {
			evs := ret.events
			for len(evs) > 0 {
				n := len(evs)
				if n > catchUpChunk {
					n = catchUpChunk
				}
				chunk := evs[:n]
				evs = evs[n:]
				if !batchMatchesPrefix(chunk, st.prefix) {
					continue
				}
				select {
				case sub <- chunk:
				case <-st.done:
					w.finishCatchUp(sub, st, "catch-up stopped", "")
					return
				}
			}
			fromRev = ret.events[len(ret.events)-1].Revision + 1
		}
		// Try to re-attach under the write lock (see function comment).
		w.Lock()
		if _, still := w.catchingUp[sub]; !still {
			// DeleteWatcher/CloseAll raced us and gave up the entry; we still own
			// the channel and must close it on the way out.
			w.Unlock()
			w.finishCatchUp(sub, st, "catch-up stopped", "")
			return
		}
		ret = w.ringLookup(fromRev)
		if ret.empty || ret.low {
			w.Unlock()
			w.finishCatchUp(sub, st, "backlog beyond ring, dropping", "drop.slow.watcher")
			return
		}
		if ret.high {
			// Level: nothing in the ring (hence nothing fanned out) >= fromRev.
			delete(w.catchingUp, sub)
			w.subs[sub] = st.prefix
			w.Unlock()
			w.metricCli.EmitCounter("watcher_hub.catch_up.recovered", 1)
			klog.InfoS("slow consumer caught up, re-attached", "chan", sub, "nextRev", fromRev)
			return
		}
		// The ring advanced while we replayed; go replay the new tail.
		w.Unlock()
	}
}

// finishCatchUp terminates a catch-up (drop or stop): removes the hub entry if
// still present and closes the subscriber channel, which this goroutine owns.
func (w *WatcherHub) finishCatchUp(sub chan []*proto.Event, st *catchUpState, msg string, dropMetric string) {
	w.Lock()
	delete(w.catchingUp, sub)
	w.Unlock()
	close(sub)
	klog.InfoS(msg, "chan", sub, "prefix", string(st.prefix))
	if dropMetric != "" {
		w.metricCli.EmitCounter(dropMetric, 1)
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
	marker := NewProgressMarker(rev)
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
