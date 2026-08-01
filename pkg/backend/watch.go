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
	"fmt"
	"io"
	"sort"
	"strconv"

	"k8s.io/klog/v2"

	proto "github.com/kubewharf/kubebrain-client/api/v2rpc"

	"github.com/kubewharf/kubebrain/pkg/metrics"
)

const (
	resultChanLength = 100

	// historyScanConcurrency caps concurrent watch-history fallback scans so a
	// reconnect storm after a cache reset cannot stampede the storage engine
	// (#30). Chosen generously so steady-state watch establishment is never
	// throttled; only an actual herd of simultaneous cache-miss scans queues.
	historyScanConcurrency = 8
)

// Watch return a channel, every event‘s ModRevision >= revision
// and has specify prefix will be read from channel
// if revision < 0, invalid revision
// if revision == 0, start reading events from read channel
func (b *backend) Watch(ctx context.Context, prefix string, revision uint64) (<-chan []*proto.Event, error) {

	klog.V(2).InfoS("WATCH", "prefix", prefix, "revision", revision)

	// starting watching right away so we don't miss anything
	ctx, cancel := context.WithCancel(ctx)
	readChan, err := b.watcherHub.AddWatcher(ctx, []byte(prefix))
	if err != nil {
		cancel()
		klog.ErrorS(err, "add watcher failed", "subscription", watchChannelID(readChan))
		return nil, err
	}

	result := make(chan []*proto.Event, resultChanLength)

	// Captured right after AddWatcher: the published-revision floor for this
	// subscription. Every event with revision > neededRevision is guaranteed to
	// arrive on readChan, so a history fallback only needs to reach this far. It
	// gates whether a SHARED history scan (snapshotted possibly before we
	// subscribed) covers enough for us; see historyWatchEvents.
	neededRevision := b.GetPublishedRevision()

	// include the current revision in list
	if revision == 0 {
		go b.processEvents(ctx, cancel, result, readChan, prefix, revision)
		return result, nil
	}

	ret := b.watchCache.FindEvents(revision)

	if ret.empty {
		currentRevision, currentErr := b.safeCurrentRevision(ctx)
		if currentErr != nil {
			cancel()
			return nil, currentErr
		}
		if revision > currentRevision {
			// watch revision is latest, no need to fetch history
			go b.processEvents(ctx, cancel, result, readChan, prefix, revision)
			return result, nil
		}
		return b.startFromHistory(ctx, cancel, result, readChan, prefix, revision, currentRevision, neededRevision, "watch history fallback")
	}

	if ret.high {
		go b.processEvents(ctx, cancel, result, readChan, prefix, revision)
		return result, nil
	}

	if ret.low {
		currentRevision, currentErr := b.safeCurrentRevision(ctx)
		if currentErr != nil {
			cancel()
			return nil, currentErr
		}
		return b.startFromHistory(ctx, cancel, result, readChan, prefix, revision, currentRevision, neededRevision, "watch history fallback from low cache")
	}

	events := filterByPrefix(ret.events, []byte(prefix))

	klog.InfoS("watch list", "prefix", prefix, "revision", revision, "latestRev", ret.newest.Revision, "cachedEvents", len(events))

	lastRevision := revision
	if len(events) > 0 {
		lastRevision = events[len(events)-1].Revision + 1
		b.catchUpEvents(result, events)
	}
	go b.processEvents(ctx, cancel, result, readChan, prefix, lastRevision)

	return result, nil
}

// startFromHistory replays [revision, currentRevision] from storage into result,
// then hands the live tail to processEvents starting just past the replayed
// events. On a history-fallback failure it cancels and returns the real error:
// historyWatchEvents distinguishes a genuine "compacted" (client re-lists) from
// a transient error (client retries), and flattening them left a truly-compacted
// watch retrying forever instead of re-listing (#55). Shared by Watch's
// empty-cache and low-cache branches, which differ only in their log label.
func (b *backend) startFromHistory(ctx context.Context, cancel context.CancelFunc,
	result chan []*proto.Event, readChan <-chan []*proto.Event,
	prefix string, revision, currentRevision, neededRevision uint64, label string) (<-chan []*proto.Event, error) {
	events, historyErr := b.historyWatchEvents(ctx, prefix, revision, currentRevision, neededRevision)
	if historyErr != nil {
		cancel()
		klog.ErrorS(historyErr, label+" failed", "prefix", prefix, "revision", revision)
		return nil, historyErr
	}
	klog.InfoS(label, "prefix", prefix, "revision", revision, "events", len(events))
	lastRevision := revision
	if len(events) > 0 {
		b.catchUpEvents(result, events)
		lastRevision = events[len(events)-1].Revision + 1
	}
	go b.processEvents(ctx, cancel, result, readChan, prefix, lastRevision)
	return result, nil
}

// catchUpEvents send stacked events to channel
func (b *backend) catchUpEvents(out chan<- []*proto.Event, events []*proto.Event) {
	batchSize := eventBatchSize
	// to avoid result chan full and hang
	if len(events) > resultChanLength*eventBatchSize {
		batchSize = len(events) / (resultChanLength - 1)
	}
	for {
		if len(events) > batchSize {
			out <- events[0:batchSize]
			events = events[batchSize:]
		} else {
			out <- events
			break
		}
	}
}

func (b *backend) historyWatchEvents(ctx context.Context, prefix string, fromRevision, currentRevision, neededRevision uint64) ([]*proto.Event, error) {
	compactRevision, err := b.GetCompactRevision(ctx)
	if err != nil {
		return nil, err
	}
	if compactRevision > 0 && fromRevision < compactRevision {
		return nil, fmt.Errorf("cache event oldest revision is compacted at %d newer than requested revision %d", compactRevision, fromRevision)
	}
	// A restart commonly asks for exactly the last committed revision while the
	// leadership hook has conservatively moved elogStart to that same value. V2
	// event entries repeat the transaction's total event count, so this one
	// revision can prove itself complete and preserve sub-revision order without
	// falling through to the key-sorted object scan. If it is legacy/incomplete,
	// retain the normal conservative fallback below.
	if elogStart, hasEventLogStart := b.getEventLogStart(ctx); fromRevision == currentRevision && hasEventLogStart && fromRevision <= elogStart {
		if exact, served, exactErr := b.eventLogWatchEvents(ctx, prefix, fromRevision, currentRevision); exactErr != nil {
			return nil, exactErr
		} else if served {
			return exact, nil
		}
	}

	// Collapse a reconnect herd into one shared storage scan (#30): watchers
	// reconnecting to the same prefix at nearby revisions — HA-apiserver replicas
	// re-establishing a resource's watch after a cold-cache failover — share a
	// single scan whose result each reuses read-only, instead of each issuing its
	// own. Exact-revision keying would rarely match (replicas relist at slightly
	// different revisions), so the key is BUCKETED: reconnects whose revisions
	// share a HistoryScanRevBucket-wide window collapse onto one scan.
	//
	// The shared scan runs from the bucket floor (so it serves every caller in
	// the bucket) up to currentRevision; each caller then keeps only the events
	// at or after ITS own revision. scanFrom is anchored near fromRevision — it
	// is NOT dropped to the compact watermark: without an active compactor the
	// watermark is 0, and emitting every version since revision 1 would be an
	// unbounded O(all-history) list that never returns (and breaks the
	// massive-scale invariant). Bucketing widens the shared window by at most one
	// bucket beyond what the caller asked for; the full-prefix read is identical.
	bucket := b.config.HistoryScanRevBucket
	if bucket == 0 {
		bucket = defaultHistoryScanRevBucket
	}
	scanFrom := fromRevision - (fromRevision % bucket)
	if scanFrom < 1 {
		scanFrom = 1
	}
	// Never scan BELOW the compact watermark: those versions may be GC'd, and it
	// keeps scanFrom (hence the shared key and window) consistent for everyone in
	// the bucket. Clamp to compactRevision itself, not compactRevision+1: the
	// version at exactly compactRevision is retained by compaction, and a watcher
	// reconnecting at fromRevision == compactRevision (accepted by the strict
	// fromRevision < compactRevision guard above) must still receive the event at
	// its own start revision — clamping to +1 would silently drop it.
	if compactRevision > 0 && scanFrom < compactRevision {
		scanFrom = compactRevision
	}
	// Lift the bucket floor just above the event-log watermark when the caller
	// itself starts above it (review #51): right after a leadership change
	// pushes the watermark, a reconnect's own fromRevision often clears it while
	// its bucket floor lands below — disqualifying the log for the WHOLE herd
	// and degrading every reconnect to the full-prefix scan this path exists to
	// avoid. The lift stays <= fromRevision, so the shared window still covers
	// this caller; members whose fromRevision sits at/below the watermark
	// compute their own lower floor (and key) and simply don't share this scan.
	if elogStart, ok := b.getEventLogStart(ctx); ok && scanFrom <= elogStart && elogStart < fromRevision {
		scanFrom = elogStart + 1
	}
	key := prefix + "\x00" + strconv.FormatUint(scanFrom, 10)
	// The executor of the shared scan reports the revision it is complete up to
	// (coveredRev) so a waiter can check the shared result reaches far enough for
	// its own from-now subscription (see the guard below).
	events, coveredRev, scanErr, _ := b.historyScanGroup.Do(ctx, key, func(sctx context.Context) ([]*proto.Event, uint64, error) {
		// Prefer the event log (#45): one bounded range read over
		// [scanFrom, currentRevision] plus point reads for the values —
		// O(events in the window) — instead of the full-prefix object scan,
		// which at 10M+ keys takes minutes and cannot outrun the write rate.
		// served=false (window predates the log, or a referenced version is
		// missing) falls back to the scan, which remains fully correct.
		evs, served, err := b.eventLogWatchEvents(sctx, prefix, scanFrom, currentRevision)
		if err != nil {
			return nil, 0, err
		}
		if !served {
			evs, err = b.boundedHistoryScan(sctx, prefix, scanFrom, currentRevision)
			if err != nil {
				return nil, 0, err
			}
		}
		// Extend coverage from the storage snapshot (currentRevision) up to the
		// ring's newest revision, so watchers that subscribed WHILE this
		// seconds-long scan ran still find their events covered and share this
		// result instead of each re-scanning (the completeness guard below). The
		// gap (currentRevision, newest] is recent, so it sits in the warm ring;
		// append it. If the ring cannot serve currentRevision+1 (still cold, or
		// already evicted), coverage stays at currentRevision and stragglers
		// re-scan — correct, just not shared.
		covered := currentRevision
		if ringEvs, ringCovered := b.ringEventsFrom([]byte(prefix), currentRevision+1); ringCovered >= currentRevision+1 {
			evs = append(evs, ringEvs...)
			covered = ringCovered
		}
		return evs, covered, nil
	})
	if scanErr != nil {
		return nil, scanErr
	}
	// Completeness guard for shared scans. A scan snapshotted at coveredRev BEFORE
	// this caller subscribed leaves a hole (coveredRev, neededRevision] that the
	// caller's from-now subscription (its readChan) will never redeliver —
	// silently dropping those events. neededRevision is the caller's
	// published-revision floor at subscribe time: every event above it is
	// guaranteed to arrive on the readChan, so history need only reach it. If the
	// shared scan stopped short, redo our own scan up to our currentRevision
	// (>= neededRevision) so the recovered history covers the hole. The executor
	// passes trivially (coveredRev == its currentRevision >= its own floor); only
	// a straggler that joined after the scan's snapshot re-scans.
	if coveredRev < neededRevision {
		own, ownErr := b.boundedHistoryScan(ctx, prefix, scanFrom, currentRevision)
		if ownErr != nil {
			return nil, ownErr
		}
		events = own
	}
	if fromRevision <= scanFrom {
		// Bucket-aligned (or clamped to the watermark): the result is already
		// exactly this caller's window — reuse it read-only, do not mutate.
		return events, nil
	}
	// Keep only the events at or after this caller's revision; a shared slice (and
	// its event pointers) must not be mutated, so allocate a new one.
	out := make([]*proto.Event, 0, len(events))
	for _, e := range events {
		if e.Revision >= fromRevision {
			out = append(out, e)
		}
	}
	return out, nil
}

// ringEventsFrom returns the watch-cache-ring events with revision >=
// fromRevision that match prefix, and the highest revision covered contiguously
// from fromRevision. It reports coveredRev == fromRevision-1 (i.e. "covers
// nothing from here") when the ring is empty, has not caught up to fromRevision
// yet, or has already evicted it — in all those cases the caller must not treat
// the ring as extending coverage. Otherwise the ring holds [oldest, newest]
// contiguously, so returning its events from fromRevision covers up to newest.
func (b *backend) ringEventsFrom(prefix []byte, fromRevision uint64) (events []*proto.Event, coveredRev uint64) {
	ret := b.watchCache.FindEvents(fromRevision)
	if ret.empty || ret.low || ret.high {
		return nil, fromRevision - 1
	}
	return filterByPrefix(ret.events, prefix), ret.newest.Revision
}

// boundedHistoryScan runs scanHistoryEvents under the historyScanSem concurrency
// gate (#30), blocking for a slot or the caller's ctx. Used both by the shared
// singleflight executor and by the completeness-guard re-scan.
func (b *backend) boundedHistoryScan(ctx context.Context, prefix string, fromRevision, currentRevision uint64) ([]*proto.Event, error) {
	select {
	case b.historyScanSem <- struct{}{}:
		defer func() { <-b.historyScanSem }()
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	return b.scanHistoryEvents(ctx, prefix, fromRevision, currentRevision)
}

// scanHistoryEvents does the actual full-prefix storage scan behind
// historyWatchEvents, returning every event under prefix with revision in
// [fromRevision, currentRevision]. It is invoked through historyScanGroup so a
// reconnect herd shares one execution rather than each issuing its own scan.
func (b *backend) scanHistoryEvents(ctx context.Context, prefix string, fromRevision, currentRevision uint64) ([]*proto.Event, error) {
	prefixBytes := []byte(prefix)
	start, end := b.historyPrefixBounds(prefixBytes)
	iter, err := b.kv.Iter(ctx, start, end, 0, 0)
	if err != nil {
		return nil, err
	}
	defer iter.Close()

	events := make([]*proto.Event, 0)
	// The scan returns every version of every key under the prefix in ascending
	// (key, revision) order, so a DELETE's prev-kv — the newest live version
	// before the tombstone — has already been read earlier in this same scan.
	// Track it here instead of issuing a separate point read per DELETE (which
	// was itself a limit-1 Iter): that turned each history fallback into 1+D
	// scans and, with N watchers reconnecting after a cache reset, a storage
	// thundering herd (#30). prevVal/prevRev are updated for every non-tombstone
	// version — including versions below fromRevision — so an in-window DELETE
	// whose prior version predates the window still recovers its prev-kv.
	var (
		curKey  []byte // user key of the version group currently being scanned
		prevVal []byte // newest non-tombstone value seen for curKey (enveloped)
		prevRev uint64 // its revision
	)
	for {
		if err := iter.Next(ctx); err != nil {
			if err == io.EOF {
				break
			}
			return nil, err
		}
		if b.ks.IsInternalStorageKey(iter.Key()) {
			continue
		}
		key, rev, err := b.coder.Decode(iter.Key())
		if err != nil {
			return nil, err
		}
		if !bytes.HasPrefix(key, prefixBytes) {
			continue
		}
		if rev == 0 || bytes.HasPrefix(key, etcdMetadataPrefix) {
			// revision key or internal metadata: not an event
			continue
		}
		if !bytes.Equal(key, curKey) {
			// entering a new key's version group; reset the tracked previous value
			curKey = append(curKey[:0], key...)
			prevVal = nil
			prevRev = 0
		}
		val := append([]byte(nil), iter.Val()...)
		isTomb := bytes.Equal(val, tombStoneBytes)

		if rev < fromRevision || rev > currentRevision {
			// out of the requested window: don't emit, but keep tracking the
			// previous live version so an in-window tombstone can still find it.
			if !isTomb {
				prevVal = val
				prevRev = rev
			}
			continue
		}

		event := &proto.Event{
			Type:     proto.Event_PUT,
			Revision: rev,
			Kv: &proto.KeyValue{
				Key:      append([]byte(nil), key...),
				Value:    val,
				Revision: rev,
			},
		}
		if isTomb {
			event.Type = proto.Event_DELETE
			if prevVal != nil {
				event.Kv.Value = prevVal
				event.Kv.Revision = prevRev
			} else {
				event.Kv.Value = nil
				event.Kv.Revision = rev
			}
		} else {
			// Prefer the metadata inlined in the value we already read (approach
			// A); fall back to a lookup for legacy un-enveloped values.
			meta, _, ok := decodeValueWithMeta(val)
			if !ok {
				var err error
				meta, err = b.GetEtcdMetadata(ctx, key, rev)
				if err != nil {
					return nil, err
				}
			}
			if meta.CreateRevision == rev && meta.Version == 1 {
				event.Type = proto.Event_CREATE
			}
			prevVal = val
			prevRev = rev
		}
		events = append(events, event)
	}

	sort.SliceStable(events, func(i, j int) bool {
		if events[i].Revision == events[j].Revision {
			return bytes.Compare(events[i].Kv.Key, events[j].Kv.Key) < 0
		}
		return events[i].Revision < events[j].Revision
	})
	return events, nil
}

func (b *backend) historyPrefixBounds(prefix []byte) (start, end []byte) {
	if len(prefix) == 0 {
		start = b.ks.ObjectKeyspaceStart()
	} else {
		encoded := b.coder.EncodeObjectKey(prefix, 0)
		start = append(append([]byte(nil), encoded[:len(encoded)-9]...), 0)
	}
	prefixEnd := PrefixEnd(prefix)
	if isFromKeyEnd(prefixEnd) {
		end = b.ks.ObjectKeyspaceEnd()
	} else {
		end = b.coder.EncodeObjectKey(prefixEnd, 0)
	}
	return start, end
}

func (b *backend) processEvents(ctx context.Context, cancel context.CancelFunc, out chan<- []*proto.Event, in <-chan []*proto.Event,
	prefix string, revision uint64) {
	prefixBytes := []byte(prefix)
	klog.InfoS("start process events chan", "prefix", prefix, "revision", revision)

	defer func() {
		klog.InfoS("events chan closed", "subscription", watchChannelID(in), "prefix", prefix)
		b.metricCli.EmitCounter("watcherhub.events_chan.closed", 1, metrics.Tag("prefix", prefix))
		close(out)
		klog.InfoS("watch channel closed", "prefix", prefix)
		cancel()
	}()

	for {
		select {
		case events, ok := <-in:
			if !ok {
				// channel closed by watcher hub due to slow process or ctx done
				return
			}
			if IsProgressMarker(events) {
				// In-band progress marker (nil Kv): forward verbatim so it is
				// neither prefix/revision-filtered nor allocation-touched
				// (filterEvents would deref event.Kv.Key). It advances a quiet
				// watch's progress downstream without carrying any event.
				select {
				case out <- events:
				case <-ctx.Done():
					return
				}
				continue
			}
			evs := filterEvents(events, revision, prefixBytes)
			if len(evs) == 0 {
				continue
			}
			// The consumer (backendShim transform goroutine) stops reading `out`
			// when its context is cancelled (client disconnect / relist) without
			// draining it. A bare `out <- evs` would then block forever once the
			// buffer fills, leaking this goroutine. Bail out on ctx.Done instead.
			select {
			case out <- evs:
			case <-ctx.Done():
				return
			}
		case <-ctx.Done():
			return
		}
	}
}

func filterByPrefix(events []*proto.Event, prefix []byte) []*proto.Event {
	filteredEventList := make([]*proto.Event, 0, len(events))

	for _, event := range events {
		if bytes.HasPrefix(event.Kv.Key, prefix) {
			filteredEventList = append(filteredEventList, event)
		}
	}

	return filteredEventList
}

// filterEvents returns the events at or after rev whose key has prefix, in a
// single pass. It allocates the result slice lazily — only once at least one
// event matches — so a batch that a watcher filters away entirely (common with
// the hub's coarse fan-out) costs no allocation. Replaces the former
// filterByPrefix(filterByRevision(...)) two-pass, two-allocation path (#69).
func filterEvents(events []*proto.Event, rev uint64, prefix []byte) []*proto.Event {
	var out []*proto.Event
	for i, event := range events {
		if event.Revision < rev {
			continue
		}
		if !bytes.HasPrefix(event.Kv.Key, prefix) {
			continue
		}
		if out == nil {
			out = make([]*proto.Event, 0, len(events)-i)
		}
		out = append(out, event)
	}
	return out
}
