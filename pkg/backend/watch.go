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

	klog.InfoS("WATCH", "prefix", prefix, "revision", revision)

	// starting watching right away so we don't miss anything
	ctx, cancel := context.WithCancel(ctx)
	readChan, err := b.watcherHub.AddWatcher(ctx, []byte(prefix))
	if err != nil {
		cancel()
		klog.ErrorS(err, "add watcher failed", "chan", readChan)
		return nil, err
	}

	result := make(chan []*proto.Event, resultChanLength)

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
		events, historyErr := b.historyWatchEvents(ctx, prefix, revision, currentRevision)
		if historyErr == nil {
			klog.InfoS("watch history fallback", "prefix", prefix, "revision", revision, "events", len(events))
			if len(events) > 0 {
				b.catchUpEvents(result, events)
				revision = events[len(events)-1].Revision + 1
			}
			go b.processEvents(ctx, cancel, result, readChan, prefix, revision)
			return result, nil
		}
		klog.ErrorS(historyErr, "watch history fallback failed", "prefix", prefix, "revision", revision)
		cancel()
		// Propagate the real fallback error. historyWatchEvents returns a proper
		// "compacted" error when the requested revision is below the compact
		// watermark (so the client re-lists) and a plain error otherwise (so the
		// client retries the watch). Do not flatten it into a generic message that
		// hides genuine compaction — that left a truly-compacted watch retrying
		// forever instead of re-listing (#55).
		return nil, historyErr
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
		events, historyErr := b.historyWatchEvents(ctx, prefix, revision, currentRevision)
		if historyErr == nil {
			klog.InfoS("watch history fallback from low cache", "prefix", prefix, "revision", revision, "oldestRev", ret.oldest.Revision, "events", len(events))
			lastRevision := revision
			if len(events) > 0 {
				b.catchUpEvents(result, events)
				lastRevision = events[len(events)-1].Revision + 1
			}
			go b.processEvents(ctx, cancel, result, readChan, prefix, lastRevision)
			return result, nil
		}
		cancel()
		klog.ErrorS(historyErr, "ret low history fallback failed", "prefix", prefix, "revision", revision, "oldestRev", ret.oldest.Revision)
		// Propagate the real fallback error rather than fabricating a
		// "cache event oldest revision ..." message: that string is treated as a
		// compaction cancel, so a transient history-scan failure (e.g. a storage
		// error) was misreported as a compaction and forced a spurious re-list at
		// a bogus revision. historyWatchEvents already returns a proper compacted
		// error when the revision is actually below the compact watermark (#55).
		return nil, historyErr
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

func (b *backend) historyWatchEvents(ctx context.Context, prefix string, fromRevision, currentRevision uint64) ([]*proto.Event, error) {
	compactRevision, err := b.GetCompactRevision(ctx)
	if err != nil {
		return nil, err
	}
	if compactRevision > 0 && fromRevision < compactRevision {
		return nil, fmt.Errorf("cache event oldest revision is compacted at %d newer than requested revision %d", compactRevision, fromRevision)
	}

	// Collapse a reconnect herd into one shared storage scan (#30): watchers
	// reconnecting at the SAME (prefix, fromRevision) — an apiserver restart
	// re-establishing a resource's watch across replicas — share a single scan
	// whose result each reuses read-only, instead of each issuing its own.
	//
	// The scan emits exactly the caller's window [fromRevision, currentRevision]
	// — it is NOT widened down to the compact watermark. Widening would let more
	// callers share, but without an active compactor the watermark is 0, so it
	// would emit every version since revision 1 — an unbounded O(all-history)
	// event list that never returns (and violates the massive-scale invariant).
	// Keeping the window at fromRevision bounds emission to what the watcher
	// actually asked for; the full-prefix read is identical either way.
	key := prefix + "\x00" + strconv.FormatUint(fromRevision, 10)
	events, scanErr, _ := b.historyScanGroup.Do(ctx, key, func(sctx context.Context) ([]*proto.Event, error) {
		// Only the executor of the shared scan consumes a concurrency slot, so a
		// herd on one (prefix,rev) costs a single slot, not one per watcher. Bound
		// it so an unrelated multi-prefix storm still can't stampede storage.
		select {
		case b.historyScanSem <- struct{}{}:
			defer func() { <-b.historyScanSem }()
		case <-sctx.Done():
			return nil, sctx.Err()
		}
		return b.scanHistoryEvents(sctx, prefix, fromRevision, currentRevision)
	})
	if scanErr != nil {
		return nil, scanErr
	}
	// The shared result already covers exactly this caller's window (read-only;
	// the shared slice must not be mutated).
	return events, nil
}

// scanHistoryEvents does the actual full-prefix storage scan behind
// historyWatchEvents, returning every event under prefix with revision in
// [fromRevision, currentRevision]. It is invoked through historyScanGroup so a
// reconnect herd shares one execution rather than each issuing its own scan.
func (b *backend) scanHistoryEvents(ctx context.Context, prefix string, fromRevision, currentRevision uint64) ([]*proto.Event, error) {
	start := b.coder.EncodeObjectKey([]byte(prefix), 0)
	end := b.coder.EncodeObjectKey(PrefixEnd([]byte(prefix)), 0)
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
		key, rev, err := b.coder.Decode(iter.Key())
		if err != nil {
			return nil, err
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

func (b *backend) processEvents(ctx context.Context, cancel context.CancelFunc, out chan<- []*proto.Event, in <-chan []*proto.Event,
	prefix string, revision uint64) {
	prefixBytes := []byte(prefix)
	klog.InfoS("start process events chan", "prefix", prefix, "revision", revision)

	defer func() {
		klog.InfoS("events chan closed", "chan", in, "prefix", prefix)
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
			if isProgressMarker(events) {
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
