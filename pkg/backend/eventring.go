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
	"sync"

	"k8s.io/klog/v2"

	"github.com/kubewharf/kubebrain/pkg/backend/common"
)

// This file holds the watch-event RING: the per-revision slot buffer and the
// PRODUCER side (notifyBatch fills a whole transaction slot;
// handleWatchEventOverflow
// resets the ring when the collector falls too far behind). The CONSUMER side
// — collectStorageWriteEvents, which drains slots in revision order — lives in
// backend.go next to the backend run loop.

type watchEventSlot struct {
	sync.Mutex
	events []*common.WatchEvent
}

func newWatchEventSlots(capacity int) []*watchEventSlot {
	slots := make([]*watchEventSlot, capacity)
	for i := range slots {
		slots[i] = &watchEventSlot{}
	}
	return slots
}

func (s *watchEventSlot) appendAll(events []*common.WatchEvent) {
	s.Lock()
	defer s.Unlock()
	s.events = append(s.events, events...)
}

func (s *watchEventSlot) take(revision uint64) []*common.WatchEvent {
	s.Lock()
	defer s.Unlock()
	if len(s.events) == 0 || s.events[0].Revision != revision {
		return nil
	}
	events := s.events
	s.events = nil
	return events
}

func (s *watchEventSlot) reset() {
	s.Lock()
	defer s.Unlock()
	s.events = nil
}

// notifyBatch is the only event-ring producer. All events from one storage
// transaction must be supplied together at their shared revision.
func (b *backend) notifyBatch(events []*common.WatchEvent) {
	if len(events) == 0 {
		return
	}
	revision := events[0].Revision
	if revision == 0 {
		// The transaction failed before its durable allocator callback ran, so no
		// revision exists and there is no ring slot to fill.
		b.metricCli.EmitCounter("watch.event.zero_revision.dropped", 1)
		return
	}
	b.notifyMu.RLock()
	cur := b.collectorRevision.Load()
	switch {
	case revision <= cur:
		// Stale: the pipeline already advanced past this revision (e.g. after an
		// overflow reset jumped the current revision forward). Drop it — appending
		// would leave a poison slot the collector can never consume in order, and
		// computing the gap below would underflow (uint64) and wrongly re-trigger
		// overflow.
		b.notifyMu.RUnlock()
		b.metricCli.EmitCounter("watch.event.buffer.stale_drop", 1)
		return
	case revision-cur >= watchersChanCapacity:
		// buffer full: the collector is too far behind for the ring to bridge.
		b.notifyMu.RUnlock()
		b.handleWatchEventOverflow(revision)
		return
	}
	b.watchEventsRingBuffer[int64(revision)%watchersChanCapacity].appendAll(events)
	b.metricCli.EmitGauge("watch.revision.lag", revision-cur)
	b.notifyMu.RUnlock()
	b.signalWrite()
}

func abortedRevision(events []*common.WatchEvent) bool {
	if len(events) == 0 {
		return false
	}
	for _, event := range events {
		if event.Valid {
			return false
		}
	}
	return true
}

func (b *backend) observeCollectedRevision(events []*common.WatchEvent) {
	if abortedRevision(events) {
		// The collector takes each revision slot once, after every producer notify
		// for that revision has coalesced. Count here rather than in notifyBatch so
		// duplicate producer notifications cannot overcount one public gap.
		b.metricCli.EmitCounter("revision.generator.aborted", 1)
	}
}

func (b *backend) handleWatchEventOverflow(revision uint64) {
	// Exclusive against appends so the wipe + revision jump + watcher close is
	// atomic: no writer can append into a slot mid-reset (which would either be
	// wiped and strand the collector, or survive as a poison slot).
	b.notifyMu.Lock()
	defer b.notifyMu.Unlock()

	currentRevision := b.collectorRevision.Load()
	// Recheck under the lock — a concurrent overflow may have already reset.
	if revision <= currentRevision || revision-currentRevision < watchersChanCapacity {
		return
	}
	b.metricCli.EmitCounter("watch.event.buffer.full", 1)

	// Jump to the highest locally observed revision, not the triggering revision:
	// every in-flight event carries a revision <= Dealt(), so after wiping all slots
	// nothing appended-but-needed is lost, and the collector recovers
	// contiguously from the next write (Dealt()+1). Appends are blocked here, so
	// no revision above the target can be sitting in a slot.
	target := b.tso.Dealt()
	if target < revision {
		target = revision
	}
	klog.ErrorS(nil, "watch event buffer full, resetting watch state", "currentRevision", currentRevision, "revision", revision, "target", target, "capacity", watchersChanCapacity)
	for i := range b.watchEventsRingBuffer {
		b.watchEventsRingBuffer[i].reset()
	}
	b.watchCache.Reset()
	b.collectorRevision.Store(target)
	b.SetCurrentRevision(target)
	// Order matters: close the existing subscribers FIRST, then jump the published
	// watermark. target = Dealt() covers revisions whose events were just wiped and
	// never fanned out, so publishedRev==target is ABOVE the true delivered frontier
	// of any existing sub. If we raised it before CloseAll, a concurrent
	// broadcastProgress (holds only the hub RLock, not notifyMu) could enqueue a
	// marker@target into a still-open healthy sub, folding its syncedRev to target
	// and advertising a revision whose wiped events it never received -> silent
	// watch gap on re-watch. After CloseAll removed those subs, a later
	// broadcastProgress finds none; any new post-reset sub only wants events > target
	// (the collector resumes at target+1), so marker@target is a safe under-report.
	b.watcherHub.CloseAll()
	b.watcherHub.AdvancePublishedRevision(target)
	b.signalWrite()
}
