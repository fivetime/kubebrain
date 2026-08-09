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
	"context"
	"sync"

	"k8s.io/klog/v2"

	proto "github.com/kubewharf/kubebrain-client/api/v2rpc"

	"github.com/kubewharf/kubebrain/pkg/backend/common"
)

// This file holds the watch-event RING: the per-revision slot buffer and the
// PRODUCER side (notify / notifyBatch fill a slot; handleWatchEventOverflow
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

// notify publishes a single watch event into the ring. ctx is retained for
// caller symmetry (unused). It is a one-element notifyBatch: both share the
// same stale-drop / overflow / append machinery so the ring's ordering
// invariants have a single implementation.
func (b *backend) notify(ctx context.Context,
	key []byte, val []byte, revision, preRevision uint64, valid bool, eventType proto.Event_EventType, err error) {
	b.notifyBatch([]*common.WatchEvent{{
		Revision:     revision,
		PrevRevision: preRevision,
		Valid:        valid,
		ResourceVerb: eventType,
		Key:          key,
		Value:        val,
		Err:          err,
	}})
}

func (b *backend) notifyBatch(events []*common.WatchEvent) {
	if len(events) == 0 {
		return
	}
	revision := events[0].Revision
	if revision == 0 {
		// No revision was consumed (e.g. update's pre-deal metadata Get failed,
		// #44): nothing to fill in the ring, drop. Not an anomalous ring fill but
		// an expected zero-consumption failure (review #51).
		b.metricCli.EmitCounter("watch.event.zero_revision.dropped", 1)
		return
	}
	if abortedRevision(events) {
		// A dealt user revision with no committed event is externally observable as
		// a gap from etcd's contiguous committed-write sequence. Count the batch
		// once (not once per key) so operators can quantify this known compatibility
		// gap under real contention and storage failures.
		b.metricCli.EmitCounter("revision.generator.aborted", 1)
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

	// Jump to the highest dealt revision, not the triggering revision: every
	// in-flight event carries a revision <= Dealt(), so after wiping all slots
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
