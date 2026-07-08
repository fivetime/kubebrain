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
	"encoding/binary"
	"io"
	"sync"
	"time"

	"k8s.io/klog/v2"

	proto "github.com/kubewharf/kubebrain-client/api/v2rpc"
	"github.com/kubewharf/kubebrain/pkg/backend/coder"
	"github.com/kubewharf/kubebrain/pkg/storage"
)

// Event-log write side (#45). Each committed write appends one 9-byte entry
// per key IN THE SAME storage transaction: [1B verb][8B prevRevision], keyed
// by coder.EncodeEventLogKey(revision, userKey). Values are reconstructed at
// replay time from the object keys the entry points to, so the log stays tiny
// (~70B/entry) instead of doubling the write volume.

const (
	// eventLogReplayConcurrency bounds the parallel object-key point reads that
	// rebuild event values during a replay.
	eventLogReplayConcurrency = 16
	// eventLogCleanupBatch bounds one cleanup transaction.
	eventLogCleanupBatch = 512
)

// appendEventLog stages this write's event-log entry onto its own batch.
func appendEventLog(batch storage.BatchWrite, revision uint64, userKey []byte, verb proto.Event_EventType, prevRev uint64) {
	batch.Put(coder.EncodeEventLogKey(revision, userKey), coder.EncodeEventLogValue(byte(verb), prevRev), 0)
}

// eventLogStart caches the log's completeness watermark: entries are complete
// for revisions STRICTLY ABOVE the stored value. It only ever moves forward.
type eventLogStart struct {
	mu  sync.Mutex
	rev uint64
	ok  bool
}

// EnsureEventLogStart advances the event-log completeness watermark to the
// current revision on leadership acquisition. Unconditional on purpose: this
// leader can only vouch for entries IT writes — the previous leader may have
// been an older binary that wrote no log at all, and a hole in the log would
// silently drop events from replays. The cost is that the window between
// leadership changes replays from the object scan (as it always did); the log
// re-accumulates from here.
func (b *backend) EnsureEventLogStart(ctx context.Context) error {
	cur, err := b.safeCurrentRevision(ctx)
	if err != nil {
		return err
	}
	batch := b.kv.BeginBatchWrite()
	batch.Put(coder.ElogMetaStartKey, uint64ToBytes(cur), 0)
	if err := batch.Commit(ctx); err != nil {
		return err
	}
	b.advanceEventLogStartCache(cur)
	klog.InfoS("event log start advanced on leadership acquisition", "rev", cur)
	return nil
}

// getEventLogStart returns the completeness watermark, caching it after the
// first read (it only advances during cleanup, which updates the cache).
func (b *backend) getEventLogStart(ctx context.Context) (uint64, bool) {
	b.elogStart.mu.Lock()
	if b.elogStart.ok {
		rev := b.elogStart.rev
		b.elogStart.mu.Unlock()
		return rev, true
	}
	b.elogStart.mu.Unlock()

	val, err := b.kv.Get(ctx, coder.ElogMetaStartKey)
	if err != nil || len(val) < 8 {
		return 0, false
	}
	rev := binary.BigEndian.Uint64(val)
	b.elogStart.mu.Lock()
	if !b.elogStart.ok || rev > b.elogStart.rev {
		b.elogStart.rev, b.elogStart.ok = rev, true
	}
	rev = b.elogStart.rev
	b.elogStart.mu.Unlock()
	return rev, true
}

func (b *backend) advanceEventLogStartCache(rev uint64) {
	b.elogStart.mu.Lock()
	if rev > b.elogStart.rev {
		b.elogStart.rev, b.elogStart.ok = rev, true
	}
	b.elogStart.mu.Unlock()
}

// eventLogWatchEvents replays [fromRevision, toRevision] for prefix from the
// event log: one bounded range read over the log entries plus parallel point
// reads of the object keys they reference. served=false means the log cannot
// answer (window predates the completeness watermark, disabled, or a referenced
// object version is unexpectedly missing) and the caller must fall back to the
// full-prefix object scan.
func (b *backend) eventLogWatchEvents(ctx context.Context, prefix string, fromRevision, toRevision uint64) (events []*proto.Event, served bool, err error) {
	start, ok := b.getEventLogStart(ctx)
	if !ok || fromRevision <= start {
		return nil, false, nil
	}
	ts := time.Now()
	iter, err := b.kv.Iter(ctx, coder.EventLogRangeStart(fromRevision), coder.EventLogRangeEnd(toRevision), 0, 0)
	if err != nil {
		return nil, false, err
	}
	defer func() { _ = iter.Close() }()

	type pending struct {
		verb    proto.Event_EventType
		rev     uint64
		prevRev uint64
		userKey []byte
	}
	var entries []pending
	prefixBytes := []byte(prefix)
	for {
		if err := iter.Next(ctx); err != nil {
			if err == io.EOF {
				break
			}
			return nil, false, err
		}
		rev, userKey, derr := coder.DecodeEventLogKey(iter.Key())
		if derr != nil {
			continue
		}
		if !hasPrefixBytes(userKey, prefixBytes) {
			continue
		}
		verbByte, prevRev, vok := coder.DecodeEventLogValue(iter.Val())
		if !vok {
			continue
		}
		entries = append(entries, pending{verb: proto.Event_EventType(verbByte), rev: rev, prevRev: prevRev,
			userKey: append([]byte(nil), userKey...)})
	}

	events = make([]*proto.Event, len(entries))
	sem := make(chan struct{}, eventLogReplayConcurrency)
	var wg sync.WaitGroup
	var loadErrMu sync.Mutex
	var loadErr error
	var incomplete bool
	for i := range entries {
		wg.Add(1)
		sem <- struct{}{}
		go func(i int) {
			defer wg.Done()
			defer func() { <-sem }()
			e := entries[i]
			// The value revision the event carries: PUT/CREATE carry their own
			// version; DELETE carries the deleted (previous) version, which
			// compaction retains as the key's newest version at/below the
			// watermark, so it is always readable for a post-watermark window.
			valueRev := e.rev
			if e.verb == proto.Event_DELETE {
				valueRev = e.prevRev
			}
			val, gerr := b.kv.Get(ctx, b.coder.EncodeObjectKey(e.userKey, valueRev))
			if gerr != nil {
				loadErrMu.Lock()
				if gerr == storage.ErrKeyNotFound {
					incomplete = true // unexpectedly GC'd: replay cannot be trusted
				} else if loadErr == nil {
					loadErr = gerr
				}
				loadErrMu.Unlock()
				return
			}
			events[i] = &proto.Event{
				Type:     e.verb,
				Revision: e.rev,
				Kv:       &proto.KeyValue{Key: e.userKey, Value: val, Revision: valueRev},
			}
		}(i)
	}
	wg.Wait()
	if loadErr != nil {
		return nil, false, loadErr
	}
	if incomplete {
		b.metricCli.EmitCounter("watch.event_log.incomplete", 1)
		return nil, false, nil
	}
	b.metricCli.EmitCounter("watch.event_log.replay", 1)
	b.metricCli.EmitHistogram("watch.event_log.replay.events", len(events))
	klog.V(2).InfoS("watch history served from event log", "prefix", prefix,
		"from", fromRevision, "to", toRevision, "events", len(events), "latency", time.Since(ts))
	return events, true, nil
}

// cleanupEventLog removes log entries at or below revision and advances the
// completeness watermark, keeping the log bounded by the compaction horizon.
// Runs on the compactor goroutine after the physical scan.
func (b *backend) cleanupEventLog(ctx context.Context, revision uint64) {
	start, ok := b.getEventLogStart(ctx)
	if ok && start >= revision {
		return
	}
	// Advance the watermark FIRST: a replay racing this cleanup must already
	// consider the window incomplete rather than read a half-deleted log.
	batch := b.kv.BeginBatchWrite()
	batch.Put(coder.ElogMetaStartKey, uint64ToBytes(revision), 0)
	if err := batch.Commit(ctx); err != nil {
		klog.ErrorS(err, "event log watermark advance failed", "revision", revision)
		return
	}
	b.advanceEventLogStartCache(revision)

	deleted := 0
	for {
		// Collect the batch's keys FIRST and only then open the write batch: a
		// storage's BatchWrite may hold engine resources (memkv holds its global
		// lock) from Begin to Commit, so it must never be opened while an iter
		// is still in progress nor abandoned without a Commit.
		iter, err := b.kv.Iter(ctx, coder.EventLogRangeStart(0), coder.EventLogRangeEnd(revision), 0, uint64(eventLogCleanupBatch))
		if err != nil {
			klog.ErrorS(err, "event log cleanup iter failed", "revision", revision)
			return
		}
		var keys [][]byte
		for {
			if err := iter.Next(ctx); err != nil {
				break
			}
			keys = append(keys, append([]byte(nil), iter.Key()...))
		}
		_ = iter.Close()
		if len(keys) == 0 {
			break
		}
		del := b.kv.BeginBatchWrite()
		for _, k := range keys {
			del.Del(k)
		}
		if err := del.Commit(ctx); err != nil {
			klog.ErrorS(err, "event log cleanup batch failed", "revision", revision)
			return
		}
		deleted += len(keys)
	}
	if deleted > 0 {
		b.metricCli.EmitCounter("watch.event_log.cleaned", deleted)
		klog.V(2).InfoS("event log cleaned", "upTo", revision, "entries", deleted)
	}
}

func hasPrefixBytes(s, prefix []byte) bool {
	if len(s) < len(prefix) {
		return false
	}
	for i := range prefix {
		if s[i] != prefix[i] {
			return false
		}
	}
	return true
}

