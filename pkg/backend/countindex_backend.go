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
	"errors"
	"fmt"
	"io"
	"sort"
	"time"

	"k8s.io/klog/v2"

	proto "github.com/kubewharf/kubebrain-client/api/v2rpc"

	"github.com/kubewharf/kubebrain/pkg/backend/coder"
	"github.com/kubewharf/kubebrain/pkg/storage"
)

// CountAtRevision returns the exact live-key count of [key,end) at revision rev
// from the in-memory index, and whether the index served it. rev==0 means the
// current revision. When the index is disabled/unavailable/not-ready for rev,
// served is false and the caller must fall back to a scan.
func (b *backend) CountAtRevision(ctx context.Context, key, end []byte, rev uint64) (count int64, served bool) {
	if b.countIndex == nil {
		return 0, false
	}
	if rev == 0 {
		cur, err := b.safeCurrentRevision(ctx)
		if err != nil {
			return 0, false
		}
		rev = cur
	}
	if isFromKeyEnd(end) {
		end = nil // "from key" range: count to the end of the keyspace
	}
	// Ready-check and count under one lock: a separate Ready() then Count() lets a
	// concurrent rebuild install a half-loaded tree in between and serve a spurious ~0.
	n, ok := b.countIndex.CountIfReady(key, end, rev)
	if !ok {
		b.metricCli.EmitCounter("count_index.miss", 1)
		return 0, false
	}
	b.metricCli.EmitCounter("count_index.hit", 1)
	return int64(n), true
}

// RebuildCountIndex rebuilds the count index from a snapshot of the live keys at
// the current revision. Call it when the node acquires leadership: the index is
// maintained forward from the ordered event collector, but only the leader's
// collector processes local writes, so a follower-turned-leader must reload the
// pre-existing keys.
//
// The load is one partition-parallel streaming scan (RangeStream), NOT a paged
// List loop. Paging at 10M+ keys takes upwards of an hour (thousands of pages,
// each queueing on the scan-worker semaphore behind count-fallback full scans
// that exist precisely because this index is not ready yet), and every page
// re-checks the compact watermark — so the apiserver's 5-minute compaction
// cadence inevitably overtakes the snapshot revision and kills the rebuild
// (#42, observed at 18.6M keys: died at 8m/2M keys, index disabled until the
// next leadership change). One streaming scan checks the watermark once at
// start and finishes in minutes.
func (b *backend) RebuildCountIndex(ctx context.Context) error {
	if b.countIndex == nil {
		return nil
	}
	b.countIndexSyncMu.Lock()
	defer b.countIndexSyncMu.Unlock()
	return b.rebuildCountIndexLocked(ctx, 0)
}

func (b *backend) rebuildCountIndexLocked(ctx context.Context, requestedRevision uint64) error {
	// Retry with a freshly captured baseRev: a compaction can still slip between
	// Reset's snapshot-revision capture and the scan's watermark check.
	var lastErr error
	for attempt := 1; ; attempt++ {
		if lastErr = b.rebuildCountIndexOnce(ctx, requestedRevision); lastErr == nil {
			return nil
		}
		if requestedRevision != 0 || attempt >= 3 || ctx.Err() != nil {
			return lastErr
		}
		klog.ErrorS(lastErr, "rebuild count index failed, retrying with a fresh snapshot revision", "attempt", attempt)
	}
}

func (b *backend) rebuildCountIndexOnce(ctx context.Context, requestedRevision uint64) error {
	ts := time.Now()
	// Floor the snapshot revision at the compact watermark+1: a just-promoted
	// leader whose committed revision has not yet caught up could otherwise capture
	// a baseRev at/below the watermark and List at a partially-GC'd revision,
	// yielding an incomplete "live keys" snapshot. safeCurrentRevision used to
	// enforce this; Reset gets a currentRev func that reproduces the floor without
	// its committed-revision write side-effect (which we must not do under the lock).
	// Rebuild runs on leadership acquisition and its snapshot boundary is
	// authoritative. This replica may have cached a watermark from before the
	// previous leader compacted; using it makes every retry choose the same
	// already-compacted base revision and leaves the index disabled.
	compactRev, cerr := b.GetCompactRevisionFresh(ctx)
	if cerr != nil {
		klog.ErrorS(cerr, "rebuild count index: read compact revision failed")
		return cerr
	}
	if requestedRevision != 0 && requestedRevision <= compactRev {
		return fmt.Errorf("count index snapshot revision %d is compacted at %d", requestedRevision, compactRev)
	}
	// baseRev is captured inside Reset under its install lock; the load then scans
	// storage at exactly baseRev. Reset loads without holding the index lock for
	// the whole scan, so the collector keeps advancing the committed revision
	// throughout — no freeze, no watch-buffer overflow during a minutes-long
	// rebuild at scale.
	var loadErr error // Reset swallows the load error (it just disables the index); capture it for the retry loop
	b.countIndex.Reset(
		func() uint64 {
			r := requestedRevision
			if r == 0 {
				r = b.tso.GetRevision()
			}
			if compactRev+1 > r {
				r = compactRev + 1
			}
			return r
		},
		func(rev uint64, emit func(key []byte, revision uint64, tombstone bool)) error {
			// One partition-parallel streaming scan of the whole keyspace at rev.
			// Workers emit batches concurrently, so keys arrive out of key order;
			// applyLocked handles out-of-order inserts (same guard that merges the
			// snapshot with concurrent collector applies). The stream ends with a
			// marker response carrying any scan error; a mid-stream error can only
			// appear there, after the producer closed the channel, so returning on
			// it never strands the scan workers. Returning the error disables the
			// index (Reset sets baseRev=0) so a half-loaded tree is never served.
			// RangeStream takes encoded object keys (List does this internally).
			// Scan the ENTIRE object keyspace, mirroring physical GC's borders
			// (#62 rationale): bounding the rebuild by config.Prefix silently
			// skipped every user key that sorted before the configured prefix,
			// and the half-loaded index then served wrong counts as
			// authoritative. Whole-keyspace also matches what the live apply
			// stream feeds the collector, so rebuild and steady-state agree.
			stream := b.scanner.RangeStream(ctx, b.ks.ObjectKeyspaceStart(), b.ks.ObjectKeyspaceEnd(), rev, true)
			for resp := range stream {
				if resp.Err != "" {
					loadErr = errors.New(resp.Err)
					klog.ErrorS(loadErr, "rebuild count index: scan failed")
					return loadErr
				}
				for _, kv := range resp.RangeResponse.Kvs {
					// The scan emits only live keys at rev (tombstones and shadowed
					// versions filtered by the worker).
					emit(kv.Key, kv.Revision, false)
				}
			}
			return nil
		})
	if loadErr != nil {
		return loadErr
	}
	rev := b.countIndex.BaseRev()
	klog.InfoS("count index rebuilt", "rev", rev, "keys", b.countIndex.Len(),
		"ready", b.countIndex.Ready(rev), "latency", time.Since(ts))
	b.metricCli.EmitGauge("count_index.keys", b.countIndex.Len())
	return nil
}

// ensureCountIndexAtRevision makes the ordered index authoritative at revision
// on replicas that do not run the leader's in-memory event collector. A cold
// follower bootstraps once from the bounded storage RangeStream; subsequent
// reads replay only durable event-log metadata. Untrusted/cleaned log windows
// force another bounded rebuild rather than advancing Ready across a hole.
func (b *backend) ensureCountIndexAtRevision(ctx context.Context, revision uint64) (bool, error) {
	if b.countIndex == nil || revision == 0 || b.countIndex.Overflowed() {
		return false, nil
	}
	if b.countIndex.Ready(revision) {
		return true, nil
	}
	b.countIndexSyncMu.Lock()
	defer b.countIndexSyncMu.Unlock()
	if b.countIndex.Overflowed() {
		return false, nil
	}
	if b.countIndex.Ready(revision) {
		return true, nil
	}
	base := b.countIndex.BaseRev()
	if base != 0 && revision < base {
		served, err := b.rebuildHistoricalCountIndexLocked(ctx, revision)
		if err != nil {
			return false, err
		}
		if served {
			b.metricCli.EmitCounter("count_index.lazy_historical_rebuild", 1)
		}
		return served, nil
	}
	if base != 0 && b.countIndex.ReadyRev() < revision {
		served, err := b.syncCountIndexFromEventLog(ctx, b.countIndex.ReadyRev()+1, revision)
		if err != nil {
			return false, err
		}
		if served && b.countIndex.Ready(revision) {
			b.metricCli.EmitCounter("count_index.lazy_replay", 1)
			return true, nil
		}
	}
	if err := b.rebuildCountIndexLocked(ctx, revision); err != nil {
		return false, err
	}
	ready := b.countIndex.Ready(revision)
	if ready {
		b.metricCli.EmitCounter("count_index.lazy_rebuild", 1)
	}
	return ready, nil
}

var errCountIndexEventLogUntrusted = errors.New("count index event-log window is not trustworthy")

// rebuildHistoricalCountIndexLocked extends an already-current index backwards
// to requestedRevision. ResetHistorical captures the discarded tree's ReadyRev
// under the install lock; the loader first scans the exact requested snapshot,
// then replays every durable event through that captured watermark. Collector
// events arriving after installation merge directly into the loading tree.
func (b *backend) rebuildHistoricalCountIndexLocked(ctx context.Context, requestedRevision uint64) (bool, error) {
	compactRev, err := b.GetCompactRevisionFresh(ctx)
	if err != nil {
		return false, err
	}
	if requestedRevision <= compactRev {
		return false, fmt.Errorf("count index snapshot revision %d is compacted at %d", requestedRevision, compactRev)
	}
	// Avoid discarding a useful current tree when cleanup has already made the
	// required forward-replay window unverifiable. Cleanup can still race after
	// this check; the replay's post-read watermark gate catches that case and
	// disables the partially rebuilt tree.
	if start, ok := b.getEventLogStart(ctx); !ok || requestedRevision < start {
		return false, nil
	}
	started := time.Now()
	var loadErr error
	b.countIndex.ResetHistorical(requestedRevision, func(baseRev, catchUpRev uint64, emit func(key []byte, revision uint64, tombstone bool)) error {
		stream := b.scanner.RangeStream(ctx, b.ks.ObjectKeyspaceStart(), b.ks.ObjectKeyspaceEnd(), baseRev, true)
		for response := range stream {
			if response.Err != "" {
				loadErr = errors.New(response.Err)
				return loadErr
			}
			for _, kv := range response.RangeResponse.Kvs {
				emit(kv.Key, kv.Revision, false)
			}
		}
		if catchUpRev > baseRev {
			served, replayErr := b.replayCountIndexEventLog(ctx, baseRev+1, catchUpRev, func(entry eventLogPending) {
				emit(entry.userKey, entry.rev, entry.verb == proto.Event_DELETE)
			})
			if replayErr != nil {
				loadErr = replayErr
				return loadErr
			}
			if !served {
				loadErr = errCountIndexEventLogUntrusted
				return loadErr
			}
		}
		return nil
	})
	if errors.Is(loadErr, errCountIndexEventLogUntrusted) {
		return false, nil
	}
	if loadErr != nil {
		return false, loadErr
	}
	ready := b.countIndex.Ready(requestedRevision)
	if ready {
		klog.InfoS("count index extended to historical revision", "rev", requestedRevision,
			"readyRev", b.countIndex.ReadyRev(), "keys", b.countIndex.Len(), "latency", time.Since(started))
	}
	return ready, nil
}

// syncCountIndexFromEventLog streams complete transaction groups in
// [fromRevision,toRevision]. It retains at most one transaction's metadata;
// values are unnecessary because TreeIndex only needs key/revision/tombstone.
func (b *backend) syncCountIndexFromEventLog(ctx context.Context, fromRevision, toRevision uint64) (bool, error) {
	served, err := b.replayCountIndexEventLog(ctx, fromRevision, toRevision, func(entry eventLogPending) {
		b.countIndex.Apply(entry.userKey, entry.rev, entry.verb == proto.Event_DELETE)
	})
	if err != nil || !served {
		return served, err
	}
	b.countIndex.SetReadyRev(toRevision)
	return true, nil
}

func (b *backend) replayCountIndexEventLog(ctx context.Context, fromRevision, toRevision uint64, apply func(eventLogPending)) (bool, error) {
	if fromRevision > toRevision {
		return true, nil
	}
	start, ok := b.getEventLogStart(ctx)
	if !ok || fromRevision <= start {
		return false, nil
	}
	timestamp, _ := storage.SnapshotTimestampFromContext(ctx)
	iter, err := b.kv.Iter(ctx, b.ks.EventLogRangeStart(fromRevision), b.ks.EventLogRangeEnd(toRevision), timestamp, 0)
	if err != nil {
		return false, err
	}
	defer iter.Close()
	var group []eventLogPending
	flush := func() (bool, error) {
		if len(group) == 0 {
			return true, nil
		}
		if group[0].ordered {
			sort.Slice(group, func(i, j int) bool { return group[i].sub < group[j].sub })
		}
		if err := validateEventLogEntries(group); err != nil {
			return false, nil
		}
		for _, entry := range group {
			apply(entry)
		}
		group = group[:0]
		return true, nil
	}
	for {
		if err := iter.Next(ctx); err != nil {
			if err == io.EOF {
				break
			}
			return false, err
		}
		revision, userKey, decodeErr := b.ks.DecodeEventLogKey(iter.Key())
		if decodeErr != nil {
			return false, nil
		}
		verb, prevRev, sub, total, ordered, valid := coder.DecodeOrderedEventLogValue(iter.Val())
		if !valid {
			return false, nil
		}
		if len(group) != 0 && group[0].rev != revision {
			served, flushErr := flush()
			if flushErr != nil || !served {
				return served, flushErr
			}
		}
		group = append(group, eventLogPending{
			verb: proto.Event_EventType(verb), rev: revision, prevRev: prevRev,
			userKey: append([]byte(nil), userKey...), sub: sub, total: total, ordered: ordered,
		})
	}
	if served, flushErr := flush(); flushErr != nil || !served {
		return served, flushErr
	}
	// cleanupEventLog advances the watermark before deleting entries. Re-read
	// storage after the iterator: if it crossed our window, the scan may have
	// observed only a suffix and the partially applied index must be rebuilt.
	if refreshed, ok := b.refreshEventLogStart(ctx); !ok || fromRevision <= refreshed {
		return false, nil
	}
	return true, nil
}
