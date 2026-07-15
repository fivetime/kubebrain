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
	"time"

	"k8s.io/klog/v2"
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
	// Retry with a freshly captured baseRev: a compaction can still slip between
	// Reset's snapshot-revision capture and the scan's watermark check.
	var lastErr error
	for attempt := 1; ; attempt++ {
		if lastErr = b.rebuildCountIndexOnce(ctx); lastErr == nil {
			return nil
		}
		if attempt >= 3 || ctx.Err() != nil {
			return lastErr
		}
		klog.ErrorS(lastErr, "rebuild count index failed, retrying with a fresh snapshot revision", "attempt", attempt)
	}
}

func (b *backend) rebuildCountIndexOnce(ctx context.Context) error {
	ts := time.Now()
	// Floor the snapshot revision at the compact watermark+1: a just-promoted
	// leader whose committed revision has not yet caught up could otherwise capture
	// a baseRev at/below the watermark and List at a partially-GC'd revision,
	// yielding an incomplete "live keys" snapshot. safeCurrentRevision used to
	// enforce this; Reset gets a currentRev func that reproduces the floor without
	// its committed-revision write side-effect (which we must not do under the lock).
	compactRev, cerr := b.GetCompactRevision(ctx)
	if cerr != nil {
		klog.ErrorS(cerr, "rebuild count index: read compact revision failed")
		return cerr
	}
	// baseRev is captured inside Reset under its install lock; the load then scans
	// storage at exactly baseRev. Reset loads without holding the index lock for
	// the whole scan, so the collector keeps advancing the committed revision
	// throughout — no freeze, no watch-buffer overflow during a minutes-long
	// rebuild at scale.
	var loadErr error // Reset swallows the load error (it just disables the index); capture it for the retry loop
	b.countIndex.Reset(
		func() uint64 {
			r := b.tso.GetRevision()
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
