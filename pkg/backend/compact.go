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
	"encoding/binary"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"k8s.io/klog/v2"

	proto "github.com/kubewharf/kubebrain-client/api/v2rpc"

	"github.com/kubewharf/kubebrain/pkg/storage"
)

func (b *backend) Compact(ctx context.Context, revision uint64) (*proto.CompactResponse, error) {
	revision = b.clampCompactRevision(revision)

	err := b.compact(ctx, revision)
	if err != nil {
		klog.Errorf("backend compact with revision %d failed %v", revision, err)
	}
	compactResponse := &proto.CompactResponse{
		Header: responseHeader(revision),
	}
	return compactResponse, err
}

// CompactAsync advances the compact watermark synchronously and schedules the
// physical version GC in the background. See the Backend interface for why the
// hot Compact RPC path uses this instead of the fully synchronous Compact.
func (b *backend) CompactAsync(ctx context.Context, revision uint64) (uint64, error) {
	revision = b.clampCompactRevision(revision)

	advanced, err := b.setCompactRecord(ctx, revision)
	if err != nil {
		klog.Errorf("backend compact-async with revision %d failed %v", revision, err)
		return 0, err
	}
	if advanced {
		// watermark moved forward; hand the physical scan to the background worker
		b.schedulePhysicalCompact(revision)
	}
	return revision, nil
}

// clampCompactRevision bounds a requested compact revision to what is safe to
// compact now: never past the current revision, and never at or above the oldest
// in-flight (uncertain) retry so a not-yet-committed op is not compacted away.
func (b *backend) clampCompactRevision(revision uint64) uint64 {
	curRevision := b.tso.GetRevision()
	if revision == 0 || revision > curRevision {
		revision = curRevision
	}
	uncertainRev := b.asyncFifoRetry.MinRevision()
	if uncertainRev != 0 {
		// head of retry queue is the uncertain event with the least revision.
		// set compact revision less than the least uncertain revision if there is uncertain event
		// so that uncertain op will not be compacted.
		revision = minUint64(uncertainRev-1, revision)
	}
	return revision
}

// compactRevCacheTTL bounds how stale a cached compact revision may be. The
// compact revision only advances (~once per compaction cycle) and is refreshed
// eagerly when this node advances it, so a short TTL that turns a per-request
// storage read into ~one read per second (#48) is safe.
const compactRevCacheTTL = time.Second

type compactRevCache struct {
	mu     sync.Mutex
	rev    uint64
	loaded time.Time
}

func (b *backend) GetCompactRevision(ctx context.Context) (uint64, error) {
	b.compactRevCache.mu.Lock()
	if !b.compactRevCache.loaded.IsZero() && time.Since(b.compactRevCache.loaded) < compactRevCacheTTL {
		rev := b.compactRevCache.rev
		b.compactRevCache.mu.Unlock()
		return rev, nil
	}
	b.compactRevCache.mu.Unlock()

	// Refresh outside the lock so a slow compact-key read can't stall every other
	// revisioned request; at TTL expiry a burst issues at most the same reads the
	// uncached path did per request, but only once per window.
	rev, err := b.loadCompactRevision(ctx)
	if err != nil {
		return 0, err
	}
	b.updateCompactRevCache(rev)
	b.compactRevCache.mu.Lock()
	rev = b.compactRevCache.rev
	b.compactRevCache.mu.Unlock()
	return rev, nil
}

// loadCompactRevision reads the persisted compact revision from storage.
func (b *backend) loadCompactRevision(ctx context.Context) (uint64, error) {
	val, err := b.kv.Get(ctx, getCompactKey(b.config.Prefix))
	if err == storage.ErrKeyNotFound {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}
	if len(val) == 0 {
		return 0, nil
	}
	return binary.BigEndian.Uint64(val), nil
}

// updateCompactRevCache advances the cached compact revision (never backwards)
// and refreshes its freshness window.
func (b *backend) updateCompactRevCache(revision uint64) {
	b.compactRevCache.mu.Lock()
	if revision > b.compactRevCache.rev {
		b.compactRevCache.rev = revision
	}
	b.compactRevCache.loaded = time.Now()
	b.compactRevCache.mu.Unlock()
}

func (b *backend) safeCurrentRevision(ctx context.Context) (uint64, error) {
	currentRevision := b.tso.GetRevision()
	compactRevision, err := b.GetCompactRevision(ctx)
	if err != nil {
		return 0, err
	}
	if compactRevision >= currentRevision {
		currentRevision = compactRevision + 1
		b.SetCurrentRevision(currentRevision)
	}
	return currentRevision, nil
}

func (b *backend) compact(ctx context.Context, revision uint64) error {
	advanced, err := b.setCompactRecord(ctx, revision)
	if err != nil {
		return err
	}
	if !advanced {
		// revision <= the already-stored compact revision: this range has already
		// been compacted at an equal-or-higher watermark. Skip the physical scan;
		// proceeding would re-scan and (via the scanner's compact-key write)
		// regress the monotonic compact watermark to a smaller revision, letting
		// later range requests at already-compacted revisions read incomplete data.
		klog.InfoS("compact skipped, revision already compacted", "revision", revision)
		return nil
	}

	b.physicalCompact(ctx, revision)
	return nil
}

// physicalCompact runs the storage version-GC scan for revision. It is shared by
// the synchronous Compact path and the background worker, and holds compactScanMu
// so the two never scan concurrently (which would double-scan and contend on the
// storage engine).
func (b *backend) physicalCompact(ctx context.Context, revision uint64) {
	b.compactScanMu.Lock()
	defer b.compactScanMu.Unlock()

	borders := b.getCompactBorders()
	// Pass all borders together so the scanner computes the events-TTL timeout
	// revision once per cycle; see Scanner.Compact for why per-border draining of
	// the shared compact-history queue would silently disable event expiry.
	if err := b.scanner.Compact(ctx, borders, revision); err != nil {
		// The logical compact watermark was already persisted, so the compaction
		// is (correctly) reported as succeeded; but the physical GC scan failed,
		// leaving garbage that a later higher-revision compaction will reclaim.
		// Surface it (log + metric) rather than swallowing it, so a persistently
		// failing GC is observable instead of silently accumulating garbage (#71).
		b.metricCli.EmitCounter("backend.compact.scan.err", 1)
		klog.ErrorS(err, "physical compaction scan failed; garbage will be reclaimed on a later compaction", "revision", revision)
	}
	if b.countIndex != nil {
		b.countIndex.Compact(revision)
	}
}

// schedulePhysicalCompact raises the background compaction target to revision and
// wakes the worker. Concurrent callers coalesce onto the highest revision.
func (b *backend) schedulePhysicalCompact(revision uint64) {
	for {
		cur := atomic.LoadUint64(&b.compactTriggerRev)
		if revision <= cur {
			break
		}
		if atomic.CompareAndSwapUint64(&b.compactTriggerRev, cur, revision) {
			break
		}
	}
	select {
	case b.compactSignal <- struct{}{}:
	default: // a wake-up is already pending; the worker will read the latest target
	}
}

// runCompactor drains background physical-compaction requests, one scan at a
// time, always compacting up to the latest requested revision (coalescing any
// requests that arrived while a scan was running).
func (b *backend) runCompactor() {
	ctx := context.Background()
	var lastScanned uint64
	for range b.compactSignal {
		for {
			target := atomic.LoadUint64(&b.compactTriggerRev)
			if target <= lastScanned {
				break
			}
			b.physicalCompact(ctx, target)
			lastScanned = target
			atomic.StoreUint64(&b.compactDoneRev, target)
		}
	}
}

// setCompactRecord raises the persisted compact revision to revision. It
// returns advanced=false (without error) when revision is not greater than the
// already-stored compact revision, so the caller can skip a redundant compaction
// that would otherwise regress the watermark.
func (b *backend) setCompactRecord(ctx context.Context, revision uint64) (advanced bool, err error) {
	// get stored compact revision
	val, err := b.kv.Get(ctx, getCompactKey(b.config.Prefix))
	if err != nil && err != storage.ErrKeyNotFound {
		klog.ErrorS(err, "get compact revision failed")
		return false, err
	}
	// stored compact revision is not nil
	if len(val) > 0 {
		// compare stored compact revision with current compact revision
		compactRevision := binary.BigEndian.Uint64(val)
		if compactRevision >= revision {
			klog.InfoS("compact revision not greater than stored, skip", "compactRev", compactRevision, "currentRev", revision)
			// revision has already been compacted; must not lower the watermark
			return false, nil
		}
	}
	revisionBytes := make([]byte, 8)
	binary.BigEndian.PutUint64(revisionBytes, revision)
	batch := b.kv.BeginBatchWrite()
	if len(val) > 0 {
		// if compact revision already set before
		batch.CAS(getCompactKey(b.config.Prefix), revisionBytes, val, 0)
	} else {
		// compact revision firstly set
		batch.PutIfNotExist(getCompactKey(b.config.Prefix), revisionBytes, 0)
	}
	err = batch.Commit(ctx)
	b.metricCli.EmitCounter("backend.set_compact_revision", 1)
	if err != nil {
		klog.ErrorS(err, "set compact key failed", "revision", revision)
		b.metricCli.EmitCounter("backend.set_compact_revision.err", 1)
		return false, err
	}
	// This node just advanced the watermark; make the cache reflect it eagerly so
	// reads reject the newly-compacted range immediately instead of after the TTL.
	b.updateCompactRevCache(revision)
	return true, nil
}

func (b *backend) getCompactBorders() [][]byte {
	// exclude skipped key prefix
	var keyPrefixes []string
	keyPrefixes = append(keyPrefixes, b.config.Prefix)
	keyPrefixes = append(keyPrefixes, b.config.SkippedPrefixes...)

	// construct compact borders
	var compactBorders [][]byte
	for _, key := range keyPrefixes {
		if !strings.HasSuffix(key, "/") {
			key = key + "/"
		}
		compactBorders = append(compactBorders, b.coder.EncodeObjectKey([]byte(key), 0))
		compactBorders = append(compactBorders, b.coder.EncodeObjectKey(PrefixEnd([]byte(key)), 0))
	}
	// KubeBrain's internal keyspaces (etcd metadata, lease records) live under the
	// reserved \x00kubebrain/ namespace, which sorts before the user prefix
	// (leading \x00) and was therefore never GC'd — lease records in particular
	// are rewritten on every keepalive, so their versions grew without bound
	// (#6/#15/#38). They are all latest-only state, so fold the whole namespace
	// into compaction: the scanner keeps the latest version <= compactRev per key
	// and retires older versions and tombstones (revoked/expired leases), exactly
	// as for user keys.
	compactBorders = append(compactBorders,
		b.coder.EncodeObjectKey(internalKeyspacePrefix, 0),
		b.coder.EncodeObjectKey(PrefixEnd(internalKeyspacePrefix), 0))
	// sort to make sure compact in right range
	sort.Slice(compactBorders, func(i, j int) bool {
		return bytes.Compare(compactBorders[i], compactBorders[j]) < 0
	})
	return compactBorders
}
