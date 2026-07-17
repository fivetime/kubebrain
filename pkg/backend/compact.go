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
	"math/rand"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"k8s.io/klog/v2"

	proto "github.com/kubewharf/kubebrain-client/api/v2rpc"

	"github.com/kubewharf/kubebrain/pkg/storage"
)

type compactContextHolder struct {
	ctx context.Context
}

func (b *backend) Compact(ctx context.Context, revision uint64) (*proto.CompactResponse, error) {
	revision = b.clampCompactRevision(revision)

	var err error
	ctx, err = b.withCurrentLeadershipEpoch(ctx)
	if err != nil {
		return &proto.CompactResponse{Header: responseHeader(revision)}, err
	}
	ctx, cancel := b.withCompactionLeadership(ctx)
	defer cancel()
	err = b.compact(ctx, revision)
	if err != nil {
		klog.Errorf("backend compact with revision %d failed %v", revision, err)
	}
	compactResponse := &proto.CompactResponse{
		Header: responseHeader(revision),
	}
	return compactResponse, err
}

func (b *backend) withCompactionLeadership(ctx context.Context) (context.Context, context.CancelFunc) {
	merged, cancel := context.WithCancel(ctx)
	holder, _ := b.compactCtx.Load().(compactContextHolder)
	if holder.ctx == nil {
		return merged, cancel
	}
	stop := context.AfterFunc(holder.ctx, cancel)
	return merged, func() {
		stop()
		cancel()
	}
}

// CompactAsync advances the compact watermark synchronously and schedules the
// physical version GC in the background. See the Backend interface for why the
// hot Compact RPC path uses this instead of the fully synchronous Compact.
func (b *backend) CompactAsync(ctx context.Context, revision uint64) (uint64, error) {
	revision = b.clampCompactRevision(revision)

	var err error
	ctx, err = b.withCurrentLeadershipEpoch(ctx)
	if err != nil {
		return 0, err
	}
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
	if revision > curRevision {
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

func (b *backend) HasCompactRevision(ctx context.Context) (bool, error) {
	val, err := b.kv.Get(ctx, getCompactKey(b.config.Prefix))
	if err == storage.ErrKeyNotFound {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return len(val) >= 8, nil
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

// GetCompactRevisionFresh reads the compact revision from storage, bypassing
// the TTL cache (and refreshing it as a side effect). Reserve it for cold
// paths that are about to hand the value to a client as authoritative — e.g.
// a compacted-watch cancel, whose CompactRevision guides where the client
// re-lists: on a follower the cached value can lag a just-proxied Compact by
// up to the TTL, and a stale-low value sends the client into another
// compacted round-trip (#33). Never call it per-request on hot paths.
func (b *backend) GetCompactRevisionFresh(ctx context.Context) (uint64, error) {
	rev, err := b.loadCompactRevision(ctx)
	if err != nil {
		return 0, err
	}
	b.updateCompactRevCache(rev)
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

	if err := b.physicalCompact(ctx, revision); err != nil {
		// The logical watermark is already durable. A Physical=true caller may
		// disconnect or hit its deadline while the scan is running; abandoning
		// the target here would leave history physically stranded until a later
		// compaction or process restart. Continue through the same background
		// worker used by logical compaction, whose context is independent of the
		// client and retries transient storage failures.
		b.schedulePhysicalCompact(revision)
		return err
	}
	return nil
}

// physicalCompact runs the storage version-GC scan for revision. It is shared by
// the synchronous Compact path and the background worker, and holds compactScanMu
// so the two never scan concurrently (which would double-scan and contend on the
// storage engine).
func (b *backend) physicalCompact(ctx context.Context, revision uint64) error {
	b.compactScanMu.Lock()
	defer b.compactScanMu.Unlock()

	// A round over a large tombstone backlog can run for an hour+ (17M keys ≈
	// 75min field-measured); its counters (scanner batches, elog cleanup) are
	// spread across phases, so without an explicit in-flight signal a long
	// round is indistinguishable from a stuck worker (#77 — a healthy round
	// was nearly mistaken for a deadlock and the leader restarted). Emit an
	// in-flight gauge plus a once-a-minute heartbeat for the whole round.
	started := time.Now()
	b.metricCli.EmitGauge("compact.scan.inflight", 1)
	klog.V(2).InfoS("physical compaction started", "revision", revision)
	heartbeatDone := make(chan struct{})
	go func() {
		ticker := time.NewTicker(time.Minute)
		defer ticker.Stop()
		for {
			select {
			case <-heartbeatDone:
				return
			case <-ticker.C:
				klog.InfoS("physical compaction in progress", "revision", revision,
					"elapsed", time.Since(started).Round(time.Second))
			}
		}
	}()
	var compactErr error
	defer func() {
		close(heartbeatDone)
		b.metricCli.EmitGauge("compact.scan.inflight", 0)
		if compactErr != nil {
			klog.ErrorS(compactErr, "physical compaction failed", "revision", revision,
				"elapsed", time.Since(started).Round(time.Second))
		} else {
			klog.InfoS("physical compaction finished", "revision", revision,
				"elapsed", time.Since(started).Round(time.Second))
		}
	}()

	if !b.tryIncrementalCompact(ctx, revision) {
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
			// Also invalidate the incremental baseline: a partial full scan may have
			// left old garbage on keys untouched since the previous baseline, which
			// no incremental pass would ever revisit.
			atomic.StoreUint64(&b.physicalBaseRev, 0)
			b.metricCli.EmitCounter("backend.compact.scan.err", 1)
			klog.ErrorS(err, "physical compaction scan failed; garbage will be reclaimed on a later compaction", "revision", revision)
			compactErr = err
		} else {
			atomic.StoreUint64(&b.physicalBaseRev, revision)
			b.incrementalStreak = 0
		}
	}
	if b.countIndex != nil {
		b.countIndex.Compact(revision)
	}
	// The event log only needs to cover what compaction has not reclaimed:
	// watches below the compact watermark are cancelled as compacted anyway, so
	// entries at/below it are dead weight — drop them and advance the log's
	// completeness watermark (#45).
	b.cleanupEventLog(ctx, revision)
	return compactErr
}

const (
	// incrementalCompactMaxKeys caps the touched-key working set of an
	// incremental GC pass. Beyond it a full keyspace scan is both cheaper
	// (sequential partition-parallel scan vs. one iterator open per key) and
	// safer, so e.g. a StorageVersionMigration rewrite storm naturally falls
	// back to today's behavior.
	incrementalCompactMaxKeys = 100_000
	// incrementalCompactMaxStreak forces a periodic full-keyspace scan after
	// this many consecutive incremental passes — a belt-and-braces bound on any
	// garbage an incremental pass could conceivably miss. At the apiserver's
	// 5-minute compaction cadence this is a full scan every ~4 hours.
	incrementalCompactMaxStreak = 48
)

// tryIncrementalCompact attempts the incremental physical-GC pass: instead of
// scanning the whole object keyspace (O(all keys) every 5 minutes at 10M+ keys),
// scan only the keys the event log records as written in (baseline, revision] —
// the only keys that can have gained garbage since the last completed pass.
// Returns true when the pass ran and the baseline advanced; false means the
// caller must run the full scan (no baseline yet, event-log window insufficient,
// working set too large, periodic safety-net full scan due, or the pass failed).
func (b *backend) tryIncrementalCompact(ctx context.Context, revision uint64) bool {
	base := atomic.LoadUint64(&b.physicalBaseRev)
	if base == 0 || revision <= base {
		return false
	}
	if b.incrementalStreak >= incrementalCompactMaxStreak {
		return false
	}
	// The event log must cover (base, revision] completely: entries at or below
	// its start watermark have been cleaned up, so a window starting past base+1
	// could hide writes (and their garbage) from the working set.
	elogStart, ok := b.getEventLogStart(ctx)
	if !ok || elogStart > base+1 {
		b.metricCli.EmitCounter("backend.compact.incremental.fallback", 1)
		return false
	}
	keys, ok := b.eventLogTouchedKeys(ctx, base, revision, incrementalCompactMaxKeys)
	if !ok {
		b.metricCli.EmitCounter("backend.compact.incremental.fallback", 1)
		return false
	}
	// Respect the --skip-key-prefix carve-outs the full scan's borders encode:
	// co-tenant object types on a shared storage cluster must not be GC'd here
	// either. (Writes to skipped prefixes should not appear in OUR event log,
	// but filtering is cheap insurance against shared-keyspace bleed.)
	keys = b.filterSkippedUserKeys(keys)
	if len(keys) > 0 {
		if err := b.scanner.CompactKeys(ctx, keys, revision); err != nil {
			b.metricCli.EmitCounter("backend.compact.incremental.err", 1)
			klog.ErrorS(err, "incremental compaction failed; falling back to full scan", "revision", revision, "keys", len(keys))
			return false
		}
	}
	atomic.StoreUint64(&b.physicalBaseRev, revision)
	b.incrementalStreak++
	b.metricCli.EmitCounter("backend.compact.incremental", 1)
	b.metricCli.EmitGauge("backend.compact.incremental.keys", float64(len(keys)))
	klog.V(2).InfoS("incremental compaction done", "revision", revision, "keys", len(keys), "streak", b.incrementalStreak)
	return true
}

// filterSkippedUserKeys drops keys under any configured --skip-key-prefix.
func (b *backend) filterSkippedUserKeys(keys [][]byte) [][]byte {
	if len(b.config.SkippedPrefixes) == 0 {
		return keys
	}
	out := keys[:0]
	for _, k := range keys {
		skipped := false
		for _, p := range b.config.SkippedPrefixes {
			prefix := p
			if !strings.HasSuffix(prefix, "/") {
				prefix += "/"
			}
			if bytes.HasPrefix(k, []byte(prefix)) {
				skipped = true
				break
			}
		}
		if !skipped {
			out = append(out, k)
		}
	}
	return out
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

func (b *backend) ResumePhysicalCompaction(ctx context.Context) error {
	// Leadership callbacks pass a context canceled when this node loses the
	// term. Every later background scan uses this lifecycle instead of an
	// immortal context, preventing old and new leaders from scanning the shared
	// TiKV keyspace concurrently.
	b.compactCtx.Store(compactContextHolder{ctx: ctx})
	revision, err := b.GetCompactRevisionFresh(ctx)
	if err != nil {
		return err
	}
	if revision != 0 {
		b.schedulePhysicalCompact(revision)
	}
	return nil
}

const physicalCompactRetryInterval = time.Second

// runCompactor drains background physical-compaction requests, one scan at a
// time, always compacting up to the latest requested revision (coalescing any
// requests that arrived while a scan was running).
func (b *backend) runCompactor() {
	var lastScanned uint64
	for range b.compactSignal {
		for {
			target := atomic.LoadUint64(&b.compactTriggerRev)
			if target <= lastScanned {
				break
			}
			ctx := context.Background()
			if holder, ok := b.compactCtx.Load().(compactContextHolder); ok && holder.ctx != nil {
				ctx = holder.ctx
			}
			if err := b.physicalCompact(ctx, target); err != nil {
				// Do not claim completion. Retry independently of a newer logical
				// compact request so transient storage failures cannot leave physical
				// history stranded forever.
				time.AfterFunc(physicalCompactRetryInterval, func() {
					b.schedulePhysicalCompact(target)
				})
				break
			}
			lastScanned = target
			atomic.StoreUint64(&b.compactDoneRev, target)
			// Export the physical-GC watermark so operators can see whether GC is
			// keeping up with the logical compact watermark. A gap that only grows
			// (physical rev frozen while the logical one advances) is the signature
			// of the #62 keyspace-mismatch bug and of a stuck/backlogged GC scan.
			b.metricCli.EmitGauge("compact.physical.done_rev", target)
		}
	}
}

const (
	// autoCompactBaseInterval is how often the safety-net auto-compactor evaluates
	// whether history has grown past the configured retention; autoCompactJitter is
	// added randomly so multiple nodes / a fleet don't all compact in lock-step.
	autoCompactBaseInterval = 5 * time.Minute
	autoCompactJitter       = 1 * time.Minute
)

// runAutoCompactor is an OPT-IN (config.AutoCompactionRetention > 0) safety net:
// KubeBrain never compacts on its own — the apiserver drives compaction via the
// compact_rev_key CAS. If that loop breaks/misconfigures, MVCC versions grow
// unbounded (read amplification, ever-slower scans) — a scale killer. This
// leader-only loop caps history depth to the last N revisions by driving the SAME
// path the apiserver uses (CompactAsync), whose watermark is a monotonic CAS, so
// it is idempotent and cannot fight a healthy apiserver: whichever target is
// higher wins. With a generous retention it only ever bites when the primary
// compactor has fallen far behind. Reuses the existing physical GC (no new scan).
func (b *backend) runAutoCompactor() {
	retention := b.config.AutoCompactionRetention
	if retention == 0 {
		return
	}
	ctx := context.Background()
	rng := rand.New(rand.NewSource(time.Now().UnixNano()))
	for {
		wait := autoCompactBaseInterval + time.Duration(rng.Int63n(int64(autoCompactJitter)+1))
		timer := time.NewTimer(wait)
		<-timer.C

		// Only the leader drives compaction. leadingFresh is true on single-node /
		// unfenced test backends, so those auto-compact too when enabled.
		if !b.leadingFresh() {
			continue
		}
		cur := b.tso.GetRevision()
		compacted, cerr := b.GetCompactRevision(ctx)
		if cerr != nil {
			continue
		}
		target, act := autoCompactTarget(cur, retention, compacted, true /*already leadingFresh above*/)
		if !act {
			continue // not enough history yet, or the watermark already covers it
		}
		got, err := b.CompactAsync(ctx, target)
		if err != nil {
			b.metricCli.EmitCounter("backend.auto_compact.err", 1)
			klog.ErrorS(err, "auto-compaction safety net failed", "target", target)
			continue
		}
		b.metricCli.EmitCounter("backend.auto_compact", 1)
		b.metricCli.EmitGauge("backend.auto_compact.revision", got)
		klog.InfoS("auto-compaction safety net advanced the compact watermark",
			"target", target, "compacted", got, "retention", retention, "current", cur)
	}
}

// autoCompactTarget decides the safety-net compaction target: cap history to the
// last `retention` revisions. It returns act=false when not leading, retention is
// off, there is not yet more than `retention` revisions of history, or the
// watermark already covers the target (the primary compactor is keeping up).
func autoCompactTarget(currentRev, retention, compactRev uint64, leading bool) (target uint64, act bool) {
	if !leading || retention == 0 || currentRev <= retention {
		return 0, false
	}
	target = currentRev - retention
	if target <= compactRev {
		return 0, false
	}
	return target, true
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
	if err := b.fenceAdmit(ctx); err != nil {
		return false, err
	}
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
		// A concurrent compactor may have advanced the watermark between our read
		// and our CAS commit, which fails the CAS. Re-read: if the stored revision
		// already reached our target, the desired end state holds — treat it as
		// success (compaction is idempotent in etcd; compacting to an
		// already-compacted revision is not an error), and let that other compactor
		// own the physical GC (advanced=false). Only a still-behind watermark or a
		// genuine storage error surfaces.
		if cur, gerr := b.kv.Get(ctx, getCompactKey(b.config.Prefix)); gerr == nil && len(cur) >= 8 {
			if storedRev := binary.BigEndian.Uint64(cur); storedRev >= revision {
				b.updateCompactRevCache(storedRev)
				return false, nil
			}
		}
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
	// Physical GC scans the ENTIRE object keyspace, NOT a range derived from
	// config.Prefix. config.Prefix (--key-prefix) is never prepended to stored
	// keys — it only namespaces the leader-election lock — so bounding compaction
	// by it silently skipped ALL user data whenever it did not match the client's
	// real key prefix. E.g. --key-prefix=/kubebrain while the apiserver writes
	// /registry/*: the derived border [{magic}/kubebrain/, {magic}/kubebrain0)
	// contains zero rows, so physical GC scanned nothing and every tombstone /
	// superseded version accumulated forever (the "compact" counter froze while
	// LISTs degraded). The whole-keyspace border below covers every user prefix
	// AND KubeBrain's internal \x00kubebrain/ namespace (etcd metadata + lease
	// records — all latest-only, rewritten every keepalive, #6/#15/#38) in one
	// range, so GC is correct no matter how --key-prefix is set. Safe because the
	// scanner never touches versions above the compact revision (scanner.go: "if
	// curRevision > w.revision { continue }"), so co-tenants at higher revisions
	// on a shared TiKV are untouched.
	compactBorders := [][]byte{
		b.ks.ObjectKeyspaceStart(),
		b.ks.ObjectKeyspaceEnd(),
	}
	// SkippedPrefixes (--skip-key-prefix) carve holes OUT of the scanned keyspace:
	// their start/end points sort into the border list and, once scanner.Compact
	// pairs consecutive borders as [start,end) include-ranges, the skipped ranges
	// fall between pairs and are never scanned. Used when several KubeBrain
	// clusters share one TiKV cluster and each must not GC the others' object
	// types.
	for _, prefix := range b.config.SkippedPrefixes {
		key := prefix
		if !strings.HasSuffix(key, "/") {
			key = key + "/"
		}
		compactBorders = append(compactBorders,
			b.coder.EncodeObjectKey([]byte(key), 0),
			b.coder.EncodeObjectKey(PrefixEnd([]byte(key)), 0))
	}
	// sort to make sure compact in right range
	sort.Slice(compactBorders, func(i, j int) bool {
		return bytes.Compare(compactBorders[i], compactBorders[j]) < 0
	})
	return compactBorders
}
