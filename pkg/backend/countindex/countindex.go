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

// Package countindex is an in-memory, key-sorted, versioned index (etcd
// treeIndex-style) maintained eagerly on the leader and caught up lazily on a
// follower when a decoded RangeStream needs ordered pagination. It answers the exact number of
// live keys in a range at any (non-compacted) revision without scanning
// storage. The first count at a revision uses O(range) traversal; repeated
// counts at that pinned revision use a lazily built live-key rank snapshot and
// answer in O(log N), rooting paginated-count read amplification.
// See docs/read_amp_a_index_cn.md.
package countindex

import (
	"bytes"
	"sort"
	"sync"

	"github.com/google/btree"
)

type revEntry struct {
	revision  uint64
	tombstone bool
}

// LatestState is one live key and its exact latest revision in an atomically
// sampled ready tree. Key is copied before the tree lock is released.
type LatestState struct {
	Key      []byte
	Revision uint64
}

// keyItem holds a key's revision history in ascending revision order.
type keyItem struct {
	key  []byte
	revs []revEntry
}

func (a *keyItem) Less(b btree.Item) bool {
	return bytes.Compare(a.key, b.(*keyItem).key) < 0
}

// liveAt reports whether the key is live at revision rev: the newest revision
// entry <= rev exists and is not a tombstone. Callers must only query rev >=
// the compaction revision.
func (k *keyItem) liveAt(rev uint64) bool {
	latest := sort.Search(len(k.revs), func(i int) bool {
		return k.revs[i].revision > rev
	}) - 1
	return latest >= 0 && !k.revs[latest].tombstone
}

// pruneRevs drops entries that are no longer needed once revisions <= compactRev
// are compacted: keep the newest entry <= compactRev (it defines the state at
// compactRev) plus everything after it.
func pruneRevs(revs []revEntry, compactRev uint64) []revEntry {
	firstFuture := sort.Search(len(revs), func(i int) bool {
		return revs[i].revision > compactRev
	})
	if firstFuture == 0 {
		// Every entry is newer than the watermark. In particular, do not drop a
		// future-only tombstone: a concurrent non-blocking rebuild may install its
		// older live baseline later, and losing the tombstone would resurrect it.
		return revs
	}
	keepFrom := firstFuture - 1
	// If the kept baseline is a tombstone and nothing newer exists, the key is
	// dead as of compactRev and can be dropped entirely.
	if keepFrom == len(revs)-1 && revs[keepFrom].tombstone {
		return nil
	}
	if keepFrom == 0 {
		return revs
	}
	out := make([]revEntry, len(revs)-keepFrom)
	copy(out, revs[keepFrom:])
	return out
}

// TreeIndex is the concurrency-safe count index.
type TreeIndex struct {
	mu   sync.RWMutex
	tree *btree.BTree
	// baseRev is the revision of the complete snapshot the index was rebuilt
	// from; the index has no key history below it, so counts at rev < baseRev
	// must fall back to a scan. readyRev is the highest revision applied.
	baseRev  uint64
	readyRev uint64
	// maxKeys caps the number of tracked keys (0 = unlimited); once exceeded the
	// index disables itself (overflowed) so callers fall back to a scan instead
	// of risking OOM.
	maxKeys    int
	overflowed bool
	// gen counts Reset installs. A rebuild that lost leadership can still be
	// draining its scan when a re-election starts the next rebuild; its stale
	// emit closure and completion would otherwise pollute the fresh tree
	// (stale snapshot rows resurrecting deleted keys) or flip loading/baseRev
	// under the newer rebuild (review #51). Every Reset captures its own gen;
	// emits and completion from an older gen are dropped.
	gen uint64
	// pageGen fences ordered page cursors. Reset and Compact invalidate a cursor,
	// but Compact must not change gen: doing so during a Reset load would make
	// that rebuild discard its own completion and leave loading=true forever.
	pageGen uint64
	// loading is true while Reset is bulk-loading a snapshot. Reset does NOT hold
	// the lock for the whole (minutes-long at scale) load — it loads with per-key
	// locking so the ordered collector can keep applying live events between keys
	// (no committed-revision freeze, no watch-buffer overflow on failover). While
	// loading, the tree is only partially populated, so Ready reports false and
	// counts fall back to a scan until the load completes.
	loading bool
	// rankMu protects the lazy immutable live-key snapshot. It is always taken
	// after mu (read or write), which keeps cache construction atomic with tree
	// mutations without making cache hits take the exclusive tree lock.
	rankMu sync.Mutex
	rank   rankSnapshot
}

type rankSnapshot struct {
	// candidateRev is the revision seen once. We deliberately keep its first
	// query on the O(range) path so a one-off narrow count does not pay an O(N)
	// full-index build. A second query at the same revision builds keys.
	candidateRev   uint64
	candidateValid bool
	revision       uint64
	valid          bool
	keys           []*keyItem
}

// New builds an empty index. maxKeys caps tracked keys (0 = unlimited).
func New(maxKeys int) *TreeIndex {
	return &TreeIndex{tree: btree.New(32), maxKeys: maxKeys}
}

func (t *TreeIndex) checkOverflowLocked() {
	if t.maxKeys > 0 && t.tree.Len() > t.maxKeys {
		t.overflowed = true
		t.tree = btree.New(32) // free memory; counts fall back to a scan
		t.clearRankLocked()
	}
}

// clearRankLocked is called with t.mu held exclusively. Lock ordering is
// t.mu -> rankMu everywhere.
func (t *TreeIndex) clearRankLocked() {
	t.rankMu.Lock()
	t.rank = rankSnapshot{}
	t.rankMu.Unlock()
}

// invalidateRankAtLocked invalidates snapshots whose historical view can be
// changed by an event at rev. A later event cannot affect a pinned older view,
// so retaining it is both correct and important for pagination during writes.
func (t *TreeIndex) invalidateRankAtLocked(rev uint64) {
	t.rankMu.Lock()
	if (t.rank.valid && rev <= t.rank.revision) ||
		(t.rank.candidateValid && rev <= t.rank.candidateRev) {
		t.rank = rankSnapshot{}
	}
	t.rankMu.Unlock()
}

// Apply records that key changed at rev (tombstone=true for a delete). It must
// be called in ascending revision order (from the ordered event collector).
func (t *TreeIndex) Apply(key []byte, rev uint64, tombstone bool) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.overflowed {
		return
	}
	t.invalidateRankAtLocked(rev)
	t.applyLocked(key, rev, tombstone)
	t.checkOverflowLocked()
	if rev > t.readyRev {
		t.readyRev = rev
	}
}

func (t *TreeIndex) applyLocked(key []byte, rev uint64, tombstone bool) {
	if it := t.tree.Get(&keyItem{key: key}); it != nil {
		ki := it.(*keyItem)
		// Keep revs ascending (liveAt/pruneRevs require it). The ordered collector
		// normally appends the newest revision, but a NON-blocking rebuild
		// interleaves the snapshot (revisions == baseRev) with concurrent collector
		// applies (revisions > baseRev) in EITHER order, so insert at the sorted
		// position instead of only appending — appending an older snapshot entry
		// after a newer delete, and then reading back with the old "skip if <= last"
		// guard, would have dropped the older state and mis-counted historical
		// revisions. A duplicate revision (idempotent re-apply) is a no-op.
		// Fast path: the ordered collector appends the newest revision, so keep an
		// O(1) tail-compare and only fall back to the O(log n) search for the rare
		// out-of-order (rebuild snapshot vs concurrent apply) case.
		if n := len(ki.revs); n > 0 && rev > ki.revs[n-1].revision {
			ki.revs = append(ki.revs, revEntry{revision: rev, tombstone: tombstone})
			return
		}
		i := sort.Search(len(ki.revs), func(j int) bool { return ki.revs[j].revision >= rev })
		if i < len(ki.revs) && ki.revs[i].revision == rev {
			return
		}
		ki.revs = append(ki.revs, revEntry{})
		copy(ki.revs[i+1:], ki.revs[i:])
		ki.revs[i] = revEntry{revision: rev, tombstone: tombstone}
		return
	}
	t.tree.ReplaceOrInsert(&keyItem{
		key:  append([]byte(nil), key...),
		revs: []revEntry{{revision: rev, tombstone: tombstone}},
	})
}

// Reset rebuilds the index from a complete live snapshot, used on leadership
// acquisition. It does NOT hold the index lock for the whole load — which at
// scale takes minutes and would block the ordered collector, freezing the
// committed revision and overflowing the watch-event buffer (repeated cold-ring
// resets during every failover). Instead it installs a fresh tree under a brief
// lock, then loads the snapshot with PER-KEY locking so the collector keeps
// applying live events between keys. While loading, Ready() is false, so no
// query sees the half-populated tree.
//
// baseRev is captured under the install lock as max(currentRev(), readyRev): the
// committed revision, floored at the highest revision already applied to the
// (about-to-be-discarded) tree. The floor matters because the collector Apply()s
// a live event BEFORE it advances the committed revision, so an event at
// readyRev can already be in the old tree while currentRev() still reads
// readyRev-1; snapshotting at the lower value would drop that event from the new
// tree yet leave readyRev claiming coverage for it. Taking the max snapshots
// storage at a revision that includes every applied event, so the collector's
// subsequent applies (baseRev+1, ...) land on the fresh tree while the snapshot
// fills in the state as of baseRev; the two merge via applyLocked's out-of-order
// guard, leaving the tree complete and current.
//
// load returns an error if the snapshot scan did not complete (a transient
// storage error mid-load). On error the index is disabled (baseRev=0 → Ready
// false) so counts fall back to a scan rather than serving a silent undercount
// from a half-loaded tree.
func (t *TreeIndex) Reset(currentRev func() uint64, load func(baseRev uint64, emit func(key []byte, rev uint64, tombstone bool)) error) {
	t.reset(currentRev(), true, func(baseRev, _ uint64, emit func(key []byte, rev uint64, tombstone bool)) error {
		return load(baseRev, emit)
	})
}

// ResetHistorical replaces the tree with an exact historical snapshot and lets
// load replay the durable gap through catchUpRev before the tree becomes ready.
// Unlike Reset, baseRev is intentionally not floored at the discarded tree's
// ready watermark. Events applied after installation merge into the loading
// tree; catchUpRev captures every event applied before installation, so the
// loader can close the only possible gap.
func (t *TreeIndex) ResetHistorical(baseRev uint64, load func(baseRev, catchUpRev uint64, emit func(key []byte, rev uint64, tombstone bool)) error) {
	t.reset(baseRev, false, load)
}

func (t *TreeIndex) reset(baseRev uint64, floorAtReady bool, load func(baseRev, catchUpRev uint64, emit func(key []byte, rev uint64, tombstone bool)) error) {
	t.mu.Lock()
	t.gen++
	t.pageGen++
	myGen := t.gen
	catchUpRev := t.readyRev
	if floorAtReady && catchUpRev > baseRev {
		baseRev = t.readyRev
	}
	if catchUpRev < baseRev {
		catchUpRev = baseRev
	}
	t.tree = btree.New(32)
	t.clearRankLocked()
	t.overflowed = false
	t.loading = true
	t.baseRev = baseRev
	if t.readyRev < baseRev {
		t.readyRev = baseRev
	}
	t.mu.Unlock()

	err := load(baseRev, catchUpRev, func(key []byte, rev uint64, tombstone bool) {
		t.mu.Lock()
		if t.gen == myGen && !t.overflowed {
			t.applyLocked(key, rev, tombstone)
			t.checkOverflowLocked()
		}
		t.mu.Unlock()
	})

	t.mu.Lock()
	if t.gen == myGen {
		t.loading = false
		if err != nil {
			// Incomplete snapshot: the tree is missing every key past the failed page.
			// Disable the index so Ready() is false and counts fall back to a scan until
			// the next rebuild, instead of serving a silent undercount.
			t.baseRev = 0
		}
	}
	t.mu.Unlock()
}

// Ready reports whether the index can answer a count at revision rev, i.e. it
// has been rebuilt (baseRev>0), rev is within [baseRev, readyRev].
func (t *TreeIndex) Ready(rev uint64) bool {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return !t.overflowed && !t.loading && t.baseRev > 0 && rev >= t.baseRev && rev <= t.readyRev
}

func (t *TreeIndex) BaseRev() uint64 {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.baseRev
}

// SetReadyRev advances the revision through which the index is accurate.
func (t *TreeIndex) SetReadyRev(rev uint64) {
	t.mu.Lock()
	if rev > t.readyRev {
		t.readyRev = rev
	}
	t.mu.Unlock()
}

func (t *TreeIndex) ReadyRev() uint64 {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.readyRev
}

// LatestIfReady returns the key's latest indexed state at the tree's current
// ready revision. The state and ready revision are sampled under one read lock,
// so a concurrent Reset/Apply cannot splice an expectation from two generations.
// found=false with ready=true is authoritative absence at readyRev.
func (t *TreeIndex) LatestIfReady(key []byte) (revision uint64, tombstone, found bool, readyRev uint64, ready bool) {
	t.mu.RLock()
	defer t.mu.RUnlock()
	if t.overflowed || t.loading || t.baseRev == 0 || t.readyRev < t.baseRev {
		return 0, false, false, t.readyRev, false
	}
	readyRev = t.readyRev
	item := t.tree.Get(&keyItem{key: key})
	if item == nil {
		return 0, false, false, readyRev, true
	}
	revisions := item.(*keyItem).revs
	latest := sort.Search(len(revisions), func(i int) bool {
		return revisions[i].revision > readyRev
	}) - 1
	if latest < 0 {
		return 0, false, false, readyRev, true
	}
	entry := revisions[latest]
	return entry.revision, entry.tombstone, true, readyRev, true
}

// LatestRangeIfReady returns up to limit live states in [start,end) at the
// tree's current ready revision. A nil/empty end means the rest of keyspace and
// limit <= 0 means unlimited. States and readyRev come from one read-locked
// generation, so callers can compare a physical range page without splicing a
// concurrent Reset or Apply into its expectation.
func (t *TreeIndex) LatestRangeIfReady(start, end []byte, limit int) (states []LatestState, readyRev uint64, more, ready bool) {
	t.mu.RLock()
	defer t.mu.RUnlock()
	if t.overflowed || t.loading || t.baseRev == 0 || t.readyRev < t.baseRev {
		return nil, t.readyRev, false, false
	}
	readyRev = t.readyRev
	visit := func(it btree.Item) bool {
		ki := it.(*keyItem)
		latest := sort.Search(len(ki.revs), func(i int) bool {
			return ki.revs[i].revision > readyRev
		}) - 1
		if latest < 0 || ki.revs[latest].tombstone {
			return true
		}
		if limit > 0 && len(states) >= limit {
			more = true
			return false
		}
		states = append(states, LatestState{
			Key: append([]byte(nil), ki.key...), Revision: ki.revs[latest].revision,
		})
		return true
	}
	if len(end) == 0 {
		t.tree.AscendGreaterOrEqual(&keyItem{key: start}, visit)
	} else {
		t.tree.AscendRange(&keyItem{key: start}, &keyItem{key: end}, visit)
	}
	return states, readyRev, more, true
}

func (t *TreeIndex) Len() int {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.tree.Len()
}

// Overflowed reports whether the index disabled itself after exceeding maxKeys
// (counts then fall back to a scan). Exposed for live observability.
func (t *TreeIndex) Overflowed() bool {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.overflowed
}

// Count returns the number of live keys in [start, end) at revision rev. A nil
// end means "to the end of the keyspace".
func (t *TreeIndex) Count(start, end []byte, rev uint64) int {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.countLocked(start, end, rev)
}

// CountIfReady atomically checks readiness AND counts under a single lock, so a
// concurrent Reset cannot swap in a fresh, half-loaded tree between a separate
// Ready() check and Count() (which would serve a spurious ~0 as an authoritative
// hit). It returns (count, true) only if the index can answer at rev; otherwise
// (0, false) and the caller must fall back to a scan.
func (t *TreeIndex) CountIfReady(start, end []byte, rev uint64) (int, bool) {
	t.mu.RLock()
	defer t.mu.RUnlock()
	if t.overflowed || t.loading || t.baseRev == 0 || rev < t.baseRev || rev > t.readyRev {
		return 0, false
	}
	return t.countLocked(start, end, rev), true
}

// KeysPageIfReady returns at most limit live user keys in [start,end), ordered
// lexicographically, and a generation token for the next page. after is an
// exclusive cursor; pass nil for the first page. A non-zero generation must
// still match the installed tree, otherwise the caller must abandon the stream
// and retry from a fresh snapshot rather than splice pages from two rebuilds.
//
// Keys are copied while the read lock is held so callers can perform TiKV reads
// without blocking the ordered event collector. Events newer than rev may be
// appended between pages but cannot change liveAt(rev); Reset/Compact are
// fenced by pageGen.
func (t *TreeIndex) KeysPageIfReady(start, end, after []byte, rev uint64, limit int, generation uint64) (keys [][]byte, nextGeneration uint64, more bool, ok bool) {
	return t.keysPageIfReady(start, end, after, rev, limit, 0, generation)
}

// KeysPageBytesIfReady additionally bounds the sum of copied user-key bytes.
// The first live key is always admitted even when it alone exceeds maxBytes so
// callers can make progress with a legal oversized key.
func (t *TreeIndex) KeysPageBytesIfReady(start, end, after []byte, rev uint64, limit, maxBytes int, generation uint64) (keys [][]byte, nextGeneration uint64, more bool, ok bool) {
	if maxBytes <= 0 {
		return nil, 0, false, false
	}
	return t.keysPageIfReady(start, end, after, rev, limit, maxBytes, generation)
}

func (t *TreeIndex) keysPageIfReady(start, end, after []byte, rev uint64, limit, maxBytes int, generation uint64) (keys [][]byte, nextGeneration uint64, more bool, ok bool) {
	t.mu.RLock()
	defer t.mu.RUnlock()
	if limit <= 0 || t.overflowed || t.loading || t.baseRev == 0 ||
		rev < t.baseRev || rev > t.readyRev || (generation != 0 && generation != t.pageGen) {
		return nil, 0, false, false
	}
	nextGeneration = t.pageGen
	keyBytes := 0
	seek := start
	if len(after) != 0 && bytes.Compare(after, seek) >= 0 {
		seek = after
	}
	t.tree.AscendGreaterOrEqual(&keyItem{key: seek}, func(it btree.Item) bool {
		ki := it.(*keyItem)
		if len(after) != 0 && bytes.Compare(ki.key, after) <= 0 {
			return true
		}
		if len(end) != 0 && bytes.Compare(ki.key, end) >= 0 {
			return false
		}
		if !ki.liveAt(rev) {
			return true
		}
		if len(keys) == limit || (maxBytes > 0 && len(keys) != 0 && keyBytes+len(ki.key) > maxBytes) {
			more = true
			return false
		}
		keys = append(keys, append([]byte(nil), ki.key...))
		keyBytes += len(ki.key)
		return true
	})
	return keys, nextGeneration, more, true
}

func (t *TreeIndex) countLocked(start, end []byte, rev uint64) int {
	t.rankMu.Lock()
	if t.rank.valid && t.rank.revision == rev {
		n := countRank(t.rank.keys, start, end)
		t.rankMu.Unlock()
		return n
	}
	if t.rank.candidateValid && t.rank.candidateRev == rev {
		keys := make([]*keyItem, 0, t.tree.Len())
		t.tree.Ascend(func(it btree.Item) bool {
			ki := it.(*keyItem)
			if ki.liveAt(rev) {
				keys = append(keys, ki)
			}
			return true
		})
		t.rank = rankSnapshot{revision: rev, valid: true, keys: keys}
		n := countRank(keys, start, end)
		t.rankMu.Unlock()
		return n
	}
	// Preserve a still-correct snapshot for another pinned revision until this
	// revision proves hot enough (a second query) to replace it.
	t.rank.candidateRev = rev
	t.rank.candidateValid = true
	t.rankMu.Unlock()
	return t.countRangeLocked(start, end, rev)
}

func countRank(keys []*keyItem, start, end []byte) int {
	lo := sort.Search(len(keys), func(i int) bool {
		return bytes.Compare(keys[i].key, start) >= 0
	})
	hi := len(keys)
	if len(end) != 0 {
		hi = sort.Search(len(keys), func(i int) bool {
			return bytes.Compare(keys[i].key, end) >= 0
		})
	}
	if hi < lo {
		return 0
	}
	return hi - lo
}

func (t *TreeIndex) countRangeLocked(start, end []byte, rev uint64) int {
	n := 0
	iter := func(it btree.Item) bool {
		if it.(*keyItem).liveAt(rev) {
			n++
		}
		return true
	}
	if len(end) == 0 {
		t.tree.AscendGreaterOrEqual(&keyItem{key: start}, iter)
	} else {
		t.tree.AscendRange(&keyItem{key: start}, &keyItem{key: end}, iter)
	}
	return n
}

// Compact prunes revisions <= compactRev, dropping keys that are dead as of it.
func (t *TreeIndex) Compact(compactRev uint64) {
	t.mu.Lock()
	defer t.mu.Unlock()
	// A paged reader at a now-compacted revision must not silently continue
	// against the pruned tree. Invalidate its generation; the RangeStream layer
	// will surface compaction/retry rather than returning an incomplete list.
	t.pageGen++
	t.clearRankLocked()
	var dead []*keyItem
	t.tree.Ascend(func(it btree.Item) bool {
		ki := it.(*keyItem)
		ki.revs = pruneRevs(ki.revs, compactRev)
		if len(ki.revs) == 0 {
			dead = append(dead, ki)
		}
		return true
	})
	for _, ki := range dead {
		t.tree.Delete(ki)
	}
}
