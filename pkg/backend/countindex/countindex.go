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
// treeIndex-style) maintained on the leader. It answers the exact number of
// live keys in a range at any (non-compacted) revision in O(range) without
// scanning storage, rooting the O(N^2) paginated-count read amplification.
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
	latest := -1
	for i := range k.revs {
		if k.revs[i].revision <= rev {
			latest = i
		} else {
			break
		}
	}
	return latest >= 0 && !k.revs[latest].tombstone
}

// pruneRevs drops entries that are no longer needed once revisions <= compactRev
// are compacted: keep the newest entry <= compactRev (it defines the state at
// compactRev) plus everything after it.
func pruneRevs(revs []revEntry, compactRev uint64) []revEntry {
	keepFrom := 0
	for i := range revs {
		if revs[i].revision <= compactRev {
			keepFrom = i
		} else {
			break
		}
	}
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
	// loading is true while Reset is bulk-loading a snapshot. Reset does NOT hold
	// the lock for the whole (minutes-long at scale) load — it loads with per-key
	// locking so the ordered collector can keep applying live events between keys
	// (no committed-revision freeze, no watch-buffer overflow on failover). While
	// loading, the tree is only partially populated, so Ready reports false and
	// counts fall back to a scan until the load completes.
	loading bool
}

// New builds an empty index. maxKeys caps tracked keys (0 = unlimited).
func New(maxKeys int) *TreeIndex {
	return &TreeIndex{tree: btree.New(32), maxKeys: maxKeys}
}

func (t *TreeIndex) checkOverflowLocked() {
	if t.maxKeys > 0 && t.tree.Len() > t.maxKeys {
		t.overflowed = true
		t.tree = btree.New(32) // free memory; counts fall back to a scan
	}
}

// Apply records that key changed at rev (tombstone=true for a delete). It must
// be called in ascending revision order (from the ordered event collector).
func (t *TreeIndex) Apply(key []byte, rev uint64, tombstone bool) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.overflowed {
		return
	}
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
// baseRev is captured under the install lock via currentRev(): it is the
// committed revision at that instant, so the collector's subsequent applies
// (baseRev+1, ...) land on the fresh tree while the snapshot fills in the state
// as of baseRev. The two merge via applyLocked's out-of-order guard, leaving the
// tree complete and current; readyRev is whatever the collector has advanced it
// to (>= baseRev), not regressed. load receives baseRev so it can scan storage
// at exactly that revision.
func (t *TreeIndex) Reset(currentRev func() uint64, load func(baseRev uint64, emit func(key []byte, rev uint64, tombstone bool))) {
	t.mu.Lock()
	baseRev := currentRev()
	t.tree = btree.New(32)
	t.overflowed = false
	t.loading = true
	t.baseRev = baseRev
	if t.readyRev < baseRev {
		t.readyRev = baseRev
	}
	t.mu.Unlock()

	load(baseRev, func(key []byte, rev uint64, tombstone bool) {
		t.mu.Lock()
		if !t.overflowed {
			t.applyLocked(key, rev, tombstone)
			t.checkOverflowLocked()
		}
		t.mu.Unlock()
	})

	t.mu.Lock()
	t.loading = false
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

func (t *TreeIndex) Len() int {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.tree.Len()
}

// Count returns the number of live keys in [start, end) at revision rev. A nil
// end means "to the end of the keyspace".
func (t *TreeIndex) Count(start, end []byte, rev uint64) int {
	t.mu.RLock()
	defer t.mu.RUnlock()
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
