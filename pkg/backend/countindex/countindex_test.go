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

package countindex

import (
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

func k(s string) []byte { return []byte(s) }

func TestCountLiveKeysAtRevision(t *testing.T) {
	idx := New(0)
	// /a created@1, /b created@2, /c created@3
	idx.Apply(k("/a"), 1, false)
	idx.Apply(k("/b"), 2, false)
	idx.Apply(k("/c"), 3, false)
	require.Equal(t, 3, idx.Count(k("/"), k("0"), 3))
	// count at an earlier revision sees fewer keys
	require.Equal(t, 1, idx.Count(k("/"), k("0"), 1))
	require.Equal(t, 2, idx.Count(k("/"), k("0"), 2))
	// sub-range
	require.Equal(t, 2, idx.Count(k("/a"), k("/c"), 3)) // [/a,/c) => /a,/b
	require.Equal(t, uint64(3), idx.ReadyRev())
}

func TestRepeatedRevisionBuildsRankSnapshot(t *testing.T) {
	idx := New(0)
	idx.Apply(k("/a"), 1, false)
	idx.Apply(k("/b"), 2, false)
	idx.Apply(k("/c"), 3, false)
	idx.Apply(k("/b"), 4, true)

	// A one-off query stays on the range iterator and only nominates the
	// revision, avoiding a full-index build for narrow requests.
	require.Equal(t, 1, idx.Count(k("/a"), k("/c"), 4))
	require.False(t, idx.rank.valid)
	require.True(t, idx.rank.candidateValid)
	require.Equal(t, uint64(4), idx.rank.candidateRev)

	// The second query at the pinned revision builds the live sorted snapshot;
	// subsequent shrinking pagination ranges are rank lookups over the same data.
	require.Equal(t, 2, idx.Count(k("/"), k("0"), 4))
	require.True(t, idx.rank.valid)
	require.Equal(t, uint64(4), idx.rank.revision)
	require.Len(t, idx.rank.keys, 2)
	keys := idx.rank.keys
	require.Equal(t, 1, idx.Count(k("/c"), k("0"), 4))
	require.Equal(t, keys, idx.rank.keys)

	// The first query at another revision only nominates it and leaves the hot
	// revision-4 snapshot available to concurrent pinned pagination.
	require.Equal(t, 3, idx.Count(k("/"), k("0"), 3))
	require.Equal(t, uint64(4), idx.rank.revision)
	require.Equal(t, keys, idx.rank.keys)
	require.Equal(t, 1, idx.Count(k("/c"), k("0"), 4))
	require.Zero(t, idx.Count(k("/z"), k("/a"), 4), "reversed empty range")
}

func TestRankSnapshotInvalidationPreservesPinnedRevision(t *testing.T) {
	idx := New(0)
	idx.Apply(k("/a"), 10, false)
	idx.Count(k("/"), k("0"), 10)
	idx.Count(k("/"), k("0"), 10)
	require.True(t, idx.rank.valid)

	// A later mutation cannot change the view at revision 10, so pagination at
	// that pinned revision keeps its snapshot and remains correct.
	idx.Apply(k("/b"), 11, false)
	require.True(t, idx.rank.valid)
	require.Equal(t, 1, idx.Count(k("/"), k("0"), 10))

	// Reset/compaction and an out-of-order rebuild event can change the indexed
	// historical view and must invalidate the snapshot.
	idx.Apply(k("/z"), 9, false)
	require.False(t, idx.rank.valid)
	require.Equal(t, 2, idx.Count(k("/"), k("0"), 10))
	idx.Count(k("/"), k("0"), 10)
	require.True(t, idx.rank.valid)
	idx.Compact(10)
	require.False(t, idx.rank.valid)
}

func TestRankSnapshotConcurrentPinnedCountAndLaterApply(t *testing.T) {
	idx := New(0)
	for i := 1; i <= 100; i++ {
		idx.Apply([]byte(fmt.Sprintf("/k/%03d", i)), uint64(i), false)
	}
	idx.Count(k("/k/"), k("/k0"), 100)
	idx.Count(k("/k/"), k("/k0"), 100)

	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := 101; i <= 200; i++ {
			idx.Apply([]byte(fmt.Sprintf("/k/%03d", i)), uint64(i), false)
		}
	}()
	for i := 0; i < 500; i++ {
		require.Equal(t, 100, idx.Count(k("/k/"), k("/k0"), 100))
	}
	<-done
	require.Equal(t, 100, idx.Count(k("/k/"), k("/k0"), 100))
	require.Equal(t, 200, idx.Count(k("/k/"), k("/k0"), 200))
	require.Equal(t, 200, idx.Count(k("/k/"), k("/k0"), 200))
}

func BenchmarkRepeatedCountAtPinnedRevision(b *testing.B) {
	idx := New(0)
	for i := 0; i < 100_000; i++ {
		idx.Apply([]byte(fmt.Sprintf("/registry/pods/%06d", i)), uint64(i+1), false)
	}
	start := []byte("/registry/pods/050000")
	rev := idx.ReadyRev()

	b.Run("range-traversal", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			idx.mu.RLock()
			_ = idx.countRangeLocked(start, nil, rev)
			idx.mu.RUnlock()
		}
	})
	b.Run("hot-rank-snapshot", func(b *testing.B) {
		// Nominate and build outside the timed region.
		idx.Count(start, nil, rev)
		idx.Count(start, nil, rev)
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_ = idx.Count(start, nil, rev)
		}
	})
}

func TestDeleteAndRecreateAcrossRevisions(t *testing.T) {
	idx := New(0)
	idx.Apply(k("/x"), 1, false) // put
	idx.Apply(k("/x"), 5, true)  // delete
	idx.Apply(k("/x"), 9, false) // recreate

	require.Equal(t, 1, idx.Count(k("/"), k("0"), 1)) // live after put
	require.Equal(t, 1, idx.Count(k("/"), k("0"), 4)) // still live before delete
	require.Equal(t, 0, idx.Count(k("/"), k("0"), 5)) // deleted at 5
	require.Equal(t, 0, idx.Count(k("/"), k("0"), 8)) // still deleted
	require.Equal(t, 1, idx.Count(k("/"), k("0"), 9)) // recreated at 9
}

func TestCompactPrunesAndKeepsCounts(t *testing.T) {
	idx := New(0)
	idx.Apply(k("/live"), 1, false)
	idx.Apply(k("/live"), 4, false) // updated
	idx.Apply(k("/gone"), 2, false)
	idx.Apply(k("/gone"), 6, true) // deleted

	idx.Compact(6) // compact away <=6
	// /gone is dead as of 6 -> dropped; /live remains
	require.Equal(t, 1, idx.Len())
	require.Equal(t, 1, idx.Count(k("/"), k("0"), 7))
	require.Equal(t, 0, idx.Count(k("/gone"), k("/gone0"), 7))
}

func TestCompactPreservesFutureOnlyTombstoneDuringRebuild(t *testing.T) {
	revs := []revEntry{{revision: 105, tombstone: true}}
	require.Equal(t, revs, pruneRevs(revs, 50),
		"a compaction older than every entry must not discard a future tombstone")
}

func TestResetBulkLoad(t *testing.T) {
	idx := New(0)
	idx.Apply(k("/stale"), 1, false) // will be discarded by reset
	idx.Reset(func() uint64 { return 100 }, func(baseRev uint64, emit func(key []byte, rev uint64, tombstone bool)) error {
		emit(k("/a"), 90, false)
		emit(k("/b"), 95, false)
		return nil
	})
	require.Equal(t, uint64(100), idx.ReadyRev())
	require.Equal(t, 2, idx.Count(k("/"), k("0"), 100))
	require.Equal(t, 0, idx.Count(k("/stale"), k("/stale0"), 100))
	// catch-up applies after reset advance normally
	idx.Apply(k("/c"), 101, false)
	require.Equal(t, 3, idx.Count(k("/"), k("0"), 101))
	require.Equal(t, uint64(101), idx.ReadyRev())
}

// TestCountMatchesBruteForce cross-checks the index against a naive recompute
// over a randomized-ish workload.
func TestCountMatchesBruteForce(t *testing.T) {
	idx := New(0)
	type ev struct {
		key  string
		rev  uint64
		tomb bool
	}
	var log []ev
	rev := uint64(0)
	for i := 0; i < 200; i++ {
		rev++
		key := fmt.Sprintf("/reg/%02d", i%25)
		tomb := i%7 == 0
		idx.Apply(k(key), rev, tomb)
		log = append(log, ev{key, rev, tomb})
	}
	brute := func(atRev uint64) int {
		latest := map[string]ev{}
		for _, e := range log {
			if e.rev <= atRev {
				latest[e.key] = e
			}
		}
		n := 0
		for _, e := range latest {
			if !e.tomb {
				n++
			}
		}
		return n
	}
	for _, at := range []uint64{1, 50, 100, 150, 200} {
		require.Equal(t, brute(at), idx.Count(k("/"), k("0"), at), "rev=%d", at)
	}
}

func TestOverflowDisablesIndex(t *testing.T) {
	idx := New(3)
	idx.Reset(func() uint64 { return 0 }, func(baseRev uint64, emit func(key []byte, rev uint64, tombstone bool)) error { return nil })
	for i := 0; i < 5; i++ {
		idx.Apply([]byte(fmt.Sprintf("/k%d", i)), uint64(i+1), false)
	}
	require.False(t, idx.Ready(5), "index must disable itself past maxKeys")
}

// TestSetReadyRevAdvancesReadyWatermark pins that the ready watermark can be
// advanced past the last applied live-key revision (for committed revisions that
// carry no Apply — CAS-failed/abandoned/bookkeeping writes), so a count at the
// current revision is answerable instead of failing Ready() and forcing a scan.
func TestSetReadyRevAdvancesReadyWatermark(t *testing.T) {
	idx := New(0)
	idx.Reset(func() uint64 { return 100 }, func(baseRev uint64, emit func(key []byte, rev uint64, tombstone bool)) error {
		emit([]byte("a"), 100, false)
		return nil
	})
	require.True(t, idx.Ready(100))
	require.False(t, idx.Ready(105), "must not be ready beyond baseRev before advancing")

	idx.SetReadyRev(105) // committed advanced to 105 with no live-key change
	require.True(t, idx.Ready(105), "ready watermark must advance to the committed revision")
	require.False(t, idx.Ready(106))
	require.EqualValues(t, 1, idx.Count([]byte("a"), []byte("z"), 105))

	idx.SetReadyRev(103) // never moves backwards
	require.True(t, idx.Ready(105))
}

// TestResetNonBlockingConcurrentApply pins the non-blocking rebuild: while Reset
// is loading the snapshot it must (a) report Ready()==false so no query sees the
// half-populated tree, and (b) NOT hold the lock — the collector can apply live
// events concurrently, in either order relative to the snapshot, and the final
// index is complete and correct at both current and historical revisions.
func TestResetNonBlockingConcurrentApply(t *testing.T) {
	idx := New(0)
	release := make(chan struct{})
	loadStarted := make(chan struct{})
	done := make(chan struct{})
	go func() {
		idx.Reset(func() uint64 { return 100 }, func(baseRev uint64, emit func(key []byte, rev uint64, tombstone bool)) error {
			if baseRev != 100 {
				t.Errorf("baseRev = %d, want 100", baseRev)
			}
			emit([]byte("a"), 100, false)
			close(loadStarted)
			<-release // block mid-load; the lock must NOT be held here
			emit([]byte("b"), 100, false)
			emit([]byte("c"), 100, false)
			return nil
		})
		close(done)
	}()
	<-loadStarted

	// Mid-load: not ready, and concurrent applies do not block (would deadlock if
	// Reset held the lock across the whole load).
	require.False(t, idx.Ready(100), "must not be ready while loading")
	idx.Apply([]byte("b"), 105, true)  // b deleted at 105 (arrives BEFORE its snapshot emit)
	idx.Apply([]byte("d"), 106, false) // d created at 106 (never in the snapshot)
	idx.Compact(50)                    // must fence page cursors without invalidating this rebuild
	require.False(t, idx.Ready(106), "still loading")

	close(release)
	<-done

	require.True(t, idx.Ready(106), "ready after load completes")
	// Live at 106: a(100), c(100), d(106); b deleted at 105 -> not live.
	require.EqualValues(t, 3, idx.Count([]byte("a"), []byte("z"), 106))
	// Historical at 100: a,b,c live, d not yet -> 3 (b's snapshot state preserved
	// despite its delete arriving first, thanks to sorted insert).
	require.EqualValues(t, 3, idx.Count([]byte("a"), []byte("z"), 100))
	// At 105: b deleted -> a,c = 2.
	require.EqualValues(t, 2, idx.Count([]byte("a"), []byte("z"), 105))
}

// TestResetPartialLoadDisablesIndex pins that a snapshot load that fails partway
// (a transient storage error) leaves the index NOT ready, so counts fall back to
// a scan instead of serving a silent undercount from a half-populated tree.
func TestResetPartialLoadDisablesIndex(t *testing.T) {
	idx := New(0)
	idx.SetReadyRev(50) // pretend the collector had advanced coverage before rebuild
	loadErr := errors.New("list failed mid-scan")
	idx.Reset(func() uint64 { return 100 }, func(baseRev uint64, emit func(key []byte, rev uint64, tombstone bool)) error {
		emit([]byte("a"), 90, false) // only a prefix of the keyspace loaded
		return loadErr
	})
	require.False(t, idx.Ready(100), "a partial (errored) load must not be served")
	require.EqualValues(t, 0, idx.BaseRev(), "errored load disables the index (baseRev=0)")
	if _, ok := idx.CountIfReady([]byte("a"), []byte("z"), 100); ok {
		t.Fatal("CountIfReady must report not-served after a partial load")
	}
}

// TestResetFloorsBaseRevAtReadyRev pins the install-gap fix: the collector
// Apply()s an event (advancing readyRev) BEFORE the committed revision catches
// up, so Reset must floor baseRev at readyRev — otherwise it would snapshot below
// the already-applied event, drop it from the fresh tree, yet keep readyRev
// claiming coverage for it (a permanently wrong count until the next rebuild).
func TestResetFloorsBaseRevAtReadyRev(t *testing.T) {
	idx := New(0)
	idx.Apply([]byte("a"), 100, false) // applied to the (soon-discarded) tree; readyRev=100
	require.EqualValues(t, 100, idx.ReadyRev())

	// currentRev() lags at 99 (committed not yet advanced past the applied event).
	// The load snapshots at baseRev and must therefore see revision 100.
	var loadedAt uint64
	idx.Reset(func() uint64 { return 99 }, func(baseRev uint64, emit func(key []byte, rev uint64, tombstone bool)) error {
		loadedAt = baseRev
		emit([]byte("a"), 100, false) // snapshot at baseRev(=100) includes a@100
		return nil
	})
	require.EqualValues(t, 100, loadedAt, "baseRev must be floored at readyRev, not the lagging committed rev")
	require.True(t, idx.Ready(100))
	require.EqualValues(t, 1, idx.Count([]byte("a"), []byte("z"), 100), "the applied event must not be lost")
}

// TestResetHistoricalReplaysInstallGap pins the historical rebuild primitive:
// it deliberately moves baseRev backwards, remains unavailable until the
// caller has replayed the old base-to-ready gap, and merges collector events
// that arrive after the fresh tree is installed.
func TestResetHistoricalReplaysInstallGap(t *testing.T) {
	idx := New(0)
	idx.Reset(func() uint64 { return 100 }, func(_ uint64, emit func(key []byte, rev uint64, tombstone bool)) error {
		emit([]byte("a"), 100, false)
		return nil
	})
	idx.Apply([]byte("b"), 105, false)

	loadStarted := make(chan struct{})
	release := make(chan struct{})
	done := make(chan struct{})
	go func() {
		idx.ResetHistorical(90, func(baseRev, catchUpRev uint64, emit func(key []byte, rev uint64, tombstone bool)) error {
			require.EqualValues(t, 90, baseRev)
			require.EqualValues(t, 105, catchUpRev,
				"the loader must close every revision already claimed by the discarded tree")
			emit([]byte("a"), 90, false) // exact historical snapshot
			close(loadStarted)
			<-release
			emit([]byte("a"), 102, true)  // durable gap replay
			emit([]byte("b"), 105, false) // idempotent with prior/current state
			return nil
		})
		close(done)
	}()
	<-loadStarted
	require.False(t, idx.Ready(90), "a partially replayed historical tree must never be served")
	idx.Apply([]byte("c"), 106, false) // collector event after tree installation
	close(release)
	<-done

	require.EqualValues(t, 90, idx.BaseRev())
	require.True(t, idx.Ready(106))
	require.EqualValues(t, 1, idx.Count([]byte("a"), []byte("z"), 90))
	require.Zero(t, idx.Count([]byte("a"), []byte("z"), 104))
	require.EqualValues(t, 1, idx.Count([]byte("a"), []byte("z"), 105))
	require.EqualValues(t, 2, idx.Count([]byte("a"), []byte("z"), 106))
}

func TestKeysPageIfReadyOrdersLiveHistoricalKeysAndFencesTreeChanges(t *testing.T) {
	idx := New(0)
	idx.Reset(func() uint64 { return 10 }, func(_ uint64, emit func(key []byte, rev uint64, tombstone bool)) error {
		for _, key := range [][]byte{[]byte("a\x00"), []byte("a"), []byte("a$"), []byte("b"), []byte("z")} {
			emit(key, 10, false)
		}
		return nil
	})
	idx.Apply([]byte("a$"), 11, true)
	idx.Apply([]byte("c"), 12, false)

	page1, generation, more, ok := idx.KeysPageIfReady([]byte("a"), []byte("z"), nil, 12, 2, 0)
	require.True(t, ok)
	require.True(t, more)
	require.Equal(t, [][]byte{[]byte("a"), []byte("a\x00")}, page1)
	page2, sameGeneration, more, ok := idx.KeysPageIfReady([]byte("a"), []byte("z"), page1[len(page1)-1], 12, 2, generation)
	require.True(t, ok)
	require.False(t, more)
	require.Equal(t, generation, sameGeneration)
	require.Equal(t, [][]byte{[]byte("b"), []byte("c")}, page2)

	// At the older revision, the later tombstone/create do not affect the view.
	historical, _, historicalMore, ok := idx.KeysPageIfReady([]byte("a"), []byte("z"), nil, 10, 8, 0)
	require.True(t, ok)
	require.False(t, historicalMore)
	require.Equal(t, [][]byte{[]byte("a"), []byte("a\x00"), []byte("a$"), []byte("b")}, historical)

	idx.Compact(10)
	_, _, _, ok = idx.KeysPageIfReady([]byte("a"), []byte("z"), page1[len(page1)-1], 12, 2, generation)
	require.False(t, ok, "compaction must invalidate an in-flight page generation")
}

func TestKeysPageBytesIfReadyBoundsCopiedKeysAndAdvancesPastOversizedKey(t *testing.T) {
	idx := New(0)
	idx.Reset(func() uint64 { return 10 }, func(_ uint64, emit func(key []byte, rev uint64, tombstone bool)) error {
		for _, key := range [][]byte{[]byte("aaaaaaaaaaaa"), []byte("bbbbbb"), []byte("cccccc")} {
			emit(key, 10, false)
		}
		return nil
	})

	page1, generation, more, ok := idx.KeysPageBytesIfReady(nil, nil, nil, 10, 300, 10, 0)
	require.True(t, ok)
	require.True(t, more)
	require.Equal(t, [][]byte{[]byte("aaaaaaaaaaaa")}, page1,
		"one legal key larger than the page byte budget must still make progress")
	page2, sameGeneration, more, ok := idx.KeysPageBytesIfReady(nil, nil, page1[0], 10, 300, 10, generation)
	require.True(t, ok)
	require.True(t, more)
	require.Equal(t, generation, sameGeneration)
	require.Equal(t, [][]byte{[]byte("bbbbbb")}, page2)
	page3, _, more, ok := idx.KeysPageBytesIfReady(nil, nil, page2[0], 10, 300, 10, generation)
	require.True(t, ok)
	require.False(t, more)
	require.Equal(t, [][]byte{[]byte("cccccc")}, page3)

	_, _, _, ok = idx.KeysPageBytesIfReady(nil, nil, nil, 10, 300, 0, 0)
	require.False(t, ok)
}
