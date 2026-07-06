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

func TestResetBulkLoad(t *testing.T) {
	idx := New(0)
	idx.Apply(k("/stale"), 1, false) // will be discarded by reset
	idx.Reset(func() uint64 { return 100 }, func(baseRev uint64, emit func(key []byte, rev uint64, tombstone bool)) {
		emit(k("/a"), 90, false)
		emit(k("/b"), 95, false)
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
	idx.Reset(func() uint64 { return 0 }, func(baseRev uint64, emit func(key []byte, rev uint64, tombstone bool)) {})
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
	idx.Reset(func() uint64 { return 100 }, func(baseRev uint64, emit func(key []byte, rev uint64, tombstone bool)) {
		emit([]byte("a"), 100, false)
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
		idx.Reset(func() uint64 { return 100 }, func(baseRev uint64, emit func(key []byte, rev uint64, tombstone bool)) {
			if baseRev != 100 {
				t.Errorf("baseRev = %d, want 100", baseRev)
			}
			emit([]byte("a"), 100, false)
			close(loadStarted)
			<-release // block mid-load; the lock must NOT be held here
			emit([]byte("b"), 100, false)
			emit([]byte("c"), 100, false)
		})
		close(done)
	}()
	<-loadStarted

	// Mid-load: not ready, and concurrent applies do not block (would deadlock if
	// Reset held the lock across the whole load).
	require.False(t, idx.Ready(100), "must not be ready while loading")
	idx.Apply([]byte("b"), 105, true)  // b deleted at 105 (arrives BEFORE its snapshot emit)
	idx.Apply([]byte("d"), 106, false) // d created at 106 (never in the snapshot)
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
