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
	idx := New()
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
	idx := New()
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
	idx := New()
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
	idx := New()
	idx.Apply(k("/stale"), 1, false) // will be discarded by reset
	idx.Reset(100, func(emit func(key []byte, rev uint64, tombstone bool)) {
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
	idx := New()
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
