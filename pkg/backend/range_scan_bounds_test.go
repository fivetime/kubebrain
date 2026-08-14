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
	"bytes"
	"testing"

	"github.com/kubewharf/kubebrain/pkg/backend/coder"
	"github.com/stretchr/testify/require"
)

func TestDecodedUserRangeScanBoundsNeverExcludeMatchingEncodedKey(t *testing.T) {
	b := &backend{coder: coder.DefaultKeyspace().NewCoder(), ks: coder.DefaultKeyspace()}
	keys := exhaustiveShortKeys([]byte{0, '#', '$', '%', 'a', 0xff}, 2)
	revisions := []uint64{0, 1, 0x2400000000000000, 1<<63 - 1}
	for _, start := range keys {
		for _, end := range keys {
			if bytes.Compare(start, end) >= 0 {
				continue
			}
			scanStart, scanEnd, exactKeys := b.decodedUserRangeScanPlan(start, end)
			exact := make(map[string]struct{}, len(exactKeys))
			for _, key := range exactKeys {
				exact[string(key)] = struct{}{}
				require.GreaterOrEqual(t, bytes.Compare(key, start), 0)
				require.Less(t, bytes.Compare(key, end), 0)
			}
			for _, key := range keys {
				if bytes.Compare(key, start) < 0 || bytes.Compare(key, end) >= 0 {
					continue
				}
				for _, revision := range revisions {
					if _, reconciled := exact[string(key)]; reconciled {
						continue
					}
					encoded := b.coder.EncodeObjectKey(key, revision)
					require.LessOrEqualf(t, bytes.Compare(scanStart, encoded), 0,
						"start=%x end=%x key=%x revision=%x", start, end, key, revision)
					require.Positivef(t, bytes.Compare(scanEnd, encoded),
						"start=%x end=%x key=%x revision=%x", start, end, key, revision)
				}
			}
		}
	}
}

func TestDecodedUserRangeScanBoundsNarrowsPrefixAndFallsBackForEndAncestor(t *testing.T) {
	b := &backend{coder: coder.DefaultKeyspace().NewCoder(), ks: coder.DefaultKeyspace()}
	start := []byte("$prefix/")
	end := PrefixEnd(start)
	scanStart, scanEnd, exactKeys := b.decodedUserRangeScanPlan(start, end)
	require.Greater(t, bytes.Compare(scanStart, b.ks.ObjectKeyspaceStart()), 0)
	require.Less(t, bytes.Compare(scanEnd, b.ks.ObjectKeyspaceEnd()), 0)
	require.Empty(t, exactKeys)

	scanStart, scanEnd, exactKeys = b.decodedUserRangeScanPlan([]byte("a"), []byte{'a', 0, 'z'})
	require.Greater(t, bytes.Compare(scanStart, b.ks.ObjectKeyspaceStart()), 0)
	require.Less(t, bytes.Compare(scanEnd, b.ks.ObjectKeyspaceEnd()), 0)
	require.Equal(t, [][]byte{[]byte("a")}, exactKeys)

	// The proper-prefix relation alone is not a reason to fall back. Every
	// valid encoded version of "a" sorts before the next user byte 'z'.
	scanStart, scanEnd, exactKeys = b.decodedUserRangeScanPlan([]byte("a"), []byte("az"))
	require.Greater(t, bytes.Compare(scanStart, b.ks.ObjectKeyspaceStart()), 0)
	require.Less(t, bytes.Compare(scanEnd, b.ks.ObjectKeyspaceEnd()), 0)
	require.Empty(t, exactKeys)

	// When the next byte equals the legacy delimiter, the following byte can
	// still put raw end on either side of the valid revision suffix.
	scanStart, scanEnd, exactKeys = b.decodedUserRangeScanPlan([]byte("a"), []byte{'a', '$', 'z'})
	require.Greater(t, bytes.Compare(scanStart, b.ks.ObjectKeyspaceStart()), 0)
	require.Less(t, bytes.Compare(scanEnd, b.ks.ObjectKeyspaceEnd()), 0)
	require.Equal(t, [][]byte{[]byte("a")}, exactKeys)
	scanStart, scanEnd, exactKeys = b.decodedUserRangeScanPlan([]byte("a"), []byte{'a', '$', 0xff})
	require.Greater(t, bytes.Compare(scanStart, b.ks.ObjectKeyspaceStart()), 0)
	require.Less(t, bytes.Compare(scanEnd, b.ks.ObjectKeyspaceEnd()), 0)
	require.Empty(t, exactKeys)
}

func TestDecodedUserRangeScanPlanBoundsPathologicalAncestorExpansion(t *testing.T) {
	b := &backend{coder: coder.DefaultKeyspace().NewCoder(), ks: coder.DefaultKeyspace()}
	end := make([]byte, maxDecodedRangeExactAncestors+2)
	scanStart, scanEnd, exactKeys := b.decodedUserRangeScanPlan(nil, end)
	require.Equal(t, b.ks.ObjectKeyspaceStart(), scanStart)
	require.Equal(t, b.ks.ObjectKeyspaceEnd(), scanEnd)
	require.Empty(t, exactKeys)

	// Also exercise the byte budget independently of the count budget.
	end = bytes.Repeat([]byte{0}, maxDecodedRangeExactAncestorBytes/2+2)
	scanStart, scanEnd, exactKeys = b.decodedUserRangeScanPlan(end[:len(end)-3], end)
	require.Equal(t, b.ks.ObjectKeyspaceStart(), scanStart)
	require.Equal(t, b.ks.ObjectKeyspaceEnd(), scanEnd)
	require.Empty(t, exactKeys)
}

func TestDecodedUserRangeScanPlanLinearHelpersMatchEncodedModel(t *testing.T) {
	b := &backend{coder: coder.DefaultKeyspace().NewCoder(), ks: coder.DefaultKeyspace()}
	keys := exhaustiveShortKeys([]byte{0, '#', '$', '%', 0x7f, 0xff}, 3)
	for _, end := range keys {
		if len(end) == 0 {
			continue
		}
		encodedEnd := b.coder.EncodeObjectKey(end, 0)
		rawEnd := encodedEnd[:len(encodedEnd)-9]
		for prefixLen := 0; prefixLen < len(end); prefixLen++ {
			want := bytes.Compare(b.coder.EncodeObjectKey(end[:prefixLen], 1<<63-1), rawEnd) >= 0
			require.Equalf(t, want, encodedAncestorMayReachEnd(end, prefixLen),
				"end=%x prefixLen=%d", end, prefixLen)
		}
		for _, start := range keys {
			if bytes.Compare(start, end) >= 0 {
				continue
			}
			want := len(end)
			for prefixLen := 0; prefixLen < len(end); prefixLen++ {
				if bytes.Compare(end[:prefixLen], start) >= 0 {
					want = prefixLen
					break
				}
			}
			require.Equalf(t, want, firstEndPrefixInRange(start, end),
				"start=%x end=%x", start, end)
		}
	}
}

func exhaustiveShortKeys(alphabet []byte, maxLen int) [][]byte {
	keys := [][]byte{{}}
	level := [][]byte{{}}
	for length := 1; length <= maxLen; length++ {
		next := make([][]byte, 0, len(level)*len(alphabet))
		for _, prefix := range level {
			for _, value := range alphabet {
				key := append(append([]byte(nil), prefix...), value)
				keys = append(keys, key)
				next = append(next, key)
			}
		}
		level = next
	}
	return keys
}
