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
			scanStart, scanEnd := b.decodedUserRangeScanBounds(start, end)
			for _, key := range keys {
				if bytes.Compare(key, start) < 0 || bytes.Compare(key, end) >= 0 {
					continue
				}
				for _, revision := range revisions {
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
	scanStart, scanEnd := b.decodedUserRangeScanBounds(start, end)
	require.Greater(t, bytes.Compare(scanStart, b.ks.ObjectKeyspaceStart()), 0)
	require.Less(t, bytes.Compare(scanEnd, b.ks.ObjectKeyspaceEnd()), 0)

	scanStart, scanEnd = b.decodedUserRangeScanBounds([]byte("a"), []byte{'a', 0, 'z'})
	require.Equal(t, b.ks.ObjectKeyspaceStart(), scanStart)
	require.Equal(t, b.ks.ObjectKeyspaceEnd(), scanEnd)
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
