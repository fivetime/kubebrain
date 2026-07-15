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

package coder

import (
	"bytes"
	"math"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestEventLogRangeEndNoWrapAtMaxRevision pins the review-51 leftover: the old
// EncodeEventLogKey(rev+1) bound wrapped to revision 0 at rev == MaxUint64,
// producing an end that sorted BELOW the start so the range read silently
// scanned nothing.
func TestEventLogRangeEndNoWrapAtMaxRevision(t *testing.T) {
	start := DefaultKeyspace().EventLogRangeStart(math.MaxUint64)
	end := DefaultKeyspace().EventLogRangeEnd(math.MaxUint64)
	require.Positive(t, bytes.Compare(end, start), "range end must sort above start even at MaxUint64")

	// Every entry at MaxUint64 (any userKey) must fall inside [start, end).
	entry := DefaultKeyspace().EncodeEventLogKey(math.MaxUint64, []byte("/registry/pods/p1"))
	require.LessOrEqual(t, bytes.Compare(start, entry), 0, "entry at MaxUint64 must be at/above the start")
	require.Positive(t, bytes.Compare(end, entry), "entry at MaxUint64 must be below the exclusive end")
}

// TestEventLogRangeEndExclusiveBoundary checks the end is a correct exclusive
// upper bound at representative revisions (including carry-inducing ones): every
// entry AT rev is below it, and it does not reach into rev+1.
func TestEventLogRangeEndExclusiveBoundary(t *testing.T) {
	for _, rev := range []uint64{0, 1, 255, 256, 1<<16 - 1, 1 << 16, 1<<32 - 1, 1 << 32, math.MaxUint64 - 1} {
		end := DefaultKeyspace().EventLogRangeEnd(rev)
		// An entry at rev, even with the largest plausible userKey, is inside.
		inside := DefaultKeyspace().EncodeEventLogKey(rev, bytes.Repeat([]byte{0xff}, 64))
		require.Positivef(t, bytes.Compare(end, inside), "rev %d: entry must sort below the exclusive end", rev)
		// The end must not reach the first entry of rev+1 (it excludes rev+1).
		next := DefaultKeyspace().EncodeEventLogKey(rev+1, nil)
		require.LessOrEqualf(t, bytes.Compare(end, next), 0, "rev %d: end must not exceed rev+1's start", rev)
	}
}
