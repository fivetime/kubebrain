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
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// TestCollectorStallStateNote pins the event-collector stall watchdog decision
// (borrowed idea from kine's gap/fill: a single missing revision must not freeze
// the whole watch stream). A revision below Dealt() whose ring slot stays empty
// past the skip threshold is treated as a dead-writer hole and skipped; a live
// (still-in-flight) writer — always below the write timeout — is never skipped;
// followers never skip; and the warning is one-shot.
func TestCollectorStallStateNote(t *testing.T) {
	const warnAfter = 3 * time.Second
	const skipAfter = 30 * time.Second
	base := time.Unix(1_000_000, 0)

	t.Run("caught up (nextRevision > dealt) never stalls", func(t *testing.T) {
		var s collectorStallState
		s.rev = 42 // pretend we were tracking something
		warn, skip := s.note(101, 100, true, base, warnAfter, skipAfter)
		require.False(t, warn)
		require.False(t, skip)
		require.Zero(t, s.rev, "caught-up must reset tracking")
	})

	t.Run("follower never stalls even with a hole", func(t *testing.T) {
		var s collectorStallState
		warn, skip := s.note(50, 100, false /*leading*/, base, warnAfter, skipAfter)
		require.False(t, warn)
		require.False(t, skip)
		require.Zero(t, s.rev)
	})

	t.Run("live writer within the write timeout is never skipped", func(t *testing.T) {
		var s collectorStallState
		// First observation arms tracking.
		warn, skip := s.note(50, 100, true, base, warnAfter, skipAfter)
		require.False(t, warn)
		require.False(t, skip)
		// Still empty but well under warnAfter (a normal ~sub-second write).
		warn, skip = s.note(50, 100, true, base.Add(900*time.Millisecond), warnAfter, skipAfter)
		require.False(t, warn)
		require.False(t, skip)
	})

	t.Run("warns once, then skips a provably-dead revision", func(t *testing.T) {
		var s collectorStallState
		s.note(50, 100, true, base, warnAfter, skipAfter) // arm

		warn, skip := s.note(50, 100, true, base.Add(warnAfter), warnAfter, skipAfter)
		require.True(t, warn, "should warn at warnAfter")
		require.False(t, skip)

		warn, skip = s.note(50, 100, true, base.Add(warnAfter+time.Second), warnAfter, skipAfter)
		require.False(t, warn, "warning is one-shot")
		require.False(t, skip)

		warn, skip = s.note(50, 100, true, base.Add(skipAfter), warnAfter, skipAfter)
		require.False(t, warn)
		require.True(t, skip, "should skip once the writer is provably dead")
		require.Zero(t, s.rev, "skip resets tracking so the next revision is re-evaluated")
	})

	t.Run("a different hole revision re-arms tracking", func(t *testing.T) {
		var s collectorStallState
		s.note(50, 100, true, base, warnAfter, skipAfter)
		// Collector advanced to 51 (still a hole); the long wait on 50 must not
		// carry over and instantly skip 51.
		warn, skip := s.note(51, 100, true, base.Add(skipAfter), warnAfter, skipAfter)
		require.False(t, warn)
		require.False(t, skip)
		require.Equal(t, uint64(51), s.rev)
	})

	t.Run("reset clears tracking", func(t *testing.T) {
		var s collectorStallState
		s.note(50, 100, true, base, warnAfter, skipAfter)
		s.reset()
		require.Zero(t, s.rev)
	})
}
