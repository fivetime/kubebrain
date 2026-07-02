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
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	proto "github.com/kubewharf/kubebrain-client/api/v2rpc"
)

// TestWatchOverflowRecoversWithoutRegressOrStall pins the fixes for the
// watch-overflow races: the reset must not move the committed revision backwards
// (#21), a stale event delivered after the reset must be dropped rather than
// left as a poison slot or re-triggering overflow via unsigned underflow (#21),
// and the event collector must recover — deliver new writes contiguously —
// rather than stalling (#9).
func TestWatchOverflowRecoversWithoutRegressOrStall(t *testing.T) {
	s, closer := newTestSuites(t, memKvStorage)
	defer closer()
	b := s.backend.(*backend)

	start := b.GetCurrentRevision()
	require.Greater(t, start, uint64(0))

	// Force an overflow: an event whose revision is a full ring capacity ahead.
	over := start + watchersChanCapacity + 5
	b.notify(s.ctx, []byte(prefix+"/x"), []byte("v"), over, 0, true, proto.Event_PUT, nil)

	cur := b.GetCurrentRevision()
	require.GreaterOrEqual(t, cur, start, "committed revision must never regress")
	require.Equal(t, over, cur, "overflow must jump the committed revision to the target")

	// A stale event (revision < current) must be dropped, not regress the
	// revision and not wedge the collector.
	b.notify(s.ctx, []byte(prefix+"/stale"), []byte("v"), start+7, 0, true, proto.Event_PUT, nil)
	require.Equal(t, over, b.GetCurrentRevision(), "a stale event must not move the committed revision backwards")

	// The pipeline must recover: a fresh write is delivered and the committed
	// revision advances past the reset watermark.
	r, err := s.backend.Create(s.ctx, &proto.CreateRequest{Key: []byte(prefix + "/after"), Value: []byte("v")})
	require.NoError(t, err)
	require.True(t, r.Succeeded)
	require.Greater(t, r.Header.Revision, cur)
	require.Eventually(t, func() bool {
		return b.GetCurrentRevision() >= r.Header.Revision
	}, 5*time.Second, 2*time.Millisecond, "event collector stalled after overflow reset")
}

// TestConcurrentNotifyDuringOverflowNoRaceNoStall hammers notify() from many
// goroutines while overflow resets fire, then confirms the pipeline still
// recovers. Run under -race, it validates that the overflow reset is
// synchronized against concurrent appends (#9): pre-fix the reset wiped slots
// while writers appended, a data race that could strand the collector.
func TestConcurrentNotifyDuringOverflowNoRaceNoStall(t *testing.T) {
	s, closer := newTestSuites(t, memKvStorage)
	defer closer()
	b := s.backend.(*backend)
	start := b.GetCurrentRevision()

	const goroutines, perG = 24, 200
	var wg sync.WaitGroup
	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := 0; i < perG; i++ {
				rev := start + uint64(g*perG+i) + 1
				b.notify(s.ctx, []byte(prefix+"/c"), []byte("v"), rev, 0, true, proto.Event_PUT, nil)
				if i%50 == 0 {
					// occasionally trigger an overflow reset
					b.notify(s.ctx, []byte(prefix+"/c"), []byte("v"),
						b.GetCurrentRevision()+watchersChanCapacity+uint64(g)+1, 0, true, proto.Event_PUT, nil)
				}
			}
		}(g)
	}
	wg.Wait()

	// Force a clean overflow so the current revision covers every dealt revision,
	// then a fresh write must be delivered contiguously — proving no stall.
	b.notify(s.ctx, []byte(prefix+"/c"), []byte("v"), b.GetCurrentRevision()+watchersChanCapacity+1, 0, true, proto.Event_PUT, nil)
	r, err := s.backend.Create(s.ctx, &proto.CreateRequest{Key: []byte(prefix + "/recover"), Value: []byte("v")})
	require.NoError(t, err)
	require.Eventually(t, func() bool {
		return b.GetCurrentRevision() >= r.Header.Revision
	}, 5*time.Second, 2*time.Millisecond, "event collector stalled after concurrent overflow storm")
}
