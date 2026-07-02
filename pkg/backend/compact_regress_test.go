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
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	proto "github.com/kubewharf/kubebrain-client/api/v2rpc"
)

// TestCompactDoesNotRegressWatermark pins that compaction never lowers the
// compact revision. Previously backend.compact proceeded to the scanner even
// when the requested revision was already compacted, and the scanner's
// unconditional Put of the compact key regressed the (monotonic) watermark,
// after which range requests at already-compacted revisions returned incomplete
// data instead of ErrCompacted.
func TestCompactDoesNotRegressWatermark(t *testing.T) {
	s, closer := newTestSuites(t, memKvStorage)
	defer closer()
	ctx := s.ctx

	var revs []uint64
	for i := 0; i < 6; i++ {
		r, err := s.backend.Create(ctx, &proto.CreateRequest{
			Key:   []byte(fmt.Sprintf("%s/k%d", prefix, i)),
			Value: []byte("v"),
		})
		require.NoError(t, err)
		require.True(t, r.Succeeded)
		revs = append(revs, r.Header.Revision)
	}
	high := revs[5]
	low := revs[1]
	require.Less(t, low, high)

	// Wait for the async event collector to commit up to high, otherwise
	// backend.Compact clamps the target down to the current committed revision.
	require.Eventually(t, func() bool {
		return s.backend.GetCurrentRevision() >= high
	}, 5*time.Second, 2*time.Millisecond)

	// Compact at the high revision.
	_, err := s.backend.Compact(ctx, high)
	require.NoError(t, err)
	got, err := s.backend.GetCompactRevision(ctx)
	require.NoError(t, err)
	require.Equal(t, high, got)

	// Compacting at a lower revision must be a no-op for the watermark.
	_, err = s.backend.Compact(ctx, low)
	require.NoError(t, err)
	got, err = s.backend.GetCompactRevision(ctx)
	require.NoError(t, err)
	require.Equal(t, high, got, "compact watermark regressed to %d (want %d): a lower compact must never lower the watermark", got, high)
}
