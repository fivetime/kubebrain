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

package tso

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// TestCommitIsMonotonic pins that the committed revision never moves backwards:
// a stale Commit (e.g. the event collector racing a watch-overflow reset) must
// not pull the cluster read revision back.
func TestCommitIsMonotonic(t *testing.T) {
	n := NewTSO()
	n.Init(100)
	n.Commit(150)
	require.Equal(t, uint64(150), n.GetRevision())
	n.Commit(120) // stale, must be ignored
	require.Equal(t, uint64(150), n.GetRevision())
	require.GreaterOrEqual(t, n.Dealt(), uint64(150), "Dealt must track the highest committed revision")
	n.Commit(200)
	require.Equal(t, uint64(200), n.GetRevision())
}

func TestDealtTracksHighestHandedOut(t *testing.T) {
	n := NewTSO()
	n.Init(10)
	require.Equal(t, uint64(10), n.Dealt())
	r, _ := n.Deal()
	require.Equal(t, uint64(11), r)
	require.Equal(t, uint64(11), n.Dealt())
}
