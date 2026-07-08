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

package etcd

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/api/v3/mvccpb"

	prommetrics "github.com/kubewharf/kubebrain/pkg/metrics/prometheus"
)

// TestPrevHintMonotonicUnderLaggingStream pins the multi-stream safety of the
// prev-hint cache (#45): a slower watcher stream republishing an OLDER event
// must never shadow the newer hint, or a faster stream would serve a stale
// PrevKv.
func TestPrevHintMonotonicUnderLaggingStream(t *testing.T) {
	c := newPrevHintCache(16)
	kv20 := &mvccpb.KeyValue{Key: []byte("k"), ModRevision: 20}
	c.note("k", 20, kv20, false)
	// Lagging stream replays the key's older event.
	c.note("k", 10, &mvccpb.KeyValue{Key: []byte("k"), ModRevision: 10}, false)
	e, ok := c.get("k")
	require.True(t, ok)
	assert.Equal(t, uint64(20), e.rev, "older republish must not shadow the newer hint")
	assert.Same(t, kv20, e.kv)
}

// TestPrevHintHitAndMissBoundaries pins the hit condition hint.rev < revision:
// a hint at or above the queried revision means events >= revision exist and
// the previous version cannot be inferred; tombstones are never a PUT's prev.
func TestPrevHintHitAndMissBoundaries(t *testing.T) {
	b := &backendShim{prevHints: newPrevHintCache(16), metricCli: prommetrics.NewMetrics()}
	kv := &mvccpb.KeyValue{Key: []byte("k"), ModRevision: 10}
	b.noteEvent([]byte("k"), 10, kv, false)

	got, ok := b.hintedPreviousEtcdKv([]byte("k"), 20)
	require.True(t, ok, "hint below the queried revision is the previous version")
	assert.Same(t, kv, got)

	_, ok = b.hintedPreviousEtcdKv([]byte("k"), 10)
	assert.False(t, ok, "hint at the queried revision must miss")
	_, ok = b.hintedPreviousEtcdKv([]byte("k"), 5)
	assert.False(t, ok, "hint above the queried revision must miss")

	b.noteEvent([]byte("k"), 30, nil, true) // DELETE
	_, ok = b.hintedPreviousEtcdKv([]byte("k"), 40)
	assert.False(t, ok, "tombstone hint is never a valid previous version for a PUT")
}

// TestPrevHintSurvivesGenerationRotation pins the two-generation bound: a hot
// key written before rotation is still answerable (promoted), and rotation
// keeps memory bounded rather than dropping correctness.
func TestPrevHintSurvivesGenerationRotation(t *testing.T) {
	c := newPrevHintCache(2)
	c.note("hot", 10, &mvccpb.KeyValue{ModRevision: 10}, false)
	c.note("a", 11, nil, false)
	c.note("b", 12, nil, false) // rotation point (cap=2)
	c.note("c", 13, nil, false)
	e, ok := c.get("hot")
	require.True(t, ok, "entry from the previous generation must still resolve")
	assert.Equal(t, uint64(10), e.rev)

	// A newer entry in the old generation must win over a stale note.
	c2 := newPrevHintCache(1)
	c2.note("k", 20, &mvccpb.KeyValue{ModRevision: 20}, false)
	c2.note("x", 1, nil, false) // rotates "k" into prev
	c2.note("k", 10, &mvccpb.KeyValue{ModRevision: 10}, false)
	e, ok = c2.get("k")
	require.True(t, ok)
	assert.Equal(t, uint64(20), e.rev, "stale note must promote the newer prev-generation entry, not shadow it")
}
