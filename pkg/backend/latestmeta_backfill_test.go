// Copyright 2026 ByteDance and/or its affiliates
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package backend

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/kubewharf/kubebrain/pkg/metrics"
)

func TestLatestMetadataBackfillQueueIsBoundedAndDeduplicated(t *testing.T) {
	queue := newLatestMetadataBackfillQueue()
	b := &backend{latestMetadataBackfill: queue}
	first := latestMetadataBackfillTask{
		key: []byte("first"), revision: 1,
		expectedMetadata: []byte("observed"), expectedMetadataFound: true,
	}
	b.queueLatestMetadataBackfill(first)
	b.queueLatestMetadataBackfill(first)
	require.Len(t, queue.ch, 1)
	require.Len(t, queue.pending, 1)

	// Fill the remaining slots with distinct identities. An additional task is
	// dropped synchronously and must not leave an unbounded pending-map entry.
	for i := 1; i < latestMetadataBackfillQueueCapacity; i++ {
		b.queueLatestMetadataBackfill(latestMetadataBackfillTask{
			key: []byte(fmt.Sprintf("key-%04d", i)), revision: uint64(i + 1),
		})
	}
	require.Len(t, queue.ch, latestMetadataBackfillQueueCapacity)
	require.Len(t, queue.pending, latestMetadataBackfillQueueCapacity)
	dropped := latestMetadataBackfillTask{key: []byte("overflow"), revision: latestMetadataBackfillQueueCapacity + 1}
	b.queueLatestMetadataBackfill(dropped)
	require.Len(t, queue.ch, latestMetadataBackfillQueueCapacity)
	require.Len(t, queue.pending, latestMetadataBackfillQueueCapacity)
	_, retained := queue.pending[latestMetadataBackfillTaskID(dropped)]
	require.False(t, retained)
}

func TestLatestMetadataBackfillQueueHasByteBudget(t *testing.T) {
	queue := newLatestMetadataBackfillQueue()
	b := &backend{latestMetadataBackfill: queue}
	for i := 0; i < 5; i++ {
		key := bytes.Repeat([]byte{byte(i + 1)}, (2<<20)-1024)
		b.queueLatestMetadataBackfill(latestMetadataBackfillTask{key: key, revision: uint64(i + 1)})
	}
	require.Less(t, len(queue.ch), 5, "the byte budget, not the much larger item limit, must reject oversized accumulation")
	require.LessOrEqual(t, queue.bytes, latestMetadataBackfillQueueMaxBytes)
	require.Len(t, queue.pending, len(queue.ch))
}

func TestLatestMetadataBackfillQueueOwnsTaskBytes(t *testing.T) {
	queue := newLatestMetadataBackfillQueue()
	b := &backend{latestMetadataBackfill: queue}
	task := latestMetadataBackfillTask{
		key: []byte("key"), revision: 7,
		expectedMetadata: []byte("old"), expectedMetadataFound: true,
	}
	b.queueLatestMetadataBackfill(task)
	task.key[0] = 'X'
	task.expectedMetadata[0] = 'X'
	queued := <-queue.ch
	require.Equal(t, []byte("key"), queued.key)
	require.Equal(t, []byte("old"), queued.expectedMetadata)
}

func TestLatestMetadataBackfillMetricsInitializeAuthoritativeZero(t *testing.T) {
	recorder := &compactMetricRecorder{}
	initLatestMetadataBackfillMetrics(recorder)
	require.Len(t, recorder.records, len(latestMetadataBackfillOutcomes))
	for i, outcome := range latestMetadataBackfillOutcomes {
		require.Equal(t, compactMetricRecord{
			kind: "counter", name: "backend.latest_metadata.backfill_outcome", value: int64(0),
			tags: []metrics.T{metrics.Tag("outcome", outcome)},
		}, recorder.records[i])
	}
}
