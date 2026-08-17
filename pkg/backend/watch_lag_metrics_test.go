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
	"testing"

	"github.com/stretchr/testify/require"

	proto "github.com/kubewharf/kubebrain-client/api/v2rpc"

	"github.com/kubewharf/kubebrain/pkg/backend/common"
)

func watchLagGauge(recorder *recordCounters) float64 {
	recorder.mu.Lock()
	defer recorder.mu.Unlock()
	return recorder.g["watch.revision.lag"]
}

func TestWatchRevisionLagFallsAsCollectorCatchesUp(t *testing.T) {
	recorder := newRecordCounters()
	b := &backend{
		metricCli:             recorder,
		writeSignal:           make(chan struct{}, 1),
		watchEventsRingBuffer: newWatchEventSlots(watchersChanCapacity),
	}
	b.collectorRevision.Store(10)

	b.notifyBatch([]*common.WatchEvent{{
		Revision:     15,
		Valid:        true,
		ResourceVerb: proto.Event_PUT,
		Key:          []byte("key"),
		Value:        []byte("value"),
	}})
	require.Equal(t, uint64(15), b.watchRevisionHighWatermark.Load())
	require.Equal(t, float64(5), watchLagGauge(recorder))

	// Out-of-order notifications must not move the backlog high-watermark back.
	b.noteWatchRevision(13)
	b.setCollectorRevision(12)
	require.Equal(t, uint64(15), b.watchRevisionHighWatermark.Load())
	require.Equal(t, float64(3), watchLagGauge(recorder))

	// Collector progress refreshes the gauge even when no later write arrives.
	b.setCollectorRevision(15)
	require.Equal(t, float64(0), watchLagGauge(recorder))
}
