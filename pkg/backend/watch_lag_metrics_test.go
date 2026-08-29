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
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	proto "github.com/kubewharf/kubebrain-client/api/v2rpc"

	"github.com/kubewharf/kubebrain/pkg/backend/common"
	"github.com/kubewharf/kubebrain/pkg/backend/tso"
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

func TestObservedRevisionCannotMakeCommittedEventNotificationStale(t *testing.T) {
	recorder := newRecordCounters()
	b := &backend{
		tso:                   tso.NewTSO(),
		metricCli:             recorder,
		commitNotify:          newCommitNotify(),
		writeSignal:           make(chan struct{}, 1),
		watchEventsRingBuffer: newWatchEventSlots(watchersChanCapacity),
	}
	b.tso.Init(10)
	b.collectorRevision.Store(10)

	// Models /revision observing the durable counter after TiKV commit but
	// before TxnApply publishes that transaction to the event ring.
	b.SetCurrentRevision(11)
	require.Equal(t, uint64(10), b.collectorRevision.Load(),
		"an observed durable revision is not proof of collector publication")

	b.notifyBatch([]*common.WatchEvent{{
		Revision: 11, Valid: true, ResourceVerb: proto.Event_PUT,
		Key: []byte("key"), Value: []byte("value"),
	}})
	require.NotEmpty(t, b.watchEventsRingBuffer[11%watchersChanCapacity].take(11),
		"the committed event must remain available to the ordered collector")
	recorder.mu.Lock()
	require.Zero(t, recorder.c["watch.event.buffer.stale_drop"])
	recorder.mu.Unlock()
}

func TestWatchRevisionLagHeartbeatPublishesIdleStateAndStops(t *testing.T) {
	recorder := newRecordCounters()
	b := &backend{metricCli: recorder}
	b.watchRevisionHighWatermark.Store(15)
	b.collectorRevision.Store(10)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		b.runWatchRevisionLagMetrics(ctx, time.Millisecond)
	}()
	require.Eventually(t, func() bool {
		return watchLagGauge(recorder) == 5
	}, time.Second, time.Millisecond)

	// Change only internal state: the heartbeat, rather than an enqueue or
	// collector-side metric call, must publish the authoritative idle zero.
	b.collectorRevision.Store(15)
	require.Eventually(t, func() bool {
		return watchLagGauge(recorder) == 0
	}, time.Second, time.Millisecond)

	cancel()
	require.Eventually(t, func() bool {
		select {
		case <-done:
			return true
		default:
			return false
		}
	}, time.Second, time.Millisecond)
}
