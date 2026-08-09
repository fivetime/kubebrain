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

	"github.com/kubewharf/kubebrain/pkg/backend/common"
)

func TestAbortedRevisionClassifiesWholeEventBatch(t *testing.T) {
	valid := func(ok bool) *common.WatchEvent { return &common.WatchEvent{Valid: ok} }
	for _, test := range []struct {
		name   string
		events []*common.WatchEvent
		want   bool
	}{
		{name: "empty", events: nil, want: false},
		{name: "committed", events: []*common.WatchEvent{valid(true)}, want: false},
		{name: "atomic committed batch", events: []*common.WatchEvent{valid(true), valid(true)}, want: false},
		{name: "mixed is not an aborted revision", events: []*common.WatchEvent{valid(false), valid(true)}, want: false},
		{name: "failed single write", events: []*common.WatchEvent{valid(false)}, want: true},
		{name: "failed transaction counts once", events: []*common.WatchEvent{valid(false), valid(false)}, want: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			require.Equal(t, test.want, abortedRevision(test.events))
		})
	}
}

func TestNotifyBatchCountsAbortedRevisionOncePerTransaction(t *testing.T) {
	recorder := &compactMetricRecorder{}
	b := &backend{
		metricCli:             recorder,
		watchEventsRingBuffer: newWatchEventSlots(watchersChanCapacity),
		writeSignal:           make(chan struct{}, 1),
	}
	b.notifyBatch([]*common.WatchEvent{{Revision: 0, Valid: false}})
	b.notifyBatch([]*common.WatchEvent{
		{Revision: 1, Valid: false},
		{Revision: 1, Valid: false},
	})
	b.notifyBatch([]*common.WatchEvent{{Revision: 2, Valid: true}})

	aborted := 0
	for _, record := range recorder.records {
		if record.kind == "counter" && record.name == "revision.generator.aborted" {
			aborted++
			require.Equal(t, 1, record.value)
		}
	}
	require.Equal(t, 1, aborted, "a failed multi-key transaction must count one aborted revision")
}

func TestInitRevisionMetricsPublishesZeroBaseline(t *testing.T) {
	recorder := &compactMetricRecorder{}
	initRevisionMetrics(recorder)
	require.Equal(t, []compactMetricRecord{{
		kind: "counter", name: "revision.generator.aborted", value: 0,
	}}, recorder.records)
}
