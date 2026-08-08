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
	"errors"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"

	"github.com/kubewharf/kubebrain/pkg/metrics"
)

type recordedHistogram struct {
	name  string
	value interface{}
	tags  []metrics.T
}

type recordingMetrics struct {
	histograms []recordedHistogram
	counters   []recordedCounter
}

type recordedCounter struct {
	name  string
	value interface{}
	tags  []metrics.T
}

func (r *recordingMetrics) GetGrpcServerOption() []grpc.ServerOption { return nil }

func (r *recordingMetrics) GetHttpHandlers() map[string]http.Handler { return nil }

func (r *recordingMetrics) EmitCounter(name string, value interface{}, tags ...metrics.T) error {
	r.counters = append(r.counters, recordedCounter{
		name:  name,
		value: value,
		tags:  append([]metrics.T(nil), tags...),
	})
	return nil
}

func (r *recordingMetrics) EmitGauge(string, interface{}, ...metrics.T) error { return nil }

func (r *recordingMetrics) EmitHistogram(name string, value interface{}, tags ...metrics.T) error {
	r.histograms = append(r.histograms, recordedHistogram{
		name:  name,
		value: value,
		tags:  append([]metrics.T(nil), tags...),
	})
	return nil
}

func TestEmitEtcdRequestDurationUsesUpstreamMetricNameAndLabels(t *testing.T) {
	rec := &recordingMetrics{}

	emitEtcdRequestDuration(rec, "Range", 1500*time.Millisecond, nil)
	emitEtcdRequestDuration(rec, "Put", 2*time.Second, errors.New("boom"))

	require.Len(t, rec.histograms, 2)
	require.Equal(t, "etcd.server.request.duration.seconds", rec.histograms[0].name)
	require.Equal(t, 1.5, rec.histograms[0].value)
	require.Equal(t, []metrics.T{
		metrics.Tag("type", "Range"),
		metrics.Tag("success", "true"),
	}, rec.histograms[0].tags)

	require.Equal(t, "etcd.server.request.duration.seconds", rec.histograms[1].name)
	require.Equal(t, 2.0, rec.histograms[1].value)
	require.Equal(t, []metrics.T{
		metrics.Tag("type", "Put"),
		metrics.Tag("success", "false"),
	}, rec.histograms[1].tags)
}

func TestEmitWatchSendLoopDurationsUseUpstreamMetricNames(t *testing.T) {
	rec := &recordingMetrics{}

	emitWatchSendLoopWatchStreamDuration(rec, 2*time.Second, 4)
	emitWatchSendLoopControlStreamDuration(rec, 1500*time.Millisecond)
	emitWatchSendLoopProgressDuration(rec, 250*time.Millisecond)

	require.Len(t, rec.histograms, 4)
	require.Equal(t, "etcd_debugging.server.watch_send_loop.watch_stream.duration.seconds", rec.histograms[0].name)
	require.Equal(t, 2.0, rec.histograms[0].value)
	require.Empty(t, rec.histograms[0].tags)
	require.Equal(t, "etcd_debugging.server.watch_send_loop.watch_stream.duration_per_event.seconds", rec.histograms[1].name)
	require.Equal(t, 0.5, rec.histograms[1].value)
	require.Empty(t, rec.histograms[1].tags)
	require.Equal(t, "etcd_debugging.server.watch_send_loop.control_stream.duration.seconds", rec.histograms[2].name)
	require.Equal(t, 1.5, rec.histograms[2].value)
	require.Empty(t, rec.histograms[2].tags)
	require.Equal(t, "etcd_debugging.server.watch_send_loop.progress.duration.seconds", rec.histograms[3].name)
	require.Equal(t, 0.25, rec.histograms[3].value)
	require.Empty(t, rec.histograms[3].tags)
}

func TestEtcdMVCCWatchEventCounterUsesUpstreamMetricName(t *testing.T) {
	rec := &recordingMetrics{}

	initEtcdMVCCWatchEventCounter(rec)
	emitEtcdMVCCWatchEventCounter(rec, 3)

	require.Equal(t, []recordedCounter{
		{name: "etcd_debugging.mvcc.events_total", value: 0},
		{name: "etcd_debugging.mvcc.events_total", value: 3},
	}, rec.counters)
}
