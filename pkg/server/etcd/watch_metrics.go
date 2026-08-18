// Copyright 2022 ByteDance and/or its affiliates
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

import "github.com/kubewharf/kubebrain/pkg/metrics"

var watchBackendIntegrityKinds = []string{"invalid_result", "invalid_revision"}

func initWatchBackendIntegrityMetrics(metricCli metrics.Metrics) {
	if metricCli == nil {
		return
	}
	for _, kind := range watchBackendIntegrityKinds {
		_ = metricCli.EmitCounter("watch.backend.integrity_failure", 0, metrics.Tag("kind", kind))
	}
}

func emitWatchBackendIntegrityFailure(metricCli metrics.Metrics, kind string) {
	if metricCli == nil {
		return
	}
	// Keep the legacy counters for existing dashboards while publishing one
	// bounded taxonomy that production monitoring can initialize and reconcile.
	_ = metricCli.EmitCounter("watch.backend."+kind, 1)
	_ = metricCli.EmitCounter("watch.backend.integrity_failure", 1, metrics.Tag("kind", kind))
}

var etcdWatchSendLoopDurationMetrics = []string{
	"etcd_debugging.server.watch_send_loop.watch_stream.duration.seconds",
	"etcd_debugging.server.watch_send_loop.watch_stream.duration_per_event.seconds",
	"etcd_debugging.server.watch_send_loop.control_stream.duration.seconds",
	"etcd_debugging.server.watch_send_loop.progress.duration.seconds",
}

func initEtcdWatchSendLoopDurationMetrics(metricCli metrics.Metrics) {
	registrar, ok := metricCli.(metrics.HistogramRegistrar)
	if !ok {
		return
	}
	for _, name := range etcdWatchSendLoopDurationMetrics {
		_ = registrar.RegisterHistogram(name)
	}
}

func initEtcdMVCCWatchEventCounter(metricCli metrics.Metrics) {
	emitEtcdMVCCWatchEventCounter(metricCli, 0)
}

func initEtcdMVCCWatchPendingEventGauge(metricCli metrics.Metrics) {
	emitEtcdMVCCWatchPendingEventGauge(metricCli, 0)
}

func emitEtcdMVCCWatchEventCounter(metricCli metrics.Metrics, value int) {
	if metricCli == nil {
		return
	}
	_ = metricCli.EmitCounter("etcd_debugging.mvcc.events_total", value)
}

func (b *backendShim) addEtcdMVCCPendingWatchEvents(delta int) {
	if b == nil || delta == 0 {
		return
	}
	total := b.mvccPendingWatchEvents.Add(int64(delta))
	emitEtcdMVCCWatchPendingEventGauge(b.metricCli, total)
}

func emitEtcdMVCCWatchPendingEventGauge(metricCli metrics.Metrics, value int64) {
	if metricCli == nil {
		return
	}
	_ = metricCli.EmitGauge("etcd_debugging.mvcc.pending_events_total", value)
}

// RefreshWatchMetrics emits etcd-compatible watch stream gauges from local
// gRPC stream state. KubeBrain tracks individual backend watchers separately;
// this gauge is only the number of active Watch RPC streams on this member.
func (s *RPCServer) RefreshWatchMetrics() {
	if s == nil || s.metricCli == nil {
		return
	}
	s.metricCli.EmitGauge("etcd_debugging.mvcc.watch_stream_total", s.activeWatchStreams.Load())
}
