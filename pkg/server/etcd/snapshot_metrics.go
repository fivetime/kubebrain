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
	"time"

	"github.com/kubewharf/kubebrain/pkg/metrics"
)

const etcdBackendSnapshotDurationMetric = "etcd.disk.backend_snapshot_duration_seconds"

const (
	snapshotActiveMetric            = "maintenance.snapshot.active"
	snapshotAdmissionRejectedMetric = "maintenance.snapshot.admission_rejected"
)

const (
	snapshotFailureSource   = "source"
	snapshotFailureProxy    = "proxy"
	snapshotFailureSend     = "send"
	snapshotFailureProtocol = "protocol"
)

var snapshotFailureStages = []string{
	snapshotFailureSource,
	snapshotFailureProxy,
	snapshotFailureSend,
	snapshotFailureProtocol,
}

func initEtcdBackendSnapshotDuration(metricCli metrics.Metrics) {
	if registrar, ok := metricCli.(metrics.HistogramRegistrar); ok {
		_ = registrar.RegisterHistogram(etcdBackendSnapshotDurationMetric)
	}
}

func emitEtcdBackendSnapshotDuration(metricCli metrics.Metrics, duration time.Duration) {
	if metricCli == nil {
		return
	}
	_ = metricCli.EmitHistogram(etcdBackendSnapshotDurationMetric, duration.Seconds())
}

func initSnapshotFailureMetrics(metricCli metrics.Metrics) {
	if metricCli == nil {
		return
	}
	for _, stage := range snapshotFailureStages {
		_ = metricCli.EmitCounter("maintenance.snapshot.failure", int64(0), metrics.Tag("stage", stage))
	}
	_ = metricCli.EmitCounter("maintenance.snapshot.proxy_retry", int64(0))
	_ = metricCli.EmitCounter(snapshotAdmissionRejectedMetric, int64(0))
	_ = metricCli.EmitGauge(snapshotActiveMetric, int64(0))
}

func emitSnapshotFailure(metricCli metrics.Metrics, stage string) {
	if metricCli == nil {
		return
	}
	_ = metricCli.EmitCounter("maintenance.snapshot.failure", 1, metrics.Tag("stage", stage))
}

func emitSnapshotAdmissionRejected(metricCli metrics.Metrics) {
	if metricCli == nil {
		return
	}
	_ = metricCli.EmitCounter(snapshotAdmissionRejectedMetric, 1)
}

func emitSnapshotActive(metricCli metrics.Metrics, active bool) {
	if metricCli == nil {
		return
	}
	value := int64(0)
	if active {
		value = 1
	}
	_ = metricCli.EmitGauge(snapshotActiveMetric, value)
}
