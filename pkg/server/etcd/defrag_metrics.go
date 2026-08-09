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

import "github.com/kubewharf/kubebrain/pkg/metrics"

const (
	etcdBackendDefragDurationMetric = "etcd.disk.backend_defrag_duration_seconds"
	etcdDefragInflightMetric        = "etcd.disk.defrag_inflight"
)

func initEtcdBackendDefragMetrics(metricCli metrics.Metrics) {
	if metricCli == nil {
		return
	}
	if registrar, ok := metricCli.(metrics.HistogramRegistrar); ok {
		_ = registrar.RegisterHistogram(etcdBackendDefragDurationMetric)
	}
	// TiKV/PD own physical compaction. This process never runs an embedded
	// bbolt defrag, including when the compatibility RPC returns its no-op
	// success response, so its exact process-local inflight state is zero.
	_ = metricCli.EmitGauge(etcdDefragInflightMetric, 0)
}
