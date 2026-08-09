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

package backend

import (
	"time"

	"github.com/kubewharf/kubebrain/pkg/metrics"
)

func initEtcdMVCCCompactionMetrics(metricCli metrics.Metrics) {
	emitEtcdMVCCDBCompactionLast(metricCli, 0)
	emitEtcdMVCCDBCompactionKeys(metricCli, 0)
}

func emitEtcdMVCCDBCompactionTotalDuration(metricCli metrics.Metrics, duration time.Duration) {
	if metricCli == nil {
		return
	}
	_ = metricCli.EmitHistogram(
		"etcd_debugging.mvcc.db_compaction_total_duration_milliseconds",
		float64(duration)/float64(time.Millisecond),
	)
}

func emitEtcdMVCCDBCompactionLast(metricCli metrics.Metrics, unixSeconds int64) {
	if metricCli == nil {
		return
	}
	_ = metricCli.EmitGauge("etcd_debugging.mvcc.db_compaction_last", unixSeconds)
}

func emitEtcdMVCCIndexCompactionPause(metricCli metrics.Metrics, duration time.Duration) {
	if metricCli == nil {
		return
	}
	_ = metricCli.EmitHistogram(
		"etcd_debugging.mvcc.index_compaction_pause_duration_milliseconds",
		float64(duration)/float64(time.Millisecond),
	)
}

func emitEtcdMVCCDBCompactionKeys(metricCli metrics.Metrics, count int64) {
	if metricCli == nil {
		return
	}
	_ = metricCli.EmitCounter("etcd_debugging.mvcc.db_compaction_keys_total", count)
}
