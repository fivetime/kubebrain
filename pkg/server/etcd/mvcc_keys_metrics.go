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
	"context"

	"github.com/kubewharf/kubebrain/pkg/metrics"
)

var allKeysRangeEnd = []byte{0}

func initEtcdMVCCKeysGauge(metricCli metrics.Metrics) {
	emitEtcdMVCCKeysGauge(metricCli, 0)
}

func emitEtcdMVCCKeysGauge(metricCli metrics.Metrics, value int64) {
	if metricCli == nil {
		return
	}
	_ = metricCli.EmitGauge("etcd_debugging.mvcc.keys_total", value)
}

// RefreshMVCCKeysMetric emits the upstream-compatible live MVCC key gauge from
// the exact count index only. It deliberately does not fall back to a full scan:
// a metric refresh must not become a periodic O(keyspace) production workload.
func (s *RPCServer) RefreshMVCCKeysMetric(ctx context.Context) bool {
	if s == nil || s.backend == nil {
		return false
	}
	count, served := s.backend.CountAtRevision(ctx, nil, allKeysRangeEnd, 0)
	if !served {
		if s.metricCli != nil {
			_ = s.metricCli.EmitCounter("mvcc.keys_total.refresh.miss", 1)
		}
		return false
	}
	emitEtcdMVCCKeysGauge(s.metricCli, count)
	return true
}
