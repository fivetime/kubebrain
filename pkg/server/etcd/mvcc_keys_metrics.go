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
	"time"

	"github.com/kubewharf/kubebrain/pkg/metrics"
)

// etcd encodes the complete user-key range as [0x00, 0x00). The start must
// not be nil: a follower whose local count index is unavailable forwards this
// count as a KV.Range to the leader, and the etcd API rejects an empty key.
var allKeysRangeBoundary = []byte{0}

func initEtcdMVCCKeysGauge(metricCli metrics.Metrics) {
	emitEtcdMVCCKeysGauge(metricCli, 0)
	if metricCli == nil {
		return
	}
	_ = metricCli.EmitCounter("mvcc.keys_total.refresh.miss", int64(0))
	_ = metricCli.EmitGauge("mvcc.keys_total.refresh.last_success_timestamp_seconds", int64(0))
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
	count, served := s.backend.CountAtRevision(ctx, allKeysRangeBoundary, allKeysRangeBoundary, 0)
	if !served {
		if s.metricCli != nil {
			_ = s.metricCli.EmitCounter("mvcc.keys_total.refresh.miss", 1)
		}
		return false
	}
	emitEtcdMVCCKeysGauge(s.metricCli, count)
	if s.metricCli != nil {
		_ = s.metricCli.EmitGauge("mvcc.keys_total.refresh.last_success_timestamp_seconds", time.Now().Unix())
	}
	return true
}
