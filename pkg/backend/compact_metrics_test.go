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
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"

	"github.com/kubewharf/kubebrain/pkg/metrics"
)

type compactMetricRecord struct {
	kind  string
	name  string
	value interface{}
}

type compactMetricRecorder struct {
	mu      sync.Mutex
	records []compactMetricRecord
}

func (r *compactMetricRecorder) GetGrpcServerOption() []grpc.ServerOption { return nil }
func (r *compactMetricRecorder) GetHttpHandlers() map[string]http.Handler { return nil }

func (r *compactMetricRecorder) EmitCounter(name string, value interface{}, _ ...metrics.T) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.records = append(r.records, compactMetricRecord{kind: "counter", name: name, value: value})
	return nil
}
func (r *compactMetricRecorder) EmitGauge(name string, value interface{}, _ ...metrics.T) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.records = append(r.records, compactMetricRecord{kind: "gauge", name: name, value: value})
	return nil
}
func (r *compactMetricRecorder) EmitHistogram(name string, value interface{}, _ ...metrics.T) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.records = append(r.records, compactMetricRecord{kind: "histogram", name: name, value: value})
	return nil
}

func TestEtcdMVCCCompactionMetricsUseUpstreamNames(t *testing.T) {
	rec := &compactMetricRecorder{}

	initEtcdMVCCCompactionMetrics(rec)
	emitEtcdMVCCDBCompactionTotalDuration(rec, 1500*time.Millisecond)
	emitEtcdMVCCDBCompactionLast(rec, 123)
	emitEtcdMVCCIndexCompactionPause(rec, 250*time.Millisecond)

	require.Equal(t, []compactMetricRecord{
		{kind: "gauge", name: "etcd_debugging.mvcc.db_compaction_last", value: int64(0)},
		{kind: "counter", name: "etcd_debugging.mvcc.db_compaction_keys_total", value: int64(0)},
		{kind: "histogram", name: "etcd_debugging.mvcc.db_compaction_total_duration_milliseconds", value: 1500.0},
		{kind: "gauge", name: "etcd_debugging.mvcc.db_compaction_last", value: int64(123)},
		{kind: "histogram", name: "etcd_debugging.mvcc.index_compaction_pause_duration_milliseconds", value: 250.0},
	}, rec.records)
}
