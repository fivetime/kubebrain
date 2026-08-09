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

var etcdBackendBboltCommitPhaseMetrics = []string{
	"etcd_debugging.disk.backend_commit_rebalance_duration_seconds",
	"etcd_debugging.disk.backend_commit_spill_duration_seconds",
	"etcd_debugging.disk.backend_commit_write_duration_seconds",
}

func initEtcdBackendBboltCommitPhaseMetrics(metricCli metrics.Metrics) {
	registrar, ok := metricCli.(metrics.HistogramRegistrar)
	if !ok {
		return
	}
	// These are process-local bbolt commit phases. KubeBrain reports the
	// equivalent TiKV/Badger atomic commit through etcd_disk_backend_commit,
	// but must not attribute a distributed transaction to bbolt internals that
	// never execute in this process.
	for _, name := range etcdBackendBboltCommitPhaseMetrics {
		_ = registrar.RegisterHistogram(name)
	}
}
