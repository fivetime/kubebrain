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

func initEtcdMVCCOperationCounters(metricCli metrics.Metrics) {
	emitEtcdMVCCRangeCounter(metricCli, 0)
	emitEtcdMVCCPutCounter(metricCli, 0)
	emitEtcdMVCCDeleteCounter(metricCli, 0)
	emitEtcdMVCCTxnCounter(metricCli, 0)
}

func emitEtcdMVCCRangeCounter(metricCli metrics.Metrics, value int) {
	_ = metricCli.EmitCounter("etcd.mvcc.range_total", value)
}

func emitEtcdMVCCPutCounter(metricCli metrics.Metrics, value int) {
	_ = metricCli.EmitCounter("etcd.mvcc.put_total", value)
}

func emitEtcdMVCCDeleteCounter(metricCli metrics.Metrics, value int) {
	_ = metricCli.EmitCounter("etcd.mvcc.delete_total", value)
}

func emitEtcdMVCCTxnCounter(metricCli metrics.Metrics, value int) {
	_ = metricCli.EmitCounter("etcd.mvcc.txn_total", value)
}
