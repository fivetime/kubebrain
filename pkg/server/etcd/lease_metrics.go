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

func initEtcdLeaseLifecycleMetrics(metricCli metrics.Metrics) {
	emitEtcdLeaseGrantedCounter(metricCli, 0)
	emitEtcdLeaseRevokedCounter(metricCli, 0)
	emitEtcdLeaseRenewedCounter(metricCli, 0)
}

func initEtcdLeaseExpiredCounter(metricCli metrics.Metrics) {
	emitEtcdLeaseExpiredCounter(metricCli, 0)
}

func emitEtcdLeaseGrantedCounter(metricCli metrics.Metrics, value int) {
	if metricCli == nil {
		return
	}
	_ = metricCli.EmitCounter("etcd_debugging.lease.granted_total", value)
}

func emitEtcdLeaseRevokedCounter(metricCli metrics.Metrics, value int) {
	if metricCli == nil {
		return
	}
	_ = metricCli.EmitCounter("etcd_debugging.lease.revoked_total", value)
}

func emitEtcdLeaseRenewedCounter(metricCli metrics.Metrics, value int) {
	if metricCli == nil {
		return
	}
	_ = metricCli.EmitCounter("etcd_debugging.lease.renewed_total", value)
}

func emitEtcdLeaseTTLHistogram(metricCli metrics.Metrics, ttl int64) {
	if metricCli == nil {
		return
	}
	_ = metricCli.EmitHistogram("etcd_debugging.lease.ttl_total", ttl)
}

func emitEtcdLeaseExpiredCounter(metricCli metrics.Metrics, value int) {
	if metricCli == nil {
		return
	}
	_ = metricCli.EmitCounter("etcd_debugging.server.lease_expired_total", value)
}
