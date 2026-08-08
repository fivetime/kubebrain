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

	"google.golang.org/grpc/stats"

	"github.com/kubewharf/kubebrain/pkg/metrics"
)

func initEtcdClientGRPCBytesCounters(metricCli metrics.Metrics) {
	emitEtcdClientGRPCReceivedBytesCounter(metricCli, 0)
	emitEtcdClientGRPCSentBytesCounter(metricCli, 0)
}

func emitEtcdClientGRPCReceivedBytesCounter(metricCli metrics.Metrics, value int) {
	if metricCli == nil {
		return
	}
	_ = metricCli.EmitCounter("etcd.network.client_grpc_received_bytes_total", value)
}

func emitEtcdClientGRPCSentBytesCounter(metricCli metrics.Metrics, value int) {
	if metricCli == nil {
		return
	}
	_ = metricCli.EmitCounter("etcd.network.client_grpc_sent_bytes_total", value)
}

type etcdClientGRPCBytesStatsHandler struct {
	metricCli metrics.Metrics
}

func newEtcdClientGRPCBytesStatsHandler(metricCli metrics.Metrics) stats.Handler {
	return etcdClientGRPCBytesStatsHandler{metricCli: metricCli}
}

func (h etcdClientGRPCBytesStatsHandler) TagRPC(ctx context.Context, _ *stats.RPCTagInfo) context.Context {
	return ctx
}

func (h etcdClientGRPCBytesStatsHandler) HandleRPC(_ context.Context, stat stats.RPCStats) {
	switch s := stat.(type) {
	case *stats.InPayload:
		emitEtcdClientGRPCReceivedBytesCounter(h.metricCli, s.Length)
	case *stats.OutPayload:
		emitEtcdClientGRPCSentBytesCounter(h.metricCli, s.Length)
	}
}

func (h etcdClientGRPCBytesStatsHandler) TagConn(ctx context.Context, _ *stats.ConnTagInfo) context.Context {
	return ctx
}

func (h etcdClientGRPCBytesStatsHandler) HandleConn(context.Context, stats.ConnStats) {}
