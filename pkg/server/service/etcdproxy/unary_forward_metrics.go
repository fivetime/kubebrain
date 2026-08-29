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

package etcdproxy

import (
	"context"
	"time"

	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/kubewharf/kubebrain/pkg/metrics"
	"github.com/kubewharf/kubebrain/pkg/server/proxyprotocol"
)

const (
	unaryForwardDurationMetric = "peer.proxy.unary.duration_seconds"
	unaryForwardRetryMetric    = "peer.proxy.unary.retry"
)

const (
	unaryForwardRPCRange       = "range"
	unaryForwardRPCTxn         = "txn"
	unaryForwardRPCPut         = "put"
	unaryForwardRPCDeleteRange = "delete_range"
)

var unaryForwardRPCs = []string{
	unaryForwardRPCRange,
	unaryForwardRPCTxn,
	unaryForwardRPCPut,
	unaryForwardRPCDeleteRange,
}

const (
	unaryForwardStageWaitReady = "wait_ready"
	unaryForwardStageForward   = "forward"
)

var unaryForwardStages = []string{
	unaryForwardStageWaitReady,
	unaryForwardStageForward,
}

const (
	unaryForwardOutcomeSuccess     = "success"
	unaryForwardOutcomeCaller      = "caller"
	unaryForwardOutcomeDrained     = "drained"
	unaryForwardOutcomeTopology    = "topology"
	unaryForwardOutcomeTransport   = "transport"
	unaryForwardOutcomeApplication = "application"
)

var unaryForwardOutcomes = []string{
	unaryForwardOutcomeSuccess,
	unaryForwardOutcomeCaller,
	unaryForwardOutcomeDrained,
	unaryForwardOutcomeTopology,
	unaryForwardOutcomeTransport,
	unaryForwardOutcomeApplication,
}

const unaryForwardRetryDrain = "drain"

func initUnaryForwardMetrics(metricCli metrics.Metrics) {
	if metricCli == nil {
		return
	}
	if registrar, ok := metricCli.(metrics.HistogramRegistrar); ok {
		for _, rpc := range unaryForwardRPCs {
			for _, stage := range unaryForwardStages {
				for _, outcome := range unaryForwardOutcomes {
					_ = registrar.RegisterHistogram(unaryForwardDurationMetric,
						metrics.Tag("rpc", rpc), metrics.Tag("stage", stage), metrics.Tag("outcome", outcome))
				}
			}
		}
	}
	for _, rpc := range unaryForwardRPCs {
		_ = metricCli.EmitCounter(unaryForwardRetryMetric, int64(0),
			metrics.Tag("rpc", rpc), metrics.Tag("reason", unaryForwardRetryDrain))
	}
}

func emitUnaryForwardDuration(metricCli metrics.Metrics, rpc, stage string, started time.Time,
	ctx context.Context, client *clientv3.Client, err error,
) {
	if metricCli == nil {
		return
	}
	_ = metricCli.EmitHistogram(unaryForwardDurationMetric, time.Since(started).Seconds(),
		metrics.Tag("rpc", rpc), metrics.Tag("stage", stage), metrics.Tag("outcome", classifyUnaryForwardOutcome(ctx, client, err)))
}

func emitUnaryForwardDrainRetry(metricCli metrics.Metrics, rpc string) {
	if metricCli == nil {
		return
	}
	_ = metricCli.EmitCounter(unaryForwardRetryMetric, int64(1),
		metrics.Tag("rpc", rpc), metrics.Tag("reason", unaryForwardRetryDrain))
}

func classifyUnaryForwardOutcome(ctx context.Context, client *clientv3.Client, err error) string {
	if err == nil {
		return unaryForwardOutcomeSuccess
	}
	if ctx != nil && ctx.Err() != nil {
		return unaryForwardOutcomeCaller
	}
	if proxyprotocol.IsPeerDrainedBeforeAdmission(err) {
		return unaryForwardOutcomeDrained
	}
	for _, topologyErr := range []error{
		rpctypes.ErrGRPCNoLeader,
		rpctypes.ErrGRPCNotLeader,
		rpctypes.ErrGRPCLeaderChanged,
		rpctypes.ErrGRPCStopped,
	} {
		if sameGRPCStatus(err, topologyErr) {
			return unaryForwardOutcomeTopology
		}
	}
	if shouldResetForwardClient(client, err) {
		return unaryForwardOutcomeTransport
	}
	return unaryForwardOutcomeApplication
}
