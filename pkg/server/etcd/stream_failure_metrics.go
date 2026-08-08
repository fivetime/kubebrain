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
	"errors"
	"strings"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/kubewharf/kubebrain/pkg/metrics"
)

func initEtcdServerStreamFailureCounters(metricCli metrics.Metrics) {
	for _, api := range []string{"watch", "lease-keepalive"} {
		emitEtcdServerStreamFailureCounter(metricCli, "receive", api, 0)
		emitEtcdServerStreamFailureCounter(metricCli, "send", api, 0)
	}
}

func emitEtcdServerStreamFailureCounter(metricCli metrics.Metrics, failureType, api string, value int) {
	if metricCli == nil {
		return
	}
	_ = metricCli.EmitCounter("etcd.network.server_stream_failures_total", value,
		metrics.Tag("Type", failureType),
		metrics.Tag("API", api),
	)
}

func shouldCountServerStreamFailure(ctx context.Context, err error) bool {
	if err == nil {
		return false
	}
	if ctx != nil && ctx.Err() != nil {
		return false
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return false
	}
	ev, ok := status.FromError(err)
	if !ok {
		return true
	}
	switch ev.Code() {
	case codes.Canceled, codes.DeadlineExceeded:
		return false
	case codes.Unavailable:
		msg := ev.Message()
		if msg == "client disconnected" {
			return false
		}
		if strings.HasPrefix(msg, "stream error: ") && strings.HasSuffix(msg, "; CANCEL") {
			return false
		}
	}
	return true
}
