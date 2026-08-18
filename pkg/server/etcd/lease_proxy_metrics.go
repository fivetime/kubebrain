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
	"fmt"

	"go.etcd.io/etcd/api/v3/etcdserverpb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/kubewharf/kubebrain/pkg/metrics"
)

const (
	leaseProxyRPCGrant      = "grant"
	leaseProxyRPCRevoke     = "revoke"
	leaseProxyRPCKeepAlive  = "keep_alive"
	leaseProxyRPCTimeToLive = "time_to_live"
	leaseProxyRPCLeases     = "leases"
)

var leaseProxyRPCs = []string{
	leaseProxyRPCGrant,
	leaseProxyRPCRevoke,
	leaseProxyRPCKeepAlive,
	leaseProxyRPCTimeToLive,
	leaseProxyRPCLeases,
}

func initLeaseProxyIntegrityMetrics(metricCli metrics.Metrics) {
	if metricCli == nil {
		return
	}
	for _, rpc := range leaseProxyRPCs {
		_ = metricCli.EmitCounter("lease.proxy.integrity_failure", int64(0), metrics.Tag("rpc", rpc))
	}
}

func validateLeaseProxyResult[T any](metricCli metrics.Metrics, rpc string, response *T, err error) (*T, error) {
	if (response == nil) != (err == nil) {
		if response != nil {
			headerResponse, ok := any(response).(interface {
				GetHeader() *etcdserverpb.ResponseHeader
			})
			if !ok || headerResponse.GetHeader() == nil {
				emitLeaseProxyIntegrityFailure(metricCli, rpc)
				return nil, status.Error(codes.DataLoss, fmt.Sprintf("leader lease %s proxy returned a response without a header", rpc))
			}
		}
		return response, err
	}
	emitLeaseProxyIntegrityFailure(metricCli, rpc)
	if response == nil {
		return nil, status.Error(codes.DataLoss, fmt.Sprintf("leader lease %s proxy returned neither response nor error", rpc))
	}
	return nil, status.Error(codes.DataLoss, fmt.Sprintf("leader lease %s proxy returned both response and error", rpc))
}

func emitLeaseProxyIntegrityFailure(metricCli metrics.Metrics, rpc string) {
	if metricCli != nil {
		_ = metricCli.EmitCounter("lease.proxy.integrity_failure", 1, metrics.Tag("rpc", rpc))
	}
}
