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
			if headerResponse.GetHeader().GetRevision() < 0 {
				emitLeaseProxyIntegrityFailure(metricCli, rpc)
				return nil, status.Error(codes.DataLoss, fmt.Sprintf("leader lease %s proxy returned a response with a negative header revision", rpc))
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

func validateLeaseProxyResponseID[T interface{ GetID() int64 }](metricCli metrics.Metrics, rpc string, expectedID int64, response T, err error) (T, error) {
	if err != nil {
		return response, err
	}
	if response.GetID() == expectedID {
		return response, nil
	}
	emitLeaseProxyIntegrityFailure(metricCli, rpc)
	var zero T
	return zero, status.Errorf(codes.DataLoss, "leader lease %s proxy returned lease ID %d for request ID %d", rpc, response.GetID(), expectedID)
}

func validateLeaseGrantProxyPayload(metricCli metrics.Metrics, request *etcdserverpb.LeaseGrantRequest, response *etcdserverpb.LeaseGrantResponse, err error) (*etcdserverpb.LeaseGrantResponse, error) {
	if err != nil {
		return response, err
	}
	if response.GetError() != "" {
		emitLeaseProxyIntegrityFailure(metricCli, leaseProxyRPCGrant)
		return nil, status.Error(codes.DataLoss, "leader lease grant proxy returned success with a legacy error")
	}
	if response.GetTTL() <= 0 {
		emitLeaseProxyIntegrityFailure(metricCli, leaseProxyRPCGrant)
		return nil, status.Errorf(codes.DataLoss, "leader lease grant proxy returned non-positive granted TTL %d", response.GetTTL())
	}
	if response.GetTTL() > maxLeaseTTL {
		emitLeaseProxyIntegrityFailure(metricCli, leaseProxyRPCGrant)
		return nil, status.Errorf(codes.DataLoss, "leader lease grant proxy returned granted TTL %d above maximum %d", response.GetTTL(), maxLeaseTTL)
	}
	if response.GetTTL() < request.GetTTL() {
		emitLeaseProxyIntegrityFailure(metricCli, leaseProxyRPCGrant)
		return nil, status.Errorf(codes.DataLoss, "leader lease grant proxy returned granted TTL %d below requested TTL %d", response.GetTTL(), request.GetTTL())
	}
	return response, nil
}

func validateLeaseKeepAliveProxyPayload(metricCli metrics.Metrics, response *etcdserverpb.LeaseKeepAliveResponse, err error) (*etcdserverpb.LeaseKeepAliveResponse, error) {
	if err != nil {
		return response, err
	}
	if response.GetTTL() >= 0 {
		return response, nil
	}
	emitLeaseProxyIntegrityFailure(metricCli, leaseProxyRPCKeepAlive)
	return nil, status.Errorf(codes.DataLoss, "leader lease keep_alive proxy returned negative TTL %d", response.GetTTL())
}

func validateLeaseTimeToLiveProxyPayload(metricCli metrics.Metrics, keysRequested bool, response *etcdserverpb.LeaseTimeToLiveResponse, err error) (*etcdserverpb.LeaseTimeToLiveResponse, error) {
	if err != nil {
		return response, err
	}
	if !keysRequested && len(response.GetKeys()) != 0 {
		emitLeaseProxyIntegrityFailure(metricCli, leaseProxyRPCTimeToLive)
		return nil, status.Error(codes.DataLoss, "leader lease time_to_live proxy returned keys when none were requested")
	}
	if response.GetTTL() == -1 && response.GetGrantedTTL() == 0 {
		if len(response.GetKeys()) == 0 {
			return response, nil
		}
		emitLeaseProxyIntegrityFailure(metricCli, leaseProxyRPCTimeToLive)
		return nil, status.Errorf(codes.DataLoss, "leader lease time_to_live proxy returned malformed not-found payload with granted TTL %d and %d keys", response.GetGrantedTTL(), len(response.GetKeys()))
	}
	if response.GetGrantedTTL() > maxLeaseTTL {
		emitLeaseProxyIntegrityFailure(metricCli, leaseProxyRPCTimeToLive)
		return nil, status.Errorf(codes.DataLoss, "leader lease time_to_live proxy returned granted TTL %d above maximum %d", response.GetGrantedTTL(), maxLeaseTTL)
	}
	if response.GetGrantedTTL() > 0 {
		seen := make(map[string]struct{}, len(response.GetKeys()))
		for _, key := range response.GetKeys() {
			if len(key) == 0 {
				emitLeaseProxyIntegrityFailure(metricCli, leaseProxyRPCTimeToLive)
				return nil, status.Error(codes.DataLoss, "leader lease time_to_live proxy returned an empty attached key")
			}
			if _, exists := seen[string(key)]; exists {
				emitLeaseProxyIntegrityFailure(metricCli, leaseProxyRPCTimeToLive)
				return nil, status.Error(codes.DataLoss, "leader lease time_to_live proxy returned a duplicate attached key")
			}
			seen[string(key)] = struct{}{}
		}
		return response, nil
	}
	emitLeaseProxyIntegrityFailure(metricCli, leaseProxyRPCTimeToLive)
	return nil, status.Errorf(codes.DataLoss, "leader lease time_to_live proxy returned invalid TTL %d and granted TTL %d", response.GetTTL(), response.GetGrantedTTL())
}

func validateLeaseLeasesProxyPayload(metricCli metrics.Metrics, response *etcdserverpb.LeaseLeasesResponse, err error) (*etcdserverpb.LeaseLeasesResponse, error) {
	if err != nil {
		return response, err
	}
	seen := make(map[int64]struct{}, len(response.GetLeases()))
	for _, lease := range response.GetLeases() {
		if lease == nil {
			emitLeaseProxyIntegrityFailure(metricCli, leaseProxyRPCLeases)
			return nil, status.Error(codes.DataLoss, "leader lease leases proxy returned a nil lease status")
		}
		if lease.GetID() == 0 {
			emitLeaseProxyIntegrityFailure(metricCli, leaseProxyRPCLeases)
			return nil, status.Error(codes.DataLoss, "leader lease leases proxy returned the reserved zero lease ID")
		}
		if _, exists := seen[lease.GetID()]; exists {
			emitLeaseProxyIntegrityFailure(metricCli, leaseProxyRPCLeases)
			return nil, status.Errorf(codes.DataLoss, "leader lease leases proxy returned duplicate lease ID %d", lease.GetID())
		}
		seen[lease.GetID()] = struct{}{}
	}
	return response, nil
}

func emitLeaseProxyIntegrityFailure(metricCli metrics.Metrics, rpc string) {
	if metricCli != nil {
		_ = metricCli.EmitCounter("lease.proxy.integrity_failure", 1, metrics.Tag("rpc", rpc))
	}
}
