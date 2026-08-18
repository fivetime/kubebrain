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
	"go.etcd.io/etcd/api/v3/etcdserverpb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/kubewharf/kubebrain/pkg/metrics"
)

const clusterProxyRPCMemberList = "member_list"

func initClusterProxyIntegrityMetrics(metricCli metrics.Metrics) {
	if metricCli != nil {
		_ = metricCli.EmitCounter("cluster.proxy.integrity_failure", int64(0), metrics.Tag("rpc", clusterProxyRPCMemberList))
	}
}

func validateClusterProxyResult[T any](metricCli metrics.Metrics, rpc string, response *T, err error) (*T, error) {
	if (response == nil) != (err == nil) {
		if response != nil {
			headerResponse, ok := any(response).(interface {
				GetHeader() *etcdserverpb.ResponseHeader
			})
			if !ok || headerResponse.GetHeader() == nil {
				emitClusterProxyIntegrityFailure(metricCli, rpc)
				return nil, status.Error(codes.DataLoss, "leader member_list proxy returned a response without a header")
			}
			if headerResponse.GetHeader().GetRevision() < 0 {
				emitClusterProxyIntegrityFailure(metricCli, rpc)
				return nil, status.Error(codes.DataLoss, "leader member_list proxy returned a response with a negative header revision")
			}
		}
		return response, err
	}
	emitClusterProxyIntegrityFailure(metricCli, rpc)
	if response == nil {
		return nil, status.Error(codes.DataLoss, "leader member_list proxy returned neither response nor error")
	}
	return nil, status.Error(codes.DataLoss, "leader member_list proxy returned both response and error")
}

func validateMemberListProxyPayload(metricCli metrics.Metrics, response *etcdserverpb.MemberListResponse, err error) (*etcdserverpb.MemberListResponse, error) {
	if err != nil {
		return response, err
	}
	var previousID uint64
	for i, member := range response.GetMembers() {
		if member == nil {
			emitClusterProxyIntegrityFailure(metricCli, clusterProxyRPCMemberList)
			return nil, status.Error(codes.DataLoss, "leader member_list proxy returned a nil member")
		}
		if i > 0 && member.GetID() <= previousID {
			emitClusterProxyIntegrityFailure(metricCli, clusterProxyRPCMemberList)
			return nil, status.Error(codes.DataLoss, "leader member_list proxy returned members outside strict ID order")
		}
		previousID = member.GetID()
	}
	return response, nil
}

func emitClusterProxyIntegrityFailure(metricCli metrics.Metrics, rpc string) {
	if metricCli != nil {
		_ = metricCli.EmitCounter("cluster.proxy.integrity_failure", 1, metrics.Tag("rpc", rpc))
	}
}
