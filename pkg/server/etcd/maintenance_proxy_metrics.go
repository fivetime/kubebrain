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
	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/kubewharf/kubebrain/pkg/metrics"
)

const (
	maintenanceProxyRPCAlarm      = "alarm"
	maintenanceProxyRPCDefragment = "defragment"
	maintenanceProxyRPCStatus     = "status"
	maintenanceProxyRPCHash       = "hash"
	maintenanceProxyRPCHashKV     = "hash_kv"
	maintenanceProxyRPCDowngrade  = "downgrade"
)

var maintenanceProxyRPCs = []string{
	maintenanceProxyRPCAlarm,
	maintenanceProxyRPCDefragment,
	maintenanceProxyRPCStatus,
	maintenanceProxyRPCHash,
	maintenanceProxyRPCHashKV,
	maintenanceProxyRPCDowngrade,
}

func initMaintenanceProxyIntegrityMetrics(metricCli metrics.Metrics) {
	if metricCli == nil {
		return
	}
	for _, rpc := range maintenanceProxyRPCs {
		_ = metricCli.EmitCounter("maintenance.proxy.integrity_failure", int64(0), metrics.Tag("rpc", rpc))
	}
}

func validateMaintenanceProxyResult[T any](
	metricCli metrics.Metrics, rpc string, response *T, err error,
) (*T, error) {
	if (response == nil) != (err == nil) {
		if response != nil && rpc != maintenanceProxyRPCDefragment {
			headerResponse, ok := any(response).(interface {
				GetHeader() *etcdserverpb.ResponseHeader
			})
			if !ok || headerResponse.GetHeader() == nil {
				emitMaintenanceProxyIntegrityFailure(metricCli, rpc)
				return nil, status.Error(codes.DataLoss, fmt.Sprintf("leader %s proxy returned a response without a header", rpc))
			}
			if headerResponse.GetHeader().GetRevision() < 0 {
				emitMaintenanceProxyIntegrityFailure(metricCli, rpc)
				return nil, status.Error(codes.DataLoss, fmt.Sprintf("leader %s proxy returned a response with a negative header revision", rpc))
			}
		}
		return response, err
	}
	emitMaintenanceProxyIntegrityFailure(metricCli, rpc)
	if response == nil {
		return nil, status.Error(codes.DataLoss, fmt.Sprintf("leader %s proxy returned neither response nor error", rpc))
	}
	return nil, status.Error(codes.DataLoss, fmt.Sprintf("leader %s proxy returned both response and error", rpc))
}

func validateAlarmProxyPayload(metricCli metrics.Metrics, request *etcdserverpb.AlarmRequest, response *etcdserverpb.AlarmResponse, err error) (*etcdserverpb.AlarmResponse, error) {
	if err != nil {
		return response, err
	}
	fail := func(message string) (*etcdserverpb.AlarmResponse, error) {
		emitMaintenanceProxyIntegrityFailure(metricCli, maintenanceProxyRPCAlarm)
		return nil, status.Error(codes.DataLoss, message)
	}
	alarms := response.GetAlarms()
	matchRequest := func(alarm *etcdserverpb.AlarmMember, allowResolvedDefaultOwner bool) bool {
		return alarm.GetAlarm() == request.GetAlarm() &&
			(alarm.GetMemberID() == request.GetMemberID() || (allowResolvedDefaultOwner && request.GetMemberID() == 0))
	}
	switch request.GetAction() {
	case etcdserverpb.AlarmRequest_GET:
		seen := make(map[struct {
			memberID uint64
			alarm    etcdserverpb.AlarmType
		}]struct{}, len(alarms))
		for _, alarm := range alarms {
			if alarm == nil || alarm.GetAlarm() == etcdserverpb.AlarmType_NONE {
				return fail("leader alarm proxy returned a nil or NONE alarm")
			}
			if request.GetAlarm() != etcdserverpb.AlarmType_NONE && alarm.GetAlarm() != request.GetAlarm() {
				return fail("leader alarm proxy returned an alarm outside the requested filter")
			}
			identity := struct {
				memberID uint64
				alarm    etcdserverpb.AlarmType
			}{memberID: alarm.GetMemberID(), alarm: alarm.GetAlarm()}
			if _, exists := seen[identity]; exists {
				return fail("leader alarm proxy returned a duplicate alarm")
			}
			seen[identity] = struct{}{}
		}
	case etcdserverpb.AlarmRequest_ACTIVATE:
		if request.GetAlarm() == etcdserverpb.AlarmType_NONE {
			if len(alarms) != 0 {
				return fail("leader alarm proxy returned alarms for a NONE activation")
			}
			return response, nil
		}
		if len(alarms) != 1 || alarms[0] == nil || !matchRequest(alarms[0], true) {
			return fail("leader alarm proxy returned an activation result inconsistent with the request")
		}
	case etcdserverpb.AlarmRequest_DEACTIVATE:
		if request.GetAlarm() == etcdserverpb.AlarmType_NONE {
			if len(alarms) != 0 {
				return fail("leader alarm proxy returned alarms for a NONE deactivation")
			}
			return response, nil
		}
		if len(alarms) > 1 || (len(alarms) == 1 && (alarms[0] == nil || !matchRequest(alarms[0], false))) {
			return fail("leader alarm proxy returned a deactivation result inconsistent with the request")
		}
	default:
		return fail("leader alarm proxy returned success for an unknown action")
	}
	return response, nil
}

func validateStatusProxyPayload(metricCli metrics.Metrics, response *etcdserverpb.StatusResponse, err error) (*etcdserverpb.StatusResponse, error) {
	if err != nil {
		return response, err
	}
	fail := func(message string) (*etcdserverpb.StatusResponse, error) {
		emitMaintenanceProxyIntegrityFailure(metricCli, maintenanceProxyRPCStatus)
		return nil, status.Error(codes.DataLoss, message)
	}
	if response.GetVersion() == "" {
		return fail("leader status proxy returned an empty version")
	}
	if response.GetDbSize() < 0 || response.GetDbSizeInUse() < 0 || response.GetDbSizeInUse() > response.GetDbSize() {
		return fail("leader status proxy returned invalid database sizes")
	}
	if response.GetDbSizeQuota() <= 0 {
		return fail("leader status proxy returned a non-positive database quota")
	}
	if response.GetRaftAppliedIndex() > response.GetRaftIndex() {
		return fail("leader status proxy returned an applied index above its committed index")
	}
	if response.GetDowngradeInfo() == nil {
		return fail("leader status proxy returned no downgrade information")
	}
	noLeader := false
	for _, statusErr := range response.GetErrors() {
		if statusErr == "" {
			return fail("leader status proxy returned an empty status error")
		}
		if statusErr == rpctypes.ErrNoLeader.Error() {
			noLeader = true
		}
	}
	if noLeader != (response.GetLeader() == 0) {
		return fail("leader status proxy returned inconsistent leader health")
	}
	return response, nil
}

func validateHashProxyPayload(metricCli metrics.Metrics, response *etcdserverpb.HashResponse, err error) (*etcdserverpb.HashResponse, error) {
	if err != nil {
		return response, err
	}
	if response.GetHeader().GetRevision() <= 0 {
		emitMaintenanceProxyIntegrityFailure(metricCli, maintenanceProxyRPCHash)
		return nil, status.Error(codes.DataLoss, "leader hash proxy returned a non-positive current revision")
	}
	return response, nil
}

func validateHashKVProxyPayload(metricCli metrics.Metrics, request *etcdserverpb.HashKVRequest, response *etcdserverpb.HashKVResponse, err error) (*etcdserverpb.HashKVResponse, error) {
	if err != nil {
		return response, err
	}
	fail := func(message string) (*etcdserverpb.HashKVResponse, error) {
		emitMaintenanceProxyIntegrityFailure(metricCli, maintenanceProxyRPCHashKV)
		return nil, status.Error(codes.DataLoss, message)
	}
	currentRevision := response.GetHeader().GetRevision()
	if currentRevision <= 0 {
		return fail("leader hash_kv proxy returned a non-positive current revision")
	}
	hashRevision := response.GetHashRevision()
	if request.GetRevision() == 0 {
		if hashRevision != currentRevision {
			return fail("leader hash_kv proxy returned a latest hash revision different from current revision")
		}
	} else if hashRevision != request.GetRevision() {
		return fail("leader hash_kv proxy returned a hash revision different from the request")
	}
	compactRevision := response.GetCompactRevision()
	if compactRevision < -1 {
		return fail("leader hash_kv proxy returned a compact revision below -1")
	}
	if hashRevision > 0 && (hashRevision > currentRevision || compactRevision > hashRevision) {
		return fail("leader hash_kv proxy returned inconsistent revision chronology")
	}
	return response, nil
}

func validateDowngradeProxyPayload(metricCli metrics.Metrics, request *etcdserverpb.DowngradeRequest, response *etcdserverpb.DowngradeResponse, err error) (*etcdserverpb.DowngradeResponse, error) {
	if err != nil {
		return response, err
	}
	fail := func(message string) (*etcdserverpb.DowngradeResponse, error) {
		emitMaintenanceProxyIntegrityFailure(metricCli, maintenanceProxyRPCDowngrade)
		return nil, status.Error(codes.DataLoss, message)
	}
	switch request.GetAction() {
	case etcdserverpb.DowngradeRequest_VALIDATE,
		etcdserverpb.DowngradeRequest_ENABLE,
		etcdserverpb.DowngradeRequest_CANCEL:
	default:
		return fail("leader downgrade proxy returned success for an unknown action")
	}
	if response.GetVersion() != ClusterVersion {
		return fail("leader downgrade proxy returned an unexpected cluster version")
	}
	return response, nil
}

func emitMaintenanceProxyIntegrityFailure(metricCli metrics.Metrics, rpc string) {
	if metricCli != nil {
		_ = metricCli.EmitCounter("maintenance.proxy.integrity_failure", 1, metrics.Tag("rpc", rpc))
	}
}
