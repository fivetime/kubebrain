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

	"github.com/Masterminds/semver/v3"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/prototext"

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
			header := headerResponse.GetHeader()
			if header.GetRevision() < 0 {
				emitMaintenanceProxyIntegrityFailure(metricCli, rpc)
				return nil, status.Error(codes.DataLoss, fmt.Sprintf("leader %s proxy returned a response with a negative header revision", rpc))
			}
			if header.GetClusterId() == 0 {
				emitMaintenanceProxyIntegrityFailure(metricCli, rpc)
				return nil, status.Error(codes.DataLoss, fmt.Sprintf("leader %s proxy returned a response with a zero cluster ID", rpc))
			}
			if header.GetMemberId() == 0 {
				emitMaintenanceProxyIntegrityFailure(metricCli, rpc)
				return nil, status.Error(codes.DataLoss, fmt.Sprintf("leader %s proxy returned a response with a zero member ID", rpc))
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
	serverVersion, parseErr := semver.StrictNewVersion(response.GetVersion())
	if parseErr != nil {
		return fail("leader status proxy returned an invalid server version")
	}
	hasStatusFields34 := serverVersion.Major() > 3 ||
		(serverVersion.Major() == 3 && serverVersion.Minor() >= 4)
	if !hasStatusFields34 &&
		(response.GetRaftAppliedIndex() != 0 || len(response.GetErrors()) != 0 ||
			response.GetDbSizeInUse() != 0 || response.GetIsLearner()) {
		return fail("leader status proxy returned 3.4 fields for a pre-3.4 server")
	}
	hasVersionedStatusFields := serverVersion.Major() > 3 ||
		(serverVersion.Major() == 3 && serverVersion.Minor() >= 6)
	downgradeInfo := response.GetDowngradeInfo()
	if !hasVersionedStatusFields {
		if response.GetStorageVersion() != "" || response.GetDbSizeQuota() != 0 || downgradeInfo != nil {
			return fail("leader status proxy returned 3.6 fields for a pre-3.6 server")
		}
	}
	// StorageVersion is empty on pre-3.6 backends. Whenever present, upstream's
	// UnsafeSetStorageVersion normalizes it to a canonical major.minor.0 release.
	if response.GetStorageVersion() != "" {
		storageVersion, parseErr := semver.StrictNewVersion(response.GetStorageVersion())
		if parseErr != nil || storageVersion.Patch() != 0 || storageVersion.Prerelease() != "" ||
			storageVersion.Metadata() != "" || storageVersion.String() != response.GetStorageVersion() {
			return fail("leader status proxy returned an invalid storage version")
		}
	}
	// Upstream samples Size and SizeInUse through two independent atomic loads.
	// A concurrent backend commit can therefore make the later in-use sample
	// temporarily exceed the earlier allocated-size sample.
	if response.GetDbSize() < 0 || response.GetDbSizeInUse() < 0 {
		return fail("leader status proxy returned invalid database sizes")
	}
	// Upstream uses a negative configured quota to disable quota enforcement
	// and publishes that sentinel unchanged in Status. Zero is replaced by the
	// default quota before the response is returned.
	if hasVersionedStatusFields && response.GetDbSizeQuota() == 0 {
		return fail("leader status proxy returned a zero database quota")
	}
	if hasVersionedStatusFields && downgradeInfo == nil {
		return fail("leader status proxy returned no downgrade information")
	}
	if downgradeInfo != nil {
		if !downgradeInfo.GetEnabled() {
			if downgradeInfo.GetTargetVersion() != "" {
				return fail("leader status proxy returned inconsistent downgrade information")
			}
		} else {
			targetVersion, parseErr := semver.StrictNewVersion(downgradeInfo.GetTargetVersion())
			if parseErr != nil || targetVersion.Prerelease() != "" || targetVersion.Metadata() != "" ||
				targetVersion.Patch() != 0 || targetVersion.Major() != serverVersion.Major() ||
				(targetVersion.Minor() != serverVersion.Minor() &&
					(serverVersion.Minor() == 0 || targetVersion.Minor() != serverVersion.Minor()-1)) {
				return fail("leader status proxy returned inconsistent downgrade information")
			}
		}
	}
	if !hasStatusFields34 {
		return response, nil
	}
	statusErrors := response.GetErrors()
	alarmOffset := 0
	if response.GetLeader() == 0 {
		// Upstream appends ErrNoLeader exactly once before iterating its alarm
		// store. Preserve that order so duplicate or displaced diagnostics cannot
		// cross the trusted leader-proxy boundary as an apparently valid Status.
		if len(statusErrors) == 0 || statusErrors[0] != rpctypes.ErrNoLeader.Error() {
			return fail("leader status proxy returned inconsistent leader health")
		}
		alarmOffset = 1
	}
	seenAlarms := make(map[struct {
		memberID uint64
		alarm    etcdserverpb.AlarmType
	}]struct{}, len(statusErrors)-alarmOffset)
	for _, statusErr := range statusErrors[alarmOffset:] {
		// Status emits AlarmMember.String(), not arbitrary diagnostic strings.
		// Parse with protobuf's own text codec and require one of its two
		// build-dependent compact renderings, including omitted zero fields and
		// numeric unknown enums.
		var alarm etcdserverpb.AlarmMember
		if parseErr := prototext.Unmarshal([]byte(statusErr), &alarm); parseErr != nil ||
			alarm.GetAlarm() == etcdserverpb.AlarmType_NONE || !matchesAlarmStatusError(&alarm, statusErr) {
			return fail("leader status proxy returned an invalid status alarm")
		}
		identity := struct {
			memberID uint64
			alarm    etcdserverpb.AlarmType
		}{memberID: alarm.GetMemberID(), alarm: alarm.GetAlarm()}
		if _, exists := seenAlarms[identity]; exists {
			return fail("leader status proxy returned a duplicate status alarm")
		}
		seenAlarms[identity] = struct{}{}
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
