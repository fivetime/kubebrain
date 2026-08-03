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
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	alarmMutationUnsupportedMessage  = "etcd alarm mutation does not represent TiKV capacity; use PD/TiKV alerts and DBaaS remediation"
	snapshotUnsupportedMessage       = "etcd snapshot is unavailable on TiKV; use the DBaaS logical backup and restore workflow"
	moveLeaderUnsupportedMessage     = "KubeBrain leadership is managed automatically; use DBaaS rollout or failover orchestration"
	downgradeUnsupportedMessage      = "in-place etcd protocol downgrade is unavailable; use a DBaaS versioned rollout or rollback"
	memberMutationUnsupportedMessage = "KubeBrain replicas are stateless; scale or reconfigure them through the DBaaS control plane"
)

const platformManagedErrorReason = "KUBEBRAIN_PLATFORM_MANAGED"

// platformManagedError keeps the etcd-facing status stable while giving
// DBaaS-aware callers a machine-readable replacement contract. Generic etcd
// clients continue to see codes.Unimplemented and the actionable message.
func platformManagedError(message, capability, operationType string, metadata map[string]string) error {
	details := map[string]string{
		"capability": capability,
	}
	if operationType != "" {
		details["operation_type"] = operationType
	}
	for key, value := range metadata {
		details[key] = value
	}
	withDetails, err := status.New(codes.Unimplemented, message).WithDetails(&errdetails.ErrorInfo{
		Reason:   platformManagedErrorReason,
		Domain:   "dbaas.kubebrain.io",
		Metadata: details,
	})
	if err != nil {
		return status.Error(codes.Unimplemented, message)
	}
	return withDetails.Err()
}

func snapshotPlatformManagedError() error {
	return platformManagedError(snapshotUnsupportedMessage, "maintenance.snapshot", "Backup", map[string]string{
		"artifact_format":              "kubebrain.logical.v2",
		"conversion_requires_prefix":   "/",
		"conversion_tool":              "kubebrain-logical-etcd-snapshot",
		"etcd_snapshot_restore_usable": "false",
		"converted_snapshot_auth":      "disabled",
	})
}

func memberMutationPlatformManagedError() error {
	return platformManagedError(memberMutationUnsupportedMessage, "cluster.member_mutation", "", nil)
}

func moveLeaderPlatformManagedError() error {
	return platformManagedError(moveLeaderUnsupportedMessage, "maintenance.move_leader", "", nil)
}

func downgradePlatformManagedError() error {
	return platformManagedError(downgradeUnsupportedMessage, "maintenance.downgrade", "", nil)
}
