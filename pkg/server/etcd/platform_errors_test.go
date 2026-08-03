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
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/status"
)

func TestPlatformManagedErrorsIdentifyTheirCapability(t *testing.T) {
	tests := []struct {
		name       string
		err        error
		capability string
	}{
		{name: "member mutation", err: memberMutationPlatformManagedError(), capability: "cluster.member_mutation"},
		{name: "move leader", err: moveLeaderPlatformManagedError(), capability: "maintenance.move_leader"},
		{name: "downgrade", err: downgradePlatformManagedError(), capability: "maintenance.downgrade"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			info := requirePlatformManagedErrorInfo(t, test.err)
			require.Equal(t, test.capability, info.Metadata["capability"])
			require.Empty(t, info.Metadata["operation_type"])
		})
	}
}

func requirePlatformManagedErrorInfo(t *testing.T, err error) *errdetails.ErrorInfo {
	t.Helper()
	for _, detail := range status.Convert(err).Details() {
		if info, ok := detail.(*errdetails.ErrorInfo); ok {
			require.Equal(t, platformManagedErrorReason, info.Reason)
			require.Equal(t, "dbaas.kubebrain.io", info.Domain)
			return info
		}
	}
	require.FailNow(t, "platform-managed ErrorInfo is missing")
	return nil
}
