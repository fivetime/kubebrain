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
	"bytes"
	"fmt"

	"go.etcd.io/etcd/api/v3/authpb"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/kubewharf/kubebrain/pkg/metrics"
)

const (
	authProxyActionEnable               = "auth_enable"
	authProxyActionDisable              = "auth_disable"
	authProxyActionStatus               = "auth_status"
	authProxyActionAuthenticate         = "authenticate"
	authProxyActionUserAdd              = "user_add"
	authProxyActionUserGet              = "user_get"
	authProxyActionUserList             = "user_list"
	authProxyActionUserDelete           = "user_delete"
	authProxyActionUserChangePassword   = "user_change_password"
	authProxyActionUserGrantRole        = "user_grant_role"
	authProxyActionUserRevokeRole       = "user_revoke_role"
	authProxyActionRoleAdd              = "role_add"
	authProxyActionRoleGet              = "role_get"
	authProxyActionRoleList             = "role_list"
	authProxyActionRoleDelete           = "role_delete"
	authProxyActionRoleGrantPermission  = "role_grant_permission"
	authProxyActionRoleRevokePermission = "role_revoke_permission"
)

var authProxyActions = []string{
	authProxyActionEnable,
	authProxyActionDisable,
	authProxyActionStatus,
	authProxyActionAuthenticate,
	authProxyActionUserAdd,
	authProxyActionUserGet,
	authProxyActionUserList,
	authProxyActionUserDelete,
	authProxyActionUserChangePassword,
	authProxyActionUserGrantRole,
	authProxyActionUserRevokeRole,
	authProxyActionRoleAdd,
	authProxyActionRoleGet,
	authProxyActionRoleList,
	authProxyActionRoleDelete,
	authProxyActionRoleGrantPermission,
	authProxyActionRoleRevokePermission,
}

func initAuthProxyIntegrityMetrics(metricCli metrics.Metrics) {
	if metricCli == nil {
		return
	}
	for _, action := range authProxyActions {
		_ = metricCli.EmitCounter("auth.proxy.integrity_failure", int64(0), metrics.Tag("action", action))
	}
}

func validateAuthProxyResult[T any](metricCli metrics.Metrics, action string, response *T, err error) (*T, error) {
	if (response == nil) != (err == nil) {
		if response != nil {
			headerResponse, ok := any(response).(interface {
				GetHeader() *etcdserverpb.ResponseHeader
			})
			if !ok || headerResponse.GetHeader() == nil {
				emitAuthProxyIntegrityFailure(metricCli, action)
				return nil, status.Error(codes.DataLoss, fmt.Sprintf("leader %s proxy returned a response without a header", action))
			}
			if headerResponse.GetHeader().GetRevision() < 0 {
				emitAuthProxyIntegrityFailure(metricCli, action)
				return nil, status.Error(codes.DataLoss, fmt.Sprintf("leader %s proxy returned a response with a negative header revision", action))
			}
		}
		return response, err
	}
	emitAuthProxyIntegrityFailure(metricCli, action)
	if response == nil {
		return nil, status.Error(codes.DataLoss, fmt.Sprintf("leader %s proxy returned neither response nor error", action))
	}
	return nil, status.Error(codes.DataLoss, fmt.Sprintf("leader %s proxy returned both response and error", action))
}

func validateAuthNameListProxyPayload[T any](metricCli metrics.Metrics, action string, names []string, response *T, err error) (*T, error) {
	if err != nil {
		return response, err
	}
	for index, name := range names {
		if name == "" {
			emitAuthProxyIntegrityFailure(metricCli, action)
			return nil, status.Error(codes.DataLoss, fmt.Sprintf("leader %s proxy returned an empty name", action))
		}
		if index > 0 && names[index-1] >= name {
			emitAuthProxyIntegrityFailure(metricCli, action)
			return nil, status.Error(codes.DataLoss, fmt.Sprintf("leader %s proxy returned duplicate or unsorted names", action))
		}
	}
	return response, nil
}

func validateAuthRoleGetProxyPayload(metricCli metrics.Metrics, request *etcdserverpb.AuthRoleGetRequest, response *etcdserverpb.AuthRoleGetResponse, err error) (*etcdserverpb.AuthRoleGetResponse, error) {
	if err != nil {
		return response, err
	}
	fail := func(message string) (*etcdserverpb.AuthRoleGetResponse, error) {
		emitAuthProxyIntegrityFailure(metricCli, authProxyActionRoleGet)
		return nil, status.Error(codes.DataLoss, message)
	}
	permissions := response.GetPerm()
	if request.GetRole() == "root" {
		if len(permissions) != 1 || permissions[0] == nil ||
			permissions[0].GetPermType() != authpb.READWRITE || len(permissions[0].GetKey()) != 0 ||
			!bytes.Equal(permissions[0].GetRangeEnd(), []byte{0}) {
			return fail("leader role_get proxy returned a non-canonical root permission")
		}
		return response, nil
	}
	for index, permission := range permissions {
		if permission == nil || !validPermissionRange(permission.GetKey(), permission.GetRangeEnd()) {
			return fail("leader role_get proxy returned an invalid permission range")
		}
		if index > 0 && bytes.Compare(permissions[index-1].GetKey(), permission.GetKey()) > 0 {
			return fail("leader role_get proxy returned unsorted permissions")
		}
	}
	return response, nil
}

func emitAuthProxyIntegrityFailure(metricCli metrics.Metrics, action string) {
	if metricCli != nil {
		_ = metricCli.EmitCounter("auth.proxy.integrity_failure", 1, metrics.Tag("action", action))
	}
}
