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
	"testing"

	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestAuthEnableRemainsUnsupportedUntilDataPlaneIsProtected(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()

	_, err := server.AuthEnable(context.Background(), &etcdserverpb.AuthEnableRequest{})
	require.Equal(t, codes.Unimplemented, status.Code(err))

}

func TestAuthRPCBootstrapAndEnabledSafetyBoundary(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()
	ctx := context.Background()

	statusResp, err := server.AuthStatus(ctx, &etcdserverpb.AuthStatusRequest{})
	require.NoError(t, err)
	require.False(t, statusResp.Enabled)
	_, err = server.UserAdd(ctx, &etcdserverpb.AuthUserAddRequest{Name: "root", Password: "secret"})
	require.NoError(t, err)
	_, err = server.RoleAdd(ctx, &etcdserverpb.AuthRoleAddRequest{Name: "root"})
	require.NoError(t, err)
	_, err = server.UserGrantRole(ctx, &etcdserverpb.AuthUserGrantRoleRequest{User: "root", Role: "root"})
	require.NoError(t, err)
	users, err := server.UserList(ctx, &etcdserverpb.AuthUserListRequest{})
	require.NoError(t, err)
	require.Equal(t, []string{"root"}, users.Users)
	root, err := server.RoleGet(ctx, &etcdserverpb.AuthRoleGetRequest{Role: "root"})
	require.NoError(t, err)
	require.Len(t, root.Perm, 1)

	require.NoError(t, server.auth.enable(ctx))
	authenticated, err := server.Authenticate(ctx, &etcdserverpb.AuthenticateRequest{Name: "root", Password: "secret"})
	require.NoError(t, err)
	require.NotEmpty(t, authenticated.Token)
	_, err = server.UserList(ctx, &etcdserverpb.AuthUserListRequest{})
	require.Equal(t, codes.Unimplemented, status.Code(err), "enabled auth management must not be exposed before request authentication")
}
