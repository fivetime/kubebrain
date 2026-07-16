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
	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
	"google.golang.org/grpc/metadata"
)

func TestAuthEnableValidatesBootstrapAndDisableRequiresRoot(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()
	ctx := context.Background()
	_, err := server.AuthEnable(ctx, &etcdserverpb.AuthEnableRequest{})
	require.ErrorIs(t, err, rpctypes.ErrRootUserNotExist)
	require.NoError(t, server.auth.userAdd(ctx, &etcdserverpb.AuthUserAddRequest{Name: "root", Password: "secret"}))
	require.NoError(t, server.auth.roleAdd(ctx, "root"))
	require.NoError(t, server.auth.userGrantRole(ctx, "root", "root"))
	_, err = server.AuthEnable(ctx, &etcdserverpb.AuthEnableRequest{})
	require.NoError(t, err)
	_, err = server.AuthDisable(ctx, &etcdserverpb.AuthDisableRequest{})
	require.ErrorIs(t, err, rpctypes.ErrUserEmpty)
	authenticated, err := server.Authenticate(ctx, &etcdserverpb.AuthenticateRequest{Name: "root", Password: "secret"})
	require.NoError(t, err)
	rootCtx := metadata.NewIncomingContext(ctx, metadata.Pairs(rpctypes.TokenFieldNameGRPC, authenticated.Token))
	_, err = server.AuthDisable(rootCtx, &etcdserverpb.AuthDisableRequest{})
	require.NoError(t, err)
	statusResp, err := server.AuthStatus(ctx, &etcdserverpb.AuthStatusRequest{})
	require.NoError(t, err)
	require.False(t, statusResp.Enabled)
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
	require.ErrorIs(t, err, rpctypes.ErrUserEmpty)
	rootCtx := metadata.NewIncomingContext(ctx, metadata.Pairs(rpctypes.TokenFieldNameGRPC, authenticated.Token))
	users, err = server.UserList(rootCtx, &etcdserverpb.AuthUserListRequest{})
	require.NoError(t, err)
	require.Equal(t, []string{"root"}, users.Users)
}

func TestAuthRPCEnabledAdminAndSelfRules(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()
	aliceCtx := setupAuthKVUser(t, server)
	plain := context.Background()
	rootAuth, err := server.Authenticate(plain, &etcdserverpb.AuthenticateRequest{Name: "root", Password: "root-secret"})
	require.NoError(t, err)
	rootCtx := metadata.NewIncomingContext(plain, metadata.Pairs(rpctypes.TokenFieldNameGRPC, rootAuth.Token))

	statusResp, err := server.AuthStatus(plain, &etcdserverpb.AuthStatusRequest{})
	require.NoError(t, err)
	require.True(t, statusResp.Enabled)
	statusResp, err = server.AuthStatus(aliceCtx, &etcdserverpb.AuthStatusRequest{})
	require.NoError(t, err)
	require.True(t, statusResp.Enabled)
	self, err := server.UserGet(aliceCtx, &etcdserverpb.AuthUserGetRequest{Name: "alice"})
	require.NoError(t, err)
	require.Contains(t, self.Roles, "allowed")
	_, err = server.UserGet(aliceCtx, &etcdserverpb.AuthUserGetRequest{Name: "root"})
	require.ErrorIs(t, err, rpctypes.ErrPermissionDenied)
	_, err = server.RoleGet(aliceCtx, &etcdserverpb.AuthRoleGetRequest{Role: "allowed"})
	require.NoError(t, err)
	_, err = server.RoleGet(aliceCtx, &etcdserverpb.AuthRoleGetRequest{Role: "root"})
	require.ErrorIs(t, err, rpctypes.ErrPermissionDenied)
	_, err = server.UserList(aliceCtx, &etcdserverpb.AuthUserListRequest{})
	require.ErrorIs(t, err, rpctypes.ErrPermissionDenied)
	_, err = server.RoleAdd(aliceCtx, &etcdserverpb.AuthRoleAddRequest{Name: "forbidden"})
	require.ErrorIs(t, err, rpctypes.ErrPermissionDenied)

	_, err = server.RoleAdd(rootCtx, &etcdserverpb.AuthRoleAddRequest{Name: "operator"})
	require.NoError(t, err)
	rootAuth, err = server.Authenticate(plain, &etcdserverpb.AuthenticateRequest{Name: "root", Password: "root-secret"})
	require.NoError(t, err)
	rootCtx = metadata.NewIncomingContext(plain, metadata.Pairs(rpctypes.TokenFieldNameGRPC, rootAuth.Token))
	roles, err := server.RoleList(rootCtx, &etcdserverpb.AuthRoleListRequest{})
	require.NoError(t, err)
	require.Contains(t, roles.Roles, "operator")
}
