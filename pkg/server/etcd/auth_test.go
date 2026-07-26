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
	"errors"
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"

	"github.com/kubewharf/kubebrain/pkg/storage"
)

type authRevisionBarrierShim struct {
	BackendShim
	current uint64
}

func (s *authRevisionBarrierShim) GetCurrentRevision() uint64 {
	return s.current
}

func (s *authRevisionBarrierShim) SetCurrentRevision(revision uint64) {
	s.current = revision
}

func TestAuthEnableValidatesBootstrapAndDisableRequiresRoot(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()
	ctx := context.Background()
	_, err := server.AuthEnable(ctx, &etcdserverpb.AuthEnableRequest{})
	requireAuthRPCError(t, err, rpctypes.ErrRootUserNotExist, codes.Unknown, "etcdserver: root user does not exist")
	require.NoError(t, server.auth.userAdd(ctx, &etcdserverpb.AuthUserAddRequest{Name: "root", Password: "secret"}))
	require.NoError(t, server.auth.roleAdd(ctx, "root"))
	require.NoError(t, server.auth.userGrantRole(ctx, "root", "root"))
	_, err = server.AuthEnable(ctx, &etcdserverpb.AuthEnableRequest{})
	require.NoError(t, err)
	_, err = server.AuthDisable(ctx, &etcdserverpb.AuthDisableRequest{})
	requireAuthRPCError(t, err, rpctypes.ErrUserEmpty, codes.Unknown, "etcdserver: user name is empty")
	authenticated, err := server.Authenticate(ctx, &etcdserverpb.AuthenticateRequest{Name: "root", Password: "secret"})
	require.NoError(t, err)
	rootCtx := metadata.NewIncomingContext(ctx, metadata.Pairs(rpctypes.TokenFieldNameGRPC, authenticated.Token))
	_, err = server.AuthDisable(rootCtx, &etcdserverpb.AuthDisableRequest{})
	require.NoError(t, err)
	statusResp, err := server.AuthStatus(ctx, &etcdserverpb.AuthStatusRequest{})
	require.NoError(t, err)
	require.False(t, statusResp.Enabled)
}

func TestAuthEnableUsesImplicitEtcdRootRole(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()
	ctx := context.Background()
	require.NoError(t, server.auth.userAdd(ctx, &etcdserverpb.AuthUserAddRequest{Name: "root", Password: "secret"}))
	require.NoError(t, server.auth.userGrantRole(ctx, "root", "root"))
	_, err := server.AuthEnable(ctx, &etcdserverpb.AuthEnableRequest{})
	require.NoError(t, err)
	authenticated, err := server.Authenticate(ctx, &etcdserverpb.AuthenticateRequest{Name: "root", Password: "secret"})
	require.NoError(t, err)
	rootCtx := metadata.NewIncomingContext(ctx, metadata.Pairs(rpctypes.TokenFieldNameGRPC, authenticated.Token))
	_, err = server.UserAdd(rootCtx, &etcdserverpb.AuthUserAddRequest{Name: "alice", Password: "secret"})
	require.NoError(t, err)
}

func TestAuthRPCHeadersTrackCurrentUserRevision(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()
	const revision = uint64(12345)
	server.backend.SetCurrentRevision(revision)

	ctx := context.Background()
	statusResponse, err := server.AuthStatus(ctx, &etcdserverpb.AuthStatusRequest{})
	require.NoError(t, err)
	require.Equal(t, int64(revision), statusResponse.Header.Revision)

	roleAddResponse, err := server.RoleAdd(ctx, &etcdserverpb.AuthRoleAddRequest{Name: "header-role"})
	require.NoError(t, err)
	require.Equal(t, int64(revision), roleAddResponse.Header.Revision)

	roleGetResponse, err := server.RoleGet(ctx, &etcdserverpb.AuthRoleGetRequest{Role: "header-role"})
	require.NoError(t, err)
	require.Equal(t, int64(revision), roleGetResponse.Header.Revision)
}

func TestAuthRPCHeadersOverGRPCMatchEtcd(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()

	grpcServer := grpc.NewServer(server.ClientServerOptions()...)
	etcdserverpb.RegisterKVServer(grpcServer, server)
	etcdserverpb.RegisterAuthServer(grpcServer, server)
	listener := bufconn.Listen(1 << 20)
	go func() { _ = grpcServer.Serve(listener) }()
	t.Cleanup(grpcServer.Stop)

	conn, err := grpc.NewClient("passthrough:///bufnet",
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
			return listener.Dial()
		}),
	)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, conn.Close()) })
	kvClient := etcdserverpb.NewKVClient(conn)
	authClient := etcdserverpb.NewAuthClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	put, err := kvClient.Put(ctx, &etcdserverpb.PutRequest{
		Key: []byte("/a975/auth-header/key"), Value: []byte("value"),
	})
	require.NoError(t, err)
	require.NotNil(t, put.Header)
	roleName := "a975-auth-header-role"

	statusResponse, err := authClient.AuthStatus(ctx, &etcdserverpb.AuthStatusRequest{})
	require.NoError(t, err)
	roleAddResponse, err := authClient.RoleAdd(ctx, &etcdserverpb.AuthRoleAddRequest{Name: roleName})
	require.NoError(t, err)
	roleGetResponse, err := authClient.RoleGet(ctx, &etcdserverpb.AuthRoleGetRequest{Role: roleName})
	require.NoError(t, err)

	for _, header := range []*etcdserverpb.ResponseHeader{
		statusResponse.Header,
		roleAddResponse.Header,
		roleGetResponse.Header,
	} {
		require.NotNil(t, header)
		require.Equal(t, put.Header.Revision, header.Revision)
		require.NotZero(t, header.ClusterId)
		require.NotZero(t, header.MemberId)
		require.Positive(t, header.RaftTerm)
	}
}

func TestAuthRPCHeaderWaitsForLeaderRevision(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()
	shim := &authRevisionBarrierShim{BackendShim: server.backend, current: 100}
	server.backend = shim
	server.peers = testPeerService{syncReadFn: func(context.Context) error {
		shim.SetCurrentRevision(200)
		return nil
	}}

	response, err := server.AuthStatus(context.Background(), &etcdserverpb.AuthStatusRequest{})
	require.NoError(t, err)
	require.Equal(t, int64(200), response.Header.Revision)
}

func TestAuthRPCHeaderFailsClosedWhenRevisionBarrierFails(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()
	server.peers = testPeerService{syncReadFn: func(context.Context) error {
		return storage.ErrUnavailable
	}}

	response, err := server.AuthStatus(context.Background(), &etcdserverpb.AuthStatusRequest{})
	require.Nil(t, response)
	requireReadBarrierUnavailable(t, err, storage.ErrUnavailable.Error())
}

func TestAuthenticateReadBarrierPrecedesPasswordCheck(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()
	ctx := context.Background()
	require.NoError(t, server.auth.userAdd(ctx, &etcdserverpb.AuthUserAddRequest{Name: "root", Password: "secret"}))
	require.NoError(t, server.auth.roleAdd(ctx, "root"))
	require.NoError(t, server.auth.userGrantRole(ctx, "root", "root"))
	require.NoError(t, server.auth.enable(ctx))
	barrierErr := errors.New("authenticate read barrier failed")
	server.peers = testPeerService{syncReadFn: func(context.Context) error {
		return barrierErr
	}}

	response, err := server.Authenticate(ctx, &etcdserverpb.AuthenticateRequest{Name: "root", Password: "wrong"})
	require.Nil(t, response)
	requireReadBarrierUnavailable(t, err, barrierErr.Error())
}

func TestAuthStatusRejectsInvalidTokenWhenEnabled(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()
	plain := context.Background()
	setupAuthKVUser(t, server)

	response, err := server.AuthStatus(plain, &etcdserverpb.AuthStatusRequest{})
	require.NoError(t, err)
	require.True(t, response.Enabled)

	badCtx := metadata.NewIncomingContext(plain, metadata.Pairs(rpctypes.TokenFieldNameGRPC, "not-a-valid-token"))
	response, err = server.AuthStatus(badCtx, &etcdserverpb.AuthStatusRequest{})
	require.Nil(t, response)
	requireAuthRPCError(t, err, rpctypes.ErrInvalidAuthToken, codes.Unknown, "etcdserver: invalid auth token")

	goodAuth, err := server.Authenticate(plain, &etcdserverpb.AuthenticateRequest{Name: "root", Password: "root-secret"})
	require.NoError(t, err)
	goodCtx := metadata.NewIncomingContext(plain, metadata.Pairs(rpctypes.TokenFieldNameGRPC, goodAuth.Token))
	response, err = server.AuthStatus(goodCtx, &etcdserverpb.AuthStatusRequest{})
	require.NoError(t, err)
	require.True(t, response.Enabled)
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
	requireAuthRPCError(t, err, rpctypes.ErrUserEmpty, codes.Unknown, "etcdserver: user name is empty")
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
	requireAuthRPCError(t, err, rpctypes.ErrPermissionDenied, codes.Unknown, "etcdserver: permission denied")
	_, err = server.RoleGet(aliceCtx, &etcdserverpb.AuthRoleGetRequest{Role: "allowed"})
	require.NoError(t, err)
	_, err = server.RoleGet(aliceCtx, &etcdserverpb.AuthRoleGetRequest{Role: "root"})
	requireAuthRPCError(t, err, rpctypes.ErrPermissionDenied, codes.Unknown, "etcdserver: permission denied")
	_, err = server.UserList(aliceCtx, &etcdserverpb.AuthUserListRequest{})
	requireAuthRPCError(t, err, rpctypes.ErrPermissionDenied, codes.Unknown, "etcdserver: permission denied")
	_, err = server.RoleAdd(aliceCtx, &etcdserverpb.AuthRoleAddRequest{Name: "forbidden"})
	requireAuthRPCError(t, err, rpctypes.ErrPermissionDenied, codes.Unknown, "etcdserver: permission denied")

	_, err = server.RoleAdd(rootCtx, &etcdserverpb.AuthRoleAddRequest{Name: "operator"})
	require.NoError(t, err)
	rootAuth, err = server.Authenticate(plain, &etcdserverpb.AuthenticateRequest{Name: "root", Password: "root-secret"})
	require.NoError(t, err)
	rootCtx = metadata.NewIncomingContext(plain, metadata.Pairs(rpctypes.TokenFieldNameGRPC, rootAuth.Token))
	roles, err := server.RoleList(rootCtx, &etcdserverpb.AuthRoleListRequest{})
	require.NoError(t, err)
	require.Contains(t, roles.Roles, "operator")
}

func TestAuthRPCUserGetEmptyNameWithoutIdentityMatchesEtcd(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()
	setupAuthKVUser(t, server)
	plain := context.Background()

	_, err := server.UserGet(plain, &etcdserverpb.AuthUserGetRequest{Name: "alice"})
	requireAuthRPCError(t, err, rpctypes.ErrUserEmpty, codes.Unknown, "etcdserver: user name is empty")
	_, err = server.UserGet(plain, &etcdserverpb.AuthUserGetRequest{Name: ""})
	requireAuthRPCError(t, err, rpctypes.ErrUserNotFound, codes.Unknown, "etcdserver: user name not found")
}

func TestAuthRPCClientCertificateAdminErrorsMatchEtcd(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()
	setupAuthKVUser(t, server)
	server.SetClientCertAuth(true)
	ctx := context.Background()

	emptyCN := verifiedTLSContext(ctx, "")
	_, err := server.UserList(emptyCN, &etcdserverpb.AuthUserListRequest{})
	requireAuthRPCError(t, err, rpctypes.ErrUserEmpty, codes.Unknown, "etcdserver: user name is empty")
	_, err = server.UserGet(emptyCN, &etcdserverpb.AuthUserGetRequest{Name: "alice"})
	requireAuthRPCError(t, err, rpctypes.ErrUserEmpty, codes.Unknown, "etcdserver: user name is empty")
	_, err = server.RoleGet(emptyCN, &etcdserverpb.AuthRoleGetRequest{Role: "allowed"})
	requireAuthRPCError(t, err, rpctypes.ErrUserEmpty, codes.Unknown, "etcdserver: user name is empty")
	_, err = server.AuthDisable(emptyCN, &etcdserverpb.AuthDisableRequest{})
	requireAuthRPCError(t, err, rpctypes.ErrUserEmpty, codes.Unknown, "etcdserver: user name is empty")

	unknownCN := verifiedTLSContext(ctx, "external-cn")
	_, err = server.UserList(unknownCN, &etcdserverpb.AuthUserListRequest{})
	requireAuthRPCError(t, err, rpctypes.ErrUserNotFound, codes.Unknown, "etcdserver: user name not found")
	_, err = server.UserGet(unknownCN, &etcdserverpb.AuthUserGetRequest{Name: "alice"})
	requireAuthRPCError(t, err, rpctypes.ErrUserNotFound, codes.Unknown, "etcdserver: user name not found")
	_, err = server.UserGet(unknownCN, &etcdserverpb.AuthUserGetRequest{Name: "external-cn"})
	requireAuthRPCError(t, err, rpctypes.ErrUserNotFound, codes.Unknown, "etcdserver: user name not found")
	_, err = server.RoleGet(unknownCN, &etcdserverpb.AuthRoleGetRequest{Role: "allowed"})
	requireAuthRPCError(t, err, rpctypes.ErrUserNotFound, codes.Unknown, "etcdserver: user name not found")

	aliceCN := verifiedTLSContext(ctx, "alice")
	self, err := server.UserGet(aliceCN, &etcdserverpb.AuthUserGetRequest{Name: "alice"})
	require.NoError(t, err)
	require.Contains(t, self.Roles, "allowed")
	_, err = server.RoleGet(aliceCN, &etcdserverpb.AuthRoleGetRequest{Role: "allowed"})
	require.NoError(t, err)
	_, err = server.UserList(aliceCN, &etcdserverpb.AuthUserListRequest{})
	requireAuthRPCError(t, err, rpctypes.ErrPermissionDenied, codes.Unknown, "etcdserver: permission denied")
}

func requireAuthRPCError(t *testing.T, err error, want error, code codes.Code, message string) {
	t.Helper()
	require.ErrorIs(t, err, want)
	require.Equal(t, code, status.Code(err))
	require.Equal(t, message, status.Convert(err).Message())
}
