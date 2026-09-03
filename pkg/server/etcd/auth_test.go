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
	"encoding/base64"
	"errors"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/api/v3/authpb"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
	"golang.org/x/crypto/bcrypt"
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

type failingColdAuthRevisionShim struct {
	BackendShim
	current uint64
	err     error
}

func (s *failingColdAuthRevisionShim) GetCurrentRevision() uint64 { return s.current }
func (s *failingColdAuthRevisionShim) SetCurrentRevision(revision uint64) {
	s.current = revision
}
func (s *failingColdAuthRevisionShim) GetDurableRevision(context.Context) (uint64, error) {
	return 0, s.err
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

func TestAuthStatusRestoresColdLeaderHeaderFromDurableRevision(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()
	shim := &coldDurableRevisionShim{BackendShim: server.backend, durable: 234}
	server.backend = shim
	server.peers = testPeerService{isLeader: true}

	response, err := server.AuthStatus(context.Background(), &etcdserverpb.AuthStatusRequest{})
	require.NoError(t, err)
	require.Equal(t, int64(234), response.GetHeader().GetRevision())
	require.Equal(t, uint64(234), shim.GetCurrentRevision())
}

func TestAuthMutationFailsBeforeWriteWhenColdRevisionUnavailable(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()
	revisionErr := errors.New("durable auth revision unavailable")
	server.backend = &failingColdAuthRevisionShim{BackendShim: server.backend, err: revisionErr}
	server.peers = testPeerService{isLeader: true}

	response, err := server.RoleAdd(context.Background(), &etcdserverpb.AuthRoleAddRequest{Name: "must-not-exist"})
	require.Nil(t, response)
	require.ErrorIs(t, err, revisionErr)
	snapshot, snapshotErr := server.tokens.snapshots.current(context.Background())
	require.NoError(t, snapshotErr)
	require.NotContains(t, snapshot.Roles, "must-not-exist")
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

	request := &etcdserverpb.AuthenticateRequest{Name: "root", Password: "wrong"}
	response, err := server.Authenticate(ctx, request)
	require.Nil(t, response)
	requireReadBarrierUnavailable(t, err, barrierErr.Error())
	require.Equal(t, "wrong", request.Password, "upstream installs password cleanup only after the read barrier")
}

func TestAuthenticateClearsPasswordAfterReadBarrier(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()
	ctx := context.Background()
	setupAuthKVUser(t, server)

	success := &etcdserverpb.AuthenticateRequest{Name: "root", Password: "root-secret"}
	response, err := server.Authenticate(ctx, success)
	require.NoError(t, err)
	require.NotEmpty(t, response.Token)
	require.Empty(t, success.Password)

	failure := &etcdserverpb.AuthenticateRequest{Name: "root", Password: "wrong"}
	response, err = server.Authenticate(ctx, failure)
	require.Nil(t, response)
	requireAuthRPCError(t, err, rpctypes.ErrAuthFailed, codes.Unknown, "etcdserver: authentication failed, invalid user ID or password")
	require.Empty(t, failure.Password)
}

func TestUserPasswordRequestsReplacePlaintextBeforeApply(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()
	ctx := context.Background()

	add := &etcdserverpb.AuthUserAddRequest{Name: "alice", Password: "first"}
	_, err := server.UserAdd(ctx, add)
	require.NoError(t, err)
	require.Empty(t, add.Password)
	hashedAdd, err := base64.StdEncoding.DecodeString(add.HashedPassword)
	require.NoError(t, err)
	require.NoError(t, bcrypt.CompareHashAndPassword(hashedAdd, []byte("first")))

	duplicate := &etcdserverpb.AuthUserAddRequest{Name: "alice", Password: "duplicate"}
	_, err = server.UserAdd(ctx, duplicate)
	requireAuthRPCError(t, err, rpctypes.ErrUserAlreadyExist, codes.Unknown, "etcdserver: user name already exists")
	require.Empty(t, duplicate.Password)
	hashedDuplicate, err := base64.StdEncoding.DecodeString(duplicate.HashedPassword)
	require.NoError(t, err)
	require.NoError(t, bcrypt.CompareHashAndPassword(hashedDuplicate, []byte("duplicate")))

	noPassword := &etcdserverpb.AuthUserAddRequest{
		Name: "nopass", Password: "ignored", Options: &authpb.UserAddOptions{NoPassword: true},
	}
	_, err = server.UserAdd(ctx, noPassword)
	require.NoError(t, err)
	require.Equal(t, "ignored", noPassword.Password)
	require.Empty(t, noPassword.HashedPassword)

	change := &etcdserverpb.AuthUserChangePasswordRequest{Name: "alice", Password: "second"}
	_, err = server.UserChangePassword(ctx, change)
	require.NoError(t, err)
	require.Empty(t, change.Password)
	hashedChange, err := base64.StdEncoding.DecodeString(change.HashedPassword)
	require.NoError(t, err)
	require.NoError(t, bcrypt.CompareHashAndPassword(hashedChange, []byte("second")))

	emptyPasswordHash, err := bcrypt.GenerateFromPassword([]byte("third"), bcrypt.MinCost)
	require.NoError(t, err)
	emptyPassword := &etcdserverpb.AuthUserChangePasswordRequest{
		Name:           "alice",
		HashedPassword: base64.StdEncoding.EncodeToString(emptyPasswordHash),
	}
	_, err = server.UserChangePassword(ctx, emptyPassword)
	require.NoError(t, err)
	require.Empty(t, emptyPassword.Password)
	require.Equal(t, base64.StdEncoding.EncodeToString(emptyPasswordHash), emptyPassword.HashedPassword)

	missing := &etcdserverpb.AuthUserChangePasswordRequest{Name: "missing", Password: "secret"}
	_, err = server.UserChangePassword(ctx, missing)
	requireAuthRPCError(t, err, rpctypes.ErrUserNotFound, codes.Unknown, "etcdserver: user name not found")
	require.Empty(t, missing.Password)
	hashedMissing, err := base64.StdEncoding.DecodeString(missing.HashedPassword)
	require.NoError(t, err)
	require.NoError(t, bcrypt.CompareHashAndPassword(hashedMissing, []byte("secret")))
}

func TestUserPasswordRequestsReplacePlaintextBeforeAuthFailureLikeEtcd(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()
	aliceCtx := setupAuthKVUser(t, server)

	add := &etcdserverpb.AuthUserAddRequest{Name: "blocked-add", Password: "add-secret"}
	_, err := server.UserAdd(aliceCtx, add)
	requireAuthRPCError(t, err, rpctypes.ErrPermissionDenied, codes.Unknown, "etcdserver: permission denied")
	require.Empty(t, add.Password)
	hashedAdd, err := base64.StdEncoding.DecodeString(add.HashedPassword)
	require.NoError(t, err)
	require.NoError(t, bcrypt.CompareHashAndPassword(hashedAdd, []byte("add-secret")))

	change := &etcdserverpb.AuthUserChangePasswordRequest{Name: "alice", Password: "change-secret"}
	_, err = server.UserChangePassword(aliceCtx, change)
	requireAuthRPCError(t, err, rpctypes.ErrPermissionDenied, codes.Unknown, "etcdserver: permission denied")
	require.Empty(t, change.Password)
	hashedChange, err := base64.StdEncoding.DecodeString(change.HashedPassword)
	require.NoError(t, err)
	require.NoError(t, bcrypt.CompareHashAndPassword(hashedChange, []byte("change-secret")))
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

func TestFollowerAuthStatusProxiesToLeader(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()

	request := &etcdserverpb.AuthStatusRequest{}
	called := false
	server.peers = testPeerService{
		isLeader:     false,
		proxyEnabled: true,
		authStatusFn: func(_ context.Context, got *etcdserverpb.AuthStatusRequest) (*etcdserverpb.AuthStatusResponse, error) {
			called = true
			require.Same(t, request, got)
			return &etcdserverpb.AuthStatusResponse{
				Header: proxiedResponseHeader(server, 123), Enabled: true, AuthRevision: 17,
			}, nil
		},
	}

	response, err := server.AuthStatus(context.Background(), request)
	require.NoError(t, err)
	require.True(t, called)
	require.True(t, response.GetEnabled())
	require.Equal(t, uint64(17), response.GetAuthRevision())
	require.Equal(t, int64(123), response.GetHeader().GetRevision())
}

func TestFollowerRejectsZeroAuthStatusProxyRevision(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()
	rec := &recordingMetrics{}
	server.metricCli = rec
	initAuthProxyIntegrityMetrics(rec)
	server.peers = testPeerService{
		isLeader: false, proxyEnabled: true,
		authStatusFn: func(context.Context, *etcdserverpb.AuthStatusRequest) (*etcdserverpb.AuthStatusResponse, error) {
			return &etcdserverpb.AuthStatusResponse{Header: proxiedResponseHeader(server, 2), Enabled: true}, nil
		},
	}

	response, err := server.AuthStatus(context.Background(), &etcdserverpb.AuthStatusRequest{})
	require.Nil(t, response)
	require.Equal(t, codes.DataLoss, status.Code(err))
	require.Equal(t, []interface{}{int64(0), 1}, recordedAuthProxyIntegrityValues(rec, authProxyActionStatus))
}

func TestFollowerAuthReadsRejectInvalidProxyResults(t *testing.T) {
	for _, tc := range []struct {
		name      string
		action    string
		configure func(*testPeerService, string)
		invoke    func(*RPCServer) (any, error)
	}{
		{
			name: "auth_status", action: authProxyActionStatus,
			configure: func(peers *testPeerService, shape string) {
				peers.authStatusFn = func(context.Context, *etcdserverpb.AuthStatusRequest) (*etcdserverpb.AuthStatusResponse, error) {
					if shape == "mixed" {
						return &etcdserverpb.AuthStatusResponse{}, errors.New("mixed")
					}
					if shape == "missing_header" {
						return &etcdserverpb.AuthStatusResponse{}, nil
					}
					return nil, nil
				}
			},
			invoke: func(server *RPCServer) (any, error) {
				return server.AuthStatus(context.Background(), &etcdserverpb.AuthStatusRequest{})
			},
		},
		{
			name: "authenticate", action: authProxyActionAuthenticate,
			configure: func(peers *testPeerService, shape string) {
				peers.authenticateFn = func(context.Context, *etcdserverpb.AuthenticateRequest) (*etcdserverpb.AuthenticateResponse, error) {
					if shape == "mixed" {
						return &etcdserverpb.AuthenticateResponse{}, errors.New("mixed")
					}
					if shape == "missing_header" {
						return &etcdserverpb.AuthenticateResponse{}, nil
					}
					return nil, nil
				}
			},
			invoke: func(server *RPCServer) (any, error) {
				return server.Authenticate(context.Background(), &etcdserverpb.AuthenticateRequest{Name: "user", Password: "secret"})
			},
		},
		{
			name: "user_get", action: authProxyActionUserGet,
			configure: func(peers *testPeerService, shape string) {
				peers.userGetFn = func(context.Context, *etcdserverpb.AuthUserGetRequest) (*etcdserverpb.AuthUserGetResponse, error) {
					if shape == "mixed" {
						return &etcdserverpb.AuthUserGetResponse{}, errors.New("mixed")
					}
					if shape == "missing_header" {
						return &etcdserverpb.AuthUserGetResponse{}, nil
					}
					return nil, nil
				}
			},
			invoke: func(server *RPCServer) (any, error) {
				return server.UserGet(context.Background(), &etcdserverpb.AuthUserGetRequest{Name: "user"})
			},
		},
		{
			name: "user_list", action: authProxyActionUserList,
			configure: func(peers *testPeerService, shape string) {
				peers.userListFn = func(context.Context, *etcdserverpb.AuthUserListRequest) (*etcdserverpb.AuthUserListResponse, error) {
					if shape == "mixed" {
						return &etcdserverpb.AuthUserListResponse{}, errors.New("mixed")
					}
					if shape == "missing_header" {
						return &etcdserverpb.AuthUserListResponse{}, nil
					}
					return nil, nil
				}
			},
			invoke: func(server *RPCServer) (any, error) {
				return server.UserList(context.Background(), &etcdserverpb.AuthUserListRequest{})
			},
		},
		{
			name: "role_get", action: authProxyActionRoleGet,
			configure: func(peers *testPeerService, shape string) {
				peers.roleGetFn = func(context.Context, *etcdserverpb.AuthRoleGetRequest) (*etcdserverpb.AuthRoleGetResponse, error) {
					if shape == "mixed" {
						return &etcdserverpb.AuthRoleGetResponse{}, errors.New("mixed")
					}
					if shape == "missing_header" {
						return &etcdserverpb.AuthRoleGetResponse{}, nil
					}
					return nil, nil
				}
			},
			invoke: func(server *RPCServer) (any, error) {
				return server.RoleGet(context.Background(), &etcdserverpb.AuthRoleGetRequest{Role: "role"})
			},
		},
		{
			name: "role_list", action: authProxyActionRoleList,
			configure: func(peers *testPeerService, shape string) {
				peers.roleListFn = func(context.Context, *etcdserverpb.AuthRoleListRequest) (*etcdserverpb.AuthRoleListResponse, error) {
					if shape == "mixed" {
						return &etcdserverpb.AuthRoleListResponse{}, errors.New("mixed")
					}
					if shape == "missing_header" {
						return &etcdserverpb.AuthRoleListResponse{}, nil
					}
					return nil, nil
				}
			},
			invoke: func(server *RPCServer) (any, error) {
				return server.RoleList(context.Background(), &etcdserverpb.AuthRoleListRequest{})
			},
		},
	} {
		for _, shape := range []string{"nil", "mixed", "missing_header"} {
			t.Run(tc.name+"/"+shape, func(t *testing.T) {
				server, closeFn := newTestRPCServer(t)
				defer closeFn()
				rec := &recordingMetrics{}
				server.metricCli = rec
				initAuthProxyIntegrityMetrics(rec)
				peers := testPeerService{isLeader: false, proxyEnabled: true}
				tc.configure(&peers, shape)
				server.peers = peers

				response, err := tc.invoke(server)
				require.Nil(t, response)
				require.Equal(t, codes.DataLoss, status.Code(err))
				require.Equal(t, []interface{}{int64(0), 1}, recordedAuthProxyIntegrityValues(rec, tc.action))
			})
		}
	}
}

func TestFollowerAuthReadsProxyToLeader(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()

	var calls []string
	server.peers = testPeerService{
		isLeader: false, proxyEnabled: true,
		userGetFn: func(_ context.Context, req *etcdserverpb.AuthUserGetRequest) (*etcdserverpb.AuthUserGetResponse, error) {
			calls = append(calls, "user-get:"+req.GetName())
			return &etcdserverpb.AuthUserGetResponse{Header: proxiedResponseHeader(server, 1), Roles: []string{"r"}}, nil
		},
		userListFn: func(context.Context, *etcdserverpb.AuthUserListRequest) (*etcdserverpb.AuthUserListResponse, error) {
			calls = append(calls, "user-list")
			return &etcdserverpb.AuthUserListResponse{Header: proxiedResponseHeader(server, 2), Users: []string{"u"}}, nil
		},
		roleGetFn: func(_ context.Context, req *etcdserverpb.AuthRoleGetRequest) (*etcdserverpb.AuthRoleGetResponse, error) {
			calls = append(calls, "role-get:"+req.GetRole())
			return &etcdserverpb.AuthRoleGetResponse{Header: proxiedResponseHeader(server, 3), Perm: []*authpb.Permission{
				{PermType: authpb.READ, Key: []byte("same"), RangeEnd: []byte("same-b")},
				{PermType: authpb.WRITE, Key: []byte("same"), RangeEnd: []byte("same-c")},
				{PermType: authpb.WRITE, Key: []byte("same"), RangeEnd: []byte("same-c")},
			}}, nil
		},
		roleListFn: func(context.Context, *etcdserverpb.AuthRoleListRequest) (*etcdserverpb.AuthRoleListResponse, error) {
			calls = append(calls, "role-list")
			return &etcdserverpb.AuthRoleListResponse{Header: proxiedResponseHeader(server, 4), Roles: []string{"r"}}, nil
		},
	}

	user, err := server.UserGet(context.Background(), &etcdserverpb.AuthUserGetRequest{Name: "u"})
	require.NoError(t, err)
	require.Equal(t, []string{"r"}, user.GetRoles())
	users, err := server.UserList(context.Background(), &etcdserverpb.AuthUserListRequest{})
	require.NoError(t, err)
	require.Equal(t, []string{"u"}, users.GetUsers())
	role, err := server.RoleGet(context.Background(), &etcdserverpb.AuthRoleGetRequest{Role: "r"})
	require.NoError(t, err)
	require.Len(t, role.GetPerm(), 3)
	roles, err := server.RoleList(context.Background(), &etcdserverpb.AuthRoleListRequest{})
	require.NoError(t, err)
	require.Equal(t, []string{"r"}, roles.GetRoles())
	require.Equal(t, []string{"user-get:u", "user-list", "role-get:r", "role-list"}, calls)
}

func TestFollowerRejectsInvalidAuthNameCollectionProxyPayload(t *testing.T) {
	for _, tt := range []struct {
		name      string
		action    string
		configure func(*testPeerService)
		invoke    func(*RPCServer) (any, error)
	}{
		{
			name: "unsorted user roles", action: authProxyActionUserGet,
			configure: func(peers *testPeerService) {
				peers.userGetFn = func(context.Context, *etcdserverpb.AuthUserGetRequest) (*etcdserverpb.AuthUserGetResponse, error) {
					return &etcdserverpb.AuthUserGetResponse{Header: txnHeader(2), Roles: []string{"writer", "reader"}}, nil
				}
			},
			invoke: func(server *RPCServer) (any, error) {
				return server.UserGet(context.Background(), &etcdserverpb.AuthUserGetRequest{Name: "alice"})
			},
		},
		{
			name: "duplicate user role", action: authProxyActionUserGet,
			configure: func(peers *testPeerService) {
				peers.userGetFn = func(context.Context, *etcdserverpb.AuthUserGetRequest) (*etcdserverpb.AuthUserGetResponse, error) {
					return &etcdserverpb.AuthUserGetResponse{Header: txnHeader(2), Roles: []string{"reader", "reader"}}, nil
				}
			},
			invoke: func(server *RPCServer) (any, error) {
				return server.UserGet(context.Background(), &etcdserverpb.AuthUserGetRequest{Name: "alice"})
			},
		},
		{
			name: "empty user", action: authProxyActionUserList,
			configure: func(peers *testPeerService) {
				peers.userListFn = func(context.Context, *etcdserverpb.AuthUserListRequest) (*etcdserverpb.AuthUserListResponse, error) {
					return &etcdserverpb.AuthUserListResponse{Header: txnHeader(2), Users: []string{""}}, nil
				}
			},
			invoke: func(server *RPCServer) (any, error) {
				return server.UserList(context.Background(), &etcdserverpb.AuthUserListRequest{})
			},
		},
		{
			name: "duplicate role", action: authProxyActionRoleList,
			configure: func(peers *testPeerService) {
				peers.roleListFn = func(context.Context, *etcdserverpb.AuthRoleListRequest) (*etcdserverpb.AuthRoleListResponse, error) {
					return &etcdserverpb.AuthRoleListResponse{Header: txnHeader(2), Roles: []string{"reader", "reader"}}, nil
				}
			},
			invoke: func(server *RPCServer) (any, error) {
				return server.RoleList(context.Background(), &etcdserverpb.AuthRoleListRequest{})
			},
		},
		{
			name: "unsorted users", action: authProxyActionUserList,
			configure: func(peers *testPeerService) {
				peers.userListFn = func(context.Context, *etcdserverpb.AuthUserListRequest) (*etcdserverpb.AuthUserListResponse, error) {
					return &etcdserverpb.AuthUserListResponse{Header: txnHeader(2), Users: []string{"root", "alice"}}, nil
				}
			},
			invoke: func(server *RPCServer) (any, error) {
				return server.UserList(context.Background(), &etcdserverpb.AuthUserListRequest{})
			},
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			server, closeFn := newTestRPCServer(t)
			defer closeFn()
			rec := &recordingMetrics{}
			server.metricCli = rec
			initAuthProxyIntegrityMetrics(rec)
			peers := testPeerService{isLeader: false, proxyEnabled: true}
			tt.configure(&peers)
			server.peers = peers

			response, err := tt.invoke(server)
			require.Nil(t, response)
			require.Equal(t, codes.DataLoss, status.Code(err))
			require.Equal(t, []interface{}{int64(0), 1}, recordedAuthProxyIntegrityValues(rec, tt.action))
		})
	}
}

func TestFollowerRejectsInvalidRoleGetProxyPayload(t *testing.T) {
	for _, tt := range []struct {
		name     string
		request  *etcdserverpb.AuthRoleGetRequest
		response *etcdserverpb.AuthRoleGetResponse
	}{
		{name: "nil permission", request: &etcdserverpb.AuthRoleGetRequest{Role: "reader"}, response: &etcdserverpb.AuthRoleGetResponse{Header: txnHeader(2), Perm: []*authpb.Permission{nil}}},
		{name: "invalid range", request: &etcdserverpb.AuthRoleGetRequest{Role: "reader"}, response: &etcdserverpb.AuthRoleGetResponse{Header: txnHeader(2), Perm: []*authpb.Permission{{PermType: authpb.READ, Key: []byte("z"), RangeEnd: []byte("a")}}}},
		{name: "unsorted", request: &etcdserverpb.AuthRoleGetRequest{Role: "reader"}, response: &etcdserverpb.AuthRoleGetResponse{Header: txnHeader(2), Perm: []*authpb.Permission{{PermType: authpb.READ, Key: []byte("b")}, {PermType: authpb.READ, Key: []byte("a")}}}},
		{name: "non-canonical root", request: &etcdserverpb.AuthRoleGetRequest{Role: "root"}, response: &etcdserverpb.AuthRoleGetResponse{Header: txnHeader(2), Perm: []*authpb.Permission{{PermType: authpb.READWRITE, Key: []byte("a"), RangeEnd: []byte{0}}}}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			server, closeFn := newTestRPCServer(t)
			defer closeFn()
			rec := &recordingMetrics{}
			server.metricCli = rec
			initAuthProxyIntegrityMetrics(rec)
			server.peers = testPeerService{
				isLeader: false, proxyEnabled: true,
				roleGetFn: func(context.Context, *etcdserverpb.AuthRoleGetRequest) (*etcdserverpb.AuthRoleGetResponse, error) {
					return tt.response, nil
				},
			}

			response, err := server.RoleGet(context.Background(), tt.request)
			require.Nil(t, response)
			require.Equal(t, codes.DataLoss, status.Code(err))
			require.Equal(t, []interface{}{int64(0), 1}, recordedAuthProxyIntegrityValues(rec, authProxyActionRoleGet))
		})
	}
}

func TestFollowerAuthenticateProxiesAndClearsPassword(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()

	request := &etcdserverpb.AuthenticateRequest{Name: "alice", Password: "secret"}
	server.peers = testPeerService{
		isLeader: false, proxyEnabled: true,
		syncReadFn: func(context.Context) error {
			t.Fatal("proxying follower must not execute Authenticate read barrier locally")
			return nil
		},
		authenticateFn: func(_ context.Context, got *etcdserverpb.AuthenticateRequest) (*etcdserverpb.AuthenticateResponse, error) {
			require.Same(t, request, got)
			require.Equal(t, "secret", got.GetPassword())
			return nil, rpctypes.ErrAuthNotEnabled
		},
	}

	response, err := server.Authenticate(context.Background(), request)
	require.Nil(t, response)
	require.ErrorIs(t, err, rpctypes.ErrAuthNotEnabled)
	require.Empty(t, request.GetPassword())
}

func TestFollowerPasswordMutationsClearPlaintext(t *testing.T) {
	proxyErr := errors.New("injected auth proxy failure")

	t.Run("user add", func(t *testing.T) {
		server, closeFn := newTestRPCServer(t)
		defer closeFn()

		request := &etcdserverpb.AuthUserAddRequest{Name: "alice", Password: "add-secret"}
		server.peers = testPeerService{
			isLeader: false, proxyEnabled: true,
			userAddFn: func(_ context.Context, got *etcdserverpb.AuthUserAddRequest) (*etcdserverpb.AuthUserAddResponse, error) {
				require.Same(t, request, got)
				require.Equal(t, "add-secret", got.GetPassword())
				return nil, proxyErr
			},
		}

		response, err := server.UserAdd(context.Background(), request)
		require.Nil(t, response)
		require.ErrorIs(t, err, proxyErr)
		require.Empty(t, request.GetPassword())
	})

	t.Run("user change password", func(t *testing.T) {
		server, closeFn := newTestRPCServer(t)
		defer closeFn()

		request := &etcdserverpb.AuthUserChangePasswordRequest{Name: "alice", Password: "change-secret"}
		server.peers = testPeerService{
			isLeader: false, proxyEnabled: true,
			changePasswordFn: func(_ context.Context, got *etcdserverpb.AuthUserChangePasswordRequest) (*etcdserverpb.AuthUserChangePasswordResponse, error) {
				require.Same(t, request, got)
				require.Equal(t, "change-secret", got.GetPassword())
				return nil, proxyErr
			},
		}

		response, err := server.UserChangePassword(context.Background(), request)
		require.Nil(t, response)
		require.ErrorIs(t, err, proxyErr)
		require.Empty(t, request.GetPassword())
	})
}

func TestFollowerRejectsEmptyAuthenticateProxyToken(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()
	rec := &recordingMetrics{}
	server.metricCli = rec
	initAuthProxyIntegrityMetrics(rec)
	request := &etcdserverpb.AuthenticateRequest{Name: "alice", Password: "secret"}
	server.peers = testPeerService{
		isLeader: false, proxyEnabled: true,
		authenticateFn: func(context.Context, *etcdserverpb.AuthenticateRequest) (*etcdserverpb.AuthenticateResponse, error) {
			return &etcdserverpb.AuthenticateResponse{Header: txnHeader(2)}, nil
		},
	}

	response, err := server.Authenticate(context.Background(), request)
	require.Nil(t, response)
	require.Equal(t, codes.DataLoss, status.Code(err))
	require.Empty(t, request.Password)
	require.Equal(t, []interface{}{int64(0), 1}, recordedAuthProxyIntegrityValues(rec, authProxyActionAuthenticate))
}

func TestBearerPrefixedAuthTokenMatchesEtcd(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()
	ctx := context.Background()
	setupAuthKVUser(t, server)

	auth, err := server.Authenticate(ctx, &etcdserverpb.AuthenticateRequest{Name: "root", Password: "root-secret"})
	require.NoError(t, err)

	bearerCtx := metadata.NewIncomingContext(ctx, metadata.Pairs(rpctypes.TokenFieldNameGRPC, "Bearer "+auth.Token))
	response, err := server.UserList(bearerCtx, &etcdserverpb.AuthUserListRequest{})
	require.NoError(t, err)
	require.Contains(t, response.Users, "root")

	lowercaseBearerCtx := metadata.NewIncomingContext(ctx, metadata.Pairs(rpctypes.TokenFieldNameGRPC, "bearer "+auth.Token))
	response, err = server.UserList(lowercaseBearerCtx, &etcdserverpb.AuthUserListRequest{})
	require.Nil(t, response)
	requireAuthRPCError(t, err, rpctypes.ErrInvalidAuthToken, codes.Unknown, "etcdserver: invalid auth token")
}

func TestAuthReadRPCsReloadSnapshotAfterBarrier(t *testing.T) {
	t.Run("auth status", func(t *testing.T) {
		server, closeFn := newTestRPCServer(t)
		defer closeFn()
		ctx := context.Background()
		require.NoError(t, server.auth.userAdd(ctx, &etcdserverpb.AuthUserAddRequest{
			Name: "root", Options: &authpb.UserAddOptions{NoPassword: true},
		}))
		require.NoError(t, server.auth.roleAdd(ctx, "root"))
		require.NoError(t, server.auth.userGrantRole(ctx, "root", "root"))
		server.peers = testPeerService{syncReadFn: func(context.Context) error {
			return server.auth.enable(ctx)
		}}

		response, err := server.AuthStatus(ctx, &etcdserverpb.AuthStatusRequest{})
		require.NoError(t, err)
		require.True(t, response.Enabled)
	})

	t.Run("user get", func(t *testing.T) {
		server, closeFn := newTestRPCServer(t)
		defer closeFn()
		ctx := context.Background()
		server.peers = testPeerService{syncReadFn: func(context.Context) error {
			return server.auth.userAdd(ctx, &etcdserverpb.AuthUserAddRequest{
				Name: "created-during-barrier", Options: &authpb.UserAddOptions{NoPassword: true},
			})
		}}

		response, err := server.UserGet(ctx, &etcdserverpb.AuthUserGetRequest{Name: "created-during-barrier"})
		require.NoError(t, err)
		require.Empty(t, response.Roles)
	})

	t.Run("user list", func(t *testing.T) {
		server, closeFn := newTestRPCServer(t)
		defer closeFn()
		ctx := context.Background()
		server.peers = testPeerService{syncReadFn: func(context.Context) error {
			return server.auth.userAdd(ctx, &etcdserverpb.AuthUserAddRequest{
				Name: "listed-after-barrier", Options: &authpb.UserAddOptions{NoPassword: true},
			})
		}}

		response, err := server.UserList(ctx, &etcdserverpb.AuthUserListRequest{})
		require.NoError(t, err)
		require.Equal(t, []string{"listed-after-barrier"}, response.Users)
	})

	t.Run("role get", func(t *testing.T) {
		server, closeFn := newTestRPCServer(t)
		defer closeFn()
		ctx := context.Background()
		server.peers = testPeerService{syncReadFn: func(context.Context) error {
			return server.auth.roleAdd(ctx, "created-during-barrier")
		}}

		response, err := server.RoleGet(ctx, &etcdserverpb.AuthRoleGetRequest{Role: "created-during-barrier"})
		require.NoError(t, err)
		require.Empty(t, response.Perm)
	})

	t.Run("role list", func(t *testing.T) {
		server, closeFn := newTestRPCServer(t)
		defer closeFn()
		ctx := context.Background()
		server.peers = testPeerService{syncReadFn: func(context.Context) error {
			return server.auth.roleAdd(ctx, "listed-after-barrier")
		}}

		response, err := server.RoleList(ctx, &etcdserverpb.AuthRoleListRequest{})
		require.NoError(t, err)
		require.Equal(t, []string{"listed-after-barrier"}, response.Roles)
	})
}

func TestAuthReadRPCsReauthorizeAfterBarrier(t *testing.T) {
	t.Run("user list admin revoked", func(t *testing.T) {
		server, closeFn := newTestRPCServer(t)
		defer closeFn()
		ctx := context.Background()
		setupAuthKVUser(t, server)
		rootAuth, err := server.Authenticate(ctx, &etcdserverpb.AuthenticateRequest{
			Name: "root", Password: "root-secret",
		})
		require.NoError(t, err)
		rootCtx := metadata.NewIncomingContext(ctx, metadata.Pairs(
			rpctypes.TokenFieldNameGRPC, rootAuth.Token,
		))
		_, err = server.UserAdd(rootCtx, &etcdserverpb.AuthUserAddRequest{
			Name: "operator", Password: "operator-secret",
		})
		require.NoError(t, err)
		_, err = server.UserGrantRole(rootCtx, &etcdserverpb.AuthUserGrantRoleRequest{
			User: "operator", Role: "root",
		})
		require.NoError(t, err)
		operatorAuth, err := server.Authenticate(ctx, &etcdserverpb.AuthenticateRequest{
			Name: "operator", Password: "operator-secret",
		})
		require.NoError(t, err)
		operatorCtx := metadata.NewIncomingContext(ctx, metadata.Pairs(
			rpctypes.TokenFieldNameGRPC, operatorAuth.Token,
		))
		server.peers = testPeerService{syncReadFn: func(context.Context) error {
			return server.auth.userRevokeRole(ctx, "operator", "root")
		}}

		_, err = server.UserList(operatorCtx, &etcdserverpb.AuthUserListRequest{})
		requireAuthRPCError(t, err, rpctypes.ErrPermissionDenied, codes.Unknown, "etcdserver: permission denied")
	})

	t.Run("role get membership revoked", func(t *testing.T) {
		server, closeFn := newTestRPCServer(t)
		defer closeFn()
		ctx := context.Background()
		aliceCtx := setupAuthKVUser(t, server)
		server.peers = testPeerService{syncReadFn: func(context.Context) error {
			return server.auth.userRevokeRole(ctx, "alice", "allowed")
		}}

		_, err := server.RoleGet(aliceCtx, &etcdserverpb.AuthRoleGetRequest{Role: "allowed"})
		requireAuthRPCError(t, err, rpctypes.ErrPermissionDenied, codes.Unknown, "etcdserver: permission denied")
	})
}

func TestAuthenticateRechecksTokenAfterHeaderBarrier(t *testing.T) {
	t.Run("password changed", func(t *testing.T) {
		server, closeFn := newTestRPCServer(t)
		defer closeFn()
		ctx := context.Background()
		setupAuthKVUser(t, server)
		var barriers atomic.Int32
		server.peers = testPeerService{syncReadFn: func(context.Context) error {
			if barriers.Add(1) == 2 {
				return server.auth.userChangePassword(ctx, "root", "changed-secret", "")
			}
			return nil
		}}

		_, err := server.Authenticate(ctx, &etcdserverpb.AuthenticateRequest{
			Name: "root", Password: "root-secret",
		})
		requireAuthRPCError(t, err, rpctypes.ErrAuthFailed, codes.Unknown, "etcdserver: authentication failed, invalid user ID or password")

		response, err := server.Authenticate(ctx, &etcdserverpb.AuthenticateRequest{
			Name: "root", Password: "changed-secret",
		})
		require.NoError(t, err)
		_, err = server.tokens.verify(ctx, response.Token)
		require.NoError(t, err)
	})

	t.Run("unrelated auth mutation", func(t *testing.T) {
		server, closeFn := newTestRPCServer(t)
		defer closeFn()
		ctx := context.Background()
		setupAuthKVUser(t, server)
		var barriers atomic.Int32
		server.peers = testPeerService{syncReadFn: func(context.Context) error {
			if barriers.Add(1) == 2 {
				return server.auth.roleAdd(ctx, "created-during-header-barrier")
			}
			return nil
		}}

		response, err := server.Authenticate(ctx, &etcdserverpb.AuthenticateRequest{
			Name: "root", Password: "root-secret",
		})
		require.NoError(t, err)
		claims, err := server.tokens.verify(ctx, response.Token)
		require.NoError(t, err)
		snapshot, err := server.tokens.snapshots.current(ctx)
		require.NoError(t, err)
		require.Equal(t, snapshot.Config.Revision, claims.Revision)
		require.NotNil(t, snapshot.Roles["created-during-header-barrier"])
	})
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
