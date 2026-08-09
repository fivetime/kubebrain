package etcd

import (
	"context"
	"encoding/base64"
	"errors"
	"time"

	"go.etcd.io/etcd/api/v3/authpb"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
	"golang.org/x/crypto/bcrypt"
	"google.golang.org/protobuf/proto"
)

func (s *RPCServer) authRPCHeader(ctx context.Context) (*etcdserverpb.ResponseHeader, error) {
	if err := s.peers.SyncReadRevision(ctx); err != nil {
		return nil, readBarrierStatusErr(err)
	}
	revision, err := safeBackendRevision(ctx, s.backend)
	if err != nil {
		return nil, err
	}
	return &etcdserverpb.ResponseHeader{Revision: int64(revision)}, nil
}

func (s *RPCServer) AuthEnable(ctx context.Context, _ *etcdserverpb.AuthEnableRequest) (_ *etcdserverpb.AuthEnableResponse, retErr error) {
	snapshot, err := s.tokens.snapshots.current(ctx)
	if err != nil {
		return nil, err
	}
	if snapshot.Config.Enabled {
		if _, err = s.authAdminSnapshot(ctx); err != nil {
			return nil, err
		}
	}
	header, err := s.authRPCHeader(ctx)
	if err != nil {
		return nil, err
	}
	defer beginEtcdApply(s.metricCli, "AuthEnable", &retErr)()
	s.nativeAuthBoundary.Lock()
	defer s.nativeAuthBoundary.Unlock()
	if err = s.auth.enable(ctx); err != nil {
		return nil, err
	}
	return &etcdserverpb.AuthEnableResponse{Header: header}, nil
}

func (s *RPCServer) AuthDisable(ctx context.Context, _ *etcdserverpb.AuthDisableRequest) (_ *etcdserverpb.AuthDisableResponse, retErr error) {
	if _, err := s.authAdminSnapshot(ctx); err != nil {
		return nil, err
	}
	header, err := s.authRPCHeader(ctx)
	if err != nil {
		return nil, err
	}
	defer beginEtcdApply(s.metricCli, "AuthDisable", &retErr)()
	s.nativeAuthBoundary.Lock()
	defer s.nativeAuthBoundary.Unlock()
	if err := s.auth.disable(ctx); err != nil {
		return nil, err
	}
	return &etcdserverpb.AuthDisableResponse{Header: header}, nil
}

func (s *RPCServer) authAdminSnapshot(ctx context.Context) (*authSnapshot, error) {
	snapshot, err := s.tokens.snapshots.current(ctx)
	if err != nil {
		return nil, err
	}
	if !snapshot.Config.Enabled {
		return snapshot, nil
	}
	caller, err := s.authCallerFromContext(ctx)
	if err != nil {
		return nil, err
	}
	if err := caller.adminError(); err != nil {
		return nil, err
	}
	return caller.snapshot, nil
}

func (s *RPCServer) authStatusSnapshot(ctx context.Context) (*authSnapshot, error) {
	snapshot, err := s.tokens.snapshots.current(ctx)
	if err != nil {
		return nil, err
	}
	if snapshot.Config.Enabled {
		if credential, ok := authCredentialFromContext(ctx); ok {
			token, err := authTokenFromCredential(credential)
			if err != nil {
				return nil, err
			}
			if _, err = s.tokens.verify(ctx, token); err != nil {
				return nil, err
			}
			snapshot, err = s.tokens.snapshots.current(ctx)
			if err != nil {
				return nil, err
			}
		}
	}
	return snapshot, nil
}

func (s *RPCServer) AuthStatus(ctx context.Context, _ *etcdserverpb.AuthStatusRequest) (_ *etcdserverpb.AuthStatusResponse, retErr error) {
	if _, err := s.authStatusSnapshot(ctx); err != nil {
		return nil, err
	}
	header, err := s.authRPCHeader(ctx)
	if err != nil {
		return nil, err
	}
	// The first snapshot validates authentication metadata before the simulated
	// raft boundary; the state read below corresponds to AuthStatus's apply.
	defer beginEtcdApply(s.metricCli, "unknown", &retErr)()
	snapshot, err := s.authStatusSnapshot(ctx)
	if err != nil {
		return nil, err
	}
	// Upstream's dispatch does not assign an op for AuthStatus and therefore
	// exposes the historical label value "unknown". Preserve that contract.
	return &etcdserverpb.AuthStatusResponse{Header: header, Enabled: snapshot.Config.Enabled, AuthRevision: snapshot.Config.Revision}, nil
}

func (s *RPCServer) Authenticate(ctx context.Context, request *etcdserverpb.AuthenticateRequest) (_ *etcdserverpb.AuthenticateResponse, retErr error) {
	if err := s.peers.SyncReadRevision(ctx); err != nil {
		return nil, readBarrierStatusErr(err)
	}
	defer func() {
		if request != nil {
			request.Password = ""
		}
	}()
	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		token, err := s.tokens.authenticateWithIssue(ctx, request.Name, request.Password,
			func(issue func() (string, error)) (string, error) {
				start := time.Now()
				token, issueErr := issue()
				emitEtcdApplyDuration(s.metricCli, "Authenticate", time.Since(start), issueErr)
				return token, issueErr
			})
		if err != nil {
			return nil, err
		}
		header, err := s.authRPCHeader(ctx)
		if err != nil {
			return nil, err
		}
		claims, err := s.tokens.verify(ctx, token)
		if err != nil {
			if errors.Is(err, rpctypes.ErrInvalidAuthToken) {
				continue
			}
			return nil, err
		}
		snapshot, err := s.tokens.snapshots.current(ctx)
		if err != nil {
			return nil, err
		}
		if claims.Revision != snapshot.Config.Revision {
			continue
		}
		return &etcdserverpb.AuthenticateResponse{Header: header, Token: token}, nil
	}
}

func (s *RPCServer) UserAdd(ctx context.Context, request *etcdserverpb.AuthUserAddRequest) (_ *etcdserverpb.AuthUserAddResponse, retErr error) {
	if request.Options == nil || !request.Options.NoPassword {
		hashedPassword, err := bcrypt.GenerateFromPassword([]byte(request.Password), s.auth.bcryptCost)
		if err != nil {
			return nil, err
		}
		request.HashedPassword = base64.StdEncoding.EncodeToString(hashedPassword)
		request.Password = ""
	}
	if _, err := s.authAdminSnapshot(ctx); err != nil {
		return nil, err
	}
	header, err := s.authRPCHeader(ctx)
	if err != nil {
		return nil, err
	}
	defer beginEtcdApply(s.metricCli, "AuthUserAdd", &retErr)()
	if err := s.auth.userAddHashed(ctx, request); err != nil {
		return nil, err
	}
	return &etcdserverpb.AuthUserAddResponse{Header: header}, nil
}

func (s *RPCServer) userReadSnapshot(ctx context.Context, username string) (*authSnapshot, error) {
	snapshot, err := s.tokens.snapshots.current(ctx)
	if err != nil {
		return nil, err
	}
	if snapshot.Config.Enabled {
		caller, callerErr := s.authCallerFromContext(ctx)
		if callerErr != nil {
			if !errors.Is(callerErr, rpctypes.ErrUserEmpty) || username != "" {
				return nil, callerErr
			}
			caller = &authCaller{snapshot: snapshot}
		}
		adminErr := caller.adminError()
		if adminErr != nil && caller.username != username {
			return nil, adminErr
		}
		snapshot = caller.snapshot
	}
	return snapshot, nil
}

func (s *RPCServer) UserGet(ctx context.Context, request *etcdserverpb.AuthUserGetRequest) (_ *etcdserverpb.AuthUserGetResponse, retErr error) {
	if err := s.validateEtcdApplyAuthInfo(ctx); err != nil {
		return nil, err
	}
	header, err := s.authRPCHeader(ctx)
	if err != nil {
		return nil, err
	}
	defer beginEtcdApply(s.metricCli, "AuthUserGet", &retErr)()
	snapshot, err := s.userReadSnapshot(ctx, request.Name)
	if err != nil {
		return nil, err
	}
	user := snapshot.Users[request.Name]
	if user == nil {
		return nil, rpctypes.ErrUserNotFound
	}
	return &etcdserverpb.AuthUserGetResponse{Header: header, Roles: append([]string(nil), user.Roles...)}, nil
}

func (s *RPCServer) UserList(ctx context.Context, _ *etcdserverpb.AuthUserListRequest) (_ *etcdserverpb.AuthUserListResponse, retErr error) {
	if _, err := s.authAdminSnapshot(ctx); err != nil {
		return nil, err
	}
	header, err := s.authRPCHeader(ctx)
	if err != nil {
		return nil, err
	}
	snapshot, err := s.authAdminSnapshot(ctx)
	if err != nil {
		return nil, err
	}
	defer beginEtcdApply(s.metricCli, "AuthUserList", &retErr)()
	return &etcdserverpb.AuthUserListResponse{Header: header, Users: authUserNames(snapshot)}, nil
}

func (s *RPCServer) UserDelete(ctx context.Context, request *etcdserverpb.AuthUserDeleteRequest) (_ *etcdserverpb.AuthUserDeleteResponse, retErr error) {
	if _, err := s.authAdminSnapshot(ctx); err != nil {
		return nil, err
	}
	header, err := s.authRPCHeader(ctx)
	if err != nil {
		return nil, err
	}
	defer beginEtcdApply(s.metricCli, "AuthUserDelete", &retErr)()
	if err := s.auth.userDelete(ctx, request.Name); err != nil {
		return nil, err
	}
	return &etcdserverpb.AuthUserDeleteResponse{Header: header}, nil
}

func (s *RPCServer) UserChangePassword(ctx context.Context, request *etcdserverpb.AuthUserChangePasswordRequest) (_ *etcdserverpb.AuthUserChangePasswordResponse, retErr error) {
	if request.Password != "" {
		hashedPassword, err := bcrypt.GenerateFromPassword([]byte(request.Password), s.auth.bcryptCost)
		if err != nil {
			return nil, err
		}
		request.HashedPassword = base64.StdEncoding.EncodeToString(hashedPassword)
		request.Password = ""
	}
	if _, err := s.authAdminSnapshot(ctx); err != nil {
		return nil, err
	}
	header, err := s.authRPCHeader(ctx)
	if err != nil {
		return nil, err
	}
	defer beginEtcdApply(s.metricCli, "AuthUserChangePassword", &retErr)()
	if err := s.auth.userChangePassword(ctx, request.Name, request.Password, request.HashedPassword); err != nil {
		return nil, err
	}
	return &etcdserverpb.AuthUserChangePasswordResponse{Header: header}, nil
}

func (s *RPCServer) UserGrantRole(ctx context.Context, request *etcdserverpb.AuthUserGrantRoleRequest) (_ *etcdserverpb.AuthUserGrantRoleResponse, retErr error) {
	if _, err := s.authAdminSnapshot(ctx); err != nil {
		return nil, err
	}
	header, err := s.authRPCHeader(ctx)
	if err != nil {
		return nil, err
	}
	defer beginEtcdApply(s.metricCli, "AuthUserGrantRole", &retErr)()
	if err := s.auth.userGrantRole(ctx, request.User, request.Role); err != nil {
		return nil, err
	}
	return &etcdserverpb.AuthUserGrantRoleResponse{Header: header}, nil
}

func (s *RPCServer) UserRevokeRole(ctx context.Context, request *etcdserverpb.AuthUserRevokeRoleRequest) (_ *etcdserverpb.AuthUserRevokeRoleResponse, retErr error) {
	if _, err := s.authAdminSnapshot(ctx); err != nil {
		return nil, err
	}
	header, err := s.authRPCHeader(ctx)
	if err != nil {
		return nil, err
	}
	defer beginEtcdApply(s.metricCli, "AuthUserRevokeRole", &retErr)()
	if err := s.auth.userRevokeRole(ctx, request.Name, request.Role); err != nil {
		return nil, err
	}
	return &etcdserverpb.AuthUserRevokeRoleResponse{Header: header}, nil
}

func (s *RPCServer) RoleAdd(ctx context.Context, request *etcdserverpb.AuthRoleAddRequest) (_ *etcdserverpb.AuthRoleAddResponse, retErr error) {
	if _, err := s.authAdminSnapshot(ctx); err != nil {
		return nil, err
	}
	header, err := s.authRPCHeader(ctx)
	if err != nil {
		return nil, err
	}
	defer beginEtcdApply(s.metricCli, "AuthRoleAdd", &retErr)()
	if err := s.auth.roleAdd(ctx, request.Name); err != nil {
		return nil, err
	}
	return &etcdserverpb.AuthRoleAddResponse{Header: header}, nil
}

func (s *RPCServer) roleReadSnapshot(ctx context.Context, roleName string) (*authSnapshot, error) {
	snapshot, err := s.tokens.snapshots.current(ctx)
	if err != nil {
		return nil, err
	}
	if snapshot.Config.Enabled {
		caller, callerErr := s.authCallerFromContext(ctx)
		if callerErr != nil {
			return nil, callerErr
		}
		adminErr := caller.adminError()
		if adminErr != nil && !caller.hasRole(roleName) {
			return nil, adminErr
		}
		snapshot = caller.snapshot
	}
	return snapshot, nil
}

func (s *RPCServer) RoleGet(ctx context.Context, request *etcdserverpb.AuthRoleGetRequest) (_ *etcdserverpb.AuthRoleGetResponse, retErr error) {
	if err := s.validateEtcdApplyAuthInfo(ctx); err != nil {
		return nil, err
	}
	header, err := s.authRPCHeader(ctx)
	if err != nil {
		return nil, err
	}
	defer beginEtcdApply(s.metricCli, "AuthRoleGet", &retErr)()
	snapshot, err := s.roleReadSnapshot(ctx, request.Role)
	if err != nil {
		return nil, err
	}
	role := snapshot.Roles[request.Role]
	if role == nil {
		return nil, rpctypes.ErrRoleNotFound
	}
	permissions := make([]*authpb.Permission, 0, len(role.KeyPermission))
	if request.Role == "root" {
		permissions = append(permissions, &authpb.Permission{PermType: authpb.READWRITE, Key: []byte{}, RangeEnd: []byte{0}})
	} else {
		for _, permission := range role.KeyPermission {
			permissions = append(permissions, proto.Clone(permission).(*authpb.Permission))
		}
	}
	return &etcdserverpb.AuthRoleGetResponse{Header: header, Perm: permissions}, nil
}

func (s *RPCServer) RoleList(ctx context.Context, _ *etcdserverpb.AuthRoleListRequest) (_ *etcdserverpb.AuthRoleListResponse, retErr error) {
	if _, err := s.authAdminSnapshot(ctx); err != nil {
		return nil, err
	}
	header, err := s.authRPCHeader(ctx)
	if err != nil {
		return nil, err
	}
	snapshot, err := s.authAdminSnapshot(ctx)
	if err != nil {
		return nil, err
	}
	defer beginEtcdApply(s.metricCli, "AuthRoleList", &retErr)()
	return &etcdserverpb.AuthRoleListResponse{Header: header, Roles: authRoleNames(snapshot)}, nil
}

func (s *RPCServer) RoleDelete(ctx context.Context, request *etcdserverpb.AuthRoleDeleteRequest) (_ *etcdserverpb.AuthRoleDeleteResponse, retErr error) {
	if _, err := s.authAdminSnapshot(ctx); err != nil {
		return nil, err
	}
	header, err := s.authRPCHeader(ctx)
	if err != nil {
		return nil, err
	}
	defer beginEtcdApply(s.metricCli, "AuthRoleDelete", &retErr)()
	if err := s.auth.roleDelete(ctx, request.Role); err != nil {
		return nil, err
	}
	return &etcdserverpb.AuthRoleDeleteResponse{Header: header}, nil
}

func (s *RPCServer) RoleGrantPermission(ctx context.Context, request *etcdserverpb.AuthRoleGrantPermissionRequest) (_ *etcdserverpb.AuthRoleGrantPermissionResponse, retErr error) {
	if _, err := s.authAdminSnapshot(ctx); err != nil {
		return nil, err
	}
	header, err := s.authRPCHeader(ctx)
	if err != nil {
		return nil, err
	}
	defer beginEtcdApply(s.metricCli, "AuthRoleGrantPermission", &retErr)()
	if err := s.auth.roleGrantPermission(ctx, request.Name, request.Perm); err != nil {
		return nil, err
	}
	return &etcdserverpb.AuthRoleGrantPermissionResponse{Header: header}, nil
}

func (s *RPCServer) RoleRevokePermission(ctx context.Context, request *etcdserverpb.AuthRoleRevokePermissionRequest) (_ *etcdserverpb.AuthRoleRevokePermissionResponse, retErr error) {
	if _, err := s.authAdminSnapshot(ctx); err != nil {
		return nil, err
	}
	header, err := s.authRPCHeader(ctx)
	if err != nil {
		return nil, err
	}
	defer beginEtcdApply(s.metricCli, "AuthRoleRevokePermission", &retErr)()
	if err := s.auth.roleRevokePermission(ctx, request.Role, request.Key, request.RangeEnd); err != nil {
		return nil, err
	}
	return &etcdserverpb.AuthRoleRevokePermissionResponse{Header: header}, nil
}
