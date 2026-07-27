package etcd

import (
	"context"
	"errors"

	"go.etcd.io/etcd/api/v3/authpb"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
	"google.golang.org/protobuf/proto"
)

func (s *RPCServer) authRPCHeader(ctx context.Context) (*etcdserverpb.ResponseHeader, error) {
	if err := s.peers.SyncReadRevision(ctx); err != nil {
		return nil, readBarrierStatusErr(err)
	}
	return &etcdserverpb.ResponseHeader{Revision: int64(s.backend.GetCurrentRevision())}, nil
}

func (s *RPCServer) AuthEnable(ctx context.Context, _ *etcdserverpb.AuthEnableRequest) (*etcdserverpb.AuthEnableResponse, error) {
	snapshot, err := s.tokens.snapshots.current(ctx)
	if err != nil {
		return nil, err
	}
	if snapshot.Config.Enabled {
		if _, err = s.authAdminSnapshot(ctx); err != nil {
			return nil, err
		}
	}
	if err = s.auth.enable(ctx); err != nil {
		return nil, err
	}
	header, err := s.authRPCHeader(ctx)
	if err != nil {
		return nil, err
	}
	return &etcdserverpb.AuthEnableResponse{Header: header}, nil
}

func (s *RPCServer) AuthDisable(ctx context.Context, _ *etcdserverpb.AuthDisableRequest) (*etcdserverpb.AuthDisableResponse, error) {
	if _, err := s.authAdminSnapshot(ctx); err != nil {
		return nil, err
	}
	if err := s.auth.disable(ctx); err != nil {
		return nil, err
	}
	header, err := s.authRPCHeader(ctx)
	if err != nil {
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

func (s *RPCServer) AuthStatus(ctx context.Context, _ *etcdserverpb.AuthStatusRequest) (*etcdserverpb.AuthStatusResponse, error) {
	if _, err := s.authStatusSnapshot(ctx); err != nil {
		return nil, err
	}
	header, err := s.authRPCHeader(ctx)
	if err != nil {
		return nil, err
	}
	snapshot, err := s.authStatusSnapshot(ctx)
	if err != nil {
		return nil, err
	}
	return &etcdserverpb.AuthStatusResponse{Header: header, Enabled: snapshot.Config.Enabled, AuthRevision: snapshot.Config.Revision}, nil
}

func (s *RPCServer) Authenticate(ctx context.Context, request *etcdserverpb.AuthenticateRequest) (*etcdserverpb.AuthenticateResponse, error) {
	if err := s.peers.SyncReadRevision(ctx); err != nil {
		return nil, readBarrierStatusErr(err)
	}
	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		token, err := s.tokens.authenticate(ctx, request.Name, request.Password)
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

func (s *RPCServer) UserAdd(ctx context.Context, request *etcdserverpb.AuthUserAddRequest) (*etcdserverpb.AuthUserAddResponse, error) {
	if _, err := s.authAdminSnapshot(ctx); err != nil {
		return nil, err
	}
	if err := s.auth.userAdd(ctx, request); err != nil {
		return nil, err
	}
	header, err := s.authRPCHeader(ctx)
	if err != nil {
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

func (s *RPCServer) UserGet(ctx context.Context, request *etcdserverpb.AuthUserGetRequest) (*etcdserverpb.AuthUserGetResponse, error) {
	if _, err := s.userReadSnapshot(ctx, request.Name); err != nil {
		return nil, err
	}
	header, err := s.authRPCHeader(ctx)
	if err != nil {
		return nil, err
	}
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

func (s *RPCServer) UserList(ctx context.Context, _ *etcdserverpb.AuthUserListRequest) (*etcdserverpb.AuthUserListResponse, error) {
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
	return &etcdserverpb.AuthUserListResponse{Header: header, Users: authUserNames(snapshot)}, nil
}

func (s *RPCServer) UserDelete(ctx context.Context, request *etcdserverpb.AuthUserDeleteRequest) (*etcdserverpb.AuthUserDeleteResponse, error) {
	if _, err := s.authAdminSnapshot(ctx); err != nil {
		return nil, err
	}
	if err := s.auth.userDelete(ctx, request.Name); err != nil {
		return nil, err
	}
	header, err := s.authRPCHeader(ctx)
	if err != nil {
		return nil, err
	}
	return &etcdserverpb.AuthUserDeleteResponse{Header: header}, nil
}

func (s *RPCServer) UserChangePassword(ctx context.Context, request *etcdserverpb.AuthUserChangePasswordRequest) (*etcdserverpb.AuthUserChangePasswordResponse, error) {
	if _, err := s.authAdminSnapshot(ctx); err != nil {
		return nil, err
	}
	if err := s.auth.userChangePassword(ctx, request.Name, request.Password, request.HashedPassword); err != nil {
		return nil, err
	}
	header, err := s.authRPCHeader(ctx)
	if err != nil {
		return nil, err
	}
	return &etcdserverpb.AuthUserChangePasswordResponse{Header: header}, nil
}

func (s *RPCServer) UserGrantRole(ctx context.Context, request *etcdserverpb.AuthUserGrantRoleRequest) (*etcdserverpb.AuthUserGrantRoleResponse, error) {
	if _, err := s.authAdminSnapshot(ctx); err != nil {
		return nil, err
	}
	if err := s.auth.userGrantRole(ctx, request.User, request.Role); err != nil {
		return nil, err
	}
	header, err := s.authRPCHeader(ctx)
	if err != nil {
		return nil, err
	}
	return &etcdserverpb.AuthUserGrantRoleResponse{Header: header}, nil
}

func (s *RPCServer) UserRevokeRole(ctx context.Context, request *etcdserverpb.AuthUserRevokeRoleRequest) (*etcdserverpb.AuthUserRevokeRoleResponse, error) {
	if _, err := s.authAdminSnapshot(ctx); err != nil {
		return nil, err
	}
	if err := s.auth.userRevokeRole(ctx, request.Name, request.Role); err != nil {
		return nil, err
	}
	header, err := s.authRPCHeader(ctx)
	if err != nil {
		return nil, err
	}
	return &etcdserverpb.AuthUserRevokeRoleResponse{Header: header}, nil
}

func (s *RPCServer) RoleAdd(ctx context.Context, request *etcdserverpb.AuthRoleAddRequest) (*etcdserverpb.AuthRoleAddResponse, error) {
	if _, err := s.authAdminSnapshot(ctx); err != nil {
		return nil, err
	}
	if err := s.auth.roleAdd(ctx, request.Name); err != nil {
		return nil, err
	}
	header, err := s.authRPCHeader(ctx)
	if err != nil {
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

func (s *RPCServer) RoleGet(ctx context.Context, request *etcdserverpb.AuthRoleGetRequest) (*etcdserverpb.AuthRoleGetResponse, error) {
	if _, err := s.roleReadSnapshot(ctx, request.Role); err != nil {
		return nil, err
	}
	header, err := s.authRPCHeader(ctx)
	if err != nil {
		return nil, err
	}
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

func (s *RPCServer) RoleList(ctx context.Context, _ *etcdserverpb.AuthRoleListRequest) (*etcdserverpb.AuthRoleListResponse, error) {
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
	return &etcdserverpb.AuthRoleListResponse{Header: header, Roles: authRoleNames(snapshot)}, nil
}

func (s *RPCServer) RoleDelete(ctx context.Context, request *etcdserverpb.AuthRoleDeleteRequest) (*etcdserverpb.AuthRoleDeleteResponse, error) {
	if _, err := s.authAdminSnapshot(ctx); err != nil {
		return nil, err
	}
	if err := s.auth.roleDelete(ctx, request.Role); err != nil {
		return nil, err
	}
	header, err := s.authRPCHeader(ctx)
	if err != nil {
		return nil, err
	}
	return &etcdserverpb.AuthRoleDeleteResponse{Header: header}, nil
}

func (s *RPCServer) RoleGrantPermission(ctx context.Context, request *etcdserverpb.AuthRoleGrantPermissionRequest) (*etcdserverpb.AuthRoleGrantPermissionResponse, error) {
	if _, err := s.authAdminSnapshot(ctx); err != nil {
		return nil, err
	}
	if err := s.auth.roleGrantPermission(ctx, request.Name, request.Perm); err != nil {
		return nil, err
	}
	header, err := s.authRPCHeader(ctx)
	if err != nil {
		return nil, err
	}
	return &etcdserverpb.AuthRoleGrantPermissionResponse{Header: header}, nil
}

func (s *RPCServer) RoleRevokePermission(ctx context.Context, request *etcdserverpb.AuthRoleRevokePermissionRequest) (*etcdserverpb.AuthRoleRevokePermissionResponse, error) {
	if _, err := s.authAdminSnapshot(ctx); err != nil {
		return nil, err
	}
	if err := s.auth.roleRevokePermission(ctx, request.Role, request.Key, request.RangeEnd); err != nil {
		return nil, err
	}
	header, err := s.authRPCHeader(ctx)
	if err != nil {
		return nil, err
	}
	return &etcdserverpb.AuthRoleRevokePermissionResponse{Header: header}, nil
}
