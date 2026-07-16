package etcd

import (
	"context"

	"go.etcd.io/etcd/api/v3/authpb"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

func authRPCHeader() *etcdserverpb.ResponseHeader { return &etcdserverpb.ResponseHeader{} }

func (s *RPCServer) requireAuthDisabled(ctx context.Context) (*authSnapshot, error) {
	snapshot, err := s.tokens.snapshots.current(ctx)
	if err != nil {
		return nil, err
	}
	if snapshot.Config.Enabled {
		return nil, status.Error(codes.Unimplemented, "authenticated auth management is not enabled yet")
	}
	return snapshot, nil
}

func (s *RPCServer) AuthStatus(ctx context.Context, _ *etcdserverpb.AuthStatusRequest) (*etcdserverpb.AuthStatusResponse, error) {
	snapshot, err := s.tokens.snapshots.current(ctx)
	if err != nil {
		return nil, err
	}
	return &etcdserverpb.AuthStatusResponse{Header: authRPCHeader(), Enabled: snapshot.Config.Enabled, AuthRevision: snapshot.Config.Revision}, nil
}

func (s *RPCServer) Authenticate(ctx context.Context, request *etcdserverpb.AuthenticateRequest) (*etcdserverpb.AuthenticateResponse, error) {
	token, err := s.tokens.authenticate(ctx, request.Name, request.Password)
	if err != nil {
		return nil, err
	}
	return &etcdserverpb.AuthenticateResponse{Header: authRPCHeader(), Token: token}, nil
}

func (s *RPCServer) UserAdd(ctx context.Context, request *etcdserverpb.AuthUserAddRequest) (*etcdserverpb.AuthUserAddResponse, error) {
	if _, err := s.requireAuthDisabled(ctx); err != nil {
		return nil, err
	}
	if err := s.auth.userAdd(ctx, request); err != nil {
		return nil, err
	}
	return &etcdserverpb.AuthUserAddResponse{Header: authRPCHeader()}, nil
}

func (s *RPCServer) UserGet(ctx context.Context, request *etcdserverpb.AuthUserGetRequest) (*etcdserverpb.AuthUserGetResponse, error) {
	snapshot, err := s.requireAuthDisabled(ctx)
	if err != nil {
		return nil, err
	}
	user := snapshot.Users[request.Name]
	if user == nil {
		return nil, rpctypes.ErrUserNotFound
	}
	return &etcdserverpb.AuthUserGetResponse{Header: authRPCHeader(), Roles: append([]string(nil), user.Roles...)}, nil
}

func (s *RPCServer) UserList(ctx context.Context, _ *etcdserverpb.AuthUserListRequest) (*etcdserverpb.AuthUserListResponse, error) {
	snapshot, err := s.requireAuthDisabled(ctx)
	if err != nil {
		return nil, err
	}
	return &etcdserverpb.AuthUserListResponse{Header: authRPCHeader(), Users: authUserNames(snapshot)}, nil
}

func (s *RPCServer) UserDelete(ctx context.Context, request *etcdserverpb.AuthUserDeleteRequest) (*etcdserverpb.AuthUserDeleteResponse, error) {
	if _, err := s.requireAuthDisabled(ctx); err != nil {
		return nil, err
	}
	if err := s.auth.userDelete(ctx, request.Name); err != nil {
		return nil, err
	}
	return &etcdserverpb.AuthUserDeleteResponse{Header: authRPCHeader()}, nil
}

func (s *RPCServer) UserChangePassword(ctx context.Context, request *etcdserverpb.AuthUserChangePasswordRequest) (*etcdserverpb.AuthUserChangePasswordResponse, error) {
	if _, err := s.requireAuthDisabled(ctx); err != nil {
		return nil, err
	}
	if err := s.auth.userChangePassword(ctx, request.Name, request.Password, request.HashedPassword); err != nil {
		return nil, err
	}
	return &etcdserverpb.AuthUserChangePasswordResponse{Header: authRPCHeader()}, nil
}

func (s *RPCServer) UserGrantRole(ctx context.Context, request *etcdserverpb.AuthUserGrantRoleRequest) (*etcdserverpb.AuthUserGrantRoleResponse, error) {
	if _, err := s.requireAuthDisabled(ctx); err != nil {
		return nil, err
	}
	if err := s.auth.userGrantRole(ctx, request.User, request.Role); err != nil {
		return nil, err
	}
	return &etcdserverpb.AuthUserGrantRoleResponse{Header: authRPCHeader()}, nil
}

func (s *RPCServer) UserRevokeRole(ctx context.Context, request *etcdserverpb.AuthUserRevokeRoleRequest) (*etcdserverpb.AuthUserRevokeRoleResponse, error) {
	if _, err := s.requireAuthDisabled(ctx); err != nil {
		return nil, err
	}
	if err := s.auth.userRevokeRole(ctx, request.Name, request.Role); err != nil {
		return nil, err
	}
	return &etcdserverpb.AuthUserRevokeRoleResponse{Header: authRPCHeader()}, nil
}

func (s *RPCServer) RoleAdd(ctx context.Context, request *etcdserverpb.AuthRoleAddRequest) (*etcdserverpb.AuthRoleAddResponse, error) {
	if _, err := s.requireAuthDisabled(ctx); err != nil {
		return nil, err
	}
	if err := s.auth.roleAdd(ctx, request.Name); err != nil {
		return nil, err
	}
	return &etcdserverpb.AuthRoleAddResponse{Header: authRPCHeader()}, nil
}

func (s *RPCServer) RoleGet(ctx context.Context, request *etcdserverpb.AuthRoleGetRequest) (*etcdserverpb.AuthRoleGetResponse, error) {
	snapshot, err := s.requireAuthDisabled(ctx)
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
	return &etcdserverpb.AuthRoleGetResponse{Header: authRPCHeader(), Perm: permissions}, nil
}

func (s *RPCServer) RoleList(ctx context.Context, _ *etcdserverpb.AuthRoleListRequest) (*etcdserverpb.AuthRoleListResponse, error) {
	snapshot, err := s.requireAuthDisabled(ctx)
	if err != nil {
		return nil, err
	}
	return &etcdserverpb.AuthRoleListResponse{Header: authRPCHeader(), Roles: authRoleNames(snapshot)}, nil
}

func (s *RPCServer) RoleDelete(ctx context.Context, request *etcdserverpb.AuthRoleDeleteRequest) (*etcdserverpb.AuthRoleDeleteResponse, error) {
	if _, err := s.requireAuthDisabled(ctx); err != nil {
		return nil, err
	}
	if err := s.auth.roleDelete(ctx, request.Role); err != nil {
		return nil, err
	}
	return &etcdserverpb.AuthRoleDeleteResponse{Header: authRPCHeader()}, nil
}

func (s *RPCServer) RoleGrantPermission(ctx context.Context, request *etcdserverpb.AuthRoleGrantPermissionRequest) (*etcdserverpb.AuthRoleGrantPermissionResponse, error) {
	if _, err := s.requireAuthDisabled(ctx); err != nil {
		return nil, err
	}
	if err := s.auth.roleGrantPermission(ctx, request.Name, request.Perm); err != nil {
		return nil, err
	}
	return &etcdserverpb.AuthRoleGrantPermissionResponse{Header: authRPCHeader()}, nil
}

func (s *RPCServer) RoleRevokePermission(ctx context.Context, request *etcdserverpb.AuthRoleRevokePermissionRequest) (*etcdserverpb.AuthRoleRevokePermissionResponse, error) {
	if _, err := s.requireAuthDisabled(ctx); err != nil {
		return nil, err
	}
	if err := s.auth.roleRevokePermission(ctx, request.Role, request.Key, request.RangeEnd); err != nil {
		return nil, err
	}
	return &etcdserverpb.AuthRoleRevokePermissionResponse{Header: authRPCHeader()}, nil
}
