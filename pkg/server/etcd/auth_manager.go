package etcd

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/base64"
	"errors"
	"sort"

	"go.etcd.io/etcd/api/v3/authpb"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
	"golang.org/x/crypto/bcrypt"
	"google.golang.org/protobuf/proto"

	"github.com/kubewharf/kubebrain/pkg/storage"
)

var errNoPasswordUser = errors.New("auth: authentication failed, password was given for no password user")

const authUserTokenGenerationBytes = 16

func newUserTokenGeneration(name string) (*authpb.User, error) {
	generation := make([]byte, authUserTokenGenerationBytes)
	if _, err := rand.Read(generation); err != nil {
		return nil, err
	}
	return &authpb.User{Name: []byte(name), Password: generation}, nil
}

type authManager struct {
	repo       *authRepository
	bcryptCost int
}

func newAuthManager(backend BackendShim) *authManager {
	return &authManager{repo: newAuthRepository(backend), bcryptCost: bcrypt.DefaultCost}
}

func retryAuthMutation(ctx context.Context, operation func(*authSnapshot) error, load func(context.Context) (*authSnapshot, error)) error {
	for attempt := 0; ; attempt++ {
		snapshot, err := load(ctx)
		if err != nil {
			return err
		}
		if err = operation(snapshot); !errors.Is(err, storage.ErrCASFailed) {
			return err
		}
		if err = waitAuthRetry(ctx, attempt); err != nil {
			return err
		}
	}
}

func authPassword(request *etcdserverpb.AuthUserAddRequest, cost int) ([]byte, error) {
	if request.Options != nil && request.Options.NoPassword {
		return nil, nil
	}
	if request.Password != "" {
		return bcrypt.GenerateFromPassword([]byte(request.Password), cost)
	}
	if request.HashedPassword != "" {
		password, err := base64.StdEncoding.DecodeString(request.HashedPassword)
		if err != nil {
			return nil, errNoPasswordUser
		}
		return password, nil
	}
	return bcrypt.GenerateFromPassword(nil, cost)
}

func (m *authManager) userAdd(ctx context.Context, request *etcdserverpb.AuthUserAddRequest) error {
	if request.Name == "" {
		return rpctypes.ErrUserEmpty
	}
	password, err := authPassword(request, m.bcryptCost)
	if err != nil {
		return err
	}
	generation, err := newUserTokenGeneration(request.Name)
	if err != nil {
		return err
	}
	return retryAuthMutation(ctx, func(snapshot *authSnapshot) error {
		if snapshot.Users[request.Name] != nil {
			return rpctypes.ErrUserAlreadyExist
		}
		options := &authpb.UserAddOptions{}
		if request.Options != nil {
			options = proto.Clone(request.Options).(*authpb.UserAddOptions)
		}
		user := &authpb.User{Name: []byte(request.Name), Password: password, Options: options}
		_, err := m.repo.mutate(ctx, snapshot.Config,
			authMutation{Key: authRecordKey(authUsersKey, request.Name), Value: user},
			authMutation{Key: authRecordKey(authTokenGenerationsKey, request.Name), Value: generation},
		)
		return err
	}, m.repo.load)
}

func (m *authManager) roleAdd(ctx context.Context, name string) error {
	if name == "" {
		return rpctypes.ErrRoleEmpty
	}
	return retryAuthMutation(ctx, func(snapshot *authSnapshot) error {
		if snapshot.Roles[name] != nil {
			return rpctypes.ErrRoleAlreadyExist
		}
		role := &authpb.Role{Name: []byte(name)}
		_, err := m.repo.mutate(ctx, snapshot.Config, authMutation{Key: authRecordKey(authRolesKey, name), Value: role})
		return err
	}, m.repo.load)
}

func (m *authManager) userGrantRole(ctx context.Context, username, roleName string) error {
	return retryAuthMutation(ctx, func(snapshot *authSnapshot) error {
		user := snapshot.Users[username]
		if user == nil {
			return rpctypes.ErrUserNotFound
		}
		if roleName != "root" && snapshot.Roles[roleName] == nil {
			return rpctypes.ErrRoleNotFound
		}
		for _, existing := range user.Roles {
			if existing == roleName {
				return nil
			}
		}
		updated := proto.Clone(user).(*authpb.User)
		updated.Roles = append(updated.Roles, roleName)
		sort.Strings(updated.Roles)
		_, err := m.repo.mutate(ctx, snapshot.Config, authMutation{
			Key: authRecordKey(authUsersKey, username), Value: updated,
			Expected: user, ExpectedExists: true,
		})
		return err
	}, m.repo.load)
}

func (m *authManager) enable(ctx context.Context) error {
	return retryAuthMutation(ctx, func(snapshot *authSnapshot) error {
		if snapshot.Config.Enabled {
			return nil
		}
		root := snapshot.Users["root"]
		if root == nil {
			return rpctypes.ErrRootUserNotExist
		}
		hasRoot := false
		for _, role := range root.Roles {
			if role == "root" {
				hasRoot = true
				break
			}
		}
		if !hasRoot {
			return rpctypes.ErrRootRoleNotExist
		}
		_, err := m.repo.enable(ctx, snapshot.Config)
		return err
	}, m.repo.load)
}

func (m *authManager) disable(ctx context.Context) error {
	return retryAuthMutation(ctx, func(snapshot *authSnapshot) error {
		if !snapshot.Config.Enabled {
			return nil
		}
		_, err := m.repo.mutateConfig(ctx, snapshot.Config, false)
		return err
	}, m.repo.load)
}

func (m *authManager) userDelete(ctx context.Context, name string) error {
	return retryAuthMutation(ctx, func(snapshot *authSnapshot) error {
		if snapshot.Config.Enabled && name == "root" {
			return rpctypes.ErrInvalidAuthMgmt
		}
		user := snapshot.Users[name]
		if user == nil {
			return rpctypes.ErrUserNotFound
		}
		generation := snapshot.TokenGenerations[name]
		generationMutation := authMutation{Key: authRecordKey(authTokenGenerationsKey, name), Delete: true}
		if generation != nil {
			generationMutation.Expected = generation
			generationMutation.ExpectedExists = true
		}
		_, err := m.repo.mutate(ctx, snapshot.Config,
			authMutation{Key: authRecordKey(authUsersKey, name), Delete: true, Expected: user, ExpectedExists: true},
			generationMutation,
		)
		return err
	}, m.repo.load)
}

func authChangedPassword(password, hashed string, cost int) ([]byte, error) {
	if password != "" {
		return bcrypt.GenerateFromPassword([]byte(password), cost)
	}
	if hashed != "" {
		encoded, err := base64.StdEncoding.DecodeString(hashed)
		if err != nil {
			return nil, errNoPasswordUser
		}
		return encoded, nil
	}
	return bcrypt.GenerateFromPassword(nil, cost)
}

func (m *authManager) userChangePassword(ctx context.Context, name, password, hashed string) error {
	generation, err := newUserTokenGeneration(name)
	if err != nil {
		return err
	}
	return retryAuthMutation(ctx, func(snapshot *authSnapshot) error {
		user := snapshot.Users[name]
		if user == nil {
			return rpctypes.ErrUserNotFound
		}
		updated := proto.Clone(user).(*authpb.User)
		if user.Options == nil || !user.Options.NoPassword {
			encoded, err := authChangedPassword(password, hashed, m.bcryptCost)
			if err != nil {
				return err
			}
			updated.Password = encoded
		}
		oldGeneration := snapshot.TokenGenerations[name]
		generationMutation := authMutation{Key: authRecordKey(authTokenGenerationsKey, name), Value: generation}
		if oldGeneration != nil {
			generationMutation.Expected = oldGeneration
			generationMutation.ExpectedExists = true
		}
		_, err := m.repo.mutate(ctx, snapshot.Config,
			authMutation{Key: authRecordKey(authUsersKey, name), Value: updated, Expected: user, ExpectedExists: true},
			generationMutation,
		)
		return err
	}, m.repo.load)
}

func (m *authManager) userRevokeRole(ctx context.Context, name, roleName string) error {
	return retryAuthMutation(ctx, func(snapshot *authSnapshot) error {
		if snapshot.Config.Enabled && name == "root" && roleName == "root" {
			return rpctypes.ErrInvalidAuthMgmt
		}
		user := snapshot.Users[name]
		if user == nil {
			return rpctypes.ErrUserNotFound
		}
		updated := proto.Clone(user).(*authpb.User)
		updated.Roles = updated.Roles[:0]
		for _, role := range user.Roles {
			if role != roleName {
				updated.Roles = append(updated.Roles, role)
			}
		}
		if len(updated.Roles) == len(user.Roles) {
			return rpctypes.ErrRoleNotGranted
		}
		_, err := m.repo.mutate(ctx, snapshot.Config, authMutation{
			Key: authRecordKey(authUsersKey, name), Value: updated, Expected: user, ExpectedExists: true,
		})
		return err
	}, m.repo.load)
}

func (m *authManager) roleDelete(ctx context.Context, name string) error {
	return retryAuthMutation(ctx, func(snapshot *authSnapshot) error {
		if snapshot.Config.Enabled && name == "root" {
			return rpctypes.ErrInvalidAuthMgmt
		}
		role := snapshot.Roles[name]
		if role == nil {
			return rpctypes.ErrRoleNotFound
		}
		mutations := []authMutation{{
			Key: authRecordKey(authRolesKey, name), Delete: true, Expected: role, ExpectedExists: true,
		}}
		for username, user := range snapshot.Users {
			updated := proto.Clone(user).(*authpb.User)
			updated.Roles = updated.Roles[:0]
			for _, assigned := range user.Roles {
				if assigned != name {
					updated.Roles = append(updated.Roles, assigned)
				}
			}
			if len(updated.Roles) != len(user.Roles) {
				mutations = append(mutations, authMutation{
					Key: authRecordKey(authUsersKey, username), Value: updated, Expected: user, ExpectedExists: true,
				})
			}
		}
		_, err := m.repo.mutate(ctx, snapshot.Config, mutations...)
		return err
	}, m.repo.load)
}

func validPermissionRange(key, end []byte) bool {
	if len(key) == 0 {
		return false
	}
	return len(end) == 0 || bytes.Compare(key, end) < 0 || (len(end) == 1 && end[0] == 0)
}

func (m *authManager) roleGrantPermission(ctx context.Context, name string, permission *authpb.Permission) error {
	if permission == nil {
		return rpctypes.ErrGRPCPermissionNotGiven
	}
	if !validPermissionRange(permission.Key, permission.RangeEnd) {
		return rpctypes.ErrInvalidAuthMgmt
	}
	return retryAuthMutation(ctx, func(snapshot *authSnapshot) error {
		role := snapshot.Roles[name]
		if role == nil {
			return rpctypes.ErrRoleNotFound
		}
		updated := proto.Clone(role).(*authpb.Role)
		newPermission := proto.Clone(permission).(*authpb.Permission)
		replaced := false
		for i, existing := range updated.KeyPermission {
			if bytes.Equal(existing.Key, permission.Key) && bytes.Equal(existing.RangeEnd, permission.RangeEnd) {
				updated.KeyPermission[i] = newPermission
				replaced = true
				break
			}
		}
		if !replaced {
			updated.KeyPermission = append(updated.KeyPermission, newPermission)
		}
		sort.Slice(updated.KeyPermission, func(i, j int) bool {
			return bytes.Compare(updated.KeyPermission[i].Key, updated.KeyPermission[j].Key) < 0
		})
		_, err := m.repo.mutate(ctx, snapshot.Config, authMutation{
			Key: authRecordKey(authRolesKey, name), Value: updated, Expected: role, ExpectedExists: true,
		})
		return err
	}, m.repo.load)
}

func (m *authManager) roleRevokePermission(ctx context.Context, name string, key, end []byte) error {
	return retryAuthMutation(ctx, func(snapshot *authSnapshot) error {
		role := snapshot.Roles[name]
		if role == nil {
			return rpctypes.ErrRoleNotFound
		}
		updated := proto.Clone(role).(*authpb.Role)
		updated.KeyPermission = updated.KeyPermission[:0]
		for _, permission := range role.KeyPermission {
			if !bytes.Equal(permission.Key, key) || !bytes.Equal(permission.RangeEnd, end) {
				updated.KeyPermission = append(updated.KeyPermission, permission)
			}
		}
		if len(updated.KeyPermission) == len(role.KeyPermission) {
			return rpctypes.ErrPermissionNotGranted
		}
		_, err := m.repo.mutate(ctx, snapshot.Config, authMutation{
			Key: authRecordKey(authRolesKey, name), Value: updated, Expected: role, ExpectedExists: true,
		})
		return err
	}, m.repo.load)
}
