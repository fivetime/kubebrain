package etcd

import (
	"context"
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

const authMutationRetries = 16

type authManager struct{ repo *authRepository }

func newAuthManager(backend BackendShim) *authManager {
	return &authManager{repo: newAuthRepository(backend)}
}

func retryAuthMutation(ctx context.Context, operation func(*authSnapshot) error, load func(context.Context) (*authSnapshot, error)) error {
	for i := 0; i < authMutationRetries; i++ {
		snapshot, err := load(ctx)
		if err != nil {
			return err
		}
		if err = operation(snapshot); !errors.Is(err, storage.ErrCASFailed) {
			return err
		}
	}
	return storage.ErrUnavailable
}

func authPassword(request *etcdserverpb.AuthUserAddRequest) ([]byte, error) {
	if request.Options != nil && request.Options.NoPassword {
		return nil, nil
	}
	if request.HashedPassword != "" {
		return base64.StdEncoding.DecodeString(request.HashedPassword)
	}
	return bcrypt.GenerateFromPassword([]byte(request.Password), bcrypt.DefaultCost)
}

func (m *authManager) userAdd(ctx context.Context, request *etcdserverpb.AuthUserAddRequest) error {
	if request.Name == "" {
		return rpctypes.ErrUserEmpty
	}
	password, err := authPassword(request)
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
		_, err := m.repo.mutate(ctx, snapshot.Config, authMutation{Key: authRecordKey(authUsersKey, request.Name), Value: user})
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
		if snapshot.Roles[roleName] == nil {
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
		if !hasRoot || snapshot.Roles["root"] == nil {
			return rpctypes.ErrRootRoleNotExist
		}
		_, err := m.repo.mutateConfig(ctx, snapshot.Config, true)
		return err
	}, m.repo.load)
}
