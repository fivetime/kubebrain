package etcd

import (
	"context"
	"encoding/base64"
	"encoding/binary"
	"errors"
	"fmt"
	"sort"

	"go.etcd.io/etcd/api/v3/authpb"
	"google.golang.org/protobuf/proto"

	"github.com/kubewharf/kubebrain/pkg/backend"
	"github.com/kubewharf/kubebrain/pkg/storage"
)

var (
	authConfigKey = []byte("auth/config")
	authUsersKey  = []byte("auth/users/")
	authRolesKey  = []byte("auth/roles/")
)

type authConfig struct {
	Enabled  bool
	Revision uint64
}

func encodeAuthConfig(config authConfig) []byte {
	value := make([]byte, 9)
	if config.Enabled {
		value[0] = 1
	}
	binary.BigEndian.PutUint64(value[1:], config.Revision)
	return value
}

func decodeAuthConfig(value []byte) (authConfig, error) {
	if len(value) != 9 || value[0] > 1 {
		return authConfig{}, fmt.Errorf("invalid auth config encoding")
	}
	return authConfig{Enabled: value[0] == 1, Revision: binary.BigEndian.Uint64(value[1:])}, nil
}

func authRecordKey(prefix []byte, name string) []byte {
	encoded := base64.RawURLEncoding.EncodeToString([]byte(name))
	key := make([]byte, 0, len(prefix)+len(encoded))
	key = append(key, prefix...)
	return append(key, encoded...)
}

type authSnapshot struct {
	Config authConfig
	Users  map[string]*authpb.User
	Roles  map[string]*authpb.Role
}

type authRepository struct{ backend BackendShim }

func newAuthRepository(backend BackendShim) *authRepository {
	return &authRepository{backend: backend}
}

func (r *authRepository) loadConfig(ctx context.Context) (authConfig, error) {
	config := authConfig{}
	configValue, err := r.backend.InternalGet(ctx, authConfigKey)
	if err == nil {
		config, err = decodeAuthConfig(configValue)
	}
	if err != nil && !errors.Is(err, storage.ErrKeyNotFound) {
		return authConfig{}, err
	}
	return config, nil
}

func (r *authRepository) load(ctx context.Context) (*authSnapshot, error) {
	for i := 0; i < authMutationRetries; i++ {
		config, err := r.loadConfig(ctx)
		if err != nil {
			return nil, err
		}
		snapshot, err := r.loadRecords(ctx, config)
		if err != nil {
			return nil, err
		}
		current, err := r.loadConfig(ctx)
		if err != nil {
			return nil, err
		}
		if current == config {
			return snapshot, nil
		}
	}
	return nil, storage.ErrUnavailable
}

func (r *authRepository) loadRecords(ctx context.Context, config authConfig) (*authSnapshot, error) {
	usersRaw, err := r.backend.InternalRange(ctx, authUsersKey)
	if err != nil {
		return nil, err
	}
	rolesRaw, err := r.backend.InternalRange(ctx, authRolesKey)
	if err != nil {
		return nil, err
	}
	snapshot := &authSnapshot{Config: config, Users: make(map[string]*authpb.User), Roles: make(map[string]*authpb.Role)}
	for key, value := range usersRaw {
		var user authpb.User
		if err := proto.Unmarshal(value, &user); err != nil {
			return nil, fmt.Errorf("decode auth user %q: %w", key, err)
		}
		snapshot.Users[string(user.Name)] = &user
	}
	for key, value := range rolesRaw {
		var role authpb.Role
		if err := proto.Unmarshal(value, &role); err != nil {
			return nil, fmt.Errorf("decode auth role %q: %w", key, err)
		}
		snapshot.Roles[string(role.Name)] = &role
	}
	return snapshot, nil
}

type authMutation struct {
	Key            []byte
	Value          proto.Message
	Delete         bool
	Expected       proto.Message
	ExpectedExists bool
}

func marshalAuthRecord(message proto.Message) ([]byte, error) {
	if message == nil {
		return nil, nil
	}
	return proto.MarshalOptions{Deterministic: true}.Marshal(message)
}

// mutate atomically applies auth records and advances the independent auth
// revision. expected is the snapshot config the caller made its decision from;
// concurrent mutations fail with storage.ErrCASFailed and must be re-evaluated.
func (r *authRepository) mutate(ctx context.Context, expected authConfig, mutations ...authMutation) (authConfig, error) {
	return r.mutateConfig(ctx, expected, expected.Enabled, mutations...)
}

func (r *authRepository) mutateConfig(ctx context.Context, expected authConfig, enabled bool, mutations ...authMutation) (authConfig, error) {
	next := authConfig{Enabled: enabled, Revision: expected.Revision + 1}
	ops := make([]backend.InternalCASOp, 0, len(mutations)+1)
	configOp := backend.InternalCASOp{Key: authConfigKey, Value: encodeAuthConfig(next)}
	if expected.Revision != 0 || expected.Enabled {
		configOp.ExpectedExists = true
		configOp.Expected = encodeAuthConfig(expected)
	}
	ops = append(ops, configOp)
	for _, mutation := range mutations {
		value, err := marshalAuthRecord(mutation.Value)
		if err != nil {
			return authConfig{}, err
		}
		oldValue, err := marshalAuthRecord(mutation.Expected)
		if err != nil {
			return authConfig{}, err
		}
		ops = append(ops, backend.InternalCASOp{
			Key: mutation.Key, Value: value, Delete: mutation.Delete,
			Expected: oldValue, ExpectedExists: mutation.ExpectedExists,
		})
	}
	if err := r.backend.InternalCAS(ctx, ops); err != nil {
		return authConfig{}, err
	}
	return next, nil
}

func authUserNames(snapshot *authSnapshot) []string {
	names := make([]string, 0, len(snapshot.Users))
	for name := range snapshot.Users {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

func authRoleNames(snapshot *authSnapshot) []string {
	names := make([]string, 0, len(snapshot.Roles))
	for name := range snapshot.Roles {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}
