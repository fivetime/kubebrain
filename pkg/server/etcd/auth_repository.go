package etcd

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"sort"
	"strings"
	"time"

	"go.etcd.io/etcd/api/v3/authpb"
	"google.golang.org/protobuf/proto"

	"github.com/kubewharf/kubebrain/pkg/backend"
	"github.com/kubewharf/kubebrain/pkg/storage"
)

var (
	authConfigKey           = []byte("auth/config")
	authUsersKey            = []byte("auth/users/")
	authRolesKey            = []byte("auth/roles/")
	authTokenGenerationsKey = []byte("auth/token-generations/")
)

const initialAuthRevision = 1

var errInvalidAuthMetadata = errors.New("auth metadata is inconsistent")
var errAuthRevisionExhausted = errors.New("etcd auth revision space exhausted")

type invalidAuthMetadataError struct {
	cause error
}

func (e *invalidAuthMetadataError) Error() string { return e.cause.Error() }
func (e *invalidAuthMetadataError) Unwrap() error { return e.cause }
func (e *invalidAuthMetadataError) Is(target error) bool {
	return target == errInvalidAuthMetadata
}

func markInvalidAuthMetadata(err error) error {
	if err == nil || errors.Is(err, errInvalidAuthMetadata) {
		return err
	}
	return &invalidAuthMetadataError{cause: err}
}

func waitAuthRetry(ctx context.Context, attempt int) error {
	shift := attempt
	if shift > 7 {
		shift = 7
	}
	delay := 50 * time.Microsecond * time.Duration(1<<shift)
	if delay > 5*time.Millisecond {
		delay = 5 * time.Millisecond
	}
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-timer.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

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
		return authConfig{}, markInvalidAuthMetadata(fmt.Errorf("invalid auth config encoding"))
	}
	config := authConfig{Enabled: value[0] == 1, Revision: binary.BigEndian.Uint64(value[1:])}
	if config.Revision == 0 {
		return authConfig{}, markInvalidAuthMetadata(fmt.Errorf("auth config revision is zero"))
	}
	return config, nil
}

func decodeAuthRecordIdentity(prefix []byte, key string) (string, error) {
	if !strings.HasPrefix(key, string(prefix)) {
		return "", markInvalidAuthMetadata(fmt.Errorf("auth record key %q is outside prefix %q", key, prefix))
	}
	encoded := strings.TrimPrefix(key, string(prefix))
	if encoded == "" {
		return "", markInvalidAuthMetadata(fmt.Errorf("auth record key %q has an empty identity", key))
	}
	identity, err := base64.RawURLEncoding.DecodeString(encoded)
	if err != nil {
		return "", markInvalidAuthMetadata(fmt.Errorf("decode auth record identity from key %q: %v", key, err))
	}
	if len(identity) == 0 {
		return "", markInvalidAuthMetadata(fmt.Errorf("auth record key %q has an empty identity", key))
	}
	return string(identity), nil
}

func authRecordKey(prefix []byte, name string) []byte {
	encoded := base64.RawURLEncoding.EncodeToString([]byte(name))
	key := make([]byte, 0, len(prefix)+len(encoded))
	key = append(key, prefix...)
	return append(key, encoded...)
}

type authSnapshot struct {
	Config           authConfig
	Users            map[string]*authpb.User
	Roles            map[string]*authpb.Role
	TokenGenerations map[string]*authpb.User
}

type authRepository struct{ backend BackendShim }

func newAuthRepository(backend BackendShim) *authRepository {
	return &authRepository{backend: backend}
}

func (r *authRepository) loadConfig(ctx context.Context) (authConfig, error) {
	config := authConfig{Revision: initialAuthRevision}
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
	for attempt := 0; ; attempt++ {
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
		if err = waitAuthRetry(ctx, attempt); err != nil {
			return nil, err
		}
	}
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
	generationsRaw, err := r.backend.InternalRange(ctx, authTokenGenerationsKey)
	if err != nil {
		return nil, err
	}
	snapshot := &authSnapshot{
		Config: config, Users: make(map[string]*authpb.User), Roles: make(map[string]*authpb.Role),
		TokenGenerations: make(map[string]*authpb.User),
	}
	for key, value := range usersRaw {
		identity, identityErr := decodeAuthRecordIdentity(authUsersKey, key)
		if identityErr != nil {
			return nil, identityErr
		}
		var user authpb.User
		if err := proto.Unmarshal(value, &user); err != nil {
			return nil, markInvalidAuthMetadata(fmt.Errorf("decode auth user %q: %w", key, err))
		}
		if !bytes.Equal([]byte(identity), user.Name) {
			return nil, markInvalidAuthMetadata(fmt.Errorf(
				"auth user key identity %q disagrees with payload name %q", identity, user.Name,
			))
		}
		snapshot.Users[identity] = &user
	}
	for key, value := range rolesRaw {
		identity, identityErr := decodeAuthRecordIdentity(authRolesKey, key)
		if identityErr != nil {
			return nil, identityErr
		}
		var role authpb.Role
		if err := proto.Unmarshal(value, &role); err != nil {
			return nil, markInvalidAuthMetadata(fmt.Errorf("decode auth role %q: %w", key, err))
		}
		if !bytes.Equal([]byte(identity), role.Name) {
			return nil, markInvalidAuthMetadata(fmt.Errorf(
				"auth role key identity %q disagrees with payload name %q", identity, role.Name,
			))
		}
		snapshot.Roles[identity] = &role
	}
	for key, value := range generationsRaw {
		identity, identityErr := decodeAuthRecordIdentity(authTokenGenerationsKey, key)
		if identityErr != nil {
			return nil, identityErr
		}
		var generation authpb.User
		if err := proto.Unmarshal(value, &generation); err != nil {
			return nil, markInvalidAuthMetadata(fmt.Errorf("decode auth token generation %q: %w", key, err))
		}
		if !bytes.Equal([]byte(identity), generation.Name) {
			return nil, markInvalidAuthMetadata(fmt.Errorf(
				"auth token generation key identity %q disagrees with payload name %q", identity, generation.Name,
			))
		}
		snapshot.TokenGenerations[identity] = &generation
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

// enable changes only the persisted feature flag. etcd does not advance the
// auth revision when authentication is enabled; disabling does advance it.
func (r *authRepository) enable(ctx context.Context, expected authConfig) (authConfig, error) {
	next := authConfig{Enabled: true, Revision: expected.Revision}
	op := backend.InternalCASOp{
		Key: authConfigKey, Value: encodeAuthConfig(next),
		Expected: encodeAuthConfig(expected), ExpectedExists: true,
	}
	if err := r.backend.InternalCAS(ctx, []backend.InternalCASOp{op}); err != nil {
		return authConfig{}, err
	}
	return next, nil
}

func (r *authRepository) mutateConfig(ctx context.Context, expected authConfig, enabled bool, mutations ...authMutation) (authConfig, error) {
	if expected.Revision == math.MaxUint64 {
		return authConfig{}, errAuthRevisionExhausted
	}
	next := authConfig{Enabled: enabled, Revision: expected.Revision + 1}
	ops := make([]backend.InternalCASOp, 0, len(mutations)+1)
	configOp := backend.InternalCASOp{Key: authConfigKey, Value: encodeAuthConfig(next)}
	// An absent config record represents etcd's initialized revision 1. Every
	// successful mutation writes revision >= 2, so later snapshots require it.
	if expected.Revision != initialAuthRevision || expected.Enabled {
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
