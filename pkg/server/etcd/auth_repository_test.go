package etcd

import (
	"bytes"
	"context"
	"errors"
	"math"
	"testing"

	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/api/v3/authpb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	"github.com/kubewharf/kubebrain/pkg/storage"
)

type authMetadataReadErrorBackend struct {
	BackendShim
	err error
}

func (b *authMetadataReadErrorBackend) InternalGet(ctx context.Context, key []byte) ([]byte, error) {
	if bytes.Equal(key, authConfigKey) {
		return nil, b.err
	}
	return b.BackendShim.InternalGet(ctx, key)
}

func TestAuthRepositoryPersistsAndRecoversAtomicSnapshot(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()
	repo := newAuthRepository(server.backend)
	ctx := context.Background()

	empty, err := repo.load(ctx)
	require.NoError(t, err)
	require.Equal(t, authConfig{Revision: initialAuthRevision}, empty.Config)

	user := &authpb.User{Name: []byte("root"), Password: []byte("bcrypt-hash"), Roles: []string{"root"}}
	role := &authpb.Role{Name: []byte("root"), KeyPermission: []*authpb.Permission{{PermType: authpb.READWRITE, Key: []byte{0}, RangeEnd: []byte{0}}}}
	next, err := repo.mutate(ctx, empty.Config,
		authMutation{Key: authRecordKey(authUsersKey, "root"), Value: user},
		authMutation{Key: authRecordKey(authRolesKey, "root"), Value: role},
	)
	require.NoError(t, err)
	require.EqualValues(t, 2, next.Revision)

	recovered, err := repo.load(ctx)
	require.NoError(t, err)
	require.Equal(t, next, recovered.Config)
	require.True(t, proto.Equal(user, recovered.Users["root"]))
	require.True(t, proto.Equal(role, recovered.Roles["root"]))
	require.Equal(t, []string{"root"}, authUserNames(recovered))
	require.Equal(t, []string{"root"}, authRoleNames(recovered))

	changed := &authpb.User{Name: []byte("root"), Password: []byte("new-hash"), Roles: []string{"root"}}
	_, err = repo.mutate(ctx, empty.Config,
		authMutation{Key: authRecordKey(authUsersKey, "root"), Value: changed, Expected: user, ExpectedExists: true},
	)
	require.ErrorIs(t, err, storage.ErrCASFailed, "stale auth revision must reject the whole mutation")
	afterConflict, err := repo.load(ctx)
	require.NoError(t, err)
	require.Equal(t, next, afterConflict.Config)
	require.True(t, proto.Equal(user, afterConflict.Users["root"]), "conflicting mutation must not partially replace user")
}

func TestAuthRepositoryClassifiesMalformedPersistentMetadata(t *testing.T) {
	tests := []struct {
		name  string
		key   []byte
		value func(*testing.T) []byte
		want  string
	}{
		{
			name: "invalid config", key: authConfigKey,
			value: func(*testing.T) []byte { return []byte{2} },
			want:  "invalid auth config encoding",
		},
		{
			name: "zero config revision", key: authConfigKey,
			value: func(*testing.T) []byte {
				return encodeAuthConfig(authConfig{})
			},
			want: "auth config revision is zero",
		},
		{
			name: "malformed user", key: authRecordKey(authUsersKey, "alice"),
			value: func(*testing.T) []byte { return []byte{0xff} },
			want:  "decode auth user",
		},
		{
			name: "user key mismatch", key: authRecordKey(authUsersKey, "alice"),
			value: func(t *testing.T) []byte {
				value, err := proto.Marshal(&authpb.User{Name: []byte("bob")})
				require.NoError(t, err)
				return value
			},
			want: `auth user key identity "alice" disagrees with payload name "bob"`,
		},
		{
			name: "unsorted user roles", key: authRecordKey(authUsersKey, "alice"),
			value: func(t *testing.T) []byte {
				value, err := proto.Marshal(&authpb.User{Name: []byte("alice"), Roles: []string{"z", "root"}})
				require.NoError(t, err)
				return value
			},
			want: `auth user "alice" roles are not sorted`,
		},
		{
			name: "role key mismatch", key: authRecordKey(authRolesKey, "reader"),
			value: func(t *testing.T) []byte {
				value, err := proto.Marshal(&authpb.Role{Name: []byte("writer")})
				require.NoError(t, err)
				return value
			},
			want: `auth role key identity "reader" disagrees with payload name "writer"`,
		},
		{
			name: "role permission invalid range", key: authRecordKey(authRolesKey, "reader"),
			value: func(t *testing.T) []byte {
				value, err := proto.Marshal(&authpb.Role{
					Name: []byte("reader"), KeyPermission: []*authpb.Permission{{
						PermType: authpb.READ, RangeEnd: []byte{0},
					}},
				})
				require.NoError(t, err)
				return value
			},
			want: `auth role "reader" permission 0 has invalid range`,
		},
		{
			name: "unsorted role permissions", key: authRecordKey(authRolesKey, "reader"),
			value: func(t *testing.T) []byte {
				value, err := proto.Marshal(&authpb.Role{
					Name: []byte("reader"), KeyPermission: []*authpb.Permission{
						{PermType: authpb.READ, Key: []byte("z")},
						{PermType: authpb.READ, Key: []byte("a")},
					},
				})
				require.NoError(t, err)
				return value
			},
			want: `auth role "reader" permissions are not sorted`,
		},
		{
			name: "token generation key mismatch", key: authRecordKey(authTokenGenerationsKey, "alice"),
			value: func(t *testing.T) []byte {
				value, err := proto.Marshal(&authpb.User{Name: []byte("bob"), Password: make([]byte, authUserTokenGenerationBytes)})
				require.NoError(t, err)
				return value
			},
			want: `auth token generation key identity "alice" disagrees with payload name "bob"`,
		},
		{
			name: "token generation wrong length", key: authRecordKey(authTokenGenerationsKey, "alice"),
			value: func(t *testing.T) []byte {
				value, err := proto.Marshal(&authpb.User{Name: []byte("alice"), Password: make([]byte, authUserTokenGenerationBytes-1)})
				require.NoError(t, err)
				return value
			},
			want: `auth token generation "alice" has length 15, want 16`,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			server, closeFn := newTestRPCServer(t)
			defer closeFn()
			ctx := context.Background()
			require.NoError(t, server.backend.InternalPut(ctx, test.key, test.value(t)))

			_, err := newAuthRepository(server.backend).load(ctx)
			require.ErrorIs(t, err, errInvalidAuthMetadata)
			require.ErrorContains(t, err, test.want)
			require.NotContains(t, err.Error(), errInvalidAuthMetadata.Error(),
				"classification must not replace the existing operator diagnostic")
		})
	}
}

func TestAuthRepositoryRevisionExhaustionDoesNotWrapOrMutate(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()
	ctx := context.Background()
	repo := newAuthRepository(server.backend)
	maximum := authConfig{Revision: math.MaxUint64}
	require.NoError(t, server.backend.InternalPut(ctx, authConfigKey, encodeAuthConfig(maximum)))

	_, err := repo.mutate(ctx, maximum, authMutation{
		Key:   authRecordKey(authRolesKey, "must-not-exist"),
		Value: &authpb.Role{Name: []byte("must-not-exist")},
	})
	require.ErrorIs(t, err, errAuthRevisionExhausted)

	after, err := repo.load(ctx)
	require.NoError(t, err)
	require.Equal(t, maximum, after.Config)
	require.NotContains(t, after.Roles, "must-not-exist")
}

func TestAuthRepositoryPreservesUnknownPermissionTypeAsDenyOnly(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()
	ctx := context.Background()
	role := &authpb.Role{Name: []byte("reader"), KeyPermission: []*authpb.Permission{{
		PermType: authpb.Permission_Type(99), Key: []byte("/unknown/"), RangeEnd: []byte("/unknown0"),
	}}}
	value, err := proto.Marshal(role)
	require.NoError(t, err)
	require.NoError(t, server.backend.InternalPut(ctx, authRecordKey(authRolesKey, "reader"), value))

	snapshot, err := newAuthRepository(server.backend).load(ctx)
	require.NoError(t, err)
	require.Equal(t, authpb.Permission_Type(99), snapshot.Roles["reader"].KeyPermission[0].PermType)
}

func TestAuthRepositoryPreservesMetadataReadStatusError(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()
	want := status.Error(codes.Unavailable, "auth metadata backend unavailable")
	repo := newAuthRepository(&authMetadataReadErrorBackend{BackendShim: server.backend, err: want})

	_, err := repo.load(context.Background())
	require.ErrorIs(t, err, want)
	require.False(t, errors.Is(err, errInvalidAuthMetadata))
	require.Equal(t, codes.Unavailable, status.Code(err))
}
