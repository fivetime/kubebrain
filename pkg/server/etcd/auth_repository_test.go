package etcd

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/api/v3/authpb"
	"google.golang.org/protobuf/proto"

	"github.com/kubewharf/kubebrain/pkg/storage"
)

func TestAuthRepositoryPersistsAndRecoversAtomicSnapshot(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()
	repo := newAuthRepository(server.backend)
	ctx := context.Background()

	empty, err := repo.load(ctx)
	require.NoError(t, err)
	require.Equal(t, authConfig{Revision: initialAuthRevision}, empty.Config)

	user := &authpb.User{Name: []byte("root"), Password: []byte("bcrypt-hash"), Roles: []string{"root"}}
	role := &authpb.Role{Name: []byte("root"), KeyPermission: []*authpb.Permission{{PermType: authpb.READWRITE, Key: []byte{}, RangeEnd: []byte{0}}}}
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
