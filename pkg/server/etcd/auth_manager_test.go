package etcd

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
	"golang.org/x/crypto/bcrypt"
)

func TestAuthManagerBootstrapPersistsWithIndependentRevision(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()
	manager := newAuthManager(server.backend)
	ctx := context.Background()
	before := server.backend.GetCurrentRevision()

	require.ErrorIs(t, manager.enable(ctx), rpctypes.ErrRootUserNotExist)
	require.NoError(t, manager.userAdd(ctx, &etcdserverpb.AuthUserAddRequest{Name: "root", Password: "secret"}))
	require.ErrorIs(t, manager.enable(ctx), rpctypes.ErrRootRoleNotExist)
	require.NoError(t, manager.roleAdd(ctx, "root"))
	require.NoError(t, manager.userGrantRole(ctx, "root", "root"))
	require.NoError(t, manager.enable(ctx))
	require.NoError(t, manager.enable(ctx), "enable must be idempotent")
	require.Equal(t, before, server.backend.GetCurrentRevision(), "auth mutations must not consume user revision")

	snapshot, err := manager.repo.load(ctx)
	require.NoError(t, err)
	require.True(t, snapshot.Config.Enabled)
	require.EqualValues(t, 4, snapshot.Config.Revision)
	require.Equal(t, []string{"root"}, snapshot.Users["root"].Roles)
	require.NoError(t, bcrypt.CompareHashAndPassword(snapshot.Users["root"].Password, []byte("secret")))
}

func TestAuthManagerBootstrapErrors(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()
	manager := newAuthManager(server.backend)
	ctx := context.Background()

	require.ErrorIs(t, manager.userAdd(ctx, &etcdserverpb.AuthUserAddRequest{}), rpctypes.ErrUserEmpty)
	require.ErrorIs(t, manager.roleAdd(ctx, ""), rpctypes.ErrRoleEmpty)
	require.ErrorIs(t, manager.userGrantRole(ctx, "missing", "missing"), rpctypes.ErrUserNotFound)
	require.NoError(t, manager.userAdd(ctx, &etcdserverpb.AuthUserAddRequest{Name: "alice", Password: "pw"}))
	require.ErrorIs(t, manager.userAdd(ctx, &etcdserverpb.AuthUserAddRequest{Name: "alice", Password: "pw"}), rpctypes.ErrUserAlreadyExist)
	require.ErrorIs(t, manager.userGrantRole(ctx, "alice", "missing"), rpctypes.ErrRoleNotFound)
	require.NoError(t, manager.roleAdd(ctx, "reader"))
	require.ErrorIs(t, manager.roleAdd(ctx, "reader"), rpctypes.ErrRoleAlreadyExist)
	require.NoError(t, manager.userGrantRole(ctx, "alice", "reader"))
	require.NoError(t, manager.userGrantRole(ctx, "alice", "reader"), "grant role must be idempotent")
}
