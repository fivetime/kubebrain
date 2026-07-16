package etcd

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/api/v3/authpb"
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

func TestAuthManagerUserRolePermissionLifecycle(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()
	manager := newAuthManager(server.backend)
	ctx := context.Background()

	require.NoError(t, manager.userAdd(ctx, &etcdserverpb.AuthUserAddRequest{Name: "alice", Password: "old"}))
	require.NoError(t, manager.roleAdd(ctx, "reader"))
	require.NoError(t, manager.userGrantRole(ctx, "alice", "reader"))
	permission := &authpb.Permission{PermType: authpb.READ, Key: []byte("/apps/"), RangeEnd: []byte("/apps0")}
	require.NoError(t, manager.roleGrantPermission(ctx, "reader", permission))

	// Granting the same range updates its type instead of duplicating it.
	permission.PermType = authpb.READWRITE
	require.NoError(t, manager.roleGrantPermission(ctx, "reader", permission))
	snapshot, err := manager.repo.load(ctx)
	require.NoError(t, err)
	require.Len(t, snapshot.Roles["reader"].KeyPermission, 1)
	require.Equal(t, authpb.READWRITE, snapshot.Roles["reader"].KeyPermission[0].PermType)

	require.NoError(t, manager.userChangePassword(ctx, "alice", "new", ""))
	snapshot, err = manager.repo.load(ctx)
	require.NoError(t, err)
	require.NoError(t, bcrypt.CompareHashAndPassword(snapshot.Users["alice"].Password, []byte("new")))
	require.ErrorIs(t, manager.roleRevokePermission(ctx, "reader", []byte("missing"), nil), rpctypes.ErrPermissionNotGranted)
	require.NoError(t, manager.roleRevokePermission(ctx, "reader", permission.Key, permission.RangeEnd))
	require.ErrorIs(t, manager.userRevokeRole(ctx, "alice", "missing"), rpctypes.ErrRoleNotGranted)
	require.NoError(t, manager.userRevokeRole(ctx, "alice", "reader"))
	require.NoError(t, manager.userGrantRole(ctx, "alice", "reader"))

	// Role deletion and user-role unlink are one auth revision / storage batch.
	require.NoError(t, manager.roleDelete(ctx, "reader"))
	snapshot, err = manager.repo.load(ctx)
	require.NoError(t, err)
	require.Nil(t, snapshot.Roles["reader"])
	require.Empty(t, snapshot.Users["alice"].Roles)
	require.NoError(t, manager.userDelete(ctx, "alice"))
	snapshot, err = manager.repo.load(ctx)
	require.NoError(t, err)
	require.Nil(t, snapshot.Users["alice"])
}

func TestAuthManagerProtectsRootWhileEnabled(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()
	manager := newAuthManager(server.backend)
	ctx := context.Background()
	require.NoError(t, manager.userAdd(ctx, &etcdserverpb.AuthUserAddRequest{Name: "root", Password: "secret"}))
	require.NoError(t, manager.roleAdd(ctx, "root"))
	require.NoError(t, manager.userGrantRole(ctx, "root", "root"))
	require.NoError(t, manager.enable(ctx))
	require.ErrorIs(t, manager.userDelete(ctx, "root"), rpctypes.ErrInvalidAuthMgmt)
	require.ErrorIs(t, manager.roleDelete(ctx, "root"), rpctypes.ErrInvalidAuthMgmt)
	require.ErrorIs(t, manager.userRevokeRole(ctx, "root", "root"), rpctypes.ErrInvalidAuthMgmt)
	require.NoError(t, manager.disable(ctx))
	require.NoError(t, manager.userDelete(ctx, "root"))
}

func TestAuthManagerRejectsInvalidPermissionRanges(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()
	manager := newAuthManager(server.backend)
	ctx := context.Background()
	require.NoError(t, manager.roleAdd(ctx, "reader"))
	require.ErrorIs(t, manager.roleGrantPermission(ctx, "reader", nil), rpctypes.ErrGRPCPermissionNotGiven)
	require.ErrorIs(t, manager.roleGrantPermission(ctx, "reader", &authpb.Permission{}), rpctypes.ErrInvalidAuthMgmt)
	require.ErrorIs(t, manager.roleGrantPermission(ctx, "reader", &authpb.Permission{Key: []byte("z"), RangeEnd: []byte("a")}), rpctypes.ErrInvalidAuthMgmt)
	require.NoError(t, manager.roleGrantPermission(ctx, "reader", &authpb.Permission{Key: []byte("z"), RangeEnd: []byte{0}}))
}

func TestAuthManagerBootstrapErrors(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()
	manager := newAuthManager(server.backend)
	ctx := context.Background()

	require.ErrorIs(t, manager.userAdd(ctx, &etcdserverpb.AuthUserAddRequest{}), rpctypes.ErrUserEmpty)
	require.ErrorIs(t, manager.userAdd(ctx, &etcdserverpb.AuthUserAddRequest{Name: "bad-hash", HashedPassword: "%%%"}), errNoPasswordUser)
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

func TestAuthManagerPlaintextPasswordOverridesHash(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()
	manager := newAuthManager(server.backend)
	ctx := context.Background()
	require.NoError(t, manager.userAdd(ctx, &etcdserverpb.AuthUserAddRequest{
		Name: "alice", Password: "plaintext", HashedPassword: "%%%",
	}))
	snapshot, err := manager.repo.load(ctx)
	require.NoError(t, err)
	require.NoError(t, bcrypt.CompareHashAndPassword(snapshot.Users["alice"].Password, []byte("plaintext")))

	require.NoError(t, manager.userChangePassword(ctx, "alice", "changed", "%%%"))
	snapshot, err = manager.repo.load(ctx)
	require.NoError(t, err)
	require.NoError(t, bcrypt.CompareHashAndPassword(snapshot.Users["alice"].Password, []byte("changed")))
}

func TestAuthManagerRootRoleIsImplicit(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()
	manager := newAuthManager(server.backend)
	ctx := context.Background()
	require.NoError(t, manager.userAdd(ctx, &etcdserverpb.AuthUserAddRequest{Name: "root", Password: "secret"}))
	require.NoError(t, manager.userGrantRole(ctx, "root", "root"))
	beforeEnable, err := manager.repo.load(ctx)
	require.NoError(t, err)
	require.NoError(t, manager.enable(ctx))
	afterEnable, err := manager.repo.load(ctx)
	require.NoError(t, err)
	require.Equal(t, beforeEnable.Config.Revision, afterEnable.Config.Revision)
	require.NoError(t, manager.disable(ctx))
	afterDisable, err := manager.repo.load(ctx)
	require.NoError(t, err)
	require.Equal(t, afterEnable.Config.Revision+1, afterDisable.Config.Revision)
}
