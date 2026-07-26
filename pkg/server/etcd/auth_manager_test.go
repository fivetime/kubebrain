package etcd

import (
	"bytes"
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/api/v3/authpb"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
	"golang.org/x/crypto/bcrypt"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/kubewharf/kubebrain/pkg/storage"
)

func runConcurrentAuthOperations(count int, operation func(int) error) []error {
	errs := make([]error, count)
	start := make(chan struct{})
	var ready sync.WaitGroup
	var done sync.WaitGroup
	ready.Add(count)
	done.Add(count)
	for i := 0; i < count; i++ {
		go func(index int) {
			defer done.Done()
			ready.Done()
			<-start
			errs[index] = operation(index)
		}(i)
	}
	ready.Wait()
	close(start)
	done.Wait()
	return errs
}

func TestAuthManagerBootstrapPersistsWithIndependentRevision(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()
	manager := newAuthManager(server.backend)
	ctx := context.Background()
	before := server.backend.GetCurrentRevision()

	requireAuthManagerError(t, manager.enable(ctx), rpctypes.ErrRootUserNotExist, codes.Unknown, "etcdserver: root user does not exist")
	require.NoError(t, manager.userAdd(ctx, &etcdserverpb.AuthUserAddRequest{Name: "root", Password: "secret"}))
	requireAuthManagerError(t, manager.enable(ctx), rpctypes.ErrRootRoleNotExist, codes.Unknown, "etcdserver: root user does not have root role")
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

func TestAuthManagerUsesConfiguredBcryptCost(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()
	server.SetAuthConfiguration("simple", uint(bcrypt.MinCost), 300)
	require.NoError(t, server.auth.userAdd(context.Background(), &etcdserverpb.AuthUserAddRequest{
		Name: "configured", Password: "secret",
	}))
	snapshot, err := server.auth.repo.load(context.Background())
	require.NoError(t, err)
	cost, err := bcrypt.Cost(snapshot.Users["configured"].Password)
	require.NoError(t, err)
	require.Equal(t, bcrypt.MinCost, cost)

	server.SetAuthConfiguration("simple", uint(bcrypt.MaxCost+1), 300)
	require.Equal(t, bcrypt.DefaultCost, server.auth.bcryptCost, "invalid cost must use etcd's default")
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
	requireAuthManagerError(t, manager.roleRevokePermission(ctx, "reader", []byte("missing"), nil), rpctypes.ErrPermissionNotGranted, codes.Unknown, "etcdserver: permission is not granted to the role")
	require.NoError(t, manager.roleRevokePermission(ctx, "reader", permission.Key, permission.RangeEnd))
	requireAuthManagerError(t, manager.userRevokeRole(ctx, "alice", "missing"), rpctypes.ErrRoleNotGranted, codes.Unknown, "etcdserver: role is not granted to the user")
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

func TestAuthManagerRoleGrantPermissionSameKeySearchMatchesEtcd(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()
	manager := newAuthManager(server.backend)
	ctx := context.Background()
	require.NoError(t, manager.roleAdd(ctx, "reader"))

	require.NoError(t, manager.roleGrantPermission(ctx, "reader", &authpb.Permission{
		PermType: authpb.READ, Key: []byte("a"), RangeEnd: []byte("c"),
	}))
	require.NoError(t, manager.roleGrantPermission(ctx, "reader", &authpb.Permission{
		PermType: authpb.WRITE, Key: []byte("a"), RangeEnd: []byte("b"),
	}))
	require.NoError(t, manager.roleGrantPermission(ctx, "reader", &authpb.Permission{
		PermType: authpb.READWRITE, Key: []byte("a"), RangeEnd: []byte("b"),
	}))
	snapshot, err := manager.repo.load(ctx)
	require.NoError(t, err)
	perms := snapshot.Roles["reader"].KeyPermission
	require.Len(t, perms, 3)
	var sameRangeTypes []authpb.Permission_Type
	for _, perm := range perms {
		if bytes.Equal(perm.RangeEnd, []byte("b")) {
			sameRangeTypes = append(sameRangeTypes, perm.PermType)
		}
	}
	require.ElementsMatch(t, []authpb.Permission_Type{authpb.WRITE, authpb.READWRITE}, sameRangeTypes)

	require.NoError(t, manager.roleRevokePermission(ctx, "reader", []byte("a"), []byte("b")))
	snapshot, err = manager.repo.load(ctx)
	require.NoError(t, err)
	require.Len(t, snapshot.Roles["reader"].KeyPermission, 1)
	require.Equal(t, []byte("c"), snapshot.Roles["reader"].KeyPermission[0].RangeEnd)
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
	requireAuthManagerError(t, manager.userDelete(ctx, "root"), rpctypes.ErrInvalidAuthMgmt, codes.Unknown, "etcdserver: invalid auth management")
	requireAuthManagerError(t, manager.roleDelete(ctx, "root"), rpctypes.ErrInvalidAuthMgmt, codes.Unknown, "etcdserver: invalid auth management")
	requireAuthManagerError(t, manager.userRevokeRole(ctx, "root", "root"), rpctypes.ErrInvalidAuthMgmt, codes.Unknown, "etcdserver: invalid auth management")
	require.NoError(t, manager.disable(ctx))
	require.NoError(t, manager.userDelete(ctx, "root"))
}

func TestAuthManagerRejectsInvalidPermissionRanges(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()
	manager := newAuthManager(server.backend)
	ctx := context.Background()
	require.NoError(t, manager.roleAdd(ctx, "reader"))
	requireAuthManagerError(t, manager.roleGrantPermission(ctx, "reader", nil), rpctypes.ErrGRPCPermissionNotGiven, codes.InvalidArgument, "etcdserver: permission not given")
	requireAuthManagerError(t, manager.roleGrantPermission(ctx, "reader", &authpb.Permission{}), rpctypes.ErrInvalidAuthMgmt, codes.Unknown, "etcdserver: invalid auth management")
	requireAuthManagerError(t, manager.roleGrantPermission(ctx, "reader", &authpb.Permission{Key: []byte("z"), RangeEnd: []byte("a")}), rpctypes.ErrInvalidAuthMgmt, codes.Unknown, "etcdserver: invalid auth management")
	require.NoError(t, manager.roleGrantPermission(ctx, "reader", &authpb.Permission{Key: []byte("z"), RangeEnd: []byte{0}}))
}

func TestAuthManagerBootstrapErrors(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()
	manager := newAuthManager(server.backend)
	ctx := context.Background()

	requireAuthManagerError(t, manager.userAdd(ctx, &etcdserverpb.AuthUserAddRequest{}), rpctypes.ErrUserEmpty, codes.Unknown, "etcdserver: user name is empty")
	requireAuthManagerError(t, manager.roleAdd(ctx, ""), rpctypes.ErrRoleEmpty, codes.Unknown, "etcdserver: role name is empty")
	requireAuthManagerError(t, manager.userGrantRole(ctx, "missing", "missing"), rpctypes.ErrUserNotFound, codes.Unknown, "etcdserver: user name not found")
	require.NoError(t, manager.userAdd(ctx, &etcdserverpb.AuthUserAddRequest{Name: "alice", Password: "pw"}))
	requireAuthManagerError(t, manager.userAdd(ctx, &etcdserverpb.AuthUserAddRequest{Name: "alice", Password: "pw"}), rpctypes.ErrUserAlreadyExist, codes.Unknown, "etcdserver: user name already exists")
	requireAuthManagerError(t, manager.userGrantRole(ctx, "alice", "missing"), rpctypes.ErrRoleNotFound, codes.Unknown, "etcdserver: role name not found")
	require.ErrorIs(t, manager.userChangePassword(ctx, "alice", "", "%%%"), errNoPasswordUser)
	require.NoError(t, manager.roleAdd(ctx, "reader"))
	requireAuthManagerError(t, manager.roleAdd(ctx, "reader"), rpctypes.ErrRoleAlreadyExist, codes.Unknown, "etcdserver: role name already exists")
	require.NoError(t, manager.userGrantRole(ctx, "alice", "reader"))
	require.NoError(t, manager.userGrantRole(ctx, "alice", "reader"), "grant role must be idempotent")
}

func TestAuthManagerUserAddIgnoresHashedPasswordLikePublicEtcd(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()
	manager := newAuthManager(server.backend)
	ctx := context.Background()
	require.NoError(t, manager.userAdd(ctx, &etcdserverpb.AuthUserAddRequest{
		Name: "empty", HashedPassword: "%%%",
	}))
	snapshot, err := manager.repo.load(ctx)
	require.NoError(t, err)
	require.NoError(t, bcrypt.CompareHashAndPassword(snapshot.Users["empty"].Password, nil))

	require.NoError(t, manager.userAdd(ctx, &etcdserverpb.AuthUserAddRequest{
		Name: "alice", Password: "plaintext", HashedPassword: "%%%",
	}))
	snapshot, err = manager.repo.load(ctx)
	require.NoError(t, err)
	require.NoError(t, bcrypt.CompareHashAndPassword(snapshot.Users["alice"].Password, []byte("plaintext")))

	require.NoError(t, manager.userChangePassword(ctx, "alice", "changed", "%%%"))
	snapshot, err = manager.repo.load(ctx)
	require.NoError(t, err)
	require.NoError(t, bcrypt.CompareHashAndPassword(snapshot.Users["alice"].Password, []byte("changed")))
}

func TestAuthManagerEmptyPasswordChangeMatchesEtcd(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()
	manager := newAuthManager(server.backend)
	ctx := context.Background()
	require.NoError(t, manager.userAdd(ctx, &etcdserverpb.AuthUserAddRequest{
		Name: "root", Password: "secret",
	}))
	require.NoError(t, manager.roleAdd(ctx, "root"))
	require.NoError(t, manager.userGrantRole(ctx, "root", "root"))
	require.NoError(t, manager.enable(ctx))

	_, err := server.Authenticate(ctx, &etcdserverpb.AuthenticateRequest{Name: "root", Password: "secret"})
	require.NoError(t, err)
	require.NoError(t, manager.userChangePassword(ctx, "root", "", ""))
	_, err = server.Authenticate(ctx, &etcdserverpb.AuthenticateRequest{Name: "root", Password: ""})
	requireAuthManagerError(t, err, rpctypes.ErrAuthFailed, codes.Unknown, "etcdserver: authentication failed, invalid user ID or password")
	_, err = server.Authenticate(ctx, &etcdserverpb.AuthenticateRequest{Name: "root", Password: "secret"})
	requireAuthManagerError(t, err, rpctypes.ErrAuthFailed, codes.Unknown, "etcdserver: authentication failed, invalid user ID or password")
}

func TestAuthManagerConcurrentDistinctMutationsHaveNoLostRevision(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()
	manager := newAuthManager(server.backend)
	ctx := context.Background()
	const count = 64

	errs := runConcurrentAuthOperations(count, func(index int) error {
		return manager.roleAdd(ctx, fmt.Sprintf("role-%02d", index))
	})
	for _, err := range errs {
		require.NoError(t, err)
	}
	snapshot, err := manager.repo.load(ctx)
	require.NoError(t, err)
	require.Len(t, snapshot.Roles, count)
	require.Equal(t, uint64(initialAuthRevision+count), snapshot.Config.Revision)
}

func TestAuthManagerConcurrentDuplicateMutationCommitsOnce(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()
	manager := newAuthManager(server.backend)
	ctx := context.Background()
	const count = 32

	errs := runConcurrentAuthOperations(count, func(int) error {
		return manager.roleAdd(ctx, "shared")
	})
	succeeded := 0
	for _, err := range errs {
		if err == nil {
			succeeded++
			continue
		}
		requireAuthManagerError(t, err, rpctypes.ErrRoleAlreadyExist, codes.Unknown, "etcdserver: role name already exists")
	}
	require.Equal(t, 1, succeeded)
	snapshot, err := manager.repo.load(ctx)
	require.NoError(t, err)
	require.Len(t, snapshot.Roles, 1)
	require.Equal(t, uint64(initialAuthRevision+1), snapshot.Config.Revision)
}

func TestRetryAuthMutationStopsAtContextDeadline(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	err := retryAuthMutation(ctx, func(*authSnapshot) error {
		return storage.ErrCASFailed
	}, func(context.Context) (*authSnapshot, error) {
		return &authSnapshot{}, nil
	})
	require.ErrorIs(t, err, context.DeadlineExceeded)
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

func requireAuthManagerError(t *testing.T, err error, want error, code codes.Code, message string) {
	t.Helper()
	require.ErrorIs(t, err, want)
	require.Equal(t, code, status.Code(err))
	require.Equal(t, message, status.Convert(err).Message())
}
