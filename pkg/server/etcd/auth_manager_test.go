package etcd

import (
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
	require.ErrorIs(t, err, rpctypes.ErrAuthFailed)
	_, err = server.Authenticate(ctx, &etcdserverpb.AuthenticateRequest{Name: "root", Password: "secret"})
	require.ErrorIs(t, err, rpctypes.ErrAuthFailed)
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
		require.ErrorIs(t, err, rpctypes.ErrRoleAlreadyExist)
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
