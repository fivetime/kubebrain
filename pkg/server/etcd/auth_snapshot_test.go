package etcd

import (
	"bytes"
	"context"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
	"google.golang.org/grpc/metadata"
)

type authRangeHookBackend struct {
	BackendShim
	onUsers sync.Once
	hook    func()
}

func (b *authRangeHookBackend) InternalRange(ctx context.Context, prefix []byte) (map[string][]byte, error) {
	values, err := b.BackendShim.InternalRange(ctx, prefix)
	if err == nil && bytes.Equal(prefix, authUsersKey) {
		b.onUsers.Do(b.hook)
	}
	return values, err
}

func TestAuthSnapshotCacheInvalidatesOnPersistedRevision(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()
	ctx := context.Background()
	manager := newAuthManager(server.backend)
	cache := newAuthSnapshotCache(server.backend)

	initial, err := cache.current(ctx)
	require.NoError(t, err)
	require.Equal(t, uint64(initialAuthRevision), initial.Config.Revision)

	require.NoError(t, manager.userAdd(ctx, &etcdserverpb.AuthUserAddRequest{Name: "alice", Password: "secret"}))
	updated, err := cache.current(ctx)
	require.NoError(t, err)
	require.Greater(t, updated.Config.Revision, initial.Config.Revision)
	require.NotNil(t, updated.Users["alice"])

	reused, err := cache.current(ctx)
	require.NoError(t, err)
	require.Same(t, updated, reused)
}

func TestAuthComponentsReloadRevisionAndPermissionsFromDurableBackend(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()
	ctx := context.Background()
	_ = setupAuthKVUser(t, server)

	require.NoError(t, server.auth.userAdd(ctx, &etcdserverpb.AuthUserAddRequest{
		Name: "transient", Password: "secret",
	}))
	require.NoError(t, server.auth.userDelete(ctx, "transient"))
	before, err := server.auth.repo.load(ctx)
	require.NoError(t, err)
	require.True(t, before.Config.Enabled)
	require.Nil(t, before.Users["transient"])
	require.NotNil(t, before.Users["alice"])
	require.NotNil(t, before.Roles["allowed"])

	// Model a process restart at the auth component boundary: no in-memory
	// snapshot or token state is retained, while the durable DBaaS backend is.
	server.auth = newAuthManager(server.backend)
	server.tokens = newAuthTokenManager(server.backend)
	after, err := server.auth.repo.load(ctx)
	require.NoError(t, err)
	require.Equal(t, before.Config, after.Config)
	require.Nil(t, after.Users["transient"])
	require.Equal(t, before.Users["alice"].Roles, after.Users["alice"].Roles)
	require.Equal(t, before.Roles["allowed"].KeyPermission, after.Roles["allowed"].KeyPermission)

	token, err := server.tokens.authenticate(ctx, "alice", "secret")
	require.NoError(t, err)
	authenticated := metadata.NewIncomingContext(
		ctx, metadata.Pairs(rpctypes.TokenFieldNameGRPC, token),
	)
	status, err := server.AuthStatus(authenticated, &etcdserverpb.AuthStatusRequest{})
	require.NoError(t, err)
	require.True(t, status.Enabled)
	require.Equal(t, before.Config.Revision, status.AuthRevision)
	_, err = server.Put(authenticated, &etcdserverpb.PutRequest{
		Key: []byte("/allowed/reloaded"), Value: []byte("value"),
	})
	require.NoError(t, err)
	_, err = server.Put(authenticated, &etcdserverpb.PutRequest{
		Key: []byte("/denied/reloaded"), Value: []byte("secret"),
	})
	require.ErrorIs(t, err, rpctypes.ErrPermissionDenied)
}

func TestAuthRepositoryLoadReturnsSelfConsistentSnapshot(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()
	ctx := context.Background()
	manager := newAuthManager(server.backend)
	require.NoError(t, manager.userAdd(ctx, &etcdserverpb.AuthUserAddRequest{Name: "alice", Password: "secret"}))

	snapshot, err := newAuthRepository(server.backend).load(ctx)
	require.NoError(t, err)
	require.Equal(t, uint64(2), snapshot.Config.Revision)
	require.NotNil(t, snapshot.Users["alice"])
}

func TestAuthRepositoryRetriesMutationDuringSnapshotLoad(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()
	ctx := context.Background()
	manager := newAuthManager(server.backend)
	require.NoError(t, manager.userAdd(ctx, &etcdserverpb.AuthUserAddRequest{Name: "alice", Password: "secret"}))

	hooked := &authRangeHookBackend{BackendShim: server.backend}
	hooked.hook = func() { require.NoError(t, manager.roleAdd(ctx, "reader")) }
	snapshot, err := newAuthRepository(hooked).load(ctx)
	require.NoError(t, err)
	require.Equal(t, uint64(3), snapshot.Config.Revision)
	require.NotNil(t, snapshot.Users["alice"])
	require.NotNil(t, snapshot.Roles["reader"], "load must retry instead of publishing records from mixed auth revisions")
}
