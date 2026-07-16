package etcd

import (
	"bytes"
	"context"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
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
