package etcd

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/api/v3/authpb"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
	"google.golang.org/grpc/metadata"
)

func TestAuthCallerAndPermissionRangeUnion(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()
	ctx := context.Background()
	m := server.auth
	require.NoError(t, m.userAdd(ctx, &etcdserverpb.AuthUserAddRequest{Name: "root", Password: "root-secret"}))
	require.NoError(t, m.roleAdd(ctx, "root"))
	require.NoError(t, m.userGrantRole(ctx, "root", "root"))
	require.NoError(t, m.userAdd(ctx, &etcdserverpb.AuthUserAddRequest{Name: "alice", Password: "secret"}))
	for _, role := range []string{"left", "right"} {
		require.NoError(t, m.roleAdd(ctx, role))
		require.NoError(t, m.userGrantRole(ctx, "alice", role))
	}
	require.NoError(t, m.roleGrantPermission(ctx, "left", &authpb.Permission{PermType: authpb.READ, Key: []byte("a"), RangeEnd: []byte("m")}))
	require.NoError(t, m.roleGrantPermission(ctx, "right", &authpb.Permission{PermType: authpb.READWRITE, Key: []byte("m"), RangeEnd: []byte("z")}))
	require.NoError(t, m.enable(ctx))
	token, err := server.tokens.authenticate(ctx, "alice", "secret")
	require.NoError(t, err)
	caller, err := server.authCallerFromContext(metadata.NewIncomingContext(ctx, metadata.Pairs(rpctypes.TokenFieldNameGRPC, token)))
	require.NoError(t, err)
	require.NoError(t, caller.require([]byte("b"), []byte("y"), authpb.READ), "adjacent permissions from separate roles must merge")
	require.ErrorIs(t, caller.require([]byte("b"), []byte("y"), authpb.WRITE), rpctypes.ErrPermissionDenied)
	require.NoError(t, caller.require([]byte("m"), nil, authpb.WRITE))
	require.ErrorIs(t, caller.require([]byte("z"), nil, authpb.READ), rpctypes.ErrPermissionDenied)
}

func TestAuthCallerFailsClosedAndDisabledBypasses(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()
	ctx := context.Background()
	caller, err := server.authCallerFromContext(ctx)
	require.NoError(t, err)
	require.Nil(t, caller)

	_, _ = bootstrapAuthForToken(t, server)
	_, err = server.authCallerFromContext(ctx)
	require.ErrorIs(t, err, rpctypes.ErrUserEmpty)
}

func TestAuthPermissionOpenEndedAndGap(t *testing.T) {
	caller := &authCaller{username: "alice", snapshot: &authSnapshot{
		Users: map[string]*authpb.User{"alice": {Name: []byte("alice"), Roles: []string{"reader"}}},
		Roles: map[string]*authpb.Role{"reader": {Name: []byte("reader"), KeyPermission: []*authpb.Permission{
			{PermType: authpb.READ, Key: []byte("a"), RangeEnd: []byte("m")},
			{PermType: authpb.READ, Key: []byte("n"), RangeEnd: []byte{0}},
		}}},
	}}
	require.True(t, caller.permits([]byte("n"), []byte{0}, authpb.READ))
	require.True(t, caller.permits([]byte("zz"), nil, authpb.READ))
	require.False(t, caller.permits([]byte("b"), []byte("z"), authpb.READ), "a gap between m and n must deny the whole range")
	require.False(t, caller.permits([]byte("n"), []byte{0}, authpb.WRITE))
}
