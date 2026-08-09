package etcd

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/api/v3/authpb"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
)

func watchCreate(key string) *etcdserverpb.WatchRequest {
	return &etcdserverpb.WatchRequest{RequestUnion: &etcdserverpb.WatchRequest_CreateRequest{
		CreateRequest: &etcdserverpb.WatchCreateRequest{Key: []byte(key)},
	}}
}

func TestAuthWatchRejectsUnauthorizedCreateAndKeepsStreamUsable(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()
	ctx := setupAuthKVUser(t, server)
	stream := &scriptedWatchServer{
		fakeWatchServer: &fakeWatchServer{ctx: ctx},
		reqs:            []*etcdserverpb.WatchRequest{watchCreate("/denied/key"), watchCreate("/allowed/key")},
	}
	requireWatchCanceled(t, server.Watch(stream))
	require.GreaterOrEqual(t, len(stream.sent), 2)
	require.Equal(t, int64(-1), stream.sent[0].WatchId)
	require.True(t, stream.sent[0].Created)
	require.True(t, stream.sent[0].Canceled)
	require.Equal(t, rpctypes.ErrGRPCPermissionDenied.Error(), stream.sent[0].CancelReason)
	require.True(t, stream.sent[1].Created, "a rejected create must not terminate the multiplexed stream")
	require.False(t, stream.sent[1].Canceled)
}

func TestAuthWatchMissingTokenReturnsCanceledCreate(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()
	_ = setupAuthKVUser(t, server)
	stream := &scriptedWatchServer{
		fakeWatchServer: &fakeWatchServer{},
		reqs:            []*etcdserverpb.WatchRequest{watchCreate("/allowed/key")},
	}
	requireWatchCanceled(t, server.Watch(stream))
	require.Len(t, stream.sent, 1)
	require.True(t, stream.sent[0].Canceled)
	require.Equal(t, rpctypes.ErrGRPCUserEmpty.Error(), stream.sent[0].CancelReason)
}

func TestAuthWatchFromKeyDoesNotUseExactKeyPermission(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()
	ctx := setupAuthKVUser(t, server)
	require.NoError(t, server.auth.roleRevokePermission(context.Background(), "allowed", []byte("/allowed/"), []byte("/allowed0")))
	require.NoError(t, server.auth.roleGrantPermission(context.Background(), "allowed", &authpb.Permission{
		PermType: authpb.READ, Key: []byte("/allowed/key"),
	}))

	stream := &scriptedWatchServer{
		fakeWatchServer: &fakeWatchServer{ctx: ctx},
		reqs: []*etcdserverpb.WatchRequest{{RequestUnion: &etcdserverpb.WatchRequest_CreateRequest{
			CreateRequest: &etcdserverpb.WatchCreateRequest{
				Key: []byte("/allowed/key"), RangeEnd: []byte{0},
			},
		}}},
	}
	requireWatchCanceled(t, server.Watch(stream))
	require.Len(t, stream.sent, 1)
	require.True(t, stream.sent[0].Created)
	require.True(t, stream.sent[0].Canceled)
	require.Equal(t, int64(-1), stream.sent[0].WatchId)
	require.Equal(t, rpctypes.ErrGRPCPermissionDenied.Error(), stream.sent[0].CancelReason)
}
