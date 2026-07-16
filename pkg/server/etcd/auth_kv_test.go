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

func setupAuthKVUser(t *testing.T, server *RPCServer) context.Context {
	t.Helper()
	ctx := context.Background()
	m := server.auth
	require.NoError(t, m.userAdd(ctx, &etcdserverpb.AuthUserAddRequest{Name: "root", Password: "root-secret"}))
	require.NoError(t, m.roleAdd(ctx, "root"))
	require.NoError(t, m.userGrantRole(ctx, "root", "root"))
	require.NoError(t, m.userAdd(ctx, &etcdserverpb.AuthUserAddRequest{Name: "alice", Password: "secret"}))
	require.NoError(t, m.roleAdd(ctx, "allowed"))
	require.NoError(t, m.userGrantRole(ctx, "alice", "allowed"))
	require.NoError(t, m.roleGrantPermission(ctx, "allowed", &authpb.Permission{
		PermType: authpb.READWRITE, Key: []byte("/allowed/"), RangeEnd: []byte("/allowed0"),
	}))
	require.NoError(t, m.enable(ctx))
	token, err := server.tokens.authenticate(ctx, "alice", "secret")
	require.NoError(t, err)
	return metadata.NewIncomingContext(ctx, metadata.Pairs(rpctypes.TokenFieldNameGRPC, token))
}

func TestAuthKVUnaryHandlersEnforcePermissions(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()
	ctx := setupAuthKVUser(t, server)

	_, err := server.Put(ctx, &etcdserverpb.PutRequest{Key: []byte("/allowed/a"), Value: []byte("ok")})
	require.NoError(t, err)
	_, err = server.Put(ctx, &etcdserverpb.PutRequest{Key: []byte("/denied/a"), Value: []byte("no")})
	require.ErrorIs(t, err, rpctypes.ErrPermissionDenied)
	resp, err := server.Range(ctx, &etcdserverpb.RangeRequest{Key: []byte("/allowed/a")})
	require.NoError(t, err)
	require.Len(t, resp.Kvs, 1)
	_, err = server.Range(ctx, &etcdserverpb.RangeRequest{Key: []byte("/denied/a")})
	require.ErrorIs(t, err, rpctypes.ErrPermissionDenied)

	denied, err := server.backend.Get(context.Background(), &etcdserverpb.RangeRequest{Key: []byte("/denied/a")})
	require.NoError(t, err)
	require.Empty(t, denied.Kvs, "denied put must not reach storage")
}

func TestAuthTxnChecksBothBranchesBeforeWriting(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()
	ctx := setupAuthKVUser(t, server)
	txn := &etcdserverpb.TxnRequest{
		Success: []*etcdserverpb.RequestOp{{Request: &etcdserverpb.RequestOp_RequestPut{
			RequestPut: &etcdserverpb.PutRequest{Key: []byte("/allowed/created"), Value: []byte("value")},
		}}},
		Failure: []*etcdserverpb.RequestOp{{Request: &etcdserverpb.RequestOp_RequestRange{
			RequestRange: &etcdserverpb.RangeRequest{Key: []byte("/denied/secret")},
		}}},
	}
	_, err := server.Txn(ctx, txn)
	require.ErrorIs(t, err, rpctypes.ErrPermissionDenied)
	stored, err := server.backend.Get(context.Background(), &etcdserverpb.RangeRequest{Key: []byte("/allowed/created")})
	require.NoError(t, err)
	require.Empty(t, stored.Kvs, "authorization must reject the full txn before its selected branch writes")
}

func TestAuthTxnChecksNestedCompare(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()
	ctx := setupAuthKVUser(t, server)
	txn := &etcdserverpb.TxnRequest{Success: []*etcdserverpb.RequestOp{{Request: &etcdserverpb.RequestOp_RequestTxn{
		RequestTxn: &etcdserverpb.TxnRequest{
			Compare: []*etcdserverpb.Compare{{Key: []byte("/denied/secret")}},
			Success: []*etcdserverpb.RequestOp{{Request: &etcdserverpb.RequestOp_RequestPut{
				RequestPut: &etcdserverpb.PutRequest{Key: []byte("/allowed/nested"), Value: []byte("value")},
			}}},
		},
	}}}}
	_, err := server.Txn(ctx, txn)
	require.ErrorIs(t, err, rpctypes.ErrPermissionDenied)
	stored, err := server.backend.Get(context.Background(), &etcdserverpb.RangeRequest{Key: []byte("/allowed/nested")})
	require.NoError(t, err)
	require.Empty(t, stored.Kvs)
}

func TestAuthDeletePrevKVRequiresReadAndWrite(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()
	ctx := context.Background()
	require.NoError(t, server.auth.userAdd(ctx, &etcdserverpb.AuthUserAddRequest{Name: "root", Password: "root-secret"}))
	require.NoError(t, server.auth.roleAdd(ctx, "root"))
	require.NoError(t, server.auth.userGrantRole(ctx, "root", "root"))
	require.NoError(t, server.auth.userAdd(ctx, &etcdserverpb.AuthUserAddRequest{Name: "writer", Password: "secret"}))
	require.NoError(t, server.auth.roleAdd(ctx, "writer"))
	require.NoError(t, server.auth.userGrantRole(ctx, "writer", "writer"))
	require.NoError(t, server.auth.roleGrantPermission(ctx, "writer", &authpb.Permission{PermType: authpb.WRITE, Key: []byte("key")}))
	_, err := server.Put(ctx, &etcdserverpb.PutRequest{Key: []byte("key"), Value: []byte("value")})
	require.NoError(t, err)
	require.NoError(t, server.auth.enable(ctx))
	token, err := server.tokens.authenticate(ctx, "writer", "secret")
	require.NoError(t, err)
	ctx = metadata.NewIncomingContext(ctx, metadata.Pairs(rpctypes.TokenFieldNameGRPC, token))

	_, err = server.DeleteRange(ctx, &etcdserverpb.DeleteRangeRequest{Key: []byte("key"), PrevKv: true})
	require.ErrorIs(t, err, rpctypes.ErrPermissionDenied)
	stored, err := server.backend.Get(context.Background(), &etcdserverpb.RangeRequest{Key: []byte("key")})
	require.NoError(t, err)
	require.Len(t, stored.Kvs, 1, "read denial for PrevKV must happen before deletion")
}
