package etcd

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/api/v3/authpb"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
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

func TestAuthRangeReadBarrierPrecedesAuthLikeEtcd(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()
	setupAuthKVUser(t, server)
	plain := context.Background()
	barrierErr := errors.New("leader read barrier failed")
	var barrierCalls int
	server.peers = testPeerService{syncReadFn: func(context.Context) error {
		barrierCalls++
		return barrierErr
	}}

	_, err := server.Range(plain, &etcdserverpb.RangeRequest{Key: []byte("/allowed/a")})
	require.Equal(t, codes.Unavailable, status.Code(err))
	require.Equal(t, barrierErr.Error(), status.Convert(err).Message())
	require.Equal(t, 1, barrierCalls)

	_, err = server.Range(plain, &etcdserverpb.RangeRequest{Key: []byte("/allowed/a"), Serializable: true})
	require.ErrorIs(t, err, rpctypes.ErrUserEmpty)
	require.Equal(t, 1, barrierCalls)

	stream := &fakeRangeStreamServer{ctx: plain}
	err = server.RangeStream(&etcdserverpb.RangeRequest{Key: []byte("/allowed/"), RangeEnd: []byte("/allowed0")}, stream)
	require.Equal(t, codes.Unavailable, status.Code(err))
	require.Equal(t, barrierErr.Error(), status.Convert(err).Message())
	require.Equal(t, 2, barrierCalls)

	stream = &fakeRangeStreamServer{ctx: plain}
	err = server.RangeStream(&etcdserverpb.RangeRequest{Key: []byte("/allowed/"), RangeEnd: []byte("/allowed0"), Serializable: true}, stream)
	require.ErrorIs(t, err, rpctypes.ErrUserEmpty)
	require.Equal(t, 2, barrierCalls)
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

func TestAuthLeasedPutRechecksAttachmentsAfterLeaderAdmission(t *testing.T) {
	for _, tc := range []struct {
		name string
		call func(context.Context, *RPCServer, int64) error
	}{
		{
			name: "put",
			call: func(ctx context.Context, server *RPCServer, leaseID int64) error {
				_, err := server.Put(ctx, &etcdserverpb.PutRequest{
					Key: []byte("/allowed/put"), Value: []byte("value"), Lease: leaseID,
				})
				return err
			},
		},
		{
			name: "nested txn put",
			call: func(ctx context.Context, server *RPCServer, leaseID int64) error {
				_, err := server.Txn(ctx, &etcdserverpb.TxnRequest{
					Success: []*etcdserverpb.RequestOp{{Request: &etcdserverpb.RequestOp_RequestTxn{
						RequestTxn: &etcdserverpb.TxnRequest{
							Success: []*etcdserverpb.RequestOp{{Request: &etcdserverpb.RequestOp_RequestPut{
								RequestPut: &etcdserverpb.PutRequest{
									Key: []byte("/allowed/txn"), Value: []byte("value"), Lease: leaseID,
								},
							}}},
						},
					}}},
				})
				return err
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			server, closeFn := newTestRPCServer(t)
			defer closeFn()
			aliceCtx := setupAuthKVUser(t, server)
			lease, err := server.LeaseGrant(aliceCtx, &etcdserverpb.LeaseGrantRequest{TTL: 60})
			require.NoError(t, err)
			_, err = server.Put(aliceCtx, &etcdserverpb.PutRequest{
				Key: []byte("/allowed/existing"), Value: []byte("allowed"), Lease: lease.ID,
			})
			require.NoError(t, err)

			admitted := make(chan struct{})
			release := make(chan struct{})
			var admittedOnce sync.Once
			server.peers = testPeerService{
				isLeader: true,
				epochFn: func() (uint64, bool) {
					admittedOnce.Do(func() { close(admitted) })
					<-release
					return 1, true
				},
			}

			done := make(chan error, 1)
			go func() { done <- tc.call(aliceCtx, server, lease.ID) }()
			select {
			case <-admitted:
			case <-time.After(time.Second):
				t.Fatal("leased write did not pass its initial authorization")
			}

			// Model a root writer that attached a key Alice cannot write while
			// this request was between admission authorization and the lease lock.
			server.bindKeyToLease(context.Background(), lease.ID, "/denied/concurrent")
			close(release)
			require.ErrorIs(t, <-done, rpctypes.ErrPermissionDenied)

			key := []byte("/allowed/put")
			if tc.name == "nested txn put" {
				key = []byte("/allowed/txn")
			}
			stored, err := server.backend.Get(context.Background(), &etcdserverpb.RangeRequest{Key: key})
			require.NoError(t, err)
			require.Empty(t, stored.Kvs, "stale lease authorization must not reach storage")
		})
	}
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

func TestAuthTxnPutPrevKVRequiresReadAndWrite(t *testing.T) {
	for _, tc := range []struct {
		name string
		txn  func(*etcdserverpb.PutRequest) *etcdserverpb.TxnRequest
	}{
		{
			name: "top level",
			txn: func(put *etcdserverpb.PutRequest) *etcdserverpb.TxnRequest {
				return &etcdserverpb.TxnRequest{Success: []*etcdserverpb.RequestOp{{
					Request: &etcdserverpb.RequestOp_RequestPut{RequestPut: put},
				}}}
			},
		},
		{
			name: "nested",
			txn: func(put *etcdserverpb.PutRequest) *etcdserverpb.TxnRequest {
				return &etcdserverpb.TxnRequest{Success: []*etcdserverpb.RequestOp{{
					Request: &etcdserverpb.RequestOp_RequestTxn{RequestTxn: &etcdserverpb.TxnRequest{
						Success: []*etcdserverpb.RequestOp{{
							Request: &etcdserverpb.RequestOp_RequestPut{RequestPut: put},
						}},
					}},
				}}}
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			server, closeFn := newTestRPCServer(t)
			defer closeFn()
			ctx := context.Background()
			require.NoError(t, server.auth.userAdd(ctx, &etcdserverpb.AuthUserAddRequest{Name: "root", Password: "root-secret"}))
			require.NoError(t, server.auth.roleAdd(ctx, "root"))
			require.NoError(t, server.auth.userGrantRole(ctx, "root", "root"))
			require.NoError(t, server.auth.userAdd(ctx, &etcdserverpb.AuthUserAddRequest{Name: "writer", Password: "secret"}))
			require.NoError(t, server.auth.roleAdd(ctx, "writer"))
			require.NoError(t, server.auth.userGrantRole(ctx, "writer", "writer"))
			require.NoError(t, server.auth.roleGrantPermission(ctx, "writer", &authpb.Permission{
				PermType: authpb.WRITE, Key: []byte("key"),
			}))
			_, err := server.Put(ctx, &etcdserverpb.PutRequest{Key: []byte("key"), Value: []byte("before")})
			require.NoError(t, err)
			require.NoError(t, server.auth.enable(ctx))
			token, err := server.tokens.authenticate(ctx, "writer", "secret")
			require.NoError(t, err)
			writerCtx := metadata.NewIncomingContext(ctx, metadata.Pairs(rpctypes.TokenFieldNameGRPC, token))

			_, err = server.Txn(writerCtx, tc.txn(&etcdserverpb.PutRequest{
				Key: []byte("key"), Value: []byte("after"), PrevKv: true,
			}))
			require.ErrorIs(t, err, rpctypes.ErrPermissionDenied)
			stored, err := server.backend.Get(context.Background(), &etcdserverpb.RangeRequest{Key: []byte("key")})
			require.NoError(t, err)
			require.Len(t, stored.Kvs, 1)
			require.Equal(t, []byte("before"), stored.Kvs[0].Value, "read denial for PrevKV must happen before txn mutation")
		})
	}
}
