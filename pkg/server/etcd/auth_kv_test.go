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
	requireAuthKVError(t, err, rpctypes.ErrPermissionDenied, codes.Unknown, "etcdserver: permission denied")
	resp, err := server.Range(ctx, &etcdserverpb.RangeRequest{Key: []byte("/allowed/a")})
	require.NoError(t, err)
	require.Len(t, resp.Kvs, 1)
	_, err = server.Range(ctx, &etcdserverpb.RangeRequest{Key: []byte("/denied/a")})
	requireAuthKVError(t, err, rpctypes.ErrPermissionDenied, codes.Unknown, "etcdserver: permission denied")

	denied, err := server.backend.Get(context.Background(), &etcdserverpb.RangeRequest{Key: []byte("/denied/a")})
	require.NoError(t, err)
	require.Empty(t, denied.Kvs, "denied put must not reach storage")
}

func TestAuthCompactAdminCheckPrecedesFollowerRoutingLikeEtcd(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()
	aliceCtx := setupAuthKVUser(t, server)
	rootToken, err := server.tokens.authenticate(context.Background(), "root", "root-secret")
	require.NoError(t, err)
	rootCtx := metadata.NewIncomingContext(context.Background(), metadata.Pairs(
		rpctypes.TokenFieldNameGRPC, rootToken,
	))
	invalidCtx := metadata.NewIncomingContext(context.Background(), metadata.Pairs(
		rpctypes.TokenFieldNameGRPC, "invalid-token",
	))

	request := &etcdserverpb.CompactionRequest{Revision: 1}
	for _, proxyEnabled := range []bool{false, true} {
		t.Run(map[bool]string{false: "rejecting_follower", true: "proxying_follower"}[proxyEnabled], func(t *testing.T) {
			var forwarded int
			var forwardedTokens [][]string
			server.peers = testPeerService{
				isLeader: false, proxyEnabled: proxyEnabled,
				compactFn: func(ctx context.Context, req *etcdserverpb.CompactionRequest) (*etcdserverpb.CompactionResponse, error) {
					forwarded++
					require.Equal(t, request, req)
					md, _ := metadata.FromOutgoingContext(ctx)
					forwardedTokens = append(forwardedTokens, md.Get(rpctypes.TokenFieldNameGRPC))
					return &etcdserverpb.CompactionResponse{Header: txnHeader(1)}, nil
				},
			}

			_, err := server.Compact(context.Background(), request)
			requireAuthKVError(t, err, rpctypes.ErrUserEmpty, codes.Unknown, "etcdserver: user name is empty")
			_, err = server.Compact(invalidCtx, request)
			requireAuthKVError(t, err, rpctypes.ErrInvalidAuthToken, codes.Unknown, "etcdserver: invalid auth token")
			_, err = server.Compact(aliceCtx, request)
			requireAuthKVError(t, err, rpctypes.ErrPermissionDenied, codes.Unknown, "etcdserver: permission denied")
			require.Zero(t, forwarded, "unauthorized Compact must not reach follower routing")

			_, err = server.Compact(rootCtx, request)
			if proxyEnabled {
				require.NoError(t, err)
				require.Equal(t, 1, forwarded)
				require.Equal(t, []string{rootToken}, forwardedTokens[0])
			} else {
				require.Error(t, err)
				require.Equal(t, codes.Unavailable, status.Code(err))
				require.Zero(t, forwarded)
			}
		})
	}
}

func TestAuthRangeReadBarrierPrecedesAuthLikeEtcd(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()
	setupAuthKVUser(t, server)
	plain := context.Background()
	barrierErr := errors.New("leader read barrier failed")
	var barrierCalls int
	server.peers = testPeerService{
		proxyEnabled: true,
		syncReadFn: func(context.Context) error {
			barrierCalls++
			return barrierErr
		},
		rangeFn: func(context.Context, *etcdserverpb.RangeRequest) (*etcdserverpb.RangeResponse, error) {
			t.Fatal("linearizable Range must establish its barrier before any historical proxy")
			return nil, nil
		},
	}

	_, err := server.Range(plain, &etcdserverpb.RangeRequest{Key: []byte("/allowed/a")})
	requireReadBarrierUnavailable(t, err, barrierErr.Error())
	require.Equal(t, 1, barrierCalls)

	_, err = server.Range(plain, &etcdserverpb.RangeRequest{
		Key: []byte("/allowed/a"), Revision: 1,
	})
	requireReadBarrierUnavailable(t, err, barrierErr.Error())
	require.Equal(t, 2, barrierCalls, "historical linearizable Range must also fence before auth")

	_, err = server.Range(plain, &etcdserverpb.RangeRequest{Key: []byte("/allowed/a"), Serializable: true})
	requireAuthKVError(t, err, rpctypes.ErrUserEmpty, codes.Unknown, "etcdserver: user name is empty")
	require.Equal(t, 2, barrierCalls)

	stream := &fakeRangeStreamServer{ctx: plain}
	err = server.RangeStream(&etcdserverpb.RangeRequest{Key: []byte("/allowed/"), RangeEnd: []byte("/allowed0")}, stream)
	requireReadBarrierUnavailable(t, err, barrierErr.Error())
	require.Equal(t, 3, barrierCalls)

	stream = &fakeRangeStreamServer{ctx: plain}
	err = server.RangeStream(&etcdserverpb.RangeRequest{Key: []byte("/allowed/"), RangeEnd: []byte("/allowed0"), Serializable: true}, stream)
	requireAuthKVError(t, err, rpctypes.ErrUserEmpty, codes.Unknown, "etcdserver: user name is empty")
	require.Equal(t, 3, barrierCalls)
}

func TestAuthRangeValidationPrecedesReadBarrierAndAuthLikeEtcd(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()
	setupAuthKVUser(t, server)
	var barrierCalls int
	server.peers = testPeerService{
		syncReadFn: func(context.Context) error {
			barrierCalls++
			return errors.New("validation must not reach read barrier")
		},
	}
	plain := context.Background()

	tests := []struct {
		name    string
		request *etcdserverpb.RangeRequest
		want    error
		message string
	}{
		{
			name: "empty key before invalid sort",
			request: &etcdserverpb.RangeRequest{
				SortOrder: etcdserverpb.RangeRequest_SortOrder(99),
			},
			want: rpctypes.ErrGRPCEmptyKey, message: "etcdserver: key is not provided",
		},
		{
			name: "invalid sort before barrier and auth",
			request: &etcdserverpb.RangeRequest{
				Key: []byte("/allowed/a"), SortTarget: etcdserverpb.RangeRequest_SortTarget(99),
			},
			want: rpctypes.ErrGRPCInvalidSortOption, message: "etcdserver: invalid sort option",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			response, err := server.Range(plain, tt.request)
			require.Nil(t, response)
			requireDirectKVError(t, err, tt.want, codes.InvalidArgument, tt.message)

			stream := &fakeRangeStreamServer{ctx: plain}
			err = server.RangeStream(tt.request, stream)
			requireDirectKVError(t, err, tt.want, codes.InvalidArgument, tt.message)
			require.Empty(t, stream.sent)
		})
	}
	require.Zero(t, barrierCalls)

	stream := &fakeRangeStreamServer{ctx: plain}
	err := server.RangeStream(&etcdserverpb.RangeRequest{
		Key: []byte("/allowed/"), RangeEnd: []byte("/allowed0"),
		SortOrder:  etcdserverpb.RangeRequest_DESCEND,
		SortTarget: etcdserverpb.RangeRequest_KEY,
	}, stream)
	require.Equal(t, codes.Unimplemented, status.Code(err))
	require.Equal(t, "RangeStream does not support custom sort orders", status.Convert(err).Message())
	require.Empty(t, stream.sent)
	require.Zero(t, barrierCalls)
}

func TestAuthPutValidationPrecedesLeadershipWhileValidWriteRoutesBeforeAuthLikeEtcd(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()
	setupAuthKVUser(t, server)
	server.peers = testPeerService{isLeader: false, proxyEnabled: false}
	plain := context.Background()

	tests := []struct {
		name    string
		request *etcdserverpb.PutRequest
		want    error
		message string
	}{
		{
			name: "empty key before both ignore conflicts",
			request: &etcdserverpb.PutRequest{
				Value: []byte("value"), Lease: 123, IgnoreValue: true, IgnoreLease: true,
			},
			want: rpctypes.ErrGRPCEmptyKey, message: "etcdserver: key is not provided",
		},
		{
			name: "ignore value before ignore lease",
			request: &etcdserverpb.PutRequest{
				Key: []byte("/allowed/a"), Value: []byte("value"), Lease: 123,
				IgnoreValue: true, IgnoreLease: true,
			},
			want: rpctypes.ErrGRPCValueProvided, message: "etcdserver: value is provided",
		},
		{
			name: "ignore lease conflict",
			request: &etcdserverpb.PutRequest{
				Key: []byte("/allowed/a"), Lease: 123, IgnoreLease: true,
			},
			want: rpctypes.ErrGRPCLeaseProvided, message: "etcdserver: lease is provided",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			response, err := server.Put(plain, tt.request)
			require.Nil(t, response)
			requireDirectKVError(t, err, tt.want, codes.InvalidArgument, tt.message)
		})
	}

	response, err := server.Put(plain, &etcdserverpb.PutRequest{
		Key: []byte("/allowed/a"), Value: []byte("value"),
	})
	require.Nil(t, response)
	require.Equal(t, codes.Unavailable, status.Code(err))
	require.NotErrorIs(t, err, rpctypes.ErrUserEmpty)
}

func TestAuthDeleteRangeAdmissionAndReversedRangePriorityMatchEtcd(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()
	plain := context.Background()
	_, err := server.Put(plain, &etcdserverpb.PutRequest{Key: []byte("/allowed/x"), Value: []byte("allowed")})
	require.NoError(t, err)
	_, err = server.Put(plain, &etcdserverpb.PutRequest{Key: []byte("/denied/x"), Value: []byte("denied")})
	require.NoError(t, err)
	aliceCtx := setupAuthKVUser(t, server)
	beforeRevision := server.backend.GetCurrentRevision()

	allowed, err := server.DeleteRange(aliceCtx, &etcdserverpb.DeleteRangeRequest{
		Key: []byte("/allowed/y"), RangeEnd: []byte("/allowed/x"), PrevKv: true,
	})
	require.NoError(t, err)
	require.Zero(t, allowed.Deleted)
	require.Empty(t, allowed.PrevKvs)
	require.Equal(t, int64(beforeRevision), allowed.Header.Revision)
	require.Equal(t, beforeRevision, server.backend.GetCurrentRevision())

	denied, err := server.DeleteRange(aliceCtx, &etcdserverpb.DeleteRangeRequest{
		Key: []byte("/denied/y"), RangeEnd: []byte("/denied/x"), PrevKv: true,
	})
	require.Nil(t, denied)
	requireAuthKVError(t, err, rpctypes.ErrPermissionDenied, codes.Unknown, "etcdserver: permission denied")
	require.Equal(t, beforeRevision, server.backend.GetCurrentRevision())

	server.peers = testPeerService{isLeader: false, proxyEnabled: false}
	invalid, err := server.DeleteRange(plain, &etcdserverpb.DeleteRangeRequest{PrevKv: true})
	require.Nil(t, invalid)
	requireDirectKVError(t, err, rpctypes.ErrGRPCEmptyKey, codes.InvalidArgument, "etcdserver: key is not provided")

	valid, err := server.DeleteRange(plain, &etcdserverpb.DeleteRangeRequest{Key: []byte("/allowed/x")})
	require.Nil(t, valid)
	require.Equal(t, codes.Unavailable, status.Code(err))
	require.NotErrorIs(t, err, rpctypes.ErrUserEmpty)
}

func TestAuthReadonlyTxnBarrierPrecedesAuthLikeEtcd(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()
	authenticated := setupAuthKVUser(t, server)
	plain := context.Background()
	barrierErr := errors.New("leader txn read barrier failed")
	var barrierCalls int
	server.peers = testPeerService{syncReadFn: func(context.Context) error {
		barrierCalls++
		return barrierErr
	}}

	readTxn := func(serializable bool) *etcdserverpb.TxnRequest {
		return &etcdserverpb.TxnRequest{Success: []*etcdserverpb.RequestOp{{
			Request: &etcdserverpb.RequestOp_RequestRange{RequestRange: &etcdserverpb.RangeRequest{
				Key: []byte("/allowed/a"), Serializable: serializable,
			}},
		}}}
	}
	invalid := readTxn(false)
	invalid.Success[0].GetRequestRange().Key = nil
	_, err := server.Txn(plain, invalid)
	require.Equal(t, codes.InvalidArgument, status.Code(err))
	require.Equal(t, "etcdserver: key is not provided", status.Convert(err).Message())
	require.Zero(t, barrierCalls, "static request validation must precede the read barrier")

	_, err = server.Txn(plain, readTxn(false))
	requireReadBarrierUnavailable(t, err, barrierErr.Error())
	require.Equal(t, 1, barrierCalls)

	_, err = server.Txn(plain, readTxn(true))
	requireAuthKVError(t, err, rpctypes.ErrUserEmpty, codes.Unknown, "etcdserver: user name is empty")
	require.Equal(t, 1, barrierCalls)

	server.peers = testPeerService{isLeader: true, syncReadFn: func(context.Context) error {
		barrierCalls++
		return nil
	}}
	resp, err := server.Txn(authenticated, readTxn(false))
	require.NoError(t, err)
	require.NotNil(t, resp.Header)
	require.Len(t, resp.Responses, 1)
	require.Equal(t, 2, barrierCalls, "a linearizable read-only txn must establish exactly one barrier")
}

func TestAuthNonAdminWriteLeadershipFailurePrecedesAuthLikeEtcd(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()
	setupAuthKVUser(t, server)
	server.peers = testPeerService{isLeader: false, noLeader: true}

	for _, tc := range []struct {
		name string
		call func() error
	}{
		{
			name: "put",
			call: func() error {
				_, err := server.Put(context.Background(), &etcdserverpb.PutRequest{
					Key: []byte("/allowed/write-order"), Value: []byte("value"),
				})
				return err
			},
		},
		{
			name: "delete range",
			call: func() error {
				_, err := server.DeleteRange(context.Background(), &etcdserverpb.DeleteRangeRequest{
					Key: []byte("/allowed/write-order"),
				})
				return err
			},
		},
		{
			name: "write txn",
			call: func() error {
				_, err := server.Txn(context.Background(), &etcdserverpb.TxnRequest{
					Success: []*etcdserverpb.RequestOp{{Request: &etcdserverpb.RequestOp_RequestPut{
						RequestPut: &etcdserverpb.PutRequest{
							Key: []byte("/allowed/write-order"), Value: []byte("value"),
						},
					}}},
				})
				return err
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.call()
			require.Equal(t, codes.Unavailable, status.Code(err))
			require.NotContains(t, status.Convert(err).Message(), "user name is empty")
		})
	}
}

func TestAuthKVFutureJWTRevisionAllowsWritesButNotSerializedReads(t *testing.T) {
	now := time.Unix(2_000_000_000, 0)
	for _, tc := range []struct {
		name string
		call func(context.Context, *RPCServer) error
		key  []byte
		want []byte
	}{
		{
			name: "put",
			key:  []byte("/allowed/future-put"),
			want: []byte("value"),
			call: func(ctx context.Context, server *RPCServer) error {
				_, err := server.Put(ctx, &etcdserverpb.PutRequest{
					Key: []byte("/allowed/future-put"), Value: []byte("value"),
				})
				return err
			},
		},
		{
			name: "delete range",
			key:  []byte("/allowed/future-delete"),
			call: func(ctx context.Context, server *RPCServer) error {
				_, err := server.Put(ctx, &etcdserverpb.PutRequest{
					Key: []byte("/allowed/future-delete"), Value: []byte("before"),
				})
				require.NoError(t, err)
				_, err = server.DeleteRange(ctx, &etcdserverpb.DeleteRangeRequest{
					Key: []byte("/allowed/future-delete"),
				})
				return err
			},
		},
		{
			name: "txn write",
			key:  []byte("/allowed/future-txn"),
			want: []byte("value"),
			call: func(ctx context.Context, server *RPCServer) error {
				_, err := server.Txn(ctx, &etcdserverpb.TxnRequest{
					Success: []*etcdserverpb.RequestOp{{Request: &etcdserverpb.RequestOp_RequestPut{
						RequestPut: &etcdserverpb.PutRequest{
							Key: []byte("/allowed/future-txn"), Value: []byte("value"),
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
			server.tokens.now = func() time.Time { return now }
			secret := writeJWTKey(t, "secret", []byte("shared-secret"))
			require.NoError(t, server.tokens.configureProvider(
				"jwt,sign-method=HS256,priv-key="+secret,
			))
			setupAuthKVUser(t, server)
			snapshot, err := server.tokens.snapshots.current(context.Background())
			require.NoError(t, err)
			token, err := server.tokens.jwt.issue("alice", snapshot.Config.Revision+100, now)
			require.NoError(t, err)
			ctx := metadata.NewIncomingContext(
				context.Background(), metadata.Pairs(rpctypes.TokenFieldNameGRPC, token),
			)

			require.NoError(t, tc.call(ctx, server))
			stored, err := server.backend.Get(context.Background(), &etcdserverpb.RangeRequest{Key: tc.key})
			require.NoError(t, err)
			if tc.want == nil {
				require.Empty(t, stored.Kvs)
			} else {
				require.Len(t, stored.Kvs, 1)
				require.Equal(t, tc.want, stored.Kvs[0].Value)
			}
			_, err = server.Range(ctx, &etcdserverpb.RangeRequest{Key: []byte("/allowed/read-back"), Serializable: true})
			requireAuthKVError(t, err, rpctypes.ErrAuthOldRevision, codes.Unknown, "etcdserver: revision of auth store is old")
		})
	}
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
	requireAuthKVError(t, err, rpctypes.ErrPermissionDenied, codes.Unknown, "etcdserver: permission denied")
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
	requireAuthKVError(t, err, rpctypes.ErrPermissionDenied, codes.Unknown, "etcdserver: permission denied")
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
			requireAuthKVError(t, <-done, rpctypes.ErrPermissionDenied, codes.Unknown, "etcdserver: permission denied")

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
	requireAuthKVError(t, err, rpctypes.ErrPermissionDenied, codes.Unknown, "etcdserver: permission denied")
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
			requireAuthKVError(t, err, rpctypes.ErrPermissionDenied, codes.Unknown, "etcdserver: permission denied")
			stored, err := server.backend.Get(context.Background(), &etcdserverpb.RangeRequest{Key: []byte("key")})
			require.NoError(t, err)
			require.Len(t, stored.Kvs, 1)
			require.Equal(t, []byte("before"), stored.Kvs[0].Value, "read denial for PrevKV must happen before txn mutation")
		})
	}
}

func requireAuthKVError(t *testing.T, err error, want error, code codes.Code, message string) {
	t.Helper()
	require.ErrorIs(t, err, want)
	require.Equal(t, code, status.Code(err))
	require.Equal(t, message, status.Convert(err).Message())
}
