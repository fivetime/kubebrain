// Copyright 2026 ByteDance and/or its affiliates
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package etcd

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"math"
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/mvccpb"
	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
)

func TestRawGRPCTxnCompareMatrix(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()

	grpcServer := grpc.NewServer(server.ClientServerOptions()...)
	etcdserverpb.RegisterKVServer(grpcServer, server)
	etcdserverpb.RegisterLeaseServer(grpcServer, server)
	listener := bufconn.Listen(1 << 20)
	go func() { _ = grpcServer.Serve(listener) }()
	t.Cleanup(grpcServer.Stop)

	conn, err := grpc.NewClient("passthrough:///txn-compare-matrix-client",
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
			return listener.Dial()
		}))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, conn.Close()) })
	kv := etcdserverpb.NewKVClient(conn)
	lease := etcdserverpb.NewLeaseClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	prefix := fmt.Sprintf("/a1004/txn-compare-matrix-client/%d/", time.Now().UnixNano())
	end := []byte(clientv3.GetPrefixRangeEnd(prefix))
	grant, err := lease.LeaseGrant(ctx, &etcdserverpb.LeaseGrantRequest{TTL: 300})
	require.NoError(t, err)
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cleanupCancel()
		_, _ = lease.LeaseRevoke(cleanupCtx, &etcdserverpb.LeaseRevokeRequest{ID: grant.ID})
	})

	keyA := []byte(prefix + "a")
	keyB := []byte(prefix + "b")
	keyLease := []byte(prefix + "leased")
	putA, err := kv.Put(ctx, &etcdserverpb.PutRequest{Key: keyA, Value: []byte("same")})
	require.NoError(t, err)
	_, err = kv.Put(ctx, &etcdserverpb.PutRequest{Key: keyB, Value: []byte("different")})
	require.NoError(t, err)
	_, err = kv.Put(ctx, &etcdserverpb.PutRequest{Key: keyLease, Value: []byte("leased"), Lease: grant.ID})
	require.NoError(t, err)
	updateA, err := kv.Put(ctx, &etcdserverpb.PutRequest{Key: keyA, Value: []byte("same")})
	require.NoError(t, err)

	absent := []byte(prefix + "absent")
	emptyPrefix := prefix + "empty/"
	emptyKey := []byte(emptyPrefix)
	emptyEnd := []byte(clientv3.GetPrefixRangeEnd(emptyPrefix))
	emptyFromKey := []byte(prefix + "z")
	tests := []struct {
		name      string
		compare   *etcdserverpb.Compare
		succeeded bool
	}{
		{name: "point-value-equal", compare: txnClientValueCompare(keyA, nil, etcdserverpb.Compare_EQUAL, "same"), succeeded: true},
		{name: "point-version-equal", compare: txnClientIntCompare(keyA, nil, etcdserverpb.Compare_VERSION, etcdserverpb.Compare_EQUAL, 2), succeeded: true},
		{name: "point-create-equal", compare: txnClientIntCompare(keyA, nil, etcdserverpb.Compare_CREATE, etcdserverpb.Compare_EQUAL, putA.Header.Revision), succeeded: true},
		{name: "point-mod-equal", compare: txnClientIntCompare(keyA, nil, etcdserverpb.Compare_MOD, etcdserverpb.Compare_EQUAL, updateA.Header.Revision), succeeded: true},
		{name: "point-lease-equal", compare: txnClientIntCompare(keyLease, nil, etcdserverpb.Compare_LEASE, etcdserverpb.Compare_EQUAL, grant.ID), succeeded: true},
		{name: "absent-version-equal-zero", compare: txnClientIntCompare(absent, nil, etcdserverpb.Compare_VERSION, etcdserverpb.Compare_EQUAL, 0), succeeded: true},
		{name: "absent-version-not-equal-zero", compare: txnClientIntCompare(absent, nil, etcdserverpb.Compare_VERSION, etcdserverpb.Compare_NOT_EQUAL, 0), succeeded: false},
		{name: "absent-value-equal-empty", compare: txnClientValueCompare(absent, nil, etcdserverpb.Compare_EQUAL, ""), succeeded: false},
		{name: "absent-value-not-equal", compare: txnClientValueCompare(absent, nil, etcdserverpb.Compare_NOT_EQUAL, "value"), succeeded: false},
		{name: "empty-range-version-equal-zero", compare: txnClientIntCompare(emptyKey, emptyEnd, etcdserverpb.Compare_VERSION, etcdserverpb.Compare_EQUAL, 0), succeeded: true},
		{name: "empty-range-value-not-equal", compare: txnClientValueCompare(emptyKey, emptyEnd, etcdserverpb.Compare_NOT_EQUAL, "value"), succeeded: false},
		{name: "multi-version-greater-zero", compare: txnClientIntCompare([]byte(prefix), end, etcdserverpb.Compare_VERSION, etcdserverpb.Compare_GREATER, 0), succeeded: true},
		{name: "multi-version-equal-one", compare: txnClientIntCompare([]byte(prefix), end, etcdserverpb.Compare_VERSION, etcdserverpb.Compare_EQUAL, 1), succeeded: false},
		{name: "multi-create-less-after-update", compare: txnClientIntCompare([]byte(prefix), end, etcdserverpb.Compare_CREATE, etcdserverpb.Compare_LESS, updateA.Header.Revision+1), succeeded: true},
		{name: "multi-value-not-equal-missing", compare: txnClientValueCompare([]byte(prefix), end, etcdserverpb.Compare_NOT_EQUAL, "missing"), succeeded: true},
		{name: "multi-value-equal-same", compare: txnClientValueCompare([]byte(prefix), end, etcdserverpb.Compare_EQUAL, "same"), succeeded: false},
		{name: "multi-lease-equal-grant", compare: txnClientIntCompare([]byte(prefix), end, etcdserverpb.Compare_LEASE, etcdserverpb.Compare_EQUAL, grant.ID), succeeded: false},
		{name: "from-key-version-greater-zero", compare: txnClientIntCompare(keyB, []byte{0}, etcdserverpb.Compare_VERSION, etcdserverpb.Compare_GREATER, 0), succeeded: true},
		{name: "from-key-value-equal-different", compare: txnClientValueCompare(keyB, []byte{0}, etcdserverpb.Compare_EQUAL, "different"), succeeded: false},
		{name: "from-key-empty-version-equal-zero", compare: txnClientIntCompare(emptyFromKey, []byte{0}, etcdserverpb.Compare_VERSION, etcdserverpb.Compare_EQUAL, 0), succeeded: true},
		{name: "from-key-empty-value-not-equal", compare: txnClientValueCompare(emptyFromKey, []byte{0}, etcdserverpb.Compare_NOT_EQUAL, "anything"), succeeded: false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			resp, txnErr := kv.Txn(ctx, &etcdserverpb.TxnRequest{Compare: []*etcdserverpb.Compare{tt.compare}})
			require.NoError(t, txnErr)
			require.Equal(t, tt.succeeded, resp.Succeeded)
		})
	}
}

func TestRawGRPCTxnOperationValidationMessages(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()

	grpcServer := grpc.NewServer(server.ClientServerOptions()...)
	etcdserverpb.RegisterKVServer(grpcServer, server)
	listener := bufconn.Listen(1 << 20)
	go func() { _ = grpcServer.Serve(listener) }()
	t.Cleanup(grpcServer.Stop)

	conn, err := grpc.NewClient("passthrough:///txn-operation-validation-client",
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
			return listener.Dial()
		}))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, conn.Close()) })
	kv := etcdserverpb.NewKVClient(conn)

	key := []byte("/a1005/txn-operation-validation")
	tests := []struct {
		name        string
		op          *etcdserverpb.RequestOp
		wantCode    codes.Code
		wantMessage string
	}{
		{
			name: "put-empty-key-precedes-ignore-value",
			op: &etcdserverpb.RequestOp{Request: &etcdserverpb.RequestOp_RequestPut{
				RequestPut: &etcdserverpb.PutRequest{Value: []byte("value"), IgnoreValue: true},
			}},
			wantCode: codes.InvalidArgument, wantMessage: "etcdserver: key is not provided",
		},
		{
			name: "put-ignore-value-precedes-ignore-lease",
			op: &etcdserverpb.RequestOp{Request: &etcdserverpb.RequestOp_RequestPut{
				RequestPut: &etcdserverpb.PutRequest{
					Key: key, Value: []byte("value"), Lease: 1, IgnoreValue: true, IgnoreLease: true,
				},
			}},
			wantCode: codes.InvalidArgument, wantMessage: "etcdserver: value is provided",
		},
		{
			name: "put-ignore-lease-with-lease",
			op: &etcdserverpb.RequestOp{Request: &etcdserverpb.RequestOp_RequestPut{
				RequestPut: &etcdserverpb.PutRequest{Key: key, Lease: 1, IgnoreLease: true},
			}},
			wantCode: codes.InvalidArgument, wantMessage: "etcdserver: lease is provided",
		},
		{
			name: "put-missing-lease-precedes-missing-ignore-value-key",
			op: &etcdserverpb.RequestOp{Request: &etcdserverpb.RequestOp_RequestPut{
				RequestPut: &etcdserverpb.PutRequest{Key: key, Lease: 987654321, IgnoreValue: true},
			}},
			wantCode: codes.NotFound, wantMessage: "etcdserver: requested lease not found",
		},
		{
			name: "range-empty-key-precedes-invalid-sort",
			op: &etcdserverpb.RequestOp{Request: &etcdserverpb.RequestOp_RequestRange{
				RequestRange: &etcdserverpb.RangeRequest{SortOrder: 99, SortTarget: 99},
			}},
			wantCode: codes.InvalidArgument, wantMessage: "etcdserver: key is not provided",
		},
		{
			name: "range-invalid-order-precedes-target",
			op: &etcdserverpb.RequestOp{Request: &etcdserverpb.RequestOp_RequestRange{
				RequestRange: &etcdserverpb.RangeRequest{Key: key, SortOrder: 99, SortTarget: 99},
			}},
			wantCode: codes.InvalidArgument, wantMessage: "etcdserver: invalid sort option",
		},
		{
			name: "range-invalid-target",
			op: &etcdserverpb.RequestOp{Request: &etcdserverpb.RequestOp_RequestRange{
				RequestRange: &etcdserverpb.RangeRequest{Key: key, SortTarget: 99},
			}},
			wantCode: codes.InvalidArgument, wantMessage: "etcdserver: invalid sort option",
		},
		{
			name: "delete-empty-key",
			op: &etcdserverpb.RequestOp{Request: &etcdserverpb.RequestOp_RequestDeleteRange{
				RequestDeleteRange: &etcdserverpb.DeleteRangeRequest{RangeEnd: []byte{0}},
			}},
			wantCode: codes.InvalidArgument, wantMessage: "etcdserver: key is not provided",
		},
		{
			name:        "empty-operation",
			op:          &etcdserverpb.RequestOp{},
			wantCode:    codes.InvalidArgument,
			wantMessage: "etcdserver: key not found",
		},
		{
			name:        "nil-operation",
			op:          nil,
			wantCode:    codes.InvalidArgument,
			wantMessage: "etcdserver: key not found",
		},
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			resp, callErr := kv.Txn(ctx, &etcdserverpb.TxnRequest{Success: []*etcdserverpb.RequestOp{tt.op}})
			require.Nil(t, resp)
			require.EqualError(t, callErr, status.Error(tt.wantCode, tt.wantMessage).Error())
			require.Equal(t, tt.wantCode, status.Code(callErr))
			require.Equal(t, tt.wantMessage, status.Convert(callErr).Message())
		})
	}
}

func TestRawGRPCTxnNestedOperationValidationMessagesMatchEtcd(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()

	grpcServer := grpc.NewServer(server.ClientServerOptions()...)
	etcdserverpb.RegisterKVServer(grpcServer, server)
	listener := bufconn.Listen(1 << 20)
	go func() { _ = grpcServer.Serve(listener) }()
	t.Cleanup(grpcServer.Stop)

	conn, err := grpc.NewClient("passthrough:///txn-nested-operation-validation-client",
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
			return listener.Dial()
		}))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, conn.Close()) })
	kv := etcdserverpb.NewKVClient(conn)

	key := []byte("/a3012/txn-nested-operation-validation")
	tests := []struct {
		name        string
		nested      *etcdserverpb.TxnRequest
		wantCode    codes.Code
		wantMessage string
	}{
		{
			name: "nested-success-empty-operation",
			nested: &etcdserverpb.TxnRequest{
				Success: []*etcdserverpb.RequestOp{{}},
			},
			wantCode: codes.InvalidArgument, wantMessage: "etcdserver: key not found",
		},
		{
			name: "nested-success-nil-operation",
			nested: &etcdserverpb.TxnRequest{
				Success: []*etcdserverpb.RequestOp{nil},
			},
			wantCode: codes.InvalidArgument, wantMessage: "etcdserver: key not found",
		},
		{
			name: "nested-compare-empty-key",
			nested: &etcdserverpb.TxnRequest{
				Compare: []*etcdserverpb.Compare{{
					Result:      etcdserverpb.Compare_EQUAL,
					Target:      etcdserverpb.Compare_VERSION,
					TargetUnion: &etcdserverpb.Compare_Version{Version: 0},
				}},
			},
			wantCode: codes.InvalidArgument, wantMessage: "etcdserver: key is not provided",
		},
		{
			name: "nested-put-empty-key",
			nested: &etcdserverpb.TxnRequest{
				Success: []*etcdserverpb.RequestOp{{
					Request: &etcdserverpb.RequestOp_RequestPut{
						RequestPut: &etcdserverpb.PutRequest{Value: []byte("value")},
					},
				}},
			},
			wantCode: codes.InvalidArgument, wantMessage: "etcdserver: key is not provided",
		},
		{
			name: "nested-failure-invalid-range-sort",
			nested: &etcdserverpb.TxnRequest{
				Failure: []*etcdserverpb.RequestOp{{
					Request: &etcdserverpb.RequestOp_RequestRange{
						RequestRange: &etcdserverpb.RangeRequest{Key: key, SortOrder: 99, SortTarget: 99},
					},
				}},
			},
			wantCode: codes.InvalidArgument, wantMessage: "etcdserver: invalid sort option",
		},
		{
			name: "nested-delete-empty-key",
			nested: &etcdserverpb.TxnRequest{
				Success: []*etcdserverpb.RequestOp{{
					Request: &etcdserverpb.RequestOp_RequestDeleteRange{
						RequestDeleteRange: &etcdserverpb.DeleteRangeRequest{RangeEnd: []byte{0}},
					},
				}},
			},
			wantCode: codes.InvalidArgument, wantMessage: "etcdserver: key is not provided",
		},
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			resp, callErr := kv.Txn(ctx, &etcdserverpb.TxnRequest{
				Success: []*etcdserverpb.RequestOp{{
					Request: &etcdserverpb.RequestOp_RequestTxn{RequestTxn: tt.nested},
				}},
			})
			require.Nil(t, resp)
			require.EqualError(t, callErr, status.Error(tt.wantCode, tt.wantMessage).Error())
			require.Equal(t, tt.wantCode, status.Code(callErr))
			require.Equal(t, tt.wantMessage, status.Convert(callErr).Message())
		})
	}
}

func TestRawGRPCTxnExecutionValidationOrderAndBudget(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()

	grpcServer := grpc.NewServer(server.ClientServerOptions()...)
	etcdserverpb.RegisterKVServer(grpcServer, server)
	listener := bufconn.Listen(1 << 20)
	go func() { _ = grpcServer.Serve(listener) }()
	t.Cleanup(grpcServer.Stop)

	conn, err := grpc.NewClient("passthrough:///txn-operation-budget-client",
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
			return listener.Dial()
		}))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, conn.Close()) })
	kv := etcdserverpb.NewKVClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	missingLeasePut := txnClientPutOp(&etcdserverpb.PutRequest{
		Key: []byte("/a1005/txn-validation-order/missing-lease"), Value: []byte("value"), Lease: 987654321,
	})
	futureRange := txnClientRangeOp(&etcdserverpb.RangeRequest{
		Key: []byte("/a1005/txn-validation-order/future"), Revision: math.MaxInt64,
	})
	_, err = kv.Txn(ctx, &etcdserverpb.TxnRequest{Success: []*etcdserverpb.RequestOp{missingLeasePut, futureRange}})
	requireRawGRPCTxnError(t, err, codes.NotFound, "etcdserver: requested lease not found", rpctypes.ErrGRPCLeaseNotFound)
	_, err = kv.Txn(ctx, &etcdserverpb.TxnRequest{Success: []*etcdserverpb.RequestOp{futureRange, missingLeasePut}})
	requireRawGRPCTxnError(t, err, codes.OutOfRange, "etcdserver: mvcc: required revision is a future revision", rpctypes.ErrGRPCFutureRev)

	rangeOp := func(suffix byte) *etcdserverpb.RequestOp {
		key := append([]byte("/a1005/txn-budget/"), suffix)
		return txnClientRangeOp(&etcdserverpb.RangeRequest{Key: key})
	}
	rangeOps := func(n int) []*etcdserverpb.RequestOp {
		ops := make([]*etcdserverpb.RequestOp, n)
		for i := range ops {
			ops[i] = rangeOp(byte(i))
		}
		return ops
	}
	nested := func(ops []*etcdserverpb.RequestOp) *etcdserverpb.RequestOp {
		return &etcdserverpb.RequestOp{Request: &etcdserverpb.RequestOp_RequestTxn{
			RequestTxn: &etcdserverpb.TxnRequest{Success: ops},
		}}
	}
	compares := make([]*etcdserverpb.Compare, defaultMaxTxnOps)
	for i := range compares {
		compares[i] = &etcdserverpb.Compare{
			Key:         append([]byte("/a1005/txn-budget-compare/"), byte(i)),
			Result:      etcdserverpb.Compare_EQUAL,
			Target:      etcdserverpb.Compare_VERSION,
			TargetUnion: &etcdserverpb.Compare_Version{Version: 0},
		}
	}
	emptyCompares := func(n int) []*etcdserverpb.Compare {
		compares := make([]*etcdserverpb.Compare, n)
		for i := range compares {
			compares[i] = &etcdserverpb.Compare{}
		}
		return compares
	}
	emptyOps := func(n int) []*etcdserverpb.RequestOp {
		ops := make([]*etcdserverpb.RequestOp, n)
		for i := range ops {
			ops[i] = &etcdserverpb.RequestOp{}
		}
		return ops
	}
	tests := []struct {
		name          string
		txn           *etcdserverpb.TxnRequest
		wantErr       bool
		wantSucceeded bool
	}{
		{name: "top-level-at-limit", txn: &etcdserverpb.TxnRequest{Success: rangeOps(defaultMaxTxnOps)}, wantSucceeded: true},
		{name: "top-level-over-limit", txn: &etcdserverpb.TxnRequest{Success: rangeOps(defaultMaxTxnOps + 1)}, wantErr: true},
		{name: "nested-exact-remaining-budget", txn: &etcdserverpb.TxnRequest{
			Success: append(rangeOps(defaultMaxTxnOps-2), nested(rangeOps(1))),
		}, wantSucceeded: true},
		{name: "nested-over-remaining-budget", txn: &etcdserverpb.TxnRequest{
			Success: append(rangeOps(defaultMaxTxnOps-1), nested(rangeOps(1))),
		}, wantErr: true},
		{name: "compare-max-does-not-charge-range-child", txn: &etcdserverpb.TxnRequest{
			Compare: compares, Success: []*etcdserverpb.RequestOp{rangeOp(0)},
		}, wantSucceeded: true},
		{name: "compare-over-limit-precedes-empty-key", txn: &etcdserverpb.TxnRequest{
			Compare: emptyCompares(defaultMaxTxnOps + 1),
		}, wantErr: true},
		{name: "success-over-limit-precedes-empty-op", txn: &etcdserverpb.TxnRequest{
			Success: emptyOps(defaultMaxTxnOps + 1),
		}, wantErr: true},
		{name: "unselected-failure-nested-over-budget", txn: &etcdserverpb.TxnRequest{
			Success: rangeOps(1), Failure: append(rangeOps(defaultMaxTxnOps-1), nested(rangeOps(1))),
		}, wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			resp, callErr := kv.Txn(ctx, tt.txn)
			if tt.wantErr {
				require.Nil(t, resp)
				requireRawGRPCTxnError(t, callErr, codes.InvalidArgument, "etcdserver: too many operations in txn request", rpctypes.ErrGRPCTooManyOps)
				return
			}
			require.NoError(t, callErr)
			require.NotNil(t, resp)
			require.Equal(t, tt.wantSucceeded, resp.Succeeded)
		})
	}
}

func TestClientTxnBasicErrorsMatchEtcd(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()

	grpcServer := grpc.NewServer(server.ClientServerOptions()...)
	etcdserverpb.RegisterKVServer(grpcServer, server)
	listener := bufconn.Listen(1 << 20)
	go func() { _ = grpcServer.Serve(listener) }()
	t.Cleanup(grpcServer.Stop)

	client, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{"bufnet"},
		DialTimeout: time.Second,
		DialOptions: []grpc.DialOption{
			grpc.WithTransportCredentials(insecure.NewCredentials()),
			grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
				return listener.Dial()
			}),
		},
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, client.Close()) })

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err = client.Txn(ctx).
		Then(clientv3.OpPut("/a1131/txn/basic-error/duplicate", "one"), clientv3.OpPut("/a1131/txn/basic-error/duplicate", "two")).
		Commit()
	requireClientTxnError(t, err, codes.Unknown, "etcdserver: duplicate key given in txn request", rpctypes.ErrDuplicateKey)

	_, err = client.Txn(ctx).Then(clientv3.OpGet("")).Commit()
	requireClientTxnError(t, err, codes.Unknown, "etcdserver: key is not provided", rpctypes.ErrEmptyKey)

	_, err = client.Txn(ctx).
		Then(clientv3.OpGet("/a1169/txn/basic-error/invalid-sort",
			clientv3.WithSort(clientv3.SortTarget(99), clientv3.SortOrder(99)))).
		Commit()
	requireClientTxnError(t, err, codes.Unknown, "etcdserver: invalid sort option", rpctypes.ErrInvalidSortOption)
	_, err = client.Txn(ctx).
		Then(clientv3.OpGet("",
			clientv3.WithSort(clientv3.SortTarget(99), clientv3.SortOrder(99)))).
		Commit()
	requireClientTxnError(t, err, codes.Unknown, "etcdserver: key is not provided", rpctypes.ErrEmptyKey)

	ops := make([]clientv3.Op, defaultMaxTxnOps+1)
	for i := range ops {
		ops[i] = clientv3.OpPut(fmt.Sprintf("/a1131/txn/basic-error/too-many/%d", i), "")
	}
	_, err = client.Txn(ctx).Then(ops...).Commit()
	requireClientTxnError(t, err, codes.Unknown, "etcdserver: too many operations in txn request", rpctypes.ErrTooManyOps)

	cmps := make([]clientv3.Cmp, defaultMaxTxnOps+1)
	for i := range cmps {
		cmps[i] = clientv3.Compare(clientv3.Version(""), "=", 0)
	}
	_, err = client.Txn(ctx).If(cmps...).Commit()
	requireClientTxnError(t, err, codes.Unknown, "etcdserver: too many operations in txn request", rpctypes.ErrTooManyOps)
}

func TestClientTxnSinglePutSuccessResponseMatchesEtcd(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()

	grpcServer := grpc.NewServer(server.ClientServerOptions()...)
	etcdserverpb.RegisterKVServer(grpcServer, server)
	listener := bufconn.Listen(1 << 20)
	go func() { _ = grpcServer.Serve(listener) }()
	t.Cleanup(grpcServer.Stop)

	client, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{"bufnet"},
		DialTimeout: time.Second,
		DialOptions: []grpc.DialOption{
			grpc.WithTransportCredentials(insecure.NewCredentials()),
			grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
				return listener.Dial()
			}),
		},
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, client.Close()) })

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	key := fmt.Sprintf("/a2127/txn-single-put/%d", time.Now().UnixNano())
	txn, err := client.Txn(ctx).Then(clientv3.OpPut(key, "bar")).Commit()
	require.NoError(t, err)
	require.True(t, txn.Succeeded)
	require.NotNil(t, txn.Header)
	require.Positive(t, txn.Header.Revision)
	require.Len(t, txn.Responses, 1)
	put := txn.Responses[0].GetResponsePut()
	require.NotNil(t, put)
	require.NotNil(t, put.Header)
	require.Equal(t, txn.Header.Revision, put.Header.Revision)

	got, err := client.Get(ctx, key)
	require.NoError(t, err)
	require.NotNil(t, got.Header)
	require.Equal(t, txn.Header.Revision, got.Header.Revision)
	require.Len(t, got.Kvs, 1)
	require.Equal(t, key, string(got.Kvs[0].Key))
	require.Equal(t, []byte("bar"), got.Kvs[0].Value)
	require.Equal(t, txn.Header.Revision, got.Kvs[0].CreateRevision)
	require.Equal(t, txn.Header.Revision, got.Kvs[0].ModRevision)
	require.Equal(t, int64(1), got.Kvs[0].Version)
	require.Zero(t, got.Kvs[0].Lease)
}

func TestClientTxnNoSpaceIsTyped(t *testing.T) {
	server := newQuotaRPCServer(t, 6)

	grpcServer := grpc.NewServer(server.ClientServerOptions()...)
	server.Register(grpcServer)
	listener := bufconn.Listen(1 << 20)
	go func() { _ = grpcServer.Serve(listener) }()
	t.Cleanup(grpcServer.Stop)

	client, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{"bufnet"},
		DialTimeout: time.Second,
		DialOptions: []grpc.DialOption{
			grpc.WithTransportCredentials(insecure.NewCredentials()),
			grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
				return listener.Dial()
			}),
		},
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, client.Close()) })

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err = client.Put(ctx, "key", "123")
	require.NoError(t, err)

	_, err = client.Txn(ctx).Then(clientv3.OpPut("x", "y")).Commit()
	requireClientTxnError(t, err, codes.Unknown, "etcdserver: mvcc: database space exceeded", rpctypes.ErrNoSpace)
}

func TestRawGRPCTxnCompareEnumFallthroughMatchesEtcd(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()

	grpcServer := grpc.NewServer(server.ClientServerOptions()...)
	etcdserverpb.RegisterKVServer(grpcServer, server)
	listener := bufconn.Listen(1 << 20)
	go func() { _ = grpcServer.Serve(listener) }()
	t.Cleanup(grpcServer.Stop)

	conn, err := grpc.NewClient("passthrough:///txn-compare-enum-client",
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
			return listener.Dial()
		}))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, conn.Close()) })
	kv := etcdserverpb.NewKVClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	tests := []struct {
		name         string
		target       etcdserverpb.Compare_CompareTarget
		result       etcdserverpb.Compare_CompareResult
		compareValue []byte
		succeeded    bool
		value        []byte
	}{
		{name: "unknown-result", target: etcdserverpb.Compare_MOD, result: 99, succeeded: true, value: []byte("success")},
		{name: "unknown-target-equal", target: 99, result: etcdserverpb.Compare_EQUAL, succeeded: true, value: []byte("success")},
		{name: "unknown-target-not-equal", target: 99, result: etcdserverpb.Compare_NOT_EQUAL, value: []byte("failure")},
		{name: "absent-value-equal-empty", target: etcdserverpb.Compare_VALUE, result: etcdserverpb.Compare_EQUAL, value: []byte("failure")},
		{name: "absent-value-not-equal", target: etcdserverpb.Compare_VALUE, result: etcdserverpb.Compare_NOT_EQUAL, compareValue: []byte("value"), value: []byte("failure")},
		{name: "absent-value-unknown-result", target: etcdserverpb.Compare_VALUE, result: 99, value: []byte("failure")},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			key := []byte(fmt.Sprintf("/a1039/txn-compare-enum/%s/%d", tt.name, time.Now().UnixNano()))
			compare := &etcdserverpb.Compare{
				Key:         key,
				Target:      tt.target,
				Result:      tt.result,
				TargetUnion: &etcdserverpb.Compare_ModRevision{ModRevision: 0},
			}
			if tt.target == etcdserverpb.Compare_VALUE {
				compare.TargetUnion = &etcdserverpb.Compare_Value{Value: tt.compareValue}
			}
			response, txnErr := kv.Txn(ctx, &etcdserverpb.TxnRequest{
				Compare: []*etcdserverpb.Compare{compare},
				Success: []*etcdserverpb.RequestOp{
					txnClientPutOp(&etcdserverpb.PutRequest{Key: key, Value: []byte("success")}),
				},
				Failure: []*etcdserverpb.RequestOp{
					txnClientPutOp(&etcdserverpb.PutRequest{Key: key, Value: []byte("failure")}),
				},
			})
			require.NoError(t, txnErr)
			require.Equal(t, tt.succeeded, response.Succeeded)

			got, rangeErr := kv.Range(ctx, &etcdserverpb.RangeRequest{Key: key})
			require.NoError(t, rangeErr)
			require.Len(t, got.Kvs, 1)
			require.Equal(t, tt.value, got.Kvs[0].Value)
		})
	}
}

func TestRawGRPCTxnRangeCompareEnumFallthroughMatchesEtcd(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()

	grpcServer := grpc.NewServer(server.ClientServerOptions()...)
	etcdserverpb.RegisterKVServer(grpcServer, server)
	listener := bufconn.Listen(1 << 20)
	go func() { _ = grpcServer.Serve(listener) }()
	t.Cleanup(grpcServer.Stop)

	conn, err := grpc.NewClient("passthrough:///txn-range-compare-enum-client",
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
			return listener.Dial()
		}))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, conn.Close()) })
	kv := etcdserverpb.NewKVClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	base := fmt.Sprintf("/a3008/txn-range-compare-enum/%d/", time.Now().UnixNano())
	prefix := base + "keys/"
	rangeEnd := []byte(clientv3.GetPrefixRangeEnd(prefix))
	resultPrefix := []byte(base + "results/")
	putA, err := kv.Put(ctx, &etcdserverpb.PutRequest{Key: []byte(prefix + "a"), Value: []byte("alpha")})
	require.NoError(t, err)
	putB, err := kv.Put(ctx, &etcdserverpb.PutRequest{Key: []byte(prefix + "b"), Value: []byte("omega")})
	require.NoError(t, err)
	require.Greater(t, putB.Header.Revision, putA.Header.Revision)

	tests := []struct {
		name         string
		target       etcdserverpb.Compare_CompareTarget
		result       etcdserverpb.Compare_CompareResult
		compareValue []byte
		succeeded    bool
	}{
		{name: "unknown-result-mod", target: etcdserverpb.Compare_MOD, result: etcdserverpb.Compare_CompareResult(99), succeeded: true},
		{name: "unknown-result-value", target: etcdserverpb.Compare_VALUE, result: etcdserverpb.Compare_CompareResult(99), compareValue: []byte("zzz"), succeeded: true},
		{name: "unknown-target-equal", target: etcdserverpb.Compare_CompareTarget(99), result: etcdserverpb.Compare_EQUAL, succeeded: true},
		{name: "unknown-target-not-equal", target: etcdserverpb.Compare_CompareTarget(99), result: etcdserverpb.Compare_NOT_EQUAL, succeeded: false},
		{name: "unknown-target-greater", target: etcdserverpb.Compare_CompareTarget(99), result: etcdserverpb.Compare_GREATER, succeeded: false},
		{name: "unknown-target-unknown-result", target: etcdserverpb.Compare_CompareTarget(99), result: etcdserverpb.Compare_CompareResult(99), succeeded: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			resultKey := append(append([]byte(nil), resultPrefix...), []byte(tt.name)...)
			compare := &etcdserverpb.Compare{
				Key:         []byte(prefix),
				RangeEnd:    rangeEnd,
				Target:      tt.target,
				Result:      tt.result,
				TargetUnion: &etcdserverpb.Compare_ModRevision{ModRevision: 0},
			}
			if tt.target == etcdserverpb.Compare_VALUE {
				compare.TargetUnion = &etcdserverpb.Compare_Value{Value: tt.compareValue}
			}
			response, txnErr := kv.Txn(ctx, &etcdserverpb.TxnRequest{
				Compare: []*etcdserverpb.Compare{compare},
				Success: []*etcdserverpb.RequestOp{
					txnClientPutOp(&etcdserverpb.PutRequest{Key: resultKey, Value: []byte("success")}),
				},
				Failure: []*etcdserverpb.RequestOp{
					txnClientPutOp(&etcdserverpb.PutRequest{Key: resultKey, Value: []byte("failure")}),
				},
			})
			require.NoError(t, txnErr)
			require.Equal(t, tt.succeeded, response.Succeeded)
			require.NotNil(t, response.Header)
			require.Greater(t, response.Header.Revision, putB.Header.Revision)

			got, rangeErr := kv.Range(ctx, &etcdserverpb.RangeRequest{Key: resultKey})
			require.NoError(t, rangeErr)
			require.Len(t, got.Kvs, 1)
			if tt.succeeded {
				require.Equal(t, []byte("success"), got.Kvs[0].Value)
			} else {
				require.Equal(t, []byte("failure"), got.Kvs[0].Value)
			}
			require.Equal(t, response.Header.Revision, got.Kvs[0].CreateRevision)
			require.Equal(t, response.Header.Revision, got.Kvs[0].ModRevision)
			require.Equal(t, int64(1), got.Kvs[0].Version)
		})
	}
}

func TestRawGRPCTxnFromKeyCompareEnumFallthroughMatchesEtcd(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()

	grpcServer := grpc.NewServer(server.ClientServerOptions()...)
	etcdserverpb.RegisterKVServer(grpcServer, server)
	listener := bufconn.Listen(1 << 20)
	go func() { _ = grpcServer.Serve(listener) }()
	t.Cleanup(grpcServer.Stop)

	conn, err := grpc.NewClient("passthrough:///txn-from-key-compare-enum-client",
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
			return listener.Dial()
		}))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, conn.Close()) })
	kv := etcdserverpb.NewKVClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	base := fmt.Sprintf("/a3009/txn-from-key-compare-enum/%d/", time.Now().UnixNano())
	resultPrefix := []byte(base + "00-results/")
	start := base + "10-keys/"
	putBefore, err := kv.Put(ctx, &etcdserverpb.PutRequest{Key: []byte(base + "05-before"), Value: []byte("before")})
	require.NoError(t, err)
	putA, err := kv.Put(ctx, &etcdserverpb.PutRequest{Key: []byte(start + "a"), Value: []byte("alpha")})
	require.NoError(t, err)
	putB, err := kv.Put(ctx, &etcdserverpb.PutRequest{Key: []byte(start + "b"), Value: []byte("omega")})
	require.NoError(t, err)
	require.Greater(t, putA.Header.Revision, putBefore.Header.Revision)
	require.Greater(t, putB.Header.Revision, putA.Header.Revision)

	tests := []struct {
		name         string
		target       etcdserverpb.Compare_CompareTarget
		result       etcdserverpb.Compare_CompareResult
		compareValue []byte
		succeeded    bool
	}{
		{name: "unknown-result-mod", target: etcdserverpb.Compare_MOD, result: etcdserverpb.Compare_CompareResult(99), succeeded: true},
		{name: "unknown-result-value", target: etcdserverpb.Compare_VALUE, result: etcdserverpb.Compare_CompareResult(99), compareValue: []byte("zzz"), succeeded: true},
		{name: "unknown-target-equal", target: etcdserverpb.Compare_CompareTarget(99), result: etcdserverpb.Compare_EQUAL, succeeded: true},
		{name: "unknown-target-not-equal", target: etcdserverpb.Compare_CompareTarget(99), result: etcdserverpb.Compare_NOT_EQUAL, succeeded: false},
		{name: "unknown-target-greater", target: etcdserverpb.Compare_CompareTarget(99), result: etcdserverpb.Compare_GREATER, succeeded: false},
		{name: "unknown-target-unknown-result", target: etcdserverpb.Compare_CompareTarget(99), result: etcdserverpb.Compare_CompareResult(99), succeeded: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			resultKey := append(append([]byte(nil), resultPrefix...), []byte(tt.name)...)
			compare := &etcdserverpb.Compare{
				Key:         []byte(start),
				RangeEnd:    []byte{0},
				Target:      tt.target,
				Result:      tt.result,
				TargetUnion: &etcdserverpb.Compare_ModRevision{ModRevision: 0},
			}
			if tt.target == etcdserverpb.Compare_VALUE {
				compare.TargetUnion = &etcdserverpb.Compare_Value{Value: tt.compareValue}
			}
			response, txnErr := kv.Txn(ctx, &etcdserverpb.TxnRequest{
				Compare: []*etcdserverpb.Compare{compare},
				Success: []*etcdserverpb.RequestOp{
					txnClientPutOp(&etcdserverpb.PutRequest{Key: resultKey, Value: []byte("success")}),
				},
				Failure: []*etcdserverpb.RequestOp{
					txnClientPutOp(&etcdserverpb.PutRequest{Key: resultKey, Value: []byte("failure")}),
				},
			})
			require.NoError(t, txnErr)
			require.Equal(t, tt.succeeded, response.Succeeded)
			require.NotNil(t, response.Header)
			require.Greater(t, response.Header.Revision, putB.Header.Revision)

			got, rangeErr := kv.Range(ctx, &etcdserverpb.RangeRequest{Key: resultKey})
			require.NoError(t, rangeErr)
			require.Len(t, got.Kvs, 1)
			if tt.succeeded {
				require.Equal(t, []byte("success"), got.Kvs[0].Value)
			} else {
				require.Equal(t, []byte("failure"), got.Kvs[0].Value)
			}
			require.Equal(t, response.Header.Revision, got.Kvs[0].CreateRevision)
			require.Equal(t, response.Header.Revision, got.Kvs[0].ModRevision)
			require.Equal(t, int64(1), got.Kvs[0].Version)
		})
	}
}

func TestRawGRPCTxnEmptyRangeCompareEnumFallthroughMatchesEtcd(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()

	grpcServer := grpc.NewServer(server.ClientServerOptions()...)
	etcdserverpb.RegisterKVServer(grpcServer, server)
	listener := bufconn.Listen(1 << 20)
	go func() { _ = grpcServer.Serve(listener) }()
	t.Cleanup(grpcServer.Stop)

	conn, err := grpc.NewClient("passthrough:///txn-empty-range-compare-enum-client",
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
			return listener.Dial()
		}))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, conn.Close()) })
	kv := etcdserverpb.NewKVClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	base := fmt.Sprintf("/a3010/txn-empty-range-compare-enum/%d/", time.Now().UnixNano())
	emptyPrefix := base + "empty/"
	emptyRangeEnd := []byte(clientv3.GetPrefixRangeEnd(emptyPrefix))
	resultPrefix := []byte(base + "results/")
	seed, err := kv.Put(ctx, &etcdserverpb.PutRequest{Key: []byte(base + "seed"), Value: []byte("seed")})
	require.NoError(t, err)

	tests := []struct {
		name         string
		target       etcdserverpb.Compare_CompareTarget
		result       etcdserverpb.Compare_CompareResult
		compareValue []byte
		succeeded    bool
	}{
		{name: "unknown-result-mod", target: etcdserverpb.Compare_MOD, result: etcdserverpb.Compare_CompareResult(99), succeeded: true},
		{name: "unknown-result-value", target: etcdserverpb.Compare_VALUE, result: etcdserverpb.Compare_CompareResult(99), compareValue: []byte("zzz"), succeeded: false},
		{name: "unknown-target-equal", target: etcdserverpb.Compare_CompareTarget(99), result: etcdserverpb.Compare_EQUAL, succeeded: true},
		{name: "unknown-target-not-equal", target: etcdserverpb.Compare_CompareTarget(99), result: etcdserverpb.Compare_NOT_EQUAL, succeeded: false},
		{name: "unknown-target-greater", target: etcdserverpb.Compare_CompareTarget(99), result: etcdserverpb.Compare_GREATER, succeeded: false},
		{name: "unknown-target-unknown-result", target: etcdserverpb.Compare_CompareTarget(99), result: etcdserverpb.Compare_CompareResult(99), succeeded: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			resultKey := append(append([]byte(nil), resultPrefix...), []byte(tt.name)...)
			compare := &etcdserverpb.Compare{
				Key:         []byte(emptyPrefix),
				RangeEnd:    emptyRangeEnd,
				Target:      tt.target,
				Result:      tt.result,
				TargetUnion: &etcdserverpb.Compare_ModRevision{ModRevision: 0},
			}
			if tt.target == etcdserverpb.Compare_VALUE {
				compare.TargetUnion = &etcdserverpb.Compare_Value{Value: tt.compareValue}
			}
			response, txnErr := kv.Txn(ctx, &etcdserverpb.TxnRequest{
				Compare: []*etcdserverpb.Compare{compare},
				Success: []*etcdserverpb.RequestOp{
					txnClientPutOp(&etcdserverpb.PutRequest{Key: resultKey, Value: []byte("success")}),
				},
				Failure: []*etcdserverpb.RequestOp{
					txnClientPutOp(&etcdserverpb.PutRequest{Key: resultKey, Value: []byte("failure")}),
				},
			})
			require.NoError(t, txnErr)
			require.Equal(t, tt.succeeded, response.Succeeded)
			require.NotNil(t, response.Header)
			require.Greater(t, response.Header.Revision, seed.Header.Revision)

			got, rangeErr := kv.Range(ctx, &etcdserverpb.RangeRequest{Key: resultKey})
			require.NoError(t, rangeErr)
			require.Len(t, got.Kvs, 1)
			if tt.succeeded {
				require.Equal(t, []byte("success"), got.Kvs[0].Value)
			} else {
				require.Equal(t, []byte("failure"), got.Kvs[0].Value)
			}
			require.Equal(t, response.Header.Revision, got.Kvs[0].CreateRevision)
			require.Equal(t, response.Header.Revision, got.Kvs[0].ModRevision)
			require.Equal(t, int64(1), got.Kvs[0].Version)
		})
	}
}

func TestRawGRPCTxnEmptyFromKeyCompareEnumFallthroughMatchesEtcd(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()

	grpcServer := grpc.NewServer(server.ClientServerOptions()...)
	etcdserverpb.RegisterKVServer(grpcServer, server)
	listener := bufconn.Listen(1 << 20)
	go func() { _ = grpcServer.Serve(listener) }()
	t.Cleanup(grpcServer.Stop)

	conn, err := grpc.NewClient("passthrough:///txn-empty-from-key-compare-enum-client",
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
			return listener.Dial()
		}))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, conn.Close()) })
	kv := etcdserverpb.NewKVClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	base := fmt.Sprintf("/a3011/txn-empty-from-key-compare-enum/%d/", time.Now().UnixNano())
	resultPrefix := []byte(base + "00-results/")
	start := base + "90-empty/"
	seed, err := kv.Put(ctx, &etcdserverpb.PutRequest{Key: []byte(base + "05-seed"), Value: []byte("seed")})
	require.NoError(t, err)

	tests := []struct {
		name         string
		target       etcdserverpb.Compare_CompareTarget
		result       etcdserverpb.Compare_CompareResult
		compareValue []byte
		succeeded    bool
	}{
		{name: "unknown-result-mod", target: etcdserverpb.Compare_MOD, result: etcdserverpb.Compare_CompareResult(99), succeeded: true},
		{name: "unknown-result-value", target: etcdserverpb.Compare_VALUE, result: etcdserverpb.Compare_CompareResult(99), compareValue: []byte("zzz"), succeeded: false},
		{name: "unknown-target-equal", target: etcdserverpb.Compare_CompareTarget(99), result: etcdserverpb.Compare_EQUAL, succeeded: true},
		{name: "unknown-target-not-equal", target: etcdserverpb.Compare_CompareTarget(99), result: etcdserverpb.Compare_NOT_EQUAL, succeeded: false},
		{name: "unknown-target-greater", target: etcdserverpb.Compare_CompareTarget(99), result: etcdserverpb.Compare_GREATER, succeeded: false},
		{name: "unknown-target-unknown-result", target: etcdserverpb.Compare_CompareTarget(99), result: etcdserverpb.Compare_CompareResult(99), succeeded: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			resultKey := append(append([]byte(nil), resultPrefix...), []byte(tt.name)...)
			compare := &etcdserverpb.Compare{
				Key:         []byte(start),
				RangeEnd:    []byte{0},
				Target:      tt.target,
				Result:      tt.result,
				TargetUnion: &etcdserverpb.Compare_ModRevision{ModRevision: 0},
			}
			if tt.target == etcdserverpb.Compare_VALUE {
				compare.TargetUnion = &etcdserverpb.Compare_Value{Value: tt.compareValue}
			}
			response, txnErr := kv.Txn(ctx, &etcdserverpb.TxnRequest{
				Compare: []*etcdserverpb.Compare{compare},
				Success: []*etcdserverpb.RequestOp{
					txnClientPutOp(&etcdserverpb.PutRequest{Key: resultKey, Value: []byte("success")}),
				},
				Failure: []*etcdserverpb.RequestOp{
					txnClientPutOp(&etcdserverpb.PutRequest{Key: resultKey, Value: []byte("failure")}),
				},
			})
			require.NoError(t, txnErr)
			require.Equal(t, tt.succeeded, response.Succeeded)
			require.NotNil(t, response.Header)
			require.Greater(t, response.Header.Revision, seed.Header.Revision)

			got, rangeErr := kv.Range(ctx, &etcdserverpb.RangeRequest{Key: resultKey})
			require.NoError(t, rangeErr)
			require.Len(t, got.Kvs, 1)
			if tt.succeeded {
				require.Equal(t, []byte("success"), got.Kvs[0].Value)
			} else {
				require.Equal(t, []byte("failure"), got.Kvs[0].Value)
			}
			require.Equal(t, response.Header.Revision, got.Kvs[0].CreateRevision)
			require.Equal(t, response.Header.Revision, got.Kvs[0].ModRevision)
			require.Equal(t, int64(1), got.Kvs[0].Version)
		})
	}
}

func TestRawGRPCTxnCompareMissingTargetUnionDefaultsMatchEtcd(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()

	grpcServer := grpc.NewServer(server.ClientServerOptions()...)
	etcdserverpb.RegisterKVServer(grpcServer, server)
	listener := bufconn.Listen(1 << 20)
	go func() { _ = grpcServer.Serve(listener) }()
	t.Cleanup(grpcServer.Stop)

	conn, err := grpc.NewClient("passthrough:///txn-compare-missing-union-client",
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
			return listener.Dial()
		}))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, conn.Close()) })
	kv := etcdserverpb.NewKVClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	prefix := fmt.Sprintf("/a2185/txn-compare-missing-union/%d/", time.Now().UnixNano())
	key := []byte(prefix + "key")
	put, err := kv.Put(ctx, &etcdserverpb.PutRequest{Key: key, Value: []byte("value")})
	require.NoError(t, err)

	tests := []struct {
		name      string
		target    etcdserverpb.Compare_CompareTarget
		result    etcdserverpb.Compare_CompareResult
		succeeded bool
	}{
		{name: "mod-greater-default-zero", target: etcdserverpb.Compare_MOD, result: etcdserverpb.Compare_GREATER, succeeded: true},
		{name: "create-greater-default-zero", target: etcdserverpb.Compare_CREATE, result: etcdserverpb.Compare_GREATER, succeeded: true},
		{name: "version-greater-default-zero", target: etcdserverpb.Compare_VERSION, result: etcdserverpb.Compare_GREATER, succeeded: true},
		{name: "lease-equal-default-zero", target: etcdserverpb.Compare_LEASE, result: etcdserverpb.Compare_EQUAL, succeeded: true},
		{name: "value-equal-default-empty", target: etcdserverpb.Compare_VALUE, result: etcdserverpb.Compare_EQUAL, succeeded: false},
		{name: "value-greater-default-empty", target: etcdserverpb.Compare_VALUE, result: etcdserverpb.Compare_GREATER, succeeded: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			resultKey := []byte(prefix + tt.name)
			response, txnErr := kv.Txn(ctx, &etcdserverpb.TxnRequest{
				Compare: []*etcdserverpb.Compare{{
					Key:    key,
					Target: tt.target,
					Result: tt.result,
				}},
				Success: []*etcdserverpb.RequestOp{
					txnClientPutOp(&etcdserverpb.PutRequest{Key: resultKey, Value: []byte("success")}),
				},
				Failure: []*etcdserverpb.RequestOp{
					txnClientPutOp(&etcdserverpb.PutRequest{Key: resultKey, Value: []byte("failure")}),
				},
			})
			require.NoError(t, txnErr)
			require.Equal(t, tt.succeeded, response.Succeeded)
			require.NotNil(t, response.Header)
			require.Greater(t, response.Header.Revision, put.Header.Revision)

			got, rangeErr := kv.Range(ctx, &etcdserverpb.RangeRequest{Key: resultKey})
			require.NoError(t, rangeErr)
			require.Len(t, got.Kvs, 1)
			if tt.succeeded {
				require.Equal(t, []byte("success"), got.Kvs[0].Value)
			} else {
				require.Equal(t, []byte("failure"), got.Kvs[0].Value)
			}
			require.Equal(t, response.Header.Revision, got.Kvs[0].CreateRevision)
			require.Equal(t, response.Header.Revision, got.Kvs[0].ModRevision)
			require.Equal(t, int64(1), got.Kvs[0].Version)
		})
	}
}

func TestRawGRPCTxnRangeCompareMissingTargetUnionDefaultsMatchEtcd(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()

	grpcServer := grpc.NewServer(server.ClientServerOptions()...)
	etcdserverpb.RegisterKVServer(grpcServer, server)
	listener := bufconn.Listen(1 << 20)
	go func() { _ = grpcServer.Serve(listener) }()
	t.Cleanup(grpcServer.Stop)

	conn, err := grpc.NewClient("passthrough:///txn-range-compare-missing-union-client",
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
			return listener.Dial()
		}))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, conn.Close()) })
	kv := etcdserverpb.NewKVClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	base := fmt.Sprintf("/a3004/txn-range-compare-missing-union/%d/", time.Now().UnixNano())
	prefix := base + "keys/"
	rangeEnd := []byte(clientv3.GetPrefixRangeEnd(prefix))
	resultPrefix := []byte(base + "results/")
	putA, err := kv.Put(ctx, &etcdserverpb.PutRequest{Key: []byte(prefix + "a"), Value: []byte("alpha")})
	require.NoError(t, err)
	putB, err := kv.Put(ctx, &etcdserverpb.PutRequest{Key: []byte(prefix + "b"), Value: []byte("omega")})
	require.NoError(t, err)
	require.Greater(t, putB.Header.Revision, putA.Header.Revision)

	tests := []struct {
		name      string
		target    etcdserverpb.Compare_CompareTarget
		result    etcdserverpb.Compare_CompareResult
		succeeded bool
	}{
		{name: "mod-greater-default-zero", target: etcdserverpb.Compare_MOD, result: etcdserverpb.Compare_GREATER, succeeded: true},
		{name: "create-greater-default-zero", target: etcdserverpb.Compare_CREATE, result: etcdserverpb.Compare_GREATER, succeeded: true},
		{name: "version-greater-default-zero", target: etcdserverpb.Compare_VERSION, result: etcdserverpb.Compare_GREATER, succeeded: true},
		{name: "lease-equal-default-zero", target: etcdserverpb.Compare_LEASE, result: etcdserverpb.Compare_EQUAL, succeeded: true},
		{name: "lease-not-equal-default-zero", target: etcdserverpb.Compare_LEASE, result: etcdserverpb.Compare_NOT_EQUAL, succeeded: false},
		{name: "value-equal-default-empty", target: etcdserverpb.Compare_VALUE, result: etcdserverpb.Compare_EQUAL, succeeded: false},
		{name: "value-greater-default-empty", target: etcdserverpb.Compare_VALUE, result: etcdserverpb.Compare_GREATER, succeeded: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			resultKey := append(append([]byte(nil), resultPrefix...), []byte(tt.name)...)
			response, txnErr := kv.Txn(ctx, &etcdserverpb.TxnRequest{
				Compare: []*etcdserverpb.Compare{{
					Key:      []byte(prefix),
					RangeEnd: rangeEnd,
					Target:   tt.target,
					Result:   tt.result,
				}},
				Success: []*etcdserverpb.RequestOp{
					txnClientPutOp(&etcdserverpb.PutRequest{Key: resultKey, Value: []byte("success")}),
				},
				Failure: []*etcdserverpb.RequestOp{
					txnClientPutOp(&etcdserverpb.PutRequest{Key: resultKey, Value: []byte("failure")}),
				},
			})
			require.NoError(t, txnErr)
			require.Equal(t, tt.succeeded, response.Succeeded)
			require.NotNil(t, response.Header)
			require.Greater(t, response.Header.Revision, putB.Header.Revision)

			got, rangeErr := kv.Range(ctx, &etcdserverpb.RangeRequest{Key: resultKey})
			require.NoError(t, rangeErr)
			require.Len(t, got.Kvs, 1)
			if tt.succeeded {
				require.Equal(t, []byte("success"), got.Kvs[0].Value)
			} else {
				require.Equal(t, []byte("failure"), got.Kvs[0].Value)
			}
			require.Equal(t, response.Header.Revision, got.Kvs[0].CreateRevision)
			require.Equal(t, response.Header.Revision, got.Kvs[0].ModRevision)
			require.Equal(t, int64(1), got.Kvs[0].Version)
		})
	}
}

func TestRawGRPCTxnEmptyRangeCompareMissingTargetUnionDefaultsMatchEtcd(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()

	grpcServer := grpc.NewServer(server.ClientServerOptions()...)
	etcdserverpb.RegisterKVServer(grpcServer, server)
	listener := bufconn.Listen(1 << 20)
	go func() { _ = grpcServer.Serve(listener) }()
	t.Cleanup(grpcServer.Stop)

	conn, err := grpc.NewClient("passthrough:///txn-empty-range-compare-missing-union-client",
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
			return listener.Dial()
		}))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, conn.Close()) })
	kv := etcdserverpb.NewKVClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	base := fmt.Sprintf("/a3005/txn-empty-range-compare-missing-union/%d/", time.Now().UnixNano())
	emptyPrefix := base + "empty/"
	emptyRangeEnd := []byte(clientv3.GetPrefixRangeEnd(emptyPrefix))
	resultPrefix := []byte(base + "results/")
	seed, err := kv.Put(ctx, &etcdserverpb.PutRequest{Key: []byte(base + "seed"), Value: []byte("seed")})
	require.NoError(t, err)

	tests := []struct {
		name      string
		target    etcdserverpb.Compare_CompareTarget
		result    etcdserverpb.Compare_CompareResult
		succeeded bool
	}{
		{name: "mod-equal-default-zero", target: etcdserverpb.Compare_MOD, result: etcdserverpb.Compare_EQUAL, succeeded: true},
		{name: "version-equal-default-zero", target: etcdserverpb.Compare_VERSION, result: etcdserverpb.Compare_EQUAL, succeeded: true},
		{name: "create-greater-default-zero", target: etcdserverpb.Compare_CREATE, result: etcdserverpb.Compare_GREATER, succeeded: false},
		{name: "lease-not-equal-default-zero", target: etcdserverpb.Compare_LEASE, result: etcdserverpb.Compare_NOT_EQUAL, succeeded: false},
		{name: "value-equal-default-empty", target: etcdserverpb.Compare_VALUE, result: etcdserverpb.Compare_EQUAL, succeeded: false},
		{name: "value-unknown-result", target: etcdserverpb.Compare_VALUE, result: etcdserverpb.Compare_CompareResult(99), succeeded: false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			resultKey := append(append([]byte(nil), resultPrefix...), []byte(tt.name)...)
			response, txnErr := kv.Txn(ctx, &etcdserverpb.TxnRequest{
				Compare: []*etcdserverpb.Compare{{
					Key:      []byte(emptyPrefix),
					RangeEnd: emptyRangeEnd,
					Target:   tt.target,
					Result:   tt.result,
				}},
				Success: []*etcdserverpb.RequestOp{
					txnClientPutOp(&etcdserverpb.PutRequest{Key: resultKey, Value: []byte("success")}),
				},
				Failure: []*etcdserverpb.RequestOp{
					txnClientPutOp(&etcdserverpb.PutRequest{Key: resultKey, Value: []byte("failure")}),
				},
			})
			require.NoError(t, txnErr)
			require.Equal(t, tt.succeeded, response.Succeeded)
			require.NotNil(t, response.Header)
			require.Greater(t, response.Header.Revision, seed.Header.Revision)

			got, rangeErr := kv.Range(ctx, &etcdserverpb.RangeRequest{Key: resultKey})
			require.NoError(t, rangeErr)
			require.Len(t, got.Kvs, 1)
			if tt.succeeded {
				require.Equal(t, []byte("success"), got.Kvs[0].Value)
			} else {
				require.Equal(t, []byte("failure"), got.Kvs[0].Value)
			}
			require.Equal(t, response.Header.Revision, got.Kvs[0].CreateRevision)
			require.Equal(t, response.Header.Revision, got.Kvs[0].ModRevision)
			require.Equal(t, int64(1), got.Kvs[0].Version)
		})
	}
}

func TestRawGRPCTxnFromKeyCompareMissingTargetUnionDefaultsMatchEtcd(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()

	grpcServer := grpc.NewServer(server.ClientServerOptions()...)
	etcdserverpb.RegisterKVServer(grpcServer, server)
	listener := bufconn.Listen(1 << 20)
	go func() { _ = grpcServer.Serve(listener) }()
	t.Cleanup(grpcServer.Stop)

	conn, err := grpc.NewClient("passthrough:///txn-from-key-compare-missing-union-client",
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
			return listener.Dial()
		}))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, conn.Close()) })
	kv := etcdserverpb.NewKVClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	base := fmt.Sprintf("/a3006/txn-from-key-compare-missing-union/%d/", time.Now().UnixNano())
	resultPrefix := []byte(base + "00-results/")
	start := base + "10-keys/"
	putBefore, err := kv.Put(ctx, &etcdserverpb.PutRequest{Key: []byte(base + "05-before"), Value: []byte("before")})
	require.NoError(t, err)
	putA, err := kv.Put(ctx, &etcdserverpb.PutRequest{Key: []byte(start + "a"), Value: []byte("alpha")})
	require.NoError(t, err)
	putB, err := kv.Put(ctx, &etcdserverpb.PutRequest{Key: []byte(start + "b"), Value: []byte("omega")})
	require.NoError(t, err)
	require.Greater(t, putA.Header.Revision, putBefore.Header.Revision)
	require.Greater(t, putB.Header.Revision, putA.Header.Revision)

	tests := []struct {
		name      string
		target    etcdserverpb.Compare_CompareTarget
		result    etcdserverpb.Compare_CompareResult
		succeeded bool
	}{
		{name: "mod-greater-default-zero", target: etcdserverpb.Compare_MOD, result: etcdserverpb.Compare_GREATER, succeeded: true},
		{name: "create-greater-default-zero", target: etcdserverpb.Compare_CREATE, result: etcdserverpb.Compare_GREATER, succeeded: true},
		{name: "version-greater-default-zero", target: etcdserverpb.Compare_VERSION, result: etcdserverpb.Compare_GREATER, succeeded: true},
		{name: "lease-equal-default-zero", target: etcdserverpb.Compare_LEASE, result: etcdserverpb.Compare_EQUAL, succeeded: true},
		{name: "lease-not-equal-default-zero", target: etcdserverpb.Compare_LEASE, result: etcdserverpb.Compare_NOT_EQUAL, succeeded: false},
		{name: "value-equal-default-empty", target: etcdserverpb.Compare_VALUE, result: etcdserverpb.Compare_EQUAL, succeeded: false},
		{name: "value-greater-default-empty", target: etcdserverpb.Compare_VALUE, result: etcdserverpb.Compare_GREATER, succeeded: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			resultKey := append(append([]byte(nil), resultPrefix...), []byte(tt.name)...)
			response, txnErr := kv.Txn(ctx, &etcdserverpb.TxnRequest{
				Compare: []*etcdserverpb.Compare{{
					Key:      []byte(start),
					RangeEnd: []byte{0},
					Target:   tt.target,
					Result:   tt.result,
				}},
				Success: []*etcdserverpb.RequestOp{
					txnClientPutOp(&etcdserverpb.PutRequest{Key: resultKey, Value: []byte("success")}),
				},
				Failure: []*etcdserverpb.RequestOp{
					txnClientPutOp(&etcdserverpb.PutRequest{Key: resultKey, Value: []byte("failure")}),
				},
			})
			require.NoError(t, txnErr)
			require.Equal(t, tt.succeeded, response.Succeeded)
			require.NotNil(t, response.Header)
			require.Greater(t, response.Header.Revision, putB.Header.Revision)

			got, rangeErr := kv.Range(ctx, &etcdserverpb.RangeRequest{Key: resultKey})
			require.NoError(t, rangeErr)
			require.Len(t, got.Kvs, 1)
			if tt.succeeded {
				require.Equal(t, []byte("success"), got.Kvs[0].Value)
			} else {
				require.Equal(t, []byte("failure"), got.Kvs[0].Value)
			}
			require.Equal(t, response.Header.Revision, got.Kvs[0].CreateRevision)
			require.Equal(t, response.Header.Revision, got.Kvs[0].ModRevision)
			require.Equal(t, int64(1), got.Kvs[0].Version)
		})
	}
}

func TestRawGRPCTxnEmptyFromKeyCompareMissingTargetUnionDefaultsMatchEtcd(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()

	grpcServer := grpc.NewServer(server.ClientServerOptions()...)
	etcdserverpb.RegisterKVServer(grpcServer, server)
	listener := bufconn.Listen(1 << 20)
	go func() { _ = grpcServer.Serve(listener) }()
	t.Cleanup(grpcServer.Stop)

	conn, err := grpc.NewClient("passthrough:///txn-empty-from-key-compare-missing-union-client",
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
			return listener.Dial()
		}))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, conn.Close()) })
	kv := etcdserverpb.NewKVClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	base := fmt.Sprintf("/a3007/txn-empty-from-key-compare-missing-union/%d/", time.Now().UnixNano())
	resultPrefix := []byte(base + "00-results/")
	start := base + "90-empty/"
	seed, err := kv.Put(ctx, &etcdserverpb.PutRequest{Key: []byte(base + "05-seed"), Value: []byte("seed")})
	require.NoError(t, err)

	tests := []struct {
		name      string
		target    etcdserverpb.Compare_CompareTarget
		result    etcdserverpb.Compare_CompareResult
		succeeded bool
	}{
		{name: "mod-equal-default-zero", target: etcdserverpb.Compare_MOD, result: etcdserverpb.Compare_EQUAL, succeeded: true},
		{name: "version-equal-default-zero", target: etcdserverpb.Compare_VERSION, result: etcdserverpb.Compare_EQUAL, succeeded: true},
		{name: "create-greater-default-zero", target: etcdserverpb.Compare_CREATE, result: etcdserverpb.Compare_GREATER, succeeded: false},
		{name: "lease-not-equal-default-zero", target: etcdserverpb.Compare_LEASE, result: etcdserverpb.Compare_NOT_EQUAL, succeeded: false},
		{name: "value-equal-default-empty", target: etcdserverpb.Compare_VALUE, result: etcdserverpb.Compare_EQUAL, succeeded: false},
		{name: "value-unknown-result", target: etcdserverpb.Compare_VALUE, result: etcdserverpb.Compare_CompareResult(99), succeeded: false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			resultKey := append(append([]byte(nil), resultPrefix...), []byte(tt.name)...)
			response, txnErr := kv.Txn(ctx, &etcdserverpb.TxnRequest{
				Compare: []*etcdserverpb.Compare{{
					Key:      []byte(start),
					RangeEnd: []byte{0},
					Target:   tt.target,
					Result:   tt.result,
				}},
				Success: []*etcdserverpb.RequestOp{
					txnClientPutOp(&etcdserverpb.PutRequest{Key: resultKey, Value: []byte("success")}),
				},
				Failure: []*etcdserverpb.RequestOp{
					txnClientPutOp(&etcdserverpb.PutRequest{Key: resultKey, Value: []byte("failure")}),
				},
			})
			require.NoError(t, txnErr)
			require.Equal(t, tt.succeeded, response.Succeeded)
			require.NotNil(t, response.Header)
			require.Greater(t, response.Header.Revision, seed.Header.Revision)

			got, rangeErr := kv.Range(ctx, &etcdserverpb.RangeRequest{Key: resultKey})
			require.NoError(t, rangeErr)
			require.Len(t, got.Kvs, 1)
			if tt.succeeded {
				require.Equal(t, []byte("success"), got.Kvs[0].Value)
			} else {
				require.Equal(t, []byte("failure"), got.Kvs[0].Value)
			}
			require.Equal(t, response.Header.Revision, got.Kvs[0].CreateRevision)
			require.Equal(t, response.Header.Revision, got.Kvs[0].ModRevision)
			require.Equal(t, int64(1), got.Kvs[0].Version)
		})
	}
}

func TestRawGRPCTxnFromKeyExecutionStagedView(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()

	grpcServer := grpc.NewServer(server.ClientServerOptions()...)
	etcdserverpb.RegisterKVServer(grpcServer, server)
	listener := bufconn.Listen(1 << 20)
	go func() { _ = grpcServer.Serve(listener) }()
	t.Cleanup(grpcServer.Stop)

	conn, err := grpc.NewClient("passthrough:///txn-from-key-execution-client",
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
			return listener.Dial()
		}))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, conn.Close()) })
	kv := etcdserverpb.NewKVClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	for _, tt := range []struct {
		name           string
		putFirst       bool
		wantDeleted    int64
		wantPrev       []txnClientKV
		wantTxnRange   []txnClientKV
		wantFinalRange []txnClientKV
	}{
		{
			name:        "put-then-delete",
			putFirst:    true,
			wantDeleted: 3,
			wantPrev: []txnClientKV{
				{Key: "b", Value: "seed-b", Version: 1},
				{Key: "c", Value: "seed-c", Version: 1},
				{Key: "d", Value: "txn-d", Version: 1, CreatedInTxn: true, ModifiedInTxn: true},
			},
			wantTxnRange:   []txnClientKV{{Key: "a", Value: "seed-a", Version: 1}},
			wantFinalRange: []txnClientKV{{Key: "a", Value: "seed-a", Version: 1}},
		},
		{
			name:        "delete-then-put",
			wantDeleted: 2,
			wantPrev: []txnClientKV{
				{Key: "b", Value: "seed-b", Version: 1},
				{Key: "c", Value: "seed-c", Version: 1},
			},
			wantTxnRange: []txnClientKV{
				{Key: "a", Value: "seed-a", Version: 1},
				{Key: "d", Value: "txn-d", Version: 1, CreatedInTxn: true, ModifiedInTxn: true},
			},
			wantFinalRange: []txnClientKV{
				{Key: "a", Value: "seed-a", Version: 1},
				{Key: "d", Value: "txn-d", Version: 1, CreatedInTxn: true, ModifiedInTxn: true},
			},
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			prefix := string(bytes.Repeat([]byte{0xff}, 64)) +
				fmt.Sprintf("/a1006/txn-from-key-execution/%d/%s/", time.Now().UnixNano(), tt.name)
			end := []byte(clientv3.GetPrefixRangeEnd(prefix))
			for _, suffix := range []string{"a", "b", "c"} {
				_, putErr := kv.Put(ctx, &etcdserverpb.PutRequest{
					Key: []byte(prefix + suffix), Value: []byte("seed-" + suffix),
				})
				require.NoError(t, putErr)
			}

			put := txnClientPutOp(&etcdserverpb.PutRequest{Key: []byte(prefix + "d"), Value: []byte("txn-d")})
			del := txnClientDeleteOp(&etcdserverpb.DeleteRangeRequest{
				Key: []byte(prefix + "b"), RangeEnd: []byte{0}, PrevKv: true,
			})
			ops := []*etcdserverpb.RequestOp{del, put}
			deleteIndex := 0
			if tt.putFirst {
				ops = []*etcdserverpb.RequestOp{put, del}
				deleteIndex = 1
			}
			ops = append(ops, txnClientRangeOp(&etcdserverpb.RangeRequest{
				Key: []byte(prefix), RangeEnd: end,
			}))
			txn, txnErr := kv.Txn(ctx, &etcdserverpb.TxnRequest{Success: ops})
			require.NoError(t, txnErr)
			require.True(t, txn.Succeeded)
			require.Len(t, txn.Responses, 3)
			deleted := txn.Responses[deleteIndex].GetResponseDeleteRange()
			txnRange := txn.Responses[2].GetResponseRange()
			require.NotNil(t, deleted)
			require.NotNil(t, txnRange)
			require.Equal(t, tt.wantDeleted, deleted.Deleted)
			require.Equal(t, txn.Header.Revision, deleted.Header.Revision)
			require.Equal(t, txn.Header.Revision, txnRange.Header.Revision)
			require.Equal(t, tt.wantPrev, txnClientKVs(deleted.PrevKvs, prefix, txn.Header.Revision))
			require.Equal(t, tt.wantTxnRange, txnClientKVs(txnRange.Kvs, prefix, txn.Header.Revision))

			final, finalErr := kv.Range(ctx, &etcdserverpb.RangeRequest{Key: []byte(prefix), RangeEnd: end})
			require.NoError(t, finalErr)
			require.GreaterOrEqual(t, final.Header.Revision, txn.Header.Revision)
			require.Equal(t, tt.wantFinalRange, txnClientKVs(final.Kvs, prefix, txn.Header.Revision))
		})
	}
}

func TestRawGRPCTxnDuplicateIntervalValidation(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()

	grpcServer := grpc.NewServer(server.ClientServerOptions()...)
	etcdserverpb.RegisterKVServer(grpcServer, server)
	listener := bufconn.Listen(1 << 20)
	go func() { _ = grpcServer.Serve(listener) }()
	t.Cleanup(grpcServer.Stop)

	conn, err := grpc.NewClient("passthrough:///txn-duplicate-interval-client",
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
			return listener.Dial()
		}))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, conn.Close()) })
	kv := etcdserverpb.NewKVClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	prefix := fmt.Sprintf("/a1007/txn-duplicate-interval/%d/", time.Now().UnixNano())
	key := []byte(prefix + "abc")
	put := txnClientPutOp(&etcdserverpb.PutRequest{Key: key, Value: []byte("value")})
	deleteKey := txnClientDeleteOp(&etcdserverpb.DeleteRangeRequest{Key: key})
	deleteContaining := txnClientDeleteOp(&etcdserverpb.DeleteRangeRequest{
		Key: []byte(prefix + "a"), RangeEnd: []byte(prefix + "b"),
	})
	deleteBefore := txnClientDeleteOp(&etcdserverpb.DeleteRangeRequest{
		Key: []byte(prefix + "abb"), RangeEnd: key,
	})
	nestedDelete := txnClientTxnOp([]*etcdserverpb.RequestOp{deleteContaining}, nil)
	nestedDeleteBoth := txnClientTxnOp(
		[]*etcdserverpb.RequestOp{deleteContaining},
		[]*etcdserverpb.RequestOp{deleteContaining},
	)
	nestedPut := txnClientTxnOp([]*etcdserverpb.RequestOp{put}, nil)
	nestedPutBoth := txnClientTxnOp(
		[]*etcdserverpb.RequestOp{put},
		[]*etcdserverpb.RequestOp{put},
	)

	tests := []struct {
		name    string
		ops     []*etcdserverpb.RequestOp
		wantErr bool
	}{
		{name: "duplicate-put", ops: []*etcdserverpb.RequestOp{put, put}, wantErr: true},
		{name: "put-and-point-delete", ops: []*etcdserverpb.RequestOp{put, deleteKey}, wantErr: true},
		{name: "put-and-containing-delete", ops: []*etcdserverpb.RequestOp{put, deleteContaining}, wantErr: true},
		{name: "put-and-nested-containing-delete", ops: []*etcdserverpb.RequestOp{put, nestedDelete}, wantErr: true},
		{name: "containing-delete-and-nested-put", ops: []*etcdserverpb.RequestOp{deleteContaining, nestedPut}, wantErr: true},
		{name: "duplicate-sibling-nested-put", ops: []*etcdserverpb.RequestOp{nestedPutBoth, nestedPutBoth}, wantErr: true},
		{name: "disjoint-delete-and-mutually-exclusive-put", ops: []*etcdserverpb.RequestOp{deleteBefore, nestedPutBoth}},
		{name: "nested-overlapping-deletes", ops: []*etcdserverpb.RequestOp{nestedDelete, nestedDeleteBoth}},
		{name: "repeated-overlapping-deletes", ops: []*etcdserverpb.RequestOp{deleteKey, deleteContaining, deleteKey, deleteContaining}},
		{name: "put-and-disjoint-delete", ops: []*etcdserverpb.RequestOp{put, deleteBefore}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			resp, callErr := kv.Txn(ctx, &etcdserverpb.TxnRequest{Success: tt.ops})
			if tt.wantErr {
				require.Nil(t, resp)
				requireRawGRPCTxnError(t, callErr, codes.InvalidArgument, "etcdserver: duplicate key given in txn request", rpctypes.ErrGRPCDuplicateKey)
				return
			}
			require.NoError(t, callErr)
			require.NotNil(t, resp)
			require.True(t, resp.Succeeded)
		})
	}
}

func TestRawGRPCTxnAllowsSameKeyInMutuallyExclusiveBranchesMatchesEtcd(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()

	grpcServer := grpc.NewServer(server.ClientServerOptions()...)
	etcdserverpb.RegisterKVServer(grpcServer, server)
	listener := bufconn.Listen(1 << 20)
	go func() { _ = grpcServer.Serve(listener) }()
	t.Cleanup(grpcServer.Stop)

	conn, err := grpc.NewClient("passthrough:///txn-mutually-exclusive-duplicate-key-client",
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
			return listener.Dial()
		}))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, conn.Close()) })
	kv := etcdserverpb.NewKVClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	prefix := fmt.Sprintf("/a3013/txn-mutually-exclusive-duplicate-key/%d/", time.Now().UnixNano())
	topKey := []byte(prefix + "top")
	topResp, err := kv.Txn(ctx, &etcdserverpb.TxnRequest{
		Compare: []*etcdserverpb.Compare{
			txnClientIntCompare([]byte(prefix+"missing"), nil, etcdserverpb.Compare_VERSION, etcdserverpb.Compare_EQUAL, 0),
		},
		Success: []*etcdserverpb.RequestOp{
			txnClientPutOp(&etcdserverpb.PutRequest{Key: topKey, Value: []byte("success")}),
		},
		Failure: []*etcdserverpb.RequestOp{
			txnClientPutOp(&etcdserverpb.PutRequest{Key: topKey, Value: []byte("failure")}),
		},
	})
	require.NoError(t, err)
	require.NotNil(t, topResp)
	require.True(t, topResp.Succeeded)
	require.NotNil(t, topResp.Header)
	require.Len(t, topResp.Responses, 1)
	require.NotNil(t, topResp.Responses[0].GetResponsePut())

	top, err := kv.Range(ctx, &etcdserverpb.RangeRequest{Key: topKey})
	require.NoError(t, err)
	require.Len(t, top.Kvs, 1)
	require.Equal(t, []byte("success"), top.Kvs[0].Value)
	require.Equal(t, topResp.Header.Revision, top.Kvs[0].CreateRevision)
	require.Equal(t, topResp.Header.Revision, top.Kvs[0].ModRevision)
	require.Equal(t, int64(1), top.Kvs[0].Version)

	nestedKey := []byte(prefix + "nested")
	nestedResp, err := kv.Txn(ctx, &etcdserverpb.TxnRequest{
		Success: []*etcdserverpb.RequestOp{{
			Request: &etcdserverpb.RequestOp_RequestTxn{RequestTxn: &etcdserverpb.TxnRequest{
				Compare: []*etcdserverpb.Compare{
					txnClientIntCompare([]byte(prefix+"nested-missing"), nil, etcdserverpb.Compare_VERSION, etcdserverpb.Compare_EQUAL, 1),
				},
				Success: []*etcdserverpb.RequestOp{
					txnClientPutOp(&etcdserverpb.PutRequest{Key: nestedKey, Value: []byte("then")}),
				},
				Failure: []*etcdserverpb.RequestOp{
					txnClientPutOp(&etcdserverpb.PutRequest{Key: nestedKey, Value: []byte("else")}),
				},
			}},
		}},
	})
	require.NoError(t, err)
	require.NotNil(t, nestedResp)
	require.True(t, nestedResp.Succeeded)
	require.NotNil(t, nestedResp.Header)
	require.Len(t, nestedResp.Responses, 1)
	nestedTxn := nestedResp.Responses[0].GetResponseTxn()
	require.NotNil(t, nestedTxn)
	require.False(t, nestedTxn.Succeeded)
	require.Len(t, nestedTxn.Responses, 1)
	require.NotNil(t, nestedTxn.Responses[0].GetResponsePut())

	nested, err := kv.Range(ctx, &etcdserverpb.RangeRequest{Key: nestedKey})
	require.NoError(t, err)
	require.Len(t, nested.Kvs, 1)
	require.Equal(t, []byte("else"), nested.Kvs[0].Value)
	require.Equal(t, nestedResp.Header.Revision, nested.Kvs[0].CreateRevision)
	require.Equal(t, nestedResp.Header.Revision, nested.Kvs[0].ModRevision)
	require.Equal(t, int64(1), nested.Kvs[0].Version)
}

func TestRawGRPCTxnAllowsDeletePutOverlapInMutuallyExclusiveBranchesMatchesEtcd(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()

	grpcServer := grpc.NewServer(server.ClientServerOptions()...)
	etcdserverpb.RegisterKVServer(grpcServer, server)
	listener := bufconn.Listen(1 << 20)
	go func() { _ = grpcServer.Serve(listener) }()
	t.Cleanup(grpcServer.Stop)

	conn, err := grpc.NewClient("passthrough:///txn-mutually-exclusive-delete-put-client",
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
			return listener.Dial()
		}))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, conn.Close()) })
	kv := etcdserverpb.NewKVClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	prefix := fmt.Sprintf("/a3014/txn-mutually-exclusive-delete-put/%d/", time.Now().UnixNano())

	topKey := []byte(prefix + "top")
	seedTop, err := kv.Put(ctx, &etcdserverpb.PutRequest{Key: topKey, Value: []byte("old")})
	require.NoError(t, err)
	topResp, err := kv.Txn(ctx, &etcdserverpb.TxnRequest{
		Compare: []*etcdserverpb.Compare{
			txnClientIntCompare([]byte(prefix+"missing"), nil, etcdserverpb.Compare_VERSION, etcdserverpb.Compare_EQUAL, 1),
		},
		Success: []*etcdserverpb.RequestOp{
			txnClientDeleteOp(&etcdserverpb.DeleteRangeRequest{Key: topKey}),
		},
		Failure: []*etcdserverpb.RequestOp{
			txnClientPutOp(&etcdserverpb.PutRequest{Key: topKey, Value: []byte("failure-put")}),
		},
	})
	require.NoError(t, err)
	require.NotNil(t, topResp)
	require.False(t, topResp.Succeeded)
	require.NotNil(t, topResp.Header)
	require.Greater(t, topResp.Header.Revision, seedTop.Header.Revision)
	require.Len(t, topResp.Responses, 1)
	require.NotNil(t, topResp.Responses[0].GetResponsePut())

	top, err := kv.Range(ctx, &etcdserverpb.RangeRequest{Key: topKey})
	require.NoError(t, err)
	require.Len(t, top.Kvs, 1)
	require.Equal(t, []byte("failure-put"), top.Kvs[0].Value)
	require.Equal(t, seedTop.Header.Revision, top.Kvs[0].CreateRevision)
	require.Equal(t, topResp.Header.Revision, top.Kvs[0].ModRevision)
	require.Equal(t, int64(2), top.Kvs[0].Version)

	nestedKey := []byte(prefix + "nested")
	seedNested, err := kv.Put(ctx, &etcdserverpb.PutRequest{Key: nestedKey, Value: []byte("old")})
	require.NoError(t, err)
	nestedResp, err := kv.Txn(ctx, &etcdserverpb.TxnRequest{
		Success: []*etcdserverpb.RequestOp{{
			Request: &etcdserverpb.RequestOp_RequestTxn{RequestTxn: &etcdserverpb.TxnRequest{
				Compare: []*etcdserverpb.Compare{
					txnClientIntCompare([]byte(prefix+"nested-missing"), nil, etcdserverpb.Compare_VERSION, etcdserverpb.Compare_EQUAL, 0),
				},
				Success: []*etcdserverpb.RequestOp{
					txnClientDeleteOp(&etcdserverpb.DeleteRangeRequest{Key: nestedKey}),
				},
				Failure: []*etcdserverpb.RequestOp{
					txnClientPutOp(&etcdserverpb.PutRequest{Key: nestedKey, Value: []byte("else-put")}),
				},
			}},
		}},
	})
	require.NoError(t, err)
	require.NotNil(t, nestedResp)
	require.True(t, nestedResp.Succeeded)
	require.NotNil(t, nestedResp.Header)
	require.Greater(t, nestedResp.Header.Revision, seedNested.Header.Revision)
	require.Len(t, nestedResp.Responses, 1)
	nestedTxn := nestedResp.Responses[0].GetResponseTxn()
	require.NotNil(t, nestedTxn)
	require.True(t, nestedTxn.Succeeded)
	require.Len(t, nestedTxn.Responses, 1)
	deleted := nestedTxn.Responses[0].GetResponseDeleteRange()
	require.NotNil(t, deleted)
	require.Equal(t, int64(1), deleted.Deleted)

	nested, err := kv.Range(ctx, &etcdserverpb.RangeRequest{Key: nestedKey})
	require.NoError(t, err)
	require.Empty(t, nested.Kvs)
	require.NotNil(t, nested.Header)
	require.GreaterOrEqual(t, nested.Header.Revision, nestedResp.Header.Revision)
}

func TestRawGRPCTxnSelectedPutIgnoresUnselectedDeleteOverlapMatchesEtcd(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()

	grpcServer := grpc.NewServer(server.ClientServerOptions()...)
	etcdserverpb.RegisterKVServer(grpcServer, server)
	listener := bufconn.Listen(1 << 20)
	go func() { _ = grpcServer.Serve(listener) }()
	t.Cleanup(grpcServer.Stop)

	conn, err := grpc.NewClient("passthrough:///txn-selected-put-unselected-delete-client",
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
			return listener.Dial()
		}))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, conn.Close()) })
	kv := etcdserverpb.NewKVClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	prefix := fmt.Sprintf("/a3015/txn-selected-put-unselected-delete/%d/", time.Now().UnixNano())

	topKey := []byte(prefix + "top")
	seedTop, err := kv.Put(ctx, &etcdserverpb.PutRequest{Key: topKey, Value: []byte("old")})
	require.NoError(t, err)
	topResp, err := kv.Txn(ctx, &etcdserverpb.TxnRequest{
		Compare: []*etcdserverpb.Compare{
			txnClientIntCompare([]byte(prefix+"missing"), nil, etcdserverpb.Compare_VERSION, etcdserverpb.Compare_EQUAL, 0),
		},
		Success: []*etcdserverpb.RequestOp{
			txnClientPutOp(&etcdserverpb.PutRequest{Key: topKey, Value: []byte("success-put")}),
		},
		Failure: []*etcdserverpb.RequestOp{
			txnClientDeleteOp(&etcdserverpb.DeleteRangeRequest{Key: topKey}),
		},
	})
	require.NoError(t, err)
	require.NotNil(t, topResp)
	require.True(t, topResp.Succeeded)
	require.NotNil(t, topResp.Header)
	require.Greater(t, topResp.Header.Revision, seedTop.Header.Revision)
	require.Len(t, topResp.Responses, 1)
	require.NotNil(t, topResp.Responses[0].GetResponsePut())

	top, err := kv.Range(ctx, &etcdserverpb.RangeRequest{Key: topKey})
	require.NoError(t, err)
	require.Len(t, top.Kvs, 1)
	require.Equal(t, []byte("success-put"), top.Kvs[0].Value)
	require.Equal(t, seedTop.Header.Revision, top.Kvs[0].CreateRevision)
	require.Equal(t, topResp.Header.Revision, top.Kvs[0].ModRevision)
	require.Equal(t, int64(2), top.Kvs[0].Version)

	nestedKey := []byte(prefix + "nested")
	seedNested, err := kv.Put(ctx, &etcdserverpb.PutRequest{Key: nestedKey, Value: []byte("old")})
	require.NoError(t, err)
	nestedResp, err := kv.Txn(ctx, &etcdserverpb.TxnRequest{
		Success: []*etcdserverpb.RequestOp{{
			Request: &etcdserverpb.RequestOp_RequestTxn{RequestTxn: &etcdserverpb.TxnRequest{
				Compare: []*etcdserverpb.Compare{
					txnClientIntCompare([]byte(prefix+"nested-missing"), nil, etcdserverpb.Compare_VERSION, etcdserverpb.Compare_EQUAL, 0),
				},
				Success: []*etcdserverpb.RequestOp{
					txnClientPutOp(&etcdserverpb.PutRequest{Key: nestedKey, Value: []byte("then-put")}),
				},
				Failure: []*etcdserverpb.RequestOp{
					txnClientDeleteOp(&etcdserverpb.DeleteRangeRequest{Key: nestedKey}),
				},
			}},
		}},
	})
	require.NoError(t, err)
	require.NotNil(t, nestedResp)
	require.True(t, nestedResp.Succeeded)
	require.NotNil(t, nestedResp.Header)
	require.Greater(t, nestedResp.Header.Revision, seedNested.Header.Revision)
	require.Len(t, nestedResp.Responses, 1)
	nestedTxn := nestedResp.Responses[0].GetResponseTxn()
	require.NotNil(t, nestedTxn)
	require.True(t, nestedTxn.Succeeded)
	require.Len(t, nestedTxn.Responses, 1)
	require.NotNil(t, nestedTxn.Responses[0].GetResponsePut())

	nested, err := kv.Range(ctx, &etcdserverpb.RangeRequest{Key: nestedKey})
	require.NoError(t, err)
	require.Len(t, nested.Kvs, 1)
	require.Equal(t, []byte("then-put"), nested.Kvs[0].Value)
	require.Equal(t, seedNested.Header.Revision, nested.Kvs[0].CreateRevision)
	require.Equal(t, nestedResp.Header.Revision, nested.Kvs[0].ModRevision)
	require.Equal(t, int64(2), nested.Kvs[0].Version)
}

func TestRawGRPCTxnNestedResponseHeadersMatchEtcd(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()

	grpcServer := grpc.NewServer(server.ClientServerOptions()...)
	etcdserverpb.RegisterKVServer(grpcServer, server)
	listener := bufconn.Listen(1 << 20)
	go func() { _ = grpcServer.Serve(listener) }()
	t.Cleanup(grpcServer.Stop)

	conn, err := grpc.NewClient("passthrough:///txn-nested-response-header-client",
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
			return listener.Dial()
		}))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, conn.Close()) })
	kv := etcdserverpb.NewKVClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	prefix := fmt.Sprintf("/a3016/txn-nested-response-header/%d/", time.Now().UnixNano())
	key := []byte(prefix + "key")
	seed, err := kv.Put(ctx, &etcdserverpb.PutRequest{Key: key, Value: []byte("seed")})
	require.NoError(t, err)

	thenResp, err := kv.Txn(ctx, &etcdserverpb.TxnRequest{
		Success: []*etcdserverpb.RequestOp{{
			Request: &etcdserverpb.RequestOp_RequestTxn{RequestTxn: &etcdserverpb.TxnRequest{
				Compare: []*etcdserverpb.Compare{
					txnClientIntCompare(key, nil, etcdserverpb.Compare_VERSION, etcdserverpb.Compare_EQUAL, 1),
				},
				Success: []*etcdserverpb.RequestOp{
					txnClientPutOp(&etcdserverpb.PutRequest{Key: key, Value: []byte("then")}),
					txnClientRangeOp(&etcdserverpb.RangeRequest{Key: key}),
				},
				Failure: []*etcdserverpb.RequestOp{
					txnClientDeleteOp(&etcdserverpb.DeleteRangeRequest{Key: key}),
				},
			}},
		}},
	})
	require.NoError(t, err)
	require.NotNil(t, thenResp)
	require.True(t, thenResp.Succeeded)
	require.NotNil(t, thenResp.Header)
	require.Greater(t, thenResp.Header.Revision, seed.Header.Revision)
	require.Len(t, thenResp.Responses, 1)
	thenNested := thenResp.Responses[0].GetResponseTxn()
	require.NotNil(t, thenNested)
	require.NotNil(t, thenNested.Header)
	require.Zero(t, thenNested.Header.Revision)
	require.True(t, thenNested.Succeeded)
	require.Len(t, thenNested.Responses, 2)
	thenPut := thenNested.Responses[0].GetResponsePut()
	require.NotNil(t, thenPut)
	require.NotNil(t, thenPut.Header)
	require.Equal(t, thenResp.Header.Revision, thenPut.Header.Revision)
	thenRange := thenNested.Responses[1].GetResponseRange()
	require.NotNil(t, thenRange)
	require.NotNil(t, thenRange.Header)
	require.Equal(t, thenResp.Header.Revision, thenRange.Header.Revision)
	require.Len(t, thenRange.Kvs, 1)
	require.Equal(t, []byte("then"), thenRange.Kvs[0].Value)
	require.Equal(t, seed.Header.Revision, thenRange.Kvs[0].CreateRevision)
	require.Equal(t, thenResp.Header.Revision, thenRange.Kvs[0].ModRevision)
	require.Equal(t, int64(2), thenRange.Kvs[0].Version)

	elseResp, err := kv.Txn(ctx, &etcdserverpb.TxnRequest{
		Success: []*etcdserverpb.RequestOp{{
			Request: &etcdserverpb.RequestOp_RequestTxn{RequestTxn: &etcdserverpb.TxnRequest{
				Compare: []*etcdserverpb.Compare{
					txnClientIntCompare(key, nil, etcdserverpb.Compare_VERSION, etcdserverpb.Compare_EQUAL, 1),
				},
				Success: []*etcdserverpb.RequestOp{
					txnClientPutOp(&etcdserverpb.PutRequest{Key: key, Value: []byte("must-not-write")}),
				},
				Failure: []*etcdserverpb.RequestOp{
					txnClientDeleteOp(&etcdserverpb.DeleteRangeRequest{Key: key, PrevKv: true}),
				},
			}},
		}},
	})
	require.NoError(t, err)
	require.NotNil(t, elseResp)
	require.True(t, elseResp.Succeeded)
	require.NotNil(t, elseResp.Header)
	require.Greater(t, elseResp.Header.Revision, thenResp.Header.Revision)
	require.Len(t, elseResp.Responses, 1)
	elseNested := elseResp.Responses[0].GetResponseTxn()
	require.NotNil(t, elseNested)
	require.NotNil(t, elseNested.Header)
	require.Zero(t, elseNested.Header.Revision)
	require.False(t, elseNested.Succeeded)
	require.Len(t, elseNested.Responses, 1)
	elseDelete := elseNested.Responses[0].GetResponseDeleteRange()
	require.NotNil(t, elseDelete)
	require.NotNil(t, elseDelete.Header)
	require.Equal(t, elseResp.Header.Revision, elseDelete.Header.Revision)
	require.Equal(t, int64(1), elseDelete.Deleted)
	require.Len(t, elseDelete.PrevKvs, 1)
	require.Equal(t, key, elseDelete.PrevKvs[0].Key)
	require.Equal(t, []byte("then"), elseDelete.PrevKvs[0].Value)
	require.Equal(t, seed.Header.Revision, elseDelete.PrevKvs[0].CreateRevision)
	require.Equal(t, thenResp.Header.Revision, elseDelete.PrevKvs[0].ModRevision)
	require.Equal(t, int64(2), elseDelete.PrevKvs[0].Version)

	final, err := kv.Range(ctx, &etcdserverpb.RangeRequest{Key: key})
	require.NoError(t, err)
	require.Empty(t, final.Kvs)
	require.NotNil(t, final.Header)
	require.GreaterOrEqual(t, final.Header.Revision, elseResp.Header.Revision)
}

func TestRawGRPCTxnNestedPutPrevKVResponseMatchesEtcd(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()

	grpcServer := grpc.NewServer(server.ClientServerOptions()...)
	etcdserverpb.RegisterKVServer(grpcServer, server)
	listener := bufconn.Listen(1 << 20)
	go func() { _ = grpcServer.Serve(listener) }()
	t.Cleanup(grpcServer.Stop)

	conn, err := grpc.NewClient("passthrough:///txn-nested-put-prev-kv-client",
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
			return listener.Dial()
		}))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, conn.Close()) })
	kv := etcdserverpb.NewKVClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	prefix := fmt.Sprintf("/a3017/txn-nested-put-prev-kv/%d/", time.Now().UnixNano())
	key := []byte(prefix + "key")
	other := []byte(prefix + "other")
	seed, err := kv.Put(ctx, &etcdserverpb.PutRequest{Key: key, Value: []byte("old")})
	require.NoError(t, err)

	resp, err := kv.Txn(ctx, &etcdserverpb.TxnRequest{
		Success: []*etcdserverpb.RequestOp{{
			Request: &etcdserverpb.RequestOp_RequestTxn{RequestTxn: &etcdserverpb.TxnRequest{
				Success: []*etcdserverpb.RequestOp{
					txnClientPutOp(&etcdserverpb.PutRequest{Key: key, Value: []byte("new"), PrevKv: true}),
					txnClientPutOp(&etcdserverpb.PutRequest{Key: other, Value: []byte("other")}),
					txnClientRangeOp(&etcdserverpb.RangeRequest{Key: key}),
				},
			}},
		}},
	})
	require.NoError(t, err)
	require.NotNil(t, resp)
	require.True(t, resp.Succeeded)
	require.NotNil(t, resp.Header)
	require.Greater(t, resp.Header.Revision, seed.Header.Revision)
	require.Len(t, resp.Responses, 1)

	nested := resp.Responses[0].GetResponseTxn()
	require.NotNil(t, nested)
	require.NotNil(t, nested.Header)
	require.Zero(t, nested.Header.Revision)
	require.True(t, nested.Succeeded)
	require.Len(t, nested.Responses, 3)

	put := nested.Responses[0].GetResponsePut()
	require.NotNil(t, put)
	require.NotNil(t, put.Header)
	require.Equal(t, resp.Header.Revision, put.Header.Revision)
	require.NotNil(t, put.PrevKv)
	require.Equal(t, key, put.PrevKv.Key)
	require.Equal(t, []byte("old"), put.PrevKv.Value)
	require.Equal(t, seed.Header.Revision, put.PrevKv.CreateRevision)
	require.Equal(t, seed.Header.Revision, put.PrevKv.ModRevision)
	require.Equal(t, int64(1), put.PrevKv.Version)

	otherPut := nested.Responses[1].GetResponsePut()
	require.NotNil(t, otherPut)
	require.NotNil(t, otherPut.Header)
	require.Equal(t, resp.Header.Revision, otherPut.Header.Revision)
	require.Nil(t, otherPut.PrevKv)

	ranged := nested.Responses[2].GetResponseRange()
	require.NotNil(t, ranged)
	require.NotNil(t, ranged.Header)
	require.Equal(t, resp.Header.Revision, ranged.Header.Revision)
	require.Len(t, ranged.Kvs, 1)
	require.Equal(t, key, ranged.Kvs[0].Key)
	require.Equal(t, []byte("new"), ranged.Kvs[0].Value)
	require.Equal(t, seed.Header.Revision, ranged.Kvs[0].CreateRevision)
	require.Equal(t, resp.Header.Revision, ranged.Kvs[0].ModRevision)
	require.Equal(t, int64(2), ranged.Kvs[0].Version)

	final, err := kv.Range(ctx, &etcdserverpb.RangeRequest{Key: []byte(prefix), RangeEnd: []byte(clientv3.GetPrefixRangeEnd(prefix))})
	require.NoError(t, err)
	require.NotNil(t, final.Header)
	require.GreaterOrEqual(t, final.Header.Revision, resp.Header.Revision)
	require.Len(t, final.Kvs, 2)
	require.Equal(t, key, final.Kvs[0].Key)
	require.Equal(t, other, final.Kvs[1].Key)
}

func TestRawGRPCTxnNestedPutIgnoreOptionsPrevKVResponseMatchesEtcd(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()

	grpcServer := grpc.NewServer(server.ClientServerOptions()...)
	etcdserverpb.RegisterKVServer(grpcServer, server)
	etcdserverpb.RegisterLeaseServer(grpcServer, server)
	listener := bufconn.Listen(1 << 20)
	go func() { _ = grpcServer.Serve(listener) }()
	t.Cleanup(grpcServer.Stop)

	conn, err := grpc.NewClient("passthrough:///txn-nested-put-ignore-options-prev-kv-client",
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
			return listener.Dial()
		}))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, conn.Close()) })
	kv := etcdserverpb.NewKVClient(conn)
	lease := etcdserverpb.NewLeaseClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	prefix := fmt.Sprintf("/a3018/txn-nested-put-ignore-options-prev-kv/%d/", time.Now().UnixNano())
	key := []byte(prefix + "key")
	leaseA, err := lease.LeaseGrant(ctx, &etcdserverpb.LeaseGrantRequest{TTL: 300})
	require.NoError(t, err)
	leaseB, err := lease.LeaseGrant(ctx, &etcdserverpb.LeaseGrantRequest{TTL: 300})
	require.NoError(t, err)
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cleanupCancel()
		_, _ = lease.LeaseRevoke(cleanupCtx, &etcdserverpb.LeaseRevokeRequest{ID: leaseA.ID})
		_, _ = lease.LeaseRevoke(cleanupCtx, &etcdserverpb.LeaseRevokeRequest{ID: leaseB.ID})
	})
	seed, err := kv.Put(ctx, &etcdserverpb.PutRequest{Key: key, Value: []byte("old"), Lease: leaseA.ID})
	require.NoError(t, err)

	ignoreValueResp, err := kv.Txn(ctx, &etcdserverpb.TxnRequest{
		Success: []*etcdserverpb.RequestOp{{
			Request: &etcdserverpb.RequestOp_RequestTxn{RequestTxn: &etcdserverpb.TxnRequest{
				Success: []*etcdserverpb.RequestOp{
					txnClientPutOp(&etcdserverpb.PutRequest{Key: key, Lease: leaseB.ID, IgnoreValue: true, PrevKv: true}),
					txnClientRangeOp(&etcdserverpb.RangeRequest{Key: key}),
				},
			}},
		}},
	})
	require.NoError(t, err)
	require.NotNil(t, ignoreValueResp)
	require.True(t, ignoreValueResp.Succeeded)
	require.NotNil(t, ignoreValueResp.Header)
	require.Greater(t, ignoreValueResp.Header.Revision, seed.Header.Revision)
	require.Len(t, ignoreValueResp.Responses, 1)

	ignoreValueNested := ignoreValueResp.Responses[0].GetResponseTxn()
	require.NotNil(t, ignoreValueNested)
	require.NotNil(t, ignoreValueNested.Header)
	require.Zero(t, ignoreValueNested.Header.Revision)
	require.True(t, ignoreValueNested.Succeeded)
	require.Len(t, ignoreValueNested.Responses, 2)

	ignoreValuePut := ignoreValueNested.Responses[0].GetResponsePut()
	require.NotNil(t, ignoreValuePut)
	require.NotNil(t, ignoreValuePut.Header)
	require.Equal(t, ignoreValueResp.Header.Revision, ignoreValuePut.Header.Revision)
	require.NotNil(t, ignoreValuePut.PrevKv)
	require.Equal(t, key, ignoreValuePut.PrevKv.Key)
	require.Equal(t, []byte("old"), ignoreValuePut.PrevKv.Value)
	require.Equal(t, seed.Header.Revision, ignoreValuePut.PrevKv.CreateRevision)
	require.Equal(t, seed.Header.Revision, ignoreValuePut.PrevKv.ModRevision)
	require.Equal(t, int64(1), ignoreValuePut.PrevKv.Version)
	require.Equal(t, leaseA.ID, ignoreValuePut.PrevKv.Lease)

	ignoreValueRange := ignoreValueNested.Responses[1].GetResponseRange()
	require.NotNil(t, ignoreValueRange)
	require.NotNil(t, ignoreValueRange.Header)
	require.Equal(t, ignoreValueResp.Header.Revision, ignoreValueRange.Header.Revision)
	require.Len(t, ignoreValueRange.Kvs, 1)
	require.Equal(t, key, ignoreValueRange.Kvs[0].Key)
	require.Equal(t, []byte("old"), ignoreValueRange.Kvs[0].Value)
	require.Equal(t, seed.Header.Revision, ignoreValueRange.Kvs[0].CreateRevision)
	require.Equal(t, ignoreValueResp.Header.Revision, ignoreValueRange.Kvs[0].ModRevision)
	require.Equal(t, int64(2), ignoreValueRange.Kvs[0].Version)
	require.Equal(t, leaseB.ID, ignoreValueRange.Kvs[0].Lease)

	ignoreLeaseResp, err := kv.Txn(ctx, &etcdserverpb.TxnRequest{
		Success: []*etcdserverpb.RequestOp{{
			Request: &etcdserverpb.RequestOp_RequestTxn{RequestTxn: &etcdserverpb.TxnRequest{
				Success: []*etcdserverpb.RequestOp{
					txnClientPutOp(&etcdserverpb.PutRequest{Key: key, Value: []byte("new"), IgnoreLease: true, PrevKv: true}),
					txnClientRangeOp(&etcdserverpb.RangeRequest{Key: key}),
				},
			}},
		}},
	})
	require.NoError(t, err)
	require.NotNil(t, ignoreLeaseResp)
	require.True(t, ignoreLeaseResp.Succeeded)
	require.NotNil(t, ignoreLeaseResp.Header)
	require.Greater(t, ignoreLeaseResp.Header.Revision, ignoreValueResp.Header.Revision)
	require.Len(t, ignoreLeaseResp.Responses, 1)

	ignoreLeaseNested := ignoreLeaseResp.Responses[0].GetResponseTxn()
	require.NotNil(t, ignoreLeaseNested)
	require.NotNil(t, ignoreLeaseNested.Header)
	require.Zero(t, ignoreLeaseNested.Header.Revision)
	require.True(t, ignoreLeaseNested.Succeeded)
	require.Len(t, ignoreLeaseNested.Responses, 2)

	ignoreLeasePut := ignoreLeaseNested.Responses[0].GetResponsePut()
	require.NotNil(t, ignoreLeasePut)
	require.NotNil(t, ignoreLeasePut.Header)
	require.Equal(t, ignoreLeaseResp.Header.Revision, ignoreLeasePut.Header.Revision)
	require.NotNil(t, ignoreLeasePut.PrevKv)
	require.Equal(t, key, ignoreLeasePut.PrevKv.Key)
	require.Equal(t, []byte("old"), ignoreLeasePut.PrevKv.Value)
	require.Equal(t, seed.Header.Revision, ignoreLeasePut.PrevKv.CreateRevision)
	require.Equal(t, ignoreValueResp.Header.Revision, ignoreLeasePut.PrevKv.ModRevision)
	require.Equal(t, int64(2), ignoreLeasePut.PrevKv.Version)
	require.Equal(t, leaseB.ID, ignoreLeasePut.PrevKv.Lease)

	ranged := ignoreLeaseNested.Responses[1].GetResponseRange()
	require.NotNil(t, ranged)
	require.NotNil(t, ranged.Header)
	require.Equal(t, ignoreLeaseResp.Header.Revision, ranged.Header.Revision)
	require.Len(t, ranged.Kvs, 1)
	require.Equal(t, key, ranged.Kvs[0].Key)
	require.Equal(t, []byte("new"), ranged.Kvs[0].Value)
	require.Equal(t, seed.Header.Revision, ranged.Kvs[0].CreateRevision)
	require.Equal(t, ignoreLeaseResp.Header.Revision, ranged.Kvs[0].ModRevision)
	require.Equal(t, int64(3), ranged.Kvs[0].Version)
	require.Equal(t, leaseB.ID, ranged.Kvs[0].Lease)
}

func TestRawGRPCTxnNestedDeleteRangePrevKVsResponseMatchesEtcd(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()

	grpcServer := grpc.NewServer(server.ClientServerOptions()...)
	etcdserverpb.RegisterKVServer(grpcServer, server)
	etcdserverpb.RegisterLeaseServer(grpcServer, server)
	listener := bufconn.Listen(1 << 20)
	go func() { _ = grpcServer.Serve(listener) }()
	t.Cleanup(grpcServer.Stop)

	conn, err := grpc.NewClient("passthrough:///txn-nested-delete-range-prev-kvs-client",
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
			return listener.Dial()
		}))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, conn.Close()) })
	kv := etcdserverpb.NewKVClient(conn)
	lease := etcdserverpb.NewLeaseClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	prefix := fmt.Sprintf("/a3019/txn-nested-delete-range-prev-kvs/%d/", time.Now().UnixNano())
	keyA := []byte(prefix + "a")
	keyB := []byte(prefix + "b")
	keyC := []byte(prefix + "c")
	keyD := []byte(prefix + "d")
	leaseA, err := lease.LeaseGrant(ctx, &etcdserverpb.LeaseGrantRequest{TTL: 300})
	require.NoError(t, err)
	leaseB, err := lease.LeaseGrant(ctx, &etcdserverpb.LeaseGrantRequest{TTL: 300})
	require.NoError(t, err)
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cleanupCancel()
		_, _ = lease.LeaseRevoke(cleanupCtx, &etcdserverpb.LeaseRevokeRequest{ID: leaseA.ID})
		_, _ = lease.LeaseRevoke(cleanupCtx, &etcdserverpb.LeaseRevokeRequest{ID: leaseB.ID})
	})
	seedA, err := kv.Put(ctx, &etcdserverpb.PutRequest{Key: keyA, Value: []byte("va")})
	require.NoError(t, err)
	seedB, err := kv.Put(ctx, &etcdserverpb.PutRequest{Key: keyB, Value: []byte("vb"), Lease: leaseA.ID})
	require.NoError(t, err)
	seedC, err := kv.Put(ctx, &etcdserverpb.PutRequest{Key: keyC, Value: []byte("vc"), Lease: leaseA.ID})
	require.NoError(t, err)
	updateC, err := kv.Put(ctx, &etcdserverpb.PutRequest{Key: keyC, Value: []byte("vc2"), Lease: leaseB.ID})
	require.NoError(t, err)
	seedD, err := kv.Put(ctx, &etcdserverpb.PutRequest{Key: keyD, Value: []byte("vd")})
	require.NoError(t, err)

	resp, err := kv.Txn(ctx, &etcdserverpb.TxnRequest{
		Success: []*etcdserverpb.RequestOp{{
			Request: &etcdserverpb.RequestOp_RequestTxn{RequestTxn: &etcdserverpb.TxnRequest{
				Success: []*etcdserverpb.RequestOp{
					txnClientDeleteOp(&etcdserverpb.DeleteRangeRequest{Key: keyB, RangeEnd: keyD, PrevKv: true}),
					txnClientRangeOp(&etcdserverpb.RangeRequest{Key: []byte(prefix), RangeEnd: []byte(clientv3.GetPrefixRangeEnd(prefix))}),
				},
			}},
		}},
	})
	require.NoError(t, err)
	require.NotNil(t, resp)
	require.True(t, resp.Succeeded)
	require.NotNil(t, resp.Header)
	require.Greater(t, resp.Header.Revision, seedD.Header.Revision)
	require.Len(t, resp.Responses, 1)

	nested := resp.Responses[0].GetResponseTxn()
	require.NotNil(t, nested)
	require.NotNil(t, nested.Header)
	require.Zero(t, nested.Header.Revision)
	require.True(t, nested.Succeeded)
	require.Len(t, nested.Responses, 2)

	deleted := nested.Responses[0].GetResponseDeleteRange()
	require.NotNil(t, deleted)
	require.NotNil(t, deleted.Header)
	require.Equal(t, resp.Header.Revision, deleted.Header.Revision)
	require.Equal(t, int64(2), deleted.Deleted)
	require.Len(t, deleted.PrevKvs, 2)
	require.Equal(t, keyB, deleted.PrevKvs[0].Key)
	require.Equal(t, []byte("vb"), deleted.PrevKvs[0].Value)
	require.Equal(t, seedB.Header.Revision, deleted.PrevKvs[0].CreateRevision)
	require.Equal(t, seedB.Header.Revision, deleted.PrevKvs[0].ModRevision)
	require.Equal(t, int64(1), deleted.PrevKvs[0].Version)
	require.Equal(t, leaseA.ID, deleted.PrevKvs[0].Lease)
	require.Equal(t, keyC, deleted.PrevKvs[1].Key)
	require.Equal(t, []byte("vc2"), deleted.PrevKvs[1].Value)
	require.Equal(t, seedC.Header.Revision, deleted.PrevKvs[1].CreateRevision)
	require.Equal(t, updateC.Header.Revision, deleted.PrevKvs[1].ModRevision)
	require.Equal(t, int64(2), deleted.PrevKvs[1].Version)
	require.Equal(t, leaseB.ID, deleted.PrevKvs[1].Lease)

	ranged := nested.Responses[1].GetResponseRange()
	require.NotNil(t, ranged)
	require.NotNil(t, ranged.Header)
	require.Equal(t, resp.Header.Revision, ranged.Header.Revision)
	require.Len(t, ranged.Kvs, 2)
	require.Equal(t, keyA, ranged.Kvs[0].Key)
	require.Equal(t, []byte("va"), ranged.Kvs[0].Value)
	require.Equal(t, seedA.Header.Revision, ranged.Kvs[0].CreateRevision)
	require.Equal(t, seedA.Header.Revision, ranged.Kvs[0].ModRevision)
	require.Equal(t, int64(1), ranged.Kvs[0].Version)
	require.Equal(t, keyD, ranged.Kvs[1].Key)
	require.Equal(t, []byte("vd"), ranged.Kvs[1].Value)
	require.Equal(t, seedD.Header.Revision, ranged.Kvs[1].CreateRevision)
	require.Equal(t, seedD.Header.Revision, ranged.Kvs[1].ModRevision)
	require.Equal(t, int64(1), ranged.Kvs[1].Version)

	final, err := kv.Range(ctx, &etcdserverpb.RangeRequest{Key: []byte(prefix), RangeEnd: []byte(clientv3.GetPrefixRangeEnd(prefix))})
	require.NoError(t, err)
	require.NotNil(t, final.Header)
	require.GreaterOrEqual(t, final.Header.Revision, resp.Header.Revision)
	require.Len(t, final.Kvs, 2)
	require.Equal(t, keyA, final.Kvs[0].Key)
	require.Equal(t, keyD, final.Kvs[1].Key)
}

func TestClientTxnNestedDuplicateIntervalValidation(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()

	grpcServer := grpc.NewServer(server.ClientServerOptions()...)
	etcdserverpb.RegisterKVServer(grpcServer, server)
	listener := bufconn.Listen(1 << 20)
	go func() { _ = grpcServer.Serve(listener) }()
	t.Cleanup(grpcServer.Stop)

	client, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{"bufnet"},
		DialTimeout: time.Second,
		DialOptions: []grpc.DialOption{
			grpc.WithTransportCredentials(insecure.NewCredentials()),
			grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
				return listener.Dial()
			}),
		},
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, client.Close()) })

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	prefix := fmt.Sprintf("/a1040/txn-duplicate-interval/%d/", time.Now().UnixNano())
	key := prefix + "abc"
	deleteContaining := clientv3.OpDelete(prefix+"a", clientv3.WithRange(prefix+"b"))
	nestedDelete := clientv3.OpTxn(nil, []clientv3.Op{deleteContaining}, nil)
	nestedPut := clientv3.OpTxn(nil, []clientv3.Op{clientv3.OpPut(key, "value")}, nil)

	tests := []struct {
		name    string
		ops     []clientv3.Op
		wantErr bool
	}{
		{name: "duplicate-put", ops: []clientv3.Op{clientv3.OpPut(key, "one"), clientv3.OpPut(key, "two")}, wantErr: true},
		{name: "put-and-containing-delete", ops: []clientv3.Op{clientv3.OpPut(key, "value"), deleteContaining}, wantErr: true},
		{name: "put-and-nested-containing-delete", ops: []clientv3.Op{clientv3.OpPut(key, "value"), nestedDelete}, wantErr: true},
		{name: "containing-delete-and-nested-put", ops: []clientv3.Op{deleteContaining, nestedPut}, wantErr: true},
		{name: "put-and-disjoint-delete", ops: []clientv3.Op{
			clientv3.OpPut(key, "value"),
			clientv3.OpDelete(prefix+"abb", clientv3.WithRange(key)),
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			txn, txnErr := client.Txn(ctx).Then(tt.ops...).Commit()
			if tt.wantErr {
				requireClientTxnError(t, txnErr, codes.Unknown, "etcdserver: duplicate key given in txn request")
				require.Nil(t, txn)
				return
			}
			require.NoError(t, txnErr)
			require.NotNil(t, txn)
			require.True(t, txn.Succeeded)
		})
	}
}

func TestClientTxnCrossKeyFastShapeMatchesEtcd(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()

	grpcServer := grpc.NewServer(server.ClientServerOptions()...)
	etcdserverpb.RegisterKVServer(grpcServer, server)
	listener := bufconn.Listen(1 << 20)
	go func() { _ = grpcServer.Serve(listener) }()
	t.Cleanup(grpcServer.Stop)

	client, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{"bufnet"},
		DialTimeout: time.Second,
		DialOptions: []grpc.DialOption{
			grpc.WithTransportCredentials(insecure.NewCredentials()),
			grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
				return listener.Dial()
			}),
		},
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, client.Close()) })

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	prefix := fmt.Sprintf("/a1055/client-txn-cross-key/%d/", time.Now().UnixNano())

	createGuard := prefix + "create-guard"
	createTarget := prefix + "create-target"
	_, err = client.Put(ctx, createTarget, "old")
	require.NoError(t, err)
	createTxn, err := client.Txn(ctx).
		If(clientv3.Compare(clientv3.ModRevision(createGuard), "=", 0)).
		Then(clientv3.OpPut(createTarget, "updated")).
		Commit()
	require.NoError(t, err)
	require.True(t, createTxn.Succeeded)
	created, err := client.Get(ctx, createTarget)
	require.NoError(t, err)
	require.Len(t, created.Kvs, 1)
	require.Equal(t, "updated", string(created.Kvs[0].Value))
	require.Equal(t, int64(2), created.Kvs[0].Version)

	updateGuard := prefix + "update-guard"
	updateTarget := prefix + "update-target"
	guard, err := client.Put(ctx, updateGuard, "guard")
	require.NoError(t, err)
	updateTxn, err := client.Txn(ctx).
		If(clientv3.Compare(clientv3.ModRevision(updateGuard), "=", guard.Header.Revision)).
		Then(clientv3.OpPut(updateTarget, "created")).
		Commit()
	require.NoError(t, err)
	require.True(t, updateTxn.Succeeded)
	updated, err := client.Get(ctx, updateTarget)
	require.NoError(t, err)
	require.Len(t, updated.Kvs, 1)
	require.Equal(t, "created", string(updated.Kvs[0].Value))

	deleteGuard := prefix + "delete-guard"
	deleteTarget := prefix + "delete-target"
	guard, err = client.Put(ctx, deleteGuard, "guard")
	require.NoError(t, err)
	_, err = client.Put(ctx, deleteTarget, "target")
	require.NoError(t, err)
	deleteTxn, err := client.Txn(ctx).
		If(clientv3.Compare(clientv3.ModRevision(deleteGuard), "=", guard.Header.Revision)).
		Then(clientv3.OpDelete(deleteTarget)).
		Commit()
	require.NoError(t, err)
	require.True(t, deleteTxn.Succeeded)
	require.Len(t, deleteTxn.Responses, 1)
	require.Equal(t, int64(1), deleteTxn.Responses[0].GetResponseDeleteRange().Deleted)
	deleted, err := client.Get(ctx, deleteTarget)
	require.NoError(t, err)
	require.Empty(t, deleted.Kvs)

	staleGuard := prefix + "stale-guard"
	staleTarget := prefix + "stale-target"
	guard, err = client.Put(ctx, staleGuard, "guard")
	require.NoError(t, err)
	_, err = client.Delete(ctx, staleGuard)
	require.NoError(t, err)
	staleTxn, err := client.Txn(ctx).
		If(clientv3.Compare(clientv3.ModRevision(staleGuard), "=", guard.Header.Revision)).
		Then(clientv3.OpPut(staleTarget, "must-not-create")).
		Commit()
	require.NoError(t, err)
	require.False(t, staleTxn.Succeeded)
	stale, err := client.Get(ctx, staleTarget)
	require.NoError(t, err)
	require.Empty(t, stale.Kvs)
}

func TestClientTxnPrefixCompareRequiresAllKeysToMatch(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()

	grpcServer := grpc.NewServer(server.ClientServerOptions()...)
	etcdserverpb.RegisterKVServer(grpcServer, server)
	listener := bufconn.Listen(1 << 20)
	go func() { _ = grpcServer.Serve(listener) }()
	t.Cleanup(grpcServer.Stop)

	client, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{"bufnet"},
		DialTimeout: time.Second,
		DialOptions: []grpc.DialOption{
			grpc.WithTransportCredentials(insecure.NewCredentials()),
			grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
				return listener.Dial()
			}),
		},
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, client.Close()) })

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	prefix := fmt.Sprintf("/a1139/txn-prefix-compare/%d/foo/", time.Now().UnixNano())
	foo, err := client.Put(ctx, prefix, "bar")
	require.NoError(t, err)
	_, err = client.Put(ctx, prefix+"a", "baz")
	require.NoError(t, err)

	txn, err := client.Txn(ctx).
		If(clientv3.Compare(clientv3.CreateRevision(prefix), "=", foo.Header.Revision).WithPrefix()).
		Then(clientv3.OpPut(prefix+"result", "success")).
		Else(clientv3.OpPut(prefix+"result", "failure")).
		Commit()
	require.NoError(t, err)
	require.False(t, txn.Succeeded)
	require.NotNil(t, txn.Header)
	require.Positive(t, txn.Header.Revision)
	require.Len(t, txn.Responses, 1)
	failurePut := txn.Responses[0].GetResponsePut()
	require.NotNil(t, failurePut)
	require.NotNil(t, failurePut.Header)
	require.Equal(t, txn.Header.Revision, failurePut.Header.Revision)

	result, err := client.Get(ctx, prefix+"result")
	require.NoError(t, err)
	require.NotNil(t, result.Header)
	require.Equal(t, txn.Header.Revision, result.Header.Revision)
	require.Len(t, result.Kvs, 1)
	require.Equal(t, "failure", string(result.Kvs[0].Value))
	require.Equal(t, txn.Header.Revision, result.Kvs[0].CreateRevision)
	require.Equal(t, txn.Header.Revision, result.Kvs[0].ModRevision)
	require.Equal(t, int64(1), result.Kvs[0].Version)
}

func TestClientNestedTxnResponseAndFinalState(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()

	grpcServer := grpc.NewServer(server.ClientServerOptions()...)
	etcdserverpb.RegisterKVServer(grpcServer, server)
	listener := bufconn.Listen(1 << 20)
	go func() { _ = grpcServer.Serve(listener) }()
	t.Cleanup(grpcServer.Stop)

	client, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{"bufnet"},
		DialTimeout: time.Second,
		DialOptions: []grpc.DialOption{
			grpc.WithTransportCredentials(insecure.NewCredentials()),
			grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
				return listener.Dial()
			}),
		},
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, client.Close()) })

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	for _, tt := range []struct {
		name                  string
		outerValue            string
		cValue                string
		wantOuterSucceeded    bool
		wantMiddle            bool
		wantInnerSucceeded    bool
		wantFinalRelativeKeys []string
	}{
		{
			name:                  "outer-then-middle-then-inner-failure",
			outerValue:            "yes",
			cValue:                "other",
			wantOuterSucceeded:    true,
			wantMiddle:            true,
			wantFinalRelativeKeys: []string{"a", "b", "ctrl", "middle-put"},
		},
		{
			name:                  "outer-else-failure-txn-success",
			outerValue:            "no",
			cValue:                "inner",
			wantOuterSucceeded:    false,
			wantInnerSucceeded:    true,
			wantFinalRelativeKeys: []string{"a", "b", "c", "ctrl", "failure-put"},
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			prefix := fmt.Sprintf("/a1008/client-nested-txn/%d/%s/", time.Now().UnixNano(), tt.name)
			ctrl, a, b, c := prefix+"ctrl", prefix+"a", prefix+"b", prefix+"c"
			_, err = client.Txn(ctx).Then(
				clientv3.OpPut(ctrl, tt.outerValue),
				clientv3.OpPut(a, "middle"),
				clientv3.OpPut(b, "seed-b"),
				clientv3.OpPut(c, tt.cValue),
			).Commit()
			require.NoError(t, err)

			inner := clientv3.OpTxn(
				[]clientv3.Cmp{clientv3.Compare(clientv3.Value(c), "=", "inner")},
				[]clientv3.Op{
					clientv3.OpPut(prefix+"inner-put", "inner-value", clientv3.WithPrevKV()),
					clientv3.OpGet(prefix, clientv3.WithPrefix()),
				},
				[]clientv3.Op{clientv3.OpDelete(c, clientv3.WithPrevKV())},
			)
			middle := clientv3.OpTxn(
				[]clientv3.Cmp{clientv3.Compare(clientv3.Value(a), "=", "middle")},
				[]clientv3.Op{
					inner,
					clientv3.OpPut(prefix+"middle-put", "middle-value", clientv3.WithPrevKV()),
				},
				[]clientv3.Op{
					clientv3.OpDelete(b, clientv3.WithPrevKV()),
					clientv3.OpGet(prefix, clientv3.WithPrefix()),
				},
			)
			failure := clientv3.OpTxn(
				[]clientv3.Cmp{clientv3.Compare(clientv3.Version(prefix+"missing"), "=", 0)},
				[]clientv3.Op{
					clientv3.OpPut(prefix+"failure-put", "failure-value", clientv3.WithPrevKV()),
					clientv3.OpGet(b),
				},
				[]clientv3.Op{clientv3.OpDelete(a, clientv3.WithPrevKV())},
			)
			txn, err := client.Txn(ctx).
				If(clientv3.Compare(clientv3.Value(ctrl), "=", "yes")).
				Then(middle, clientv3.OpGet(prefix, clientv3.WithPrefix())).
				Else(failure, clientv3.OpGet(prefix, clientv3.WithPrefix())).
				Commit()
			require.NoError(t, err)
			require.Equal(t, tt.wantOuterSucceeded, txn.Succeeded)
			require.Len(t, txn.Responses, 2)

			nested := txn.Responses[0].GetResponseTxn()
			require.NotNil(t, nested)
			require.NotNil(t, nested.Header)
			require.Zero(t, nested.Header.Revision)
			require.NotZero(t, txn.Header.Revision)
			if tt.wantMiddle {
				require.True(t, nested.Succeeded)
				require.Len(t, nested.Responses, 2)
				innerResponse := nested.Responses[0].GetResponseTxn()
				require.NotNil(t, innerResponse)
				require.NotNil(t, innerResponse.Header)
				require.Zero(t, innerResponse.Header.Revision)
				require.Equal(t, tt.wantInnerSucceeded, innerResponse.Succeeded)
				require.Len(t, innerResponse.Responses, 1)
				deleted := innerResponse.Responses[0].GetResponseDeleteRange()
				require.NotNil(t, deleted)
				require.NotNil(t, deleted.Header)
				require.Equal(t, txn.Header.Revision, deleted.Header.Revision)
				require.Equal(t, int64(1), deleted.Deleted)
				require.Equal(t, []txnClientKV{{Key: "c", Value: tt.cValue, Version: 1}}, txnClientKVs(deleted.PrevKvs, prefix, txn.Header.Revision))
				put := nested.Responses[1].GetResponsePut()
				require.NotNil(t, put)
				require.NotNil(t, put.Header)
				require.Equal(t, txn.Header.Revision, put.Header.Revision)
				require.Nil(t, put.PrevKv)
			} else {
				require.Equal(t, tt.wantInnerSucceeded, nested.Succeeded)
				require.Len(t, nested.Responses, 2)
				put := nested.Responses[0].GetResponsePut()
				require.NotNil(t, put)
				require.NotNil(t, put.Header)
				require.Equal(t, txn.Header.Revision, put.Header.Revision)
				require.Nil(t, put.PrevKv)
				gotB := nested.Responses[1].GetResponseRange()
				require.NotNil(t, gotB)
				require.NotNil(t, gotB.Header)
				require.Equal(t, txn.Header.Revision, gotB.Header.Revision)
				require.Equal(t, []txnClientKV{{Key: "b", Value: "seed-b", Version: 1}}, txnClientKVs(gotB.Kvs, prefix, txn.Header.Revision))
			}
			rangeResponse := txn.Responses[1].GetResponseRange()
			require.NotNil(t, rangeResponse)
			require.Equal(t, txn.Header.Revision, rangeResponse.Header.Revision)
			require.Equal(t, tt.wantFinalRelativeKeys, txnClientKVKeys(txnClientKVs(rangeResponse.Kvs, prefix, txn.Header.Revision)))

			final, err := client.Get(ctx, prefix, clientv3.WithPrefix(), clientv3.WithSort(clientv3.SortByKey, clientv3.SortAscend))
			require.NoError(t, err)
			require.GreaterOrEqual(t, final.Header.Revision, txn.Header.Revision)
			require.Equal(t, tt.wantFinalRelativeKeys, txnClientKVKeys(txnClientKVs(final.Kvs, prefix, txn.Header.Revision)))
		})
	}
}

func TestClientTxnIgnoreLeaseAndBadLeaseBranches(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()

	grpcServer := grpc.NewServer(server.ClientServerOptions()...)
	etcdserverpb.RegisterKVServer(grpcServer, server)
	etcdserverpb.RegisterLeaseServer(grpcServer, server)
	listener := bufconn.Listen(1 << 20)
	go func() { _ = grpcServer.Serve(listener) }()
	t.Cleanup(grpcServer.Stop)

	client, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{"bufnet"},
		DialTimeout: time.Second,
		DialOptions: []grpc.DialOption{
			grpc.WithTransportCredentials(insecure.NewCredentials()),
			grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
				return listener.Dial()
			}),
		},
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, client.Close()) })

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	key := fmt.Sprintf("/a1009/client-txn-ignore-lease/%d", time.Now().UnixNano())
	leaseA, err := client.Grant(ctx, 300)
	require.NoError(t, err)
	leaseB, err := client.Grant(ctx, 300)
	require.NoError(t, err)
	revokedA, revokedB := false, false
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cleanupCancel()
		if !revokedA {
			_, _ = client.Revoke(cleanupCtx, leaseA.ID)
		}
		if !revokedB {
			_, _ = client.Revoke(cleanupCtx, leaseB.ID)
		}
	})

	_, err = client.Put(ctx, key, "old", clientv3.WithLease(leaseA.ID))
	require.NoError(t, err)
	ignoreValue, err := client.Txn(ctx).Then(
		clientv3.OpPut(key, "", clientv3.WithIgnoreValue(), clientv3.WithLease(leaseB.ID), clientv3.WithPrevKV()),
		clientv3.OpGet(key),
	).Commit()
	require.NoError(t, err)
	require.True(t, ignoreValue.Succeeded)
	require.NotNil(t, ignoreValue.Header)
	require.Len(t, ignoreValue.Responses, 2)
	ignoreValuePut := ignoreValue.Responses[0].GetResponsePut()
	require.NotNil(t, ignoreValuePut)
	require.NotNil(t, ignoreValuePut.Header)
	require.Equal(t, ignoreValue.Header.Revision, ignoreValuePut.Header.Revision)
	ignoreValuePrev := ignoreValuePut.PrevKv
	require.NotNil(t, ignoreValuePrev)
	require.Equal(t, "old", string(ignoreValuePrev.Value))
	require.Equal(t, int64(leaseA.ID), ignoreValuePrev.Lease)
	staged := ignoreValue.Responses[1].GetResponseRange()
	require.NotNil(t, staged)
	require.NotNil(t, staged.Header)
	require.Equal(t, ignoreValue.Header.Revision, staged.Header.Revision)
	require.Len(t, staged.Kvs, 1)
	require.Equal(t, "old", string(staged.Kvs[0].Value))
	require.Equal(t, int64(leaseB.ID), staged.Kvs[0].Lease)

	ignoreLease, err := client.Txn(ctx).Then(
		clientv3.OpPut(key, "new", clientv3.WithIgnoreLease(), clientv3.WithPrevKV()),
	).Commit()
	require.NoError(t, err)
	require.True(t, ignoreLease.Succeeded)
	require.NotNil(t, ignoreLease.Header)
	require.Len(t, ignoreLease.Responses, 1)
	ignoreLeasePut := ignoreLease.Responses[0].GetResponsePut()
	require.NotNil(t, ignoreLeasePut)
	require.NotNil(t, ignoreLeasePut.Header)
	require.Equal(t, ignoreLease.Header.Revision, ignoreLeasePut.Header.Revision)
	ignoreLeasePrev := ignoreLeasePut.PrevKv
	require.NotNil(t, ignoreLeasePrev)
	require.Equal(t, "old", string(ignoreLeasePrev.Value))
	require.Equal(t, int64(leaseB.ID), ignoreLeasePrev.Lease)
	final, err := client.Get(ctx, key)
	require.NoError(t, err)
	require.NotNil(t, final.Header)
	require.Equal(t, ignoreLease.Header.Revision, final.Header.Revision)
	require.Len(t, final.Kvs, 1)
	require.Equal(t, "new", string(final.Kvs[0].Value))
	require.Equal(t, int64(leaseB.ID), final.Kvs[0].Lease)

	_, err = client.Revoke(ctx, leaseA.ID)
	require.NoError(t, err)
	revokedA = true
	afterA, err := client.Get(ctx, key)
	require.NoError(t, err)
	require.Len(t, afterA.Kvs, 1)
	require.Equal(t, int64(leaseB.ID), afterA.Kvs[0].Lease)
	_, err = client.Revoke(ctx, leaseB.ID)
	require.NoError(t, err)
	revokedB = true
	afterB, err := client.Get(ctx, key)
	require.NoError(t, err)
	require.Empty(t, afterB.Kvs)

	branchKey := key + "/branch"
	unselectedBadLease, err := client.Txn(ctx).
		If(clientv3.Compare(clientv3.Version(branchKey), "=", 0)).
		Then(clientv3.OpPut(branchKey, "valid")).
		Else(clientv3.OpPut(branchKey, "invalid", clientv3.WithLease(clientv3.LeaseID(math.MaxInt64)))).
		Commit()
	require.NoError(t, err)
	require.True(t, unselectedBadLease.Succeeded)
	_, err = client.Delete(ctx, branchKey)
	require.NoError(t, err)
	_, selectedBadLeaseErr := client.Txn(ctx).
		If(clientv3.Compare(clientv3.Version(branchKey), ">", 0)).
		Then(clientv3.OpPut(branchKey, "valid")).
		Else(clientv3.OpPut(branchKey, "invalid", clientv3.WithLease(clientv3.LeaseID(math.MaxInt64)))).
		Commit()
	requireClientTxnError(t, selectedBadLeaseErr, codes.Unknown, "etcdserver: requested lease not found", rpctypes.ErrLeaseNotFound)
	afterBadLease, err := client.Get(ctx, branchKey)
	require.NoError(t, err)
	require.Empty(t, afterBadLease.Kvs)
}

func TestClientTxnDeleteFromKeyWithLeasesPrevKVMatchesEtcd(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()

	grpcServer := grpc.NewServer(server.ClientServerOptions()...)
	etcdserverpb.RegisterKVServer(grpcServer, server)
	etcdserverpb.RegisterLeaseServer(grpcServer, server)
	listener := bufconn.Listen(1 << 20)
	go func() { _ = grpcServer.Serve(listener) }()
	t.Cleanup(grpcServer.Stop)

	client, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{"bufnet"},
		DialTimeout: time.Second,
		DialOptions: []grpc.DialOption{
			grpc.WithTransportCredentials(insecure.NewCredentials()),
			grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
				return listener.Dial()
			}),
		},
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, client.Close()) })

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	leaseA, err := client.Grant(ctx, 300)
	require.NoError(t, err)
	leaseB, err := client.Grant(ctx, 300)
	require.NoError(t, err)
	prefix := "/a2153/client-txn-delete-fromkey-leased/"
	before, err := client.Put(ctx, prefix+"a", "va", clientv3.WithLease(leaseA.ID))
	require.NoError(t, err)
	putB, err := client.Put(ctx, prefix+"b", "vb", clientv3.WithLease(leaseA.ID))
	require.NoError(t, err)
	putC, err := client.Put(ctx, prefix+"c", "vc", clientv3.WithLease(leaseB.ID))
	require.NoError(t, err)
	putD, err := client.Put(ctx, prefix+"d", "vd", clientv3.WithLease(leaseA.ID))
	require.NoError(t, err)
	updateC, err := client.Put(ctx, prefix+"c", "vc2", clientv3.WithLease(leaseB.ID))
	require.NoError(t, err)

	ttlABefore, err := client.TimeToLive(ctx, leaseA.ID, clientv3.WithAttachedKeys())
	require.NoError(t, err)
	require.ElementsMatch(t, []string{prefix + "a", prefix + "b", prefix + "d"}, leaseClientAttachedKeys(ttlABefore.Keys))
	ttlBBefore, err := client.TimeToLive(ctx, leaseB.ID, clientv3.WithAttachedKeys())
	require.NoError(t, err)
	require.Equal(t, []string{prefix + "c"}, leaseClientAttachedKeys(ttlBBefore.Keys))

	txn, err := client.Txn(ctx).Then(
		clientv3.OpDelete(prefix+"b", clientv3.WithFromKey(), clientv3.WithPrevKV()),
		clientv3.OpGet(prefix, clientv3.WithPrefix(), clientv3.WithSort(clientv3.SortByKey, clientv3.SortAscend)),
	).Commit()
	require.NoError(t, err)
	require.True(t, txn.Succeeded)
	require.NotNil(t, txn.Header)
	require.Equal(t, updateC.Header.Revision+1, txn.Header.Revision)
	require.Len(t, txn.Responses, 2)

	deleted := txn.Responses[0].GetResponseDeleteRange()
	require.NotNil(t, deleted)
	require.NotNil(t, deleted.Header)
	require.Equal(t, txn.Header.Revision, deleted.Header.Revision)
	require.Equal(t, int64(3), deleted.Deleted)
	require.Len(t, deleted.PrevKvs, 3)
	require.Equal(t, []byte(prefix+"b"), deleted.PrevKvs[0].Key)
	require.Equal(t, []byte("vb"), deleted.PrevKvs[0].Value)
	require.Equal(t, putB.Header.Revision, deleted.PrevKvs[0].CreateRevision)
	require.Equal(t, putB.Header.Revision, deleted.PrevKvs[0].ModRevision)
	require.Equal(t, int64(1), deleted.PrevKvs[0].Version)
	require.Equal(t, int64(leaseA.ID), deleted.PrevKvs[0].Lease)
	require.Equal(t, []byte(prefix+"c"), deleted.PrevKvs[1].Key)
	require.Equal(t, []byte("vc2"), deleted.PrevKvs[1].Value)
	require.Equal(t, putC.Header.Revision, deleted.PrevKvs[1].CreateRevision)
	require.Equal(t, updateC.Header.Revision, deleted.PrevKvs[1].ModRevision)
	require.Equal(t, int64(2), deleted.PrevKvs[1].Version)
	require.Equal(t, int64(leaseB.ID), deleted.PrevKvs[1].Lease)
	require.Equal(t, []byte(prefix+"d"), deleted.PrevKvs[2].Key)
	require.Equal(t, []byte("vd"), deleted.PrevKvs[2].Value)
	require.Equal(t, putD.Header.Revision, deleted.PrevKvs[2].CreateRevision)
	require.Equal(t, putD.Header.Revision, deleted.PrevKvs[2].ModRevision)
	require.Equal(t, int64(1), deleted.PrevKvs[2].Version)
	require.Equal(t, int64(leaseA.ID), deleted.PrevKvs[2].Lease)

	staged := txn.Responses[1].GetResponseRange()
	require.NotNil(t, staged)
	require.NotNil(t, staged.Header)
	require.Equal(t, txn.Header.Revision, staged.Header.Revision)
	require.Equal(t, []txnClientKV{
		{Key: "a", Value: "va", Version: 1},
	}, txnClientKVs(staged.Kvs, prefix, txn.Header.Revision))
	require.Equal(t, int64(leaseA.ID), staged.Kvs[0].Lease)

	ttlAAfter, err := client.TimeToLive(ctx, leaseA.ID, clientv3.WithAttachedKeys())
	require.NoError(t, err)
	require.Equal(t, []string{prefix + "a"}, leaseClientAttachedKeys(ttlAAfter.Keys))
	ttlBAfter, err := client.TimeToLive(ctx, leaseB.ID, clientv3.WithAttachedKeys())
	require.NoError(t, err)
	require.Empty(t, ttlBAfter.Keys)

	historical, err := client.Get(ctx, prefix,
		clientv3.WithPrefix(),
		clientv3.WithRev(updateC.Header.Revision),
		clientv3.WithSort(clientv3.SortByKey, clientv3.SortAscend),
	)
	require.NoError(t, err)
	require.Equal(t, []txnClientKV{
		{Key: "a", Value: "va", Version: 1},
		{Key: "b", Value: "vb", Version: 1},
		{Key: "c", Value: "vc2", Version: 2},
		{Key: "d", Value: "vd", Version: 1},
	}, txnClientKVs(historical.Kvs, prefix, txn.Header.Revision))
	require.Equal(t, int64(leaseA.ID), historical.Kvs[0].Lease)
	require.Equal(t, int64(leaseA.ID), historical.Kvs[1].Lease)
	require.Equal(t, int64(leaseB.ID), historical.Kvs[2].Lease)
	require.Equal(t, int64(leaseA.ID), historical.Kvs[3].Lease)
	require.Equal(t, before.Header.Revision, historical.Kvs[0].CreateRevision)
}

func TestClientTxnDeleteRangeWithLeasesPrevKVMatchesEtcd(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()

	grpcServer := grpc.NewServer(server.ClientServerOptions()...)
	etcdserverpb.RegisterKVServer(grpcServer, server)
	etcdserverpb.RegisterLeaseServer(grpcServer, server)
	listener := bufconn.Listen(1 << 20)
	go func() { _ = grpcServer.Serve(listener) }()
	t.Cleanup(grpcServer.Stop)

	client, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{"bufnet"},
		DialTimeout: time.Second,
		DialOptions: []grpc.DialOption{
			grpc.WithTransportCredentials(insecure.NewCredentials()),
			grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
				return listener.Dial()
			}),
		},
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, client.Close()) })

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	leaseA, err := client.Grant(ctx, 300)
	require.NoError(t, err)
	leaseB, err := client.Grant(ctx, 300)
	require.NoError(t, err)
	prefix := "/a2154/client-txn-delete-range-leased/"
	putA, err := client.Put(ctx, prefix+"a", "va", clientv3.WithLease(leaseA.ID))
	require.NoError(t, err)
	putB, err := client.Put(ctx, prefix+"b", "vb", clientv3.WithLease(leaseB.ID))
	require.NoError(t, err)
	putC, err := client.Put(ctx, prefix+"c", "vc", clientv3.WithLease(leaseA.ID))
	require.NoError(t, err)
	updateB, err := client.Put(ctx, prefix+"b", "vb2", clientv3.WithLease(leaseB.ID))
	require.NoError(t, err)

	ttlABefore, err := client.TimeToLive(ctx, leaseA.ID, clientv3.WithAttachedKeys())
	require.NoError(t, err)
	require.ElementsMatch(t, []string{prefix + "a", prefix + "c"}, leaseClientAttachedKeys(ttlABefore.Keys))
	ttlBBefore, err := client.TimeToLive(ctx, leaseB.ID, clientv3.WithAttachedKeys())
	require.NoError(t, err)
	require.Equal(t, []string{prefix + "b"}, leaseClientAttachedKeys(ttlBBefore.Keys))

	txn, err := client.Txn(ctx).Then(
		clientv3.OpDelete(prefix+"a", clientv3.WithRange(prefix+"c"), clientv3.WithPrevKV()),
		clientv3.OpGet(prefix, clientv3.WithPrefix(), clientv3.WithSort(clientv3.SortByKey, clientv3.SortAscend)),
	).Commit()
	require.NoError(t, err)
	require.True(t, txn.Succeeded)
	require.NotNil(t, txn.Header)
	require.Equal(t, updateB.Header.Revision+1, txn.Header.Revision)
	require.Len(t, txn.Responses, 2)

	deleted := txn.Responses[0].GetResponseDeleteRange()
	require.NotNil(t, deleted)
	require.NotNil(t, deleted.Header)
	require.Equal(t, txn.Header.Revision, deleted.Header.Revision)
	require.Equal(t, int64(2), deleted.Deleted)
	require.Len(t, deleted.PrevKvs, 2)
	require.Equal(t, []byte(prefix+"a"), deleted.PrevKvs[0].Key)
	require.Equal(t, []byte("va"), deleted.PrevKvs[0].Value)
	require.Equal(t, putA.Header.Revision, deleted.PrevKvs[0].CreateRevision)
	require.Equal(t, putA.Header.Revision, deleted.PrevKvs[0].ModRevision)
	require.Equal(t, int64(1), deleted.PrevKvs[0].Version)
	require.Equal(t, int64(leaseA.ID), deleted.PrevKvs[0].Lease)
	require.Equal(t, []byte(prefix+"b"), deleted.PrevKvs[1].Key)
	require.Equal(t, []byte("vb2"), deleted.PrevKvs[1].Value)
	require.Equal(t, putB.Header.Revision, deleted.PrevKvs[1].CreateRevision)
	require.Equal(t, updateB.Header.Revision, deleted.PrevKvs[1].ModRevision)
	require.Equal(t, int64(2), deleted.PrevKvs[1].Version)
	require.Equal(t, int64(leaseB.ID), deleted.PrevKvs[1].Lease)

	staged := txn.Responses[1].GetResponseRange()
	require.NotNil(t, staged)
	require.NotNil(t, staged.Header)
	require.Equal(t, txn.Header.Revision, staged.Header.Revision)
	require.Equal(t, []txnClientKV{
		{Key: "c", Value: "vc", Version: 1},
	}, txnClientKVs(staged.Kvs, prefix, txn.Header.Revision))
	require.Equal(t, int64(leaseA.ID), staged.Kvs[0].Lease)

	ttlAAfter, err := client.TimeToLive(ctx, leaseA.ID, clientv3.WithAttachedKeys())
	require.NoError(t, err)
	require.Equal(t, []string{prefix + "c"}, leaseClientAttachedKeys(ttlAAfter.Keys))
	ttlBAfter, err := client.TimeToLive(ctx, leaseB.ID, clientv3.WithAttachedKeys())
	require.NoError(t, err)
	require.Empty(t, ttlBAfter.Keys)

	historical, err := client.Get(ctx, prefix,
		clientv3.WithPrefix(),
		clientv3.WithRev(updateB.Header.Revision),
		clientv3.WithSort(clientv3.SortByKey, clientv3.SortAscend),
	)
	require.NoError(t, err)
	require.Equal(t, []txnClientKV{
		{Key: "a", Value: "va", Version: 1},
		{Key: "b", Value: "vb2", Version: 2},
		{Key: "c", Value: "vc", Version: 1},
	}, txnClientKVs(historical.Kvs, prefix, txn.Header.Revision))
	require.Equal(t, int64(leaseA.ID), historical.Kvs[0].Lease)
	require.Equal(t, int64(leaseB.ID), historical.Kvs[1].Lease)
	require.Equal(t, int64(leaseA.ID), historical.Kvs[2].Lease)
	require.Equal(t, putC.Header.Revision, historical.Kvs[2].CreateRevision)
}

func TestClientTxnDeletePrefixWithLeasesPrevKVMatchesEtcd(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()

	grpcServer := grpc.NewServer(server.ClientServerOptions()...)
	etcdserverpb.RegisterKVServer(grpcServer, server)
	etcdserverpb.RegisterLeaseServer(grpcServer, server)
	listener := bufconn.Listen(1 << 20)
	go func() { _ = grpcServer.Serve(listener) }()
	t.Cleanup(grpcServer.Stop)

	client, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{"bufnet"},
		DialTimeout: time.Second,
		DialOptions: []grpc.DialOption{
			grpc.WithTransportCredentials(insecure.NewCredentials()),
			grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
				return listener.Dial()
			}),
		},
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, client.Close()) })

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	leaseA, err := client.Grant(ctx, 300)
	require.NoError(t, err)
	leaseB, err := client.Grant(ctx, 300)
	require.NoError(t, err)
	root := "/a2155/client-txn-delete-prefix-leased/"
	prefix := root + "items/"
	putA, err := client.Put(ctx, prefix+"a", "va", clientv3.WithLease(leaseA.ID))
	require.NoError(t, err)
	putB, err := client.Put(ctx, prefix+"b", "vb", clientv3.WithLease(leaseB.ID))
	require.NoError(t, err)
	outsideA, err := client.Put(ctx, root+"outside-a", "outside-a", clientv3.WithLease(leaseA.ID))
	require.NoError(t, err)
	outsideB, err := client.Put(ctx, root+"outside-b", "outside-b", clientv3.WithLease(leaseB.ID))
	require.NoError(t, err)
	updateB, err := client.Put(ctx, prefix+"b", "vb2", clientv3.WithLease(leaseB.ID))
	require.NoError(t, err)

	ttlABefore, err := client.TimeToLive(ctx, leaseA.ID, clientv3.WithAttachedKeys())
	require.NoError(t, err)
	require.ElementsMatch(t, []string{prefix + "a", root + "outside-a"}, leaseClientAttachedKeys(ttlABefore.Keys))
	ttlBBefore, err := client.TimeToLive(ctx, leaseB.ID, clientv3.WithAttachedKeys())
	require.NoError(t, err)
	require.ElementsMatch(t, []string{prefix + "b", root + "outside-b"}, leaseClientAttachedKeys(ttlBBefore.Keys))

	txn, err := client.Txn(ctx).Then(
		clientv3.OpDelete(prefix, clientv3.WithPrefix(), clientv3.WithPrevKV()),
		clientv3.OpGet(root, clientv3.WithPrefix(), clientv3.WithSort(clientv3.SortByKey, clientv3.SortAscend)),
	).Commit()
	require.NoError(t, err)
	require.True(t, txn.Succeeded)
	require.NotNil(t, txn.Header)
	require.Equal(t, updateB.Header.Revision+1, txn.Header.Revision)
	require.Len(t, txn.Responses, 2)

	deleted := txn.Responses[0].GetResponseDeleteRange()
	require.NotNil(t, deleted)
	require.NotNil(t, deleted.Header)
	require.Equal(t, txn.Header.Revision, deleted.Header.Revision)
	require.Equal(t, int64(2), deleted.Deleted)
	require.Len(t, deleted.PrevKvs, 2)
	require.Equal(t, []byte(prefix+"a"), deleted.PrevKvs[0].Key)
	require.Equal(t, []byte("va"), deleted.PrevKvs[0].Value)
	require.Equal(t, putA.Header.Revision, deleted.PrevKvs[0].CreateRevision)
	require.Equal(t, putA.Header.Revision, deleted.PrevKvs[0].ModRevision)
	require.Equal(t, int64(1), deleted.PrevKvs[0].Version)
	require.Equal(t, int64(leaseA.ID), deleted.PrevKvs[0].Lease)
	require.Equal(t, []byte(prefix+"b"), deleted.PrevKvs[1].Key)
	require.Equal(t, []byte("vb2"), deleted.PrevKvs[1].Value)
	require.Equal(t, putB.Header.Revision, deleted.PrevKvs[1].CreateRevision)
	require.Equal(t, updateB.Header.Revision, deleted.PrevKvs[1].ModRevision)
	require.Equal(t, int64(2), deleted.PrevKvs[1].Version)
	require.Equal(t, int64(leaseB.ID), deleted.PrevKvs[1].Lease)

	staged := txn.Responses[1].GetResponseRange()
	require.NotNil(t, staged)
	require.NotNil(t, staged.Header)
	require.Equal(t, txn.Header.Revision, staged.Header.Revision)
	require.Equal(t, []txnClientKV{
		{Key: "outside-a", Value: "outside-a", Version: 1},
		{Key: "outside-b", Value: "outside-b", Version: 1},
	}, txnClientKVs(staged.Kvs, root, txn.Header.Revision))
	require.Equal(t, int64(leaseA.ID), staged.Kvs[0].Lease)
	require.Equal(t, int64(leaseB.ID), staged.Kvs[1].Lease)

	ttlAAfter, err := client.TimeToLive(ctx, leaseA.ID, clientv3.WithAttachedKeys())
	require.NoError(t, err)
	require.Equal(t, []string{root + "outside-a"}, leaseClientAttachedKeys(ttlAAfter.Keys))
	ttlBAfter, err := client.TimeToLive(ctx, leaseB.ID, clientv3.WithAttachedKeys())
	require.NoError(t, err)
	require.Equal(t, []string{root + "outside-b"}, leaseClientAttachedKeys(ttlBAfter.Keys))

	historical, err := client.Get(ctx, root,
		clientv3.WithPrefix(),
		clientv3.WithRev(updateB.Header.Revision),
		clientv3.WithSort(clientv3.SortByKey, clientv3.SortAscend),
	)
	require.NoError(t, err)
	require.Equal(t, []txnClientKV{
		{Key: "items/a", Value: "va", Version: 1},
		{Key: "items/b", Value: "vb2", Version: 2},
		{Key: "outside-a", Value: "outside-a", Version: 1},
		{Key: "outside-b", Value: "outside-b", Version: 1},
	}, txnClientKVs(historical.Kvs, root, txn.Header.Revision))
	require.Equal(t, int64(leaseA.ID), historical.Kvs[0].Lease)
	require.Equal(t, int64(leaseB.ID), historical.Kvs[1].Lease)
	require.Equal(t, int64(leaseA.ID), historical.Kvs[2].Lease)
	require.Equal(t, int64(leaseB.ID), historical.Kvs[3].Lease)
	require.Equal(t, outsideA.Header.Revision, historical.Kvs[2].CreateRevision)
	require.Equal(t, outsideB.Header.Revision, historical.Kvs[3].CreateRevision)
}

func TestClientTxnNoOpDeleteWithPrevKVDoesNotConsumeRevisionMatchesEtcd(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()

	grpcServer := grpc.NewServer(server.ClientServerOptions()...)
	etcdserverpb.RegisterKVServer(grpcServer, server)
	listener := bufconn.Listen(1 << 20)
	go func() { _ = grpcServer.Serve(listener) }()
	t.Cleanup(grpcServer.Stop)

	client, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{"bufnet"},
		DialTimeout: time.Second,
		DialOptions: []grpc.DialOption{
			grpc.WithTransportCredentials(insecure.NewCredentials()),
			grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
				return listener.Dial()
			}),
		},
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, client.Close()) })

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	prefix := "/a2156/client-txn-noop-delete-prevkv/"
	putA, err := client.Put(ctx, prefix+"a", "va")
	require.NoError(t, err)
	putB, err := client.Put(ctx, prefix+"b", "vb")
	require.NoError(t, err)

	txn, err := client.Txn(ctx).Then(
		clientv3.OpDelete(prefix+"missing", clientv3.WithPrevKV()),
		clientv3.OpDelete(prefix+"b", clientv3.WithRange(prefix+"b"), clientv3.WithPrevKV()),
		clientv3.OpDelete(prefix+"c", clientv3.WithRange(prefix+"b"), clientv3.WithPrevKV()),
		clientv3.OpGet(prefix, clientv3.WithPrefix(), clientv3.WithSort(clientv3.SortByKey, clientv3.SortAscend)),
	).Commit()
	require.NoError(t, err)
	require.True(t, txn.Succeeded)
	require.NotNil(t, txn.Header)
	require.Equal(t, putB.Header.Revision, txn.Header.Revision)
	require.Len(t, txn.Responses, 4)

	for i := 0; i < 3; i++ {
		deleted := txn.Responses[i].GetResponseDeleteRange()
		require.NotNil(t, deleted)
		require.NotNil(t, deleted.Header)
		require.Equal(t, txn.Header.Revision, deleted.Header.Revision)
		require.Zero(t, deleted.Deleted)
		require.Empty(t, deleted.PrevKvs)
	}

	current := txn.Responses[3].GetResponseRange()
	require.NotNil(t, current)
	require.NotNil(t, current.Header)
	require.Equal(t, txn.Header.Revision, current.Header.Revision)
	require.Len(t, current.Kvs, 2)
	require.Equal(t, []byte(prefix+"a"), current.Kvs[0].Key)
	require.Equal(t, []byte("va"), current.Kvs[0].Value)
	require.Equal(t, putA.Header.Revision, current.Kvs[0].CreateRevision)
	require.Equal(t, putA.Header.Revision, current.Kvs[0].ModRevision)
	require.Equal(t, int64(1), current.Kvs[0].Version)
	require.Equal(t, []byte(prefix+"b"), current.Kvs[1].Key)
	require.Equal(t, []byte("vb"), current.Kvs[1].Value)
	require.Equal(t, putB.Header.Revision, current.Kvs[1].CreateRevision)
	require.Equal(t, putB.Header.Revision, current.Kvs[1].ModRevision)
	require.Equal(t, int64(1), current.Kvs[1].Version)

	after, err := client.Get(ctx, prefix, clientv3.WithPrefix(), clientv3.WithSort(clientv3.SortByKey, clientv3.SortAscend))
	require.NoError(t, err)
	require.NotNil(t, after.Header)
	require.Equal(t, putB.Header.Revision, after.Header.Revision)
	require.Len(t, after.Kvs, 2)
	require.Equal(t, []byte(prefix+"a"), after.Kvs[0].Key)
	require.Equal(t, []byte("va"), after.Kvs[0].Value)
	require.Equal(t, putA.Header.Revision, after.Kvs[0].CreateRevision)
	require.Equal(t, putA.Header.Revision, after.Kvs[0].ModRevision)
	require.Equal(t, int64(1), after.Kvs[0].Version)
	require.Equal(t, []byte(prefix+"b"), after.Kvs[1].Key)
	require.Equal(t, []byte("vb"), after.Kvs[1].Value)
	require.Equal(t, putB.Header.Revision, after.Kvs[1].CreateRevision)
	require.Equal(t, putB.Header.Revision, after.Kvs[1].ModRevision)
	require.Equal(t, int64(1), after.Kvs[1].Version)
}

func TestClientTxnDeleteLeasedPointKeyWithPrevKVMatchesEtcd(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()

	grpcServer := grpc.NewServer(server.ClientServerOptions()...)
	etcdserverpb.RegisterKVServer(grpcServer, server)
	etcdserverpb.RegisterLeaseServer(grpcServer, server)
	listener := bufconn.Listen(1 << 20)
	go func() { _ = grpcServer.Serve(listener) }()
	t.Cleanup(grpcServer.Stop)

	client, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{"bufnet"},
		DialTimeout: time.Second,
		DialOptions: []grpc.DialOption{
			grpc.WithTransportCredentials(insecure.NewCredentials()),
			grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
				return listener.Dial()
			}),
		},
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, client.Close()) })

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	lease, err := client.Grant(ctx, 300)
	require.NoError(t, err)
	prefix := "/a2157/client-txn-delete-point-leased/"
	key := prefix + "key"
	put, err := client.Put(ctx, key, "one", clientv3.WithLease(lease.ID))
	require.NoError(t, err)
	update, err := client.Put(ctx, key, "two", clientv3.WithLease(lease.ID))
	require.NoError(t, err)

	ttlBefore, err := client.TimeToLive(ctx, lease.ID, clientv3.WithAttachedKeys())
	require.NoError(t, err)
	require.Equal(t, []string{key}, leaseClientAttachedKeys(ttlBefore.Keys))

	txn, err := client.Txn(ctx).Then(
		clientv3.OpDelete(key, clientv3.WithPrevKV()),
		clientv3.OpGet(key),
	).Commit()
	require.NoError(t, err)
	require.True(t, txn.Succeeded)
	require.NotNil(t, txn.Header)
	require.Equal(t, update.Header.Revision+1, txn.Header.Revision)
	require.Len(t, txn.Responses, 2)

	deleted := txn.Responses[0].GetResponseDeleteRange()
	require.NotNil(t, deleted)
	require.NotNil(t, deleted.Header)
	require.Equal(t, txn.Header.Revision, deleted.Header.Revision)
	require.Equal(t, int64(1), deleted.Deleted)
	require.Len(t, deleted.PrevKvs, 1)
	prev := deleted.PrevKvs[0]
	require.Equal(t, []byte(key), prev.Key)
	require.Equal(t, []byte("two"), prev.Value)
	require.Equal(t, put.Header.Revision, prev.CreateRevision)
	require.Equal(t, update.Header.Revision, prev.ModRevision)
	require.Equal(t, int64(2), prev.Version)
	require.Equal(t, int64(lease.ID), prev.Lease)

	staged := txn.Responses[1].GetResponseRange()
	require.NotNil(t, staged)
	require.NotNil(t, staged.Header)
	require.Equal(t, txn.Header.Revision, staged.Header.Revision)
	require.Empty(t, staged.Kvs)

	ttlAfter, err := client.TimeToLive(ctx, lease.ID, clientv3.WithAttachedKeys())
	require.NoError(t, err)
	require.Empty(t, ttlAfter.Keys)

	historical, err := client.Get(ctx, key, clientv3.WithRev(update.Header.Revision))
	require.NoError(t, err)
	require.Len(t, historical.Kvs, 1)
	require.Equal(t, []byte("two"), historical.Kvs[0].Value)
	require.Equal(t, int64(lease.ID), historical.Kvs[0].Lease)

	current, err := client.Get(ctx, key)
	require.NoError(t, err)
	require.NotNil(t, current.Header)
	require.Equal(t, txn.Header.Revision, current.Header.Revision)
	require.Empty(t, current.Kvs)
}

func TestClientDoOpTxnDeleteLeasedPointKeyWithPrevKVMatchesEtcd(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()

	grpcServer := grpc.NewServer(server.ClientServerOptions()...)
	etcdserverpb.RegisterKVServer(grpcServer, server)
	etcdserverpb.RegisterLeaseServer(grpcServer, server)
	listener := bufconn.Listen(1 << 20)
	go func() { _ = grpcServer.Serve(listener) }()
	t.Cleanup(grpcServer.Stop)

	client, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{"bufnet"},
		DialTimeout: time.Second,
		DialOptions: []grpc.DialOption{
			grpc.WithTransportCredentials(insecure.NewCredentials()),
			grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
				return listener.Dial()
			}),
		},
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, client.Close()) })

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	lease, err := client.Grant(ctx, 300)
	require.NoError(t, err)
	prefix := "/a2158/client-do-optxn-delete-point-leased/"
	key := prefix + "key"
	put, err := client.Put(ctx, key, "one", clientv3.WithLease(lease.ID))
	require.NoError(t, err)
	update, err := client.Put(ctx, key, "two", clientv3.WithLease(lease.ID))
	require.NoError(t, err)

	ttlBefore, err := client.TimeToLive(ctx, lease.ID, clientv3.WithAttachedKeys())
	require.NoError(t, err)
	require.Equal(t, []string{key}, leaseClientAttachedKeys(ttlBefore.Keys))

	opResponse, err := client.Do(ctx, clientv3.OpTxn(
		nil,
		[]clientv3.Op{
			clientv3.OpDelete(key, clientv3.WithPrevKV()),
			clientv3.OpGet(key),
		},
		nil,
	))
	require.NoError(t, err)
	txn := opResponse.Txn()
	require.NotNil(t, txn)
	require.NotNil(t, txn.Header)
	require.Equal(t, update.Header.Revision+1, txn.Header.Revision)
	require.True(t, txn.Succeeded)
	require.Len(t, txn.Responses, 2)

	deleted := txn.Responses[0].GetResponseDeleteRange()
	require.NotNil(t, deleted)
	require.NotNil(t, deleted.Header)
	require.Equal(t, txn.Header.Revision, deleted.Header.Revision)
	require.Equal(t, int64(1), deleted.Deleted)
	require.Len(t, deleted.PrevKvs, 1)
	prev := deleted.PrevKvs[0]
	require.Equal(t, []byte(key), prev.Key)
	require.Equal(t, []byte("two"), prev.Value)
	require.Equal(t, put.Header.Revision, prev.CreateRevision)
	require.Equal(t, update.Header.Revision, prev.ModRevision)
	require.Equal(t, int64(2), prev.Version)
	require.Equal(t, int64(lease.ID), prev.Lease)

	staged := txn.Responses[1].GetResponseRange()
	require.NotNil(t, staged)
	require.NotNil(t, staged.Header)
	require.Equal(t, deleted.Header.Revision, staged.Header.Revision)
	require.Empty(t, staged.Kvs)

	ttlAfter, err := client.TimeToLive(ctx, lease.ID, clientv3.WithAttachedKeys())
	require.NoError(t, err)
	require.Empty(t, ttlAfter.Keys)

	historical, err := client.Get(ctx, key, clientv3.WithRev(update.Header.Revision))
	require.NoError(t, err)
	require.Len(t, historical.Kvs, 1)
	require.Equal(t, []byte("two"), historical.Kvs[0].Value)
	require.Equal(t, int64(lease.ID), historical.Kvs[0].Lease)

	current, err := client.Get(ctx, key)
	require.NoError(t, err)
	require.NotNil(t, current.Header)
	require.Equal(t, deleted.Header.Revision, current.Header.Revision)
	require.Empty(t, current.Kvs)
}

func TestClientDoOpTxnDeleteRangeWithLeasesPrevKVMatchesEtcd(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()

	grpcServer := grpc.NewServer(server.ClientServerOptions()...)
	etcdserverpb.RegisterKVServer(grpcServer, server)
	etcdserverpb.RegisterLeaseServer(grpcServer, server)
	listener := bufconn.Listen(1 << 20)
	go func() { _ = grpcServer.Serve(listener) }()
	t.Cleanup(grpcServer.Stop)

	client, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{"bufnet"},
		DialTimeout: time.Second,
		DialOptions: []grpc.DialOption{
			grpc.WithTransportCredentials(insecure.NewCredentials()),
			grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
				return listener.Dial()
			}),
		},
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, client.Close()) })

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	leaseA, err := client.Grant(ctx, 300)
	require.NoError(t, err)
	leaseB, err := client.Grant(ctx, 300)
	require.NoError(t, err)
	prefix := "/a2159/client-do-optxn-delete-range-leased/"
	putA, err := client.Put(ctx, prefix+"a", "va", clientv3.WithLease(leaseA.ID))
	require.NoError(t, err)
	putB, err := client.Put(ctx, prefix+"b", "vb", clientv3.WithLease(leaseB.ID))
	require.NoError(t, err)
	putC, err := client.Put(ctx, prefix+"c", "vc", clientv3.WithLease(leaseA.ID))
	require.NoError(t, err)
	updateB, err := client.Put(ctx, prefix+"b", "vb2", clientv3.WithLease(leaseB.ID))
	require.NoError(t, err)

	ttlABefore, err := client.TimeToLive(ctx, leaseA.ID, clientv3.WithAttachedKeys())
	require.NoError(t, err)
	require.ElementsMatch(t, []string{prefix + "a", prefix + "c"}, leaseClientAttachedKeys(ttlABefore.Keys))
	ttlBBefore, err := client.TimeToLive(ctx, leaseB.ID, clientv3.WithAttachedKeys())
	require.NoError(t, err)
	require.Equal(t, []string{prefix + "b"}, leaseClientAttachedKeys(ttlBBefore.Keys))

	opResponse, err := client.Do(ctx, clientv3.OpTxn(
		nil,
		[]clientv3.Op{
			clientv3.OpDelete(prefix+"a", clientv3.WithRange(prefix+"c"), clientv3.WithPrevKV()),
			clientv3.OpGet(prefix, clientv3.WithPrefix(), clientv3.WithSort(clientv3.SortByKey, clientv3.SortAscend)),
		},
		nil,
	))
	require.NoError(t, err)
	txn := opResponse.Txn()
	require.NotNil(t, txn)
	require.NotNil(t, txn.Header)
	require.Equal(t, updateB.Header.Revision+1, txn.Header.Revision)
	require.True(t, txn.Succeeded)
	require.Len(t, txn.Responses, 2)

	deleted := txn.Responses[0].GetResponseDeleteRange()
	require.NotNil(t, deleted)
	require.NotNil(t, deleted.Header)
	require.Equal(t, txn.Header.Revision, deleted.Header.Revision)
	require.Equal(t, int64(2), deleted.Deleted)
	require.Len(t, deleted.PrevKvs, 2)
	require.Equal(t, []byte(prefix+"a"), deleted.PrevKvs[0].Key)
	require.Equal(t, []byte("va"), deleted.PrevKvs[0].Value)
	require.Equal(t, putA.Header.Revision, deleted.PrevKvs[0].CreateRevision)
	require.Equal(t, putA.Header.Revision, deleted.PrevKvs[0].ModRevision)
	require.Equal(t, int64(1), deleted.PrevKvs[0].Version)
	require.Equal(t, int64(leaseA.ID), deleted.PrevKvs[0].Lease)
	require.Equal(t, []byte(prefix+"b"), deleted.PrevKvs[1].Key)
	require.Equal(t, []byte("vb2"), deleted.PrevKvs[1].Value)
	require.Equal(t, putB.Header.Revision, deleted.PrevKvs[1].CreateRevision)
	require.Equal(t, updateB.Header.Revision, deleted.PrevKvs[1].ModRevision)
	require.Equal(t, int64(2), deleted.PrevKvs[1].Version)
	require.Equal(t, int64(leaseB.ID), deleted.PrevKvs[1].Lease)

	staged := txn.Responses[1].GetResponseRange()
	require.NotNil(t, staged)
	require.NotNil(t, staged.Header)
	require.Equal(t, txn.Header.Revision, staged.Header.Revision)
	require.Equal(t, []txnClientKV{
		{Key: "c", Value: "vc", Version: 1},
	}, txnClientKVs(staged.Kvs, prefix, txn.Header.Revision))
	require.Equal(t, int64(leaseA.ID), staged.Kvs[0].Lease)

	ttlAAfter, err := client.TimeToLive(ctx, leaseA.ID, clientv3.WithAttachedKeys())
	require.NoError(t, err)
	require.Equal(t, []string{prefix + "c"}, leaseClientAttachedKeys(ttlAAfter.Keys))
	ttlBAfter, err := client.TimeToLive(ctx, leaseB.ID, clientv3.WithAttachedKeys())
	require.NoError(t, err)
	require.Empty(t, ttlBAfter.Keys)

	historical, err := client.Get(ctx, prefix,
		clientv3.WithPrefix(),
		clientv3.WithRev(updateB.Header.Revision),
		clientv3.WithSort(clientv3.SortByKey, clientv3.SortAscend),
	)
	require.NoError(t, err)
	require.Equal(t, []txnClientKV{
		{Key: "a", Value: "va", Version: 1},
		{Key: "b", Value: "vb2", Version: 2},
		{Key: "c", Value: "vc", Version: 1},
	}, txnClientKVs(historical.Kvs, prefix, txn.Header.Revision))
	require.Equal(t, int64(leaseA.ID), historical.Kvs[0].Lease)
	require.Equal(t, int64(leaseB.ID), historical.Kvs[1].Lease)
	require.Equal(t, int64(leaseA.ID), historical.Kvs[2].Lease)
	require.Equal(t, putC.Header.Revision, historical.Kvs[2].CreateRevision)
}

func TestClientDoOpTxnDeletePrefixWithLeasesPrevKVMatchesEtcd(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()

	grpcServer := grpc.NewServer(server.ClientServerOptions()...)
	etcdserverpb.RegisterKVServer(grpcServer, server)
	etcdserverpb.RegisterLeaseServer(grpcServer, server)
	listener := bufconn.Listen(1 << 20)
	go func() { _ = grpcServer.Serve(listener) }()
	t.Cleanup(grpcServer.Stop)

	client, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{"bufnet"},
		DialTimeout: time.Second,
		DialOptions: []grpc.DialOption{
			grpc.WithTransportCredentials(insecure.NewCredentials()),
			grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
				return listener.Dial()
			}),
		},
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, client.Close()) })

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	leaseA, err := client.Grant(ctx, 300)
	require.NoError(t, err)
	leaseB, err := client.Grant(ctx, 300)
	require.NoError(t, err)
	root := "/a2160/client-do-optxn-delete-prefix-leased/"
	prefix := root + "items/"
	putA, err := client.Put(ctx, prefix+"a", "va", clientv3.WithLease(leaseA.ID))
	require.NoError(t, err)
	putB, err := client.Put(ctx, prefix+"b", "vb", clientv3.WithLease(leaseB.ID))
	require.NoError(t, err)
	outsideA, err := client.Put(ctx, root+"outside-a", "outside-a", clientv3.WithLease(leaseA.ID))
	require.NoError(t, err)
	outsideB, err := client.Put(ctx, root+"outside-b", "outside-b", clientv3.WithLease(leaseB.ID))
	require.NoError(t, err)
	updateB, err := client.Put(ctx, prefix+"b", "vb2", clientv3.WithLease(leaseB.ID))
	require.NoError(t, err)

	ttlABefore, err := client.TimeToLive(ctx, leaseA.ID, clientv3.WithAttachedKeys())
	require.NoError(t, err)
	require.ElementsMatch(t, []string{prefix + "a", root + "outside-a"}, leaseClientAttachedKeys(ttlABefore.Keys))
	ttlBBefore, err := client.TimeToLive(ctx, leaseB.ID, clientv3.WithAttachedKeys())
	require.NoError(t, err)
	require.ElementsMatch(t, []string{prefix + "b", root + "outside-b"}, leaseClientAttachedKeys(ttlBBefore.Keys))

	opResponse, err := client.Do(ctx, clientv3.OpTxn(
		nil,
		[]clientv3.Op{
			clientv3.OpDelete(prefix, clientv3.WithPrefix(), clientv3.WithPrevKV()),
			clientv3.OpGet(root, clientv3.WithPrefix(), clientv3.WithSort(clientv3.SortByKey, clientv3.SortAscend)),
		},
		nil,
	))
	require.NoError(t, err)
	txn := opResponse.Txn()
	require.NotNil(t, txn)
	require.NotNil(t, txn.Header)
	require.Equal(t, updateB.Header.Revision+1, txn.Header.Revision)
	require.True(t, txn.Succeeded)
	require.Len(t, txn.Responses, 2)

	deleted := txn.Responses[0].GetResponseDeleteRange()
	require.NotNil(t, deleted)
	require.NotNil(t, deleted.Header)
	require.Equal(t, txn.Header.Revision, deleted.Header.Revision)
	require.Equal(t, int64(2), deleted.Deleted)
	require.Len(t, deleted.PrevKvs, 2)
	require.Equal(t, []byte(prefix+"a"), deleted.PrevKvs[0].Key)
	require.Equal(t, []byte("va"), deleted.PrevKvs[0].Value)
	require.Equal(t, putA.Header.Revision, deleted.PrevKvs[0].CreateRevision)
	require.Equal(t, putA.Header.Revision, deleted.PrevKvs[0].ModRevision)
	require.Equal(t, int64(1), deleted.PrevKvs[0].Version)
	require.Equal(t, int64(leaseA.ID), deleted.PrevKvs[0].Lease)
	require.Equal(t, []byte(prefix+"b"), deleted.PrevKvs[1].Key)
	require.Equal(t, []byte("vb2"), deleted.PrevKvs[1].Value)
	require.Equal(t, putB.Header.Revision, deleted.PrevKvs[1].CreateRevision)
	require.Equal(t, updateB.Header.Revision, deleted.PrevKvs[1].ModRevision)
	require.Equal(t, int64(2), deleted.PrevKvs[1].Version)
	require.Equal(t, int64(leaseB.ID), deleted.PrevKvs[1].Lease)

	staged := txn.Responses[1].GetResponseRange()
	require.NotNil(t, staged)
	require.NotNil(t, staged.Header)
	require.Equal(t, txn.Header.Revision, staged.Header.Revision)
	require.Equal(t, []txnClientKV{
		{Key: "outside-a", Value: "outside-a", Version: 1},
		{Key: "outside-b", Value: "outside-b", Version: 1},
	}, txnClientKVs(staged.Kvs, root, txn.Header.Revision))
	require.Equal(t, int64(leaseA.ID), staged.Kvs[0].Lease)
	require.Equal(t, int64(leaseB.ID), staged.Kvs[1].Lease)

	ttlAAfter, err := client.TimeToLive(ctx, leaseA.ID, clientv3.WithAttachedKeys())
	require.NoError(t, err)
	require.Equal(t, []string{root + "outside-a"}, leaseClientAttachedKeys(ttlAAfter.Keys))
	ttlBAfter, err := client.TimeToLive(ctx, leaseB.ID, clientv3.WithAttachedKeys())
	require.NoError(t, err)
	require.Equal(t, []string{root + "outside-b"}, leaseClientAttachedKeys(ttlBAfter.Keys))

	historical, err := client.Get(ctx, root,
		clientv3.WithPrefix(),
		clientv3.WithRev(updateB.Header.Revision),
		clientv3.WithSort(clientv3.SortByKey, clientv3.SortAscend),
	)
	require.NoError(t, err)
	require.Equal(t, []txnClientKV{
		{Key: "items/a", Value: "va", Version: 1},
		{Key: "items/b", Value: "vb2", Version: 2},
		{Key: "outside-a", Value: "outside-a", Version: 1},
		{Key: "outside-b", Value: "outside-b", Version: 1},
	}, txnClientKVs(historical.Kvs, root, txn.Header.Revision))
	require.Equal(t, int64(leaseA.ID), historical.Kvs[0].Lease)
	require.Equal(t, int64(leaseB.ID), historical.Kvs[1].Lease)
	require.Equal(t, int64(leaseA.ID), historical.Kvs[2].Lease)
	require.Equal(t, int64(leaseB.ID), historical.Kvs[3].Lease)
	require.Equal(t, outsideA.Header.Revision, historical.Kvs[2].CreateRevision)
	require.Equal(t, outsideB.Header.Revision, historical.Kvs[3].CreateRevision)
}

func TestClientDoOpTxnDeleteFromKeyWithLeasesPrevKVMatchesEtcd(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()

	grpcServer := grpc.NewServer(server.ClientServerOptions()...)
	etcdserverpb.RegisterKVServer(grpcServer, server)
	etcdserverpb.RegisterLeaseServer(grpcServer, server)
	listener := bufconn.Listen(1 << 20)
	go func() { _ = grpcServer.Serve(listener) }()
	t.Cleanup(grpcServer.Stop)

	client, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{"bufnet"},
		DialTimeout: time.Second,
		DialOptions: []grpc.DialOption{
			grpc.WithTransportCredentials(insecure.NewCredentials()),
			grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
				return listener.Dial()
			}),
		},
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, client.Close()) })

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	leaseA, err := client.Grant(ctx, 300)
	require.NoError(t, err)
	leaseB, err := client.Grant(ctx, 300)
	require.NoError(t, err)
	prefix := "/a2161/client-do-optxn-delete-fromkey-leased/"
	before, err := client.Put(ctx, prefix+"a", "va", clientv3.WithLease(leaseA.ID))
	require.NoError(t, err)
	putB, err := client.Put(ctx, prefix+"b", "vb", clientv3.WithLease(leaseA.ID))
	require.NoError(t, err)
	putC, err := client.Put(ctx, prefix+"c", "vc", clientv3.WithLease(leaseB.ID))
	require.NoError(t, err)
	putD, err := client.Put(ctx, prefix+"d", "vd", clientv3.WithLease(leaseA.ID))
	require.NoError(t, err)
	updateC, err := client.Put(ctx, prefix+"c", "vc2", clientv3.WithLease(leaseB.ID))
	require.NoError(t, err)

	ttlABefore, err := client.TimeToLive(ctx, leaseA.ID, clientv3.WithAttachedKeys())
	require.NoError(t, err)
	require.ElementsMatch(t, []string{prefix + "a", prefix + "b", prefix + "d"}, leaseClientAttachedKeys(ttlABefore.Keys))
	ttlBBefore, err := client.TimeToLive(ctx, leaseB.ID, clientv3.WithAttachedKeys())
	require.NoError(t, err)
	require.Equal(t, []string{prefix + "c"}, leaseClientAttachedKeys(ttlBBefore.Keys))

	opResponse, err := client.Do(ctx, clientv3.OpTxn(
		nil,
		[]clientv3.Op{
			clientv3.OpDelete(prefix+"b", clientv3.WithFromKey(), clientv3.WithPrevKV()),
			clientv3.OpGet(prefix, clientv3.WithPrefix(), clientv3.WithSort(clientv3.SortByKey, clientv3.SortAscend)),
		},
		nil,
	))
	require.NoError(t, err)
	txn := opResponse.Txn()
	require.NotNil(t, txn)
	require.NotNil(t, txn.Header)
	require.Equal(t, updateC.Header.Revision+1, txn.Header.Revision)
	require.True(t, txn.Succeeded)
	require.Len(t, txn.Responses, 2)

	deleted := txn.Responses[0].GetResponseDeleteRange()
	require.NotNil(t, deleted)
	require.NotNil(t, deleted.Header)
	require.Equal(t, txn.Header.Revision, deleted.Header.Revision)
	require.Equal(t, int64(3), deleted.Deleted)
	require.Len(t, deleted.PrevKvs, 3)
	require.Equal(t, []byte(prefix+"b"), deleted.PrevKvs[0].Key)
	require.Equal(t, []byte("vb"), deleted.PrevKvs[0].Value)
	require.Equal(t, putB.Header.Revision, deleted.PrevKvs[0].CreateRevision)
	require.Equal(t, putB.Header.Revision, deleted.PrevKvs[0].ModRevision)
	require.Equal(t, int64(1), deleted.PrevKvs[0].Version)
	require.Equal(t, int64(leaseA.ID), deleted.PrevKvs[0].Lease)
	require.Equal(t, []byte(prefix+"c"), deleted.PrevKvs[1].Key)
	require.Equal(t, []byte("vc2"), deleted.PrevKvs[1].Value)
	require.Equal(t, putC.Header.Revision, deleted.PrevKvs[1].CreateRevision)
	require.Equal(t, updateC.Header.Revision, deleted.PrevKvs[1].ModRevision)
	require.Equal(t, int64(2), deleted.PrevKvs[1].Version)
	require.Equal(t, int64(leaseB.ID), deleted.PrevKvs[1].Lease)
	require.Equal(t, []byte(prefix+"d"), deleted.PrevKvs[2].Key)
	require.Equal(t, []byte("vd"), deleted.PrevKvs[2].Value)
	require.Equal(t, putD.Header.Revision, deleted.PrevKvs[2].CreateRevision)
	require.Equal(t, putD.Header.Revision, deleted.PrevKvs[2].ModRevision)
	require.Equal(t, int64(1), deleted.PrevKvs[2].Version)
	require.Equal(t, int64(leaseA.ID), deleted.PrevKvs[2].Lease)

	staged := txn.Responses[1].GetResponseRange()
	require.NotNil(t, staged)
	require.NotNil(t, staged.Header)
	require.Equal(t, txn.Header.Revision, staged.Header.Revision)
	require.Equal(t, []txnClientKV{
		{Key: "a", Value: "va", Version: 1},
	}, txnClientKVs(staged.Kvs, prefix, txn.Header.Revision))
	require.Equal(t, int64(leaseA.ID), staged.Kvs[0].Lease)

	ttlAAfter, err := client.TimeToLive(ctx, leaseA.ID, clientv3.WithAttachedKeys())
	require.NoError(t, err)
	require.Equal(t, []string{prefix + "a"}, leaseClientAttachedKeys(ttlAAfter.Keys))
	ttlBAfter, err := client.TimeToLive(ctx, leaseB.ID, clientv3.WithAttachedKeys())
	require.NoError(t, err)
	require.Empty(t, ttlBAfter.Keys)

	historical, err := client.Get(ctx, prefix,
		clientv3.WithPrefix(),
		clientv3.WithRev(updateC.Header.Revision),
		clientv3.WithSort(clientv3.SortByKey, clientv3.SortAscend),
	)
	require.NoError(t, err)
	require.Equal(t, []txnClientKV{
		{Key: "a", Value: "va", Version: 1},
		{Key: "b", Value: "vb", Version: 1},
		{Key: "c", Value: "vc2", Version: 2},
		{Key: "d", Value: "vd", Version: 1},
	}, txnClientKVs(historical.Kvs, prefix, txn.Header.Revision))
	require.Equal(t, int64(leaseA.ID), historical.Kvs[0].Lease)
	require.Equal(t, int64(leaseA.ID), historical.Kvs[1].Lease)
	require.Equal(t, int64(leaseB.ID), historical.Kvs[2].Lease)
	require.Equal(t, int64(leaseA.ID), historical.Kvs[3].Lease)
	require.Equal(t, before.Header.Revision, historical.Kvs[0].CreateRevision)
}

func TestClientDoOpTxnNoOpDeleteWithPrevKVDoesNotConsumeRevisionMatchesEtcd(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()

	grpcServer := grpc.NewServer(server.ClientServerOptions()...)
	etcdserverpb.RegisterKVServer(grpcServer, server)
	listener := bufconn.Listen(1 << 20)
	go func() { _ = grpcServer.Serve(listener) }()
	t.Cleanup(grpcServer.Stop)

	client, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{"bufnet"},
		DialTimeout: time.Second,
		DialOptions: []grpc.DialOption{
			grpc.WithTransportCredentials(insecure.NewCredentials()),
			grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
				return listener.Dial()
			}),
		},
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, client.Close()) })

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	prefix := "/a2162/client-do-optxn-noop-delete-prevkv/"
	putA, err := client.Put(ctx, prefix+"a", "va")
	require.NoError(t, err)
	putB, err := client.Put(ctx, prefix+"b", "vb")
	require.NoError(t, err)

	opResponse, err := client.Do(ctx, clientv3.OpTxn(
		nil,
		[]clientv3.Op{
			clientv3.OpDelete(prefix+"missing", clientv3.WithPrevKV()),
			clientv3.OpDelete(prefix+"b", clientv3.WithRange(prefix+"b"), clientv3.WithPrevKV()),
			clientv3.OpDelete(prefix+"c", clientv3.WithRange(prefix+"b"), clientv3.WithPrevKV()),
			clientv3.OpGet(prefix, clientv3.WithPrefix(), clientv3.WithSort(clientv3.SortByKey, clientv3.SortAscend)),
		},
		nil,
	))
	require.NoError(t, err)
	txn := opResponse.Txn()
	require.NotNil(t, txn)
	require.NotNil(t, txn.Header)
	require.Equal(t, putB.Header.Revision, txn.Header.Revision)
	require.True(t, txn.Succeeded)
	require.Len(t, txn.Responses, 4)

	for i := 0; i < 3; i++ {
		deleted := txn.Responses[i].GetResponseDeleteRange()
		require.NotNil(t, deleted)
		require.NotNil(t, deleted.Header)
		require.Equal(t, txn.Header.Revision, deleted.Header.Revision)
		require.Zero(t, deleted.Deleted)
		require.Empty(t, deleted.PrevKvs)
	}

	current := txn.Responses[3].GetResponseRange()
	require.NotNil(t, current)
	require.NotNil(t, current.Header)
	require.Equal(t, txn.Header.Revision, current.Header.Revision)
	require.Len(t, current.Kvs, 2)
	require.Equal(t, []byte(prefix+"a"), current.Kvs[0].Key)
	require.Equal(t, []byte("va"), current.Kvs[0].Value)
	require.Equal(t, putA.Header.Revision, current.Kvs[0].CreateRevision)
	require.Equal(t, putA.Header.Revision, current.Kvs[0].ModRevision)
	require.Equal(t, int64(1), current.Kvs[0].Version)
	require.Equal(t, []byte(prefix+"b"), current.Kvs[1].Key)
	require.Equal(t, []byte("vb"), current.Kvs[1].Value)
	require.Equal(t, putB.Header.Revision, current.Kvs[1].CreateRevision)
	require.Equal(t, putB.Header.Revision, current.Kvs[1].ModRevision)
	require.Equal(t, int64(1), current.Kvs[1].Version)

	after, err := client.Get(ctx, prefix, clientv3.WithPrefix(), clientv3.WithSort(clientv3.SortByKey, clientv3.SortAscend))
	require.NoError(t, err)
	require.NotNil(t, after.Header)
	require.Equal(t, putB.Header.Revision, after.Header.Revision)
	require.Len(t, after.Kvs, 2)
	require.Equal(t, []byte(prefix+"a"), after.Kvs[0].Key)
	require.Equal(t, []byte("va"), after.Kvs[0].Value)
	require.Equal(t, putA.Header.Revision, after.Kvs[0].CreateRevision)
	require.Equal(t, putA.Header.Revision, after.Kvs[0].ModRevision)
	require.Equal(t, int64(1), after.Kvs[0].Version)
	require.Equal(t, []byte(prefix+"b"), after.Kvs[1].Key)
	require.Equal(t, []byte("vb"), after.Kvs[1].Value)
	require.Equal(t, putB.Header.Revision, after.Kvs[1].CreateRevision)
	require.Equal(t, putB.Header.Revision, after.Kvs[1].ModRevision)
	require.Equal(t, int64(1), after.Kvs[1].Version)
}

func TestClientDoOpTxnFutureRevisionRejectsBeforeWritesMatchesEtcd(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()

	grpcServer := grpc.NewServer(server.ClientServerOptions()...)
	etcdserverpb.RegisterKVServer(grpcServer, server)
	listener := bufconn.Listen(1 << 20)
	go func() { _ = grpcServer.Serve(listener) }()
	t.Cleanup(grpcServer.Stop)

	client, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{"bufnet"},
		DialTimeout: time.Second,
		DialOptions: []grpc.DialOption{
			grpc.WithTransportCredentials(insecure.NewCredentials()),
			grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
				return listener.Dial()
			}),
		},
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, client.Close()) })

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	prefix := "/a2175/client-do-optxn-future-revision/"
	key := prefix + "key"
	writeKey := prefix + "write"
	seed, err := client.Put(ctx, key, "value")
	require.NoError(t, err)

	_, err = client.Do(ctx, clientv3.OpTxn(
		nil,
		[]clientv3.Op{
			clientv3.OpGet(key, clientv3.WithRev(math.MaxInt64)),
			clientv3.OpPut(writeKey, "must-not-commit"),
		},
		nil,
	))
	requireClientTxnError(t, err, codes.Unknown, "etcdserver: mvcc: required revision is a future revision", rpctypes.ErrFutureRev)

	after, err := client.Get(ctx, prefix, clientv3.WithPrefix(), clientv3.WithSort(clientv3.SortByKey, clientv3.SortAscend))
	require.NoError(t, err)
	require.NotNil(t, after.Header)
	require.Equal(t, seed.Header.Revision, after.Header.Revision)
	require.Len(t, after.Kvs, 1)
	require.Equal(t, []byte(key), after.Kvs[0].Key)
	require.Equal(t, []byte("value"), after.Kvs[0].Value)
	require.Equal(t, seed.Header.Revision, after.Kvs[0].CreateRevision)
	require.Equal(t, seed.Header.Revision, after.Kvs[0].ModRevision)
	require.Equal(t, int64(1), after.Kvs[0].Version)
}

func TestClientDoOpTxnCompactedRevisionRejectsBeforeWritesMatchesEtcd(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()

	grpcServer := grpc.NewServer(server.ClientServerOptions()...)
	etcdserverpb.RegisterKVServer(grpcServer, server)
	listener := bufconn.Listen(1 << 20)
	go func() { _ = grpcServer.Serve(listener) }()
	t.Cleanup(grpcServer.Stop)

	client, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{"bufnet"},
		DialTimeout: time.Second,
		DialOptions: []grpc.DialOption{
			grpc.WithTransportCredentials(insecure.NewCredentials()),
			grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
				return listener.Dial()
			}),
		},
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, client.Close()) })

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	prefix := "/a2176/client-do-optxn-compacted-revision/"
	key := prefix + "key"
	writeKey := prefix + "write"
	seed, err := client.Put(ctx, key, "value")
	require.NoError(t, err)
	compactedRev := seed.Header.Revision - 1
	require.Positive(t, compactedRev)
	compact, err := client.Compact(ctx, seed.Header.Revision)
	require.NoError(t, err)

	_, err = client.Do(ctx, clientv3.OpTxn(
		nil,
		[]clientv3.Op{
			clientv3.OpGet(key, clientv3.WithRev(compactedRev)),
			clientv3.OpPut(writeKey, "must-not-commit"),
		},
		nil,
	))
	requireClientTxnError(t, err, codes.Unknown, "etcdserver: mvcc: required revision has been compacted", rpctypes.ErrCompacted)

	after, err := client.Get(ctx, prefix, clientv3.WithPrefix(), clientv3.WithSort(clientv3.SortByKey, clientv3.SortAscend))
	require.NoError(t, err)
	require.NotNil(t, after.Header)
	require.GreaterOrEqual(t, after.Header.Revision, compact.Header.Revision)
	require.Len(t, after.Kvs, 1)
	require.Equal(t, []byte(key), after.Kvs[0].Key)
	require.Equal(t, []byte("value"), after.Kvs[0].Value)
	require.Equal(t, seed.Header.Revision, after.Kvs[0].CreateRevision)
	require.Equal(t, seed.Header.Revision, after.Kvs[0].ModRevision)
	require.Equal(t, int64(1), after.Kvs[0].Version)
}

func TestClientDoOpTxnUnselectedFutureRevisionBranchIsIgnoredMatchesEtcd(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()

	grpcServer := grpc.NewServer(server.ClientServerOptions()...)
	etcdserverpb.RegisterKVServer(grpcServer, server)
	listener := bufconn.Listen(1 << 20)
	go func() { _ = grpcServer.Serve(listener) }()
	t.Cleanup(grpcServer.Stop)

	client, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{"bufnet"},
		DialTimeout: time.Second,
		DialOptions: []grpc.DialOption{
			grpc.WithTransportCredentials(insecure.NewCredentials()),
			grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
				return listener.Dial()
			}),
		},
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, client.Close()) })

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	prefix := "/a2177/client-do-optxn-unselected-future-revision/"
	key := prefix + "key"
	unselectedWriteKey := prefix + "unselected-write"
	seed, err := client.Put(ctx, key, "value")
	require.NoError(t, err)

	opResponse, err := client.Do(ctx, clientv3.OpTxn(
		[]clientv3.Cmp{clientv3.Compare(clientv3.Version(prefix+"missing"), ">", 0)},
		[]clientv3.Op{
			clientv3.OpGet(key, clientv3.WithRev(math.MaxInt64)),
			clientv3.OpPut(unselectedWriteKey, "must-not-commit"),
		},
		[]clientv3.Op{
			clientv3.OpGet(key),
		},
	))
	require.NoError(t, err)
	txn := opResponse.Txn()
	require.NotNil(t, txn)
	require.NotNil(t, txn.Header)
	require.Equal(t, seed.Header.Revision, txn.Header.Revision)
	require.False(t, txn.Succeeded)
	require.Len(t, txn.Responses, 1)

	selected := txn.Responses[0].GetResponseRange()
	require.NotNil(t, selected)
	require.NotNil(t, selected.Header)
	require.Equal(t, txn.Header.Revision, selected.Header.Revision)
	require.Len(t, selected.Kvs, 1)
	require.Equal(t, []byte(key), selected.Kvs[0].Key)
	require.Equal(t, []byte("value"), selected.Kvs[0].Value)
	require.Equal(t, seed.Header.Revision, selected.Kvs[0].CreateRevision)
	require.Equal(t, seed.Header.Revision, selected.Kvs[0].ModRevision)
	require.Equal(t, int64(1), selected.Kvs[0].Version)

	after, err := client.Get(ctx, prefix, clientv3.WithPrefix(), clientv3.WithSort(clientv3.SortByKey, clientv3.SortAscend))
	require.NoError(t, err)
	require.NotNil(t, after.Header)
	require.Equal(t, seed.Header.Revision, after.Header.Revision)
	require.Len(t, after.Kvs, 1)
	require.Equal(t, []byte(key), after.Kvs[0].Key)
	require.Equal(t, []byte("value"), after.Kvs[0].Value)
	require.Equal(t, seed.Header.Revision, after.Kvs[0].CreateRevision)
	require.Equal(t, seed.Header.Revision, after.Kvs[0].ModRevision)
	require.Equal(t, int64(1), after.Kvs[0].Version)
}

func TestClientDoOpTxnUnselectedCompactedRevisionBranchIsIgnoredMatchesEtcd(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()

	grpcServer := grpc.NewServer(server.ClientServerOptions()...)
	etcdserverpb.RegisterKVServer(grpcServer, server)
	listener := bufconn.Listen(1 << 20)
	go func() { _ = grpcServer.Serve(listener) }()
	t.Cleanup(grpcServer.Stop)

	client, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{"bufnet"},
		DialTimeout: time.Second,
		DialOptions: []grpc.DialOption{
			grpc.WithTransportCredentials(insecure.NewCredentials()),
			grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
				return listener.Dial()
			}),
		},
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, client.Close()) })

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	prefix := "/a2178/client-do-optxn-unselected-compacted-revision/"
	key := prefix + "key"
	unselectedWriteKey := prefix + "unselected-write"
	seed, err := client.Put(ctx, key, "value")
	require.NoError(t, err)
	compactedRev := seed.Header.Revision - 1
	require.Positive(t, compactedRev)
	compact, err := client.Compact(ctx, seed.Header.Revision)
	require.NoError(t, err)

	opResponse, err := client.Do(ctx, clientv3.OpTxn(
		[]clientv3.Cmp{clientv3.Compare(clientv3.Version(prefix+"missing"), ">", 0)},
		[]clientv3.Op{
			clientv3.OpGet(key, clientv3.WithRev(compactedRev)),
			clientv3.OpPut(unselectedWriteKey, "must-not-commit"),
		},
		[]clientv3.Op{
			clientv3.OpGet(key),
		},
	))
	require.NoError(t, err)
	txn := opResponse.Txn()
	require.NotNil(t, txn)
	require.NotNil(t, txn.Header)
	require.GreaterOrEqual(t, txn.Header.Revision, compact.Header.Revision)
	require.False(t, txn.Succeeded)
	require.Len(t, txn.Responses, 1)

	selected := txn.Responses[0].GetResponseRange()
	require.NotNil(t, selected)
	require.NotNil(t, selected.Header)
	require.Equal(t, txn.Header.Revision, selected.Header.Revision)
	require.Len(t, selected.Kvs, 1)
	require.Equal(t, []byte(key), selected.Kvs[0].Key)
	require.Equal(t, []byte("value"), selected.Kvs[0].Value)
	require.Equal(t, seed.Header.Revision, selected.Kvs[0].CreateRevision)
	require.Equal(t, seed.Header.Revision, selected.Kvs[0].ModRevision)
	require.Equal(t, int64(1), selected.Kvs[0].Version)

	after, err := client.Get(ctx, prefix, clientv3.WithPrefix(), clientv3.WithSort(clientv3.SortByKey, clientv3.SortAscend))
	require.NoError(t, err)
	require.NotNil(t, after.Header)
	require.GreaterOrEqual(t, after.Header.Revision, txn.Header.Revision)
	require.Len(t, after.Kvs, 1)
	require.Equal(t, []byte(key), after.Kvs[0].Key)
	require.Equal(t, []byte("value"), after.Kvs[0].Value)
	require.Equal(t, seed.Header.Revision, after.Kvs[0].CreateRevision)
	require.Equal(t, seed.Header.Revision, after.Kvs[0].ModRevision)
	require.Equal(t, int64(1), after.Kvs[0].Version)
}

func TestClientDoOpTxnUnselectedFailureFutureRevisionBranchIsIgnoredMatchesEtcd(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()

	grpcServer := grpc.NewServer(server.ClientServerOptions()...)
	etcdserverpb.RegisterKVServer(grpcServer, server)
	listener := bufconn.Listen(1 << 20)
	go func() { _ = grpcServer.Serve(listener) }()
	t.Cleanup(grpcServer.Stop)

	client, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{"bufnet"},
		DialTimeout: time.Second,
		DialOptions: []grpc.DialOption{
			grpc.WithTransportCredentials(insecure.NewCredentials()),
			grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
				return listener.Dial()
			}),
		},
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, client.Close()) })

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	prefix := "/a2179/client-do-optxn-unselected-failure-future-revision/"
	key := prefix + "key"
	unselectedWriteKey := prefix + "unselected-write"
	seed, err := client.Put(ctx, key, "value")
	require.NoError(t, err)

	opResponse, err := client.Do(ctx, clientv3.OpTxn(
		[]clientv3.Cmp{clientv3.Compare(clientv3.Version(key), "=", 1)},
		[]clientv3.Op{
			clientv3.OpGet(key),
		},
		[]clientv3.Op{
			clientv3.OpGet(key, clientv3.WithRev(math.MaxInt64)),
			clientv3.OpPut(unselectedWriteKey, "must-not-commit"),
		},
	))
	require.NoError(t, err)
	txn := opResponse.Txn()
	require.NotNil(t, txn)
	require.NotNil(t, txn.Header)
	require.Equal(t, seed.Header.Revision, txn.Header.Revision)
	require.True(t, txn.Succeeded)
	require.Len(t, txn.Responses, 1)

	selected := txn.Responses[0].GetResponseRange()
	require.NotNil(t, selected)
	require.NotNil(t, selected.Header)
	require.Equal(t, txn.Header.Revision, selected.Header.Revision)
	require.Len(t, selected.Kvs, 1)
	require.Equal(t, []byte(key), selected.Kvs[0].Key)
	require.Equal(t, []byte("value"), selected.Kvs[0].Value)
	require.Equal(t, seed.Header.Revision, selected.Kvs[0].CreateRevision)
	require.Equal(t, seed.Header.Revision, selected.Kvs[0].ModRevision)
	require.Equal(t, int64(1), selected.Kvs[0].Version)

	after, err := client.Get(ctx, prefix, clientv3.WithPrefix(), clientv3.WithSort(clientv3.SortByKey, clientv3.SortAscend))
	require.NoError(t, err)
	require.NotNil(t, after.Header)
	require.Equal(t, seed.Header.Revision, after.Header.Revision)
	require.Len(t, after.Kvs, 1)
	require.Equal(t, []byte(key), after.Kvs[0].Key)
	require.Equal(t, []byte("value"), after.Kvs[0].Value)
	require.Equal(t, seed.Header.Revision, after.Kvs[0].CreateRevision)
	require.Equal(t, seed.Header.Revision, after.Kvs[0].ModRevision)
	require.Equal(t, int64(1), after.Kvs[0].Version)
}

func TestClientDoOpTxnUnselectedFailureCompactedRevisionBranchIsIgnoredMatchesEtcd(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()

	grpcServer := grpc.NewServer(server.ClientServerOptions()...)
	etcdserverpb.RegisterKVServer(grpcServer, server)
	listener := bufconn.Listen(1 << 20)
	go func() { _ = grpcServer.Serve(listener) }()
	t.Cleanup(grpcServer.Stop)

	client, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{"bufnet"},
		DialTimeout: time.Second,
		DialOptions: []grpc.DialOption{
			grpc.WithTransportCredentials(insecure.NewCredentials()),
			grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
				return listener.Dial()
			}),
		},
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, client.Close()) })

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	prefix := "/a2180/client-do-optxn-unselected-failure-compacted-revision/"
	key := prefix + "key"
	unselectedWriteKey := prefix + "unselected-write"
	seed, err := client.Put(ctx, key, "value")
	require.NoError(t, err)
	compactedRev := seed.Header.Revision - 1
	require.Positive(t, compactedRev)
	compact, err := client.Compact(ctx, seed.Header.Revision)
	require.NoError(t, err)

	opResponse, err := client.Do(ctx, clientv3.OpTxn(
		[]clientv3.Cmp{clientv3.Compare(clientv3.Version(key), "=", 1)},
		[]clientv3.Op{
			clientv3.OpGet(key),
		},
		[]clientv3.Op{
			clientv3.OpGet(key, clientv3.WithRev(compactedRev)),
			clientv3.OpPut(unselectedWriteKey, "must-not-commit"),
		},
	))
	require.NoError(t, err)
	txn := opResponse.Txn()
	require.NotNil(t, txn)
	require.NotNil(t, txn.Header)
	require.GreaterOrEqual(t, txn.Header.Revision, compact.Header.Revision)
	require.True(t, txn.Succeeded)
	require.Len(t, txn.Responses, 1)

	selected := txn.Responses[0].GetResponseRange()
	require.NotNil(t, selected)
	require.NotNil(t, selected.Header)
	require.Equal(t, txn.Header.Revision, selected.Header.Revision)
	require.Len(t, selected.Kvs, 1)
	require.Equal(t, []byte(key), selected.Kvs[0].Key)
	require.Equal(t, []byte("value"), selected.Kvs[0].Value)
	require.Equal(t, seed.Header.Revision, selected.Kvs[0].CreateRevision)
	require.Equal(t, seed.Header.Revision, selected.Kvs[0].ModRevision)
	require.Equal(t, int64(1), selected.Kvs[0].Version)

	after, err := client.Get(ctx, prefix, clientv3.WithPrefix(), clientv3.WithSort(clientv3.SortByKey, clientv3.SortAscend))
	require.NoError(t, err)
	require.NotNil(t, after.Header)
	require.GreaterOrEqual(t, after.Header.Revision, txn.Header.Revision)
	require.Len(t, after.Kvs, 1)
	require.Equal(t, []byte(key), after.Kvs[0].Key)
	require.Equal(t, []byte("value"), after.Kvs[0].Value)
	require.Equal(t, seed.Header.Revision, after.Kvs[0].CreateRevision)
	require.Equal(t, seed.Header.Revision, after.Kvs[0].ModRevision)
	require.Equal(t, int64(1), after.Kvs[0].Version)
}

func TestClientNestedTxnDeleteLeasedPointKeyWithPrevKVMatchesEtcd(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()

	grpcServer := grpc.NewServer(server.ClientServerOptions()...)
	etcdserverpb.RegisterKVServer(grpcServer, server)
	etcdserverpb.RegisterLeaseServer(grpcServer, server)
	listener := bufconn.Listen(1 << 20)
	go func() { _ = grpcServer.Serve(listener) }()
	t.Cleanup(grpcServer.Stop)

	client, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{"bufnet"},
		DialTimeout: time.Second,
		DialOptions: []grpc.DialOption{
			grpc.WithTransportCredentials(insecure.NewCredentials()),
			grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
				return listener.Dial()
			}),
		},
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, client.Close()) })

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	lease, err := client.Grant(ctx, 300)
	require.NoError(t, err)
	prefix := "/a2163/client-nested-txn-delete-point-leased/"
	key := prefix + "key"
	put, err := client.Put(ctx, key, "one", clientv3.WithLease(lease.ID))
	require.NoError(t, err)
	update, err := client.Put(ctx, key, "two", clientv3.WithLease(lease.ID))
	require.NoError(t, err)

	ttlBefore, err := client.TimeToLive(ctx, lease.ID, clientv3.WithAttachedKeys())
	require.NoError(t, err)
	require.Equal(t, []string{key}, leaseClientAttachedKeys(ttlBefore.Keys))

	txn, err := client.Txn(ctx).Then(
		clientv3.OpTxn(
			nil,
			[]clientv3.Op{
				clientv3.OpDelete(key, clientv3.WithPrevKV()),
				clientv3.OpGet(key),
			},
			nil,
		),
	).Commit()
	require.NoError(t, err)
	require.True(t, txn.Succeeded)
	require.NotNil(t, txn.Header)
	require.Equal(t, update.Header.Revision+1, txn.Header.Revision)
	require.Len(t, txn.Responses, 1)

	nested := txn.Responses[0].GetResponseTxn()
	require.NotNil(t, nested)
	require.NotNil(t, nested.Header)
	require.Zero(t, nested.Header.Revision)
	require.True(t, nested.Succeeded)
	require.Len(t, nested.Responses, 2)

	deleted := nested.Responses[0].GetResponseDeleteRange()
	require.NotNil(t, deleted)
	require.NotNil(t, deleted.Header)
	require.Equal(t, txn.Header.Revision, deleted.Header.Revision)
	require.Equal(t, int64(1), deleted.Deleted)
	require.Len(t, deleted.PrevKvs, 1)
	prev := deleted.PrevKvs[0]
	require.Equal(t, []byte(key), prev.Key)
	require.Equal(t, []byte("two"), prev.Value)
	require.Equal(t, put.Header.Revision, prev.CreateRevision)
	require.Equal(t, update.Header.Revision, prev.ModRevision)
	require.Equal(t, int64(2), prev.Version)
	require.Equal(t, int64(lease.ID), prev.Lease)

	staged := nested.Responses[1].GetResponseRange()
	require.NotNil(t, staged)
	require.NotNil(t, staged.Header)
	require.Equal(t, txn.Header.Revision, staged.Header.Revision)
	require.Empty(t, staged.Kvs)

	ttlAfter, err := client.TimeToLive(ctx, lease.ID, clientv3.WithAttachedKeys())
	require.NoError(t, err)
	require.Empty(t, ttlAfter.Keys)

	historical, err := client.Get(ctx, key, clientv3.WithRev(update.Header.Revision))
	require.NoError(t, err)
	require.Len(t, historical.Kvs, 1)
	require.Equal(t, []byte("two"), historical.Kvs[0].Value)
	require.Equal(t, int64(lease.ID), historical.Kvs[0].Lease)

	current, err := client.Get(ctx, key)
	require.NoError(t, err)
	require.NotNil(t, current.Header)
	require.Equal(t, txn.Header.Revision, current.Header.Revision)
	require.Empty(t, current.Kvs)
}

func TestClientNestedTxnDeleteRangeWithLeasesPrevKVMatchesEtcd(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()

	grpcServer := grpc.NewServer(server.ClientServerOptions()...)
	etcdserverpb.RegisterKVServer(grpcServer, server)
	etcdserverpb.RegisterLeaseServer(grpcServer, server)
	listener := bufconn.Listen(1 << 20)
	go func() { _ = grpcServer.Serve(listener) }()
	t.Cleanup(grpcServer.Stop)

	client, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{"bufnet"},
		DialTimeout: time.Second,
		DialOptions: []grpc.DialOption{
			grpc.WithTransportCredentials(insecure.NewCredentials()),
			grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
				return listener.Dial()
			}),
		},
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, client.Close()) })

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	leaseA, err := client.Grant(ctx, 300)
	require.NoError(t, err)
	leaseB, err := client.Grant(ctx, 300)
	require.NoError(t, err)
	prefix := "/a2164/client-nested-txn-delete-range-leased/"
	putA, err := client.Put(ctx, prefix+"a", "va", clientv3.WithLease(leaseA.ID))
	require.NoError(t, err)
	putB, err := client.Put(ctx, prefix+"b", "vb", clientv3.WithLease(leaseB.ID))
	require.NoError(t, err)
	putC, err := client.Put(ctx, prefix+"c", "vc", clientv3.WithLease(leaseA.ID))
	require.NoError(t, err)
	updateB, err := client.Put(ctx, prefix+"b", "vb2", clientv3.WithLease(leaseB.ID))
	require.NoError(t, err)

	ttlABefore, err := client.TimeToLive(ctx, leaseA.ID, clientv3.WithAttachedKeys())
	require.NoError(t, err)
	require.ElementsMatch(t, []string{prefix + "a", prefix + "c"}, leaseClientAttachedKeys(ttlABefore.Keys))
	ttlBBefore, err := client.TimeToLive(ctx, leaseB.ID, clientv3.WithAttachedKeys())
	require.NoError(t, err)
	require.Equal(t, []string{prefix + "b"}, leaseClientAttachedKeys(ttlBBefore.Keys))

	txn, err := client.Txn(ctx).Then(
		clientv3.OpTxn(
			nil,
			[]clientv3.Op{
				clientv3.OpDelete(prefix+"a", clientv3.WithRange(prefix+"c"), clientv3.WithPrevKV()),
				clientv3.OpGet(prefix, clientv3.WithPrefix(), clientv3.WithSort(clientv3.SortByKey, clientv3.SortAscend)),
			},
			nil,
		),
	).Commit()
	require.NoError(t, err)
	require.True(t, txn.Succeeded)
	require.NotNil(t, txn.Header)
	require.Equal(t, updateB.Header.Revision+1, txn.Header.Revision)
	require.Len(t, txn.Responses, 1)

	nested := txn.Responses[0].GetResponseTxn()
	require.NotNil(t, nested)
	require.NotNil(t, nested.Header)
	require.Zero(t, nested.Header.Revision)
	require.True(t, nested.Succeeded)
	require.Len(t, nested.Responses, 2)

	deleted := nested.Responses[0].GetResponseDeleteRange()
	require.NotNil(t, deleted)
	require.NotNil(t, deleted.Header)
	require.Equal(t, txn.Header.Revision, deleted.Header.Revision)
	require.Equal(t, int64(2), deleted.Deleted)
	require.Len(t, deleted.PrevKvs, 2)
	require.Equal(t, []byte(prefix+"a"), deleted.PrevKvs[0].Key)
	require.Equal(t, []byte("va"), deleted.PrevKvs[0].Value)
	require.Equal(t, putA.Header.Revision, deleted.PrevKvs[0].CreateRevision)
	require.Equal(t, putA.Header.Revision, deleted.PrevKvs[0].ModRevision)
	require.Equal(t, int64(1), deleted.PrevKvs[0].Version)
	require.Equal(t, int64(leaseA.ID), deleted.PrevKvs[0].Lease)
	require.Equal(t, []byte(prefix+"b"), deleted.PrevKvs[1].Key)
	require.Equal(t, []byte("vb2"), deleted.PrevKvs[1].Value)
	require.Equal(t, putB.Header.Revision, deleted.PrevKvs[1].CreateRevision)
	require.Equal(t, updateB.Header.Revision, deleted.PrevKvs[1].ModRevision)
	require.Equal(t, int64(2), deleted.PrevKvs[1].Version)
	require.Equal(t, int64(leaseB.ID), deleted.PrevKvs[1].Lease)

	staged := nested.Responses[1].GetResponseRange()
	require.NotNil(t, staged)
	require.NotNil(t, staged.Header)
	require.Equal(t, txn.Header.Revision, staged.Header.Revision)
	require.Equal(t, []txnClientKV{
		{Key: "c", Value: "vc", Version: 1},
	}, txnClientKVs(staged.Kvs, prefix, txn.Header.Revision))
	require.Equal(t, int64(leaseA.ID), staged.Kvs[0].Lease)

	ttlAAfter, err := client.TimeToLive(ctx, leaseA.ID, clientv3.WithAttachedKeys())
	require.NoError(t, err)
	require.Equal(t, []string{prefix + "c"}, leaseClientAttachedKeys(ttlAAfter.Keys))
	ttlBAfter, err := client.TimeToLive(ctx, leaseB.ID, clientv3.WithAttachedKeys())
	require.NoError(t, err)
	require.Empty(t, ttlBAfter.Keys)

	historical, err := client.Get(ctx, prefix,
		clientv3.WithPrefix(),
		clientv3.WithRev(updateB.Header.Revision),
		clientv3.WithSort(clientv3.SortByKey, clientv3.SortAscend),
	)
	require.NoError(t, err)
	require.Equal(t, []txnClientKV{
		{Key: "a", Value: "va", Version: 1},
		{Key: "b", Value: "vb2", Version: 2},
		{Key: "c", Value: "vc", Version: 1},
	}, txnClientKVs(historical.Kvs, prefix, txn.Header.Revision))
	require.Equal(t, int64(leaseA.ID), historical.Kvs[0].Lease)
	require.Equal(t, int64(leaseB.ID), historical.Kvs[1].Lease)
	require.Equal(t, int64(leaseA.ID), historical.Kvs[2].Lease)
	require.Equal(t, putC.Header.Revision, historical.Kvs[2].CreateRevision)
}

func TestClientNestedTxnDeletePrefixWithLeasesPrevKVMatchesEtcd(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()

	grpcServer := grpc.NewServer(server.ClientServerOptions()...)
	etcdserverpb.RegisterKVServer(grpcServer, server)
	etcdserverpb.RegisterLeaseServer(grpcServer, server)
	listener := bufconn.Listen(1 << 20)
	go func() { _ = grpcServer.Serve(listener) }()
	t.Cleanup(grpcServer.Stop)

	client, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{"bufnet"},
		DialTimeout: time.Second,
		DialOptions: []grpc.DialOption{
			grpc.WithTransportCredentials(insecure.NewCredentials()),
			grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
				return listener.Dial()
			}),
		},
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, client.Close()) })

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	leaseA, err := client.Grant(ctx, 300)
	require.NoError(t, err)
	leaseB, err := client.Grant(ctx, 300)
	require.NoError(t, err)
	root := "/a2165/client-nested-txn-delete-prefix-leased/"
	prefix := root + "items/"
	putA, err := client.Put(ctx, prefix+"a", "va", clientv3.WithLease(leaseA.ID))
	require.NoError(t, err)
	putB, err := client.Put(ctx, prefix+"b", "vb", clientv3.WithLease(leaseB.ID))
	require.NoError(t, err)
	outsideA, err := client.Put(ctx, root+"outside-a", "outside-a", clientv3.WithLease(leaseA.ID))
	require.NoError(t, err)
	outsideB, err := client.Put(ctx, root+"outside-b", "outside-b", clientv3.WithLease(leaseB.ID))
	require.NoError(t, err)
	updateB, err := client.Put(ctx, prefix+"b", "vb2", clientv3.WithLease(leaseB.ID))
	require.NoError(t, err)

	ttlABefore, err := client.TimeToLive(ctx, leaseA.ID, clientv3.WithAttachedKeys())
	require.NoError(t, err)
	require.ElementsMatch(t, []string{prefix + "a", root + "outside-a"}, leaseClientAttachedKeys(ttlABefore.Keys))
	ttlBBefore, err := client.TimeToLive(ctx, leaseB.ID, clientv3.WithAttachedKeys())
	require.NoError(t, err)
	require.ElementsMatch(t, []string{prefix + "b", root + "outside-b"}, leaseClientAttachedKeys(ttlBBefore.Keys))

	txn, err := client.Txn(ctx).Then(
		clientv3.OpTxn(
			nil,
			[]clientv3.Op{
				clientv3.OpDelete(prefix, clientv3.WithPrefix(), clientv3.WithPrevKV()),
				clientv3.OpGet(root, clientv3.WithPrefix(), clientv3.WithSort(clientv3.SortByKey, clientv3.SortAscend)),
			},
			nil,
		),
	).Commit()
	require.NoError(t, err)
	require.True(t, txn.Succeeded)
	require.NotNil(t, txn.Header)
	require.Equal(t, updateB.Header.Revision+1, txn.Header.Revision)
	require.Len(t, txn.Responses, 1)

	nested := txn.Responses[0].GetResponseTxn()
	require.NotNil(t, nested)
	require.NotNil(t, nested.Header)
	require.Zero(t, nested.Header.Revision)
	require.True(t, nested.Succeeded)
	require.Len(t, nested.Responses, 2)

	deleted := nested.Responses[0].GetResponseDeleteRange()
	require.NotNil(t, deleted)
	require.NotNil(t, deleted.Header)
	require.Equal(t, txn.Header.Revision, deleted.Header.Revision)
	require.Equal(t, int64(2), deleted.Deleted)
	require.Len(t, deleted.PrevKvs, 2)
	require.Equal(t, []byte(prefix+"a"), deleted.PrevKvs[0].Key)
	require.Equal(t, []byte("va"), deleted.PrevKvs[0].Value)
	require.Equal(t, putA.Header.Revision, deleted.PrevKvs[0].CreateRevision)
	require.Equal(t, putA.Header.Revision, deleted.PrevKvs[0].ModRevision)
	require.Equal(t, int64(1), deleted.PrevKvs[0].Version)
	require.Equal(t, int64(leaseA.ID), deleted.PrevKvs[0].Lease)
	require.Equal(t, []byte(prefix+"b"), deleted.PrevKvs[1].Key)
	require.Equal(t, []byte("vb2"), deleted.PrevKvs[1].Value)
	require.Equal(t, putB.Header.Revision, deleted.PrevKvs[1].CreateRevision)
	require.Equal(t, updateB.Header.Revision, deleted.PrevKvs[1].ModRevision)
	require.Equal(t, int64(2), deleted.PrevKvs[1].Version)
	require.Equal(t, int64(leaseB.ID), deleted.PrevKvs[1].Lease)

	staged := nested.Responses[1].GetResponseRange()
	require.NotNil(t, staged)
	require.NotNil(t, staged.Header)
	require.Equal(t, txn.Header.Revision, staged.Header.Revision)
	require.Equal(t, []txnClientKV{
		{Key: "outside-a", Value: "outside-a", Version: 1},
		{Key: "outside-b", Value: "outside-b", Version: 1},
	}, txnClientKVs(staged.Kvs, root, txn.Header.Revision))
	require.Equal(t, int64(leaseA.ID), staged.Kvs[0].Lease)
	require.Equal(t, int64(leaseB.ID), staged.Kvs[1].Lease)

	ttlAAfter, err := client.TimeToLive(ctx, leaseA.ID, clientv3.WithAttachedKeys())
	require.NoError(t, err)
	require.Equal(t, []string{root + "outside-a"}, leaseClientAttachedKeys(ttlAAfter.Keys))
	ttlBAfter, err := client.TimeToLive(ctx, leaseB.ID, clientv3.WithAttachedKeys())
	require.NoError(t, err)
	require.Equal(t, []string{root + "outside-b"}, leaseClientAttachedKeys(ttlBAfter.Keys))

	historical, err := client.Get(ctx, root,
		clientv3.WithPrefix(),
		clientv3.WithRev(updateB.Header.Revision),
		clientv3.WithSort(clientv3.SortByKey, clientv3.SortAscend),
	)
	require.NoError(t, err)
	require.Equal(t, []txnClientKV{
		{Key: "items/a", Value: "va", Version: 1},
		{Key: "items/b", Value: "vb2", Version: 2},
		{Key: "outside-a", Value: "outside-a", Version: 1},
		{Key: "outside-b", Value: "outside-b", Version: 1},
	}, txnClientKVs(historical.Kvs, root, txn.Header.Revision))
	require.Equal(t, int64(leaseA.ID), historical.Kvs[0].Lease)
	require.Equal(t, int64(leaseB.ID), historical.Kvs[1].Lease)
	require.Equal(t, int64(leaseA.ID), historical.Kvs[2].Lease)
	require.Equal(t, int64(leaseB.ID), historical.Kvs[3].Lease)
	require.Equal(t, outsideA.Header.Revision, historical.Kvs[2].CreateRevision)
	require.Equal(t, outsideB.Header.Revision, historical.Kvs[3].CreateRevision)
}

func TestClientNestedTxnDeleteFromKeyWithLeasesPrevKVMatchesEtcd(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()

	grpcServer := grpc.NewServer(server.ClientServerOptions()...)
	etcdserverpb.RegisterKVServer(grpcServer, server)
	etcdserverpb.RegisterLeaseServer(grpcServer, server)
	listener := bufconn.Listen(1 << 20)
	go func() { _ = grpcServer.Serve(listener) }()
	t.Cleanup(grpcServer.Stop)

	client, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{"bufnet"},
		DialTimeout: time.Second,
		DialOptions: []grpc.DialOption{
			grpc.WithTransportCredentials(insecure.NewCredentials()),
			grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
				return listener.Dial()
			}),
		},
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, client.Close()) })

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	leaseA, err := client.Grant(ctx, 300)
	require.NoError(t, err)
	leaseB, err := client.Grant(ctx, 300)
	require.NoError(t, err)
	prefix := "/a2166/client-nested-txn-delete-fromkey-leased/"
	before, err := client.Put(ctx, prefix+"a", "va", clientv3.WithLease(leaseA.ID))
	require.NoError(t, err)
	putB, err := client.Put(ctx, prefix+"b", "vb", clientv3.WithLease(leaseA.ID))
	require.NoError(t, err)
	putC, err := client.Put(ctx, prefix+"c", "vc", clientv3.WithLease(leaseB.ID))
	require.NoError(t, err)
	putD, err := client.Put(ctx, prefix+"d", "vd", clientv3.WithLease(leaseA.ID))
	require.NoError(t, err)
	updateC, err := client.Put(ctx, prefix+"c", "vc2", clientv3.WithLease(leaseB.ID))
	require.NoError(t, err)

	ttlABefore, err := client.TimeToLive(ctx, leaseA.ID, clientv3.WithAttachedKeys())
	require.NoError(t, err)
	require.ElementsMatch(t, []string{prefix + "a", prefix + "b", prefix + "d"}, leaseClientAttachedKeys(ttlABefore.Keys))
	ttlBBefore, err := client.TimeToLive(ctx, leaseB.ID, clientv3.WithAttachedKeys())
	require.NoError(t, err)
	require.Equal(t, []string{prefix + "c"}, leaseClientAttachedKeys(ttlBBefore.Keys))

	txn, err := client.Txn(ctx).Then(
		clientv3.OpTxn(
			nil,
			[]clientv3.Op{
				clientv3.OpDelete(prefix+"b", clientv3.WithFromKey(), clientv3.WithPrevKV()),
				clientv3.OpGet(prefix, clientv3.WithPrefix(), clientv3.WithSort(clientv3.SortByKey, clientv3.SortAscend)),
			},
			nil,
		),
	).Commit()
	require.NoError(t, err)
	require.True(t, txn.Succeeded)
	require.NotNil(t, txn.Header)
	require.Equal(t, updateC.Header.Revision+1, txn.Header.Revision)
	require.Len(t, txn.Responses, 1)

	nested := txn.Responses[0].GetResponseTxn()
	require.NotNil(t, nested)
	require.NotNil(t, nested.Header)
	require.Zero(t, nested.Header.Revision)
	require.True(t, nested.Succeeded)
	require.Len(t, nested.Responses, 2)

	deleted := nested.Responses[0].GetResponseDeleteRange()
	require.NotNil(t, deleted)
	require.NotNil(t, deleted.Header)
	require.Equal(t, txn.Header.Revision, deleted.Header.Revision)
	require.Equal(t, int64(3), deleted.Deleted)
	require.Len(t, deleted.PrevKvs, 3)
	require.Equal(t, []byte(prefix+"b"), deleted.PrevKvs[0].Key)
	require.Equal(t, []byte("vb"), deleted.PrevKvs[0].Value)
	require.Equal(t, putB.Header.Revision, deleted.PrevKvs[0].CreateRevision)
	require.Equal(t, putB.Header.Revision, deleted.PrevKvs[0].ModRevision)
	require.Equal(t, int64(1), deleted.PrevKvs[0].Version)
	require.Equal(t, int64(leaseA.ID), deleted.PrevKvs[0].Lease)
	require.Equal(t, []byte(prefix+"c"), deleted.PrevKvs[1].Key)
	require.Equal(t, []byte("vc2"), deleted.PrevKvs[1].Value)
	require.Equal(t, putC.Header.Revision, deleted.PrevKvs[1].CreateRevision)
	require.Equal(t, updateC.Header.Revision, deleted.PrevKvs[1].ModRevision)
	require.Equal(t, int64(2), deleted.PrevKvs[1].Version)
	require.Equal(t, int64(leaseB.ID), deleted.PrevKvs[1].Lease)
	require.Equal(t, []byte(prefix+"d"), deleted.PrevKvs[2].Key)
	require.Equal(t, []byte("vd"), deleted.PrevKvs[2].Value)
	require.Equal(t, putD.Header.Revision, deleted.PrevKvs[2].CreateRevision)
	require.Equal(t, putD.Header.Revision, deleted.PrevKvs[2].ModRevision)
	require.Equal(t, int64(1), deleted.PrevKvs[2].Version)
	require.Equal(t, int64(leaseA.ID), deleted.PrevKvs[2].Lease)

	staged := nested.Responses[1].GetResponseRange()
	require.NotNil(t, staged)
	require.NotNil(t, staged.Header)
	require.Equal(t, txn.Header.Revision, staged.Header.Revision)
	require.Equal(t, []txnClientKV{
		{Key: "a", Value: "va", Version: 1},
	}, txnClientKVs(staged.Kvs, prefix, txn.Header.Revision))
	require.Equal(t, int64(leaseA.ID), staged.Kvs[0].Lease)

	ttlAAfter, err := client.TimeToLive(ctx, leaseA.ID, clientv3.WithAttachedKeys())
	require.NoError(t, err)
	require.Equal(t, []string{prefix + "a"}, leaseClientAttachedKeys(ttlAAfter.Keys))
	ttlBAfter, err := client.TimeToLive(ctx, leaseB.ID, clientv3.WithAttachedKeys())
	require.NoError(t, err)
	require.Empty(t, ttlBAfter.Keys)

	historical, err := client.Get(ctx, prefix,
		clientv3.WithPrefix(),
		clientv3.WithRev(updateC.Header.Revision),
		clientv3.WithSort(clientv3.SortByKey, clientv3.SortAscend),
	)
	require.NoError(t, err)
	require.Equal(t, []txnClientKV{
		{Key: "a", Value: "va", Version: 1},
		{Key: "b", Value: "vb", Version: 1},
		{Key: "c", Value: "vc2", Version: 2},
		{Key: "d", Value: "vd", Version: 1},
	}, txnClientKVs(historical.Kvs, prefix, txn.Header.Revision))
	require.Equal(t, int64(leaseA.ID), historical.Kvs[0].Lease)
	require.Equal(t, int64(leaseA.ID), historical.Kvs[1].Lease)
	require.Equal(t, int64(leaseB.ID), historical.Kvs[2].Lease)
	require.Equal(t, int64(leaseA.ID), historical.Kvs[3].Lease)
	require.Equal(t, before.Header.Revision, historical.Kvs[0].CreateRevision)
}

func TestClientNestedTxnNoOpDeleteWithPrevKVDoesNotConsumeRevisionMatchesEtcd(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()

	grpcServer := grpc.NewServer(server.ClientServerOptions()...)
	etcdserverpb.RegisterKVServer(grpcServer, server)
	listener := bufconn.Listen(1 << 20)
	go func() { _ = grpcServer.Serve(listener) }()
	t.Cleanup(grpcServer.Stop)

	client, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{"bufnet"},
		DialTimeout: time.Second,
		DialOptions: []grpc.DialOption{
			grpc.WithTransportCredentials(insecure.NewCredentials()),
			grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
				return listener.Dial()
			}),
		},
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, client.Close()) })

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	prefix := "/a2167/client-nested-txn-noop-delete-prevkv/"
	putA, err := client.Put(ctx, prefix+"a", "va")
	require.NoError(t, err)
	putB, err := client.Put(ctx, prefix+"b", "vb")
	require.NoError(t, err)

	txn, err := client.Txn(ctx).Then(
		clientv3.OpTxn(
			nil,
			[]clientv3.Op{
				clientv3.OpDelete(prefix+"missing", clientv3.WithPrevKV()),
				clientv3.OpDelete(prefix+"b", clientv3.WithRange(prefix+"b"), clientv3.WithPrevKV()),
				clientv3.OpDelete(prefix+"c", clientv3.WithRange(prefix+"b"), clientv3.WithPrevKV()),
				clientv3.OpGet(prefix, clientv3.WithPrefix(), clientv3.WithSort(clientv3.SortByKey, clientv3.SortAscend)),
			},
			nil,
		),
	).Commit()
	require.NoError(t, err)
	require.True(t, txn.Succeeded)
	require.NotNil(t, txn.Header)
	require.Equal(t, putB.Header.Revision, txn.Header.Revision)
	require.Len(t, txn.Responses, 1)

	nested := txn.Responses[0].GetResponseTxn()
	require.NotNil(t, nested)
	require.NotNil(t, nested.Header)
	require.Zero(t, nested.Header.Revision)
	require.True(t, nested.Succeeded)
	require.Len(t, nested.Responses, 4)

	for i := 0; i < 3; i++ {
		deleted := nested.Responses[i].GetResponseDeleteRange()
		require.NotNil(t, deleted)
		require.NotNil(t, deleted.Header)
		require.Equal(t, txn.Header.Revision, deleted.Header.Revision)
		require.Zero(t, deleted.Deleted)
		require.Empty(t, deleted.PrevKvs)
	}

	current := nested.Responses[3].GetResponseRange()
	require.NotNil(t, current)
	require.NotNil(t, current.Header)
	require.Equal(t, txn.Header.Revision, current.Header.Revision)
	require.Len(t, current.Kvs, 2)
	require.Equal(t, []byte(prefix+"a"), current.Kvs[0].Key)
	require.Equal(t, []byte("va"), current.Kvs[0].Value)
	require.Equal(t, putA.Header.Revision, current.Kvs[0].CreateRevision)
	require.Equal(t, putA.Header.Revision, current.Kvs[0].ModRevision)
	require.Equal(t, int64(1), current.Kvs[0].Version)
	require.Equal(t, []byte(prefix+"b"), current.Kvs[1].Key)
	require.Equal(t, []byte("vb"), current.Kvs[1].Value)
	require.Equal(t, putB.Header.Revision, current.Kvs[1].CreateRevision)
	require.Equal(t, putB.Header.Revision, current.Kvs[1].ModRevision)
	require.Equal(t, int64(1), current.Kvs[1].Version)

	after, err := client.Get(ctx, prefix, clientv3.WithPrefix(), clientv3.WithSort(clientv3.SortByKey, clientv3.SortAscend))
	require.NoError(t, err)
	require.NotNil(t, after.Header)
	require.Equal(t, putB.Header.Revision, after.Header.Revision)
	require.Len(t, after.Kvs, 2)
	require.Equal(t, []byte(prefix+"a"), after.Kvs[0].Key)
	require.Equal(t, []byte("va"), after.Kvs[0].Value)
	require.Equal(t, putA.Header.Revision, after.Kvs[0].CreateRevision)
	require.Equal(t, putA.Header.Revision, after.Kvs[0].ModRevision)
	require.Equal(t, int64(1), after.Kvs[0].Version)
	require.Equal(t, []byte(prefix+"b"), after.Kvs[1].Key)
	require.Equal(t, []byte("vb"), after.Kvs[1].Value)
	require.Equal(t, putB.Header.Revision, after.Kvs[1].CreateRevision)
	require.Equal(t, putB.Header.Revision, after.Kvs[1].ModRevision)
	require.Equal(t, int64(1), after.Kvs[1].Version)
}

func TestClientNestedTxnFailureNoOpDeleteWithPrevKVDoesNotConsumeRevisionMatchesEtcd(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()

	grpcServer := grpc.NewServer(server.ClientServerOptions()...)
	etcdserverpb.RegisterKVServer(grpcServer, server)
	listener := bufconn.Listen(1 << 20)
	go func() { _ = grpcServer.Serve(listener) }()
	t.Cleanup(grpcServer.Stop)

	client, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{"bufnet"},
		DialTimeout: time.Second,
		DialOptions: []grpc.DialOption{
			grpc.WithTransportCredentials(insecure.NewCredentials()),
			grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
				return listener.Dial()
			}),
		},
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, client.Close()) })

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	prefix := "/a2168/client-nested-txn-failure-noop-delete-prevkv/"
	putA, err := client.Put(ctx, prefix+"a", "va")
	require.NoError(t, err)
	putB, err := client.Put(ctx, prefix+"b", "vb")
	require.NoError(t, err)

	txn, err := client.Txn(ctx).Then(
		clientv3.OpTxn(
			[]clientv3.Cmp{clientv3.Compare(clientv3.Version(prefix+"missing"), ">", 0)},
			[]clientv3.Op{clientv3.OpPut(prefix+"unexpected", "bad")},
			[]clientv3.Op{
				clientv3.OpDelete(prefix+"missing", clientv3.WithPrevKV()),
				clientv3.OpDelete(prefix+"b", clientv3.WithRange(prefix+"b"), clientv3.WithPrevKV()),
				clientv3.OpDelete(prefix+"c", clientv3.WithRange(prefix+"b"), clientv3.WithPrevKV()),
				clientv3.OpGet(prefix, clientv3.WithPrefix(), clientv3.WithSort(clientv3.SortByKey, clientv3.SortAscend)),
			},
		),
	).Commit()
	require.NoError(t, err)
	require.True(t, txn.Succeeded)
	require.NotNil(t, txn.Header)
	require.Equal(t, putB.Header.Revision, txn.Header.Revision)
	require.Len(t, txn.Responses, 1)

	nested := txn.Responses[0].GetResponseTxn()
	require.NotNil(t, nested)
	require.NotNil(t, nested.Header)
	require.Zero(t, nested.Header.Revision)
	require.False(t, nested.Succeeded)
	require.Len(t, nested.Responses, 4)

	for i := 0; i < 3; i++ {
		deleted := nested.Responses[i].GetResponseDeleteRange()
		require.NotNil(t, deleted)
		require.NotNil(t, deleted.Header)
		require.Equal(t, txn.Header.Revision, deleted.Header.Revision)
		require.Zero(t, deleted.Deleted)
		require.Empty(t, deleted.PrevKvs)
	}

	current := nested.Responses[3].GetResponseRange()
	require.NotNil(t, current)
	require.NotNil(t, current.Header)
	require.Equal(t, txn.Header.Revision, current.Header.Revision)
	require.Len(t, current.Kvs, 2)
	require.Equal(t, []byte(prefix+"a"), current.Kvs[0].Key)
	require.Equal(t, []byte("va"), current.Kvs[0].Value)
	require.Equal(t, putA.Header.Revision, current.Kvs[0].CreateRevision)
	require.Equal(t, putA.Header.Revision, current.Kvs[0].ModRevision)
	require.Equal(t, int64(1), current.Kvs[0].Version)
	require.Equal(t, []byte(prefix+"b"), current.Kvs[1].Key)
	require.Equal(t, []byte("vb"), current.Kvs[1].Value)
	require.Equal(t, putB.Header.Revision, current.Kvs[1].CreateRevision)
	require.Equal(t, putB.Header.Revision, current.Kvs[1].ModRevision)
	require.Equal(t, int64(1), current.Kvs[1].Version)

	after, err := client.Get(ctx, prefix, clientv3.WithPrefix(), clientv3.WithSort(clientv3.SortByKey, clientv3.SortAscend))
	require.NoError(t, err)
	require.NotNil(t, after.Header)
	require.Equal(t, putB.Header.Revision, after.Header.Revision)
	require.Len(t, after.Kvs, 2)
	require.Equal(t, []byte(prefix+"a"), after.Kvs[0].Key)
	require.Equal(t, []byte("va"), after.Kvs[0].Value)
	require.Equal(t, putA.Header.Revision, after.Kvs[0].CreateRevision)
	require.Equal(t, putA.Header.Revision, after.Kvs[0].ModRevision)
	require.Equal(t, int64(1), after.Kvs[0].Version)
	require.Equal(t, []byte(prefix+"b"), after.Kvs[1].Key)
	require.Equal(t, []byte("vb"), after.Kvs[1].Value)
	require.Equal(t, putB.Header.Revision, after.Kvs[1].CreateRevision)
	require.Equal(t, putB.Header.Revision, after.Kvs[1].ModRevision)
	require.Equal(t, int64(1), after.Kvs[1].Version)
}

func TestClientTxnHeaderRevisions(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()

	grpcServer := grpc.NewServer(server.ClientServerOptions()...)
	etcdserverpb.RegisterKVServer(grpcServer, server)
	listener := bufconn.Listen(1 << 20)
	go func() { _ = grpcServer.Serve(listener) }()
	t.Cleanup(grpcServer.Stop)

	client, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{"bufnet"},
		DialTimeout: time.Second,
		DialOptions: []grpc.DialOption{
			grpc.WithTransportCredentials(insecure.NewCredentials()),
			grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
				return listener.Dial()
			}),
		},
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, client.Close()) })

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	prefix := fmt.Sprintf("/a1010/client-txn-revision/%d/", time.Now().UnixNano())
	key := prefix + "key"
	seed, err := client.Put(ctx, key, "one")
	require.NoError(t, err)

	readOnly, err := client.Txn(ctx).Then(clientv3.OpGet(key)).Commit()
	require.NoError(t, err)
	require.True(t, readOnly.Succeeded)
	require.Equal(t, seed.Header.Revision, readOnly.Header.Revision)
	require.Len(t, readOnly.Responses, 1)
	readRange := readOnly.Responses[0].GetResponseRange()
	require.NotNil(t, readRange)
	require.Equal(t, seed.Header.Revision, readRange.Header.Revision)
	require.Len(t, readRange.Kvs, 1)
	require.Equal(t, seed.Header.Revision, readRange.Kvs[0].ModRevision)

	emptyDelete, err := client.Txn(ctx).Then(clientv3.OpDelete(prefix + "missing")).Commit()
	require.NoError(t, err)
	require.True(t, emptyDelete.Succeeded)
	require.Equal(t, seed.Header.Revision, emptyDelete.Header.Revision)
	require.Len(t, emptyDelete.Responses, 1)
	emptyDeleteResp := emptyDelete.Responses[0].GetResponseDeleteRange()
	require.NotNil(t, emptyDeleteResp)
	require.Zero(t, emptyDeleteResp.Deleted)
	require.Equal(t, seed.Header.Revision, emptyDeleteResp.Header.Revision)

	write, err := client.Txn(ctx).Then(clientv3.OpPut(key, "two")).Commit()
	require.NoError(t, err)
	require.True(t, write.Succeeded)
	require.Equal(t, seed.Header.Revision+1, write.Header.Revision)
	require.Len(t, write.Responses, 1)
	putResp := write.Responses[0].GetResponsePut()
	require.NotNil(t, putResp)
	require.Equal(t, write.Header.Revision, putResp.Header.Revision)

	failure, err := client.Txn(ctx).
		If(clientv3.Compare(clientv3.Version(key), "=", 0)).
		Then(clientv3.OpPut(key, "created")).
		Else(clientv3.OpGet(key)).
		Commit()
	require.NoError(t, err)
	require.False(t, failure.Succeeded)
	require.Equal(t, write.Header.Revision, failure.Header.Revision)
	require.Len(t, failure.Responses, 1)
	failureRange := failure.Responses[0].GetResponseRange()
	require.NotNil(t, failureRange)
	require.Equal(t, write.Header.Revision, failureRange.Header.Revision)
	require.Len(t, failureRange.Kvs, 1)
	require.Equal(t, "two", string(failureRange.Kvs[0].Value))
	require.Equal(t, write.Header.Revision, failureRange.Kvs[0].ModRevision)
}

func TestClientTxnRangeRevisionBoundaries(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()

	grpcServer := grpc.NewServer(server.ClientServerOptions()...)
	etcdserverpb.RegisterKVServer(grpcServer, server)
	listener := bufconn.Listen(1 << 20)
	go func() { _ = grpcServer.Serve(listener) }()
	t.Cleanup(grpcServer.Stop)

	client, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{"bufnet"},
		DialTimeout: time.Second,
		DialOptions: []grpc.DialOption{
			grpc.WithTransportCredentials(insecure.NewCredentials()),
			grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
				return listener.Dial()
			}),
		},
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, client.Close()) })

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	prefix := fmt.Sprintf("/a1037/txn-range-revision/%d/", time.Now().UnixNano())
	key := prefix + "key"
	writeKey := prefix + "write"
	seed, err := client.Put(ctx, key, "value")
	require.NoError(t, err)
	_, err = client.Compact(ctx, seed.Header.Revision)
	require.NoError(t, err)

	_, err = client.Txn(ctx).
		Then(
			clientv3.OpGet(key, clientv3.WithRev(-1)),
			clientv3.OpPut(writeKey, "must-not-commit"),
		).
		Commit()
	requireClientTxnError(t, err, codes.Unknown, "etcdserver: mvcc: required revision has been compacted", rpctypes.ErrCompacted)
	afterCompacted, err := client.Get(ctx, writeKey)
	require.NoError(t, err)
	require.NotNil(t, afterCompacted.Header)
	require.Empty(t, afterCompacted.Kvs)

	unselected, err := client.Txn(ctx).
		If(clientv3.Compare(clientv3.Version(key), "=", 0)).
		Then(clientv3.OpGet(key, clientv3.WithRev(-1))).
		Else(clientv3.OpGet(key)).
		Commit()
	require.NoError(t, err)
	require.False(t, unselected.Succeeded)
	require.NotNil(t, unselected.Header)
	require.Equal(t, afterCompacted.Header.Revision, unselected.Header.Revision)
	require.Len(t, unselected.Responses, 1)
	unselectedRange := unselected.Responses[0].GetResponseRange()
	require.NotNil(t, unselectedRange)
	require.NotNil(t, unselectedRange.Header)
	require.Equal(t, unselected.Header.Revision, unselectedRange.Header.Revision)
	require.Len(t, unselectedRange.Kvs, 1)

	_, err = client.Txn(ctx).
		Then(
			clientv3.OpGet(key, clientv3.WithRev(math.MaxInt64)),
			clientv3.OpPut(writeKey, "must-not-commit"),
		).
		Commit()
	requireClientTxnError(t, err, codes.Unknown, "etcdserver: mvcc: required revision is a future revision", rpctypes.ErrFutureRev)
	afterFuture, err := client.Get(ctx, writeKey)
	require.NoError(t, err)
	require.Empty(t, afterFuture.Kvs)
}

func TestClientTxnUnselectedFutureRevisionBranchIsIgnoredMatchesEtcd(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()

	grpcServer := grpc.NewServer(server.ClientServerOptions()...)
	etcdserverpb.RegisterKVServer(grpcServer, server)
	listener := bufconn.Listen(1 << 20)
	go func() { _ = grpcServer.Serve(listener) }()
	t.Cleanup(grpcServer.Stop)

	client, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{"bufnet"},
		DialTimeout: time.Second,
		DialOptions: []grpc.DialOption{
			grpc.WithTransportCredentials(insecure.NewCredentials()),
			grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
				return listener.Dial()
			}),
		},
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, client.Close()) })

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	prefix := "/a2181/client-txn-unselected-future-revision/"
	key := prefix + "key"
	unselectedWriteKey := prefix + "unselected-write"
	seed, err := client.Put(ctx, key, "value")
	require.NoError(t, err)

	txn, err := client.Txn(ctx).
		If(clientv3.Compare(clientv3.Version(prefix+"missing"), ">", 0)).
		Then(
			clientv3.OpGet(key, clientv3.WithRev(math.MaxInt64)),
			clientv3.OpPut(unselectedWriteKey, "must-not-commit"),
		).
		Else(clientv3.OpGet(key)).
		Commit()
	require.NoError(t, err)
	require.False(t, txn.Succeeded)
	require.NotNil(t, txn.Header)
	require.Equal(t, seed.Header.Revision, txn.Header.Revision)
	require.Len(t, txn.Responses, 1)

	selected := txn.Responses[0].GetResponseRange()
	require.NotNil(t, selected)
	require.NotNil(t, selected.Header)
	require.Equal(t, txn.Header.Revision, selected.Header.Revision)
	require.Len(t, selected.Kvs, 1)
	require.Equal(t, []byte(key), selected.Kvs[0].Key)
	require.Equal(t, []byte("value"), selected.Kvs[0].Value)
	require.Equal(t, seed.Header.Revision, selected.Kvs[0].CreateRevision)
	require.Equal(t, seed.Header.Revision, selected.Kvs[0].ModRevision)
	require.Equal(t, int64(1), selected.Kvs[0].Version)

	after, err := client.Get(ctx, prefix, clientv3.WithPrefix(), clientv3.WithSort(clientv3.SortByKey, clientv3.SortAscend))
	require.NoError(t, err)
	require.NotNil(t, after.Header)
	require.Equal(t, seed.Header.Revision, after.Header.Revision)
	require.Len(t, after.Kvs, 1)
	require.Equal(t, []byte(key), after.Kvs[0].Key)
	require.Equal(t, []byte("value"), after.Kvs[0].Value)
	require.Equal(t, seed.Header.Revision, after.Kvs[0].CreateRevision)
	require.Equal(t, seed.Header.Revision, after.Kvs[0].ModRevision)
	require.Equal(t, int64(1), after.Kvs[0].Version)
}

func TestClientTxnUnselectedCompactedRevisionBranchIsIgnoredMatchesEtcd(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()

	grpcServer := grpc.NewServer(server.ClientServerOptions()...)
	etcdserverpb.RegisterKVServer(grpcServer, server)
	listener := bufconn.Listen(1 << 20)
	go func() { _ = grpcServer.Serve(listener) }()
	t.Cleanup(grpcServer.Stop)

	client, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{"bufnet"},
		DialTimeout: time.Second,
		DialOptions: []grpc.DialOption{
			grpc.WithTransportCredentials(insecure.NewCredentials()),
			grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
				return listener.Dial()
			}),
		},
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, client.Close()) })

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	prefix := "/a2182/client-txn-unselected-compacted-revision/"
	key := prefix + "key"
	unselectedWriteKey := prefix + "unselected-write"
	seed, err := client.Put(ctx, key, "value")
	require.NoError(t, err)
	compactedRev := seed.Header.Revision - 1
	require.Positive(t, compactedRev)
	compact, err := client.Compact(ctx, seed.Header.Revision)
	require.NoError(t, err)

	txn, err := client.Txn(ctx).
		If(clientv3.Compare(clientv3.Version(prefix+"missing"), ">", 0)).
		Then(
			clientv3.OpGet(key, clientv3.WithRev(compactedRev)),
			clientv3.OpPut(unselectedWriteKey, "must-not-commit"),
		).
		Else(clientv3.OpGet(key)).
		Commit()
	require.NoError(t, err)
	require.False(t, txn.Succeeded)
	require.NotNil(t, txn.Header)
	require.GreaterOrEqual(t, txn.Header.Revision, compact.Header.Revision)
	require.Len(t, txn.Responses, 1)

	selected := txn.Responses[0].GetResponseRange()
	require.NotNil(t, selected)
	require.NotNil(t, selected.Header)
	require.Equal(t, txn.Header.Revision, selected.Header.Revision)
	require.Len(t, selected.Kvs, 1)
	require.Equal(t, []byte(key), selected.Kvs[0].Key)
	require.Equal(t, []byte("value"), selected.Kvs[0].Value)
	require.Equal(t, seed.Header.Revision, selected.Kvs[0].CreateRevision)
	require.Equal(t, seed.Header.Revision, selected.Kvs[0].ModRevision)
	require.Equal(t, int64(1), selected.Kvs[0].Version)

	after, err := client.Get(ctx, prefix, clientv3.WithPrefix(), clientv3.WithSort(clientv3.SortByKey, clientv3.SortAscend))
	require.NoError(t, err)
	require.NotNil(t, after.Header)
	require.GreaterOrEqual(t, after.Header.Revision, txn.Header.Revision)
	require.Len(t, after.Kvs, 1)
	require.Equal(t, []byte(key), after.Kvs[0].Key)
	require.Equal(t, []byte("value"), after.Kvs[0].Value)
	require.Equal(t, seed.Header.Revision, after.Kvs[0].CreateRevision)
	require.Equal(t, seed.Header.Revision, after.Kvs[0].ModRevision)
	require.Equal(t, int64(1), after.Kvs[0].Version)
}

func TestClientTxnUnselectedFailureFutureRevisionBranchIsIgnoredMatchesEtcd(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()

	grpcServer := grpc.NewServer(server.ClientServerOptions()...)
	etcdserverpb.RegisterKVServer(grpcServer, server)
	listener := bufconn.Listen(1 << 20)
	go func() { _ = grpcServer.Serve(listener) }()
	t.Cleanup(grpcServer.Stop)

	client, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{"bufnet"},
		DialTimeout: time.Second,
		DialOptions: []grpc.DialOption{
			grpc.WithTransportCredentials(insecure.NewCredentials()),
			grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
				return listener.Dial()
			}),
		},
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, client.Close()) })

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	prefix := "/a2183/client-txn-unselected-failure-future-revision/"
	key := prefix + "key"
	unselectedWriteKey := prefix + "unselected-write"
	seed, err := client.Put(ctx, key, "value")
	require.NoError(t, err)

	txn, err := client.Txn(ctx).
		If(clientv3.Compare(clientv3.Version(key), "=", 1)).
		Then(clientv3.OpGet(key)).
		Else(
			clientv3.OpGet(key, clientv3.WithRev(math.MaxInt64)),
			clientv3.OpPut(unselectedWriteKey, "must-not-commit"),
		).
		Commit()
	require.NoError(t, err)
	require.True(t, txn.Succeeded)
	require.NotNil(t, txn.Header)
	require.Equal(t, seed.Header.Revision, txn.Header.Revision)
	require.Len(t, txn.Responses, 1)

	selected := txn.Responses[0].GetResponseRange()
	require.NotNil(t, selected)
	require.NotNil(t, selected.Header)
	require.Equal(t, txn.Header.Revision, selected.Header.Revision)
	require.Len(t, selected.Kvs, 1)
	require.Equal(t, []byte(key), selected.Kvs[0].Key)
	require.Equal(t, []byte("value"), selected.Kvs[0].Value)
	require.Equal(t, seed.Header.Revision, selected.Kvs[0].CreateRevision)
	require.Equal(t, seed.Header.Revision, selected.Kvs[0].ModRevision)
	require.Equal(t, int64(1), selected.Kvs[0].Version)

	after, err := client.Get(ctx, prefix, clientv3.WithPrefix(), clientv3.WithSort(clientv3.SortByKey, clientv3.SortAscend))
	require.NoError(t, err)
	require.NotNil(t, after.Header)
	require.Equal(t, seed.Header.Revision, after.Header.Revision)
	require.Len(t, after.Kvs, 1)
	require.Equal(t, []byte(key), after.Kvs[0].Key)
	require.Equal(t, []byte("value"), after.Kvs[0].Value)
	require.Equal(t, seed.Header.Revision, after.Kvs[0].CreateRevision)
	require.Equal(t, seed.Header.Revision, after.Kvs[0].ModRevision)
	require.Equal(t, int64(1), after.Kvs[0].Version)
}

func TestClientTxnUnselectedFailureCompactedRevisionBranchIsIgnoredMatchesEtcd(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()

	grpcServer := grpc.NewServer(server.ClientServerOptions()...)
	etcdserverpb.RegisterKVServer(grpcServer, server)
	listener := bufconn.Listen(1 << 20)
	go func() { _ = grpcServer.Serve(listener) }()
	t.Cleanup(grpcServer.Stop)

	client, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{"bufnet"},
		DialTimeout: time.Second,
		DialOptions: []grpc.DialOption{
			grpc.WithTransportCredentials(insecure.NewCredentials()),
			grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
				return listener.Dial()
			}),
		},
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, client.Close()) })

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	prefix := "/a2184/client-txn-unselected-failure-compacted-revision/"
	key := prefix + "key"
	unselectedWriteKey := prefix + "unselected-write"
	seed, err := client.Put(ctx, key, "value")
	require.NoError(t, err)
	compactedRev := seed.Header.Revision - 1
	require.Positive(t, compactedRev)
	compact, err := client.Compact(ctx, seed.Header.Revision)
	require.NoError(t, err)

	txn, err := client.Txn(ctx).
		If(clientv3.Compare(clientv3.Version(key), "=", 1)).
		Then(clientv3.OpGet(key)).
		Else(
			clientv3.OpGet(key, clientv3.WithRev(compactedRev)),
			clientv3.OpPut(unselectedWriteKey, "must-not-commit"),
		).
		Commit()
	require.NoError(t, err)
	require.True(t, txn.Succeeded)
	require.NotNil(t, txn.Header)
	require.GreaterOrEqual(t, txn.Header.Revision, compact.Header.Revision)
	require.Len(t, txn.Responses, 1)

	selected := txn.Responses[0].GetResponseRange()
	require.NotNil(t, selected)
	require.NotNil(t, selected.Header)
	require.Equal(t, txn.Header.Revision, selected.Header.Revision)
	require.Len(t, selected.Kvs, 1)
	require.Equal(t, []byte(key), selected.Kvs[0].Key)
	require.Equal(t, []byte("value"), selected.Kvs[0].Value)
	require.Equal(t, seed.Header.Revision, selected.Kvs[0].CreateRevision)
	require.Equal(t, seed.Header.Revision, selected.Kvs[0].ModRevision)
	require.Equal(t, int64(1), selected.Kvs[0].Version)

	after, err := client.Get(ctx, prefix, clientv3.WithPrefix(), clientv3.WithSort(clientv3.SortByKey, clientv3.SortAscend))
	require.NoError(t, err)
	require.NotNil(t, after.Header)
	require.GreaterOrEqual(t, after.Header.Revision, txn.Header.Revision)
	require.Len(t, after.Kvs, 1)
	require.Equal(t, []byte(key), after.Kvs[0].Key)
	require.Equal(t, []byte("value"), after.Kvs[0].Value)
	require.Equal(t, seed.Header.Revision, after.Kvs[0].CreateRevision)
	require.Equal(t, seed.Header.Revision, after.Kvs[0].ModRevision)
	require.Equal(t, int64(1), after.Kvs[0].Version)
}

func TestClientNestedTxnFutureRevisionRejectsTxnBeforeWritesMatchesEtcd(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()

	grpcServer := grpc.NewServer(server.ClientServerOptions()...)
	etcdserverpb.RegisterKVServer(grpcServer, server)
	listener := bufconn.Listen(1 << 20)
	go func() { _ = grpcServer.Serve(listener) }()
	t.Cleanup(grpcServer.Stop)

	client, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{"bufnet"},
		DialTimeout: time.Second,
		DialOptions: []grpc.DialOption{
			grpc.WithTransportCredentials(insecure.NewCredentials()),
			grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
				return listener.Dial()
			}),
		},
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, client.Close()) })

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	prefix := "/a2169/client-nested-txn-future-revision/"
	key := prefix + "key"
	nestedWriteKey := prefix + "nested-write"
	outerWriteKey := prefix + "outer-write"
	seed, err := client.Put(ctx, key, "value")
	require.NoError(t, err)

	_, err = client.Txn(ctx).Then(
		clientv3.OpTxn(
			nil,
			[]clientv3.Op{
				clientv3.OpGet(key, clientv3.WithRev(math.MaxInt64)),
				clientv3.OpPut(nestedWriteKey, "must-not-commit"),
			},
			nil,
		),
		clientv3.OpPut(outerWriteKey, "must-not-commit"),
	).Commit()
	requireClientTxnError(t, err, codes.Unknown, "etcdserver: mvcc: required revision is a future revision", rpctypes.ErrFutureRev)

	after, err := client.Get(ctx, prefix, clientv3.WithPrefix(), clientv3.WithSort(clientv3.SortByKey, clientv3.SortAscend))
	require.NoError(t, err)
	require.NotNil(t, after.Header)
	require.Equal(t, seed.Header.Revision, after.Header.Revision)
	require.Len(t, after.Kvs, 1)
	require.Equal(t, []byte(key), after.Kvs[0].Key)
	require.Equal(t, []byte("value"), after.Kvs[0].Value)
	require.Equal(t, seed.Header.Revision, after.Kvs[0].CreateRevision)
	require.Equal(t, seed.Header.Revision, after.Kvs[0].ModRevision)
	require.Equal(t, int64(1), after.Kvs[0].Version)
}

func TestClientNestedTxnUnselectedFutureRevisionBranchIsIgnoredMatchesEtcd(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()

	grpcServer := grpc.NewServer(server.ClientServerOptions()...)
	etcdserverpb.RegisterKVServer(grpcServer, server)
	listener := bufconn.Listen(1 << 20)
	go func() { _ = grpcServer.Serve(listener) }()
	t.Cleanup(grpcServer.Stop)

	client, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{"bufnet"},
		DialTimeout: time.Second,
		DialOptions: []grpc.DialOption{
			grpc.WithTransportCredentials(insecure.NewCredentials()),
			grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
				return listener.Dial()
			}),
		},
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, client.Close()) })

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	prefix := "/a2171/client-nested-txn-unselected-future-revision/"
	key := prefix + "key"
	unselectedWriteKey := prefix + "unselected-write"
	seed, err := client.Put(ctx, key, "value")
	require.NoError(t, err)

	txn, err := client.Txn(ctx).Then(
		clientv3.OpTxn(
			[]clientv3.Cmp{clientv3.Compare(clientv3.Version(prefix+"missing"), ">", 0)},
			[]clientv3.Op{
				clientv3.OpGet(key, clientv3.WithRev(math.MaxInt64)),
				clientv3.OpPut(unselectedWriteKey, "must-not-commit"),
			},
			[]clientv3.Op{
				clientv3.OpGet(key),
			},
		),
	).Commit()
	require.NoError(t, err)
	require.True(t, txn.Succeeded)
	require.NotNil(t, txn.Header)
	require.Equal(t, seed.Header.Revision, txn.Header.Revision)
	require.Len(t, txn.Responses, 1)

	nested := txn.Responses[0].GetResponseTxn()
	require.NotNil(t, nested)
	require.NotNil(t, nested.Header)
	require.Zero(t, nested.Header.Revision)
	require.False(t, nested.Succeeded)
	require.Len(t, nested.Responses, 1)

	selected := nested.Responses[0].GetResponseRange()
	require.NotNil(t, selected)
	require.NotNil(t, selected.Header)
	require.Equal(t, txn.Header.Revision, selected.Header.Revision)
	require.Len(t, selected.Kvs, 1)
	require.Equal(t, []byte(key), selected.Kvs[0].Key)
	require.Equal(t, []byte("value"), selected.Kvs[0].Value)
	require.Equal(t, seed.Header.Revision, selected.Kvs[0].CreateRevision)
	require.Equal(t, seed.Header.Revision, selected.Kvs[0].ModRevision)
	require.Equal(t, int64(1), selected.Kvs[0].Version)

	after, err := client.Get(ctx, prefix, clientv3.WithPrefix(), clientv3.WithSort(clientv3.SortByKey, clientv3.SortAscend))
	require.NoError(t, err)
	require.NotNil(t, after.Header)
	require.Equal(t, seed.Header.Revision, after.Header.Revision)
	require.Len(t, after.Kvs, 1)
	require.Equal(t, []byte(key), after.Kvs[0].Key)
	require.Equal(t, []byte("value"), after.Kvs[0].Value)
	require.Equal(t, seed.Header.Revision, after.Kvs[0].CreateRevision)
	require.Equal(t, seed.Header.Revision, after.Kvs[0].ModRevision)
	require.Equal(t, int64(1), after.Kvs[0].Version)
}

func TestClientNestedTxnUnselectedCompactedRevisionBranchIsIgnoredMatchesEtcd(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()

	grpcServer := grpc.NewServer(server.ClientServerOptions()...)
	etcdserverpb.RegisterKVServer(grpcServer, server)
	listener := bufconn.Listen(1 << 20)
	go func() { _ = grpcServer.Serve(listener) }()
	t.Cleanup(grpcServer.Stop)

	client, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{"bufnet"},
		DialTimeout: time.Second,
		DialOptions: []grpc.DialOption{
			grpc.WithTransportCredentials(insecure.NewCredentials()),
			grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
				return listener.Dial()
			}),
		},
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, client.Close()) })

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	prefix := "/a2172/client-nested-txn-unselected-compacted-revision/"
	key := prefix + "key"
	unselectedWriteKey := prefix + "unselected-write"
	seed, err := client.Put(ctx, key, "value")
	require.NoError(t, err)
	compactedRev := seed.Header.Revision - 1
	require.Positive(t, compactedRev)
	compact, err := client.Compact(ctx, seed.Header.Revision)
	require.NoError(t, err)

	txn, err := client.Txn(ctx).Then(
		clientv3.OpTxn(
			[]clientv3.Cmp{clientv3.Compare(clientv3.Version(prefix+"missing"), ">", 0)},
			[]clientv3.Op{
				clientv3.OpGet(key, clientv3.WithRev(compactedRev)),
				clientv3.OpPut(unselectedWriteKey, "must-not-commit"),
			},
			[]clientv3.Op{
				clientv3.OpGet(key),
			},
		),
	).Commit()
	require.NoError(t, err)
	require.True(t, txn.Succeeded)
	require.NotNil(t, txn.Header)
	require.GreaterOrEqual(t, txn.Header.Revision, compact.Header.Revision)
	require.Len(t, txn.Responses, 1)

	nested := txn.Responses[0].GetResponseTxn()
	require.NotNil(t, nested)
	require.NotNil(t, nested.Header)
	require.Zero(t, nested.Header.Revision)
	require.False(t, nested.Succeeded)
	require.Len(t, nested.Responses, 1)

	selected := nested.Responses[0].GetResponseRange()
	require.NotNil(t, selected)
	require.NotNil(t, selected.Header)
	require.Equal(t, txn.Header.Revision, selected.Header.Revision)
	require.Len(t, selected.Kvs, 1)
	require.Equal(t, []byte(key), selected.Kvs[0].Key)
	require.Equal(t, []byte("value"), selected.Kvs[0].Value)
	require.Equal(t, seed.Header.Revision, selected.Kvs[0].CreateRevision)
	require.Equal(t, seed.Header.Revision, selected.Kvs[0].ModRevision)
	require.Equal(t, int64(1), selected.Kvs[0].Version)

	after, err := client.Get(ctx, prefix, clientv3.WithPrefix(), clientv3.WithSort(clientv3.SortByKey, clientv3.SortAscend))
	require.NoError(t, err)
	require.NotNil(t, after.Header)
	require.GreaterOrEqual(t, after.Header.Revision, txn.Header.Revision)
	require.Len(t, after.Kvs, 1)
	require.Equal(t, []byte(key), after.Kvs[0].Key)
	require.Equal(t, []byte("value"), after.Kvs[0].Value)
	require.Equal(t, seed.Header.Revision, after.Kvs[0].CreateRevision)
	require.Equal(t, seed.Header.Revision, after.Kvs[0].ModRevision)
	require.Equal(t, int64(1), after.Kvs[0].Version)
}

func TestClientNestedTxnUnselectedFailureFutureRevisionBranchIsIgnoredMatchesEtcd(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()

	grpcServer := grpc.NewServer(server.ClientServerOptions()...)
	etcdserverpb.RegisterKVServer(grpcServer, server)
	listener := bufconn.Listen(1 << 20)
	go func() { _ = grpcServer.Serve(listener) }()
	t.Cleanup(grpcServer.Stop)

	client, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{"bufnet"},
		DialTimeout: time.Second,
		DialOptions: []grpc.DialOption{
			grpc.WithTransportCredentials(insecure.NewCredentials()),
			grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
				return listener.Dial()
			}),
		},
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, client.Close()) })

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	prefix := "/a2173/client-nested-txn-unselected-failure-future-revision/"
	key := prefix + "key"
	unselectedWriteKey := prefix + "unselected-write"
	seed, err := client.Put(ctx, key, "value")
	require.NoError(t, err)

	txn, err := client.Txn(ctx).Then(
		clientv3.OpTxn(
			[]clientv3.Cmp{clientv3.Compare(clientv3.Version(key), "=", 1)},
			[]clientv3.Op{
				clientv3.OpGet(key),
			},
			[]clientv3.Op{
				clientv3.OpGet(key, clientv3.WithRev(math.MaxInt64)),
				clientv3.OpPut(unselectedWriteKey, "must-not-commit"),
			},
		),
	).Commit()
	require.NoError(t, err)
	require.True(t, txn.Succeeded)
	require.NotNil(t, txn.Header)
	require.Equal(t, seed.Header.Revision, txn.Header.Revision)
	require.Len(t, txn.Responses, 1)

	nested := txn.Responses[0].GetResponseTxn()
	require.NotNil(t, nested)
	require.NotNil(t, nested.Header)
	require.Zero(t, nested.Header.Revision)
	require.True(t, nested.Succeeded)
	require.Len(t, nested.Responses, 1)

	selected := nested.Responses[0].GetResponseRange()
	require.NotNil(t, selected)
	require.NotNil(t, selected.Header)
	require.Equal(t, txn.Header.Revision, selected.Header.Revision)
	require.Len(t, selected.Kvs, 1)
	require.Equal(t, []byte(key), selected.Kvs[0].Key)
	require.Equal(t, []byte("value"), selected.Kvs[0].Value)
	require.Equal(t, seed.Header.Revision, selected.Kvs[0].CreateRevision)
	require.Equal(t, seed.Header.Revision, selected.Kvs[0].ModRevision)
	require.Equal(t, int64(1), selected.Kvs[0].Version)

	after, err := client.Get(ctx, prefix, clientv3.WithPrefix(), clientv3.WithSort(clientv3.SortByKey, clientv3.SortAscend))
	require.NoError(t, err)
	require.NotNil(t, after.Header)
	require.Equal(t, seed.Header.Revision, after.Header.Revision)
	require.Len(t, after.Kvs, 1)
	require.Equal(t, []byte(key), after.Kvs[0].Key)
	require.Equal(t, []byte("value"), after.Kvs[0].Value)
	require.Equal(t, seed.Header.Revision, after.Kvs[0].CreateRevision)
	require.Equal(t, seed.Header.Revision, after.Kvs[0].ModRevision)
	require.Equal(t, int64(1), after.Kvs[0].Version)
}

func TestClientNestedTxnUnselectedFailureCompactedRevisionBranchIsIgnoredMatchesEtcd(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()

	grpcServer := grpc.NewServer(server.ClientServerOptions()...)
	etcdserverpb.RegisterKVServer(grpcServer, server)
	listener := bufconn.Listen(1 << 20)
	go func() { _ = grpcServer.Serve(listener) }()
	t.Cleanup(grpcServer.Stop)

	client, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{"bufnet"},
		DialTimeout: time.Second,
		DialOptions: []grpc.DialOption{
			grpc.WithTransportCredentials(insecure.NewCredentials()),
			grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
				return listener.Dial()
			}),
		},
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, client.Close()) })

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	prefix := "/a2174/client-nested-txn-unselected-failure-compacted-revision/"
	key := prefix + "key"
	unselectedWriteKey := prefix + "unselected-write"
	seed, err := client.Put(ctx, key, "value")
	require.NoError(t, err)
	compactedRev := seed.Header.Revision - 1
	require.Positive(t, compactedRev)
	compact, err := client.Compact(ctx, seed.Header.Revision)
	require.NoError(t, err)

	txn, err := client.Txn(ctx).Then(
		clientv3.OpTxn(
			[]clientv3.Cmp{clientv3.Compare(clientv3.Version(key), "=", 1)},
			[]clientv3.Op{
				clientv3.OpGet(key),
			},
			[]clientv3.Op{
				clientv3.OpGet(key, clientv3.WithRev(compactedRev)),
				clientv3.OpPut(unselectedWriteKey, "must-not-commit"),
			},
		),
	).Commit()
	require.NoError(t, err)
	require.True(t, txn.Succeeded)
	require.NotNil(t, txn.Header)
	require.GreaterOrEqual(t, txn.Header.Revision, compact.Header.Revision)
	require.Len(t, txn.Responses, 1)

	nested := txn.Responses[0].GetResponseTxn()
	require.NotNil(t, nested)
	require.NotNil(t, nested.Header)
	require.Zero(t, nested.Header.Revision)
	require.True(t, nested.Succeeded)
	require.Len(t, nested.Responses, 1)

	selected := nested.Responses[0].GetResponseRange()
	require.NotNil(t, selected)
	require.NotNil(t, selected.Header)
	require.Equal(t, txn.Header.Revision, selected.Header.Revision)
	require.Len(t, selected.Kvs, 1)
	require.Equal(t, []byte(key), selected.Kvs[0].Key)
	require.Equal(t, []byte("value"), selected.Kvs[0].Value)
	require.Equal(t, seed.Header.Revision, selected.Kvs[0].CreateRevision)
	require.Equal(t, seed.Header.Revision, selected.Kvs[0].ModRevision)
	require.Equal(t, int64(1), selected.Kvs[0].Version)

	after, err := client.Get(ctx, prefix, clientv3.WithPrefix(), clientv3.WithSort(clientv3.SortByKey, clientv3.SortAscend))
	require.NoError(t, err)
	require.NotNil(t, after.Header)
	require.GreaterOrEqual(t, after.Header.Revision, txn.Header.Revision)
	require.Len(t, after.Kvs, 1)
	require.Equal(t, []byte(key), after.Kvs[0].Key)
	require.Equal(t, []byte("value"), after.Kvs[0].Value)
	require.Equal(t, seed.Header.Revision, after.Kvs[0].CreateRevision)
	require.Equal(t, seed.Header.Revision, after.Kvs[0].ModRevision)
	require.Equal(t, int64(1), after.Kvs[0].Version)
}

func TestClientNestedTxnCompactedRevisionRejectsTxnBeforeWritesMatchesEtcd(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()

	grpcServer := grpc.NewServer(server.ClientServerOptions()...)
	etcdserverpb.RegisterKVServer(grpcServer, server)
	listener := bufconn.Listen(1 << 20)
	go func() { _ = grpcServer.Serve(listener) }()
	t.Cleanup(grpcServer.Stop)

	client, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{"bufnet"},
		DialTimeout: time.Second,
		DialOptions: []grpc.DialOption{
			grpc.WithTransportCredentials(insecure.NewCredentials()),
			grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
				return listener.Dial()
			}),
		},
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, client.Close()) })

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	prefix := "/a2170/client-nested-txn-compacted-revision/"
	key := prefix + "key"
	nestedWriteKey := prefix + "nested-write"
	outerWriteKey := prefix + "outer-write"
	seed, err := client.Put(ctx, key, "value")
	require.NoError(t, err)
	compactedRev := seed.Header.Revision - 1
	require.Positive(t, compactedRev)
	compact, err := client.Compact(ctx, seed.Header.Revision)
	require.NoError(t, err)

	_, err = client.Txn(ctx).Then(
		clientv3.OpTxn(
			nil,
			[]clientv3.Op{
				clientv3.OpGet(key, clientv3.WithRev(compactedRev)),
				clientv3.OpPut(nestedWriteKey, "must-not-commit"),
			},
			nil,
		),
		clientv3.OpPut(outerWriteKey, "must-not-commit"),
	).Commit()
	requireClientTxnError(t, err, codes.Unknown, "etcdserver: mvcc: required revision has been compacted", rpctypes.ErrCompacted)

	after, err := client.Get(ctx, prefix, clientv3.WithPrefix(), clientv3.WithSort(clientv3.SortByKey, clientv3.SortAscend))
	require.NoError(t, err)
	require.NotNil(t, after.Header)
	require.GreaterOrEqual(t, after.Header.Revision, compact.Header.Revision)
	require.Len(t, after.Kvs, 1)
	require.Equal(t, []byte(key), after.Kvs[0].Key)
	require.Equal(t, []byte("value"), after.Kvs[0].Value)
	require.Equal(t, seed.Header.Revision, after.Kvs[0].CreateRevision)
	require.Equal(t, seed.Header.Revision, after.Kvs[0].ModRevision)
	require.Equal(t, int64(1), after.Kvs[0].Version)
}

func TestClientTxnIntraTxnVersionSemantics(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()

	grpcServer := grpc.NewServer(server.ClientServerOptions()...)
	etcdserverpb.RegisterKVServer(grpcServer, server)
	listener := bufconn.Listen(1 << 20)
	go func() { _ = grpcServer.Serve(listener) }()
	t.Cleanup(grpcServer.Stop)

	client, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{"bufnet"},
		DialTimeout: time.Second,
		DialOptions: []grpc.DialOption{
			grpc.WithTransportCredentials(insecure.NewCredentials()),
			grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
				return listener.Dial()
			}),
		},
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, client.Close()) })

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	prefix := fmt.Sprintf("/a1011/client-txn-version/%d/", time.Now().UnixNano())
	key := prefix + "key"
	created := prefix + "created"
	seed, err := client.Put(ctx, key, "one")
	require.NoError(t, err)

	initial, err := client.Get(ctx, key)
	require.NoError(t, err)
	require.Len(t, initial.Kvs, 1)
	require.Equal(t, int64(1), initial.Kvs[0].Version)
	require.Equal(t, seed.Header.Revision, initial.Kvs[0].CreateRevision)
	require.Equal(t, seed.Header.Revision, initial.Kvs[0].ModRevision)

	txn, err := client.Txn(ctx).
		If(clientv3.Compare(clientv3.Version(key), "=", 1)).
		Then(
			clientv3.OpPut(key, "two"),
			clientv3.OpGet(key),
			clientv3.OpPut(created, "new"),
			clientv3.OpGet(created),
		).
		Else(clientv3.OpPut(prefix+"unexpected", "bad")).
		Commit()
	require.NoError(t, err)
	require.True(t, txn.Succeeded)
	require.Equal(t, seed.Header.Revision+1, txn.Header.Revision)
	require.Len(t, txn.Responses, 4)

	updatePut := txn.Responses[0].GetResponsePut()
	require.NotNil(t, updatePut)
	require.Equal(t, txn.Header.Revision, updatePut.Header.Revision)
	updatedRead := txn.Responses[1].GetResponseRange()
	require.NotNil(t, updatedRead)
	require.Equal(t, txn.Header.Revision, updatedRead.Header.Revision)
	require.Len(t, updatedRead.Kvs, 1)
	require.Equal(t, "two", string(updatedRead.Kvs[0].Value))
	require.Equal(t, int64(2), updatedRead.Kvs[0].Version)
	require.Equal(t, seed.Header.Revision, updatedRead.Kvs[0].CreateRevision)
	require.Equal(t, txn.Header.Revision, updatedRead.Kvs[0].ModRevision)

	createPut := txn.Responses[2].GetResponsePut()
	require.NotNil(t, createPut)
	require.Equal(t, txn.Header.Revision, createPut.Header.Revision)
	createdRead := txn.Responses[3].GetResponseRange()
	require.NotNil(t, createdRead)
	require.Equal(t, txn.Header.Revision, createdRead.Header.Revision)
	require.Len(t, createdRead.Kvs, 1)
	require.Equal(t, "new", string(createdRead.Kvs[0].Value))
	require.Equal(t, int64(1), createdRead.Kvs[0].Version)
	require.Equal(t, txn.Header.Revision, createdRead.Kvs[0].CreateRevision)
	require.Equal(t, txn.Header.Revision, createdRead.Kvs[0].ModRevision)
}

func TestClientTxnAmbiguousResponseCommitsAtMostOnce(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()

	grpcServer := grpc.NewServer(server.ClientServerOptions()...)
	server.Register(grpcServer)
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	go func() { _ = grpcServer.Serve(listener) }()
	t.Cleanup(func() {
		grpcServer.Stop()
		_ = listener.Close()
	})

	directEndpoint := listener.Addr().String()
	bridge := newClientLeasingTCPBridge(t, directEndpoint)
	newClient := func(endpoint string) *clientv3.Client {
		client, newErr := clientv3.New(clientv3.Config{
			Endpoints:   []string{endpoint},
			DialTimeout: time.Second,
		})
		require.NoError(t, newErr)
		t.Cleanup(func() { require.NoError(t, client.Close()) })
		return client
	}
	throughBridge := newClient(bridge.Endpoint())
	direct := newClient(directEndpoint)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	prefix := fmt.Sprintf("/a1078/txn-at-most-once/%d/", time.Now().UnixNano())
	keys := []string{prefix + "a", prefix + "b", prefix + "c"}
	seed, err := direct.Txn(ctx).Then(
		clientv3.OpPut(keys[0], "seed"),
		clientv3.OpPut(keys[1], "seed"),
		clientv3.OpPut(keys[2], "seed"),
	).Commit()
	require.NoError(t, err)
	require.True(t, seed.Succeeded)
	previousVersions := []int64{1, 1, 1}

	const attempts = 3
	for attempt := 0; attempt < attempts; attempt++ {
		warm, warmErr := throughBridge.Get(ctx, prefix, clientv3.WithPrefix())
		require.NoError(t, warmErr)
		require.Len(t, warm.Kvs, len(keys))

		value := fmt.Sprintf("attempt-%d", attempt)
		droppedBefore := bridge.DroppedBytes()
		bridge.BlackholeResponses()
		callCtx, callCancel := context.WithTimeout(ctx, 750*time.Millisecond)
		_, txnErr := throughBridge.Txn(callCtx).Then(
			clientv3.OpTxn(nil, []clientv3.Op{
				clientv3.OpPut(keys[0], value),
				clientv3.OpPut(keys[1], value),
			}, nil),
			clientv3.OpPut(keys[2], value),
		).Commit()
		callCancel()
		require.True(t,
			errors.Is(txnErr, context.DeadlineExceeded) ||
				status.Code(txnErr) == codes.DeadlineExceeded,
			"attempt %d returned unexpected error: %v", attempt, txnErr)
		require.Eventually(t, func() bool {
			return bridge.DroppedBytes() > droppedBefore
		}, 2*time.Second, 10*time.Millisecond)
		bridge.Unblackhole()
		bridge.DropConnections()

		observed := make([]*clientv3.GetResponse, 0, len(keys))
		require.Eventually(t, func() bool {
			observed = observed[:0]
			for _, key := range keys {
				response, getErr := direct.Get(ctx, key)
				if getErr != nil || len(response.Kvs) != 1 || string(response.Kvs[0].Value) != value {
					return false
				}
				observed = append(observed, response)
			}
			return true
		}, 5*time.Second, 20*time.Millisecond)

		revision := observed[0].Kvs[0].ModRevision
		for index, response := range observed {
			kv := response.Kvs[0]
			require.Equal(t, previousVersions[index]+1, kv.Version,
				"attempt %d key %d advanced more than once", attempt, index)
			require.Equal(t, revision, kv.ModRevision,
				"attempt %d committed keys at different revisions", attempt)
			previousVersions[index] = kv.Version
		}
	}
}

func txnClientValueCompare(
	key, rangeEnd []byte,
	result etcdserverpb.Compare_CompareResult,
	value string,
) *etcdserverpb.Compare {
	return &etcdserverpb.Compare{
		Key: key, RangeEnd: rangeEnd, Result: result, Target: etcdserverpb.Compare_VALUE,
		TargetUnion: &etcdserverpb.Compare_Value{Value: []byte(value)},
	}
}

func txnClientIntCompare(
	key, rangeEnd []byte,
	target etcdserverpb.Compare_CompareTarget,
	result etcdserverpb.Compare_CompareResult,
	value int64,
) *etcdserverpb.Compare {
	compare := &etcdserverpb.Compare{Key: key, RangeEnd: rangeEnd, Result: result, Target: target}
	switch target {
	case etcdserverpb.Compare_VERSION:
		compare.TargetUnion = &etcdserverpb.Compare_Version{Version: value}
	case etcdserverpb.Compare_CREATE:
		compare.TargetUnion = &etcdserverpb.Compare_CreateRevision{CreateRevision: value}
	case etcdserverpb.Compare_MOD:
		compare.TargetUnion = &etcdserverpb.Compare_ModRevision{ModRevision: value}
	case etcdserverpb.Compare_LEASE:
		compare.TargetUnion = &etcdserverpb.Compare_Lease{Lease: value}
	}
	return compare
}

func txnClientPutOp(request *etcdserverpb.PutRequest) *etcdserverpb.RequestOp {
	return &etcdserverpb.RequestOp{Request: &etcdserverpb.RequestOp_RequestPut{RequestPut: request}}
}

func txnClientRangeOp(request *etcdserverpb.RangeRequest) *etcdserverpb.RequestOp {
	return &etcdserverpb.RequestOp{Request: &etcdserverpb.RequestOp_RequestRange{RequestRange: request}}
}

func txnClientDeleteOp(request *etcdserverpb.DeleteRangeRequest) *etcdserverpb.RequestOp {
	return &etcdserverpb.RequestOp{Request: &etcdserverpb.RequestOp_RequestDeleteRange{RequestDeleteRange: request}}
}

func txnClientTxnOp(success, failure []*etcdserverpb.RequestOp) *etcdserverpb.RequestOp {
	return &etcdserverpb.RequestOp{Request: &etcdserverpb.RequestOp_RequestTxn{
		RequestTxn: &etcdserverpb.TxnRequest{Success: success, Failure: failure},
	}}
}

func requireRawGRPCTxnError(t *testing.T, err error, code codes.Code, message string, wantErrorIs ...error) {
	t.Helper()
	require.EqualError(t, err, status.Error(code, message).Error())
	for _, want := range wantErrorIs {
		require.ErrorIs(t, err, want)
	}
	require.Equal(t, code, status.Code(err))
	require.Equal(t, message, status.Convert(err).Message())
}

func requireClientTxnError(t *testing.T, err error, code codes.Code, message string, wantErrorIs ...error) {
	t.Helper()
	require.EqualError(t, err, message)
	for _, want := range wantErrorIs {
		require.ErrorIs(t, err, want)
	}
	require.Equal(t, code, status.Code(err))
	require.Equal(t, message, status.Convert(err).Message())
}

type txnClientKV struct {
	Key           string
	Value         string
	Version       int64
	CreatedInTxn  bool
	ModifiedInTxn bool
}

func txnClientKVs(kvs []*mvccpb.KeyValue, prefix string, txnRevision int64) []txnClientKV {
	out := make([]txnClientKV, 0, len(kvs))
	for _, kv := range kvs {
		out = append(out, txnClientKV{
			Key:           string(bytes.TrimPrefix(kv.Key, []byte(prefix))),
			Value:         string(kv.Value),
			Version:       kv.Version,
			CreatedInTxn:  kv.CreateRevision == txnRevision,
			ModifiedInTxn: kv.ModRevision == txnRevision,
		})
	}
	return out
}

func txnClientKVKeys(kvs []txnClientKV) []string {
	keys := make([]string, 0, len(kvs))
	for _, kv := range kvs {
		keys = append(keys, kv.Key)
	}
	return keys
}
