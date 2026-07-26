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
	"fmt"
	"math"
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/mvccpb"
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
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			resp, callErr := kv.Txn(ctx, &etcdserverpb.TxnRequest{Success: []*etcdserverpb.RequestOp{tt.op}})
			require.Nil(t, resp)
			require.Error(t, callErr)
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
	requireRawGRPCTxnError(t, err, codes.NotFound, "etcdserver: requested lease not found")
	_, err = kv.Txn(ctx, &etcdserverpb.TxnRequest{Success: []*etcdserverpb.RequestOp{futureRange, missingLeasePut}})
	requireRawGRPCTxnError(t, err, codes.OutOfRange, "etcdserver: mvcc: required revision is a future revision")

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
		{name: "unselected-failure-nested-over-budget", txn: &etcdserverpb.TxnRequest{
			Success: rangeOps(1), Failure: append(rangeOps(defaultMaxTxnOps-1), nested(rangeOps(1))),
		}, wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			resp, callErr := kv.Txn(ctx, tt.txn)
			if tt.wantErr {
				require.Nil(t, resp)
				requireRawGRPCTxnError(t, callErr, codes.InvalidArgument, "etcdserver: too many operations in txn request")
				return
			}
			require.NoError(t, callErr)
			require.NotNil(t, resp)
			require.Equal(t, tt.wantSucceeded, resp.Succeeded)
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
				requireRawGRPCTxnError(t, callErr, codes.InvalidArgument, "etcdserver: duplicate key given in txn request")
				return
			}
			require.NoError(t, callErr)
			require.NotNil(t, resp)
			require.True(t, resp.Succeeded)
		})
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

func requireRawGRPCTxnError(t *testing.T, err error, code codes.Code, message string) {
	t.Helper()
	require.Error(t, err)
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
