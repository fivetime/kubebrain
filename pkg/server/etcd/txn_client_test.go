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
			require.NotZero(t, txn.Header.Revision)
			if tt.wantMiddle {
				require.True(t, nested.Succeeded)
				require.Len(t, nested.Responses, 2)
				innerResponse := nested.Responses[0].GetResponseTxn()
				require.NotNil(t, innerResponse)
				require.Equal(t, tt.wantInnerSucceeded, innerResponse.Succeeded)
				require.Len(t, innerResponse.Responses, 1)
				deleted := innerResponse.Responses[0].GetResponseDeleteRange()
				require.NotNil(t, deleted)
				require.Equal(t, int64(1), deleted.Deleted)
				require.Equal(t, []txnClientKV{{Key: "c", Value: tt.cValue, Version: 1}}, txnClientKVs(deleted.PrevKvs, prefix, txn.Header.Revision))
				put := nested.Responses[1].GetResponsePut()
				require.NotNil(t, put)
				require.Nil(t, put.PrevKv)
			} else {
				require.Equal(t, tt.wantInnerSucceeded, nested.Succeeded)
				require.Len(t, nested.Responses, 2)
				put := nested.Responses[0].GetResponsePut()
				require.NotNil(t, put)
				require.Nil(t, put.PrevKv)
				gotB := nested.Responses[1].GetResponseRange()
				require.NotNil(t, gotB)
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
	require.Len(t, ignoreValue.Responses, 2)
	ignoreValuePrev := ignoreValue.Responses[0].GetResponsePut().PrevKv
	require.NotNil(t, ignoreValuePrev)
	require.Equal(t, "old", string(ignoreValuePrev.Value))
	require.Equal(t, int64(leaseA.ID), ignoreValuePrev.Lease)
	staged := ignoreValue.Responses[1].GetResponseRange()
	require.NotNil(t, staged)
	require.Len(t, staged.Kvs, 1)
	require.Equal(t, "old", string(staged.Kvs[0].Value))
	require.Equal(t, int64(leaseB.ID), staged.Kvs[0].Lease)

	ignoreLease, err := client.Txn(ctx).Then(
		clientv3.OpPut(key, "new", clientv3.WithIgnoreLease(), clientv3.WithPrevKV()),
	).Commit()
	require.NoError(t, err)
	require.True(t, ignoreLease.Succeeded)
	require.Len(t, ignoreLease.Responses, 1)
	ignoreLeasePrev := ignoreLease.Responses[0].GetResponsePut().PrevKv
	require.NotNil(t, ignoreLeasePrev)
	require.Equal(t, "old", string(ignoreLeasePrev.Value))
	require.Equal(t, int64(leaseB.ID), ignoreLeasePrev.Lease)
	final, err := client.Get(ctx, key)
	require.NoError(t, err)
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
	require.Error(t, selectedBadLeaseErr)
	require.Equal(t, codes.Unknown, status.Code(selectedBadLeaseErr))
	require.Equal(t, "etcdserver: requested lease not found", status.Convert(selectedBadLeaseErr).Message())
	afterBadLease, err := client.Get(ctx, branchKey)
	require.NoError(t, err)
	require.Empty(t, afterBadLease.Kvs)
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
	requireClientTxnError(t, err, codes.Unknown, "etcdserver: mvcc: required revision has been compacted")
	afterCompacted, err := client.Get(ctx, writeKey)
	require.NoError(t, err)
	require.Empty(t, afterCompacted.Kvs)

	unselected, err := client.Txn(ctx).
		If(clientv3.Compare(clientv3.Version(key), "=", 0)).
		Then(clientv3.OpGet(key, clientv3.WithRev(-1))).
		Else(clientv3.OpGet(key)).
		Commit()
	require.NoError(t, err)
	require.False(t, unselected.Succeeded)
	require.Len(t, unselected.Responses, 1)
	require.Len(t, unselected.Responses[0].GetResponseRange().Kvs, 1)

	_, err = client.Txn(ctx).
		Then(
			clientv3.OpGet(key, clientv3.WithRev(math.MaxInt64)),
			clientv3.OpPut(writeKey, "must-not-commit"),
		).
		Commit()
	requireClientTxnError(t, err, codes.Unknown, "etcdserver: mvcc: required revision is a future revision")
	afterFuture, err := client.Get(ctx, writeKey)
	require.NoError(t, err)
	require.Empty(t, afterFuture.Kvs)
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

func requireClientTxnError(t *testing.T, err error, code codes.Code, message string) {
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

func txnClientKVKeys(kvs []txnClientKV) []string {
	keys := make([]string, 0, len(kvs))
	for _, kv := range kvs {
		keys = append(keys, kv.Key)
	}
	return keys
}
