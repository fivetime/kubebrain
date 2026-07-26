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
	"context"
	"fmt"
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
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
