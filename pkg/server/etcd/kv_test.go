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
	"math"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/mvccpb"
	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/namespace"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"

	"github.com/kubewharf/kubebrain/pkg/backend"
	"github.com/kubewharf/kubebrain/pkg/metrics/mock"
	"github.com/kubewharf/kubebrain/pkg/server/service/etcdproxy"
	"github.com/kubewharf/kubebrain/pkg/server/service/leader"
	"github.com/kubewharf/kubebrain/pkg/storage/memkv"
)

type testPeerService struct {
	isLeader         bool
	isLeaderFn       func() bool
	hasLeaderFn      func() bool
	epochFn          func() (uint64, bool)
	noLeader         bool
	leaderInfo       string
	syncReadFn       func(context.Context) error
	leadershipTermFn func(context.Context) (uint64, error)
	currentTermFn    func() uint64
	proxyEnabled     bool
	rangeFn          func(context.Context, *etcdserverpb.RangeRequest) (*etcdserverpb.RangeResponse, error)
	putFn            func(context.Context, *etcdserverpb.PutRequest) (*etcdserverpb.PutResponse, error)
	deleteRangeFn    func(context.Context, *etcdserverpb.DeleteRangeRequest) (*etcdserverpb.DeleteRangeResponse, error)
	compactFn        func(context.Context, *etcdserverpb.CompactionRequest) (*etcdserverpb.CompactionResponse, error)
	watchFn          func(context.Context, []byte, []byte, uint64) (<-chan etcdproxy.WatchResult, error)
	leaseGrantFn     func(context.Context, *etcdserverpb.LeaseGrantRequest) (*etcdserverpb.LeaseGrantResponse, error)
	leaseKeepAliveFn func(context.Context, *etcdserverpb.LeaseKeepAliveRequest) (*etcdserverpb.LeaseKeepAliveResponse, error)
	txnFn            func(context.Context, *etcdserverpb.TxnRequest) (*etcdserverpb.TxnResponse, error)
}

type compareDeleteTrapBackendShim struct {
	BackendShim
	called bool
}

func (b *compareDeleteTrapBackendShim) CompareDelete(
	context.Context,
	*etcdserverpb.DeleteRangeRequest,
	int64,
	bool,
) (*etcdserverpb.TxnResponse, error) {
	b.called = true
	return nil, errors.New("CompareDelete fast path must not handle ranged delete")
}

func (s testPeerService) SyncReadRevision(ctx context.Context) error {
	if s.syncReadFn != nil {
		return s.syncReadFn(ctx)
	}
	return nil
}

func (testPeerService) Close() error {
	return nil
}

func (testPeerService) Campaign(context.Context) {
}

func (testPeerService) RefreshLeaderInfo(context.Context) error { return nil }

func (s testPeerService) GetLeaderInfo() string {
	if s.noLeader {
		return ""
	}
	if s.leaderInfo != "" {
		return s.leaderInfo
	}
	return "test-peer"
}

func (s testPeerService) LeadershipTerm(ctx context.Context) (uint64, error) {
	if s.leadershipTermFn != nil {
		return s.leadershipTermFn(ctx)
	}
	return 1, nil
}

func (s testPeerService) CurrentLeadershipTerm() uint64 {
	if s.currentTermFn != nil {
		return s.currentTermFn()
	}
	return 1
}

func (s testPeerService) IsLeader() bool {
	if s.isLeaderFn != nil {
		return s.isLeaderFn()
	}
	return s.isLeader
}

func (s testPeerService) HasLeader() bool {
	if s.hasLeaderFn != nil {
		return s.hasLeaderFn()
	}
	return s.IsLeader() || !s.noLeader
}

func (s testPeerService) EpochAndLeadingFresh() (uint64, bool) {
	if s.epochFn != nil {
		return s.epochFn()
	}
	return 0, s.IsLeader()
}

func (s testPeerService) GetElectionInfo() (leader.ElectionInfo, error) {
	return leader.ElectionInfo{LeaderAddress: "test-peer", IsLeader: s.isLeader}, nil
}

func (s testPeerService) EtcdProxyEnabled() bool {
	return s.proxyEnabled
}

func (s testPeerService) Ready() error {
	return nil
}

func (s testPeerService) Txn(ctx context.Context, txn *etcdserverpb.TxnRequest) (*etcdserverpb.TxnResponse, error) {
	if s.txnFn != nil {
		return s.txnFn(ctx, txn)
	}
	return nil, nil
}

type staleCurrentRevisionShim struct {
	BackendShim
	current uint64
}

func (s staleCurrentRevisionShim) GetCurrentRevision() uint64 { return s.current }

func TestSerializableReadonlyTxnUsesOneDurableFollowerSnapshot(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()
	ctx := context.Background()

	compareKey := []byte("/registry/serializable-txn/compare")
	branchKey := []byte("/registry/serializable-txn/branch")
	first, err := server.Put(ctx, &etcdserverpb.PutRequest{Key: compareKey, Value: []byte("old")})
	require.NoError(t, err)
	second, err := server.Put(ctx, &etcdserverpb.PutRequest{Key: compareKey, Value: []byte("new")})
	require.NoError(t, err)
	branch, err := server.Put(ctx, &etcdserverpb.PutRequest{Key: branchKey, Value: []byte("visible")})
	require.NoError(t, err)
	require.Eventually(t, func() bool {
		rev, getErr := server.backend.GetDurableRevision(ctx)
		return getErr == nil && rev >= uint64(branch.Header.Revision)
	}, time.Second, time.Millisecond)

	server.backend = staleCurrentRevisionShim{BackendShim: server.backend, current: uint64(first.Header.Revision)}
	server.peers = testPeerService{
		isLeader: false,
		syncReadFn: func(context.Context) error {
			t.Fatal("serializable read-only txn must not perform leader revision sync")
			return nil
		},
	}
	resp, err := server.Txn(ctx, &etcdserverpb.TxnRequest{
		Compare: []*etcdserverpb.Compare{{
			Key: compareKey, Target: etcdserverpb.Compare_VALUE, Result: etcdserverpb.Compare_EQUAL,
			TargetUnion: &etcdserverpb.Compare_Value{Value: []byte("new")},
		}},
		Success: []*etcdserverpb.RequestOp{{Request: &etcdserverpb.RequestOp_RequestRange{
			RequestRange: &etcdserverpb.RangeRequest{Key: branchKey, Serializable: true},
		}}},
		Failure: []*etcdserverpb.RequestOp{{Request: &etcdserverpb.RequestOp_RequestRange{
			RequestRange: &etcdserverpb.RangeRequest{Key: compareKey, Serializable: true},
		}}},
	})
	require.NoError(t, err)
	require.True(t, resp.Succeeded)
	require.Equal(t, branch.Header.Revision, resp.Header.Revision)
	rangeResp := resp.Responses[0].GetResponseRange()
	require.NotNil(t, rangeResp)
	require.NotNil(t, rangeResp.Header)
	require.Equal(t, resp.Header.Revision, rangeResp.Header.Revision)
	require.Less(t, second.Header.Revision, rangeResp.Kvs[0].ModRevision)
	require.Equal(t, []byte("visible"), rangeResp.Kvs[0].Value)
}

func TestReadonlyTxnWithNonSerializableRangeStillRoutesToLeader(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()
	proxied := false
	server.peers = testPeerService{isLeader: false, proxyEnabled: true, txnFn: func(_ context.Context, _ *etcdserverpb.TxnRequest) (*etcdserverpb.TxnResponse, error) {
		proxied = true
		return &etcdserverpb.TxnResponse{Header: txnHeader(42), Succeeded: true}, nil
	}}
	resp, err := server.Txn(context.Background(), &etcdserverpb.TxnRequest{
		Success: []*etcdserverpb.RequestOp{{Request: &etcdserverpb.RequestOp_RequestRange{
			RequestRange: &etcdserverpb.RangeRequest{Key: []byte("key")},
		}}},
	})
	require.NoError(t, err)
	require.True(t, proxied)
	require.Equal(t, int64(42), resp.Header.Revision)
}

func TestTxnWithoutComparesIgnoresNonEmptyFailureBranch(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()
	ctx := context.Background()

	failure := []*etcdserverpb.RequestOp{{Request: &etcdserverpb.RequestOp_RequestRange{
		RequestRange: &etcdserverpb.RangeRequest{Key: []byte("/txn/unreachable")},
	}}}
	empty, err := server.Txn(ctx, &etcdserverpb.TxnRequest{Failure: failure})
	require.NoError(t, err)
	require.True(t, empty.Succeeded)
	require.Empty(t, empty.Responses)
	require.NotNil(t, empty.Header)

	written, err := server.Txn(ctx, &etcdserverpb.TxnRequest{
		Success: []*etcdserverpb.RequestOp{{Request: &etcdserverpb.RequestOp_RequestPut{
			RequestPut: &etcdserverpb.PutRequest{Key: []byte("/txn/unconditional"), Value: []byte("value")},
		}}},
		Failure: failure,
	})
	require.NoError(t, err)
	require.True(t, written.Succeeded)
	require.NotNil(t, written.Header)
	require.Len(t, written.Responses, 1)
	putResp := written.Responses[0].GetResponsePut()
	require.NotNil(t, putResp)
	require.NotNil(t, putResp.Header)
	require.Equal(t, written.Header.Revision, putResp.Header.Revision)

	stored, err := server.Range(ctx, &etcdserverpb.RangeRequest{Key: []byte("/txn/unconditional")})
	require.NoError(t, err)
	require.NotNil(t, stored.Header)
	require.Equal(t, written.Header.Revision, stored.Header.Revision)
	require.Len(t, stored.Kvs, 1)
	require.Equal(t, []byte("value"), stored.Kvs[0].Value)
}

func TestNestedTxnWithoutComparesIgnoresNonEmptyFailureBranch(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()

	nested := &etcdserverpb.TxnRequest{
		Success: []*etcdserverpb.RequestOp{{Request: &etcdserverpb.RequestOp_RequestRange{
			RequestRange: &etcdserverpb.RangeRequest{Key: []byte("/txn/nested/success")},
		}}},
		Failure: []*etcdserverpb.RequestOp{{Request: &etcdserverpb.RequestOp_RequestPut{
			RequestPut: &etcdserverpb.PutRequest{Key: []byte("/txn/nested/failure"), Value: []byte("must-not-write")},
		}}},
	}
	resp, err := server.Txn(context.Background(), &etcdserverpb.TxnRequest{
		Success: []*etcdserverpb.RequestOp{{Request: &etcdserverpb.RequestOp_RequestTxn{
			RequestTxn: nested,
		}}},
	})
	require.NoError(t, err)
	require.True(t, resp.Succeeded)
	require.Len(t, resp.Responses, 1)
	nestedResp := resp.Responses[0].GetResponseTxn()
	require.NotNil(t, nestedResp)
	require.NotNil(t, nestedResp.Header)
	require.Zero(t, nestedResp.Header.Revision)
	require.True(t, nestedResp.Succeeded)
	require.Len(t, nestedResp.Responses, 1)
	rangeResp := nestedResp.Responses[0].GetResponseRange()
	require.NotNil(t, rangeResp)
	require.NotNil(t, rangeResp.Header)
	require.Equal(t, resp.Header.Revision, rangeResp.Header.Revision)

	stored, err := server.Range(context.Background(), &etcdserverpb.RangeRequest{Key: []byte("/txn/nested/failure")})
	require.NoError(t, err)
	require.Empty(t, stored.Kvs)
}

func TestHistoricalRangeUsesDurableFollowerWatermark(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()
	ctx := context.Background()
	key := []byte("/registry/durable-history/key")
	first, err := server.Put(ctx, &etcdserverpb.PutRequest{Key: key, Value: []byte("v1")})
	require.NoError(t, err)
	_, err = server.Put(ctx, &etcdserverpb.PutRequest{Key: key, Value: []byte("v2")})
	require.NoError(t, err)
	require.Eventually(t, func() bool {
		rev, getErr := server.backend.GetDurableRevision(ctx)
		return getErr == nil && rev > uint64(first.Header.Revision)
	}, time.Second, time.Millisecond)

	server.peers = testPeerService{isLeader: false, syncReadFn: func(context.Context) error {
		t.Fatal("bounded historical read must not require leader revision sync")
		return nil
	}}
	resp, err := server.Range(ctx, &etcdserverpb.RangeRequest{Key: key, Revision: first.Header.Revision, Serializable: true})
	require.NoError(t, err)
	require.Len(t, resp.Kvs, 1)
	require.Equal(t, []byte("v1"), resp.Kvs[0].Value)
}

func TestLinearizableHistoricalRangeRoutesToLeader(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()
	proxied := false
	server.peers = testPeerService{
		isLeader:     false,
		proxyEnabled: true,
		rangeFn: func(_ context.Context, request *etcdserverpb.RangeRequest) (*etcdserverpb.RangeResponse, error) {
			proxied = true
			require.False(t, request.Serializable)
			require.Equal(t, int64(7), request.Revision)
			return &etcdserverpb.RangeResponse{Header: txnHeader(42)}, nil
		},
	}

	response, err := server.Range(context.Background(), &etcdserverpb.RangeRequest{
		Key: []byte("/historical-linearizable"), Revision: 7,
	})
	require.NoError(t, err)
	require.True(t, proxied)
	require.Equal(t, int64(42), response.Header.Revision)
}

func (s testPeerService) Range(ctx context.Context, req *etcdserverpb.RangeRequest) (*etcdserverpb.RangeResponse, error) {
	if s.rangeFn != nil {
		return s.rangeFn(ctx, req)
	}
	return nil, nil
}

func (s testPeerService) Put(ctx context.Context, req *etcdserverpb.PutRequest) (*etcdserverpb.PutResponse, error) {
	if s.putFn != nil {
		return s.putFn(ctx, req)
	}
	return nil, nil
}

func (s testPeerService) DeleteRange(ctx context.Context, req *etcdserverpb.DeleteRangeRequest) (*etcdserverpb.DeleteRangeResponse, error) {
	if s.deleteRangeFn != nil {
		return s.deleteRangeFn(ctx, req)
	}
	return nil, nil
}

func (s testPeerService) Compact(ctx context.Context, req *etcdserverpb.CompactionRequest) (*etcdserverpb.CompactionResponse, error) {
	if s.compactFn != nil {
		return s.compactFn(ctx, req)
	}
	return nil, nil
}

func (s testPeerService) Watch(ctx context.Context, key, rangeEnd []byte, revision uint64) (<-chan etcdproxy.WatchResult, error) {
	if s.watchFn != nil {
		return s.watchFn(ctx, key, rangeEnd, revision)
	}
	return nil, nil
}

func (s testPeerService) LeaseGrant(ctx context.Context, req *etcdserverpb.LeaseGrantRequest) (*etcdserverpb.LeaseGrantResponse, error) {
	if s.leaseGrantFn != nil {
		return s.leaseGrantFn(ctx, req)
	}
	return nil, nil
}

func (testPeerService) LeaseRevoke(context.Context, *etcdserverpb.LeaseRevokeRequest) (*etcdserverpb.LeaseRevokeResponse, error) {
	return nil, nil
}

func (s testPeerService) LeaseKeepAlive(ctx context.Context, req *etcdserverpb.LeaseKeepAliveRequest) (*etcdserverpb.LeaseKeepAliveResponse, error) {
	if s.leaseKeepAliveFn != nil {
		return s.leaseKeepAliveFn(ctx, req)
	}
	return nil, nil
}

func (testPeerService) LeaseTimeToLive(context.Context, *etcdserverpb.LeaseTimeToLiveRequest) (*etcdserverpb.LeaseTimeToLiveResponse, error) {
	return nil, nil
}

func (testPeerService) LeaseLeases(context.Context, *etcdserverpb.LeaseLeasesRequest) (*etcdserverpb.LeaseLeasesResponse, error) {
	return nil, nil
}

func newTestRPCServer(t *testing.T) (*RPCServer, func()) {
	ctrl := gomock.NewController(t)
	metrics := mock.NewMinimalMetrics(ctrl)
	kv := memkv.NewKvStorage()
	b := backend.NewBackend(kv, backend.Config{
		Identity:                "test-peer",
		EnableEtcdCompatibility: true,
	}, metrics)
	server := New(b, metrics, testPeerService{isLeader: true})
	return server, func() {
		server.stopLeases()
		require.NoError(t, kv.Close())
		ctrl.Finish()
	}
}

func TestPutCreatesAndOverwritesKey(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()

	ctx := context.Background()
	putResp, err := server.Put(ctx, &etcdserverpb.PutRequest{
		Key:   []byte("/registry/pods/a"),
		Value: []byte("v1"),
	})
	require.NoError(t, err)
	require.NotNil(t, putResp.Header)

	rangeResp, err := server.Range(ctx, &etcdserverpb.RangeRequest{Key: []byte("/registry/pods/a")})
	require.NoError(t, err)
	require.Len(t, rangeResp.Kvs, 1)
	require.Equal(t, []byte("v1"), rangeResp.Kvs[0].Value)
	firstRevision := rangeResp.Kvs[0].ModRevision

	overwriteResp, err := server.Put(ctx, &etcdserverpb.PutRequest{
		Key:    []byte("/registry/pods/a"),
		Value:  []byte("v2"),
		PrevKv: true,
	})
	require.NoError(t, err)
	require.NotNil(t, overwriteResp.PrevKv)
	require.Equal(t, []byte("v1"), overwriteResp.PrevKv.Value)

	rangeResp, err = server.Range(ctx, &etcdserverpb.RangeRequest{Key: []byte("/registry/pods/a")})
	require.NoError(t, err)
	require.Len(t, rangeResp.Kvs, 1)
	require.Equal(t, []byte("v2"), rangeResp.Kvs[0].Value)
	require.Greater(t, rangeResp.Kvs[0].ModRevision, firstRevision)
}

func TestSerializableRangeBypassesLeaderRevisionSync(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()
	ctx := context.Background()
	key := []byte("/serializable/range")
	_, err := server.Put(ctx, &etcdserverpb.PutRequest{Key: key, Value: []byte("value")})
	require.NoError(t, err)

	syncErr := errors.New("leader revision unavailable")
	server.peers = testPeerService{syncReadFn: func(context.Context) error { return syncErr }}
	resp, err := server.Range(ctx, &etcdserverpb.RangeRequest{Key: key, Serializable: true})
	require.NoError(t, err)
	require.Len(t, resp.Kvs, 1)
	require.Equal(t, "value", string(resp.Kvs[0].Value))

	_, err = server.Range(ctx, &etcdserverpb.RangeRequest{Key: key})
	require.Equal(t, codes.Unavailable, status.Code(err),
		"linearizable Range must expose a retryable leader read barrier failure")
	require.Equal(t, syncErr.Error(), status.Convert(err).Message())
}

func TestSerializableRangeAndTxnHeadersMatchCurrentRevision(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()
	ctx := context.Background()

	key := []byte("/registry/serializable/header")
	first, err := server.Put(ctx, &etcdserverpb.PutRequest{Key: key, Value: []byte("v1")})
	require.NoError(t, err)
	second, err := server.Put(ctx, &etcdserverpb.PutRequest{Key: key, Value: []byte("v2")})
	require.NoError(t, err)

	current, err := server.Range(ctx, &etcdserverpb.RangeRequest{Key: key, Serializable: true})
	require.NoError(t, err)
	require.Equal(t, second.Header.Revision, current.Header.Revision)
	require.Len(t, current.Kvs, 1)
	require.Equal(t, []byte("v2"), current.Kvs[0].Value)

	historical, err := server.Range(ctx, &etcdserverpb.RangeRequest{
		Key: key, Revision: first.Header.Revision, Serializable: true,
	})
	require.NoError(t, err)
	require.Equal(t, second.Header.Revision, historical.Header.Revision)
	require.Len(t, historical.Kvs, 1)
	require.Equal(t, []byte("v1"), historical.Kvs[0].Value)

	txn, err := server.Txn(ctx, &etcdserverpb.TxnRequest{
		Compare: []*etcdserverpb.Compare{{
			Key: key, Target: etcdserverpb.Compare_VERSION, Result: etcdserverpb.Compare_GREATER,
			TargetUnion: &etcdserverpb.Compare_Version{Version: 0},
		}},
		Success: []*etcdserverpb.RequestOp{{Request: &etcdserverpb.RequestOp_RequestRange{
			RequestRange: &etcdserverpb.RangeRequest{Key: key, Serializable: true},
		}}},
		Failure: []*etcdserverpb.RequestOp{{Request: &etcdserverpb.RequestOp_RequestRange{
			RequestRange: &etcdserverpb.RangeRequest{Key: key, Serializable: true},
		}}},
	})
	require.NoError(t, err)
	require.True(t, txn.Succeeded)
	require.Equal(t, second.Header.Revision, txn.Header.Revision)
	txnRange := txn.Responses[0].GetResponseRange()
	require.NotNil(t, txnRange)
	require.Equal(t, second.Header.Revision, txnRange.Header.Revision)
	require.Len(t, txnRange.Kvs, 1)
	require.Equal(t, []byte("v2"), txnRange.Kvs[0].Value)
}

func TestPutIgnoreLeasePreservesExistingLease(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()

	ctx := context.Background()
	key := []byte("/registry/pods/ignore-lease")
	leaseResp, err := server.LeaseGrant(ctx, &etcdserverpb.LeaseGrantRequest{TTL: 30, ID: 24680})
	require.NoError(t, err)

	_, err = server.Put(ctx, &etcdserverpb.PutRequest{
		Key:   key,
		Value: []byte("leased"),
		Lease: leaseResp.ID,
	})
	require.NoError(t, err)

	_, err = server.Put(ctx, &etcdserverpb.PutRequest{
		Key:         key,
		Value:       []byte("updated"),
		IgnoreLease: true,
	})
	require.NoError(t, err)

	rangeResp, err := server.Range(ctx, &etcdserverpb.RangeRequest{Key: key})
	require.NoError(t, err)
	require.Len(t, rangeResp.Kvs, 1)
	require.Equal(t, []byte("updated"), rangeResp.Kvs[0].Value)

	ttlResp, err := server.LeaseTimeToLive(ctx, &etcdserverpb.LeaseTimeToLiveRequest{ID: leaseResp.ID, Keys: true})
	require.NoError(t, err)
	require.Equal(t, [][]byte{key}, ttlResp.Keys)
}

func TestPutIgnoreValueUpdatesLeasePreservesValue(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()

	ctx := context.Background()
	key := []byte("/registry/pods/ignore-value")
	leaseOne, err := server.LeaseGrant(ctx, &etcdserverpb.LeaseGrantRequest{TTL: 30, ID: 24682})
	require.NoError(t, err)
	leaseTwo, err := server.LeaseGrant(ctx, &etcdserverpb.LeaseGrantRequest{TTL: 30, ID: 24683})
	require.NoError(t, err)

	_, err = server.Put(ctx, &etcdserverpb.PutRequest{
		Key:   key,
		Value: []byte("original"),
		Lease: leaseOne.ID,
	})
	require.NoError(t, err)

	_, err = server.Put(ctx, &etcdserverpb.PutRequest{
		Key:         key,
		Lease:       leaseTwo.ID,
		IgnoreValue: true,
	})
	require.NoError(t, err)

	rangeResp, err := server.Range(ctx, &etcdserverpb.RangeRequest{Key: key})
	require.NoError(t, err)
	require.Len(t, rangeResp.Kvs, 1)
	require.Equal(t, []byte("original"), rangeResp.Kvs[0].Value)

	ttlOne, err := server.LeaseTimeToLive(ctx, &etcdserverpb.LeaseTimeToLiveRequest{ID: leaseOne.ID, Keys: true})
	require.NoError(t, err)
	require.Empty(t, ttlOne.Keys)
	ttlTwo, err := server.LeaseTimeToLive(ctx, &etcdserverpb.LeaseTimeToLiveRequest{ID: leaseTwo.ID, Keys: true})
	require.NoError(t, err)
	require.Equal(t, [][]byte{key}, ttlTwo.Keys)
}

func TestPutDifferentialScenarioMatchesEtcd(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()

	ctx := context.Background()
	prefix := "/registry/pods/put-differential/"
	key := []byte(prefix + "key")
	missing := []byte(prefix + "missing")
	base, err := server.Range(ctx, &etcdserverpb.RangeRequest{
		Key: []byte(prefix), RangeEnd: []byte("/registry/pods/put-differential0"),
	})
	require.NoError(t, err)
	baseRev := base.Header.Revision
	leaseA, err := server.LeaseGrant(ctx, &etcdserverpb.LeaseGrantRequest{TTL: 300, ID: 62001})
	require.NoError(t, err)
	leaseB, err := server.LeaseGrant(ctx, &etcdserverpb.LeaseGrantRequest{TTL: 300, ID: 62002})
	require.NoError(t, err)

	create, err := server.Put(ctx, &etcdserverpb.PutRequest{
		Key: key, Value: []byte("one"), Lease: leaseA.ID, PrevKv: true,
	})
	require.NoError(t, err)
	require.Equal(t, int64(1), create.Header.Revision-baseRev)
	require.Nil(t, create.PrevKv)

	rebind, err := server.Put(ctx, &etcdserverpb.PutRequest{
		Key: key, Value: []byte("two"), Lease: leaseB.ID, PrevKv: true,
	})
	require.NoError(t, err)
	require.Equal(t, int64(2), rebind.Header.Revision-baseRev)
	require.NotNil(t, rebind.PrevKv)
	require.Equal(t, []byte("one"), rebind.PrevKv.Value)
	require.Equal(t, leaseA.ID, rebind.PrevKv.Lease)

	ignoreValue, err := server.Put(ctx, &etcdserverpb.PutRequest{
		Key: key, Lease: leaseA.ID, IgnoreValue: true, PrevKv: true,
	})
	require.NoError(t, err)
	require.Equal(t, int64(3), ignoreValue.Header.Revision-baseRev)
	require.NotNil(t, ignoreValue.PrevKv)
	require.Equal(t, []byte("two"), ignoreValue.PrevKv.Value)
	require.Equal(t, leaseB.ID, ignoreValue.PrevKv.Lease)

	ignoreLease, err := server.Put(ctx, &etcdserverpb.PutRequest{
		Key: key, Value: []byte("three"), IgnoreLease: true, PrevKv: true,
	})
	require.NoError(t, err)
	require.Equal(t, int64(4), ignoreLease.Header.Revision-baseRev)
	require.NotNil(t, ignoreLease.PrevKv)
	require.Equal(t, []byte("two"), ignoreLease.PrevKv.Value)
	require.Equal(t, leaseA.ID, ignoreLease.PrevKv.Lease)

	current, err := server.Range(ctx, &etcdserverpb.RangeRequest{Key: key})
	require.NoError(t, err)
	require.Len(t, current.Kvs, 1)
	require.Equal(t, []byte("three"), current.Kvs[0].Value)
	require.Equal(t, leaseA.ID, current.Kvs[0].Lease)
	require.Equal(t, create.Header.Revision, current.Kvs[0].CreateRevision)
	require.Equal(t, ignoreLease.Header.Revision, current.Kvs[0].ModRevision)
	require.Equal(t, int64(4), current.Kvs[0].Version)

	ttlA, err := server.LeaseTimeToLive(ctx, &etcdserverpb.LeaseTimeToLiveRequest{ID: leaseA.ID, Keys: true})
	require.NoError(t, err)
	require.Equal(t, [][]byte{key}, ttlA.Keys)
	ttlB, err := server.LeaseTimeToLive(ctx, &etcdserverpb.LeaseTimeToLiveRequest{ID: leaseB.ID, Keys: true})
	require.NoError(t, err)
	require.Empty(t, ttlB.Keys)

	tests := []struct {
		name        string
		req         *etcdserverpb.PutRequest
		wantErr     error
		wantCode    codes.Code
		wantMessage string
	}{
		{
			name:        "missing lease",
			req:         &etcdserverpb.PutRequest{Key: key, Value: []byte("bad"), Lease: 62999},
			wantErr:     rpctypes.ErrGRPCLeaseNotFound,
			wantCode:    codes.NotFound,
			wantMessage: "etcdserver: requested lease not found",
		},
		{
			name:        "missing ignore value key",
			req:         &etcdserverpb.PutRequest{Key: missing, IgnoreValue: true},
			wantErr:     rpctypes.ErrGRPCKeyNotFound,
			wantCode:    codes.InvalidArgument,
			wantMessage: "etcdserver: key not found",
		},
		{
			name:        "missing key and lease",
			req:         &etcdserverpb.PutRequest{Key: missing, Lease: 62999, IgnoreValue: true},
			wantErr:     rpctypes.ErrGRPCLeaseNotFound,
			wantCode:    codes.NotFound,
			wantMessage: "etcdserver: requested lease not found",
		},
		{
			name:        "empty key",
			req:         &etcdserverpb.PutRequest{Value: []byte("bad")},
			wantErr:     rpctypes.ErrGRPCEmptyKey,
			wantCode:    codes.InvalidArgument,
			wantMessage: "etcdserver: key is not provided",
		},
		{
			name:        "value with ignore value",
			req:         &etcdserverpb.PutRequest{Key: key, Value: []byte("bad"), IgnoreValue: true},
			wantErr:     rpctypes.ErrGRPCValueProvided,
			wantCode:    codes.InvalidArgument,
			wantMessage: "etcdserver: value is provided",
		},
		{
			name:        "lease with ignore lease",
			req:         &etcdserverpb.PutRequest{Key: key, Lease: leaseA.ID, IgnoreLease: true},
			wantErr:     rpctypes.ErrGRPCLeaseProvided,
			wantCode:    codes.InvalidArgument,
			wantMessage: "etcdserver: lease is provided",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := server.Put(ctx, tt.req)
			requireDirectKVError(t, err, tt.wantErr, tt.wantCode, tt.wantMessage)
		})
	}
}

func TestPutRejectsInvalidRequest(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()

	tests := []struct {
		name        string
		req         *etcdserverpb.PutRequest
		wantErr     error
		wantMessage string
	}{
		{
			name:        "empty key",
			req:         &etcdserverpb.PutRequest{Value: []byte("v1")},
			wantErr:     rpctypes.ErrGRPCEmptyKey,
			wantMessage: "etcdserver: key is not provided",
		},
		{
			name:        "empty key precedes ignore value conflict",
			req:         &etcdserverpb.PutRequest{Value: []byte("v1"), IgnoreValue: true},
			wantErr:     rpctypes.ErrGRPCEmptyKey,
			wantMessage: "etcdserver: key is not provided",
		},
		{
			name:        "empty key precedes ignore lease conflict",
			req:         &etcdserverpb.PutRequest{Lease: 123, IgnoreLease: true},
			wantErr:     rpctypes.ErrGRPCEmptyKey,
			wantMessage: "etcdserver: key is not provided",
		},
		{
			name:        "ignore value with value",
			req:         &etcdserverpb.PutRequest{Key: []byte("/registry/pods/invalid-put"), Value: []byte("v1"), IgnoreValue: true},
			wantErr:     rpctypes.ErrGRPCValueProvided,
			wantMessage: "etcdserver: value is provided",
		},
		{
			name: "ignore value precedes ignore lease conflict",
			req: &etcdserverpb.PutRequest{
				Key:         []byte("/registry/pods/invalid-put"),
				Value:       []byte("v1"),
				Lease:       123,
				IgnoreValue: true,
				IgnoreLease: true,
			},
			wantErr:     rpctypes.ErrGRPCValueProvided,
			wantMessage: "etcdserver: value is provided",
		},
		{
			name:        "ignore lease with lease",
			req:         &etcdserverpb.PutRequest{Key: []byte("/registry/pods/invalid-put"), Lease: 123, IgnoreLease: true},
			wantErr:     rpctypes.ErrGRPCLeaseProvided,
			wantMessage: "etcdserver: lease is provided",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := server.Put(context.Background(), tt.req)
			requireDirectKVError(t, err, tt.wantErr, codes.InvalidArgument, tt.wantMessage)
		})
	}
}

func TestRangeKeysOnlyOmitsValuesForGet(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()

	ctx := context.Background()
	key := []byte("/registry/pods/keys-only-get")
	_, err := server.Put(ctx, &etcdserverpb.PutRequest{
		Key:   key,
		Value: []byte("hidden"),
	})
	require.NoError(t, err)

	rangeResp, err := server.Range(ctx, &etcdserverpb.RangeRequest{
		Key:      key,
		KeysOnly: true,
	})
	require.NoError(t, err)
	require.Equal(t, int64(1), rangeResp.Count)
	require.Len(t, rangeResp.Kvs, 1)
	require.Equal(t, key, rangeResp.Kvs[0].Key)
	require.Empty(t, rangeResp.Kvs[0].Value)
	require.NotZero(t, rangeResp.Kvs[0].ModRevision)
}

func TestRangeKeysOnlyOmitsValuesForList(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()

	ctx := context.Background()
	prefix := "/registry/pods/keys-only-list/"
	for _, suffix := range []string{"a", "b"} {
		_, err := server.Put(ctx, &etcdserverpb.PutRequest{
			Key:   []byte(prefix + suffix),
			Value: []byte("hidden-" + suffix),
		})
		require.NoError(t, err)
	}

	var rangeResp *etcdserverpb.RangeResponse
	require.Eventually(t, func() bool {
		var err error
		rangeResp, err = server.Range(ctx, &etcdserverpb.RangeRequest{
			Key:      []byte(prefix),
			RangeEnd: []byte("/registry/pods/keys-only-list0"),
			KeysOnly: true,
		})
		return err == nil && len(rangeResp.Kvs) == 2
	}, time.Second, 10*time.Millisecond)
	require.Equal(t, int64(2), rangeResp.Count)
	require.Len(t, rangeResp.Kvs, 2)
	for _, kv := range rangeResp.Kvs {
		require.Contains(t, string(kv.Key), prefix)
		require.Empty(t, kv.Value)
		require.NotZero(t, kv.ModRevision)
	}
}

func TestRangeKeysOnlyLimitedCurrentAndHistoricalRevisions(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()

	ctx := context.Background()
	prefix := "/registry/pods/keys-only-limit-hole/"
	end := []byte("/registry/pods/keys-only-limit-hole0")
	var historicalRevision int64
	for _, suffix := range []string{"00", "01", "02", "03", "04", "05", "06", "07"} {
		response, err := server.Put(ctx, &etcdserverpb.PutRequest{
			Key: []byte(prefix + suffix), Value: []byte("initial-" + suffix),
		})
		require.NoError(t, err)
		historicalRevision = response.Header.Revision
	}
	for _, suffix := range []string{"08", "09", "10", "11"} {
		_, err := server.Put(ctx, &etcdserverpb.PutRequest{
			Key: []byte(prefix + suffix), Value: []byte("later-" + suffix),
		})
		require.NoError(t, err)
	}
	_, err := server.DeleteRange(ctx, &etcdserverpb.DeleteRangeRequest{
		Key: []byte(prefix + "02"),
	})
	require.NoError(t, err)

	assertRange := func(name string, revision, limit int64, wantSuffixes []string, wantCount int64, wantMore bool) {
		t.Helper()
		var response *etcdserverpb.RangeResponse
		require.Eventually(t, func() bool {
			var rangeErr error
			response, rangeErr = server.Range(ctx, &etcdserverpb.RangeRequest{
				Key: []byte(prefix), RangeEnd: end, Revision: revision,
				Limit: limit, KeysOnly: true,
			})
			return rangeErr == nil && response.Count == wantCount && len(response.Kvs) == len(wantSuffixes)
		}, time.Second, 10*time.Millisecond, name)
		require.Equal(t, wantMore, response.More, name)
		for i, suffix := range wantSuffixes {
			require.Equal(t, []byte(prefix+suffix), response.Kvs[i].Key, name)
			require.Empty(t, response.Kvs[i].Value, name)
		}
	}

	assertRange("current page skips deleted key", 0, 3, []string{"00", "01", "03"}, 11, true)
	assertRange("historical page includes then-live key", historicalRevision, 3, []string{"00", "01", "02"}, 8, true)
	assertRange("current large page reports full count", 0, 20,
		[]string{"00", "01", "03", "04", "05", "06", "07", "08", "09", "10", "11"}, 11, false)
}

func TestRangeKeysOnlyLimitAcrossTombstonesAndRecreateMatchesEtcd(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()

	ctx := context.Background()
	prefix := "/registry/pods/tombstone-limit/"
	end := []byte("/registry/pods/tombstone-limit0")
	var beforeDeletes int64
	for _, suffix := range []string{"a", "b", "c", "d"} {
		resp, err := server.Put(ctx, &etcdserverpb.PutRequest{
			Key: []byte(prefix + suffix), Value: []byte("value-" + suffix),
		})
		require.NoError(t, err)
		beforeDeletes = resp.Header.Revision
	}
	_, err := server.DeleteRange(ctx, &etcdserverpb.DeleteRangeRequest{Key: []byte(prefix + "b")})
	require.NoError(t, err)
	deleted, err := server.DeleteRange(ctx, &etcdserverpb.DeleteRangeRequest{Key: []byte(prefix + "d")})
	require.NoError(t, err)
	_, err = server.Put(ctx, &etcdserverpb.PutRequest{Key: []byte(prefix + "c"), Value: []byte("updated-c")})
	require.NoError(t, err)
	_, err = server.Put(ctx, &etcdserverpb.PutRequest{Key: []byte(prefix + "b"), Value: []byte("recreated-b")})
	require.NoError(t, err)
	_, err = server.Put(ctx, &etcdserverpb.PutRequest{Key: []byte(prefix + "e"), Value: []byte("value-e")})
	require.NoError(t, err)

	assertPage := func(name string, revision, limit int64, wantSuffixes []string, wantCount int64, wantMore bool) {
		t.Helper()
		resp, rangeErr := server.Range(ctx, &etcdserverpb.RangeRequest{
			Key: []byte(prefix), RangeEnd: end, Revision: revision,
			Limit: limit, KeysOnly: true,
		})
		require.NoError(t, rangeErr, name)
		require.Equal(t, wantCount, resp.Count, name)
		require.Equal(t, wantMore, resp.More, name)
		require.Len(t, resp.Kvs, len(wantSuffixes), name)
		for i, suffix := range wantSuffixes {
			require.Equal(t, []byte(prefix+suffix), resp.Kvs[i].Key, name)
			require.Empty(t, resp.Kvs[i].Value, name)
		}
	}

	assertPage("before-deletes", beforeDeletes, 2, []string{"a", "b"}, 4, true)
	assertPage("after-deletes", deleted.Header.Revision, 1, []string{"a"}, 2, true)
	assertPage("after-recreate", 0, 2, []string{"a", "b"}, 4, true)
}

func TestRangeCountOnlyTakesPrecedenceOverKeysOnly(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()

	ctx := context.Background()
	prefix := "/registry/pods/keys-and-count/"
	for _, suffix := range []string{"a", "b", "c"} {
		_, err := server.Put(ctx, &etcdserverpb.PutRequest{
			Key:   []byte(prefix + suffix),
			Value: []byte("hidden-" + suffix),
		})
		require.NoError(t, err)
	}

	var response *etcdserverpb.RangeResponse
	require.Eventually(t, func() bool {
		var err error
		response, err = server.Range(ctx, &etcdserverpb.RangeRequest{
			Key:       []byte(prefix),
			RangeEnd:  []byte("/registry/pods/keys-and-count0"),
			KeysOnly:  true,
			CountOnly: true,
		})
		return err == nil && response.Count == 3
	}, time.Second, 10*time.Millisecond)
	require.Empty(t, response.Kvs)
	require.False(t, response.More)
	require.NotNil(t, response.Header)
	require.Positive(t, response.Header.Revision)
}

func TestRangeEmptyNonFromKeyRangeMatchesEtcd(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()

	ctx := context.Background()
	for _, key := range []string{
		"/registry/pods/empty-range/a",
		"/registry/pods/empty-range/b",
	} {
		_, err := server.Put(ctx, &etcdserverpb.PutRequest{
			Key:   []byte(key),
			Value: []byte("value"),
		})
		require.NoError(t, err)
	}

	for _, tc := range []struct {
		name     string
		key      []byte
		rangeEnd []byte
	}{
		{
			name:     "same start and end",
			key:      []byte("/registry/pods/empty-range/a"),
			rangeEnd: []byte("/registry/pods/empty-range/a"),
		},
		{
			name:     "start after end",
			key:      []byte("/registry/pods/empty-range/b"),
			rangeEnd: []byte("/registry/pods/empty-range/a"),
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			resp, err := server.Range(ctx, &etcdserverpb.RangeRequest{
				Key:      tc.key,
				RangeEnd: tc.rangeEnd,
			})
			require.NoError(t, err)
			require.Equal(t, int64(0), resp.Count)
			require.Empty(t, resp.Kvs)
			require.False(t, resp.More)
			require.NotNil(t, resp.Header)
		})
	}
}

func TestRangeSortsByKeyDescending(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()

	ctx := context.Background()
	prefix := "/registry/pods/sort-key/"
	for _, suffix := range []string{"a", "b", "c"} {
		_, err := server.Put(ctx, &etcdserverpb.PutRequest{
			Key:   []byte(prefix + suffix),
			Value: []byte(suffix),
		})
		require.NoError(t, err)
	}

	var rangeResp *etcdserverpb.RangeResponse
	require.Eventually(t, func() bool {
		var err error
		rangeResp, err = server.Range(ctx, &etcdserverpb.RangeRequest{
			Key:        []byte(prefix),
			RangeEnd:   []byte("/registry/pods/sort-key0"),
			SortTarget: etcdserverpb.RangeRequest_KEY,
			SortOrder:  etcdserverpb.RangeRequest_DESCEND,
		})
		return err == nil && len(rangeResp.Kvs) == 3
	}, time.Second, 10*time.Millisecond)
	require.Equal(t, [][]byte{
		[]byte(prefix + "c"),
		[]byte(prefix + "b"),
		[]byte(prefix + "a"),
	}, [][]byte{
		rangeResp.Kvs[0].Key,
		rangeResp.Kvs[1].Key,
		rangeResp.Kvs[2].Key,
	})
}

func TestRangeSortsByValueAscending(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()

	ctx := context.Background()
	valuesByKey := map[string]string{
		"/registry/pods/sort-value/a": "3",
		"/registry/pods/sort-value/b": "1",
		"/registry/pods/sort-value/c": "2",
	}
	for key, value := range valuesByKey {
		_, err := server.Put(ctx, &etcdserverpb.PutRequest{
			Key:   []byte(key),
			Value: []byte(value),
		})
		require.NoError(t, err)
	}

	var rangeResp *etcdserverpb.RangeResponse
	require.Eventually(t, func() bool {
		var err error
		rangeResp, err = server.Range(ctx, &etcdserverpb.RangeRequest{
			Key:        []byte("/registry/pods/sort-value/"),
			RangeEnd:   []byte("/registry/pods/sort-value0"),
			SortTarget: etcdserverpb.RangeRequest_VALUE,
		})
		return err == nil && len(rangeResp.Kvs) == 3
	}, time.Second, 10*time.Millisecond)
	require.Equal(t, [][]byte{[]byte("1"), []byte("2"), []byte("3")}, [][]byte{
		rangeResp.Kvs[0].Value,
		rangeResp.Kvs[1].Value,
		rangeResp.Kvs[2].Value,
	})
}

func TestRangeKeysOnlySortsByValueBeforeElidingValues(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()

	ctx := context.Background()
	prefix := "/registry/pods/keys-only-sort-value/"
	for _, seed := range []struct {
		key   string
		value string
	}{
		{key: "a", value: "z"},
		{key: "b", value: "m"},
		{key: "c", value: "a"},
		{key: "d", value: "n"},
	} {
		_, err := server.Put(ctx, &etcdserverpb.PutRequest{
			Key: []byte(prefix + seed.key), Value: []byte(seed.value),
		})
		require.NoError(t, err)
	}

	var resp *etcdserverpb.RangeResponse
	require.Eventually(t, func() bool {
		var err error
		resp, err = server.Range(ctx, &etcdserverpb.RangeRequest{
			Key: []byte(prefix), RangeEnd: []byte("/registry/pods/keys-only-sort-value0"),
			Limit: 2, KeysOnly: true,
			SortOrder:  etcdserverpb.RangeRequest_DESCEND,
			SortTarget: etcdserverpb.RangeRequest_VALUE,
		})
		return err == nil && resp.Count == 4 && len(resp.Kvs) == 2
	}, time.Second, 10*time.Millisecond)
	require.True(t, resp.More)
	require.Equal(t, [][]byte{[]byte(prefix + "a"), []byte(prefix + "d")}, [][]byte{
		resp.Kvs[0].Key,
		resp.Kvs[1].Key,
	})
	require.Empty(t, resp.Kvs[0].Value)
	require.Empty(t, resp.Kvs[1].Value)
}

func TestRangeNonKeyNoneSortUsesEtcdLimitLookahead(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()

	ctx := context.Background()
	prefix := "/registry/pods/sort-value-limit/"
	for key, value := range map[string]string{
		"a": "z",
		"b": "y",
		"c": "a",
		"d": "0",
	} {
		_, err := server.Put(ctx, &etcdserverpb.PutRequest{
			Key: []byte(prefix + key), Value: []byte(value),
		})
		require.NoError(t, err)
	}

	var resp *etcdserverpb.RangeResponse
	require.Eventually(t, func() bool {
		var err error
		resp, err = server.Range(ctx, &etcdserverpb.RangeRequest{
			Key: []byte(prefix), RangeEnd: []byte("/registry/pods/sort-value-limit0"),
			Limit: 2, SortTarget: etcdserverpb.RangeRequest_VALUE,
		})
		return err == nil && resp.Count == 4
	}, time.Second, 10*time.Millisecond)
	require.True(t, resp.More)
	require.Equal(t, [][]byte{[]byte(prefix + "c"), []byte(prefix + "b")}, [][]byte{
		resp.Kvs[0].Key,
		resp.Kvs[1].Key,
	})
	require.Equal(t, [][]byte{[]byte("a"), []byte("y")}, [][]byte{
		resp.Kvs[0].Value,
		resp.Kvs[1].Value,
	})
}

func TestRangeCreateAndModNoneSortUseEtcdLimitLookahead(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()

	ctx := context.Background()
	prefix := "/registry/pods/sort-create-mod-limit/"
	for _, seed := range []struct {
		key   string
		value string
	}{
		{key: "d", value: "n"},
		{key: "c", value: "a"},
		{key: "b", value: "m"},
		{key: "a", value: "z"},
	} {
		_, err := server.Put(ctx, &etcdserverpb.PutRequest{
			Key: []byte(prefix + seed.key), Value: []byte(seed.value),
		})
		require.NoError(t, err)
	}
	_, err := server.Put(ctx, &etcdserverpb.PutRequest{
		Key: []byte(prefix + "b"), Value: []byte("y"),
	})
	require.NoError(t, err)

	for _, tc := range []struct {
		name       string
		sortTarget etcdserverpb.RangeRequest_SortTarget
		wantKeys   [][]byte
	}{
		{
			name:       "create",
			sortTarget: etcdserverpb.RangeRequest_CREATE,
			wantKeys:   [][]byte{[]byte(prefix + "c"), []byte(prefix + "b")},
		},
		{
			name:       "mod",
			sortTarget: etcdserverpb.RangeRequest_MOD,
			wantKeys:   [][]byte{[]byte(prefix + "c"), []byte(prefix + "a")},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var resp *etcdserverpb.RangeResponse
			require.Eventually(t, func() bool {
				var err error
				resp, err = server.Range(ctx, &etcdserverpb.RangeRequest{
					Key: []byte(prefix), RangeEnd: []byte("/registry/pods/sort-create-mod-limit0"),
					Limit: 2, SortTarget: tc.sortTarget,
				})
				return err == nil && resp.Count == 4 && len(resp.Kvs) == 2
			}, time.Second, 10*time.Millisecond)
			require.True(t, resp.More)
			require.Equal(t, tc.wantKeys, [][]byte{resp.Kvs[0].Key, resp.Kvs[1].Key})
		})
	}
}

func TestRangeMaxIntLimitDoesNotOverflow(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()

	ctx := context.Background()
	prefix := "/registry/pods/max-int-limit/"
	values := map[string]string{"a": "z", "b": "a", "c": "m"}
	var latestRevision int64
	for suffix, value := range values {
		putResp, err := server.Put(ctx, &etcdserverpb.PutRequest{
			Key: []byte(prefix + suffix), Value: []byte(value),
		})
		require.NoError(t, err)
		if putResp.Header.Revision > latestRevision {
			latestRevision = putResp.Header.Revision
		}
	}

	resp, err := server.Range(ctx, &etcdserverpb.RangeRequest{
		Key: []byte(prefix), RangeEnd: []byte("/registry/pods/max-int-limit0"),
		Limit: math.MaxInt64,
	})
	require.NoError(t, err)
	require.NotNil(t, resp.Header)
	require.Equal(t, latestRevision, resp.Header.Revision)
	require.Equal(t, int64(3), resp.Count)
	require.Len(t, resp.Kvs, 3)
	require.False(t, resp.More)

	valueSorted, err := server.Range(ctx, &etcdserverpb.RangeRequest{
		Key: []byte(prefix), RangeEnd: []byte("/registry/pods/max-int-limit0"),
		Limit: math.MaxInt64, SortTarget: etcdserverpb.RangeRequest_VALUE,
	})
	require.NoError(t, err)
	require.NotNil(t, valueSorted.Header)
	require.Equal(t, latestRevision, valueSorted.Header.Revision)
	require.Equal(t, int64(3), valueSorted.Count)
	require.Len(t, valueSorted.Kvs, 3)
	require.False(t, valueSorted.More)
	require.Equal(t, [][]byte{[]byte(prefix + "b"), []byte(prefix + "c"), []byte(prefix + "a")}, [][]byte{
		valueSorted.Kvs[0].Key, valueSorted.Kvs[1].Key, valueSorted.Kvs[2].Key,
	})

	txn, err := server.Txn(ctx, &etcdserverpb.TxnRequest{Success: []*etcdserverpb.RequestOp{{
		Request: &etcdserverpb.RequestOp_RequestRange{
			RequestRange: &etcdserverpb.RangeRequest{
				Key: []byte(prefix), RangeEnd: []byte("/registry/pods/max-int-limit0"),
				Limit: math.MaxInt64, SortTarget: etcdserverpb.RangeRequest_VALUE,
			},
		},
	}}})
	require.NoError(t, err)
	require.NotNil(t, txn.Header)
	require.Equal(t, latestRevision, txn.Header.Revision)
	require.Len(t, txn.Responses, 1)
	txnRange := txn.Responses[0].GetResponseRange()
	require.NotNil(t, txnRange)
	require.NotNil(t, txnRange.Header)
	require.Equal(t, txn.Header.Revision, txnRange.Header.Revision)
	require.Equal(t, int64(3), txnRange.Count)
	require.Len(t, txnRange.Kvs, 3)
	require.False(t, txnRange.More)
	require.Equal(t, [][]byte{[]byte(prefix + "b"), []byte(prefix + "c"), []byte(prefix + "a")}, [][]byte{
		txnRange.Kvs[0].Key, txnRange.Kvs[1].Key, txnRange.Kvs[2].Key,
	})
}

func TestRangeNegativeLimitMatchesEtcd(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()

	ctx := context.Background()
	prefix := "/registry/pods/negative-limit/"
	end := []byte("/registry/pods/negative-limit0")
	var latestRevision int64
	for _, suffix := range []string{"c", "a", "b"} {
		putResp, err := server.Put(ctx, &etcdserverpb.PutRequest{
			Key: []byte(prefix + suffix), Value: []byte("value-" + suffix),
		})
		require.NoError(t, err)
		latestRevision = putResp.Header.Revision
	}

	resp, err := server.Range(ctx, &etcdserverpb.RangeRequest{
		Key: []byte(prefix), RangeEnd: end, Limit: -1,
		SortOrder: etcdserverpb.RangeRequest_ASCEND, SortTarget: etcdserverpb.RangeRequest_KEY,
	})
	require.NoError(t, err)
	require.NotNil(t, resp.Header)
	require.Equal(t, latestRevision, resp.Header.Revision)
	require.Equal(t, int64(3), resp.Count)
	require.False(t, resp.More)
	require.Equal(t, [][]byte{[]byte(prefix + "a"), []byte(prefix + "b"), []byte(prefix + "c")}, [][]byte{
		resp.Kvs[0].Key, resp.Kvs[1].Key, resp.Kvs[2].Key,
	})

	txn, err := server.Txn(ctx, &etcdserverpb.TxnRequest{Success: []*etcdserverpb.RequestOp{{
		Request: &etcdserverpb.RequestOp_RequestRange{
			RequestRange: &etcdserverpb.RangeRequest{
				Key: []byte(prefix), RangeEnd: end, Limit: -1,
				SortOrder: etcdserverpb.RangeRequest_DESCEND, SortTarget: etcdserverpb.RangeRequest_KEY,
			},
		},
	}}})
	require.NoError(t, err)
	require.NotNil(t, txn.Header)
	require.Equal(t, latestRevision, txn.Header.Revision)
	require.Len(t, txn.Responses, 1)
	txnRange := txn.Responses[0].GetResponseRange()
	require.NotNil(t, txnRange)
	require.NotNil(t, txnRange.Header)
	require.Equal(t, txn.Header.Revision, txnRange.Header.Revision)
	require.Equal(t, int64(3), txnRange.Count)
	require.False(t, txnRange.More)
	require.Equal(t, [][]byte{[]byte(prefix + "c"), []byte(prefix + "b"), []byte(prefix + "a")}, [][]byte{
		txnRange.Kvs[0].Key, txnRange.Kvs[1].Key, txnRange.Kvs[2].Key,
	})
}

func TestTxnRangeNonKeyNoneSortUsesEtcdLimitLookahead(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()

	ctx := context.Background()
	prefix := "/registry/pods/txn-sort-value-limit/"
	var latestRevision int64
	for _, seed := range []struct {
		key   string
		value string
	}{
		{key: "a", value: "z"},
		{key: "b", value: "y"},
		{key: "c", value: "a"},
		{key: "d", value: "0"},
	} {
		putResp, err := server.Put(ctx, &etcdserverpb.PutRequest{
			Key: []byte(prefix + seed.key), Value: []byte(seed.value),
		})
		require.NoError(t, err)
		latestRevision = putResp.Header.Revision
	}

	txn, err := server.Txn(ctx, &etcdserverpb.TxnRequest{Success: []*etcdserverpb.RequestOp{
		{Request: &etcdserverpb.RequestOp_RequestRange{RequestRange: &etcdserverpb.RangeRequest{
			Key: []byte(prefix), RangeEnd: []byte("/registry/pods/txn-sort-value-limit0"),
			Limit: 2, SortTarget: etcdserverpb.RangeRequest_VALUE,
		}}},
	}})
	require.NoError(t, err)
	require.NotNil(t, txn.Header)
	require.Equal(t, latestRevision, txn.Header.Revision)
	require.Len(t, txn.Responses, 1)
	resp := txn.Responses[0].GetResponseRange()
	require.NotNil(t, resp)
	require.NotNil(t, resp.Header)
	require.Equal(t, txn.Header.Revision, resp.Header.Revision)
	require.Equal(t, int64(4), resp.Count)
	require.True(t, resp.More)
	require.Equal(t, [][]byte{[]byte(prefix + "c"), []byte(prefix + "b")}, [][]byte{
		resp.Kvs[0].Key,
		resp.Kvs[1].Key,
	})
	require.Equal(t, [][]byte{[]byte("a"), []byte("y")}, [][]byte{
		resp.Kvs[0].Value,
		resp.Kvs[1].Value,
	})
}

func TestRangeRejectsInvalidSortOptions(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()

	tests := []struct {
		name        string
		req         *etcdserverpb.RangeRequest
		wantErr     error
		wantMessage string
	}{
		{
			name: "empty key precedes invalid sort",
			req: &etcdserverpb.RangeRequest{
				SortOrder:  etcdserverpb.RangeRequest_SortOrder(99),
				SortTarget: etcdserverpb.RangeRequest_SortTarget(99),
			},
			wantErr:     rpctypes.ErrGRPCEmptyKey,
			wantMessage: "etcdserver: key is not provided",
		},
		{
			name: "invalid sort order",
			req: &etcdserverpb.RangeRequest{
				Key:       []byte("/registry/pods/invalid-sort"),
				SortOrder: etcdserverpb.RangeRequest_SortOrder(99),
			},
			wantErr:     rpctypes.ErrGRPCInvalidSortOption,
			wantMessage: "etcdserver: invalid sort option",
		},
		{
			name: "invalid sort target",
			req: &etcdserverpb.RangeRequest{
				Key:        []byte("/registry/pods/invalid-sort"),
				SortTarget: etcdserverpb.RangeRequest_SortTarget(99),
			},
			wantErr:     rpctypes.ErrGRPCInvalidSortOption,
			wantMessage: "etcdserver: invalid sort option",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := server.Range(context.Background(), tt.req)
			requireDirectKVError(t, err, tt.wantErr, codes.InvalidArgument, tt.wantMessage)
		})
	}
}

func TestRangeFiltersByModRevision(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()

	ctx := context.Background()
	prefix := "/registry/pods/filter-mod/"
	putA, err := server.Put(ctx, &etcdserverpb.PutRequest{
		Key:   []byte(prefix + "a"),
		Value: []byte("old"),
	})
	require.NoError(t, err)
	putB, err := server.Put(ctx, &etcdserverpb.PutRequest{
		Key:   []byte(prefix + "b"),
		Value: []byte("new"),
	})
	require.NoError(t, err)

	var rangeResp *etcdserverpb.RangeResponse
	require.Eventually(t, func() bool {
		var err error
		rangeResp, err = server.Range(ctx, &etcdserverpb.RangeRequest{
			Key:            []byte(prefix),
			RangeEnd:       []byte("/registry/pods/filter-mod0"),
			MinModRevision: putB.Header.Revision,
		})
		return err == nil && len(rangeResp.Kvs) == 1
	}, time.Second, 10*time.Millisecond)
	require.Equal(t, int64(2), rangeResp.Count)
	require.Equal(t, []byte(prefix+"b"), rangeResp.Kvs[0].Key)

	countResp, err := server.Range(ctx, &etcdserverpb.RangeRequest{
		Key:            []byte(prefix),
		RangeEnd:       []byte("/registry/pods/filter-mod0"),
		MaxModRevision: putA.Header.Revision,
		CountOnly:      true,
	})
	require.NoError(t, err)
	require.Equal(t, int64(2), countResp.Count)
	require.Empty(t, countResp.Kvs)
}

func TestRangeContradictoryModRevisionFiltersPreserveEtcdCount(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()

	ctx := context.Background()
	prefix := "/registry/pods/filter-mod-contradictory/"
	for _, seed := range []struct {
		key   string
		value string
	}{
		{key: "a", value: "a"},
		{key: "b", value: "old"},
		{key: "c", value: "c"},
	} {
		_, err := server.Put(ctx, &etcdserverpb.PutRequest{
			Key: []byte(prefix + seed.key), Value: []byte(seed.value),
		})
		require.NoError(t, err)
	}
	updateB, err := server.Put(ctx, &etcdserverpb.PutRequest{
		Key: []byte(prefix + "b"), Value: []byte("new"),
	})
	require.NoError(t, err)

	var rangeResp *etcdserverpb.RangeResponse
	require.Eventually(t, func() bool {
		var err error
		rangeResp, err = server.Range(ctx, &etcdserverpb.RangeRequest{
			Key: []byte(prefix), RangeEnd: []byte("/registry/pods/filter-mod-contradictory0"),
			MinModRevision: updateB.Header.Revision,
			MaxModRevision: updateB.Header.Revision - 1,
			Limit:          1,
		})
		return err == nil && rangeResp.Count == 3
	}, time.Second, 10*time.Millisecond)
	require.NotNil(t, rangeResp.Header)
	require.Equal(t, updateB.Header.Revision, rangeResp.Header.Revision)
	require.Empty(t, rangeResp.Kvs)
	require.False(t, rangeResp.More)

	txn, err := server.Txn(ctx, &etcdserverpb.TxnRequest{Success: []*etcdserverpb.RequestOp{{
		Request: &etcdserverpb.RequestOp_RequestRange{
			RequestRange: &etcdserverpb.RangeRequest{
				Key: []byte(prefix), RangeEnd: []byte("/registry/pods/filter-mod-contradictory0"),
				MinModRevision: updateB.Header.Revision,
				MaxModRevision: updateB.Header.Revision - 1,
				Limit:          1,
			},
		},
	}}})
	require.NoError(t, err)
	require.NotNil(t, txn.Header)
	require.Equal(t, updateB.Header.Revision, txn.Header.Revision)
	require.Len(t, txn.Responses, 1)
	txnRange := txn.Responses[0].GetResponseRange()
	require.NotNil(t, txnRange)
	require.NotNil(t, txnRange.Header)
	require.Equal(t, txn.Header.Revision, txnRange.Header.Revision)
	require.Equal(t, int64(3), txnRange.Count)
	require.Empty(t, txnRange.Kvs)
	require.False(t, txnRange.More)
}

func TestRangeAppliesLimitAfterModRevisionFilter(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()

	ctx := context.Background()
	prefix := "/registry/pods/filter-limit/"
	putA, err := server.Put(ctx, &etcdserverpb.PutRequest{
		Key:   []byte(prefix + "a"),
		Value: []byte("old"),
	})
	require.NoError(t, err)
	_, err = server.Put(ctx, &etcdserverpb.PutRequest{
		Key:   []byte(prefix + "b"),
		Value: []byte("newer-1"),
	})
	require.NoError(t, err)
	_, err = server.Put(ctx, &etcdserverpb.PutRequest{
		Key:   []byte(prefix + "c"),
		Value: []byte("newer-2"),
	})
	require.NoError(t, err)

	var rangeResp *etcdserverpb.RangeResponse
	require.Eventually(t, func() bool {
		var err error
		rangeResp, err = server.Range(ctx, &etcdserverpb.RangeRequest{
			Key:            []byte(prefix),
			RangeEnd:       []byte("/registry/pods/filter-limit0"),
			MinModRevision: putA.Header.Revision + 1,
			Limit:          1,
		})
		return err == nil && rangeResp.Count == 3 && len(rangeResp.Kvs) == 1
	}, time.Second, 10*time.Millisecond)
	require.True(t, rangeResp.More)
	require.Equal(t, []byte(prefix+"b"), rangeResp.Kvs[0].Key)
}

func TestRangeLimitCountReportsTotalMatches(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()

	ctx := context.Background()
	prefix := "/registry/pods/limit-count/"
	for _, name := range []string{"a", "b", "c", "d", "e"} {
		_, err := server.Put(ctx, &etcdserverpb.PutRequest{
			Key:   []byte(prefix + name),
			Value: []byte(name),
		})
		require.NoError(t, err)
	}

	var rangeResp *etcdserverpb.RangeResponse
	require.Eventually(t, func() bool {
		var err error
		rangeResp, err = server.Range(ctx, &etcdserverpb.RangeRequest{
			Key:      []byte(prefix),
			RangeEnd: []byte("/registry/pods/limit-count0"),
			Limit:    2,
		})
		return err == nil && rangeResp.Count == 5 && len(rangeResp.Kvs) == 2
	}, time.Second, 10*time.Millisecond)
	require.True(t, rangeResp.More)
	require.Equal(t, []byte(prefix+"a"), rangeResp.Kvs[0].Key)
	require.Equal(t, []byte(prefix+"b"), rangeResp.Kvs[1].Key)

	var continueResp *etcdserverpb.RangeResponse
	require.Eventually(t, func() bool {
		var err error
		continueResp, err = server.Range(ctx, &etcdserverpb.RangeRequest{
			Key:      append([]byte(prefix+"b"), 0),
			RangeEnd: []byte("/registry/pods/limit-count0"),
			Limit:    2,
		})
		return err == nil && continueResp.Count == 3 && len(continueResp.Kvs) == 2
	}, time.Second, 10*time.Millisecond)
	require.True(t, continueResp.More)
	require.Equal(t, []byte(prefix+"c"), continueResp.Kvs[0].Key)
	require.Equal(t, []byte(prefix+"d"), continueResp.Kvs[1].Key)
}

func TestRangeCountOnlyLimitWithModRevisionFilterDoesNotTruncateCount(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()

	ctx := context.Background()
	prefix := "/registry/pods/filter-count-limit/"
	first, err := server.Put(ctx, &etcdserverpb.PutRequest{
		Key:   []byte(prefix + "a"),
		Value: []byte("a"),
	})
	require.NoError(t, err)
	_, err = server.Put(ctx, &etcdserverpb.PutRequest{
		Key:   []byte(prefix + "b"),
		Value: []byte("b"),
	})
	require.NoError(t, err)
	_, err = server.Put(ctx, &etcdserverpb.PutRequest{
		Key:   []byte(prefix + "c"),
		Value: []byte("c"),
	})
	require.NoError(t, err)

	var rangeResp *etcdserverpb.RangeResponse
	require.Eventually(t, func() bool {
		var err error
		rangeResp, err = server.Range(ctx, &etcdserverpb.RangeRequest{
			Key:            []byte(prefix),
			RangeEnd:       []byte("/registry/pods/filter-count-limit0"),
			MinModRevision: first.Header.Revision,
			Limit:          1,
			CountOnly:      true,
		})
		return err == nil && rangeResp.Count == 3
	}, time.Second, 10*time.Millisecond)
	require.Empty(t, rangeResp.Kvs)
	require.False(t, rangeResp.More)
}

func TestHistoricalRangeCountOnlyLimitNeverReportsMore(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()

	ctx := context.Background()
	prefix := "/registry/pods/historical-count-limit/"
	end := []byte("/registry/pods/historical-count-limit0")
	var historicalRevision int64
	for _, suffix := range []string{"a", "b", "c"} {
		resp, err := server.Put(ctx, &etcdserverpb.PutRequest{
			Key: []byte(prefix + suffix), Value: []byte("initial"),
		})
		require.NoError(t, err)
		historicalRevision = resp.Header.Revision
	}
	_, err := server.Put(ctx, &etcdserverpb.PutRequest{
		Key: []byte(prefix + "a"), Value: []byte("updated"),
	})
	require.NoError(t, err)

	resp, err := server.Range(ctx, &etcdserverpb.RangeRequest{
		Key: []byte(prefix), RangeEnd: end, Revision: historicalRevision,
		Limit: 1, CountOnly: true,
	})
	require.NoError(t, err)
	require.Equal(t, int64(3), resp.Count)
	require.Empty(t, resp.Kvs)
	require.False(t, resp.More)
}

func TestRangeCreateRevisionFilter(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()

	ctx := context.Background()
	prefix := "/registry/pods/create-filter/"
	first, err := server.Put(ctx, &etcdserverpb.PutRequest{
		Key:   []byte(prefix + "a"),
		Value: []byte("a"),
	})
	require.NoError(t, err)
	second, err := server.Put(ctx, &etcdserverpb.PutRequest{
		Key:   []byte(prefix + "b"),
		Value: []byte("b"),
	})
	require.NoError(t, err)
	_, err = server.Put(ctx, &etcdserverpb.PutRequest{
		Key:   []byte(prefix + "c"),
		Value: []byte("c"),
	})
	require.NoError(t, err)

	var rangeResp *etcdserverpb.RangeResponse
	require.Eventually(t, func() bool {
		var err error
		rangeResp, err = server.Range(ctx, &etcdserverpb.RangeRequest{
			Key:               []byte(prefix),
			RangeEnd:          []byte("/registry/pods/create-filter0"),
			MinCreateRevision: second.Header.Revision,
			Limit:             1,
		})
		return err == nil && rangeResp.Count == 3 && len(rangeResp.Kvs) == 1
	}, time.Second, 10*time.Millisecond)
	require.True(t, rangeResp.More)
	require.Equal(t, []byte(prefix+"b"), rangeResp.Kvs[0].Key)

	rangeResp, err = server.Range(ctx, &etcdserverpb.RangeRequest{
		Key:               []byte(prefix),
		RangeEnd:          []byte("/registry/pods/create-filter0"),
		MinCreateRevision: first.Header.Revision,
		MaxCreateRevision: second.Header.Revision,
	})
	require.NoError(t, err)
	require.Equal(t, int64(3), rangeResp.Count)
	require.Len(t, rangeResp.Kvs, 2)
}

func TestRangeCreateAndVersionSortTargetsAreSupported(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()

	ctx := context.Background()
	prefix := "/registry/pods/sort-target/"
	_, err := server.Put(ctx, &etcdserverpb.PutRequest{Key: []byte(prefix + "b"), Value: []byte("b")})
	require.NoError(t, err)
	_, err = server.Put(ctx, &etcdserverpb.PutRequest{Key: []byte(prefix + "a"), Value: []byte("a")})
	require.NoError(t, err)

	var createResp *etcdserverpb.RangeResponse
	require.Eventually(t, func() bool {
		var err error
		createResp, err = server.Range(ctx, &etcdserverpb.RangeRequest{
			Key:        []byte(prefix),
			RangeEnd:   []byte("/registry/pods/sort-target0"),
			SortTarget: etcdserverpb.RangeRequest_CREATE,
			SortOrder:  etcdserverpb.RangeRequest_ASCEND,
		})
		return err == nil && len(createResp.Kvs) == 2
	}, time.Second, 10*time.Millisecond)
	require.Equal(t, []byte(prefix+"b"), createResp.Kvs[0].Key)
	require.Equal(t, []byte(prefix+"a"), createResp.Kvs[1].Key)
	require.Less(t, createResp.Kvs[0].CreateRevision, createResp.Kvs[1].CreateRevision)

	versionResp, err := server.Range(ctx, &etcdserverpb.RangeRequest{
		Key:        []byte(prefix),
		RangeEnd:   []byte("/registry/pods/sort-target0"),
		SortTarget: etcdserverpb.RangeRequest_VERSION,
		SortOrder:  etcdserverpb.RangeRequest_ASCEND,
	})
	require.NoError(t, err)
	require.Len(t, versionResp.Kvs, 2)
	require.LessOrEqual(t, versionResp.Kvs[0].Version, versionResp.Kvs[1].Version)
}

func TestRangeFutureRevisionMatchesEtcd(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()

	ctx := context.Background()
	putResp, err := server.Put(ctx, &etcdserverpb.PutRequest{
		Key:   []byte("/registry/pods/future-rev/a"),
		Value: []byte("a"),
	})
	require.NoError(t, err)

	tests := []struct {
		name string
		req  *etcdserverpb.RangeRequest
	}{
		{
			name: "get",
			req: &etcdserverpb.RangeRequest{
				Key:      []byte("/registry/pods/future-rev/a"),
				Revision: putResp.Header.Revision + 1,
			},
		},
		{
			name: "list",
			req: &etcdserverpb.RangeRequest{
				Key:      []byte("/registry/pods/future-rev/"),
				RangeEnd: []byte("/registry/pods/future-rev0"),
				Revision: putResp.Header.Revision + 1,
			},
		},
		{
			name: "count",
			req: &etcdserverpb.RangeRequest{
				Key:       []byte("/registry/pods/future-rev/"),
				RangeEnd:  []byte("/registry/pods/future-rev0"),
				Revision:  putResp.Header.Revision + 1,
				CountOnly: true,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := server.Range(ctx, tt.req)
			requireDirectKVError(t, err, rpctypes.ErrGRPCFutureRev, codes.OutOfRange, "etcdserver: mvcc: required revision is a future revision")
		})
	}
}

func TestFollowerHistoricalRangeProxiesToLeader(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	metrics := mock.NewMinimalMetrics(ctrl)
	kv := memkv.NewKvStorage()
	defer func() {
		require.NoError(t, kv.Close())
	}()
	backendStore := backend.NewBackend(kv, backend.Config{
		Identity:                "test-peer",
		EnableEtcdCompatibility: true,
	}, metrics)

	called := false
	server := New(backendStore, metrics, testPeerService{
		isLeader:     false,
		proxyEnabled: true,
		rangeFn: func(_ context.Context, req *etcdserverpb.RangeRequest) (*etcdserverpb.RangeResponse, error) {
			called = true
			require.Equal(t, []byte("/registry/pods/historical"), req.Key)
			require.Equal(t, int64(10), req.Revision)
			return &etcdserverpb.RangeResponse{
				Header: &etcdserverpb.ResponseHeader{Revision: 20},
				Kvs: []*mvccpb.KeyValue{{
					Key:         req.Key,
					Value:       []byte("leader"),
					ModRevision: 10,
				}},
				Count: 1,
			}, nil
		},
	})
	defer server.stopLeases()

	resp, err := server.Range(context.Background(), &etcdserverpb.RangeRequest{
		Key:      []byte("/registry/pods/historical"),
		Revision: 10,
	})
	require.NoError(t, err)
	require.True(t, called)
	require.Equal(t, int64(1), resp.Count)
	require.Equal(t, []byte("leader"), resp.Kvs[0].Value)
}

func TestRangeCompactedRevisionMatchesEtcd(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()

	ctx := context.Background()
	putResp, err := server.Put(ctx, &etcdserverpb.PutRequest{
		Key:   []byte("/registry/pods/compacted"),
		Value: []byte("v1"),
	})
	require.NoError(t, err)
	putResp, err = server.Put(ctx, &etcdserverpb.PutRequest{
		Key:   []byte("/registry/pods/compacted"),
		Value: []byte("v2"),
	})
	require.NoError(t, err)
	require.Eventually(t, func() bool {
		return server.backend.GetCurrentRevision() >= uint64(putResp.Header.Revision)
	}, time.Second, 10*time.Millisecond)
	compactRev := putResp.Header.Revision

	_, err = server.Compact(ctx, &etcdserverpb.CompactionRequest{Revision: compactRev})
	require.NoError(t, err)

	_, err = server.Range(ctx, &etcdserverpb.RangeRequest{
		Key:      []byte("/registry/pods/compacted"),
		Revision: compactRev - 1,
	})
	requireDirectKVError(t, err, rpctypes.ErrGRPCCompacted, codes.OutOfRange, "etcdserver: mvcc: required revision has been compacted")

	resp, err := server.Range(ctx, &etcdserverpb.RangeRequest{
		Key:      []byte("/registry/pods/compacted"),
		Revision: compactRev,
	})
	require.NoError(t, err)
	require.Len(t, resp.Kvs, 1)
}

func TestCompactFutureAndRepeatedRevisionMatchEtcd(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()

	ctx := context.Background()
	putResp, err := server.Put(ctx, &etcdserverpb.PutRequest{
		Key:   []byte("/registry/pods/compact-errors"),
		Value: []byte("v1"),
	})
	require.NoError(t, err)
	require.Eventually(t, func() bool {
		return server.backend.GetCurrentRevision() >= uint64(putResp.Header.Revision)
	}, time.Second, 10*time.Millisecond)
	compactRev := putResp.Header.Revision

	_, err = server.Compact(ctx, &etcdserverpb.CompactionRequest{Revision: compactRev + 1})
	requireDirectKVError(t, err, rpctypes.ErrGRPCFutureRev, codes.OutOfRange, "etcdserver: mvcc: required revision is a future revision")

	_, err = server.Compact(ctx, &etcdserverpb.CompactionRequest{Revision: compactRev})
	require.NoError(t, err)

	_, err = server.Compact(ctx, &etcdserverpb.CompactionRequest{Revision: compactRev})
	requireDirectKVError(t, err, rpctypes.ErrGRPCCompacted, codes.OutOfRange, "etcdserver: mvcc: required revision has been compacted")
}

func TestCompactDifferentialScenarioMatchesEtcd(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()

	ctx := context.Background()
	key := []byte("/registry/pods/compact-differential")
	first, err := server.Put(ctx, &etcdserverpb.PutRequest{Key: key, Value: []byte("v1")})
	require.NoError(t, err)
	second, err := server.Put(ctx, &etcdserverpb.PutRequest{Key: key, Value: []byte("v2")})
	require.NoError(t, err)
	third, err := server.Put(ctx, &etcdserverpb.PutRequest{
		Key: []byte("/registry/pods/compact-differential-tail"), Value: []byte("tail"),
	})
	require.NoError(t, err)
	require.Eventually(t, func() bool {
		return server.backend.GetCurrentRevision() >= uint64(third.Header.Revision)
	}, time.Second, 10*time.Millisecond)

	compact, err := server.Compact(ctx, &etcdserverpb.CompactionRequest{Revision: second.Header.Revision})
	require.NoError(t, err)
	require.GreaterOrEqual(t, compact.Header.Revision, second.Header.Revision)
	require.LessOrEqual(t, compact.Header.Revision, third.Header.Revision)

	boundary, err := server.Range(ctx, &etcdserverpb.RangeRequest{Key: key, Revision: second.Header.Revision})
	require.NoError(t, err)
	require.Len(t, boundary.Kvs, 1)
	require.Equal(t, []byte("v2"), boundary.Kvs[0].Value)

	_, err = server.Range(ctx, &etcdserverpb.RangeRequest{Key: key, Revision: first.Header.Revision})
	requireDirectKVError(t, err, rpctypes.ErrGRPCCompacted, codes.OutOfRange, "etcdserver: mvcc: required revision has been compacted")

	for _, revision := range []int64{second.Header.Revision, first.Header.Revision, -1} {
		_, err := server.Compact(ctx, &etcdserverpb.CompactionRequest{Revision: revision})
		requireDirectKVError(t, err, rpctypes.ErrGRPCCompacted, codes.OutOfRange, "etcdserver: mvcc: required revision has been compacted")
	}

	_, err = server.Compact(ctx, &etcdserverpb.CompactionRequest{Revision: third.Header.Revision + 1000})
	requireDirectKVError(t, err, rpctypes.ErrGRPCFutureRev, codes.OutOfRange, "etcdserver: mvcc: required revision is a future revision")

	current, err := server.Range(ctx, &etcdserverpb.RangeRequest{Key: key})
	require.NoError(t, err)
	require.Len(t, current.Kvs, 1)
	require.Equal(t, []byte("v2"), current.Kvs[0].Value)
}

func TestCompactZeroPreservesHistoryAndIsDurablyRepeatable(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()

	ctx := context.Background()
	key := []byte("/registry/pods/compact-zero")
	first, err := server.Put(ctx, &etcdserverpb.PutRequest{Key: key, Value: []byte("v1")})
	require.NoError(t, err)
	_, err = server.Put(ctx, &etcdserverpb.PutRequest{Key: key, Value: []byte("v2")})
	require.NoError(t, err)

	_, err = server.Compact(ctx, &etcdserverpb.CompactionRequest{Revision: 0})
	require.NoError(t, err)
	hasMarker, err := server.backend.HasCompactRevision(ctx)
	require.NoError(t, err)
	require.True(t, hasMarker)

	historical, err := server.Range(ctx, &etcdserverpb.RangeRequest{Key: key, Revision: first.Header.Revision})
	require.NoError(t, err)
	require.Len(t, historical.Kvs, 1)
	require.Equal(t, []byte("v1"), historical.Kvs[0].Value)

	_, err = server.Compact(ctx, &etcdserverpb.CompactionRequest{Revision: 0})
	requireDirectKVError(t, err, rpctypes.ErrGRPCCompacted, codes.OutOfRange, "etcdserver: mvcc: required revision has been compacted")
}

func TestCompactNegativeRevisionCannotDiscardHistory(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()

	ctx := context.Background()
	key := []byte("/registry/pods/compact-negative")
	first, err := server.Put(ctx, &etcdserverpb.PutRequest{Key: key, Value: []byte("v1")})
	require.NoError(t, err)
	_, err = server.Put(ctx, &etcdserverpb.PutRequest{Key: key, Value: []byte("v2")})
	require.NoError(t, err)

	_, err = server.Compact(ctx, &etcdserverpb.CompactionRequest{Revision: -1})
	requireDirectKVError(t, err, rpctypes.ErrGRPCCompacted, codes.OutOfRange, "etcdserver: mvcc: required revision has been compacted")
	hasMarker, err := server.backend.HasCompactRevision(ctx)
	require.NoError(t, err)
	require.False(t, hasMarker)

	historical, err := server.Range(ctx, &etcdserverpb.RangeRequest{Key: key, Revision: first.Header.Revision})
	require.NoError(t, err)
	require.Len(t, historical.Kvs, 1)
	require.Equal(t, []byte("v1"), historical.Kvs[0].Value)
}

func TestCompactPhysicalRevisionBoundariesMatchEtcd(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()

	ctx := context.Background()
	putResp, err := server.Put(ctx, &etcdserverpb.PutRequest{
		Key: []byte("/registry/pods/compact-physical-boundary"), Value: []byte("value"),
	})
	require.NoError(t, err)
	require.Eventually(t, func() bool {
		return server.backend.GetCurrentRevision() >= uint64(putResp.Header.Revision)
	}, time.Second, 10*time.Millisecond)
	_, err = server.Compact(ctx, &etcdserverpb.CompactionRequest{Revision: putResp.Header.Revision})
	require.NoError(t, err)

	tests := []struct {
		name        string
		revision    int64
		wantErr     error
		wantMessage string
	}{
		{
			name:        "zero",
			wantErr:     rpctypes.ErrGRPCCompacted,
			wantMessage: "etcdserver: mvcc: required revision has been compacted",
		},
		{
			name:        "negative",
			revision:    -1,
			wantErr:     rpctypes.ErrGRPCCompacted,
			wantMessage: "etcdserver: mvcc: required revision has been compacted",
		},
		{
			name:        "max-int",
			revision:    math.MaxInt64,
			wantErr:     rpctypes.ErrGRPCFutureRev,
			wantMessage: "etcdserver: mvcc: required revision is a future revision",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := server.Compact(ctx, &etcdserverpb.CompactionRequest{
				Revision: tt.revision, Physical: true,
			})
			requireDirectKVError(t, err, tt.wantErr, codes.OutOfRange, tt.wantMessage)
		})
	}
}

func TestRangeNegativeRevisionFollowsFirstRevision(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()

	ctx := context.Background()
	key := []byte("/registry/pods/negative-range-revision")
	put, err := server.Put(ctx, &etcdserverpb.PutRequest{Key: key, Value: []byte("value")})
	require.NoError(t, err)
	rangeOp := func(revision int64) *etcdserverpb.RequestOp {
		return &etcdserverpb.RequestOp{
			Request: &etcdserverpb.RequestOp_RequestRange{
				RequestRange: &etcdserverpb.RangeRequest{Key: key, Revision: revision},
			},
		}
	}

	resp, err := server.Range(ctx, &etcdserverpb.RangeRequest{Key: key, Revision: -1})
	require.NoError(t, err)
	require.Len(t, resp.Kvs, 1)

	resp, err = server.Range(ctx, &etcdserverpb.RangeRequest{Key: key, Revision: -2})
	require.NoError(t, err)
	require.Len(t, resp.Kvs, 1)

	txnResp, err := server.Txn(ctx, &etcdserverpb.TxnRequest{
		Success: []*etcdserverpb.RequestOp{rangeOp(-1)},
	})
	require.NoError(t, err)
	require.Len(t, txnResp.Responses, 1)

	_, err = server.Txn(ctx, &etcdserverpb.TxnRequest{
		Success: []*etcdserverpb.RequestOp{rangeOp(-2)},
	})
	requireDirectKVError(t, err, rpctypes.ErrGRPCCompacted, codes.OutOfRange, "etcdserver: mvcc: required revision has been compacted")

	txnResp, err = server.Txn(ctx, &etcdserverpb.TxnRequest{
		Compare: []*etcdserverpb.Compare{{
			Key: key, Target: etcdserverpb.Compare_VERSION, Result: etcdserverpb.Compare_GREATER,
			TargetUnion: &etcdserverpb.Compare_Version{Version: 0},
		}},
		Success: []*etcdserverpb.RequestOp{rangeOp(0)},
		Failure: []*etcdserverpb.RequestOp{rangeOp(-1)},
	})
	require.NoError(t, err)
	require.True(t, txnResp.Succeeded)

	_, err = server.Compact(ctx, &etcdserverpb.CompactionRequest{Revision: put.Header.Revision})
	require.NoError(t, err)

	resp, err = server.Range(ctx, &etcdserverpb.RangeRequest{Key: key, Revision: -1})
	require.NoError(t, err)
	require.Len(t, resp.Kvs, 1)
	_, err = server.Txn(ctx, &etcdserverpb.TxnRequest{
		Success: []*etcdserverpb.RequestOp{rangeOp(-1)},
	})
	requireDirectKVError(t, err, rpctypes.ErrGRPCCompacted, codes.OutOfRange, "etcdserver: mvcc: required revision has been compacted")

	txnResp, err = server.Txn(ctx, &etcdserverpb.TxnRequest{
		Compare: []*etcdserverpb.Compare{{
			Key: key, Target: etcdserverpb.Compare_VERSION, Result: etcdserverpb.Compare_GREATER,
			TargetUnion: &etcdserverpb.Compare_Version{Version: 0},
		}},
		Success: []*etcdserverpb.RequestOp{rangeOp(0)},
		Failure: []*etcdserverpb.RequestOp{rangeOp(-1)},
	})
	require.NoError(t, err)
	require.True(t, txnResp.Succeeded)

	_, err = server.Txn(ctx, &etcdserverpb.TxnRequest{
		Success: []*etcdserverpb.RequestOp{rangeOp(math.MaxInt64)},
	})
	requireDirectKVError(t, err, rpctypes.ErrGRPCFutureRev, codes.OutOfRange, "etcdserver: mvcc: required revision is a future revision")
}

func TestCompactOlderRevisionReturnsCurrentHeader(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()

	ctx := context.Background()
	first, err := server.Put(ctx, &etcdserverpb.PutRequest{
		Key:   []byte("/registry/pods/compact-header"),
		Value: []byte("v1"),
	})
	require.NoError(t, err)
	second, err := server.Put(ctx, &etcdserverpb.PutRequest{
		Key:   []byte("/registry/pods/compact-header"),
		Value: []byte("v2"),
	})
	require.NoError(t, err)
	require.Eventually(t, func() bool {
		return server.backend.GetCurrentRevision() >= uint64(second.Header.Revision)
	}, time.Second, 10*time.Millisecond)

	resp, err := server.Compact(ctx, &etcdserverpb.CompactionRequest{Revision: first.Header.Revision})
	require.NoError(t, err)
	require.Equal(t, second.Header.Revision, resp.Header.Revision)
}

type compactLagShim struct {
	BackendShim
	currentRevision uint64
	actualCompact   uint64
}

func (s compactLagShim) GetCurrentRevision() uint64 {
	return s.currentRevision
}

func (s compactLagShim) GetCompactRevision(context.Context) (uint64, error) {
	return 0, nil
}

func (s compactLagShim) Compact(context.Context, uint64) (*etcdserverpb.TxnResponse, error) {
	return &etcdserverpb.TxnResponse{
		Header: &etcdserverpb.ResponseHeader{Revision: int64(s.actualCompact)},
	}, nil
}

func TestCompactReturnsRetryableErrorWhenBackendCompactsBelowRequestedRevision(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()
	server.backend = compactLagShim{
		BackendShim:     server.backend,
		currentRevision: 20,
		actualCompact:   9,
	}

	// Physical=true routes to the synchronous Compact that compactLagShim
	// overrides to report a below-requested compacted revision.
	_, err := server.Compact(context.Background(), &etcdserverpb.CompactionRequest{Revision: 10, Physical: true})
	requireDirectKVStatusError(t, err, codes.Unavailable, "etcdserver: mvcc: compact revision 9 is pending behind requested revision 10")
}

func TestCompactIsFencedAcrossLeadershipChange(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()

	ctx := context.Background()
	put, err := server.Put(ctx, &etcdserverpb.PutRequest{
		Key:   []byte("/registry/pods/compact-fence"),
		Value: []byte("v"),
	})
	require.NoError(t, err)
	require.Eventually(t, func() bool {
		return server.backend.GetCurrentRevision() >= uint64(put.Header.Revision)
	}, time.Second, 10*time.Millisecond)

	// testPeerService admits the request at epoch 0. Change the backend's
	// commit-time epoch to 1 so setCompactRecord must reject the stale leader
	// before opening its CAS batch.
	server.backend.(*backendShim).backend.SetLeadershipFence(func() (uint64, bool) { return 1, true })
	_, err = server.Compact(ctx, &etcdserverpb.CompactionRequest{Revision: put.Header.Revision})
	requireDirectKVStatusError(t, err, codes.Unavailable, "write rejected: leadership changed during commit, retry on current leader")

	hasMarker, markerErr := server.backend.HasCompactRevision(ctx)
	require.NoError(t, markerErr)
	require.False(t, hasMarker, "a deposed leader must not advance the shared compact watermark")
}

func TestFollowerCompactProxiesToLeader(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	metrics := mock.NewMinimalMetrics(ctrl)
	kv := memkv.NewKvStorage()
	defer func() {
		require.NoError(t, kv.Close())
	}()
	backendStore := backend.NewBackend(kv, backend.Config{
		Identity:                "test-peer",
		EnableEtcdCompatibility: true,
	}, metrics)

	called := false
	server := New(backendStore, metrics, testPeerService{
		isLeader:     false,
		proxyEnabled: true,
		compactFn: func(_ context.Context, req *etcdserverpb.CompactionRequest) (*etcdserverpb.CompactionResponse, error) {
			called = true
			require.Equal(t, int64(123), req.Revision)
			return &etcdserverpb.CompactionResponse{Header: &etcdserverpb.ResponseHeader{Revision: 456}}, nil
		},
	})

	resp, err := server.Compact(context.Background(), &etcdserverpb.CompactionRequest{Revision: 123})
	defer server.stopLeases()
	require.NoError(t, err)
	require.True(t, called)
	require.Equal(t, int64(456), resp.Header.Revision)
}

func TestRangeWithFromKeyMatchesEtcd(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()

	ctx := context.Background()
	for _, key := range []string{
		"/registry/from-key/a",
		"/registry/from-key/b",
		"/registry/from-key/c",
	} {
		_, err := server.Put(ctx, &etcdserverpb.PutRequest{Key: []byte(key), Value: []byte(key)})
		require.NoError(t, err)
	}

	require.Eventually(t, func() bool {
		resp, err := server.Range(ctx, &etcdserverpb.RangeRequest{
			Key:      []byte("/registry/from-key/b"),
			RangeEnd: []byte{0},
		})
		return err == nil &&
			len(resp.Kvs) >= 2 &&
			bytes.Equal(resp.Kvs[0].Key, []byte("/registry/from-key/b")) &&
			bytes.Equal(resp.Kvs[1].Key, []byte("/registry/from-key/c"))
	}, time.Second, 10*time.Millisecond)

	countResp, err := server.Range(ctx, &etcdserverpb.RangeRequest{
		Key:       []byte("/registry/from-key/b"),
		RangeEnd:  []byte{0},
		CountOnly: true,
	})
	require.NoError(t, err)
	require.GreaterOrEqual(t, countResp.Count, int64(2))
	require.Empty(t, countResp.Kvs)
}

func TestRangePreservesBinaryUserKeyOrdering(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()

	ctx := context.Background()
	var lastPutRevision int64
	for i, key := range [][]byte{
		{0x00},
		{0x00, 0x00},
		{0x00, 0x01},
		{0xfe},
		{0xfe, 0x00},
		{0xfe, 0x01},
		{0xff},
		{0xff, 0x00},
		{0xff, 0x01},
	} {
		putResp, err := server.Put(ctx, &etcdserverpb.PutRequest{Key: key, Value: []byte{byte(i)}})
		require.NoError(t, err)
		lastPutRevision = putResp.Header.Revision
	}

	nul, err := server.Range(ctx, &etcdserverpb.RangeRequest{
		Key: []byte{0x00}, RangeEnd: []byte{0x01},
	})
	require.NoError(t, err)
	require.NotNil(t, nul.Header)
	require.Equal(t, lastPutRevision, nul.Header.Revision)
	require.Len(t, nul.Kvs, 3)
	require.Equal(t, [][]byte{{0x00}, {0x00, 0x00}, {0x00, 0x01}}, [][]byte{
		nul.Kvs[0].Key, nul.Kvs[1].Key, nul.Kvs[2].Key,
	})

	fromFF, err := server.Range(ctx, &etcdserverpb.RangeRequest{
		Key: []byte{0xff}, RangeEnd: []byte{0}, Limit: 2,
	})
	require.NoError(t, err)
	require.NotNil(t, fromFF.Header)
	require.Equal(t, lastPutRevision, fromFF.Header.Revision)
	require.Equal(t, int64(3), fromFF.Count)
	require.True(t, fromFF.More)
	require.Len(t, fromFF.Kvs, 2)
	require.Equal(t, [][]byte{{0xff}, {0xff, 0x00}}, [][]byte{
		fromFF.Kvs[0].Key, fromFF.Kvs[1].Key,
	})

	highPrefix, err := server.Range(ctx, &etcdserverpb.RangeRequest{
		Key: []byte{0xfe}, RangeEnd: []byte{0xff},
	})
	require.NoError(t, err)
	require.NotNil(t, highPrefix.Header)
	require.Equal(t, lastPutRevision, highPrefix.Header.Revision)
	require.Len(t, highPrefix.Kvs, 3)
	require.Equal(t, [][]byte{{0xfe}, {0xfe, 0x00}, {0xfe, 0x01}}, [][]byte{
		highPrefix.Kvs[0].Key, highPrefix.Kvs[1].Key, highPrefix.Kvs[2].Key,
	})

	deleted, err := server.DeleteRange(ctx, &etcdserverpb.DeleteRangeRequest{
		Key: []byte{0xfe}, RangeEnd: []byte{0xff}, PrevKv: true,
	})
	require.NoError(t, err)
	require.NotNil(t, deleted.Header)
	require.Equal(t, lastPutRevision+1, deleted.Header.Revision)
	require.Equal(t, int64(3), deleted.Deleted)
	require.Len(t, deleted.PrevKvs, 3)
	require.Equal(t, [][]byte{{0xfe}, {0xfe, 0x00}, {0xfe, 0x01}}, [][]byte{
		deleted.PrevKvs[0].Key, deleted.PrevKvs[1].Key, deleted.PrevKvs[2].Key,
	})
}

func TestTxnBinaryMutationScenarioMatchesEtcd(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()

	ctx := context.Background()
	keys := [][]byte{
		{0x00},
		{0x00, 0x00},
		{0x00, 0x01},
		{0x01},
		{0xfe},
		{0xfe, 0x00},
		{0xfe, 0x01},
		{0xff},
	}
	var lastPutRevision int64
	for i, key := range keys {
		putResp, err := server.Put(ctx, &etcdserverpb.PutRequest{
			Key: key, Value: []byte{byte('a' + i)},
		})
		require.NoError(t, err)
		lastPutRevision = putResp.Header.Revision
	}

	txnRange, err := server.Txn(ctx, &etcdserverpb.TxnRequest{
		Success: []*etcdserverpb.RequestOp{{
			Request: &etcdserverpb.RequestOp_RequestRange{
				RequestRange: &etcdserverpb.RangeRequest{
					Key: []byte{0x00}, RangeEnd: []byte{0x01},
				},
			},
		}},
	})
	require.NoError(t, err)
	require.NotNil(t, txnRange.Header)
	require.Equal(t, lastPutRevision, txnRange.Header.Revision)
	require.Len(t, txnRange.Responses, 1)
	ranged := txnRange.Responses[0].GetResponseRange()
	require.NotNil(t, ranged)
	require.NotNil(t, ranged.Header)
	require.Equal(t, txnRange.Header.Revision, ranged.Header.Revision)
	require.Len(t, ranged.Kvs, 3)
	require.Equal(t, [][]byte{{0x00}, {0x00, 0x00}, {0x00, 0x01}}, [][]byte{
		ranged.Kvs[0].Key, ranged.Kvs[1].Key, ranged.Kvs[2].Key,
	})
	require.Equal(t, [][]byte{[]byte("a"), []byte("b"), []byte("c")}, [][]byte{
		ranged.Kvs[0].Value, ranged.Kvs[1].Value, ranged.Kvs[2].Value,
	})

	txnDelete, err := server.Txn(ctx, &etcdserverpb.TxnRequest{
		Success: []*etcdserverpb.RequestOp{{
			Request: &etcdserverpb.RequestOp_RequestDeleteRange{
				RequestDeleteRange: &etcdserverpb.DeleteRangeRequest{
					Key: []byte{0x00}, RangeEnd: []byte{0x01}, PrevKv: true,
				},
			},
		}},
	})
	require.NoError(t, err)
	require.NotNil(t, txnDelete.Header)
	require.Equal(t, lastPutRevision+1, txnDelete.Header.Revision)
	require.Len(t, txnDelete.Responses, 1)
	deleted := txnDelete.Responses[0].GetResponseDeleteRange()
	require.NotNil(t, deleted)
	require.NotNil(t, deleted.Header)
	require.Equal(t, txnDelete.Header.Revision, deleted.Header.Revision)
	require.Equal(t, int64(3), deleted.Deleted)
	require.Len(t, deleted.PrevKvs, 3)
	require.Equal(t, [][]byte{{0x00}, {0x00, 0x00}, {0x00, 0x01}}, [][]byte{
		deleted.PrevKvs[0].Key, deleted.PrevKvs[1].Key, deleted.PrevKvs[2].Key,
	})
	require.Equal(t, [][]byte{[]byte("a"), []byte("b"), []byte("c")}, [][]byte{
		deleted.PrevKvs[0].Value, deleted.PrevKvs[1].Value, deleted.PrevKvs[2].Value,
	})

	afterTxnDelete, err := server.Range(ctx, &etcdserverpb.RangeRequest{
		Key: []byte{0x00}, RangeEnd: []byte{0x01},
	})
	require.NoError(t, err)
	require.Empty(t, afterTxnDelete.Kvs)

	standaloneDelete, err := server.DeleteRange(ctx, &etcdserverpb.DeleteRangeRequest{
		Key: []byte{0xfe}, RangeEnd: []byte{0xff}, PrevKv: true,
	})
	require.NoError(t, err)
	require.NotNil(t, standaloneDelete.Header)
	require.Equal(t, txnDelete.Header.Revision+1, standaloneDelete.Header.Revision)
	require.Equal(t, int64(3), standaloneDelete.Deleted)
	require.Len(t, standaloneDelete.PrevKvs, 3)
	require.Equal(t, [][]byte{{0xfe}, {0xfe, 0x00}, {0xfe, 0x01}}, [][]byte{
		standaloneDelete.PrevKvs[0].Key, standaloneDelete.PrevKvs[1].Key, standaloneDelete.PrevKvs[2].Key,
	})
	require.Equal(t, [][]byte{[]byte("e"), []byte("f"), []byte("g")}, [][]byte{
		standaloneDelete.PrevKvs[0].Value, standaloneDelete.PrevKvs[1].Value, standaloneDelete.PrevKvs[2].Value,
	})

	afterStandaloneDelete, err := server.Range(ctx, &etcdserverpb.RangeRequest{
		Key: []byte{0xfe}, RangeEnd: []byte{0xff},
	})
	require.NoError(t, err)
	require.Empty(t, afterStandaloneDelete.Kvs)
}

func TestDeleteRangeWithFromKeyMatchesEtcd(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()

	ctx := context.Background()
	var lastPutRevision int64
	for _, key := range []string{
		"/registry/from-key-delete/a",
		"/registry/from-key-delete/b",
		"/registry/from-key-delete/c",
	} {
		putResp, err := server.Put(ctx, &etcdserverpb.PutRequest{Key: []byte(key), Value: []byte(key)})
		require.NoError(t, err)
		lastPutRevision = putResp.Header.Revision
	}

	require.Eventually(t, func() bool {
		resp, err := server.Range(ctx, &etcdserverpb.RangeRequest{
			Key:      []byte("/registry/from-key-delete/b"),
			RangeEnd: []byte{0},
		})
		return err == nil &&
			len(resp.Kvs) >= 2 &&
			bytes.Equal(resp.Kvs[0].Key, []byte("/registry/from-key-delete/b")) &&
			bytes.Equal(resp.Kvs[1].Key, []byte("/registry/from-key-delete/c"))
	}, time.Second, 10*time.Millisecond)

	resp, err := server.DeleteRange(ctx, &etcdserverpb.DeleteRangeRequest{
		Key:      []byte("/registry/from-key-delete/b"),
		RangeEnd: []byte{0},
		PrevKv:   true,
	})
	require.NoError(t, err)
	require.NotNil(t, resp.Header)
	require.Equal(t, lastPutRevision+1, resp.Header.Revision)
	require.GreaterOrEqual(t, resp.Deleted, int64(2))
	require.GreaterOrEqual(t, len(resp.PrevKvs), 2)

	require.Eventually(t, func() bool {
		remaining, err := server.Range(ctx, &etcdserverpb.RangeRequest{
			Key:      []byte("/registry/from-key-delete/"),
			RangeEnd: []byte("/registry/from-key-delete0"),
		})
		return err == nil &&
			len(remaining.Kvs) == 1 &&
			bytes.Equal(remaining.Kvs[0].Key, []byte("/registry/from-key-delete/a"))
	}, time.Second, 10*time.Millisecond)
}

func TestDeleteRangeBoundaryHighPrefixMatchesEtcd(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()

	ctx := context.Background()
	for _, tc := range []struct {
		name           string
		start          string
		rangeEnd       func(prefix string) []byte
		wantDeleted    int64
		wantPrev       []string
		wantRemaining  []string
		wantAdvanceRev bool
	}{
		{
			name: "from-key", start: "b", rangeEnd: func(string) []byte { return []byte{0} },
			wantDeleted: 2, wantPrev: []string{"b", "c"}, wantRemaining: []string{"a"},
			wantAdvanceRev: true,
		},
		{
			name: "equal-empty", start: "b", rangeEnd: func(prefix string) []byte { return []byte(prefix + "b") },
			wantRemaining: []string{"a", "b", "c"},
		},
		{
			name: "reverse-empty", start: "c", rangeEnd: func(prefix string) []byte { return []byte(prefix + "b") },
			wantRemaining: []string{"a", "b", "c"},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			prefix := string(bytes.Repeat([]byte{0xff}, 64)) + "/registry/delete-boundary/" + tc.name + "/"
			end := prefixEnd([]byte(prefix))
			var putRevisions []int64
			for _, suffix := range []string{"a", "b", "c"} {
				put, err := server.Put(ctx, &etcdserverpb.PutRequest{
					Key: []byte(prefix + suffix), Value: []byte("value-" + suffix),
				})
				require.NoError(t, err)
				putRevisions = append(putRevisions, put.Header.Revision)
			}
			require.Equal(t, []int64{1, 1}, []int64{
				putRevisions[1] - putRevisions[0],
				putRevisions[2] - putRevisions[1],
			})
			lastPutRevision := putRevisions[2]

			deleted, err := server.DeleteRange(ctx, &etcdserverpb.DeleteRangeRequest{
				Key: []byte(prefix + tc.start), RangeEnd: tc.rangeEnd(prefix), PrevKv: true,
			})
			require.NoError(t, err)
			require.NotNil(t, deleted.Header)
			wantRevision := lastPutRevision
			if tc.wantAdvanceRev {
				wantRevision++
			}
			require.Equal(t, wantRevision, deleted.Header.Revision)
			require.Equal(t, tc.wantDeleted, deleted.Deleted)
			require.Equal(t, tc.wantAdvanceRev, deleted.Header.Revision > lastPutRevision)
			require.Len(t, deleted.PrevKvs, len(tc.wantPrev))
			for i, suffix := range tc.wantPrev {
				require.Equal(t, []byte(prefix+suffix), deleted.PrevKvs[i].Key)
				require.Equal(t, []byte("value-"+suffix), deleted.PrevKvs[i].Value)
				require.Less(t, deleted.PrevKvs[i].ModRevision, deleted.Header.Revision)
			}

			remaining, err := server.Range(ctx, &etcdserverpb.RangeRequest{Key: []byte(prefix), RangeEnd: end})
			require.NoError(t, err)
			require.Equal(t, deleted.Header.Revision, remaining.Header.Revision)
			require.Len(t, remaining.Kvs, len(tc.wantRemaining))
			for i, suffix := range tc.wantRemaining {
				require.Equal(t, []byte(prefix+suffix), remaining.Kvs[i].Key)
				require.Equal(t, []byte("value-"+suffix), remaining.Kvs[i].Value)
			}
		})
	}
}

func TestDeleteRangeDeletesSingleKey(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()

	ctx := context.Background()
	putResp, err := server.Put(ctx, &etcdserverpb.PutRequest{
		Key:   []byte("/registry/services/a"),
		Value: []byte("svc"),
	})
	require.NoError(t, err)

	deleteResp, err := server.DeleteRange(ctx, &etcdserverpb.DeleteRangeRequest{
		Key:    []byte("/registry/services/a"),
		PrevKv: true,
	})
	require.NoError(t, err)
	require.NotNil(t, deleteResp.Header)
	require.Equal(t, putResp.Header.Revision+1, deleteResp.Header.Revision)
	require.Equal(t, int64(1), deleteResp.Deleted)
	require.Len(t, deleteResp.PrevKvs, 1)
	require.Equal(t, []byte("svc"), deleteResp.PrevKvs[0].Value)

	rangeResp, err := server.Range(ctx, &etcdserverpb.RangeRequest{Key: []byte("/registry/services/a")})
	require.NoError(t, err)
	require.NotNil(t, rangeResp.Header)
	require.Equal(t, deleteResp.Header.Revision, rangeResp.Header.Revision)
	require.Empty(t, rangeResp.Kvs)
}

func TestDeleteRangeMissingPointDoesNotConsumeRevision(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()

	ctx := context.Background()
	before := int64(server.backend.GetCurrentRevision())
	for i := 0; i < 3; i++ {
		resp, err := server.DeleteRange(ctx, &etcdserverpb.DeleteRangeRequest{
			Key:    []byte("/registry/delete-missing-point"),
			PrevKv: true,
		})
		require.NoError(t, err)
		require.Equal(t, int64(0), resp.Deleted)
		require.Empty(t, resp.PrevKvs)
		require.Equal(t, before, resp.Header.Revision)
		require.Equal(t, uint64(before), server.backend.GetCurrentRevision())
	}
}

func TestDeleteRangeDifferentialScenarioMatchesEtcd(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()

	ctx := context.Background()
	prefix := "/registry/delete-differential/"
	rangeEnd := []byte("/registry/delete-differential0")
	empty, err := server.Range(ctx, &etcdserverpb.RangeRequest{Key: []byte(prefix), RangeEnd: rangeEnd})
	require.NoError(t, err)
	baseRev := empty.Header.Revision

	putA, err := server.Put(ctx, &etcdserverpb.PutRequest{Key: []byte(prefix + "a"), Value: []byte("va")})
	require.NoError(t, err)
	putB, err := server.Put(ctx, &etcdserverpb.PutRequest{Key: []byte(prefix + "b"), Value: []byte("vb")})
	require.NoError(t, err)
	putC, err := server.Put(ctx, &etcdserverpb.PutRequest{Key: []byte(prefix + "c"), Value: []byte("vc")})
	require.NoError(t, err)
	updateB, err := server.Put(ctx, &etcdserverpb.PutRequest{Key: []byte(prefix + "b"), Value: []byte("vb2")})
	require.NoError(t, err)
	require.Equal(t, []int64{1, 2, 3, 4}, []int64{
		putA.Header.Revision - baseRev,
		putB.Header.Revision - baseRev,
		putC.Header.Revision - baseRev,
		updateB.Header.Revision - baseRev,
	})

	deleted, err := server.DeleteRange(ctx, &etcdserverpb.DeleteRangeRequest{
		Key: []byte(prefix + "a"), RangeEnd: []byte(prefix + "c"), PrevKv: true,
	})
	require.NoError(t, err)
	require.Equal(t, int64(5), deleted.Header.Revision-baseRev)
	require.Equal(t, int64(2), deleted.Deleted)
	require.Len(t, deleted.PrevKvs, 2)
	require.Equal(t, [][]byte{[]byte(prefix + "a"), []byte(prefix + "b")}, [][]byte{
		deleted.PrevKvs[0].Key, deleted.PrevKvs[1].Key,
	})
	require.Equal(t, [][]byte{[]byte("va"), []byte("vb2")}, [][]byte{
		deleted.PrevKvs[0].Value, deleted.PrevKvs[1].Value,
	})

	historical, err := server.Range(ctx, &etcdserverpb.RangeRequest{
		Key: []byte(prefix), RangeEnd: rangeEnd, Revision: deleted.Header.Revision - 1,
		SortOrder: etcdserverpb.RangeRequest_ASCEND, SortTarget: etcdserverpb.RangeRequest_KEY,
	})
	require.NoError(t, err)
	require.Len(t, historical.Kvs, 3)
	require.Equal(t, [][]byte{[]byte(prefix + "a"), []byte(prefix + "b"), []byte(prefix + "c")}, [][]byte{
		historical.Kvs[0].Key, historical.Kvs[1].Key, historical.Kvs[2].Key,
	})
	require.Equal(t, [][]byte{[]byte("va"), []byte("vb2"), []byte("vc")}, [][]byte{
		historical.Kvs[0].Value, historical.Kvs[1].Value, historical.Kvs[2].Value,
	})

	current, err := server.Range(ctx, &etcdserverpb.RangeRequest{
		Key: []byte(prefix), RangeEnd: rangeEnd,
		SortOrder: etcdserverpb.RangeRequest_ASCEND, SortTarget: etcdserverpb.RangeRequest_KEY,
	})
	require.NoError(t, err)
	require.Len(t, current.Kvs, 1)
	require.Equal(t, []byte(prefix+"c"), current.Kvs[0].Key)

	emptyRange, err := server.DeleteRange(ctx, &etcdserverpb.DeleteRangeRequest{
		Key: []byte(prefix + "c"), RangeEnd: []byte(prefix + "c"), PrevKv: true,
	})
	require.NoError(t, err)
	require.Equal(t, deleted.Header.Revision, emptyRange.Header.Revision)
	require.Equal(t, int64(0), emptyRange.Deleted)
	require.Empty(t, emptyRange.PrevKvs)

	missing, err := server.DeleteRange(ctx, &etcdserverpb.DeleteRangeRequest{
		Key: []byte(prefix + "missing"), PrevKv: true,
	})
	require.NoError(t, err)
	require.Equal(t, deleted.Header.Revision, missing.Header.Revision)
	require.Equal(t, int64(0), missing.Deleted)
	require.Empty(t, missing.PrevKvs)
}

func TestDeleteRangeRejectsEmptyKey(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()

	_, err := server.DeleteRange(context.Background(), &etcdserverpb.DeleteRangeRequest{})
	requireDirectKVError(t, err, rpctypes.ErrGRPCEmptyKey, codes.InvalidArgument, "etcdserver: key is not provided")
}

func TestNamespacedEmptyKeyFromKeyDeleteMatchesEtcd(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()

	ctx := context.Background()
	prefix := []byte("/registry/namespace-empty-key/")
	var lastPutRevision int64
	for _, suffix := range []byte{'a', 'b'} {
		putResp, err := server.Put(ctx, &etcdserverpb.PutRequest{
			Key:   append(append([]byte{}, prefix...), suffix),
			Value: []byte{'v', suffix},
		})
		require.NoError(t, err)
		lastPutRevision = putResp.Header.Revision
	}

	_, err := server.DeleteRange(ctx, &etcdserverpb.DeleteRangeRequest{})
	requireDirectKVError(t, err, rpctypes.ErrGRPCEmptyKey, codes.InvalidArgument, "etcdserver: key is not provided")

	visible, err := server.Range(ctx, &etcdserverpb.RangeRequest{
		Key: prefix, RangeEnd: []byte{0},
	})
	require.NoError(t, err)
	require.Len(t, visible.Kvs, 2)
	require.Equal(t, [][]byte{
		append(append([]byte{}, prefix...), 'a'),
		append(append([]byte{}, prefix...), 'b'),
	}, [][]byte{visible.Kvs[0].Key, visible.Kvs[1].Key})

	deleted, err := server.DeleteRange(ctx, &etcdserverpb.DeleteRangeRequest{
		Key: prefix, RangeEnd: []byte{0}, PrevKv: true,
	})
	require.NoError(t, err)
	require.NotNil(t, deleted.Header)
	require.Equal(t, lastPutRevision+1, deleted.Header.Revision)
	require.Equal(t, int64(2), deleted.Deleted)
	require.Len(t, deleted.PrevKvs, 2)

	remaining, err := server.Range(ctx, &etcdserverpb.RangeRequest{
		Key: prefix, RangeEnd: []byte{0},
	})
	require.NoError(t, err)
	require.NotNil(t, remaining.Header)
	require.Equal(t, deleted.Header.Revision, remaining.Header.Revision)
	require.Empty(t, remaining.Kvs)
}

func TestNamespacedFromKeyPrefixEndPreservesAdjacentKeys(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()

	ctx := context.Background()
	prefix := []byte("/registry/namespace-prefix/tenant/")
	prefixEnd := []byte("/registry/namespace-prefix/tenant0")
	adjacentKey := []byte("/registry/namespace-prefix/tenant0/outside")
	var lastPutRevision int64
	for _, suffix := range []byte{'a', 'b', 'c'} {
		putResp, err := server.Put(ctx, &etcdserverpb.PutRequest{
			Key:   append(append([]byte{}, prefix...), suffix),
			Value: []byte{'v', suffix},
		})
		require.NoError(t, err)
		lastPutRevision = putResp.Header.Revision
	}
	adjacentPut, err := server.Put(ctx, &etcdserverpb.PutRequest{Key: adjacentKey, Value: []byte("outside")})
	require.NoError(t, err)
	lastPutRevision = adjacentPut.Header.Revision

	visible, err := server.Range(ctx, &etcdserverpb.RangeRequest{
		Key:      prefix,
		RangeEnd: prefixEnd,
	})
	require.NoError(t, err)
	require.Equal(t, int64(3), visible.Count)
	require.Len(t, visible.Kvs, 3)
	require.Equal(t, [][]byte{
		append(append([]byte{}, prefix...), 'a'),
		append(append([]byte{}, prefix...), 'b'),
		append(append([]byte{}, prefix...), 'c'),
	}, [][]byte{visible.Kvs[0].Key, visible.Kvs[1].Key, visible.Kvs[2].Key})

	deleted, err := server.DeleteRange(ctx, &etcdserverpb.DeleteRangeRequest{
		Key:      prefix,
		RangeEnd: prefixEnd,
		PrevKv:   true,
	})
	require.NoError(t, err)
	require.NotNil(t, deleted.Header)
	require.Equal(t, lastPutRevision+1, deleted.Header.Revision)
	require.Equal(t, int64(3), deleted.Deleted)
	require.Len(t, deleted.PrevKvs, 3)
	require.Equal(t, [][]byte{
		append(append([]byte{}, prefix...), 'a'),
		append(append([]byte{}, prefix...), 'b'),
		append(append([]byte{}, prefix...), 'c'),
	}, [][]byte{deleted.PrevKvs[0].Key, deleted.PrevKvs[1].Key, deleted.PrevKvs[2].Key})

	remainingNamespace, err := server.Range(ctx, &etcdserverpb.RangeRequest{
		Key:      prefix,
		RangeEnd: prefixEnd,
	})
	require.NoError(t, err)
	require.NotNil(t, remainingNamespace.Header)
	require.Equal(t, deleted.Header.Revision, remainingNamespace.Header.Revision)
	require.Empty(t, remainingNamespace.Kvs)
	adjacent, err := server.Range(ctx, &etcdserverpb.RangeRequest{Key: adjacentKey})
	require.NoError(t, err)
	require.NotNil(t, adjacent.Header)
	require.Equal(t, deleted.Header.Revision, adjacent.Header.Revision)
	require.Len(t, adjacent.Kvs, 1)
	require.Equal(t, []byte("outside"), adjacent.Kvs[0].Value)
}

func TestClientNamespaceTxnWatchStripsPrefixes(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()

	grpcServer := grpc.NewServer(server.ClientServerOptions()...)
	etcdserverpb.RegisterKVServer(grpcServer, server)
	etcdserverpb.RegisterWatchServer(grpcServer, server)
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
	tenantPrefix := "/registry/client-namespace/tenant/"
	outsideKey := "/registry/client-namespace/tenant0/outside"
	namespacedKV := namespace.NewKV(client.KV, tenantPrefix)
	namespacedWatcher := namespace.NewWatcher(client.Watcher, tenantPrefix)
	for _, key := range []string{"a", "b", "c"} {
		_, err = namespacedKV.Put(ctx, key, "seed-"+key)
		require.NoError(t, err)
	}
	_, err = client.Put(ctx, outsideKey, "outside")
	require.NoError(t, err)
	namespacedKeys := func(kvs []*mvccpb.KeyValue) []string {
		keys := make([]string, 0, len(kvs))
		for _, kv := range kvs {
			keys = append(keys, string(kv.Key))
		}
		return keys
	}

	initial, err := namespacedKV.Get(ctx, "", clientv3.WithFromKey(), clientv3.WithSort(clientv3.SortByKey, clientv3.SortAscend))
	require.NoError(t, err)
	require.Equal(t, []string{"a", "b", "c"}, namespacedKeys(initial.Kvs))

	watchCtx, watchCancel := context.WithCancel(ctx)
	defer watchCancel()
	watch := namespacedWatcher.Watch(watchCtx, "",
		clientv3.WithPrefix(), clientv3.WithRev(initial.Header.Revision+1), clientv3.WithPrevKV())
	txn, err := namespacedKV.Txn(ctx).
		If(clientv3.Compare(clientv3.Value("a"), "=", "seed-a")).
		Then(
			clientv3.OpTxn(nil, []clientv3.Op{
				clientv3.OpPut("a", "updated-a", clientv3.WithPrevKV()),
				clientv3.OpPut("d", "created-d"),
			}, nil),
			clientv3.OpDelete("b", clientv3.WithPrevKV()),
		).
		Commit()
	require.NoError(t, err)
	require.True(t, txn.Succeeded)
	require.Len(t, txn.Responses, 2)

	nested := txn.Responses[0].GetResponseTxn()
	require.NotNil(t, nested)
	require.Len(t, nested.Responses, 2)
	nestedPut := nested.Responses[0].GetResponsePut()
	require.NotNil(t, nestedPut)
	require.NotNil(t, nestedPut.PrevKv)
	require.Equal(t, []byte("a"), nestedPut.PrevKv.Key)
	deleted := txn.Responses[1].GetResponseDeleteRange()
	require.NotNil(t, deleted)
	require.Len(t, deleted.PrevKvs, 1)
	require.Equal(t, []byte("b"), deleted.PrevKvs[0].Key)

	events := make(map[string]string)
	prevKeys := make(map[string]string)
	for len(events) < 3 {
		select {
		case response, ok := <-watch:
			require.True(t, ok)
			require.NoError(t, response.Err())
			for _, event := range response.Events {
				key := string(event.Kv.Key)
				events[key] = string(event.Kv.Value)
				if event.PrevKv != nil {
					prevKeys[key] = string(event.PrevKv.Key)
				}
				require.False(t, bytes.HasPrefix(event.Kv.Key, []byte(tenantPrefix)))
				if event.PrevKv != nil {
					require.False(t, bytes.HasPrefix(event.PrevKv.Key, []byte(tenantPrefix)))
				}
				require.Equal(t, txn.Header.Revision, event.Kv.ModRevision)
			}
		case <-ctx.Done():
			t.Fatalf("timed out waiting for namespace watch events: %v", ctx.Err())
		}
	}
	require.Equal(t, map[string]string{"a": "updated-a", "b": "", "d": "created-d"}, events)
	require.Equal(t, map[string]string{"a": "a", "b": "b"}, prevKeys)

	postTxn, err := namespacedKV.Get(ctx, "", clientv3.WithFromKey(), clientv3.WithSort(clientv3.SortByKey, clientv3.SortAscend))
	require.NoError(t, err)
	require.Equal(t, []string{"a", "c", "d"}, namespacedKeys(postTxn.Kvs))
	deletedAll, err := namespacedKV.Delete(ctx, "", clientv3.WithFromKey(), clientv3.WithPrevKV())
	require.NoError(t, err)
	require.Equal(t, int64(3), deletedAll.Deleted)
	require.Equal(t, []string{"a", "c", "d"}, namespacedKeys(deletedAll.PrevKvs))

	empty, err := namespacedKV.Get(ctx, "", clientv3.WithFromKey())
	require.NoError(t, err)
	require.Empty(t, empty.Kvs)
	outside, err := client.Get(ctx, outsideKey)
	require.NoError(t, err)
	require.Len(t, outside.Kvs, 1)
	require.Equal(t, "outside", string(outside.Kvs[0].Value))
}

func TestClientNamespacePutResponsesStripPrevKVPrefix(t *testing.T) {
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
	tenantPrefix := "/registry/client-namespace-do-put/tenant/"
	namespacedKV := namespace.NewKV(client.KV, tenantPrefix)
	_, err = namespacedKV.Put(ctx, "key", "old")
	require.NoError(t, err)

	putOpResponse, err := namespacedKV.Do(ctx, clientv3.OpPut("key", "new", clientv3.WithPrevKV()))
	require.NoError(t, err)
	putOp := putOpResponse.Put()
	require.NotNil(t, putOp)
	require.NotNil(t, putOp.PrevKv)
	require.Equal(t, []byte("key"), putOp.PrevKv.Key)
	require.Equal(t, []byte("old"), putOp.PrevKv.Value)

	raw, err := client.Get(ctx, tenantPrefix+"key")
	require.NoError(t, err)
	require.Len(t, raw.Kvs, 1)
	require.Equal(t, []byte("new"), raw.Kvs[0].Value)

	putResponse, err := namespacedKV.Put(ctx, "key", "newer", clientv3.WithPrevKV())
	require.NoError(t, err)
	require.NotNil(t, putResponse.PrevKv)
	require.Equal(t, []byte("key"), putResponse.PrevKv.Key)
	require.Equal(t, []byte("new"), putResponse.PrevKv.Value)

	raw, err = client.Get(ctx, tenantPrefix+"key")
	require.NoError(t, err)
	require.Len(t, raw.Kvs, 1)
	require.Equal(t, []byte("newer"), raw.Kvs[0].Value)
}

func TestClientNamespaceEmptyKeyFromKeyDeleteMatchesEtcd(t *testing.T) {
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
	tenantPrefix := string(bytes.Repeat([]byte{0xff}, 16)) + "/registry/client-namespace-empty-key/"
	tenantEnd := clientv3.GetPrefixRangeEnd(tenantPrefix)
	namespacedKV := namespace.NewKV(client.KV, tenantPrefix)
	for _, key := range []string{"a", "b"} {
		_, err = namespacedKV.Put(ctx, key, "value-"+key)
		require.NoError(t, err)
	}

	_, err = client.Delete(ctx, "")
	requireClientKVError(t, err, rpctypes.ErrEmptyKey, codes.Unknown, "etcdserver: key is not provided")

	visible, err := namespacedKV.Get(ctx, "", clientv3.WithFromKey())
	require.NoError(t, err)
	require.Len(t, visible.Kvs, 2)
	require.Equal(t, [][]byte{[]byte("a"), []byte("b")}, [][]byte{visible.Kvs[0].Key, visible.Kvs[1].Key})

	deleted, err := namespacedKV.Delete(ctx, "", clientv3.WithFromKey())
	require.NoError(t, err)
	require.Equal(t, int64(2), deleted.Deleted)

	remaining, err := client.Get(ctx, tenantPrefix, clientv3.WithRange(tenantEnd))
	require.NoError(t, err)
	require.Empty(t, remaining.Kvs)

	for _, key := range []string{"c", "d"} {
		_, err = namespacedKV.Put(ctx, key, "value-"+key)
		require.NoError(t, err)
	}
	visiblePrefix, err := namespacedKV.Get(ctx, "", clientv3.WithPrefix())
	require.NoError(t, err)
	require.Len(t, visiblePrefix.Kvs, 2)
	require.Equal(t, [][]byte{[]byte("c"), []byte("d")}, [][]byte{visiblePrefix.Kvs[0].Key, visiblePrefix.Kvs[1].Key})
	getPrefixOpResponse, err := namespacedKV.Do(ctx, clientv3.OpGet("", clientv3.WithPrefix()))
	require.NoError(t, err)
	getPrefixOp := getPrefixOpResponse.Get()
	require.NotNil(t, getPrefixOp)
	require.Len(t, getPrefixOp.Kvs, 2)
	require.Equal(t, [][]byte{[]byte("c"), []byte("d")}, [][]byte{getPrefixOp.Kvs[0].Key, getPrefixOp.Kvs[1].Key})
	getFromKeyOpResponse, err := namespacedKV.Do(ctx, clientv3.OpGet("", clientv3.WithFromKey()))
	require.NoError(t, err)
	getFromKeyOp := getFromKeyOpResponse.Get()
	require.NotNil(t, getFromKeyOp)
	require.Len(t, getFromKeyOp.Kvs, 2)
	require.Equal(t, [][]byte{[]byte("c"), []byte("d")}, [][]byte{getFromKeyOp.Kvs[0].Key, getFromKeyOp.Kvs[1].Key})
	deletedPrefix, err := namespacedKV.Delete(ctx, "", clientv3.WithPrefix())
	require.NoError(t, err)
	require.Equal(t, int64(2), deletedPrefix.Deleted)

	remaining, err = client.Get(ctx, tenantPrefix, clientv3.WithRange(tenantEnd))
	require.NoError(t, err)
	require.Empty(t, remaining.Kvs)

	_, err = namespacedKV.Do(ctx, clientv3.OpDelete(""))
	requireClientKVError(t, err, rpctypes.ErrEmptyKey, codes.Unknown, "etcdserver: key is not provided")

	for _, key := range []string{"e", "f"} {
		_, err = namespacedKV.Put(ctx, key, "value-"+key)
		require.NoError(t, err)
	}
	deleteOpResponse, err := namespacedKV.Do(ctx, clientv3.OpDelete("", clientv3.WithPrefix(), clientv3.WithPrevKV()))
	require.NoError(t, err)
	deleteOp := deleteOpResponse.Del()
	require.NotNil(t, deleteOp)
	require.Equal(t, int64(2), deleteOp.Deleted)
	require.Equal(t, [][]byte{[]byte("e"), []byte("f")}, [][]byte{deleteOp.PrevKvs[0].Key, deleteOp.PrevKvs[1].Key})

	remaining, err = client.Get(ctx, tenantPrefix, clientv3.WithRange(tenantEnd))
	require.NoError(t, err)
	require.Empty(t, remaining.Kvs)

	for _, key := range []string{"g", "h"} {
		_, err = namespacedKV.Put(ctx, key, "value-"+key)
		require.NoError(t, err)
	}
	deleteFromKeyOpResponse, err := namespacedKV.Do(ctx, clientv3.OpDelete("", clientv3.WithFromKey(), clientv3.WithPrevKV()))
	require.NoError(t, err)
	deleteFromKeyOp := deleteFromKeyOpResponse.Del()
	require.NotNil(t, deleteFromKeyOp)
	require.Equal(t, int64(2), deleteFromKeyOp.Deleted)
	require.Len(t, deleteFromKeyOp.PrevKvs, 2)
	require.Equal(t, [][]byte{[]byte("g"), []byte("h")}, [][]byte{deleteFromKeyOp.PrevKvs[0].Key, deleteFromKeyOp.PrevKvs[1].Key})

	remaining, err = client.Get(ctx, tenantPrefix, clientv3.WithRange(tenantEnd))
	require.NoError(t, err)
	require.Empty(t, remaining.Kvs)
}

func TestDeleteRangeDeletesRange(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()

	ctx := context.Background()
	for _, key := range []string{"/registry/configmaps/a", "/registry/configmaps/b", "/registry/secrets/a"} {
		_, err := server.Put(ctx, &etcdserverpb.PutRequest{
			Key:   []byte(key),
			Value: []byte(key),
		})
		require.NoError(t, err)
	}
	require.Eventually(t, func() bool {
		rangeResp, err := server.Range(ctx, &etcdserverpb.RangeRequest{
			Key:      []byte("/registry/configmaps/"),
			RangeEnd: []byte("/registry/configmaps0"),
		})
		return err == nil && len(rangeResp.Kvs) == 2
	}, time.Second, 10*time.Millisecond)

	deleteResp, err := server.DeleteRange(ctx, &etcdserverpb.DeleteRangeRequest{
		Key:      []byte("/registry/configmaps/"),
		RangeEnd: []byte("/registry/configmaps0"),
		PrevKv:   true,
	})
	require.NoError(t, err)
	require.Equal(t, int64(2), deleteResp.Deleted)
	require.Len(t, deleteResp.PrevKvs, 2)
	for _, prevKV := range deleteResp.PrevKvs {
		require.Greater(t, deleteResp.Header.Revision, prevKV.ModRevision)
	}
	require.Eventually(t, func() bool {
		return server.backend.GetCurrentRevision() >= uint64(deleteResp.Header.Revision)
	}, time.Second, 10*time.Millisecond)
	watchCtx, watchCancel := context.WithCancel(ctx)
	defer watchCancel()
	watchCh, err := server.backend.Watch(watchCtx, "/registry/configmaps/", uint64(deleteResp.Header.Revision))
	require.NoError(t, err)
	deleteEvents := (<-watchCh).Events
	require.Len(t, deleteEvents, 2)
	for _, event := range deleteEvents {
		require.Equal(t, mvccpb.DELETE, event.Type)
		require.Equal(t, deleteResp.Header.Revision, event.Kv.ModRevision)
	}

	require.Eventually(t, func() bool {
		rangeResp, err := server.Range(ctx, &etcdserverpb.RangeRequest{
			Key:      []byte("/registry/configmaps/"),
			RangeEnd: []byte("/registry/configmaps0"),
		})
		return err == nil && len(rangeResp.Kvs) == 0
	}, time.Second, 10*time.Millisecond)

	secretResp, err := server.Range(ctx, &etcdserverpb.RangeRequest{Key: []byte("/registry/secrets/a")})
	require.NoError(t, err)
	require.Len(t, secretResp.Kvs, 1)
}

func TestDeleteRangeEmptyNonFromKeyRangeDoesNotDelete(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()

	ctx := context.Background()
	keys := [][]byte{
		[]byte("/registry/configmaps/empty-delete/a"),
		[]byte("/registry/configmaps/empty-delete/b"),
	}
	for _, key := range keys {
		_, err := server.Put(ctx, &etcdserverpb.PutRequest{
			Key: key, Value: []byte("value-" + string(key[len(key)-1])),
		})
		require.NoError(t, err)
	}
	before := int64(server.backend.GetCurrentRevision())

	tests := []struct {
		name     string
		key      []byte
		rangeEnd []byte
	}{
		{name: "equal", key: keys[0], rangeEnd: keys[0]},
		{name: "reverse", key: keys[1], rangeEnd: keys[0]},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			deleteResp, err := server.DeleteRange(ctx, &etcdserverpb.DeleteRangeRequest{
				Key: tt.key, RangeEnd: tt.rangeEnd, PrevKv: true,
			})
			require.NoError(t, err)
			require.Equal(t, int64(0), deleteResp.Deleted)
			require.Empty(t, deleteResp.PrevKvs)
			require.NotNil(t, deleteResp.Header)
			require.Equal(t, before, deleteResp.Header.Revision)
			require.Equal(t, uint64(before), server.backend.GetCurrentRevision())
		})
	}

	for _, key := range keys {
		getResp, err := server.Range(ctx, &etcdserverpb.RangeRequest{Key: key})
		require.NoError(t, err)
		require.Equal(t, int64(1), getResp.Count)
	}
}

func TestTxnDeleteRangeEmptyNonFromKeyRangeDoesNotDelete(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()

	ctx := context.Background()
	key := []byte("/registry/generic-txn/empty-delete/a")
	_, err := server.Put(ctx, &etcdserverpb.PutRequest{
		Key:   key,
		Value: []byte("value"),
	})
	require.NoError(t, err)

	txnResp, err := server.Txn(ctx, &etcdserverpb.TxnRequest{
		Success: []*etcdserverpb.RequestOp{
			{Request: &etcdserverpb.RequestOp_RequestDeleteRange{
				RequestDeleteRange: &etcdserverpb.DeleteRangeRequest{
					Key:      key,
					RangeEnd: key,
					PrevKv:   true,
				},
			}},
		},
	})
	require.NoError(t, err)
	require.True(t, txnResp.Succeeded)
	require.Len(t, txnResp.Responses, 1)
	deleteResp := txnResp.Responses[0].GetResponseDeleteRange()
	require.NotNil(t, deleteResp)
	require.NotNil(t, deleteResp.Header)
	require.Equal(t, txnResp.Header.Revision, deleteResp.Header.Revision)
	require.Equal(t, int64(0), deleteResp.Deleted)
	require.Empty(t, deleteResp.PrevKvs)

	getResp, err := server.Range(ctx, &etcdserverpb.RangeRequest{Key: key})
	require.NoError(t, err)
	require.NotNil(t, getResp.Header)
	require.Equal(t, txnResp.Header.Revision, getResp.Header.Revision)
	require.Equal(t, int64(1), getResp.Count)
	require.Equal(t, []byte("value"), getResp.Kvs[0].Value)
}

func TestTxnCompareDeleteRangeEmptyNonFromKeyRangeUsesGenericPath(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()

	ctx := context.Background()
	key := []byte("/registry/generic-txn/compare-empty-delete/a")
	putResp, err := server.Put(ctx, &etcdserverpb.PutRequest{
		Key:   key,
		Value: []byte("value"),
	})
	require.NoError(t, err)

	txnResp, err := server.Txn(ctx, &etcdserverpb.TxnRequest{
		Compare: []*etcdserverpb.Compare{
			{
				Key:    key,
				Target: etcdserverpb.Compare_MOD,
				Result: etcdserverpb.Compare_EQUAL,
				TargetUnion: &etcdserverpb.Compare_ModRevision{
					ModRevision: putResp.Header.Revision,
				},
			},
		},
		Success: []*etcdserverpb.RequestOp{
			{Request: &etcdserverpb.RequestOp_RequestDeleteRange{
				RequestDeleteRange: &etcdserverpb.DeleteRangeRequest{
					Key:      key,
					RangeEnd: key,
					PrevKv:   true,
				},
			}},
		},
	})
	require.NoError(t, err)
	require.True(t, txnResp.Succeeded)
	require.Len(t, txnResp.Responses, 1)
	deleteResp := txnResp.Responses[0].GetResponseDeleteRange()
	require.NotNil(t, deleteResp)
	require.NotNil(t, deleteResp.Header)
	require.Equal(t, txnResp.Header.Revision, deleteResp.Header.Revision)
	require.Equal(t, int64(0), deleteResp.Deleted)
	require.Empty(t, deleteResp.PrevKvs)

	getResp, err := server.Range(ctx, &etcdserverpb.RangeRequest{Key: key})
	require.NoError(t, err)
	require.Equal(t, int64(1), getResp.Count)
	require.Equal(t, []byte("value"), getResp.Kvs[0].Value)
}

func TestFollowerPutAndDeleteRangeProxyToLeader(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	metrics := mock.NewMinimalMetrics(ctrl)
	kv := memkv.NewKvStorage()
	defer func() {
		require.NoError(t, kv.Close())
	}()
	b := backend.NewBackend(kv, backend.Config{
		Identity:                "follower",
		EnableEtcdCompatibility: true,
	}, metrics)

	var putForwarded, deleteForwarded bool
	server := New(b, metrics, testPeerService{
		isLeader:     false,
		proxyEnabled: true,
		putFn: func(_ context.Context, req *etcdserverpb.PutRequest) (*etcdserverpb.PutResponse, error) {
			putForwarded = true
			require.Equal(t, []byte("/registry/follower-put"), req.Key)
			return &etcdserverpb.PutResponse{Header: &etcdserverpb.ResponseHeader{Revision: 10}}, nil
		},
		deleteRangeFn: func(_ context.Context, req *etcdserverpb.DeleteRangeRequest) (*etcdserverpb.DeleteRangeResponse, error) {
			deleteForwarded = true
			require.Equal(t, []byte("/registry/follower-put"), req.Key)
			return &etcdserverpb.DeleteRangeResponse{Header: &etcdserverpb.ResponseHeader{Revision: 11}, Deleted: 1}, nil
		},
	})
	defer server.stopLeases()

	putResp, err := server.Put(context.Background(), &etcdserverpb.PutRequest{Key: []byte("/registry/follower-put"), Value: []byte("v")})
	require.NoError(t, err)
	require.Equal(t, int64(10), putResp.Header.Revision)
	require.True(t, putForwarded)

	deleteResp, err := server.DeleteRange(context.Background(), &etcdserverpb.DeleteRangeRequest{Key: []byte("/registry/follower-put")})
	require.NoError(t, err)
	require.Equal(t, int64(1), deleteResp.Deleted)
	require.True(t, deleteForwarded)
}

func TestTxnCreateUpdateDeletePath(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()

	ctx := context.Background()
	key := []byte("/registry/deployments/a")

	createResp, err := server.Txn(ctx, &etcdserverpb.TxnRequest{
		Compare: []*etcdserverpb.Compare{{
			Key:         key,
			Target:      etcdserverpb.Compare_MOD,
			Result:      etcdserverpb.Compare_EQUAL,
			TargetUnion: &etcdserverpb.Compare_ModRevision{ModRevision: 0},
		}},
		Success: []*etcdserverpb.RequestOp{{
			Request: &etcdserverpb.RequestOp_RequestPut{
				RequestPut: &etcdserverpb.PutRequest{Key: key, Value: []byte("v1")},
			},
		}},
	})
	require.NoError(t, err)
	require.True(t, createResp.Succeeded)

	getResp, err := server.Range(ctx, &etcdserverpb.RangeRequest{Key: key})
	require.NoError(t, err)
	require.Len(t, getResp.Kvs, 1)

	updateResp, err := server.Txn(ctx, &etcdserverpb.TxnRequest{
		Compare: []*etcdserverpb.Compare{{
			Key:         key,
			Target:      etcdserverpb.Compare_MOD,
			Result:      etcdserverpb.Compare_EQUAL,
			TargetUnion: &etcdserverpb.Compare_ModRevision{ModRevision: getResp.Kvs[0].ModRevision},
		}},
		Success: []*etcdserverpb.RequestOp{{
			Request: &etcdserverpb.RequestOp_RequestPut{
				RequestPut: &etcdserverpb.PutRequest{Key: key, Value: []byte("v2")},
			},
		}},
		Failure: []*etcdserverpb.RequestOp{{
			Request: &etcdserverpb.RequestOp_RequestRange{
				RequestRange: &etcdserverpb.RangeRequest{Key: key},
			},
		}},
	})
	require.NoError(t, err)
	require.True(t, updateResp.Succeeded)

	deleteResp, err := server.Txn(ctx, &etcdserverpb.TxnRequest{
		Success: []*etcdserverpb.RequestOp{
			{Request: &etcdserverpb.RequestOp_RequestRange{RequestRange: &etcdserverpb.RangeRequest{Key: key}}},
			{Request: &etcdserverpb.RequestOp_RequestDeleteRange{RequestDeleteRange: &etcdserverpb.DeleteRangeRequest{Key: key}}},
		},
	})
	require.NoError(t, err)
	require.True(t, deleteResp.Succeeded)
}

func TestTxnCreateWithFailureRange(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()

	ctx := context.Background()
	key := []byte("/registry/generic-txn/create")
	_, err := server.Put(ctx, &etcdserverpb.PutRequest{Key: key, Value: []byte("exists")})
	require.NoError(t, err)

	resp, err := server.Txn(ctx, &etcdserverpb.TxnRequest{
		Compare: []*etcdserverpb.Compare{{
			Key:         key,
			Target:      etcdserverpb.Compare_MOD,
			Result:      etcdserverpb.Compare_EQUAL,
			TargetUnion: &etcdserverpb.Compare_ModRevision{ModRevision: 0},
		}},
		Success: []*etcdserverpb.RequestOp{{
			Request: &etcdserverpb.RequestOp_RequestPut{
				RequestPut: &etcdserverpb.PutRequest{Key: key, Value: []byte("created")},
			},
		}},
		Failure: []*etcdserverpb.RequestOp{{
			Request: &etcdserverpb.RequestOp_RequestRange{
				RequestRange: &etcdserverpb.RangeRequest{Key: key},
			},
		}},
	})
	require.NoError(t, err)
	require.False(t, resp.Succeeded)
	require.Len(t, resp.Responses, 1)
	rangeResp := resp.Responses[0].GetResponseRange()
	require.NotNil(t, rangeResp)
	require.NotNil(t, rangeResp.Header)
	require.Equal(t, resp.Header.Revision, rangeResp.Header.Revision)
	require.Len(t, rangeResp.Kvs, 1)
	require.Equal(t, []byte("exists"), rangeResp.Kvs[0].Value)
}

func TestTxnCreateWithoutFailureRangeReturnsEmptyFailureResponse(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()

	ctx := context.Background()
	key := []byte("/registry/generic-txn/create-empty-failure")
	_, err := server.Put(ctx, &etcdserverpb.PutRequest{Key: key, Value: []byte("exists")})
	require.NoError(t, err)

	resp, err := server.Txn(ctx, &etcdserverpb.TxnRequest{
		Compare: []*etcdserverpb.Compare{{
			Key:         key,
			Target:      etcdserverpb.Compare_MOD,
			Result:      etcdserverpb.Compare_EQUAL,
			TargetUnion: &etcdserverpb.Compare_ModRevision{ModRevision: 0},
		}},
		Success: []*etcdserverpb.RequestOp{{
			Request: &etcdserverpb.RequestOp_RequestPut{
				RequestPut: &etcdserverpb.PutRequest{Key: key, Value: []byte("created")},
			},
		}},
	})
	require.NoError(t, err)
	require.False(t, resp.Succeeded)
	require.Empty(t, resp.Responses)
}

func TestTxnCompareFailureRangeHeaderMatchesCurrentRevision(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()

	ctx := context.Background()
	key := []byte("/registry/generic-txn/compare-failure-header")
	seed, err := server.Put(ctx, &etcdserverpb.PutRequest{Key: key, Value: []byte("exists")})
	require.NoError(t, err)

	resp, err := server.Txn(ctx, &etcdserverpb.TxnRequest{
		Compare: []*etcdserverpb.Compare{{
			Key:         key,
			Target:      etcdserverpb.Compare_VERSION,
			Result:      etcdserverpb.Compare_EQUAL,
			TargetUnion: &etcdserverpb.Compare_Version{Version: 0},
		}},
		Success: []*etcdserverpb.RequestOp{{
			Request: &etcdserverpb.RequestOp_RequestPut{
				RequestPut: &etcdserverpb.PutRequest{Key: key, Value: []byte("created")},
			},
		}},
		Failure: []*etcdserverpb.RequestOp{{
			Request: &etcdserverpb.RequestOp_RequestRange{
				RequestRange: &etcdserverpb.RangeRequest{Key: key},
			},
		}},
	})
	require.NoError(t, err)
	require.False(t, resp.Succeeded)
	require.Equal(t, seed.Header.Revision, resp.Header.Revision)
	require.Len(t, resp.Responses, 1)

	failureRange := resp.Responses[0].GetResponseRange()
	require.NotNil(t, failureRange)
	require.Equal(t, seed.Header.Revision, failureRange.Header.Revision)
	require.Len(t, failureRange.Kvs, 1)
	require.Equal(t, seed.Header.Revision, failureRange.Kvs[0].CreateRevision)
	require.Equal(t, seed.Header.Revision, failureRange.Kvs[0].ModRevision)
	require.Equal(t, []byte("exists"), failureRange.Kvs[0].Value)
}

func TestTxnHeaderRevisionDeltasMatchEtcd(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()

	ctx := context.Background()
	prefix := []byte("/registry/generic-txn/revision-delta/")
	key := append([]byte(nil), prefix...)
	key = append(key, []byte("key")...)
	missingKey := append([]byte(nil), prefix...)
	missingKey = append(missingKey, []byte("missing")...)
	seed, err := server.Put(ctx, &etcdserverpb.PutRequest{Key: key, Value: []byte("one")})
	require.NoError(t, err)

	readOnly, err := server.Txn(ctx, &etcdserverpb.TxnRequest{Success: []*etcdserverpb.RequestOp{{
		Request: &etcdserverpb.RequestOp_RequestRange{
			RequestRange: &etcdserverpb.RangeRequest{Key: key},
		},
	}}})
	require.NoError(t, err)
	require.Equal(t, seed.Header.Revision, readOnly.Header.Revision)

	emptyDelete, err := server.Txn(ctx, &etcdserverpb.TxnRequest{Success: []*etcdserverpb.RequestOp{{
		Request: &etcdserverpb.RequestOp_RequestDeleteRange{
			RequestDeleteRange: &etcdserverpb.DeleteRangeRequest{Key: missingKey},
		},
	}}})
	require.NoError(t, err)
	require.Equal(t, seed.Header.Revision, emptyDelete.Header.Revision)

	write, err := server.Txn(ctx, &etcdserverpb.TxnRequest{Success: []*etcdserverpb.RequestOp{{
		Request: &etcdserverpb.RequestOp_RequestPut{
			RequestPut: &etcdserverpb.PutRequest{Key: key, Value: []byte("two")},
		},
	}}})
	require.NoError(t, err)
	require.Equal(t, seed.Header.Revision+1, write.Header.Revision)
}

func TestTxnCreateWithPrevKVReturnsNilPrevKV(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()

	ctx := context.Background()
	key := []byte("/registry/generic-txn/create-prev-kv")

	resp, err := server.Txn(ctx, &etcdserverpb.TxnRequest{
		Compare: []*etcdserverpb.Compare{{
			Key:         key,
			Target:      etcdserverpb.Compare_MOD,
			Result:      etcdserverpb.Compare_EQUAL,
			TargetUnion: &etcdserverpb.Compare_ModRevision{ModRevision: 0},
		}},
		Success: []*etcdserverpb.RequestOp{{
			Request: &etcdserverpb.RequestOp_RequestPut{
				RequestPut: &etcdserverpb.PutRequest{Key: key, Value: []byte("created"), PrevKv: true},
			},
		}},
	})
	require.NoError(t, err)
	require.True(t, resp.Succeeded)
	require.NotNil(t, resp.Header)
	require.Len(t, resp.Responses, 1)
	put := resp.Responses[0].GetResponsePut()
	require.NotNil(t, put)
	require.NotNil(t, put.Header)
	require.Equal(t, resp.Header.Revision, put.Header.Revision)
	require.Nil(t, put.PrevKv)
}

func TestTxnComparePutWithoutFailureRange(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()

	ctx := context.Background()
	key := []byte("/registry/generic-txn/update")
	putResp, err := server.Put(ctx, &etcdserverpb.PutRequest{Key: key, Value: []byte("v1")})
	require.NoError(t, err)

	updateResp, err := server.Txn(ctx, &etcdserverpb.TxnRequest{
		Compare: []*etcdserverpb.Compare{{
			Key:         key,
			Target:      etcdserverpb.Compare_MOD,
			Result:      etcdserverpb.Compare_EQUAL,
			TargetUnion: &etcdserverpb.Compare_ModRevision{ModRevision: putResp.Header.Revision},
		}},
		Success: []*etcdserverpb.RequestOp{{
			Request: &etcdserverpb.RequestOp_RequestPut{
				RequestPut: &etcdserverpb.PutRequest{Key: key, Value: []byte("v2")},
			},
		}},
	})
	require.NoError(t, err)
	require.True(t, updateResp.Succeeded)
	require.Len(t, updateResp.Responses, 1)
	require.NotNil(t, updateResp.Responses[0].GetResponsePut())

	staleResp, err := server.Txn(ctx, &etcdserverpb.TxnRequest{
		Compare: []*etcdserverpb.Compare{{
			Key:         key,
			Target:      etcdserverpb.Compare_MOD,
			Result:      etcdserverpb.Compare_EQUAL,
			TargetUnion: &etcdserverpb.Compare_ModRevision{ModRevision: putResp.Header.Revision},
		}},
		Success: []*etcdserverpb.RequestOp{{
			Request: &etcdserverpb.RequestOp_RequestPut{
				RequestPut: &etcdserverpb.PutRequest{Key: key, Value: []byte("v3")},
			},
		}},
	})
	require.NoError(t, err)
	require.False(t, staleResp.Succeeded)
	require.Empty(t, staleResp.Responses)
}

func TestTxnCrossKeyCompareMutationsUseGenericPath(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()
	ctx := context.Background()

	t.Run("create compare can update existing different key", func(t *testing.T) {
		guardKey := []byte("/registry/cross-key/create-guard")
		targetKey := []byte("/registry/cross-key/create-target")
		_, err := server.Put(ctx, &etcdserverpb.PutRequest{Key: targetKey, Value: []byte("old")})
		require.NoError(t, err)
		response, err := server.Txn(ctx, &etcdserverpb.TxnRequest{
			Compare: []*etcdserverpb.Compare{{
				Key: guardKey, Target: etcdserverpb.Compare_MOD, Result: etcdserverpb.Compare_EQUAL,
				TargetUnion: &etcdserverpb.Compare_ModRevision{ModRevision: 0},
			}},
			Success: []*etcdserverpb.RequestOp{{
				Request: &etcdserverpb.RequestOp_RequestPut{RequestPut: &etcdserverpb.PutRequest{
					Key: targetKey, Value: []byte("updated"),
				}},
			}},
		})
		require.NoError(t, err)
		require.True(t, response.Succeeded)
		current, err := server.Range(ctx, &etcdserverpb.RangeRequest{Key: targetKey})
		require.NoError(t, err)
		require.Equal(t, []byte("updated"), current.Kvs[0].Value)
		require.Equal(t, int64(2), current.Kvs[0].Version)
	})

	t.Run("mod compare can put different key", func(t *testing.T) {
		guardKey := []byte("/registry/cross-key/update-guard")
		targetKey := []byte("/registry/cross-key/update-target")
		guard, err := server.Put(ctx, &etcdserverpb.PutRequest{Key: guardKey, Value: []byte("guard")})
		require.NoError(t, err)
		response, err := server.Txn(ctx, &etcdserverpb.TxnRequest{
			Compare: []*etcdserverpb.Compare{{
				Key: guardKey, Target: etcdserverpb.Compare_MOD, Result: etcdserverpb.Compare_EQUAL,
				TargetUnion: &etcdserverpb.Compare_ModRevision{ModRevision: guard.Header.Revision},
			}},
			Success: []*etcdserverpb.RequestOp{{
				Request: &etcdserverpb.RequestOp_RequestPut{RequestPut: &etcdserverpb.PutRequest{
					Key: targetKey, Value: []byte("created"),
				}},
			}},
		})
		require.NoError(t, err)
		require.True(t, response.Succeeded)
		current, err := server.Range(ctx, &etcdserverpb.RangeRequest{Key: targetKey})
		require.NoError(t, err)
		require.Equal(t, []byte("created"), current.Kvs[0].Value)
	})

	t.Run("mod compare can delete different key", func(t *testing.T) {
		guardKey := []byte("/registry/cross-key/delete-guard")
		targetKey := []byte("/registry/cross-key/delete-target")
		guard, err := server.Put(ctx, &etcdserverpb.PutRequest{Key: guardKey, Value: []byte("guard")})
		require.NoError(t, err)
		_, err = server.Put(ctx, &etcdserverpb.PutRequest{Key: targetKey, Value: []byte("target")})
		require.NoError(t, err)
		response, err := server.Txn(ctx, &etcdserverpb.TxnRequest{
			Compare: []*etcdserverpb.Compare{{
				Key: guardKey, Target: etcdserverpb.Compare_MOD, Result: etcdserverpb.Compare_EQUAL,
				TargetUnion: &etcdserverpb.Compare_ModRevision{ModRevision: guard.Header.Revision},
			}},
			Success: []*etcdserverpb.RequestOp{{
				Request: &etcdserverpb.RequestOp_RequestDeleteRange{
					RequestDeleteRange: &etcdserverpb.DeleteRangeRequest{Key: targetKey},
				},
			}},
		})
		require.NoError(t, err)
		require.True(t, response.Succeeded)
		current, err := server.Range(ctx, &etcdserverpb.RangeRequest{Key: targetKey})
		require.NoError(t, err)
		require.Empty(t, current.Kvs)
	})
}

func TestTxnRangeCompareSelectsDeleteRangeBranch(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()

	ctx := context.Background()
	prefix := "/registry/range-compare-delete/"
	end := []byte("/registry/range-compare-delete0")
	for _, suffix := range []string{"a", "b", "c"} {
		_, err := server.Put(ctx, &etcdserverpb.PutRequest{
			Key: []byte(prefix + suffix), Value: []byte("v-" + suffix),
		})
		require.NoError(t, err)
	}

	response, err := server.Txn(ctx, &etcdserverpb.TxnRequest{
		Compare: []*etcdserverpb.Compare{{
			Key: []byte(prefix), RangeEnd: end,
			Target: etcdserverpb.Compare_VERSION, Result: etcdserverpb.Compare_GREATER,
			TargetUnion: &etcdserverpb.Compare_Version{Version: 0},
		}},
		Success: []*etcdserverpb.RequestOp{{
			Request: &etcdserverpb.RequestOp_RequestDeleteRange{
				RequestDeleteRange: &etcdserverpb.DeleteRangeRequest{
					Key: []byte(prefix + "b"), RangeEnd: end, PrevKv: true,
				},
			},
		}},
		Failure: []*etcdserverpb.RequestOp{{
			Request: &etcdserverpb.RequestOp_RequestPut{
				RequestPut: &etcdserverpb.PutRequest{Key: []byte(prefix + "failure"), Value: []byte("unexpected")},
			},
		}},
	})
	require.NoError(t, err)
	require.True(t, response.Succeeded)
	require.Len(t, response.Responses, 1)
	deleteResp := response.Responses[0].GetResponseDeleteRange()
	require.NotNil(t, deleteResp)
	require.NotNil(t, deleteResp.Header)
	require.Equal(t, response.Header.Revision, deleteResp.Header.Revision)
	require.Equal(t, int64(2), deleteResp.Deleted)
	require.Len(t, deleteResp.PrevKvs, 2)
	require.Equal(t, []byte(prefix+"b"), deleteResp.PrevKvs[0].Key)
	require.Equal(t, []byte(prefix+"c"), deleteResp.PrevKvs[1].Key)

	current, err := server.Range(ctx, &etcdserverpb.RangeRequest{Key: []byte(prefix), RangeEnd: end})
	require.NoError(t, err)
	require.NotNil(t, current.Header)
	require.Equal(t, response.Header.Revision, current.Header.Revision)
	require.Equal(t, int64(1), current.Count)
	require.Equal(t, []byte(prefix+"a"), current.Kvs[0].Key)

	missPrefix := "/registry/range-compare-delete-miss/"
	missEnd := []byte("/registry/range-compare-delete-miss0")
	for _, suffix := range []string{"a", "b"} {
		_, err = server.Put(ctx, &etcdserverpb.PutRequest{
			Key: []byte(missPrefix + suffix), Value: []byte("v-" + suffix),
		})
		require.NoError(t, err)
	}
	response, err = server.Txn(ctx, &etcdserverpb.TxnRequest{
		Compare: []*etcdserverpb.Compare{{
			Key: []byte(missPrefix), RangeEnd: missEnd,
			Target: etcdserverpb.Compare_VALUE, Result: etcdserverpb.Compare_EQUAL,
			TargetUnion: &etcdserverpb.Compare_Value{Value: []byte("not-present")},
		}},
		Success: []*etcdserverpb.RequestOp{{
			Request: &etcdserverpb.RequestOp_RequestDeleteRange{
				RequestDeleteRange: &etcdserverpb.DeleteRangeRequest{Key: []byte(missPrefix), RangeEnd: missEnd},
			},
		}},
		Failure: []*etcdserverpb.RequestOp{{
			Request: &etcdserverpb.RequestOp_RequestPut{
				RequestPut: &etcdserverpb.PutRequest{Key: []byte(missPrefix + "failure"), Value: []byte("selected")},
			},
		}},
	})
	require.NoError(t, err)
	require.False(t, response.Succeeded)
	putFailure := response.Responses[0].GetResponsePut()
	require.NotNil(t, putFailure)
	require.NotNil(t, putFailure.Header)
	require.Equal(t, response.Header.Revision, putFailure.Header.Revision)

	current, err = server.Range(ctx, &etcdserverpb.RangeRequest{Key: []byte(missPrefix), RangeEnd: missEnd})
	require.NoError(t, err)
	require.NotNil(t, current.Header)
	require.Equal(t, response.Header.Revision, current.Header.Revision)
	require.Equal(t, int64(3), current.Count)
	require.Equal(t, []byte(missPrefix+"a"), current.Kvs[0].Key)
	require.Equal(t, []byte(missPrefix+"b"), current.Kvs[1].Key)
	require.Equal(t, []byte(missPrefix+"failure"), current.Kvs[2].Key)
}

func TestTxnComparePutWithPrevKV(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()

	ctx := context.Background()
	key := []byte("/registry/generic-txn/update-prev-kv")
	putResp, err := server.Put(ctx, &etcdserverpb.PutRequest{Key: key, Value: []byte("v1")})
	require.NoError(t, err)

	updateResp, err := server.Txn(ctx, &etcdserverpb.TxnRequest{
		Compare: []*etcdserverpb.Compare{{
			Key:         key,
			Target:      etcdserverpb.Compare_MOD,
			Result:      etcdserverpb.Compare_EQUAL,
			TargetUnion: &etcdserverpb.Compare_ModRevision{ModRevision: putResp.Header.Revision},
		}},
		Success: []*etcdserverpb.RequestOp{{
			Request: &etcdserverpb.RequestOp_RequestPut{
				RequestPut: &etcdserverpb.PutRequest{Key: key, Value: []byte("v2"), PrevKv: true},
			},
		}},
	})
	require.NoError(t, err)
	require.True(t, updateResp.Succeeded)
	require.Len(t, updateResp.Responses, 1)
	put := updateResp.Responses[0].GetResponsePut()
	require.NotNil(t, put)
	require.NotNil(t, put.PrevKv)
	require.Equal(t, []byte("v1"), put.PrevKv.Value)
	require.Equal(t, putResp.Header.Revision, put.PrevKv.ModRevision)
}

func TestTxnComparePutIgnoreLeasePreservesExistingLease(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()

	ctx := context.Background()
	key := []byte("/registry/generic-txn/update-ignore-lease")
	leaseResp, err := server.LeaseGrant(ctx, &etcdserverpb.LeaseGrantRequest{TTL: 30, ID: 24681})
	require.NoError(t, err)
	putResp, err := server.Put(ctx, &etcdserverpb.PutRequest{Key: key, Value: []byte("v1"), Lease: leaseResp.ID})
	require.NoError(t, err)

	updateResp, err := server.Txn(ctx, &etcdserverpb.TxnRequest{
		Compare: []*etcdserverpb.Compare{{
			Key:         key,
			Target:      etcdserverpb.Compare_MOD,
			Result:      etcdserverpb.Compare_EQUAL,
			TargetUnion: &etcdserverpb.Compare_ModRevision{ModRevision: putResp.Header.Revision},
		}},
		Success: []*etcdserverpb.RequestOp{{
			Request: &etcdserverpb.RequestOp_RequestPut{
				RequestPut: &etcdserverpb.PutRequest{Key: key, Value: []byte("v2"), IgnoreLease: true},
			},
		}},
	})
	require.NoError(t, err)
	require.True(t, updateResp.Succeeded)

	ttlResp, err := server.LeaseTimeToLive(ctx, &etcdserverpb.LeaseTimeToLiveRequest{ID: leaseResp.ID, Keys: true})
	require.NoError(t, err)
	require.Equal(t, [][]byte{key}, ttlResp.Keys)

	rangeResp, err := server.Range(ctx, &etcdserverpb.RangeRequest{Key: key})
	require.NoError(t, err)
	require.Len(t, rangeResp.Kvs, 1)
	require.Equal(t, []byte("v2"), rangeResp.Kvs[0].Value)
}

func TestTxnComparePutIgnoreValueUpdatesLeasePreservesValue(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()

	ctx := context.Background()
	key := []byte("/registry/generic-txn/update-ignore-value")
	leaseOne, err := server.LeaseGrant(ctx, &etcdserverpb.LeaseGrantRequest{TTL: 30, ID: 24684})
	require.NoError(t, err)
	leaseTwo, err := server.LeaseGrant(ctx, &etcdserverpb.LeaseGrantRequest{TTL: 30, ID: 24685})
	require.NoError(t, err)
	putResp, err := server.Put(ctx, &etcdserverpb.PutRequest{Key: key, Value: []byte("v1"), Lease: leaseOne.ID})
	require.NoError(t, err)

	updateResp, err := server.Txn(ctx, &etcdserverpb.TxnRequest{
		Compare: []*etcdserverpb.Compare{{
			Key:         key,
			Target:      etcdserverpb.Compare_MOD,
			Result:      etcdserverpb.Compare_EQUAL,
			TargetUnion: &etcdserverpb.Compare_ModRevision{ModRevision: putResp.Header.Revision},
		}},
		Success: []*etcdserverpb.RequestOp{{
			Request: &etcdserverpb.RequestOp_RequestPut{
				RequestPut: &etcdserverpb.PutRequest{Key: key, Lease: leaseTwo.ID, IgnoreValue: true},
			},
		}},
	})
	require.NoError(t, err)
	require.True(t, updateResp.Succeeded)

	rangeResp, err := server.Range(ctx, &etcdserverpb.RangeRequest{Key: key})
	require.NoError(t, err)
	require.Len(t, rangeResp.Kvs, 1)
	require.Equal(t, []byte("v1"), rangeResp.Kvs[0].Value)

	ttlOne, err := server.LeaseTimeToLive(ctx, &etcdserverpb.LeaseTimeToLiveRequest{ID: leaseOne.ID, Keys: true})
	require.NoError(t, err)
	require.Empty(t, ttlOne.Keys)
	ttlTwo, err := server.LeaseTimeToLive(ctx, &etcdserverpb.LeaseTimeToLiveRequest{ID: leaseTwo.ID, Keys: true})
	require.NoError(t, err)
	require.Equal(t, [][]byte{key}, ttlTwo.Keys)
}

func TestTxnIgnoreValueThenIgnoreLeasePreservesNewLeaseBinding(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()

	ctx := context.Background()
	key := []byte("/registry/generic-txn/ignore-lease-chain")
	leaseA, err := server.LeaseGrant(ctx, &etcdserverpb.LeaseGrantRequest{TTL: 300, ID: 24701})
	require.NoError(t, err)
	leaseB, err := server.LeaseGrant(ctx, &etcdserverpb.LeaseGrantRequest{TTL: 300, ID: 24702})
	require.NoError(t, err)
	_, err = server.Put(ctx, &etcdserverpb.PutRequest{Key: key, Value: []byte("old"), Lease: leaseA.ID})
	require.NoError(t, err)

	ignoreValue, err := server.Txn(ctx, &etcdserverpb.TxnRequest{Success: []*etcdserverpb.RequestOp{
		{Request: &etcdserverpb.RequestOp_RequestPut{RequestPut: &etcdserverpb.PutRequest{
			Key: key, Lease: leaseB.ID, IgnoreValue: true, PrevKv: true,
		}}},
		{Request: &etcdserverpb.RequestOp_RequestRange{RequestRange: &etcdserverpb.RangeRequest{Key: key}}},
	}})
	require.NoError(t, err)
	require.NotNil(t, ignoreValue.Header)
	putIgnoreValue := ignoreValue.Responses[0].GetResponsePut()
	require.NotNil(t, putIgnoreValue)
	require.NotNil(t, putIgnoreValue.Header)
	require.Equal(t, ignoreValue.Header.Revision, putIgnoreValue.Header.Revision)
	require.NotNil(t, putIgnoreValue.PrevKv)
	require.Equal(t, []byte("old"), putIgnoreValue.PrevKv.Value)
	require.Equal(t, leaseA.ID, putIgnoreValue.PrevKv.Lease)
	staged := ignoreValue.Responses[1].GetResponseRange()
	require.NotNil(t, staged)
	require.NotNil(t, staged.Header)
	require.Equal(t, ignoreValue.Header.Revision, staged.Header.Revision)
	require.Len(t, staged.Kvs, 1)
	require.Equal(t, []byte("old"), staged.Kvs[0].Value)
	require.Equal(t, leaseB.ID, staged.Kvs[0].Lease)

	ignoreLease, err := server.Txn(ctx, &etcdserverpb.TxnRequest{Success: []*etcdserverpb.RequestOp{
		{Request: &etcdserverpb.RequestOp_RequestPut{RequestPut: &etcdserverpb.PutRequest{
			Key: key, Value: []byte("new"), IgnoreLease: true, PrevKv: true,
		}}},
	}})
	require.NoError(t, err)
	require.NotNil(t, ignoreLease.Header)
	require.Equal(t, ignoreValue.Header.Revision+1, ignoreLease.Header.Revision)
	putIgnoreLease := ignoreLease.Responses[0].GetResponsePut()
	require.NotNil(t, putIgnoreLease)
	require.NotNil(t, putIgnoreLease.Header)
	require.Equal(t, ignoreLease.Header.Revision, putIgnoreLease.Header.Revision)
	require.NotNil(t, putIgnoreLease.PrevKv)
	require.Equal(t, []byte("old"), putIgnoreLease.PrevKv.Value)
	require.Equal(t, leaseB.ID, putIgnoreLease.PrevKv.Lease)
	final, err := server.Range(ctx, &etcdserverpb.RangeRequest{Key: key})
	require.NoError(t, err)
	require.NotNil(t, final.Header)
	require.Equal(t, ignoreLease.Header.Revision, final.Header.Revision)
	require.Len(t, final.Kvs, 1)
	require.Equal(t, []byte("new"), final.Kvs[0].Value)
	require.Equal(t, leaseB.ID, final.Kvs[0].Lease)

	_, err = server.LeaseRevoke(ctx, &etcdserverpb.LeaseRevokeRequest{ID: leaseA.ID})
	require.NoError(t, err)
	afterA, err := server.Range(ctx, &etcdserverpb.RangeRequest{Key: key})
	require.NoError(t, err)
	require.Len(t, afterA.Kvs, 1)
	require.Equal(t, leaseB.ID, afterA.Kvs[0].Lease)
	_, err = server.LeaseRevoke(ctx, &etcdserverpb.LeaseRevokeRequest{ID: leaseB.ID})
	require.NoError(t, err)
	afterB, err := server.Range(ctx, &etcdserverpb.RangeRequest{Key: key})
	require.NoError(t, err)
	require.Empty(t, afterB.Kvs)
}

func TestTxnCompareDeleteWithoutFailureRange(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()

	ctx := context.Background()
	key := []byte("/registry/generic-txn/delete")
	putResp, err := server.Put(ctx, &etcdserverpb.PutRequest{Key: key, Value: []byte("v1")})
	require.NoError(t, err)

	deleteResp, err := server.Txn(ctx, &etcdserverpb.TxnRequest{
		Compare: []*etcdserverpb.Compare{{
			Key:         key,
			Target:      etcdserverpb.Compare_MOD,
			Result:      etcdserverpb.Compare_EQUAL,
			TargetUnion: &etcdserverpb.Compare_ModRevision{ModRevision: putResp.Header.Revision},
		}},
		Success: []*etcdserverpb.RequestOp{{
			Request: &etcdserverpb.RequestOp_RequestDeleteRange{
				RequestDeleteRange: &etcdserverpb.DeleteRangeRequest{Key: key},
			},
		}},
	})
	require.NoError(t, err)
	require.True(t, deleteResp.Succeeded)
	require.Len(t, deleteResp.Responses, 1)
	require.NotNil(t, deleteResp.Responses[0].GetResponseDeleteRange())

	staleResp, err := server.Txn(ctx, &etcdserverpb.TxnRequest{
		Compare: []*etcdserverpb.Compare{{
			Key:         key,
			Target:      etcdserverpb.Compare_MOD,
			Result:      etcdserverpb.Compare_EQUAL,
			TargetUnion: &etcdserverpb.Compare_ModRevision{ModRevision: putResp.Header.Revision},
		}},
		Success: []*etcdserverpb.RequestOp{{
			Request: &etcdserverpb.RequestOp_RequestDeleteRange{
				RequestDeleteRange: &etcdserverpb.DeleteRangeRequest{Key: key},
			},
		}},
	})
	require.NoError(t, err)
	require.False(t, staleResp.Succeeded)
	require.Empty(t, staleResp.Responses)
}

func TestTxnCompareDeleteWithPrevKV(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()

	ctx := context.Background()
	key := []byte("/registry/generic-txn/delete-prev-kv")
	putResp, err := server.Put(ctx, &etcdserverpb.PutRequest{Key: key, Value: []byte("v1")})
	require.NoError(t, err)

	deleteResp, err := server.Txn(ctx, &etcdserverpb.TxnRequest{
		Compare: []*etcdserverpb.Compare{{
			Key:         key,
			Target:      etcdserverpb.Compare_MOD,
			Result:      etcdserverpb.Compare_EQUAL,
			TargetUnion: &etcdserverpb.Compare_ModRevision{ModRevision: putResp.Header.Revision},
		}},
		Success: []*etcdserverpb.RequestOp{{
			Request: &etcdserverpb.RequestOp_RequestDeleteRange{
				RequestDeleteRange: &etcdserverpb.DeleteRangeRequest{Key: key, PrevKv: true},
			},
		}},
	})
	require.NoError(t, err)
	require.True(t, deleteResp.Succeeded)
	require.Len(t, deleteResp.Responses, 1)
	del := deleteResp.Responses[0].GetResponseDeleteRange()
	require.NotNil(t, del)
	require.Equal(t, int64(1), del.Deleted)
	require.Len(t, del.PrevKvs, 1)
	require.Equal(t, []byte("v1"), del.PrevKvs[0].Value)
	require.Equal(t, putResp.Header.Revision, del.PrevKvs[0].ModRevision)
}

func TestTxnCompareDeleteRangeBypassesCompareDeleteFastPath(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()
	trap := &compareDeleteTrapBackendShim{BackendShim: server.backend}
	server.backend = trap

	ctx := context.Background()
	prefix := "/registry/generic-txn/delete-range-fast-path/"
	guardKey := []byte(prefix + "0guard")
	end := []byte("/registry/generic-txn/delete-range-fast-path0")
	guard, err := server.Put(ctx, &etcdserverpb.PutRequest{Key: guardKey, Value: []byte("guard")})
	require.NoError(t, err)
	for _, suffix := range []string{"a", "b", "c"} {
		_, err = server.Put(ctx, &etcdserverpb.PutRequest{Key: []byte(prefix + suffix), Value: []byte("v-" + suffix)})
		require.NoError(t, err)
	}

	resp, err := server.Txn(ctx, &etcdserverpb.TxnRequest{
		Compare: []*etcdserverpb.Compare{{
			Key:         guardKey,
			Target:      etcdserverpb.Compare_MOD,
			Result:      etcdserverpb.Compare_EQUAL,
			TargetUnion: &etcdserverpb.Compare_ModRevision{ModRevision: guard.Header.Revision},
		}},
		Success: []*etcdserverpb.RequestOp{{
			Request: &etcdserverpb.RequestOp_RequestDeleteRange{
				RequestDeleteRange: &etcdserverpb.DeleteRangeRequest{
					Key: []byte(prefix + "a"), RangeEnd: end, PrevKv: true,
				},
			},
		}},
	})
	require.NoError(t, err)
	require.False(t, trap.called)
	require.True(t, resp.Succeeded)
	require.NotNil(t, resp.Header)
	require.Len(t, resp.Responses, 1)
	deleteResp := resp.Responses[0].GetResponseDeleteRange()
	require.NotNil(t, deleteResp)
	require.NotNil(t, deleteResp.Header)
	require.Equal(t, resp.Header.Revision, deleteResp.Header.Revision)
	require.Equal(t, int64(3), deleteResp.Deleted)
	require.Len(t, deleteResp.PrevKvs, 3)
	require.Equal(t, []byte(prefix+"a"), deleteResp.PrevKvs[0].Key)
	require.Equal(t, []byte(prefix+"c"), deleteResp.PrevKvs[2].Key)
}

func TestTxnCompareDeleteWithFailureRange(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()

	ctx := context.Background()
	key := []byte("/registry/generic-txn/delete-failure-range")
	putResp, err := server.Put(ctx, &etcdserverpb.PutRequest{Key: key, Value: []byte("v1")})
	require.NoError(t, err)
	_, err = server.Put(ctx, &etcdserverpb.PutRequest{Key: key, Value: []byte("v2")})
	require.NoError(t, err)

	resp, err := server.Txn(ctx, &etcdserverpb.TxnRequest{
		Compare: []*etcdserverpb.Compare{{
			Key:         key,
			Target:      etcdserverpb.Compare_MOD,
			Result:      etcdserverpb.Compare_EQUAL,
			TargetUnion: &etcdserverpb.Compare_ModRevision{ModRevision: putResp.Header.Revision},
		}},
		Success: []*etcdserverpb.RequestOp{{
			Request: &etcdserverpb.RequestOp_RequestDeleteRange{
				RequestDeleteRange: &etcdserverpb.DeleteRangeRequest{Key: key},
			},
		}},
		Failure: []*etcdserverpb.RequestOp{{
			Request: &etcdserverpb.RequestOp_RequestRange{
				RequestRange: &etcdserverpb.RangeRequest{Key: key},
			},
		}},
	})
	require.NoError(t, err)
	require.False(t, resp.Succeeded)
	require.Len(t, resp.Responses, 1)
	rangeResp := resp.Responses[0].GetResponseRange()
	require.NotNil(t, rangeResp)
	require.NotNil(t, rangeResp.Header)
	require.Equal(t, resp.Header.Revision, rangeResp.Header.Revision)
	require.Len(t, rangeResp.Kvs, 1)
	require.Equal(t, []byte("v2"), rangeResp.Kvs[0].Value)
}

func TestTxnSimpleSuccessPutRangeDeleteResponsesInOrder(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()

	ctx := context.Background()
	key := []byte("/registry/generic-txn/simple")
	deleteKey := []byte("/registry/generic-txn/simple-delete")

	_, err := server.Put(ctx, &etcdserverpb.PutRequest{Key: deleteKey, Value: []byte("delete-me")})
	require.NoError(t, err)

	resp, err := server.Txn(ctx, &etcdserverpb.TxnRequest{
		Success: []*etcdserverpb.RequestOp{
			{Request: &etcdserverpb.RequestOp_RequestPut{
				RequestPut: &etcdserverpb.PutRequest{Key: key, Value: []byte("v1")},
			}},
			{Request: &etcdserverpb.RequestOp_RequestRange{
				RequestRange: &etcdserverpb.RangeRequest{Key: key},
			}},
			{Request: &etcdserverpb.RequestOp_RequestDeleteRange{
				RequestDeleteRange: &etcdserverpb.DeleteRangeRequest{Key: deleteKey, PrevKv: true},
			}},
		},
	})
	require.NoError(t, err)
	require.True(t, resp.Succeeded)
	require.Len(t, resp.Responses, 3)
	putResp := resp.Responses[0].GetResponsePut()
	require.NotNil(t, putResp)
	require.NotNil(t, putResp.Header)
	require.Equal(t, resp.Header.Revision, putResp.Header.Revision)

	rangeResp := resp.Responses[1].GetResponseRange()
	require.NotNil(t, rangeResp)
	require.NotNil(t, rangeResp.Header)
	require.Equal(t, resp.Header.Revision, rangeResp.Header.Revision)
	require.Len(t, rangeResp.Kvs, 1)
	require.Equal(t, []byte("v1"), rangeResp.Kvs[0].Value)

	deleteResp := resp.Responses[2].GetResponseDeleteRange()
	require.NotNil(t, deleteResp)
	require.NotNil(t, deleteResp.Header)
	require.Equal(t, resp.Header.Revision, deleteResp.Header.Revision)
	require.Equal(t, int64(1), deleteResp.Deleted)
	require.Len(t, deleteResp.PrevKvs, 1)
	require.Equal(t, []byte("delete-me"), deleteResp.PrevKvs[0].Value)

	getResp, err := server.Range(ctx, &etcdserverpb.RangeRequest{Key: key})
	require.NoError(t, err)
	require.Len(t, getResp.Kvs, 1)
	require.Equal(t, []byte("v1"), getResp.Kvs[0].Value)

	deletedResp, err := server.Range(ctx, &etcdserverpb.RangeRequest{Key: deleteKey})
	require.NoError(t, err)
	require.Empty(t, deletedResp.Kvs)
}

func TestTxnRangeSeesPriorWritesButNotLaterWrites(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()

	ctx := context.Background()
	prefix := []byte("/registry/generic-txn/ordered-view/")
	keyA := append(append([]byte(nil), prefix...), 'a')
	keyB := append(append([]byte(nil), prefix...), 'b')
	resp, err := server.Txn(ctx, &etcdserverpb.TxnRequest{Success: []*etcdserverpb.RequestOp{
		{Request: &etcdserverpb.RequestOp_RequestPut{RequestPut: &etcdserverpb.PutRequest{Key: keyA, Value: []byte("a")}}},
		{Request: &etcdserverpb.RequestOp_RequestRange{RequestRange: &etcdserverpb.RangeRequest{
			Key: prefix, RangeEnd: []byte("/registry/generic-txn/ordered-view0"),
		}}},
		{Request: &etcdserverpb.RequestOp_RequestPut{RequestPut: &etcdserverpb.PutRequest{Key: keyB, Value: []byte("b")}}},
	}})
	require.NoError(t, err)
	rangeResp := resp.Responses[1].GetResponseRange()
	require.Len(t, rangeResp.Kvs, 1)
	require.Equal(t, keyA, rangeResp.Kvs[0].Key)
	require.Equal(t, resp.Header.Revision, rangeResp.Header.Revision)
	require.Equal(t, resp.Header.Revision, rangeResp.Kvs[0].ModRevision)

	final, err := server.Range(ctx, &etcdserverpb.RangeRequest{Key: prefix, RangeEnd: []byte("/registry/generic-txn/ordered-view0")})
	require.NoError(t, err)
	require.Len(t, final.Kvs, 2)
	require.Equal(t, resp.Header.Revision, final.Kvs[0].ModRevision)
	require.Equal(t, resp.Header.Revision, final.Kvs[1].ModRevision)
}

func TestTxnRangeCreateRevisionFiltersApplyToStagedView(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()

	ctx := context.Background()
	prefix := []byte("/registry/generic-txn/create-filter/")
	end := []byte("/registry/generic-txn/create-filter0")
	keyA := append(append([]byte(nil), prefix...), 'a')
	keyB := append(append([]byte(nil), prefix...), 'b')

	seed, err := server.Put(ctx, &etcdserverpb.PutRequest{Key: keyA, Value: []byte("old")})
	require.NoError(t, err)
	txnRevision := seed.Header.Revision + 1

	resp, err := server.Txn(ctx, &etcdserverpb.TxnRequest{Success: []*etcdserverpb.RequestOp{
		{Request: &etcdserverpb.RequestOp_RequestPut{RequestPut: &etcdserverpb.PutRequest{
			Key: keyB, Value: []byte("new"),
		}}},
		{Request: &etcdserverpb.RequestOp_RequestRange{RequestRange: &etcdserverpb.RangeRequest{
			Key: prefix, RangeEnd: end, MinCreateRevision: txnRevision,
		}}},
		{Request: &etcdserverpb.RequestOp_RequestRange{RequestRange: &etcdserverpb.RangeRequest{
			Key: prefix, RangeEnd: end, MaxCreateRevision: seed.Header.Revision,
		}}},
	}})
	require.NoError(t, err)
	require.Equal(t, txnRevision, resp.Header.Revision)
	require.Len(t, resp.Responses, 3)

	newOnly := resp.Responses[1].GetResponseRange()
	require.Equal(t, int64(2), newOnly.Count)
	require.False(t, newOnly.More)
	require.Equal(t, [][]byte{keyB}, [][]byte{newOnly.Kvs[0].Key})
	require.Equal(t, txnRevision, newOnly.Kvs[0].CreateRevision)
	require.Equal(t, txnRevision, newOnly.Kvs[0].ModRevision)

	oldOnly := resp.Responses[2].GetResponseRange()
	require.Equal(t, int64(2), oldOnly.Count)
	require.False(t, oldOnly.More)
	require.Equal(t, [][]byte{keyA}, [][]byte{oldOnly.Kvs[0].Key})
	require.Equal(t, seed.Header.Revision, oldOnly.Kvs[0].CreateRevision)
}

func TestTxnOverlappingDeleteRangesUseStagedViewAndOneRevision(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()

	ctx := context.Background()
	prefix := "/registry/generic-txn/overlap-delete/"
	for _, suffix := range []string{"a", "b", "c"} {
		_, err := server.Put(ctx, &etcdserverpb.PutRequest{Key: []byte(prefix + suffix), Value: []byte(suffix)})
		require.NoError(t, err)
	}
	before := int64(server.backend.GetCurrentRevision())
	resp, err := server.Txn(ctx, &etcdserverpb.TxnRequest{Success: []*etcdserverpb.RequestOp{
		{Request: &etcdserverpb.RequestOp_RequestDeleteRange{RequestDeleteRange: &etcdserverpb.DeleteRangeRequest{
			Key: []byte(prefix + "a"), RangeEnd: []byte(prefix + "c"), PrevKv: true,
		}}},
		{Request: &etcdserverpb.RequestOp_RequestDeleteRange{RequestDeleteRange: &etcdserverpb.DeleteRangeRequest{
			Key: []byte(prefix + "b"), RangeEnd: []byte(prefix + "d"), PrevKv: true,
		}}},
	}})
	require.NoError(t, err)
	require.Equal(t, before+1, resp.Header.Revision)
	first := resp.Responses[0].GetResponseDeleteRange()
	second := resp.Responses[1].GetResponseDeleteRange()
	require.NotNil(t, first)
	require.NotNil(t, first.Header)
	require.Equal(t, resp.Header.Revision, first.Header.Revision)
	require.NotNil(t, second)
	require.NotNil(t, second.Header)
	require.Equal(t, resp.Header.Revision, second.Header.Revision)
	require.Equal(t, int64(2), first.Deleted)
	require.Equal(t, [][]byte{[]byte("a"), []byte("b")}, [][]byte{first.PrevKvs[0].Value, first.PrevKvs[1].Value})
	require.Equal(t, int64(1), second.Deleted)
	require.Equal(t, []byte("c"), second.PrevKvs[0].Value)

	remaining, err := server.Range(ctx, &etcdserverpb.RangeRequest{Key: []byte(prefix), RangeEnd: []byte(prefix + "z")})
	require.NoError(t, err)
	require.Empty(t, remaining.Kvs)
}

func TestTxnIgnoreOptionsUseStagedAtomicValidation(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()

	ctx := context.Background()
	lease, err := server.LeaseGrant(ctx, &etcdserverpb.LeaseGrantRequest{TTL: 30})
	require.NoError(t, err)
	key := []byte("/registry/generic-txn/ignore/existing")
	_, err = server.Put(ctx, &etcdserverpb.PutRequest{Key: key, Value: []byte("old"), Lease: lease.ID})
	require.NoError(t, err)

	resp, err := server.Txn(ctx, &etcdserverpb.TxnRequest{Success: []*etcdserverpb.RequestOp{
		{Request: &etcdserverpb.RequestOp_RequestPut{RequestPut: &etcdserverpb.PutRequest{Key: key, IgnoreLease: true, Value: []byte("new")}}},
		{Request: &etcdserverpb.RequestOp_RequestRange{RequestRange: &etcdserverpb.RangeRequest{Key: key}}},
	}})
	require.NoError(t, err)
	require.NotNil(t, resp.Header)
	putResp := resp.Responses[0].GetResponsePut()
	require.NotNil(t, putResp)
	require.NotNil(t, putResp.Header)
	require.Equal(t, resp.Header.Revision, putResp.Header.Revision)
	rangeResp := resp.Responses[1].GetResponseRange()
	require.NotNil(t, rangeResp)
	require.NotNil(t, rangeResp.Header)
	require.Equal(t, resp.Header.Revision, rangeResp.Header.Revision)
	staged := rangeResp.Kvs[0]
	require.Equal(t, []byte("new"), staged.Value)
	require.Equal(t, lease.ID, staged.Lease)

	missing := []byte("/registry/generic-txn/ignore/missing")
	mustNotLand := []byte("/registry/generic-txn/ignore/must-not-land")
	resp, err = server.Txn(ctx, &etcdserverpb.TxnRequest{Success: []*etcdserverpb.RequestOp{
		{Request: &etcdserverpb.RequestOp_RequestPut{RequestPut: &etcdserverpb.PutRequest{Key: mustNotLand, Value: []byte("x")}}},
		{Request: &etcdserverpb.RequestOp_RequestPut{RequestPut: &etcdserverpb.PutRequest{Key: missing, IgnoreValue: true}}},
	}})
	require.Nil(t, resp)
	requireDirectKVError(t, err, rpctypes.ErrGRPCKeyNotFound, codes.InvalidArgument, "etcdserver: key not found")
	get, err := server.Range(ctx, &etcdserverpb.RangeRequest{Key: mustNotLand})
	require.NoError(t, err)
	require.Empty(t, get.Kvs)
}

func TestTxnHistoricalRangeAfterPutReadsOldValueWithTxnHeader(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()

	ctx := context.Background()
	key := []byte("/registry/generic-txn/historical-overlay")
	seed, err := server.Put(ctx, &etcdserverpb.PutRequest{Key: key, Value: []byte("old")})
	require.NoError(t, err)

	resp, err := server.Txn(ctx, &etcdserverpb.TxnRequest{Success: []*etcdserverpb.RequestOp{
		{Request: &etcdserverpb.RequestOp_RequestPut{RequestPut: &etcdserverpb.PutRequest{Key: key, Value: []byte("new")}}},
		{Request: &etcdserverpb.RequestOp_RequestRange{RequestRange: &etcdserverpb.RangeRequest{Key: key, Revision: seed.Header.Revision}}},
	}})
	require.NoError(t, err)
	historical := resp.Responses[1].GetResponseRange()
	require.Equal(t, []byte("old"), historical.Kvs[0].Value)
	require.Equal(t, seed.Header.Revision, historical.Kvs[0].ModRevision)
	require.Equal(t, resp.Header.Revision, historical.Header.Revision)

	current, err := server.Range(ctx, &etcdserverpb.RangeRequest{Key: key})
	require.NoError(t, err)
	require.Equal(t, []byte("new"), current.Kvs[0].Value)
}

func TestTxnNoOpDeleteRangesDoNotConsumeRevision(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()

	ctx := context.Background()
	before := int64(server.backend.GetCurrentRevision())
	resp, err := server.Txn(ctx, &etcdserverpb.TxnRequest{Success: []*etcdserverpb.RequestOp{
		{Request: &etcdserverpb.RequestOp_RequestDeleteRange{RequestDeleteRange: &etcdserverpb.DeleteRangeRequest{
			Key: []byte("/registry/generic-txn/noop/a"), RangeEnd: []byte("/registry/generic-txn/noop/m"),
		}}},
		{Request: &etcdserverpb.RequestOp_RequestDeleteRange{RequestDeleteRange: &etcdserverpb.DeleteRangeRequest{
			Key: []byte("/registry/generic-txn/noop/m"), RangeEnd: []byte("/registry/generic-txn/noop/z"),
		}}},
	}})
	require.NoError(t, err)
	require.Equal(t, before, resp.Header.Revision)
	first := resp.Responses[0].GetResponseDeleteRange()
	second := resp.Responses[1].GetResponseDeleteRange()
	require.NotNil(t, first)
	require.NotNil(t, first.Header)
	require.Equal(t, resp.Header.Revision, first.Header.Revision)
	require.Zero(t, first.Deleted)
	require.NotNil(t, second)
	require.NotNil(t, second.Header)
	require.Equal(t, resp.Header.Revision, second.Header.Revision)
	require.Zero(t, second.Deleted)
	require.Equal(t, uint64(before), server.backend.GetCurrentRevision())
}

func TestTxnRangeOptionsApplyToStagedView(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()

	ctx := context.Background()
	prefix := "/registry/generic-txn/range-options/"
	_, err := server.Put(ctx, &etcdserverpb.PutRequest{Key: []byte(prefix + "a"), Value: []byte("old")})
	require.NoError(t, err)
	before := int64(server.backend.GetCurrentRevision())

	resp, err := server.Txn(ctx, &etcdserverpb.TxnRequest{Success: []*etcdserverpb.RequestOp{
		{Request: &etcdserverpb.RequestOp_RequestPut{RequestPut: &etcdserverpb.PutRequest{
			Key: []byte(prefix + "b"), Value: []byte("new-b"),
		}}},
		{Request: &etcdserverpb.RequestOp_RequestPut{RequestPut: &etcdserverpb.PutRequest{
			Key: []byte(prefix + "c"), Value: []byte("new-c"),
		}}},
		{Request: &etcdserverpb.RequestOp_RequestRange{RequestRange: &etcdserverpb.RangeRequest{
			Key:            []byte(prefix),
			RangeEnd:       []byte(prefix + "z"),
			MinModRevision: before + 1,
			CountOnly:      true,
			Limit:          1,
		}}},
		{Request: &etcdserverpb.RequestOp_RequestRange{RequestRange: &etcdserverpb.RangeRequest{
			Key:        []byte(prefix),
			RangeEnd:   []byte(prefix + "z"),
			SortOrder:  etcdserverpb.RangeRequest_DESCEND,
			SortTarget: etcdserverpb.RangeRequest_KEY,
			Limit:      2,
			KeysOnly:   true,
		}}},
	}})
	require.NoError(t, err)
	require.Equal(t, before+1, resp.Header.Revision)
	firstPut := resp.Responses[0].GetResponsePut()
	require.NotNil(t, firstPut)
	require.NotNil(t, firstPut.Header)
	require.Equal(t, resp.Header.Revision, firstPut.Header.Revision)
	secondPut := resp.Responses[1].GetResponsePut()
	require.NotNil(t, secondPut)
	require.NotNil(t, secondPut.Header)
	require.Equal(t, resp.Header.Revision, secondPut.Header.Revision)

	counted := resp.Responses[2].GetResponseRange()
	require.NotNil(t, counted)
	require.NotNil(t, counted.Header)
	require.Equal(t, resp.Header.Revision, counted.Header.Revision)
	require.Equal(t, int64(3), counted.Count)
	require.Empty(t, counted.Kvs)
	require.False(t, counted.More, "CountOnly ignores Limit and never reports truncated KVs")

	limited := resp.Responses[3].GetResponseRange()
	require.NotNil(t, limited)
	require.NotNil(t, limited.Header)
	require.Equal(t, resp.Header.Revision, limited.Header.Revision)
	require.Equal(t, int64(3), limited.Count)
	require.True(t, limited.More)
	require.Equal(t, [][]byte{[]byte(prefix + "c"), []byte(prefix + "b")}, [][]byte{limited.Kvs[0].Key, limited.Kvs[1].Key})
	require.Nil(t, limited.Kvs[0].Value)
	require.Nil(t, limited.Kvs[1].Value)
}

func TestPutIgnoreLeaseRequiresExistingKey(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()

	key := []byte("/registry/put-ignore-lease-missing")
	resp, err := server.Put(context.Background(), &etcdserverpb.PutRequest{
		Key: key, Value: []byte("must-not-create"), IgnoreLease: true,
	})
	require.Nil(t, resp)
	requireDirectKVError(t, err, rpctypes.ErrGRPCKeyNotFound, codes.InvalidArgument, "etcdserver: key not found")
	get, err := server.Range(context.Background(), &etcdserverpb.RangeRequest{Key: key})
	require.NoError(t, err)
	require.Empty(t, get.Kvs)
}

func TestPutMissingLeasePrecedesMissingIgnoreValueKey(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()

	req := &etcdserverpb.PutRequest{
		Key:         []byte("/registry/pods/missing-key-and-lease"),
		Lease:       987654321,
		IgnoreValue: true,
	}
	_, putErr := server.Put(context.Background(), req)
	requireDirectKVError(t, putErr, rpctypes.ErrGRPCLeaseNotFound, codes.NotFound, "etcdserver: requested lease not found")

	_, txnErr := server.Txn(context.Background(), &etcdserverpb.TxnRequest{
		Success: []*etcdserverpb.RequestOp{{
			Request: &etcdserverpb.RequestOp_RequestPut{RequestPut: &etcdserverpb.PutRequest{
				Key:         append([]byte(nil), req.Key...),
				Lease:       req.Lease,
				IgnoreValue: req.IgnoreValue,
			}},
		}},
	})
	requireDirectKVError(t, txnErr, rpctypes.ErrGRPCLeaseNotFound, codes.NotFound, "etcdserver: requested lease not found")
}

func TestTxnExecutionValidationFollowsSelectedOperationOrder(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()

	put := &etcdserverpb.RequestOp{Request: &etcdserverpb.RequestOp_RequestPut{
		RequestPut: &etcdserverpb.PutRequest{
			Key: []byte("/registry/pods/txn-validation-order-put"), Value: []byte("value"), Lease: 987654321,
		},
	}}
	read := &etcdserverpb.RequestOp{Request: &etcdserverpb.RequestOp_RequestRange{
		RequestRange: &etcdserverpb.RangeRequest{
			Key: []byte("/registry/pods/txn-validation-order-range"), Revision: math.MaxInt64,
		},
	}}

	_, err := server.Txn(context.Background(), &etcdserverpb.TxnRequest{
		Success: []*etcdserverpb.RequestOp{put, read},
	})
	requireDirectKVError(t, err, rpctypes.ErrGRPCLeaseNotFound, codes.NotFound, "etcdserver: requested lease not found")

	_, err = server.Txn(context.Background(), &etcdserverpb.TxnRequest{
		Success: []*etcdserverpb.RequestOp{read, put},
	})
	requireDirectKVError(t, err, rpctypes.ErrGRPCFutureRev, codes.OutOfRange, "etcdserver: mvcc: required revision is a future revision")
}

func TestTxnSimpleSuccessPutWithLeaseIsRevoked(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()

	ctx := context.Background()
	key := []byte("/registry/generic-txn/simple-lease")
	leaseResp, err := server.LeaseGrant(ctx, &etcdserverpb.LeaseGrantRequest{TTL: 30, ID: 12345})
	require.NoError(t, err)

	resp, err := server.Txn(ctx, &etcdserverpb.TxnRequest{
		Success: []*etcdserverpb.RequestOp{
			{Request: &etcdserverpb.RequestOp_RequestPut{
				RequestPut: &etcdserverpb.PutRequest{Key: key, Value: []byte("leased"), Lease: leaseResp.ID},
			}},
		},
	})
	require.NoError(t, err)
	require.True(t, resp.Succeeded)
	attachment, err := server.backend.InternalGet(ctx, leaseAttachKey(string(key)))
	require.NoError(t, err)
	require.Equal(t, []byte("12345"), attachment)

	ttlResp, err := server.LeaseTimeToLive(ctx, &etcdserverpb.LeaseTimeToLiveRequest{ID: leaseResp.ID, Keys: true})
	require.NoError(t, err)
	require.Equal(t, [][]byte{key}, ttlResp.Keys)

	_, err = server.LeaseRevoke(ctx, &etcdserverpb.LeaseRevokeRequest{ID: leaseResp.ID})
	require.NoError(t, err)

	getResp, err := server.Range(ctx, &etcdserverpb.RangeRequest{Key: key})
	require.NoError(t, err)
	require.Empty(t, getResp.Kvs)
}

func TestTxnEmptySuccessMatchesEtcd(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()

	resp, err := server.Txn(context.Background(), &etcdserverpb.TxnRequest{})
	require.NoError(t, err)
	require.True(t, resp.Succeeded)
	require.Empty(t, resp.Responses)
	require.NotNil(t, resp.Header)
}

func TestTxnNestedSuccessResponseMatchesEtcd(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()

	ctx := context.Background()
	key := []byte("/registry/generic-txn/nested-exec")
	resp, err := server.Txn(ctx, &etcdserverpb.TxnRequest{
		Success: []*etcdserverpb.RequestOp{{
			Request: &etcdserverpb.RequestOp_RequestTxn{
				RequestTxn: &etcdserverpb.TxnRequest{
					Success: []*etcdserverpb.RequestOp{
						{Request: &etcdserverpb.RequestOp_RequestPut{
							RequestPut: &etcdserverpb.PutRequest{Key: key, Value: []byte("nested")},
						}},
						{Request: &etcdserverpb.RequestOp_RequestRange{
							RequestRange: &etcdserverpb.RangeRequest{Key: key},
						}},
					},
				},
			},
		}},
	})
	require.NoError(t, err)
	require.True(t, resp.Succeeded)
	require.Len(t, resp.Responses, 1)
	nested := resp.Responses[0].GetResponseTxn()
	require.NotNil(t, nested)
	require.NotNil(t, nested.Header)
	require.Zero(t, nested.Header.Revision)
	require.True(t, nested.Succeeded)
	require.Len(t, nested.Responses, 2)
	putResp := nested.Responses[0].GetResponsePut()
	require.NotNil(t, putResp)
	require.NotNil(t, putResp.Header)
	require.Equal(t, resp.Header.Revision, putResp.Header.Revision)
	rangeResp := nested.Responses[1].GetResponseRange()
	require.NotNil(t, rangeResp)
	require.NotNil(t, rangeResp.Header)
	require.Equal(t, resp.Header.Revision, rangeResp.Header.Revision)
	require.Len(t, rangeResp.Kvs, 1)
	require.Equal(t, []byte("nested"), rangeResp.Kvs[0].Value)
}

func TestTxnNestedWriteOnlyUsesSingleRevision(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()

	ctx := context.Background()
	keyA := []byte("/registry/generic-txn/nested-atomic/a")
	keyB := []byte("/registry/generic-txn/nested-atomic/b")
	resp, err := server.Txn(ctx, &etcdserverpb.TxnRequest{
		Success: []*etcdserverpb.RequestOp{
			{Request: &etcdserverpb.RequestOp_RequestPut{RequestPut: &etcdserverpb.PutRequest{Key: keyA, Value: []byte("a")}}},
			{Request: &etcdserverpb.RequestOp_RequestTxn{RequestTxn: &etcdserverpb.TxnRequest{
				Success: []*etcdserverpb.RequestOp{{Request: &etcdserverpb.RequestOp_RequestPut{
					RequestPut: &etcdserverpb.PutRequest{Key: keyB, Value: []byte("b")},
				}}},
			}}},
		},
	})
	require.NoError(t, err)
	require.True(t, resp.Succeeded)
	require.Len(t, resp.Responses, 2)
	nested := resp.Responses[1].GetResponseTxn()
	require.NotNil(t, nested)
	require.NotNil(t, nested.Header)
	require.Zero(t, nested.Header.Revision)

	gotA, err := server.Range(ctx, &etcdserverpb.RangeRequest{Key: keyA})
	require.NoError(t, err)
	gotB, err := server.Range(ctx, &etcdserverpb.RangeRequest{Key: keyB})
	require.NoError(t, err)
	require.Equal(t, resp.Header.Revision, gotA.Kvs[0].ModRevision)
	require.Equal(t, resp.Header.Revision, gotB.Kvs[0].ModRevision)
}

func TestTxnAtomicPutReturnsPrevKV(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()

	ctx := context.Background()
	key := []byte("/registry/generic-txn/atomic-prev/a")
	other := []byte("/registry/generic-txn/atomic-prev/b")
	_, err := server.Put(ctx, &etcdserverpb.PutRequest{Key: key, Value: []byte("old")})
	require.NoError(t, err)

	resp, err := server.Txn(ctx, &etcdserverpb.TxnRequest{Success: []*etcdserverpb.RequestOp{
		{Request: &etcdserverpb.RequestOp_RequestPut{RequestPut: &etcdserverpb.PutRequest{Key: key, Value: []byte("new"), PrevKv: true}}},
		{Request: &etcdserverpb.RequestOp_RequestPut{RequestPut: &etcdserverpb.PutRequest{Key: other, Value: []byte("other")}}},
	}})
	require.NoError(t, err)
	putResp := resp.Responses[0].GetResponsePut()
	require.NotNil(t, putResp)
	require.NotNil(t, putResp.PrevKv)
	require.Equal(t, key, putResp.PrevKv.Key)
	require.Equal(t, []byte("old"), putResp.PrevKv.Value)
	require.Less(t, putResp.PrevKv.ModRevision, resp.Header.Revision)
}

func TestTxnNestedCompareFailureResponseMatchesEtcd(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()

	ctx := context.Background()
	key := []byte("/registry/generic-txn/nested-compare-failure")
	_, err := server.Put(ctx, &etcdserverpb.PutRequest{Key: key, Value: []byte("existing")})
	require.NoError(t, err)

	resp, err := server.Txn(ctx, &etcdserverpb.TxnRequest{
		Success: []*etcdserverpb.RequestOp{{
			Request: &etcdserverpb.RequestOp_RequestTxn{
				RequestTxn: &etcdserverpb.TxnRequest{
					Compare: []*etcdserverpb.Compare{{
						Key:         key,
						Target:      etcdserverpb.Compare_VALUE,
						Result:      etcdserverpb.Compare_EQUAL,
						TargetUnion: &etcdserverpb.Compare_Value{Value: []byte("missing")},
					}},
					Success: []*etcdserverpb.RequestOp{{
						Request: &etcdserverpb.RequestOp_RequestPut{
							RequestPut: &etcdserverpb.PutRequest{Key: key, Value: []byte("unexpected")},
						},
					}},
					Failure: []*etcdserverpb.RequestOp{{
						Request: &etcdserverpb.RequestOp_RequestRange{
							RequestRange: &etcdserverpb.RangeRequest{Key: key},
						},
					}},
				},
			},
		}},
	})
	require.NoError(t, err)
	require.True(t, resp.Succeeded)
	require.Len(t, resp.Responses, 1)
	nested := resp.Responses[0].GetResponseTxn()
	require.NotNil(t, nested)
	require.NotNil(t, nested.Header)
	require.Zero(t, nested.Header.Revision)
	require.False(t, nested.Succeeded)
	require.Len(t, nested.Responses, 1)
	rangeResp := nested.Responses[0].GetResponseRange()
	require.NotNil(t, rangeResp)
	require.NotNil(t, rangeResp.Header)
	require.Equal(t, resp.Header.Revision, rangeResp.Header.Revision)
	require.Len(t, rangeResp.Kvs, 1)
	require.Equal(t, []byte("existing"), rangeResp.Kvs[0].Value)
}

func TestTxnNestedComparePathIsComputedBeforeWrites(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()

	ctx := context.Background()
	compareKey := []byte("/registry/generic-txn/nested-path-before-write/compare")
	resultKey := []byte("/registry/generic-txn/nested-path-before-write/result")
	resp, err := server.Txn(ctx, &etcdserverpb.TxnRequest{
		Success: []*etcdserverpb.RequestOp{
			{Request: &etcdserverpb.RequestOp_RequestPut{
				RequestPut: &etcdserverpb.PutRequest{Key: compareKey, Value: []byte("outer")},
			}},
			{Request: &etcdserverpb.RequestOp_RequestTxn{
				RequestTxn: &etcdserverpb.TxnRequest{
					Compare: []*etcdserverpb.Compare{{
						Key:         compareKey,
						Target:      etcdserverpb.Compare_VALUE,
						Result:      etcdserverpb.Compare_EQUAL,
						TargetUnion: &etcdserverpb.Compare_Value{Value: []byte("outer")},
					}},
					Success: []*etcdserverpb.RequestOp{{
						Request: &etcdserverpb.RequestOp_RequestPut{
							RequestPut: &etcdserverpb.PutRequest{Key: resultKey, Value: []byte("nested-success")},
						},
					}},
					Failure: []*etcdserverpb.RequestOp{{
						Request: &etcdserverpb.RequestOp_RequestPut{
							RequestPut: &etcdserverpb.PutRequest{Key: resultKey, Value: []byte("nested-failure")},
						},
					}},
				},
			}},
		},
	})
	require.NoError(t, err)
	require.True(t, resp.Succeeded)
	require.Len(t, resp.Responses, 2)
	nested := resp.Responses[1].GetResponseTxn()
	require.NotNil(t, nested)
	require.NotNil(t, nested.Header)
	require.Zero(t, nested.Header.Revision)
	require.False(t, nested.Succeeded)

	getResp, err := server.Range(ctx, &etcdserverpb.RangeRequest{Key: resultKey})
	require.NoError(t, err)
	require.Len(t, getResp.Kvs, 1)
	require.Equal(t, []byte("nested-failure"), getResp.Kvs[0].Value)
}

func TestTxnRangeFutureRevisionIsCheckedBeforeWrites(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()

	ctx := context.Background()
	key := []byte("/registry/generic-txn/range-future-before-write")
	futureRev := int64(server.backend.GetCurrentRevision()) + 1
	resp, err := server.Txn(ctx, &etcdserverpb.TxnRequest{
		Success: []*etcdserverpb.RequestOp{
			{Request: &etcdserverpb.RequestOp_RequestPut{
				RequestPut: &etcdserverpb.PutRequest{Key: key, Value: []byte("should-not-write")},
			}},
			{Request: &etcdserverpb.RequestOp_RequestRange{
				RequestRange: &etcdserverpb.RangeRequest{Key: key, Revision: futureRev},
			}},
		},
	})
	require.Nil(t, resp)
	requireDirectKVError(t, err, rpctypes.ErrGRPCFutureRev, codes.OutOfRange, "etcdserver: mvcc: required revision is a future revision")

	getResp, err := server.Range(ctx, &etcdserverpb.RangeRequest{Key: key})
	require.NoError(t, err)
	require.Empty(t, getResp.Kvs)
}

func TestTxnRangeCompactedRevisionIsCheckedBeforeWrites(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()

	ctx := context.Background()
	key := []byte("/registry/generic-txn/range-compacted-before-write")
	putResp, err := server.Put(ctx, &etcdserverpb.PutRequest{Key: key, Value: []byte("base")})
	require.NoError(t, err)
	putResp, err = server.Put(ctx, &etcdserverpb.PutRequest{Key: key, Value: []byte("base2")})
	require.NoError(t, err)
	require.Eventually(t, func() bool {
		return server.backend.GetCurrentRevision() >= uint64(putResp.Header.Revision)
	}, time.Second, 10*time.Millisecond)
	compactRev := putResp.Header.Revision
	_, err = server.Compact(ctx, &etcdserverpb.CompactionRequest{Revision: compactRev})
	require.NoError(t, err)

	resp, err := server.Txn(ctx, &etcdserverpb.TxnRequest{
		Success: []*etcdserverpb.RequestOp{
			{Request: &etcdserverpb.RequestOp_RequestPut{
				RequestPut: &etcdserverpb.PutRequest{Key: key, Value: []byte("should-not-write")},
			}},
			{Request: &etcdserverpb.RequestOp_RequestRange{
				RequestRange: &etcdserverpb.RangeRequest{Key: key, Revision: compactRev - 1},
			}},
		},
	})
	require.Nil(t, resp)
	requireDirectKVError(t, err, rpctypes.ErrGRPCCompacted, codes.OutOfRange, "etcdserver: mvcc: required revision has been compacted")

	getResp, err := server.Range(ctx, &etcdserverpb.RangeRequest{Key: key})
	require.NoError(t, err)
	require.Len(t, getResp.Kvs, 1)
	require.Equal(t, []byte("base2"), getResp.Kvs[0].Value)
}

func TestTxnNestedRangeFutureRevisionIsCheckedBeforeWrites(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()

	ctx := context.Background()
	key := []byte("/registry/generic-txn/nested-range-future-before-write")
	futureRev := int64(server.backend.GetCurrentRevision()) + 1
	resp, err := server.Txn(ctx, &etcdserverpb.TxnRequest{
		Success: []*etcdserverpb.RequestOp{
			{Request: &etcdserverpb.RequestOp_RequestPut{
				RequestPut: &etcdserverpb.PutRequest{Key: key, Value: []byte("should-not-write")},
			}},
			{Request: &etcdserverpb.RequestOp_RequestTxn{
				RequestTxn: &etcdserverpb.TxnRequest{
					Success: []*etcdserverpb.RequestOp{{
						Request: &etcdserverpb.RequestOp_RequestRange{
							RequestRange: &etcdserverpb.RangeRequest{Key: key, Revision: futureRev},
						},
					}},
				},
			}},
		},
	})
	require.Nil(t, resp)
	requireDirectKVError(t, err, rpctypes.ErrGRPCFutureRev, codes.OutOfRange, "etcdserver: mvcc: required revision is a future revision")

	getResp, err := server.Range(ctx, &etcdserverpb.RangeRequest{Key: key})
	require.NoError(t, err)
	require.Empty(t, getResp.Kvs)
}

func TestTxnCompareValueRunsSuccessBranch(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()

	ctx := context.Background()
	key := []byte("/registry/generic-txn/compare-value")
	_, err := server.Put(ctx, &etcdserverpb.PutRequest{Key: key, Value: []byte("v1")})
	require.NoError(t, err)

	resp, err := server.Txn(ctx, &etcdserverpb.TxnRequest{
		Compare: []*etcdserverpb.Compare{{
			Key:         key,
			Target:      etcdserverpb.Compare_VALUE,
			Result:      etcdserverpb.Compare_EQUAL,
			TargetUnion: &etcdserverpb.Compare_Value{Value: []byte("v1")},
		}},
		Success: []*etcdserverpb.RequestOp{
			{Request: &etcdserverpb.RequestOp_RequestPut{
				RequestPut: &etcdserverpb.PutRequest{Key: key, Value: []byte("v2")},
			}},
			{Request: &etcdserverpb.RequestOp_RequestRange{
				RequestRange: &etcdserverpb.RangeRequest{Key: key},
			}},
		},
		Failure: []*etcdserverpb.RequestOp{
			{Request: &etcdserverpb.RequestOp_RequestPut{
				RequestPut: &etcdserverpb.PutRequest{Key: key, Value: []byte("failure")},
			}},
		},
	})
	require.NoError(t, err)
	require.True(t, resp.Succeeded)
	require.Len(t, resp.Responses, 2)
	putResp := resp.Responses[0].GetResponsePut()
	require.NotNil(t, putResp)
	require.NotNil(t, putResp.Header)
	require.Equal(t, resp.Header.Revision, putResp.Header.Revision)
	rangeResp := resp.Responses[1].GetResponseRange()
	require.NotNil(t, rangeResp)
	require.NotNil(t, rangeResp.Header)
	require.Equal(t, resp.Header.Revision, rangeResp.Header.Revision)
	require.Len(t, rangeResp.Kvs, 1)
	require.Equal(t, []byte("v2"), rangeResp.Kvs[0].Value)
}

func TestTxnCompareValueRunsFailureBranch(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()

	ctx := context.Background()
	key := []byte("/registry/generic-txn/compare-value-failure")
	_, err := server.Put(ctx, &etcdserverpb.PutRequest{Key: key, Value: []byte("v1")})
	require.NoError(t, err)

	resp, err := server.Txn(ctx, &etcdserverpb.TxnRequest{
		Compare: []*etcdserverpb.Compare{{
			Key:         key,
			Target:      etcdserverpb.Compare_VALUE,
			Result:      etcdserverpb.Compare_EQUAL,
			TargetUnion: &etcdserverpb.Compare_Value{Value: []byte("missing")},
		}},
		Success: []*etcdserverpb.RequestOp{
			{Request: &etcdserverpb.RequestOp_RequestPut{
				RequestPut: &etcdserverpb.PutRequest{Key: key, Value: []byte("success")},
			}},
		},
		Failure: []*etcdserverpb.RequestOp{
			{Request: &etcdserverpb.RequestOp_RequestRange{
				RequestRange: &etcdserverpb.RangeRequest{Key: key},
			}},
			{Request: &etcdserverpb.RequestOp_RequestDeleteRange{
				RequestDeleteRange: &etcdserverpb.DeleteRangeRequest{Key: key, PrevKv: true},
			}},
		},
	})
	require.NoError(t, err)
	require.False(t, resp.Succeeded)
	require.Len(t, resp.Responses, 2)
	rangeResp := resp.Responses[0].GetResponseRange()
	require.NotNil(t, rangeResp)
	require.Len(t, rangeResp.Kvs, 1)
	require.Equal(t, []byte("v1"), rangeResp.Kvs[0].Value)
	deleteResp := resp.Responses[1].GetResponseDeleteRange()
	require.NotNil(t, deleteResp)
	require.NotNil(t, deleteResp.Header)
	require.Equal(t, resp.Header.Revision, deleteResp.Header.Revision)
	require.Equal(t, int64(1), deleteResp.Deleted)
}

func TestTxnCompareValueAlwaysFailsForAbsentKey(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()

	key := []byte("/registry/generic-txn/compare-value-absent")
	tests := []struct {
		name   string
		result etcdserverpb.Compare_CompareResult
		value  []byte
	}{
		{name: "equal empty", result: etcdserverpb.Compare_EQUAL},
		{name: "not equal nonempty", result: etcdserverpb.Compare_NOT_EQUAL, value: []byte("value")},
		{name: "unknown result", result: etcdserverpb.Compare_CompareResult(99)},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			resp, err := server.Txn(context.Background(), &etcdserverpb.TxnRequest{
				Compare: []*etcdserverpb.Compare{{
					Key:         key,
					Target:      etcdserverpb.Compare_VALUE,
					Result:      tt.result,
					TargetUnion: &etcdserverpb.Compare_Value{Value: tt.value},
				}},
				Success: []*etcdserverpb.RequestOp{{Request: &etcdserverpb.RequestOp_RequestPut{
					RequestPut: &etcdserverpb.PutRequest{Key: key, Value: []byte("success")},
				}}},
				Failure: []*etcdserverpb.RequestOp{{Request: &etcdserverpb.RequestOp_RequestRange{
					RequestRange: &etcdserverpb.RangeRequest{Key: key},
				}}},
			})
			require.NoError(t, err)
			require.False(t, resp.Succeeded)
			rangeResp := resp.Responses[0].GetResponseRange()
			require.NotNil(t, rangeResp)
			require.NotNil(t, rangeResp.Header)
			require.Equal(t, resp.Header.Revision, rangeResp.Header.Revision)
			require.Empty(t, rangeResp.Kvs)
		})
	}
}

func TestTxnCompareEnumDifferentialScenarioMatchesEtcd(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()

	tests := []struct {
		name          string
		target        etcdserverpb.Compare_CompareTarget
		result        etcdserverpb.Compare_CompareResult
		compareValue  []byte
		wantSucceeded bool
		wantValue     []byte
	}{
		{name: "unknown-result", target: etcdserverpb.Compare_MOD, result: 99, wantSucceeded: true, wantValue: []byte("success")},
		{name: "unknown-target-equal", target: 99, result: etcdserverpb.Compare_EQUAL, wantSucceeded: true, wantValue: []byte("success")},
		{name: "unknown-target-not-equal", target: 99, result: etcdserverpb.Compare_NOT_EQUAL, wantValue: []byte("failure")},
		{name: "absent-value-equal-empty", target: etcdserverpb.Compare_VALUE, result: etcdserverpb.Compare_EQUAL, wantValue: []byte("failure")},
		{name: "absent-value-not-equal", target: etcdserverpb.Compare_VALUE, result: etcdserverpb.Compare_NOT_EQUAL, compareValue: []byte("value"), wantValue: []byte("failure")},
		{name: "absent-value-unknown-result", target: etcdserverpb.Compare_VALUE, result: 99, wantValue: []byte("failure")},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			key := []byte("/registry/generic-txn/compare-enum/" + tt.name)
			compare := &etcdserverpb.Compare{
				Key:         key,
				Target:      tt.target,
				Result:      tt.result,
				TargetUnion: &etcdserverpb.Compare_ModRevision{ModRevision: 0},
			}
			if tt.target == etcdserverpb.Compare_VALUE {
				compare.TargetUnion = &etcdserverpb.Compare_Value{Value: tt.compareValue}
			}

			resp, err := server.Txn(context.Background(), &etcdserverpb.TxnRequest{
				Compare: []*etcdserverpb.Compare{compare},
				Success: []*etcdserverpb.RequestOp{{Request: &etcdserverpb.RequestOp_RequestPut{
					RequestPut: &etcdserverpb.PutRequest{Key: key, Value: []byte("success")},
				}}},
				Failure: []*etcdserverpb.RequestOp{{Request: &etcdserverpb.RequestOp_RequestPut{
					RequestPut: &etcdserverpb.PutRequest{Key: key, Value: []byte("failure")},
				}}},
			})
			require.NoError(t, err)
			require.Equal(t, tt.wantSucceeded, resp.Succeeded)

			ranged, err := server.Range(context.Background(), &etcdserverpb.RangeRequest{Key: key})
			require.NoError(t, err)
			require.Len(t, ranged.Kvs, 1)
			require.Equal(t, tt.wantValue, ranged.Kvs[0].Value)
		})
	}
}

func TestTxnRejectsEmptyCompareKey(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()

	_, err := server.Txn(context.Background(), &etcdserverpb.TxnRequest{
		Compare: []*etcdserverpb.Compare{{
			Target:      etcdserverpb.Compare_MOD,
			Result:      etcdserverpb.Compare_EQUAL,
			TargetUnion: &etcdserverpb.Compare_ModRevision{ModRevision: 1},
		}},
	})
	requireDirectKVError(t, err, rpctypes.ErrGRPCEmptyKey, codes.InvalidArgument, "etcdserver: key is not provided")
}

func TestTxnUnknownCompareEnumsMatchEtcdFallthrough(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()

	tests := []struct {
		name         string
		target       etcdserverpb.Compare_CompareTarget
		result       etcdserverpb.Compare_CompareResult
		compareValue []byte
		succeeded    bool
		value        []byte
	}{
		{
			name:      "unknown-result",
			target:    etcdserverpb.Compare_MOD,
			result:    99,
			succeeded: true,
			value:     []byte("success"),
		},
		{
			name:      "unknown-target-equal",
			target:    99,
			result:    etcdserverpb.Compare_EQUAL,
			succeeded: true,
			value:     []byte("success"),
		},
		{
			name:      "unknown-target-not-equal",
			target:    99,
			result:    etcdserverpb.Compare_NOT_EQUAL,
			succeeded: false,
			value:     []byte("failure"),
		},
		{
			name:      "absent-value-equal-empty",
			target:    etcdserverpb.Compare_VALUE,
			result:    etcdserverpb.Compare_EQUAL,
			succeeded: false,
			value:     []byte("failure"),
		},
		{
			name:         "absent-value-not-equal",
			target:       etcdserverpb.Compare_VALUE,
			result:       etcdserverpb.Compare_NOT_EQUAL,
			compareValue: []byte("value"),
			succeeded:    false,
			value:        []byte("failure"),
		},
		{
			name:      "absent-value-unknown-result",
			target:    etcdserverpb.Compare_VALUE,
			result:    99,
			succeeded: false,
			value:     []byte("failure"),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			key := []byte("/registry/generic-txn/unknown-compare/" + tt.name)
			compare := &etcdserverpb.Compare{
				Key:         key,
				Target:      tt.target,
				Result:      tt.result,
				TargetUnion: &etcdserverpb.Compare_ModRevision{ModRevision: 0},
			}
			if tt.target == etcdserverpb.Compare_VALUE {
				compare.TargetUnion = &etcdserverpb.Compare_Value{Value: tt.compareValue}
			}
			resp, err := server.Txn(context.Background(), &etcdserverpb.TxnRequest{
				Compare: []*etcdserverpb.Compare{compare},
				Success: []*etcdserverpb.RequestOp{{Request: &etcdserverpb.RequestOp_RequestPut{
					RequestPut: &etcdserverpb.PutRequest{Key: key, Value: []byte("success")},
				}}},
				Failure: []*etcdserverpb.RequestOp{{Request: &etcdserverpb.RequestOp_RequestPut{
					RequestPut: &etcdserverpb.PutRequest{Key: key, Value: []byte("failure")},
				}}},
			})
			require.NoError(t, err)
			require.Equal(t, tt.succeeded, resp.Succeeded)

			rangeResp, err := server.Range(context.Background(), &etcdserverpb.RangeRequest{Key: key})
			require.NoError(t, err)
			require.Len(t, rangeResp.Kvs, 1)
			require.Equal(t, tt.value, rangeResp.Kvs[0].Value)
		})
	}
}

func TestTxnRejectsInvalidRequestOps(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()

	tests := []struct {
		name        string
		op          *etcdserverpb.RequestOp
		wantErr     error
		wantMessage string
	}{
		{
			name: "put empty key",
			op: &etcdserverpb.RequestOp{Request: &etcdserverpb.RequestOp_RequestPut{
				RequestPut: &etcdserverpb.PutRequest{Value: []byte("v1")},
			}},
			wantErr:     rpctypes.ErrGRPCEmptyKey,
			wantMessage: "etcdserver: key is not provided",
		},
		{
			name: "put ignore value with value",
			op: &etcdserverpb.RequestOp{Request: &etcdserverpb.RequestOp_RequestPut{
				RequestPut: &etcdserverpb.PutRequest{Key: []byte("/registry/generic-txn/invalid-op"), Value: []byte("v1"), IgnoreValue: true},
			}},
			wantErr:     rpctypes.ErrGRPCValueProvided,
			wantMessage: "etcdserver: value is provided",
		},
		{
			name: "range invalid sort",
			op: &etcdserverpb.RequestOp{Request: &etcdserverpb.RequestOp_RequestRange{
				RequestRange: &etcdserverpb.RangeRequest{Key: []byte("/registry/generic-txn/invalid-op"), SortOrder: etcdserverpb.RangeRequest_SortOrder(99)},
			}},
			wantErr:     rpctypes.ErrGRPCInvalidSortOption,
			wantMessage: "etcdserver: invalid sort option",
		},
		{
			name: "delete empty key",
			op: &etcdserverpb.RequestOp{Request: &etcdserverpb.RequestOp_RequestDeleteRange{
				RequestDeleteRange: &etcdserverpb.DeleteRangeRequest{},
			}},
			wantErr:     rpctypes.ErrGRPCEmptyKey,
			wantMessage: "etcdserver: key is not provided",
		},
		{
			name:        "empty operation",
			op:          &etcdserverpb.RequestOp{},
			wantErr:     rpctypes.ErrGRPCKeyNotFound,
			wantMessage: "etcdserver: key not found",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := server.Txn(context.Background(), &etcdserverpb.TxnRequest{
				Success: []*etcdserverpb.RequestOp{tt.op},
			})
			requireDirectKVError(t, err, tt.wantErr, codes.InvalidArgument, tt.wantMessage)
		})
	}
}

func TestTxnOperationValidationMessagesMatchEtcd(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()

	key := []byte("/registry/generic-txn/operation-validation")
	tests := []struct {
		name        string
		op          *etcdserverpb.RequestOp
		wantErr     error
		wantCode    codes.Code
		wantMessage string
	}{
		{
			name: "put-empty-key-precedes-ignore-value",
			op: &etcdserverpb.RequestOp{Request: &etcdserverpb.RequestOp_RequestPut{
				RequestPut: &etcdserverpb.PutRequest{Value: []byte("value"), IgnoreValue: true},
			}},
			wantErr: rpctypes.ErrGRPCEmptyKey, wantCode: codes.InvalidArgument, wantMessage: "etcdserver: key is not provided",
		},
		{
			name: "put-ignore-value-precedes-ignore-lease",
			op: &etcdserverpb.RequestOp{Request: &etcdserverpb.RequestOp_RequestPut{
				RequestPut: &etcdserverpb.PutRequest{
					Key: key, Value: []byte("value"), Lease: 1, IgnoreValue: true, IgnoreLease: true,
				},
			}},
			wantErr: rpctypes.ErrGRPCValueProvided, wantCode: codes.InvalidArgument, wantMessage: "etcdserver: value is provided",
		},
		{
			name: "put-ignore-lease-with-lease",
			op: &etcdserverpb.RequestOp{Request: &etcdserverpb.RequestOp_RequestPut{
				RequestPut: &etcdserverpb.PutRequest{Key: key, Lease: 1, IgnoreLease: true},
			}},
			wantErr: rpctypes.ErrGRPCLeaseProvided, wantCode: codes.InvalidArgument, wantMessage: "etcdserver: lease is provided",
		},
		{
			name: "put-missing-lease-precedes-missing-ignore-value-key",
			op: &etcdserverpb.RequestOp{Request: &etcdserverpb.RequestOp_RequestPut{
				RequestPut: &etcdserverpb.PutRequest{Key: key, Lease: 987654321, IgnoreValue: true},
			}},
			wantErr: rpctypes.ErrGRPCLeaseNotFound, wantCode: codes.NotFound, wantMessage: "etcdserver: requested lease not found",
		},
		{
			name: "range-empty-key-precedes-invalid-sort",
			op: &etcdserverpb.RequestOp{Request: &etcdserverpb.RequestOp_RequestRange{
				RequestRange: &etcdserverpb.RangeRequest{SortOrder: 99, SortTarget: 99},
			}},
			wantErr: rpctypes.ErrGRPCEmptyKey, wantCode: codes.InvalidArgument, wantMessage: "etcdserver: key is not provided",
		},
		{
			name: "range-invalid-order-precedes-target",
			op: &etcdserverpb.RequestOp{Request: &etcdserverpb.RequestOp_RequestRange{
				RequestRange: &etcdserverpb.RangeRequest{Key: key, SortOrder: 99, SortTarget: 99},
			}},
			wantErr: rpctypes.ErrGRPCInvalidSortOption, wantCode: codes.InvalidArgument, wantMessage: "etcdserver: invalid sort option",
		},
		{
			name: "range-invalid-target",
			op: &etcdserverpb.RequestOp{Request: &etcdserverpb.RequestOp_RequestRange{
				RequestRange: &etcdserverpb.RangeRequest{Key: key, SortTarget: 99},
			}},
			wantErr: rpctypes.ErrGRPCInvalidSortOption, wantCode: codes.InvalidArgument, wantMessage: "etcdserver: invalid sort option",
		},
		{
			name: "delete-empty-key",
			op: &etcdserverpb.RequestOp{Request: &etcdserverpb.RequestOp_RequestDeleteRange{
				RequestDeleteRange: &etcdserverpb.DeleteRangeRequest{RangeEnd: []byte{0}},
			}},
			wantErr: rpctypes.ErrGRPCEmptyKey, wantCode: codes.InvalidArgument, wantMessage: "etcdserver: key is not provided",
		},
		{
			name:    "empty-operation",
			op:      &etcdserverpb.RequestOp{},
			wantErr: rpctypes.ErrGRPCKeyNotFound, wantCode: codes.InvalidArgument, wantMessage: "etcdserver: key not found",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			resp, err := server.Txn(context.Background(), &etcdserverpb.TxnRequest{
				Success: []*etcdserverpb.RequestOp{tt.op},
			})
			require.Nil(t, resp)
			requireDirectKVError(t, err, tt.wantErr, tt.wantCode, tt.wantMessage)
		})
	}
}

func TestTxnSelectedOperationValidationOrderMatchesEtcd(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()

	missingLeasePut := &etcdserverpb.RequestOp{Request: &etcdserverpb.RequestOp_RequestPut{
		RequestPut: &etcdserverpb.PutRequest{
			Key:   []byte("/registry/generic-txn/validation-order/missing-lease"),
			Value: []byte("value"),
			Lease: 987654321,
		},
	}}
	futureRange := &etcdserverpb.RequestOp{Request: &etcdserverpb.RequestOp_RequestRange{
		RequestRange: &etcdserverpb.RangeRequest{
			Key:      []byte("/registry/generic-txn/validation-order/future"),
			Revision: math.MaxInt64,
		},
	}}

	_, err := server.Txn(context.Background(), &etcdserverpb.TxnRequest{
		Success: []*etcdserverpb.RequestOp{missingLeasePut, futureRange},
	})
	requireDirectKVError(t, err, rpctypes.ErrGRPCLeaseNotFound, codes.NotFound, "etcdserver: requested lease not found")

	_, err = server.Txn(context.Background(), &etcdserverpb.TxnRequest{
		Success: []*etcdserverpb.RequestOp{futureRange, missingLeasePut},
	})
	requireDirectKVError(t, err, rpctypes.ErrGRPCFutureRev, codes.OutOfRange, "etcdserver: mvcc: required revision is a future revision")
}

func TestTxnValidatesBothBranchesBeforeDuplicateKeys(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()

	key := []byte("/registry/generic-txn/validation-order")
	_, err := server.Txn(context.Background(), &etcdserverpb.TxnRequest{
		Success: []*etcdserverpb.RequestOp{
			{Request: &etcdserverpb.RequestOp_RequestPut{
				RequestPut: &etcdserverpb.PutRequest{Key: key, Value: []byte("v1")},
			}},
			{Request: &etcdserverpb.RequestOp_RequestPut{
				RequestPut: &etcdserverpb.PutRequest{Key: key, Value: []byte("v2")},
			}},
		},
		Failure: []*etcdserverpb.RequestOp{
			{Request: &etcdserverpb.RequestOp_RequestPut{
				RequestPut: &etcdserverpb.PutRequest{Value: []byte("invalid")},
			}},
		},
	})
	require.EqualError(t, err, "rpc error: code = InvalidArgument desc = etcdserver: key is not provided")
}

func TestTxnRejectsTooManyOpsLikeEtcd(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()

	ops := make([]*etcdserverpb.RequestOp, defaultMaxTxnOps+1)
	for i := range ops {
		ops[i] = &etcdserverpb.RequestOp{Request: &etcdserverpb.RequestOp_RequestRange{
			RequestRange: &etcdserverpb.RangeRequest{Key: []byte("/registry/generic-txn/too-many")},
		}}
	}

	_, err := server.Txn(context.Background(), &etcdserverpb.TxnRequest{Success: ops})
	requireDirectKVError(t, err, rpctypes.ErrGRPCTooManyOps, codes.InvalidArgument, "etcdserver: too many operations in txn request")

	compares := make([]*etcdserverpb.Compare, defaultMaxTxnOps+1)
	for i := range compares {
		compares[i] = &etcdserverpb.Compare{}
	}
	_, err = server.Txn(context.Background(), &etcdserverpb.TxnRequest{Compare: compares})
	requireDirectKVError(t, err, rpctypes.ErrGRPCTooManyOps, codes.InvalidArgument, "etcdserver: too many operations in txn request")

	emptyOps := make([]*etcdserverpb.RequestOp, defaultMaxTxnOps+1)
	for i := range emptyOps {
		emptyOps[i] = &etcdserverpb.RequestOp{}
	}
	_, err = server.Txn(context.Background(), &etcdserverpb.TxnRequest{Success: emptyOps})
	requireDirectKVError(t, err, rpctypes.ErrGRPCTooManyOps, codes.InvalidArgument, "etcdserver: too many operations in txn request")
}

func TestTxnHonorsConfiguredMaxOperations(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()
	server.SetRequestLimits(2, defaultMaxRequestBytes)
	op := func(key string) *etcdserverpb.RequestOp {
		return &etcdserverpb.RequestOp{Request: &etcdserverpb.RequestOp_RequestRange{
			RequestRange: &etcdserverpb.RangeRequest{Key: []byte(key)},
		}}
	}

	_, err := server.Txn(context.Background(), &etcdserverpb.TxnRequest{Success: []*etcdserverpb.RequestOp{
		op("/registry/limit/one"), op("/registry/limit/two"),
	}})
	require.NoError(t, err)
	_, err = server.Txn(context.Background(), &etcdserverpb.TxnRequest{Success: []*etcdserverpb.RequestOp{
		op("/registry/limit/one"), op("/registry/limit/two"), op("/registry/limit/three"),
	}})
	requireDirectKVError(t, err, rpctypes.ErrGRPCTooManyOps, codes.InvalidArgument, "etcdserver: too many operations in txn request")

	nested := &etcdserverpb.RequestOp{Request: &etcdserverpb.RequestOp_RequestTxn{
		RequestTxn: &etcdserverpb.TxnRequest{Success: []*etcdserverpb.RequestOp{op("/registry/limit/nested")}},
	}}
	_, err = server.Txn(context.Background(), &etcdserverpb.TxnRequest{Success: []*etcdserverpb.RequestOp{
		op("/registry/limit/outer"), nested,
	}})
	requireDirectKVError(t, err, rpctypes.ErrGRPCTooManyOps, codes.InvalidArgument, "etcdserver: too many operations in txn request")
}

func TestTxnRejectsTooManyNestedOpsLikeEtcd(t *testing.T) {
	ops := make([]*etcdserverpb.RequestOp, 0, defaultMaxTxnOps)
	for i := 0; i < defaultMaxTxnOps-1; i++ {
		ops = append(ops, &etcdserverpb.RequestOp{Request: &etcdserverpb.RequestOp_RequestRange{
			RequestRange: &etcdserverpb.RangeRequest{Key: []byte("/registry/generic-txn/outer")},
		}})
	}
	ops = append(ops, &etcdserverpb.RequestOp{Request: &etcdserverpb.RequestOp_RequestTxn{
		RequestTxn: &etcdserverpb.TxnRequest{
			Success: []*etcdserverpb.RequestOp{
				{Request: &etcdserverpb.RequestOp_RequestRange{
					RequestRange: &etcdserverpb.RangeRequest{Key: []byte("/registry/generic-txn/inner")},
				}},
			},
		},
	}})

	err := validateTxnRequest(&etcdserverpb.TxnRequest{
		Success: ops,
	})
	requireDirectKVError(t, err, rpctypes.ErrGRPCTooManyOps, codes.InvalidArgument, "etcdserver: too many operations in txn request")
}

func TestTxnOperationBudgetMatrixMatchesEtcd(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()

	rangeOp := func(suffix byte) *etcdserverpb.RequestOp {
		key := append([]byte("/registry/generic-txn/budget/"), suffix)
		return &etcdserverpb.RequestOp{Request: &etcdserverpb.RequestOp_RequestRange{
			RequestRange: &etcdserverpb.RangeRequest{Key: key},
		}}
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
			Key:         append([]byte("/registry/generic-txn/budget-cmp/"), byte(i)),
			Result:      etcdserverpb.Compare_EQUAL,
			Target:      etcdserverpb.Compare_VERSION,
			TargetUnion: &etcdserverpb.Compare_Version{Version: 0},
		}
	}

	for _, tc := range []struct {
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
			Compare: compares,
			Success: []*etcdserverpb.RequestOp{rangeOp(0)},
		}, wantSucceeded: true},
		{name: "unselected-failure-nested-over-budget", txn: &etcdserverpb.TxnRequest{
			Success: rangeOps(1),
			Failure: append(rangeOps(defaultMaxTxnOps-1), nested(rangeOps(1))),
		}, wantErr: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			resp, err := server.Txn(context.Background(), tc.txn)
			if tc.wantErr {
				require.Nil(t, resp)
				requireDirectKVError(t, err, rpctypes.ErrGRPCTooManyOps, codes.InvalidArgument, "etcdserver: too many operations in txn request")
				return
			}
			require.NoError(t, err)
			require.NotNil(t, resp)
			require.Equal(t, tc.wantSucceeded, resp.Succeeded)
		})
	}
}

func TestTxnRejectsDuplicatePutKeys(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()

	_, err := server.Txn(context.Background(), &etcdserverpb.TxnRequest{
		Success: []*etcdserverpb.RequestOp{
			{Request: &etcdserverpb.RequestOp_RequestPut{
				RequestPut: &etcdserverpb.PutRequest{Key: []byte("/registry/generic-txn/duplicate"), Value: []byte("v1")},
			}},
			{Request: &etcdserverpb.RequestOp_RequestPut{
				RequestPut: &etcdserverpb.PutRequest{Key: []byte("/registry/generic-txn/duplicate"), Value: []byte("v2")},
			}},
		},
	})
	requireDirectKVError(t, err, rpctypes.ErrGRPCDuplicateKey, codes.InvalidArgument, "etcdserver: duplicate key given in txn request")
}

func TestTxnRejectsPutOverlappingDeleteRange(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()

	_, err := server.Txn(context.Background(), &etcdserverpb.TxnRequest{
		Success: []*etcdserverpb.RequestOp{
			{Request: &etcdserverpb.RequestOp_RequestDeleteRange{
				RequestDeleteRange: &etcdserverpb.DeleteRangeRequest{
					Key:      []byte("/registry/generic-txn/overlap/"),
					RangeEnd: []byte("/registry/generic-txn/overlap0"),
				},
			}},
			{Request: &etcdserverpb.RequestOp_RequestPut{
				RequestPut: &etcdserverpb.PutRequest{Key: []byte("/registry/generic-txn/overlap/a"), Value: []byte("v1")},
			}},
		},
	})
	requireDirectKVError(t, err, rpctypes.ErrGRPCDuplicateKey, codes.InvalidArgument, "etcdserver: duplicate key given in txn request")
}

func TestTxnIntervalValidationMatchesEtcdFromKeySentinel(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()

	for _, tc := range []struct {
		name        string
		deleteFirst bool
		wantKey     bool
	}{
		{name: "delete then put", deleteFirst: true, wantKey: true},
		{name: "put then delete", wantKey: false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			prefix := "/registry/generic-txn/from-key/" + tc.name + "/"
			key := []byte(prefix + "z")
			deleteOp := &etcdserverpb.RequestOp{Request: &etcdserverpb.RequestOp_RequestDeleteRange{
				RequestDeleteRange: &etcdserverpb.DeleteRangeRequest{
					Key:      []byte(prefix + "m"),
					RangeEnd: []byte{0},
				},
			}}
			putOp := &etcdserverpb.RequestOp{Request: &etcdserverpb.RequestOp_RequestPut{
				RequestPut: &etcdserverpb.PutRequest{Key: key, Value: []byte("value")},
			}}
			ops := []*etcdserverpb.RequestOp{putOp, deleteOp}
			if tc.deleteFirst {
				ops = []*etcdserverpb.RequestOp{deleteOp, putOp}
			}

			resp, err := server.Txn(context.Background(), &etcdserverpb.TxnRequest{Success: ops})
			require.NoError(t, err)
			require.True(t, resp.Succeeded)
			stored, err := server.Range(context.Background(), &etcdserverpb.RangeRequest{Key: key})
			require.NoError(t, err)
			require.Equal(t, tc.wantKey, len(stored.Kvs) == 1)
		})
	}
}

func TestTxnIntervalExecutionMatchesEtcdEdgeRanges(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()

	putOp := func(key []byte) *etcdserverpb.RequestOp {
		return &etcdserverpb.RequestOp{Request: &etcdserverpb.RequestOp_RequestPut{
			RequestPut: &etcdserverpb.PutRequest{Key: key, Value: []byte("value")},
		}}
	}
	deleteOp := func(key, end []byte) *etcdserverpb.RequestOp {
		return &etcdserverpb.RequestOp{Request: &etcdserverpb.RequestOp_RequestDeleteRange{
			RequestDeleteRange: &etcdserverpb.DeleteRangeRequest{Key: key, RangeEnd: end},
		}}
	}

	for _, tc := range []struct {
		name    string
		ops     func(prefix string) ([]*etcdserverpb.RequestOp, []byte)
		wantKey bool
	}{
		{
			name: "from-key-delete-before-put-in-range",
			ops: func(prefix string) ([]*etcdserverpb.RequestOp, []byte) {
				key := []byte(prefix + "z")
				return []*etcdserverpb.RequestOp{
					deleteOp([]byte(prefix+"m"), []byte{0}),
					putOp(key),
				}, key
			},
			wantKey: true,
		},
		{
			name: "put-before-from-key-delete-in-range",
			ops: func(prefix string) ([]*etcdserverpb.RequestOp, []byte) {
				key := []byte(prefix + "z")
				return []*etcdserverpb.RequestOp{
					putOp(key),
					deleteOp([]byte(prefix+"m"), []byte{0}),
				}, key
			},
		},
		{
			name: "from-key-delete-with-put-before-range",
			ops: func(prefix string) ([]*etcdserverpb.RequestOp, []byte) {
				key := []byte(prefix + "a")
				return []*etcdserverpb.RequestOp{
					deleteOp([]byte(prefix+"m"), []byte{0}),
					putOp(key),
				}, key
			},
			wantKey: true,
		},
		{
			name: "empty-range-with-put-at-start",
			ops: func(prefix string) ([]*etcdserverpb.RequestOp, []byte) {
				key := []byte(prefix + "m")
				return []*etcdserverpb.RequestOp{
					deleteOp(key, key),
					putOp(key),
				}, key
			},
			wantKey: true,
		},
		{
			name: "reversed-range-with-put-at-start",
			ops: func(prefix string) ([]*etcdserverpb.RequestOp, []byte) {
				key := []byte(prefix + "z")
				return []*etcdserverpb.RequestOp{
					deleteOp(key, []byte(prefix+"m")),
					putOp(key),
				}, key
			},
			wantKey: true,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			prefix := string(bytes.Repeat([]byte{0xff}, 64)) + "/registry/generic-txn/edge-interval/" + tc.name + "/"
			ops, key := tc.ops(prefix)
			resp, err := server.Txn(context.Background(), &etcdserverpb.TxnRequest{Success: ops})
			require.NoError(t, err)
			require.True(t, resp.Succeeded)

			stored, err := server.Range(context.Background(), &etcdserverpb.RangeRequest{Key: key})
			require.NoError(t, err)
			require.Equal(t, tc.wantKey, len(stored.Kvs) == 1)
		})
	}
}

func TestTxnFromKeyExecutionHighPrefixMatchesEtcd(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()

	ctx := context.Background()
	putOp := func(key []byte, value string) *etcdserverpb.RequestOp {
		return &etcdserverpb.RequestOp{Request: &etcdserverpb.RequestOp_RequestPut{
			RequestPut: &etcdserverpb.PutRequest{Key: key, Value: []byte(value)},
		}}
	}
	deleteOp := func(key []byte) *etcdserverpb.RequestOp {
		return &etcdserverpb.RequestOp{Request: &etcdserverpb.RequestOp_RequestDeleteRange{
			RequestDeleteRange: &etcdserverpb.DeleteRangeRequest{Key: key, RangeEnd: []byte{0}, PrevKv: true},
		}}
	}
	rangeOp := func(prefix, end []byte) *etcdserverpb.RequestOp {
		return &etcdserverpb.RequestOp{Request: &etcdserverpb.RequestOp_RequestRange{
			RequestRange: &etcdserverpb.RangeRequest{Key: prefix, RangeEnd: end},
		}}
	}

	for _, tc := range []struct {
		name          string
		putFirst      bool
		wantDeleted   int64
		wantPrev      []string
		wantTxnRange  []string
		wantFinal     []string
		wantTxnKeyIn  map[string]struct{}
		deleteOpIndex int
	}{
		{
			name: "put-then-delete", putFirst: true, deleteOpIndex: 1,
			wantDeleted: 3, wantPrev: []string{"b", "c", "d"}, wantTxnRange: []string{"a"}, wantFinal: []string{"a"},
			wantTxnKeyIn: map[string]struct{}{"d": {}},
		},
		{
			name:        "delete-then-put",
			wantDeleted: 2, wantPrev: []string{"b", "c"}, wantTxnRange: []string{"a", "d"}, wantFinal: []string{"a", "d"},
			wantTxnKeyIn: map[string]struct{}{"d": {}},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			prefix := append(bytes.Repeat([]byte{0xff}, 64), []byte("/registry/generic-txn/from-key-exec/"+tc.name+"/")...)
			end := prefixEnd(prefix)
			t.Cleanup(func() {
				_, _ = server.DeleteRange(context.Background(), &etcdserverpb.DeleteRangeRequest{Key: prefix, RangeEnd: end})
			})
			for _, suffix := range []string{"a", "b", "c"} {
				_, err := server.Put(ctx, &etcdserverpb.PutRequest{
					Key: append(append([]byte{}, prefix...), suffix...), Value: []byte("seed-" + suffix),
				})
				require.NoError(t, err)
			}

			put := putOp(append(append([]byte{}, prefix...), 'd'), "txn-d")
			del := deleteOp(append(append([]byte{}, prefix...), 'b'))
			ops := []*etcdserverpb.RequestOp{del, put}
			if tc.putFirst {
				ops = []*etcdserverpb.RequestOp{put, del}
			}
			ops = append(ops, rangeOp(prefix, end))

			resp, err := server.Txn(ctx, &etcdserverpb.TxnRequest{Success: ops})
			require.NoError(t, err)
			require.True(t, resp.Succeeded)
			require.Len(t, resp.Responses, 3)
			deleted := resp.Responses[tc.deleteOpIndex].GetResponseDeleteRange()
			require.NotNil(t, deleted)
			require.Equal(t, resp.Header.Revision, deleted.Header.Revision)
			require.Equal(t, tc.wantDeleted, deleted.Deleted)
			requireTxnFromKeySuffixes(t, prefix, deleted.PrevKvs, tc.wantPrev, resp.Header.Revision, tc.wantTxnKeyIn)

			txnRange := resp.Responses[2].GetResponseRange()
			require.NotNil(t, txnRange)
			require.Equal(t, resp.Header.Revision, txnRange.Header.Revision)
			requireTxnFromKeySuffixes(t, prefix, txnRange.Kvs, tc.wantTxnRange, resp.Header.Revision, tc.wantTxnKeyIn)

			final, err := server.Range(ctx, &etcdserverpb.RangeRequest{Key: prefix, RangeEnd: end})
			require.NoError(t, err)
			require.GreaterOrEqual(t, final.Header.Revision, resp.Header.Revision)
			requireTxnFromKeySuffixes(t, prefix, final.Kvs, tc.wantFinal, resp.Header.Revision, tc.wantTxnKeyIn)
		})
	}
}

func requireTxnFromKeySuffixes(
	t *testing.T,
	prefix []byte,
	kvs []*mvccpb.KeyValue,
	wantSuffixes []string,
	txnRevision int64,
	wantTxnKeys map[string]struct{},
) {
	t.Helper()
	require.Len(t, kvs, len(wantSuffixes))
	for i, suffix := range wantSuffixes {
		kv := kvs[i]
		require.Equal(t, []byte(suffix), bytes.TrimPrefix(kv.Key, prefix))
		if _, ok := wantTxnKeys[suffix]; ok {
			require.Equal(t, []byte("txn-"+suffix), kv.Value)
			require.Equal(t, txnRevision, kv.CreateRevision)
			require.Equal(t, txnRevision, kv.ModRevision)
		} else {
			require.Equal(t, []byte("seed-"+suffix), kv.Value)
			require.NotEqual(t, txnRevision, kv.CreateRevision)
			require.NotEqual(t, txnRevision, kv.ModRevision)
		}
		require.Equal(t, int64(1), kv.Version)
	}
}

func TestTxnDuplicateIntervalValidationMatrixMatchesEtcd(t *testing.T) {
	prefix := "/registry/generic-txn/duplicate-interval/"
	key := []byte(prefix + "abc")
	put := &etcdserverpb.RequestOp{Request: &etcdserverpb.RequestOp_RequestPut{
		RequestPut: &etcdserverpb.PutRequest{Key: key, Value: []byte("value")},
	}}
	deleteKey := &etcdserverpb.RequestOp{Request: &etcdserverpb.RequestOp_RequestDeleteRange{
		RequestDeleteRange: &etcdserverpb.DeleteRangeRequest{Key: key},
	}}
	deleteContaining := &etcdserverpb.RequestOp{Request: &etcdserverpb.RequestOp_RequestDeleteRange{
		RequestDeleteRange: &etcdserverpb.DeleteRangeRequest{Key: []byte(prefix + "a"), RangeEnd: []byte(prefix + "b")},
	}}
	deleteBefore := &etcdserverpb.RequestOp{Request: &etcdserverpb.RequestOp_RequestDeleteRange{
		RequestDeleteRange: &etcdserverpb.DeleteRangeRequest{Key: []byte(prefix + "abb"), RangeEnd: key},
	}}
	txnOp := func(success, failure []*etcdserverpb.RequestOp) *etcdserverpb.RequestOp {
		return &etcdserverpb.RequestOp{Request: &etcdserverpb.RequestOp_RequestTxn{
			RequestTxn: &etcdserverpb.TxnRequest{Success: success, Failure: failure},
		}}
	}
	nestedDelete := txnOp([]*etcdserverpb.RequestOp{deleteContaining}, nil)
	nestedDeleteBoth := txnOp(
		[]*etcdserverpb.RequestOp{deleteContaining},
		[]*etcdserverpb.RequestOp{deleteContaining},
	)
	nestedPut := txnOp([]*etcdserverpb.RequestOp{put}, nil)
	nestedPutBoth := txnOp(
		[]*etcdserverpb.RequestOp{put},
		[]*etcdserverpb.RequestOp{put},
	)

	for _, tc := range []struct {
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
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := validateTxnRequest(&etcdserverpb.TxnRequest{Success: tc.ops})
			if tc.wantErr {
				requireDirectKVError(t, err, rpctypes.ErrGRPCDuplicateKey, codes.InvalidArgument, "etcdserver: duplicate key given in txn request")
				return
			}
			require.NoError(t, err)
		})
	}
}

func TestTxnAllowsSameKeyInDifferentBranches(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()

	key := []byte("/registry/generic-txn/branch-put")
	resp, err := server.Txn(context.Background(), &etcdserverpb.TxnRequest{
		Compare: []*etcdserverpb.Compare{
			{
				Key:         key,
				Target:      etcdserverpb.Compare_MOD,
				Result:      etcdserverpb.Compare_EQUAL,
				TargetUnion: &etcdserverpb.Compare_ModRevision{ModRevision: 0},
			},
		},
		Success: []*etcdserverpb.RequestOp{
			{Request: &etcdserverpb.RequestOp_RequestPut{
				RequestPut: &etcdserverpb.PutRequest{Key: key, Value: []byte("success")},
			}},
		},
		Failure: []*etcdserverpb.RequestOp{
			{Request: &etcdserverpb.RequestOp_RequestPut{
				RequestPut: &etcdserverpb.PutRequest{Key: key, Value: []byte("failure")},
			}},
		},
	})
	require.NoError(t, err)
	require.True(t, resp.Succeeded)

	rangeResp, err := server.Range(context.Background(), &etcdserverpb.RangeRequest{Key: key})
	require.NoError(t, err)
	require.Len(t, rangeResp.Kvs, 1)
	require.Equal(t, []byte("success"), rangeResp.Kvs[0].Value)
}

func TestTxnIntervalValidationRecursesIntoNestedTxn(t *testing.T) {
	key := []byte("/registry/generic-txn/nested/a")

	err := validateTxnRequest(&etcdserverpb.TxnRequest{
		Success: []*etcdserverpb.RequestOp{
			{Request: &etcdserverpb.RequestOp_RequestDeleteRange{
				RequestDeleteRange: &etcdserverpb.DeleteRangeRequest{
					Key:      []byte("/registry/generic-txn/nested/"),
					RangeEnd: []byte("/registry/generic-txn/nested0"),
				},
			}},
			{Request: &etcdserverpb.RequestOp_RequestTxn{
				RequestTxn: &etcdserverpb.TxnRequest{
					Success: []*etcdserverpb.RequestOp{
						{Request: &etcdserverpb.RequestOp_RequestPut{
							RequestPut: &etcdserverpb.PutRequest{Key: key, Value: []byte("v1")},
						}},
					},
				},
			}},
		},
	})
	requireDirectKVError(t, err, rpctypes.ErrGRPCDuplicateKey, codes.InvalidArgument, "etcdserver: duplicate key given in txn request")
}

func TestTxnIntervalValidationAllowsNestedThenElseSameKey(t *testing.T) {
	key := []byte("/registry/generic-txn/nested-branch/a")

	err := validateTxnRequest(&etcdserverpb.TxnRequest{
		Success: []*etcdserverpb.RequestOp{
			{Request: &etcdserverpb.RequestOp_RequestTxn{
				RequestTxn: &etcdserverpb.TxnRequest{
					Success: []*etcdserverpb.RequestOp{
						{Request: &etcdserverpb.RequestOp_RequestPut{
							RequestPut: &etcdserverpb.PutRequest{Key: key, Value: []byte("then")},
						}},
					},
					Failure: []*etcdserverpb.RequestOp{
						{Request: &etcdserverpb.RequestOp_RequestPut{
							RequestPut: &etcdserverpb.PutRequest{Key: key, Value: []byte("else")},
						}},
					},
				},
			}},
		},
	})
	require.NoError(t, err)
}

func TestTxnIntervalValidationRejectsParentPutAndNestedDeleteInEitherOrder(t *testing.T) {
	key := []byte("/registry/generic-txn/nested-delete/a")
	put := &etcdserverpb.RequestOp{Request: &etcdserverpb.RequestOp_RequestPut{
		RequestPut: &etcdserverpb.PutRequest{Key: key, Value: []byte("value")},
	}}
	nestedDelete := &etcdserverpb.RequestOp{Request: &etcdserverpb.RequestOp_RequestTxn{
		RequestTxn: &etcdserverpb.TxnRequest{
			Success: []*etcdserverpb.RequestOp{
				{Request: &etcdserverpb.RequestOp_RequestDeleteRange{
					RequestDeleteRange: &etcdserverpb.DeleteRangeRequest{
						Key:      []byte("/registry/generic-txn/nested-delete/"),
						RangeEnd: []byte("/registry/generic-txn/nested-delete0"),
					},
				}},
			},
		},
	}}

	for _, ops := range [][]*etcdserverpb.RequestOp{
		{put, nestedDelete},
		{nestedDelete, put},
	} {
		err := validateTxnRequest(&etcdserverpb.TxnRequest{Success: ops})
		requireDirectKVError(t, err, rpctypes.ErrGRPCDuplicateKey, codes.InvalidArgument, "etcdserver: duplicate key given in txn request")
	}
}

func TestTxnIntervalValidationRejectsDuplicateNestedSiblingPuts(t *testing.T) {
	key := []byte("/registry/generic-txn/nested-sibling/a")

	err := validateTxnRequest(&etcdserverpb.TxnRequest{
		Success: []*etcdserverpb.RequestOp{
			{Request: &etcdserverpb.RequestOp_RequestTxn{
				RequestTxn: &etcdserverpb.TxnRequest{
					Success: []*etcdserverpb.RequestOp{
						{Request: &etcdserverpb.RequestOp_RequestPut{
							RequestPut: &etcdserverpb.PutRequest{Key: key, Value: []byte("v1")},
						}},
					},
				},
			}},
			{Request: &etcdserverpb.RequestOp_RequestTxn{
				RequestTxn: &etcdserverpb.TxnRequest{
					Success: []*etcdserverpb.RequestOp{
						{Request: &etcdserverpb.RequestOp_RequestPut{
							RequestPut: &etcdserverpb.PutRequest{Key: key, Value: []byte("v2")},
						}},
					},
				},
			}},
		},
	})
	requireDirectKVError(t, err, rpctypes.ErrGRPCDuplicateKey, codes.InvalidArgument, "etcdserver: duplicate key given in txn request")
}

func TestTxnRangeCompareModRevisionRequiresAllKeysToMatch(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()

	ctx := context.Background()
	prefix := "/registry/generic-txn/range-compare-mod/"
	putA, err := server.Put(ctx, &etcdserverpb.PutRequest{Key: []byte(prefix + "a"), Value: []byte("v1")})
	require.NoError(t, err)
	putB, err := server.Put(ctx, &etcdserverpb.PutRequest{Key: []byte(prefix + "b"), Value: []byte("v2")})
	require.NoError(t, err)

	var resp *etcdserverpb.TxnResponse
	require.Eventually(t, func() bool {
		var err error
		resp, err = server.Txn(ctx, &etcdserverpb.TxnRequest{
			Compare: []*etcdserverpb.Compare{{
				Key:         []byte(prefix),
				RangeEnd:    []byte("/registry/generic-txn/range-compare-mod0"),
				Target:      etcdserverpb.Compare_MOD,
				Result:      etcdserverpb.Compare_GREATER,
				TargetUnion: &etcdserverpb.Compare_ModRevision{ModRevision: putA.Header.Revision - 1},
			}},
			Success: []*etcdserverpb.RequestOp{
				{Request: &etcdserverpb.RequestOp_RequestRange{
					RequestRange: &etcdserverpb.RangeRequest{Key: []byte(prefix), RangeEnd: []byte("/registry/generic-txn/range-compare-mod0")},
				}},
			},
		})
		return err == nil && resp.Succeeded && len(resp.Responses[0].GetResponseRange().Kvs) == 2
	}, time.Second, 10*time.Millisecond)

	resp, err = server.Txn(ctx, &etcdserverpb.TxnRequest{
		Compare: []*etcdserverpb.Compare{{
			Key:         []byte(prefix),
			RangeEnd:    []byte("/registry/generic-txn/range-compare-mod0"),
			Target:      etcdserverpb.Compare_MOD,
			Result:      etcdserverpb.Compare_GREATER,
			TargetUnion: &etcdserverpb.Compare_ModRevision{ModRevision: putB.Header.Revision},
		}},
		Success: []*etcdserverpb.RequestOp{
			{Request: &etcdserverpb.RequestOp_RequestPut{
				RequestPut: &etcdserverpb.PutRequest{Key: []byte(prefix + "c"), Value: []byte("should-not-write")},
			}},
		},
		Failure: []*etcdserverpb.RequestOp{
			{Request: &etcdserverpb.RequestOp_RequestRange{
				RequestRange: &etcdserverpb.RangeRequest{Key: []byte(prefix), RangeEnd: []byte("/registry/generic-txn/range-compare-mod0")},
			}},
		},
	})
	require.NoError(t, err)
	require.False(t, resp.Succeeded)
	require.Len(t, resp.Responses[0].GetResponseRange().Kvs, 2)
}

func TestTxnRangeCompareCreateRevisionRequiresAllKeysToMatch(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()

	ctx := context.Background()
	prefix := "/registry/generic-txn/range-compare-create/"
	end := []byte("/registry/generic-txn/range-compare-create0")
	putA, err := server.Put(ctx, &etcdserverpb.PutRequest{Key: []byte(prefix + "a"), Value: []byte("v1")})
	require.NoError(t, err)
	putB, err := server.Put(ctx, &etcdserverpb.PutRequest{Key: []byte(prefix + "b"), Value: []byte("v2")})
	require.NoError(t, err)

	resp, err := server.Txn(ctx, &etcdserverpb.TxnRequest{
		Compare: []*etcdserverpb.Compare{{
			Key:         []byte(prefix),
			RangeEnd:    end,
			Target:      etcdserverpb.Compare_CREATE,
			Result:      etcdserverpb.Compare_EQUAL,
			TargetUnion: &etcdserverpb.Compare_CreateRevision{CreateRevision: putA.Header.Revision},
		}},
		Success: []*etcdserverpb.RequestOp{{Request: &etcdserverpb.RequestOp_RequestPut{
			RequestPut: &etcdserverpb.PutRequest{Key: []byte(prefix + "should-not-write"), Value: []byte("bad")},
		}}},
		Failure: []*etcdserverpb.RequestOp{{Request: &etcdserverpb.RequestOp_RequestRange{
			RequestRange: &etcdserverpb.RangeRequest{Key: []byte(prefix), RangeEnd: end},
		}}},
	})
	require.NoError(t, err)
	require.False(t, resp.Succeeded)
	require.Len(t, resp.Responses[0].GetResponseRange().Kvs, 2)

	resp, err = server.Txn(ctx, &etcdserverpb.TxnRequest{
		Compare: []*etcdserverpb.Compare{{
			Key:         []byte(prefix),
			RangeEnd:    end,
			Target:      etcdserverpb.Compare_CREATE,
			Result:      etcdserverpb.Compare_LESS,
			TargetUnion: &etcdserverpb.Compare_CreateRevision{CreateRevision: putB.Header.Revision + 1},
		}},
		Success: []*etcdserverpb.RequestOp{{Request: &etcdserverpb.RequestOp_RequestRange{
			RequestRange: &etcdserverpb.RangeRequest{Key: []byte(prefix), RangeEnd: end},
		}}},
	})
	require.NoError(t, err)
	require.True(t, resp.Succeeded)
	require.Len(t, resp.Responses[0].GetResponseRange().Kvs, 2)
}

func TestTxnRangeCompareLeaseRequiresAllKeysToMatch(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()

	ctx := context.Background()
	prefix := "/registry/generic-txn/range-compare-lease/"
	end := []byte("/registry/generic-txn/range-compare-lease0")
	leaseResp, err := server.LeaseGrant(ctx, &etcdserverpb.LeaseGrantRequest{TTL: 30})
	require.NoError(t, err)
	for _, suffix := range []string{"a", "b"} {
		_, err = server.Put(ctx, &etcdserverpb.PutRequest{
			Key: []byte(prefix + suffix), Value: []byte("leased"), Lease: leaseResp.ID,
		})
		require.NoError(t, err)
	}
	_, err = server.Put(ctx, &etcdserverpb.PutRequest{Key: []byte(prefix + "c"), Value: []byte("unleased")})
	require.NoError(t, err)

	resp, err := server.Txn(ctx, &etcdserverpb.TxnRequest{
		Compare: []*etcdserverpb.Compare{{
			Key:         []byte(prefix),
			RangeEnd:    end,
			Target:      etcdserverpb.Compare_LEASE,
			Result:      etcdserverpb.Compare_EQUAL,
			TargetUnion: &etcdserverpb.Compare_Lease{Lease: leaseResp.ID},
		}},
		Success: []*etcdserverpb.RequestOp{{Request: &etcdserverpb.RequestOp_RequestPut{
			RequestPut: &etcdserverpb.PutRequest{Key: []byte(prefix + "should-not-write"), Value: []byte("bad")},
		}}},
		Failure: []*etcdserverpb.RequestOp{{Request: &etcdserverpb.RequestOp_RequestRange{
			RequestRange: &etcdserverpb.RangeRequest{Key: []byte(prefix), RangeEnd: end},
		}}},
	})
	require.NoError(t, err)
	require.False(t, resp.Succeeded)
	rangeResp := resp.Responses[0].GetResponseRange()
	require.NotNil(t, rangeResp)
	require.NotNil(t, rangeResp.Header)
	require.Equal(t, resp.Header.Revision, rangeResp.Header.Revision)
	require.Len(t, rangeResp.Kvs, 3)

	resp, err = server.Txn(ctx, &etcdserverpb.TxnRequest{
		Compare: []*etcdserverpb.Compare{{
			Key:         []byte(prefix),
			RangeEnd:    []byte(prefix + "c"),
			Target:      etcdserverpb.Compare_LEASE,
			Result:      etcdserverpb.Compare_EQUAL,
			TargetUnion: &etcdserverpb.Compare_Lease{Lease: leaseResp.ID},
		}},
		Success: []*etcdserverpb.RequestOp{{Request: &etcdserverpb.RequestOp_RequestRange{
			RequestRange: &etcdserverpb.RangeRequest{Key: []byte(prefix), RangeEnd: []byte(prefix + "c")},
		}}},
	})
	require.NoError(t, err)
	require.True(t, resp.Succeeded)
	rangeResp = resp.Responses[0].GetResponseRange()
	require.NotNil(t, rangeResp)
	require.NotNil(t, rangeResp.Header)
	require.Equal(t, resp.Header.Revision, rangeResp.Header.Revision)
	require.Len(t, rangeResp.Kvs, 2)
}

func TestTxnRangeCompareFromKeySentinelMatchesEtcd(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()

	ctx := context.Background()
	prefix := "/registry/generic-txn/range-compare-from-key/"
	for _, suffix := range []string{"a", "b", "c"} {
		_, err := server.Put(ctx, &etcdserverpb.PutRequest{
			Key: []byte(prefix + suffix), Value: []byte("v-" + suffix),
		})
		require.NoError(t, err)
	}

	resp, err := server.Txn(ctx, &etcdserverpb.TxnRequest{
		Compare: []*etcdserverpb.Compare{{
			Key:         []byte(prefix + "b"),
			RangeEnd:    []byte{0},
			Target:      etcdserverpb.Compare_VERSION,
			Result:      etcdserverpb.Compare_GREATER,
			TargetUnion: &etcdserverpb.Compare_Version{Version: 0},
		}},
		Success: []*etcdserverpb.RequestOp{{Request: &etcdserverpb.RequestOp_RequestRange{
			RequestRange: &etcdserverpb.RangeRequest{Key: []byte(prefix), RangeEnd: []byte(prefix + "d")},
		}}},
	})
	require.NoError(t, err)
	require.True(t, resp.Succeeded)
	rangeResp := resp.Responses[0].GetResponseRange()
	require.NotNil(t, rangeResp)
	require.NotNil(t, rangeResp.Header)
	require.Equal(t, resp.Header.Revision, rangeResp.Header.Revision)
	require.Len(t, rangeResp.Kvs, 3)

	resp, err = server.Txn(ctx, &etcdserverpb.TxnRequest{
		Compare: []*etcdserverpb.Compare{{
			Key:         []byte(prefix + "b"),
			RangeEnd:    []byte{0},
			Target:      etcdserverpb.Compare_VALUE,
			Result:      etcdserverpb.Compare_EQUAL,
			TargetUnion: &etcdserverpb.Compare_Value{Value: []byte("v-b")},
		}},
		Success: []*etcdserverpb.RequestOp{{Request: &etcdserverpb.RequestOp_RequestPut{
			RequestPut: &etcdserverpb.PutRequest{Key: []byte(prefix + "should-not-write"), Value: []byte("bad")},
		}}},
		Failure: []*etcdserverpb.RequestOp{{Request: &etcdserverpb.RequestOp_RequestRange{
			RequestRange: &etcdserverpb.RangeRequest{Key: []byte(prefix), RangeEnd: []byte(prefix + "d")},
		}}},
	})
	require.NoError(t, err)
	require.False(t, resp.Succeeded)
	rangeResp = resp.Responses[0].GetResponseRange()
	require.NotNil(t, rangeResp)
	require.NotNil(t, rangeResp.Header)
	require.Equal(t, resp.Header.Revision, rangeResp.Header.Revision)
	require.Len(t, rangeResp.Kvs, 3)

	resp, err = server.Txn(ctx, &etcdserverpb.TxnRequest{
		Compare: []*etcdserverpb.Compare{{
			Key:         []byte(prefix + "z"),
			RangeEnd:    []byte{0},
			Target:      etcdserverpb.Compare_VERSION,
			Result:      etcdserverpb.Compare_EQUAL,
			TargetUnion: &etcdserverpb.Compare_Version{Version: 0},
		}},
		Success: []*etcdserverpb.RequestOp{{Request: &etcdserverpb.RequestOp_RequestPut{
			RequestPut: &etcdserverpb.PutRequest{Key: []byte(prefix + "empty-version-ok"), Value: []byte("ok")},
		}}},
		Failure: []*etcdserverpb.RequestOp{{Request: &etcdserverpb.RequestOp_RequestPut{
			RequestPut: &etcdserverpb.PutRequest{Key: []byte(prefix + "empty-version-fail"), Value: []byte("bad")},
		}}},
	})
	require.NoError(t, err)
	require.True(t, resp.Succeeded)

	marker, err := server.Range(ctx, &etcdserverpb.RangeRequest{Key: []byte(prefix + "empty-version-ok")})
	require.NoError(t, err)
	require.Len(t, marker.Kvs, 1)

	resp, err = server.Txn(ctx, &etcdserverpb.TxnRequest{
		Compare: []*etcdserverpb.Compare{{
			Key:         []byte(prefix + "z"),
			RangeEnd:    []byte{0},
			Target:      etcdserverpb.Compare_VALUE,
			Result:      etcdserverpb.Compare_NOT_EQUAL,
			TargetUnion: &etcdserverpb.Compare_Value{Value: []byte("anything")},
		}},
		Success: []*etcdserverpb.RequestOp{{Request: &etcdserverpb.RequestOp_RequestPut{
			RequestPut: &etcdserverpb.PutRequest{Key: []byte(prefix + "empty-value-bad"), Value: []byte("bad")},
		}}},
		Failure: []*etcdserverpb.RequestOp{{Request: &etcdserverpb.RequestOp_RequestRange{
			RequestRange: &etcdserverpb.RangeRequest{Key: []byte(prefix + "empty-value-bad")},
		}}},
	})
	require.NoError(t, err)
	require.False(t, resp.Succeeded)
	rangeResp = resp.Responses[0].GetResponseRange()
	require.NotNil(t, rangeResp)
	require.NotNil(t, rangeResp.Header)
	require.Equal(t, resp.Header.Revision, rangeResp.Header.Revision)
	require.Empty(t, rangeResp.Kvs)
}

func TestTxnRangeCompareValueFailsForEmptyRange(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()

	resp, err := server.Txn(context.Background(), &etcdserverpb.TxnRequest{
		Compare: []*etcdserverpb.Compare{{
			Key:         []byte("/registry/generic-txn/range-compare-empty/"),
			RangeEnd:    []byte("/registry/generic-txn/range-compare-empty0"),
			Target:      etcdserverpb.Compare_VALUE,
			Result:      etcdserverpb.Compare_EQUAL,
			TargetUnion: &etcdserverpb.Compare_Value{Value: []byte{}},
		}},
		Success: []*etcdserverpb.RequestOp{
			{Request: &etcdserverpb.RequestOp_RequestPut{
				RequestPut: &etcdserverpb.PutRequest{Key: []byte("/registry/generic-txn/range-compare-empty/result"), Value: []byte("should-not-write")},
			}},
		},
		Failure: []*etcdserverpb.RequestOp{
			{Request: &etcdserverpb.RequestOp_RequestRange{
				RequestRange: &etcdserverpb.RangeRequest{Key: []byte("/registry/generic-txn/range-compare-empty/result")},
			}},
		},
	})
	require.NoError(t, err)
	require.False(t, resp.Succeeded)
	rangeResp := resp.Responses[0].GetResponseRange()
	require.NotNil(t, rangeResp)
	require.NotNil(t, rangeResp.Header)
	require.Equal(t, resp.Header.Revision, rangeResp.Header.Revision)
	require.Empty(t, rangeResp.Kvs)
}

func TestTxnRangeCompareReverseEmptyRangeMatchesEtcd(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()

	for _, test := range []struct {
		name      string
		compare   *etcdserverpb.Compare
		succeeded bool
	}{
		{
			name: "create revision uses absent zero value",
			compare: &etcdserverpb.Compare{
				Key:         []byte("owners/data"),
				RangeEnd:    []byte("owners/\x00"),
				Target:      etcdserverpb.Compare_CREATE,
				Result:      etcdserverpb.Compare_LESS,
				TargetUnion: &etcdserverpb.Compare_CreateRevision{CreateRevision: 1},
			},
			succeeded: true,
		},
		{
			name: "value compare fails for empty range",
			compare: &etcdserverpb.Compare{
				Key:         []byte("owners/data"),
				RangeEnd:    []byte("owners/\x00"),
				Target:      etcdserverpb.Compare_VALUE,
				Result:      etcdserverpb.Compare_EQUAL,
				TargetUnion: &etcdserverpb.Compare_Value{Value: []byte{}},
			},
			succeeded: false,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			resp, err := server.Txn(context.Background(), &etcdserverpb.TxnRequest{
				Compare: []*etcdserverpb.Compare{test.compare},
			})
			require.NoError(t, err)
			require.Equal(t, test.succeeded, resp.Succeeded)
		})
	}
}

func TestTxnHasRangeCompareRecursesIntoNestedBranches(t *testing.T) {
	point := &etcdserverpb.TxnRequest{Compare: []*etcdserverpb.Compare{{Key: []byte("point")}}}
	require.False(t, txnHasRangeCompare(point))

	nested := &etcdserverpb.TxnRequest{
		Failure: []*etcdserverpb.RequestOp{{
			Request: &etcdserverpb.RequestOp_RequestTxn{RequestTxn: &etcdserverpb.TxnRequest{
				Success: []*etcdserverpb.RequestOp{{
					Request: &etcdserverpb.RequestOp_RequestTxn{RequestTxn: &etcdserverpb.TxnRequest{
						Compare: []*etcdserverpb.Compare{{
							Key:      []byte("prefix/"),
							RangeEnd: []byte("prefix0"),
						}},
					}},
				}},
			}},
		}},
	}
	require.True(t, txnHasRangeCompare(nested))
}

type blockingRangeCompareShim struct {
	BackendShim
	scanned chan struct{}
	release chan struct{}
	once    sync.Once
}

func (b *blockingRangeCompareShim) List(ctx context.Context, r *etcdserverpb.RangeRequest) (*etcdserverpb.RangeResponse, error) {
	resp, err := b.BackendShim.List(ctx, r)
	if err == nil && len(r.RangeEnd) != 0 {
		b.once.Do(func() {
			close(b.scanned)
			select {
			case <-b.release:
			case <-ctx.Done():
			}
		})
	}
	return resp, err
}

func TestTxnRangeCompareExcludesPhantomInsert(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()

	shim := &blockingRangeCompareShim{
		BackendShim: server.backend,
		scanned:     make(chan struct{}),
		release:     make(chan struct{}),
	}
	server.backend = shim

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	prefix := "/registry/generic-txn/phantom/"
	txnDone := make(chan *etcdserverpb.TxnResponse, 1)
	txnErr := make(chan error, 1)
	go func() {
		resp, err := server.Txn(ctx, &etcdserverpb.TxnRequest{
			Compare: []*etcdserverpb.Compare{{
				Key:         []byte(prefix),
				RangeEnd:    []byte("/registry/generic-txn/phantom0"),
				Target:      etcdserverpb.Compare_VERSION,
				Result:      etcdserverpb.Compare_EQUAL,
				TargetUnion: &etcdserverpb.Compare_Version{Version: 0},
			}},
			Success: []*etcdserverpb.RequestOp{{
				Request: &etcdserverpb.RequestOp_RequestPut{RequestPut: &etcdserverpb.PutRequest{
					Key: []byte("/registry/generic-txn/phantom-result"), Value: []byte("success"),
				}},
			}},
		})
		txnDone <- resp
		txnErr <- err
	}()

	select {
	case <-shim.scanned:
	case <-ctx.Done():
		require.FailNow(t, "range compare did not reach scan barrier")
	}

	putDone := make(chan error, 1)
	go func() {
		_, err := server.Put(ctx, &etcdserverpb.PutRequest{
			Key: []byte(prefix + "inserted"), Value: []byte("phantom"),
		})
		putDone <- err
	}()
	select {
	case err := <-putDone:
		require.FailNow(t, "phantom insert completed before range transaction", "err=%v", err)
	case <-time.After(50 * time.Millisecond):
	}

	close(shim.release)
	resp := <-txnDone
	require.NoError(t, <-txnErr)
	require.NotNil(t, resp)
	require.True(t, resp.Succeeded)
	require.NoError(t, <-putDone)
}

func TestTxnRangeCompareLeasedPutDoesNotDeadlock(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	lease, err := server.LeaseGrant(ctx, &etcdserverpb.LeaseGrantRequest{TTL: 30})
	require.NoError(t, err)

	prefix := []byte("/registry/generic-txn/range-lease/")
	key := append(append([]byte(nil), prefix...), []byte("key")...)
	resp, err := server.Txn(ctx, &etcdserverpb.TxnRequest{
		Compare: []*etcdserverpb.Compare{{
			Key:         prefix,
			RangeEnd:    []byte("/registry/generic-txn/range-lease0"),
			Target:      etcdserverpb.Compare_VERSION,
			Result:      etcdserverpb.Compare_EQUAL,
			TargetUnion: &etcdserverpb.Compare_Version{Version: 0},
		}},
		Success: []*etcdserverpb.RequestOp{{
			Request: &etcdserverpb.RequestOp_RequestPut{RequestPut: &etcdserverpb.PutRequest{
				Key: key, Value: []byte("leased"), Lease: lease.ID,
			}},
		}},
	})
	require.NoError(t, err)
	require.True(t, resp.Succeeded)

	ttl, err := server.LeaseTimeToLive(ctx, &etcdserverpb.LeaseTimeToLiveRequest{ID: lease.ID, Keys: true})
	require.NoError(t, err)
	require.Equal(t, [][]byte{key}, ttl.Keys)
}

func TestTxnCompareCreateRevisionZeroChecksExistence(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()

	ctx := context.Background()
	key := []byte("/registry/generic-txn/compare-create-zero")

	resp, err := server.Txn(ctx, &etcdserverpb.TxnRequest{
		Compare: []*etcdserverpb.Compare{{
			Key:         key,
			Target:      etcdserverpb.Compare_CREATE,
			Result:      etcdserverpb.Compare_EQUAL,
			TargetUnion: &etcdserverpb.Compare_CreateRevision{CreateRevision: 0},
		}},
		Success: []*etcdserverpb.RequestOp{
			{Request: &etcdserverpb.RequestOp_RequestPut{
				RequestPut: &etcdserverpb.PutRequest{Key: key, Value: []byte("created")},
			}},
		},
	})
	require.NoError(t, err)
	require.True(t, resp.Succeeded)

	resp, err = server.Txn(ctx, &etcdserverpb.TxnRequest{
		Compare: []*etcdserverpb.Compare{{
			Key:         key,
			Target:      etcdserverpb.Compare_CREATE,
			Result:      etcdserverpb.Compare_EQUAL,
			TargetUnion: &etcdserverpb.Compare_CreateRevision{CreateRevision: 0},
		}},
		Success: []*etcdserverpb.RequestOp{
			{Request: &etcdserverpb.RequestOp_RequestPut{
				RequestPut: &etcdserverpb.PutRequest{Key: key, Value: []byte("should-not-write")},
			}},
		},
		Failure: []*etcdserverpb.RequestOp{
			{Request: &etcdserverpb.RequestOp_RequestRange{
				RequestRange: &etcdserverpb.RangeRequest{Key: key},
			}},
		},
	})
	require.NoError(t, err)
	require.False(t, resp.Succeeded)
	require.Len(t, resp.Responses, 1)
	rangeResp := resp.Responses[0].GetResponseRange()
	require.NotNil(t, rangeResp)
	require.NotNil(t, rangeResp.Header)
	require.Equal(t, resp.Header.Revision, rangeResp.Header.Revision)
	require.Equal(t, []byte("created"), rangeResp.Kvs[0].Value)
}

func TestTxnCompareCreateRevisionNonZeroMatchesMetadata(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()

	ctx := context.Background()
	key := []byte("/registry/generic-txn/compare-create-nonzero")
	_, err := server.Put(ctx, &etcdserverpb.PutRequest{Key: key, Value: []byte("v1")})
	require.NoError(t, err)
	rangeResp, err := server.Range(ctx, &etcdserverpb.RangeRequest{Key: key})
	require.NoError(t, err)
	require.Len(t, rangeResp.Kvs, 1)
	createRevision := rangeResp.Kvs[0].CreateRevision

	resp, err := server.Txn(ctx, &etcdserverpb.TxnRequest{
		Compare: []*etcdserverpb.Compare{{
			Key:         key,
			Target:      etcdserverpb.Compare_CREATE,
			Result:      etcdserverpb.Compare_EQUAL,
			TargetUnion: &etcdserverpb.Compare_CreateRevision{CreateRevision: createRevision},
		}},
		Success: []*etcdserverpb.RequestOp{
			{Request: &etcdserverpb.RequestOp_RequestRange{
				RequestRange: &etcdserverpb.RangeRequest{Key: key},
			}},
		},
	})
	require.NoError(t, err)
	require.True(t, resp.Succeeded)
	rangeResp = resp.Responses[0].GetResponseRange()
	require.NotNil(t, rangeResp)
	require.NotNil(t, rangeResp.Header)
	require.Equal(t, resp.Header.Revision, rangeResp.Header.Revision)
	require.Len(t, rangeResp.Kvs, 1)

	resp, err = server.Txn(ctx, &etcdserverpb.TxnRequest{
		Compare: []*etcdserverpb.Compare{{
			Key:         key,
			Target:      etcdserverpb.Compare_CREATE,
			Result:      etcdserverpb.Compare_EQUAL,
			TargetUnion: &etcdserverpb.Compare_CreateRevision{CreateRevision: createRevision + 1},
		}},
		Success: []*etcdserverpb.RequestOp{
			{Request: &etcdserverpb.RequestOp_RequestPut{
				RequestPut: &etcdserverpb.PutRequest{Key: key, Value: []byte("should-not-write")},
			}},
		},
		Failure: []*etcdserverpb.RequestOp{
			{Request: &etcdserverpb.RequestOp_RequestRange{
				RequestRange: &etcdserverpb.RangeRequest{Key: key},
			}},
		},
	})
	require.NoError(t, err)
	require.False(t, resp.Succeeded)
	rangeResp = resp.Responses[0].GetResponseRange()
	require.NotNil(t, rangeResp)
	require.NotNil(t, rangeResp.Header)
	require.Equal(t, resp.Header.Revision, rangeResp.Header.Revision)
	require.Equal(t, []byte("v1"), rangeResp.Kvs[0].Value)
}

func TestTxnCompareVersionChecksExistence(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()

	ctx := context.Background()
	key := []byte("/registry/generic-txn/compare-version")
	_, err := server.Put(ctx, &etcdserverpb.PutRequest{Key: key, Value: []byte("v1")})
	require.NoError(t, err)

	rangeResp, err := server.Range(ctx, &etcdserverpb.RangeRequest{Key: key})
	require.NoError(t, err)
	require.Len(t, rangeResp.Kvs, 1)
	require.Equal(t, int64(1), rangeResp.Kvs[0].Version)

	resp, err := server.Txn(ctx, &etcdserverpb.TxnRequest{
		Compare: []*etcdserverpb.Compare{{
			Key:         key,
			Target:      etcdserverpb.Compare_VERSION,
			Result:      etcdserverpb.Compare_EQUAL,
			TargetUnion: &etcdserverpb.Compare_Version{Version: 1},
		}},
		Success: []*etcdserverpb.RequestOp{
			{Request: &etcdserverpb.RequestOp_RequestPut{
				RequestPut: &etcdserverpb.PutRequest{Key: key, Value: []byte("matched")},
			}},
		},
	})
	require.NoError(t, err)
	require.True(t, resp.Succeeded)

	rangeResp, err = server.Range(ctx, &etcdserverpb.RangeRequest{Key: key})
	require.NoError(t, err)
	require.Len(t, rangeResp.Kvs, 1)
	require.Equal(t, int64(2), rangeResp.Kvs[0].Version)

	resp, err = server.Txn(ctx, &etcdserverpb.TxnRequest{
		Compare: []*etcdserverpb.Compare{{
			Key:         key,
			Target:      etcdserverpb.Compare_VERSION,
			Result:      etcdserverpb.Compare_GREATER,
			TargetUnion: &etcdserverpb.Compare_Version{Version: 1},
		}},
		Success: []*etcdserverpb.RequestOp{
			{Request: &etcdserverpb.RequestOp_RequestPut{
				RequestPut: &etcdserverpb.PutRequest{Key: key, Value: []byte("should-not-write")},
			}},
		},
		Failure: []*etcdserverpb.RequestOp{
			{Request: &etcdserverpb.RequestOp_RequestRange{
				RequestRange: &etcdserverpb.RangeRequest{Key: key},
			}},
		},
	})
	require.NoError(t, err)
	require.True(t, resp.Succeeded)
}

func TestTxnRangeAfterPutReportsSameTxnVersion(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()

	ctx := context.Background()
	key := []byte("/registry/generic-txn/intra-version")
	_, err := server.Put(ctx, &etcdserverpb.PutRequest{Key: key, Value: []byte("exists")})
	require.NoError(t, err)

	resp, err := server.Txn(ctx, &etcdserverpb.TxnRequest{
		Compare: []*etcdserverpb.Compare{{
			Key:         key,
			Target:      etcdserverpb.Compare_VERSION,
			Result:      etcdserverpb.Compare_EQUAL,
			TargetUnion: &etcdserverpb.Compare_Version{Version: 1},
		}},
		Success: []*etcdserverpb.RequestOp{
			{Request: &etcdserverpb.RequestOp_RequestPut{
				RequestPut: &etcdserverpb.PutRequest{Key: key, Value: []byte("version-matched")},
			}},
			{Request: &etcdserverpb.RequestOp_RequestRange{
				RequestRange: &etcdserverpb.RangeRequest{Key: key},
			}},
		},
		Failure: []*etcdserverpb.RequestOp{
			{Request: &etcdserverpb.RequestOp_RequestPut{
				RequestPut: &etcdserverpb.PutRequest{Key: key, Value: []byte("version-not-matched")},
			}},
		},
	})
	require.NoError(t, err)
	require.True(t, resp.Succeeded)
	require.Len(t, resp.Responses, 2)

	rangeResp := resp.Responses[1].GetResponseRange()
	require.NotNil(t, rangeResp)
	require.Len(t, rangeResp.Kvs, 1)
	require.Equal(t, []byte("version-matched"), rangeResp.Kvs[0].Value)
	require.Equal(t, int64(2), rangeResp.Kvs[0].Version)
	require.Equal(t, resp.Header.Revision, rangeResp.Header.Revision)
	require.Equal(t, resp.Header.Revision, rangeResp.Kvs[0].ModRevision)
}

func TestTxnCompareLeaseRunsSelectedBranch(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()

	ctx := context.Background()
	key := []byte("/registry/generic-txn/compare-lease")
	leaseResp, err := server.LeaseGrant(ctx, &etcdserverpb.LeaseGrantRequest{TTL: 30, ID: 67890})
	require.NoError(t, err)
	_, err = server.Put(ctx, &etcdserverpb.PutRequest{Key: key, Value: []byte("leased"), Lease: leaseResp.ID})
	require.NoError(t, err)

	resp, err := server.Txn(ctx, &etcdserverpb.TxnRequest{
		Compare: []*etcdserverpb.Compare{{
			Key:         key,
			Target:      etcdserverpb.Compare_LEASE,
			Result:      etcdserverpb.Compare_EQUAL,
			TargetUnion: &etcdserverpb.Compare_Lease{Lease: leaseResp.ID},
		}},
		Success: []*etcdserverpb.RequestOp{
			{Request: &etcdserverpb.RequestOp_RequestPut{
				RequestPut: &etcdserverpb.PutRequest{Key: key, Value: []byte("matched"), Lease: leaseResp.ID},
			}},
			{Request: &etcdserverpb.RequestOp_RequestRange{
				RequestRange: &etcdserverpb.RangeRequest{Key: key},
			}},
		},
	})
	require.NoError(t, err)
	require.True(t, resp.Succeeded)
	require.Len(t, resp.Responses, 2)
	rangeResp := resp.Responses[1].GetResponseRange()
	require.NotNil(t, rangeResp)
	require.NotNil(t, rangeResp.Header)
	require.Equal(t, resp.Header.Revision, rangeResp.Header.Revision)
	require.Equal(t, []byte("matched"), rangeResp.Kvs[0].Value)

	resp, err = server.Txn(ctx, &etcdserverpb.TxnRequest{
		Compare: []*etcdserverpb.Compare{{
			Key:         key,
			Target:      etcdserverpb.Compare_LEASE,
			Result:      etcdserverpb.Compare_EQUAL,
			TargetUnion: &etcdserverpb.Compare_Lease{Lease: leaseResp.ID + 1},
		}},
		Success: []*etcdserverpb.RequestOp{
			{Request: &etcdserverpb.RequestOp_RequestPut{
				RequestPut: &etcdserverpb.PutRequest{Key: key, Value: []byte("should-not-write")},
			}},
		},
		Failure: []*etcdserverpb.RequestOp{
			{Request: &etcdserverpb.RequestOp_RequestRange{
				RequestRange: &etcdserverpb.RangeRequest{Key: key},
			}},
		},
	})
	require.NoError(t, err)
	require.False(t, resp.Succeeded)
	require.Len(t, resp.Responses, 1)
	rangeResp = resp.Responses[0].GetResponseRange()
	require.NotNil(t, rangeResp)
	require.NotNil(t, rangeResp.Header)
	require.Equal(t, resp.Header.Revision, rangeResp.Header.Revision)
	require.Equal(t, []byte("matched"), rangeResp.Kvs[0].Value)
}

func TestTxnCompactRevisionCAS(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()

	ctx := context.Background()

	firstResp, err := server.Txn(ctx, compactTxn(0, "0"))
	require.NoError(t, err)
	require.True(t, firstResp.Succeeded)
	require.Len(t, firstResp.Responses, 1)
	firstPut := firstResp.Responses[0].GetResponsePut()
	require.NotNil(t, firstPut)
	require.NotNil(t, firstPut.Header)
	require.Equal(t, firstResp.Header.Revision, firstPut.Header.Revision)

	staleResp, err := server.Txn(ctx, compactTxn(0, "10"))
	require.NoError(t, err)
	require.False(t, staleResp.Succeeded)
	require.Len(t, staleResp.Responses, 1)
	staleRange := staleResp.Responses[0].GetResponseRange()
	require.NotNil(t, staleRange)
	require.NotNil(t, staleRange.Header)
	require.Equal(t, firstResp.Header.Revision, staleRange.Header.Revision)
	require.Len(t, staleRange.Kvs, 1)
	require.Equal(t, int64(1), staleRange.Kvs[0].Version)
	require.Equal(t, []byte("0"), staleRange.Kvs[0].Value)

	secondResp, err := server.Txn(ctx, compactTxn(1, "10"))
	require.NoError(t, err)
	require.True(t, secondResp.Succeeded)
	require.Equal(t, firstResp.Header.Revision+1, secondResp.Header.Revision)
	secondPut := secondResp.Responses[0].GetResponsePut()
	require.NotNil(t, secondPut)
	require.NotNil(t, secondPut.Header)
	require.Equal(t, secondResp.Header.Revision, secondPut.Header.Revision)

	currentResp, err := server.Txn(ctx, compactTxn(1, "20"))
	require.NoError(t, err)
	require.False(t, currentResp.Succeeded)
	currentRange := currentResp.Responses[0].GetResponseRange()
	require.NotNil(t, currentRange)
	require.NotNil(t, currentRange.Header)
	require.Equal(t, secondResp.Header.Revision, currentRange.Header.Revision)
	require.Len(t, currentRange.Kvs, 1)
	require.Equal(t, int64(2), currentRange.Kvs[0].Version)
	require.Equal(t, []byte("10"), currentRange.Kvs[0].Value)
}

// The leading-'/' watch-key gate (isPureWatchRequest) was removed in #78: etcd
// keys are arbitrary byte strings, and the gate silently rejected every watch
// from slash-less consumers (Cilium). Watchability of arbitrary keys is pinned
// end-to-end by TestCiliumConsumerCompatibility (pkg/endpoint).

// TestRangeAtMagicRevisionIsNotHijacked pins #53: a Range at revision 1888 (the
// former partition-magic value) must get normal etcd semantics, not be hijacked
// into returning partition metadata. Partition discovery uses the brain-protocol
// ListPartition RPC instead.
func TestRangeAtMagicRevisionIsNotHijacked(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()
	ctx := context.Background()

	// Put the current revision well above the magic value so the requested
	// revision 1888 is a valid (past) revision rather than a future one.
	server.backend.SetCurrentRevision(1_000_000)
	_, err := server.Put(ctx, &etcdserverpb.PutRequest{Key: []byte("/registry/a"), Value: []byte("v")})
	require.NoError(t, err)

	resp, err := server.Range(ctx, &etcdserverpb.RangeRequest{
		Key:      []byte("/registry/"),
		RangeEnd: []byte("/registry0"),
		Revision: 1888, // the former partition-magic value, now a plain revision
	})
	require.NoError(t, err)
	// Nothing existed as of revision 1888, so this is a normal empty range — not
	// partition boundary keys (which the old hijack would have returned).
	require.Empty(t, resp.Kvs)
	require.EqualValues(t, 0, resp.Count)
}

// TestTxnCompactRevisionConcurrentSingleWinner pins the #54/#72 fix: when
// several HA-apiserver compactors race the same version, the emulation must let
// exactly one win (an atomic version CAS), not several.
func TestTxnCompactRevisionConcurrentSingleWinner(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()
	ctx := context.Background()

	first, err := server.Txn(ctx, compactTxn(0, "0"))
	require.NoError(t, err)
	require.True(t, first.Succeeded) // create -> version 1

	const n = 16
	var wg sync.WaitGroup
	succeeded := make([]bool, n)
	errs := make([]error, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			r, e := server.Txn(ctx, compactTxn(1, "10"))
			errs[i] = e
			if e == nil {
				succeeded[i] = r.Succeeded
			}
		}(i)
	}
	wg.Wait()

	winners := 0
	for i := 0; i < n; i++ {
		require.NoError(t, errs[i])
		if succeeded[i] {
			winners++
		}
	}
	require.Equal(t, 1, winners, "exactly one concurrent compactor may win the version CAS")

	rr, err := server.Range(ctx, &etcdserverpb.RangeRequest{Key: []byte(compactRevKey)})
	require.NoError(t, err)
	require.Len(t, rr.Kvs, 1)
	require.Equal(t, int64(2), rr.Kvs[0].Version, "only one write must have applied over the seed")
}

func compactTxn(expectVersion int64, rev string) *etcdserverpb.TxnRequest {
	return &etcdserverpb.TxnRequest{
		Compare: []*etcdserverpb.Compare{{
			Key:         []byte(compactRevKey),
			Target:      etcdserverpb.Compare_VERSION,
			Result:      etcdserverpb.Compare_EQUAL,
			TargetUnion: &etcdserverpb.Compare_Version{Version: expectVersion},
		}},
		Success: []*etcdserverpb.RequestOp{{
			Request: &etcdserverpb.RequestOp_RequestPut{
				RequestPut: &etcdserverpb.PutRequest{Key: []byte(compactRevKey), Value: []byte(rev)},
			},
		}},
		Failure: []*etcdserverpb.RequestOp{{
			Request: &etcdserverpb.RequestOp_RequestRange{
				RequestRange: &etcdserverpb.RangeRequest{Key: []byte(compactRevKey)},
			},
		}},
	}
}

func requireDirectKVError(t *testing.T, err error, want error, code codes.Code, message string) {
	t.Helper()
	require.ErrorIs(t, err, want)
	require.Equal(t, code, status.Code(err))
	require.Equal(t, message, status.Convert(err).Message())
}

func requireDirectKVStatusError(t *testing.T, err error, code codes.Code, message string) {
	t.Helper()
	require.EqualError(t, err, status.Error(code, message).Error())
	require.Equal(t, code, status.Code(err))
	require.Equal(t, message, status.Convert(err).Message())
}

func requireClientKVError(t *testing.T, err error, want error, code codes.Code, message string) {
	t.Helper()
	require.ErrorIs(t, err, want)
	require.Equal(t, code, status.Code(err))
	require.Equal(t, message, status.Convert(err).Message())
}
