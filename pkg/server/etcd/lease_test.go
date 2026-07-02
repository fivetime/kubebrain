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
	"io"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"github.com/kubewharf/kubebrain/pkg/backend"
	"github.com/kubewharf/kubebrain/pkg/metrics/mock"
	"github.com/kubewharf/kubebrain/pkg/storage/memkv"
)

type fakeLeaseKeepAliveServer struct {
	ctx      context.Context
	requests []*etcdserverpb.LeaseKeepAliveRequest
	sent     []*etcdserverpb.LeaseKeepAliveResponse
}

func (f *fakeLeaseKeepAliveServer) Recv() (*etcdserverpb.LeaseKeepAliveRequest, error) {
	if len(f.requests) == 0 {
		return nil, io.EOF
	}
	req := f.requests[0]
	f.requests = f.requests[1:]
	return req, nil
}

func (f *fakeLeaseKeepAliveServer) Send(resp *etcdserverpb.LeaseKeepAliveResponse) error {
	f.sent = append(f.sent, resp)
	return nil
}

func (f *fakeLeaseKeepAliveServer) SetHeader(metadata.MD) error {
	return nil
}

func (f *fakeLeaseKeepAliveServer) SendHeader(metadata.MD) error {
	return nil
}

func (f *fakeLeaseKeepAliveServer) SetTrailer(metadata.MD) {
}

func (f *fakeLeaseKeepAliveServer) Context() context.Context {
	if f.ctx == nil {
		return context.Background()
	}
	return f.ctx
}

func (f *fakeLeaseKeepAliveServer) SendMsg(interface{}) error {
	return nil
}

func (f *fakeLeaseKeepAliveServer) RecvMsg(interface{}) error {
	return nil
}

func TestLeaseGrantBindKeepAliveAndRevoke(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()

	ctx := context.Background()
	grantResp, err := server.LeaseGrant(ctx, &etcdserverpb.LeaseGrantRequest{TTL: 30, ID: 1001})
	require.NoError(t, err)
	require.Equal(t, int64(1001), grantResp.ID)
	require.Equal(t, int64(30), grantResp.TTL)

	_, err = server.Put(ctx, &etcdserverpb.PutRequest{
		Key:   []byte("/registry/leases/a"),
		Value: []byte("value"),
		Lease: grantResp.ID,
	})
	require.NoError(t, err)

	ttlResp, err := server.LeaseTimeToLive(ctx, &etcdserverpb.LeaseTimeToLiveRequest{
		ID:   grantResp.ID,
		Keys: true,
	})
	require.NoError(t, err)
	require.Equal(t, int64(30), ttlResp.GrantedTTL)
	require.ElementsMatch(t, [][]byte{[]byte("/registry/leases/a")}, ttlResp.Keys)

	stream := &fakeLeaseKeepAliveServer{
		requests: []*etcdserverpb.LeaseKeepAliveRequest{{ID: grantResp.ID}},
	}
	require.NoError(t, server.LeaseKeepAlive(stream))
	require.Len(t, stream.sent, 1)
	require.Equal(t, grantResp.ID, stream.sent[0].ID)
	require.Equal(t, int64(30), stream.sent[0].TTL)

	_, err = server.LeaseRevoke(ctx, &etcdserverpb.LeaseRevokeRequest{ID: grantResp.ID})
	require.NoError(t, err)

	rangeResp, err := server.Range(ctx, &etcdserverpb.RangeRequest{Key: []byte("/registry/leases/a")})
	require.NoError(t, err)
	require.Empty(t, rangeResp.Kvs)
}

func TestLeaseRejectsUnknownLease(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()

	_, err := server.Put(context.Background(), &etcdserverpb.PutRequest{
		Key:   []byte("/registry/leases/missing"),
		Value: []byte("value"),
		Lease: 9999,
	})
	require.Error(t, err)
	require.Equal(t, codes.NotFound, status.Code(err))
	require.Contains(t, err.Error(), "etcdserver: requested lease not found")
}

func TestLeaseTimeToLiveUnknownLeaseMatchesEtcd(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()

	resp, err := server.LeaseTimeToLive(context.Background(), &etcdserverpb.LeaseTimeToLiveRequest{
		ID:   9999,
		Keys: true,
	})
	require.NoError(t, err)
	require.Equal(t, int64(9999), resp.ID)
	require.Equal(t, int64(-1), resp.TTL)
	require.Zero(t, resp.GrantedTTL)
	require.Empty(t, resp.Keys)
}

func TestLeaseKeepAliveUnknownLeaseMatchesEtcd(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()

	stream := &fakeLeaseKeepAliveServer{
		requests: []*etcdserverpb.LeaseKeepAliveRequest{{ID: 9999}},
	}
	require.NoError(t, server.LeaseKeepAlive(stream))
	require.Len(t, stream.sent, 1)
	require.Equal(t, int64(9999), stream.sent[0].ID)
	require.Zero(t, stream.sent[0].TTL)
}

func TestLeaseGrantDuplicateAndTooLargeTTLMatchEtcdErrors(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()

	ctx := context.Background()
	_, err := server.LeaseGrant(ctx, &etcdserverpb.LeaseGrantRequest{TTL: 30, ID: 5001})
	require.NoError(t, err)

	_, err = server.LeaseGrant(ctx, &etcdserverpb.LeaseGrantRequest{TTL: 30, ID: 5001})
	require.Error(t, err)
	require.Equal(t, codes.FailedPrecondition, status.Code(err))
	require.Contains(t, err.Error(), "etcdserver: lease already exists")

	_, err = server.LeaseGrant(ctx, &etcdserverpb.LeaseGrantRequest{TTL: maxLeaseTTL + 1, ID: 5002})
	require.Error(t, err)
	require.Equal(t, codes.OutOfRange, status.Code(err))
	require.Contains(t, err.Error(), "etcdserver: too large lease TTL")
}

func TestLeaseExpiryDeletesBoundKeys(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()

	ctx := context.Background()
	grantResp, err := server.LeaseGrant(ctx, &etcdserverpb.LeaseGrantRequest{TTL: 1, ID: 1002})
	require.NoError(t, err)

	_, err = server.Put(ctx, &etcdserverpb.PutRequest{
		Key:   []byte("/registry/leases/expire"),
		Value: []byte("value"),
		Lease: grantResp.ID,
	})
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		rangeResp, err := server.Range(ctx, &etcdserverpb.RangeRequest{Key: []byte("/registry/leases/expire")})
		return err == nil && len(rangeResp.Kvs) == 0
	}, 3*time.Second, 100*time.Millisecond)
}

func TestLeaseLeasesListsGrantedLeases(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()

	ctx := context.Background()
	_, err := server.LeaseGrant(ctx, &etcdserverpb.LeaseGrantRequest{TTL: 30, ID: 2001})
	require.NoError(t, err)
	_, err = server.LeaseGrant(ctx, &etcdserverpb.LeaseGrantRequest{TTL: 30, ID: 2002})
	require.NoError(t, err)

	resp, err := server.LeaseLeases(ctx, &etcdserverpb.LeaseLeasesRequest{})
	require.NoError(t, err)
	require.ElementsMatch(t, []*etcdserverpb.LeaseStatus{
		{ID: 2001},
		{ID: 2002},
	}, resp.Leases)
}

func TestLeaseRestoreFromBackend(t *testing.T) {
	ctrl := gomock.NewController(t)
	metrics := mock.NewMinimalMetrics(ctrl)
	kv := memkv.NewKvStorage()
	b := backend.NewBackend(kv, backend.Config{
		Identity:                "restore-test-peer",
		EnableEtcdCompatibility: true,
	}, metrics)
	server := New(b, metrics, testPeerService{isLeader: true})
	defer func() {
		server.stopLeases()
		require.NoError(t, kv.Close())
		ctrl.Finish()
	}()

	ctx := context.Background()
	grantResp, err := server.LeaseGrant(ctx, &etcdserverpb.LeaseGrantRequest{TTL: 30, ID: 3001})
	require.NoError(t, err)
	_, err = server.Put(ctx, &etcdserverpb.PutRequest{
		Key:   []byte("/registry/leases/restored"),
		Value: []byte("value"),
		Lease: grantResp.ID,
	})
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		resp, err := server.backend.Get(ctx, &etcdserverpb.RangeRequest{Key: leaseStorageKey(grantResp.ID)})
		return err == nil && len(resp.Kvs) == 1
	}, time.Second, 10*time.Millisecond)

	server.stopLeases()
	b.SetCurrentRevision(0)
	restored := New(b, metrics, testPeerService{isLeader: true})
	defer restored.stopLeases()

	ttlResp, err := restored.LeaseTimeToLive(ctx, &etcdserverpb.LeaseTimeToLiveRequest{
		ID:   grantResp.ID,
		Keys: true,
	})
	require.NoError(t, err)
	require.Equal(t, int64(30), ttlResp.GrantedTTL)
	require.ElementsMatch(t, [][]byte{[]byte("/registry/leases/restored")}, ttlResp.Keys)
}

func TestLeaseFollowerRejectsWriteRPCs(t *testing.T) {
	ctrl := gomock.NewController(t)
	metrics := mock.NewMinimalMetrics(ctrl)
	kv := memkv.NewKvStorage()
	b := backend.NewBackend(kv, backend.Config{
		Identity:                "follower-test-peer",
		EnableEtcdCompatibility: true,
	}, metrics)
	server := New(b, metrics, testPeerService{isLeader: false})
	defer func() {
		server.stopLeases()
		require.NoError(t, kv.Close())
		ctrl.Finish()
	}()

	ctx := context.Background()
	_, err := server.LeaseGrant(ctx, &etcdserverpb.LeaseGrantRequest{TTL: 30, ID: 4001})
	require.Error(t, err)

	_, err = server.LeaseRevoke(ctx, &etcdserverpb.LeaseRevokeRequest{ID: 4001})
	require.Error(t, err)

	stream := &fakeLeaseKeepAliveServer{
		requests: []*etcdserverpb.LeaseKeepAliveRequest{{ID: 4001}},
	}
	require.Error(t, server.LeaseKeepAlive(stream))
}

func TestLeaseFollowerProxiesUnaryGrant(t *testing.T) {
	ctrl := gomock.NewController(t)
	metrics := mock.NewMinimalMetrics(ctrl)
	kv := memkv.NewKvStorage()
	b := backend.NewBackend(kv, backend.Config{
		Identity:                "proxy-follower-test-peer",
		EnableEtcdCompatibility: true,
	}, metrics)
	called := false
	server := New(b, metrics, testPeerService{
		isLeader:     false,
		proxyEnabled: true,
		leaseGrantFn: func(ctx context.Context, req *etcdserverpb.LeaseGrantRequest) (*etcdserverpb.LeaseGrantResponse, error) {
			called = true
			require.Equal(t, int64(30), req.TTL)
			return &etcdserverpb.LeaseGrantResponse{ID: 6001, TTL: req.TTL}, nil
		},
	})
	defer func() {
		server.stopLeases()
		require.NoError(t, kv.Close())
		ctrl.Finish()
	}()

	resp, err := server.LeaseGrant(context.Background(), &etcdserverpb.LeaseGrantRequest{TTL: 30})
	require.NoError(t, err)
	require.True(t, called)
	require.Equal(t, int64(6001), resp.ID)
}

func TestLeaseFollowerProxiesKeepAlive(t *testing.T) {
	ctrl := gomock.NewController(t)
	metrics := mock.NewMinimalMetrics(ctrl)
	kv := memkv.NewKvStorage()
	b := backend.NewBackend(kv, backend.Config{
		Identity:                "proxy-follower-keepalive-test-peer",
		EnableEtcdCompatibility: true,
	}, metrics)
	called := false
	server := New(b, metrics, testPeerService{
		isLeader:     false,
		proxyEnabled: true,
		leaseKeepAliveFn: func(ctx context.Context, req *etcdserverpb.LeaseKeepAliveRequest) (*etcdserverpb.LeaseKeepAliveResponse, error) {
			called = true
			require.Equal(t, int64(7001), req.ID)
			return &etcdserverpb.LeaseKeepAliveResponse{ID: req.ID, TTL: 30}, nil
		},
	})
	defer func() {
		server.stopLeases()
		require.NoError(t, kv.Close())
		ctrl.Finish()
	}()

	stream := &fakeLeaseKeepAliveServer{
		requests: []*etcdserverpb.LeaseKeepAliveRequest{{ID: 7001}},
	}
	require.NoError(t, server.LeaseKeepAlive(stream))
	require.True(t, called)
	require.Len(t, stream.sent, 1)
	require.Equal(t, int64(7001), stream.sent[0].ID)
	require.Equal(t, int64(30), stream.sent[0].TTL)
}

func TestLeaseFollowerDoesNotExpireKeys(t *testing.T) {
	ctrl := gomock.NewController(t)
	metrics := mock.NewMinimalMetrics(ctrl)
	kv := memkv.NewKvStorage()
	b := backend.NewBackend(kv, backend.Config{
		Identity:                "leader-test-peer",
		EnableEtcdCompatibility: true,
	}, metrics)
	leaderServer := New(b, metrics, testPeerService{isLeader: true})
	ctx := context.Background()
	grantResp, err := leaderServer.LeaseGrant(ctx, &etcdserverpb.LeaseGrantRequest{TTL: 1, ID: 5001})
	require.NoError(t, err)
	_, err = leaderServer.Put(ctx, &etcdserverpb.PutRequest{
		Key:   []byte("/registry/leases/follower-expire"),
		Value: []byte("value"),
		Lease: grantResp.ID,
	})
	require.NoError(t, err)
	leaderServer.stopLeases()

	followerServer := New(b, metrics, testPeerService{isLeader: false})
	defer func() {
		followerServer.stopLeases()
		require.NoError(t, kv.Close())
		ctrl.Finish()
	}()

	time.Sleep(1500 * time.Millisecond)
	rangeResp, err := followerServer.Range(ctx, &etcdserverpb.RangeRequest{Key: []byte("/registry/leases/follower-expire")})
	require.NoError(t, err)
	require.Len(t, rangeResp.Kvs, 1)
}

// TestReloadLeasesAdoptsLeasesGrantedAfterSnapshot pins the failover fix: a node
// that restored its lease snapshot as a follower must, on becoming leader, pick
// up leases the real leader granted afterwards (otherwise they are orphaned —
// never kept alive or expired). It also confirms a kept-alive lease is not
// wrongly treated as expired after reload.
func TestReloadLeasesAdoptsLeasesGrantedAfterSnapshot(t *testing.T) {
	ctrl := gomock.NewController(t)
	metrics := mock.NewMinimalMetrics(ctrl)
	kv := memkv.NewKvStorage()
	b := backend.NewBackend(kv, backend.Config{
		Identity:                "reload-test-peer",
		EnableEtcdCompatibility: true,
	}, metrics)
	ctx := context.Background()

	// "old leader" grants L1 and binds a key.
	oldLeader := New(b, metrics, testPeerService{isLeader: true})
	l1, err := oldLeader.LeaseGrant(ctx, &etcdserverpb.LeaseGrantRequest{TTL: 300, ID: 7001})
	require.NoError(t, err)
	_, err = oldLeader.Put(ctx, &etcdserverpb.PutRequest{
		Key: []byte("/registry/leases/reload-k1"), Value: []byte("v"), Lease: l1.ID,
	})
	require.NoError(t, err)

	// "new leader" constructs its snapshot now (knows only L1), then the old
	// leader grants L2 afterwards — a lease the new leader never saw.
	newLeader := New(b, metrics, testPeerService{isLeader: true})
	defer func() {
		newLeader.stopLeases()
		oldLeader.stopLeases()
		require.NoError(t, kv.Close())
		ctrl.Finish()
	}()

	l2, err := oldLeader.LeaseGrant(ctx, &etcdserverpb.LeaseGrantRequest{TTL: 300, ID: 7002})
	require.NoError(t, err)

	// Before reload the new leader does not know L2 (etcd reports TTL=-1 for an
	// unknown lease, without error).
	pre, err := newLeader.LeaseTimeToLive(ctx, &etcdserverpb.LeaseTimeToLiveRequest{ID: l2.ID})
	require.NoError(t, err)
	require.Equal(t, int64(-1), pre.TTL, "new leader should not know L2 before reload")

	// Simulate leadership acquisition.
	require.NoError(t, newLeader.ReloadLeases(ctx))

	// After reload both leases are known with healthy TTLs.
	ttl1, err := newLeader.LeaseTimeToLive(ctx, &etcdserverpb.LeaseTimeToLiveRequest{ID: l1.ID})
	require.NoError(t, err)
	require.Greater(t, ttl1.TTL, int64(0), "kept-alive lease L1 must not be expired after reload")
	ttl2, err := newLeader.LeaseTimeToLive(ctx, &etcdserverpb.LeaseTimeToLiveRequest{ID: l2.ID})
	require.NoError(t, err)
	require.Greater(t, ttl2.TTL, int64(0), "lease L2 granted after snapshot must be adopted on reload")

	leases, err := newLeader.LeaseLeases(ctx, &etcdserverpb.LeaseLeasesRequest{})
	require.NoError(t, err)
	require.Len(t, leases.Leases, 2)
}
