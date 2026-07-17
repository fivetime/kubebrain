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
	"encoding/json"
	"io"
	"sync"
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
	onSend   func()
}

type observingRevisionBackend struct {
	BackendShim
	observed chan<- struct{}
}

type blockingLeaseMetaBackend struct {
	BackendShim
	entered chan struct{}
	release chan struct{}
	once    sync.Once
}

type blockingLeaseMetaDeleteBackend struct {
	BackendShim
	key     []byte
	entered chan struct{}
	release chan struct{}
	once    sync.Once
}

func (b *blockingLeaseMetaDeleteBackend) InternalDelete(ctx context.Context, key []byte) error {
	if string(key) == string(b.key) {
		b.once.Do(func() { close(b.entered) })
		select {
		case <-b.release:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return b.BackendShim.InternalDelete(ctx, key)
}

func (b *blockingLeaseMetaBackend) InternalPut(ctx context.Context, key, value []byte) error {
	if string(key) == string(leaseStorageKey(5201)) {
		b.once.Do(func() { close(b.entered) })
		select {
		case <-b.release:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return b.BackendShim.InternalPut(ctx, key, value)
}

func (b observingRevisionBackend) GetCurrentRevision() uint64 {
	select {
	case b.observed <- struct{}{}:
	default:
	}
	return b.BackendShim.GetCurrentRevision()
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
	if f.onSend != nil {
		f.onSend()
	}
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
	require.Equal(t, int64(server.backend.GetCurrentRevision()), grantResp.Header.Revision)

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
	require.Equal(t, int64(server.backend.GetCurrentRevision()), ttlResp.Header.Revision)

	stream := &fakeLeaseKeepAliveServer{
		requests: []*etcdserverpb.LeaseKeepAliveRequest{{ID: grantResp.ID}},
	}
	require.NoError(t, server.LeaseKeepAlive(stream))
	require.Len(t, stream.sent, 1)
	require.Equal(t, grantResp.ID, stream.sent[0].ID)
	require.Equal(t, int64(30), stream.sent[0].TTL)
	require.Equal(t, int64(server.backend.GetCurrentRevision()), stream.sent[0].Header.Revision)

	_, err = server.LeaseRevoke(ctx, &etcdserverpb.LeaseRevokeRequest{ID: grantResp.ID})
	require.NoError(t, err)

	rangeResp, err := server.Range(ctx, &etcdserverpb.RangeRequest{Key: []byte("/registry/leases/a")})
	require.NoError(t, err)
	require.Empty(t, rangeResp.Kvs)
}

func TestLeaseRevokeDeletesAllKeysAtOneRevision(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()
	ctx := context.Background()
	const leaseID int64 = 1011
	_, err := server.LeaseGrant(ctx, &etcdserverpb.LeaseGrantRequest{TTL: 300, ID: leaseID})
	require.NoError(t, err)

	keys := [][]byte{
		[]byte("/registry/leases/revoke-a"),
		[]byte("/registry/leases/revoke-b"),
		[]byte("/registry/leases/revoke-c"),
	}
	var lastPutRevision int64
	for _, key := range keys {
		resp, err := server.Put(ctx, &etcdserverpb.PutRequest{Key: key, Value: []byte("v"), Lease: leaseID})
		require.NoError(t, err)
		lastPutRevision = resp.Header.Revision
	}

	revoked, err := server.LeaseRevoke(ctx, &etcdserverpb.LeaseRevokeRequest{ID: leaseID})
	require.NoError(t, err)
	require.Equal(t, lastPutRevision+1, revoked.Header.Revision)
	for _, key := range keys {
		historical, err := server.Range(ctx, &etcdserverpb.RangeRequest{Key: key, Revision: revoked.Header.Revision - 1})
		require.NoError(t, err)
		require.Len(t, historical.Kvs, 1)
		current, err := server.Range(ctx, &etcdserverpb.RangeRequest{Key: key})
		require.NoError(t, err)
		require.Empty(t, current.Kvs)
	}
	require.Equal(t, uint64(revoked.Header.Revision), server.backend.GetCurrentRevision())
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
	require.Equal(t, int64(server.backend.GetCurrentRevision()), resp.Header.Revision)
}

func TestRemainingTTLTruncatesLiveSubsecondToZeroLikeEtcd(t *testing.T) {
	require.Equal(t, int64(1), remainingTTL(&leaseState{
		deadline: time.Now().Add(1500 * time.Millisecond),
	}))
	require.Equal(t, int64(0), remainingTTL(&leaseState{
		deadline: time.Now().Add(500 * time.Millisecond),
	}), "etcd truncates a live final sub-second instead of rounding it up")
	require.Equal(t, int64(0), remainingTTL(&leaseState{
		deadline: time.Now().Add(-time.Millisecond),
	}))
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
	require.Equal(t, int64(server.backend.GetCurrentRevision()), stream.sent[0].Header.Revision)
}

func TestLeaseKeepAliveCapturesRevisionBeforeRenewal(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()
	grant, err := server.LeaseGrant(context.Background(), &etcdserverpb.LeaseGrantRequest{TTL: 30, ID: 10001})
	require.NoError(t, err)

	base := server.backend
	initialRevision := base.GetCurrentRevision()
	observed := make(chan struct{}, 1)
	server.backend = observingRevisionBackend{BackendShim: base, observed: observed}
	server.leaseMu.Lock()
	locked := true
	defer func() {
		if locked {
			server.leaseMu.Unlock()
		}
	}()

	stream := &fakeLeaseKeepAliveServer{requests: []*etcdserverpb.LeaseKeepAliveRequest{{ID: grant.ID}}}
	done := make(chan error, 1)
	go func() { done <- server.LeaseKeepAlive(stream) }()

	select {
	case <-observed:
	case <-time.After(time.Second):
		t.Fatal("keepalive did not capture its response revision before waiting for lease renewal")
	}
	base.SetCurrentRevision(initialRevision + 1)
	server.leaseMu.Unlock()
	locked = false
	require.NoError(t, <-done)
	require.Len(t, stream.sent, 1)
	require.Equal(t, int64(initialRevision), stream.sent[0].Header.Revision)
	require.Equal(t, int64(30), stream.sent[0].TTL)
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

func TestLeaseGrantClampsSmallTTLLikeEtcd(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()
	ctx := context.Background()
	for i, ttl := range []int64{-1, 0, 1, 2} {
		resp, err := server.LeaseGrant(ctx, &etcdserverpb.LeaseGrantRequest{TTL: ttl, ID: int64(5100 + i)})
		require.NoError(t, err)
		require.Equal(t, minLeaseTTL, resp.TTL)
		ttlResp, err := server.LeaseTimeToLive(ctx, &etcdserverpb.LeaseTimeToLiveRequest{ID: resp.ID})
		require.NoError(t, err)
		require.Equal(t, minLeaseTTL, ttlResp.GrantedTTL)
	}
}

func TestLeaseGrantPublishesOnlyAfterMetadataCommit(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()
	ctx := context.Background()
	shim := &blockingLeaseMetaBackend{
		BackendShim: server.backend,
		entered:     make(chan struct{}),
		release:     make(chan struct{}),
	}
	server.backend = shim

	grantDone := make(chan error, 1)
	go func() {
		_, err := server.LeaseGrant(ctx, &etcdserverpb.LeaseGrantRequest{TTL: 30, ID: 5201})
		grantDone <- err
	}()
	<-shim.entered

	_, err := server.Put(ctx, &etcdserverpb.PutRequest{
		Key: []byte("/registry/leases/pending"), Value: []byte("value"), Lease: 5201,
	})
	require.Equal(t, codes.NotFound, status.Code(err))
	require.Equal(t, "etcdserver: requested lease not found", status.Convert(err).Message())
	_, err = server.LeaseGrant(ctx, &etcdserverpb.LeaseGrantRequest{TTL: 30, ID: 5201})
	require.Equal(t, codes.FailedPrecondition, status.Code(err), "pending ID must reject a duplicate grant")

	close(shim.release)
	require.NoError(t, <-grantDone)
	_, err = server.Put(ctx, &etcdserverpb.PutRequest{
		Key: []byte("/registry/leases/pending"), Value: []byte("value"), Lease: 5201,
	})
	require.NoError(t, err, "committed lease must become usable")
}

func TestLeaseGrantFailureReleasesPendingReservation(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()
	shim := &blockingLeaseMetaBackend{
		BackendShim: server.backend,
		entered:     make(chan struct{}),
		release:     make(chan struct{}),
	}
	server.backend = shim
	ctx, cancel := context.WithCancel(context.Background())

	grantDone := make(chan error, 1)
	go func() {
		_, err := server.LeaseGrant(ctx, &etcdserverpb.LeaseGrantRequest{TTL: 30, ID: 5201})
		grantDone <- err
	}()
	<-shim.entered
	cancel()
	require.ErrorIs(t, <-grantDone, context.Canceled)

	server.leaseMu.Lock()
	_, active := server.leases[5201]
	_, pending := server.pendingLeases[5201]
	server.leaseMu.Unlock()
	require.False(t, active)
	require.False(t, pending)

	close(shim.release)
	_, err := server.LeaseGrant(context.Background(), &etcdserverpb.LeaseGrantRequest{TTL: 30, ID: 5201})
	require.NoError(t, err, "failed persistence must not poison the explicit lease ID")
}

func TestLeaseGrantDoesNotPublishAcrossLeaseStateReset(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()
	shim := &blockingLeaseMetaBackend{
		BackendShim: server.backend,
		entered:     make(chan struct{}),
		release:     make(chan struct{}),
	}
	server.backend = shim

	grantDone := make(chan error, 1)
	go func() {
		_, err := server.LeaseGrant(context.Background(), &etcdserverpb.LeaseGrantRequest{TTL: 30, ID: 5201})
		grantDone <- err
	}()
	<-shim.entered
	server.stopLeases()
	close(shim.release)
	require.Equal(t, codes.Unavailable, status.Code(<-grantDone))

	server.leaseMu.Lock()
	_, active := server.leases[5201]
	_, pending := server.pendingLeases[5201]
	server.leaseMu.Unlock()
	require.False(t, active)
	require.False(t, pending)
}

func TestLeaseRegrantWaitsForPriorMetadataDelete(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()
	ctx := context.Background()
	const leaseID int64 = 5202
	_, err := server.LeaseGrant(ctx, &etcdserverpb.LeaseGrantRequest{TTL: 30, ID: leaseID})
	require.NoError(t, err)

	shim := &blockingLeaseMetaDeleteBackend{
		BackendShim: server.backend,
		key:         leaseStorageKey(leaseID),
		entered:     make(chan struct{}),
		release:     make(chan struct{}),
	}
	server.backend = shim
	revokeDone := make(chan error, 1)
	go func() {
		_, err := server.LeaseRevoke(ctx, &etcdserverpb.LeaseRevokeRequest{ID: leaseID})
		revokeDone <- err
	}()
	<-shim.entered

	grantDone := make(chan error, 1)
	go func() {
		_, err := server.LeaseGrant(ctx, &etcdserverpb.LeaseGrantRequest{TTL: 60, ID: leaseID})
		grantDone <- err
	}()
	var (
		grantErr       error
		completedEarly bool
	)
	select {
	case grantErr = <-grantDone:
		completedEarly = true
	case <-time.After(100 * time.Millisecond):
	}

	close(shim.release)
	require.NoError(t, <-revokeDone)
	if !completedEarly {
		grantErr = <-grantDone
	}
	require.False(t, completedEarly, "same-ID grant completed before prior metadata delete")
	require.NoError(t, grantErr)
	require.NoError(t, server.ReloadLeases(ctx))
	ttl, err := server.LeaseTimeToLive(ctx, &etcdserverpb.LeaseTimeToLiveRequest{ID: leaseID})
	require.NoError(t, err)
	require.Equal(t, int64(60), ttl.GrantedTTL, "replacement lease metadata must survive reload")
}

func TestLeaseMetaDoesNotAdvanceKVRevisionAndRestores(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()
	ctx := context.Background()
	before := server.backend.GetCurrentRevision()

	_, err := server.LeaseGrant(ctx, &etcdserverpb.LeaseGrantRequest{TTL: 300, ID: 5050})
	require.NoError(t, err)
	require.Equal(t, before, server.backend.GetCurrentRevision())

	restored := New(server.backend.(*backendShim).backend, server.metricCli, server.peers)
	defer restored.stopLeases()
	records, attachments, err := restored.loadLeaseRecords(ctx)
	require.NoError(t, err)
	require.Empty(t, attachments)
	require.Len(t, records, 1)
	require.Equal(t, int64(5050), records[0].ID)
	require.Equal(t, int64(300), records[0].TTL)

	_, err = server.LeaseRevoke(ctx, &etcdserverpb.LeaseRevokeRequest{ID: 5050})
	require.NoError(t, err)
	require.Equal(t, before, server.backend.GetCurrentRevision())
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
	require.Equal(t, int64(server.backend.GetCurrentRevision()), resp.Header.Revision)
}

func TestLeaseLeasesOrdersByExpiryLikeEtcd(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()

	ctx := context.Background()
	for _, id := range []int64{3003, 3001, 3002} {
		_, err := server.LeaseGrant(ctx, &etcdserverpb.LeaseGrantRequest{TTL: 30, ID: id})
		require.NoError(t, err)
	}

	server.leaseMu.Lock()
	now := time.Now()
	server.leases[3001].deadline = now.Add(time.Second)
	server.leases[3002].deadline = now.Add(2 * time.Second)
	server.leases[3003].deadline = now.Add(3 * time.Second)
	server.leaseMu.Unlock()

	resp, err := server.LeaseLeases(ctx, &etcdserverpb.LeaseLeasesRequest{})
	require.NoError(t, err)
	require.Equal(t, []*etcdserverpb.LeaseStatus{
		{ID: 3001},
		{ID: 3002},
		{ID: 3003},
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
		_, err := server.backend.InternalGet(ctx, leaseStorageKey(grantResp.ID))
		return err == nil
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

// TestKeepAliveDoesNotWriteStorage pins the write-amplification fix: a
// LeaseKeepAlive must only bump the in-memory expiry/timer and must NOT persist
// the lease record (which would mint a fresh MVCC version + watch event per
// tick). We capture the persisted record's ModRevision, drive several
// keepalives, and assert the record is untouched while the client still gets
// the TTL echoed back.
func TestKeepAliveDoesNotWriteStorage(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()

	ctx := context.Background()
	grantResp, err := server.LeaseGrant(ctx, &etcdserverpb.LeaseGrantRequest{TTL: 30, ID: 8101})
	require.NoError(t, err)

	_, err = server.Put(ctx, &etcdserverpb.PutRequest{
		Key:   []byte("/registry/leases/keepalive-noop"),
		Value: []byte("value"),
		Lease: grantResp.ID,
	})
	require.NoError(t, err)

	// Capture the persisted lease record revision once binding has landed.
	var beforeMeta []byte
	require.Eventually(t, func() bool {
		value, err := server.backend.InternalGet(ctx, leaseStorageKey(grantResp.ID))
		if err != nil {
			return false
		}
		beforeMeta = value
		return true
	}, time.Second, 10*time.Millisecond)

	const n = 5
	reqs := make([]*etcdserverpb.LeaseKeepAliveRequest, 0, n)
	for i := 0; i < n; i++ {
		reqs = append(reqs, &etcdserverpb.LeaseKeepAliveRequest{ID: grantResp.ID})
	}
	stream := &fakeLeaseKeepAliveServer{requests: reqs}
	require.NoError(t, server.LeaseKeepAlive(stream))
	require.Len(t, stream.sent, n)
	for i := 0; i < n; i++ {
		require.Equal(t, grantResp.ID, stream.sent[i].ID)
		require.Equal(t, int64(30), stream.sent[i].TTL)
	}

	// The lease record must be byte-for-byte the same version: no keepalive
	// minted a new MVCC version or a duplicate record.
	afterMeta, err := server.backend.InternalGet(ctx, leaseStorageKey(grantResp.ID))
	require.NoError(t, err)
	require.Equal(t, beforeMeta, afterMeta, "keepalive must not rewrite lease metadata")
}

// TestReloadResetsDeadlineToGrantedTTL locks the companion recovery fix: on
// restore/reload the deadline is reconstructed as now+grantedTTL, never the
// stale persisted absolute deadline. A record whose persisted deadline is far
// in the past (a lease that had been kept alive well past grant time) must be
// reloaded with a fresh full-TTL window and its bound key must NOT be expired.
func TestReloadResetsDeadlineToGrantedTTL(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()

	ctx := context.Background()
	grantResp, err := server.LeaseGrant(ctx, &etcdserverpb.LeaseGrantRequest{TTL: 300, ID: 8201})
	require.NoError(t, err)
	_, err = server.Put(ctx, &etcdserverpb.PutRequest{
		Key:   []byte("/registry/leases/reset-deadline"),
		Value: []byte("value"),
		Lease: grantResp.ID,
	})
	require.NoError(t, err)

	// Replay a record whose persisted deadline is in the past but whose granted
	// TTL is still 300s. The old code would Reset(0) the timer and delete the
	// bound key; the fix recovers deadline = now+300s.
	server.applyLeaseRecords([]leaseRecord{{
		ID:               grantResp.ID,
		TTL:              300,
		DeadlineUnixNano: time.Now().Add(-time.Hour).UnixNano(),
		Keys:             []string{"/registry/leases/reset-deadline"},
	}}, nil)

	ttlResp, err := server.LeaseTimeToLive(ctx, &etcdserverpb.LeaseTimeToLiveRequest{ID: grantResp.ID})
	require.NoError(t, err)
	require.Greater(t, ttlResp.TTL, int64(250), "reloaded lease must get a fresh full-TTL window, not the stale past deadline")

	// Give any (buggy) immediate-expiry timer a chance to fire; the bound key
	// must still be present.
	time.Sleep(100 * time.Millisecond)
	rangeResp, err := server.Range(ctx, &etcdserverpb.RangeRequest{Key: []byte("/registry/leases/reset-deadline")})
	require.NoError(t, err)
	require.Len(t, rangeResp.Kvs, 1, "reloaded live lease must not expire its bound key")
}

// TestKeptAliveLeaseSurvivesLeaderChangeWithFreshDeadline exercises the full
// failover path: a short-TTL lease is kept alive, then a new leader reloads
// state whose persisted deadline may be at/before grant time. Even if the
// persisted deadline is already in the past, the reloaded lease must survive
// with a positive TTL and its bound key must remain.
func TestKeptAliveLeaseSurvivesLeaderChangeWithFreshDeadline(t *testing.T) {
	ctrl := gomock.NewController(t)
	metrics := mock.NewMinimalMetrics(ctrl)
	kv := memkv.NewKvStorage()
	b := backend.NewBackend(kv, backend.Config{
		Identity:                "keepalive-failover-test-peer",
		EnableEtcdCompatibility: true,
	}, metrics)

	ctx := context.Background()
	oldLeader := New(b, metrics, testPeerService{isLeader: true})
	grantResp, err := oldLeader.LeaseGrant(ctx, &etcdserverpb.LeaseGrantRequest{TTL: 2, ID: 9101})
	require.NoError(t, err)
	_, err = oldLeader.Put(ctx, &etcdserverpb.PutRequest{
		Key:   []byte("/registry/leases/failover-survive"),
		Value: []byte("value"),
		Lease: grantResp.ID,
	})
	require.NoError(t, err)

	// Client keeps the lease alive across the failover instant.
	stream := &fakeLeaseKeepAliveServer{
		requests: []*etcdserverpb.LeaseKeepAliveRequest{{ID: grantResp.ID}},
	}
	require.NoError(t, oldLeader.LeaseKeepAlive(stream))

	// Simulate a persisted record whose deadline is already in the past at the
	// moment of leader change (keepalive no longer refreshes it durably).
	data, err := json.Marshal(leaseRecord{
		ID:               grantResp.ID,
		TTL:              2,
		DeadlineUnixNano: time.Now().Add(-time.Hour).UnixNano(),
		Keys:             []string{"/registry/leases/failover-survive"},
	})
	require.NoError(t, err)
	_, err = oldLeader.backend.Put(ctx, &etcdserverpb.PutRequest{
		Key:   leaseStorageKey(grantResp.ID),
		Value: data,
	})
	require.NoError(t, err)
	oldLeader.stopLeases()

	newLeader := New(b, metrics, testPeerService{isLeader: true})
	defer func() {
		newLeader.stopLeases()
		require.NoError(t, kv.Close())
		ctrl.Finish()
	}()

	// Explicit leadership-acquisition reload.
	require.NoError(t, newLeader.ReloadLeases(ctx))

	ttlResp, err := newLeader.LeaseTimeToLive(ctx, &etcdserverpb.LeaseTimeToLiveRequest{ID: grantResp.ID})
	require.NoError(t, err)
	require.Greater(t, ttlResp.TTL, int64(0), "kept-alive lease must not be expired on leader change")

	leases, err := newLeader.LeaseLeases(ctx, &etcdserverpb.LeaseLeasesRequest{})
	require.NoError(t, err)
	require.Len(t, leases.Leases, 1)

	time.Sleep(100 * time.Millisecond)
	rangeResp, err := newLeader.Range(ctx, &etcdserverpb.RangeRequest{Key: []byte("/registry/leases/failover-survive")})
	require.NoError(t, err)
	require.Len(t, rangeResp.Kvs, 1, "bound key of a kept-alive lease must survive leader change")
}
