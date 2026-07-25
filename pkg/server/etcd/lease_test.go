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
	"errors"
	"io"
	"math"
	"sort"
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
	"github.com/kubewharf/kubebrain/pkg/storage"
	"github.com/kubewharf/kubebrain/pkg/storage/memkv"
)

type fakeLeaseKeepAliveServer struct {
	ctx      context.Context
	requests []*etcdserverpb.LeaseKeepAliveRequest
	sent     []*etcdserverpb.LeaseKeepAliveResponse
	onSend   func()
	recv     func() (*etcdserverpb.LeaseKeepAliveRequest, error)
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

type failAtomicLeaseRevokeBackend struct {
	BackendShim
	leaseID int64
}

type uncertainLeaseRevokeBackend struct {
	BackendShim
	leaseID int64
	commit  bool
	failed  bool
}

type committedCheckpointErrorBackend struct {
	BackendShim
	leaseID int64
	failed  bool
	cancel  context.CancelFunc
}

func (b *committedCheckpointErrorBackend) InternalPut(ctx context.Context, key, value []byte) error {
	if err := b.BackendShim.InternalPut(ctx, key, value); err != nil {
		return err
	}
	if b.failed || string(key) != string(leaseStorageKey(b.leaseID)) {
		return nil
	}
	var record leaseRecord
	if err := json.Unmarshal(value, &record); err != nil || record.RemainingTTL == 0 {
		return nil
	}
	b.failed = true
	if b.cancel != nil {
		b.cancel()
		return ctx.Err()
	}
	return errors.New("lease checkpoint response lost after commit")
}

type rejectedLeaseMetadataBackend struct {
	BackendShim
	err error
}

func (b *rejectedLeaseMetadataBackend) InternalPut(context.Context, []byte, []byte) error {
	return b.err
}

func (b *failAtomicLeaseRevokeBackend) TxnApply(ctx context.Context, ops []backend.TxnWriteOp, guards []backend.TxnGuard, prevKV []bool) ([]*etcdserverpb.ResponseOp, uint64, []backend.TxnWriteResult, error) {
	for _, op := range ops {
		if op.Delete && op.Internal && string(op.Key) == string(leaseStorageKey(b.leaseID)) {
			return nil, 0, nil, errFakeDelete
		}
	}
	return b.BackendShim.TxnApply(ctx, ops, guards, prevKV)
}

func (b *uncertainLeaseRevokeBackend) TxnApply(ctx context.Context, ops []backend.TxnWriteOp, guards []backend.TxnGuard, prevKV []bool) ([]*etcdserverpb.ResponseOp, uint64, []backend.TxnWriteResult, error) {
	target := string(leaseStorageKey(b.leaseID))
	for _, op := range ops {
		if !b.failed && op.Delete && op.Internal && string(op.Key) == target {
			b.failed = true
			if !b.commit {
				return nil, 0, nil, storage.NewErrUncertainResult(context.DeadlineExceeded)
			}
			_, revision, _, err := b.BackendShim.TxnApply(ctx, ops, guards, prevKV)
			if err != nil {
				return nil, revision, nil, err
			}
			return nil, revision, nil, storage.NewErrUncertainResult(context.DeadlineExceeded)
		}
	}
	return b.BackendShim.TxnApply(ctx, ops, guards, prevKV)
}

func (b *blockingLeaseMetaDeleteBackend) TxnApply(ctx context.Context, ops []backend.TxnWriteOp, guards []backend.TxnGuard, prevKV []bool) ([]*etcdserverpb.ResponseOp, uint64, []backend.TxnWriteResult, error) {
	for _, op := range ops {
		if op.Delete && op.Internal && string(op.Key) == string(b.key) {
			b.once.Do(func() { close(b.entered) })
			select {
			case <-b.release:
			case <-ctx.Done():
				return nil, 0, nil, ctx.Err()
			}
			break
		}
	}
	return b.BackendShim.TxnApply(ctx, ops, guards, prevKV)
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
	if f.recv != nil {
		return f.recv()
	}
	if len(f.requests) == 0 {
		return nil, io.EOF
	}
	req := f.requests[0]
	f.requests = f.requests[1:]
	return req, nil
}

func TestLeaseKeepAliveCancellationInterruptsBlockedReceive(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()

	ctx, cancel := context.WithCancel(context.Background())
	recvStarted := make(chan struct{})
	unblockRecv := make(chan struct{})
	stream := &fakeLeaseKeepAliveServer{
		ctx: ctx,
		recv: func() (*etcdserverpb.LeaseKeepAliveRequest, error) {
			close(recvStarted)
			<-unblockRecv
			return nil, io.EOF
		},
	}
	done := make(chan error, 1)
	go func() { done <- server.LeaseKeepAlive(stream) }()
	<-recvStarted
	cancel()
	require.Equal(t, codes.Canceled, status.Code(<-done))
	close(unblockRecv)
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

func TestEmptyLeaseRevokeDeletesDurableMetadata(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()
	ctx := context.Background()
	const leaseID int64 = 2050
	_, err := server.LeaseGrant(ctx, &etcdserverpb.LeaseGrantRequest{TTL: 300, ID: leaseID})
	require.NoError(t, err)
	before := server.backend.GetCurrentRevision()

	_, err = server.LeaseRevoke(ctx, &etcdserverpb.LeaseRevokeRequest{ID: leaseID})
	require.NoError(t, err)
	require.Equal(t, before, server.backend.GetCurrentRevision(),
		"empty lease revoke only mutates internal metadata")
	_, err = server.backend.InternalGet(ctx, leaseStorageKey(leaseID))
	require.ErrorIs(t, err, storage.ErrKeyNotFound)

	records, attachments, err := server.loadLeaseRecords(ctx)
	require.NoError(t, err)
	server.applyLeaseRecords(records, attachments)
	ttl, err := server.LeaseTimeToLive(ctx, &etcdserverpb.LeaseTimeToLiveRequest{ID: leaseID})
	require.NoError(t, err)
	require.Equal(t, int64(-1), ttl.TTL,
		"an empty revoked lease must not resurrect after leader reload")
}

func TestCommittedUncertainLeaseRevokeForgetsInMemoryLease(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()
	ctx := context.Background()
	const leaseID int64 = 2051
	key := []byte("/registry/leases/uncertain-revoke")
	_, err := server.LeaseGrant(ctx, &etcdserverpb.LeaseGrantRequest{TTL: 300, ID: leaseID})
	require.NoError(t, err)
	_, err = server.Put(ctx, &etcdserverpb.PutRequest{Key: key, Value: []byte("value"), Lease: leaseID})
	require.NoError(t, err)

	shim := &uncertainLeaseRevokeBackend{
		BackendShim: server.backend,
		leaseID:     leaseID,
		commit:      true,
	}
	server.backend = shim
	revoked, err := server.LeaseRevoke(ctx, &etcdserverpb.LeaseRevokeRequest{ID: leaseID})
	require.NoError(t, err)
	require.Positive(t, revoked.Header.Revision)
	require.True(t, shim.failed)

	current, err := server.Range(ctx, &etcdserverpb.RangeRequest{Key: key})
	require.NoError(t, err)
	require.Empty(t, current.Kvs)
	_, err = shim.InternalGet(ctx, leaseStorageKey(leaseID))
	require.ErrorIs(t, err, storage.ErrKeyNotFound)
	_, err = shim.InternalGet(ctx, leaseAttachKey(string(key)))
	require.ErrorIs(t, err, storage.ErrKeyNotFound)
	ttl, err := server.LeaseTimeToLive(ctx, &etcdserverpb.LeaseTimeToLiveRequest{ID: leaseID})
	require.NoError(t, err)
	require.Equal(t, int64(-1), ttl.TTL,
		"a committed revoke must not leave an in-memory lease that keepalive can resurrect")
}

func TestUncommittedUncertainLeaseRevokeRetainsLease(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()
	ctx := context.Background()
	const leaseID int64 = 2052
	key := []byte("/registry/leases/uncommitted-revoke")
	_, err := server.LeaseGrant(ctx, &etcdserverpb.LeaseGrantRequest{TTL: 300, ID: leaseID})
	require.NoError(t, err)
	_, err = server.Put(ctx, &etcdserverpb.PutRequest{Key: key, Value: []byte("value"), Lease: leaseID})
	require.NoError(t, err)

	shim := &uncertainLeaseRevokeBackend{
		BackendShim: server.backend,
		leaseID:     leaseID,
	}
	server.backend = shim
	_, err = server.LeaseRevoke(ctx, &etcdserverpb.LeaseRevokeRequest{ID: leaseID})
	require.ErrorIs(t, err, storage.ErrUncertainResult)
	require.True(t, shim.failed)

	current, err := server.Range(ctx, &etcdserverpb.RangeRequest{Key: key})
	require.NoError(t, err)
	require.Len(t, current.Kvs, 1)
	_, err = shim.InternalGet(ctx, leaseStorageKey(leaseID))
	require.NoError(t, err)
	ttl, err := server.LeaseTimeToLive(ctx, &etcdserverpb.LeaseTimeToLiveRequest{ID: leaseID, Keys: true})
	require.NoError(t, err)
	require.Equal(t, [][]byte{key}, ttl.Keys)
}

func TestLeaseRevokeTransactionFailureRetainsKeysAndMetadata(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()
	ctx := context.Background()
	const leaseID int64 = 1012
	key := []byte("/registry/leases/atomic-failure")
	_, err := server.LeaseGrant(ctx, &etcdserverpb.LeaseGrantRequest{TTL: 300, ID: leaseID})
	require.NoError(t, err)
	_, err = server.Put(ctx, &etcdserverpb.PutRequest{Key: key, Value: []byte("value"), Lease: leaseID})
	require.NoError(t, err)

	server.backend = &failAtomicLeaseRevokeBackend{BackendShim: server.backend, leaseID: leaseID}
	_, err = server.LeaseRevoke(ctx, &etcdserverpb.LeaseRevokeRequest{ID: leaseID})
	require.ErrorIs(t, err, errFakeDelete)

	current, err := server.Range(ctx, &etcdserverpb.RangeRequest{Key: key})
	require.NoError(t, err)
	require.Len(t, current.Kvs, 1, "failed atomic revoke must retain the attached user key")
	_, err = server.backend.InternalGet(ctx, leaseStorageKey(leaseID))
	require.NoError(t, err, "failed atomic revoke must retain durable lease metadata")
	ttl, err := server.LeaseTimeToLive(ctx, &etcdserverpb.LeaseTimeToLiveRequest{ID: leaseID, Keys: true})
	require.NoError(t, err)
	require.Equal(t, [][]byte{key}, ttl.Keys)
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
	require.Equal(t, int64(-1), remainingTTL(&leaseState{
		deadline: time.Now().Add(-1500 * time.Millisecond),
	}), "etcd exposes elapsed whole seconds while an expired lease awaits revoke")
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

func TestLeaseKeepAliveAndRevokeSignedIDBoundariesMatchEtcd(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()
	ctx := context.Background()

	liveIDs := []int64{-1, math.MinInt64, math.MaxInt64}
	for _, id := range liveIDs {
		grant, err := server.LeaseGrant(ctx, &etcdserverpb.LeaseGrantRequest{ID: id, TTL: 30})
		require.NoError(t, err)
		require.Equal(t, id, grant.ID)
	}

	stream := &fakeLeaseKeepAliveServer{requests: []*etcdserverpb.LeaseKeepAliveRequest{
		{ID: 0},
		{ID: -1},
		{ID: math.MinInt64},
		{ID: math.MaxInt64},
	}}
	require.NoError(t, server.LeaseKeepAlive(stream))
	require.Len(t, stream.sent, 4)
	require.Equal(t, int64(0), stream.sent[0].ID)
	require.Zero(t, stream.sent[0].TTL)
	require.Positive(t, stream.sent[0].Header.Revision)
	for i, id := range liveIDs {
		response := stream.sent[i+1]
		require.Equal(t, id, response.ID)
		require.Positive(t, response.TTL)
		require.LessOrEqual(t, response.TTL, int64(30))
		require.Positive(t, response.Header.Revision)
	}

	for _, id := range liveIDs {
		revoked, err := server.LeaseRevoke(ctx, &etcdserverpb.LeaseRevokeRequest{ID: id})
		require.NoError(t, err)
		require.Positive(t, revoked.Header.Revision)

		_, err = server.LeaseRevoke(ctx, &etcdserverpb.LeaseRevokeRequest{ID: id})
		require.Error(t, err)
		require.Equal(t, codes.NotFound, status.Code(err))
		require.Equal(t, "etcdserver: requested lease not found", status.Convert(err).Message())
	}
	_, err := server.LeaseRevoke(ctx, &etcdserverpb.LeaseRevokeRequest{ID: 0})
	require.Error(t, err)
	require.Equal(t, codes.NotFound, status.Code(err))
	require.Equal(t, "etcdserver: requested lease not found", status.Convert(err).Message())
}

func TestLeaseKeepAliveCannotResurrectExpiredLease(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()
	ctx := context.Background()
	const leaseID int64 = 10000
	_, err := server.LeaseGrant(ctx, &etcdserverpb.LeaseGrantRequest{TTL: 30, ID: leaseID})
	require.NoError(t, err)
	key := []byte("/registry/leases/expired-keepalive")
	_, err = server.Put(ctx, &etcdserverpb.PutRequest{Key: key, Value: []byte("value"), Lease: leaseID})
	require.NoError(t, err)

	server.leaseMu.Lock()
	st := server.leases[leaseID]
	require.NotNil(t, st)
	st.timer.Stop()
	st.deadline = time.Now().Add(-time.Second)
	server.leaseMu.Unlock()

	stream := &fakeLeaseKeepAliveServer{
		requests: []*etcdserverpb.LeaseKeepAliveRequest{{ID: leaseID}},
	}
	done := make(chan error, 1)
	go func() { done <- server.LeaseKeepAlive(stream) }()
	select {
	case err := <-done:
		t.Fatalf("expired keepalive returned before revoke completed: %v", err)
	case <-time.After(50 * time.Millisecond):
	}
	server.expireLease(leaseID)
	require.NoError(t, <-done)
	require.Len(t, stream.sent, 1)
	require.Zero(t, stream.sent[0].TTL)
	ttl, err := server.LeaseTimeToLive(ctx, &etcdserverpb.LeaseTimeToLiveRequest{ID: leaseID})
	require.NoError(t, err)
	require.Equal(t, int64(-1), ttl.TTL)
	got, err := server.Range(ctx, &etcdserverpb.RangeRequest{Key: key})
	require.NoError(t, err)
	require.Empty(t, got.Kvs)
}

func TestExpiredLeaseKeepAliveWaitsAcrossFailedRevoke(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()
	ctx := context.Background()
	const leaseID int64 = 10003
	key := []byte("/registry/leases/expired-keepalive-retry")
	_, err := server.LeaseGrant(ctx, &etcdserverpb.LeaseGrantRequest{TTL: 30, ID: leaseID})
	require.NoError(t, err)
	_, err = server.Put(ctx, &etcdserverpb.PutRequest{Key: key, Value: []byte("value"), Lease: leaseID})
	require.NoError(t, err)

	server.leaseMu.Lock()
	server.leases[leaseID].timer.Stop()
	server.leases[leaseID].deadline = time.Now().Add(-time.Second)
	server.leaseMu.Unlock()

	base := server.backend
	server.backend = &failAtomicLeaseRevokeBackend{BackendShim: base, leaseID: leaseID}
	stream := &fakeLeaseKeepAliveServer{
		requests: []*etcdserverpb.LeaseKeepAliveRequest{{ID: leaseID}},
	}
	done := make(chan error, 1)
	go func() { done <- server.LeaseKeepAlive(stream) }()
	server.expireLease(leaseID)
	select {
	case err := <-done:
		t.Fatalf("keepalive returned while failed revoke left keys readable: %v", err)
	case <-time.After(50 * time.Millisecond):
	}
	got, err := server.Range(ctx, &etcdserverpb.RangeRequest{Key: key})
	require.NoError(t, err)
	require.Len(t, got.Kvs, 1)

	server.backend = base
	server.expireLease(leaseID)
	require.NoError(t, <-done)
	require.Len(t, stream.sent, 1)
	require.Zero(t, stream.sent[0].TTL)
	got, err = server.Range(ctx, &etcdserverpb.RangeRequest{Key: key})
	require.NoError(t, err)
	require.Empty(t, got.Kvs)
}

func TestExpiredLeaseKeepAliveWaitHonorsStreamCancellation(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()
	const leaseID int64 = 10004
	_, err := server.LeaseGrant(context.Background(), &etcdserverpb.LeaseGrantRequest{TTL: 30, ID: leaseID})
	require.NoError(t, err)

	server.leaseMu.Lock()
	server.leases[leaseID].timer.Stop()
	server.leases[leaseID].deadline = time.Now().Add(-time.Second)
	server.leaseMu.Unlock()

	ctx, cancel := context.WithCancel(context.Background())
	stream := &fakeLeaseKeepAliveServer{
		ctx:      ctx,
		requests: []*etcdserverpb.LeaseKeepAliveRequest{{ID: leaseID}},
	}
	done := make(chan error, 1)
	go func() { done <- server.LeaseKeepAlive(stream) }()
	select {
	case err := <-done:
		t.Fatalf("expired keepalive returned before revoke or cancellation: %v", err)
	case <-time.After(50 * time.Millisecond):
	}
	cancel()
	require.Equal(t, codes.Canceled, status.Code(<-done))
	require.Empty(t, stream.sent)
}

func TestStaleExpiryCallbackDoesNotRevokeRenewedLease(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()
	ctx := context.Background()
	const leaseID int64 = 10002
	_, err := server.LeaseGrant(ctx, &etcdserverpb.LeaseGrantRequest{TTL: 30, ID: leaseID})
	require.NoError(t, err)

	server.leaseMu.Lock()
	st := server.leases[leaseID]
	require.NotNil(t, st)
	st.deadline = time.Now().Add(30 * time.Second)
	server.leaseMu.Unlock()

	// Model a timer callback that was queued before a successful keepalive reset
	// the timer. The callback must observe the new deadline and stand down.
	server.expireLease(leaseID)
	ttl, err := server.LeaseTimeToLive(ctx, &etcdserverpb.LeaseTimeToLiveRequest{ID: leaseID})
	require.NoError(t, err)
	require.Positive(t, ttl.TTL)
	require.Equal(t, int64(30), ttl.GrantedTTL)
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

func TestLeaseTimeToLiveZeroIDMatchesEtcd(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()

	ctx := context.Background()
	withoutKeys, err := server.LeaseTimeToLive(ctx, &etcdserverpb.LeaseTimeToLiveRequest{})
	require.NoError(t, err)
	withKeys, err := server.LeaseTimeToLive(ctx, &etcdserverpb.LeaseTimeToLiveRequest{Keys: true})
	require.NoError(t, err)

	require.Equal(t, int64(0), withoutKeys.ID)
	require.Equal(t, int64(-1), withoutKeys.TTL)
	require.Equal(t, int64(0), withoutKeys.GrantedTTL)
	require.Empty(t, withoutKeys.Keys)
	require.Empty(t, withKeys.Keys)
	require.Equal(t, withoutKeys.ID, withKeys.ID)
	require.Equal(t, withoutKeys.TTL, withKeys.TTL)
	require.Equal(t, withoutKeys.GrantedTTL, withKeys.GrantedTTL)
	require.NotNil(t, withoutKeys.Header)
	require.NotNil(t, withKeys.Header)
	require.Equal(t, withoutKeys.Header.Revision, withKeys.Header.Revision)
	require.Equal(t, int64(server.backend.GetCurrentRevision()), withoutKeys.Header.Revision)
}

func TestLeaseTimeToLiveLiveKeysAndLeaseListBoundary(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()

	ctx := context.Background()
	const leaseID int64 = 5160
	grant, err := server.LeaseGrant(ctx, &etcdserverpb.LeaseGrantRequest{TTL: 300, ID: leaseID})
	require.NoError(t, err)
	require.Equal(t, leaseID, grant.ID)
	keys := [][]byte{
		[]byte("/registry/leases/read-boundary/z"),
		[]byte("/registry/leases/read-boundary/a"),
	}
	for _, key := range keys {
		_, err := server.Put(ctx, &etcdserverpb.PutRequest{Key: key, Value: []byte("value"), Lease: leaseID})
		require.NoError(t, err)
	}

	withoutKeys, err := server.LeaseTimeToLive(ctx, &etcdserverpb.LeaseTimeToLiveRequest{ID: leaseID})
	require.NoError(t, err)
	require.Equal(t, leaseID, withoutKeys.ID)
	require.Positive(t, withoutKeys.TTL)
	require.LessOrEqual(t, withoutKeys.TTL, int64(300))
	require.Equal(t, int64(300), withoutKeys.GrantedTTL)
	require.Empty(t, withoutKeys.Keys)
	require.NotNil(t, withoutKeys.Header)
	require.Positive(t, withoutKeys.Header.Revision)

	withKeys, err := server.LeaseTimeToLive(ctx, &etcdserverpb.LeaseTimeToLiveRequest{ID: leaseID, Keys: true})
	require.NoError(t, err)
	require.Equal(t, leaseID, withKeys.ID)
	require.Positive(t, withKeys.TTL)
	require.LessOrEqual(t, withKeys.TTL, int64(300))
	require.Equal(t, int64(300), withKeys.GrantedTTL)
	sort.Slice(keys, func(i, j int) bool { return string(keys[i]) < string(keys[j]) })
	sort.Slice(withKeys.Keys, func(i, j int) bool { return string(withKeys.Keys[i]) < string(withKeys.Keys[j]) })
	require.Equal(t, keys, withKeys.Keys)

	list, err := server.LeaseLeases(ctx, &etcdserverpb.LeaseLeasesRequest{})
	require.NoError(t, err)
	require.NotNil(t, list.Header)
	require.Positive(t, list.Header.Revision)
	containsLive := false
	for _, status := range list.Leases {
		require.NotZero(t, status.ID)
		containsLive = containsLive || status.ID == leaseID
	}
	require.True(t, containsLive)
}

func TestCorruptAlarmDefersNaturalLeaseExpiry(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()
	ctx := context.Background()
	const leaseID = int64(5151)
	key := []byte("corrupt-expiry")

	_, err := server.LeaseGrant(ctx, &etcdserverpb.LeaseGrantRequest{TTL: 30, ID: leaseID})
	require.NoError(t, err)
	_, err = server.Put(ctx, &etcdserverpb.PutRequest{Key: key, Value: []byte("leased"), Lease: leaseID})
	require.NoError(t, err)
	server.leaseMu.Lock()
	state := server.leases[leaseID]
	require.NotNil(t, state)
	state.deadline = time.Now().Add(-time.Second)
	state.timer.Stop()
	server.leaseMu.Unlock()

	require.NoError(t, server.backend.ArmCorrupt(ctx, 1))
	server.expireLeaseWithContext(ctx, leaseID)
	read, err := server.Range(ctx, &etcdserverpb.RangeRequest{Key: key})
	require.NoError(t, err)
	require.Len(t, read.Kvs, 1)
	server.leaseMu.Lock()
	_, retained := server.leases[leaseID]
	server.leaseMu.Unlock()
	require.True(t, retained)

	removed, err := server.backend.DisarmCorrupt(ctx, 1)
	require.NoError(t, err)
	require.True(t, removed)
	server.expireLeaseWithContext(ctx, leaseID)
	read, err = server.Range(ctx, &etcdserverpb.RangeRequest{Key: key})
	require.NoError(t, err)
	require.Empty(t, read.Kvs)
	server.leaseMu.Lock()
	_, retained = server.leases[leaseID]
	server.leaseMu.Unlock()
	require.False(t, retained)
}

func TestAutomaticLeaseIDStaysPositiveAfterMaxExplicitIDReload(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()

	server.leaseID = 100
	server.applyLeaseRecords([]leaseRecord{
		{ID: math.MaxInt64, TTL: 300},
		{ID: math.MinInt64, TTL: 300},
		{ID: -1, TTL: 300},
	}, nil)

	require.Equal(t, int64(100), server.leaseID,
		"explicit/restored IDs must not reseed etcd's independent auto-ID generator")
	require.Equal(t, int64(101), server.nextLeaseID())

	overflow := newLeaseManager(nil, math.MaxInt64)
	require.Equal(t, int64(1), overflow.nextLeaseID(),
		"automatic IDs wrap within the positive int64 range and skip zero")
}

func TestLoadLeaseRecordsRejectsInvalidAttachmentMetadata(t *testing.T) {
	tests := []struct {
		name  string
		write func(context.Context, *RPCServer) error
	}{
		{
			name: "legacy_user_mvcc_attachment",
			write: func(ctx context.Context, server *RPCServer) error {
				_, err := server.backend.Put(ctx, &etcdserverpb.PutRequest{
					Key:   leaseAttachKey("/lease/bad-legacy"),
					Value: []byte("not-an-id"),
				})
				return err
			},
		},
		{
			name: "internal_attachment",
			write: func(ctx context.Context, server *RPCServer) error {
				return server.backend.InternalPut(ctx, leaseAttachKey("/lease/bad-internal"), []byte("not-an-id"))
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			server, closeFn := newTestRPCServer(t)
			defer closeFn()
			ctx := context.Background()
			require.NoError(t, test.write(ctx, server))

			_, _, err := server.loadLeaseRecords(ctx)
			require.ErrorContains(t, err, "decode lease attachment")
		})
	}
}

func TestLoadLeaseRecordsRejectsMalformedLeaseMetadata(t *testing.T) {
	tests := []struct {
		name  string
		raw   func(*testing.T, int64) []byte
		write func(context.Context, *RPCServer, int64, []byte) error
		want  string
	}{
		{
			name: "legacy_user_mvcc_unknown_field",
			raw:  leaseRecordWithUnknownField,
			write: func(ctx context.Context, server *RPCServer, id int64, raw []byte) error {
				_, err := server.backend.Put(ctx, &etcdserverpb.PutRequest{Key: leaseStorageKey(id), Value: raw})
				return err
			},
			want: "decode legacy lease metadata",
		},
		{
			name: "legacy_user_mvcc_trailing_json",
			raw:  leaseRecordWithTrailingJSON,
			write: func(ctx context.Context, server *RPCServer, id int64, raw []byte) error {
				_, err := server.backend.Put(ctx, &etcdserverpb.PutRequest{Key: leaseStorageKey(id), Value: raw})
				return err
			},
			want: "lease metadata contains trailing JSON",
		},
		{
			name: "internal_unknown_field",
			raw:  leaseRecordWithUnknownField,
			write: func(ctx context.Context, server *RPCServer, id int64, raw []byte) error {
				return server.backend.InternalPut(ctx, leaseStorageKey(id), raw)
			},
			want: "decode lease metadata",
		},
		{
			name: "internal_trailing_json",
			raw:  leaseRecordWithTrailingJSON,
			write: func(ctx context.Context, server *RPCServer, id int64, raw []byte) error {
				return server.backend.InternalPut(ctx, leaseStorageKey(id), raw)
			},
			want: "lease metadata contains trailing JSON",
		},
	}
	for index, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			server, closeFn := newTestRPCServer(t)
			defer closeFn()
			ctx := context.Background()
			id := int64(7100 + index)
			require.NoError(t, test.write(ctx, server, id, test.raw(t, id)))

			_, _, err := server.loadLeaseRecords(ctx)
			require.ErrorContains(t, err, test.want)
		})
	}
}

func leaseRecordWithUnknownField(t *testing.T, id int64) []byte {
	t.Helper()
	base := canonicalLeaseRecord(t, id)
	out := append([]byte(nil), base[:len(base)-1]...)
	return append(out, []byte(`,"unexpected":true}`)...)
}

func leaseRecordWithTrailingJSON(t *testing.T, id int64) []byte {
	t.Helper()
	return append(canonicalLeaseRecord(t, id), []byte(` {}`)...)
}

func canonicalLeaseRecord(t *testing.T, id int64) []byte {
	t.Helper()
	raw, err := json.Marshal(leaseRecord{ID: id, TTL: 30})
	require.NoError(t, err)
	return raw
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

func TestLeaseFollowerKeepAlivePreservesClientCancellation(t *testing.T) {
	ctrl := gomock.NewController(t)
	metrics := mock.NewMinimalMetrics(ctrl)
	kv := memkv.NewKvStorage()
	b := backend.NewBackend(kv, backend.Config{
		Identity:                "proxy-follower-keepalive-cancel-test-peer",
		EnableEtcdCompatibility: true,
	}, metrics)
	ctx, cancel := context.WithCancel(context.Background())
	forwarding := make(chan struct{})
	server := New(b, metrics, testPeerService{
		isLeader:     false,
		proxyEnabled: true,
		leaseKeepAliveFn: func(forwardCtx context.Context, req *etcdserverpb.LeaseKeepAliveRequest) (*etcdserverpb.LeaseKeepAliveResponse, error) {
			require.Equal(t, int64(7002), req.ID)
			close(forwarding)
			<-forwardCtx.Done()
			return nil, forwardCtx.Err()
		},
	})
	defer func() {
		server.stopLeases()
		require.NoError(t, kv.Close())
		ctrl.Finish()
	}()

	stream := &fakeLeaseKeepAliveServer{
		ctx: ctx, requests: []*etcdserverpb.LeaseKeepAliveRequest{{ID: 7002}},
	}
	done := make(chan error, 1)
	go func() { done <- server.LeaseKeepAlive(stream) }()
	<-forwarding
	cancel()

	require.Equal(t, codes.Canceled, status.Code(<-done))
	require.Empty(t, stream.sent)
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

func TestLeaseSnapshotIsUnavailableUntilReloadCompletes(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()

	ctx := context.Background()
	lease, err := server.LeaseGrant(ctx, &etcdserverpb.LeaseGrantRequest{TTL: 300, ID: 7101})
	require.NoError(t, err)

	server.PrepareLeaseReload()

	_, err = server.Put(ctx, &etcdserverpb.PutRequest{
		Key: []byte("/registry/leases/reloading"), Value: []byte("value"), Lease: lease.ID,
	})
	require.Equal(t, codes.Unavailable, status.Code(err))
	require.Contains(t, err.Error(), "lease state is reloading")

	_, err = server.LeaseTimeToLive(ctx, &etcdserverpb.LeaseTimeToLiveRequest{ID: lease.ID})
	require.Equal(t, codes.Unavailable, status.Code(err))

	_, err = server.LeaseGrant(ctx, &etcdserverpb.LeaseGrantRequest{TTL: 300, ID: 7102})
	require.Equal(t, codes.Unavailable, status.Code(err))

	require.NoError(t, server.ReloadLeases(ctx))
	_, err = server.Put(ctx, &etcdserverpb.PutRequest{
		Key: []byte("/registry/leases/reloading"), Value: []byte("value"), Lease: lease.ID,
	})
	require.NoError(t, err)
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

func TestLeaseCheckpointBoundsReloadAndRenewClearsIt(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()
	ctx := context.Background()
	const leaseID int64 = 8202
	grant, err := server.LeaseGrant(ctx, &etcdserverpb.LeaseGrantRequest{TTL: 600, ID: leaseID})
	require.NoError(t, err)
	require.Equal(t, int64(600), grant.TTL)

	server.leaseMu.Lock()
	st := server.leases[leaseID]
	require.NotNil(t, st)
	st.timer.Stop()
	st.checkpointTimer.Stop()
	st.deadline = time.Now().Add(240 * time.Second)
	server.leaseMu.Unlock()
	beforeRevision := server.backend.GetCurrentRevision()

	server.checkpointLease(leaseID)
	data, err := server.backend.InternalGet(ctx, leaseStorageKey(leaseID))
	require.NoError(t, err)
	var checkpoint leaseRecord
	require.NoError(t, json.Unmarshal(data, &checkpoint))
	require.InDelta(t, 240, checkpoint.RemainingTTL, 1)
	require.Equal(t, beforeRevision, server.backend.GetCurrentRevision(),
		"internal lease checkpoint must not advance user MVCC")

	records, attachments, err := server.loadLeaseRecords(ctx)
	require.NoError(t, err)
	server.applyLeaseRecords(records, attachments)
	restored, err := server.LeaseTimeToLive(ctx, &etcdserverpb.LeaseTimeToLiveRequest{ID: leaseID})
	require.NoError(t, err)
	require.InDelta(t, checkpoint.RemainingTTL, restored.TTL, 1,
		"reload must use checkpointed remaining TTL instead of the full grant")
	require.Equal(t, int64(600), restored.GrantedTTL)

	stream := &fakeLeaseKeepAliveServer{
		requests: []*etcdserverpb.LeaseKeepAliveRequest{{ID: leaseID}},
	}
	require.NoError(t, server.LeaseKeepAlive(stream))
	require.Len(t, stream.sent, 1)
	require.Equal(t, int64(600), stream.sent[0].TTL)
	data, err = server.backend.InternalGet(ctx, leaseStorageKey(leaseID))
	require.NoError(t, err)
	checkpoint = leaseRecord{}
	require.NoError(t, json.Unmarshal(data, &checkpoint))
	require.Zero(t, checkpoint.RemainingTTL)
	require.Equal(t, beforeRevision, server.backend.GetCurrentRevision(),
		"checkpoint clear must remain outside user MVCC")
}

func TestLeaseRenewClearsCommittedCheckpointAfterLostWriteResponse(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()
	ctx := context.Background()
	const leaseID int64 = 8203
	_, err := server.LeaseGrant(ctx, &etcdserverpb.LeaseGrantRequest{TTL: 600, ID: leaseID})
	require.NoError(t, err)

	server.leaseMu.Lock()
	st := server.leases[leaseID]
	require.NotNil(t, st)
	st.timer.Stop()
	st.checkpointTimer.Stop()
	st.deadline = time.Now().Add(240 * time.Second)
	server.leaseMu.Unlock()

	checkpointCtx, cancelCheckpoint := context.WithCancel(context.Background())
	ambiguous := &committedCheckpointErrorBackend{
		BackendShim: server.backend,
		leaseID:     leaseID,
		cancel:      cancelCheckpoint,
	}
	server.backend = ambiguous
	server.checkpointLeaseWithContext(checkpointCtx, leaseID)
	require.True(t, ambiguous.failed, "the checkpoint write must commit before its response is lost")
	require.ErrorIs(t, checkpointCtx.Err(), context.Canceled)

	stream := &fakeLeaseKeepAliveServer{
		requests: []*etcdserverpb.LeaseKeepAliveRequest{{ID: leaseID}},
	}
	require.NoError(t, server.LeaseKeepAlive(stream))
	require.Len(t, stream.sent, 1)
	require.Equal(t, int64(600), stream.sent[0].TTL)

	data, err := server.backend.InternalGet(ctx, leaseStorageKey(leaseID))
	require.NoError(t, err)
	var record leaseRecord
	require.NoError(t, json.Unmarshal(data, &record))
	require.Zero(t, record.RemainingTTL,
		"renew must clear a checkpoint whose commit was confirmed after its response was lost")
}

func TestLeaseMetadataFailedWriteRequiresExactReadback(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()
	ctx := context.Background()
	const leaseID int64 = 8204
	_, err := server.LeaseGrant(ctx, &etcdserverpb.LeaseGrantRequest{TTL: 600, ID: leaseID})
	require.NoError(t, err)

	writeErr := errors.New("lease metadata write rejected")
	server.backend = &rejectedLeaseMetadataBackend{
		BackendShim: server.backend,
		err:         writeErr,
	}
	err = server.persistLeaseCheckpoint(ctx, leaseID, 600, 240)
	require.ErrorIs(t, err, writeErr)
	require.ErrorContains(t, err, "lease metadata differs after failed write")
}

func TestSpreadLeaseExpiriesMatchesEtcdPromotionRate(t *testing.T) {
	const revokeRate = 10
	base := time.Now().Add(30 * time.Second)

	belowThreshold := make([]*leaseState, revokeRate-1)
	for i := range belowThreshold {
		belowThreshold[i] = &leaseState{id: int64(i + 1), deadline: base}
	}
	spreadLeaseExpiries(belowThreshold, revokeRate)
	for _, st := range belowThreshold {
		require.Equal(t, base, st.deadline,
			"etcd does not spread a recovered set smaller than the revoke rate")
	}

	leases := make([]*leaseState, revokeRate*10)
	for i := range leases {
		// Reverse IDs ensure the helper's deterministic tie-break also gets
		// exercised instead of inheriting input order.
		leases[i] = &leaseState{id: int64(len(leases) - i), deadline: base}
	}
	spreadLeaseExpiries(leases, revokeRate)
	sort.Slice(leases, func(i, j int) bool { return leases[i].deadline.Before(leases[j].deadline) })

	target := (3 * revokeRate) / 4
	for i := 0; i < target; i++ {
		require.Equal(t, base, leases[i].deadline)
	}
	require.GreaterOrEqual(t, leases[target].deadline.Sub(base), time.Second,
		"the first lease above etcd's 75%% target must move into the next window")
	require.GreaterOrEqual(t, leases[len(leases)-1].deadline.Sub(base), 14*time.Second,
		"a large promotion pile-up must be distributed over multiple seconds")

	buckets := make(map[int64]int)
	for _, st := range leases {
		bucket := int64(st.deadline.Sub(base) / time.Second)
		buckets[bucket]++
	}
	for second, count := range buckets {
		require.LessOrEqualf(t, count, revokeRate, "second %d exceeds the configured revoke rate", second)
	}
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
