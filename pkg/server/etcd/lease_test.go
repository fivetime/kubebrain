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
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"github.com/kubewharf/kubebrain/pkg/backend"
	"github.com/kubewharf/kubebrain/pkg/metrics"
	"github.com/kubewharf/kubebrain/pkg/metrics/mock"
	"github.com/kubewharf/kubebrain/pkg/storage"
	"github.com/kubewharf/kubebrain/pkg/storage/memkv"
)

type fakeLeaseKeepAliveServer struct {
	ctx      context.Context
	requests []*etcdserverpb.LeaseKeepAliveRequest
	sent     []*etcdserverpb.LeaseKeepAliveResponse
	sendErr  error
	onSend   func()
	recv     func() (*etcdserverpb.LeaseKeepAliveRequest, error)
}

type observingRevisionBackend struct {
	BackendShim
	observed chan<- struct{}
}

type blockingStartupLeaseBackend struct {
	BackendShim
	entered chan struct{}
	once    sync.Once
}

func (b *blockingStartupLeaseBackend) GetFollowerSnapshotTimestamp(ctx context.Context) (uint64, error) {
	b.once.Do(func() { close(b.entered) })
	<-ctx.Done()
	return 0, ctx.Err()
}

type blockingLeaseMetaBackend struct {
	BackendShim
	entered chan struct{}
	release chan struct{}
	once    sync.Once
}

type blockingLeaseCheckpointBackend struct {
	BackendShim
	leaseID int64
	entered chan struct{}
	release chan struct{}
	once    sync.Once
	mu      sync.Mutex
	blocked bool
}

type committedBlockingLeaseCheckpointBackend struct {
	BackendShim
	leaseID int64
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

type retryableFailedGrantCleanupBackend struct {
	BackendShim
	leaseID        int64
	cleanupAllowed atomic.Bool
	cleanupCalls   atomic.Int64
}

func (b *retryableFailedGrantCleanupBackend) InternalPutCorruptGuarded(context.Context, []byte, []byte) error {
	return errFakeDelete
}

func (b *retryableFailedGrantCleanupBackend) InternalCAS(ctx context.Context, ops []backend.InternalCASOp) error {
	for _, op := range ops {
		if op.Delete && string(op.Key) == string(leaseStorageKey(b.leaseID)) {
			b.cleanupCalls.Add(1)
			if !b.cleanupAllowed.Load() {
				return errFakeDelete
			}
		}
	}
	return b.BackendShim.InternalCAS(ctx, ops)
}

type uncertainLeaseRevokeBackend struct {
	BackendShim
	leaseID   int64
	commit    bool
	failed    bool
	retryGate <-chan struct{}
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
		return storage.NewErrUncertainResult(ctx.Err())
	}
	return storage.NewErrUncertainResult(errors.New("lease checkpoint response lost after commit"))
}

type rejectedLeaseMetadataBackend struct {
	BackendShim
	err error
}

type corruptGuardedLeaseMetadataBackend struct {
	BackendShim
	called bool
}

type rejectedGuardedLeaseMetadataBackend struct {
	BackendShim
	err error
}

type rejectedLeaseMetadataCASBackend struct {
	BackendShim
	err error
}

type committedUncertainLeaseMetadataCASBackend struct {
	BackendShim
	failed bool
}

func (b *corruptGuardedLeaseMetadataBackend) InternalPutCorruptGuarded(context.Context, []byte, []byte) error {
	b.called = true
	return backend.ErrCorruptAlarmActive
}

func (b *rejectedGuardedLeaseMetadataBackend) InternalPutCorruptGuarded(context.Context, []byte, []byte) error {
	return b.err
}

func (b *rejectedLeaseMetadataCASBackend) InternalCAS(context.Context, []backend.InternalCASOp) error {
	return b.err
}

func (b *committedUncertainLeaseMetadataCASBackend) InternalCAS(ctx context.Context, ops []backend.InternalCASOp) error {
	if err := b.BackendShim.InternalCAS(ctx, ops); err != nil {
		return err
	}
	if b.failed {
		return nil
	}
	b.failed = true
	return storage.NewErrUncertainResult(context.DeadlineExceeded)
}

func (b *rejectedLeaseMetadataBackend) InternalPut(context.Context, []byte, []byte) error {
	return b.err
}

type failedLeaseMetadataReadbackBackend struct {
	rejectedLeaseMetadataBackend
	readErr error
}

func (b *failedLeaseMetadataReadbackBackend) InternalGet(context.Context, []byte) ([]byte, error) {
	return nil, b.readErr
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
	targeted := false
	for _, op := range ops {
		if op.Delete && op.Internal && string(op.Key) == target {
			targeted = true
		}
		if !b.failed && targeted {
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
	if targeted && b.retryGate != nil {
		select {
		case <-b.retryGate:
		case <-ctx.Done():
			return nil, 0, nil, ctx.Err()
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

func (b *blockingLeaseMetaBackend) InternalPutCorruptGuarded(ctx context.Context, key, value []byte) error {
	if string(key) == string(leaseStorageKey(5201)) {
		b.once.Do(func() { close(b.entered) })
		select {
		case <-b.release:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return b.BackendShim.InternalPutCorruptGuarded(ctx, key, value)
}

func (b *blockingLeaseCheckpointBackend) block(ctx context.Context, key []byte) error {
	if string(key) != string(leaseStorageKey(b.leaseID)) {
		return nil
	}
	b.mu.Lock()
	if b.blocked {
		b.mu.Unlock()
		return nil
	}
	b.blocked = true
	b.mu.Unlock()
	b.once.Do(func() { close(b.entered) })
	select {
	case <-b.release:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (b *blockingLeaseCheckpointBackend) InternalPut(ctx context.Context, key, value []byte) error {
	if err := b.block(ctx, key); err != nil {
		return err
	}
	return b.BackendShim.InternalPut(ctx, key, value)
}

func (b *blockingLeaseCheckpointBackend) InternalCAS(ctx context.Context, ops []backend.InternalCASOp) error {
	for _, op := range ops {
		if err := b.block(ctx, op.Key); err != nil {
			return err
		}
	}
	return b.BackendShim.InternalCAS(ctx, ops)
}

func (b *committedBlockingLeaseCheckpointBackend) InternalCAS(ctx context.Context, ops []backend.InternalCASOp) error {
	err := b.BackendShim.InternalCAS(ctx, ops)
	for _, op := range ops {
		if string(op.Key) != string(leaseStorageKey(b.leaseID)) {
			continue
		}
		b.once.Do(func() { close(b.entered) })
		select {
		case <-b.release:
		case <-ctx.Done():
			return ctx.Err()
		}
		break
	}
	return err
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
	requireLeaseCanceled(t, <-done)
	close(unblockRecv)
}

func TestLeaseKeepAliveServerStreamFailureMetricsCountUnexpectedReceiveAndSendErrors(t *testing.T) {
	rec := &recordingMetrics{}
	manager := &leaseManager{srv: &RPCServer{metricCli: rec}}

	recvErr := errors.New("injected lease keepalive receive failure")
	err := manager.leaseKeepAlive(&fakeLeaseKeepAliveServer{
		ctx: context.Background(),
		recv: func() (*etcdserverpb.LeaseKeepAliveRequest, error) {
			return nil, recvErr
		},
	})
	require.ErrorIs(t, err, recvErr)

	sendErr := errors.New("injected lease keepalive send failure")
	err = manager.sendLeaseKeepAliveResponse(&fakeLeaseKeepAliveServer{
		ctx:     context.Background(),
		sendErr: sendErr,
	}, &etcdserverpb.LeaseKeepAliveResponse{})
	require.ErrorIs(t, err, sendErr)

	rec.mu.Lock()
	defer rec.mu.Unlock()
	require.Contains(t, rec.counters, recordedCounter{
		name:  "etcd.network.server_stream_failures_total",
		value: 1,
		tags: []metrics.T{
			metrics.Tag("Type", "receive"),
			metrics.Tag("API", "lease-keepalive"),
		},
	})
	require.Contains(t, rec.counters, recordedCounter{
		name:  "etcd.network.server_stream_failures_total",
		value: 1,
		tags: []metrics.T{
			metrics.Tag("Type", "send"),
			metrics.Tag("API", "lease-keepalive"),
		},
	})
}

func (f *fakeLeaseKeepAliveServer) Send(resp *etcdserverpb.LeaseKeepAliveResponse) error {
	if f.sendErr != nil {
		return f.sendErr
	}
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

	records, attachments, _, _, err := server.loadLeaseRecords(ctx)
	require.NoError(t, err)
	server.applyLeaseRecords(records, attachments)
	ttl, err := server.LeaseTimeToLive(ctx, &etcdserverpb.LeaseTimeToLiveRequest{ID: leaseID})
	require.NoError(t, err)
	require.Equal(t, int64(-1), ttl.TTL,
		"an empty revoked lease must not resurrect after leader reload")
}

func TestLeaseGrantPersistsMetadataThroughCorruptCommitGuard(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()
	ctx := context.Background()
	const leaseID int64 = 4503001
	guarded := &corruptGuardedLeaseMetadataBackend{BackendShim: server.backend}
	server.backend = guarded

	_, err := server.LeaseGrant(ctx, &etcdserverpb.LeaseGrantRequest{TTL: 300, ID: leaseID})
	requireDirectLeaseError(t, err, rpctypes.ErrGRPCCorrupt, codes.DataLoss, "etcdserver: corrupt cluster")
	require.True(t, guarded.called)
	_, err = guarded.InternalGet(ctx, leaseStorageKey(leaseID))
	require.ErrorIs(t, err, storage.ErrKeyNotFound)
	ttl, err := server.LeaseTimeToLive(ctx, &etcdserverpb.LeaseTimeToLiveRequest{ID: leaseID})
	require.NoError(t, err)
	require.Equal(t, int64(-1), ttl.TTL, "a rejected grant must not publish an in-memory lease")
}

func TestLeaseGrantCleanupDoesNotDeleteMetadataAfterCorruptActivation(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()
	ctx := context.Background()
	const (
		leaseID  int64  = 4505001
		memberID uint64 = 4505001
	)
	metadata, err := json.Marshal(leaseRecord{ID: leaseID, TTL: 300})
	require.NoError(t, err)
	require.NoError(t, server.backend.InternalPut(ctx, leaseStorageKey(leaseID), metadata))
	require.NoError(t, server.backend.ArmCorrupt(ctx, memberID))

	err = server.deleteLeaseState(ctx, leaseID)
	require.ErrorIs(t, err, backend.ErrCorruptAlarmActive)
	stored, err := server.backend.InternalGet(ctx, leaseStorageKey(leaseID))
	require.NoError(t, err)
	require.Equal(t, metadata, stored, "grant compensation ordered after CORRUPT must not delete lease metadata")

	removed, err := server.backend.DisarmCorrupt(ctx, memberID)
	require.NoError(t, err)
	require.True(t, removed)
	require.NoError(t, server.deleteLeaseState(ctx, leaseID))
	_, err = server.backend.InternalGet(ctx, leaseStorageKey(leaseID))
	require.ErrorIs(t, err, storage.ErrKeyNotFound)
}

func TestLeaseAttachmentRepairStopsWhileCorruptAlarmIsActive(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()
	ctx := context.Background()
	const (
		key      = "/registry/events/ns/corrupt-attachment"
		oldLease = int64(4506001)
		newLease = int64(4506002)
		memberID = uint64(4506001)
	)
	attachmentKey := leaseAttachKey(key)
	require.NoError(t, server.backend.InternalPut(ctx, attachmentKey, []byte(strconv.FormatInt(oldLease, 10))))
	require.NoError(t, server.backend.ArmCorrupt(ctx, memberID))

	err := server.attachKeyToStorage(ctx, newLease, key)
	require.ErrorIs(t, err, backend.ErrCorruptAlarmActive)
	stored, err := server.backend.InternalGet(ctx, attachmentKey)
	require.NoError(t, err)
	require.Equal(t, []byte(strconv.FormatInt(oldLease, 10)), stored,
		"repair ordered after CORRUPT must not rebind the attachment")

	err = server.detachKeyFromStorage(ctx, key)
	require.ErrorIs(t, err, backend.ErrCorruptAlarmActive)
	stored, err = server.backend.InternalGet(ctx, attachmentKey)
	require.NoError(t, err)
	require.Equal(t, []byte(strconv.FormatInt(oldLease, 10)), stored,
		"repair ordered after CORRUPT must not remove the attachment")

	removed, err := server.backend.DisarmCorrupt(ctx, memberID)
	require.NoError(t, err)
	require.True(t, removed)
	require.NoError(t, server.attachKeyToStorage(ctx, newLease, key))
	require.NoError(t, server.detachKeyFromStorage(ctx, key))
	_, err = server.backend.InternalGet(ctx, attachmentKey)
	require.ErrorIs(t, err, storage.ErrKeyNotFound)
}

func TestLegacyLeaseMigrationMetadataStopsWhileCorruptAlarmIsActive(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()
	ctx := context.Background()
	const (
		leaseID  = int64(4506003)
		memberID = uint64(4506003)
	)
	canonical, err := json.Marshal(leaseRecord{ID: leaseID, TTL: 300, RemainingTTL: 200})
	require.NoError(t, err)
	require.NoError(t, server.backend.InternalPut(ctx, leaseStorageKey(leaseID), canonical))
	require.NoError(t, server.backend.ArmCorrupt(ctx, memberID))

	err = server.persistMigratedLeaseMeta(ctx, leaseID, 300, 200)
	require.ErrorIs(t, err, backend.ErrCorruptAlarmActive)
	stored, err := server.backend.InternalGet(ctx, leaseStorageKey(leaseID))
	require.NoError(t, err)
	require.Equal(t, canonical, stored,
		"an identical pre-existing record must not disguise a definite CORRUPT rejection as success")

	// LeaseCheckpoint is an upstream internal maintenance request, not a
	// derived migration repair, and remains admissible while CORRUPT is active.
	require.NoError(t, server.persistLeaseCheckpoint(ctx, leaseID, 300, 199))
	stored, err = server.backend.InternalGet(ctx, leaseStorageKey(leaseID))
	require.NoError(t, err)
	var checkpoint leaseRecord
	require.NoError(t, json.Unmarshal(stored, &checkpoint))
	require.Equal(t, int64(199), checkpoint.RemainingTTL)
}

func TestLeaseMetadataReadbackCannotHideDefiniteWriteFence(t *testing.T) {
	for index, fenceErr := range []error{backend.ErrLeadershipFenced, backend.ErrRestorationFenced} {
		t.Run(fenceErr.Error(), func(t *testing.T) {
			server, closeFn := newTestRPCServer(t)
			defer closeFn()
			ctx := context.Background()
			leaseID := int64(4507001 + index)
			canonical, err := json.Marshal(leaseRecord{ID: leaseID, TTL: 300, RemainingTTL: 200})
			require.NoError(t, err)
			require.NoError(t, server.backend.InternalPut(ctx, leaseStorageKey(leaseID), canonical))
			server.backend = &rejectedGuardedLeaseMetadataBackend{BackendShim: server.backend, err: fenceErr}

			err = server.persistMigratedLeaseMeta(ctx, leaseID, 300, 200)
			require.ErrorIs(t, err, fenceErr,
				"identical bytes from an earlier term must not hide a definite write fence")
		})
	}
}

func TestLeaseCheckpointReadbackCannotHideDefiniteCASRejection(t *testing.T) {
	for index, rejectErr := range []error{
		backend.ErrLeadershipFenced,
		backend.ErrRestorationFenced,
		backend.ErrInternalWriteGuardConflict,
	} {
		t.Run(rejectErr.Error(), func(t *testing.T) {
			server, closeFn := newTestRPCServer(t)
			defer closeFn()
			ctx := context.Background()
			leaseID := int64(4508001 + index)
			// Pre-existing updated bytes could belong to an earlier checkpoint or
			// another safety epoch; they do not prove this rejected CAS committed.
			updated, err := json.Marshal(leaseRecord{ID: leaseID, TTL: 300})
			require.NoError(t, err)
			require.NoError(t, server.backend.InternalPut(ctx, leaseStorageKey(leaseID), updated))
			server.backend = &rejectedLeaseMetadataCASBackend{BackendShim: server.backend, err: rejectErr}

			err = server.persistLeaseCheckpointCAS(ctx, leaseID, 300, 200, 0)
			require.ErrorIs(t, err, rejectErr,
				"identical bytes must not hide a definite checkpoint CAS rejection")
		})
	}
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

func TestUncommittedUncertainLeaseRevokeFailsClosedUntilRetrySucceeds(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()
	ctx := context.Background()
	const leaseID int64 = 2052
	key := []byte("/registry/leases/uncommitted-revoke")
	_, err := server.LeaseGrant(ctx, &etcdserverpb.LeaseGrantRequest{TTL: 300, ID: leaseID})
	require.NoError(t, err)
	_, err = server.Put(ctx, &etcdserverpb.PutRequest{Key: key, Value: []byte("value"), Lease: leaseID})
	require.NoError(t, err)

	retryGate := make(chan struct{})
	shim := &uncertainLeaseRevokeBackend{
		BackendShim: server.backend,
		leaseID:     leaseID,
		retryGate:   retryGate,
	}
	server.backend = shim
	recorder := &recordingMetrics{}
	server.metricCli = recorder
	_, err = server.LeaseRevoke(ctx, &etcdserverpb.LeaseRevokeRequest{ID: leaseID})
	require.ErrorIs(t, err, storage.ErrUncertainResult)
	require.True(t, shim.failed)

	current, err := server.Range(ctx, &etcdserverpb.RangeRequest{Key: key})
	require.NoError(t, err)
	require.Len(t, current.Kvs, 1)
	_, err = shim.InternalGet(ctx, leaseStorageKey(leaseID))
	require.NoError(t, err)
	_, err = server.LeaseTimeToLive(ctx, &etcdserverpb.LeaseTimeToLiveRequest{ID: leaseID, Keys: true})
	require.ErrorIs(t, err, errLeaseRevokePending)
	_, err = server.LeaseLeases(ctx, &etcdserverpb.LeaseLeasesRequest{})
	require.ErrorIs(t, err, errLeaseRevokePending)
	_, err = server.refreshLease(ctx, leaseID)
	require.ErrorIs(t, err, errLeaseRevokePending)
	_, err = server.Put(ctx, &etcdserverpb.PutRequest{
		Key: []byte("/registry/leases/pending-revoke-new"), Value: []byte("value"), Lease: leaseID,
	})
	require.ErrorIs(t, err, errLeaseRevokePending)
	newKey, rangeErr := server.Range(ctx, &etcdserverpb.RangeRequest{Key: []byte("/registry/leases/pending-revoke-new")})
	require.NoError(t, rangeErr)
	require.Empty(t, newKey.Kvs)

	close(retryGate)
	require.Eventually(t, func() bool {
		ttl, ttlErr := server.LeaseTimeToLive(ctx, &etcdserverpb.LeaseTimeToLiveRequest{ID: leaseID})
		return ttlErr == nil && ttl.TTL == -1
	}, 5*time.Second, 10*time.Millisecond)
	current, err = server.Range(ctx, &etcdserverpb.RangeRequest{Key: key})
	require.NoError(t, err)
	require.Empty(t, current.Kvs)
	_, err = shim.InternalGet(ctx, leaseStorageKey(leaseID))
	require.ErrorIs(t, err, storage.ErrKeyNotFound)
	require.Contains(t, recorder.counters, recordedCounter{
		name: "lease.revoke_reconcile", value: 1, tags: []metrics.T{metrics.Tag("outcome", "retry")},
	})
	require.Contains(t, recorder.counters, recordedCounter{
		name: "lease.revoke_reconcile", value: 1, tags: []metrics.T{metrics.Tag("outcome", "success")},
	})
}

func TestUncertainLeaseRevokeRetryStopsAtAtomicCorruptFence(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()
	ctx := context.Background()
	const (
		leaseID  int64  = 2054
		memberID uint64 = 2054
	)
	key := []byte("/registry/leases/uncertain-revoke-corrupt")
	_, err := server.LeaseGrant(ctx, &etcdserverpb.LeaseGrantRequest{TTL: 300, ID: leaseID})
	require.NoError(t, err)
	_, err = server.Put(ctx, &etcdserverpb.PutRequest{Key: key, Value: []byte("value"), Lease: leaseID})
	require.NoError(t, err)

	base := server.backend
	retryGate := make(chan struct{})
	shim := &uncertainLeaseRevokeBackend{
		BackendShim: base,
		leaseID:     leaseID,
		retryGate:   retryGate,
	}
	server.backend = shim
	recorder := &recordingMetrics{}
	server.metricCli = recorder
	_, err = server.LeaseRevoke(ctx, &etcdserverpb.LeaseRevokeRequest{ID: leaseID})
	require.ErrorIs(t, err, storage.ErrUncertainResult)
	require.NoError(t, base.ArmCorrupt(ctx, memberID))
	close(retryGate)

	require.Eventually(t, func() bool {
		recorder.mu.Lock()
		defer recorder.mu.Unlock()
		count := 0
		for _, counter := range recorder.counters {
			if counter.name == "lease.revoke_reconcile" && counter.value == 1 &&
				len(counter.tags) == 1 && counter.tags[0] == metrics.Tag("outcome", "retry") {
				count++
			}
		}
		return count >= 2 // scheduled once, then rejected by the atomic CORRUPT guard
	}, 5*time.Second, 10*time.Millisecond)

	current, err := server.Range(ctx, &etcdserverpb.RangeRequest{Key: key})
	require.NoError(t, err)
	require.Len(t, current.Kvs, 1, "CORRUPT must fence the replayed user-key delete")
	_, err = base.InternalGet(ctx, leaseStorageKey(leaseID))
	require.NoError(t, err, "CORRUPT must fence the replayed lease metadata delete")
	_, err = base.InternalGet(ctx, leaseAttachKey(string(key)))
	require.NoError(t, err, "CORRUPT must fence the replayed attachment delete")
	_, err = server.LeaseTimeToLive(ctx, &etcdserverpb.LeaseTimeToLiveRequest{ID: leaseID})
	require.ErrorIs(t, err, errLeaseRevokePending,
		"the fenced lease must remain unusable while its revoke outcome is pending")

	removed, err := base.DisarmCorrupt(ctx, memberID)
	require.NoError(t, err)
	require.True(t, removed)
	require.Eventually(t, func() bool {
		ttl, ttlErr := server.LeaseTimeToLive(ctx, &etcdserverpb.LeaseTimeToLiveRequest{ID: leaseID})
		return ttlErr == nil && ttl.TTL == -1
	}, 5*time.Second, 10*time.Millisecond)
	current, err = server.Range(ctx, &etcdserverpb.RangeRequest{Key: key})
	require.NoError(t, err)
	require.Empty(t, current.Kvs)
	_, err = base.InternalGet(ctx, leaseStorageKey(leaseID))
	require.ErrorIs(t, err, storage.ErrKeyNotFound)
	_, err = base.InternalGet(ctx, leaseAttachKey(string(key)))
	require.ErrorIs(t, err, storage.ErrKeyNotFound)
}

func TestUncertainLeaseRevokeHandsOffAcrossLeadershipEpoch(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()
	ctx := context.Background()
	const leaseID int64 = 2053
	_, err := server.LeaseGrant(ctx, &etcdserverpb.LeaseGrantRequest{TTL: 300, ID: leaseID})
	require.NoError(t, err)
	recorder := &recordingMetrics{}
	server.metricCli = recorder
	var epoch atomic.Uint64
	epoch.Store(1)
	server.peers = testPeerService{
		isLeaderFn: func() bool { return true },
		epochFn:    func() (uint64, bool) { return epoch.Load(), true },
	}

	server.scheduleUncertainLeaseRevoke(leaseID, 1)
	epoch.Store(2)
	require.Eventually(t, func() bool {
		recorder.mu.Lock()
		defer recorder.mu.Unlock()
		for _, counter := range recorder.counters {
			if counter.name == "lease.revoke_reconcile" && counter.value == 1 &&
				len(counter.tags) == 1 && counter.tags[0] == metrics.Tag("outcome", "handoff") {
				return true
			}
		}
		return false
	}, 5*time.Second, 10*time.Millisecond)
	server.leaseMu.Lock()
	require.True(t, server.leases[leaseID].revokePending,
		"old epoch must leave the pending marker for authoritative reload")
	server.leaseMu.Unlock()
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
	requireDirectLeaseError(t, err, rpctypes.ErrGRPCLeaseNotFound, codes.NotFound, "etcdserver: requested lease not found")
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
		requireDirectLeaseError(t, err, rpctypes.ErrGRPCLeaseNotFound, codes.NotFound, "etcdserver: requested lease not found")
	}
	_, err := server.LeaseRevoke(ctx, &etcdserverpb.LeaseRevokeRequest{ID: 0})
	requireDirectLeaseError(t, err, rpctypes.ErrGRPCLeaseNotFound, codes.NotFound, "etcdserver: requested lease not found")
}

func TestLeaseSignedIDReadBoundariesMatchEtcd(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()
	ctx := context.Background()

	for _, id := range []int64{-1, math.MinInt64, math.MaxInt64} {
		t.Run(leaseTestName(id), func(t *testing.T) {
			key := []byte("/registry/leases/signed-read/" + leaseTestName(id))
			before, err := server.Range(ctx, &etcdserverpb.RangeRequest{Key: key})
			require.NoError(t, err)
			baseRevision := before.Header.Revision
			grant, err := server.LeaseGrant(ctx, &etcdserverpb.LeaseGrantRequest{ID: id, TTL: 300})
			require.NoError(t, err)
			require.Equal(t, id, grant.ID)
			require.Equal(t, baseRevision, grant.Header.Revision)
			put, err := server.Put(ctx, &etcdserverpb.PutRequest{Key: key, Value: []byte("value"), Lease: id})
			require.NoError(t, err)
			require.Equal(t, baseRevision+1, put.Header.Revision)

			withoutKeys, err := server.LeaseTimeToLive(ctx, &etcdserverpb.LeaseTimeToLiveRequest{ID: id})
			require.NoError(t, err)
			require.Equal(t, id, withoutKeys.ID)
			require.Positive(t, withoutKeys.TTL)
			require.LessOrEqual(t, withoutKeys.TTL, int64(300))
			require.Equal(t, int64(300), withoutKeys.GrantedTTL)
			require.Empty(t, withoutKeys.Keys)
			require.Equal(t, put.Header.Revision, withoutKeys.Header.Revision)

			withKeys, err := server.LeaseTimeToLive(ctx, &etcdserverpb.LeaseTimeToLiveRequest{ID: id, Keys: true})
			require.NoError(t, err)
			require.Equal(t, id, withKeys.ID)
			require.Positive(t, withKeys.TTL)
			require.LessOrEqual(t, withKeys.TTL, int64(300))
			require.Equal(t, int64(300), withKeys.GrantedTTL)
			require.ElementsMatch(t, [][]byte{key}, withKeys.Keys)
			require.Equal(t, put.Header.Revision, withKeys.Header.Revision)

			list, err := server.LeaseLeases(ctx, &etcdserverpb.LeaseLeasesRequest{})
			require.NoError(t, err)
			require.Contains(t, leaseIDsFromList(list), id)
			require.Equal(t, put.Header.Revision, list.Header.Revision)

			revoke, err := server.LeaseRevoke(ctx, &etcdserverpb.LeaseRevokeRequest{ID: id})
			require.NoError(t, err)
			require.Equal(t, put.Header.Revision+1, revoke.Header.Revision)
			unknown, err := server.LeaseTimeToLive(ctx, &etcdserverpb.LeaseTimeToLiveRequest{ID: id, Keys: true})
			require.NoError(t, err)
			require.Equal(t, id, unknown.ID)
			require.Equal(t, int64(-1), unknown.TTL)
			require.Zero(t, unknown.GrantedTTL)
			require.Empty(t, unknown.Keys)
			require.Equal(t, revoke.Header.Revision, unknown.Header.Revision)
			after, err := server.Range(ctx, &etcdserverpb.RangeRequest{Key: key})
			require.NoError(t, err)
			require.Empty(t, after.Kvs)
			require.Equal(t, revoke.Header.Revision, after.Header.Revision)
		})
	}
}

func leaseTestName(id int64) string {
	switch id {
	case -1:
		return "negative-one"
	case math.MinInt64:
		return "minimum"
	case math.MaxInt64:
		return "maximum"
	default:
		return "custom"
	}
}

func leaseIDsFromList(resp *etcdserverpb.LeaseLeasesResponse) []int64 {
	ids := make([]int64, 0, len(resp.Leases))
	for _, lease := range resp.Leases {
		ids = append(ids, lease.ID)
	}
	return ids
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
	requireLeaseCanceled(t, <-done)
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

func TestLeaseKeepAliveRecapturesEpochAfterLeaderStartupWait(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()
	const leaseID int64 = 10005
	_, err := server.LeaseGrant(context.Background(), &etcdserverpb.LeaseGrantRequest{TTL: 30, ID: leaseID})
	require.NoError(t, err)

	var epoch atomic.Uint64
	epoch.Store(1)
	var forwarded atomic.Int64
	server.peers = testPeerService{
		proxyEnabled: true,
		epochFn: func() (uint64, bool) {
			return epoch.Load(), true
		},
		leaseKeepAliveFn: func(context.Context, *etcdserverpb.LeaseKeepAliveRequest) (*etcdserverpb.LeaseKeepAliveResponse, error) {
			forwarded.Add(1)
			return &etcdserverpb.LeaseKeepAliveResponse{Header: txnHeader(1), ID: leaseID, TTL: 30}, nil
		},
	}
	server.PrepareLeaseReload()
	server.SetLeaderReady(false)
	observed := make(chan struct{}, 1)
	server.backend = observingRevisionBackend{BackendShim: server.backend, observed: observed}

	stream := &fakeLeaseKeepAliveServer{requests: []*etcdserverpb.LeaseKeepAliveRequest{{ID: leaseID}}}
	done := make(chan error, 1)
	go func() { done <- server.LeaseKeepAlive(stream) }()
	select {
	case <-observed:
	case <-time.After(time.Second):
		t.Fatal("keepalive did not enter the old leadership epoch")
	}

	// The same ingress replica wins a successor term while this already-received
	// stream message is parked behind startup. Once the new snapshot is ready it
	// must renew locally under term 2, not treat term 1 as a reason to self-proxy.
	epoch.Store(2)
	require.NoError(t, server.ReloadLeases(context.Background()))
	server.SetLeaderReady(true)

	require.NoError(t, <-done)
	require.Zero(t, forwarded.Load())
	require.Len(t, stream.sent, 1)
	require.Equal(t, leaseID, stream.sent[0].ID)
	require.Positive(t, stream.sent[0].TTL)
}

func TestLeaseGrantDuplicateAndTooLargeTTLMatchEtcdErrors(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()

	ctx := context.Background()
	_, err := server.LeaseGrant(ctx, &etcdserverpb.LeaseGrantRequest{TTL: 30, ID: 5001})
	require.NoError(t, err)

	_, err = server.LeaseGrant(ctx, &etcdserverpb.LeaseGrantRequest{TTL: 30, ID: 5001})
	requireDirectLeaseError(t, err, rpctypes.ErrGRPCLeaseExist, codes.FailedPrecondition, "etcdserver: lease already exists")

	_, err = server.LeaseGrant(ctx, &etcdserverpb.LeaseGrantRequest{TTL: maxLeaseTTL + 1, ID: 5001})
	requireDirectLeaseError(t, err, rpctypes.ErrGRPCLeaseTTLTooLarge, codes.OutOfRange, "etcdserver: too large lease TTL")

	_, err = server.LeaseGrant(ctx, &etcdserverpb.LeaseGrantRequest{TTL: maxLeaseTTL + 1, ID: 5002})
	requireDirectLeaseError(t, err, rpctypes.ErrGRPCLeaseTTLTooLarge, codes.OutOfRange, "etcdserver: too large lease TTL")
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

func TestLeaseGrantMaximumTTLAndAutomaticIDMatchEtcd(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()

	ctx := context.Background()
	maximum, err := server.LeaseGrant(ctx, &etcdserverpb.LeaseGrantRequest{TTL: maxLeaseTTL, ID: 5110})
	require.NoError(t, err)
	require.Equal(t, int64(5110), maximum.ID)
	require.Equal(t, maxLeaseTTL, maximum.TTL)
	maxTTL, err := server.LeaseTimeToLive(ctx, &etcdserverpb.LeaseTimeToLiveRequest{ID: maximum.ID})
	require.NoError(t, err)
	require.Equal(t, maxLeaseTTL, maxTTL.GrantedTTL)

	automaticRequest := &etcdserverpb.LeaseGrantRequest{TTL: 10}
	automatic, err := server.LeaseGrant(ctx, automaticRequest)
	require.NoError(t, err)
	require.NotZero(t, automatic.ID)
	require.Equal(t, automatic.ID, automaticRequest.ID)
	require.Equal(t, int64(10), automatic.TTL)
	autoTTL, err := server.LeaseTimeToLive(ctx, &etcdserverpb.LeaseTimeToLiveRequest{ID: automatic.ID})
	require.NoError(t, err)
	require.Equal(t, int64(10), autoTTL.GrantedTTL)
}

func TestLeaseGrantAutomaticIDRewritesBeforeAuthLikeEtcd(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()
	setupAuthKVUser(t, server)

	request := &etcdserverpb.LeaseGrantRequest{TTL: 30}
	response, err := server.LeaseGrant(context.Background(), request)
	require.Nil(t, response)
	requireAuthLeaseError(t, err, rpctypes.ErrUserEmpty, codes.Unknown, "etcdserver: user name is empty")
	require.Positive(t, request.ID)

	oversized := &etcdserverpb.LeaseGrantRequest{TTL: maxLeaseTTL + 1}
	response, err = server.LeaseGrant(context.Background(), oversized)
	require.Nil(t, response)
	requireAuthLeaseError(t, err, rpctypes.ErrUserEmpty, codes.Unknown, "etcdserver: user name is empty")
	require.Positive(t, oversized.ID,
		"upstream allocates an automatic ID before auth, while TTL validation remains behind auth/raft apply")
}

func TestLeaseGrantAutomaticIDRewritesBeforeTTLValidationLikeEtcd(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()

	request := &etcdserverpb.LeaseGrantRequest{TTL: maxLeaseTTL + 1}
	response, err := server.LeaseGrant(context.Background(), request)
	require.Nil(t, response)
	requireDirectLeaseError(t, err, rpctypes.ErrGRPCLeaseTTLTooLarge, codes.OutOfRange, "etcdserver: too large lease TTL")
	require.Positive(t, request.ID)
}

func TestLeaseGrantAutomaticIDRewritesBeforeNoSpaceLikeEtcd(t *testing.T) {
	server := newQuotaRPCServer(t, 0)
	ctx := context.Background()
	_, err := server.Alarm(ctx, &etcdserverpb.AlarmRequest{
		Action: etcdserverpb.AlarmRequest_ACTIVATE,
		Alarm:  etcdserverpb.AlarmType_NOSPACE,
	})
	require.NoError(t, err)

	request := &etcdserverpb.LeaseGrantRequest{TTL: 30}
	response, err := server.LeaseGrant(ctx, request)
	require.Nil(t, response)
	requireQuotaNoSpaceError(t, err)
	require.Positive(t, request.ID)
}

func TestLeaseGrantQuotaPreflightRejectsBeforeAutomaticIDLikeEtcd(t *testing.T) {
	server := newQuotaRPCServer(t, 6)
	ctx := context.Background()
	_, err := server.Put(ctx, &etcdserverpb.PutRequest{Key: []byte("key"), Value: []byte("123")})
	require.NoError(t, err)

	request := &etcdserverpb.LeaseGrantRequest{TTL: 30}
	response, err := server.LeaseGrant(ctx, request)
	require.Nil(t, response)
	requireQuotaNoSpaceError(t, err)
	require.Zero(t, request.ID,
		"upstream quotaLeaseServer rejects an unavailable request before EtcdServer allocates an automatic ID")
	alarms, err := server.Alarm(ctx, &etcdserverpb.AlarmRequest{Action: etcdserverpb.AlarmRequest_GET})
	require.NoError(t, err)
	require.Equal(t, []*etcdserverpb.AlarmMember{{
		MemberID: server.memberIDForPeerIdentity(server.backend.GetResourceLock().Identity()),
		Alarm:    etcdserverpb.AlarmType_NOSPACE,
	}}, alarms.Alarms)
}

func TestLeaseGrantAutomaticIDRewritesBeforeCorruptLikeEtcd(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()

	ctx := context.Background()
	_, err := server.Alarm(ctx, &etcdserverpb.AlarmRequest{
		Action:   etcdserverpb.AlarmRequest_ACTIVATE,
		Alarm:    etcdserverpb.AlarmType_CORRUPT,
		MemberID: 42,
	})
	require.NoError(t, err)

	request := &etcdserverpb.LeaseGrantRequest{TTL: 30}
	response, err := server.LeaseGrant(ctx, request)
	require.Nil(t, response)
	requireMaintenanceDirectError(t, err, rpctypes.ErrGRPCCorrupt, codes.DataLoss, "etcdserver: corrupt cluster")
	require.Positive(t, request.ID)
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
	recorder := &recordingMetrics{}
	server.metricCli = recorder
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
	require.Contains(t, recorder.counters, recordedCounter{
		name: "lease.background.failure", value: 1,
		tags: []metrics.T{metrics.Tag("operation", "expire_corrupt_deferred")},
	})

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

// TestSlowLeaseExpiryDoesNotBlockUnrelatedRenewal mirrors etcd lessor.Renew's
// per-lease deadline update: applying one expired lease's key deletion may be
// slow, but it must not consume another lease's entire short TTL budget.
func TestSlowLeaseExpiryDoesNotBlockUnrelatedRenewal(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	t.Cleanup(closeFn)
	ctx := context.Background()
	const expiredID, liveID int64 = 5152, 5153
	expiredKey := []byte("slow-expiry")

	_, err := server.LeaseGrant(ctx, &etcdserverpb.LeaseGrantRequest{TTL: 30, ID: expiredID})
	require.NoError(t, err)
	_, err = server.Put(ctx, &etcdserverpb.PutRequest{Key: expiredKey, Value: []byte("leased"), Lease: expiredID})
	require.NoError(t, err)
	_, err = server.LeaseGrant(ctx, &etcdserverpb.LeaseGrantRequest{TTL: 3, ID: liveID})
	require.NoError(t, err)

	server.leaseMu.Lock()
	expired := server.leases[expiredID]
	require.NotNil(t, expired)
	expired.deadline = time.Now().Add(-time.Second)
	expired.timer.Stop()
	server.leaseMu.Unlock()

	shim := &blockingLeaseMetaDeleteBackend{
		BackendShim: server.backend,
		key:         leaseStorageKey(expiredID),
		entered:     make(chan struct{}),
		release:     make(chan struct{}),
	}
	server.backend = shim
	t.Cleanup(func() {
		select {
		case <-shim.release:
		default:
			close(shim.release)
		}
	})

	expiryDone := make(chan struct{})
	go func() {
		server.expireLeaseWithContext(ctx, expiredID)
		close(expiryDone)
	}()
	<-shim.entered

	renewed := make(chan error, 1)
	stream := &fakeLeaseKeepAliveServer{
		ctx:      ctx,
		requests: []*etcdserverpb.LeaseKeepAliveRequest{{ID: liveID}},
	}
	go func() {
		renewed <- server.LeaseKeepAlive(stream)
	}()
	select {
	case renewErr := <-renewed:
		require.NoError(t, renewErr)
		require.Len(t, stream.sent, 1)
		require.Equal(t, liveID, stream.sent[0].ID)
		require.Equal(t, int64(3), stream.sent[0].TTL)
	case <-time.After(500 * time.Millisecond):
		t.Fatal("unrelated lease renewal blocked behind slow TiKV expiry deletion")
	}

	close(shim.release)
	select {
	case <-expiryDone:
	case <-time.After(time.Second):
		t.Fatal("lease expiry did not finish after releasing the backend")
	}
}

func TestLeaseRenewDoesNotBypassItsOwnSlowExpiry(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	t.Cleanup(closeFn)
	ctx := context.Background()
	const leaseID int64 = 5154
	key := []byte("own-slow-expiry")

	_, err := server.LeaseGrant(ctx, &etcdserverpb.LeaseGrantRequest{TTL: 30, ID: leaseID})
	require.NoError(t, err)
	_, err = server.Put(ctx, &etcdserverpb.PutRequest{Key: key, Value: []byte("leased"), Lease: leaseID})
	require.NoError(t, err)
	server.leaseMu.Lock()
	state := server.leases[leaseID]
	state.deadline = time.Now().Add(-time.Second)
	state.timer.Stop()
	server.leaseMu.Unlock()

	shim := &blockingLeaseMetaDeleteBackend{
		BackendShim: server.backend,
		key:         leaseStorageKey(leaseID),
		entered:     make(chan struct{}),
		release:     make(chan struct{}),
	}
	server.backend = shim
	t.Cleanup(func() {
		select {
		case <-shim.release:
		default:
			close(shim.release)
		}
	})
	expiryDone := make(chan struct{})
	go func() {
		server.expireLeaseWithContext(ctx, leaseID)
		close(expiryDone)
	}()
	<-shim.entered

	renewed := make(chan error, 1)
	go func() {
		_, renewErr := server.refreshLease(ctx, leaseID)
		renewed <- renewErr
	}()
	select {
	case err := <-renewed:
		t.Fatalf("same-lease renewal bypassed an expiry already deleting its keys: %v", err)
	case <-time.After(100 * time.Millisecond):
	}
	close(shim.release)
	requireDirectLeaseError(t, <-renewed, rpctypes.ErrGRPCLeaseNotFound, codes.NotFound,
		"etcdserver: requested lease not found")
	<-expiryDone
}

func TestLeaseRenewDoesNotBypassNonTeardownWriteFence(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()
	ctx := context.Background()
	const leaseID int64 = 5155
	_, err := server.LeaseGrant(ctx, &etcdserverpb.LeaseGrantRequest{TTL: 30, ID: leaseID})
	require.NoError(t, err)

	server.leaseWriteMu.Lock()
	renewed := make(chan error, 1)
	go func() {
		_, renewErr := server.refreshLease(ctx, leaseID)
		renewed <- renewErr
	}()
	select {
	case err := <-renewed:
		server.leaseWriteMu.Unlock()
		t.Fatalf("renewal bypassed a non-teardown lease write fence: %v", err)
	case <-time.After(100 * time.Millisecond):
	}
	server.leaseWriteMu.Unlock()
	require.NoError(t, <-renewed)
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

func TestAutomaticLeaseIDsAreScopedByStaticMember(t *testing.T) {
	now := time.Unix(1_700_000_000, 123_000_000)
	first := newLeaseManager(nil, 0)
	second := newLeaseManager(nil, 0)
	first.configureAutomaticLeaseIDs(1, now)
	second.configureAutomaticLeaseIDs(2, now)

	firstIDs := make(map[int64]struct{}, 1024)
	for range 1024 {
		id := first.nextLeaseID()
		require.Positive(t, id)
		firstIDs[id] = struct{}{}
	}
	require.Len(t, firstIDs, 1024)
	for range 1024 {
		id := second.nextLeaseID()
		require.Positive(t, id)
		_, collision := firstIDs[id]
		require.False(t, collision, "different members must not collide even with the same clock seed")
	}
}

func TestAutomaticLeaseMemberPrefixIsStableAcrossMemberOrder(t *testing.T) {
	members := []*etcdserverpb.Member{{ID: 900}, {ID: 100}, {ID: 500}}
	prefix, ok := automaticLeaseMemberPrefix(members, 500)
	require.True(t, ok)
	require.Equal(t, uint16(2), prefix)

	prefix, ok = automaticLeaseMemberPrefix([]*etcdserverpb.Member{{ID: 500}, {ID: 900}, {ID: 100}}, 500)
	require.True(t, ok)
	require.Equal(t, uint16(2), prefix)

	_, ok = automaticLeaseMemberPrefix(members, 777)
	require.False(t, ok)
	_, ok = automaticLeaseMemberPrefix([]*etcdserverpb.Member{{ID: 0}}, 0)
	require.False(t, ok)
	_, ok = automaticLeaseMemberPrefix([]*etcdserverpb.Member{{ID: 500}, {ID: 500}}, 500)
	require.False(t, ok)
}

func TestLeaseManagerInitializesWithoutPeerService(t *testing.T) {
	manager := newLeaseManager(&RPCServer{}, 42)
	t.Cleanup(manager.close)

	require.Equal(t, int64(42), manager.leaseID)
	require.True(t, manager.leaseReady.Load())
	require.Zero(t, manager.leaseReadyEpoch.Load())
}

func TestStaleLeaseTimerCallbacksCannotAdoptReloadedGeneration(t *testing.T) {
	server, b, cleanup := newLeaseTestServer(t)
	defer cleanup()
	ctx := context.Background()
	const leaseID int64 = 100031
	var leadershipEpoch atomic.Uint64
	leadershipEpoch.Store(1)
	server.peers = testPeerService{
		isLeaderFn: func() bool { return true },
		epochFn:    func() (uint64, bool) { return leadershipEpoch.Load(), true },
	}

	server.leaseMu.Lock()
	oldGeneration := server.leaseGeneration
	server.leaseGeneration++
	newGeneration := server.leaseGeneration
	st := &leaseState{
		id:       leaseID,
		ttl:      300,
		deadline: time.Now().Add(-time.Second),
		keys:     make(map[string]struct{}),
		revoked:  make(chan struct{}),
	}
	server.leases[leaseID] = st
	server.leaseMu.Unlock()
	meta, err := jsonMarshalLeaseRecord(leaseID, 300, nil)
	require.NoError(t, err)
	require.NoError(t, b.InternalPut(ctx, leaseStorageKey(leaseID), meta))

	// A callback may already be queued when applyLeaseRecords stops its timer.
	// It belongs to oldGeneration and must not revoke a same-ID lease loaded into
	// the successor snapshot.
	epoch, _ := server.peers.EpochAndLeadingFresh()
	server.expireLeaseGenerationWithContext(ctx, leaseID, oldGeneration, epoch)
	server.leaseMu.Lock()
	require.Same(t, st, server.leases[leaseID])
	require.Equal(t, newGeneration, server.leaseGeneration)
	st.deadline = time.Now().Add(100 * time.Second)
	server.leaseMu.Unlock()
	stored, err := b.InternalGet(ctx, leaseStorageKey(leaseID))
	require.NoError(t, err)
	require.Equal(t, meta, stored)

	// The same generation fence applies to queued checkpoint callbacks: they may
	// neither overwrite durable remaining TTL nor publish it into the new object.
	server.checkpointLeaseGenerationWithContext(ctx, leaseID, oldGeneration, epoch)
	server.leaseMu.Lock()
	require.Zero(t, st.remainingTTL)
	require.Same(t, st, server.leases[leaseID])
	server.leaseMu.Unlock()
	stored, err = b.InternalGet(ctx, leaseStorageKey(leaseID))
	require.NoError(t, err)
	require.Equal(t, meta, stored)

	// Generation and leadership epoch are independent fences. Even before the
	// successor has replaced the map, an old-term callback must not adopt the new
	// epoch token and mutate the still-current object.
	server.leaseMu.Lock()
	st.deadline = time.Now().Add(-time.Second)
	server.leaseMu.Unlock()
	leadershipEpoch.Store(2)
	server.expireLeaseGenerationWithContext(ctx, leaseID, newGeneration, epoch)
	server.checkpointLeaseGenerationWithContext(ctx, leaseID, newGeneration, epoch)
	server.leaseMu.Lock()
	require.Same(t, st, server.leases[leaseID])
	require.Zero(t, st.remainingTTL)
	server.leaseMu.Unlock()
	stored, err = b.InternalGet(ctx, leaseStorageKey(leaseID))
	require.NoError(t, err)
	require.Equal(t, meta, stored)
}

func TestLoadLeaseRecordsRejectsInvalidAttachmentMetadata(t *testing.T) {
	tests := []struct {
		name  string
		write func(context.Context, *RPCServer) error
		want  string
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
			want: `decode lease attachment for key "/lease/bad-legacy": strconv.ParseInt: parsing "not-an-id": invalid syntax`,
		},
		{
			name: "internal_attachment",
			write: func(ctx context.Context, server *RPCServer) error {
				return server.backend.InternalPut(ctx, leaseAttachKey("/lease/bad-internal"), []byte("not-an-id"))
			},
			want: `decode lease attachment for key "/lease/bad-internal": strconv.ParseInt: parsing "not-an-id": invalid syntax`,
		},
		{
			name: "reserved_zero_attachment",
			write: func(ctx context.Context, server *RPCServer) error {
				return server.backend.InternalPut(ctx, leaseAttachKey("/lease/zero"), []byte("0"))
			},
			want: `lease attachment for key "/lease/zero" references reserved id 0`,
		},
		{
			name: "empty_attachment_key",
			write: func(ctx context.Context, server *RPCServer) error {
				return server.backend.InternalPut(ctx, leaseAttachKey(""), []byte("1"))
			},
			want: `lease attachment has empty user key`,
		},
		{
			name: "noncanonical_attachment",
			write: func(ctx context.Context, server *RPCServer) error {
				return server.backend.InternalPut(ctx, leaseAttachKey("/lease/noncanonical"), []byte("01"))
			},
			want: `lease attachment for key "/lease/noncanonical" has noncanonical value "01"`,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			server, closeFn := newTestRPCServer(t)
			defer closeFn()
			ctx := context.Background()
			require.NoError(t, test.write(ctx, server))

			_, _, _, _, err := server.loadLeaseRecords(ctx)
			require.ErrorIs(t, err, errInvalidLeaseMetadata)
			require.EqualError(t, err, test.want)
		})
	}
}

func TestLeaseAttachmentRecordIncarnationEncoding(t *testing.T) {
	const incarnation = "0123456789abcdef0123456789abcdef"
	tests := []struct {
		name        string
		value       string
		wantID      int64
		wantVersion string
		wantErr     bool
	}{
		{name: "legacy", value: "17", wantID: 17},
		{name: "incarnated", value: "-17@" + incarnation, wantID: -17, wantVersion: incarnation},
		{name: "empty incarnation", value: "17@", wantErr: true},
		{name: "short incarnation", value: "17@0123456789abcdef", wantErr: true},
		{name: "uppercase incarnation", value: "17@0123456789ABCDEF0123456789ABCDEF", wantErr: true},
		{name: "multiple separators", value: "17@" + incarnation + "@x", wantErr: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			record, err := parseLeaseAttachmentRecordDetails("/lease/incarnation", []byte(test.value))
			if test.wantErr {
				require.ErrorIs(t, err, errInvalidLeaseMetadata)
				return
			}
			require.NoError(t, err)
			require.Equal(t, test.wantID, record.ID)
			require.Equal(t, test.wantVersion, record.Incarnation)
			require.Equal(t, test.value, string(encodeLeaseAttachmentRecord(record.ID, record.Incarnation)))
		})
	}
}

func TestLoadLeaseRecordsPreservesCanonicalNegativeLeaseIdentity(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()
	ctx := context.Background()
	data, err := json.Marshal(leaseRecord{ID: -1, TTL: 30})
	require.NoError(t, err)
	require.NoError(t, server.backend.InternalPut(ctx, leaseStorageKey(-1), data))
	require.NoError(t, server.backend.InternalPut(ctx, leaseAttachKey("/lease/negative"), []byte("-1")))

	records, attachments, _, _, err := server.loadLeaseRecords(ctx)
	require.NoError(t, err)
	require.Len(t, records, 1)
	require.Equal(t, int64(-1), records[0].ID)
	require.Equal(t, int64(-1), attachments["/lease/negative"])
}

func TestLoadLeaseRecordsRejectsAmbiguousLegacyKeyOwners(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()
	ctx := context.Background()
	const key = "/lease/ambiguous-legacy-owner"
	for _, id := range []int64{73_100, 73_101} {
		data, err := json.Marshal(leaseRecord{ID: id, TTL: 30, Keys: []string{key}})
		require.NoError(t, err)
		_, err = server.backend.Put(ctx, &etcdserverpb.PutRequest{Key: leaseStorageKey(id), Value: data})
		require.NoError(t, err)
	}

	_, _, _, _, err := server.loadLeaseRecords(ctx)
	require.ErrorIs(t, err, errInvalidLeaseMetadata)
	require.EqualError(t, err, `legacy lease key "/lease/ambiguous-legacy-owner" has conflicting owners 73100 and 73101`)
}

func TestLoadLeaseRecordsRejectsAmbiguousRetainedLegacyOwners(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()
	ctx := context.Background()
	const key = "/lease/ambiguous-retained-legacy-owner"
	for _, id := range []int64{73_102, 73_103} {
		legacy, err := json.Marshal(leaseRecord{ID: id, TTL: 30, Keys: []string{key}})
		require.NoError(t, err)
		_, err = server.backend.Put(ctx, &etcdserverpb.PutRequest{Key: leaseStorageKey(id), Value: legacy})
		require.NoError(t, err)
		canonical, err := json.Marshal(leaseRecord{ID: id, TTL: 60})
		require.NoError(t, err)
		require.NoError(t, server.backend.InternalPut(ctx, leaseStorageKey(id), canonical))
	}

	_, _, _, _, err := server.loadLeaseRecords(ctx)
	require.ErrorIs(t, err, errInvalidLeaseMetadata)
	require.EqualError(t, err, `legacy lease key "/lease/ambiguous-retained-legacy-owner" has conflicting owners 73102 and 73103`)
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
			want: `decode legacy lease metadata for key "\x00kubebrain/leases/7100": json: unknown field "unexpected"`,
		},
		{
			name: "legacy_user_mvcc_trailing_json",
			raw:  leaseRecordWithTrailingJSON,
			write: func(ctx context.Context, server *RPCServer, id int64, raw []byte) error {
				_, err := server.backend.Put(ctx, &etcdserverpb.PutRequest{Key: leaseStorageKey(id), Value: raw})
				return err
			},
			want: `decode legacy lease metadata for key "\x00kubebrain/leases/7101": lease metadata contains trailing JSON`,
		},
		{
			name: "internal_unknown_field",
			raw:  leaseRecordWithUnknownField,
			write: func(ctx context.Context, server *RPCServer, id int64, raw []byte) error {
				return server.backend.InternalPut(ctx, leaseStorageKey(id), raw)
			},
			want: `decode lease metadata: json: unknown field "unexpected"`,
		},
		{
			name: "internal_trailing_json",
			raw:  leaseRecordWithTrailingJSON,
			write: func(ctx context.Context, server *RPCServer, id int64, raw []byte) error {
				return server.backend.InternalPut(ctx, leaseStorageKey(id), raw)
			},
			want: `decode lease metadata: lease metadata contains trailing JSON`,
		},
		{
			name: "internal_oversized_ttl",
			raw: func(t *testing.T, id int64) []byte {
				value, err := json.Marshal(leaseRecord{ID: id, TTL: maxLeaseTTL + 1})
				require.NoError(t, err)
				return value
			},
			write: func(ctx context.Context, server *RPCServer, id int64, raw []byte) error {
				return server.backend.InternalPut(ctx, leaseStorageKey(id), raw)
			},
			want: `decode lease metadata: lease ttl 9000000001 exceeds maximum 9000000000`,
		},
		{
			name: "internal_reserved_zero_id",
			raw: func(t *testing.T, _ int64) []byte {
				value, err := json.Marshal(leaseRecord{ID: 0, TTL: 30})
				require.NoError(t, err)
				return value
			},
			write: func(ctx context.Context, server *RPCServer, id int64, raw []byte) error {
				return server.backend.InternalPut(ctx, leaseStorageKey(id), raw)
			},
			want: `decode lease metadata: lease id 0 is reserved for no lease`,
		},
		{
			name: "legacy_empty_attached_key",
			raw: func(t *testing.T, id int64) []byte {
				value, err := json.Marshal(leaseRecord{ID: id, TTL: 30, Keys: []string{""}})
				require.NoError(t, err)
				return value
			},
			write: func(ctx context.Context, server *RPCServer, id int64, raw []byte) error {
				_, err := server.backend.Put(ctx, &etcdserverpb.PutRequest{Key: leaseStorageKey(id), Value: raw})
				return err
			},
			want: `decode legacy lease metadata for key "\x00kubebrain/leases/7106": lease key 0 is empty`,
		},
		{
			name: "internal_oversized_remaining_ttl",
			raw: func(t *testing.T, id int64) []byte {
				value, err := json.Marshal(leaseRecord{ID: id, TTL: 30, RemainingTTL: maxLeaseTTL + 1})
				require.NoError(t, err)
				return value
			},
			write: func(ctx context.Context, server *RPCServer, id int64, raw []byte) error {
				return server.backend.InternalPut(ctx, leaseStorageKey(id), raw)
			},
			want: `decode lease metadata: lease remaining ttl 9000000001 exceeds maximum 9000000000`,
		},
	}
	for index, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			server, closeFn := newTestRPCServer(t)
			defer closeFn()
			ctx := context.Background()
			id := int64(7100 + index)
			require.NoError(t, test.write(ctx, server, id, test.raw(t, id)))

			_, _, _, _, err := server.loadLeaseRecords(ctx)
			require.ErrorIs(t, err, errInvalidLeaseMetadata)
			require.EqualError(t, err, test.want)
		})
	}
}

func TestLoadLeaseRecordsRejectsMismatchedStorageIdentity(t *testing.T) {
	tests := []struct {
		name  string
		write func(context.Context, *RPCServer, []byte) error
		want  string
	}{
		{
			name: "legacy user MVCC key",
			write: func(ctx context.Context, server *RPCServer, raw []byte) error {
				_, err := server.backend.Put(ctx, &etcdserverpb.PutRequest{Key: leaseStorageKey(7200), Value: raw})
				return err
			},
			want: "legacy lease metadata key id 7200 disagrees with payload id 7201",
		},
		{
			name: "internal key",
			write: func(ctx context.Context, server *RPCServer, raw []byte) error {
				return server.backend.InternalPut(ctx, leaseStorageKey(7200), raw)
			},
			want: "lease metadata key id 7200 disagrees with payload id 7201",
		},
		{
			name: "noncanonical internal key",
			write: func(ctx context.Context, server *RPCServer, raw []byte) error {
				return server.backend.InternalPut(ctx, append(append([]byte(nil), leaseStoragePrefix...), []byte("07201")...), raw)
			},
			want: `lease metadata key "\x00kubebrain/leases/07201" is not canonical`,
		},
	}
	raw := canonicalLeaseRecord(t, 7201)
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			server, closeFn := newTestRPCServer(t)
			defer closeFn()
			ctx := context.Background()
			require.NoError(t, test.write(ctx, server, raw))

			_, _, _, _, err := server.loadLeaseRecords(ctx)
			require.ErrorIs(t, err, errInvalidLeaseMetadata)
			require.ErrorContains(t, err, test.want)
		})
	}
}

func TestLoadLeaseRecordsKeepsInternalOverrideForMatchingLegacyIdentity(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()
	ctx := context.Background()
	const id int64 = 7220
	legacy, err := json.Marshal(leaseRecord{ID: id, TTL: 30, Keys: []string{"legacy-key"}})
	require.NoError(t, err)
	_, err = server.backend.Put(ctx, &etcdserverpb.PutRequest{Key: leaseStorageKey(id), Value: legacy})
	require.NoError(t, err)
	current, err := json.Marshal(leaseRecord{ID: id, TTL: 60})
	require.NoError(t, err)
	require.NoError(t, server.backend.InternalPut(ctx, leaseStorageKey(id), current))

	records, _, _, _, err := server.loadLeaseRecords(ctx)
	require.NoError(t, err)
	require.Equal(t, []leaseRecord{{
		ID: id, TTL: 60, LegacyStorage: true, LegacyKeys: []string{"legacy-key"},
	}}, records, "canonical values win while legacy cleanup metadata remains retryable")
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
	requireDirectLeaseError(t, err, rpctypes.ErrGRPCLeaseNotFound, codes.NotFound, "etcdserver: requested lease not found")
	_, err = server.LeaseGrant(ctx, &etcdserverpb.LeaseGrantRequest{TTL: 30, ID: 5201})
	requireDirectLeaseError(t, err, rpctypes.ErrGRPCLeaseExist, codes.FailedPrecondition, "etcdserver: lease already exists")

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

func TestLeaseGrantCleanupFailureRetainsReservationUntilRetrySucceeds(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()
	const leaseID int64 = 5203
	shim := &retryableFailedGrantCleanupBackend{BackendShim: server.backend, leaseID: leaseID}
	server.backend = shim
	recorder := &recordingMetrics{}
	server.metricCli = recorder

	_, err := server.LeaseGrant(context.Background(), &etcdserverpb.LeaseGrantRequest{TTL: 30, ID: leaseID})
	require.Error(t, err)
	require.GreaterOrEqual(t, shim.cleanupCalls.Load(), int64(1))

	server.leaseMu.Lock()
	_, pending := server.pendingLeases[leaseID]
	server.leaseMu.Unlock()
	require.True(t, pending, "failed compensation must retain the explicit ID reservation")
	_, err = server.LeaseGrant(context.Background(), &etcdserverpb.LeaseGrantRequest{TTL: 30, ID: leaseID})
	requireDirectLeaseError(t, err, rpctypes.ErrGRPCLeaseExist, codes.FailedPrecondition, "etcdserver: lease already exists")

	shim.cleanupAllowed.Store(true)
	require.Eventually(t, func() bool {
		server.leaseMu.Lock()
		defer server.leaseMu.Unlock()
		_, pending = server.pendingLeases[leaseID]
		return !pending
	}, 5*time.Second, 10*time.Millisecond)
	require.Contains(t, recorder.counters, recordedCounter{
		name: "lease.grant_cleanup", value: 1, tags: []metrics.T{metrics.Tag("outcome", "retry")},
	})
	require.Contains(t, recorder.counters, recordedCounter{
		name: "lease.grant_cleanup", value: 1, tags: []metrics.T{metrics.Tag("outcome", "success")},
	})
}

func TestPendingLeaseReleaseIsGenerationConditional(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()
	const leaseID int64 = 5204
	server.leaseMu.Lock()
	server.leaseGeneration = 2
	server.pendingLeases[leaseID] = 2
	server.leaseMu.Unlock()
	metadata := []byte(`{"id":5204,"ttl":30}`)
	require.NoError(t, server.backend.InternalPut(context.Background(), leaseStorageKey(leaseID), metadata))

	require.False(t, server.releasePendingLease(leaseID, 1))
	epoch, _ := server.peers.EpochAndLeadingFresh()
	server.compensateFailedLeaseGrant(context.Background(), leaseID, 1, epoch, []byte(`{"id":5204,"ttl":30}`))
	server.leaseMu.Lock()
	require.Equal(t, uint64(2), server.pendingLeases[leaseID])
	server.leaseMu.Unlock()
	stored, err := server.backend.InternalGet(context.Background(), leaseStorageKey(leaseID))
	require.NoError(t, err)
	require.Equal(t, metadata, stored, "an old generation must hand off without deleting newer metadata")
	require.True(t, server.releasePendingLease(leaseID, 2))
}

func TestFailedLeaseGrantCleanupDeletesOnlyExactIncarnation(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()
	ctx := context.Background()
	const leaseID int64 = 5205
	expected := []byte(`{"id":5205,"incarnation":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","ttl":30}`)
	replacement := []byte(`{"id":5205,"incarnation":"bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb","ttl":30}`)
	require.NoError(t, server.backend.InternalPut(ctx, leaseStorageKey(leaseID), replacement))

	resolved, err := server.deleteFailedLeaseGrantIfMatch(ctx, leaseID, expected)
	require.False(t, resolved)
	require.ErrorIs(t, err, errLeaseGrantCleanupSuperseded)
	stored, err := server.backend.InternalGet(ctx, leaseStorageKey(leaseID))
	require.NoError(t, err)
	require.Equal(t, replacement, stored)

	require.NoError(t, server.backend.InternalPut(ctx, leaseStorageKey(leaseID), expected))
	require.NoError(t, server.backend.ArmCorrupt(ctx, uint64(leaseID)))
	resolved, err = server.deleteFailedLeaseGrantIfMatch(ctx, leaseID, expected)
	require.False(t, resolved)
	require.ErrorIs(t, err, backend.ErrCorruptAlarmActive)
	stored, err = server.backend.InternalGet(ctx, leaseStorageKey(leaseID))
	require.NoError(t, err)
	require.Equal(t, expected, stored, "grant compensation must remain fail-closed under CORRUPT")
	removed, err := server.backend.DisarmCorrupt(ctx, uint64(leaseID))
	require.NoError(t, err)
	require.True(t, removed)

	resolved, err = server.deleteFailedLeaseGrantIfMatch(ctx, leaseID, expected)
	require.NoError(t, err)
	require.True(t, resolved)
	_, err = server.backend.InternalGet(ctx, leaseStorageKey(leaseID))
	require.ErrorIs(t, err, storage.ErrKeyNotFound)
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
	requireDirectLeaseStatusError(t, <-grantDone, codes.Unavailable, "write rejected: leadership changed during commit, retry on current leader")

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
	records, attachments, _, _, err := restored.loadLeaseRecords(ctx)
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

func TestLeaseExpiredCounterCountsOnlyNaturalExpiration(t *testing.T) {
	metricRecorder := &recordingMetrics{}
	kv := memkv.NewKvStorage()
	b := backend.NewBackend(kv, backend.Config{
		Identity:                "lease-expired-metric-peer",
		EnableEtcdCompatibility: true,
	}, metricRecorder)
	server := New(b, metricRecorder, testPeerService{isLeader: true})
	defer func() {
		server.stopLeases()
		require.NoError(t, kv.Close())
	}()

	countExpiredIncrements := func() int {
		metricRecorder.mu.Lock()
		defer metricRecorder.mu.Unlock()
		return countRecordedCounterValue(metricRecorder.counters, "etcd_debugging.server.lease_expired_total", 1)
	}

	ctx := context.Background()
	_, err := server.LeaseGrant(ctx, &etcdserverpb.LeaseGrantRequest{TTL: 30, ID: 11001})
	require.NoError(t, err)
	_, err = server.LeaseRevoke(ctx, &etcdserverpb.LeaseRevokeRequest{ID: 11001})
	require.NoError(t, err)
	require.Zero(t, countExpiredIncrements(), "explicit LeaseRevoke must not increment the natural-expiration counter")

	_, err = server.LeaseGrant(ctx, &etcdserverpb.LeaseGrantRequest{TTL: 1, ID: 11002})
	require.NoError(t, err)
	require.Eventually(t, func() bool {
		return countExpiredIncrements() == 1
	}, 3*time.Second, 100*time.Millisecond)
}

func TestLeaseLifecycleMetricsCountSuccessfulLeaderOperations(t *testing.T) {
	metricRecorder := &recordingMetrics{}
	kv := memkv.NewKvStorage()
	b := backend.NewBackend(kv, backend.Config{
		Identity:                "lease-lifecycle-metric-peer",
		EnableEtcdCompatibility: true,
	}, metricRecorder)
	server := New(b, metricRecorder, testPeerService{isLeader: true})
	defer func() {
		server.stopLeases()
		require.NoError(t, kv.Close())
	}()

	ctx := context.Background()
	grant, err := server.LeaseGrant(ctx, &etcdserverpb.LeaseGrantRequest{TTL: 30, ID: 12001})
	require.NoError(t, err)
	require.Equal(t, int64(30), grant.TTL)

	stream := &fakeLeaseKeepAliveServer{
		ctx:      ctx,
		requests: []*etcdserverpb.LeaseKeepAliveRequest{{ID: grant.ID}},
	}
	require.NoError(t, server.LeaseKeepAlive(stream))
	require.Len(t, stream.sent, 1)
	require.Equal(t, grant.ID, stream.sent[0].ID)
	require.Equal(t, int64(30), stream.sent[0].TTL)

	_, err = server.LeaseRevoke(ctx, &etcdserverpb.LeaseRevokeRequest{ID: grant.ID})
	require.NoError(t, err)

	metricRecorder.mu.Lock()
	defer metricRecorder.mu.Unlock()
	require.Equal(t, 1, countRecordedCounterValue(metricRecorder.counters, "etcd_debugging.lease.granted_total", 1))
	require.Equal(t, 1, countRecordedCounterValue(metricRecorder.counters, "etcd_debugging.lease.renewed_total", 1))
	require.Equal(t, 1, countRecordedCounterValue(metricRecorder.counters, "etcd_debugging.lease.revoked_total", 1))
	require.Contains(t, metricRecorder.histograms, recordedHistogram{
		name:  "etcd_debugging.lease.ttl_total",
		value: int64(30),
	})
}

func TestLeaseNaturalExpirationCountsRevokedLifecycleMetric(t *testing.T) {
	metricRecorder := &recordingMetrics{}
	kv := memkv.NewKvStorage()
	b := backend.NewBackend(kv, backend.Config{
		Identity:                "lease-expired-revoked-metric-peer",
		EnableEtcdCompatibility: true,
	}, metricRecorder)
	server := New(b, metricRecorder, testPeerService{isLeader: true})
	defer func() {
		server.stopLeases()
		require.NoError(t, kv.Close())
	}()

	_, err := server.LeaseGrant(context.Background(), &etcdserverpb.LeaseGrantRequest{TTL: 1, ID: 12002})
	require.NoError(t, err)
	require.Eventually(t, func() bool {
		metricRecorder.mu.Lock()
		defer metricRecorder.mu.Unlock()
		return countRecordedCounterValue(metricRecorder.counters, "etcd_debugging.server.lease_expired_total", 1) == 1 &&
			countRecordedCounterValue(metricRecorder.counters, "etcd_debugging.lease.revoked_total", 1) == 1
	}, 3*time.Second, 100*time.Millisecond)
}

func countRecordedCounterValue(counters []recordedCounter, name string, value interface{}) int {
	total := 0
	for _, counter := range counters {
		if counter.name == name && counter.value == value {
			total++
		}
	}
	return total
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

func TestLeaseStartupRestoreIsBoundedWhenPDIsUnavailable(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()
	recorder := &recordingMetrics{}
	server.metricCli = recorder
	backend := &blockingStartupLeaseBackend{
		BackendShim: server.backend,
		entered:     make(chan struct{}),
	}
	server.backend = backend

	start := time.Now()
	err := server.restoreLeasesAtStartup(context.Background(), 25*time.Millisecond)
	require.ErrorIs(t, err, context.DeadlineExceeded)
	require.Less(t, time.Since(start), time.Second)
	require.False(t, server.leaseReady.Load(), "a failed constructor restore must leave lease reads closed")
	require.Contains(t, recorder.counters, recordedCounter{name: "lease.startup_restore.failure", value: 1})
	select {
	case <-backend.entered:
	default:
		t.Fatal("startup restore did not attempt to obtain a PD snapshot timestamp")
	}
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
	automaticGrant := &etcdserverpb.LeaseGrantRequest{TTL: 30}
	response, err := server.LeaseGrant(ctx, automaticGrant)
	require.Nil(t, response)
	requireLeaseFollowerUnavailable(t, err, "lease grant error addr is follower-test-peer leader test-peer")
	require.Positive(t, automaticGrant.ID)

	_, err = server.LeaseGrant(ctx, &etcdserverpb.LeaseGrantRequest{TTL: 30, ID: 4001})
	requireLeaseFollowerUnavailable(t, err, "lease grant error addr is follower-test-peer leader test-peer")

	_, err = server.LeaseRevoke(ctx, &etcdserverpb.LeaseRevokeRequest{ID: 4001})
	requireLeaseFollowerUnavailable(t, err, "lease revoke error addr is follower-test-peer leader test-peer")

	stream := &fakeLeaseKeepAliveServer{
		requests: []*etcdserverpb.LeaseKeepAliveRequest{{ID: 4001}},
	}
	requireLeaseFollowerUnavailable(t, server.LeaseKeepAlive(stream), "lease keepalive error addr is follower-test-peer leader test-peer")
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
			require.Positive(t, req.ID)
			return &etcdserverpb.LeaseGrantResponse{Header: txnHeader(1), ID: req.ID, TTL: req.TTL}, nil
		},
	})
	defer func() {
		server.stopLeases()
		require.NoError(t, kv.Close())
		ctrl.Finish()
	}()

	request := &etcdserverpb.LeaseGrantRequest{TTL: 30}
	resp, err := server.LeaseGrant(context.Background(), request)
	require.NoError(t, err)
	require.True(t, called)
	require.Positive(t, request.ID)
	require.Equal(t, request.ID, resp.ID)
}

func TestLeaseFollowerRejectsInvalidUnaryProxyResults(t *testing.T) {
	for _, tc := range []struct {
		name      string
		rpc       string
		configure func(*testPeerService, string)
		invoke    func(*RPCServer) (any, error)
	}{
		{
			name: "grant", rpc: leaseProxyRPCGrant,
			configure: func(peers *testPeerService, shape string) {
				peers.leaseGrantFn = func(context.Context, *etcdserverpb.LeaseGrantRequest) (*etcdserverpb.LeaseGrantResponse, error) {
					if shape == "mixed" {
						return &etcdserverpb.LeaseGrantResponse{}, errors.New("mixed")
					}
					if shape == "missing_header" {
						return &etcdserverpb.LeaseGrantResponse{}, nil
					}
					return nil, nil
				}
			},
			invoke: func(server *RPCServer) (any, error) {
				return server.LeaseGrant(context.Background(), &etcdserverpb.LeaseGrantRequest{TTL: 30})
			},
		},
		{
			name: "revoke", rpc: leaseProxyRPCRevoke,
			configure: func(peers *testPeerService, shape string) {
				peers.leaseRevokeFn = func(context.Context, *etcdserverpb.LeaseRevokeRequest) (*etcdserverpb.LeaseRevokeResponse, error) {
					if shape == "mixed" {
						return &etcdserverpb.LeaseRevokeResponse{}, errors.New("mixed")
					}
					if shape == "missing_header" {
						return &etcdserverpb.LeaseRevokeResponse{}, nil
					}
					return nil, nil
				}
			},
			invoke: func(server *RPCServer) (any, error) {
				return server.LeaseRevoke(context.Background(), &etcdserverpb.LeaseRevokeRequest{ID: 1})
			},
		},
		{
			name: "time_to_live", rpc: leaseProxyRPCTimeToLive,
			configure: func(peers *testPeerService, shape string) {
				peers.leaseTTLFn = func(context.Context, *etcdserverpb.LeaseTimeToLiveRequest) (*etcdserverpb.LeaseTimeToLiveResponse, error) {
					if shape == "mixed" {
						return &etcdserverpb.LeaseTimeToLiveResponse{}, errors.New("mixed")
					}
					if shape == "missing_header" {
						return &etcdserverpb.LeaseTimeToLiveResponse{}, nil
					}
					return nil, nil
				}
			},
			invoke: func(server *RPCServer) (any, error) {
				return server.LeaseTimeToLive(context.Background(), &etcdserverpb.LeaseTimeToLiveRequest{ID: 1})
			},
		},
		{
			name: "leases", rpc: leaseProxyRPCLeases,
			configure: func(peers *testPeerService, shape string) {
				peers.leaseLeasesFn = func(context.Context, *etcdserverpb.LeaseLeasesRequest) (*etcdserverpb.LeaseLeasesResponse, error) {
					if shape == "mixed" {
						return &etcdserverpb.LeaseLeasesResponse{}, errors.New("mixed")
					}
					if shape == "missing_header" {
						return &etcdserverpb.LeaseLeasesResponse{}, nil
					}
					return nil, nil
				}
			},
			invoke: func(server *RPCServer) (any, error) {
				return server.LeaseLeases(context.Background(), &etcdserverpb.LeaseLeasesRequest{})
			},
		},
	} {
		for _, shape := range []string{"nil", "mixed", "missing_header"} {
			t.Run(tc.name+"/"+shape, func(t *testing.T) {
				server, closeFn := newTestRPCServer(t)
				defer closeFn()
				rec := &recordingMetrics{}
				server.metricCli = rec
				initLeaseProxyIntegrityMetrics(rec)
				peers := testPeerService{isLeader: false, proxyEnabled: true}
				tc.configure(&peers, shape)
				server.peers = peers

				response, err := tc.invoke(server)
				require.Nil(t, response)
				require.Equal(t, codes.DataLoss, status.Code(err))
				require.Equal(t, []interface{}{int64(0), 1}, recordedLeaseProxyIntegrityValues(rec, tc.rpc))
			})
		}
	}
}

func TestLeaseFollowerKeepAliveRejectsInvalidProxyResult(t *testing.T) {
	for _, shape := range []string{"nil", "mixed", "missing_header", "negative_revision", "mismatched_id", "negative_ttl"} {
		t.Run(shape, func(t *testing.T) {
			server, closeFn := newTestRPCServer(t)
			defer closeFn()
			rec := &recordingMetrics{}
			server.metricCli = rec
			initLeaseProxyIntegrityMetrics(rec)
			server.peers = testPeerService{
				isLeader: false, proxyEnabled: true,
				leaseKeepAliveFn: func(context.Context, *etcdserverpb.LeaseKeepAliveRequest) (*etcdserverpb.LeaseKeepAliveResponse, error) {
					if shape == "mixed" {
						return &etcdserverpb.LeaseKeepAliveResponse{}, errors.New("mixed")
					}
					if shape == "missing_header" {
						return &etcdserverpb.LeaseKeepAliveResponse{}, nil
					}
					if shape == "negative_revision" {
						return &etcdserverpb.LeaseKeepAliveResponse{Header: txnHeader(-1)}, nil
					}
					if shape == "mismatched_id" {
						return &etcdserverpb.LeaseKeepAliveResponse{Header: txnHeader(1), ID: 2, TTL: 30}, nil
					}
					if shape == "negative_ttl" {
						return &etcdserverpb.LeaseKeepAliveResponse{Header: txnHeader(1), ID: 1, TTL: -1}, nil
					}
					return nil, nil
				},
			}
			stream := &fakeLeaseKeepAliveServer{requests: []*etcdserverpb.LeaseKeepAliveRequest{{ID: 1}}}

			err := server.LeaseKeepAlive(stream)
			require.Equal(t, codes.DataLoss, status.Code(err))
			require.Empty(t, stream.sent)
			require.Equal(t, []interface{}{int64(0), 1},
				recordedLeaseProxyIntegrityValues(rec, leaseProxyRPCKeepAlive))
		})
	}
}

func TestLeaseFollowerRejectsMismatchedUnaryProxyResponseID(t *testing.T) {
	for _, test := range []struct {
		name      string
		rpc       string
		configure func(*testPeerService)
		invoke    func(*RPCServer) (any, error)
	}{
		{
			name: "grant", rpc: leaseProxyRPCGrant,
			configure: func(peers *testPeerService) {
				peers.leaseGrantFn = func(context.Context, *etcdserverpb.LeaseGrantRequest) (*etcdserverpb.LeaseGrantResponse, error) {
					return &etcdserverpb.LeaseGrantResponse{Header: txnHeader(1), ID: -2, TTL: 30}, nil
				}
			},
			invoke: func(server *RPCServer) (any, error) {
				return server.LeaseGrant(context.Background(), &etcdserverpb.LeaseGrantRequest{ID: -1, TTL: 30})
			},
		},
		{
			name: "time_to_live", rpc: leaseProxyRPCTimeToLive,
			configure: func(peers *testPeerService) {
				peers.leaseTTLFn = func(context.Context, *etcdserverpb.LeaseTimeToLiveRequest) (*etcdserverpb.LeaseTimeToLiveResponse, error) {
					return &etcdserverpb.LeaseTimeToLiveResponse{Header: txnHeader(1), ID: math.MaxInt64, TTL: -1}, nil
				}
			},
			invoke: func(server *RPCServer) (any, error) {
				return server.LeaseTimeToLive(context.Background(), &etcdserverpb.LeaseTimeToLiveRequest{ID: math.MinInt64})
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			server, closeFn := newTestRPCServer(t)
			defer closeFn()
			rec := &recordingMetrics{}
			server.metricCli = rec
			initLeaseProxyIntegrityMetrics(rec)
			peers := testPeerService{isLeader: false, proxyEnabled: true}
			test.configure(&peers)
			server.peers = peers

			response, err := test.invoke(server)
			require.Nil(t, response)
			require.Equal(t, codes.DataLoss, status.Code(err))
			require.ErrorContains(t, err, "for request ID")
			require.Equal(t, []interface{}{int64(0), 1}, recordedLeaseProxyIntegrityValues(rec, test.rpc))
		})
	}
}

func TestLeaseFollowerRejectsInvalidUnaryProxyPayload(t *testing.T) {
	for _, test := range []struct {
		name      string
		rpc       string
		configure func(*testPeerService)
		invoke    func(*RPCServer) (any, error)
		message   string
	}{
		{
			name: "grant non-positive ttl", rpc: leaseProxyRPCGrant,
			configure: func(peers *testPeerService) {
				peers.leaseGrantFn = func(context.Context, *etcdserverpb.LeaseGrantRequest) (*etcdserverpb.LeaseGrantResponse, error) {
					return &etcdserverpb.LeaseGrantResponse{Header: txnHeader(1), ID: -1, TTL: 0}, nil
				}
			},
			invoke: func(server *RPCServer) (any, error) {
				return server.LeaseGrant(context.Background(), &etcdserverpb.LeaseGrantRequest{ID: -1, TTL: 30})
			},
			message: "non-positive granted TTL 0",
		},
		{
			name: "ttl keys not requested", rpc: leaseProxyRPCTimeToLive,
			configure: func(peers *testPeerService) {
				peers.leaseTTLFn = func(context.Context, *etcdserverpb.LeaseTimeToLiveRequest) (*etcdserverpb.LeaseTimeToLiveResponse, error) {
					return &etcdserverpb.LeaseTimeToLiveResponse{Header: txnHeader(1), ID: math.MinInt64, TTL: 1, GrantedTTL: 30, Keys: [][]byte{[]byte("secret")}}, nil
				}
			},
			invoke: func(server *RPCServer) (any, error) {
				return server.LeaseTimeToLive(context.Background(), &etcdserverpb.LeaseTimeToLiveRequest{ID: math.MinInt64})
			},
			message: "returned keys when none were requested",
		},
		{
			name: "malformed ttl not found", rpc: leaseProxyRPCTimeToLive,
			configure: func(peers *testPeerService) {
				peers.leaseTTLFn = func(context.Context, *etcdserverpb.LeaseTimeToLiveRequest) (*etcdserverpb.LeaseTimeToLiveResponse, error) {
					return &etcdserverpb.LeaseTimeToLiveResponse{Header: txnHeader(1), ID: math.MinInt64, TTL: -1, GrantedTTL: 30}, nil
				}
			},
			invoke: func(server *RPCServer) (any, error) {
				return server.LeaseTimeToLive(context.Background(), &etcdserverpb.LeaseTimeToLiveRequest{ID: math.MinInt64, Keys: true})
			},
			message: "malformed not-found payload",
		},
		{
			name: "ttl empty attached key", rpc: leaseProxyRPCTimeToLive,
			configure: func(peers *testPeerService) {
				peers.leaseTTLFn = func(context.Context, *etcdserverpb.LeaseTimeToLiveRequest) (*etcdserverpb.LeaseTimeToLiveResponse, error) {
					return &etcdserverpb.LeaseTimeToLiveResponse{Header: txnHeader(1), ID: math.MinInt64, TTL: 1, GrantedTTL: 30, Keys: [][]byte{{}}}, nil
				}
			},
			invoke: func(server *RPCServer) (any, error) {
				return server.LeaseTimeToLive(context.Background(), &etcdserverpb.LeaseTimeToLiveRequest{ID: math.MinInt64, Keys: true})
			},
			message: "empty attached key",
		},
		{
			name: "ttl duplicate attached key", rpc: leaseProxyRPCTimeToLive,
			configure: func(peers *testPeerService) {
				peers.leaseTTLFn = func(context.Context, *etcdserverpb.LeaseTimeToLiveRequest) (*etcdserverpb.LeaseTimeToLiveResponse, error) {
					return &etcdserverpb.LeaseTimeToLiveResponse{Header: txnHeader(1), ID: math.MinInt64, TTL: 1, GrantedTTL: 30, Keys: [][]byte{[]byte("key"), []byte("key")}}, nil
				}
			},
			invoke: func(server *RPCServer) (any, error) {
				return server.LeaseTimeToLive(context.Background(), &etcdserverpb.LeaseTimeToLiveRequest{ID: math.MinInt64, Keys: true})
			},
			message: "duplicate attached key",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			server, closeFn := newTestRPCServer(t)
			defer closeFn()
			rec := &recordingMetrics{}
			server.metricCli = rec
			initLeaseProxyIntegrityMetrics(rec)
			peers := testPeerService{isLeader: false, proxyEnabled: true}
			test.configure(&peers)
			server.peers = peers

			response, err := test.invoke(server)
			require.Nil(t, response)
			require.Equal(t, codes.DataLoss, status.Code(err))
			require.ErrorContains(t, err, test.message)
			require.Equal(t, []interface{}{int64(0), 1}, recordedLeaseProxyIntegrityValues(rec, test.rpc))
		})
	}
}

func TestLeaseFollowerRejectsInvalidLeaseListProxyPayload(t *testing.T) {
	for _, test := range []struct {
		name    string
		leases  []*etcdserverpb.LeaseStatus
		message string
	}{
		{name: "nil status", leases: []*etcdserverpb.LeaseStatus{nil}, message: "nil lease status"},
		{name: "zero id", leases: []*etcdserverpb.LeaseStatus{{ID: 0}}, message: "reserved zero lease ID"},
		{name: "duplicate id", leases: []*etcdserverpb.LeaseStatus{{ID: -1}, {ID: -1}}, message: "duplicate lease ID -1"},
	} {
		t.Run(test.name, func(t *testing.T) {
			server, closeFn := newTestRPCServer(t)
			defer closeFn()
			rec := &recordingMetrics{}
			server.metricCli = rec
			initLeaseProxyIntegrityMetrics(rec)
			server.peers = testPeerService{
				isLeader: false, proxyEnabled: true,
				leaseLeasesFn: func(context.Context, *etcdserverpb.LeaseLeasesRequest) (*etcdserverpb.LeaseLeasesResponse, error) {
					return &etcdserverpb.LeaseLeasesResponse{Header: txnHeader(1), Leases: test.leases}, nil
				},
			}

			response, err := server.LeaseLeases(context.Background(), &etcdserverpb.LeaseLeasesRequest{})
			require.Nil(t, response)
			require.Equal(t, codes.DataLoss, status.Code(err))
			require.ErrorContains(t, err, test.message)
			require.Equal(t, []interface{}{int64(0), 1}, recordedLeaseProxyIntegrityValues(rec, leaseProxyRPCLeases))
		})
	}
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
			return &etcdserverpb.LeaseKeepAliveResponse{Header: txnHeader(1), ID: req.ID, TTL: 30}, nil
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

func TestLeaseFollowerKeepAliveWaitsForSuccessorLeader(t *testing.T) {
	ctrl := gomock.NewController(t)
	metrics := mock.NewMinimalMetrics(ctrl)
	kv := memkv.NewKvStorage()
	b := backend.NewBackend(kv, backend.Config{
		Identity:                "proxy-follower-keepalive-no-leader-test-peer",
		EnableEtcdCompatibility: true,
	}, metrics)
	var attempts atomic.Int32
	server := New(b, metrics, testPeerService{
		isLeader:     false,
		proxyEnabled: true,
		leaseKeepAliveFn: func(ctx context.Context, req *etcdserverpb.LeaseKeepAliveRequest) (*etcdserverpb.LeaseKeepAliveResponse, error) {
			require.Equal(t, int64(7002), req.ID)
			if attempts.Add(1) < 3 {
				return nil, rpctypes.ErrGRPCNoLeader
			}
			return &etcdserverpb.LeaseKeepAliveResponse{Header: txnHeader(1), ID: req.ID, TTL: 30}, nil
		},
	})
	defer func() {
		server.stopLeases()
		require.NoError(t, kv.Close())
		ctrl.Finish()
	}()

	stream := &fakeLeaseKeepAliveServer{
		requests: []*etcdserverpb.LeaseKeepAliveRequest{{ID: 7002}},
	}
	require.NoError(t, server.LeaseKeepAlive(stream))
	require.Equal(t, int32(3), attempts.Load())
	require.Len(t, stream.sent, 1)
	require.Equal(t, int64(30), stream.sent[0].TTL)
}

func TestLeaseFollowerKeepAliveReroutesConsumedMessageWhenIngressBecomesLeader(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()
	const leaseID int64 = 7003
	_, err := server.LeaseGrant(context.Background(), &etcdserverpb.LeaseGrantRequest{ID: leaseID, TTL: 30})
	require.NoError(t, err)

	epoch := server.leaseReadyEpoch.Load()
	var leading atomic.Bool
	var forwarded atomic.Int32
	server.peers = testPeerService{
		proxyEnabled: true,
		epochFn: func() (uint64, bool) {
			return epoch, leading.Load()
		},
		leaseKeepAliveFn: func(context.Context, *etcdserverpb.LeaseKeepAliveRequest) (*etcdserverpb.LeaseKeepAliveResponse, error) {
			forwarded.Add(1)
			leading.Store(true)
			return nil, rpctypes.ErrGRPCLeaderChanged
		},
	}

	stream := &fakeLeaseKeepAliveServer{requests: []*etcdserverpb.LeaseKeepAliveRequest{{ID: leaseID}}}
	require.NoError(t, server.LeaseKeepAlive(stream))
	require.Equal(t, int32(1), forwarded.Load())
	require.Len(t, stream.sent, 1)
	require.Equal(t, leaseID, stream.sent[0].ID)
	require.Positive(t, stream.sent[0].TTL)
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

	requireLeaseCanceled(t, <-done)
	require.Empty(t, stream.sent)
}

func TestLeaseKeepAliveRejectsDemotionWhileWaitingForRenewal(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()

	const leaseID = int64(7003)
	_, err := server.LeaseGrant(context.Background(), &etcdserverpb.LeaseGrantRequest{ID: leaseID, TTL: 30})
	require.NoError(t, err)

	var leading atomic.Bool
	leading.Store(true)
	routed := make(chan struct{})
	var routedOnce sync.Once
	server.peers = testPeerService{isLeaderFn: func() bool {
		isLeader := leading.Load()
		if isLeader {
			routedOnce.Do(func() { close(routed) })
		}
		return isLeader
	}}
	server.leaseManager.leaseCheckpointMu.Lock()
	stream := &fakeLeaseKeepAliveServer{requests: []*etcdserverpb.LeaseKeepAliveRequest{{ID: leaseID}}}
	done := make(chan error, 1)
	go func() { done <- server.LeaseKeepAlive(stream) }()
	<-routed
	leading.Store(false)
	server.leaseManager.leaseCheckpointMu.Unlock()

	requireLeaseFollowerUnavailable(t, <-done, "lease keepalive error addr is test-peer leader test-peer")
	require.Empty(t, stream.sent)
}

func TestLeaseCheckpointLockDoesNotSerializeUnrelatedLeases(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()

	unlockFirst := server.leaseManager.lockLeaseCheckpoint(7003)
	differentLease := make(chan struct{})
	go func() {
		unlock := server.leaseManager.lockLeaseCheckpoint(7004)
		close(differentLease)
		unlock()
	}()
	select {
	case <-differentLease:
	case <-time.After(time.Second):
		t.Fatal("an unrelated lease was serialized behind the held checkpoint lock")
	}

	sameLease := make(chan struct{})
	go func() {
		unlock := server.leaseManager.lockLeaseCheckpoint(7003)
		close(sameLease)
		unlock()
	}()
	select {
	case <-sameLease:
		t.Fatal("the same lease entered two checkpoint transitions concurrently")
	case <-time.After(50 * time.Millisecond):
	}
	unlockFirst()
	select {
	case <-sameLease:
	case <-time.After(time.Second):
		t.Fatal("the same lease did not resume after its checkpoint transition completed")
	}
}

func TestLeaseKeepAliveRejectsStaleOrChangedEpochWhileWaitingForRenewal(t *testing.T) {
	tests := []struct {
		name      string
		nextEpoch uint64
		nextFresh bool
	}{
		{name: "stale freshness", nextEpoch: 1, nextFresh: false},
		{name: "changed epoch", nextEpoch: 2, nextFresh: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			server, closeFn := newTestRPCServer(t)
			defer closeFn()
			const leaseID int64 = 70031
			_, err := server.LeaseGrant(context.Background(), &etcdserverpb.LeaseGrantRequest{ID: leaseID, TTL: 30})
			require.NoError(t, err)

			var epoch atomic.Uint64
			epoch.Store(1)
			var fresh atomic.Bool
			fresh.Store(true)
			routed := make(chan struct{})
			var routedOnce sync.Once
			server.peers = testPeerService{
				isLeaderFn: func() bool { return true },
				epochFn: func() (uint64, bool) {
					if fresh.Load() {
						routedOnce.Do(func() { close(routed) })
					}
					return epoch.Load(), fresh.Load()
				},
			}
			server.leaseReadyEpoch.Store(1)
			server.leaseMu.Lock()
			before := server.leases[leaseID].deadline
			server.leaseMu.Unlock()
			server.leaseCheckpointMu.Lock()
			stream := &fakeLeaseKeepAliveServer{requests: []*etcdserverpb.LeaseKeepAliveRequest{{ID: leaseID}}}
			done := make(chan error, 1)
			go func() { done <- server.LeaseKeepAlive(stream) }()
			<-routed
			epoch.Store(tt.nextEpoch)
			fresh.Store(tt.nextFresh)
			server.leaseCheckpointMu.Unlock()

			requireLeaseFollowerUnavailable(t, <-done, "lease keepalive error addr is test-peer leader test-peer")
			require.Empty(t, stream.sent)
			server.leaseMu.Lock()
			after := server.leases[leaseID].deadline
			server.leaseMu.Unlock()
			require.Equal(t, before, after, "a stale or superseded term must not extend its private lease deadline")
		})
	}
}

func TestLeaseWritesKeepInitialStaleLeadershipDecision(t *testing.T) {
	tests := []struct {
		name string
		op   string
		call func(*RPCServer) error
	}{
		{
			name: "grant",
			op:   "lease grant",
			call: func(server *RPCServer) error {
				_, err := server.LeaseGrant(context.Background(), &etcdserverpb.LeaseGrantRequest{ID: 70032, TTL: 30})
				return err
			},
		},
		{
			name: "revoke",
			op:   "lease revoke",
			call: func(server *RPCServer) error {
				_, err := server.LeaseRevoke(context.Background(), &etcdserverpb.LeaseRevokeRequest{ID: 70032})
				return err
			},
		},
		{
			name: "keepalive",
			op:   "lease keepalive",
			call: func(server *RPCServer) error {
				return server.LeaseKeepAlive(&fakeLeaseKeepAliveServer{
					requests: []*etcdserverpb.LeaseKeepAliveRequest{{ID: 70032}},
				})
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			server, closeFn := newTestRPCServer(t)
			defer closeFn()
			var checks atomic.Int32
			server.peers = testPeerService{
				isLeaderFn: func() bool { return true },
				epochFn: func() (uint64, bool) {
					if checks.Add(1) == 1 {
						return 1, false
					}
					return 2, true
				},
			}

			requireLeaseFollowerUnavailable(t, tt.call(server), tt.op+" error addr is test-peer leader test-peer")
			require.Equal(t, int32(1), checks.Load(),
				"routing must not turn an already-observed stale decision into an empty local success")
		})
	}
}

func TestLeaseKeepAliveProxiesDemotionWhileWaitingForRenewal(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()

	const leaseID = int64(7004)
	_, err := server.LeaseGrant(context.Background(), &etcdserverpb.LeaseGrantRequest{ID: leaseID, TTL: 30})
	require.NoError(t, err)

	var leading atomic.Bool
	leading.Store(true)
	routed := make(chan struct{})
	var routedOnce sync.Once
	proxied := make(chan struct{}, 1)
	server.peers = testPeerService{
		isLeaderFn: func() bool {
			isLeader := leading.Load()
			if isLeader {
				routedOnce.Do(func() { close(routed) })
			}
			return isLeader
		},
		proxyEnabled: true,
		leaseKeepAliveFn: func(_ context.Context, req *etcdserverpb.LeaseKeepAliveRequest) (*etcdserverpb.LeaseKeepAliveResponse, error) {
			proxied <- struct{}{}
			return &etcdserverpb.LeaseKeepAliveResponse{Header: txnHeader(1), ID: req.ID, TTL: 29}, nil
		},
	}
	server.leaseManager.leaseCheckpointMu.Lock()
	stream := &fakeLeaseKeepAliveServer{requests: []*etcdserverpb.LeaseKeepAliveRequest{{ID: leaseID}}}
	done := make(chan error, 1)
	go func() { done <- server.LeaseKeepAlive(stream) }()
	<-routed
	leading.Store(false)
	server.leaseManager.leaseCheckpointMu.Unlock()

	require.NoError(t, <-done)
	require.Len(t, stream.sent, 1)
	require.Equal(t, leaseID, stream.sent[0].ID)
	require.Equal(t, int64(29), stream.sent[0].TTL)
	select {
	case <-proxied:
	default:
		t.Fatal("keepalive was not forwarded after demotion")
	}
}

func requireLeaseCanceled(t *testing.T, err error) {
	t.Helper()
	require.Equal(t, codes.Canceled, status.Code(err))
	require.Equal(t, "context canceled", status.Convert(err).Message())
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
	followerServer.leaseMu.Lock()
	recovered := followerServer.leases[grantResp.ID]
	var recoveredExpiryTimer, recoveredCheckpointTimer *time.Timer
	if recovered != nil {
		recoveredExpiryTimer = recovered.timer
		recoveredCheckpointTimer = recovered.checkpointTimer
	}
	followerServer.leaseMu.Unlock()
	require.NotNil(t, recovered)
	require.Nil(t, recoveredExpiryTimer,
		"initAndRecover must not arm primary-only expiry work before promotion")
	require.Nil(t, recoveredCheckpointTimer,
		"initAndRecover must not arm primary-only checkpoint work before promotion")

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
	l1, err := oldLeader.LeaseGrant(ctx, &etcdserverpb.LeaseGrantRequest{TTL: 600, ID: 7001})
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
	newLeader.leaseMu.Lock()
	constructorExpiryTimer := newLeader.leases[l1.ID].timer
	constructorCheckpointTimer := newLeader.leases[l1.ID].checkpointTimer
	newLeader.leaseMu.Unlock()
	require.Nil(t, constructorExpiryTimer,
		"constructor recovery must wait for the leadership reload to promote leases")
	require.Nil(t, constructorCheckpointTimer)

	l2, err := oldLeader.LeaseGrant(ctx, &etcdserverpb.LeaseGrantRequest{TTL: 600, ID: 7002})
	require.NoError(t, err)

	// Before reload the new leader does not know L2 (etcd reports TTL=-1 for an
	// unknown lease, without error).
	pre, err := newLeader.LeaseTimeToLive(ctx, &etcdserverpb.LeaseTimeToLiveRequest{ID: l2.ID})
	require.NoError(t, err)
	require.Equal(t, int64(-1), pre.TTL, "new leader should not know L2 before reload")

	// Simulate leadership acquisition.
	require.NoError(t, newLeader.ReloadLeases(ctx))
	newLeader.leaseMu.Lock()
	promotedExpiryTimer := newLeader.leases[l1.ID].timer
	promotedCheckpointTimer := newLeader.leases[l1.ID].checkpointTimer
	newLeader.leaseMu.Unlock()
	require.NotNil(t, promotedExpiryTimer,
		"leadership reload must arm expiry work for the primary")
	require.NotNil(t, promotedCheckpointTimer)

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
	requireDirectLeaseStatusError(t, err, codes.Unavailable, "etcdserver: lease state is reloading")

	_, err = server.LeaseTimeToLive(ctx, &etcdserverpb.LeaseTimeToLiveRequest{ID: lease.ID})
	requireDirectLeaseStatusError(t, err, codes.Unavailable, "etcdserver: lease state is reloading")

	_, err = server.LeaseGrant(ctx, &etcdserverpb.LeaseGrantRequest{TTL: 300, ID: 7102})
	requireDirectLeaseStatusError(t, err, codes.Unavailable, "etcdserver: lease state is reloading")

	require.NoError(t, server.ReloadLeases(ctx))
	_, err = server.Put(ctx, &etcdserverpb.PutRequest{
		Key: []byte("/registry/leases/reloading"), Value: []byte("value"), Lease: lease.ID,
	})
	require.NoError(t, err)
}

func TestLeaseExpiryWaitsForCurrentEpochReload(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()
	ctx := context.Background()
	const leaseID int64 = 7103
	key := []byte("/registry/leases/pre-ready-expiry")

	_, err := server.LeaseGrant(ctx, &etcdserverpb.LeaseGrantRequest{TTL: 30, ID: leaseID})
	require.NoError(t, err)
	_, err = server.Put(ctx, &etcdserverpb.PutRequest{Key: key, Value: []byte("must-survive"), Lease: leaseID})
	require.NoError(t, err)

	server.PrepareLeaseReload()
	server.leaseMu.Lock()
	server.leases[leaseID].deadline = time.Now().Add(-time.Second)
	server.leaseMu.Unlock()
	server.expireLease(leaseID)

	beforeReady, err := server.Range(ctx, &etcdserverpb.RangeRequest{Key: key})
	require.NoError(t, err)
	require.Len(t, beforeReady.Kvs, 1,
		"a queued expiry callback must not mutate storage before ReloadLeases")

	require.NoError(t, server.ReloadLeases(ctx))
	server.leaseMu.Lock()
	server.leases[leaseID].deadline = time.Now().Add(-time.Second)
	server.leaseMu.Unlock()
	server.expireLease(leaseID)

	afterReady, err := server.Range(ctx, &etcdserverpb.RangeRequest{Key: key})
	require.NoError(t, err)
	require.Empty(t, afterReady.Kvs, "the current ready epoch must still expire the lease")
}

func TestLeaseCheckpointWaitsForCurrentEpochReload(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()
	ctx := context.Background()
	const leaseID int64 = 7104

	_, err := server.LeaseGrant(ctx, &etcdserverpb.LeaseGrantRequest{TTL: 600, ID: leaseID})
	require.NoError(t, err)
	server.PrepareLeaseReload()
	server.leaseMu.Lock()
	server.leases[leaseID].deadline = time.Now().Add(240 * time.Second)
	server.leaseMu.Unlock()
	server.checkpointLease(leaseID)

	data, err := server.backend.InternalGet(ctx, leaseStorageKey(leaseID))
	require.NoError(t, err)
	var beforeReady leaseRecord
	require.NoError(t, json.Unmarshal(data, &beforeReady))
	require.Zero(t, beforeReady.RemainingTTL,
		"a queued checkpoint callback must not persist before ReloadLeases")

	require.NoError(t, server.ReloadLeases(ctx))
	server.leaseMu.Lock()
	server.leases[leaseID].deadline = time.Now().Add(240 * time.Second)
	server.leaseMu.Unlock()
	server.checkpointLease(leaseID)

	data, err = server.backend.InternalGet(ctx, leaseStorageKey(leaseID))
	require.NoError(t, err)
	var afterReady leaseRecord
	require.NoError(t, json.Unmarshal(data, &afterReady))
	require.InDelta(t, 240, afterReady.RemainingTTL, 1,
		"the current ready epoch must still persist lease checkpoints")
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

func TestLeaseKeepAliveRevokeBufferBoundaryMatchesEtcd(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()

	ctx := context.Background()
	grantResp, err := server.LeaseGrant(ctx, &etcdserverpb.LeaseGrantRequest{TTL: 30, ID: 8102})
	require.NoError(t, err)
	key := []byte("/registry/leases/keepalive-revoke-buffer")
	_, err = server.Put(ctx, &etcdserverpb.PutRequest{
		Key: key, Value: []byte("value"), Lease: grantResp.ID,
	})
	require.NoError(t, err)

	stream := &fakeLeaseKeepAliveServer{requests: []*etcdserverpb.LeaseKeepAliveRequest{
		{ID: grantResp.ID},
		{ID: grantResp.ID},
		{ID: grantResp.ID},
	}}
	require.NoError(t, server.LeaseKeepAlive(stream))
	require.Len(t, stream.sent, 3)
	for _, response := range stream.sent {
		require.Equal(t, grantResp.ID, response.ID)
		require.Positive(t, response.TTL)
		require.NotNil(t, response.Header)
	}

	_, err = server.LeaseRevoke(ctx, &etcdserverpb.LeaseRevokeRequest{ID: grantResp.ID})
	require.NoError(t, err)
	current, err := server.Range(ctx, &etcdserverpb.RangeRequest{Key: key})
	require.NoError(t, err)
	require.Empty(t, current.Kvs)
	ttl, err := server.LeaseTimeToLive(ctx, &etcdserverpb.LeaseTimeToLiveRequest{ID: grantResp.ID})
	require.NoError(t, err)
	require.Equal(t, int64(-1), ttl.TTL)

	afterRevoke := &fakeLeaseKeepAliveServer{requests: []*etcdserverpb.LeaseKeepAliveRequest{{ID: grantResp.ID}}}
	require.NoError(t, server.LeaseKeepAlive(afterRevoke))
	require.Len(t, afterRevoke.sent, 1)
	require.Equal(t, grantResp.ID, afterRevoke.sent[0].ID)
	require.Zero(t, afterRevoke.sent[0].TTL)
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

	records, attachments, _, _, err := server.loadLeaseRecords(ctx)
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

func TestLeaseRecoveryClampsPersistedGrantedTTLToMinimumLikeEtcd(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()
	ctx := context.Background()

	records := []struct {
		record       leaseRecord
		maxRecovered int64
	}{
		{record: leaseRecord{ID: 82_030, TTL: math.MinInt64}, maxRecovered: minLeaseTTL},
		{record: leaseRecord{ID: 82_031, TTL: 0, RemainingTTL: 30}, maxRecovered: 30},
	}
	for _, record := range records {
		data, err := json.Marshal(record.record)
		require.NoError(t, err)
		require.NoError(t, server.backend.InternalPut(ctx, leaseStorageKey(record.record.ID), data))
	}

	require.NoError(t, server.ReloadLeases(ctx))
	for _, record := range records {
		response, err := server.LeaseTimeToLive(ctx, &etcdserverpb.LeaseTimeToLiveRequest{ID: record.record.ID})
		require.NoError(t, err)
		require.Equal(t, minLeaseTTL, response.GrantedTTL)
		require.Positive(t, response.TTL)
		require.LessOrEqual(t, response.TTL, record.maxRecovered)
	}
}

func TestLeaseRecoveryExtendsDeadlineByElectionWindowLikeEtcdPromote(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()
	ctx := context.Background()
	const leaseID int64 = 82_032
	const remainingTTL int64 = 7
	extension := 3 * time.Second
	server.SetLeasePromotionExtension(extension)
	record, err := json.Marshal(leaseRecord{ID: leaseID, TTL: 30, RemainingTTL: remainingTTL})
	require.NoError(t, err)
	require.NoError(t, server.backend.InternalPut(ctx, leaseStorageKey(leaseID), record))

	before := time.Now()
	require.NoError(t, server.ReloadLeases(ctx))

	server.leaseMu.Lock()
	deadline := server.leases[leaseID].deadline
	server.leaseMu.Unlock()
	require.WithinDuration(t, before.Add(time.Duration(remainingTTL)*time.Second+extension), deadline, 250*time.Millisecond,
		"promotion must preserve lease lifetime lost during the election window")
	ttl, err := server.LeaseTimeToLive(ctx, &etcdserverpb.LeaseTimeToLiveRequest{ID: leaseID})
	require.NoError(t, err)
	require.GreaterOrEqual(t, ttl.TTL, remainingTTL+int64(extension/time.Second)-1,
		"the public TTL must include the election window immediately after promotion")
	require.Equal(t, time.Duration(math.MaxInt64), leaseRecoveryDuration(maxLeaseTTL, time.Duration(math.MaxInt64)),
		"an extreme configured election window must saturate instead of wrapping the deadline into the past")
}

// TestLeaseRevokeCompletesWhileRenewCheckpointIsBlocked mirrors upstream
// f8f1074b4/TestLeaseRevokeDuringRenew. A slow remaining-TTL checkpoint must
// not make the higher-priority Revoke wait, and a renewal that resumes after
// Revoke completed must report lease-not-found rather than stale success.
func TestLeaseRevokeCompletesWhileRenewCheckpointIsBlocked(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()
	ctx := context.Background()
	const leaseID int64 = 9201

	_, err := server.LeaseGrant(ctx, &etcdserverpb.LeaseGrantRequest{TTL: 30, ID: leaseID})
	require.NoError(t, err)
	require.NoError(t, server.persistLeaseCheckpoint(ctx, leaseID, 30, 10))
	server.leaseMu.Lock()
	server.leases[leaseID].remainingTTL = 10
	server.leaseMu.Unlock()

	shim := &blockingLeaseCheckpointBackend{
		BackendShim: server.backend,
		leaseID:     leaseID,
		entered:     make(chan struct{}),
		release:     make(chan struct{}),
	}
	server.backend = shim
	defer func() {
		select {
		case <-shim.release:
		default:
			close(shim.release)
		}
	}()

	renewDone := make(chan error, 1)
	go func() {
		_, renewErr := server.refreshLease(ctx, leaseID)
		renewDone <- renewErr
	}()
	<-shim.entered

	revokeDone := make(chan error, 1)
	go func() {
		_, revokeErr := server.LeaseRevoke(ctx, &etcdserverpb.LeaseRevokeRequest{ID: leaseID})
		revokeDone <- revokeErr
	}()
	select {
	case revokeErr := <-revokeDone:
		require.NoError(t, revokeErr)
	case <-time.After(time.Second):
		t.Fatal("LeaseRevoke remained blocked behind a slow renewal checkpoint")
	}
	regenerated, err := server.LeaseGrant(ctx, &etcdserverpb.LeaseGrantRequest{TTL: 60, ID: leaseID})
	require.NoError(t, err)
	require.Equal(t, int64(60), regenerated.TTL)

	close(shim.release)
	requireDirectLeaseError(t, <-renewDone, rpctypes.ErrGRPCLeaseNotFound, codes.NotFound,
		"etcdserver: requested lease not found")
	ttl, err := server.LeaseTimeToLive(ctx, &etcdserverpb.LeaseTimeToLiveRequest{ID: leaseID})
	require.NoError(t, err)
	require.Equal(t, int64(60), ttl.GrantedTTL, "old renewal must not modify the regranted lease generation")
	require.Positive(t, ttl.TTL)
}

func TestLeaseRenewDoesNotSucceedWhenRevokeWinsAfterCheckpointCAS(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()
	ctx := context.Background()
	const leaseID int64 = 9202

	_, err := server.LeaseGrant(ctx, &etcdserverpb.LeaseGrantRequest{TTL: 30, ID: leaseID})
	require.NoError(t, err)
	require.NoError(t, server.persistLeaseCheckpoint(ctx, leaseID, 30, 10))
	server.leaseMu.Lock()
	server.leases[leaseID].remainingTTL = 10
	server.leaseMu.Unlock()

	shim := &committedBlockingLeaseCheckpointBackend{
		BackendShim: server.backend,
		leaseID:     leaseID,
		entered:     make(chan struct{}),
		release:     make(chan struct{}),
	}
	server.backend = shim
	defer func() {
		select {
		case <-shim.release:
		default:
			close(shim.release)
		}
	}()

	renewDone := make(chan error, 1)
	go func() {
		_, renewErr := server.refreshLease(ctx, leaseID)
		renewDone <- renewErr
	}()
	<-shim.entered
	_, err = server.LeaseRevoke(ctx, &etcdserverpb.LeaseRevokeRequest{ID: leaseID})
	require.NoError(t, err)
	close(shim.release)
	requireDirectLeaseError(t, <-renewDone, rpctypes.ErrGRPCLeaseNotFound, codes.NotFound,
		"etcdserver: requested lease not found")
	_, err = server.backend.InternalGet(ctx, leaseStorageKey(leaseID))
	require.ErrorIs(t, err, storage.ErrKeyNotFound, "completed Revoke must win over the committed checkpoint clear")
}

func TestLeaseKeepAliveRejectsDemotionDuringCheckpointClear(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()
	ctx := context.Background()
	const leaseID int64 = 9203

	_, err := server.LeaseGrant(ctx, &etcdserverpb.LeaseGrantRequest{TTL: 30, ID: leaseID})
	require.NoError(t, err)
	require.NoError(t, server.persistLeaseCheckpoint(ctx, leaseID, 30, 10))
	server.leaseMu.Lock()
	server.leases[leaseID].remainingTTL = 10
	server.leaseMu.Unlock()

	shim := &blockingLeaseCheckpointBackend{
		BackendShim: server.backend,
		leaseID:     leaseID,
		entered:     make(chan struct{}),
		release:     make(chan struct{}),
	}
	server.backend = shim
	defer func() {
		select {
		case <-shim.release:
		default:
			close(shim.release)
		}
	}()
	var leading atomic.Bool
	leading.Store(true)
	server.peers = testPeerService{isLeaderFn: leading.Load}

	stream := &fakeLeaseKeepAliveServer{requests: []*etcdserverpb.LeaseKeepAliveRequest{{ID: leaseID}}}
	done := make(chan error, 1)
	go func() { done <- server.LeaseKeepAlive(stream) }()
	<-shim.entered
	leading.Store(false)
	close(shim.release)

	requireLeaseFollowerUnavailable(t, <-done, "lease keepalive error addr is test-peer leader test-peer")
	require.Empty(t, stream.sent)
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
	writeErr := errors.New("lease metadata write rejected")
	uncertainWriteErr := storage.NewErrUncertainResult(writeErr)
	readErr := errors.New("lease metadata read rejected")
	tests := []struct {
		name    string
		wrap    func(BackendShim) BackendShim
		want    string
		wantErr error
	}{
		{
			name: "different_readback",
			wrap: func(base BackendShim) BackendShim {
				return &rejectedLeaseMetadataBackend{BackendShim: base, err: uncertainWriteErr}
			},
			want:    "uncertain error: lease metadata write rejected\nlease metadata differs after failed write",
			wantErr: writeErr,
		},
		{
			name: "readback_error",
			wrap: func(base BackendShim) BackendShim {
				return &failedLeaseMetadataReadbackBackend{
					rejectedLeaseMetadataBackend: rejectedLeaseMetadataBackend{BackendShim: base, err: uncertainWriteErr},
					readErr:                      readErr,
				}
			},
			want:    "uncertain error: lease metadata write rejected\ninspect lease metadata after failed write: lease metadata read rejected",
			wantErr: readErr,
		},
	}
	for index, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			server, closeFn := newTestRPCServer(t)
			defer closeFn()
			ctx := context.Background()
			leaseID := int64(8204 + index)
			_, err := server.LeaseGrant(ctx, &etcdserverpb.LeaseGrantRequest{TTL: 600, ID: leaseID})
			require.NoError(t, err)

			server.backend = test.wrap(server.backend)
			err = server.persistLeaseCheckpoint(ctx, leaseID, 600, 240)
			require.ErrorIs(t, err, writeErr)
			require.ErrorIs(t, err, test.wantErr)
			require.EqualError(t, err, test.want)
		})
	}
}

func TestLeaseCheckpointCASReconcilesOnlyExplicitUncertainCommit(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()
	ctx := context.Background()
	const leaseID int64 = 4509001
	expected, err := json.Marshal(leaseRecord{ID: leaseID, TTL: 300, RemainingTTL: 200})
	require.NoError(t, err)
	require.NoError(t, server.backend.InternalPut(ctx, leaseStorageKey(leaseID), expected))
	uncertain := &committedUncertainLeaseMetadataCASBackend{BackendShim: server.backend}
	server.backend = uncertain

	require.NoError(t, server.persistLeaseCheckpointCAS(ctx, leaseID, 300, 200, 0))
	require.True(t, uncertain.failed)
	stored, err := server.backend.InternalGet(ctx, leaseStorageKey(leaseID))
	require.NoError(t, err)
	var record leaseRecord
	require.NoError(t, json.Unmarshal(stored, &record))
	require.Zero(t, record.RemainingTTL)
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

func requireDirectLeaseError(t *testing.T, err error, want error, code codes.Code, message string) {
	t.Helper()
	require.EqualError(t, err, status.Error(code, message).Error())
	require.ErrorIs(t, err, want)
	require.Equal(t, code, status.Code(err))
	require.Equal(t, message, status.Convert(err).Message())
}

func requireDirectLeaseStatusError(t *testing.T, err error, code codes.Code, message string) {
	t.Helper()
	require.EqualError(t, err, status.Error(code, message).Error())
	require.Equal(t, code, status.Code(err))
	require.Equal(t, message, status.Convert(err).Message())
}
