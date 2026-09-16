package etcd

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/kubewharf/kubebrain/pkg/backend"
	"github.com/kubewharf/kubebrain/pkg/storage"
	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Unlike the upstream maintenance Snapshot implementation, the current local
// admission CAS rejects an overlapping request before its deadline. A bounded
// single-artifact implementation should let the caller cancel its wait without
// opening a second history scan or disrupting the active capture.
func TestSnapshotQueuedCallerDeadlineDoesNotRejectOrStartHistory(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()
	_, err := server.Put(context.Background(), &etcdserverpb.PutRequest{
		Key: []byte("snapshot-queue-seed"), Value: []byte("value"),
	})
	require.NoError(t, err)
	wrapped := &concurrentSnapshotAdmissionBackend{
		BackendShim: server.backend,
		started:     make(chan struct{}, 2), release: make(chan struct{}),
	}
	server.backend = wrapped
	firstCtx, cancelFirst := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancelFirst()
	firstDone := make(chan error, 1)
	go func() {
		firstDone <- server.Snapshot(&etcdserverpb.SnapshotRequest{}, &maintenanceSnapshotServer{ctx: firstCtx})
	}()
	select {
	case <-wrapped.started:
	case <-firstCtx.Done():
		t.Fatal("first snapshot did not enter its history scan")
	}
	secondCtx, cancelSecond := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancelSecond()
	secondErr := server.Snapshot(&etcdserverpb.SnapshotRequest{}, &maintenanceSnapshotServer{ctx: secondCtx})
	close(wrapped.release)
	require.NoError(t, <-firstDone, "waiting caller must not cancel the active capture")
	require.Equal(t, int64(1), wrapped.calls.Load(), "waiting caller must not allocate another history capture")
	require.True(t, errors.Is(secondErr, context.DeadlineExceeded) || status.Code(secondErr) == codes.DeadlineExceeded,
		"queued caller should reach its own deadline, not a local concurrency rejection: %v", secondErr)
}

func TestSnapshotQueuedCallerCapturesAfterActiveRequestFinishes(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()
	wrapped := &concurrentSnapshotAdmissionBackend{
		BackendShim: server.backend, started: make(chan struct{}, 2), release: make(chan struct{}),
	}
	server.backend = wrapped
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	firstDone, secondDone := make(chan error, 1), make(chan error, 1)
	go func() {
		firstDone <- server.Snapshot(&etcdserverpb.SnapshotRequest{}, &maintenanceSnapshotServer{ctx: ctx})
	}()
	select {
	case <-wrapped.started:
	case <-ctx.Done():
		t.Fatal("active capture did not start")
	}
	second := &maintenanceSnapshotServer{ctx: ctx}
	go func() { secondDone <- server.Snapshot(&etcdserverpb.SnapshotRequest{}, second) }()
	select {
	case err := <-secondDone:
		t.Fatalf("queued Snapshot returned while first capture held its slot: %v", err)
	case <-wrapped.started:
		t.Fatal("second history scan started while first capture held its slot")
	case <-time.After(100 * time.Millisecond):
	}
	close(wrapped.release)
	require.NoError(t, <-firstDone)
	require.NoError(t, <-secondDone)
	require.Equal(t, int64(2), wrapped.calls.Load())
	require.GreaterOrEqual(t, len(second.responses), 2)
	require.False(t, server.snapshotActive.Load())
}

func TestSnapshotAdmissionCancellationDoesNotConsumeSlot(t *testing.T) {
	var gate snapshotAdmission
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := gate.acquire(canceled)
	require.ErrorIs(t, err, context.Canceled)
	waited, err := gate.acquire(context.Background())
	require.NoError(t, err)
	require.False(t, waited)
	deadline, cancelDeadline := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancelDeadline()
	waited, err = gate.acquire(deadline)
	require.True(t, waited)
	require.ErrorIs(t, err, context.DeadlineExceeded)
	gate.release()
	waited, err = gate.acquire(context.Background())
	require.NoError(t, err)
	require.False(t, waited, "canceled waiter must not consume the next slot")
	gate.release()
}

func TestSnapshotQueuedCallerLosesAdminPermission(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()
	callerCtx := setupAuthKVUser(t, server)
	require.NoError(t, server.auth.userGrantRole(context.Background(), "alice", "root"))
	caller, err := server.authCallerFromContext(callerCtx)
	require.NoError(t, err)
	require.NoError(t, caller.adminError())
	require.NoError(t, server.snapshotAdmission.semaphore().Acquire(context.Background(), 1))
	var release sync.Once
	releaseSlot := func() { release.Do(server.snapshotAdmission.release) }
	defer releaseSlot()
	ctx, cancel := context.WithTimeout(callerCtx, 5*time.Second)
	defer cancel()
	stream := &maintenanceSnapshotServer{ctx: ctx}
	done := make(chan error, 1)
	go func() { done <- server.Snapshot(&etcdserverpb.SnapshotRequest{}, stream) }()
	select {
	case err := <-done:
		t.Fatalf("authorized queued request returned before slot release: %v", err)
	case <-time.After(100 * time.Millisecond):
	}
	require.NoError(t, server.auth.userRevokeRole(context.Background(), "alice", "root"))
	releaseSlot()
	err = <-done
	require.ErrorIs(t, err, rpctypes.ErrPermissionDenied)
	require.Empty(t, stream.responses, "revoked caller must receive no snapshot bytes")
	require.False(t, server.snapshotActive.Load())
}

func TestSnapshotQueuedCallerDetectsLeadershipLoss(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()
	require.NoError(t, server.snapshotAdmission.semaphore().Acquire(context.Background(), 1))
	var release sync.Once
	releaseSlot := func() { release.Do(server.snapshotAdmission.release) }
	defer releaseSlot()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	stream := &maintenanceSnapshotServer{ctx: ctx}
	done := make(chan error, 1)
	// Epoch changes are synchronized through the admission semaphore release.
	var leading atomic.Bool
	leading.Store(true)
	server.peers = testPeerService{isLeader: true, epochFn: func() (uint64, bool) { return 7, leading.Load() }}
	go func() { done <- server.Snapshot(&etcdserverpb.SnapshotRequest{}, stream) }()
	select {
	case err := <-done:
		t.Fatalf("queued request returned before slot release: %v", err)
	case <-time.After(100 * time.Millisecond):
	}
	leading.Store(false)
	releaseSlot()
	require.ErrorIs(t, <-done, rpctypes.ErrGRPCLeaderChanged)
	require.Empty(t, stream.responses)
	require.False(t, server.snapshotActive.Load())
}

func TestSnapshotQueuedAnonymousCallerSeesAuthEnable(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()
	setupAuthKVUser(t, server)
	require.NoError(t, server.auth.disable(context.Background()))
	require.NoError(t, server.snapshotAdmission.semaphore().Acquire(context.Background(), 1))
	var release sync.Once
	releaseSlot := func() { release.Do(server.snapshotAdmission.release) }
	defer releaseSlot()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	stream := &maintenanceSnapshotServer{ctx: ctx}
	done := make(chan error, 1)
	go func() { done <- server.Snapshot(&etcdserverpb.SnapshotRequest{}, stream) }()
	select {
	case err := <-done:
		t.Fatalf("anonymous request returned before slot release while auth disabled: %v", err)
	case <-time.After(100 * time.Millisecond):
	}
	require.NoError(t, server.auth.enable(context.Background()))
	releaseSlot()
	require.ErrorIs(t, <-done, rpctypes.ErrUserEmpty)
	require.Empty(t, stream.responses)
	require.False(t, server.snapshotActive.Load())
}

type expiringQueuedSnapshotBackend struct {
	*availableSnapshotCheckpointBackend
	expired atomic.Bool
	reads   atomic.Int64
}

func (b *expiringQueuedSnapshotBackend) GetSerializableCheckpoint() (backend.SerializableCheckpoint, error) {
	b.reads.Add(1)
	if b.expired.Load() {
		return backend.SerializableCheckpoint{}, backend.ErrSerializableCheckpointUnavailable
	}
	return b.availableSnapshotCheckpointBackend.GetSerializableCheckpoint()
}

func TestSnapshotQueuedCallerRechecksProtectedCheckpoint(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()
	ctx, cancel := context.WithTimeout(storage.WithSnapshotIteratorFallback(context.Background()), 5*time.Second)
	defer cancel()
	_, err := server.Put(ctx, &etcdserverpb.PutRequest{Key: []byte("checkpoint-queue"), Value: []byte("value")})
	require.NoError(t, err)
	ts, err := server.backend.GetFollowerSnapshotTimestamp(ctx)
	require.NoError(t, err)
	shim := &expiringQueuedSnapshotBackend{availableSnapshotCheckpointBackend: &availableSnapshotCheckpointBackend{
		BackendShim: server.backend,
		checkpoint:  backend.SerializableCheckpoint{Revision: server.backend.GetCurrentRevision(), Timestamp: ts, ValidUntil: time.Now().Add(time.Minute)},
		probeErr:    status.Error(codes.Unavailable, "PD unavailable"),
	}}
	server.backend = shim
	server.tokens.snapshots = newAuthSnapshotCache(shim)
	require.NoError(t, server.snapshotAdmission.semaphore().Acquire(context.Background(), 1))
	var release sync.Once
	releaseSlot := func() { release.Do(server.snapshotAdmission.release) }
	defer releaseSlot()
	stream := &maintenanceSnapshotServer{ctx: ctx}
	done := make(chan error, 1)
	go func() { done <- server.Snapshot(&etcdserverpb.SnapshotRequest{}, stream) }()
	require.Eventually(t, func() bool { return shim.reads.Load() == 1 }, time.Second, time.Millisecond)
	select {
	case err := <-done:
		t.Fatalf("queued checkpoint request returned before slot release: %v", err)
	case <-time.After(100 * time.Millisecond):
	}
	shim.expired.Store(true)
	releaseSlot()
	err = <-done
	require.Equal(t, codes.Unavailable, status.Code(err))
	require.ErrorContains(t, err, backend.ErrSerializableCheckpointUnavailable.Error())
	require.Equal(t, int64(2), shim.reads.Load())
	require.Empty(t, stream.responses)
	require.False(t, server.snapshotActive.Load())
}
