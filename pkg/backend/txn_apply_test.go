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

package backend

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"

	proto "github.com/kubewharf/kubebrain-client/api/v2rpc"

	"github.com/kubewharf/kubebrain/pkg/metrics/mock"
	"github.com/kubewharf/kubebrain/pkg/storage"
	imemkv "github.com/kubewharf/kubebrain/pkg/storage/memkv"
)

type commitThenUncertainStorage struct {
	storage.KvStorage
	trigger                 atomic.Bool
	failReadsAfterUncertain int32
	remainingReadFailures   atomic.Int32
	blockReads              atomic.Bool
	readBlocked             chan struct{}
	releaseReads            chan struct{}
	readBlockedOnce         sync.Once
}

func (s *commitThenUncertainStorage) BeginBatchWrite() storage.BatchWrite {
	return &commitThenUncertainBatch{BatchWrite: s.KvStorage.BeginBatchWrite(), storage: s}
}

func (s *commitThenUncertainStorage) Get(ctx context.Context, key []byte) ([]byte, error) {
	if s.blockReads.Load() && s.releaseReads != nil {
		s.readBlockedOnce.Do(func() { close(s.readBlocked) })
		select {
		case <-s.releaseReads:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	for {
		remaining := s.remainingReadFailures.Load()
		if remaining == 0 {
			return s.KvStorage.Get(ctx, key)
		}
		if s.remainingReadFailures.CompareAndSwap(remaining, remaining-1) {
			return nil, storage.ErrUnavailable
		}
	}
}

type commitThenUncertainBatch struct {
	storage.BatchWrite
	storage *commitThenUncertainStorage
}

func (b *commitThenUncertainBatch) Commit(ctx context.Context) error {
	uncertain := b.storage.trigger.CompareAndSwap(true, false)
	err := b.BatchWrite.Commit(ctx)
	if err == nil && uncertain {
		b.storage.remainingReadFailures.Store(b.storage.failReadsAfterUncertain)
		if b.storage.releaseReads != nil {
			b.storage.blockReads.Store(true)
		}
		return storage.NewErrUncertainResult(context.DeadlineExceeded)
	}
	return err
}

type uncommittedUncertainStorage struct {
	storage.KvStorage
	trigger atomic.Bool
}

func (s *uncommittedUncertainStorage) BeginBatchWrite() storage.BatchWrite {
	if s.trigger.CompareAndSwap(true, false) {
		return uncertainNoopBatch{}
	}
	return s.KvStorage.BeginBatchWrite()
}

type uncertainNoopBatch struct{}

func (uncertainNoopBatch) PutIfNotExist([]byte, []byte, int64) {}
func (uncertainNoopBatch) CAS([]byte, []byte, []byte, int64)   {}
func (uncertainNoopBatch) Put([]byte, []byte, int64)           {}
func (uncertainNoopBatch) Del([]byte)                          {}
func (uncertainNoopBatch) DelCurrent(storage.Iter)             {}
func (uncertainNoopBatch) Commit(context.Context) error {
	return storage.NewErrUncertainResult(context.DeadlineExceeded)
}

type transientCASStorage struct {
	storage.KvStorage
	failUntil atomic.Int64
}

func (s *transientCASStorage) BeginBatchWrite() storage.BatchWrite {
	if time.Now().UnixNano() < s.failUntil.Load() {
		return transientCASBatch{}
	}
	return s.KvStorage.BeginBatchWrite()
}

type transientCASBatch struct{}

func (transientCASBatch) PutIfNotExist([]byte, []byte, int64) {}
func (transientCASBatch) CAS([]byte, []byte, []byte, int64)   {}
func (transientCASBatch) Put([]byte, []byte, int64)           {}
func (transientCASBatch) Del([]byte)                          {}
func (transientCASBatch) DelCurrent(storage.Iter)             {}
func (transientCASBatch) Commit(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(50 * time.Millisecond):
		return storage.ErrCASFailed
	}
}

func newTxnApplyBackend(t *testing.T) (*backend, context.Context) {
	ctrl := gomock.NewController(t)
	t.Cleanup(ctrl.Finish)
	m := mock.NewMinimalMetrics(ctrl)
	kv := imemkv.NewKvStorage()
	t.Cleanup(func() { require.NoError(t, kv.Close()) })
	b := NewBackend(kv, Config{Prefix: prefix, Identity: getStorageIdentity(), EnableEtcdCompatibility: true}, m).(*backend)
	b.SetCurrentRevision(uint64(time.Now().UnixNano()))
	return b, context.Background()
}

func liveValue(t *testing.T, b *backend, ctx context.Context, key []byte) (string, uint64) {
	t.Helper()
	resp, err := b.Get(ctx, &proto.GetRequest{Key: key})
	require.NoError(t, err)
	if resp.Kv == nil {
		return "", 0
	}
	return string(StripInlineValue(resp.Kv.Value)), resp.Kv.Revision
}

func TestTxnApplyHonorsCallerDeadlineBeyondBackendFallback(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	metrics := mock.NewMinimalMetrics(ctrl)
	raw := imemkv.NewKvStorage()
	defer func() { require.NoError(t, raw.Close()) }()
	kv := &transientCASStorage{KvStorage: raw}
	b := NewBackend(kv, Config{
		Prefix: prefix, Identity: getStorageIdentity(), EnableEtcdCompatibility: true,
	}, metrics).(*backend)
	b.SetCurrentRevision(uint64(time.Now().UnixNano()))
	kv.failUntil.Store(time.Now().Add(1100 * time.Millisecond).UnixNano())

	ctx, cancel := context.WithTimeout(context.Background(), 2500*time.Millisecond)
	defer cancel()
	started := time.Now()
	results, revision, err := b.TxnApply(ctx, []TxnWriteOp{{
		Key: []byte(prefix + "/deadline/recovered"), Value: []byte("value"),
	}}, nil)
	require.NoError(t, err)
	require.Greater(t, time.Since(started), time.Second)
	require.NotZero(t, revision)
	require.Len(t, results, 1)
	require.True(t, results[0].Created)
}

func TestTxnApplyWithoutCallerDeadlineRetainsFallback(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	metrics := mock.NewMinimalMetrics(ctrl)
	raw := imemkv.NewKvStorage()
	defer func() { require.NoError(t, raw.Close()) }()
	kv := &transientCASStorage{KvStorage: raw}
	b := NewBackend(kv, Config{
		Prefix: prefix, Identity: getStorageIdentity(), EnableEtcdCompatibility: true,
	}, metrics).(*backend)
	b.SetCurrentRevision(uint64(time.Now().UnixNano()))
	kv.failUntil.Store(time.Now().Add(3 * time.Second).UnixNano())

	started := time.Now()
	_, _, err := b.TxnApply(context.Background(), []TxnWriteOp{{
		Key: []byte(prefix + "/deadline/fallback"), Value: []byte("value"),
	}}, nil)
	require.ErrorIs(t, err, storage.ErrUnavailable)
	require.Greater(t, time.Since(started), 900*time.Millisecond)
	require.Less(t, time.Since(started), 3*time.Second)
}

// TestTxnApplySingleRevisionAtomic verifies a mixed put/delete txn applies all
// writes at ONE revision (the core #4 Tier 1 fix) and the state is correct.
func TestTxnApplySingleRevisionAtomic(t *testing.T) {
	b, ctx := newTxnApplyBackend(t)

	// seed: existing key to update, existing key to delete
	upd := []byte(prefix + "/reg/upd")
	del := []byte(prefix + "/reg/del")
	cre := []byte(prefix + "/reg/cre")
	seed := func(k []byte, v string) {
		cr, err := b.Create(ctx, &proto.CreateRequest{Key: k, Value: []byte(v)})
		require.NoError(t, err)
		require.True(t, cr.Succeeded)
	}
	seed(upd, "u0")
	seed(del, "d0")
	waitCommitted(t, b, b.GetCurrentRevision())

	results, rev, err := b.TxnApply(ctx, []TxnWriteOp{
		{Key: cre, Value: []byte("c1")}, // create
		{Key: upd, Value: []byte("u1")}, // update
		{Delete: true, Key: del},        // delete
	}, nil)
	require.NoError(t, err)
	require.NotZero(t, rev)
	require.Len(t, results, 3)

	// all ops share the single txn revision
	for _, r := range results {
		require.Equal(t, rev, r.Revision, "op %s must be at the single txn revision", r.Key)
	}
	require.True(t, results[0].Created, "create result")
	require.Equal(t, uint64(1), results[0].Meta.Version)
	require.Equal(t, rev, results[0].Meta.CreateRevision)
	require.False(t, results[1].Created, "update result")
	require.EqualValues(t, 2, results[1].Meta.Version, "update bumps version to 2")
	require.True(t, results[2].Deleted, "delete result")
	require.Equal(t, "d0", string(StripInlineValue(results[2].PrevValue)))

	waitCommitted(t, b, rev)

	// state: create + update present at rev, delete gone
	v, r := liveValue(t, b, ctx, cre)
	require.Equal(t, "c1", v)
	require.Equal(t, rev, r)
	v, r = liveValue(t, b, ctx, upd)
	require.Equal(t, "u1", v)
	require.Equal(t, rev, r)
	v, _ = liveValue(t, b, ctx, del)
	require.Equal(t, "", v, "deleted key must be gone")
}

func TestTxnApplyCommittedUncertainResultResolvesAsOneTransaction(t *testing.T) {
	defaultRetryInterval, defaultCheckInterval := retryInterval, checkInterval
	retryInterval, checkInterval = 10*time.Millisecond, 10*time.Millisecond
	defer func() { retryInterval, checkInterval = defaultRetryInterval, defaultCheckInterval }()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	metrics := mock.NewMinimalMetrics(ctrl)
	mem := imemkv.NewKvStorage()
	defer func() { require.NoError(t, mem.Close()) }()
	store := &commitThenUncertainStorage{KvStorage: mem, failReadsAfterUncertain: 3}
	b := NewBackend(store, Config{
		Prefix: prefix, Identity: getStorageIdentity(), EnableEtcdCompatibility: true,
	}, metrics).(*backend)
	b.SetCurrentRevision(uint64(time.Now().UnixNano()))
	ctx := context.Background()
	left := []byte(prefix + "/txn-uncertain/left")
	right := []byte(prefix + "/txn-uncertain/right")

	store.trigger.Store(true)
	_, revision, err := b.TxnApply(ctx, []TxnWriteOp{
		{Key: left, Value: []byte("left")},
		{Key: right, Value: []byte("right")},
	}, nil)
	require.ErrorIs(t, err, storage.ErrUncertainResult)
	require.NotZero(t, revision)
	require.Eventually(t, func() bool {
		return b.GetCurrentRevision() >= revision
	}, 2*time.Second, time.Millisecond, "whole-txn resolver did not publish the committed revision")

	leftValue, leftRevision := liveValue(t, b, ctx, left)
	rightValue, rightRevision := liveValue(t, b, ctx, right)
	require.Equal(t, "left", leftValue)
	require.Equal(t, "right", rightValue)
	require.Equal(t, revision, leftRevision)
	require.Equal(t, revision, rightRevision)

	// Wait well past the shortened legacy single-key retry interval. The old
	// path rewrote each key independently here, producing two newer revisions.
	time.Sleep(100 * time.Millisecond)
	_, leftRevision = liveValue(t, b, ctx, left)
	_, rightRevision = liveValue(t, b, ctx, right)
	require.Equal(t, revision, leftRevision)
	require.Equal(t, revision, rightRevision)

	events := getEventsFromRev(ctx, b, revision, 2)
	require.Len(t, events, 2)
	require.Equal(t, revision, events[0].Kv.Revision)
	require.Equal(t, revision, events[1].Kv.Revision)
}

func TestTxnApplyUncertainResultPinsCompactionUntilResolved(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	metrics := mock.NewMinimalMetrics(ctrl)
	mem := imemkv.NewKvStorage()
	defer func() { require.NoError(t, mem.Close()) }()
	store := &commitThenUncertainStorage{
		KvStorage:    mem,
		readBlocked:  make(chan struct{}),
		releaseReads: make(chan struct{}),
	}
	b := NewBackend(store, Config{
		Prefix: prefix, Identity: getStorageIdentity(), EnableEtcdCompatibility: true,
	}, metrics).(*backend)
	b.SetCurrentRevision(uint64(time.Now().UnixNano()))
	ctx := context.Background()

	store.trigger.Store(true)
	_, revision, err := b.TxnApply(ctx, []TxnWriteOp{
		{Key: []byte(prefix + "/txn-uncertain-pin/left"), Value: []byte("left")},
		{Key: []byte(prefix + "/txn-uncertain-pin/right"), Value: []byte("right")},
	}, nil)
	require.ErrorIs(t, err, storage.ErrUncertainResult)
	require.NotZero(t, revision)

	select {
	case <-store.readBlocked:
	case <-time.After(time.Second):
		t.Fatal("uncertain transaction resolver did not reach marker read")
	}
	require.Equal(t, revision, b.uncertainTxnPins.min())
	require.Equal(t, revision-1, b.clampCompactRevision(revision+100),
		"compaction must not delete the commit markers before resolution")

	close(store.releaseReads)
	require.Eventually(t, func() bool {
		return b.GetCurrentRevision() >= revision && b.uncertainTxnPins.min() == 0
	}, 2*time.Second, time.Millisecond, "resolver must publish the transaction and release its compaction pin")
	require.Equal(t, revision, b.clampCompactRevision(revision),
		"resolved transactions must no longer constrain compaction")
}

func TestTxnApplyUncommittedUncertainResultSkipsAsOneTransaction(t *testing.T) {
	defaultRetryInterval, defaultCheckInterval := retryInterval, checkInterval
	retryInterval, checkInterval = 10*time.Millisecond, 10*time.Millisecond
	defer func() { retryInterval, checkInterval = defaultRetryInterval, defaultCheckInterval }()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	metrics := mock.NewMinimalMetrics(ctrl)
	mem := imemkv.NewKvStorage()
	defer func() { require.NoError(t, mem.Close()) }()
	store := &uncommittedUncertainStorage{KvStorage: mem}
	b := NewBackend(store, Config{
		Prefix: prefix, Identity: getStorageIdentity(), EnableEtcdCompatibility: true,
	}, metrics).(*backend)
	b.SetCurrentRevision(uint64(time.Now().UnixNano()))
	ctx := context.Background()
	left := []byte(prefix + "/txn-uncertain-not-committed/left")
	right := []byte(prefix + "/txn-uncertain-not-committed/right")

	store.trigger.Store(true)
	_, revision, err := b.TxnApply(ctx, []TxnWriteOp{
		{Key: left, Value: []byte("left")},
		{Key: right, Value: []byte("right")},
	}, nil)
	require.ErrorIs(t, err, storage.ErrUncertainResult)
	require.Eventually(t, func() bool {
		return b.GetCurrentRevision() >= revision
	}, time.Second, time.Millisecond, "whole-txn resolver did not skip the uncommitted revision")
	leftValue, leftRevision := liveValue(t, b, ctx, left)
	rightValue, rightRevision := liveValue(t, b, ctx, right)
	require.Empty(t, leftValue)
	require.Empty(t, rightValue)
	require.Zero(t, leftRevision)
	require.Zero(t, rightRevision)

	time.Sleep(100 * time.Millisecond)
	_, leftRevision = liveValue(t, b, ctx, left)
	_, rightRevision = liveValue(t, b, ctx, right)
	require.Zero(t, leftRevision)
	require.Zero(t, rightRevision)
}

// TestTxnApplyRecreateOverTombstone verifies a put on a previously-deleted key
// creates it fresh (version resets to 1, new create revision).
func TestTxnApplyRecreateOverTombstone(t *testing.T) {
	b, ctx := newTxnApplyBackend(t)
	key := []byte(prefix + "/reg/phoenix")

	cr, err := b.Create(ctx, &proto.CreateRequest{Key: key, Value: []byte("v1")})
	require.NoError(t, err)
	dr, err := b.Delete(ctx, &proto.DeleteRequest{Key: key})
	require.NoError(t, err)
	require.True(t, dr.Succeeded)
	_ = cr
	waitCommitted(t, b, b.GetCurrentRevision())

	results, rev, err := b.TxnApply(ctx, []TxnWriteOp{{Key: key, Value: []byte("reborn")}}, nil)
	require.NoError(t, err)
	require.True(t, results[0].Created, "put over tombstone must be a create")
	require.EqualValues(t, 1, results[0].Meta.Version)
	require.Equal(t, rev, results[0].Meta.CreateRevision)
	waitCommitted(t, b, rev)
	v, r := liveValue(t, b, ctx, key)
	require.Equal(t, "reborn", v)
	require.Equal(t, rev, r)
}

// TestTxnApplyNoOpDeleteConsumesNoRevision verifies deleting absent keys makes
// no write and does not advance the revision.
func TestTxnApplyNoOpDeleteConsumesNoRevision(t *testing.T) {
	b, ctx := newTxnApplyBackend(t)
	before := b.GetCurrentRevision()
	results, rev, err := b.TxnApply(ctx, []TxnWriteOp{
		{Delete: true, Key: []byte(prefix + "/reg/ghost1")},
		{Delete: true, Key: []byte(prefix + "/reg/ghost2")},
	}, nil)
	require.NoError(t, err)
	require.Len(t, results, 2)
	require.False(t, results[0].Deleted)
	require.Equal(t, before, rev, "no-op txn must not advance the revision")
	require.Equal(t, before, b.GetCurrentRevision())
}

func TestTxnApplyCommitsInternalMetadataAtOnlyUserRevision(t *testing.T) {
	b, ctx := newTxnApplyBackend(t)
	before := b.GetCurrentRevision()
	userKey := []byte(prefix + "/reg/leased")
	internalKey := []byte("leasekeys/" + string(userKey))

	results, rev, err := b.TxnApply(ctx, []TxnWriteOp{
		{Key: userKey, Value: []byte("value"), Lease: 42},
		{Internal: true, Key: internalKey, Value: []byte("42")},
	}, nil)
	require.NoError(t, err)
	require.Equal(t, before+1, rev)
	require.Len(t, results, 2)
	require.True(t, results[0].Created)
	require.False(t, results[1].Created)
	value, err := b.InternalGet(ctx, internalKey)
	require.NoError(t, err)
	require.Equal(t, []byte("42"), value)
	require.Equal(t, rev, b.GetCurrentRevision(), "internal op must not allocate a second revision")
}

func TestTxnApplyCommitsInternalOnlyMutationWithoutUserRevision(t *testing.T) {
	b, ctx := newTxnApplyBackend(t)
	internalKey := []byte("leases/empty")
	require.NoError(t, b.InternalPut(ctx, internalKey, []byte("meta")))
	before := b.GetCurrentRevision()

	results, rev, err := b.TxnApply(ctx, []TxnWriteOp{
		{Internal: true, Delete: true, Key: internalKey},
	}, nil)
	require.NoError(t, err)
	require.Len(t, results, 1)
	require.Equal(t, before, rev)
	require.Equal(t, before, b.GetCurrentRevision(),
		"internal-only transaction must not advance user MVCC")
	_, err = b.InternalGet(ctx, internalKey)
	require.ErrorIs(t, err, storage.ErrKeyNotFound,
		"internal-only delete must not be mistaken for a no-op")
}

// TestTxnApplyContendedSameKeyRetries drives concurrent TxnApply updates to the
// SAME key: each must eventually win via the CAS-retry loop (no lost update, no
// error), and the final version equals the number of writers + 1 (the seed).
func TestTxnApplyContendedSameKeyRetries(t *testing.T) {
	b, ctx := newTxnApplyBackend(t)
	key := []byte(prefix + "/reg/hot")
	cr, err := b.Create(ctx, &proto.CreateRequest{Key: key, Value: []byte("v0")})
	require.NoError(t, err)
	require.True(t, cr.Succeeded)
	waitCommitted(t, b, b.GetCurrentRevision())

	const writers = 24
	var wg sync.WaitGroup
	errs := make([]error, writers)
	for i := 0; i < writers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			_, _, e := b.TxnApply(ctx, []TxnWriteOp{{Key: key, Value: []byte(fmt.Sprintf("v%d", i+1))}}, nil)
			errs[i] = e
		}(i)
	}
	wg.Wait()
	for i := 0; i < writers; i++ {
		require.NoError(t, errs[i], "writer %d", i)
	}
	waitCommitted(t, b, b.GetCurrentRevision())
	// version must have advanced exactly writers times over the seed (v1),
	// proving every contended write applied (serialized), none lost.
	meta, err := b.GetEtcdMetadata(ctx, key, b.GetCurrentRevision())
	require.NoError(t, err)
	require.EqualValues(t, writers+1, meta.Version, "every contended write must apply exactly once")
}

// TestTxnApplyGuard verifies the #4 Tier 2 OCC guard: a txn applies when its
// compare guard's key is unchanged, and fails with ErrTxnGuardConflict (writing
// nothing) once the guarded key has been modified.
func TestTxnApplyGuard(t *testing.T) {
	b, ctx := newTxnApplyBackend(t)
	guardKey := []byte(prefix + "/reg/guard")
	a := []byte(prefix + "/reg/ga")
	bk := []byte(prefix + "/reg/gb")

	cr, err := b.Create(ctx, &proto.CreateRequest{Key: guardKey, Value: []byte("g")})
	require.NoError(t, err)
	guardRev := cr.Header.Revision
	waitCommitted(t, b, b.GetCurrentRevision())

	// guard unchanged -> applies
	_, rev, err := b.TxnApply(ctx,
		[]TxnWriteOp{{Key: a, Value: []byte("va")}, {Key: bk, Value: []byte("vb")}},
		[]TxnGuard{{Key: guardKey, Revision: guardRev}})
	require.NoError(t, err)
	require.NotZero(t, rev)
	waitCommitted(t, b, rev)
	v, _ := liveValue(t, b, ctx, a)
	require.Equal(t, "va", v)

	// modify the guarded key
	u, err := b.Update(ctx, &proto.UpdateRequest{Kv: &proto.KeyValue{Key: guardKey, Value: []byte("g2"), Revision: guardRev}})
	require.NoError(t, err)
	require.True(t, u.Succeeded)
	waitCommitted(t, b, b.GetCurrentRevision())

	// guard now stale -> conflict, and a/b are NOT overwritten
	_, _, err = b.TxnApply(ctx,
		[]TxnWriteOp{{Key: a, Value: []byte("va2")}, {Key: bk, Value: []byte("vb2")}},
		[]TxnGuard{{Key: guardKey, Revision: guardRev}})
	require.ErrorIs(t, err, ErrTxnGuardConflict)
	va, _ := liveValue(t, b, ctx, a)
	require.Equal(t, "va", va, "a must not be overwritten when the guard conflicts")
	vb, _ := liveValue(t, b, ctx, bk)
	require.Equal(t, "vb", vb, "b must not be overwritten when the guard conflicts")
}

func TestTxnApplyAbsentGuardOverlappingPut(t *testing.T) {
	b, ctx := newTxnApplyBackend(t)
	key := []byte(prefix + "/reg/absent-overlap")

	results, _, err := b.TxnApply(ctx,
		[]TxnWriteOp{{Key: key, Value: []byte("created")}},
		[]TxnGuard{{Key: key, Absent: true}})
	require.NoError(t, err)
	require.True(t, results[0].Created)

	_, _, err = b.TxnApply(ctx,
		[]TxnWriteOp{{Key: key, Value: []byte("must-not-overwrite")}},
		[]TxnGuard{{Key: key, Absent: true}})
	require.ErrorIs(t, err, ErrTxnGuardConflict)
	value, _ := liveValue(t, b, ctx, key)
	require.Equal(t, "created", value)
}

func TestTxnApplyAbsentGuardDisjointFromWrite(t *testing.T) {
	b, ctx := newTxnApplyBackend(t)
	guardKey := []byte(prefix + "/reg/absent-guard")
	writeKey := []byte(prefix + "/reg/absent-result")

	_, _, err := b.TxnApply(ctx,
		[]TxnWriteOp{{Key: writeKey, Value: []byte("allowed")}},
		[]TxnGuard{{Key: guardKey, Absent: true}})
	require.NoError(t, err)

	created, err := b.Create(ctx, &proto.CreateRequest{Key: guardKey, Value: []byte("now-present")})
	require.NoError(t, err)
	require.True(t, created.Succeeded)

	_, _, err = b.TxnApply(ctx,
		[]TxnWriteOp{{Key: writeKey, Value: []byte("must-not-write")}},
		[]TxnGuard{{Key: guardKey, Absent: true}})
	require.ErrorIs(t, err, ErrTxnGuardConflict)
	value, _ := liveValue(t, b, ctx, writeKey)
	require.Equal(t, "allowed", value)
}

func TestTxnApplyAbsentGuardAcceptsTombstone(t *testing.T) {
	b, ctx := newTxnApplyBackend(t)
	guardKey := []byte(prefix + "/reg/tombstoned-guard")
	writeKey := []byte(prefix + "/reg/tombstoned-result")

	created, err := b.Create(ctx, &proto.CreateRequest{Key: guardKey, Value: []byte("old")})
	require.NoError(t, err)
	require.True(t, created.Succeeded)
	deleted, err := b.Delete(ctx, &proto.DeleteRequest{Key: guardKey})
	require.NoError(t, err)
	require.True(t, deleted.Succeeded)

	_, _, err = b.TxnApply(ctx,
		[]TxnWriteOp{{Key: writeKey, Value: []byte("allowed")}},
		[]TxnGuard{{Key: guardKey, Absent: true}})
	require.NoError(t, err)
	value, _ := liveValue(t, b, ctx, writeKey)
	require.Equal(t, "allowed", value)
}

// TestTxnApplyConcurrentDistinctKeys stress-tests many concurrent single-key
// TxnApply calls: every write must land, at a unique revision, with no lost
// update or stall.
func TestTxnApplyConcurrentDistinctKeys(t *testing.T) {
	b, ctx := newTxnApplyBackend(t)
	const n = 64
	var wg sync.WaitGroup
	revs := make([]uint64, n)
	errs := make([]error, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			key := []byte(fmt.Sprintf("%s/reg/c%03d", prefix, i))
			_, rev, err := b.TxnApply(ctx, []TxnWriteOp{{Key: key, Value: []byte("v")}}, nil)
			revs[i] = rev
			errs[i] = err
		}(i)
	}
	wg.Wait()
	seen := map[uint64]bool{}
	for i := 0; i < n; i++ {
		require.NoError(t, errs[i])
		require.NotZero(t, revs[i])
		require.False(t, seen[revs[i]], "revision %d reused across concurrent txns", revs[i])
		seen[revs[i]] = true
	}
	waitCommitted(t, b, b.GetCurrentRevision())
	for i := 0; i < n; i++ {
		v, _ := liveValue(t, b, ctx, []byte(fmt.Sprintf("%s/reg/c%03d", prefix, i)))
		require.Equal(t, "v", v, "key %d must be present", i)
	}
}
