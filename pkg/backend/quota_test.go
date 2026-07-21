package backend

import (
	"bytes"
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"

	"github.com/kubewharf/kubebrain/pkg/metrics/mock"
	"github.com/kubewharf/kubebrain/pkg/storage"
	imemkv "github.com/kubewharf/kubebrain/pkg/storage/memkv"
)

type blockSecondAlarmReadStorage struct {
	storage.KvStorage
	alarmKey []byte
	enabled  atomic.Bool
	reads    atomic.Int32
	blocked  chan struct{}
	release  chan struct{}
	once     sync.Once
}

type blockFirstCommitStorage struct {
	storage.KvStorage
	trigger   atomic.Bool
	committed chan struct{}
	release   chan struct{}
}

func (s *blockFirstCommitStorage) BeginBatchWrite() storage.BatchWrite {
	return &blockFirstCommitBatch{
		BatchWrite: s.KvStorage.BeginBatchWrite(),
		storage:    s,
	}
}

type blockFirstCommitBatch struct {
	storage.BatchWrite
	storage *blockFirstCommitStorage
}

func (b *blockFirstCommitBatch) Commit(ctx context.Context) error {
	err := b.BatchWrite.Commit(ctx)
	if err == nil && b.storage.trigger.CompareAndSwap(true, false) {
		close(b.storage.committed)
		select {
		case <-b.storage.release:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return err
}

func (s *blockSecondAlarmReadStorage) Get(ctx context.Context, key []byte) ([]byte, error) {
	if s.enabled.Load() && bytes.Equal(key, s.alarmKey) && s.reads.Add(1) == 2 {
		s.once.Do(func() { close(s.blocked) })
		select {
		case <-s.release:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	return s.KvStorage.Get(ctx, key)
}

func newQuotaBackend(t *testing.T, quota int64) (*backend, context.Context) {
	t.Helper()
	ctrl := gomock.NewController(t)
	t.Cleanup(ctrl.Finish)
	kv := imemkv.NewKvStorage()
	t.Cleanup(func() { require.NoError(t, kv.Close()) })
	b := NewBackend(kv, Config{
		Prefix:                  prefix,
		Identity:                getStorageIdentity(),
		EnableEtcdCompatibility: true,
		QuotaBackendBytes:       quota,
	}, mock.NewMinimalMetrics(ctrl)).(*backend)
	b.SetCurrentRevision(uint64(time.Now().UnixNano()))
	require.NoError(t, b.EnsureQuotaInitialized(context.Background()))
	return b, context.Background()
}

func TestLogicalQuotaTracksLatestBytesAndPersistsNoSpace(t *testing.T) {
	b, ctx := newQuotaBackend(t, 10)
	key := []byte("a")

	_, firstRevision, err := b.TxnApply(ctx, []TxnWriteOp{{
		Key: key, Value: []byte("1234"),
	}}, nil)
	require.NoError(t, err)
	usage, quota, alarm, err := b.QuotaStatus(ctx)
	require.NoError(t, err)
	require.Equal(t, int64(5), usage)
	require.Equal(t, int64(10), quota)
	require.False(t, alarm)

	_, updateRevision, err := b.TxnApply(ctx, []TxnWriteOp{{
		Key: key, Value: []byte("1234567"),
	}}, nil)
	require.NoError(t, err)
	require.Greater(t, updateRevision, firstRevision)
	usage, _, _, err = b.QuotaStatus(ctx)
	require.NoError(t, err)
	require.Equal(t, int64(8), usage)

	_, fullRevision, err := b.TxnApply(ctx, []TxnWriteOp{{
		Key: []byte("b"), Value: []byte("x"),
	}}, nil)
	require.NoError(t, err)
	usage, _, _, err = b.QuotaStatus(ctx)
	require.NoError(t, err)
	require.Equal(t, int64(10), usage)

	_, rejectedRevision, err := b.TxnApply(ctx, []TxnWriteOp{{
		Key: []byte("c"), Value: []byte("x"),
	}}, nil)
	require.ErrorIs(t, err, ErrNoSpace)
	require.Equal(t, fullRevision, rejectedRevision, "quota rejection must not allocate an MVCC revision")
	require.Equal(t, fullRevision, b.GetCurrentRevision())
	usage, _, alarm, err = b.QuotaStatus(ctx)
	require.NoError(t, err)
	require.Equal(t, int64(10), usage)
	require.True(t, alarm)
	removed, err := b.DisarmNoSpace(ctx, b.quotaAlarmMemberID())
	require.NoError(t, err)
	require.True(t, removed, "etcd permits alarm deactivation at the quota limit")
	_, _, err = b.TxnApply(ctx, []TxnWriteOp{{
		Key: key, Value: []byte("1"),
	}}, nil)
	require.ErrorIs(t, err, ErrNoSpace, "the next Put at the limit must re-arm NOSPACE")
	_, _, alarm, err = b.QuotaStatus(ctx)
	require.NoError(t, err)
	require.True(t, alarm)

	_, _, err = b.TxnApply(ctx, []TxnWriteOp{{Key: []byte("b"), Delete: true}}, nil)
	require.NoError(t, err, "NOSPACE must allow deletes that recover capacity")
	usage, _, alarm, err = b.QuotaStatus(ctx)
	require.NoError(t, err)
	require.Equal(t, int64(8), usage)
	require.True(t, alarm, "capacity recovery does not implicitly disarm etcd's sticky alarm")
	removed, err = b.DisarmNoSpace(ctx, b.quotaAlarmMemberID())
	require.NoError(t, err)
	require.True(t, removed)
	_, _, alarm, err = b.QuotaStatus(ctx)
	require.NoError(t, err)
	require.False(t, alarm)
}

func TestLogicalQuotaAlarmRejectsAllPutsUntilCapacityRecovery(t *testing.T) {
	b, ctx := newQuotaBackend(t, 6)
	key := []byte("key")
	_, _, err := b.TxnApply(ctx, []TxnWriteOp{{Key: key, Value: []byte("123")}}, nil)
	require.NoError(t, err)

	_, _, err = b.TxnApply(ctx, []TxnWriteOp{{Key: key, Value: []byte("1234")}}, nil)
	require.ErrorIs(t, err, ErrNoSpace)
	value, _ := liveValue(t, b, ctx, key)
	require.Equal(t, "123", value)

	_, _, err = b.TxnApply(ctx, []TxnWriteOp{{Key: key, Value: []byte("1")}}, nil)
	require.ErrorIs(t, err, ErrNoSpace, "sticky NOSPACE caps shrinking puts too")
	_, _, err = b.TxnApply(ctx, []TxnWriteOp{{Key: key, Delete: true}}, nil)
	require.NoError(t, err)
	usage, _, alarm, err := b.QuotaStatus(ctx)
	require.NoError(t, err)
	require.Zero(t, usage)
	require.True(t, alarm)
	removed, err := b.DisarmNoSpace(ctx, b.quotaAlarmMemberID())
	require.NoError(t, err)
	require.True(t, removed)
	_, _, err = b.TxnApply(ctx, []TxnWriteOp{{Key: key, Value: []byte("1")}}, nil)
	require.NoError(t, err)
}

func TestQuotaDisabledPreservesExistingBehavior(t *testing.T) {
	b, ctx := newQuotaBackend(t, 0)
	_, _, err := b.TxnApply(ctx, []TxnWriteOp{{
		Key: []byte("large"), Value: make([]byte, 1024),
	}}, nil)
	require.NoError(t, err)
	usage, quota, alarm, err := b.QuotaStatus(ctx)
	require.NoError(t, err)
	require.Zero(t, usage)
	require.Zero(t, quota)
	require.False(t, alarm)
	owner, err := b.ArmNoSpace(ctx, 0)
	require.NoError(t, err)
	require.Zero(t, owner)
	usage, quota, alarm, err = b.QuotaStatus(ctx)
	require.NoError(t, err)
	require.Zero(t, usage)
	require.Zero(t, quota)
	require.True(t, alarm)
	_, _, err = b.TxnApply(ctx, []TxnWriteOp{{Key: []byte("blocked"), Value: []byte("value")}}, nil)
	require.ErrorIs(t, err, ErrNoSpace)
	_, _, err = b.TxnApply(ctx, []TxnWriteOp{{Key: []byte("large"), Delete: true}}, nil)
	require.NoError(t, err, "delete-only writes remain available under NOSPACE")
	removed, err := b.DisarmNoSpace(ctx, 0)
	require.NoError(t, err)
	require.True(t, removed)
	_, _, err = b.TxnApply(ctx, []TxnWriteOp{{Key: []byte("restored"), Value: []byte("value")}}, nil)
	require.NoError(t, err)
}

func TestNoSpaceAlarmPersistsOwnerAndGuardsDisarm(t *testing.T) {
	b, ctx := newQuotaBackend(t, 10)
	wantOwner := b.quotaAlarmMemberID()
	require.NoError(t, b.activateNoSpace(ctx))
	owner, active, err := b.NoSpaceAlarm(ctx)
	require.NoError(t, err)
	require.Equal(t, wantOwner, owner)
	require.True(t, active)

	removed, err := b.DisarmNoSpace(ctx, ^uint64(0))
	require.NoError(t, err)
	require.False(t, removed)
	_, active, err = b.NoSpaceAlarm(ctx)
	require.NoError(t, err)
	require.True(t, active)

	removed, err = b.DisarmNoSpace(ctx, wantOwner)
	require.NoError(t, err)
	require.True(t, removed)
	_, active, err = b.NoSpaceAlarm(ctx)
	require.NoError(t, err)
	require.False(t, active)

	require.NoError(t, b.InternalPut(ctx, quotaAlarmKey, []byte{1}))
	owner, active, err = b.NoSpaceAlarm(ctx)
	require.NoError(t, err)
	require.True(t, active)
	require.Equal(t, wantOwner, owner, "legacy alarm metadata maps to the local owner")
	removed, err = b.DisarmNoSpace(ctx, ^uint64(0))
	require.NoError(t, err)
	require.True(t, removed, "legacy metadata accepts any owner during rolling upgrade")
}

func TestArmNoSpacePreservesExplicitZeroMember(t *testing.T) {
	b, ctx := newQuotaBackend(t, 10)
	owner, err := b.ArmNoSpace(ctx, 0)
	require.NoError(t, err)
	require.Zero(t, owner)

	members, err := b.NoSpaceAlarms(ctx)
	require.NoError(t, err)
	require.Equal(t, []uint64{0}, members)
	removed, err := b.DisarmNoSpace(ctx, 0)
	require.NoError(t, err)
	require.True(t, removed)
}

func TestArmNoSpacePersistsEveryExplicitMember(t *testing.T) {
	b, ctx := newQuotaBackend(t, 10)
	const explicitOwner uint64 = 424242

	owner, err := b.ArmNoSpace(ctx, explicitOwner)
	require.NoError(t, err)
	require.Equal(t, explicitOwner, owner)

	owner, err = b.ArmNoSpace(ctx, explicitOwner+1)
	require.NoError(t, err)
	require.Equal(t, explicitOwner+1, owner)
	members, err := b.NoSpaceAlarms(ctx)
	require.NoError(t, err)
	require.Equal(t, []uint64{explicitOwner, explicitOwner + 1}, members)

	owner, err = b.ArmNoSpace(ctx, explicitOwner)
	require.NoError(t, err)
	require.Equal(t, explicitOwner, owner, "duplicate activation is idempotent")
	members, err = b.NoSpaceAlarms(ctx)
	require.NoError(t, err)
	require.Equal(t, []uint64{explicitOwner, explicitOwner + 1}, members)
}

func TestArmNoSpaceConcurrentActivationPersistsEveryMember(t *testing.T) {
	b, ctx := newQuotaBackend(t, 10)
	const workers = 32
	start := make(chan struct{})
	owners := make(chan uint64, workers)
	errs := make(chan error, workers)
	var wg sync.WaitGroup
	for i := uint64(1); i <= workers; i++ {
		wg.Add(1)
		go func(requested uint64) {
			defer wg.Done()
			<-start
			owner, err := b.ArmNoSpace(ctx, requested)
			owners <- owner
			errs <- err
		}(i)
	}
	close(start)
	wg.Wait()
	close(owners)
	close(errs)

	for err := range errs {
		require.NoError(t, err)
	}
	members, err := b.NoSpaceAlarms(ctx)
	require.NoError(t, err)
	require.Len(t, members, workers)
	returned := make(map[uint64]bool, workers)
	for owner := range owners {
		returned[owner] = true
	}
	for memberID := uint64(1); memberID <= workers; memberID++ {
		require.Equal(t, memberID, members[memberID-1])
		require.True(t, returned[memberID])
	}
}

func TestQuotaAlarmSetEncodingRejectsNonCanonicalMetadata(t *testing.T) {
	require.Equal(t, []uint64{7}, mustDecodeQuotaAlarms(t, encodeQuotaAlarms([]uint64{7})))
	require.Equal(t, []uint64{7, 9}, mustDecodeQuotaAlarms(t, encodeQuotaAlarms([]uint64{7, 9})))
	for _, malformed := range [][]byte{
		{}, {quotaAlarmSetTag}, append([]byte{quotaAlarmSetTag}, make([]byte, 8)...),
		encodeQuotaAlarms([]uint64{9, 7}), encodeQuotaAlarms([]uint64{7, 7}),
	} {
		_, err := decodeQuotaAlarms(malformed)
		require.Error(t, err)
	}
}

func mustDecodeQuotaAlarms(t *testing.T, raw []byte) []uint64 {
	t.Helper()
	members, err := decodeQuotaAlarms(raw)
	require.NoError(t, err)
	return members
}

func TestDisarmNoSpaceRemovesOnlyRequestedMember(t *testing.T) {
	b, ctx := newQuotaBackend(t, 10)
	for _, memberID := range []uint64{11, 22} {
		_, err := b.ArmNoSpace(ctx, memberID)
		require.NoError(t, err)
	}

	removed, err := b.DisarmNoSpace(ctx, 11)
	require.NoError(t, err)
	require.True(t, removed)
	members, err := b.NoSpaceAlarms(ctx)
	require.NoError(t, err)
	require.Equal(t, []uint64{22}, members)
	_, _, active, err := b.QuotaStatus(ctx)
	require.NoError(t, err)
	require.True(t, active)

	removed, err = b.DisarmNoSpace(ctx, 11)
	require.NoError(t, err)
	require.False(t, removed)
	removed, err = b.DisarmNoSpace(ctx, 22)
	require.NoError(t, err)
	require.True(t, removed)
	members, err = b.NoSpaceAlarms(ctx)
	require.NoError(t, err)
	require.Empty(t, members)
}

func TestArmNoSpaceReturnsCommittedOwnerWhenConcurrentDisarmWinsResponseRace(t *testing.T) {
	ctrl := gomock.NewController(t)
	t.Cleanup(ctrl.Finish)
	base := imemkv.NewKvStorage()
	t.Cleanup(func() { require.NoError(t, base.Close()) })
	store := &blockFirstCommitStorage{
		KvStorage: base,
		committed: make(chan struct{}),
		release:   make(chan struct{}),
	}
	b := NewBackend(store, Config{
		Prefix:                  prefix,
		Identity:                getStorageIdentity(),
		EnableEtcdCompatibility: true,
		QuotaBackendBytes:       10,
	}, mock.NewMinimalMetrics(ctrl)).(*backend)
	require.NoError(t, b.EnsureQuotaInitialized(context.Background()))

	const owner uint64 = 424242
	store.trigger.Store(true)
	type result struct {
		owner uint64
		err   error
	}
	activated := make(chan result, 1)
	go func() {
		gotOwner, armErr := b.ArmNoSpace(context.Background(), owner)
		activated <- result{owner: gotOwner, err: armErr}
	}()
	<-store.committed

	removed, err := b.DisarmNoSpace(context.Background(), owner)
	require.NoError(t, err)
	require.True(t, removed)
	close(store.release)

	got := <-activated
	require.NoError(t, got.err)
	require.Equal(t, owner, got.owner, "ACTIVATE returns the owner persisted at its linearization point")
	_, active, err := b.NoSpaceAlarm(context.Background())
	require.NoError(t, err)
	require.False(t, active, "the later DEACTIVATE remains the final state")
}

func TestArmNoSpaceRetriesWhenConflictingOwnerIsConcurrentlyDisarmed(t *testing.T) {
	ctrl := gomock.NewController(t)
	t.Cleanup(ctrl.Finish)
	base := imemkv.NewKvStorage()
	t.Cleanup(func() { require.NoError(t, base.Close()) })
	store := &blockSecondAlarmReadStorage{
		KvStorage: base,
		blocked:   make(chan struct{}),
		release:   make(chan struct{}),
	}
	b := NewBackend(store, Config{
		Prefix:                  prefix,
		Identity:                getStorageIdentity(),
		EnableEtcdCompatibility: true,
		QuotaBackendBytes:       10,
	}, mock.NewMinimalMetrics(ctrl)).(*backend)
	store.alarmKey = b.ks.EncodeInternalKey(quotaAlarmKey)
	require.NoError(t, b.EnsureQuotaInitialized(context.Background()))

	const (
		existingOwner  uint64 = 424242
		requestedOwner uint64 = 424243
	)
	_, err := b.ArmNoSpace(context.Background(), existingOwner)
	require.NoError(t, err)
	store.enabled.Store(true)

	type result struct {
		owner uint64
		err   error
	}
	activated := make(chan result, 1)
	go func() {
		owner, armErr := b.ArmNoSpace(context.Background(), requestedOwner)
		activated <- result{owner: owner, err: armErr}
	}()
	<-store.blocked

	removed, err := b.DisarmNoSpace(context.Background(), existingOwner)
	require.NoError(t, err)
	require.True(t, removed)
	close(store.release)

	got := <-activated
	require.NoError(t, got.err)
	require.Equal(t, requestedOwner, got.owner)
	persisted, active, err := b.NoSpaceAlarm(context.Background())
	require.NoError(t, err)
	require.True(t, active)
	require.Equal(t, requestedOwner, persisted)
}

func TestArmNoSpaceReconcilesCommittedUncertainActivation(t *testing.T) {
	ctrl := gomock.NewController(t)
	t.Cleanup(ctrl.Finish)
	base := imemkv.NewKvStorage()
	t.Cleanup(func() { require.NoError(t, base.Close()) })
	store := &commitThenUncertainStorage{KvStorage: base}
	store.failReadsAfterUncertain = 2
	b := NewBackend(store, Config{
		Prefix:                  prefix,
		Identity:                getStorageIdentity(),
		EnableEtcdCompatibility: true,
		QuotaBackendBytes:       10,
	}, mock.NewMinimalMetrics(ctrl)).(*backend)
	require.NoError(t, b.EnsureQuotaInitialized(context.Background()))

	store.trigger.Store(true)
	const requestedOwner uint64 = 424242
	owner, err := b.ArmNoSpace(context.Background(), requestedOwner)
	require.NoError(t, err)
	require.Equal(t, requestedOwner, owner)
}

func TestArmNoSpacePreservesUncommittedUncertainError(t *testing.T) {
	ctrl := gomock.NewController(t)
	t.Cleanup(ctrl.Finish)
	base := imemkv.NewKvStorage()
	t.Cleanup(func() { require.NoError(t, base.Close()) })
	store := &uncommittedUncertainStorage{KvStorage: base}
	b := NewBackend(store, Config{
		Prefix:                  prefix,
		Identity:                getStorageIdentity(),
		EnableEtcdCompatibility: true,
		QuotaBackendBytes:       10,
	}, mock.NewMinimalMetrics(ctrl)).(*backend)
	require.NoError(t, b.EnsureQuotaInitialized(context.Background()))

	store.trigger.Store(true)
	_, err := b.ArmNoSpace(context.Background(), 424242)
	require.ErrorIs(t, err, storage.ErrUncertainResult)
	_, active, readErr := b.NoSpaceAlarm(context.Background())
	require.NoError(t, readErr)
	require.False(t, active)
}

func TestDisarmNoSpaceReconcilesCommittedUncertainDeletion(t *testing.T) {
	ctrl := gomock.NewController(t)
	t.Cleanup(ctrl.Finish)
	base := imemkv.NewKvStorage()
	t.Cleanup(func() { require.NoError(t, base.Close()) })
	store := &commitThenUncertainStorage{
		KvStorage:               base,
		failReadsAfterUncertain: 2,
	}
	b := NewBackend(store, Config{
		Prefix:                  prefix,
		Identity:                getStorageIdentity(),
		EnableEtcdCompatibility: true,
		QuotaBackendBytes:       10,
	}, mock.NewMinimalMetrics(ctrl)).(*backend)
	require.NoError(t, b.EnsureQuotaInitialized(context.Background()))

	const owner uint64 = 424242
	_, err := b.ArmNoSpace(context.Background(), owner)
	require.NoError(t, err)
	store.trigger.Store(true)
	removed, err := b.DisarmNoSpace(context.Background(), owner)
	require.NoError(t, err)
	require.True(t, removed)
	_, active, err := b.NoSpaceAlarm(context.Background())
	require.NoError(t, err)
	require.False(t, active)
}

func TestDisarmNoSpaceConcurrentDeletionIsIdempotent(t *testing.T) {
	ctrl := gomock.NewController(t)
	t.Cleanup(ctrl.Finish)
	base := imemkv.NewKvStorage()
	t.Cleanup(func() { require.NoError(t, base.Close()) })
	store := &blockSecondAlarmReadStorage{
		KvStorage: base,
		blocked:   make(chan struct{}),
		release:   make(chan struct{}),
	}
	b := NewBackend(store, Config{
		Prefix:                  prefix,
		Identity:                getStorageIdentity(),
		EnableEtcdCompatibility: true,
		QuotaBackendBytes:       10,
	}, mock.NewMinimalMetrics(ctrl)).(*backend)
	store.alarmKey = b.ks.EncodeInternalKey(quotaAlarmKey)
	require.NoError(t, b.EnsureQuotaInitialized(context.Background()))

	const owner uint64 = 424242
	_, err := b.ArmNoSpace(context.Background(), owner)
	require.NoError(t, err)
	store.enabled.Store(true)

	type result struct {
		removed bool
		err     error
	}
	delayed := make(chan result, 1)
	go func() {
		removed, disarmErr := b.DisarmNoSpace(context.Background(), owner)
		delayed <- result{removed: removed, err: disarmErr}
	}()
	<-store.blocked

	removed, err := b.DisarmNoSpace(context.Background(), owner)
	require.NoError(t, err)
	require.True(t, removed)
	close(store.release)

	got := <-delayed
	require.NoError(t, got.err)
	require.False(t, got.removed, "the later linearized duplicate returns a successful empty result")
	_, active, err := b.NoSpaceAlarm(context.Background())
	require.NoError(t, err)
	require.False(t, active)
}

func TestDisarmNoSpacePreservesUncommittedUncertainError(t *testing.T) {
	ctrl := gomock.NewController(t)
	t.Cleanup(ctrl.Finish)
	base := imemkv.NewKvStorage()
	t.Cleanup(func() { require.NoError(t, base.Close()) })
	store := &uncommittedUncertainStorage{KvStorage: base}
	b := NewBackend(store, Config{
		Prefix:                  prefix,
		Identity:                getStorageIdentity(),
		EnableEtcdCompatibility: true,
		QuotaBackendBytes:       10,
	}, mock.NewMinimalMetrics(ctrl)).(*backend)
	require.NoError(t, b.EnsureQuotaInitialized(context.Background()))

	const owner uint64 = 424242
	_, err := b.ArmNoSpace(context.Background(), owner)
	require.NoError(t, err)
	store.trigger.Store(true)
	removed, err := b.DisarmNoSpace(context.Background(), owner)
	require.ErrorIs(t, err, storage.ErrUncertainResult)
	require.False(t, removed)
	persisted, active, readErr := b.NoSpaceAlarm(context.Background())
	require.NoError(t, readErr)
	require.True(t, active)
	require.Equal(t, owner, persisted)
}

func TestDisarmNoSpaceReconcilesReplacementAfterUncertainDeletion(t *testing.T) {
	ctrl := gomock.NewController(t)
	t.Cleanup(ctrl.Finish)
	base := imemkv.NewKvStorage()
	t.Cleanup(func() { require.NoError(t, base.Close()) })
	store := &commitThenUncertainStorage{
		KvStorage:    base,
		readBlocked:  make(chan struct{}),
		releaseReads: make(chan struct{}),
	}
	b := NewBackend(store, Config{
		Prefix:                  prefix,
		Identity:                getStorageIdentity(),
		EnableEtcdCompatibility: true,
		QuotaBackendBytes:       10,
	}, mock.NewMinimalMetrics(ctrl)).(*backend)
	require.NoError(t, b.EnsureQuotaInitialized(context.Background()))

	const (
		owner       uint64 = 424242
		replacement uint64 = 424243
	)
	_, err := b.ArmNoSpace(context.Background(), owner)
	require.NoError(t, err)
	store.trigger.Store(true)
	type result struct {
		removed bool
		err     error
	}
	resultCh := make(chan result, 1)
	go func() {
		removed, disarmErr := b.DisarmNoSpace(context.Background(), owner)
		resultCh <- result{removed: removed, err: disarmErr}
	}()
	<-store.readBlocked

	batch := base.BeginBatchWrite()
	batch.PutIfNotExist(
		b.ks.EncodeInternalKey(quotaAlarmKey),
		encodeQuotaAlarm(replacement),
		0,
	)
	require.NoError(t, batch.Commit(context.Background()))
	close(store.releaseReads)

	got := <-resultCh
	require.NoError(t, got.err)
	require.True(t, got.removed)
	persisted, active, err := b.NoSpaceAlarm(context.Background())
	require.NoError(t, err)
	require.True(t, active)
	require.Equal(t, replacement, persisted)
}

func TestQuotaInitializationCountsExistingLiveDataOnce(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	kv := imemkv.NewKvStorage()
	defer func() { require.NoError(t, kv.Close()) }()
	metrics := mock.NewMinimalMetrics(ctrl)
	ctx := context.Background()

	unlimited := NewBackend(kv, Config{
		Prefix:                  prefix,
		Identity:                getStorageIdentity(),
		EnableEtcdCompatibility: true,
	}, metrics).(*backend)
	unlimited.SetCurrentRevision(uint64(time.Now().UnixNano()))
	_, _, err := unlimited.TxnApply(ctx, []TxnWriteOp{
		{Key: []byte("live"), Value: []byte("value")},
		{Key: []byte("deleted"), Value: []byte("ignored")},
	}, nil)
	require.NoError(t, err)
	_, _, err = unlimited.TxnApply(ctx, []TxnWriteOp{{Key: []byte("deleted"), Delete: true}}, nil)
	require.NoError(t, err)

	limited := NewBackend(kv, Config{
		Prefix:                  prefix,
		Identity:                getStorageIdentity(),
		EnableEtcdCompatibility: true,
		QuotaBackendBytes:       100,
	}, metrics).(*backend)
	limited.SetCurrentRevision(unlimited.GetCurrentRevision())
	_, _, _, err = limited.QuotaStatus(ctx)
	require.ErrorIs(t, err, ErrQuotaUninitialized)
	require.NoError(t, limited.EnsureQuotaInitialized(ctx))
	usage, quota, alarm, err := limited.QuotaStatus(ctx)
	require.NoError(t, err)
	require.Equal(t, int64(len("live")+len("value")), usage)
	require.Equal(t, int64(100), quota)
	require.False(t, alarm)

	_, _, err = limited.TxnApply(ctx, []TxnWriteOp{{Key: []byte("later"), Value: []byte("x")}}, nil)
	require.NoError(t, err)
	require.NoError(t, limited.EnsureQuotaInitialized(ctx))
	usage, _, _, err = limited.QuotaStatus(ctx)
	require.NoError(t, err)
	require.Equal(t, int64(len("live")+len("value")+len("later")+len("x")), usage)
}

func TestQuotaReenableRebuildsUsageDirtyWhileDisabled(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	kv := imemkv.NewKvStorage()
	defer func() { require.NoError(t, kv.Close()) }()
	metrics := mock.NewMinimalMetrics(ctrl)
	ctx := context.Background()

	limited := NewBackend(kv, Config{
		Prefix:                  prefix,
		Identity:                getStorageIdentity(),
		EnableEtcdCompatibility: true,
		QuotaBackendBytes:       100,
	}, metrics).(*backend)
	limited.SetCurrentRevision(uint64(time.Now().UnixNano()))
	require.NoError(t, limited.EnsureQuotaInitialized(ctx))
	_, _, err := limited.TxnApply(ctx, []TxnWriteOp{{
		Key: []byte("a"), Value: []byte("x"),
	}}, nil)
	require.NoError(t, err)

	unlimited := NewBackend(kv, Config{
		Prefix:                  prefix,
		Identity:                getStorageIdentity(),
		EnableEtcdCompatibility: true,
	}, metrics).(*backend)
	unlimited.SetCurrentRevision(limited.GetCurrentRevision())
	require.NoError(t, unlimited.EnsureQuotaInitialized(ctx))
	_, _, err = unlimited.TxnApply(ctx, []TxnWriteOp{{
		Key: []byte("b"), Value: []byte("y"),
	}}, nil)
	require.NoError(t, err)

	stale, err := limited.InternalGet(ctx, quotaUsageKey)
	require.NoError(t, err)
	staleUsage, err := decodeQuotaUsage(stale)
	require.NoError(t, err)
	require.Equal(t, int64(2), staleUsage, "disabled writes do not maintain quota usage")

	reenabled := NewBackend(kv, Config{
		Prefix:                  prefix,
		Identity:                getStorageIdentity(),
		EnableEtcdCompatibility: true,
		QuotaBackendBytes:       3,
	}, metrics).(*backend)
	reenabled.SetCurrentRevision(unlimited.GetCurrentRevision())
	_, _, _, err = reenabled.QuotaStatus(ctx)
	require.ErrorIs(t, err, ErrQuotaUninitialized)
	require.NoError(t, reenabled.EnsureQuotaInitialized(ctx))

	usage, quota, alarm, err := reenabled.QuotaStatus(ctx)
	require.NoError(t, err)
	require.Equal(t, int64(4), usage)
	require.Equal(t, int64(3), quota)
	require.True(t, alarm, "rebuilt overage must activate NOSPACE before serving")
}

func TestQuotaInitializationRebuildsLegacyUsageWithoutTrackingMarker(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	kv := imemkv.NewKvStorage()
	defer func() { require.NoError(t, kv.Close()) }()
	metrics := mock.NewMinimalMetrics(ctrl)
	ctx := context.Background()

	unlimited := NewBackend(kv, Config{
		Prefix:                  prefix,
		Identity:                getStorageIdentity(),
		EnableEtcdCompatibility: true,
	}, metrics).(*backend)
	unlimited.SetCurrentRevision(uint64(time.Now().UnixNano()))
	_, _, err := unlimited.TxnApply(ctx, []TxnWriteOp{{
		Key: []byte("legacy"), Value: []byte("value"),
	}}, nil)
	require.NoError(t, err)
	require.NoError(t, unlimited.InternalPut(ctx, quotaUsageKey, encodeQuotaUsage(1)))
	require.NoError(t, unlimited.InternalDelete(ctx, quotaTrackingKey))

	limited := NewBackend(kv, Config{
		Prefix:                  prefix,
		Identity:                getStorageIdentity(),
		EnableEtcdCompatibility: true,
		QuotaBackendBytes:       100,
	}, metrics).(*backend)
	limited.SetCurrentRevision(unlimited.GetCurrentRevision())
	require.NoError(t, limited.EnsureQuotaInitialized(ctx))

	usage, _, alarm, err := limited.QuotaStatus(ctx)
	require.NoError(t, err)
	require.Equal(t, int64(len("legacy")+len("value")), usage)
	require.False(t, alarm)
}

func TestQuotaInitializationActivatesNoSpaceForExistingOverage(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	kv := imemkv.NewKvStorage()
	defer func() { require.NoError(t, kv.Close()) }()
	metrics := mock.NewMinimalMetrics(ctrl)
	ctx := context.Background()

	unlimited := NewBackend(kv, Config{
		Prefix:                  prefix,
		Identity:                getStorageIdentity(),
		EnableEtcdCompatibility: true,
	}, metrics).(*backend)
	unlimited.SetCurrentRevision(uint64(time.Now().UnixNano()))
	_, _, err := unlimited.TxnApply(ctx, []TxnWriteOp{{
		Key: []byte("existing"), Value: []byte("value"),
	}}, nil)
	require.NoError(t, err)

	limited := NewBackend(kv, Config{
		Prefix:                  prefix,
		Identity:                getStorageIdentity(),
		EnableEtcdCompatibility: true,
		QuotaBackendBytes:       5,
	}, metrics).(*backend)
	limited.SetCurrentRevision(unlimited.GetCurrentRevision())
	require.NoError(t, limited.EnsureQuotaInitialized(ctx))
	usage, quota, alarm, err := limited.QuotaStatus(ctx)
	require.NoError(t, err)
	require.Equal(t, int64(len("existing")+len("value")), usage)
	require.Equal(t, int64(5), quota)
	require.True(t, alarm)
	removed, err := limited.DisarmNoSpace(ctx, limited.quotaAlarmMemberID())
	require.NoError(t, err)
	require.True(t, removed, "startup overage does not prevent explicit deactivation")
	_, _, err = limited.TxnApply(ctx, []TxnWriteOp{{
		Key: []byte("existing"), Value: []byte("x"),
	}}, nil)
	require.ErrorIs(t, err, ErrNoSpace)
	_, _, alarm, err = limited.QuotaStatus(ctx)
	require.NoError(t, err)
	require.True(t, alarm, "an over-quota Put after disarm must restore capped state")
}

func TestQuotaInitializationRestoresPersistedManualAlarmMetricBelowQuota(t *testing.T) {
	ctrl := gomock.NewController(t)
	t.Cleanup(ctrl.Finish)
	store := imemkv.NewKvStorage()
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	config := Config{
		Prefix:                  prefix,
		Identity:                getStorageIdentity(),
		EnableEtcdCompatibility: true,
		QuotaBackendBytes:       100,
	}
	initial := NewBackend(store, config, mock.NewMinimalMetrics(ctrl)).(*backend)
	require.NoError(t, initial.EnsureQuotaInitialized(context.Background()))
	_, err := initial.ArmNoSpace(context.Background(), 424242)
	require.NoError(t, err)

	recorder := newRecordCounters()
	restarted := NewBackend(store, config, recorder).(*backend)
	require.NoError(t, restarted.EnsureQuotaInitialized(context.Background()))
	require.Equal(t, float64(1), recorder.gauge("quota.nospace"))
	usage, quota, active, err := restarted.QuotaStatus(context.Background())
	require.NoError(t, err)
	require.Zero(t, usage)
	require.Equal(t, int64(100), quota)
	require.True(t, active)
}

func TestQuotaInitializationReconcilesCommittedUncertainUsage(t *testing.T) {
	ctrl := gomock.NewController(t)
	t.Cleanup(ctrl.Finish)
	base := imemkv.NewKvStorage()
	t.Cleanup(func() { require.NoError(t, base.Close()) })
	metrics := mock.NewMinimalMetrics(ctrl)
	ctx := context.Background()

	unlimited := NewBackend(base, Config{
		Prefix:                  prefix,
		Identity:                getStorageIdentity(),
		EnableEtcdCompatibility: true,
	}, metrics).(*backend)
	unlimited.SetCurrentRevision(uint64(time.Now().UnixNano()))
	_, _, err := unlimited.TxnApply(ctx, []TxnWriteOp{{
		Key: []byte("existing"), Value: []byte("value"),
	}}, nil)
	require.NoError(t, err)

	store := &commitThenUncertainStorage{
		KvStorage:               base,
		failReadsAfterUncertain: 2,
	}
	limited := NewBackend(store, Config{
		Prefix:                  prefix,
		Identity:                getStorageIdentity(),
		EnableEtcdCompatibility: true,
		QuotaBackendBytes:       100,
	}, metrics).(*backend)
	limited.SetCurrentRevision(unlimited.GetCurrentRevision())
	store.trigger.Store(true)
	require.NoError(t, limited.EnsureQuotaInitialized(ctx))

	usage, quota, alarm, err := limited.QuotaStatus(ctx)
	require.NoError(t, err)
	require.Equal(t, int64(len("existing")+len("value")), usage)
	require.Equal(t, int64(100), quota)
	require.False(t, alarm)
}

func TestQuotaInitializationPreservesUncommittedUncertainUsageError(t *testing.T) {
	ctrl := gomock.NewController(t)
	t.Cleanup(ctrl.Finish)
	base := imemkv.NewKvStorage()
	t.Cleanup(func() { require.NoError(t, base.Close()) })
	store := &uncommittedUncertainStorage{KvStorage: base}
	b := NewBackend(store, Config{
		Prefix:                  prefix,
		Identity:                getStorageIdentity(),
		EnableEtcdCompatibility: true,
		QuotaBackendBytes:       100,
	}, mock.NewMinimalMetrics(ctrl)).(*backend)
	b.SetCurrentRevision(uint64(time.Now().UnixNano()))

	store.trigger.Store(true)
	err := b.EnsureQuotaInitialized(context.Background())
	require.ErrorIs(t, err, storage.ErrUncertainResult)
	_, _, _, statusErr := b.QuotaStatus(context.Background())
	require.ErrorIs(t, statusErr, ErrQuotaUninitialized)
}

func TestQuotaInitializationReconcilesCommittedUncertainOverageAlarm(t *testing.T) {
	ctrl := gomock.NewController(t)
	t.Cleanup(ctrl.Finish)
	base := imemkv.NewKvStorage()
	t.Cleanup(func() { require.NoError(t, base.Close()) })
	store := &commitThenUncertainStorage{
		KvStorage:               base,
		failReadsAfterUncertain: 2,
	}
	b := NewBackend(store, Config{
		Prefix:                  prefix,
		Identity:                getStorageIdentity(),
		EnableEtcdCompatibility: true,
		QuotaBackendBytes:       5,
	}, mock.NewMinimalMetrics(ctrl)).(*backend)
	b.SetCurrentRevision(uint64(time.Now().UnixNano()))
	require.NoError(t, b.InternalPut(context.Background(), quotaUsageKey, encodeQuotaUsage(10)))
	require.NoError(t, b.InternalPut(context.Background(), quotaTrackingKey, quotaTrackingClean))

	store.trigger.Store(true)
	require.NoError(t, b.EnsureQuotaInitialized(context.Background()))
	usage, quota, alarm, err := b.QuotaStatus(context.Background())
	require.NoError(t, err)
	require.Equal(t, int64(10), usage)
	require.Equal(t, int64(5), quota)
	require.True(t, alarm)
}

func TestQuotaUsageCommitsAtomicallyWithUncertainUserTransaction(t *testing.T) {
	ctrl := gomock.NewController(t)
	t.Cleanup(ctrl.Finish)
	base := imemkv.NewKvStorage()
	t.Cleanup(func() { require.NoError(t, base.Close()) })
	store := &commitThenUncertainStorage{
		KvStorage:               base,
		failReadsAfterUncertain: 2,
	}
	b := NewBackend(store, Config{
		Prefix:                  prefix,
		Identity:                getStorageIdentity(),
		EnableEtcdCompatibility: true,
		QuotaBackendBytes:       100,
	}, mock.NewMinimalMetrics(ctrl)).(*backend)
	b.SetCurrentRevision(uint64(time.Now().UnixNano()))
	ctx := context.Background()
	require.NoError(t, b.EnsureQuotaInitialized(ctx))

	left := []byte("quota-uncertain-left")
	right := []byte("quota-uncertain-right")
	store.trigger.Store(true)
	_, revision, err := b.TxnApply(ctx, []TxnWriteOp{
		{Key: left, Value: []byte("left")},
		{Key: right, Value: []byte("right")},
	}, nil)
	require.ErrorIs(t, err, storage.ErrUncertainResult)
	require.Eventually(t, func() bool {
		return b.GetCurrentRevision() >= revision
	}, 2*time.Second, time.Millisecond)

	usage, quota, alarm, err := b.QuotaStatus(ctx)
	require.NoError(t, err)
	require.Equal(t, int64(len(left)+len("left")+len(right)+len("right")), usage)
	require.Equal(t, int64(100), quota)
	require.False(t, alarm)
	leftValue, leftRevision := liveValue(t, b, ctx, left)
	rightValue, rightRevision := liveValue(t, b, ctx, right)
	require.Equal(t, "left", leftValue)
	require.Equal(t, "right", rightValue)
	require.Equal(t, revision, leftRevision)
	require.Equal(t, revision, rightRevision)
}

func TestQuotaUsageRollsBackAtomicallyWithUncommittedUncertainUserTransaction(t *testing.T) {
	ctrl := gomock.NewController(t)
	t.Cleanup(ctrl.Finish)
	base := imemkv.NewKvStorage()
	t.Cleanup(func() { require.NoError(t, base.Close()) })
	store := &uncommittedUncertainStorage{KvStorage: base}
	b := NewBackend(store, Config{
		Prefix:                  prefix,
		Identity:                getStorageIdentity(),
		EnableEtcdCompatibility: true,
		QuotaBackendBytes:       100,
	}, mock.NewMinimalMetrics(ctrl)).(*backend)
	b.SetCurrentRevision(uint64(time.Now().UnixNano()))
	ctx := context.Background()
	require.NoError(t, b.EnsureQuotaInitialized(ctx))

	left := []byte("quota-uncommitted-left")
	right := []byte("quota-uncommitted-right")
	store.trigger.Store(true)
	_, revision, err := b.TxnApply(ctx, []TxnWriteOp{
		{Key: left, Value: []byte("left")},
		{Key: right, Value: []byte("right")},
	}, nil)
	require.ErrorIs(t, err, storage.ErrUncertainResult)
	require.Eventually(t, func() bool {
		return b.GetCurrentRevision() >= revision
	}, 2*time.Second, time.Millisecond)

	usage, quota, alarm, err := b.QuotaStatus(ctx)
	require.NoError(t, err)
	require.Zero(t, usage)
	require.Equal(t, int64(100), quota)
	require.False(t, alarm)
	leftValue, leftRevision := liveValue(t, b, ctx, left)
	rightValue, rightRevision := liveValue(t, b, ctx, right)
	require.Empty(t, leftValue)
	require.Empty(t, rightValue)
	require.Zero(t, leftRevision)
	require.Zero(t, rightRevision)
}
