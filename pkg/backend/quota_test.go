package backend

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"

	"github.com/kubewharf/kubebrain/pkg/metrics/mock"
	"github.com/kubewharf/kubebrain/pkg/storage"
	imemkv "github.com/kubewharf/kubebrain/pkg/storage/memkv"
)

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
	_, err = b.DisarmNoSpace(ctx, b.quotaAlarmMemberID())
	require.ErrorIs(t, err, ErrNoSpace)

	_, _, err = b.TxnApply(ctx, []TxnWriteOp{{Key: []byte("b"), Delete: true}}, nil)
	require.NoError(t, err, "NOSPACE must allow deletes that recover capacity")
	usage, _, alarm, err = b.QuotaStatus(ctx)
	require.NoError(t, err)
	require.Equal(t, int64(8), usage)
	require.True(t, alarm, "capacity recovery does not implicitly disarm etcd's sticky alarm")
	removed, err := b.DisarmNoSpace(ctx, b.quotaAlarmMemberID())
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
	_, err = b.ArmNoSpace(ctx, 0)
	require.ErrorIs(t, err, ErrQuotaDisabled)
	removed, err := b.DisarmNoSpace(ctx, b.quotaAlarmMemberID())
	require.NoError(t, err)
	require.False(t, removed)
}

func TestNoSpaceAlarmPersistsOwnerAndGuardsDisarm(t *testing.T) {
	b, ctx := newQuotaBackend(t, 10)
	wantOwner := b.quotaAlarmMemberID()
	owner, err := b.ArmNoSpace(ctx, 0)
	require.NoError(t, err)
	require.Equal(t, wantOwner, owner)
	owner, active, err := b.NoSpaceAlarm(ctx)
	require.NoError(t, err)
	require.True(t, active)
	require.Equal(t, wantOwner, owner)

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

func TestArmNoSpacePersistsExplicitOwnerAndKeepsFirstOwner(t *testing.T) {
	b, ctx := newQuotaBackend(t, 10)
	const explicitOwner uint64 = 424242

	owner, err := b.ArmNoSpace(ctx, explicitOwner)
	require.NoError(t, err)
	require.Equal(t, explicitOwner, owner)

	owner, err = b.ArmNoSpace(ctx, explicitOwner+1)
	require.NoError(t, err)
	require.Equal(t, explicitOwner, owner, "repeated activation must preserve the persisted owner")
}

func TestArmNoSpaceConcurrentActivationReturnsPersistedOwner(t *testing.T) {
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
	persisted, active, err := b.NoSpaceAlarm(ctx)
	require.NoError(t, err)
	require.True(t, active)
	require.NotZero(t, persisted)
	for owner := range owners {
		require.Equal(t, persisted, owner)
	}
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
	_, err = limited.DisarmNoSpace(ctx, limited.quotaAlarmMemberID())
	require.ErrorIs(t, err, ErrNoSpace)
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

	store.trigger.Store(true)
	require.NoError(t, b.EnsureQuotaInitialized(context.Background()))
	usage, quota, alarm, err := b.QuotaStatus(context.Background())
	require.NoError(t, err)
	require.Equal(t, int64(10), usage)
	require.Equal(t, int64(5), quota)
	require.True(t, alarm)
}
