package backend

import (
	"context"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"

	"github.com/kubewharf/kubebrain/pkg/metrics/mock"
	"github.com/kubewharf/kubebrain/pkg/storage"
	imemkv "github.com/kubewharf/kubebrain/pkg/storage/memkv"
)

func TestInternalCASAtomicAndRevisionNeutral(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	kv := imemkv.NewKvStorage()
	defer func() { require.NoError(t, kv.Close()) }()
	b := NewBackend(kv, Config{Prefix: prefix, Identity: getStorageIdentity(), EnableEtcdCompatibility: true}, mock.NewMinimalMetrics(ctrl)).(*backend)
	revision := uint64(time.Now().UnixNano())
	b.SetCurrentRevision(revision)
	ctx := context.Background()

	require.NoError(t, b.InternalCAS(ctx, []InternalCASOp{
		{Key: []byte("auth/config"), Value: []byte("r1")},
		{Key: []byte("auth/users/root"), Value: []byte("user-v1")},
	}))
	require.Equal(t, revision, b.GetCurrentRevision(), "internal transaction must not consume user revision")

	require.ErrorIs(t, b.InternalCAS(ctx, []InternalCASOp{
		{Key: []byte("auth/config"), ExpectedExists: true, Expected: []byte("stale"), Value: []byte("r2")},
		{Key: []byte("auth/users/root"), ExpectedExists: true, Expected: []byte("user-v1"), Value: []byte("user-v2")},
	}), storage.ErrCASFailed)
	config, err := b.InternalGet(ctx, []byte("auth/config"))
	require.NoError(t, err)
	require.Equal(t, "r1", string(config))
	user, err := b.InternalGet(ctx, []byte("auth/users/root"))
	require.NoError(t, err)
	require.Equal(t, "user-v1", string(user), "guard conflict must leave every op unapplied")

	require.NoError(t, b.InternalCAS(ctx, []InternalCASOp{
		{Key: []byte("auth/config"), ExpectedExists: true, Expected: []byte("r1"), Value: []byte("r2")},
		{Key: []byte("auth/users/root"), ExpectedExists: true, Expected: []byte("user-v1"), Delete: true},
	}))
	config, err = b.InternalGet(ctx, []byte("auth/config"))
	require.NoError(t, err)
	require.Equal(t, "r2", string(config))
	_, err = b.InternalGet(ctx, []byte("auth/users/root"))
	require.ErrorIs(t, err, storage.ErrKeyNotFound)
	require.Equal(t, revision, b.GetCurrentRevision())
}

func TestInternalPutCorruptGuardedIsRevisionNeutralAndRejectsActiveAlarm(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	kv := imemkv.NewKvStorage()
	defer func() { require.NoError(t, kv.Close()) }()
	b := NewBackend(kv, Config{Prefix: prefix, Identity: getStorageIdentity(), EnableEtcdCompatibility: true}, mock.NewMinimalMetrics(ctrl)).(*backend)
	revision := uint64(time.Now().UnixNano())
	b.SetCurrentRevision(revision)
	ctx := context.Background()
	key := []byte("leases/4503001")

	require.NoError(t, b.ArmCorrupt(ctx, 4503001))
	require.ErrorIs(t, b.InternalPutCorruptGuarded(ctx, key, []byte("lease")), ErrCorruptAlarmActive)
	_, err := b.InternalGet(ctx, key)
	require.ErrorIs(t, err, storage.ErrKeyNotFound)
	removed, err := b.DisarmCorrupt(ctx, 4503001)
	require.NoError(t, err)
	require.True(t, removed)

	require.NoError(t, b.InternalPutCorruptGuarded(ctx, key, []byte("lease")))
	value, err := b.InternalGet(ctx, key)
	require.NoError(t, err)
	require.Equal(t, []byte("lease"), value)
	require.Equal(t, revision, b.GetCurrentRevision(), "guarded internal metadata must not consume a user revision")
}

func TestInternalCASCorruptCommitGuardRejectsActiveAlarm(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	kv := imemkv.NewKvStorage()
	defer func() { require.NoError(t, kv.Close()) }()
	b := NewBackend(kv, Config{Prefix: prefix, Identity: getStorageIdentity(), EnableEtcdCompatibility: true}, mock.NewMinimalMetrics(ctrl)).(*backend)
	ctx := context.Background()
	key := []byte("leases/guarded-delete")
	value := []byte("lease-generation-a")
	require.NoError(t, b.InternalPut(ctx, key, value))
	require.NoError(t, b.ArmCorrupt(ctx, 4503002))

	err := b.InternalCAS(WithCorruptAlarmCommitGuard(ctx), []InternalCASOp{{
		Key: key, Expected: value, ExpectedExists: true, Delete: true,
	}})
	require.ErrorIs(t, err, ErrCorruptAlarmActive)
	stored, err := b.InternalGet(ctx, key)
	require.NoError(t, err)
	require.Equal(t, value, stored)

	removed, err := b.DisarmCorrupt(ctx, 4503002)
	require.NoError(t, err)
	require.True(t, removed)
	require.NoError(t, b.InternalCAS(WithCorruptAlarmCommitGuard(ctx), []InternalCASOp{{
		Key: key, Expected: value, ExpectedExists: true, Delete: true,
	}}))
	_, err = b.InternalGet(ctx, key)
	require.ErrorIs(t, err, storage.ErrKeyNotFound)
}
