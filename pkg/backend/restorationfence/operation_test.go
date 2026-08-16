package restorationfence

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/kubewharf/kubebrain/pkg/storage"
	"github.com/kubewharf/kubebrain/pkg/storage/memkv"
)

func testToken(t *testing.T, operation string) Token {
	t.Helper()
	token, err := NewToken(operation, strings.Repeat("a", 64), 42, "a1001")
	require.NoError(t, err)
	return token
}

func TestAcquireVerifyReleaseRestorationFence(t *testing.T) {
	store := memkv.NewKvStorage()
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	ctx := context.Background()
	token := testToken(t, "restore-1")

	resumed, err := Acquire(ctx, store, "/kubebrain-internal/ks-a1001", token)
	require.NoError(t, err)
	require.False(t, resumed)
	require.NoError(t, Verify(ctx, store, "/kubebrain-internal/ks-a1001", token))
	resumed, err = Acquire(ctx, store, "/kubebrain-internal/ks-a1001", token)
	require.NoError(t, err)
	require.True(t, resumed)

	other := testToken(t, "restore-2")
	_, err = Acquire(ctx, store, "/kubebrain-internal/ks-a1001", other)
	require.ErrorContains(t, err, "held by another operation")
	require.NoError(t, Release(ctx, store, "/kubebrain-internal/ks-a1001", token))
	require.NoError(t, VerifyOpen(ctx, store, "/kubebrain-internal/ks-a1001"))
	_, err = Acquire(ctx, store, "/kubebrain-internal/ks-a1001", other)
	require.NoError(t, err)
}

func TestAcquireRejectsPartiallyForeignFence(t *testing.T) {
	store := memkv.NewKvStorage()
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	ctx := context.Background()
	prefix := "/kubebrain-internal/ks-a1001"
	foreign, err := testToken(t, "restore-2").Bytes()
	require.NoError(t, err)
	batch := store.BeginBatchWrite()
	batch.Put(ControlKey(prefix), []byte(Open), 0)
	batch.Put(ShardKey(prefix, 17), foreign, 0)
	require.NoError(t, batch.Commit(ctx))

	_, err = Acquire(ctx, store, prefix, testToken(t, "restore-1"))
	require.ErrorContains(t, err, "held by another operation")
	value, err := store.Get(ctx, ControlKey(prefix))
	require.NoError(t, err)
	require.Equal(t, []byte(Open), value, "failed preflight must not partially close the fence")
}

func TestAcquireRejectsPartiallyMatchingFence(t *testing.T) {
	store := memkv.NewKvStorage()
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	ctx := context.Background()
	prefix := "/kubebrain-internal/ks-a1001"
	token := testToken(t, "restore-1")
	encoded, err := token.Bytes()
	require.NoError(t, err)
	batch := store.BeginBatchWrite()
	batch.Put(ControlKey(prefix), encoded, 0)
	batch.Put(ShardKey(prefix, 0), []byte(Open), 0)
	require.NoError(t, batch.Commit(ctx))

	_, err = Acquire(ctx, store, prefix, token)
	require.ErrorContains(t, err, "partially held")
	value, err := store.Get(ctx, ShardKey(prefix, 0))
	require.NoError(t, err)
	require.Equal(t, []byte(Open), value)
}

func TestAcquireFromTransfersExactPredecessor(t *testing.T) {
	store := memkv.NewKvStorage()
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	ctx := context.Background()
	prefix := "/kubebrain-internal/ks-a1001"
	predecessor := testToken(t, "source-capture")
	target := testToken(t, "target-restore")
	_, err := Acquire(ctx, store, prefix, predecessor)
	require.NoError(t, err)

	resumed, err := AcquireFrom(ctx, store, prefix, target, &predecessor)
	require.NoError(t, err)
	require.False(t, resumed)
	require.NoError(t, Verify(ctx, store, prefix, target))
	resumed, err = AcquireFrom(ctx, store, prefix, target, &predecessor)
	require.NoError(t, err)
	require.True(t, resumed)
}

func TestAcquireFromRejectsForeignToken(t *testing.T) {
	store := memkv.NewKvStorage()
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	ctx := context.Background()
	prefix := "/kubebrain-internal/ks-a1001"
	foreign := testToken(t, "foreign")
	_, err := Acquire(ctx, store, prefix, foreign)
	require.NoError(t, err)

	predecessor := testToken(t, "source-capture")
	_, err = AcquireFrom(ctx, store, prefix, testToken(t, "target-restore"), &predecessor)
	require.ErrorContains(t, err, "held by another operation")
	require.NoError(t, Verify(ctx, store, prefix, foreign))
}

func TestReleaseRejectsForeignShardWithoutOpeningAnyKey(t *testing.T) {
	store := memkv.NewKvStorage()
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	ctx := context.Background()
	prefix := "/kubebrain-internal/ks-a1001"
	token := testToken(t, "restore-1")
	_, err := Acquire(ctx, store, prefix, token)
	require.NoError(t, err)
	foreign, err := testToken(t, "restore-2").Bytes()
	require.NoError(t, err)
	batch := store.BeginBatchWrite()
	batch.Put(ShardKey(prefix, 17), foreign, 0)
	require.NoError(t, batch.Commit(ctx))

	err = Release(ctx, store, prefix, token)
	require.Error(t, err)
	control, err := store.Get(ctx, ControlKey(prefix))
	require.NoError(t, err)
	want, err := token.Bytes()
	require.NoError(t, err)
	require.Equal(t, want, control, "failed atomic release must not open the control key")
}

func TestReleaseIfKeepsFenceClosedWhenAtomicGuardFails(t *testing.T) {
	store := memkv.NewKvStorage()
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	ctx := context.Background()
	prefix := "/kubebrain-internal/ks-a1001"
	token := testToken(t, "restore-1")
	_, err := Acquire(ctx, store, prefix, token)
	require.NoError(t, err)
	sentinel := errors.New("completion evidence changed")

	err = ReleaseIf(ctx, store, prefix, token, func(context.Context, storage.AtomicBatch) error {
		return sentinel
	})

	require.ErrorIs(t, err, sentinel)
	require.NoError(t, Verify(ctx, store, prefix, token))
}
