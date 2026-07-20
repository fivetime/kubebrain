package backend

import (
	"context"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"

	"github.com/kubewharf/kubebrain/pkg/metrics/mock"
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
	require.ErrorIs(t, b.DisarmNoSpace(ctx), ErrNoSpace)

	_, _, err = b.TxnApply(ctx, []TxnWriteOp{{Key: []byte("b"), Delete: true}}, nil)
	require.NoError(t, err, "NOSPACE must allow deletes that recover capacity")
	usage, _, alarm, err = b.QuotaStatus(ctx)
	require.NoError(t, err)
	require.Equal(t, int64(8), usage)
	require.True(t, alarm, "capacity recovery does not implicitly disarm etcd's sticky alarm")
	require.NoError(t, b.DisarmNoSpace(ctx))
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
	require.NoError(t, b.DisarmNoSpace(ctx))
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
	require.ErrorIs(t, b.ArmNoSpace(ctx), ErrQuotaDisabled)
	require.NoError(t, b.DisarmNoSpace(ctx))
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
	require.ErrorIs(t, limited.DisarmNoSpace(ctx), ErrNoSpace)
}
