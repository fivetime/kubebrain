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
