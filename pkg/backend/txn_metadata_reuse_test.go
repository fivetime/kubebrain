package backend

import (
	"bytes"
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	proto "github.com/kubewharf/kubebrain-client/api/v2rpc"
	"github.com/kubewharf/kubebrain/pkg/metrics/mock"
	"github.com/kubewharf/kubebrain/pkg/storage"
	imemkv "github.com/kubewharf/kubebrain/pkg/storage/memkv"
	"github.com/stretchr/testify/require"
)

type metadataReuseContextKey struct{}

type metadataReuseStore struct {
	storage.KvStorage
	metadataKey []byte
	reads       atomic.Int64
	readErr     error
}

func (s *metadataReuseStore) Get(ctx context.Context, key []byte) ([]byte, error) {
	if ctx.Value(metadataReuseContextKey{}) == s && bytes.Equal(key, s.metadataKey) {
		s.reads.Add(1)
		if s.readErr != nil {
			return nil, s.readErr
		}
	}
	return s.KvStorage.Get(ctx, key)
}

func (s *metadataReuseStore) BatchGet(ctx context.Context, keys [][]byte) (map[string][]byte, error) {
	return s.KvStorage.(storage.BatchGetter).BatchGet(ctx, keys)
}

func (s *metadataReuseStore) UnwrapKvStorage() storage.KvStorage { return s.KvStorage }

func TestTxnApplyReusesValidatedLegacyMetadata(t *testing.T) {
	testTxnApplyLegacyMetadataRead(t, nil)
}

func TestTxnApplyLegacyMetadataReadFailurePreservesObject(t *testing.T) {
	testTxnApplyLegacyMetadataRead(t, errors.New("injected legacy metadata read failure"))
}

func testTxnApplyLegacyMetadataRead(t *testing.T, readErr error) {
	t.Helper()
	ctrl := gomock.NewController(t)
	store := &metadataReuseStore{KvStorage: imemkv.NewKvStorage(), readErr: readErr}
	b := NewBackend(store, Config{Prefix: prefix, Identity: getStorageIdentity(), EnableEtcdCompatibility: true}, mock.NewMinimalMetrics(ctrl)).(*backend)
	t.Cleanup(func() { require.NoError(t, b.Close()) })
	b.SetCurrentRevision(uint64(time.Now().UnixNano()))
	key := []byte(prefix + "/metadata-reuse/key")
	created, err := b.Create(t.Context(), &proto.CreateRequest{Key: key, Value: []byte("old")})
	require.NoError(t, err)
	waitCommitted(t, b, created.Header.Revision)
	store.metadataKey = b.coder.EncodeObjectKey(b.etcdMetadataUserKey(key), created.Header.Revision)
	legacy := b.kv.BeginBatchWrite()
	legacy.Put(b.coder.EncodeObjectKey(key, created.Header.Revision), []byte("old"), 0)
	b.putEtcdMetadata(legacy, key, created.Header.Revision, EtcdMetadata{CreateRevision: created.Header.Revision, Version: 1})
	require.NoError(t, legacy.Commit(t.Context()))
	ctx := context.WithValue(t.Context(), metadataReuseContextKey{}, store)
	results, revision, err := b.TxnApply(WithPreviousLease(ctx, 0), []TxnWriteOp{{
		Key: key, Value: []byte("new"), PrevLeaseKnown: true, PrevLease: 0,
	}}, []TxnGuard{{Key: key, Revision: created.Header.Revision}})
	if readErr != nil {
		require.ErrorIs(t, err, readErr)
		require.Empty(t, results)
		require.EqualValues(t, 0, revision)
		require.EqualValues(t, 1, store.reads.Load())
		value, currentRevision := liveValue(t, b, t.Context(), key)
		require.Equal(t, "old", value)
		require.Equal(t, created.Header.Revision, currentRevision)
		stored, getErr := b.kv.Get(t.Context(), b.coder.EncodeObjectKey(key, currentRevision))
		require.NoError(t, getErr)
		require.Equal(t, []byte("old"), stored, "failed preparation must not migrate or replace the old object")
		return
	}
	require.NoError(t, err)
	require.EqualValues(t, 1, store.reads.Load(), "one validated legacy metadata lookup per write preparation, not RPC count")
	require.Len(t, results, 1)
	require.Equal(t, created.Header.Revision, results[0].Meta.CreateRevision)
	require.EqualValues(t, 2, results[0].Meta.Version)
	require.Equal(t, []byte("old"), StripInlineValue(results[0].PrevValue))
	waitCommitted(t, b, revision)
	value, currentRevision := liveValue(t, b, t.Context(), key)
	require.Equal(t, "new", value)
	require.Equal(t, revision, currentRevision)
}
