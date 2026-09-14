package backend

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"sync/atomic"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/kubewharf/kubebrain/pkg/metrics/mock"
	"github.com/kubewharf/kubebrain/pkg/storage"
	"github.com/kubewharf/kubebrain/pkg/storage/memkv"
	"github.com/stretchr/testify/require"
)

type txnPreparationContextKey struct{}
type txnPreparationTestStore struct {
	storage.KvStorage
	target                 []byte
	pointReads, batchReads atomic.Int32
	afterRead              func(context.Context) error
}

func (s *txnPreparationTestStore) Get(ctx context.Context, key []byte) ([]byte, error) {
	if ctx.Value(txnPreparationContextKey{}) == s && bytes.Equal(key, s.target) {
		s.pointReads.Add(1)
	}
	return s.KvStorage.Get(ctx, key)
}

func (s *txnPreparationTestStore) BatchGet(ctx context.Context, keys [][]byte) (map[string][]byte, error) {
	values, err := s.KvStorage.(storage.BatchGetter).BatchGet(ctx, keys)
	if err != nil || ctx.Value(txnPreparationContextKey{}) != s {
		return values, err
	}
	for _, key := range keys {
		if !bytes.Equal(key, s.target) {
			continue
		}
		s.batchReads.Add(1)
		if s.afterRead != nil {
			if err := s.afterRead(ctx); err != nil {
				return nil, err
			}
		}
		break
	}
	return values, nil
}

func TestTxnPreparationReusesAlarmSnapshot(t *testing.T) {
	for _, mode := range []string{"unchanged", "unchanged-write", "changed-guard", "batch-error"} {
		t.Run(mode, func(t *testing.T) {
			raw := memkv.NewKvStorage()
			s := &txnPreparationTestStore{KvStorage: raw}
			b := NewBackend(s, Config{Prefix: "/txn-preparation", EnableEtcdCompatibility: true}, mock.NewMinimalMetrics(gomock.NewController(t))).(*backend)
			t.Cleanup(func() { require.NoError(t, b.Close()) })
			b.SetCurrentRevision(100)
			key, written := []byte("guard"), []byte("written")
			_, revision, err := b.TxnApply(context.Background(), []TxnWriteOp{{Key: key, Value: []byte("original")}, {Key: written, Value: []byte("before")}}, nil)
			require.NoError(t, err)
			s.target = b.coder.EncodeRevisionKey(key)
			if mode == "unchanged-write" {
				s.target = b.coder.EncodeRevisionKey(written)
			}
			wantErr := errors.New("batch unavailable")
			if mode == "changed-guard" {
				s.afterRead = func(ctx context.Context) error {
					batch := raw.BeginBatchWrite()
					batch.Put(s.target, binary.BigEndian.AppendUint64(nil, revision+1), 0)
					return batch.Commit(ctx)
				}
			} else if mode == "batch-error" {
				s.afterRead = func(context.Context) error { return wantErr }
			}
			ctx := context.WithValue(context.Background(), txnPreparationContextKey{}, s)
			_, _, err = b.TxnApply(ctx, []TxnWriteOp{{Key: written, Value: []byte("after")}}, []TxnGuard{{Key: key, Revision: revision}})
			switch mode {
			case "unchanged", "unchanged-write":
				require.NoError(t, err)
			case "changed-guard":
				require.ErrorIs(t, err, ErrTxnGuardConflict)
			case "batch-error":
				require.ErrorIs(t, err, wantErr)
			}
			require.EqualValues(t, 1, s.batchReads.Load())
			require.Zero(t, s.pointReads.Load(), "must not open another snapshot for prefetched index")
			value, gotRevision := liveValue(t, b, context.Background(), written)
			if mode == "unchanged" || mode == "unchanged-write" {
				require.Equal(t, "after", value)
				require.Equal(t, revision+1, gotRevision)
			} else {
				require.Equal(t, "before", value)
				require.Equal(t, revision, gotRevision)
				require.Equal(t, revision, b.GetCurrentRevision())
			}
		})
	}
}

func TestTxnPreparationFixedSnapshotAndKeyFamilies(t *testing.T) {
	b, ctx := newTxnApplyBackend(t)
	ops := []TxnWriteOp{{Key: []byte("same")}, {Key: []byte("same"), Internal: true}}
	guards := []TxnGuard{{Key: []byte("same"), Absent: true}, {Key: []byte("other"), Absent: true}}
	require.ElementsMatch(t, [][]byte{b.coder.EncodeRevisionKey([]byte("same")), b.ks.EncodeInternalKey([]byte("same")), b.coder.EncodeRevisionKey([]byte("other"))}, b.txnPreparationKeys(ctx, ops, guards))
	require.Nil(t, b.txnPreparationKeys(storage.WithSnapshotTimestamp(ctx, 123), ops, guards))
	fallback := &backend{kv: noBatchGet{KvStorage: b.kv}}
	require.Nil(t, fallback.txnPreparationKeys(ctx, ops, guards))
	key := []byte("absent-prefetch-entry")
	_, err := b.readTxnPreparationKey(ctx, key, map[string][]byte{})
	require.ErrorIs(t, err, storage.ErrKeyNotFound)
}
