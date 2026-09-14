package backend

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/kubewharf/kubebrain/pkg/metrics/mock"
	"github.com/kubewharf/kubebrain/pkg/storage"
	"github.com/kubewharf/kubebrain/pkg/storage/memkv"
	"github.com/stretchr/testify/require"
)

func TestTxnPreviousObjectsBatchRead(t *testing.T) {
	for _, mode := range []string{"update", "delete", "batch-error", "corrupt"} {
		t.Run(mode, func(t *testing.T) {
			raw := memkv.NewKvStorage()
			s := &txnPreparationTestStore{KvStorage: raw}
			b := NewBackend(s, Config{Prefix: "/previous-batch", EnableEtcdCompatibility: true}, mock.NewMinimalMetrics(gomock.NewController(t))).(*backend)
			t.Cleanup(func() { require.NoError(t, b.Close()) })
			b.SetCurrentRevision(100)
			a, z := []byte("/previous-batch/a"), []byte("/previous-batch/z")
			_, rev, err := b.TxnApply(context.Background(), []TxnWriteOp{{Key: a, Value: []byte("a")}, {Key: z, Value: []byte("z")}}, nil)
			require.NoError(t, err)
			s.target = b.coder.EncodeObjectKey(a, rev)
			failure := errors.New("previous batch unavailable")
			if mode == "batch-error" {
				s.afterRead = func(context.Context) error { return failure }
			}
			if mode == "corrupt" {
				batch := raw.BeginBatchWrite()
				batch.Put(s.target, []byte{0, 'k', 'b', 3}, 0)
				require.NoError(t, batch.Commit(context.Background()))
			}
			ctx := context.WithValue(context.Background(), txnPreparationContextKey{}, s)
			ops := []TxnWriteOp{{Key: a, Value: []byte("new-a"), Delete: mode == "delete"}, {Key: z, Value: []byte("new-z"), DiscardPrevValue: true}}
			results, newRev, err := b.TxnApply(ctx, ops, nil)
			require.EqualValues(t, 1, s.batchReads.Load())
			if mode == "corrupt" {
				require.EqualValues(t, 1, s.pointReads.Load(), "durable corruption witnessing must retain its independent reread")
			} else {
				require.Zero(t, s.pointReads.Load(), "present previous object must come from shared batch")
			}
			switch mode {
			case "batch-error", "corrupt":
				if mode == "batch-error" {
					require.ErrorIs(t, err, failure)
				} else {
					require.ErrorIs(t, err, ErrInvalidMVCCMetadata)
				}
				require.Nil(t, results)
				require.Equal(t, rev, b.GetCurrentRevision())
				index, readErr := raw.Get(context.Background(), b.coder.EncodeRevisionKey(z))
				require.NoError(t, readErr)
				require.Equal(t, binary.BigEndian.AppendUint64(nil, rev), index)
			default:
				require.NoError(t, err)
				require.Equal(t, rev+1, newRev)
				require.Len(t, results, 2)
				require.Equal(t, []byte("a"), StripInlineValue(results[0].PrevValue))
				value, gotRev := liveValue(t, b, context.Background(), z)
				require.Equal(t, "new-z", value)
				require.Equal(t, newRev, gotRev)
			}
		})
	}
}

func TestTxnPreviousObjectsBatchBound(t *testing.T) {
	s := &txnPreparationTestStore{KvStorage: memkv.NewKvStorage()}
	b := NewBackend(s, Config{Prefix: "/previous-bound", EnableEtcdCompatibility: true}, mock.NewMinimalMetrics(gomock.NewController(t))).(*backend)
	t.Cleanup(func() { require.NoError(t, b.Close()) })
	ctx := context.WithValue(context.Background(), txnPreparationContextKey{}, s)
	ops := make([]TxnWriteOp, maxTxnPreviousPrefetchKeys+1)
	indexes := make(map[string][]byte, len(ops))
	for i := range ops {
		ops[i] = TxnWriteOp{Key: []byte(fmt.Sprintf("/bound/%d", i))}
		indexes[string(b.coder.EncodeRevisionKey(ops[i].Key))] = binary.BigEndian.AppendUint64(nil, 100)
	}
	s.target = b.coder.EncodeObjectKey(ops[0].Key, 100)
	values, err := b.prefetchTxnPreviousObjects(ctx, ops, indexes)
	require.NoError(t, err)
	require.Nil(t, values)
	require.Zero(t, s.batchReads.Load(), "oversized prefetch must fall back before reading any old values")
	_, err = b.prefetchTxnPreviousObjects(ctx, ops[:maxTxnPreviousPrefetchKeys], indexes)
	require.NoError(t, err)
	require.EqualValues(t, 1, s.batchReads.Load(), "the inclusive boundary remains eligible")
}

func TestTxnPreviousObjectsBatchFallback(t *testing.T) {
	b, ctx := newTxnApplyBackend(t)
	a, z := []byte("a"), []byte("z")
	ops := []TxnWriteOp{{Key: a}, {Key: z}}
	for _, mode := range []string{"single", "fixed", "missing", "malformed", "tombstone", "no-batch", "no-index-snapshot"} {
		t.Run(mode, func(t *testing.T) {
			indexes := map[string][]byte{
				string(b.coder.EncodeRevisionKey(a)): binary.BigEndian.AppendUint64(nil, 100),
				string(b.coder.EncodeRevisionKey(z)): binary.BigEndian.AppendUint64(nil, 100),
			}
			callCtx, callOps, target := ctx, ops, b
			switch mode {
			case "single":
				callOps = ops[:1]
			case "fixed":
				callCtx = storage.WithSnapshotTimestamp(ctx, 123)
			case "missing":
				delete(indexes, string(b.coder.EncodeRevisionKey(z)))
			case "malformed":
				indexes[string(b.coder.EncodeRevisionKey(z))] = []byte{1}
			case "tombstone":
				indexes[string(b.coder.EncodeRevisionKey(z))] = append(binary.BigEndian.AppendUint64(nil, 100), 1)
			case "no-batch":
				target = &backend{kv: noBatchGet{KvStorage: b.kv}}
			case "no-index-snapshot":
				indexes = nil
			}
			values, err := target.prefetchTxnPreviousObjects(callCtx, callOps, indexes)
			require.NoError(t, err)
			require.Nil(t, values)
		})
	}
}

func TestTxnPreviousObjectConsumesPrefetchedReference(t *testing.T) {
	b, ctx := newTxnApplyBackend(t)
	key := []byte("/consume-prefetched")
	physicalKey := string(b.coder.EncodeObjectKey(key, 100))
	values := map[string][]byte{physicalKey: []byte("previous"), "unrelated": []byte("keep")}
	value, err := b.readPrefetchedTxnPreviousObject(ctx, key, 100, values)
	require.NoError(t, err)
	require.Equal(t, []byte("previous"), value)
	require.NotContains(t, values, physicalKey)
	require.Equal(t, []byte("keep"), values["unrelated"])
}

func TestTxnPreviousObjectBatchMissRetainsLookup(t *testing.T) {
	b, ctx := newTxnApplyBackend(t)
	key := []byte("/batch-miss")
	_, revision, err := b.TxnApply(ctx, []TxnWriteOp{{Key: key, Value: []byte("previous")}}, nil)
	require.NoError(t, err)
	// A partial batch response is not proof that the physical version is absent.
	// Retain the original point/historical lookup instead of treating it as a
	// create, skipping metadata validation, or returning an empty previous value.
	value, err := b.readPrefetchedTxnPreviousObject(ctx, key, revision, map[string][]byte{})
	require.NoError(t, err)
	require.Equal(t, []byte("previous"), StripInlineValue(value))
}
