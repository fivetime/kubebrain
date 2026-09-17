package backend

import (
	"context"
	"fmt"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"

	"github.com/kubewharf/kubebrain/pkg/metrics/mock"
	"github.com/kubewharf/kubebrain/pkg/storage/memkv"
)

func TestCorruptDisarmUsesBoundedHistoricalObjectReads(t *testing.T) {
	for _, repeated := range []bool{false, true} {
		t.Run(fmt.Sprintf("repeated-key=%v", repeated), func(t *testing.T) {
			ctx := context.Background()
			store := &countingWitnessIndexBatchStorage{KvStorage: memkv.NewKvStorage()}
			b := NewBackend(store, Config{
				Prefix: prefix + "/disarm-batches", Identity: getStorageIdentity(), EnableEtcdCompatibility: true,
			}, mock.NewMinimalMetrics(gomock.NewController(t))).(*backend)
			t.Cleanup(func() { require.NoError(t, b.Close()) })
			b.SetCurrentRevision(100)
			const transactions = txnRevisionIndexValidationBatch + 88
			for i := 0; i < transactions; i++ {
				key := []byte(fmt.Sprintf("%s/disarm-batches/%04d", prefix, i))
				if repeated {
					key = []byte(prefix + "/disarm-batches/repeated")
				}
				_, _, err := b.TxnApply(ctx, []TxnWriteOp{{Key: key, Value: []byte("value")}}, nil)
				require.NoError(t, err)
			}
			require.NoError(t, b.ArmCorrupt(ctx, 41001))
			store.calls.Store(0)
			store.maxKeys.Store(0)
			removed, err := b.DisarmCorrupt(ctx, 41001)
			require.NoError(t, err)
			require.True(t, removed)
			// Each window must validate historical objects, current indexes and
			// their current objects. Allow fixed metadata overhead, never N+1
			// historical object reads hidden by the distinct startup path.
			const windows = (transactions + txnRevisionIndexValidationBatch - 1) / txnRevisionIndexValidationBatch
			require.LessOrEqual(t, store.calls.Load(), int32(3*windows+2))
			require.LessOrEqual(t, store.maxKeys.Load(), int32(eventLogBatchGetSize))
		})
	}
}

func TestCorruptDisarmBatchesRetainHistoricalObjectValidation(t *testing.T) {
	for _, at := range []int{0, txnRevisionIndexValidationBatch, txnRevisionIndexValidationBatch + 86} {
		for _, missing := range []bool{false, true} {
			t.Run(fmt.Sprintf("history=%d/missing=%v", at, missing), func(t *testing.T) {
				b, ctx := newTxnApplyBackend(t)
				key := []byte(prefix + "/disarm-history-integrity")
				var target uint64
				for i := 0; i < txnRevisionIndexValidationBatch+88; i++ {
					_, revision, err := b.TxnApply(ctx, []TxnWriteOp{{Key: key, Value: []byte("value")}}, nil)
					require.NoError(t, err)
					if i == at {
						target = revision
					}
				}
				objectKey := b.coder.EncodeObjectKey(key, target)
				healthy, err := b.kv.Get(ctx, objectKey)
				require.NoError(t, err)
				corrupt := b.kv.BeginBatchWrite()
				if missing {
					corrupt.Del(objectKey)
				} else {
					// Arbitrary bytes are legal legacy values. Truncate a reserved
					// metadata envelope instead, and prove this fixture is invalid.
					invalid := append([]byte(nil), valueMetaMagicV3...)
					_, _, _, decodeErr := DecodeInlineValueChecked(invalid)
					require.ErrorIs(t, decodeErr, ErrInvalidMVCCMetadata)
					corrupt.Put(objectKey, invalid, 0)
				}
				require.NoError(t, corrupt.Commit(ctx))
				member := b.localAlarmMemberID()
				require.NoError(t, b.ArmCorrupt(ctx, member))
				removed, err := b.DisarmCorrupt(ctx, member)
				require.ErrorIs(t, err, ErrTxnWitnessCorrupt)
				require.False(t, removed, "healthy current object cannot hide corrupt historical versions")
				members, err := b.CorruptAlarms(ctx)
				require.NoError(t, err)
				require.Contains(t, members, member)
				repair := b.kv.BeginBatchWrite()
				repair.Put(objectKey, healthy, 0)
				require.NoError(t, repair.Commit(ctx))
				removed, err = b.DisarmCorrupt(ctx, member)
				require.NoError(t, err)
				require.True(t, removed)
			})
		}
	}
}
