package backend

import (
	"context"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/kubewharf/kubebrain/pkg/metrics/mock"
	"github.com/kubewharf/kubebrain/pkg/storage/memkv"
	"github.com/stretchr/testify/require"
)

// The index/alarm preparation snapshot is earlier than quota admission. Moving
// quota reads into that snapshot must not silently hide a newly raised NOSPACE
// alarm or a dirty tracking state. Usage CAS alone does not protect either key.
func TestTxnQuotaAdmissionObservesChangesAfterIndexPreparation(t *testing.T) {
	for _, mode := range []string{"nospace", "dirty-tracking"} {
		t.Run(mode, func(t *testing.T) {
			raw := memkv.NewKvStorage()
			s := &txnPreparationTestStore{KvStorage: raw}
			b := NewBackend(s, Config{Prefix: "/quota-preparation-order", EnableEtcdCompatibility: true,
				QuotaBackendBytes: 1024}, mock.NewMinimalMetrics(gomock.NewController(t))).(*backend)
			t.Cleanup(func() { require.NoError(t, b.Close()) })
			ctx := context.Background()
			b.SetCurrentRevision(100)
			require.NoError(t, b.EnsureQuotaInitialized(ctx))
			key := []byte("key")
			_, revision, err := b.TxnApply(ctx, []TxnWriteOp{{Key: key, Value: []byte("before")}}, nil)
			require.NoError(t, err)
			usageKey := b.ks.EncodeInternalKey(quotaUsageKey)
			usageBefore, err := raw.Get(ctx, usageKey)
			require.NoError(t, err)
			stateKey, stateValue, wantErr := quotaTrackingKey, quotaTrackingDirty, ErrQuotaUninitialized
			if mode == "nospace" {
				stateKey, stateValue, wantErr = quotaAlarmKey, []byte{quotaAlarmSetTag}, ErrNoSpace
			}
			s.target = b.coder.EncodeRevisionKey(key)
			s.afterRead = func(callbackCtx context.Context) error {
				// Commit only the admission state: unchanged usage must not make
				// this concurrent change invisible to the pending user write.
				batch := raw.BeginBatchWrite()
				batch.Put(b.ks.EncodeInternalKey(stateKey), stateValue, 0)
				return batch.Commit(callbackCtx)
			}
			marked := context.WithValue(ctx, txnPreparationContextKey{}, s)
			_, _, err = b.TxnApply(marked, []TxnWriteOp{{Key: key, Value: []byte("after")}}, nil)
			require.ErrorIs(t, err, wantErr)
			require.EqualValues(t, 1, s.batchReads.Load(), "must inject after the actual index snapshot")
			value, gotRevision := liveValue(t, b, ctx, key)
			require.Equal(t, "before", value)
			require.Equal(t, revision, gotRevision)
			require.Equal(t, revision, b.GetCurrentRevision())
			usageAfter, err := raw.Get(ctx, usageKey)
			require.NoError(t, err)
			require.Equal(t, usageBefore, usageAfter)
		})
	}
}
