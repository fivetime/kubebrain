// Copyright 2026 ByteDance and/or its affiliates
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package backend

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"

	"github.com/kubewharf/kubebrain/pkg/metrics/mock"
	"github.com/kubewharf/kubebrain/pkg/storage"
	imemkv "github.com/kubewharf/kubebrain/pkg/storage/memkv"
)

var errWitnessScanUnavailable = errors.New("witness scan unavailable")

type witnessScanErrorStorage struct{ storage.KvStorage }

func (w witnessScanErrorStorage) Iter(context.Context, []byte, []byte, uint64, uint64) (storage.Iter, error) {
	return nil, errWitnessScanUnavailable
}

type witnessEventScanErrorStorage struct {
	storage.KvStorage
	calls atomic.Int32
}

func (w *witnessEventScanErrorStorage) Iter(ctx context.Context, start, end []byte, timestamp, limit uint64) (storage.Iter, error) {
	if w.calls.Add(1) == 2 {
		return nil, errWitnessScanUnavailable
	}
	return w.KvStorage.Iter(ctx, start, end, timestamp, limit)
}

func TestLeadershipRestartValidatesPersistentTxnWitnessAndRecoversAfterRepair(t *testing.T) {
	ctrl := gomock.NewController(t)
	t.Cleanup(ctrl.Finish)
	store := imemkv.NewKvStorage()
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	config := Config{Prefix: prefix, Identity: getStorageIdentity(), EnableEtcdCompatibility: true}
	ctx := context.Background()

	initial := NewBackend(store, config, mock.NewMinimalMetrics(ctrl)).(*backend)
	initial.SetCurrentRevision(uint64(time.Now().UnixNano()))
	keys := [][]byte{
		[]byte(prefix + "/restart-witness/left"),
		[]byte(prefix + "/restart-witness/right"),
	}
	_, revision, err := initial.TxnApply(ctx, []TxnWriteOp{
		{Key: keys[0], Value: []byte("left")},
		{Key: keys[1], Value: []byte("right")},
	}, nil)
	require.NoError(t, err)

	witnessKey := initial.ks.EncodeInternalKey(txnWitnessLogicalKey(revision))
	witness, err := store.Get(ctx, witnessKey)
	require.NoError(t, err)
	_, err = decodeTxnWitness(witness)
	require.NoError(t, err, "successful user transaction must persist a restart witness")

	markerKey := initial.ks.EncodeEventLogKey(revision, keys[1])
	markerValue, err := store.Get(ctx, markerKey)
	require.NoError(t, err)
	corrupt := store.BeginBatchWrite()
	corrupt.Put(markerKey, []byte("corrupt-marker"), 0)
	require.NoError(t, corrupt.Commit(ctx))

	restarted := NewBackend(store, config, mock.NewMinimalMetrics(ctrl)).(*backend)
	require.NoError(t, restarted.InitializeLeadershipRevision(ctx, 0),
		"witness corruption is converted into a persistent alarm, not an unsafe startup failure")
	members, err := restarted.CorruptAlarms(ctx)
	require.NoError(t, err)
	require.Equal(t, []uint64{restarted.localAlarmMemberID()}, members)

	repair := store.BeginBatchWrite()
	repair.Put(markerKey, markerValue, 0)
	require.NoError(t, repair.Commit(ctx))
	removed, err := restarted.DisarmCorrupt(ctx, restarted.localAlarmMemberID())
	require.NoError(t, err)
	require.True(t, removed)
	require.NoError(t, restarted.InitializeLeadershipRevision(ctx, 0))
	members, err = restarted.CorruptAlarms(ctx)
	require.NoError(t, err)
	require.Empty(t, members, "a repaired witness remains healthy after explicit disarm")
}

func TestLeadershipRestartStillValidatesWitnessBelowLeadershipWatermark(t *testing.T) {
	b, ctx := newTxnApplyBackend(t)
	key := []byte(prefix + "/restart-witness/failover-watermark")
	_, revision, err := b.TxnApply(ctx, []TxnWriteOp{{Key: key, Value: []byte("value")}}, nil)
	require.NoError(t, err)
	// Leadership acquisition also advances this watermark. It must not suppress
	// validation of a seal written by the new binary in the previous term.
	require.NoError(t, b.advanceEventLogStartStorage(ctx, revision))
	batch := b.kv.BeginBatchWrite()
	batch.Put(b.ks.EncodeEventLogKey(revision, key), []byte("corrupt-after-failover"), 0)
	require.NoError(t, batch.Commit(ctx))
	require.NoError(t, b.InitializeLeadershipRevision(ctx, 0))
	members, err := b.CorruptAlarms(ctx)
	require.NoError(t, err)
	require.Equal(t, []uint64{b.localAlarmMemberID()}, members)
}

func TestEventLogCleanupDeletesWitnessBeforeCoveredEntries(t *testing.T) {
	b, ctx := newTxnApplyBackend(t)
	key := []byte(prefix + "/restart-witness/cleanup")
	_, revision, err := b.TxnApply(ctx, []TxnWriteOp{{Key: key, Value: []byte("value")}}, nil)
	require.NoError(t, err)
	witnessKey := b.ks.EncodeInternalKey(txnWitnessLogicalKey(revision))
	_, err = b.kv.Get(ctx, witnessKey)
	require.NoError(t, err)

	b.cleanupEventLog(ctx, revision+1)
	_, err = b.kv.Get(ctx, witnessKey)
	require.ErrorIs(t, err, storage.ErrKeyNotFound)
	_, err = b.kv.Get(ctx, b.ks.EncodeEventLogKey(revision, key))
	require.ErrorIs(t, err, storage.ErrKeyNotFound)
}

func TestLeadershipWitnessScanTransportErrorDoesNotArmCorrupt(t *testing.T) {
	ctrl := gomock.NewController(t)
	t.Cleanup(ctrl.Finish)
	store := imemkv.NewKvStorage()
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	config := Config{Prefix: prefix, Identity: getStorageIdentity(), EnableEtcdCompatibility: true}
	b := NewBackend(witnessScanErrorStorage{KvStorage: store}, config, mock.NewMinimalMetrics(ctrl)).(*backend)
	err := b.InitializeLeadershipRevision(context.Background(), 0)
	require.ErrorIs(t, err, errWitnessScanUnavailable)
	members, alarmErr := b.CorruptAlarms(context.Background())
	require.NoError(t, alarmErr)
	require.Empty(t, members)
}

func TestLeadershipWitnessEventScanTransportErrorDoesNotArmCorrupt(t *testing.T) {
	ctrl := gomock.NewController(t)
	t.Cleanup(ctrl.Finish)
	store := imemkv.NewKvStorage()
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	config := Config{Prefix: prefix, Identity: getStorageIdentity(), EnableEtcdCompatibility: true}
	ctx := context.Background()
	initial := NewBackend(store, config, mock.NewMinimalMetrics(ctrl)).(*backend)
	initial.SetCurrentRevision(uint64(time.Now().UnixNano()))
	_, _, err := initial.TxnApply(ctx, []TxnWriteOp{{
		Key: []byte(prefix + "/restart-witness/transient-event-scan"), Value: []byte("value"),
	}}, nil)
	require.NoError(t, err)

	wrapped := &witnessEventScanErrorStorage{KvStorage: store}
	restarted := NewBackend(wrapped, config, mock.NewMinimalMetrics(ctrl)).(*backend)
	err = restarted.InitializeLeadershipRevision(ctx, 0)
	require.ErrorIs(t, err, errWitnessScanUnavailable)
	members, alarmErr := restarted.CorruptAlarms(ctx)
	require.NoError(t, alarmErr)
	require.Empty(t, members)
}

func TestLeadershipWitnessStreamingMergeValidatesSparseLargeWindow(t *testing.T) {
	b, ctx := newTxnApplyBackend(t)
	const transactions = 256
	var revisions []uint64
	for i := 0; i < transactions; i++ {
		_, revision, err := b.TxnApply(ctx, []TxnWriteOp{
			{Key: []byte(fmt.Sprintf("%s/restart-witness/stream/%04d/z", prefix, i)), Value: []byte("z")},
			{Key: []byte(fmt.Sprintf("%s/restart-witness/stream/%04d/a", prefix, i)), Value: []byte("a")},
		}, nil)
		require.NoError(t, err)
		revisions = append(revisions, revision)
	}
	// Model a legacy event revision with no seal between sealed revisions; the
	// merge must skip it without assigning it to either neighboring witness.
	legacy := b.kv.BeginBatchWrite()
	legacy.Del(b.ks.EncodeInternalKey(txnWitnessLogicalKey(revisions[transactions/2])))
	require.NoError(t, legacy.Commit(ctx))

	require.NoError(t, b.InitializeLeadershipRevision(ctx, 0))
	members, err := b.CorruptAlarms(ctx)
	require.NoError(t, err)
	require.Empty(t, members)

	corruptRevision := revisions[transactions-2]
	corruptKey := b.ks.EncodeEventLogKey(corruptRevision,
		[]byte(fmt.Sprintf("%s/restart-witness/stream/%04d/z", prefix, transactions-2)))
	batch := b.kv.BeginBatchWrite()
	batch.Del(corruptKey)
	require.NoError(t, batch.Commit(ctx))
	require.NoError(t, b.InitializeLeadershipRevision(ctx, 0))
	members, err = b.CorruptAlarms(ctx)
	require.NoError(t, err)
	require.Equal(t, []uint64{b.localAlarmMemberID()}, members)
}

func TestLeadershipWithoutWitnessDoesNotOpenEventScan(t *testing.T) {
	ctrl := gomock.NewController(t)
	t.Cleanup(ctrl.Finish)
	store := imemkv.NewKvStorage()
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	config := Config{Prefix: prefix, Identity: getStorageIdentity(), EnableEtcdCompatibility: true}
	wrapped := &witnessEventScanErrorStorage{KvStorage: store}
	b := NewBackend(wrapped, config, mock.NewMinimalMetrics(ctrl)).(*backend)
	require.NoError(t, b.InitializeLeadershipRevision(context.Background(), 0))
	require.Equal(t, int32(1), wrapped.calls.Load(), "legacy/no-witness startup must not scan the event log")
}
