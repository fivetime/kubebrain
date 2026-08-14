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

type blockingWitnessScanStorage struct {
	storage.KvStorage
	enabled  atomic.Bool
	entered  chan struct{}
	release  chan struct{}
	signaled atomic.Bool
}

type blockingRevisionIndexRecheckStorage struct {
	storage.KvStorage
	indexKey []byte
	enabled  atomic.Bool
	reads    atomic.Int32
	blocked  chan struct{}
	release  chan struct{}
}

type countingWitnessIndexBatchStorage struct {
	storage.KvStorage
	calls atomic.Int32
}

func (s *countingWitnessIndexBatchStorage) BatchGet(
	ctx context.Context, keys [][]byte,
) (map[string][]byte, error) {
	s.calls.Add(1)
	return s.KvStorage.(storage.BatchGetter).BatchGet(ctx, keys)
}

func (s *blockingRevisionIndexRecheckStorage) Get(ctx context.Context, key []byte) ([]byte, error) {
	if s.enabled.Load() && string(key) == string(s.indexKey) && s.reads.Add(1) == 3 {
		close(s.blocked)
		select {
		case <-s.release:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	return s.KvStorage.Get(ctx, key)
}

func (w *blockingWitnessScanStorage) Iter(ctx context.Context, start, end []byte, timestamp, limit uint64) (storage.Iter, error) {
	if w.enabled.Load() && w.signaled.CompareAndSwap(false, true) {
		close(w.entered)
		select {
		case <-w.release:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	return w.KvStorage.Iter(ctx, start, end, timestamp, limit)
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
	removed, err := restarted.DisarmCorrupt(ctx, restarted.localAlarmMemberID())
	require.ErrorIs(t, err, ErrTxnWitnessCorrupt)
	require.False(t, removed, "unrepaired durable evidence must keep the write fence armed")
	members, err = restarted.CorruptAlarms(ctx)
	require.NoError(t, err)
	require.Equal(t, []uint64{restarted.localAlarmMemberID()}, members)

	repair := store.BeginBatchWrite()
	repair.Put(markerKey, markerValue, 0)
	require.NoError(t, repair.Commit(ctx))
	removed, err = restarted.DisarmCorrupt(ctx, restarted.localAlarmMemberID())
	require.NoError(t, err)
	require.True(t, removed)
	require.NoError(t, restarted.InitializeLeadershipRevision(ctx, 0))
	members, err = restarted.CorruptAlarms(ctx)
	require.NoError(t, err)
	require.Empty(t, members, "a repaired witness remains healthy after explicit disarm")
}

func TestLeadershipRestartArmsCorruptForWitnessedRevisionIndex(t *testing.T) {
	for _, test := range []struct {
		name    string
		corrupt func(*backend, []byte, uint64, uint64) []byte
	}{
		{
			name: "stale valid revision",
			corrupt: func(_ *backend, _ []byte, createRevision, _ uint64) []byte {
				return uint64ToBytes(createRevision)
			},
		},
		{
			name: "live tombstone mismatch",
			corrupt: func(_ *backend, _ []byte, _, updateRevision uint64) []byte {
				return append(uint64ToBytes(updateRevision), 0)
			},
		},
		{
			name:    "missing",
			corrupt: func(_ *backend, _ []byte, _, _ uint64) []byte { return nil },
		},
		{
			name:    "malformed",
			corrupt: func(_ *backend, _ []byte, _, _ uint64) []byte { return []byte{1} },
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			b, ctx := newTxnApplyBackend(t)
			key := []byte(prefix + "/restart-witness/index/" + test.name)
			_, createRevision, err := b.TxnApply(ctx, []TxnWriteOp{{Key: key, Value: []byte("v1")}}, nil)
			require.NoError(t, err)
			_, updateRevision, err := b.TxnApply(ctx, []TxnWriteOp{{Key: key, Value: []byte("v2")}}, nil)
			require.NoError(t, err)
			indexKey := b.coder.EncodeRevisionKey(key)
			healthyIndex, err := b.kv.Get(ctx, indexKey)
			require.NoError(t, err)

			corrupt := b.kv.BeginBatchWrite()
			if raw := test.corrupt(b, key, createRevision, updateRevision); raw == nil {
				corrupt.Del(indexKey)
			} else {
				corrupt.Put(indexKey, raw, 0)
			}
			require.NoError(t, corrupt.Commit(ctx))

			require.NoError(t, b.InitializeLeadershipRevision(ctx, 0),
				"durable index corruption is converted into a persistent alarm")
			members, alarmErr := b.CorruptAlarms(ctx)
			require.NoError(t, alarmErr)
			require.Equal(t, []uint64{b.localAlarmMemberID()}, members)
			removed, disarmErr := b.DisarmCorrupt(ctx, b.localAlarmMemberID())
			require.ErrorIs(t, disarmErr, ErrTxnWitnessCorrupt)
			require.False(t, removed)

			repair := b.kv.BeginBatchWrite()
			repair.Put(indexKey, healthyIndex, 0)
			require.NoError(t, repair.Commit(ctx))
			removed, disarmErr = b.DisarmCorrupt(ctx, b.localAlarmMemberID())
			require.NoError(t, disarmErr)
			require.True(t, removed)
			require.NoError(t, b.InitializeLeadershipRevision(ctx, 0))
			members, alarmErr = b.CorruptAlarms(ctx)
			require.NoError(t, alarmErr)
			require.Empty(t, members)
		})
	}
}

func TestLeadershipRevisionIndexRepairBeforeAlarmDoesNotArmCorrupt(t *testing.T) {
	ctrl := gomock.NewController(t)
	t.Cleanup(ctrl.Finish)
	raw := imemkv.NewKvStorage()
	t.Cleanup(func() { require.NoError(t, raw.Close()) })
	store := &blockingRevisionIndexRecheckStorage{
		KvStorage: raw, blocked: make(chan struct{}), release: make(chan struct{}),
	}
	b := NewBackend(store, Config{
		Prefix:   prefix + "/restart-witness/index-repair-race",
		Identity: getStorageIdentity(), EnableEtcdCompatibility: true,
	}, mock.NewMinimalMetrics(ctrl)).(*backend)
	b.SetCurrentRevision(uint64(time.Now().UnixNano()))
	ctx := context.Background()
	key := []byte(prefix + "/restart-witness/index-repair-race/key")
	_, createRevision, err := b.TxnApply(ctx, []TxnWriteOp{{Key: key, Value: []byte("v1")}}, nil)
	require.NoError(t, err)
	_, _, err = b.TxnApply(ctx, []TxnWriteOp{{Key: key, Value: []byte("v2")}}, nil)
	require.NoError(t, err)
	store.indexKey = b.coder.EncodeRevisionKey(key)
	healthyIndex, err := raw.Get(ctx, store.indexKey)
	require.NoError(t, err)
	corrupt := raw.BeginBatchWrite()
	corrupt.Put(store.indexKey, uint64ToBytes(createRevision), 0)
	require.NoError(t, corrupt.Commit(ctx))
	store.enabled.Store(true)

	done := make(chan error, 1)
	go func() { done <- b.InitializeLeadershipRevision(ctx, 0) }()
	select {
	case <-store.blocked:
	case <-time.After(time.Second):
		t.Fatal("leadership validation did not reach the final revision-index evidence check")
	}
	repair := raw.BeginBatchWrite()
	repair.Put(store.indexKey, healthyIndex, 0)
	require.NoError(t, repair.Commit(ctx))
	close(store.release)
	select {
	case initErr := <-done:
		require.NoError(t, initErr)
	case <-time.After(time.Second):
		t.Fatal("leadership validation did not finish after revision-index repair")
	}
	members, alarmErr := b.CorruptAlarms(ctx)
	require.NoError(t, alarmErr)
	require.Empty(t, members, "a repaired index must not leave a stale CORRUPT alarm")
}

func TestLeadershipRevisionIndexValidationIgnoresCompactedCleanupWindow(t *testing.T) {
	b, ctx := newTxnApplyBackend(t)
	key := []byte(prefix + "/restart-witness/index-compacted-window")
	_, revision, err := b.TxnApply(ctx, []TxnWriteOp{{Key: key, Value: []byte("value")}}, nil)
	require.NoError(t, err)
	_, compactRevision, err := b.TxnApply(ctx, []TxnWriteOp{{
		Key: []byte(prefix + "/restart-witness/index-compacted-window/advance"), Value: []byte("value"),
	}}, nil)
	require.NoError(t, err)
	require.Greater(t, compactRevision, revision)

	// Model the physical-GC phase after the durable watermark advances but
	// before cleanupEventLog deletes the older seal and event marker.
	compact := b.kv.BeginBatchWrite()
	compact.Put(getCompactKey(b.config.Prefix), uint64ToBytes(compactRevision), 0)
	compact.Del(b.coder.EncodeRevisionKey(key))
	compact.Del(b.coder.EncodeObjectKey(key, revision))
	require.NoError(t, compact.Commit(ctx))

	require.NoError(t, b.InitializeLeadershipRevision(ctx, 0))
	members, alarmErr := b.CorruptAlarms(ctx)
	require.NoError(t, alarmErr)
	require.Empty(t, members, "planned physical GC below the compact watermark is not corruption")
}

func TestLeadershipRevisionIndexValidationUsesBoundedBatchGets(t *testing.T) {
	ctrl := gomock.NewController(t)
	t.Cleanup(ctrl.Finish)
	raw := imemkv.NewKvStorage()
	t.Cleanup(func() { require.NoError(t, raw.Close()) })
	store := &countingWitnessIndexBatchStorage{KvStorage: raw}
	b := NewBackend(store, Config{
		Prefix:   prefix + "/restart-witness/index-batches",
		Identity: getStorageIdentity(), EnableEtcdCompatibility: true,
	}, mock.NewMinimalMetrics(ctrl)).(*backend)
	b.SetCurrentRevision(uint64(time.Now().UnixNano()))
	ctx := context.Background()
	const transactions = txnRevisionIndexValidationBatch + 88
	for i := 0; i < transactions; i++ {
		_, _, err := b.TxnApply(ctx, []TxnWriteOp{{
			Key: []byte(fmt.Sprintf("%s/restart-witness/index-batches/%04d", prefix, i)), Value: []byte("value"),
		}}, nil)
		require.NoError(t, err)
	}
	store.calls.Store(0)

	require.NoError(t, b.InitializeLeadershipRevision(ctx, 0))
	require.Equal(t, int32(2), store.calls.Load(),
		"leadership index validation must batch across transactions, not issue an N+1 read per witness")
}

func TestLeadershipRevisionIndexValidationBoundsSingleLargeTransactionBatch(t *testing.T) {
	ctrl := gomock.NewController(t)
	t.Cleanup(ctrl.Finish)
	raw := imemkv.NewKvStorage()
	t.Cleanup(func() { require.NoError(t, raw.Close()) })
	store := &countingWitnessIndexBatchStorage{KvStorage: raw}
	b := NewBackend(store, Config{
		Prefix:   prefix + "/restart-witness/index-large-txn",
		Identity: getStorageIdentity(), EnableEtcdCompatibility: true,
	}, mock.NewMinimalMetrics(ctrl)).(*backend)
	b.SetCurrentRevision(uint64(time.Now().UnixNano()))
	ctx := context.Background()
	const operations = txnRevisionIndexValidationBatch + 88
	ops := make([]TxnWriteOp, operations)
	for i := range ops {
		ops[i] = TxnWriteOp{
			Key: []byte(fmt.Sprintf("%s/restart-witness/index-large-txn/%04d", prefix, i)), Value: []byte("value"),
		}
	}
	_, _, err := b.TxnApply(ctx, ops, nil)
	require.NoError(t, err)
	store.calls.Store(0)

	require.NoError(t, b.InitializeLeadershipRevision(ctx, 0))
	require.Equal(t, int32(2), store.calls.Load(),
		"one large transaction must be split into bounded revision-index batches")
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

func TestCorruptDisarmWitnessScanErrorKeepsAlarm(t *testing.T) {
	ctrl := gomock.NewController(t)
	t.Cleanup(ctrl.Finish)
	store := imemkv.NewKvStorage()
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	config := Config{Prefix: prefix, Identity: getStorageIdentity(), EnableEtcdCompatibility: true}
	ctx := context.Background()
	b := NewBackend(witnessScanErrorStorage{KvStorage: store}, config, mock.NewMinimalMetrics(ctrl)).(*backend)
	memberID := b.localAlarmMemberID()
	require.NoError(t, b.ArmCorrupt(ctx, memberID))

	removed, err := b.DisarmCorrupt(ctx, memberID)
	require.ErrorIs(t, err, errWitnessScanUnavailable)
	require.False(t, removed)
	members, alarmErr := b.CorruptAlarms(ctx)
	require.NoError(t, alarmErr)
	require.Equal(t, []uint64{memberID}, members, "an unverifiable witness set must keep the write fence armed")
}

func TestCorruptDisarmLinearizesConcurrentRearm(t *testing.T) {
	ctrl := gomock.NewController(t)
	t.Cleanup(ctrl.Finish)
	store := imemkv.NewKvStorage()
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	wrapped := &blockingWitnessScanStorage{
		KvStorage: store, entered: make(chan struct{}), release: make(chan struct{}),
	}
	config := Config{Prefix: prefix, Identity: getStorageIdentity(), EnableEtcdCompatibility: true}
	ctx := context.Background()
	b := NewBackend(wrapped, config, mock.NewMinimalMetrics(ctrl)).(*backend)
	b.SetCurrentRevision(uint64(time.Now().UnixNano()))
	_, _, err := b.TxnApply(ctx, []TxnWriteOp{{
		Key: []byte(prefix + "/restart-witness/concurrent-rearm"), Value: []byte("value"),
	}}, nil)
	require.NoError(t, err)
	memberID := b.localAlarmMemberID()
	require.NoError(t, b.ArmCorrupt(ctx, memberID))

	wrapped.enabled.Store(true)
	type disarmResult struct {
		removed bool
		err     error
	}
	disarmed := make(chan disarmResult, 1)
	go func() {
		removed, disarmErr := b.DisarmCorrupt(ctx, memberID)
		disarmed <- disarmResult{removed: removed, err: disarmErr}
	}()
	<-wrapped.entered
	rearmed := make(chan error, 1)
	go func() { rearmed <- b.ArmCorrupt(ctx, memberID) }()
	select {
	case err := <-rearmed:
		require.Failf(t, "concurrent rearm returned before disarm linearized", "err=%v", err)
	case <-time.After(100 * time.Millisecond):
	}
	close(wrapped.release)
	result := <-disarmed
	require.NoError(t, result.err)
	require.True(t, result.removed)
	require.NoError(t, <-rearmed)
	members, err := b.CorruptAlarms(ctx)
	require.NoError(t, err)
	require.Equal(t, []uint64{memberID}, members,
		"a concurrent resolver rearm ordered after disarm must remain persistent")
}

func TestCorruptDisarmRejectsCrossReplicaRearm(t *testing.T) {
	ctrl := gomock.NewController(t)
	t.Cleanup(ctrl.Finish)
	store := imemkv.NewKvStorage()
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	wrapped := &blockingWitnessScanStorage{
		KvStorage: store, entered: make(chan struct{}), release: make(chan struct{}),
	}
	config := Config{Prefix: prefix, Identity: getStorageIdentity(), EnableEtcdCompatibility: true}
	ctx := context.Background()
	disarmBackend := NewBackend(wrapped, config, mock.NewMinimalMetrics(ctrl)).(*backend)
	rearmBackend := NewBackend(wrapped, config, mock.NewMinimalMetrics(ctrl)).(*backend)
	disarmBackend.SetCurrentRevision(uint64(time.Now().UnixNano()))
	_, _, err := disarmBackend.TxnApply(ctx, []TxnWriteOp{{
		Key: []byte(prefix + "/restart-witness/cross-replica-rearm"), Value: []byte("value"),
	}}, nil)
	require.NoError(t, err)
	memberID := disarmBackend.localAlarmMemberID()
	require.NoError(t, disarmBackend.ArmCorrupt(ctx, memberID))

	wrapped.enabled.Store(true)
	type disarmResult struct {
		removed bool
		err     error
	}
	disarmed := make(chan disarmResult, 1)
	go func() {
		removed, disarmErr := disarmBackend.DisarmCorrupt(ctx, memberID)
		disarmed <- disarmResult{removed: removed, err: disarmErr}
	}()
	<-wrapped.entered
	// The second backend models a resolver in another process: it shares TiKV
	// metadata but not the leader-local alarm mutex.
	rearmed := make(chan error, 1)
	go func() { rearmed <- rearmBackend.ArmCorrupt(ctx, memberID) }()
	require.NoError(t, <-rearmed)
	close(wrapped.release)
	result := <-disarmed
	require.ErrorIs(t, result.err, ErrCorruptAlarmChanged)
	require.False(t, result.removed)
	members, err := disarmBackend.CorruptAlarms(ctx)
	require.NoError(t, err)
	require.Equal(t, []uint64{memberID}, members,
		"a cross-replica rearm during validation requires a fresh operator decision")
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

func TestLeadershipRejectsFutureWitnessVersionWithoutCorruptAlarm(t *testing.T) {
	b, ctx := newTxnApplyBackend(t)
	key := []byte(prefix + "/restart-witness/future-version")
	_, revision, err := b.TxnApply(ctx, []TxnWriteOp{{Key: key, Value: []byte("value")}}, nil)
	require.NoError(t, err)
	witnessKey := b.ks.EncodeInternalKey(txnWitnessLogicalKey(revision))
	raw, err := b.kv.Get(ctx, witnessKey)
	require.NoError(t, err)
	future := append([]byte(nil), raw...)
	future[0] = txnWitnessVersion + 1
	batch := b.kv.BeginBatchWrite()
	batch.Put(witnessKey, future, 0)
	require.NoError(t, batch.Commit(ctx))

	err = b.InitializeLeadershipRevision(ctx, 0)
	require.ErrorIs(t, err, ErrTxnWitnessUnsupportedVersion)
	require.ErrorContains(t, err, fmt.Sprintf("revision %d", revision))
	members, alarmErr := b.CorruptAlarms(ctx)
	require.NoError(t, alarmErr)
	require.Empty(t, members, "future metadata requires roll-forward, not a corruption repair")
	stored, err := b.kv.Get(ctx, witnessKey)
	require.NoError(t, err)
	require.Equal(t, future, stored, "an older binary must preserve opaque future witness bytes")
	require.NoError(t, b.ArmCorrupt(ctx, b.localAlarmMemberID()))
	removed, err := b.DisarmCorrupt(ctx, b.localAlarmMemberID())
	require.ErrorIs(t, err, ErrTxnWitnessUnsupportedVersion)
	require.False(t, removed, "an incompatible binary cannot validate evidence well enough to disarm")

	repair := b.kv.BeginBatchWrite()
	repair.Put(witnessKey, raw, 0)
	require.NoError(t, repair.Commit(ctx))
	removed, err = b.DisarmCorrupt(ctx, b.localAlarmMemberID())
	require.NoError(t, err)
	require.True(t, removed)
	require.NoError(t, b.InitializeLeadershipRevision(ctx, 0), "rolling forward to a compatible decoder can lead")
}

func TestLeaseIncarnationFormatFenceForcesOldBinaryRollForward(t *testing.T) {
	b, ctx := newTxnApplyBackend(t)
	require.NoError(t, b.EnsureLeaseIncarnationFormatFence(ctx))
	require.NoError(t, b.EnsureLeaseIncarnationFormatFence(ctx), "format fence must be idempotent")

	raw, err := b.InternalGet(ctx, leaseIncarnationFormatFenceKey())
	require.NoError(t, err)
	require.Equal(t, leaseIncarnationFormatFenceValue, raw)
	require.ErrorIs(t, func() error {
		_, decodeErr := decodeTxnWitness(raw)
		return decodeErr
	}(), ErrTxnWitnessUnsupportedVersion,
		"the previous decoder must withdraw before publishing leadership")
	require.NoError(t, b.InitializeLeadershipRevision(ctx, 0),
		"the incarnation-aware decoder must recognize its format fence")
	members, err := b.CorruptAlarms(ctx)
	require.NoError(t, err)
	require.Empty(t, members, "a supported roll-forward marker is not corruption")
}

func TestMalformedLeaseIncarnationFormatFenceArmsCorrupt(t *testing.T) {
	b, ctx := newTxnApplyBackend(t)
	require.NoError(t, b.EnsureLeaseIncarnationFormatFence(ctx))
	batch := b.kv.BeginBatchWrite()
	batch.Put(b.ks.EncodeInternalKey(leaseIncarnationFormatFenceKey()), []byte{leaseIncarnationFenceVersion}, 0)
	require.NoError(t, batch.Commit(ctx))

	require.NoError(t, b.InitializeLeadershipRevision(ctx, 0))
	members, err := b.CorruptAlarms(ctx)
	require.NoError(t, err)
	require.Equal(t, []uint64{b.localAlarmMemberID()}, members)
}

func TestLeadershipMalformedCurrentWitnessStillArmsCorrupt(t *testing.T) {
	b, ctx := newTxnApplyBackend(t)
	key := []byte(prefix + "/restart-witness/malformed-current")
	_, revision, err := b.TxnApply(ctx, []TxnWriteOp{{Key: key, Value: []byte("value")}}, nil)
	require.NoError(t, err)
	witnessKey := b.ks.EncodeInternalKey(txnWitnessLogicalKey(revision))
	batch := b.kv.BeginBatchWrite()
	batch.Put(witnessKey, []byte{txnWitnessVersion}, 0)
	require.NoError(t, batch.Commit(ctx))

	require.NoError(t, b.InitializeLeadershipRevision(ctx, 0))
	members, err := b.CorruptAlarms(ctx)
	require.NoError(t, err)
	require.Equal(t, []uint64{b.localAlarmMemberID()}, members)
}
