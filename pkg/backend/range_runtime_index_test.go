// Copyright 2026 ByteDance and/or its affiliates
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package backend

import (
	"bytes"
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"

	proto "github.com/kubewharf/kubebrain-client/api/v2rpc"

	"github.com/kubewharf/kubebrain/pkg/metrics/mock"
	"github.com/kubewharf/kubebrain/pkg/storage"
	imemkv "github.com/kubewharf/kubebrain/pkg/storage/memkv"
)

func newRuntimeIndexBackend(t *testing.T) (*backend, storage.KvStorage, context.Context, []byte) {
	t.Helper()
	ctrl := gomock.NewController(t)
	t.Cleanup(ctrl.Finish)
	store := imemkv.NewKvStorage()
	pfx := fmt.Sprintf("/kubebrain/runtime-index/%d", time.Now().UnixNano())
	b := NewBackend(store, Config{
		Prefix: pfx, Identity: getStorageIdentity(),
		EnableEtcdCompatibility: true, EnableCountIndex: true,
	}, mock.NewMinimalMetrics(ctrl)).(*backend)
	t.Cleanup(func() { require.NoError(t, b.Close()) })
	b.SetCurrentRevision(uint64(time.Now().UnixNano()))
	ctx := context.Background()
	require.NoError(t, b.EnsureEventLogStart(ctx))
	return b, store, ctx, []byte(pfx + "/key")
}

func applyRuntimeIndexVersions(t *testing.T, b *backend, ctx context.Context, key []byte) (uint64, uint64) {
	t.Helper()
	_, createRevision, err := b.TxnApply(ctx, []TxnWriteOp{{Key: key, Value: []byte("v1")}}, nil)
	require.NoError(t, err)
	_, updateRevision, err := b.TxnApply(ctx, []TxnWriteOp{{Key: key, Value: []byte("v2")}}, nil)
	require.NoError(t, err)
	require.Eventually(t, func() bool { return b.GetCurrentRevision() >= updateRevision }, 5*time.Second, 2*time.Millisecond)
	require.NoError(t, b.RebuildCountIndex(ctx))
	return createRevision, updateRevision
}

func TestLatestGetArmsCorruptForWitnessedStaleRevisionIndex(t *testing.T) {
	b, store, ctx, key := newRuntimeIndexBackend(t)
	createRevision, _ := applyRuntimeIndexVersions(t, b, ctx, key)
	indexKey := b.coder.EncodeRevisionKey(key)
	healthyIndex, err := store.Get(ctx, indexKey)
	require.NoError(t, err)

	corrupt := store.BeginBatchWrite()
	corrupt.Put(indexKey, uint64ToBytes(createRevision), 0)
	require.NoError(t, corrupt.Commit(ctx))

	resp, err := b.Get(ctx, &proto.GetRequest{Key: key})
	require.Nil(t, resp)
	require.ErrorIs(t, err, ErrInvalidMVCCMetadata)
	alarms, alarmErr := b.CorruptAlarms(ctx)
	require.NoError(t, alarmErr)
	require.Equal(t, []uint64{b.localAlarmMemberID()}, alarms)
	removed, disarmErr := b.DisarmCorrupt(ctx, b.localAlarmMemberID())
	require.False(t, removed)
	require.ErrorIs(t, disarmErr, ErrTxnWitnessCorrupt)

	repair := store.BeginBatchWrite()
	repair.Put(indexKey, healthyIndex, 0)
	require.NoError(t, repair.Commit(ctx))
	removed, disarmErr = b.DisarmCorrupt(ctx, b.localAlarmMemberID())
	require.NoError(t, disarmErr)
	require.True(t, removed)
	resp, err = b.Get(ctx, &proto.GetRequest{Key: key})
	require.NoError(t, err)
	require.True(t, bytes.HasSuffix(resp.GetKv().GetValue(), []byte("v2")))
}

func TestLatestGetArmsCorruptForWitnessedMissingOrMismatchedIndexTarget(t *testing.T) {
	for _, target := range []string{
		"index-missing", "index-malformed", "object-missing", "index-tombstone", "object-tombstone",
	} {
		t.Run(target, func(t *testing.T) {
			b, store, ctx, key := newRuntimeIndexBackend(t)
			_, updateRevision := applyRuntimeIndexVersions(t, b, ctx, key)
			corruptKey := b.coder.EncodeRevisionKey(key)
			if target == "object-missing" || target == "object-tombstone" {
				corruptKey = b.coder.EncodeObjectKey(key, updateRevision)
			}
			healthy, err := store.Get(ctx, corruptKey)
			require.NoError(t, err)
			corrupt := store.BeginBatchWrite()
			switch target {
			case "index-missing", "object-missing":
				corrupt.Del(corruptKey)
			case "index-malformed":
				corrupt.Put(corruptKey, []byte{1}, 0)
			case "index-tombstone":
				corrupt.Put(corruptKey, append(uint64ToBytes(updateRevision), 0), 0)
			case "object-tombstone":
				corrupt.Put(corruptKey, tombStoneBytes, 0)
			}
			require.NoError(t, corrupt.Commit(ctx))

			resp, getErr := b.Get(ctx, &proto.GetRequest{Key: key})
			require.Nil(t, resp)
			require.ErrorIs(t, getErr, ErrInvalidMVCCMetadata)
			alarms, alarmErr := b.CorruptAlarms(ctx)
			require.NoError(t, alarmErr)
			require.Equal(t, []uint64{b.localAlarmMemberID()}, alarms)

			repair := store.BeginBatchWrite()
			repair.Put(corruptKey, healthy, 0)
			require.NoError(t, repair.Commit(ctx))
			removed, disarmErr := b.DisarmCorrupt(ctx, b.localAlarmMemberID())
			require.NoError(t, disarmErr)
			require.True(t, removed)
		})
	}
}

func TestLatestGetUnwitnessedIndexContradictionDoesNotArm(t *testing.T) {
	b, store, ctx, key := newRuntimeIndexBackend(t)
	createRevision, updateRevision := applyRuntimeIndexVersions(t, b, ctx, key)
	corrupt := store.BeginBatchWrite()
	corrupt.Del(b.ks.EncodeInternalKey(txnWitnessLogicalKey(updateRevision)))
	corrupt.Put(b.coder.EncodeRevisionKey(key), uint64ToBytes(createRevision), 0)
	require.NoError(t, corrupt.Commit(ctx))

	resp, err := b.Get(ctx, &proto.GetRequest{Key: key})
	require.Nil(t, resp)
	require.ErrorIs(t, err, ErrInvalidMVCCMetadata)
	alarms, alarmErr := b.CorruptAlarms(ctx)
	require.NoError(t, alarmErr)
	require.Empty(t, alarms)
}

func TestLatestRevisionIndexNewerThanReadySnapshotIsNotRejected(t *testing.T) {
	b, _, ctx, key := newRuntimeIndexBackend(t)
	_, readyRevision := applyRuntimeIndexVersions(t, b, ctx, key)
	expectation := b.sampleLatestIndexExpectation(ctx, key, true)
	verified, err := validateLatestRevisionIndex(key, readyRevision+1, false, expectation)
	require.NoError(t, err)
	require.False(t, verified, "a newer leader's physical write must bypass a stale local expectation")
}

func TestPinnedSnapshotDoesNotUseRuntimeLatestIndexExpectation(t *testing.T) {
	b, _, ctx, key := newRuntimeIndexBackend(t)
	applyRuntimeIndexVersions(t, b, ctx, key)
	pinned := storage.WithSnapshotTimestamp(ctx, 1)
	expectation := b.sampleLatestIndexExpectation(pinned, key, true)
	require.False(t, expectation.ready)
}

func TestHistoricalGetDoesNotUseRuntimeLatestIndexExpectation(t *testing.T) {
	b, store, ctx, key := newRuntimeIndexBackend(t)
	createRevision, _ := applyRuntimeIndexVersions(t, b, ctx, key)
	corrupt := store.BeginBatchWrite()
	corrupt.Put(b.coder.EncodeRevisionKey(key), uint64ToBytes(createRevision), 0)
	require.NoError(t, corrupt.Commit(ctx))

	resp, err := b.Get(ctx, &proto.GetRequest{Key: key, Revision: createRevision})
	require.NoError(t, err)
	require.True(t, bytes.HasSuffix(resp.GetKv().GetValue(), []byte("v1")))
	alarms, alarmErr := b.CorruptAlarms(ctx)
	require.NoError(t, alarmErr)
	require.Empty(t, alarms)
}
