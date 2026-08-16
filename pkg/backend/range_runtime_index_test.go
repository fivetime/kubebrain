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

	"github.com/kubewharf/kubebrain/pkg/backend/streamerror"
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
	return b, store, ctx, []byte("runtime-index-key")
}

func runtimeIndexRangeEnd(key []byte) []byte {
	end := append([]byte(nil), key...)
	end[len(end)-1]++
	return end
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
			if target == "object-missing" || target == "object-tombstone" {
				// Keep the complete count-index witness at the same durable
				// watermark as the injected object corruption. A concurrently
				// advanced durable watermark deliberately makes an older index
				// advisory, as it would be on a lagging follower.
				require.NoError(t, b.RebuildCountIndex(ctx))
			}

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

func TestLatestListArmsCorruptForWitnessedRevisionIndexOrObjectSplit(t *testing.T) {
	for _, target := range []string{
		"index-stale", "index-missing", "index-malformed", "object-missing", "index-tombstone", "object-tombstone",
	} {
		t.Run(target, func(t *testing.T) {
			b, store, ctx, key := newRuntimeIndexBackend(t)
			createRevision, updateRevision := applyRuntimeIndexVersions(t, b, ctx, key)
			corruptKey := b.coder.EncodeRevisionKey(key)
			if target == "object-missing" || target == "object-tombstone" {
				corruptKey = b.coder.EncodeObjectKey(key, updateRevision)
			}
			corrupt := store.BeginBatchWrite()
			switch target {
			case "index-stale":
				corrupt.Put(corruptKey, uint64ToBytes(createRevision), 0)
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

			resp, listErr := b.List(ctx, &proto.RangeRequest{Key: key, End: runtimeIndexRangeEnd(key)})
			require.Nil(t, resp)
			require.ErrorIs(t, listErr, ErrInvalidMVCCMetadata)
			alarms, alarmErr := b.CorruptAlarms(ctx)
			require.NoError(t, alarmErr)
			require.Equal(t, []uint64{b.localAlarmMemberID()}, alarms)
		})
	}
}

func TestLatestListUnwitnessedIndexContradictionDoesNotArm(t *testing.T) {
	b, store, ctx, key := newRuntimeIndexBackend(t)
	createRevision, updateRevision := applyRuntimeIndexVersions(t, b, ctx, key)
	corrupt := store.BeginBatchWrite()
	corrupt.Del(b.ks.EncodeInternalKey(txnWitnessLogicalKey(updateRevision)))
	corrupt.Put(b.coder.EncodeRevisionKey(key), uint64ToBytes(createRevision), 0)
	require.NoError(t, corrupt.Commit(ctx))

	resp, err := b.List(ctx, &proto.RangeRequest{Key: key, End: runtimeIndexRangeEnd(key)})
	require.Nil(t, resp)
	require.ErrorIs(t, err, ErrInvalidMVCCMetadata)
	alarms, alarmErr := b.CorruptAlarms(ctx)
	require.NoError(t, alarmErr)
	require.Empty(t, alarms)
}

func TestLatestListRevisionIndexNewerThanReadySnapshotIsNotRejected(t *testing.T) {
	b, store, ctx, key := newRuntimeIndexBackend(t)
	_, updateRevision := applyRuntimeIndexVersions(t, b, ctx, key)
	newer := store.BeginBatchWrite()
	newer.Put(b.coder.EncodeRevisionKey(key), uint64ToBytes(updateRevision+1), 0)
	require.NoError(t, newer.Commit(ctx))

	resp, err := b.List(ctx, &proto.RangeRequest{Key: key, End: runtimeIndexRangeEnd(key)})
	require.NoError(t, err)
	require.Len(t, resp.GetKvs(), 1)
	alarms, alarmErr := b.CorruptAlarms(ctx)
	require.NoError(t, alarmErr)
	require.Empty(t, alarms)
}

func TestLatestDecodedListArmsCorruptWhenMalformedIndexFailsBeforeObjectScan(t *testing.T) {
	b, store, ctx, _ := newRuntimeIndexBackend(t)
	key := []byte("/runtime-index-key")
	applyRuntimeIndexVersions(t, b, ctx, key)
	corrupt := store.BeginBatchWrite()
	corrupt.Put(b.coder.EncodeRevisionKey(key), []byte{1}, 0)
	require.NoError(t, corrupt.Commit(ctx))

	resp, err := b.List(ctx, &proto.RangeRequest{
		Key: key, End: append(append([]byte(nil), key...), 0),
	})
	require.Nil(t, resp)
	require.ErrorIs(t, err, ErrInvalidMVCCMetadata)
	alarms, alarmErr := b.CorruptAlarms(ctx)
	require.NoError(t, alarmErr)
	require.Equal(t, []uint64{b.localAlarmMemberID()}, alarms)
}

func TestHistoricalListDoesNotUseRuntimeLatestRangeExpectation(t *testing.T) {
	b, store, ctx, key := newRuntimeIndexBackend(t)
	createRevision, _ := applyRuntimeIndexVersions(t, b, ctx, key)
	corrupt := store.BeginBatchWrite()
	corrupt.Put(b.coder.EncodeRevisionKey(key), uint64ToBytes(createRevision), 0)
	require.NoError(t, corrupt.Commit(ctx))

	resp, err := b.List(ctx, &proto.RangeRequest{
		Key: key, End: runtimeIndexRangeEnd(key), Revision: createRevision,
	})
	require.NoError(t, err)
	require.Len(t, resp.GetKvs(), 1)
	require.Equal(t, createRevision, resp.GetKvs()[0].GetRevision())
	alarms, alarmErr := b.CorruptAlarms(ctx)
	require.NoError(t, alarmErr)
	require.Empty(t, alarms)
}

func TestPinnedSnapshotDoesNotUseRuntimeLatestRangeExpectation(t *testing.T) {
	b, _, ctx, key := newRuntimeIndexBackend(t)
	applyRuntimeIndexVersions(t, b, ctx, key)
	pinned := storage.WithSnapshotTimestamp(ctx, 1)
	expectation := b.sampleLatestRangeExpectation(pinned, key, runtimeIndexRangeEnd(key), true, 0)
	require.False(t, expectation.ready)
}

func TestLatestListLimitValidatesOnlyAuthoritativeExpectationPage(t *testing.T) {
	b, _, ctx, _ := newRuntimeIndexBackend(t)
	for _, key := range [][]byte{[]byte("range-a"), []byte("range-b"), []byte("range-c")} {
		_, _, err := b.TxnApply(ctx, []TxnWriteOp{{Key: key, Value: append([]byte("value-"), key...)}}, nil)
		require.NoError(t, err)
	}
	require.Eventually(t, func() bool { return b.GetCurrentRevision() != 0 }, 5*time.Second, 2*time.Millisecond)
	require.NoError(t, b.RebuildCountIndex(ctx))

	resp, err := b.List(ctx, &proto.RangeRequest{Key: []byte("range-a"), End: []byte("range-z"), Limit: 1})
	require.NoError(t, err)
	require.Len(t, resp.GetKvs(), 1)
	require.Equal(t, []byte("range-a"), resp.GetKvs()[0].GetKey())
	require.True(t, resp.GetMore())
}

func collectRuntimeIndexStream(t *testing.T, stream <-chan *proto.StreamRangeResponse) ([]*proto.KeyValue, error) {
	t.Helper()
	var kvs []*proto.KeyValue
	var terminal error
	for chunk := range stream {
		kvs = append(kvs, chunk.GetRangeResponse().GetKvs()...)
		if chunk.GetErr() != "" {
			decoded, ok := streamerror.Decode(chunk.GetErr())
			require.True(t, ok)
			terminal = decoded
		}
	}
	return kvs, terminal
}

func TestLatestRangeStreamArmsCorruptBeforeExposingSplitChunk(t *testing.T) {
	for _, target := range []string{
		"index-stale", "index-missing", "index-malformed", "index-tombstone", "object-missing", "object-tombstone",
	} {
		t.Run(target, func(t *testing.T) {
			b, store, ctx, key := newRuntimeIndexBackend(t)
			createRevision, updateRevision := applyRuntimeIndexVersions(t, b, ctx, key)
			corrupt := store.BeginBatchWrite()
			switch target {
			case "index-stale":
				corrupt.Put(b.coder.EncodeRevisionKey(key), uint64ToBytes(createRevision), 0)
			case "index-missing":
				corrupt.Del(b.coder.EncodeRevisionKey(key))
			case "index-malformed":
				corrupt.Put(b.coder.EncodeRevisionKey(key), []byte{1}, 0)
			case "index-tombstone":
				corrupt.Put(b.coder.EncodeRevisionKey(key), append(uint64ToBytes(updateRevision), 0), 0)
			case "object-missing":
				corrupt.Del(b.coder.EncodeObjectKey(key, updateRevision))
			case "object-tombstone":
				corrupt.Put(b.coder.EncodeObjectKey(key, updateRevision), tombStoneBytes, 0)
			}
			require.NoError(t, corrupt.Commit(ctx))

			stream, err := b.RangeStream(ctx, key, runtimeIndexRangeEnd(key), 0)
			require.NoError(t, err)
			kvs, terminal := collectRuntimeIndexStream(t, stream)
			require.Empty(t, kvs, "the contradictory first chunk must not become visible")
			require.ErrorIs(t, terminal, ErrInvalidMVCCMetadata)
			alarms, alarmErr := b.CorruptAlarms(ctx)
			require.NoError(t, alarmErr)
			require.Equal(t, []uint64{b.localAlarmMemberID()}, alarms)
		})
	}
}

func TestLatestDecodedRangeStreamRejectsStaleIndexSelectedObject(t *testing.T) {
	b, store, ctx, _ := newRuntimeIndexBackend(t)
	key := []byte("/runtime-index-stream-key")
	createRevision, _ := applyRuntimeIndexVersions(t, b, ctx, key)
	corrupt := store.BeginBatchWrite()
	corrupt.Put(b.coder.EncodeRevisionKey(key), uint64ToBytes(createRevision), 0)
	require.NoError(t, corrupt.Commit(ctx))

	stream, err := b.RangeStream(ctx, key, append(append([]byte(nil), key...), 0), 0)
	require.NoError(t, err)
	kvs, terminal := collectRuntimeIndexStream(t, stream)
	require.Empty(t, kvs)
	require.ErrorIs(t, terminal, ErrInvalidMVCCMetadata)
	alarms, alarmErr := b.CorruptAlarms(ctx)
	require.NoError(t, alarmErr)
	require.Equal(t, []uint64{b.localAlarmMemberID()}, alarms)
}

func TestHistoricalRangeStreamDoesNotUseLatestIndexExpectation(t *testing.T) {
	b, store, ctx, key := newRuntimeIndexBackend(t)
	createRevision, _ := applyRuntimeIndexVersions(t, b, ctx, key)
	corrupt := store.BeginBatchWrite()
	corrupt.Put(b.coder.EncodeRevisionKey(key), uint64ToBytes(createRevision), 0)
	require.NoError(t, corrupt.Commit(ctx))

	stream, err := b.RangeStream(ctx, key, runtimeIndexRangeEnd(key), createRevision)
	require.NoError(t, err)
	kvs, terminal := collectRuntimeIndexStream(t, stream)
	require.NoError(t, terminal)
	require.Len(t, kvs, 1)
	require.Equal(t, createRevision, kvs[0].GetRevision())
	alarms, alarmErr := b.CorruptAlarms(ctx)
	require.NoError(t, alarmErr)
	require.Empty(t, alarms)
}

func TestPinnedLatestRangeStreamMarkerRetainsRuntimeIndexValidation(t *testing.T) {
	b, store, ctx, key := newRuntimeIndexBackend(t)
	createRevision, updateRevision := applyRuntimeIndexVersions(t, b, ctx, key)
	corrupt := store.BeginBatchWrite()
	corrupt.Put(b.coder.EncodeRevisionKey(key), uint64ToBytes(createRevision), 0)
	require.NoError(t, corrupt.Commit(ctx))

	stream, err := b.RangeStream(
		WithLatestRangeStream(ctx), key, runtimeIndexRangeEnd(key), updateRevision,
	)
	require.NoError(t, err)
	kvs, terminal := collectRuntimeIndexStream(t, stream)
	require.Empty(t, kvs)
	require.ErrorIs(t, terminal, ErrInvalidMVCCMetadata)
	alarms, alarmErr := b.CorruptAlarms(ctx)
	require.NoError(t, alarmErr)
	require.Equal(t, []uint64{b.localAlarmMemberID()}, alarms)
}

func TestLatestRangeStreamExpectationFencesResetBetweenPages(t *testing.T) {
	b, _, ctx, _ := newRuntimeIndexBackend(t)
	ops := make([]TxnWriteOp, 301)
	keys := make([][]byte, len(ops))
	for i := range ops {
		keys[i] = []byte(fmt.Sprintf("stream-page-%03d", i))
		ops[i] = TxnWriteOp{Key: keys[i], Value: []byte("value")}
	}
	_, revision, err := b.TxnApply(ctx, ops, nil)
	require.NoError(t, err)
	require.Eventually(t, func() bool { return b.GetCurrentRevision() >= revision }, 5*time.Second, 2*time.Millisecond)
	require.NoError(t, b.RebuildCountIndex(ctx))

	expectation := b.newLatestRangeStreamExpectation([]byte("stream-page-"), []byte("stream-page."), revision, true)
	require.NotNil(t, expectation)
	require.Len(t, expectation.pending, latestRangeStreamIndexPage)
	require.True(t, expectation.more)
	firstPage := make([]*proto.KeyValue, latestRangeStreamIndexPage)
	for i := range firstPage {
		firstPage[i] = &proto.KeyValue{Key: keys[i], Revision: revision}
	}
	require.NoError(t, expectation.validate(ctx, firstPage))

	b.countIndex.Reset(func() uint64 { return revision }, func(_ uint64, emit func([]byte, uint64, bool)) error {
		for _, key := range keys {
			emit(key, revision, false)
		}
		return nil
	})
	err = expectation.validate(ctx, []*proto.KeyValue{{Key: keys[300], Revision: revision}})
	require.ErrorContains(t, err, "ordering index changed during stream")
	alarms, alarmErr := b.CorruptAlarms(ctx)
	require.NoError(t, alarmErr)
	require.Empty(t, alarms, "a generation change is a retryable stream failure, not corruption evidence")
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

func TestLatestReadsIgnoreCountIndexContradictionBehindDurableRevision(t *testing.T) {
	for _, read := range []string{"get", "list"} {
		t.Run(read, func(t *testing.T) {
			b, store, ctx, key := newRuntimeIndexBackend(t)
			_, readyRevision := applyRuntimeIndexVersions(t, b, ctx, key)
			_, deleteRevision, err := b.TxnApply(ctx, []TxnWriteOp{{Key: key, Delete: true}}, nil)
			require.NoError(t, err)
			require.Greater(t, deleteRevision, readyRevision)

			// Model a follower whose collector snapshot still says the key was
			// live while the shared TiKV state has already committed and cleaned
			// the newer delete's latest-index row.
			b.countIndex.Invalidate()
			b.countIndex.Reset(func() uint64 { return readyRevision }, func(_ uint64, emit func([]byte, uint64, bool)) error {
				emit(key, readyRevision, false)
				return nil
			})
			cleanup := store.BeginBatchWrite()
			cleanup.Del(b.coder.EncodeRevisionKey(key))
			require.NoError(t, cleanup.Commit(ctx))

			switch read {
			case "get":
				resp, getErr := b.Get(ctx, &proto.GetRequest{Key: key})
				require.NoError(t, getErr)
				require.Nil(t, resp.GetKv())
			case "list":
				resp, listErr := b.List(ctx, &proto.RangeRequest{Key: key, End: runtimeIndexRangeEnd(key)})
				require.NoError(t, listErr)
				require.Empty(t, resp.GetKvs())
			}
			alarms, alarmErr := b.CorruptAlarms(ctx)
			require.NoError(t, alarmErr)
			require.Empty(t, alarms)
		})
	}
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
