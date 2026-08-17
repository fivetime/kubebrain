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
	"context"
	"fmt"
	"path"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"

	proto "github.com/kubewharf/kubebrain-client/api/v2rpc"
	"github.com/kubewharf/kubebrain/pkg/backend/coder"
	"github.com/kubewharf/kubebrain/pkg/backend/streamerror"
	"github.com/kubewharf/kubebrain/pkg/metrics"
	"github.com/kubewharf/kubebrain/pkg/metrics/mock"
	"github.com/kubewharf/kubebrain/pkg/storage"
	imemkv "github.com/kubewharf/kubebrain/pkg/storage/memkv"
)

// noBatchGet wraps a KvStorage as the plain KvStorage interface, so the concrete
// backend's BatchGet method is NOT promoted — a type assertion to
// storage.BatchGetter fails and the replay path takes its per-key-Get fallback.
type noBatchGet struct{ storage.KvStorage }

func TestEnsureEventLogStartResetsPublishedRevisionForNewLeader(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	m := mock.NewMinimalMetrics(ctrl)
	kv := imemkv.NewKvStorage()
	defer func() { require.NoError(t, kv.Close()) }()

	b := NewBackend(kv, Config{
		Prefix: "/kubebrain/elog_leader", Identity: getStorageIdentity(), EnableEtcdCompatibility: true,
	}, m).(*backend)
	const current = uint64(12345)
	b.SetCurrentRevision(current)
	require.Zero(t, b.GetPublishedRevision(), "a follower may not have published the prior leader's writes")

	oldSub, err := b.watcherHub.AddWatcher(context.Background(), []byte("/registry/"))
	require.NoError(t, err)
	require.NoError(t, b.EnsureEventLogStart(context.Background()))

	require.Equal(t, current, b.GetPublishedRevision())
	_, open := <-oldSub
	require.False(t, open, "old-term subscribers must close before the watermark advances")
}

// TestEventLogReplayFallbackMatchesBatchGet verifies the per-key-Get fallback
// (for backends WITHOUT storage.BatchGetter) replays events identical to the
// authoritative full-prefix scan: same CREATE/PUT/DELETE, with the DELETE
// reusing the PUT's version via the deduplicated object key. It wraps memkv in
// noBatchGet so the type assertion in loadEventValues fails and the fallback
// path runs, then asserts the replayed stream agrees with scanHistoryEvents
// event-for-event. (TestEventLogReplayMatchesWrites covers the batched path,
// since raw memkv implements BatchGetter.)
func TestEventLogReplayFallbackMatchesBatchGet(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	m := mock.NewMinimalMetrics(ctrl)

	rawKv := imemkv.NewKvStorage()
	defer func() { require.NoError(t, rawKv.Close()) }()
	_, isBG := interface{}(rawKv).(storage.BatchGetter)
	require.True(t, isBG, "memkv must implement BatchGetter")
	kv := noBatchGet{rawKv}
	_, wrapIsBG := interface{}(kv).(storage.BatchGetter)
	require.False(t, wrapIsBG, "noBatchGet must hide BatchGet to force the fallback path")

	pfx := fmt.Sprintf("/kubebrain/elog_fb/%d", time.Now().UnixNano())
	b := NewBackend(kv, Config{
		Prefix: pfx, Identity: getStorageIdentity(), EnableEtcdCompatibility: true,
	}, m).(*backend)
	b.SetCurrentRevision(uint64(time.Now().UnixNano()))
	ctx := context.Background()
	require.NoError(t, b.EnsureEventLogStart(ctx))
	startRev := b.GetCurrentRevision()

	key := path.Join(pfx, "obj")
	cresp, err := b.Create(ctx, &proto.CreateRequest{Key: []byte(key), Value: []byte("v1")})
	require.NoError(t, err)
	require.True(t, cresp.Succeeded)
	uresp, err := b.Update(ctx, &proto.UpdateRequest{
		Kv: &proto.KeyValue{Key: []byte(key), Value: []byte("v2"), Revision: cresp.Header.Revision}})
	require.NoError(t, err)
	require.True(t, uresp.Succeeded)
	dresp, err := b.Delete(ctx, &proto.DeleteRequest{Key: []byte(key), Revision: uresp.Header.Revision})
	require.NoError(t, err)
	require.True(t, dresp.Succeeded)
	deleteRev := dresp.Header.Revision
	waitUntilRevisionEqualOrTimeout(b, deleteRev)

	events, served, err := b.eventLogWatchEvents(ctx, pfx, startRev+1, deleteRev)
	require.NoError(t, err)
	require.True(t, served)
	require.Len(t, events, 3)

	require.Equal(t, proto.Event_CREATE, events[0].Type)
	require.Equal(t, proto.Event_PUT, events[1].Type)
	require.Equal(t, proto.Event_DELETE, events[2].Type)
	// DELETE reuses the deleted (previous) version and must still carry its value
	// — this is exactly the object-key dedup the fallback loop has to resolve.
	require.Equal(t, uresp.Header.Revision, events[2].Kv.Revision)
	require.NotEmpty(t, events[2].Kv.Value, "DELETE must carry the deleted value")

	// The fallback replay must agree with the authoritative full-prefix scan
	// event-for-event on (type, revision, value-revision, value).
	scanEvents, err := b.scanHistoryEvents(ctx, pfx, startRev+1, deleteRev)
	require.NoError(t, err)
	require.Len(t, scanEvents, 3)
	for i := range scanEvents {
		require.Equal(t, scanEvents[i].Type, events[i].Type, "event %d type", i)
		require.Equal(t, scanEvents[i].Revision, events[i].Revision, "event %d revision", i)
		require.Equal(t, scanEvents[i].Kv.Revision, events[i].Kv.Revision, "event %d value revision", i)
		require.Equal(t, scanEvents[i].Kv.Value, events[i].Kv.Value, "event %d value", i)
	}
}

// TestEventLogReplayMatchesWrites pins #45: after create/update/delete, the
// event-log replay reconstructs exactly the events a watcher saw — same types,
// revisions, keys and (enveloped) values — without a full-prefix scan, and CAS
// misses/holes never surface.
func TestEventLogReplayMatchesWrites(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	m := mock.NewMinimalMetrics(ctrl)
	kv := imemkv.NewKvStorage()
	defer func() { require.NoError(t, kv.Close()) }()

	pfx := fmt.Sprintf("/kubebrain/elog_test/%d", time.Now().UnixNano())
	b := NewBackend(kv, Config{
		Prefix: pfx, Identity: getStorageIdentity(), EnableEtcdCompatibility: true,
	}, m).(*backend)
	b.SetCurrentRevision(uint64(time.Now().UnixNano()))
	ctx := context.Background()
	require.NoError(t, b.EnsureEventLogStart(ctx))
	startRev := b.GetCurrentRevision()

	key := path.Join(pfx, "obj")
	cresp, err := b.Create(ctx, &proto.CreateRequest{Key: []byte(key), Value: []byte("v1")})
	require.NoError(t, err)
	require.True(t, cresp.Succeeded)
	createRev := cresp.Header.Revision

	uresp, err := b.Update(ctx, &proto.UpdateRequest{
		Kv: &proto.KeyValue{Key: []byte(key), Value: []byte("v2"), Revision: createRev}})
	require.NoError(t, err)
	require.True(t, uresp.Succeeded)
	updateRev := uresp.Header.Revision

	dresp, err := b.Delete(ctx, &proto.DeleteRequest{Key: []byte(key), Revision: updateRev})
	require.NoError(t, err)
	require.True(t, dresp.Succeeded)
	deleteRev := dresp.Header.Revision

	waitUntilRevisionEqualOrTimeout(b, deleteRev)

	events, served, err := b.eventLogWatchEvents(ctx, pfx, startRev+1, deleteRev)
	require.NoError(t, err)
	require.True(t, served, "window above the watermark must be served from the log")
	require.Len(t, events, 3)

	require.Equal(t, proto.Event_CREATE, events[0].Type)
	require.Equal(t, createRev, events[0].Revision)
	require.Equal(t, createRev, events[0].Kv.Revision)

	require.Equal(t, proto.Event_PUT, events[1].Type)
	require.Equal(t, updateRev, events[1].Revision)

	require.Equal(t, proto.Event_DELETE, events[2].Type)
	require.Equal(t, deleteRev, events[2].Revision)
	// DELETE carries the deleted (previous) version, like the scan path.
	require.Equal(t, updateRev, events[2].Kv.Revision)
	require.NotEmpty(t, events[2].Kv.Value, "DELETE must carry the deleted value")

	// The replay must agree with the full-prefix scan fallback event-for-event
	// on (type, revision, value-revision).
	scanEvents, err := b.scanHistoryEvents(ctx, pfx, startRev+1, deleteRev)
	require.NoError(t, err)
	require.Len(t, scanEvents, 3)
	for i := range scanEvents {
		require.Equal(t, scanEvents[i].Revision, events[i].Revision, "event %d revision", i)
		require.Equal(t, scanEvents[i].Kv.Revision, events[i].Kv.Revision, "event %d value revision", i)
		require.Equal(t, scanEvents[i].Kv.Value, events[i].Kv.Value, "event %d value", i)
	}

	// Prefix filtering: an unrelated prefix replays to zero events.
	other, served, err := b.eventLogWatchEvents(ctx, pfx+"/nothing-here", startRev+1, deleteRev)
	require.NoError(t, err)
	require.True(t, served)
	require.Empty(t, other)
}

// TestEventLogReplayCoversTxnApply pins review #51's critical finding: the
// TxnApply commit path (leased puts, generic etcd txns) must stage event-log
// entries like every other write path — the replay treats a missing entry at
// a committed revision as a failed-CAS hole and silently skips it, so a
// leaked write here is a silently lost watch event.
func TestEventLogReplayCoversTxnApply(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	m := mock.NewMinimalMetrics(ctrl)
	kv := imemkv.NewKvStorage()
	defer func() { require.NoError(t, kv.Close()) }()

	pfx := fmt.Sprintf("/kubebrain/elog_txn_test/%d", time.Now().UnixNano())
	b := NewBackend(kv, Config{
		Prefix: pfx, Identity: getStorageIdentity(), EnableEtcdCompatibility: true,
	}, m).(*backend)
	b.SetCurrentRevision(uint64(time.Now().UnixNano()))
	ctx := context.Background()
	require.NoError(t, b.EnsureEventLogStart(ctx))
	startRev := b.GetCurrentRevision()

	keyA := path.Join(pfx, "a")
	keyB := path.Join(pfx, "b")

	// Deliberately use reverse lexical order: replay must preserve etcd's
	// transaction sub-revision order, not the physical event-log key order.
	_, createRev, err := b.TxnApply(ctx, []TxnWriteOp{
		{Key: []byte(keyB), Value: []byte("b1")},
		{Key: []byte(keyA), Value: []byte("a1")},
	}, nil)
	require.NoError(t, err)
	// txn update of B + delete of A, again in reverse lexical order.
	_, mixedRev, err := b.TxnApply(ctx, []TxnWriteOp{
		{Key: []byte(keyB), Value: []byte("b2")},
		{Delete: true, Key: []byte(keyA)},
	}, nil)
	require.NoError(t, err)

	waitUntilRevisionEqualOrTimeout(b, mixedRev)

	events, served, err := b.eventLogWatchEvents(ctx, pfx, startRev+1, mixedRev)
	require.NoError(t, err)
	require.True(t, served)
	require.Len(t, events, 4, "every TxnApply write must replay from the log")

	require.Equal(t, []string{keyB, keyA, keyB, keyA}, []string{
		string(events[0].Kv.Key), string(events[1].Kv.Key), string(events[2].Kv.Key), string(events[3].Kv.Key),
	})
	for _, e := range events[:2] {
		require.Equal(t, createRev, e.Revision, "txn writes share one revision")
		require.Equal(t, proto.Event_CREATE, e.Type)
	}

	// Physically compact exactly at the mixed transaction. etcd still permits a
	// watch starting at the compact watermark, so the DELETE's preceding value
	// must remain available to reconstruct that boundary event after restart.
	_, err = b.Compact(ctx, mixedRev)
	require.NoError(t, err)

	// Simulate leadership reacquisition moving the conservative watermark over
	// the latest transaction. The public history path must use the self-contained
	// exact-revision log, retain B,A operation order rather than key-sort A,B,
	// and recover the DELETE value after physical compaction.
	require.NoError(t, b.EnsureEventLogStart(ctx))
	recovered, err := b.historyWatchEvents(ctx, pfx, mixedRev, mixedRev, mixedRev)
	require.NoError(t, err)
	require.Len(t, recovered, 2)
	require.Equal(t, []string{keyB, keyA}, []string{
		string(recovered[0].Kv.Key), string(recovered[1].Kv.Key),
	})
	require.Equal(t, proto.Event_DELETE, recovered[1].Type)
	_, deletedValue, inlined := DecodeInlineValue(recovered[1].Kv.Value)
	require.True(t, inlined)
	require.Equal(t, []byte("a1"), deletedValue)
}

// TestEventLogWatermarkGates pins the completeness watermark: replays at or
// below it (pre-log history, cleanup, leadership change) must refuse and fall
// back rather than serve a window with holes.
func TestEventLogWatermarkGates(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	m := mock.NewMinimalMetrics(ctrl)
	kv := imemkv.NewKvStorage()
	defer func() { require.NoError(t, kv.Close()) }()

	pfx := fmt.Sprintf("/kubebrain/elog_gate/%d", time.Now().UnixNano())
	b := NewBackend(kv, Config{
		Prefix: pfx, Identity: getStorageIdentity(), EnableEtcdCompatibility: true,
	}, m).(*backend)
	b.SetCurrentRevision(uint64(time.Now().UnixNano()))
	ctx := context.Background()
	require.NoError(t, b.EnsureEventLogStart(ctx))
	startRev := b.GetCurrentRevision()

	key := path.Join(pfx, "k")
	cresp, err := b.Create(ctx, &proto.CreateRequest{Key: []byte(key), Value: []byte("v")})
	require.NoError(t, err)
	rev := cresp.Header.Revision
	waitUntilRevisionEqualOrTimeout(b, rev)

	// A window starting at or below the watermark is refused.
	_, served, err := b.eventLogWatchEvents(ctx, pfx, startRev, rev)
	require.NoError(t, err)
	require.False(t, served, "window touching the watermark must fall back")

	// Cleanup through a compact revision retains the exactly-equal boundary,
	// which Range/Watch still permit, and its ordered entry is self-contained.
	b.cleanupEventLog(ctx, rev)
	_, served, err = b.eventLogWatchEvents(ctx, pfx, rev, rev)
	require.NoError(t, err)
	require.True(t, served, "compact boundary events must remain replayable")

	// A fresh leadership acquisition advances the conservative watermark, but a
	// new-format exact revision whose repeated count proves every event is present
	// can still replay safely (needed for restart catch-up at current revision).
	c2, err := b.Create(ctx, &proto.CreateRequest{Key: []byte(key + "2"), Value: []byte("v")})
	require.NoError(t, err)
	waitUntilRevisionEqualOrTimeout(b, c2.Header.Revision)
	require.NoError(t, b.EnsureEventLogStart(ctx))
	_, served, err = b.eventLogWatchEvents(ctx, pfx, c2.Header.Revision, c2.Header.Revision)
	require.NoError(t, err)
	require.True(t, served, "a self-contained exact revision must survive the leadership watermark")

	// A claimed total larger than the physical entry set cannot bypass the
	// watermark; otherwise one lost txn event would be silently accepted.
	corrupt := kv.BeginBatchWrite()
	corrupt.Put(b.ks.EncodeEventLogKey(c2.Header.Revision, []byte(key+"2")),
		coder.EncodeOrderedEventLogValue(byte(proto.Event_CREATE), 0, 0, 2), 0)
	require.NoError(t, corrupt.Commit(ctx))
	_, served, err = b.eventLogWatchEvents(ctx, pfx, c2.Header.Revision, c2.Header.Revision)
	require.NoError(t, err)
	require.False(t, served, "an incomplete ordered revision must fall back")
	alarms, alarmErr := b.CorruptAlarms(ctx)
	require.NoError(t, alarmErr)
	require.Empty(t, alarms, "a revision at the cleanup/leadership watermark is untrusted, not corruption")
}

func TestEventLogReplayRejectsInvalidOrderedVerbInTrustedWindow(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	m := mock.NewMinimalMetrics(ctrl)
	kv := imemkv.NewKvStorage()
	defer func() { require.NoError(t, kv.Close()) }()

	pfx := fmt.Sprintf("/kubebrain/elog_invalid_verb/%d", time.Now().UnixNano())
	b := NewBackend(kv, Config{Prefix: pfx, Identity: getStorageIdentity(), EnableEtcdCompatibility: true}, m).(*backend)
	b.SetCurrentRevision(uint64(time.Now().UnixNano()))
	ctx := context.Background()
	require.NoError(t, b.EnsureEventLogStart(ctx))
	key := path.Join(pfx, "key")
	created, err := b.Create(ctx, &proto.CreateRequest{Key: []byte(key), Value: []byte("value")})
	require.NoError(t, err)
	waitUntilRevisionEqualOrTimeout(b, created.Header.Revision)

	batch := kv.BeginBatchWrite()
	batch.CAS(
		b.ks.EncodeEventLogKey(created.Header.Revision, []byte(key)),
		coder.EncodeOrderedEventLogValue(0xff, 0, 0, 1),
		coder.EncodeOrderedEventLogValue(byte(proto.Event_CREATE), 0, 0, 1), 0,
	)
	require.NoError(t, batch.Commit(ctx))

	events, served, err := b.eventLogWatchEvents(ctx, pfx, created.Header.Revision, created.Header.Revision)
	require.ErrorIs(t, err, ErrInvalidMVCCMetadata)
	require.ErrorContains(t, err, "unsupported verb")
	require.False(t, served)
	require.Empty(t, events)
	alarms, alarmErr := b.CorruptAlarms(ctx)
	require.NoError(t, alarmErr)
	require.Equal(t, []uint64{b.localAlarmMemberID()}, alarms)

	// Once a leadership watermark explicitly makes this revision untrusted,
	// the same malformed legacy window may use the object-scan compatibility
	// fallback; it must not be mislabeled as trusted corruption.
	require.NoError(t, b.EnsureEventLogStart(ctx))
	events, served, err = b.eventLogWatchEvents(ctx, pfx, created.Header.Revision, created.Header.Revision)
	require.NoError(t, err)
	require.False(t, served)
	require.Empty(t, events)
}

func TestEventLogReplayRejectsMissingReferencedObjectInTrustedWindow(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	m := mock.NewMinimalMetrics(ctrl)
	kv := imemkv.NewKvStorage()
	defer func() { require.NoError(t, kv.Close()) }()

	pfx := fmt.Sprintf("/kubebrain/elog_missing_object/%d", time.Now().UnixNano())
	b := NewBackend(kv, Config{Prefix: pfx, Identity: getStorageIdentity(), EnableEtcdCompatibility: true}, m).(*backend)
	b.SetCurrentRevision(uint64(time.Now().UnixNano()))
	ctx := context.Background()
	require.NoError(t, b.EnsureEventLogStart(ctx))
	key := path.Join(pfx, "key")
	created, err := b.Create(ctx, &proto.CreateRequest{Key: []byte(key), Value: []byte("value")})
	require.NoError(t, err)
	waitUntilRevisionEqualOrTimeout(b, created.Header.Revision)

	objectKey := b.coder.EncodeObjectKey([]byte(key), created.Header.Revision)
	objectValue, err := kv.Get(ctx, objectKey)
	require.NoError(t, err)
	batch := kv.BeginBatchWrite()
	batch.Del(objectKey)
	require.NoError(t, batch.Commit(ctx))

	events, served, err := b.eventLogWatchEvents(ctx, pfx, created.Header.Revision, created.Header.Revision)
	require.ErrorIs(t, err, ErrInvalidMVCCMetadata)
	require.ErrorContains(t, err, "referenced object version is missing")
	require.False(t, served)
	require.Empty(t, events)
	alarms, alarmErr := b.CorruptAlarms(ctx)
	require.NoError(t, alarmErr)
	require.Equal(t, []uint64{b.localAlarmMemberID()}, alarms,
		"a missing object covered by a trusted durable witness must persist CORRUPT")
	_, writeErr := b.Create(ctx, &proto.CreateRequest{Key: []byte(path.Join(pfx, "blocked")), Value: []byte("blocked")})
	require.ErrorIs(t, writeErr, ErrCorruptAlarmActive)
	removed, disarmErr := b.DisarmCorrupt(ctx, b.localAlarmMemberID())
	require.ErrorIs(t, disarmErr, ErrTxnWitnessCorrupt)
	require.False(t, removed, "the missing referenced object must keep writes fenced")

	repair := kv.BeginBatchWrite()
	repair.Put(objectKey, objectValue, 0)
	require.NoError(t, repair.Commit(ctx))
	removed, disarmErr = b.DisarmCorrupt(ctx, b.localAlarmMemberID())
	require.NoError(t, disarmErr)
	require.True(t, removed)
	events, served, err = b.eventLogWatchEvents(ctx, pfx, created.Header.Revision, created.Header.Revision)
	require.NoError(t, err)
	require.True(t, served)
	require.Len(t, events, 1)
}

func TestEventLogReplayRejectsCorruptReferencedObjectInTrustedWindow(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	m := mock.NewMinimalMetrics(ctrl)
	kv := imemkv.NewKvStorage()
	defer func() { require.NoError(t, kv.Close()) }()

	pfx := fmt.Sprintf("/kubebrain/elog_corrupt_object/%d", time.Now().UnixNano())
	b := NewBackend(kv, Config{Prefix: pfx, Identity: getStorageIdentity(), EnableEtcdCompatibility: true}, m).(*backend)
	b.SetCurrentRevision(uint64(time.Now().UnixNano()))
	ctx := context.Background()
	require.NoError(t, b.EnsureEventLogStart(ctx))
	key := path.Join(pfx, "key")
	created, err := b.Create(ctx, &proto.CreateRequest{Key: []byte(key), Value: []byte("value")})
	require.NoError(t, err)
	waitUntilRevisionEqualOrTimeout(b, created.Header.Revision)

	objectKey := b.coder.EncodeObjectKey([]byte(key), created.Header.Revision)
	objectValue, err := kv.Get(ctx, objectKey)
	require.NoError(t, err)
	corrupt := kv.BeginBatchWrite()
	corrupt.Put(objectKey, []byte{0, 'k', 'b', 3}, 0)
	require.NoError(t, corrupt.Commit(ctx))

	events, served, err := b.eventLogWatchEvents(ctx, pfx, created.Header.Revision, created.Header.Revision)
	require.ErrorIs(t, err, ErrInvalidMVCCMetadata)
	require.ErrorContains(t, err, "inline value metadata")
	require.False(t, served)
	require.Empty(t, events)
	alarms, alarmErr := b.CorruptAlarms(ctx)
	require.NoError(t, alarmErr)
	require.Equal(t, []uint64{b.localAlarmMemberID()}, alarms)
	_, writeErr := b.Create(ctx, &proto.CreateRequest{Key: []byte(path.Join(pfx, "blocked")), Value: []byte("blocked")})
	require.ErrorIs(t, writeErr, ErrCorruptAlarmActive)
	removed, disarmErr := b.DisarmCorrupt(ctx, b.localAlarmMemberID())
	require.ErrorIs(t, disarmErr, ErrTxnWitnessCorrupt)
	require.False(t, removed)

	repair := kv.BeginBatchWrite()
	repair.Put(objectKey, objectValue, 0)
	require.NoError(t, repair.Commit(ctx))
	removed, disarmErr = b.DisarmCorrupt(ctx, b.localAlarmMemberID())
	require.NoError(t, disarmErr)
	require.True(t, removed)
	events, served, err = b.eventLogWatchEvents(ctx, pfx, created.Header.Revision, created.Header.Revision)
	require.NoError(t, err)
	require.True(t, served)
	require.Len(t, events, 1)
}

func TestGetArmsCorruptForWitnessedInvalidObjectValue(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	m := mock.NewMinimalMetrics(ctrl)
	kv := imemkv.NewKvStorage()
	defer func() { require.NoError(t, kv.Close()) }()

	pfx := fmt.Sprintf("/kubebrain/get_corrupt_object/%d", time.Now().UnixNano())
	b := NewBackend(kv, Config{Prefix: pfx, Identity: getStorageIdentity(), EnableEtcdCompatibility: true}, m).(*backend)
	b.SetCurrentRevision(uint64(time.Now().UnixNano()))
	ctx := context.Background()
	require.NoError(t, b.EnsureEventLogStart(ctx))
	key := path.Join(pfx, "key")
	created, err := b.Create(ctx, &proto.CreateRequest{Key: []byte(key), Value: []byte("value")})
	require.NoError(t, err)
	waitUntilRevisionEqualOrTimeout(b, created.Header.Revision)

	objectKey := b.coder.EncodeObjectKey([]byte(key), created.Header.Revision)
	objectValue, err := kv.Get(ctx, objectKey)
	require.NoError(t, err)
	corrupt := kv.BeginBatchWrite()
	corrupt.Put(objectKey, []byte{0, 'k', 'b', 3}, 0)
	require.NoError(t, corrupt.Commit(ctx))

	resp, err := b.Get(ctx, &proto.GetRequest{Key: []byte(key)})
	require.ErrorIs(t, err, ErrInvalidMVCCMetadata)
	require.Nil(t, resp)
	alarms, alarmErr := b.CorruptAlarms(ctx)
	require.NoError(t, alarmErr)
	require.Equal(t, []uint64{b.localAlarmMemberID()}, alarms)
	_, writeErr := b.Create(ctx, &proto.CreateRequest{Key: []byte(path.Join(pfx, "blocked")), Value: []byte("blocked")})
	require.ErrorIs(t, writeErr, ErrCorruptAlarmActive)
	removed, disarmErr := b.DisarmCorrupt(ctx, b.localAlarmMemberID())
	require.ErrorIs(t, disarmErr, ErrTxnWitnessCorrupt)
	require.False(t, removed)

	repair := kv.BeginBatchWrite()
	repair.Put(objectKey, objectValue, 0)
	require.NoError(t, repair.Commit(ctx))
	removed, disarmErr = b.DisarmCorrupt(ctx, b.localAlarmMemberID())
	require.NoError(t, disarmErr)
	require.True(t, removed)
	resp, err = b.Get(ctx, &proto.GetRequest{Key: []byte(key)})
	require.NoError(t, err)
	require.Equal(t, objectValue, resp.GetKv().GetValue())
}

func TestGetInvalidObjectWithoutWitnessDoesNotArmCorrupt(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	m := mock.NewMinimalMetrics(ctrl)
	kv := imemkv.NewKvStorage()
	defer func() { require.NoError(t, kv.Close()) }()

	pfx := fmt.Sprintf("/kubebrain/get_unwitnessed_corrupt_object/%d", time.Now().UnixNano())
	b := NewBackend(kv, Config{Prefix: pfx, Identity: getStorageIdentity(), EnableEtcdCompatibility: true}, m).(*backend)
	b.SetCurrentRevision(uint64(time.Now().UnixNano()))
	ctx := context.Background()
	require.NoError(t, b.EnsureEventLogStart(ctx))
	key := path.Join(pfx, "key")
	created, err := b.Create(ctx, &proto.CreateRequest{Key: []byte(key), Value: []byte("value")})
	require.NoError(t, err)
	waitUntilRevisionEqualOrTimeout(b, created.Header.Revision)

	batch := kv.BeginBatchWrite()
	batch.Del(b.ks.EncodeInternalKey(txnWitnessLogicalKey(created.Header.Revision)))
	batch.Put(b.coder.EncodeObjectKey([]byte(key), created.Header.Revision), []byte{0, 'k', 'b', 3}, 0)
	require.NoError(t, batch.Commit(ctx))

	resp, err := b.Get(ctx, &proto.GetRequest{Key: []byte(key)})
	require.ErrorIs(t, err, ErrInvalidMVCCMetadata)
	require.Nil(t, resp)
	alarms, alarmErr := b.CorruptAlarms(ctx)
	require.NoError(t, alarmErr)
	require.Empty(t, alarms, "unsealed legacy/cleaned history has no durable evidence for a safe CORRUPT disarm")
}

func TestListArmsCorruptForWitnessedInvalidObjectValue(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	m := mock.NewMinimalMetrics(ctrl)
	kv := imemkv.NewKvStorage()
	defer func() { require.NoError(t, kv.Close()) }()

	pfx := fmt.Sprintf("/kubebrain/list_corrupt_object/%d", time.Now().UnixNano())
	b := NewBackend(kv, Config{Prefix: pfx, Identity: getStorageIdentity(), EnableEtcdCompatibility: true}, m).(*backend)
	b.SetCurrentRevision(uint64(time.Now().UnixNano()))
	ctx := context.Background()
	require.NoError(t, b.EnsureEventLogStart(ctx))
	key := path.Join(pfx, "key")
	created, err := b.Create(ctx, &proto.CreateRequest{Key: []byte(key), Value: []byte("value")})
	require.NoError(t, err)
	waitUntilRevisionEqualOrTimeout(b, created.Header.Revision)

	objectKey := b.coder.EncodeObjectKey([]byte(key), created.Header.Revision)
	objectValue, err := kv.Get(ctx, objectKey)
	require.NoError(t, err)
	corrupt := kv.BeginBatchWrite()
	corrupt.Put(objectKey, []byte{0, 'k', 'b', 3}, 0)
	require.NoError(t, corrupt.Commit(ctx))

	start := []byte(pfx + "/")
	resp, err := b.List(ctx, &proto.RangeRequest{Key: start, End: PrefixEnd(start)})
	require.ErrorIs(t, err, ErrInvalidMVCCMetadata)
	require.Nil(t, resp)
	alarms, alarmErr := b.CorruptAlarms(ctx)
	require.NoError(t, alarmErr)
	require.Equal(t, []uint64{b.localAlarmMemberID()}, alarms)
	_, writeErr := b.Create(ctx, &proto.CreateRequest{Key: []byte(path.Join(pfx, "blocked")), Value: []byte("blocked")})
	require.ErrorIs(t, writeErr, ErrCorruptAlarmActive)
	removed, disarmErr := b.DisarmCorrupt(ctx, b.localAlarmMemberID())
	require.ErrorIs(t, disarmErr, ErrTxnWitnessCorrupt)
	require.False(t, removed)

	repair := kv.BeginBatchWrite()
	repair.Put(objectKey, objectValue, 0)
	require.NoError(t, repair.Commit(ctx))
	removed, disarmErr = b.DisarmCorrupt(ctx, b.localAlarmMemberID())
	require.NoError(t, disarmErr)
	require.True(t, removed)
	resp, err = b.List(ctx, &proto.RangeRequest{Key: start, End: PrefixEnd(start)})
	require.NoError(t, err)
	require.Len(t, resp.Kvs, 1)
	require.Equal(t, objectValue, resp.Kvs[0].Value)
}

func TestRangeStreamArmsCorruptForWitnessedInvalidObjectValue(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	m := mock.NewMinimalMetrics(ctrl)
	kv := imemkv.NewKvStorage()
	defer func() { require.NoError(t, kv.Close()) }()

	pfx := fmt.Sprintf("/kubebrain/range_stream_corrupt_object/%d", time.Now().UnixNano())
	b := NewBackend(kv, Config{Prefix: pfx, Identity: getStorageIdentity(), EnableEtcdCompatibility: true}, m).(*backend)
	b.SetCurrentRevision(uint64(time.Now().UnixNano()))
	ctx := context.Background()
	require.NoError(t, b.EnsureEventLogStart(ctx))
	key := path.Join(pfx, "key")
	created, err := b.Create(ctx, &proto.CreateRequest{Key: []byte(key), Value: []byte("value")})
	require.NoError(t, err)
	waitUntilRevisionEqualOrTimeout(b, created.Header.Revision)

	corrupt := kv.BeginBatchWrite()
	corrupt.Put(b.coder.EncodeObjectKey([]byte(key), created.Header.Revision), []byte{0, 'k', 'b', 3}, 0)
	require.NoError(t, corrupt.Commit(ctx))

	start := []byte(pfx + "/")
	stream, err := b.RangeStream(ctx, start, PrefixEnd(start), 0)
	require.NoError(t, err)
	var dataKvs []*proto.KeyValue
	var encodedErr string
	for chunk := range stream {
		dataKvs = append(dataKvs, chunk.GetRangeResponse().GetKvs()...)
		if chunk.GetErr() != "" {
			encodedErr = chunk.GetErr()
		}
	}
	require.Empty(t, dataKvs, "a corrupt chunk must not become partially visible")
	decodedErr, ok := streamerror.Decode(encodedErr)
	require.True(t, ok)
	require.ErrorIs(t, decodedErr, ErrInvalidMVCCMetadata)
	alarms, alarmErr := b.CorruptAlarms(ctx)
	require.NoError(t, alarmErr)
	require.Equal(t, []uint64{b.localAlarmMemberID()}, alarms)
	_, writeErr := b.Create(ctx, &proto.CreateRequest{Key: []byte(path.Join(pfx, "blocked")), Value: []byte("blocked")})
	require.ErrorIs(t, writeErr, ErrCorruptAlarmActive)
}

func TestSnapshotStreamsArmCorruptForWitnessedInvalidObjectValue(t *testing.T) {
	tests := []struct {
		name string
		read func(context.Context, *backend, uint64) (int, error)
	}{
		{
			name: "current-snapshot",
			read: func(ctx context.Context, b *backend, revision uint64) (int, error) {
				stream, err := b.SnapshotStream(ctx, revision)
				if err != nil {
					return 0, err
				}
				count := 0
				for chunk := range stream {
					count += len(chunk.GetRangeResponse().GetKvs())
					if chunk.GetErr() != "" {
						decoded, ok := streamerror.Decode(chunk.GetErr())
						if !ok {
							return count, fmt.Errorf("decode snapshot stream error %q", chunk.GetErr())
						}
						return count, decoded
					}
				}
				return count, nil
			},
		},
		{
			name: "history-snapshot",
			read: func(ctx context.Context, b *backend, revision uint64) (int, error) {
				stream, err := b.SnapshotHistoryStream(ctx, revision)
				if err != nil {
					return 0, err
				}
				count := 0
				for chunk := range stream {
					count += len(chunk.Records)
					if chunk.Err != nil {
						return count, chunk.Err
					}
				}
				return count, nil
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()
			m := mock.NewMinimalMetrics(ctrl)
			kv := imemkv.NewKvStorage()
			defer func() { require.NoError(t, kv.Close()) }()

			pfx := fmt.Sprintf("/kubebrain/%s_corrupt_object/%d", test.name, time.Now().UnixNano())
			b := NewBackend(kv, Config{Prefix: pfx, Identity: getStorageIdentity(), EnableEtcdCompatibility: true}, m).(*backend)
			b.SetCurrentRevision(uint64(time.Now().UnixNano()))
			ctx := context.Background()
			require.NoError(t, b.EnsureEventLogStart(ctx))
			key := path.Join(pfx, "key")
			created, err := b.Create(ctx, &proto.CreateRequest{Key: []byte(key), Value: []byte("value")})
			require.NoError(t, err)
			waitUntilRevisionEqualOrTimeout(b, created.Header.Revision)

			objectKey := b.coder.EncodeObjectKey([]byte(key), created.Header.Revision)
			objectValue, err := kv.Get(ctx, objectKey)
			require.NoError(t, err)
			corrupt := kv.BeginBatchWrite()
			corrupt.Put(objectKey, []byte{0, 'k', 'b', 3}, 0)
			require.NoError(t, corrupt.Commit(ctx))

			count, err := test.read(ctx, b, created.Header.Revision)
			require.ErrorIs(t, err, ErrInvalidMVCCMetadata)
			require.Zero(t, count, "a corrupt object must not enter a snapshot artifact")
			alarms, alarmErr := b.CorruptAlarms(ctx)
			require.NoError(t, alarmErr)
			require.Equal(t, []uint64{b.localAlarmMemberID()}, alarms)
			_, writeErr := b.Create(ctx, &proto.CreateRequest{Key: []byte(path.Join(pfx, "blocked")), Value: []byte("blocked")})
			require.ErrorIs(t, writeErr, ErrCorruptAlarmActive)
			removed, disarmErr := b.DisarmCorrupt(ctx, b.localAlarmMemberID())
			require.ErrorIs(t, disarmErr, ErrTxnWitnessCorrupt)
			require.False(t, removed)

			repair := kv.BeginBatchWrite()
			repair.Put(objectKey, objectValue, 0)
			require.NoError(t, repair.Commit(ctx))
			removed, disarmErr = b.DisarmCorrupt(ctx, b.localAlarmMemberID())
			require.NoError(t, disarmErr)
			require.True(t, removed)
			count, err = test.read(ctx, b, created.Header.Revision)
			require.NoError(t, err)
			require.Equal(t, 1, count)
		})
	}
}

func TestHistoryWatchFallbackArmsCorruptForWitnessedObjectValue(t *testing.T) {
	tests := []struct {
		name    string
		prepare func(*testing.T, context.Context, *backend, string) (objectRevision, fromRevision, throughRevision uint64)
	}{
		{
			name: "live-event",
			prepare: func(t *testing.T, ctx context.Context, b *backend, key string) (uint64, uint64, uint64) {
				created, err := b.Create(ctx, &proto.CreateRequest{Key: []byte(key), Value: []byte("value")})
				require.NoError(t, err)
				waitUntilRevisionEqualOrTimeout(b, created.Header.Revision)
				return created.Header.Revision, created.Header.Revision, created.Header.Revision
			},
		},
		{
			name: "delete-prev-kv",
			prepare: func(t *testing.T, ctx context.Context, b *backend, key string) (uint64, uint64, uint64) {
				created, err := b.Create(ctx, &proto.CreateRequest{Key: []byte(key), Value: []byte("value")})
				require.NoError(t, err)
				deleted, err := b.Delete(ctx, &proto.DeleteRequest{Key: []byte(key), Revision: created.Header.Revision})
				require.NoError(t, err)
				require.True(t, deleted.Succeeded)
				waitUntilRevisionEqualOrTimeout(b, deleted.Header.Revision)
				return created.Header.Revision, deleted.Header.Revision, deleted.Header.Revision
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()
			m := mock.NewMinimalMetrics(ctrl)
			kv := imemkv.NewKvStorage()
			defer func() { require.NoError(t, kv.Close()) }()

			pfx := fmt.Sprintf("/kubebrain/watch_fallback_%s/%d", test.name, time.Now().UnixNano())
			b := NewBackend(kv, Config{Prefix: pfx, Identity: getStorageIdentity(), EnableEtcdCompatibility: true}, m).(*backend)
			b.SetCurrentRevision(uint64(time.Now().UnixNano()))
			ctx := context.Background()
			require.NoError(t, b.EnsureEventLogStart(ctx))
			key := path.Join(pfx, "key")
			objectRevision, fromRevision, throughRevision := test.prepare(t, ctx, b, key)

			objectKey := b.coder.EncodeObjectKey([]byte(key), objectRevision)
			objectValue, err := kv.Get(ctx, objectKey)
			require.NoError(t, err)
			corrupt := kv.BeginBatchWrite()
			corrupt.Put(objectKey, []byte{0, 'k', 'b', 3}, 0)
			require.NoError(t, corrupt.Commit(ctx))

			events, err := b.scanHistoryEvents(ctx, key, fromRevision, throughRevision)
			require.ErrorIs(t, err, ErrInvalidMVCCMetadata)
			require.Empty(t, events)
			alarms, alarmErr := b.CorruptAlarms(ctx)
			require.NoError(t, alarmErr)
			require.Equal(t, []uint64{b.localAlarmMemberID()}, alarms)
			_, writeErr := b.Create(ctx, &proto.CreateRequest{Key: []byte(path.Join(pfx, "blocked")), Value: []byte("blocked")})
			require.ErrorIs(t, writeErr, ErrCorruptAlarmActive)
			removed, disarmErr := b.DisarmCorrupt(ctx, b.localAlarmMemberID())
			require.ErrorIs(t, disarmErr, ErrTxnWitnessCorrupt)
			require.False(t, removed)

			repair := kv.BeginBatchWrite()
			repair.Put(objectKey, objectValue, 0)
			require.NoError(t, repair.Commit(ctx))
			removed, disarmErr = b.DisarmCorrupt(ctx, b.localAlarmMemberID())
			require.NoError(t, disarmErr)
			require.True(t, removed)
			events, err = b.scanHistoryEvents(ctx, key, fromRevision, throughRevision)
			require.NoError(t, err)
			require.Len(t, events, 1)
		})
	}
}

func TestEventLogMissingObjectAfterCompactAdvanceDoesNotArmCorrupt(t *testing.T) {
	m := &compactMetricRecorder{}
	kv := imemkv.NewKvStorage()
	defer func() { require.NoError(t, kv.Close()) }()

	pfx := fmt.Sprintf("/kubebrain/elog_compact_object_race/%d", time.Now().UnixNano())
	b := NewBackend(kv, Config{Prefix: pfx, Identity: getStorageIdentity(), EnableEtcdCompatibility: true}, m).(*backend)
	b.SetCurrentRevision(uint64(time.Now().UnixNano()))
	ctx := context.Background()
	require.NoError(t, b.EnsureEventLogStart(ctx))
	victimKey := path.Join(pfx, "victim")
	victim, err := b.Create(ctx, &proto.CreateRequest{Key: []byte(victimKey), Value: []byte("victim")})
	require.NoError(t, err)
	newer, err := b.Create(ctx, &proto.CreateRequest{Key: []byte(path.Join(pfx, "newer")), Value: []byte("newer")})
	require.NoError(t, err)
	waitUntilRevisionEqualOrTimeout(b, newer.Header.Revision)

	// Model the cross-replica window where the durable compact watermark has
	// committed and physical GC removed an older object, while event-log cleanup
	// has not yet advanced its own watermark.
	batch := kv.BeginBatchWrite()
	batch.Del(b.coder.EncodeObjectKey([]byte(victimKey), victim.Header.Revision))
	require.NoError(t, batch.Commit(ctx))
	advanced, err := b.setCompactRecord(ctx, newer.Header.Revision)
	require.NoError(t, err)
	require.True(t, advanced)

	events, served, err := b.eventLogWatchEvents(ctx, pfx, victim.Header.Revision, victim.Header.Revision)
	require.NoError(t, err)
	require.False(t, served)
	require.Empty(t, events)
	alarms, alarmErr := b.CorruptAlarms(ctx)
	require.NoError(t, alarmErr)
	require.Empty(t, alarms, "a freshly compacted object is normal GC, not durable corruption")
	require.NotContains(t, m.snapshot(), compactMetricRecord{
		kind: "counter", name: "watch.event_log.corruption", value: 1,
		tags: []metrics.T{metrics.Tag("kind", eventLogCorruptionIncomplete)},
	}, "a compaction race must not increment confirmed corruption telemetry")
}

func TestEventLogReplayRejectsMissingEntryCoveredByTxnWitness(t *testing.T) {
	m := &compactMetricRecorder{}
	kv := imemkv.NewKvStorage()
	defer func() { require.NoError(t, kv.Close()) }()

	pfx := fmt.Sprintf("/kubebrain/elog_missing_entry/%d", time.Now().UnixNano())
	b := NewBackend(kv, Config{Prefix: pfx, Identity: getStorageIdentity(), EnableEtcdCompatibility: true}, m).(*backend)
	b.SetCurrentRevision(uint64(time.Now().UnixNano()))
	ctx := context.Background()
	require.NoError(t, b.EnsureEventLogStart(ctx))
	firstKey := path.Join(pfx, "first")
	first, err := b.Create(ctx, &proto.CreateRequest{Key: []byte(firstKey), Value: []byte("first")})
	require.NoError(t, err)
	secondKey := path.Join(pfx, "second")
	second, err := b.Create(ctx, &proto.CreateRequest{Key: []byte(secondKey), Value: []byte("second")})
	require.NoError(t, err)
	waitUntilRevisionEqualOrTimeout(b, second.Header.Revision)

	batch := kv.BeginBatchWrite()
	batch.Del(b.ks.EncodeEventLogKey(first.Header.Revision, []byte(firstKey)))
	require.NoError(t, batch.Commit(ctx))

	events, served, err := b.eventLogWatchEvents(ctx, pfx, first.Header.Revision, second.Header.Revision)
	require.ErrorIs(t, err, ErrInvalidMVCCMetadata)
	require.ErrorContains(t, err, "transaction witness mismatch")
	require.False(t, served)
	require.Empty(t, events)
	require.Contains(t, m.snapshot(), compactMetricRecord{
		kind: "counter", name: "watch.event_log.corruption", value: 1,
		tags: []metrics.T{metrics.Tag("kind", eventLogCorruptionWitnessMismatch)},
	}, "a trusted witness mismatch must increment confirmed corruption telemetry")
	alarms, alarmErr := b.CorruptAlarms(ctx)
	require.NoError(t, alarmErr)
	require.Equal(t, []uint64{b.localAlarmMemberID()}, alarms,
		"a trusted durable witness mismatch must persist CORRUPT, not remain a request-local error")
	_, writeErr := b.Create(ctx, &proto.CreateRequest{Key: []byte(path.Join(pfx, "blocked")), Value: []byte("blocked")})
	require.ErrorIs(t, writeErr, ErrCorruptAlarmActive)
	removed, disarmErr := b.DisarmCorrupt(ctx, b.localAlarmMemberID())
	require.ErrorIs(t, disarmErr, ErrTxnWitnessCorrupt)
	require.False(t, removed, "unrepaired durable witness evidence must keep writes fenced")

	require.NoError(t, b.EnsureEventLogStart(ctx))
	events, served, err = b.eventLogWatchEvents(ctx, pfx, first.Header.Revision, second.Header.Revision)
	require.NoError(t, err)
	require.False(t, served)
	require.Empty(t, events)
}

func TestValidateEventLogEntries(t *testing.T) {
	create := func(rev uint64, key string, ordered bool, sub, total uint32) eventLogPending {
		return eventLogPending{verb: proto.Event_CREATE, rev: rev, userKey: []byte(key), ordered: ordered, sub: sub, total: total}
	}
	put := func(rev, prev uint64, key string, ordered bool, sub, total uint32) eventLogPending {
		return eventLogPending{verb: proto.Event_PUT, rev: rev, prevRev: prev, userKey: []byte(key), ordered: ordered, sub: sub, total: total}
	}
	tests := []struct {
		name    string
		entries []eventLogPending
		wantErr string
	}{
		{name: "legacy", entries: []eventLogPending{create(2, "a", false, 0, 0)}},
		{name: "ordered", entries: []eventLogPending{create(3, "a", true, 0, 2), put(3, 2, "b", true, 1, 2)}},
		{name: "zero revision", entries: []eventLogPending{create(0, "a", false, 0, 0)}, wantErr: "revision zero"},
		{name: "mixed formats", entries: []eventLogPending{create(4, "a", false, 0, 0), put(4, 3, "b", true, 1, 2)}, wantErr: "mixes ordered and legacy"},
		{name: "subrevision gap", entries: []eventLogPending{create(5, "a", true, 0, 2), put(5, 4, "b", true, 2, 2)}, wantErr: "subrevision=2 total=2"},
		{name: "inconsistent total", entries: []eventLogPending{create(6, "a", true, 0, 3), put(6, 5, "b", true, 1, 3)}, wantErr: "total=3"},
		{name: "create previous revision", entries: []eventLogPending{{verb: proto.Event_CREATE, rev: 7, prevRev: 6, userKey: []byte("a")}}, wantErr: "CREATE event revision 7 has previous revision 6"},
		{name: "put future previous revision", entries: []eventLogPending{put(8, 8, "a", false, 0, 0)}, wantErr: "invalid previous revision 8"},
		{name: "empty key", entries: []eventLogPending{create(9, "", false, 0, 0)}, wantErr: "contains an empty key"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := validateEventLogEntries(test.entries)
			if test.wantErr == "" {
				require.NoError(t, err)
				return
			}
			require.ErrorContains(t, err, test.wantErr)
		})
	}
}
