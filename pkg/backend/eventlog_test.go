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

	// Simulate leadership reacquisition moving the conservative watermark over
	// the latest transaction. The public history path must use the self-contained
	// exact-revision log and retain B,A operation order rather than key-sort A,B.
	require.NoError(t, b.EnsureEventLogStart(ctx))
	recovered, err := b.historyWatchEvents(ctx, pfx, mixedRev, mixedRev, mixedRev)
	require.NoError(t, err)
	require.Len(t, recovered, 2)
	require.Equal(t, []string{keyB, keyA}, []string{
		string(recovered[0].Kv.Key), string(recovered[1].Kv.Key),
	})
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
}
