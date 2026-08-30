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

package etcd

import (
	"context"
	"encoding/binary"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/api/v3/mvccpb"

	proto "github.com/kubewharf/kubebrain-client/api/v2rpc"

	"github.com/kubewharf/kubebrain/pkg/backend"
	"github.com/kubewharf/kubebrain/pkg/metrics/mock"
	memkv "github.com/kubewharf/kubebrain/pkg/storage/memkv"
)

type nilStreamBackend struct{ backend.Backend }

func (b *nilStreamBackend) RangeStream(context.Context, []byte, []byte, uint64) (<-chan *proto.StreamRangeResponse, error) {
	return nil, nil
}

func (b *nilStreamBackend) SnapshotStream(context.Context, uint64) (<-chan *proto.StreamRangeResponse, error) {
	return nil, nil
}

func (b *nilStreamBackend) SnapshotHistoryStream(context.Context, uint64) (<-chan backend.SnapshotHistoryChunk, error) {
	return nil, nil
}

func (b *nilStreamBackend) Watch(context.Context, string, uint64) (<-chan []*proto.Event, error) {
	return nil, nil
}

func TestBackendShimRejectsNilBackendStreams(t *testing.T) {
	rec := &recordingMetrics{}
	initWatchBackendIntegrityMetrics(rec)
	shim := &backendShim{backend: &nilStreamBackend{}, metricCli: rec}

	rangeCh, err := shim.RangeStreamChan(context.Background(), []byte("a"), []byte("z"), 1)
	require.ErrorIs(t, err, errNilBackendStream)
	require.Nil(t, rangeCh)
	snapshotCh, err := shim.SnapshotStreamChan(context.Background(), 1)
	require.ErrorIs(t, err, errNilBackendStream)
	require.Nil(t, snapshotCh)
	historyCh, err := shim.SnapshotHistoryStreamChan(context.Background(), 1)
	require.ErrorIs(t, err, errNilBackendStream)
	require.Nil(t, historyCh)
	watchCh, err := shim.Watch(context.Background(), "a", 1)
	require.ErrorIs(t, err, errNilWatchGeneration)
	require.Nil(t, watchCh)
	require.Equal(t, []interface{}{0, 1}, recordedWatchBackendIntegrityValues(rec, "invalid_result"))
}

type scriptedBackendWatch struct {
	backend.Backend
	results <-chan []*proto.Event
}

func (b *scriptedBackendWatch) Watch(context.Context, string, uint64) (<-chan []*proto.Event, error) {
	return b.results, nil
}

func TestBackendShimWatchFailsBatchWhenAnyEventCannotBeTranslated(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()
	shim := server.backend.(*backendShim)
	input := make(chan []*proto.Event, 1)
	input <- []*proto.Event{
		{
			Type:     proto.Event_CREATE,
			Revision: 7,
			Kv: &proto.KeyValue{
				Key: []byte("/watch/must-not-partially-publish"), Value: []byte("v"), Revision: 7,
			},
		},
		nil,
	}
	close(input)
	shim.backend = &scriptedBackendWatch{Backend: shim.backend, results: input}

	results, err := shim.Watch(context.Background(), "/watch/", 1)
	require.NoError(t, err)
	result, ok := <-results
	require.True(t, ok)
	require.Error(t, result.Err)
	require.ErrorContains(t, result.Err, "invalid nil watch event")
	require.ErrorContains(t, result.Err, "event 1")
	require.Empty(t, result.Events)
	_, ok = <-results
	require.False(t, ok)
}

func TestBackendShimWatchRangeFiltersBeforeConversionAndPreservesBatchRevision(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()
	shim := server.backend.(*backendShim)
	rec := &recordingMetrics{}
	initWatchRangePrefilterMetric(rec)
	shim.metricCli = rec
	input := make(chan []*proto.Event, 2)
	input <- []*proto.Event{
		{
			Type: proto.Event_CREATE, Revision: 7,
			Kv: &proto.KeyValue{Key: []byte("/watch/target"), Value: []byte("value"), Revision: 7},
		},
		{
			// This is structurally valid but its inline envelope cannot be
			// decoded. Exact-range filtering must discard it before conversion.
			Type: proto.Event_CREATE, Revision: 9,
			Kv: &proto.KeyValue{Key: []byte("/watch/target-child"), Value: []byte{0, 'k', 'b', 2}, Revision: 9},
		},
	}
	input <- []*proto.Event{{
		Type: proto.Event_CREATE, Revision: 11,
		Kv: &proto.KeyValue{Key: []byte("/watch/unrelated"), Value: []byte("value"), Revision: 11},
	}}
	close(input)
	shim.backend = &scriptedBackendWatch{Backend: shim.backend, results: input}

	results, err := shim.WatchRange(context.Background(), "/watch/target", []byte("/watch/target"), nil, 1)
	require.NoError(t, err)

	matching, ok := <-results
	require.True(t, ok)
	require.NoError(t, matching.Err)
	require.Equal(t, uint64(9), matching.Revision, "watermark must cover the complete source batch")
	require.Len(t, matching.Events, 1)
	require.Equal(t, []byte("/watch/target"), matching.Events[0].Kv.Key)

	filtered, ok := <-results
	require.True(t, ok)
	require.NoError(t, filtered.Err)
	require.Equal(t, uint64(11), filtered.Revision, "an all-filtered batch must still advance the watch watermark")
	require.Empty(t, filtered.Events)
	_, ok = <-results
	require.False(t, ok)
	require.Equal(t, []interface{}{int64(0), 1, 1}, recordedCounterValues(rec, "watch.range_prefilter.dropped"))
}

func TestFilterBackendWatchEventsByRangeMatchesEtcdIntervals(t *testing.T) {
	events := []*proto.Event{
		{Kv: &proto.KeyValue{Key: []byte("a")}},
		{Kv: &proto.KeyValue{Key: []byte("ab")}},
		{Kv: &proto.KeyValue{Key: []byte("b")}},
		{Kv: &proto.KeyValue{Key: []byte("c")}},
	}
	keys := func(filtered []*proto.Event) []string {
		out := make([]string, 0, len(filtered))
		for _, event := range filtered {
			out = append(out, string(event.Kv.Key))
		}
		return out
	}

	require.Equal(t, []string{"b"}, keys(filterBackendWatchEventsByRange(events, []byte("b"), nil)))
	require.Equal(t, []string{"ab", "b"}, keys(filterBackendWatchEventsByRange(events, []byte("ab"), []byte("c"))))
	require.Equal(t, []string{"b", "c"}, keys(filterBackendWatchEventsByRange(events, []byte("b"), []byte{})))
	require.Equal(t, []string{"a", "ab", "b", "c"}, keys(events), "filtering must not mutate a shared source batch")
	all := filterBackendWatchEventsByRange(events, []byte("a"), []byte{})
	require.Same(t, events[0], all[0])
	require.Equal(t, &events[0], &all[0], "the all-matching hot path must reuse the source slice")

	fromKey := cloneWatchRangeBoundary([]byte{})
	require.NotNil(t, fromKey, "cloning must preserve the non-nil empty from-key sentinel")
	require.Nil(t, cloneWatchRangeBoundary(nil), "cloning must preserve exact-key nil")
}

func TestWatchEventRejectsMalformedInlineValue(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()
	shim := server.backend.(*backendShim)

	event, err := shim.watchEventToEtcdEvent(context.Background(), &proto.Event{
		Type:     proto.Event_CREATE,
		Revision: 7,
		Kv: &proto.KeyValue{
			Key: []byte("/watch/corrupt-inline"), Value: []byte{0, 'k', 'b', 2}, Revision: 7,
		},
	})
	require.ErrorIs(t, err, backend.ErrInvalidMVCCMetadata)
	require.ErrorContains(t, err, "inline value metadata v2 length 4 is shorter than 28")
	require.Nil(t, event)
}

func TestWatchEventRejectsImpossibleInlineLifecycle(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()
	shim := server.backend.(*backendShim)
	value := make([]byte, 20+len("value"))
	copy(value, []byte{0, 'k', 'b', 3})
	binary.BigEndian.PutUint64(value[4:], 8)
	binary.BigEndian.PutUint64(value[12:], 1)
	copy(value[20:], "value")

	event, err := shim.watchEventToEtcdEvent(context.Background(), &proto.Event{
		Type:     proto.Event_CREATE,
		Revision: 7,
		Kv: &proto.KeyValue{
			Key: []byte("/watch/future-create-revision"), Value: value, Revision: 7,
		},
	})
	require.ErrorIs(t, err, backend.ErrInvalidMVCCMetadata)
	require.ErrorContains(t, err, "create revision 8 exceeds mod revision 7")
	require.Nil(t, event)
}

func TestWatchEventRejectsPutRevisionDisagreement(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()
	shim := server.backend.(*backendShim)
	value := make([]byte, 20+len("value"))
	copy(value, []byte{0, 'k', 'b', 3})
	binary.BigEndian.PutUint64(value[4:], 7)
	binary.BigEndian.PutUint64(value[12:], 2)
	copy(value[20:], "value")

	for _, eventType := range []proto.Event_EventType{proto.Event_CREATE, proto.Event_PUT} {
		event, err := shim.watchEventToEtcdEvent(context.Background(), &proto.Event{
			Type:     eventType,
			Revision: 9,
			Kv: &proto.KeyValue{
				Key: []byte("/watch/revision-disagreement"), Value: value, Revision: 8,
			},
		})
		require.ErrorContains(t, err, "event revision 9 disagrees with key revision 8")
		require.Nil(t, event)
	}

	event, err := shim.watchEventToEtcdEvent(context.Background(), &proto.Event{
		Type: proto.Event_PUT, Revision: 9,
		Kv: &proto.KeyValue{Key: []byte("/watch/zero-key-revision"), Value: value},
	})
	require.ErrorContains(t, err, "PUT event has zero key revision")
	require.Nil(t, event)
}

// TestWatchPutEventKeepsInlineCreateRevisionWhenPrevKvMissing pins #52: a PUT
// (update) watch event must keep the create_revision carried inline in its value
// even when the previous-version lookup returns nil, so the update is not
// misreported as a create. Previously the nil-prevKv path overwrote
// create_revision with mod_revision (IsCreate would wrongly be true).
func TestWatchPutEventKeepsInlineCreateRevisionWhenPrevKvMissing(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	m := mock.NewMinimalMetrics(ctrl)
	kv := memkv.NewKvStorage()
	defer func() { require.NoError(t, kv.Close()) }()
	b := backend.NewBackend(kv, backend.Config{Identity: "test", EnableEtcdCompatibility: true}, m)
	b.SetCurrentRevision(uint64(time.Now().UnixNano()))
	shim := NewBackendShim(b, m).(*backendShim)
	ctx := context.Background()

	// Create and update a key to obtain a real version-2 inline envelope whose
	// create_revision remains its creation revision.
	seedKey := []byte("/registry/seed")
	cr, err := b.Create(ctx, &proto.CreateRequest{Key: seedKey, Value: []byte("v")})
	require.NoError(t, err)
	createRev := int64(cr.Header.Revision)
	updated, err := b.Update(ctx, &proto.UpdateRequest{Kv: &proto.KeyValue{
		Key: seedKey, Value: []byte("v2"), Revision: cr.Header.Revision,
	}})
	require.NoError(t, err)
	getResp, err := b.Get(ctx, &proto.GetRequest{Key: seedKey})
	require.NoError(t, err)
	require.NotNil(t, getResp.Kv)
	enveloped := getResp.Kv.Value
	require.NotEqual(t, string(enveloped), "v", "value must carry an inline-metadata envelope")

	// Build a PUT (update) event for a key with NO previous version, so
	// cachedPreviousEtcdKv returns nil, at a mod revision distinct from the inline
	// create revision.
	modRev := int64(updated.Header.Revision) + 1000
	ev, err := shim.watchEventToEtcdEvent(ctx, &proto.Event{
		Type:     proto.Event_PUT,
		Revision: uint64(modRev),
		Kv: &proto.KeyValue{
			Key:      []byte("/registry/no-prev"),
			Value:    enveloped,
			Revision: uint64(modRev),
		},
	})
	require.NoError(t, err)
	require.Equal(t, mvccpb.PUT, ev.Type)
	require.Equal(t, modRev, ev.Kv.ModRevision)
	require.Equal(t, createRev, ev.Kv.CreateRevision,
		"update must keep the inline create_revision, not mod_revision")
	require.NotEqual(t, ev.Kv.ModRevision, ev.Kv.CreateRevision,
		"an update must not look like a create (IsCreate must be false)")
	require.Nil(t, ev.PrevKv, "prev-kv lookup failed here, so PrevKv is nil (but create_revision stays correct)")
}

func TestWatchDeleteEventKeepsGenerationMetadataOnlyInPrevKV(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	m := mock.NewMinimalMetrics(ctrl)
	kv := memkv.NewKvStorage()
	defer func() { require.NoError(t, kv.Close()) }()
	b := backend.NewBackend(kv, backend.Config{Identity: "test", EnableEtcdCompatibility: true}, m)
	b.SetCurrentRevision(uint64(time.Now().UnixNano()))
	shim := NewBackendShim(b, m).(*backendShim)

	key := []byte("/lease/expired")
	create, err := b.Create(context.Background(), &proto.CreateRequest{Key: key, Value: []byte("old"), Lease: 123})
	require.NoError(t, err)
	stored, err := b.Get(context.Background(), &proto.GetRequest{Key: key})
	require.NoError(t, err)
	deleteRevision := create.Header.Revision + 1
	event, err := shim.watchEventToEtcdEvent(context.Background(), &proto.Event{
		Type:     proto.Event_DELETE,
		Revision: deleteRevision,
		Kv: &proto.KeyValue{
			Key:      key,
			Value:    stored.Kv.Value,
			Revision: create.Header.Revision,
		},
	})
	require.NoError(t, err)
	require.Equal(t, mvccpb.DELETE, event.Type)
	require.Equal(t, int64(deleteRevision), event.Kv.ModRevision)
	require.Zero(t, event.Kv.CreateRevision)
	require.Zero(t, event.Kv.Version)
	require.Zero(t, event.Kv.Lease)
	require.Empty(t, event.Kv.Value)
	require.NotNil(t, event.PrevKv)
	require.Equal(t, int64(create.Header.Revision), event.PrevKv.CreateRevision)
	require.Equal(t, int64(create.Header.Revision), event.PrevKv.ModRevision)
	require.Equal(t, int64(1), event.PrevKv.Version)
	require.Equal(t, int64(123), event.PrevKv.Lease)
	require.Equal(t, []byte("old"), event.PrevKv.Value)
}

func TestWatchDeleteEventRejectsInvalidRevisionProvenance(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()
	shim := server.backend.(*backendShim)

	for _, test := range []struct {
		name          string
		eventRevision uint64
		prevRevision  uint64
		want          string
	}{
		{name: "zero deletion", prevRevision: 7, want: "zero deletion revision"},
		{name: "zero previous", eventRevision: 8, want: "zero previous-value revision"},
		{name: "same revision", eventRevision: 8, prevRevision: 8, want: "revision 8 does not follow previous-value revision 8"},
		{name: "previous from future", eventRevision: 8, prevRevision: 9, want: "revision 8 does not follow previous-value revision 9"},
	} {
		t.Run(test.name, func(t *testing.T) {
			event, err := shim.watchEventToEtcdEvent(context.Background(), &proto.Event{
				Type: proto.Event_DELETE, Revision: test.eventRevision,
				Kv: &proto.KeyValue{Key: []byte("/watch/invalid-delete"), Value: []byte("old"), Revision: test.prevRevision},
			})
			require.ErrorContains(t, err, test.want)
			require.Nil(t, event)
		})
	}
}
