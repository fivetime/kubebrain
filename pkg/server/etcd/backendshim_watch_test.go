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
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/api/v3/mvccpb"

	proto "github.com/kubewharf/kubebrain-client/api/v2rpc"

	"github.com/kubewharf/kubebrain/pkg/backend"
	"github.com/kubewharf/kubebrain/pkg/metrics/mock"
	memkv "github.com/kubewharf/kubebrain/pkg/storage/memkv"
)

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
	shim := NewBackendShim(b, m).(*backendShim)
	ctx := context.Background()

	// Create a key to obtain a real inline-metadata (enveloped) value whose
	// create_revision is its creation revision.
	seedKey := []byte("/registry/seed")
	cr, err := b.Create(ctx, &proto.CreateRequest{Key: seedKey, Value: []byte("v")})
	require.NoError(t, err)
	createRev := int64(cr.Header.Revision)
	getResp, err := b.Get(ctx, &proto.GetRequest{Key: seedKey})
	require.NoError(t, err)
	require.NotNil(t, getResp.Kv)
	enveloped := getResp.Kv.Value
	require.NotEqual(t, string(enveloped), "v", "value must carry an inline-metadata envelope")

	// Build a PUT (update) event for a key with NO previous version, so
	// cachedPreviousEtcdKv returns nil, at a mod revision distinct from the inline
	// create revision.
	modRev := createRev + 1000
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
