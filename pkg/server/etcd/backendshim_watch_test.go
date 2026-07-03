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
