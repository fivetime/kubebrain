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
	"testing"

	"github.com/golang/mock/gomock"
	proto "github.com/kubewharf/kubebrain-client/api/v2rpc"
	"github.com/stretchr/testify/require"

	"github.com/kubewharf/kubebrain/pkg/metrics/mock"
	"github.com/kubewharf/kubebrain/pkg/storage/memkv"
)

func TestDecodedRangeReconcilesAncestorWhoseTombstoneIsPastRawEnd(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := memkv.NewKvStorage()
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	b := NewBackend(store, Config{
		Prefix: "/kubebrain/range-ancestor", Identity: getStorageIdentity(), EnableEtcdCompatibility: true,
	}, mock.NewMinimalMetrics(ctrl)).(*backend)
	b.SetCurrentRevision(0x78ffffffffffffff)
	ctx := context.Background()
	ancestor := []byte("a")
	child := []byte("a$x")
	end := []byte("a$z")

	created, err := b.Create(ctx, &proto.CreateRequest{Key: ancestor, Value: []byte("ancestor")})
	require.NoError(t, err)
	_, err = b.Create(ctx, &proto.CreateRequest{Key: child, Value: []byte("child")})
	require.NoError(t, err)
	// The create revision begins with 0x79 and lies below raw end's 0x7a,
	// while the tombstone begins with 0x7b and lies beyond it.
	b.SetCurrentRevision(0x7affffffffffffff)
	deleted, err := b.Delete(ctx, &proto.DeleteRequest{Key: ancestor})
	require.NoError(t, err)
	require.True(t, deleted.Succeeded)

	latest, err := b.List(ctx, &proto.RangeRequest{Key: ancestor, End: end})
	require.NoError(t, err)
	require.Equal(t, [][]byte{child}, [][]byte{latest.Kvs[0].Key})
	count, err := b.Count(ctx, &proto.CountRequest{Key: ancestor, End: end})
	require.NoError(t, err)
	require.Equal(t, uint64(1), count.Count)

	historical, err := b.List(ctx, &proto.RangeRequest{Key: ancestor, End: end, Revision: created.Header.Revision})
	require.NoError(t, err)
	require.Equal(t, [][]byte{ancestor}, [][]byte{historical.Kvs[0].Key})
}
