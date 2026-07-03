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
	"sync/atomic"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"

	proto "github.com/kubewharf/kubebrain-client/api/v2rpc"

	"github.com/kubewharf/kubebrain/pkg/metrics/mock"
	"github.com/kubewharf/kubebrain/pkg/storage"
	imemkv "github.com/kubewharf/kubebrain/pkg/storage/memkv"
)

// countingGetKV counts storage Get calls on a specific key.
type countingGetKV struct {
	storage.KvStorage
	matchKey []byte
	gets     int64
}

func (c *countingGetKV) Get(ctx context.Context, key []byte) ([]byte, error) {
	if bytes.Equal(key, c.matchKey) {
		atomic.AddInt64(&c.gets, 1)
	}
	return c.KvStorage.Get(ctx, key)
}

// TestGetCompactRevisionCachesAndUpdatesEagerly pins #48: GetCompactRevision must
// serve from an in-memory cache (no storage Get per revisioned request within the
// TTL), the cache must reflect a compaction eagerly, and it must refresh after
// the TTL.
func TestGetCompactRevisionCachesAndUpdatesEagerly(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	m := mock.NewMinimalMetrics(ctrl)
	kv := &countingGetKV{KvStorage: imemkv.NewKvStorage()}
	defer func() { require.NoError(t, kv.Close()) }()
	b := NewBackend(kv, Config{Prefix: prefix, Identity: getStorageIdentity(), EnableEtcdCompatibility: true}, m).(*backend)
	kv.matchKey = getCompactKey(prefix)
	b.SetCurrentRevision(uint64(time.Now().UnixNano()))
	ctx := context.Background()

	// Seed some data and compact.
	cr, err := b.Create(ctx, &proto.CreateRequest{Key: []byte(prefix + "/a"), Value: []byte("v")})
	require.NoError(t, err)
	waitCommitted(t, b, cr.Header.Revision)
	resp, err := b.Compact(ctx, cr.Header.Revision)
	require.NoError(t, err)
	compactedRev := resp.Header.Revision
	require.NotZero(t, compactedRev)

	// The compaction updated the cache eagerly, so GetCompactRevision returns the
	// new value with no extra storage read.
	before := atomic.LoadInt64(&kv.gets)
	for i := 0; i < 10; i++ {
		got, err := b.GetCompactRevision(ctx)
		require.NoError(t, err)
		require.Equal(t, compactedRev, got)
	}
	require.Equal(t, before, atomic.LoadInt64(&kv.gets),
		"GetCompactRevision must be served from cache within the TTL, doing no storage Get")

	// After the TTL it refreshes from storage exactly once for a burst of reads.
	time.Sleep(compactRevCacheTTL + 50*time.Millisecond)
	got, err := b.GetCompactRevision(ctx)
	require.NoError(t, err)
	require.Equal(t, compactedRev, got)
	require.Equal(t, before+1, atomic.LoadInt64(&kv.gets), "one refresh after TTL expiry")
}
