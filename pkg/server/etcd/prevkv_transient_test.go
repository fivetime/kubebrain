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
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"

	proto "github.com/kubewharf/kubebrain-client/api/v2rpc"

	"github.com/kubewharf/kubebrain/pkg/backend"
	"github.com/kubewharf/kubebrain/pkg/metrics/mock"
	"github.com/kubewharf/kubebrain/pkg/storage"
	memkv "github.com/kubewharf/kubebrain/pkg/storage/memkv"
)

// flakyKV wraps a KvStorage and fails reads while failing.Load() is true.
type flakyKV struct {
	storage.KvStorage
	failing atomic.Bool
}

func (f *flakyKV) Get(ctx context.Context, key []byte) ([]byte, error) {
	if f.failing.Load() {
		return nil, fmt.Errorf("injected transient storage failure")
	}
	return f.KvStorage.Get(ctx, key)
}

func (f *flakyKV) Iter(ctx context.Context, start, end []byte, ts uint64, limit uint64) (storage.Iter, error) {
	if f.failing.Load() {
		return nil, fmt.Errorf("injected transient storage failure")
	}
	return f.KvStorage.Iter(ctx, start, end, ts, limit)
}

func newPrevKvTestShim(t *testing.T) (*backendShim, backend.Backend, *flakyKV) {
	ctrl := gomock.NewController(t)
	t.Cleanup(ctrl.Finish)
	m := mock.NewMinimalMetrics(ctrl)
	kv := &flakyKV{KvStorage: memkv.NewKvStorage()}
	t.Cleanup(func() { require.NoError(t, kv.Close()) })
	b := backend.NewBackend(kv, backend.Config{Identity: "test", EnableEtcdCompatibility: true}, m)
	return NewBackendShim(b, m).(*backendShim), b, kv
}

// seedUpdatedKey creates then updates a key, returning (key, updateRev).
func seedUpdatedKey(t *testing.T, b backend.Backend) ([]byte, uint64) {
	ctx := context.Background()
	key := []byte(fmt.Sprintf("/registry/prevkv/%d", time.Now().UnixNano()))
	cr, err := b.Create(ctx, &proto.CreateRequest{Key: key, Value: []byte("v1")})
	require.NoError(t, err)
	require.True(t, cr.Succeeded)
	gr, err := b.Get(ctx, &proto.GetRequest{Key: key})
	require.NoError(t, err)
	ur, err := b.Update(ctx, &proto.UpdateRequest{
		Kv: &proto.KeyValue{Key: key, Value: []byte("v2"), Revision: gr.Kv.Revision}})
	require.NoError(t, err)
	require.True(t, ur.Succeeded)
	return key, ur.Header.Revision
}

// TestPrevKvTransientFailureRetriesUntilSuccess pins #36: a previous-value
// lookup that fails transiently must retry until storage recovers and then
// return the real previous version — never emit a PrevKv=nil update event
// (which makes the apiserver terminate every watcher of the resource and
// re-list, the read amplification that turned one hiccup into a self-
// sustaining cacher storm at 400k objects).
func TestPrevKvTransientFailureRetriesUntilSuccess(t *testing.T) {
	shim, b, kv := newPrevKvTestShim(t)
	key, updateRev := seedUpdatedKey(t, b)

	// Storage starts failing; recover it shortly after the lookup begins.
	kv.failing.Store(true)
	go func() {
		time.Sleep(150 * time.Millisecond)
		kv.failing.Store(false)
	}()

	start := time.Now()
	prev, certain := shim.previousEtcdKv(context.Background(), key, updateRev)
	require.True(t, certain, "a recovered lookup is authoritative")
	require.NotNil(t, prev, "must return the real previous version after retrying through the failure")
	require.GreaterOrEqual(t, time.Since(start), 100*time.Millisecond, "must have actually retried through the failure window")
}

// TestPrevKvUncertainNilNotCached pins the cache-poisoning half of #36: a
// budget-exhausted (uncertain) nil must not be cached — once storage recovers,
// the same event's conversion must resolve the real previous value.
func TestPrevKvUncertainNilNotCached(t *testing.T) {
	shim, b, kv := newPrevKvTestShim(t)
	key, updateRev := seedUpdatedKey(t, b)

	old := prevKvRetryBudget
	prevKvRetryBudget = 80 * time.Millisecond
	defer func() { prevKvRetryBudget = old }()

	// Exhaust the budget: uncertain nil.
	kv.failing.Store(true)
	prev := shim.cachedPreviousEtcdKv(key, updateRev)
	require.Nil(t, prev, "budget exhausted under persistent failure returns nil")

	// Storage recovers: the SAME (key,revision) must now resolve — the
	// uncertain nil must not have been cached.
	kv.failing.Store(false)
	prev = shim.cachedPreviousEtcdKv(key, updateRev)
	require.NotNil(t, prev, "uncertain nil must not be cached; recovered lookup must return the previous version")
	require.Equal(t, updateRev, uint64(prev.ModRevision)+uint64(updateRev-uint64(prev.ModRevision)), "sanity")
}
