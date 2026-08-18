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
	"go.etcd.io/etcd/api/v3/mvccpb"

	proto "github.com/kubewharf/kubebrain-client/api/v2rpc"

	"github.com/kubewharf/kubebrain/pkg/backend"
	"github.com/kubewharf/kubebrain/pkg/metrics"
	"github.com/kubewharf/kubebrain/pkg/metrics/mock"
	"github.com/kubewharf/kubebrain/pkg/storage"
	memkv "github.com/kubewharf/kubebrain/pkg/storage/memkv"
)

// flakyKV wraps a KvStorage and fails reads while failing.Load() is true.
type flakyKV struct {
	storage.KvStorage
	failing atomic.Bool
}

type blockingMetadataBackend struct {
	backend.Backend
	entered chan struct{}
	release chan struct{}
	calls   atomic.Int32
}

func (b *blockingMetadataBackend) GetEtcdMetadata(ctx context.Context, _ []byte, _ uint64) (backend.EtcdMetadata, error) {
	b.calls.Add(1)
	select {
	case b.entered <- struct{}{}:
	default:
	}
	select {
	case <-ctx.Done():
		return backend.EtcdMetadata{}, ctx.Err()
	case <-b.release:
		return backend.EtcdMetadata{CreateRevision: 7, Version: 3}, nil
	}
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
	return newPrevKvTestShimWithMetrics(t, m)
}

func newPrevKvTestShimWithMetrics(t *testing.T, metricCli metrics.Metrics) (*backendShim, backend.Backend, *flakyKV) {
	kv := &flakyKV{KvStorage: memkv.NewKvStorage()}
	t.Cleanup(func() { require.NoError(t, kv.Close()) })
	b := backend.NewBackend(kv, backend.Config{Identity: "test", EnableEtcdCompatibility: true}, metricCli)
	b.SetCurrentRevision(uint64(time.Now().UnixNano()))
	return NewBackendShim(b, metricCli).(*backendShim), b, kv
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
	rec := &recordingMetrics{}
	shim, b, kv := newPrevKvTestShimWithMetrics(t, rec)
	key, updateRev := seedUpdatedKey(t, b)

	old := prevKvRetryBudget
	prevKvRetryBudget = 80 * time.Millisecond
	defer func() { prevKvRetryBudget = old }()

	// Exhaust the budget: uncertain nil.
	kv.failing.Store(true)
	prev := shim.cachedPreviousEtcdKv(context.Background(), key, updateRev, 0, 0)
	require.Nil(t, prev, "budget exhausted under persistent failure returns nil")
	var budgetValues []interface{}
	rec.mu.Lock()
	for _, counter := range rec.counters {
		if counter.name == "watch.prev_kv.budget_exhausted" {
			budgetValues = append(budgetValues, counter.value)
		}
	}
	rec.mu.Unlock()
	require.Equal(t, []interface{}{int64(0), 1}, budgetValues,
		"the shim must publish an authoritative zero before recording exhaustion")

	// Storage recovers: the SAME (key,revision) must now resolve — the
	// uncertain nil must not have been cached.
	kv.failing.Store(false)
	prev = shim.cachedPreviousEtcdKv(context.Background(), key, updateRev, 0, 0)
	require.NotNil(t, prev, "uncertain nil must not be cached; recovered lookup must return the previous version")
	require.Equal(t, updateRev, uint64(prev.ModRevision)+uint64(updateRev-uint64(prev.ModRevision)), "sanity")
}

// TestPrevKvLookupStopsWithShim pins the component lifetime boundary: the
// shared lookup outlives any one watch, but it must not outlive its server.
func TestPrevKvLookupStopsWithShim(t *testing.T) {
	shim, b, kv := newPrevKvTestShim(t)
	key, updateRev := seedUpdatedKey(t, b)
	kv.failing.Store(true)

	done := make(chan struct{})
	go func() {
		defer close(done)
		shim.cachedPreviousEtcdKv(context.Background(), key, updateRev, 0, 0)
	}()
	time.Sleep(30 * time.Millisecond) // allow the retry loop to enter storage
	shim.Close()

	select {
	case <-done:
	case <-time.After(250 * time.Millisecond):
		t.Fatal("shared PrevKV lookup survived backend shim shutdown")
	}
}

func TestMetadataSingleflightIsolatesCallerCancellation(t *testing.T) {
	shim, rawBackend, _ := newPrevKvTestShim(t)
	probe := &blockingMetadataBackend{
		Backend: rawBackend,
		entered: make(chan struct{}, 1),
		release: make(chan struct{}),
	}
	shim.backend = probe

	firstCtx, cancelFirst := context.WithCancel(context.Background())
	firstDone := make(chan error, 1)
	go func() {
		_, err := shim.cachedMetadata(firstCtx, []byte("legacy"), 11)
		firstDone <- err
	}()
	<-probe.entered

	secondDone := make(chan error, 1)
	go func() {
		_, err := shim.cachedMetadata(context.Background(), []byte("legacy"), 11)
		secondDone <- err
	}()
	time.Sleep(20 * time.Millisecond) // let the second caller join the flight
	cancelFirst()
	require.ErrorIs(t, <-firstDone, context.Canceled)
	require.Equal(t, int32(1), probe.calls.Load())

	close(probe.release)
	require.NoError(t, <-secondDone, "one watch cancellation must not fail another watch sharing metadata")
}

func TestPrevKvSingleflightIsolatesCallerCancellation(t *testing.T) {
	shim, b, kv := newPrevKvTestShim(t)
	key, updateRev := seedUpdatedKey(t, b)
	kv.failing.Store(true)

	firstCtx, cancelFirst := context.WithCancel(context.Background())
	firstDone := make(chan *mvccpb.KeyValue, 1)
	go func() { firstDone <- shim.cachedPreviousEtcdKv(firstCtx, key, updateRev, 0, 0) }()
	time.Sleep(30 * time.Millisecond) // enter the shared retry loop

	secondDone := make(chan *mvccpb.KeyValue, 1)
	go func() { secondDone <- shim.cachedPreviousEtcdKv(context.Background(), key, updateRev, 0, 0) }()
	cancelFirst()
	require.Nil(t, <-firstDone)

	kv.failing.Store(false)
	select {
	case prev := <-secondDone:
		require.NotNil(t, prev, "one watch cancellation must not abort another watch's shared PrevKV lookup")
	case <-time.After(time.Second):
		t.Fatal("surviving caller did not receive the recovered shared PrevKV lookup")
	}
}
