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
	"errors"
	"net/http"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"

	proto "github.com/kubewharf/kubebrain-client/api/v2rpc"

	"github.com/kubewharf/kubebrain/pkg/metrics"
	"github.com/kubewharf/kubebrain/pkg/storage"
	imemkv "github.com/kubewharf/kubebrain/pkg/storage/memkv"
)

// recordCounters implements metrics.Metrics and sums emitted counters.
type recordCounters struct {
	mu sync.Mutex
	c  map[string]float64
	g  map[string]float64
}

func newRecordCounters() *recordCounters {
	return &recordCounters{c: map[string]float64{}, g: map[string]float64{}}
}

func (r *recordCounters) GetGrpcServerOption() []grpc.ServerOption { return nil }
func (r *recordCounters) GetHttpHandlers() map[string]http.Handler { return nil }
func (r *recordCounters) EmitGauge(name string, value interface{}, _ ...metrics.T) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	switch x := value.(type) {
	case int:
		r.g[name] = float64(x)
	case int64:
		r.g[name] = float64(x)
	case float64:
		r.g[name] = x
	}
	return nil
}
func (r *recordCounters) EmitHistogram(string, interface{}, ...metrics.T) error { return nil }
func (r *recordCounters) EmitCounter(name string, value interface{}, _ ...metrics.T) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	switch x := value.(type) {
	case int:
		r.c[name] += float64(x)
	case int64:
		r.c[name] += float64(x)
	case float64:
		r.c[name] += x
	}
	return nil
}
func (r *recordCounters) get(name string) float64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.c[name]
}
func (r *recordCounters) gauge(name string) float64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.g[name]
}

// armedFailPartitionsKV fails GetPartitions once armed, so a compaction scan
// fails fast (before the per-worker retry loop).
type armedFailPartitionsKV struct {
	storage.KvStorage
	armed int32
}

func (a *armedFailPartitionsKV) GetPartitions(ctx context.Context, start, end []byte) ([]storage.Partition, error) {
	if atomic.LoadInt32(&a.armed) == 1 {
		return nil, errors.New("injected partitions failure")
	}
	return a.KvStorage.GetPartitions(ctx, start, end)
}

type cancelFirstPartitionsKV struct {
	storage.KvStorage
	first   int32
	entered chan struct{}
}

func (c *cancelFirstPartitionsKV) GetPartitions(ctx context.Context, start, end []byte) ([]storage.Partition, error) {
	if atomic.CompareAndSwapInt32(&c.first, 0, 1) {
		close(c.entered)
		<-ctx.Done()
		return nil, ctx.Err()
	}
	return c.KvStorage.GetPartitions(ctx, start, end)
}

// TestCompactSurfacesScanError pins #71 and the recovery contract:
// Physical=true returns a scan failure even though the logical watermark is
// already durable, then the client-independent compactor resumes that target.
func TestCompactSurfacesScanError(t *testing.T) {
	rec := newRecordCounters()
	fkv := &armedFailPartitionsKV{KvStorage: imemkv.NewKvStorage()}
	defer func() { require.NoError(t, fkv.Close()) }()
	b := NewBackend(fkv, Config{Prefix: prefix, Identity: getStorageIdentity(), EnableEtcdCompatibility: true}, rec).(*backend)
	b.SetCurrentRevision(uint64(time.Now().UnixNano()))
	ctx := context.Background()

	cr, err := b.Create(ctx, &proto.CreateRequest{Key: []byte(prefix + "/a"), Value: []byte("v")})
	require.NoError(t, err)
	waitCommitted(t, b, cr.Header.Revision)

	// Arm the scan failure, then compact synchronously.
	atomic.StoreInt32(&fkv.armed, 1)
	resp, err := b.Compact(ctx, cr.Header.Revision)
	require.ErrorContains(t, err, "injected partitions failure")
	require.NotZero(t, resp.Header.Revision)
	watermark, loadErr := b.GetCompactRevisionFresh(ctx)
	require.NoError(t, loadErr)
	require.GreaterOrEqual(t, watermark, uint64(cr.Header.Revision),
		"logical watermark remains monotonic when physical GC fails")

	require.GreaterOrEqual(t, rec.get("backend.compact.scan.err"), 1.0,
		"a failed physical GC scan must be surfaced via a metric, not swallowed")

	atomic.StoreInt32(&fkv.armed, 0)
	require.Eventually(t, func() bool {
		return atomic.LoadUint64(&b.compactDoneRev) >= uint64(cr.Header.Revision)
	}, 5*time.Second, 5*time.Millisecond,
		"a durable logical watermark must be physically resumed after the request scan fails")
}

func TestCompactCancellationResumesPhysicalScanInBackground(t *testing.T) {
	rec := newRecordCounters()
	fkv := &cancelFirstPartitionsKV{
		KvStorage: imemkv.NewKvStorage(),
		entered:   make(chan struct{}),
	}
	defer func() { require.NoError(t, fkv.Close()) }()
	b := NewBackend(fkv, Config{Prefix: prefix, Identity: getStorageIdentity(), EnableEtcdCompatibility: true}, rec).(*backend)
	b.SetCurrentRevision(uint64(time.Now().UnixNano()))

	cr, err := b.Create(context.Background(), &proto.CreateRequest{Key: []byte(prefix + "/cancel"), Value: []byte("v")})
	require.NoError(t, err)
	waitCommitted(t, b, cr.Header.Revision)

	ctx, cancel := context.WithCancel(context.Background())
	result := make(chan error, 1)
	go func() {
		_, compactErr := b.Compact(ctx, cr.Header.Revision)
		result <- compactErr
	}()
	<-fkv.entered
	cancel()
	require.ErrorIs(t, <-result, context.Canceled)

	require.Eventually(t, func() bool {
		return atomic.LoadUint64(&b.compactDoneRev) >= uint64(cr.Header.Revision)
	}, 5*time.Second, 5*time.Millisecond,
		"physical GC must continue independently after the requesting client disconnects")
}
