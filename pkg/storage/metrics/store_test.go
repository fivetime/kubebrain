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

package metrics

import (
	"context"
	"fmt"
	"net/http"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"

	"github.com/kubewharf/kubebrain/pkg/metrics"
	imemkv "github.com/kubewharf/kubebrain/pkg/storage/memkv"
)

// recordMetrics records the summed value of each emitted counter.
type recordMetrics struct {
	mu       sync.Mutex
	counters map[string]float64
}

func (r *recordMetrics) GetGrpcServerOption() []grpc.ServerOption              { return nil }
func (r *recordMetrics) GetHttpHandlers() map[string]http.Handler              { return nil }
func (r *recordMetrics) EmitGauge(string, interface{}, ...metrics.T) error     { return nil }
func (r *recordMetrics) EmitHistogram(string, interface{}, ...metrics.T) error { return nil }

func (r *recordMetrics) EmitCounter(name string, value interface{}, _ ...metrics.T) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	var v float64
	switch x := value.(type) {
	case int:
		v = float64(x)
	case int64:
		v = float64(x)
	case float64:
		v = x
	}
	r.counters[name] += v
	return nil
}

func (r *recordMetrics) get(name string) float64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.counters[name]
}

// TestIterWrapperCountsFetchedRows pins #67: the storage.iter.fetch.success
// counter must equal the number of rows fetched, not the terminal EOF/cancel.
func TestIterWrapperCountsFetchedRows(t *testing.T) {
	rec := &recordMetrics{counters: map[string]float64{}}
	kv := imemkv.NewKvStorage()
	defer func() { require.NoError(t, kv.Close()) }()
	s := NewKvStorage(kv, rec)
	ctx := context.Background()

	const n = 5
	batch := s.BeginBatchWrite()
	for i := 0; i < n; i++ {
		batch.Put([]byte(fmt.Sprintf("k%02d", i)), []byte("v"), 0)
	}
	require.NoError(t, batch.Commit(ctx))

	it, err := s.Iter(ctx, []byte("k00"), []byte("k99"), 0, 0)
	require.NoError(t, err)
	rows := 0
	for it.Next(ctx) == nil {
		rows++
	}
	require.NoError(t, it.Close())

	require.Equal(t, n, rows, "sanity: iterated all rows")
	require.Equal(t, float64(n), rec.get("storage.iter.fetch.success"),
		"fetch.success must equal the number of rows fetched, not the EOF")
}
