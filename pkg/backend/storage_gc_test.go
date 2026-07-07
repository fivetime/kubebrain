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
	"sync/atomic"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"

	"github.com/kubewharf/kubebrain/pkg/metrics/mock"
	"github.com/kubewharf/kubebrain/pkg/storage"
	imemkv "github.com/kubewharf/kubebrain/pkg/storage/memkv"
)

// gcRecordingKV wraps a KvStorage with a storage.GarbageCollector that records
// calls, standing in for the TiKV store.
type gcRecordingKV struct {
	storage.KvStorage
	calls    atomic.Int64
	lifetime atomic.Int64
}

func (g *gcRecordingKV) GC(ctx context.Context, lifetime time.Duration) (uint64, error) {
	g.calls.Add(1)
	g.lifetime.Store(int64(lifetime))
	return uint64(g.calls.Load()), nil
}

// TestStorageGCDriverAdvancesSafepoint pins #37: when the storage implements
// GarbageCollector and a lifetime is configured, the leader periodically calls
// GC with that lifetime — the gc_worker role on a bare PD+TiKV deployment,
// without which MVCC versions accumulate forever and reads degrade.
func TestStorageGCDriverAdvancesSafepoint(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	m := mock.NewMinimalMetrics(ctrl)
	kv := &gcRecordingKV{KvStorage: imemkv.NewKvStorage()}
	defer func() { require.NoError(t, kv.Close()) }()

	lifetime := 60 * time.Millisecond
	b := NewBackend(kv, Config{
		Prefix: prefix, Identity: getStorageIdentity(),
		EnableEtcdCompatibility: true, StorageGCLifetime: lifetime,
	}, m).(*backend)
	b.SetCurrentRevision(uint64(time.Now().UnixNano()))

	// No fence registered => leadingFresh() is true (single-node), so the
	// driver runs. It must call GC repeatedly with the configured lifetime.
	require.Eventually(t, func() bool { return kv.calls.Load() >= 2 },
		5*time.Second, 10*time.Millisecond, "GC driver must fire periodically on the leader")
	require.Equal(t, int64(lifetime), kv.lifetime.Load(), "GC must receive the configured lifetime")
}

// TestStorageGCDriverDisabledByZeroLifetime pins the off switch: lifetime 0
// must not drive GC even when the storage supports it.
func TestStorageGCDriverDisabledByZeroLifetime(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	m := mock.NewMinimalMetrics(ctrl)
	kv := &gcRecordingKV{KvStorage: imemkv.NewKvStorage()}
	defer func() { require.NoError(t, kv.Close()) }()

	NewBackend(kv, Config{
		Prefix: prefix, Identity: getStorageIdentity(),
		EnableEtcdCompatibility: true, // StorageGCLifetime unset
	}, m)
	time.Sleep(150 * time.Millisecond)
	require.Zero(t, kv.calls.Load(), "GC must not run when lifetime is 0")
}
