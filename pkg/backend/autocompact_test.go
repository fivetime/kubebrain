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
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/kubewharf/kubebrain/pkg/storage"
	imemkv "github.com/kubewharf/kubebrain/pkg/storage/memkv"
)

type blockingCompactWatermarkKV struct {
	storage.KvStorage
	compactKey []byte
	armed      atomic.Bool
	once       sync.Once
	entered    chan struct{}
	release    chan struct{}
}

type failingCompactWatermarkKV struct {
	storage.KvStorage
	compactKey []byte
}

func (f *failingCompactWatermarkKV) Get(ctx context.Context, key []byte) ([]byte, error) {
	if bytes.Equal(key, f.compactKey) {
		return nil, errors.New("injected compact watermark failure")
	}
	return f.KvStorage.Get(ctx, key)
}

func (b *blockingCompactWatermarkKV) Get(ctx context.Context, key []byte) ([]byte, error) {
	if !b.armed.Load() || !bytes.Equal(key, b.compactKey) {
		return b.KvStorage.Get(ctx, key)
	}
	b.once.Do(func() { close(b.entered) })
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-b.release:
		return nil, storage.ErrKeyNotFound
	}
}

// TestAutoCompactTarget pins the safety-net auto-compaction decision: cap history
// to the last `retention` revisions, leader-only, and never fight a healthy
// apiserver compactor (skip when the watermark already covers the target).
func TestAutoCompactTarget(t *testing.T) {
	cases := []struct {
		name                       string
		cur, retention, compactRev uint64
		leading                    bool
		wantAct                    bool
		wantTarget                 uint64
	}{
		{"disabled retention 0", 1000, 0, 0, true, false, 0},
		{"not leading", 1000, 100, 0, false, false, 0},
		{"not enough history (cur<=retention)", 100, 100, 0, true, false, 0},
		{"caps to last N", 1000, 100, 0, true, true, 900},
		{"apiserver keeping up (watermark >= target)", 1000, 100, 950, true, false, 0},
		{"apiserver behind -> safety net bites", 1_000_000, 100_000, 500_000, true, true, 900_000},
		{"target exactly at watermark -> skip", 1000, 100, 900, true, false, 0},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			target, act := autoCompactTarget(c.cur, c.retention, c.compactRev, c.leading)
			if act != c.wantAct || (act && target != c.wantTarget) {
				t.Fatalf("autoCompactTarget(cur=%d ret=%d compact=%d leading=%v) = (target=%d act=%v), want (target=%d act=%v)",
					c.cur, c.retention, c.compactRev, c.leading, target, act, c.wantTarget, c.wantAct)
			}
		})
	}
}

func TestAutoCompactCycleStopsWithLeadershipContext(t *testing.T) {
	m := newRecordCounters()
	kv := &blockingCompactWatermarkKV{
		KvStorage: imemkv.NewKvStorage(), compactKey: getCompactKey(prefix),
		entered: make(chan struct{}), release: make(chan struct{}),
	}
	defer func() { require.NoError(t, kv.Close()) }()
	b := NewBackend(kv, Config{
		Prefix: prefix, Identity: getStorageIdentity(), EnableEtcdCompatibility: true,
	}, m).(*backend)
	defer stopBackendWorkersForTest(b)
	b.SetCurrentRevision(1000)
	var leading atomic.Bool
	leading.Store(true)
	b.SetLeadershipFence(func() (uint64, bool) { return 1, leading.Load() })
	leaderCtx, stopLeader := context.WithCancel(context.Background())
	b.compactCtx.Store(compactContextHolder{ctx: leaderCtx})
	kv.armed.Store(true)

	done := make(chan struct{})
	go func() {
		b.autoCompactOnce(context.Background(), 100)
		close(done)
	}()
	<-kv.entered
	leading.Store(false)
	stopLeader()
	select {
	case <-done:
	case <-time.After(250 * time.Millisecond):
		close(kv.release)
		<-done
		t.Fatal("auto-compaction watermark read outlived its leadership context")
	}
	require.Zero(t, m.get("backend.auto_compact.err"),
		"leadership retirement is not an auto-compaction failure")
	require.Zero(t, m.get("storage.compaction.failure"),
		"leadership retirement must not enter the storage-failure taxonomy")
}

func TestAutoCompactCycleAdvancesWatermarkForActiveLeader(t *testing.T) {
	m := newRecordCounters()
	kv := imemkv.NewKvStorage()
	defer func() { require.NoError(t, kv.Close()) }()
	b := NewBackend(kv, Config{
		Prefix: prefix, Identity: getStorageIdentity(), EnableEtcdCompatibility: true,
	}, m).(*backend)
	defer stopBackendWorkersForTest(b)
	b.SetCurrentRevision(1000)

	b.autoCompactOnce(context.Background(), 100)
	watermark, err := b.GetCompactRevisionFresh(context.Background())
	require.NoError(t, err)
	require.Equal(t, uint64(900), watermark)
	require.Equal(t, float64(1), m.get("backend.auto_compact"))
	require.Equal(t, float64(900), m.gauge("backend.auto_compact.revision"))
	require.Eventually(t, func() bool {
		return atomic.LoadUint64(&b.compactDoneRev) >= 900
	}, time.Second, 5*time.Millisecond, "the active leader must finish the scheduled physical GC")
}

func TestAutoCompactCycleWatermarkFailureRemainsObservable(t *testing.T) {
	m := newRecordCounters()
	kv := &failingCompactWatermarkKV{
		KvStorage: imemkv.NewKvStorage(), compactKey: getCompactKey(prefix),
	}
	defer func() { require.NoError(t, kv.Close()) }()
	b := NewBackend(kv, Config{
		Prefix: prefix, Identity: getStorageIdentity(), EnableEtcdCompatibility: true,
	}, m).(*backend)
	defer stopBackendWorkersForTest(b)
	b.SetCurrentRevision(1000)

	b.autoCompactOnce(context.Background(), 100)
	require.Equal(t, float64(1), m.get("backend.auto_compact.err"),
		"an active leader's watermark read failure must be observable")
	require.Equal(t, float64(1), m.get("storage.compaction.failure"),
		"watermark read failures must enter the fixed compaction taxonomy")
	require.Zero(t, m.get("backend.auto_compact"))
}
