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
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"

	"github.com/kubewharf/kubebrain/pkg/metrics/mock"
	"github.com/kubewharf/kubebrain/pkg/storage"
	"github.com/kubewharf/kubebrain/pkg/storage/memkv"
)

type shutdownOrderKV struct {
	storage.KvStorage
	gcEntered               chan struct{}
	gcExited                chan struct{}
	gcOnce                  sync.Once
	closeCalls              atomic.Int64
	closedBeforeWorkersExit atomic.Bool
}

func (s *shutdownOrderKV) GC(ctx context.Context, _ time.Duration) (uint64, error) {
	s.gcOnce.Do(func() { close(s.gcEntered) })
	<-ctx.Done()
	close(s.gcExited)
	return 0, ctx.Err()
}

func (s *shutdownOrderKV) Close() error {
	s.closeCalls.Add(1)
	select {
	case <-s.gcExited:
	default:
		s.closedBeforeWorkersExit.Store(true)
	}
	return s.KvStorage.Close()
}

func TestBackendCloseStopsWorkersBeforeStorage(t *testing.T) {
	ctrl := gomock.NewController(t)
	metrics := mock.NewMinimalMetrics(ctrl)
	kv := &shutdownOrderKV{
		KvStorage: memkv.NewKvStorage(),
		gcEntered: make(chan struct{}),
		gcExited:  make(chan struct{}),
	}
	b := NewBackend(kv, Config{
		Prefix:                  prefix,
		Identity:                "shutdown-test",
		EnableEtcdCompatibility: true,
		StorageGCLifetime:       time.Millisecond,
	}, metrics).(*backend)

	watchCtx, cancelWatch := context.WithCancel(context.Background())
	defer cancelWatch()
	sub, err := b.watcherHub.AddWatcher(watchCtx, nil)
	require.NoError(t, err)
	<-kv.gcEntered

	require.NoError(t, b.Close())
	require.NoError(t, b.Close(), "Close must be idempotent")
	require.False(t, kv.closedBeforeWorkersExit.Load(), "storage closed before a worker returned")
	require.Equal(t, int64(1), kv.closeCalls.Load())
	select {
	case _, ok := <-sub:
		require.False(t, ok, "backend shutdown must close watcher subscriptions")
	default:
		t.Fatal("watcher subscription remained open after Close returned")
	}
}
