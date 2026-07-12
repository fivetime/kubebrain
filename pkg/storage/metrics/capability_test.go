// Copyright 2022 ByteDance and/or its affiliates
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
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/golang/mock/gomock"

	"github.com/kubewharf/kubebrain/pkg/metrics/mock"
	"github.com/kubewharf/kubebrain/pkg/storage"
	imemkv "github.com/kubewharf/kubebrain/pkg/storage/memkv"
)

// capableStore is a KvStorage that also implements both optional interfaces
// (GarbageCollector, ExclusiveKvStorage), standing in for a backend that has
// them so we can prove the metrics wrapper does not drop them.
type capableStore struct {
	storage.KvStorage
	gcCalled        bool
	exclusiveCalled bool
}

func (s *capableStore) GC(ctx context.Context, lifetime time.Duration) (uint64, error) {
	s.gcCalled = true
	return 42, nil
}

func (s *capableStore) GetExclusiveKvStorage() storage.KvStorage {
	s.exclusiveCalled = true
	return s.KvStorage
}

// TestMetricsWrapperPreservesOptionalInterfaces pins that wrapping a store for
// metrics never hides its optional GarbageCollector / ExclusiveKvStorage
// capabilities behind the wrapper — a type assertion (scanner) or the GC driver
// must still reach them, and the forward must call through to the real store
// (review #51). Guards the O(2^n) optional-interface trap in NewKvStorage.
func TestMetricsWrapperPreservesOptionalInterfaces(t *testing.T) {
	inner := &capableStore{KvStorage: imemkv.NewKvStorage()}
	defer func() { require.NoError(t, inner.KvStorage.Close()) }()

	wrapped := NewKvStorage(inner, mock.NewMinimalMetrics(gomock.NewController(t)))

	gc, ok := wrapped.(storage.GarbageCollector)
	require.True(t, ok, "metrics-wrapped store must still satisfy GarbageCollector")
	_, err := gc.GC(context.Background(), time.Minute)
	require.NoError(t, err)
	require.True(t, inner.gcCalled, "GC must forward to the underlying store")

	excl, ok := wrapped.(storage.ExclusiveKvStorage)
	require.True(t, ok, "metrics-wrapped store must still satisfy ExclusiveKvStorage")
	require.NotNil(t, excl.GetExclusiveKvStorage())
	require.True(t, inner.exclusiveCalled, "GetExclusiveKvStorage must forward to the underlying store")
}
