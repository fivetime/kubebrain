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

// capableStore implements every optional storage capability so the test can
// prove metrics decoration does not make any of them unreachable.
type capableStore struct {
	storage.KvStorage
	gcCalled        bool
	exclusiveCalled bool
	batchGetCalled  bool
}

func (s *capableStore) GC(ctx context.Context, lifetime time.Duration) (uint64, error) {
	s.gcCalled = true
	return 42, nil
}

func (s *capableStore) GetExclusiveKvStorage() storage.KvStorage {
	s.exclusiveCalled = true
	return s.KvStorage
}

func (s *capableStore) ClusterID() uint64 {
	return 99
}

func (s *capableStore) BatchGet(ctx context.Context, keys [][]byte) (map[string][]byte, error) {
	s.batchGetCalled = true
	return map[string][]byte{string(keys[0]): []byte("value")}, nil
}

// TestMetricsWrapperPreservesOptionalInterfaces pins that wrapping a store for
// metrics never hides optional capabilities behind the wrapper. Existing direct
// GC and exclusive-storage compatibility remains intact, while generic
// discovery avoids an O(2^n) wrapper type for every capability combination.
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

	// ClusterIdentifier and BatchGetter are intentionally not fabricated on the
	// concrete wrapper type. FindCapability must discover them through any
	// number of metrics decorator layers.
	wrapped = NewKvStorage(wrapped, mock.NewMinimalMetrics(gomock.NewController(t)))
	_, directClusterID := wrapped.(storage.ClusterIdentifier)
	require.False(t, directClusterID)
	clusterID, ok := storage.FindCapability[storage.ClusterIdentifier](wrapped)
	require.True(t, ok)
	require.EqualValues(t, 99, clusterID.ClusterID())

	_, directBatchGet := wrapped.(storage.BatchGetter)
	require.False(t, directBatchGet)
	batchGetter, ok := storage.FindCapability[storage.BatchGetter](wrapped)
	require.True(t, ok)
	values, err := batchGetter.BatchGet(context.Background(), [][]byte{[]byte("key")})
	require.NoError(t, err)
	require.Equal(t, []byte("value"), values["key"])
	require.True(t, inner.batchGetCalled)

	plainInner := imemkv.NewKvStorage()
	defer func() { require.NoError(t, plainInner.Close()) }()
	plain := NewKvStorage(plainInner, mock.NewMinimalMetrics(gomock.NewController(t)))
	_, ok = storage.FindCapability[storage.ClusterIdentifier](plain)
	require.False(t, ok, "capability discovery must not fabricate ClusterIdentifier")
}
