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

	proto "github.com/kubewharf/kubebrain-client/api/v2rpc"

	"github.com/kubewharf/kubebrain/pkg/metrics/mock"
	imemkv "github.com/kubewharf/kubebrain/pkg/storage/memkv"
)

// TestBackendWatchLowCacheTransientFailureNotCompaction pins #55: when a
// low-cache (ret.low) watch falls back to storage history and that scan fails
// for a transient reason (not compaction), the error must NOT be dressed up as a
// compaction — otherwise the client re-lists at a bogus revision instead of
// simply retrying the watch.
func TestBackendWatchLowCacheTransientFailureNotCompaction(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	m := mock.NewMinimalMetrics(ctrl)
	fkv := &flakyIterKV{KvStorage: imemkv.NewKvStorage()}
	defer func() { require.NoError(t, fkv.Close()) }()
	b := NewBackend(fkv, Config{Prefix: prefix, Identity: getStorageIdentity()}, m).(*backend)
	b.SetCurrentRevision(uint64(time.Now().UnixNano()))
	ctx := context.Background()

	baseKey := prefix + "/low-transient"
	keyA := baseKey + "/a"
	createA, err := b.Create(ctx, newCreateRequest(keyA, "a1"))
	require.NoError(t, err)
	updateA, err := b.Update(ctx, &proto.UpdateRequest{Kv: &proto.KeyValue{Key: []byte(keyA), Value: []byte("a2"), Revision: createA.Header.Revision}})
	require.NoError(t, err)
	require.True(t, updateA.Succeeded)
	waitUntilRevisionEqualOrTimeout(b, updateA.Header.Revision)

	// Force ret.low: cache holds only the latest (updateA) event, but the watch
	// starts at the earlier createA revision.
	b.watchCache.Reset()
	b.watchCache.Add(newEvent(proto.Event_PUT, updateA.Header.Revision, newKeyValue(keyA, "a2", updateA.Header.Revision)))

	// Arm a transient failure for the history scan over baseKey (its start key is
	// unique to the full-prefix scan, so setup reads are unaffected). This is a
	// storage error, not compaction.
	fkv.failSubstr, _ = b.historyPrefixBounds([]byte(baseKey))
	atomic.StoreInt32(&fkv.remaining, 1)

	ch, err := b.Watch(ctx, baseKey, createA.Header.Revision)
	require.ErrorContains(t, err, "injected iter failure")
	require.Nil(t, ch)
	require.NotContains(t, err.Error(), "compacted",
		"a transient history-scan failure must not be reported as compaction")
	require.NotContains(t, err.Error(), "cache event oldest revision",
		"the fabricated compaction-looking message must not be produced")
}
