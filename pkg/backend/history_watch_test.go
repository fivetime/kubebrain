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
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"

	proto "github.com/kubewharf/kubebrain-client/api/v2rpc"

	"github.com/kubewharf/kubebrain/pkg/metrics/mock"
	"github.com/kubewharf/kubebrain/pkg/storage"
	imemkv "github.com/kubewharf/kubebrain/pkg/storage/memkv"
)

// countingIterKV counts the number of Iter calls, so a test can assert how many
// backend storage scans a single history fallback performs.
type countingIterKV struct {
	storage.KvStorage
	iters int64
}

func (c *countingIterKV) Iter(ctx context.Context, start []byte, end []byte, timestamp uint64, limit uint64) (storage.Iter, error) {
	atomic.AddInt64(&c.iters, 1)
	return c.KvStorage.Iter(ctx, start, end, timestamp, limit)
}

// TestHistoryWatchEventsNoPerTombstoneReads pins the read-amplification root fix
// for #30: a single history fallback over a prefix must perform exactly ONE
// backend scan, regardless of how many DELETE (tombstone) events fall in the
// window. The previous implementation issued an extra point read (which itself
// is a limit-1 Iter) per DELETE to fetch its prev-kv, so N reconnecting watchers
// after a cache reset each cost 1+D scans -> a storage thundering herd.
//
// The prev-kv for each DELETE is instead recovered from the ordered scan itself:
// object keys sort per-user-key with revisions ascending, so the version
// immediately preceding a tombstone has already been read in the same scan.
func TestHistoryWatchEventsNoPerTombstoneReads(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	m := mock.NewMinimalMetrics(ctrl)
	ckv := &countingIterKV{KvStorage: imemkv.NewKvStorage()}
	defer func() { require.NoError(t, ckv.Close()) }()
	// Production config inlines metadata into the value envelope, so history
	// events decode their type without a per-event metadata lookup; the only
	// residual per-event storage cost this test targets is the DELETE prev-kv.
	b := NewBackend(ckv, Config{Prefix: prefix, Identity: getStorageIdentity(), EnableEtcdCompatibility: true}, m).(*backend)
	b.SetCurrentRevision(uint64(time.Now().UnixNano()))
	ctx := context.Background()

	fromRev := b.GetCurrentRevision() + 1

	// keys 0..5: create; update the first few; delete the last three (tombstones).
	const nKeys = 6
	keys := make([][]byte, nKeys)
	for i := 0; i < nKeys; i++ {
		keys[i] = []byte(fmt.Sprintf("%s/reg/h/k%02d", prefix, i))
		cr, err := b.Create(ctx, &proto.CreateRequest{Key: keys[i], Value: []byte(fmt.Sprintf("v-%d-1", i))})
		require.NoError(t, err)
		last := cr.Header.Revision
		if i < 3 {
			u, err := b.Update(ctx, &proto.UpdateRequest{Kv: &proto.KeyValue{Key: keys[i], Value: []byte(fmt.Sprintf("v-%d-2", i)), Revision: last}})
			require.NoError(t, err)
			require.True(t, u.Succeeded)
			last = u.Header.Revision
		}
	}
	// delete keys 3,4,5 -> three tombstones in the window.
	deleted := map[string]bool{}
	for i := 3; i < nKeys; i++ {
		dr, err := b.Delete(ctx, &proto.DeleteRequest{Key: keys[i]})
		require.NoError(t, err)
		require.True(t, dr.Succeeded)
		deleted[string(keys[i])] = true
	}
	curRev := b.GetCurrentRevision()
	waitCommitted(t, b, curRev)

	atomic.StoreInt64(&ckv.iters, 0)
	events, err := b.historyWatchEvents(ctx, prefix+"/reg/h/", fromRev, b.GetCurrentRevision(), 0)
	require.NoError(t, err)

	iters := atomic.LoadInt64(&ckv.iters)
	require.Equal(t, int64(1), iters,
		"a single history fallback must do exactly one backend scan (no per-tombstone point reads); got %d", iters)

	// Correctness: latest event per key reflects create/update/delete, and each
	// DELETE carries the prev-kv (value + revision) of the version before it.
	type last struct {
		typ proto.Event_EventType
		val string
	}
	seen := map[string]last{}
	for _, e := range events {
		v := ""
		if e.Kv != nil {
			// history event values keep the inline metadata envelope; strip it
			// the same way the read path does before comparing.
			v = string(StripInlineValue(e.Kv.Value))
		}
		seen[string(e.Kv.Key)] = last{typ: e.Type, val: v}
	}
	for i := 0; i < nKeys; i++ {
		k := string(keys[i])
		l, ok := seen[k]
		require.True(t, ok, "missing events for %s", k)
		if deleted[k] {
			require.Equal(t, proto.Event_DELETE, l.typ, "%s should end DELETE", k)
			// prev-kv value is the create value (no update on keys 3..5)
			require.Equal(t, fmt.Sprintf("v-%d-1", i), l.val, "%s DELETE prev-kv value", k)
		} else if i < 3 {
			require.Equal(t, proto.Event_PUT, l.typ, "%s updated -> last PUT", k)
			require.Equal(t, fmt.Sprintf("v-%d-2", i), l.val)
		} else {
			require.Equal(t, proto.Event_CREATE, l.typ)
		}
	}
}
