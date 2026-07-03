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
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"

	proto "github.com/kubewharf/kubebrain-client/api/v2rpc"

	"github.com/kubewharf/kubebrain/pkg/metrics/mock"
	imemkv "github.com/kubewharf/kubebrain/pkg/storage/memkv"
)

// TestCountIndexMatchesScanAcrossRevisions verifies the in-memory count index
// returns exactly the same live-key count as a storage scan (List length),
// across the current and pinned historical revisions after creates/updates/
// deletes — the correctness the paginated-count optimization depends on.
func TestCountIndexMatchesScanAcrossRevisions(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	m := mock.NewMinimalMetrics(ctrl)
	kv := imemkv.NewKvStorage()
	defer func() { require.NoError(t, kv.Close()) }()
	b := NewBackend(kv, Config{
		Prefix:                  prefix,
		Identity:                getStorageIdentity(),
		EnableEtcdCompatibility: true,
		EnableCountIndex:        true,
	}, m).(*backend)
	b.SetCurrentRevision(uint64(time.Now().UnixNano()))
	ctx := context.Background()

	p := prefix + "/reg/"
	end := PrefixEnd([]byte(p))

	put := func(name string) uint64 {
		r, err := b.Update(ctx, &proto.UpdateRequest{Kv: &proto.KeyValue{Key: []byte(p + name), Value: []byte("v")}})
		require.NoError(t, err)
		return r.Header.Revision
	}
	del := func(name string) uint64 {
		r, err := b.Delete(ctx, &proto.DeleteRequest{Key: []byte(p + name)})
		require.NoError(t, err)
		return r.Header.Revision
	}
	waitCommitted := func(rev uint64) {
		require.Eventually(t, func() bool { return b.GetCurrentRevision() >= rev }, 5*time.Second, 2*time.Millisecond)
	}
	scanCount := func(rev uint64) int {
		resp, err := b.List(ctx, &proto.RangeRequest{Key: []byte(p), End: end, Revision: rev})
		require.NoError(t, err)
		return len(resp.Kvs)
	}
	assertIndexMatches := func(rev uint64) {
		c, served := b.CountAtRevision(ctx, []byte(p), end, rev)
		require.True(t, served, "index should serve count at rev %d", rev)
		require.Equal(t, scanCount(rev), int(c), "index count != scan count at rev %d", rev)
	}

	// create 5, snapshot a revision, then mutate.
	for i := 0; i < 5; i++ {
		put(fmt.Sprintf("k%02d", i))
	}
	var revAfter5 uint64
	require.Eventually(t, func() bool { revAfter5 = b.GetCurrentRevision(); return scanCount(0) == 5 }, 5*time.Second, 2*time.Millisecond)

	require.NoError(t, b.RebuildCountIndex(ctx))
	assertIndexMatches(revAfter5) // 5 live

	// delete one, add two more.
	del("k02")
	put("k05")
	rLast := put("k06")
	waitCommitted(rLast)

	assertIndexMatches(b.GetCurrentRevision()) // current: 5 - 1 + 2 = 6
	assertIndexMatches(revAfter5)              // pinned historical: still 5 live there

	// sub-range count also matches.
	subEnd := PrefixEnd([]byte(p + "k03"))
	c, served := b.CountAtRevision(ctx, []byte(p+"k00"), subEnd, b.GetCurrentRevision())
	require.True(t, served)
	sub, err := b.List(ctx, &proto.RangeRequest{Key: []byte(p + "k00"), End: subEnd, Revision: b.GetCurrentRevision()})
	require.NoError(t, err)
	require.Equal(t, len(sub.Kvs), int(c))
}
