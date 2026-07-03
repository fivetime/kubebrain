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

package etcd

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
)

func prefixKey(prefix, k string) []byte {
	return append([]byte(prefix), k...)
}

// TestCountOnlyHonorsRequestRevision pins review #1: a CountOnly range with an
// explicit point-in-time Revision must count the keys as of THAT revision, not the
// current one. Before the fix a revisioned CountOnly still took the fast
// backend.Count path (which counts at the current revision, as the CountRequest
// proto carries no revision), so deleting a key after the snapshot revision made
// WithRev(old).WithCountOnly() under-report.
func TestCountOnlyHonorsRequestRevision(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()
	ctx := context.Background()

	const prefix = "/registry/countrev/"
	end := prefixEnd([]byte(prefix))

	var rev3 int64
	for _, k := range []string{"p1", "p2", "p3"} {
		r, err := server.Put(ctx, &etcdserverpb.PutRequest{Key: prefixKey(prefix, k), Value: []byte("v")})
		require.NoError(t, err)
		rev3 = r.Header.Revision
	}
	require.Eventually(t, func() bool { return server.backend.GetCurrentRevision() >= uint64(rev3) }, 5*time.Second, 2*time.Millisecond)

	// Delete one key AFTER rev3.
	del, err := server.DeleteRange(ctx, &etcdserverpb.DeleteRangeRequest{Key: prefixKey(prefix, "p3")})
	require.NoError(t, err)
	require.Eventually(t, func() bool { return server.backend.GetCurrentRevision() >= uint64(del.Header.Revision) }, 5*time.Second, 2*time.Millisecond)

	// CountOnly at the current revision reflects the delete: 2.
	cur, err := server.Range(ctx, &etcdserverpb.RangeRequest{Key: []byte(prefix), RangeEnd: end, CountOnly: true})
	require.NoError(t, err)
	require.Equal(t, int64(2), cur.Count)
	require.Empty(t, cur.Kvs)

	// CountOnly as of rev3 (before the delete) must be 3 — the crux of #1.
	hist, err := server.Range(ctx, &etcdserverpb.RangeRequest{Key: []byte(prefix), RangeEnd: end, CountOnly: true, Revision: rev3})
	require.NoError(t, err)
	require.Equal(t, int64(3), hist.Count, "CountOnly must count as of the requested revision, not the current one")
	require.Empty(t, hist.Kvs)
}

func waitWaitGroup(t *testing.T, wg *sync.WaitGroup) {
	t.Helper()
	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("watch goroutine did not exit after context cancel")
	}
}

// TestWatchCreatedHeaderReportsCurrentRevision pins review #2: the created watch
// response must carry the store's current published revision, not 0. etcd clientv3
// uses the created header revision as the resume point for a from-now watch, so a
// disconnect after "created" but before the first event would otherwise resume from
// 0 and could skip events.
func TestWatchCreatedHeaderReportsCurrentRevision(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()
	ctx := context.Background()

	put, err := server.Put(ctx, &etcdserverpb.PutRequest{Key: []byte("/registry/watchrev/w1"), Value: []byte("v")})
	require.NoError(t, err)
	require.Eventually(t, func() bool { return server.backend.GetCurrentRevision() >= uint64(put.Header.Revision) }, 5*time.Second, 2*time.Millisecond)

	wantRev := server.backend.GetPublishedRevision()
	require.Greater(t, wantRev, uint64(0))

	wctx, cancel := context.WithCancel(ctx)
	stream := &fakeWatchServer{ctx: wctx}
	w := &watcher{
		backend:     server.backend,
		watchServer: stream,
		grpcServer:  server,
		watches:     map[int64]*watch{},
		metricCli:   server.metricCli,
	}
	// StartRevision defaults to 0 => a from-now watch.
	w.Start(wctx, &etcdserverpb.WatchCreateRequest{
		Key:      []byte("/registry/watchrev/"),
		RangeEnd: prefixEnd([]byte("/registry/watchrev/")),
	})
	cancel()
	waitWaitGroup(t, &w.wg)

	require.GreaterOrEqual(t, len(stream.sent), 1)
	require.True(t, stream.sent[0].Created)
	require.NotNil(t, stream.sent[0].Header)
	require.Equal(t, int64(wantRev), stream.sent[0].Header.Revision, "created header must report the current published revision, not 0")
}
