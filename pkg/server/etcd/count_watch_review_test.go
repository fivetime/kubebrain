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
// response must carry the store's current committed revision, not 0. etcd clientv3
// uses the created header revision as the resume point for a from-now watch, so a
// disconnect after "created" but before the first event would otherwise resume from
// 0 and could skip events.
func TestWatchCreatedHeaderReportsCurrentRevision(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()
	ctx := context.Background()

	put, err := server.Put(ctx, &etcdserverpb.PutRequest{Key: []byte("/registry/watchrev/w1"), Value: []byte("v")})
	require.NoError(t, err)
	// Freeze a deterministic publication gap after a successfully acknowledged
	// Put. Created must use the store revision, as upstream watchStream.Rev does.
	lagged := &futureProgressBackend{BackendShim: server.backend}
	lagged.published.Store(uint64(put.Header.Revision - 1))
	wantRev := server.backend.GetCurrentRevision()
	require.Greater(t, wantRev, uint64(0))

	wctx, cancel := context.WithCancel(ctx)
	stream := &fakeWatchServer{ctx: wctx}
	w := &watcher{
		backend:     lagged,
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
	require.Equal(t, int64(wantRev), stream.sent[0].Header.Revision, "created header must report current store revision despite publication lag")
}

func TestWatchCreationPublicationGapPreservesReplayBoundary(t *testing.T) {
	for _, historical := range []bool{false, true} {
		name := "from-now"
		if historical {
			name = "historical"
		}
		t.Run(name, func(t *testing.T) {
			server, closeFn := newTestRPCServer(t)
			defer closeFn()
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			key := []byte("/watch-creation-boundary")
			seed, err := server.Put(ctx, &etcdserverpb.PutRequest{Key: key, Value: []byte("seed")})
			require.NoError(t, err)
			base, err := server.Put(ctx, &etcdserverpb.PutRequest{Key: key, Value: []byte("before-create")})
			require.NoError(t, err)
			lagged := &futureProgressBackend{BackendShim: server.backend}
			lagged.published.Store(uint64(seed.Header.Revision))
			stream := &createCallbackWatchServer{
				fakeWatchServer: &fakeWatchServer{ctx: ctx},
				onCreated: func() {
					_, err := server.Put(ctx, &etcdserverpb.PutRequest{Key: key, Value: []byte("during-created")})
					require.NoError(t, err)
				},
			}
			w := &watcher{backend: lagged, grpcServer: server, watchServer: stream,
				watches: make(map[int64]*watch), metricCli: server.metricCli}
			t.Cleanup(w.Close)
			request := &etcdserverpb.WatchCreateRequest{Key: key}
			want := []int64{base.Header.Revision + 1}
			if historical {
				request.StartRevision = seed.Header.Revision
				want = []int64{seed.Header.Revision, base.Header.Revision, base.Header.Revision + 1}
			}
			w.Start(ctx, request)
			revisions := func() []int64 {
				var values []int64
				for _, response := range stream.snapshot() {
					for _, event := range response.Events {
						values = append(values, event.Kv.ModRevision)
					}
				}
				return values
			}
			require.Eventually(t, func() bool { return len(revisions()) >= len(want) }, 5*time.Second, time.Millisecond)
			w.Close()
			require.Equal(t, want, revisions(), "creation-time writes must be replayed without old from-now events or missing history")
			responses := stream.snapshot()
			require.True(t, responses[0].Created)
			require.Equal(t, base.Header.Revision, responses[0].Header.Revision)
		})
	}
}

// Model a client receiving Created followed by a transport failure before any
// event reaches it. A Send error after recording Created also prevents the
// first server generation from starting its event delivery goroutine.
type createdThenDisconnectedWatchServer struct {
	*fakeWatchServer
	afterCreated func()
}

func (s *createdThenDisconnectedWatchServer) Send(response *etcdserverpb.WatchResponse) error {
	if response.Created {
		if err := s.fakeWatchServer.Send(response); err != nil {
			return err
		}
		s.afterCreated()
	}
	return context.Canceled
}

func TestWatchCreationPublicationGapResumeBeforeFirstEvent(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()
	ctx := context.Background()
	key := []byte("/watch-created-disconnect")
	base, err := server.Put(ctx, &etcdserverpb.PutRequest{Key: key, Value: []byte("before-create")})
	require.NoError(t, err)
	lagged := &futureProgressBackend{BackendShim: server.backend}
	lagged.published.Store(uint64(base.Header.Revision - 1))
	var duringRevision int64
	stream := &createdThenDisconnectedWatchServer{
		fakeWatchServer: &fakeWatchServer{ctx: ctx},
		afterCreated: func() {
			put, err := server.Put(ctx, &etcdserverpb.PutRequest{Key: key, Value: []byte("during-created")})
			require.NoError(t, err)
			duringRevision = put.Header.Revision
		},
	}
	first := &watcher{backend: lagged, grpcServer: server, watchServer: stream,
		watches: make(map[int64]*watch), metricCli: server.metricCli}
	first.Start(ctx, &etcdserverpb.WatchCreateRequest{Key: key})
	first.Close()
	created := stream.sentResponses()
	require.Len(t, created, 1, "transport failed before the first event")
	require.True(t, created[0].Created)
	require.Empty(t, created[0].Events)
	require.Equal(t, base.Header.Revision, created[0].Header.Revision)
	require.Equal(t, base.Header.Revision+1, duringRevision)
	after, err := server.Put(ctx, &etcdserverpb.PutRequest{Key: key, Value: []byte("while-disconnected")})
	require.NoError(t, err)

	resumeCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	resumedStream := &fakeWatchServer{ctx: resumeCtx}
	resumed := &watcher{backend: server.backend, grpcServer: server, watchServer: resumedStream,
		watches: make(map[int64]*watch), metricCli: server.metricCli}
	var closeOnce sync.Once
	closeWatch := func() { closeOnce.Do(resumed.Close) }
	t.Cleanup(closeWatch)
	// clientv3 v3.7.1 resumes inclusively at the Created revision when no
	// event or progress response has arrived. Only those later responses
	// advance its resume revision by one. Replaying the boundary is expected.
	resumed.Start(resumeCtx, &etcdserverpb.WatchCreateRequest{Key: key, StartRevision: created[0].Header.Revision})
	revisions := func() []int64 {
		var result []int64
		for _, response := range resumedStream.sentResponses() {
			for _, event := range response.Events {
				result = append(result, event.Kv.ModRevision)
			}
		}
		return result
	}
	require.Eventually(t, func() bool { return len(revisions()) >= 3 }, 5*time.Second, time.Millisecond)
	closeWatch()
	require.Equal(t, []int64{base.Header.Revision, duringRevision, after.Header.Revision}, revisions(), "inclusive client resume must preserve the boundary and every subsequent event")
}
