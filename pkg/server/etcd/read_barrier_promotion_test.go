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
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/kubewharf/kubebrain/pkg/server/service/etcdproxy"
	"github.com/kubewharf/kubebrain/pkg/server/service/revision"
)

type promotionReadBackend struct {
	BackendShim
	reads    atomic.Int32
	installs atomic.Int32
	watches  atomic.Int32
}

func (b *promotionReadBackend) Watch(ctx context.Context, key string, rev uint64) (<-chan etcdproxy.WatchResult, error) {
	b.watches.Add(1)
	return b.BackendShim.Watch(ctx, key, rev)
}

func (b *promotionReadBackend) Get(ctx context.Context, req *etcdserverpb.RangeRequest) (*etcdserverpb.RangeResponse, error) {
	b.reads.Add(1)
	return b.BackendShim.Get(ctx, req)
}

func (b *promotionReadBackend) SetCurrentRevision(rev uint64) {
	b.installs.Add(1)
	b.BackendShim.SetCurrentRevision(rev)
}

// This joins the real revision syncer to the RPC handler, rather than injecting
// its final error. Election transitions remain controlled test inputs; this is
// not a test of the election implementation or the follower gRPC proxy route.
func TestReadBarrierRangeWithRealSyncerDuringPromotion(t *testing.T) {
	for _, mode := range []string{"unchanged-follower", "fresh-local-leader", "stale-local-leader"} {
		t.Run(mode, func(t *testing.T) {
			server, cleanup := newTestRPCServer(t)
			defer cleanup()
			seed, err := server.Put(context.Background(), &etcdserverpb.PutRequest{Key: []byte("promotion-key"), Value: []byte("committed")})
			require.NoError(t, err)
			backend := &promotionReadBackend{BackendShim: server.backend}
			server.backend = backend
			entered, release := make(chan struct{}), make(chan struct{})
			var enteredOnce, releaseOnce sync.Once
			var requests atomic.Int32
			unblock := func() { releaseOnce.Do(func() { close(release) }) }
			peer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				requests.Add(1)
				enteredOnce.Do(func() { close(entered) })
				select {
				case <-release:
					fmt.Fprintf(w, `{"Revision":%d}`, seed.Header.Revision)
				case <-r.Context().Done():
				}
			}))
			defer peer.Close()
			defer unblock()
			var promoted atomic.Bool
			term := func() uint64 {
				if promoted.Load() {
					return 2
				}
				return 1
			}
			peers := testPeerService{
				leaderInfo:    strings.TrimPrefix(peer.URL, "http://"),
				isLeaderFn:    promoted.Load,
				currentTermFn: term,
				epochFn:       func() (uint64, bool) { return term(), promoted.Load() && mode == "fresh-local-leader" },
			}
			syncer := revision.NewRevisionSyncer(backend, server.metricCli, peers, nil)
			defer syncer.Close()
			peers.syncReadFn = syncer.SyncReadRevision
			server.peers = peers
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			type result struct {
				response *etcdserverpb.RangeResponse
				err      error
			}
			done := make(chan result, 1)
			request := func() (*etcdserverpb.RangeResponse, error) {
				return server.Range(ctx, &etcdserverpb.RangeRequest{Key: []byte("promotion-key")})
			}
			go func() { response, err := request(); done <- result{response, err} }()
			select {
			case <-entered:
			case <-ctx.Done():
				t.Fatal("Range did not enter the real revision HTTP request")
			}
			if mode != "unchanged-follower" {
				promoted.Store(true)
			}
			unblock()
			old := <-done
			if mode == "unchanged-follower" {
				require.NoError(t, old.err)
				require.Len(t, old.response.Kvs, 1)
				require.Equal(t, []byte("committed"), old.response.Kvs[0].Value)
				require.Equal(t, int32(1), backend.installs.Load())
				require.Equal(t, int32(1), backend.reads.Load())
			} else {
				require.Equal(t, codes.Unavailable, status.Code(old.err), "old Range must not consume its deadline")
				require.ErrorContains(t, old.err, "local node became leader")
				require.Empty(t, old.response.GetKvs())
				require.Zero(t, backend.installs.Load(), "rejected peer revision must not reach backend")
				require.Zero(t, backend.reads.Load(), "failed barrier must not reach data read")
				fresh, err := request()
				if mode == "fresh-local-leader" {
					require.NoError(t, err)
					require.Len(t, fresh.Kvs, 1)
					require.Equal(t, []byte("committed"), fresh.Kvs[0].Value)
					require.Equal(t, int32(1), backend.reads.Load())
				} else {
					require.Equal(t, codes.Unavailable, status.Code(err))
					require.ErrorContains(t, err, "local leadership lease is stale")
					require.Empty(t, fresh.GetKvs())
					require.Zero(t, backend.reads.Load())
				}
				require.Zero(t, backend.installs.Load())
			}
			require.Equal(t, int32(1), requests.Load(), "promotion must not refetch from the old peer")
		})
	}
}

func TestReadBarrierWatchWithRealSyncerDuringPromotion(t *testing.T) {
	for _, fresh := range []bool{true, false} {
		t.Run(fmt.Sprintf("fresh=%t", fresh), func(t *testing.T) {
			server, cleanup := newTestRPCServer(t)
			defer cleanup()
			server.SetMaxWatches(1)
			backend := &promotionReadBackend{BackendShim: server.backend}
			server.backend = backend
			entered, release := make(chan struct{}), make(chan struct{})
			var enteredOnce, releaseOnce sync.Once
			var requests atomic.Int32
			unblock := func() { releaseOnce.Do(func() { close(release) }) }
			peer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				requests.Add(1)
				enteredOnce.Do(func() { close(entered) })
				select {
				case <-release:
					fmt.Fprint(w, `{"Revision":100}`)
				case <-r.Context().Done():
				}
			}))
			defer peer.Close()
			defer unblock()
			var promoted atomic.Bool
			term := func() uint64 {
				if promoted.Load() {
					return 2
				}
				return 1
			}
			peers := testPeerService{
				leaderInfo:    strings.TrimPrefix(peer.URL, "http://"),
				isLeaderFn:    promoted.Load,
				currentTermFn: term,
				epochFn:       func() (uint64, bool) { return term(), promoted.Load() && fresh },
			}
			syncer := revision.NewRevisionSyncer(backend, server.metricCli, peers, nil)
			defer syncer.Close()
			peers.syncReadFn = syncer.SyncReadRevision
			server.peers = peers
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			create := func() *etcdserverpb.WatchRequest {
				return &etcdserverpb.WatchRequest{RequestUnion: &etcdserverpb.WatchRequest_CreateRequest{
					CreateRequest: &etcdserverpb.WatchCreateRequest{Key: []byte("promotion-watch"), WatchId: 17},
				}}
			}
			stream := &controllableWatchServer{ctx: ctx, recv: make(chan *etcdserverpb.WatchRequest, 1)}
			stream.recv <- create()
			done := make(chan error, 1)
			go func() { done <- server.Watch(stream) }()
			oldJoined := false
			defer func() {
				cancel()
				if !oldJoined {
					<-done
				}
			}()
			select {
			case <-entered:
			case <-ctx.Done():
				t.Fatal("Watch never entered revision fetch")
			}
			promoted.Store(true)
			unblock()
			err := <-done
			oldJoined = true
			require.Equal(t, codes.Unavailable, status.Code(err))
			require.ErrorContains(t, err, "local node became leader")
			require.Empty(t, stream.snapshot(), "failed fence must not acknowledge creation")
			require.Zero(t, backend.watches.Load())
			require.Zero(t, backend.installs.Load())
			require.Zero(t, atomic.LoadInt64(&server.activeWatches), "failed create must release quota")

			// A client may start a new stream, but only fresh leadership can create
			// a local watcher. This is explicit caller recovery, not hidden retry.
			nextCtx, nextCancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer nextCancel()
			next := &controllableWatchServer{ctx: nextCtx, recv: make(chan *etcdserverpb.WatchRequest, 1)}
			next.recv <- create()
			go func() { done <- server.Watch(next) }()
			nextJoined := false
			defer func() {
				nextCancel()
				if !nextJoined {
					<-done
				}
			}()
			if fresh {
				require.Eventually(t, func() bool { return len(next.snapshot()) > 0 }, time.Second, time.Millisecond)
				response := next.snapshot()[0]
				require.True(t, response.Created)
				require.False(t, response.Canceled)
				require.Equal(t, int64(17), response.WatchId)
				put, err := server.Put(nextCtx, &etcdserverpb.PutRequest{Key: []byte("promotion-watch"), Value: []byte("after-promotion")})
				require.NoError(t, err)
				require.Eventually(t, func() bool {
					for _, response := range next.snapshot() {
						for _, event := range response.Events {
							if event.Kv.ModRevision == put.Header.Revision && string(event.Kv.Value) == "after-promotion" {
								return true
							}
						}
					}
					return false
				}, time.Second, time.Millisecond)
				nextCancel()
				err = <-done
				nextJoined = true
				requireWatchCanceled(t, err)
				require.Equal(t, int32(1), backend.watches.Load())
			} else {
				err := <-done
				nextJoined = true
				require.Equal(t, codes.Unavailable, status.Code(err))
				require.ErrorContains(t, err, "local leadership lease is stale")
				require.Empty(t, next.snapshot())
				require.Zero(t, backend.watches.Load())
			}
			require.Zero(t, atomic.LoadInt64(&server.activeWatches))
			require.Zero(t, backend.installs.Load())
			require.Equal(t, int32(1), requests.Load())
		})
	}
}
