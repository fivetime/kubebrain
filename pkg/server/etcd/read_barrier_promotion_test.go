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

	"github.com/kubewharf/kubebrain/pkg/server/service/revision"
)

type promotionReadBackend struct {
	BackendShim
	reads    atomic.Int32
	installs atomic.Int32
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
