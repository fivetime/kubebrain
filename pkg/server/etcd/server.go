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

package etcd

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"go.etcd.io/etcd/api/v3/etcdserverpb"
	"google.golang.org/grpc"
	"k8s.io/klog/v2"

	b "github.com/kubewharf/kubebrain/pkg/backend"
	"github.com/kubewharf/kubebrain/pkg/metrics"
	"github.com/kubewharf/kubebrain/pkg/server/service"
)

var (
	_ etcdserverpb.KVServer          = (*RPCServer)(nil)
	_ etcdserverpb.WatchServer       = (*RPCServer)(nil)
	_ etcdserverpb.MaintenanceServer = (*RPCServer)(nil)
	_ etcdserverpb.AuthServer        = (*RPCServer)(nil)
)

// RPCServer only support limited method of etcd grpc server
type RPCServer struct {
	etcdserverpb.UnimplementedAuthServer

	backend BackendShim

	// watcher map mutes
	sync.Mutex

	metricCli metrics.Metrics
	peers     service.PeerService

	leaseMu       sync.Mutex
	leaseID       int64
	leases        map[int64]*leaseState
	keyLeaseIndex map[string]int64
	// orphanSweepStop is non-nil while the leader-side orphaned-leased-key sweeper
	// goroutine is running; closed (and niled) when leadership is lost. Guarded by
	// leaseMu.
	orphanSweepStop chan struct{}
	// leasedKeyCount mirrors len(keyLeaseIndex) for a lock-free fast path in
	// leaseIDForKey: the read path resolves an attached lease for every returned
	// KeyValue, and the overwhelmingly common case (a range over keys that hold
	// no lease) must not take leaseMu per key. Updated under leaseMu, read with
	// atomics.
	leasedKeyCount int64
}

type leaseState struct {
	id       int64
	ttl      int64
	deadline time.Time
	keys     map[string]struct{}
	timer    *time.Timer
}

// New returns the etcd rpc server
func New(backend b.Backend, metricCli metrics.Metrics, peers service.PeerService) *RPCServer {
	server := &RPCServer{
		backend:       NewBackendShim(backend, metricCli),
		metricCli:     metricCli,
		peers:         peers,
		leaseID:       time.Now().UnixNano(),
		leases:        make(map[int64]*leaseState),
		keyLeaseIndex: make(map[string]int64),
	}
	// Wire read/watch KeyValues to carry the lease attached to each key (etcd
	// parity). Safe to set before serving: New runs single-threaded.
	server.backend.SetLeaseLookup(server.leaseIDForKey)
	// Wire follower counts to the leader's count index (#41): the index is
	// leader-only, so a follower's count fallback was a full range scan — a
	// guaranteed request-deadline timeout at 10M keys. Forward a CountOnly
	// Range preserving the caller's revision, so a paginated sequence's counts
	// stay exact; the leader answers from its index (or its own bounded
	// fallback). Any failure falls back to the local path. No recursion: the
	// leader never proxies (IsLeader guard).
	server.backend.SetCountProxy(func(ctx context.Context, r *etcdserverpb.RangeRequest) (int64, bool) {
		if peers.IsLeader() || !peers.EtcdProxyEnabled() {
			return 0, false
		}
		req := *r
		req.CountOnly = true
		req.Limit = 0
		req.KeysOnly = false
		resp, err := peers.Range(ctx, &req)
		if err != nil || resp == nil {
			server.metricCli.EmitCounter("count.proxy.err", 1)
			return 0, false
		}
		return resp.Count, true
	})
	if err := server.restoreLeases(context.Background()); err != nil {
		klog.ErrorS(err, "restore leases failed")
	}
	return server
}

func (s *RPCServer) nextLeaseID() int64 {
	return atomic.AddInt64(&s.leaseID, 1)
}

// Register register etcd grpc service
func (s *RPCServer) Register(server *grpc.Server) {
	etcdserverpb.RegisterLeaseServer(server, s)
	etcdserverpb.RegisterWatchServer(server, s)
	etcdserverpb.RegisterKVServer(server, s)
	etcdserverpb.RegisterClusterServer(server, s)
	etcdserverpb.RegisterMaintenanceServer(server, s)
	etcdserverpb.RegisterAuthServer(server, s)
}
