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
	"sync"
	"sync/atomic"

	"go.etcd.io/etcd/api/v3/etcdserverpb"
)

// leaseManager owns the lease subsystem: all lease STATE and the lease logic
// that used to live inline on RPCServer, tangled with its five other gRPC
// services. RPCServer embeds a *leaseManager (its lease gRPC handlers and the
// write-path bind/unbind/IDForKey helpers are promoted), so the lease state
// machine is a single separable unit with its own file(s) — lease.go holds the
// methods — instead of RPCServer fields mutated ad hoc from the write path.
//
// Its dependencies (backend/peers/metrics) are BORROWED from the owning
// RPCServer via srv rather than copied, so there is a single source of truth: a
// test that swaps server.backend for failure injection is seen by lease methods
// too, and the two can never diverge.
type leaseManager struct {
	// Embed the forward-compat shim HERE (not on RPCServer): leaseManager
	// implements all five Lease RPCs, and RPCServer promotes them through its
	// embedded *leaseManager. Putting UnimplementedLeaseServer on RPCServer too
	// would make LeaseGrant et al. ambiguous (two embeds at the same depth). Its
	// explicit methods override these defaults; only mustEmbedUnimplementedLeaseServer
	// is actually used.
	etcdserverpb.UnimplementedLeaseServer

	srv *RPCServer

	// leaseWriteMu orders writes that can create or change a lease binding against
	// revoke/expiry. Writers hold RLock from lease validation through the durable
	// commit and index update; teardown holds Lock through key deletion and lease
	// removal. It is separate from leaseMu so backend I/O never blocks lease-state
	// readers while still preventing a key from committing behind a completed
	// revoke.
	leaseWriteMu  sync.RWMutex
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

// newLeaseManager builds a leaseManager owned by srv; deps are read through srv.
func newLeaseManager(srv *RPCServer, initialID int64) *leaseManager {
	return &leaseManager{
		srv:           srv,
		leaseID:       initialID,
		leases:        make(map[int64]*leaseState),
		keyLeaseIndex: make(map[string]int64),
	}
}

func (m *leaseManager) nextLeaseID() int64 {
	return atomic.AddInt64(&m.leaseID, 1)
}
