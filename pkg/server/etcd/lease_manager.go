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
	"math"
	"sync"
	"sync/atomic"
	"time"

	"go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/pkg/v3/idutil"
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
	leaseWriteMu leaseWriteMutex
	// leaseTeardowns is non-zero only while the exclusive leaseWriteMu owner is
	// deleting one lease. It lets an unrelated, checkpoint-free keepalive renew
	// its in-memory deadline without waiting for slow TiKV I/O, while preserving
	// the global fence for reload, index repair, and ordinary KV mutations.
	leaseTeardowns atomic.Int64
	// leaseCheckpointMu excludes leadership reload/withdrawal from every
	// in-flight checkpoint transition. Ordinary renew/checkpoint work holds its
	// read side and is serialized only with the same lease by a bounded stripe
	// below; a slow TiKV checkpoint for one lease must not starve unrelated
	// short-TTL keepalives.
	leaseCheckpointMu    leaseWriteMutex
	leaseCheckpointLocks [256]leaseWriteMutex
	// State access remains exclusive. Read RPC admission may cancel while
	// queued; internal state transitions retain the blocking Lock/Unlock API.
	leaseMu leaseStateMutex
	leaseID int64
	// automaticLeaseIDs uses etcd's member/time/counter layout once the static
	// DBaaS member set is installed. Keeping the generator in an atomic pointer
	// makes the configuration boundary explicit and lets direct/single-node
	// users retain the legacy counter until they provide stable membership.
	automaticLeaseIDs atomic.Pointer[idutil.Generator]
	// leaseGeneration changes whenever leadership replaces or clears the active
	// snapshot. pendingLeases records the generation in which each ID was
	// reserved, so a delayed metadata commit cannot publish across that boundary.
	leaseGeneration uint64
	leases          map[int64]*leaseState
	// leaseIncarnations mirrors the immutable incarnation of each live lease for
	// attachment commits. It deliberately has an independent concurrency domain:
	// an authorized lease read holds leaseMu while a concurrent Put is allowed to
	// commit its value+attachment before waiting to publish the in-memory key
	// index. Taking leaseMu merely to encode that attachment would invert that
	// ordering and stall the commit.
	leaseIncarnations sync.Map // map[int64]string
	pendingLeases     map[int64]uint64
	keyLeaseIndex     map[string]int64
	// orphanSweepStop is non-nil while the leader-side orphaned-leased-key sweeper
	// goroutine is running; closed (and niled) when leadership is lost. Guarded by
	// leaseMu.
	orphanSweepStop     chan struct{}
	orphanSweepEpoch    uint64
	orphanSweepInterval time.Duration
	// leasePromotionExtension protects recovered leases from losing lifetime
	// during the leader-election window. Configured before Campaign starts.
	leasePromotionExtension time.Duration
	// leasedKeyCount mirrors len(keyLeaseIndex) for a lock-free fast path in
	// leaseIDForKey: the read path resolves an attached lease for every returned
	// KeyValue, and the overwhelmingly common case (a range over keys that hold
	// no lease) must not take leaseMu per key. Updated under leaseMu, read with
	// atomics.
	leasedKeyCount int64
	// leaseReady is false while a newly elected leader reloads the durable lease
	// snapshot. The stale follower map cannot answer definitive lease lookups.
	leaseReady      atomic.Bool
	leaseReadyEpoch atomic.Uint64
	// leaseTermCtx is the exact leadership lifecycle that installed the active
	// lease snapshot. Expiry/checkpoint timers capture it when they are created so
	// a blocked TiKV call is canceled before StopLeases withdraws that term.
	// Guarded by leaseMu.
	leaseTermCtx context.Context

	workerCtx    context.Context
	workerCancel context.CancelFunc
	workerMu     sync.Mutex
	workerWG     sync.WaitGroup
	workerClosed bool
	closeOnce    sync.Once
}

// newLeaseManager builds a leaseManager owned by srv; deps are read through srv.
func newLeaseManager(srv *RPCServer, initialID int64) *leaseManager {
	workerCtx, workerCancel := context.WithCancel(context.Background())
	manager := &leaseManager{
		srv:                 srv,
		leaseID:             initialID,
		leases:              make(map[int64]*leaseState),
		pendingLeases:       make(map[int64]uint64),
		keyLeaseIndex:       make(map[string]int64),
		orphanSweepInterval: orphanLeaseSweepInterval,
		leaseTermCtx:        context.Background(),
		workerCtx:           workerCtx,
		workerCancel:        workerCancel,
	}
	// Standalone/test servers do not run election callbacks. Bind their initial
	// snapshot to the epoch visible while the manager is constructed, just as a
	// leadership reload binds the replacement snapshot below.
	var epoch uint64
	if srv != nil && srv.peers != nil {
		epoch, _ = srv.peers.EpochAndLeadingFresh()
	}
	manager.leaseReadyEpoch.Store(epoch)
	manager.leaseReady.Store(true)
	return manager
}

func (m *leaseManager) startWorker(run func(context.Context)) bool {
	m.workerMu.Lock()
	defer m.workerMu.Unlock()
	if m.workerClosed {
		return false
	}
	m.workerWG.Add(1)
	go func() {
		defer m.workerWG.Done()
		run(m.workerCtx)
	}()
	return true
}

func (m *leaseManager) close() {
	m.closeOnce.Do(func() {
		m.workerMu.Lock()
		m.workerClosed = true
		m.workerCancel()
		m.workerMu.Unlock()
		m.stopLeases()
		m.workerWG.Wait()
	})
}

func (m *leaseManager) nextLeaseID() int64 {
	for {
		if generator := m.automaticLeaseIDs.Load(); generator != nil {
			// Etcd masks generated request IDs to positive int64 and retries zero.
			id := int64(generator.Next() & math.MaxInt64)
			if id != 0 {
				return id
			}
			continue
		}
		// etcd masks generated request IDs to positive int64 and retries zero.
		// Explicit lease IDs may use the full signed range, but they must never
		// force automatic allocation into negative IDs after counter overflow.
		id := atomic.AddInt64(&m.leaseID, 1) & math.MaxInt64
		if id != 0 {
			return id
		}
	}
}

func (m *leaseManager) configureAutomaticLeaseIDs(memberPrefix uint16, now time.Time) {
	m.automaticLeaseIDs.Store(idutil.NewGenerator(memberPrefix, now))
}
