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
	"bytes"
	"context"
	"errors"
	"sync/atomic"

	"github.com/kubewharf/kubebrain/pkg/backend/election"
	"github.com/kubewharf/kubebrain/pkg/storage"
)

// ErrLeadershipFenced is returned when a write is rejected at commit time because
// this node's leadership term changed (or its lease freshness lapsed) between the
// moment the write was admitted and the moment it was about to be committed. It is
// mapped to codes.Unavailable at the RPC boundary so the etcd client retries the
// request against the current leader instead of silently losing the write
// (FINDING #39 write fencing).
var ErrLeadershipFenced = errors.New("write rejected: leadership changed during commit")

type leadershipFencedStorage struct {
	storage.KvStorage
	backend *backend
	shard   atomic.Uint64
}

func (s *leadershipFencedStorage) UnwrapKvStorage() storage.KvStorage { return s.KvStorage }

func (s *leadershipFencedStorage) BeginBatchWrite() storage.BatchWrite {
	return &leadershipFencedBatch{
		BatchWrite: s.KvStorage.BeginBatchWrite(),
		storage:    s,
		shard:      s.shard.Add(1) - 1,
	}
}

type leadershipFencedBatch struct {
	storage.BatchWrite
	storage *leadershipFencedStorage
	shard   uint64
}

func (b *leadershipFencedBatch) Commit(ctx context.Context) error {
	if _, admitted := leadershipEpochFromContext(ctx); !admitted {
		return b.BatchWrite.Commit(ctx)
	}
	h, _ := b.storage.backend.fenceFn.Load().(fenceHolder)
	provider, ok := b.storage.backend.election.GetResourceLock().(election.StorageFenceTokenProvider)
	if h.fn == nil || !ok {
		return b.BatchWrite.Commit(ctx)
	}
	key, token, ok := provider.StorageFenceToken(b.shard)
	if !ok {
		// Direct-constructed tests may install an in-memory fence without ever
		// campaigning. Production cannot publish fresh leadership before its lock
		// Update installs the token, so preserve those test/single-node paths.
		return b.BatchWrite.Commit(ctx)
	}
	// Writing the same token makes this shard part of the transaction's write
	// conflict set. A successor changes every shard atomically with acquisition.
	b.BatchWrite.CAS(key, token, token, 0)
	err := b.BatchWrite.Commit(ctx)
	if !errors.Is(err, storage.ErrCASFailed) {
		return err
	}
	var conflict *storage.Conflict
	if errors.As(err, &conflict) && bytes.Equal(conflict.Key, key) {
		b.storage.backend.metricCli.EmitCounter("write.fence.reject", 1)
		return ErrLeadershipFenced
	}
	// TiKV/Badger can report commit-time write conflicts without the key. Read
	// the guard only on that error path to distinguish a leadership conflict
	// from an ordinary user-key CAS retry.
	current, getErr := b.storage.KvStorage.Get(ctx, key)
	if errors.Is(getErr, storage.ErrKeyNotFound) || (getErr == nil && !bytes.Equal(current, token)) {
		b.storage.backend.metricCli.EmitCounter("write.fence.reject", 1)
		return ErrLeadershipFenced
	}
	return err
}

// leadershipEpochKey is the context key carrying the leadership epoch a write was
// admitted under, from the RPC gate down to fenceAdmit.
type leadershipEpochKey struct{}

// WithLeadershipEpoch stamps the admit-time leadership epoch onto the context so
// fenceAdmit re-checks it just before the batch is opened. The write RPC
// gate (kv.go) captures the epoch via LeaderElection.EpochAndLeadingFresh and
// threads it through with this helper.
func WithLeadershipEpoch(ctx context.Context, epoch uint64) context.Context {
	return context.WithValue(ctx, leadershipEpochKey{}, epoch)
}

func leadershipEpochFromContext(ctx context.Context) (uint64, bool) {
	epoch, ok := ctx.Value(leadershipEpochKey{}).(uint64)
	return epoch, ok
}

// withCurrentLeadershipEpoch captures the current term for internal maintenance
// callers that did not pass through an RPC admission gate (for example the
// auto-compactor). Direct/single-node backends have no registered fence and
// remain unrestricted.
func (b *backend) withCurrentLeadershipEpoch(ctx context.Context) (context.Context, error) {
	if _, ok := leadershipEpochFromContext(ctx); ok {
		return ctx, nil
	}
	h, _ := b.fenceFn.Load().(fenceHolder)
	if h.fn == nil {
		return ctx, nil
	}
	epoch, fresh := h.fn()
	if !fresh {
		return nil, ErrLeadershipFenced
	}
	return WithLeadershipEpoch(ctx, epoch), nil
}

// fenceHolder wraps the leadership-epoch source for storage in an atomic.Value
// (which needs a single concrete type and cannot hold a bare func or nil). A zero
// holder (fn == nil) means the fence is disabled.
type fenceHolder struct {
	fn func() (uint64, bool)
}

// SetLeadershipFence registers the leadership-epoch source used by fenceAdmit.
// fn returns the node's current leadership epoch and whether it is still safely
// leading (leader AND lease-fresh). It is wired to
// LeaderElection.EpochAndLeadingFresh by the server layer. When unset (single
// node, direct-constructed test backends) the fence is disabled and writes commit
// unconditionally (fail-open). Stored atomically because the event collector's
// stall watchdog reads it concurrently with a leadership-change re-register.
func (b *backend) SetLeadershipFence(fn func() (uint64, bool)) {
	b.fenceFn.Store(fenceHolder{fn: fn})
}

// leadingFresh reports whether this node is safely leading, or true when no fence
// is registered (single-node / direct-constructed test backends). It gates
// leader-only maintenance such as the event-collector stall watchdog, which must
// not fire on a follower whose collector idles while peer-sync advances the
// revision.
func (b *backend) leadingFresh() bool {
	h, _ := b.fenceFn.Load().(fenceHolder)
	if h.fn == nil {
		return true
	}
	_, fresh := h.fn()
	return fresh
}

// fenceAdmit is the write fence's in-memory re-check, called immediately before
// a data batch is opened. If a fence is registered and the write carries
// an admit-time epoch, it re-loads the current leadership epoch/freshness and
// rejects the write (with ErrLeadershipFenced) when this node has left the term
// it was admitted under. This closes the deal->commit TOCTOU window in which a
// deposed leader would otherwise commit a write at a revision the new leader's
// collector has already advanced past — a committed-yet-unwatched write
// (split-brain, FINDING #39).
//
// leadershipFencedStorage complements this fast rejection with a sharded token
// CAS inside the storage transaction itself. The successor rotates every shard
// atomically with election acquisition, closing the remaining window where the
// storage Commit call has begun but network/2PC completion is still blocked.
//
// It fails open when no fence is registered or the context carries no epoch
// (e.g. internal/background writes), so single-node and test paths are unchanged.
func (b *backend) fenceAdmit(ctx context.Context) error {
	h, _ := b.fenceFn.Load().(fenceHolder)
	if h.fn == nil {
		return nil
	}
	admitEpoch, ok := leadershipEpochFromContext(ctx)
	if !ok {
		return nil
	}
	curEpoch, leadingFresh := h.fn()
	if !leadingFresh || curEpoch != admitEpoch {
		b.metricCli.EmitCounter("write.fence.reject", 1)
		return ErrLeadershipFenced
	}
	return nil
}
