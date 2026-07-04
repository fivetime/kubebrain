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
	"errors"
)

// ErrLeadershipFenced is returned when a write is rejected at commit time because
// this node's leadership term changed (or its lease freshness lapsed) between the
// moment the write was admitted and the moment it was about to be committed. It is
// mapped to codes.Unavailable at the RPC boundary so the etcd client retries the
// request against the current leader instead of silently losing the write
// (FINDING #39 write fencing).
var ErrLeadershipFenced = errors.New("write rejected: leadership changed during commit")

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

// fenceAdmit is the write fence's re-check, called immediately before a data
// batch is opened and committed. If a fence is registered and the write carries
// an admit-time epoch, it re-loads the current leadership epoch/freshness and
// rejects the write (with ErrLeadershipFenced) when this node has left the term
// it was admitted under. This closes the deal->commit TOCTOU window in which a
// deposed leader would otherwise commit a write at a revision the new leader's
// collector has already advanced past — a committed-yet-unwatched write
// (split-brain, FINDING #39).
//
// It is called just before storage.BeginBatchWrite rather than wrapping Commit
// because a storage batch cannot be abandoned safely (memkv holds a global lock
// from BeginBatchWrite until Commit; the optimistic-txn engines hold an open
// transaction). Only a cheap, non-blocking client-side batch build separates
// this check from the commit's storage I/O, so the fencing window is unchanged.
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
