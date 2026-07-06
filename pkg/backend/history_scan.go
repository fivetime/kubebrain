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
	"sync"

	proto "github.com/kubewharf/kubebrain-client/api/v2rpc"
)

// scanGroup deduplicates concurrent watch-history fallback scans of the same
// prefix. After a watch-cache reset — leader failover, or a watch-buffer
// overflow that calls watchCache.Reset() — the cache is cold, so a reconnect
// herd (an apiserver restart re-establishing every watch) has each watcher miss
// the ring and fall to a full-prefix storage scan. Bounded only by
// historyScanSem those N scans serialize into an O(N) storage stampede (#30).
//
// scanGroup collapses all in-flight scans of one prefix into a single shared
// scan whose result every waiter reuses read-only, turning the herd's storage
// cost into O(1). It is the classic singleflight pattern, hand-rolled rather
// than reusing golang.org/x/sync/singleflight because it needs per-caller ctx
// semantics that singleflight does not offer: a waiter returns immediately on
// its OWN ctx cancellation, and a scan that fails for the executor's ctx reason
// is retried by still-live waiters (see Do). Waiters share the result slice and
// the *proto.Event pointers, which the watch fan-out path only ever reads
// (filterEvents/catchUpEvents allocate fresh slices), so sharing is safe.
type scanGroup struct {
	mu sync.Mutex
	m  map[string]*scanCall
}

type scanCall struct {
	done       chan struct{}
	events     []*proto.Event
	coveredRev uint64
	err        error
}

// errHistoryScanAborted is the pessimistic pre-set result for an in-flight scan;
// it survives only if fn panics before setting a real result, so waiters see a
// failure instead of a silent empty scan.
var errHistoryScanAborted = errors.New("history scan aborted")

func newScanGroup() *scanGroup { return &scanGroup{m: make(map[string]*scanCall)} }

// Do runs fn for key unless an identical scan is already in flight, in which
// case it waits for and returns that scan's result. fn returns (events,
// coveredRev): coveredRev is the revision the scan is complete up to, so a
// waiter can tell whether the shared result covers far enough for its own
// from-now subscription (a scan snapshotted before the waiter subscribed may
// stop short and must not be reused blindly — see historyWatchEvents). The
// returned bool reports whether this caller shared an in-flight scan (true)
// instead of executing it (false).
//
// Cancellation safety: a waiter whose own ctx is cancelled returns immediately
// rather than blocking on a stranger's scan. And if the executing scan fails for
// a ctx reason (its caller disconnected mid-scan), waiters whose ctx is still
// alive re-execute under their own ctx — so one caller's cancellation never
// propagates a spurious failure to the others.
func (g *scanGroup) Do(ctx context.Context, key string, fn func(context.Context) ([]*proto.Event, uint64, error)) ([]*proto.Event, uint64, error, bool) {
	for {
		g.mu.Lock()
		if c, ok := g.m[key]; ok {
			g.mu.Unlock()
			select {
			case <-c.done:
				if c.err != nil && isCtxErr(c.err) && ctx.Err() == nil {
					// The executor bailed for its own ctx, not a real storage
					// failure; our ctx is still good, so retry as a fresh caller.
					continue
				}
				return c.events, c.coveredRev, c.err, true
			case <-ctx.Done():
				return nil, 0, ctx.Err(), true
			}
		}
		// err starts non-nil so that if fn panics, waiters woken by the deferred
		// close(done) see a failure (and propagate it) rather than a silent
		// (nil, nil) "empty result" — fn's normal return overwrites it.
		c := &scanCall{done: make(chan struct{}), err: errHistoryScanAborted}
		g.m[key] = c
		g.mu.Unlock()

		// Run fn in a closure with deferred cleanup so a panic in fn (storage
		// decode, iterator) still unblocks waiters and removes the map entry —
		// otherwise the orphaned entry would strand every future identical scan.
		func() {
			defer func() {
				g.mu.Lock()
				delete(g.m, key)
				g.mu.Unlock()
				close(c.done)
			}()
			c.events, c.coveredRev, c.err = fn(ctx)
		}()
		return c.events, c.coveredRev, c.err, false
	}
}

func isCtxErr(err error) bool {
	return errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded)
}
