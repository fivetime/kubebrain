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
	"sync"
	"time"

	"k8s.io/klog/v2"
)

// commitWaitBackstop bounds how long a write blocks waiting for the committed
// revision to reach it before giving up and answering anyway. It only fires if
// the event collector stalls (in which case the whole cluster is degraded);
// answering with the old early-ACK behavior keeps writes available instead of
// timing them out — the write itself is already durable.
const commitWaitBackstop = 3 * time.Second

// commitNotify wakes waiters when the committed revision advances. advance is
// called from SetCurrentRevision — the single funnel every committed-revision
// bump goes through (collector, overflow reset, compact bump, follower sync) —
// so waiters never miss a wake-up. One channel close per bump; the collector
// commits in batches, so this is a few thousand closes per second at most.
type commitNotify struct {
	mu sync.Mutex
	ch chan struct{}
}

func newCommitNotify() *commitNotify {
	return &commitNotify{ch: make(chan struct{})}
}

// advance wakes all current waiters. The caller must have already made the new
// committed revision visible (tso.Commit) BEFORE calling advance: waiters
// re-check the revision after each wake, so visibility-then-wake means a waiter
// can never sleep through the revision it waits for.
func (c *commitNotify) advance() {
	c.mu.Lock()
	close(c.ch)
	c.ch = make(chan struct{})
	c.mu.Unlock()
}

func (c *commitNotify) waitChan() <-chan struct{} {
	c.mu.Lock()
	ch := c.ch
	c.mu.Unlock()
	return ch
}

// waitCommittedRevision blocks until the committed (read-visible) revision
// reaches revision, honoring ctx and a stall backstop.
//
// Why: a write is ACKed to the client only after this returns, so a client
// that writes then immediately reads at rev=0 is guaranteed to see its own
// write (etcd's apply-then-ack semantics). Without it, the committed watermark
// — advanced asynchronously by the ordered event collector — lagged the
// storage ACK by tens of revisions (~50ms under load), and any
// write-then-read (a controller confirming its own update, an apiserver
// quorum List racing a just-ACKed write) read a world that did not yet
// contain the write: the "read version X is not as new as written version Y"
// staleness storms seen at 500k objects (#35).
//
// On ctx cancellation or backstop expiry it returns without error: the write
// is already durable, and failing it would make the client retry a committed
// write (a CAS conflict at best). Both exits are counted for observability.
func (b *backend) waitCommittedRevision(ctx context.Context, revision uint64) {
	if revision == 0 || b.tso.GetRevision() >= revision {
		return
	}
	start := time.Now()
	backstop := time.NewTimer(commitWaitBackstop)
	defer backstop.Stop()
	for {
		// Grab the wait channel BEFORE re-checking: an advance between the check
		// and the wait closes the grabbed channel, so the wake-up cannot be lost.
		ch := b.commitNotify.waitChan()
		if b.tso.GetRevision() >= revision {
			b.metricCli.EmitHistogram("write.commit_wait", time.Since(start).Milliseconds())
			return
		}
		select {
		case <-ch:
		case <-ctx.Done():
			b.metricCli.EmitCounter("write.commit_wait.ctx_done", 1)
			return
		case <-backstop.C:
			b.metricCli.EmitCounter("write.commit_wait.backstop", 1)
			klog.ErrorS(nil, "commit wait backstop fired; collector stalled?",
				"revision", revision, "committed", b.tso.GetRevision(), "waited", time.Since(start))
			return
		}
	}
}
