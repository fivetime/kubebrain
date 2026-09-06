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

	"github.com/kubewharf/kubebrain/pkg/storage"
)

// hashKVFlightKey identifies the logical snapshot a HashKV caller is allowed to
// share. observedCurrent is deliberately sampled when each caller arrives. A
// caller that arrives after an acknowledged write observes a newer revision and
// cannot join a scan that started before that write. Protected snapshots also
// include their immutable engine timestamp and complete checkpoint identity.
type hashKVFlightKey struct {
	requestedRevision         int64
	observedCurrent           uint64
	snapshotTimestamp         uint64
	checkpointRevision        uint64
	checkpointCompactRevision uint64
	scanBatchSize             int
	snapshotPinned            bool
	protectedSnapshot         bool
	checkpointPinned          bool
	iteratorFallback          bool
	scanBatchConfigured       bool
}

type hashKVFlightGroup struct {
	mu    sync.Mutex
	calls map[hashKVFlightKey]*hashKVFlightCall
}

type hashKVFlightCall struct {
	done    chan struct{}
	result  HashKVResult
	err     error
	waiters int
}

var errHashKVFlightAborted = errors.New("HashKV flight aborted")

func (b *backend) newHashKVFlightKey(ctx context.Context, revision int64) hashKVFlightKey {
	timestamp, snapshotPinned := storage.SnapshotTimestampFromContext(ctx)
	checkpoint, checkpointPinned := SerializableCheckpointFromContext(ctx)
	scanBatchSize, scanBatchConfigured := storage.ScanBatchSizeFromContext(ctx)
	return hashKVFlightKey{
		requestedRevision:         revision,
		observedCurrent:           b.GetCurrentRevision(),
		snapshotTimestamp:         timestamp,
		checkpointRevision:        checkpoint.Revision,
		checkpointCompactRevision: checkpoint.CompactRevision,
		scanBatchSize:             scanBatchSize,
		snapshotPinned:            snapshotPinned,
		protectedSnapshot:         storage.ProtectedSnapshotFromContext(ctx),
		checkpointPinned:          checkpointPinned,
		iteratorFallback:          storage.SnapshotIteratorFallbackFromContext(ctx),
		scanBatchConfigured:       scanBatchConfigured,
	}
}

// join returns executor=true to exactly one caller for a key. Complete must be
// called by that executor even when its scan panics; err starts pessimistically
// so waiters can never mistake an abandoned call for a successful empty hash.
func (g *hashKVFlightGroup) join(key hashKVFlightKey) (call *hashKVFlightCall, executor bool) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if call = g.calls[key]; call != nil {
		call.waiters++
		return call, false
	}
	if g.calls == nil {
		g.calls = make(map[hashKVFlightKey]*hashKVFlightCall)
	}
	call = &hashKVFlightCall{
		done: make(chan struct{}), err: errHashKVFlightAborted, waiters: 1,
	}
	g.calls[key] = call
	return call, true
}

func (g *hashKVFlightGroup) complete(
	key hashKVFlightKey, call *hashKVFlightCall, result HashKVResult, err error,
) {
	g.mu.Lock()
	call.result, call.err = result, err
	if g.calls[key] == call {
		delete(g.calls, key)
	}
	close(call.done)
	g.mu.Unlock()
}

func (call *hashKVFlightCall) wait(ctx context.Context) (HashKVResult, error) {
	select {
	case <-call.done:
		return call.result, call.err
	case <-ctx.Done():
		return HashKVResult{}, ctx.Err()
	}
}
