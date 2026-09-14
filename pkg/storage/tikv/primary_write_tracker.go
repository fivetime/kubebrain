package tikv

import (
	"bytes"
	"math"
	"sync"
	"time"

	"github.com/kubewharf/kubebrain/pkg/storage"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
)

// primaryWriteSample is the slowest successful, matching primary Commit RPC
// completed before a batch's synchronous Commit returns. It is not the whole
// transaction duration, a secondary-cleanup duration, or a sum of server stages.
// Details preserves missing and invalid raw fields; they must not become zeros
// in duration histograms. No key, timestamp, address, or error text is exported.
type primaryWriteSample = storage.PrimaryWriteObservation

type primaryWriteTrackerKey struct{}

// primaryWriteTracker belongs to exactly one storage batch. SDK requests can
// race with each other and with finish; late/background observations are ignored.
// A contradictory transaction identity invalidates primary Commit selection,
// not the context-scoped Prewrite transport counts. Identity is learned before
// sending Prewrite, never from its result.
type primaryWriteTracker struct {
	mu        sync.Mutex
	start     uint64
	primary   []byte
	invalid   bool
	closed    bool
	sample    primaryWriteSample
	prewrites storage.PrewriteRPCObservation
}

func (t *primaryWriteTracker) observePrewrite(response *kvrpcpb.PrewriteResponse, err error, elapsed time.Duration) {
	if elapsed < 0 {
		return
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.closed {
		return
	}
	s := &t.prewrites
	s.Requests++
	s.Duration += elapsed
	if elapsed > s.MaxDuration {
		s.MaxDuration = elapsed
	}
	switch {
	case err != nil:
		s.TransportErrors++
	case response == nil:
		s.MissingResponses++
	case response.RegionError != nil:
		s.RegionErrors++
	case len(response.Errors) != 0:
		s.KeyErrors++
	}
}

func (t *primaryWriteTracker) prewriteSnapshot() storage.PrewriteRPCObservation {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.prewrites
}

func (t *primaryWriteTracker) register(prewrite *kvrpcpb.PrewriteRequest) {
	if prewrite == nil {
		return
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.closed || t.invalid {
		return
	}
	// Async primary cleanup may run before synchronous Commit returns. Key
	// matching alone cannot make it foreground work. Conservatively exclude
	// async-enabled attempts, even if the server later falls back to 2PC.
	if prewrite.UseAsyncCommit || prewrite.StartVersion == 0 || len(prewrite.PrimaryLock) == 0 ||
		(t.start != 0 && (t.start != prewrite.StartVersion || !bytes.Equal(t.primary, prewrite.PrimaryLock))) {
		t.invalid = true
		t.primary = nil
		t.sample = primaryWriteSample{}
		return
	}
	if t.start == 0 {
		t.start = prewrite.StartVersion
		t.primary = bytes.Clone(prewrite.PrimaryLock)
	}
}

func (t *primaryWriteTracker) observe(request *kvrpcpb.CommitRequest, response *kvrpcpb.CommitResponse, err error, elapsed time.Duration) {
	if request == nil || response == nil || err != nil || response.RegionError != nil || response.Error != nil || elapsed < 0 {
		return
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.closed || t.invalid || t.start == 0 || request.StartVersion != t.start {
		return
	}
	matched := false
	for _, key := range request.Keys {
		if bytes.Equal(key, t.primary) {
			matched = true
			break
		}
	}
	if !matched {
		return
	}
	count := t.sample.SuccessfulRPCs + 1
	if t.sample.SuccessfulRPCs > 0 && elapsed <= t.sample.RPC {
		t.sample.SuccessfulRPCs = count
		return
	}
	sample := primaryWriteSample{SuccessfulRPCs: count, RPC: elapsed, Details: "absent"}
	if detail := response.ExecDetailsV2; detail != nil {
		sample.Details = "exec_only"
		if write := detail.WriteDetail; write != nil {
			sample.Details = "write"
			// Use the same observed-field validity boundary as raw RPC metrics.
			for _, nanos := range []uint64{write.PersistLogNanos, write.RaftDbSyncLogNanos,
				write.CommitLogNanos, write.ApplyLogNanos, write.StoreBatchWaitNanos,
				write.ProposeSendWaitNanos, write.ApplyBatchWaitNanos, write.ProcessNanos, write.ThrottleNanos} {
				if nanos > math.MaxInt64 {
					sample.Details = "invalid_write"
					break
				}
			}
			if sample.Details == "write" {
				sample.PersistLog = time.Duration(write.PersistLogNanos)
				sample.RaftSync = time.Duration(write.RaftDbSyncLogNanos)
				sample.CommitLog = time.Duration(write.CommitLogNanos)
			}
		}
	}
	t.sample = sample
}

func (t *primaryWriteTracker) finish() primaryWriteSample {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.closed = true
	t.primary = nil
	return t.sample
}
