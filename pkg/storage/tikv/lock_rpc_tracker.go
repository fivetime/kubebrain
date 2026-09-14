package tikv

import (
	"sync"
	"time"

	"github.com/kubewharf/kubebrain/pkg/storage"
	"github.com/tikv/client-go/v2/tikvrpc"
)

type lockRPCTrackerKey struct{}

// Each preparation/commit phase owns a distinct tracker. Closing preparation
// prevents delayed completions from being attributed to the commit phase.
type lockRPCTracker struct {
	mu          sync.Mutex
	closed      bool
	observation storage.LockRPCObservation
}

func (t *lockRPCTracker) observe(method tikvrpc.CmdType, elapsed time.Duration, err error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.closed || elapsed < 0 {
		return
	}
	var sample *storage.LockRPCSample
	switch method {
	case tikvrpc.CmdCheckTxnStatus:
		sample = &t.observation.CheckTxnStatus
	case tikvrpc.CmdResolveLock:
		sample = &t.observation.ResolveLock
	default:
		return
	}
	sample.Requests++
	sample.Duration += elapsed
	if err != nil {
		sample.TransportErrors++
	}
}

func (t *lockRPCTracker) finish() storage.LockRPCObservation {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.closed = true
	return t.observation
}
