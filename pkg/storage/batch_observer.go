package storage

import (
	"context"
	"time"
)

// BatchCommitObservation describes one storage batch attempt, not one RPC.
// Prepare includes the batch callbacks (reads, guards and staging mutations).
// SDK durations, when available, are nested within Commit, not additive to it.
// No request data is added to the timing fields. Err is the original storage
// error; observers must classify it without logging its potentially private text.
type BatchCommitObservation struct {
	Begin, Prepare, Commit            time.Duration
	CommitAttempted                   bool
	HasWriteDetails                   bool
	Prewrite, CommitTS, PrimaryCommit time.Duration
	// PrewriteRegionGroups sums SDK prewrite Region groups across attempts,
	// including retries. It is not a distinct-Region count or a protocol verdict.
	PrewriteRegionGroups      int32
	PrimaryWrite              PrimaryWriteObservation
	HasLockRPCDetails         bool
	PrepareLocks, CommitLocks LockRPCObservation
	Err                       error
}

// LockRPCObservation counts transport calls carrying a batch phase context
// that finish before that phase closes. Unmarked and late calls are excluded.
// Durations sum RPC wall time (including overlaps), not logical lock wait.
// TransportErrors does not include key/Region errors in successful responses.
type LockRPCObservation struct {
	CheckTxnStatus, ResolveLock LockRPCSample
}

type LockRPCSample struct {
	Requests, TransportErrors uint64
	Duration                  time.Duration
}

// PrimaryWriteObservation selects the slowest successful primary Commit RPC
// observed before a synchronous batch Commit returns. Retries may produce more
// than one successful RPC. Async-enabled attempts are excluded, even on fallback.
// Details is absent, exec_only, write, or invalid_write; only write permits
// duration histograms. RPC and server stages overlap and must never be added.
// SuccessfulRPCs==0 means no selected sample, not a zero-duration operation.
type PrimaryWriteObservation struct {
	SuccessfulRPCs uint64
	Details        string
	RPC            time.Duration
	PersistLog     time.Duration
	RaftSync       time.Duration
	CommitLog      time.Duration
}

type batchCommitObserverKey struct{}

// WithBatchCommitObserver attaches optional synchronous diagnostics. Observers
// must be concurrency-safe, must not panic, and must not change storage state.
func WithBatchCommitObserver(ctx context.Context, observer func(BatchCommitObservation)) context.Context {
	return context.WithValue(ctx, batchCommitObserverKey{}, observer)
}

func BatchCommitObserverFromContext(ctx context.Context) func(BatchCommitObservation) {
	observer, _ := ctx.Value(batchCommitObserverKey{}).(func(BatchCommitObservation))
	return observer
}
