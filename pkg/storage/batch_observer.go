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
	Err                               error
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
