package main

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// Test-only scheduling barrier. Never infer the Authenticate application index
// from a token suffix, and never change the production verifier's retry policy.
func waitForRestoredAuthAppliedIndex(ctx context.Context, entry uint64, applied func() uint64) error {
	if entry == 0 {
		return errors.New("missing Authenticate application index")
	}
	tick := time.NewTicker(time.Millisecond)
	defer tick.Stop()
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		if applied() >= entry {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-tick.C:
		}
	}
}

func TestRestoredAuthApplyBarrier(t *testing.T) {
	var applied atomic.Uint64
	applied.Store(41)
	require.Error(t, waitForRestoredAuthAppliedIndex(t.Context(), 0, applied.Load))
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	require.ErrorIs(t, waitForRestoredAuthAppliedIndex(ctx, 41, applied.Load), context.Canceled)
	ctx, cancel = context.WithTimeout(t.Context(), 20*time.Millisecond)
	defer cancel()
	require.ErrorIs(t, waitForRestoredAuthAppliedIndex(ctx, 42, applied.Load), context.DeadlineExceeded)
	ctx, cancel = context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	observed := make(chan struct{}, 1)
	result := make(chan error, 1)
	done := make(chan struct{})
	go func() {
		defer close(done)
		result <- waitForRestoredAuthAppliedIndex(ctx, 42, func() uint64 {
			value := applied.Load()
			select {
			case observed <- struct{}{}:
			default:
			}
			return value
		})
	}()
	defer func() { cancel(); <-done }()
	select {
	case <-observed:
	case <-ctx.Done():
		t.Fatal("barrier did not inspect applied index")
	}
	select {
	case <-result:
		t.Fatal("previous token index incorrectly released barrier")
	default:
	}
	applied.Store(42)
	require.NoError(t, <-result)
}
