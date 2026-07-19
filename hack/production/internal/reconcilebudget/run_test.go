package reconcilebudget

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestRunBoundsReconciliationAndPreservesResult(t *testing.T) {
	processed, err := Run(context.Background(), time.Second, func(context.Context) (int, error) {
		return 3, errors.New("partial failure")
	})
	require.Equal(t, 3, processed)
	require.ErrorContains(t, err, "partial failure")

	start := time.Now()
	processed, err = Run(context.Background(), 10*time.Millisecond, func(ctx context.Context) (int, error) {
		<-ctx.Done()
		return 1, ctx.Err()
	})
	require.Equal(t, 1, processed)
	require.ErrorIs(t, err, context.DeadlineExceeded)
	require.Less(t, time.Since(start), time.Second)
}

func TestRunPropagatesParentCancellation(t *testing.T) {
	parent, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := Run(parent, time.Hour, func(ctx context.Context) (int, error) {
		<-ctx.Done()
		return 0, ctx.Err()
	})
	require.ErrorIs(t, err, context.Canceled)
}

func TestRunRejectsInvalidConfiguration(t *testing.T) {
	_, err := Run(context.Background(), 0, func(context.Context) (int, error) { return 0, nil })
	require.ErrorContains(t, err, "timeout must be positive")
	_, err = Run(context.Background(), time.Second, nil)
	require.ErrorContains(t, err, "required")
}
