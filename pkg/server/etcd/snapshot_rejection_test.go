package etcd

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestSnapshotRejectionDiagnosticLimitsAreIndependentAndBounded(t *testing.T) {
	var d snapshotRejectionDiagnostics
	now := time.Now()
	for reason := snapshotRejectedCapture; reason < snapshotRejectionReasonCount; reason++ {
		require.True(t, d.allow(reason, now))
		require.False(t, d.allow(reason, now))
		require.False(t, d.allow(reason, now.Add(time.Second-time.Nanosecond)))
		require.True(t, d.allow(reason, now.Add(time.Second)))
	}
	require.False(t, d.allow(snapshotRejectionReasonCount, now))
	require.False(t, d.allow(snapshotRejectionReason(255), now))
}

func TestSnapshotRejectionDiagnosticConcurrentLimit(t *testing.T) {
	var d snapshotRejectionDiagnostics
	var accepted atomic.Int64
	var workers sync.WaitGroup
	now := time.Now()
	for range 100 {
		workers.Go(func() {
			if d.allow(snapshotRejectedCapture, now) {
				accepted.Add(1)
			}
		})
	}
	workers.Wait()
	require.Equal(t, int64(1), accepted.Load())
}
