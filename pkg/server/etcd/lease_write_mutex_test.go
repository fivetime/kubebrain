package etcd

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestLeaseWriteMutexCanceledAdmission(t *testing.T) {
	for _, shared := range []bool{false, true} {
		m := &leaseWriteMutex{}
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		acquire := m.LockContext
		if shared {
			acquire = m.RLockContext
		}
		require.ErrorIs(t, acquire(ctx), context.Canceled)
		m.Lock()
		ctx, cancel = context.WithCancel(context.Background())
		done := make(chan error, 1)
		go func() { done <- acquire(ctx) }()
		cancel()
		select {
		case err := <-done:
			m.Unlock()
			require.ErrorIs(t, err, context.Canceled)
		case <-time.After(time.Second):
			m.Unlock()
			<-done
			t.Fatal("canceled lease admission did not return")
		}
		next, stop := context.WithTimeout(context.Background(), time.Second)
		err := m.LockContext(next)
		stop()
		require.NoError(t, err)
		m.Unlock()
	}
}

func TestLeaseWriteMutexQueuedWriterBlocksNewReaders(t *testing.T) {
	var m leaseWriteMutex
	m.RLock()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- m.LockContext(ctx) }()
	// Detect an actually queued writer; no scheduler sleep is used to assume
	// queue admission. TryRLock must retain the keepalive fast-path contract.
	queued := false
	deadline := time.After(time.Second)
	for !queued {
		if m.TryRLock() {
			m.RUnlock()
		} else {
			queued = true
			break
		}
		select {
		case <-deadline:
			cancel()
			m.RUnlock()
			<-done
			t.Fatal("writer did not enter the queue")
		default:
			time.Sleep(time.Millisecond)
		}
	}
	cancel()
	err := <-done
	m.RUnlock()
	require.ErrorIs(t, err, context.Canceled)
	require.True(t, m.TryRLock(), "canceled writer must stop blocking new readers")
	m.RUnlock()
}
