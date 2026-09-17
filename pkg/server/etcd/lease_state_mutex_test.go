package etcd

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestLeaseStateMutexCancellation(t *testing.T) {
	var m leaseStateMutex
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	require.ErrorIs(t, m.LockContext(ctx), context.Canceled)
	m.Lock()
	ctx, cancel = context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- m.LockContext(ctx) }()
	select {
	case err := <-done:
		m.Unlock()
		require.ErrorIs(t, err, context.DeadlineExceeded)
	case <-time.After(time.Second):
		m.Unlock()
		<-done
		t.Fatal("canceled state waiter did not retire")
	}
	require.NoError(t, m.LockContext(context.Background()))
	m.Unlock()
	require.Panics(t, m.Unlock)
}

func TestLeaseStateMutexExclusive(t *testing.T) {
	var m leaseStateMutex
	var group sync.WaitGroup
	value := 0
	for worker := 0; worker < 8; worker++ {
		group.Add(1)
		go func() {
			defer group.Done()
			for i := 0; i < 1000; i++ {
				if i%2 == 0 {
					m.Lock()
				} else if err := m.LockContext(context.Background()); err != nil {
					t.Error(err)
					return
				}
				value++
				m.Unlock()
			}
		}()
	}
	group.Wait()
	require.Equal(t, 8000, value)
}

// Cancel while LockContext evaluates its select operands: the initial Err
// check has passed, and both cancellation and an available lock are ready.
// This uses a real cancellable context, without scheduler-dependent sleeps.
type leaseStateCancelOnDoneContext struct {
	context.Context
	cancel context.CancelFunc
}

func (c leaseStateCancelOnDoneContext) Done() <-chan struct{} {
	c.cancel()
	return c.Context.Done()
}

func TestLeaseStateMutexCancellationAtAdmission(t *testing.T) {
	var m leaseStateMutex
	for i := 0; i < 1000; i++ {
		ctx, cancel := context.WithCancel(context.Background())
		err := m.LockContext(leaseStateCancelOnDoneContext{Context: ctx, cancel: cancel})
		cancel()
		if err == nil {
			m.Unlock()
			t.Fatal("admitted a waiter whose context was canceled before selection")
		}
		require.ErrorIs(t, err, context.Canceled)
		follow, stop := context.WithTimeout(context.Background(), time.Second)
		err = m.LockContext(follow)
		stop()
		require.NoError(t, err, "cancellation must release any acquired ownership")
		m.Unlock()
	}
}
