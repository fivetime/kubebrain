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
