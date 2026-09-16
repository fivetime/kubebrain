package etcd

import (
	"context"
	"math"
	"sync"

	"golang.org/x/sync/semaphore"
)

// leaseWriteMutex preserves writer-preferred lease binding exclusion while
// allowing request admission to abandon its queue position on cancellation.
// Background lease state transitions retain the blocking Lock/RLock API,
// including the unlock/relock callbacks used by checkpoint persistence.
// The zero value is ready; it must not be copied after first use.
type leaseWriteMutex struct {
	once sync.Once
	sem  *semaphore.Weighted
}

func (m *leaseWriteMutex) semaphore() *semaphore.Weighted {
	m.once.Do(func() { m.sem = semaphore.NewWeighted(math.MaxInt64) })
	return m.sem
}

func (m *leaseWriteMutex) LockContext(ctx context.Context) error {
	return m.semaphore().Acquire(ctx, math.MaxInt64)
}

func (m *leaseWriteMutex) RLockContext(ctx context.Context) error {
	return m.semaphore().Acquire(ctx, 1)
}

func (m *leaseWriteMutex) Lock()          { _ = m.LockContext(context.Background()) }
func (m *leaseWriteMutex) RLock()         { _ = m.RLockContext(context.Background()) }
func (m *leaseWriteMutex) Unlock()        { m.semaphore().Release(math.MaxInt64) }
func (m *leaseWriteMutex) RUnlock()       { m.semaphore().Release(1) }
func (m *leaseWriteMutex) TryRLock() bool { return m.semaphore().TryAcquire(1) }
