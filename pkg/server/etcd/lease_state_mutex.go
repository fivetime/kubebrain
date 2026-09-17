package etcd

import (
	"context"
	"sync"
)

// leaseStateMutex provides exclusive state access with cancellable admission.
// Unlike the weighted binding lock it needs no per-waiter heap allocation.
// The zero value is ready; do not copy after first use.
type leaseStateMutex struct {
	once sync.Once
	held chan struct{}
}

func (m *leaseStateMutex) initialize() {
	m.once.Do(func() { m.held = make(chan struct{}, 1) })
}

func (m *leaseStateMutex) Lock() {
	m.initialize()
	m.held <- struct{}{}
}

func (m *leaseStateMutex) LockContext(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	m.initialize()
	select {
	case m.held <- struct{}{}:
		// Cancellation and availability may win the select simultaneously.
		if err := ctx.Err(); err != nil {
			m.Unlock()
			return err
		}
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (m *leaseStateMutex) Unlock() {
	m.initialize()
	select {
	case <-m.held:
	default:
		panic("unlock of unlocked lease state mutex")
	}
}
