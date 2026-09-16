package backend

import (
	"context"
	"math"
	"sync"

	"golang.org/x/sync/semaphore"
)

// logicalWriteMutex is a zero-value-ready reader/writer barrier. A queued
// exclusive acquisition prevents new readers from bypassing it. Unlike an
// RWMutex, exclusive admission can be canceled without leaving a goroutine
// waiting to acquire a lock after the request has returned.
// It must not be copied after first use; recursive acquisition is not supported.
type logicalWriteMutex struct {
	once sync.Once
	sem  *semaphore.Weighted
}

func (m *logicalWriteMutex) semaphore() *semaphore.Weighted {
	m.once.Do(func() { m.sem = semaphore.NewWeighted(math.MaxInt64) })
	return m.sem
}

func (m *logicalWriteMutex) LockContext(ctx context.Context) error {
	return m.semaphore().Acquire(ctx, math.MaxInt64)
}

func (m *logicalWriteMutex) Lock() {
	// Background cannot be canceled, so acquisition cannot fail.
	_ = m.LockContext(context.Background())
}

func (m *logicalWriteMutex) Unlock() { m.semaphore().Release(math.MaxInt64) }

func (m *logicalWriteMutex) RLock() {
	_ = m.semaphore().Acquire(context.Background(), 1)
}

func (m *logicalWriteMutex) RUnlock() { m.semaphore().Release(1) }
