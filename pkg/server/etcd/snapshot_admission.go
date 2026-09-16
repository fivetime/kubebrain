package etcd

import (
	"context"
	"sync"

	"golang.org/x/sync/semaphore"
)

// snapshotAdmission retains the single-artifact workspace bound, including the
// send phase. Waiting callers allocate no artifact and need no helper goroutine.
// Public request concurrency/rate admission remains a separate outer bound.
type snapshotAdmission struct {
	once sync.Once
	sem  *semaphore.Weighted
}

func (a *snapshotAdmission) semaphore() *semaphore.Weighted {
	a.once.Do(func() { a.sem = semaphore.NewWeighted(1) })
	return a.sem
}

func (a *snapshotAdmission) acquire(ctx context.Context) (waited bool, err error) {
	if err := ctx.Err(); err != nil {
		return false, err
	}
	if a.semaphore().TryAcquire(1) {
		return false, nil
	}
	return true, a.semaphore().Acquire(ctx, 1)
}

func (a *snapshotAdmission) release() { a.semaphore().Release(1) }
