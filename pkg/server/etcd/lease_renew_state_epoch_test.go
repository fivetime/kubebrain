package etcd

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
)

// Done is evaluated by the state mutex's select after its entry epoch check.
// Signal that exact boundary, avoiding a sleep-based guess about the waiter.
type leaseRenewStateWaitContext struct {
	context.Context
	once    sync.Once
	waiting chan struct{}
}

func (c *leaseRenewStateWaitContext) Done() <-chan struct{} {
	c.once.Do(func() { close(c.waiting) })
	return c.Context.Done()
}

func TestLeaseRenewRejectsDemotionAtStateAdmission(t *testing.T) {
	for _, tc := range []struct {
		name            string
		fast, reelected bool
	}{
		{name: "ordinary"},
		{name: "teardown-bypass", fast: true},
		{name: "ordinary-new-epoch", reelected: true},
		{name: "teardown-bypass-new-epoch", fast: true, reelected: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fast := tc.fast
			s, closeFn := newTestRPCServer(t)
			defer closeFn()
			const id = int64(78326)
			_, err := s.LeaseGrant(context.Background(), &etcdserverpb.LeaseGrantRequest{ID: id, TTL: 300})
			require.NoError(t, err)
			var leading atomic.Bool
			leading.Store(true)
			var currentEpoch atomic.Uint64
			currentEpoch.Store(7)
			s.peers = testPeerService{isLeaderFn: leading.Load, epochFn: func() (uint64, bool) {
				return currentEpoch.Load(), leading.Load()
			}}
			epoch, fresh := s.peers.EpochAndLeadingFresh()
			require.True(t, fresh)
			unlockCheckpoint := s.lockLeaseCheckpoint(id)
			if fast {
				s.leaseWriteMu.Lock()
			} else {
				s.leaseWriteMu.RLock()
			}
			var releaseOnce sync.Once
			release := func() {
				releaseOnce.Do(func() {
					if fast {
						s.leaseWriteMu.Unlock()
					} else {
						s.leaseWriteMu.RUnlock()
					}
					unlockCheckpoint()
				})
			}
			defer release()
			s.leaseMu.Lock()
			before := s.leases[id].deadline
			var stateOnce sync.Once
			releaseState := func() { stateOnce.Do(s.leaseMu.Unlock) }
			defer releaseState()
			base, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			ctx := &leaseRenewStateWaitContext{Context: base, waiting: make(chan struct{})}
			type result struct {
				ttl     int64
				renewed bool
				err     error
			}
			done := make(chan result, 1)
			go func() {
				if fast {
					ttl, renewed := s.refreshUncheckpointedLeaseDuringUnrelatedTeardown(ctx, id, epoch)
					release()
					done <- result{ttl: ttl, renewed: renewed}
					return
				}
				ttl, err := s.refreshLeaseHoldingLocks(ctx, id, epoch, s.leaseWriteMu.RUnlock, s.leaseWriteMu.RLock, nil, release)
				done <- result{ttl: ttl, err: err}
			}()
			select {
			case <-ctx.waiting:
			case <-base.Done():
				releaseState()
				<-done
				t.Fatal("renewal never reached state admission")
			}
			if tc.reelected {
				currentEpoch.Add(1)
			} else {
				leading.Store(false)
			}
			releaseState()
			got := <-done
			if fast {
				require.False(t, got.renewed)
			} else {
				require.ErrorIs(t, got.err, errLeaseDemotedDuringRenew)
			}
			require.Zero(t, got.ttl)
			s.leaseMu.Lock()
			after := s.leases[id].deadline
			s.leaseMu.Unlock()
			require.Equal(t, before, after, "demoted renewal must not extend its private deadline")
		})
	}
}
