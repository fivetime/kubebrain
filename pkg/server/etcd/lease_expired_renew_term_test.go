package etcd

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
)

func TestExpiredLeaseRenewStopsWithLeadershipTerm(t *testing.T) {
	s, closeFn := newTestRPCServer(t)
	defer closeFn()
	const id int64 = 78329
	_, err := s.LeaseGrant(context.Background(), &etcdserverpb.LeaseGrantRequest{ID: id, TTL: 300})
	require.NoError(t, err)
	term, endTerm := context.WithCancel(context.Background())
	defer endTerm()
	s.leaseMu.Lock()
	st := s.leases[id]
	st.timer.Stop()
	st.deadline = time.Now().Add(-time.Second)
	s.leaseTermCtx = term
	s.leaseMu.Unlock()
	epoch, fresh := s.peers.EpochAndLeadingFresh()
	require.True(t, fresh)
	ctx, cancel := context.WithCancel(context.Background())
	unlocked := make(chan struct{})
	done := make(chan error, 1)
	joined := make(chan struct{})
	go func() {
		defer close(joined)
		_, err := s.refreshLeaseHoldingLocks(ctx, id, epoch, func() {}, func() {}, nil, func() { close(unlocked) })
		done <- err
	}()
	defer func() { cancel(); <-joined }()
	// The expired branch releases its admission locks before waiting. The
	// callback observes this exact boundary without relying on a sleep.
	select {
	case <-unlocked:
	case <-time.After(time.Second):
		t.Fatal("renewal did not reach expired-lease wait")
	}
	endTerm()
	select {
	case err := <-done:
		require.ErrorIs(t, err, errLeaseDemotedDuringRenew)
	case <-time.After(time.Second):
		t.Fatal("expired renewal remained blocked after leadership term ended")
	}
	select {
	case <-st.revoked:
		t.Fatal("demotion must not report completed lease revocation")
	default:
	}
}

func TestExpiredLeaseRenewStopsBeforeDelayedDemotionCallback(t *testing.T) {
	for _, change := range []string{"freshness", "epoch"} {
		t.Run(change, func(t *testing.T) {
			s, closeFn := newTestRPCServer(t)
			defer closeFn()
			const id int64 = 78331
			_, err := s.LeaseGrant(context.Background(), &etcdserverpb.LeaseGrantRequest{ID: id, TTL: 300})
			require.NoError(t, err)
			var fresh atomic.Bool
			fresh.Store(true)
			var epoch atomic.Uint64
			epoch.Store(7)
			s.peers = testPeerService{
				isLeaderFn: func() bool { return true }, // Stopped-leading callback is delayed.
				epochFn:    func() (uint64, bool) { return epoch.Load(), fresh.Load() },
			}
			term, endTerm := context.WithCancel(context.Background())
			defer endTerm()
			s.leaseMu.Lock()
			st := s.leases[id]
			st.timer.Stop()
			st.deadline = time.Now().Add(-time.Second)
			s.leaseTermCtx = term
			s.leaseMu.Unlock()
			ctx, cancel := context.WithCancel(context.Background())
			unlocked := make(chan struct{})
			done := make(chan error, 1)
			joined := make(chan struct{})
			go func() {
				defer close(joined)
				_, err := s.refreshLeaseHoldingLocks(ctx, id, 7, func() {}, func() {}, nil, func() { close(unlocked) })
				done <- err
			}()
			defer func() { cancel(); <-joined }()
			select {
			case <-unlocked:
			case <-time.After(time.Second):
				t.Fatal("renewal did not reach expired wait")
			}
			if change == "freshness" {
				fresh.Store(false)
			} else {
				epoch.Add(1)
			}
			select {
			case err := <-done:
				require.ErrorIs(t, err, errLeaseDemotedDuringRenew)
			case <-time.After(time.Second):
				t.Fatal("expired renewal waited for delayed lifecycle cancellation")
			}
			require.NoError(t, term.Err(), "test must not cancel the leadership lifecycle")
			select {
			case <-st.revoked:
				t.Fatal("fencing must not pretend durable revocation completed")
			default:
			}
		})
	}
}

func TestExpiredLeaseKeepAliveRoutesAfterTermEnds(t *testing.T) {
	for _, proxy := range []bool{false, true} {
		name := "reject"
		if proxy {
			name = "forward"
		}
		t.Run(name, func(t *testing.T) {
			s, closeFn := newTestRPCServer(t)
			defer closeFn()
			const id int64 = 78330
			_, err := s.LeaseGrant(context.Background(), &etcdserverpb.LeaseGrantRequest{ID: id, TTL: 300})
			require.NoError(t, err)
			term, endTerm := context.WithCancel(context.Background())
			defer endTerm()
			observed := &leaseRenewStateWaitContext{Context: term, waiting: make(chan struct{})}
			s.leaseMu.Lock()
			st := s.leases[id]
			st.timer.Stop()
			st.deadline = time.Now().Add(-time.Second)
			s.leaseTermCtx = observed
			s.leaseMu.Unlock()
			var leading atomic.Bool
			leading.Store(true)
			var forwarded atomic.Int32
			header := proxiedResponseHeader(s, int64(s.backend.GetCurrentRevision()))
			s.peers = testPeerService{isLeaderFn: leading.Load, proxyEnabled: proxy,
				leaseKeepAliveFn: func(_ context.Context, req *etcdserverpb.LeaseKeepAliveRequest) (*etcdserverpb.LeaseKeepAliveResponse, error) {
					forwarded.Add(1)
					return &etcdserverpb.LeaseKeepAliveResponse{Header: header, ID: req.ID, TTL: 37}, nil
				}}
			ctx, cancel := context.WithCancel(context.Background())
			stream := &fakeLeaseKeepAliveServer{ctx: ctx, requests: []*etcdserverpb.LeaseKeepAliveRequest{{ID: id}}}
			done := make(chan error, 1)
			joined := make(chan struct{})
			go func() {
				defer close(joined)
				// Exercise the complete service worker, not the renewal helper;
				// no transport goroutine is needed for this routing contract.
				done <- s.leaseKeepAlive(stream)
			}()
			defer func() { cancel(); <-joined }()
			select {
			case <-observed.waiting:
			case <-time.After(time.Second):
				t.Fatal("keepalive did not enter the expired lease wait")
			}
			leading.Store(false)
			endTerm()
			select {
			case err := <-done:
				if proxy {
					require.NoError(t, err)
					require.Equal(t, int32(1), forwarded.Load())
					require.Len(t, stream.sent, 1)
					require.Equal(t, int64(37), stream.sent[0].TTL)
					require.Equal(t, id, stream.sent[0].ID)
				} else {
					requireLeaseFollowerUnavailable(t, err, "lease keepalive error addr is test-peer leader test-peer")
					require.Empty(t, stream.sent)
					require.Zero(t, forwarded.Load())
				}
			case <-time.After(time.Second):
				t.Fatal("keepalive did not route after term cancellation")
			}
		})
	}
}
