package etcd

import (
	"context"
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
