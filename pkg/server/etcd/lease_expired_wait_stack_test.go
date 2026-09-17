package etcd

import (
	"context"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
)

// Establish that a stopped expired renewal is observable without interpreting
// request Send or negative TTL as proof of entering the wait. This is a local
// diagnostic contract, not proof of a real cluster request or leadership fault.
func TestExpiredLeaseWaitRuntimeStack(t *testing.T) {
	s, closeFn := newTestRPCServer(t)
	defer closeFn()
	const id int64 = 78331
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
	done := make(chan error, 1)
	joined := make(chan struct{})
	go func() {
		defer close(joined)
		_, err := s.refreshLeaseHoldingLocks(ctx, id, epoch, func() {}, func() {}, nil, func() {})
		done <- err
	}()
	defer func() { cancel(); <-joined }()
	require.Eventually(t, func() bool {
		buf := make([]byte, 4<<20)
		n := runtime.Stack(buf, true)
		if n == len(buf) {
			return false // Never accept a truncated snapshot.
		}
		for _, block := range strings.Split(string(buf[:n]), "\n\n") {
			lines := strings.Split(block, "\n")
			if len(lines) < 3 || !strings.Contains(lines[0], "[select]") {
				continue
			}
			if strings.Contains(lines[1], ".(*leaseManager).refreshLeaseHoldingLocks(") &&
				strings.Contains(lines[2], "/lease.go:") &&
				strings.Contains(block, "TestExpiredLeaseWaitRuntimeStack.func") {
				return true
			}
		}
		return false
	}, time.Second, time.Millisecond, "expired wait must be a selectable top frame, not merely an ancestor")
	endTerm()
	select {
	case err := <-done:
		require.ErrorIs(t, err, errLeaseDemotedDuringRenew)
	case <-time.After(time.Second):
		t.Fatal("observed expired wait did not exit on term cancellation")
	}
}
