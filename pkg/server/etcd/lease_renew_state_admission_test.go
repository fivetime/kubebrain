package etcd

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
)

// A teardown holds the binding writer but permits unrelated, uncheckpointed
// renewals. This bypass must not strand a canceled worker at the state lock.
func TestLeaseRenewTeardownStateAdmission(t *testing.T) {
	s, closeFn := newTestRPCServer(t)
	defer closeFn()
	const id = int64(78325)
	_, err := s.LeaseGrant(context.Background(), &etcdserverpb.LeaseGrantRequest{ID: id, TTL: 300})
	require.NoError(t, err)
	s.leaseWriteMu.Lock()
	s.leaseTeardowns.Add(1)
	s.leaseMu.Lock()
	before := s.leases[id].deadline
	var once sync.Once
	release := func() {
		once.Do(func() {
			s.leaseMu.Unlock()
			s.leaseTeardowns.Add(-1)
			s.leaseWriteMu.Unlock()
		})
	}
	defer release()
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	done := make(chan error, 1)
	go func() { _, err := s.refreshLease(ctx, id); done <- err }()
	select {
	case err := <-done:
		require.ErrorIs(t, err, context.DeadlineExceeded)
	case <-time.After(2 * time.Second):
		release()
		select {
		case <-done:
		case <-time.After(2 * time.Second):
			t.Fatal("renewal did not retire after releasing teardown locks")
		}
		t.Fatal("canceled teardown bypass stayed queued at state admission")
	}
	require.Equal(t, before, s.leases[id].deadline, "canceled admission must not renew")
	release()
	follow, stop := context.WithTimeout(context.Background(), time.Second)
	defer stop()
	require.NoError(t, s.leaseCheckpointMu.LockContext(follow))
	s.leaseCheckpointMu.Unlock()
	_, err = s.refreshLease(follow, id)
	require.NoError(t, err, "canceled bypass must release checkpoint and binding locks")
}
