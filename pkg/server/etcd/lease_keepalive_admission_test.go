package etcd

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/api/v3/authpb"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
)

// Test the worker itself: the outer RPC can return on cancellation while a
// blocked worker survives and later extends the deadline behind the caller.
func TestLeaseKeepAliveCanceledAdmission(t *testing.T) {
	for _, owner := range []string{"binding", "checkpoint-barrier", "checkpoint-stripe"} {
		for _, deadline := range []bool{false, true} {
			name := "cancel"
			if deadline {
				name = "deadline"
			}
			t.Run(owner+"/"+name, func(t *testing.T) {
				s, closeFn := newTestRPCServer(t)
				defer closeFn()
				const id = int64(78322)
				_, err := s.LeaseGrant(context.Background(), &etcdserverpb.LeaseGrantRequest{ID: id, TTL: 300})
				require.NoError(t, err)
				s.leaseMu.Lock()
				before := s.leases[id].deadline
				s.leaseMu.Unlock()
				var unlock func()
				switch owner {
				case "binding":
					s.leaseWriteMu.Lock()
					unlock = s.leaseWriteMu.Unlock
				case "checkpoint-barrier":
					s.leaseCheckpointMu.Lock()
					unlock = s.leaseCheckpointMu.Unlock
				case "checkpoint-stripe":
					stripe := &s.leaseCheckpointLocks[uint64(id)%uint64(len(s.leaseCheckpointLocks))]
					stripe.Lock()
					unlock = stripe.Unlock
				}
				locked := true
				defer func() {
					if locked {
						unlock()
					}
				}()
				ctx, cancel := context.WithCancel(context.Background())
				if deadline {
					cancel()
					ctx, cancel = context.WithTimeout(context.Background(), 100*time.Millisecond)
				}
				defer cancel()
				received := make(chan struct{})
				first := true
				stream := &fakeLeaseKeepAliveServer{ctx: ctx, recv: func() (*etcdserverpb.LeaseKeepAliveRequest, error) {
					if !first {
						return nil, ctx.Err()
					}
					first = false
					close(received)
					return &etcdserverpb.LeaseKeepAliveRequest{ID: id}, nil
				}}
				done := make(chan error, 1)
				go func() { done <- s.leaseKeepAlive(stream) }()
				<-received
				if !deadline {
					cancel()
				}
				select {
				case err = <-done:
					want := context.Canceled
					if deadline {
						want = context.DeadlineExceeded
					}
					require.ErrorIs(t, err, want)
				case <-time.After(2 * time.Second):
					unlock()
					locked = false
					select {
					case <-done:
					case <-time.After(2 * time.Second):
						t.Fatal("worker did not retire after unlock")
					}
					t.Fatal("canceled worker did not retire while admission owner held lock")
				}
				unlock()
				locked = false
				s.leaseMu.Lock()
				after := s.leases[id].deadline
				s.leaseMu.Unlock()
				require.Equal(t, before, after)
				// All partially acquired locks must be released, including the
				// checkpoint barrier when cancellation happens at its stripe.
				followCtx, followCancel := context.WithTimeout(context.Background(), time.Second)
				defer followCancel()
				require.NoError(t, s.leaseCheckpointMu.LockContext(followCtx))
				s.leaseCheckpointMu.Unlock()
				_, err = s.refreshLease(followCtx, id)
				require.NoError(t, err)
			})
		}
	}
}

func TestLeaseAuthorizedRenewalCanceledAdmission(t *testing.T) {
	for _, role := range []string{"root", "writer"} {
		for _, owner := range []string{"binding", "checkpoint-barrier", "checkpoint-stripe"} {
			t.Run(role+"/"+owner, func(t *testing.T) {
				s, closeFn := newTestRPCServer(t)
				defer closeFn()
				const id = int64(78323)
				_, err := s.LeaseGrant(context.Background(), &etcdserverpb.LeaseGrantRequest{ID: id, TTL: 300})
				require.NoError(t, err)
				caller := &authCaller{username: "alice", snapshot: &authSnapshot{
					Users: map[string]*authpb.User{"alice": {Name: []byte("alice"), Roles: []string{role}}},
				}}
				require.Equal(t, role == "root", caller.isRoot())
				epoch, leading := s.peers.EpochAndLeadingFresh()
				require.True(t, leading)
				s.leaseMu.Lock()
				before := s.leases[id].deadline
				s.leaseMu.Unlock()
				var unlock func()
				switch owner {
				case "binding":
					s.leaseWriteMu.Lock()
					unlock = s.leaseWriteMu.Unlock
				case "checkpoint-barrier":
					s.leaseCheckpointMu.Lock()
					unlock = s.leaseCheckpointMu.Unlock
				case "checkpoint-stripe":
					stripe := &s.leaseCheckpointLocks[uint64(id)%uint64(len(s.leaseCheckpointLocks))]
					stripe.Lock()
					unlock = stripe.Unlock
				}
				var unlockOnce sync.Once
				release := func() { unlockOnce.Do(unlock) }
				defer release()
				ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
				defer cancel()
				done := make(chan error, 1)
				go func() { _, err := s.refreshLeaseAuthorized(ctx, caller, id, epoch); done <- err }()
				select {
				case err := <-done:
					require.ErrorIs(t, err, context.DeadlineExceeded)
				case <-time.After(2 * time.Second):
					release()
					select {
					case <-done:
					case <-time.After(2 * time.Second):
						t.Fatal("worker failed to exit")
					}
					t.Fatal("authorized renewal outlived canceled admission")
				}
				release()
				s.leaseMu.Lock()
				after := s.leases[id].deadline
				s.leaseMu.Unlock()
				require.Equal(t, before, after)
				followCtx, followCancel := context.WithTimeout(context.Background(), time.Second)
				defer followCancel()
				require.NoError(t, s.leaseCheckpointMu.LockContext(followCtx))
				s.leaseCheckpointMu.Unlock()
				_, err = s.refreshLease(followCtx, id)
				require.NoError(t, err)
			})
		}
	}
}
