package etcd

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/kubewharf/kubebrain/pkg/server/service"
	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
)

type publicationPausedPeers struct {
	service.PeerService
	calls   atomic.Int32
	retired atomic.Bool
	pauseAt int32
	entered chan struct{}
	resume  chan struct{}
}

func (p *publicationPausedPeers) EpochAndLeadingFresh() (uint64, bool) {
	epoch, fresh := p.PeerService.EpochAndLeadingFresh()
	fresh = fresh && !p.retired.Load()
	if p.calls.Add(1) == p.pauseAt {
		close(p.entered)
		<-p.resume
	}
	// Return the result captured before retirement. This exercises the gap
	// between the final freshness check and publishing the memory deadline.
	return epoch, fresh
}

func TestLeaseRetirementDrainsDeadlinePublication(t *testing.T) {
	for _, name := range []string{"ordinary", "unrelated_teardown", "authorized_writer"} {
		t.Run(name, func(t *testing.T) {
			s, closeFn := newTestRPCServer(t)
			defer closeFn()
			const id = int64(889341)
			_, err := s.LeaseGrant(context.Background(), &etcdserverpb.LeaseGrantRequest{ID: id, TTL: 300})
			require.NoError(t, err)
			ctx := context.Background()
			var caller *authCaller
			if name == "authorized_writer" {
				ctx = setupAuthKVUser(t, s)
				_, err = s.Put(ctx, &etcdserverpb.PutRequest{Key: []byte("/allowed/retirement"), Value: []byte("v"), Lease: id})
				require.NoError(t, err)
				caller, err = s.authCallerFromContext(ctx)
				require.NoError(t, err)
				require.NotNil(t, caller)
				require.False(t, caller.isRoot())
			}
			epoch, fresh := s.peers.EpochAndLeadingFresh()
			require.True(t, fresh)
			p := &publicationPausedPeers{PeerService: s.peers, pauseAt: 4, entered: make(chan struct{}), resume: make(chan struct{})}
			if caller != nil {
				p.pauseAt = 3 // authorized entry already carries its admitted epoch
			}
			s.peers = p
			if name == "unrelated_teardown" {
				p.pauseAt = 3
				s.leaseWriteMu.Lock()
				s.leaseTeardowns.Add(1)
				defer func() { s.leaseTeardowns.Add(-1); s.leaseWriteMu.Unlock() }()
			}
			var once sync.Once
			resume := func() { once.Do(func() { close(p.resume) }) }
			defer resume()
			renewDone := make(chan error, 1)
			go func() {
				var err error
				if caller != nil {
					_, err = s.refreshLeaseAuthorized(ctx, caller, id, epoch)
				} else {
					_, err = s.refreshLease(ctx, id)
				}
				renewDone <- err
			}()
			select {
			case <-p.entered:
			case <-time.After(time.Second):
				resume()
				<-renewDone
				t.Fatal("renewal did not reach final publication check")
			}
			p.retired.Store(true)
			stopped := make(chan struct{})
			go func() { s.StopLeases(); close(stopped) }()
			// A queued exclusive checkpoint owner prevents new readers. Observe
			// that actual queue state rather than assuming StopLeases was scheduled.
			queued := false
			deadline := time.NewTimer(time.Second)
			tick := time.NewTicker(time.Millisecond)
		wait:
			for {
				if !s.leaseCheckpointMu.TryRLock() {
					queued = true
					break
				}
				s.leaseCheckpointMu.RUnlock()
				select {
				case <-stopped:
					break wait
				case <-deadline.C:
					break wait
				case <-tick.C:
				}
			}
			deadline.Stop()
			tick.Stop()
			select {
			case <-stopped:
				resume()
				<-renewDone
				t.Fatal("retirement returned before in-flight publication drained")
			default:
			}
			resume()
			renewErr := <-renewDone
			select {
			case <-stopped:
			case <-time.After(time.Second):
				t.Fatal("retirement failed to finish after publication")
			}
			require.NoError(t, renewErr)
			require.True(t, queued, "StopLeases must acquire the exclusive checkpoint barrier")
			s.leaseMu.Lock()
			empty := len(s.leases) == 0
			s.leaseMu.Unlock()
			require.True(t, empty)
			_, err = s.refreshLease(context.Background(), id)
			require.ErrorIs(t, err, errLeaseDemotedDuringRenew)
		})
	}
}
