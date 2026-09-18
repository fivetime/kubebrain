package leader

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"k8s.io/client-go/tools/leaderelection/resourcelock"
)

func TestRenewalTermRetirementIsIrreversible(t *testing.T) {
	l := &leaderElection{renewDeadline: time.Hour}
	atomic.StoreInt32(&l.leader, 1)
	old := &renewalTerm{leader: l}
	old.renew()
	_, fresh := l.EpochAndLeadingFresh()
	require.True(t, fresh)
	old.retire()
	old.renew()
	_, fresh = l.EpochAndLeadingFresh()
	require.False(t, fresh, "a delayed old-term success must not undo retirement")

	// Campaign joins and retires the old elector before creating the next one.
	next := &renewalTerm{leader: l}
	next.renew()
	l.renewMu.RLock()
	stamp := l.lastRenew
	l.renewMu.RUnlock()
	old.retire()
	old.renew()
	l.renewMu.RLock()
	after := l.lastRenew
	l.renewMu.RUnlock()
	require.Equal(t, stamp, after, "old callbacks must neither clear nor restamp a new term")
	_, fresh = l.EpochAndLeadingFresh()
	require.True(t, fresh)
}

type delayedRenewalNotificationLock struct {
	campaignLock
	committed chan struct{}
	returnNow chan struct{}
}

func (l *delayedRenewalNotificationLock) Update(ctx context.Context, r resourcelock.LeaderElectionRecord) error {
	if err := l.campaignLock.Update(ctx, r); err != nil {
		return err
	}
	close(l.committed)
	select {
	case <-l.returnNow:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func TestRenewalTermRejectsDelayedSuccessfulStorageNotification(t *testing.T) {
	l := &leaderElection{renewDeadline: time.Hour}
	atomic.StoreInt32(&l.leader, 1)
	term := &renewalTerm{leader: l}
	underlying := &delayedRenewalNotificationLock{committed: make(chan struct{}), returnNow: make(chan struct{})}
	lock := &renewStampingLock{Interface: underlying, onRenew: term.renew, onRelease: term.retire}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- lock.Update(ctx, resourcelock.LeaderElectionRecord{HolderIdentity: underlying.Identity(), LeaseDurationSeconds: 30})
	}()
	defer cancel()
	select {
	case <-underlying.committed:
	case <-time.After(time.Second):
		cancel()
		<-done
		t.Fatal("storage mutation did not reach notification boundary")
	}
	term.retire()
	close(underlying.returnNow)
	require.NoError(t, <-done)
	_, fresh := l.EpochAndLeadingFresh()
	require.False(t, fresh)
	record, _, err := underlying.Get(ctx)
	require.NoError(t, err)
	require.Equal(t, underlying.Identity(), record.HolderIdentity,
		"local retirement must not pretend durable ownership was released")
}

func TestRenewalTermConcurrentRetirement(t *testing.T) {
	l := &leaderElection{renewDeadline: time.Hour}
	atomic.StoreInt32(&l.leader, 1)
	term := &renewalTerm{leader: l}
	var workers sync.WaitGroup
	for range 16 {
		workers.Add(1)
		go func() {
			defer workers.Done()
			for range 100 {
				term.renew()
			}
		}()
	}
	term.retire()
	workers.Wait()
	_, fresh := l.EpochAndLeadingFresh()
	require.False(t, fresh)
}
