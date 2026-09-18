package leader

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/kubewharf/kubebrain/pkg/backend"
	"github.com/kubewharf/kubebrain/pkg/backend/election"
	metricmock "github.com/kubewharf/kubebrain/pkg/metrics/mock"
	"github.com/kubewharf/kubebrain/pkg/storage/memkv"
	"github.com/stretchr/testify/require"
	"k8s.io/client-go/tools/leaderelection/resourcelock"
)

type retirementConstructorBackend struct {
	backend.Backend
	lock resourcelock.Interface
}

func (b *retirementConstructorBackend) GetResourceLock() resourcelock.Interface { return b.lock }
func (*retirementConstructorBackend) InitializeLeadershipRevision(context.Context, uint64) error {
	return nil
}

func TestRetiredConditionCannotFollowLaterAcquisition(t *testing.T) {
	kv := memkv.NewKvStorage()
	t.Cleanup(func() { require.NoError(t, kv.Close()) })
	lock := election.NewResourceLockManager(election.Config{Prefix: "/term-condition", Identity: "old", Timeout: time.Second}, kv).GetResourceLock()
	l := &leaderElection{resourceLock: lock, renewDeadline: time.Second}
	old := &renewalTerm{leader: l}
	wrapped := &renewStampingLock{Interface: lock, onRenew: old.renew, onRelease: old.retire, onOwnMutation: old.capture}
	record := resourcelock.LeaderElectionRecord{HolderIdentity: "old", LeaseDurationSeconds: 30, LeaderTransitions: 7}
	require.NoError(t, wrapped.Create(context.Background(), record))
	expected, ok := lock.(election.OwnershipConditionProvider).OwnershipConditionFor(record)
	require.True(t, ok)
	_, ok = old.retiredCondition()
	require.False(t, ok, "live term must not publish a retired condition")
	old.retire()
	got, ok := old.retiredCondition()
	require.True(t, ok)
	require.Equal(t, expected, got)
	// Mutate the shared cache to another acquisition, then deliver its callback
	// to the old term. Neither the record nor token may replace its frozen claim.
	record.LeaderTransitions++
	require.NoError(t, wrapped.Update(context.Background(), record))
	again, ok := old.retiredCondition()
	require.True(t, ok)
	require.Equal(t, expected, again)
	next := &renewalTerm{leader: l}
	next.capture(record)
	next.retire()
	nextCondition, ok := next.retiredCondition()
	require.True(t, ok)
	require.NotEqual(t, expected, nextCondition)
	old.retire()
	again, ok = old.retiredCondition()
	require.True(t, ok)
	require.Equal(t, expected, again)
}

func TestCampaignPublishesCapturedConditionAfterReleaseAndCleanup(t *testing.T) {
	kv := memkv.NewKvStorage()
	t.Cleanup(func() { require.NoError(t, kv.Close()) })
	lock := election.NewResourceLockManager(election.Config{Prefix: "/campaign-condition", Identity: "old", Timeout: time.Second}, kv).GetResourceLock()
	m := metricmock.NewMockMetrics(gomock.NewController(t))
	m.EXPECT().EmitCounter(gomock.Any(), gomock.Any(), gomock.Any()).AnyTimes()
	m.EXPECT().EmitGauge(gomock.Any(), gomock.Any(), gomock.Any()).AnyTimes()
	started, done := make(chan struct{}), make(chan struct{})
	var cleaned atomic.Bool
	type observation struct {
		condition                   election.OwnershipCondition
		available, cleaned, leading bool
	}
	observed := make(chan observation, 1)
	var l LeaderElection
	l = NewLeaderElectionWithRetirement(&retirementConstructorBackend{lock: lock}, m, nil,
		func(ctx context.Context) { close(started); <-ctx.Done() }, func() { cleaned.Store(true) },
		Config{LeaseDuration: time.Second, RenewDeadline: 200 * time.Millisecond, RetryPeriod: 10 * time.Millisecond},
		func(_ context.Context, condition election.OwnershipCondition, available bool) {
			observed <- observation{condition, available, cleaned.Load(), l.IsLeader()}
		})
	ctx, cancel := context.WithCancel(context.Background())
	go func() { defer close(done); l.Campaign(ctx) }()
	defer func() {
		cancel()
		select {
		case <-done:
		case <-time.After(2 * time.Second):
			t.Error("campaign did not join")
		}
	}()
	select {
	case <-started:
	case <-time.After(2 * time.Second):
		t.Fatal("campaign did not acquire")
	}
	cancel()
	select {
	case got := <-observed:
		require.True(t, got.available)
		require.True(t, got.cleaned)
		require.False(t, got.leading)
		require.NotEqual(t, election.OwnershipCondition{}, got.condition)
	case <-time.After(2 * time.Second):
		t.Fatal("missing retired condition")
	}
	<-done
	record, _, err := lock.Get(context.Background())
	require.NoError(t, err)
	require.Empty(t, record.HolderIdentity, "normal release must already have overwritten the shared cache")
	_, available := lock.(election.OwnershipConditionProvider).OwnershipConditionFor(*record)
	require.False(t, available, "the delivered condition cannot have been read from the released cache")
}

func TestRetiredConditionWithoutProviderFallsBack(t *testing.T) {
	term := &renewalTerm{leader: &leaderElection{resourceLock: &campaignLock{}}}
	term.capture(resourcelock.LeaderElectionRecord{})
	term.retire()
	condition, available := term.retiredCondition()
	require.False(t, available)
	require.Equal(t, election.OwnershipCondition{}, condition)
}
