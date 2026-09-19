package leader

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/kubewharf/kubebrain/pkg/backend/election"
	"github.com/kubewharf/kubebrain/pkg/metrics"
	metricmock "github.com/kubewharf/kubebrain/pkg/metrics/mock"
	"github.com/kubewharf/kubebrain/pkg/storage"
	"github.com/kubewharf/kubebrain/pkg/storage/memkv"
	"github.com/stretchr/testify/require"
	"k8s.io/client-go/tools/leaderelection/resourcelock"
)

type postJoinIdentifiedStorage struct{ storage.KvStorage }

func (postJoinIdentifiedStorage) ClusterID() uint64 { return 42 }

type postJoinReleaseLock struct {
	resourcelock.Interface
	election.RetiredOwnershipReleaser
	provider     election.OwnershipConditionProvider
	failUpdates  atomic.Bool
	missingClaim bool
	release      func(context.Context, string, election.OwnershipCondition) error
	legacyReads  atomic.Int32
}

func (l *postJoinReleaseLock) Get(ctx context.Context) (*resourcelock.LeaderElectionRecord, []byte, error) {
	if ctx.Value(campaignContextMarker{}) == nil {
		l.legacyReads.Add(1)
	}
	return l.Interface.Get(ctx)
}

func (l *postJoinReleaseLock) Update(ctx context.Context, record resourcelock.LeaderElectionRecord) error {
	if l.failUpdates.Load() {
		return storage.ErrUnavailable
	}
	return l.Interface.Update(ctx, record)
}

func (l *postJoinReleaseLock) OwnershipConditionFor(record resourcelock.LeaderElectionRecord) (election.OwnershipCondition, bool) {
	if l.missingClaim {
		return election.OwnershipCondition{}, false
	}
	return l.provider.OwnershipConditionFor(record)
}

func (l *postJoinReleaseLock) ReleaseRetiredOwnership(ctx context.Context, scope string, claim election.OwnershipCondition) error {
	return l.release(ctx, scope, claim)
}

func TestPostJoinReleaseRequiresExplicitCallbackScopeAndSnapshot(t *testing.T) {
	kv := memkv.NewKvStorage()
	defer func() { require.NoError(t, kv.Close()) }()
	config := election.Config{Prefix: "/constructor", Identity: "old", Timeout: time.Second}
	scoped := election.NewResourceLockManager(config, postJoinIdentifiedStorage{kv}).GetResourceLock()
	unscoped := election.NewResourceLockManager(config, kv).GetResourceLock()
	withoutProvider := struct {
		resourcelock.Interface
		election.RetiredOwnershipReleaser
	}{scoped, scoped.(election.RetiredOwnershipReleaser)}
	hook := func(context.Context, election.OwnershipCondition, bool) {}
	for _, tt := range []struct {
		name    string
		lock    resourcelock.Interface
		hook    func(context.Context, election.OwnershipCondition, bool)
		enabled bool
	}{
		{"scoped_opt_in", scoped, hook, true},
		{"no_callback", scoped, nil, false},
		{"no_scope", unscoped, hook, false},
		{"no_snapshot", withoutProvider, hook, false},
	} {
		t.Run(tt.name, func(t *testing.T) {
			l := NewLeaderElectionWithRetirement(&retirementConstructorBackend{lock: tt.lock}, nil, nil, nil, nil, Config{}, tt.hook).(*leaderElection)
			require.Equal(t, tt.enabled, l.retiredReleaser != nil)
		})
	}
	ordinary := NewLeaderElection(&retirementConstructorBackend{lock: scoped}, nil, nil, nil, nil, Config{}).(*leaderElection)
	require.Nil(t, ordinary.retiredReleaser, "ordinary constructor never enables experimental release")
}

func TestScopedPostJoinReleaseUsesFrozenClaimAndBoundedFallback(t *testing.T) {
	for _, mode := range []string{"success", "shutdown", "timeout", "uncertain_commit", "new_same_holder_term", "missing_claim"} {
		t.Run(mode, func(t *testing.T) {
			kv := memkv.NewKvStorage()
			defer func() { require.NoError(t, kv.Close()) }()
			base := election.NewResourceLockManager(election.Config{Prefix: "/post-join", Identity: "old", Timeout: time.Second}, postJoinIdentifiedStorage{kv}).GetResourceLock()
			lock := &postJoinReleaseLock{Interface: base, RetiredOwnershipReleaser: base.(election.RetiredOwnershipReleaser),
				provider: base.(election.OwnershipConditionProvider), missingClaim: mode == "missing_claim"}
			m := metricmock.NewMockMetrics(gomock.NewController(t))
			m.EXPECT().EmitCounter(gomock.Any(), gomock.Any(), gomock.Any()).AnyTimes()
			m.EXPECT().EmitGauge(gomock.Any(), gomock.Any(), gomock.Any()).AnyTimes()
			metricOutcome := make(chan string, 1)
			if mode != "missing_claim" {
				m.EXPECT().EmitHistogram("leader.retirement.local.duration.seconds", gomock.Any(), gomock.Any()).
					DoAndReturn(func(_ string, _ interface{}, tags ...metrics.T) error {
						if len(tags) == 1 && tags[0].Name == "outcome" {
							metricOutcome <- tags[0].Value
						}
						return nil
					}).Times(1)
			}
			started, done := make(chan struct{}), make(chan struct{})
			var joined, cleaned atomic.Bool
			ctx, cancel := context.WithCancel(context.WithValue(context.Background(), campaignContextMarker{}, true))
			defer cancel()
			type observation struct {
				available, safe, deadline, active bool
				replacementInstalled              bool
				claim                             election.OwnershipCondition
				err                               error
				contextErr                        error
			}
			local := make(chan observation, 1)
			peer := make(chan observation, 1)
			var l LeaderElection
			lock.release = func(releaseCtx context.Context, scope string, claim election.OwnershipCondition) error {
				deadline, bounded := releaseCtx.Deadline()
				_, fresh := l.EpochAndLeadingFresh()
				got := observation{safe: joined.Load() && cleaned.Load() && !l.IsLeader() && !fresh,
					deadline: bounded && time.Until(deadline) <= 10*time.Millisecond, claim: claim}
				if mode == "timeout" {
					<-releaseCtx.Done()
					got.err = releaseCtx.Err()
				} else {
					if mode == "new_same_holder_term" {
						record, _, err := base.Get(context.Background())
						if err != nil {
							got.err = err
						} else {
							record.LeaderTransitions++
							got.err = base.Update(context.Background(), *record)
							got.replacementInstalled = got.err == nil
						}
					}
					if got.err == nil {
						got.err = lock.RetiredOwnershipReleaser.ReleaseRetiredOwnership(releaseCtx, scope, claim)
						if got.err == nil && mode == "uncertain_commit" {
							got.err = storage.ErrUnavailable
						}
					}
				}
				got.contextErr = releaseCtx.Err()
				local <- got
				return got.err
			}
			l = NewLeaderElectionWithRetirement(&retirementConstructorBackend{lock: lock}, m, nil,
				func(ctx context.Context) { close(started); <-ctx.Done(); joined.Store(true) },
				func() { cleaned.Store(true) },
				Config{LeaseDuration: time.Second, RenewDeadline: 200 * time.Millisecond, RetryPeriod: 10 * time.Millisecond},
				func(ctx context.Context, claim election.OwnershipCondition, available bool) {
					_, fresh := l.EpochAndLeadingFresh()
					peer <- observation{claim: claim, available: available, active: ctx.Err() == nil,
						safe: joined.Load() && cleaned.Load() && !l.IsLeader() && !fresh}
					cancel()
				})
			require.NotNil(t, l.(*leaderElection).retiredReleaser)
			go func() { defer close(done); l.Campaign(ctx) }()
			defer func() {
				cancel()
				select {
				case <-done:
				case <-time.After(3 * time.Second):
					t.Error("campaign did not join")
				}
			}()
			select {
			case <-started:
			case <-time.After(3 * time.Second):
				t.Fatal("no acquired term")
			}
			if mode == "shutdown" {
				cancel()
			} else {
				lock.failUpdates.Store(true)
			}
			select {
			case <-done:
			case <-time.After(3 * time.Second):
				t.Fatal("retirement stalled")
			}
			require.NotEmpty(t, peer, "joined campaign must have published its retirement observation")
			gotPeer := <-peer
			require.Equal(t, mode != "shutdown", gotPeer.active)
			require.True(t, gotPeer.safe)
			require.Equal(t, mode != "missing_claim", gotPeer.available)
			require.Zero(t, lock.legacyReads.Load(), "opt-in must not enter client-go's release Get")
			if mode == "missing_claim" {
				require.Empty(t, local, "missing exact condition cannot authorize local release")
			} else {
				gotLocal := <-local
				require.NotEmpty(t, metricOutcome)
				outcome := <-metricOutcome
				if gotLocal.contextErr == context.DeadlineExceeded {
					require.Equal(t, "deadline", outcome, "even a nil commit result can arrive after the release budget")
				} else if gotLocal.err == nil {
					require.Contains(t, []string{"confirmed", "deadline"}, outcome, "deadline may expire between backend return and observation")
				} else {
					require.Contains(t, []string{"unconfirmed", "deadline"}, outcome)
				}
				require.True(t, gotLocal.safe)
				require.True(t, gotLocal.deadline)
				require.Equal(t, gotLocal.claim, gotPeer.claim, "local attempt cannot replace frozen peer condition")
				switch mode {
				case "success", "shutdown":
					require.NoError(t, gotLocal.err)
				case "timeout":
					require.ErrorIs(t, gotLocal.err, context.DeadlineExceeded)
				case "uncertain_commit":
					require.ErrorIs(t, gotLocal.err, storage.ErrUnavailable)
				case "new_same_holder_term":
					require.True(t, gotLocal.replacementInstalled, "a newer term must actually commit before testing old-claim conflict")
					require.ErrorIs(t, gotLocal.err, storage.ErrCASFailed)
				}
			}
			record, _, err := base.Get(context.Background())
			require.NoError(t, err)
			if mode == "success" || mode == "shutdown" || mode == "uncertain_commit" {
				require.Empty(t, record.HolderIdentity)
			} else {
				require.Equal(t, "old", record.HolderIdentity)
			}
		})
	}
}
