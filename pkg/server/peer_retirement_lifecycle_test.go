package server

import (
	"context"
	"crypto/tls"
	"sync/atomic"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/kubewharf/kubebrain/pkg/backend"
	"github.com/kubewharf/kubebrain/pkg/backend/election"
	metricmock "github.com/kubewharf/kubebrain/pkg/metrics/mock"
	"github.com/kubewharf/kubebrain/pkg/server/service/leader"
	"github.com/kubewharf/kubebrain/pkg/storage"
	"github.com/kubewharf/kubebrain/pkg/storage/memkv"
	"github.com/stretchr/testify/require"
	"k8s.io/client-go/tools/leaderelection/resourcelock"
)

// Only the old process's backend access fails. The healthy peer still reaches
// the same storage and commits the real conditionally fenced transaction.
type retirementPartitionedLock struct {
	resourcelock.Interface
	partitioned atomic.Bool
}

func (l *retirementPartitionedLock) Get(ctx context.Context) (*resourcelock.LeaderElectionRecord, []byte, error) {
	if l.partitioned.Load() {
		return nil, nil, storage.ErrUnavailable
	}
	return l.Interface.Get(ctx)
}
func (l *retirementPartitionedLock) Update(ctx context.Context, record resourcelock.LeaderElectionRecord) error {
	if l.partitioned.Load() {
		return storage.ErrUnavailable
	}
	return l.Interface.Update(ctx, record)
}
func (l *retirementPartitionedLock) OwnershipConditionFor(record resourcelock.LeaderElectionRecord) (election.OwnershipCondition, bool) {
	return l.Interface.(election.OwnershipConditionProvider).OwnershipConditionFor(record)
}

type retirementLifecycleBackend struct {
	backend.Backend
	lock resourcelock.Interface
}

func (b *retirementLifecycleBackend) GetResourceLock() resourcelock.Interface { return b.lock }
func (*retirementLifecycleBackend) InitializeLeadershipRevision(context.Context, uint64) error {
	return nil
}

func TestPeerRetirementCampaignPartitionToStorageRelease(t *testing.T) {
	kv := memkv.NewKvStorage()
	t.Cleanup(func() { require.NoError(t, kv.Close()) })
	store := retirementStorageFixture{kv, 42}
	config := election.Config{Prefix: "/lifecycle-retirement", Keyspace: "tenant", Identity: "old", Timeout: time.Second}
	old := &retirementPartitionedLock{Interface: election.NewResourceLockManager(config, store).GetResourceLock()}
	config.Identity = "helper"
	helper := election.NewResourceLockManager(config, store).GetResourceLock()
	scope := helper.(election.RetiredOwnershipReleaser).RetirementScope()
	pool, certs := retirementTestCertificates(t)
	auth, err := newPeerRetirementAuthorizer(scope, map[string][]string{"old": {retirementTestPin(certs[0])}})
	require.NoError(t, err)
	h, err := newStoragePeerRetirementHandler(auth, helper, time.Second, time.Second, 2, 10)
	require.NoError(t, err)
	srv := retirementHandlerServer(t, h, pool, certs, true)
	sender, err := newPeerRetirementSender(scope, "old", []string{srv.URL}, &tls.Config{RootCAs: pool, Certificates: []tls.Certificate{certs[0]}}, time.Second)
	require.NoError(t, err)
	metrics := metricmock.NewMinimalMetrics(gomock.NewController(t))
	started, done := make(chan struct{}), make(chan struct{})
	var initializedJoined, cleaned atomic.Bool
	ctx, cancel := context.WithCancel(context.Background())
	observed := make(chan bool, 1)
	var campaign leader.LeaderElection
	campaign = leader.NewLeaderElectionWithRetirement(&retirementLifecycleBackend{lock: old}, metrics, nil,
		func(ctx context.Context) { close(started); <-ctx.Done(); initializedJoined.Store(true) },
		func() { cleaned.Store(initializedJoined.Load()) },
		leader.Config{LeaseDuration: 30 * time.Second, RenewDeadline: 200 * time.Millisecond, RetryPeriod: 10 * time.Millisecond},
		func(ctx context.Context, condition election.OwnershipCondition, available bool) {
			_, fresh := campaign.EpochAndLeadingFresh()
			valid := available && cleaned.Load() && !campaign.IsLeader() && !fresh && ctx.Err() == nil
			sender.onTermRetired(ctx, condition, available)
			observed <- valid
			cancel() // End this fixture, not a protocol reactivation decision.
		})
	go func() { defer close(done); campaign.Campaign(ctx) }()
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
		t.Fatal("campaign never acquired")
	}
	old.partitioned.Store(true)
	select {
	case valid := <-observed:
		require.True(t, valid, "send only after irreversible cleanup with a live parent context")
	case <-time.After(3 * time.Second):
		t.Fatal("post-retirement callback missing")
	}
	<-done
	record, _, err := helper.Get(context.Background())
	require.NoError(t, err)
	require.Empty(t, record.HolderIdentity, "helper must commit release despite old backend isolation")
	require.True(t, old.partitioned.Load())
	require.False(t, campaign.IsLeader())
}
