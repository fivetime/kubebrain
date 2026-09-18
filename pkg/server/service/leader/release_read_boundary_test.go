package leader

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/kubewharf/kubebrain/pkg/backend/election"
	metricmock "github.com/kubewharf/kubebrain/pkg/metrics/mock"
	"github.com/kubewharf/kubebrain/pkg/storage"
	"github.com/stretchr/testify/require"
	"k8s.io/client-go/tools/leaderelection/resourcelock"
)

type campaignContextMarker struct{}

type releaseReadBoundaryLock struct {
	campaignLock
	entered chan context.Context
	resume  chan struct{}
}

func (l *releaseReadBoundaryLock) Get(ctx context.Context) (*resourcelock.LeaderElectionRecord, []byte, error) {
	if ctx.Value(campaignContextMarker{}) != nil {
		return l.campaignLock.Get(ctx)
	}
	// client-go release uses a fresh Background-derived context, unlike acquire
	// and renew. Model an unavailable backend without relying on sleep timing.
	l.entered <- ctx
	select {
	case <-l.resume:
		return nil, nil, storage.ErrUnavailable
	case <-ctx.Done():
		return nil, nil, ctx.Err()
	}
}

// Characterization of the current client-go release ordering, not an acceptance
// criterion: this explains why a post-join peer notification can be delayed by
// a backend read even after local cancellation. It does not reproduce TiKV or
// prove the cause of any particular cluster failover.
func TestCampaignReleaseReadDelaysRetirementBoundary(t *testing.T) {
	m := metricmock.NewMockMetrics(gomock.NewController(t))
	m.EXPECT().EmitCounter(gomock.Any(), gomock.Any(), gomock.Any()).AnyTimes()
	m.EXPECT().EmitGauge(gomock.Any(), gomock.Any(), gomock.Any()).AnyTimes()
	lock := &releaseReadBoundaryLock{entered: make(chan context.Context, 1), resume: make(chan struct{})}
	started, cleaned, retired, done := make(chan struct{}), make(chan struct{}), make(chan struct{}), make(chan struct{})
	l := &leaderElection{backend: &revisionRecorder{}, resourceLock: lock, metricCli: m,
		leaseDuration: 3 * time.Second, renewDeadline: time.Second, retryPeriod: 10 * time.Millisecond,
		onStartedLeading: func(ctx context.Context) { close(started); <-ctx.Done() },
		onStoppedLeading: func() { close(cleaned) },
		onTermRetired:    func(context.Context, election.OwnershipCondition, bool) { close(retired) },
	}
	ctx, cancel := context.WithCancel(context.WithValue(context.Background(), campaignContextMarker{}, true))
	var once sync.Once
	resume := func() { once.Do(func() { close(lock.resume) }) }
	await := func(ch <-chan struct{}) {
		t.Helper()
		select {
		case <-ch:
		case <-time.After(3 * time.Second):
			t.Fatal("campaign stage did not arrive")
		}
	}
	go func() { defer close(done); l.Campaign(ctx) }()
	defer func() { cancel(); resume(); await(done) }()
	await(started)
	cancel()
	select {
	case releaseCtx := <-lock.entered:
		require.NoError(t, releaseCtx.Err(), "release read does not inherit campaign cancellation")
		_, bounded := releaseCtx.Deadline()
		require.True(t, bounded)
	case <-time.After(3 * time.Second):
		t.Fatal("release read not entered")
	}
	select {
	case <-cleaned:
		t.Fatal("cleanup unexpectedly bypassed blocked release read")
	case <-retired:
		t.Fatal("peer retirement unexpectedly bypassed blocked release read")
	default:
	}
	resume()
	await(cleaned)
	await(retired)
	await(done)
}
