// Copyright 2026 ByteDance and/or its affiliates
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package leader

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"k8s.io/client-go/tools/leaderelection/resourcelock"

	metricmock "github.com/kubewharf/kubebrain/pkg/metrics/mock"
	"github.com/kubewharf/kubebrain/pkg/storage"
)

type termResourceLock struct {
	resourcelock.Interface
	record resourcelock.LeaderElectionRecord
	empty  bool
	err    error
}

type campaignLock struct {
	mu          sync.Mutex
	record      resourcelock.LeaderElectionRecord
	raw         []byte
	tso         uint64
	failUpdates atomic.Bool
}

func (l *campaignLock) Get(context.Context) (*resourcelock.LeaderElectionRecord, []byte, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	record := l.record
	return &record, append([]byte(nil), l.raw...), nil
}

func (l *campaignLock) Create(_ context.Context, record resourcelock.LeaderElectionRecord) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if len(l.raw) != 0 {
		return errors.New("already exists")
	}
	l.set(record)
	return nil
}

func (l *campaignLock) Update(_ context.Context, record resourcelock.LeaderElectionRecord) error {
	if l.failUpdates.Load() {
		return storage.ErrUnavailable
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	l.set(record)
	return nil
}

func (l *campaignLock) set(record resourcelock.LeaderElectionRecord) {
	l.record = record
	l.raw, _ = json.Marshal(record)
	l.tso++
}

func (l *campaignLock) RecordEvent(string) {}
func (l *campaignLock) Identity() string   { return "campaign-retry" }
func (l *campaignLock) Describe() string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return fmt.Sprintf("%s,%d", l.record.HolderIdentity, l.tso)
}

type revisionRecorder struct{ revision atomic.Uint64 }

func (r *revisionRecorder) SetCurrentRevision(revision uint64) { r.revision.Store(revision) }

func (l termResourceLock) Get(context.Context) (*resourcelock.LeaderElectionRecord, []byte, error) {
	if l.err != nil {
		return nil, nil, l.err
	}
	if l.empty {
		return nil, nil, nil
	}
	return &l.record, nil, nil
}

func TestLeadershipTermUsesSharedTransitionCounter(t *testing.T) {
	for _, tc := range []struct {
		name        string
		transitions int
		want        uint64
	}{
		{name: "initial holder", transitions: 0, want: 1},
		{name: "after failovers", transitions: 6, want: 7},
	} {
		t.Run(tc.name, func(t *testing.T) {
			l := &leaderElection{resourceLock: termResourceLock{record: resourcelock.LeaderElectionRecord{
				LeaderTransitions: tc.transitions,
			}}}
			term, err := l.LeadershipTerm(context.Background())
			require.NoError(t, err)
			require.Equal(t, tc.want, term)
			require.Equal(t, tc.want, l.CurrentLeadershipTerm())
		})
	}
}

func TestRenewStampingLockCachesObservedTerm(t *testing.T) {
	l := &leaderElection{}
	lock := &renewStampingLock{
		Interface: termResourceLock{record: resourcelock.LeaderElectionRecord{LeaderTransitions: 8}},
		onRenew:   func() {},
		onRecord:  l.observeLeadershipRecord,
	}
	record, _, err := lock.Get(context.Background())
	require.NoError(t, err)
	require.Equal(t, 8, record.LeaderTransitions)
	require.Equal(t, uint64(9), l.CurrentLeadershipTerm())
	l.observeLeadershipRecord(resourcelock.LeaderElectionRecord{LeaderTransitions: 3})
	require.Equal(t, uint64(9), l.CurrentLeadershipTerm(), "a stale lock read must not regress response terms")
}

func TestLeadershipTermFailsClosedOnInvalidRecord(t *testing.T) {
	l := &leaderElection{resourceLock: termResourceLock{record: resourcelock.LeaderElectionRecord{LeaderTransitions: -1}}}
	_, err := l.LeadershipTerm(context.Background())
	require.ErrorContains(t, err, "negative leader transitions")

	l.resourceLock = termResourceLock{empty: true}
	_, err = l.LeadershipTerm(context.Background())
	require.ErrorContains(t, err, "empty election record")

	l.resourceLock = termResourceLock{err: errors.New("storage unavailable")}
	_, err = l.LeadershipTerm(context.Background())
	require.ErrorContains(t, err, "storage unavailable")
}

func TestCampaignReacquiresLeadershipAfterStorageOutage(t *testing.T) {
	ctrl := gomock.NewController(t)
	m := metricmock.NewMinimalMetrics(ctrl)
	m.(*metricmock.MockMetrics).EXPECT().EmitCounter(gomock.Any(), gomock.Any()).AnyTimes()
	lock := &campaignLock{}
	revisions := &revisionRecorder{}

	var starts atomic.Int32
	var stops atomic.Int32
	var restoreOnce sync.Once
	firstStarted := make(chan struct{})
	election := &leaderElection{
		backend:      revisions,
		resourceLock: lock,
		metricCli:    m,
		onStartedLeading: func(context.Context) {
			if starts.Add(1) == 1 {
				close(firstStarted)
				lock.failUpdates.Store(true)
				restoreOnce.Do(func() {
					go func() {
						time.Sleep(1200 * time.Millisecond)
						lock.failUpdates.Store(false)
					}()
				})
			}
		},
		onStoppedLeading: func() { stops.Add(1) },
		leaseDuration:    time.Second,
		renewDeadline:    600 * time.Millisecond,
		retryPeriod:      100 * time.Millisecond,
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	done := make(chan struct{})
	go func() {
		election.Campaign(ctx)
		close(done)
	}()

	select {
	case <-firstStarted:
	case <-time.After(time.Second):
		t.Fatal("campaign did not invoke the initial leadership callback")
	}
	require.True(t, lock.failUpdates.Load())
	require.Eventually(t, func() bool {
		return starts.Load() >= 2 && stops.Load() >= 1 && election.IsLeader()
	}, 4*time.Second, 20*time.Millisecond,
		"campaign must remain alive and reacquire leadership after storage recovers")
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("campaign did not stop after context cancellation")
	}
	require.False(t, election.IsLeader())
}

// TestEpochAndLeadingFreshNotLeader verifies a non-leader is never reported fresh.
func TestEpochAndLeadingFreshNotLeader(t *testing.T) {
	ast := assert.New(t)
	l := &leaderElection{renewDeadline: defaultRenewDeadline}
	atomic.StoreUint64(&l.epoch, 3)
	// leader==0, even with a recent renew
	l.stampRenew()
	epoch, fresh := l.EpochAndLeadingFresh()
	ast.Equal(uint64(3), epoch)
	ast.False(fresh)
}

// TestEpochAndLeadingFreshLeaderRecentRenew verifies a leader that renewed within
// the validity bound is reported fresh, carrying its current epoch.
func TestEpochAndLeadingFreshLeaderRecentRenew(t *testing.T) {
	ast := assert.New(t)
	l := &leaderElection{renewDeadline: defaultRenewDeadline}
	atomic.StoreUint64(&l.epoch, 5)
	atomic.StoreInt32(&l.leader, 1)
	l.stampRenew()
	epoch, fresh := l.EpochAndLeadingFresh()
	ast.Equal(uint64(5), epoch)
	ast.True(fresh)
}

// TestEpochAndLeadingFreshStaleRenew verifies that a partitioned-but-unaware leader
// (leader flag still set, but no successful renew within the validity bound) stops
// being reported fresh, so it self-fences before its lease can be taken over. This
// is the FINDING #39 timing-margin guarantee.
func TestEpochAndLeadingFreshStaleRenew(t *testing.T) {
	ast := assert.New(t)
	l := &leaderElection{renewDeadline: defaultRenewDeadline}
	atomic.StoreUint64(&l.epoch, 9)
	atomic.StoreInt32(&l.leader, 1)
	// last successful renew is older than the validity bound
	stale := time.Now().Add(-defaultRenewDeadline - time.Second).UnixNano()
	atomic.StoreInt64(&l.lastRenewNanos, stale)

	epoch, fresh := l.EpochAndLeadingFresh()
	ast.Equal(uint64(9), epoch)
	ast.False(fresh, "leader with a renew older than the validity bound must not be fresh")
}

// TestEpochAndLeadingFreshNeverRenewed verifies that a leader flag set without any
// recorded renew (lastRenewNanos == 0) is not reported fresh.
func TestEpochAndLeadingFreshNeverRenewed(t *testing.T) {
	ast := assert.New(t)
	l := &leaderElection{renewDeadline: defaultRenewDeadline}
	atomic.StoreInt32(&l.leader, 1)
	// lastRenewNanos left at zero value
	_, fresh := l.EpochAndLeadingFresh()
	ast.False(fresh)
}

// TestStampRenewRestoresFreshness verifies a fresh renew re-admits a leader whose
// freshness had lapsed, and that the validity bound is strictly less than the
// election LeaseDuration so the fence closes before a successor can acquire.
func TestStampRenewRestoresFreshness(t *testing.T) {
	ast := assert.New(t)
	// the timing invariant the non-storage fence layers depend on
	ast.Less(defaultRenewDeadline, defaultLeaseDuration, "validity bound must be < LeaseDuration")

	l := &leaderElection{renewDeadline: defaultRenewDeadline}
	atomic.StoreInt32(&l.leader, 1)
	atomic.StoreInt64(&l.lastRenewNanos, time.Now().Add(-defaultRenewDeadline-time.Second).UnixNano())
	_, fresh := l.EpochAndLeadingFresh()
	ast.False(fresh)

	l.stampRenew()
	_, fresh = l.EpochAndLeadingFresh()
	ast.True(fresh)
}
