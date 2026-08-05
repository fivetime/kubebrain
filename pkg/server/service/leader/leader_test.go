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
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
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

type skewablePassiveClock struct {
	now     time.Time
	elapsed time.Duration
}

func (c *skewablePassiveClock) Now() time.Time { return c.now }

func (c *skewablePassiveClock) Since(time.Time) time.Duration { return c.elapsed }

func (r *revisionRecorder) InitializeLeadershipRevision(_ context.Context, revision uint64) error {
	r.revision.Store(revision)
	return nil
}

func TestHasLeaderExpiresObservedElectionRecord(t *testing.T) {
	lock := &campaignLock{
		record: resourcelock.LeaderElectionRecord{HolderIdentity: "peer"},
		tso:    1,
	}
	election := &leaderElection{
		resourceLock:  lock,
		leaseDuration: time.Second,
		renewDeadline: 500 * time.Millisecond,
	}

	election.observeLeadershipRecord(resourcelock.LeaderElectionRecord{
		HolderIdentity: "peer",
		RenewTime:      metav1.NewTime(time.Now()),
	})
	require.True(t, election.HasLeader())

	expired := &leaderElection{
		resourceLock:  lock,
		leaseDuration: 20 * time.Millisecond,
		renewDeadline: 10 * time.Millisecond,
	}
	expired.observeLeadershipRecord(resourcelock.LeaderElectionRecord{
		HolderIdentity: "peer",
		RenewTime:      metav1.NewTime(time.Now().Add(-2 * time.Second)),
	})
	require.True(t, expired.HasLeader(), "remote wall-clock age must not pre-expire a newly observed lease")
	require.Eventually(t, func() bool { return !expired.HasLeader() }, 200*time.Millisecond, time.Millisecond)

	election.observeLeadershipRecord(resourcelock.LeaderElectionRecord{
		HolderIdentity: "peer",
		RenewTime:      metav1.NewTime(time.Now().Add(-2 * time.Second)),
	})
	require.True(t, election.HasLeader(), "an older observation must not shorten leader validity")

	atomic.StoreInt32(&election.leader, 1)
	election.stampRenew()
	require.True(t, election.HasLeader(), "fresh local leadership must not depend on a cached remote record")
}

func TestHasLeaderUsesLocalObservationTimeInsteadOfWriterClock(t *testing.T) {
	lock := &campaignLock{
		record: resourcelock.LeaderElectionRecord{HolderIdentity: "peer"},
		tso:    1,
	}
	election := &leaderElection{
		resourceLock:  lock,
		leaseDuration: 20 * time.Millisecond,
		renewDeadline: 10 * time.Millisecond,
	}

	// RenewTime is supplied by the remote holder and may be arbitrarily ahead
	// of this process's wall clock. client-go deliberately expires a lease from
	// the local time at which a new raw record was observed, not this timestamp.
	election.observeLeadershipRecord(resourcelock.LeaderElectionRecord{
		HolderIdentity: "peer",
		RenewTime:      metav1.NewTime(time.Now().Add(time.Hour)),
	})
	require.True(t, election.HasLeader())
	require.Eventually(t, func() bool { return !election.HasLeader() },
		200*time.Millisecond, time.Millisecond,
		"a writer clock ahead of the observer must not extend require-leader availability")
}

func TestHasLeaderDoesNotRenewUnchangedRawRecord(t *testing.T) {
	lock := &campaignLock{
		record: resourcelock.LeaderElectionRecord{HolderIdentity: "peer"},
		tso:    1,
	}
	election := &leaderElection{
		resourceLock:  lock,
		leaseDuration: 80 * time.Millisecond,
		renewDeadline: 40 * time.Millisecond,
	}
	record := resourcelock.LeaderElectionRecord{HolderIdentity: "peer"}
	raw, err := json.Marshal(record)
	require.NoError(t, err)

	election.observeLeadershipRecordRaw(record, raw)
	time.Sleep(50 * time.Millisecond)
	election.observeLeadershipRecordRaw(record, raw)
	require.Eventually(t, func() bool { return !election.HasLeader() },
		60*time.Millisecond, time.Millisecond,
		"polling the same stored record must not renew its locally observed lease")
}

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
		onRecord:  l.observeLeadershipRecordRaw,
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
	var prepares atomic.Int32
	var preparePublishedLeader atomic.Bool
	var activeCallbacks atomic.Int32
	var maxActiveCallbacks atomic.Int32
	var restoreOnce sync.Once
	firstStarted := make(chan struct{})
	var election *leaderElection
	election = &leaderElection{
		backend:      revisions,
		resourceLock: lock,
		metricCli:    m,
		onPreparingLeading: func() {
			prepares.Add(1)
			if election.IsLeader() {
				preparePublishedLeader.Store(true)
			}
		},
		onStartedLeading: func(leadingCtx context.Context) {
			active := activeCallbacks.Add(1)
			defer activeCallbacks.Add(-1)
			for {
				maxActive := maxActiveCallbacks.Load()
				if active <= maxActive || maxActiveCallbacks.CompareAndSwap(maxActive, active) {
					break
				}
			}
			if starts.Add(1) == 1 {
				close(firstStarted)
				lock.failUpdates.Store(true)
				restoreOnce.Do(func() {
					go func() {
						time.Sleep(1200 * time.Millisecond)
						lock.failUpdates.Store(false)
					}()
				})
				<-leadingCtx.Done()
				// Model term cleanup that honors cancellation but still needs
				// bounded time to unwind storage/lease state.
				time.Sleep(250 * time.Millisecond)
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
	require.Equal(t, int32(1), maxActiveCallbacks.Load(),
		"adjacent leadership callbacks must never overlap")
	require.False(t, preparePublishedLeader.Load(),
		"preparing callback must run before leadership is published")
	require.Equal(t, starts.Load(), prepares.Load(),
		"every leadership term must prepare its dependent state exactly once")
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
	l.lastRenew = time.Now().Add(-defaultRenewDeadline - time.Second)

	epoch, fresh := l.EpochAndLeadingFresh()
	ast.Equal(uint64(9), epoch)
	ast.False(fresh, "leader with a renew older than the validity bound must not be fresh")
}

func TestEpochAndLeadingFreshUsesMonotonicElapsedTime(t *testing.T) {
	clock := &skewablePassiveClock{now: time.Now()}
	l := &leaderElection{renewDeadline: time.Second, clock: clock}
	atomic.StoreInt32(&l.leader, 1)
	l.stampRenew()
	require.True(t, func() bool { _, fresh := l.EpochAndLeadingFresh(); return fresh }())

	// Model NTP stepping the wall clock backward while monotonic time continues.
	// A UnixNano timestamp would now appear to come from the future and keep an
	// isolated old leader writable beyond the successor's acquisition window.
	clock.now = clock.now.Add(-time.Hour)
	clock.elapsed = l.renewDeadline + time.Nanosecond
	_, fresh := l.EpochAndLeadingFresh()
	require.False(t, fresh, "renew freshness must expire by monotonic elapsed time despite wall-clock rollback")
}

// TestEpochAndLeadingFreshNeverRenewed verifies that a leader flag set without any
// recorded renew (zero lastRenew) is not reported fresh.
func TestEpochAndLeadingFreshNeverRenewed(t *testing.T) {
	ast := assert.New(t)
	l := &leaderElection{renewDeadline: defaultRenewDeadline}
	atomic.StoreInt32(&l.leader, 1)
	// lastRenew left at zero value
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
	l.lastRenew = time.Now().Add(-defaultRenewDeadline - time.Second)
	_, fresh := l.EpochAndLeadingFresh()
	ast.False(fresh)

	l.stampRenew()
	_, fresh = l.EpochAndLeadingFresh()
	ast.True(fresh)
}
