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
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"k8s.io/client-go/tools/leaderelection/resourcelock"
)

type termResourceLock struct {
	resourcelock.Interface
	record resourcelock.LeaderElectionRecord
	empty  bool
	err    error
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
