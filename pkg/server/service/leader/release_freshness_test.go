// Copyright 2026 ByteDance and/or its affiliates
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package leader

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	metricmock "github.com/kubewharf/kubebrain/pkg/metrics/mock"
	"github.com/kubewharf/kubebrain/pkg/storage"
	"github.com/stretchr/testify/require"
	"k8s.io/client-go/tools/leaderelection/resourcelock"
)

func TestReleaseDoesNotResurrectStaleFreshness(t *testing.T) {
	for _, own := range []bool{false, true} {
		name := "release"
		if own {
			name = "renew"
		}
		t.Run(name, func(t *testing.T) {
			l := &leaderElection{renewDeadline: 25 * time.Second, lastRenew: time.Now().Add(-26 * time.Second)}
			atomic.StoreInt32(&l.leader, 1)
			atomic.StoreUint64(&l.epoch, 1)
			_, fresh := l.EpochAndLeadingFresh()
			require.False(t, fresh)
			underlying := &campaignLock{}
			observed := false
			lock := &renewStampingLock{Interface: underlying, onRenew: l.stampRenew,
				onRecord: func(r resourcelock.LeaderElectionRecord, _ []byte) { observed = true },
			}
			record := resourcelock.LeaderElectionRecord{LeaseDurationSeconds: 1}
			if own {
				record.HolderIdentity = underlying.Identity()
				record.LeaseDurationSeconds = 30
			}
			require.NoError(t, lock.Update(context.Background(), record))
			require.True(t, observed, "successful record must still be observed")
			_, fresh = l.EpochAndLeadingFresh()
			require.Equal(t, own, fresh, "release must not count as successful ownership renewal")
		})
	}
}

type blockedReleaseLock struct {
	campaignLock
	entered chan struct{}
	proceed chan struct{}
	once    sync.Once
	fail    bool
}

func (l *blockedReleaseLock) Update(ctx context.Context, record resourcelock.LeaderElectionRecord) error {
	if record.HolderIdentity == "" {
		l.once.Do(func() { close(l.entered) })
		select {
		case <-l.proceed:
		case <-ctx.Done():
			return ctx.Err()
		}
		if l.fail {
			return storage.ErrUnavailable
		}
	}
	return l.campaignLock.Update(ctx, record)
}

func TestCampaignFencesBeforeDurableRelease(t *testing.T) {
	for _, fail := range []bool{false, true} {
		name := "success"
		if fail {
			name = "failure"
		}
		t.Run(name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			metrics := metricmock.NewMockMetrics(ctrl)
			metrics.EXPECT().EmitCounter(gomock.Any(), gomock.Any(), gomock.Any()).AnyTimes()
			metrics.EXPECT().EmitGauge(gomock.Any(), gomock.Any(), gomock.Any()).AnyTimes()
			lock := &blockedReleaseLock{entered: make(chan struct{}), proceed: make(chan struct{}), fail: fail}
			started := make(chan struct{})
			l := &leaderElection{
				backend: &revisionRecorder{}, resourceLock: lock, metricCli: metrics,
				leaseDuration: 30 * time.Second, renewDeadline: 25 * time.Second, retryPeriod: 10 * time.Millisecond,
				onStartedLeading: func(ctx context.Context) { close(started); <-ctx.Done() },
				onStoppedLeading: func() {},
			}
			ctx, cancel := context.WithCancel(context.Background())
			done := make(chan struct{})
			var unblock sync.Once
			resume := func() { unblock.Do(func() { close(lock.proceed) }) }
			go func() { defer close(done); l.Campaign(ctx) }()
			defer func() {
				cancel()
				resume()
				select {
				case <-done:
				case <-time.After(2 * time.Second):
					t.Error("campaign did not join")
				}
			}()
			select {
			case <-started:
			case <-time.After(time.Second):
				t.Fatal("did not start")
			}
			_, fresh := l.EpochAndLeadingFresh()
			require.True(t, fresh, "acquisition must admit while current")
			cancel()
			select {
			case <-lock.entered:
			case <-time.After(time.Second):
				t.Fatal("release not entered")
			}
			// The storage call cannot complete, so OnStoppedLeading has not run yet.
			require.True(t, l.IsLeader(), "test must observe before delayed lifecycle callback")
			_, fresh = l.EpochAndLeadingFresh()
			require.False(t, fresh, "must fence before durable release, not after callback")
			resume()
			select {
			case <-done:
			case <-time.After(2 * time.Second):
				t.Fatal("release did not finish")
			}
			require.False(t, l.IsLeader())
			l.renewMu.RLock()
			invalidated := l.lastRenew.IsZero()
			l.renewMu.RUnlock()
			require.True(t, invalidated, "release outcome must not restamp ownership")
			record, _, err := lock.Get(context.Background())
			require.NoError(t, err)
			if fail {
				require.Equal(t, lock.Identity(), record.HolderIdentity)
			} else {
				require.Empty(t, record.HolderIdentity)
			}
		})
	}
}
