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
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	metricmock "github.com/kubewharf/kubebrain/pkg/metrics/mock"
	"github.com/kubewharf/kubebrain/pkg/storage"
	"k8s.io/client-go/tools/leaderelection/resourcelock"
)

// Only acquisition succeeds unless the test explicitly enables later renewals.
type acquisitionOnlyLock struct {
	campaignLock
	updates       atomic.Int32
	allowRenewals atomic.Bool
}

func (l *acquisitionOnlyLock) Update(ctx context.Context, r resourcelock.LeaderElectionRecord) error {
	if l.updates.Add(1) != 1 && !l.allowRenewals.Load() {
		return storage.ErrUnavailable
	}
	return l.campaignLock.Update(ctx, r)
}

type initializationFunc func(context.Context) error

func (f initializationFunc) InitializeLeadershipRevision(ctx context.Context, _ uint64) error {
	return f(ctx)
}

func TestInitializationPreservesRealSuccessfulRenewal(t *testing.T) {
	ctrl := gomock.NewController(t)
	m := metricmock.NewMockMetrics(ctrl)
	m.EXPECT().EmitCounter(gomock.Any(), gomock.Any(), gomock.Any()).AnyTimes()
	m.EXPECT().EmitGauge(gomock.Any(), gomock.Any(), gomock.Any()).AnyTimes()
	clock := &initializationClock{}
	clock.nanos.Store(time.Now().UnixNano())
	lock := &acquisitionOnlyLock{}
	result := make(chan bool, 1)
	l := &leaderElection{
		resourceLock: lock, metricCli: m, clock: clock,
		leaseDuration: 30 * time.Second, renewDeadline: 25 * time.Second,
		retryPeriod: 10 * time.Millisecond, onStoppedLeading: func() {},
	}
	l.backend = initializationFunc(func(ctx context.Context) error {
		clock.nanos.Add(int64(26 * time.Second))
		lock.allowRenewals.Store(true)
		timer := time.NewTimer(time.Second)
		defer timer.Stop()
		poll := time.NewTicker(time.Millisecond)
		defer poll.Stop()
		for {
			l.renewMu.RLock()
			renewed := l.lastRenew.Equal(clock.Now())
			l.renewMu.RUnlock()
			if renewed {
				return nil
			}
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-timer.C:
				return fmt.Errorf("no real elector renewal observed during initialization")
			case <-poll.C:
			}
		}
	})
	l.onStartedLeading = func(ctx context.Context) {
		_, fresh := l.EpochAndLeadingFresh()
		result <- fresh
		<-ctx.Done()
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
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
	case fresh := <-result:
		if !fresh {
			t.Fatal("successful renewal during initialization was discarded")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("initialization callback did not run")
	}
}

type initializationClock struct{ nanos atomic.Int64 }

func (c *initializationClock) Now() time.Time                  { return time.Unix(0, c.nanos.Load()) }
func (c *initializationClock) Since(t time.Time) time.Duration { return c.Now().Sub(t) }

type slowInitialization struct{ clock *initializationClock }

func (b *slowInitialization) InitializeLeadershipRevision(context.Context, uint64) error {
	// Advance only the freshness clock, not client-go's scheduling clock.
	// This isolates initialization from a concurrently expiring elector callback.
	b.clock.nanos.Add(int64(26 * time.Second))
	return nil
}

func TestInitializationMustNotInventSuccessfulRenewal(t *testing.T) {
	ctrl := gomock.NewController(t)
	m := metricmock.NewMockMetrics(ctrl)
	m.EXPECT().EmitCounter(gomock.Any(), gomock.Any(), gomock.Any()).AnyTimes()
	m.EXPECT().EmitGauge(gomock.Any(), gomock.Any(), gomock.Any()).AnyTimes()
	clock := &initializationClock{}
	clock.nanos.Store(time.Now().UnixNano())
	lock := &acquisitionOnlyLock{}
	result := make(chan bool, 1)
	l := &leaderElection{
		backend: &slowInitialization{clock}, resourceLock: lock, metricCli: m,
		clock: clock, leaseDuration: 30 * time.Second, renewDeadline: 25 * time.Second,
		retryPeriod:      500 * time.Millisecond,
		onStoppedLeading: func() {},
	}
	l.onStartedLeading = func(ctx context.Context) {
		_, fresh := l.EpochAndLeadingFresh()
		result <- fresh
		<-ctx.Done()
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
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
	case fresh := <-result:
		if fresh {
			t.Fatal("initialization refreshed leadership without a successful durable renewal")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("initialization callback did not run")
	}
}
