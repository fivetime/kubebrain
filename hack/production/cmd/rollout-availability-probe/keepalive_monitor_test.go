package main

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	clientv3 "go.etcd.io/etcd/client/v3"
)

func keepAliveResponse(revision int64) *clientv3.LeaseKeepAliveResponse {
	return &clientv3.LeaseKeepAliveResponse{
		ResponseHeader: rolloutHeader(revision),
		ID:             clientv3.LeaseID(42),
		TTL:            15,
	}
}

func immediateTime() <-chan time.Time {
	ready := make(chan time.Time, 1)
	ready <- time.Now()
	return ready
}

func TestKeepAliveMonitorContinuouslyDrainsBeyondClientQueueCapacity(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	responses := make(chan *clientv3.LeaseKeepAliveResponse, 16)
	monitor := startKeepAliveMonitor(ctx, cancel, responses, keepAliveMonitorConfig{
		label:           "public",
		clusterID:       7,
		leaseID:         42,
		grantedTTL:      15,
		initialRevision: 10,
	})
	t.Cleanup(monitor.stop)

	const responseCount = 64
	sent := make(chan struct{})
	go func() {
		defer close(sent)
		for i := 0; i < responseCount; i++ {
			responses <- keepAliveResponse(10)
		}
	}()

	select {
	case <-sent:
	case <-time.After(time.Second):
		t.Fatal("producer blocked after filling the client-sized response queue")
	}
	require.NoError(t, monitor.waitForResponses(context.Background(), responseCount, time.Second))
	snapshot := monitor.snapshot()
	require.Equal(t, responseCount, snapshot.responses)
	require.NoError(t, snapshot.err)
}

func TestKeepAliveMonitorAllowsOnlyOneCompletedRecoveryEpisode(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	initial := make(chan *clientv3.LeaseKeepAliveResponse)
	firstRecovery := make(chan *clientv3.LeaseKeepAliveResponse)
	secondRecovery := make(chan *clientv3.LeaseKeepAliveResponse)
	restarts := 0
	monitor := startKeepAliveMonitor(ctx, cancel, initial, keepAliveMonitorConfig{
		label:           "direct endpoint=ordinal-0",
		clusterID:       7,
		leaseID:         42,
		grantedTTL:      15,
		initialRevision: 10,
		recoveryTimeout: time.Second,
		retryWait:       time.Millisecond,
		restart: func(context.Context) (<-chan *clientv3.LeaseKeepAliveResponse, error) {
			restarts++
			switch restarts {
			case 1:
				return firstRecovery, nil
			case 2:
				return secondRecovery, nil
			default:
				return nil, fmt.Errorf("unexpected restart %d", restarts)
			}
		},
	})
	t.Cleanup(monitor.stop)

	close(initial)
	require.Eventually(t, func() bool { return monitor.snapshot().restarts == 1 }, time.Second, time.Millisecond)
	close(firstRecovery)
	require.Eventually(t, func() bool { return monitor.snapshot().restarts == 2 }, time.Second, time.Millisecond)
	secondRecovery <- keepAliveResponse(11)
	require.NoError(t, monitor.waitForResponses(context.Background(), 1, time.Second))
	require.False(t, monitor.snapshot().recovering)

	close(secondRecovery)
	require.Eventually(t, func() bool { return monitor.snapshot().err != nil }, time.Second, time.Millisecond)
	require.ErrorContains(t, monitor.snapshot().err, "closed after completed recovery")
	require.Equal(t, 2, restarts, "a second completed recovery episode must not restart")
}

func TestKeepAliveMonitorAllowsConfiguredCompletedRecoveryEpisodes(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	initial := make(chan *clientv3.LeaseKeepAliveResponse)
	firstRecovery := make(chan *clientv3.LeaseKeepAliveResponse, 1)
	secondRecovery := make(chan *clientv3.LeaseKeepAliveResponse, 1)
	restarts := 0
	monitor := startKeepAliveMonitor(ctx, cancel, initial, keepAliveMonitorConfig{
		label: "public", clusterID: 7, leaseID: 42, grantedTTL: 15, initialRevision: 10,
		recoveryTimeout: time.Second,
		retryWait:       time.Millisecond,
		maxRecoveries:   2,
		restart: func(context.Context) (<-chan *clientv3.LeaseKeepAliveResponse, error) {
			restarts++
			if restarts == 1 {
				return firstRecovery, nil
			}
			return secondRecovery, nil
		},
	})
	t.Cleanup(monitor.stop)

	close(initial)
	require.Eventually(t, func() bool { return monitor.snapshot().restarts == 1 }, time.Second, time.Millisecond)
	firstRecovery <- keepAliveResponse(11)
	require.Eventually(t, func() bool { return monitor.snapshot().recoveries == 1 }, time.Second, time.Millisecond)
	close(firstRecovery)
	require.Eventually(t, func() bool { return monitor.snapshot().restarts == 2 }, time.Second, time.Millisecond)
	secondRecovery <- keepAliveResponse(12)
	require.Eventually(t, func() bool { return monitor.snapshot().recoveries == 2 }, time.Second, time.Millisecond)

	close(secondRecovery)
	require.Eventually(t, func() bool { return monitor.snapshot().err != nil }, time.Second, time.Millisecond)
	require.ErrorContains(t, monitor.snapshot().err, "episodes=2 limit=2")
	require.Equal(t, 2, restarts)
}

func TestKeepAliveMonitorPublishesRestartAndValidationFailures(t *testing.T) {
	t.Run("restart", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		responses := make(chan *clientv3.LeaseKeepAliveResponse)
		monitor := startKeepAliveMonitor(ctx, cancel, responses, keepAliveMonitorConfig{
			label: "direct endpoint=failing", clusterID: 7, leaseID: 42, grantedTTL: 15, initialRevision: 10, recoveryTimeout: time.Second,
			restart: func(context.Context) (<-chan *clientv3.LeaseKeepAliveResponse, error) {
				return nil, errors.New("restart failed")
			},
		})
		t.Cleanup(monitor.stop)
		close(responses)
		require.Eventually(t, func() bool { return monitor.snapshot().err != nil }, time.Second, time.Millisecond)
		require.ErrorContains(t, monitor.snapshot().err, "restart failed")
		require.Zero(t, monitor.snapshot().restarts)
	})

	t.Run("response", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		responses := make(chan *clientv3.LeaseKeepAliveResponse, 1)
		monitor := startKeepAliveMonitor(ctx, cancel, responses, keepAliveMonitorConfig{
			label: "public", clusterID: 7, leaseID: 42, grantedTTL: 15, initialRevision: 10,
		})
		t.Cleanup(monitor.stop)
		responses <- keepAliveResponse(9)
		require.Eventually(t, func() bool { return monitor.snapshot().err != nil }, time.Second, time.Millisecond)
		require.ErrorContains(t, monitor.snapshot().err, "invalid response header")
	})
}

func TestKeepAliveMonitorWaitForFreshResponse(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	responses := make(chan *clientv3.LeaseKeepAliveResponse, 1)
	monitor := startKeepAliveMonitor(ctx, cancel, responses, keepAliveMonitorConfig{
		label: "public", clusterID: 7, leaseID: 42, grantedTTL: 15, initialRevision: 10,
	})
	t.Cleanup(monitor.stop)

	responses <- keepAliveResponse(10)
	require.NoError(t, monitor.waitForResponses(context.Background(), 1, time.Second))
	marker := time.Now()
	require.ErrorContains(t, monitor.waitForFreshResponse(context.Background(), marker, 10*time.Millisecond), "fresh response timed out")
	responses <- keepAliveResponse(11)
	require.NoError(t, monitor.waitForFreshResponse(context.Background(), marker, time.Second))
}

func TestKeepAliveMonitorRecoveryDeadlineSurvivesRepeatedClosures(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	initial := make(chan *clientv3.LeaseKeepAliveResponse)
	firstRecovery := make(chan *clientv3.LeaseKeepAliveResponse)
	secondRecovery := make(chan *clientv3.LeaseKeepAliveResponse)
	deadline := make(chan time.Time, 1)
	var timerStarts atomic.Int32
	restarts := 0
	monitor := startKeepAliveMonitor(ctx, cancel, initial, keepAliveMonitorConfig{
		label: "direct endpoint=ordinal-0", clusterID: 7, leaseID: 42, grantedTTL: 15, initialRevision: 10,
		recoveryTimeout: 30 * time.Second,
		retryWait:       500 * time.Millisecond,
		retryAfter:      func(time.Duration) <-chan time.Time { return immediateTime() },
		after: func(timeout time.Duration) <-chan time.Time {
			require.Equal(t, 30*time.Second, timeout)
			timerStarts.Add(1)
			return deadline
		},
		restart: func(context.Context) (<-chan *clientv3.LeaseKeepAliveResponse, error) {
			restarts++
			if restarts == 1 {
				return firstRecovery, nil
			}
			return secondRecovery, nil
		},
	})
	t.Cleanup(monitor.stop)

	close(initial)
	require.Eventually(t, func() bool { return monitor.snapshot().restarts == 1 }, time.Second, time.Millisecond)
	close(firstRecovery)
	require.Eventually(t, func() bool { return monitor.snapshot().restarts == 2 }, time.Second, time.Millisecond)
	require.Equal(t, int32(1), timerStarts.Load(), "replacement closure must not reset the recovery deadline")
	deadline <- time.Now()
	require.Eventually(t, func() bool { return monitor.snapshot().err != nil }, time.Second, time.Millisecond)
	require.ErrorContains(t, monitor.snapshot().err, "recovery exceeded 30s")
}

func TestKeepAliveMonitorRecoveryDeadlineBoundsBlockedRestart(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	initial := make(chan *clientv3.LeaseKeepAliveResponse)
	deadline := make(chan time.Time, 1)
	restartEntered := make(chan struct{})
	monitor := startKeepAliveMonitor(ctx, cancel, initial, keepAliveMonitorConfig{
		label: "direct endpoint=ordinal-0", clusterID: 7, leaseID: 42, grantedTTL: 15, initialRevision: 10,
		recoveryTimeout: 30 * time.Second,
		after:           func(time.Duration) <-chan time.Time { return deadline },
		restart: func(restartCtx context.Context) (<-chan *clientv3.LeaseKeepAliveResponse, error) {
			close(restartEntered)
			<-restartCtx.Done()
			return nil, restartCtx.Err()
		},
	})
	t.Cleanup(monitor.stop)

	close(initial)
	select {
	case <-restartEntered:
	case <-time.After(time.Second):
		t.Fatal("restart was not attempted")
	}
	deadline <- time.Now()
	require.Eventually(t, func() bool { return monitor.snapshot().err != nil }, time.Second, time.Millisecond)
	require.ErrorContains(t, monitor.snapshot().err, "recovery exceeded 30s")
}

func TestKeepAliveMonitorDisarmsRecoveryDeadlineAfterResponse(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	initial := make(chan *clientv3.LeaseKeepAliveResponse)
	recovered := make(chan *clientv3.LeaseKeepAliveResponse, 1)
	deadline := make(chan time.Time, 1)
	monitor := startKeepAliveMonitor(ctx, cancel, initial, keepAliveMonitorConfig{
		label: "direct endpoint=ordinal-0", clusterID: 7, leaseID: 42, grantedTTL: 15, initialRevision: 10,
		recoveryTimeout: 30 * time.Second,
		after:           func(time.Duration) <-chan time.Time { return deadline },
		restart:         func(context.Context) (<-chan *clientv3.LeaseKeepAliveResponse, error) { return recovered, nil },
	})
	t.Cleanup(monitor.stop)

	close(initial)
	require.Eventually(t, func() bool { return monitor.snapshot().recovering }, time.Second, time.Millisecond)
	recovered <- keepAliveResponse(11)
	require.NoError(t, monitor.waitForResponses(context.Background(), 1, time.Second))
	snapshot := monitor.snapshot()
	require.False(t, snapshot.recovering)
	require.Positive(t, snapshot.maxRecovery)
	deadline <- time.Now()
	require.Never(t, func() bool { return monitor.snapshot().err != nil }, 20*time.Millisecond, time.Millisecond)
}

func TestKeepAliveMonitorRejectsEmptyRecoveryTimer(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	initial := make(chan *clientv3.LeaseKeepAliveResponse)
	monitor := startKeepAliveMonitor(ctx, cancel, initial, keepAliveMonitorConfig{
		label: "direct endpoint=ordinal-0", clusterID: 7, leaseID: 42, grantedTTL: 15, initialRevision: 10,
		recoveryTimeout: 30 * time.Second,
		after:           func(time.Duration) <-chan time.Time { return nil },
		restart:         func(context.Context) (<-chan *clientv3.LeaseKeepAliveResponse, error) { return nil, nil },
	})
	t.Cleanup(monitor.stop)

	close(initial)
	require.Eventually(t, func() bool { return monitor.snapshot().err != nil }, time.Second, time.Millisecond)
	require.ErrorContains(t, monitor.snapshot().err, "recovery timer returned an empty channel")
}

func TestKeepAliveMonitorWaitsBeforeRepeatedReplacement(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	initial := make(chan *clientv3.LeaseKeepAliveResponse)
	firstRecovery := make(chan *clientv3.LeaseKeepAliveResponse)
	close(firstRecovery)
	secondRecovery := make(chan *clientv3.LeaseKeepAliveResponse)
	retryGate := make(chan time.Time, 1)
	var retryWaits atomic.Int32
	restarts := 0
	monitor := startKeepAliveMonitor(ctx, cancel, initial, keepAliveMonitorConfig{
		label: "direct endpoint=ordinal-0", clusterID: 7, leaseID: 42, grantedTTL: 15, initialRevision: 10,
		recoveryTimeout: 30 * time.Second,
		retryWait:       500 * time.Millisecond,
		retryAfter: func(wait time.Duration) <-chan time.Time {
			require.Equal(t, 500*time.Millisecond, wait)
			retryWaits.Add(1)
			return retryGate
		},
		restart: func(context.Context) (<-chan *clientv3.LeaseKeepAliveResponse, error) {
			restarts++
			if restarts == 1 {
				return firstRecovery, nil
			}
			return secondRecovery, nil
		},
	})
	t.Cleanup(monitor.stop)

	close(initial)
	require.Eventually(t, func() bool { return retryWaits.Load() == 1 }, time.Second, time.Millisecond)
	require.Equal(t, 1, monitor.snapshot().restarts)
	require.Never(t, func() bool { return monitor.snapshot().restarts > 1 }, 20*time.Millisecond, time.Millisecond,
		"an immediately closed replacement must not hot-loop")
	retryGate <- time.Now()
	require.Eventually(t, func() bool { return monitor.snapshot().restarts == 2 }, time.Second, time.Millisecond)
}
