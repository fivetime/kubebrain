package main

import (
	"context"
	"errors"
	"fmt"
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

func TestKeepAliveMonitorPublishesRestartAndValidationFailures(t *testing.T) {
	t.Run("restart", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		responses := make(chan *clientv3.LeaseKeepAliveResponse)
		monitor := startKeepAliveMonitor(ctx, cancel, responses, keepAliveMonitorConfig{
			label: "direct endpoint=failing", clusterID: 7, leaseID: 42, grantedTTL: 15, initialRevision: 10,
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
