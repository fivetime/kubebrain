package main

import (
	"context"
	"fmt"
	"sync"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
)

type keepAliveRestart func(context.Context) (<-chan *clientv3.LeaseKeepAliveResponse, error)

type keepAliveMonitorConfig struct {
	label           string
	clusterID       uint64
	leaseID         clientv3.LeaseID
	grantedTTL      int64
	initialRevision int64
	recoveryTimeout time.Duration
	after           func(time.Duration) <-chan time.Time
	retryWait       time.Duration
	retryAfter      func(time.Duration) <-chan time.Time
	restart         keepAliveRestart
}

type keepAliveMonitorSnapshot struct {
	lastRevision int64
	lastResponse time.Time
	responses    int
	restarts     int
	recovering   bool
	recoveryFrom time.Time
	maxRecovery  time.Duration
	err          error
}

type keepAliveRestartResult struct {
	responses <-chan *clientv3.LeaseKeepAliveResponse
	err       error
}

// keepAliveMonitor owns the only receive path for one clientv3 KeepAlive
// subscription. clientv3 deliberately drops responses when its 16-entry
// subscriber queue is full, so tying receives to the rollout operation loop
// would weaken the availability evidence whenever an operation stalls.
type keepAliveMonitor struct {
	ctx    context.Context
	cancel context.CancelFunc
	done   chan struct{}
	events chan struct{}
	config keepAliveMonitorConfig

	stopOnce sync.Once
	mu       sync.Mutex
	state    keepAliveMonitorSnapshot
}

func startKeepAliveMonitor(ctx context.Context, cancel context.CancelFunc, responses <-chan *clientv3.LeaseKeepAliveResponse, config keepAliveMonitorConfig) *keepAliveMonitor {
	monitor := &keepAliveMonitor{
		ctx:    ctx,
		cancel: cancel,
		done:   make(chan struct{}),
		events: make(chan struct{}, 1),
		config: config,
		state: keepAliveMonitorSnapshot{
			lastRevision: config.initialRevision,
		},
	}
	go monitor.consume(responses)
	return monitor
}

func (monitor *keepAliveMonitor) consume(responses <-chan *clientv3.LeaseKeepAliveResponse) {
	defer close(monitor.done)
	var recoveryDeadline <-chan time.Time
	for {
		select {
		case <-monitor.ctx.Done():
			return
		case <-recoveryDeadline:
			monitor.terminate(fmt.Errorf("%s lease keepalive recovery exceeded %s", monitor.config.label, monitor.config.recoveryTimeout))
			return
		case response, ok := <-responses:
			if !ok {
				if monitor.ctx.Err() != nil {
					return
				}
				var err error
				var repeated bool
				recoveryDeadline, repeated, err = monitor.beginRecovery(recoveryDeadline)
				if err != nil {
					monitor.terminate(err)
					return
				}
				if repeated {
					if err = monitor.waitBeforeRestart(recoveryDeadline); err != nil {
						if monitor.ctx.Err() == nil {
							monitor.terminate(err)
						}
						return
					}
				}
				replacement, err := monitor.restartWithinDeadline(recoveryDeadline)
				if err != nil {
					if monitor.ctx.Err() == nil {
						monitor.terminate(err)
					}
					return
				}
				monitor.mu.Lock()
				monitor.state.restarts++
				monitor.mu.Unlock()
				monitor.notify()
				responses = replacement
				continue
			}

			monitor.mu.Lock()
			minimumRevision := monitor.state.lastRevision
			monitor.mu.Unlock()
			revision, err := validateKeepAliveResponse(response, monitor.config.clusterID, minimumRevision, monitor.config.leaseID, monitor.config.grantedTTL)
			if err != nil {
				monitor.fail(fmt.Errorf("%s lease keepalive: %w", monitor.config.label, err))
				return
			}
			now := time.Now()
			monitor.mu.Lock()
			monitor.state.lastRevision = revision
			monitor.state.lastResponse = now
			monitor.state.responses++
			if monitor.state.recovering {
				recoveryDuration := now.Sub(monitor.state.recoveryFrom)
				if recoveryDuration > monitor.state.maxRecovery {
					monitor.state.maxRecovery = recoveryDuration
				}
				recoveryDeadline = nil
			}
			monitor.state.recovering = false
			monitor.state.recoveryFrom = time.Time{}
			monitor.mu.Unlock()
			monitor.notify()
		}
	}
}

func (monitor *keepAliveMonitor) beginRecovery(deadline <-chan time.Time) (<-chan time.Time, bool, error) {
	monitor.mu.Lock()
	restarts := monitor.state.restarts
	recovering := monitor.state.recovering
	monitor.mu.Unlock()
	if monitor.config.restart == nil {
		return nil, false, fmt.Errorf("%s lease keepalive closed", monitor.config.label)
	}
	if restarts > 0 && !recovering {
		return nil, false, fmt.Errorf("%s lease keepalive closed after completed recovery", monitor.config.label)
	}
	if recovering {
		return deadline, true, nil
	}
	if monitor.config.recoveryTimeout <= 0 {
		return nil, false, fmt.Errorf("%s lease keepalive recovery timeout must be positive", monitor.config.label)
	}
	after := monitor.config.after
	if after == nil {
		after = time.After
	}
	monitor.mu.Lock()
	monitor.state.recovering = true
	monitor.state.recoveryFrom = time.Now()
	monitor.mu.Unlock()
	monitor.notify()
	deadline = after(monitor.config.recoveryTimeout)
	if deadline == nil {
		return nil, false, fmt.Errorf("%s lease keepalive recovery timer returned an empty channel", monitor.config.label)
	}
	return deadline, false, nil
}

func (monitor *keepAliveMonitor) waitBeforeRestart(deadline <-chan time.Time) error {
	if monitor.config.retryWait <= 0 {
		return fmt.Errorf("%s lease keepalive replacement retry wait must be positive", monitor.config.label)
	}
	retryAfter := monitor.config.retryAfter
	if retryAfter == nil {
		retryAfter = time.After
	}
	retry := retryAfter(monitor.config.retryWait)
	if retry == nil {
		return fmt.Errorf("%s lease keepalive replacement retry timer returned an empty channel", monitor.config.label)
	}
	select {
	case <-monitor.ctx.Done():
		return monitor.ctx.Err()
	case <-deadline:
		return fmt.Errorf("%s lease keepalive recovery exceeded %s", monitor.config.label, monitor.config.recoveryTimeout)
	case <-retry:
		return nil
	}
}

func (monitor *keepAliveMonitor) restartWithinDeadline(deadline <-chan time.Time) (<-chan *clientv3.LeaseKeepAliveResponse, error) {
	result := make(chan keepAliveRestartResult, 1)
	go func() {
		responses, err := monitor.config.restart(monitor.ctx)
		result <- keepAliveRestartResult{responses: responses, err: err}
	}()
	select {
	case <-monitor.ctx.Done():
		return nil, monitor.ctx.Err()
	case <-deadline:
		return nil, fmt.Errorf("%s lease keepalive recovery exceeded %s", monitor.config.label, monitor.config.recoveryTimeout)
	case restarted := <-result:
		if restarted.err != nil {
			return nil, fmt.Errorf("restart %s lease keepalive: %w", monitor.config.label, restarted.err)
		}
		if restarted.responses == nil {
			return nil, fmt.Errorf("restart %s lease keepalive: empty response channel", monitor.config.label)
		}
		return restarted.responses, nil
	}
}

func (monitor *keepAliveMonitor) fail(err error) {
	monitor.mu.Lock()
	if monitor.state.err == nil {
		monitor.state.err = err
	}
	monitor.mu.Unlock()
	monitor.notify()
}

func (monitor *keepAliveMonitor) terminate(err error) {
	monitor.fail(err)
	monitor.cancel()
}

func (monitor *keepAliveMonitor) notify() {
	select {
	case monitor.events <- struct{}{}:
	default:
	}
}

func (monitor *keepAliveMonitor) snapshot() keepAliveMonitorSnapshot {
	monitor.mu.Lock()
	defer monitor.mu.Unlock()
	return monitor.state
}

func (monitor *keepAliveMonitor) err() error {
	return monitor.snapshot().err
}

func (monitor *keepAliveMonitor) waitForResponses(ctx context.Context, minimum int, timeout time.Duration) error {
	return monitor.wait(ctx, timeout, func(snapshot keepAliveMonitorSnapshot) bool {
		return snapshot.responses >= minimum
	}, fmt.Sprintf("%s lease keepalive produced fewer than %d responses", monitor.config.label, minimum))
}

func (monitor *keepAliveMonitor) waitForFreshResponse(ctx context.Context, notBefore time.Time, timeout time.Duration) error {
	return monitor.wait(ctx, timeout, func(snapshot keepAliveMonitorSnapshot) bool {
		return !snapshot.recovering && !snapshot.lastResponse.Before(notBefore)
	}, fmt.Sprintf("%s lease keepalive fresh response timed out", monitor.config.label))
}

func (monitor *keepAliveMonitor) wait(ctx context.Context, timeout time.Duration, ready func(keepAliveMonitorSnapshot) bool, timeoutMessage string) error {
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	for {
		snapshot := monitor.snapshot()
		if snapshot.err != nil {
			return snapshot.err
		}
		if ready(snapshot) {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-monitor.done:
			snapshot = monitor.snapshot()
			if snapshot.err != nil {
				return snapshot.err
			}
			return fmt.Errorf("%s: monitor stopped", timeoutMessage)
		case <-monitor.events:
		case <-timer.C:
			return fmt.Errorf("%s after %s", timeoutMessage, timeout)
		}
	}
}

func (monitor *keepAliveMonitor) stop() {
	monitor.stopOnce.Do(monitor.cancel)
	<-monitor.done
}
