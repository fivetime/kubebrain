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
)

func TestRetirementBoundaryWaitsForCleanupAndInitializationJoin(t *testing.T) {
	m := metricmock.NewMockMetrics(gomock.NewController(t))
	m.EXPECT().EmitCounter(gomock.Any(), gomock.Any(), gomock.Any()).AnyTimes()
	m.EXPECT().EmitGauge(gomock.Any(), gomock.Any(), gomock.Any()).AnyTimes()
	started, cleanupEntered := make(chan struct{}), make(chan struct{})
	initCanceled, initExited := make(chan struct{}), make(chan struct{})
	cleanupExited := make(chan struct{})
	initResume, cleanupResume, hookResume := make(chan struct{}), make(chan struct{}), make(chan struct{})
	var initOnce, cleanupOnce, hookOnce sync.Once
	unblockInit := func() { initOnce.Do(func() { close(initResume) }) }
	unblockCleanup := func() { cleanupOnce.Do(func() { close(cleanupResume) }) }
	unblockHook := func() { hookOnce.Do(func() { close(hookResume) }) }
	observed := make(chan bool, 1)
	l := &leaderElection{backend: &revisionRecorder{}, resourceLock: &campaignLock{}, metricCli: m,
		leaseDuration: time.Second, renewDeadline: 200 * time.Millisecond, retryPeriod: 10 * time.Millisecond,
	}
	l.onStartedLeading = func(ctx context.Context) {
		close(started)
		<-ctx.Done()
		close(initCanceled)
		<-initResume
		close(initExited)
	}
	l.onStoppedLeading = func() { close(cleanupEntered); <-cleanupResume; close(cleanupExited) }
	closed := func(c <-chan struct{}) bool {
		select {
		case <-c:
			return true
		default:
			return false
		}
	}
	l.onTermRetired = func(ctx context.Context) {
		_, fresh := l.EpochAndLeadingFresh()
		observed <- closed(initExited) && closed(cleanupExited) && !fresh && !l.IsLeader() && ctx.Err() != nil
		<-hookResume
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { defer close(done); l.Campaign(ctx) }()
	defer func() {
		cancel()
		unblockCleanup()
		unblockInit()
		unblockHook()
		select {
		case <-done:
		case <-time.After(2 * time.Second):
			t.Error("campaign failed to join")
		}
	}()
	await := func(c <-chan struct{}) {
		t.Helper()
		select {
		case <-c:
		case <-time.After(2 * time.Second):
			t.Fatal("lifecycle stage did not arrive")
		}
	}
	await(started)
	cancel()
	await(initCanceled)
	select {
	case <-observed:
		t.Fatal("retirement crossed unjoined initialization")
	default:
	}
	unblockInit()
	await(initExited)
	await(cleanupEntered)
	select {
	case <-observed:
		t.Fatal("retirement crossed blocked cleanup")
	case <-time.After(20 * time.Millisecond):
	}
	unblockCleanup()
	select {
	case valid := <-observed:
		require.True(t, valid, "retirement must follow both lifecycle exits")
	case <-time.After(2 * time.Second):
		t.Fatal("retirement boundary did not run")
	}
	select {
	case <-done:
		t.Fatal("Campaign must wait for the synchronous retirement boundary")
	default:
	}
	unblockHook()
	await(done)
}

func TestUnacquiredCampaignDoesNotReportRetirement(t *testing.T) {
	m := metricmock.NewMockMetrics(gomock.NewController(t))
	m.EXPECT().EmitCounter(gomock.Any(), gomock.Any(), gomock.Any()).AnyTimes()
	m.EXPECT().EmitGauge(gomock.Any(), gomock.Any(), gomock.Any()).AnyTimes()
	lock := &campaignLock{}
	lock.failUpdates.Store(true)
	var retired atomic.Int32
	l := &leaderElection{backend: &revisionRecorder{}, resourceLock: lock, metricCli: m,
		leaseDuration: time.Second, renewDeadline: 200 * time.Millisecond, retryPeriod: 10 * time.Millisecond,
		onStartedLeading: func(context.Context) { t.Error("unexpected acquisition") },
		onStoppedLeading: func() {}, onTermRetired: func(context.Context) { retired.Add(1) },
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	l.Campaign(ctx)
	require.Zero(t, retired.Load())
}

func TestRetirementBoundaryPrecedesReacquisition(t *testing.T) {
	m := metricmock.NewMockMetrics(gomock.NewController(t))
	m.EXPECT().EmitCounter(gomock.Any(), gomock.Any(), gomock.Any()).AnyTimes()
	m.EXPECT().EmitGauge(gomock.Any(), gomock.Any(), gomock.Any()).AnyTimes()
	var initializations, retirements atomic.Int32
	hookEntered, nextStarted := make(chan struct{}), make(chan struct{})
	hookResume := make(chan struct{})
	var once sync.Once
	resume := func() { once.Do(func() { close(hookResume) }) }
	var callbackCanceled atomic.Bool
	l := &leaderElection{
		backend: initializationFunc(func(context.Context) error {
			if initializations.Add(1) == 1 {
				return storage.ErrUnavailable
			}
			return nil
		}), resourceLock: &campaignLock{}, metricCli: m,
		leaseDuration: time.Second, renewDeadline: 200 * time.Millisecond, retryPeriod: 10 * time.Millisecond,
		onStartedLeading: func(ctx context.Context) { close(nextStarted); <-ctx.Done() },
		onStoppedLeading: func() {},
		onTermRetired: func(ctx context.Context) {
			if retirements.Add(1) == 1 {
				callbackCanceled.Store(ctx.Err() != nil)
				close(hookEntered)
				<-hookResume
			}
		},
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { defer close(done); l.Campaign(ctx) }()
	defer func() {
		cancel()
		resume()
		select {
		case <-done:
		case <-time.After(2 * time.Second):
			t.Error("campaign failed to join")
		}
	}()
	select {
	case <-hookEntered:
	case <-time.After(2 * time.Second):
		t.Fatal("retirement not reached after initialization failure")
	}
	require.False(t, callbackCanceled.Load(), "failed run must not cancel the outer campaign")
	select {
	case <-nextStarted:
		t.Fatal("reacquired before retirement callback returned")
	case <-time.After(20 * time.Millisecond):
	}
	require.Equal(t, int32(1), initializations.Load())
	resume()
	select {
	case <-nextStarted:
	case <-time.After(2 * time.Second):
		t.Fatal("failed to reacquire after retirement")
	}
	require.Equal(t, int32(2), initializations.Load())
}

func TestRetirementCleanupFollowsLateInitialization(t *testing.T) {
	for _, stage := range []string{"revision", "serving"} {
		t.Run(stage, func(t *testing.T) {
			m := metricmock.NewMockMetrics(gomock.NewController(t))
			m.EXPECT().EmitCounter(gomock.Any(), gomock.Any(), gomock.Any()).AnyTimes()
			m.EXPECT().EmitGauge(gomock.Any(), gomock.Any(), gomock.Any()).AnyTimes()
			entered, canceled, resume := make(chan struct{}), make(chan struct{}), make(chan struct{})
			cleaned, retired, done := make(chan struct{}), make(chan struct{}), make(chan struct{})
			var once sync.Once
			unblock := func() { once.Do(func() { close(resume) }) }
			var ready atomic.Bool
			pause := func(ctx context.Context) {
				close(entered)
				<-ctx.Done()
				close(canceled)
				<-resume
			}
			l := &leaderElection{resourceLock: &campaignLock{}, metricCli: m,
				leaseDuration: time.Second, renewDeadline: 200 * time.Millisecond, retryPeriod: 10 * time.Millisecond}
			l.backend = initializationFunc(func(ctx context.Context) error {
				if stage == "revision" {
					pause(ctx)
				}
				return nil // A successful operation can return after cancellation.
			})
			l.onStartedLeading = func(ctx context.Context) {
				if stage == "serving" {
					pause(ctx)
				}
				ready.Store(true)
			}
			l.onStoppedLeading = func() { ready.Store(false); close(cleaned) }
			l.onTermRetired = func(context.Context) { close(retired) }
			ctx, cancel := context.WithCancel(context.Background())
			go func() { defer close(done); l.Campaign(ctx) }()
			defer func() {
				cancel()
				unblock()
				select {
				case <-done:
				case <-time.After(2 * time.Second):
					t.Error("campaign did not join")
				}
			}()
			await := func(ch <-chan struct{}) {
				t.Helper()
				select {
				case <-ch:
				case <-time.After(2 * time.Second):
					t.Fatal("lifecycle stage did not arrive")
				}
			}
			await(entered)
			cancel()
			await(canceled)
			select {
			case <-cleaned:
				t.Fatal("cleanup ran before initialization finished publishing")
			case <-time.After(20 * time.Millisecond):
			}
			unblock()
			await(retired)
			await(done)
			require.False(t, l.IsLeader(), "late initialization must not republish retired leadership")
			require.False(t, ready.Load(), "cleanup must dominate late readiness publication")
		})
	}
}
