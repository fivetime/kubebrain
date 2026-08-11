package compat

import (
	"context"
	"errors"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/concurrency"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// TestConcurrencySessionsOverlapPDTotalLoss mirrors upstream
// TestV3LeaseFailureOverlap, but overlaps external concurrency.Session
// creation with a cross-node blackout of every PD endpoint. Requests must not
// fabricate a session while coordination is unavailable, hang forever after
// recovery, leak a successful lease, or poison later session creation.
func TestConcurrencySessionsOverlapPDTotalLoss(t *testing.T) {
	command := os.Getenv("KUBEBRAIN_PD_SESSION_OVERLAP_COMMAND")
	if command == "" {
		t.Skip("set KUBEBRAIN_PD_SESSION_OVERLAP_COMMAND to run destructive session overlap")
	}
	endpoint := os.Getenv("KUBEBRAIN_ETCD_ENDPOINT")
	if endpoint == "" {
		t.Fatal("set KUBEBRAIN_ETCD_ENDPOINT explicitly for session overlap")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	client, err := clientv3.New(clientv3.Config{Endpoints: []string{endpoint}, DialTimeout: 5 * time.Second})
	require.NoError(t, err)
	defer func() { require.NoError(t, client.Close()) }()

	baseline := startSessionWave(ctx, client, 8)
	baselineIDs := collectSessionWave(t, ctx, baseline, nil, 8, true)
	assertSessionLeasesGone(t, ctx, client, baselineIDs)

	type commandResult struct {
		output []byte
		err    error
	}
	commandDone := make(chan commandResult, 1)
	go func() {
		output, commandErr := runCompatShellCommandContext(t, ctx, command)
		commandDone <- commandResult{output: output, err: commandErr}
	}()
	commandWaited := false
	defer func() {
		if commandWaited {
			return
		}
		result := <-commandDone
		require.NoErrorf(t, result.err, "session-overlap PD total-loss cleanup: %s",
			strings.TrimSpace(string(result.output)))
	}()

	require.Eventually(t, func() bool { return reachablePDCount(t, ctx) == 0 },
		30*time.Second, 200*time.Millisecond, "all PD endpoints must become unreachable")
	during := startSessionWave(ctx, client, 8)
	earlyDuring := assertNoSuccessfulSession(t, during, 2*time.Second)

	// The runner fixes a 45-second blackout. Start another wave late enough to
	// overlap requests already in flight with the recovery boundary.
	time.Sleep(25 * time.Second)
	require.Equal(t, 0, reachablePDCount(t, ctx), "recovery-boundary wave must start during PD total loss")
	boundary := startSessionWave(ctx, client, 8)

	result := <-commandDone
	commandWaited = true
	require.NoErrorf(t, result.err, "session-overlap PD total-loss command: %s",
		strings.TrimSpace(string(result.output)))
	t.Logf("session-overlap PD total-loss command: %s", strings.TrimSpace(string(result.output)))

	duringIDs := collectSessionWave(t, ctx, during, earlyDuring, 8, false)
	boundaryIDs := collectSessionWave(t, ctx, boundary, nil, 8, false)
	t.Logf("overlapped sessions completed successfully: during=%d boundary=%d", len(duringIDs), len(boundaryIDs))
	assertSessionLeasesGone(t, ctx, client, append(duringIDs, boundaryIDs...))

	after := startSessionWave(ctx, client, 8)
	afterIDs := collectSessionWave(t, ctx, after, nil, 8, true)
	assertSessionLeasesGone(t, ctx, client, afterIDs)
}

type sessionOutcome struct {
	session *concurrency.Session
	cancel  context.CancelFunc
	err     error
}

func startSessionWave(ctx context.Context, client *clientv3.Client, count int) chan sessionOutcome {
	outcomes := make(chan sessionOutcome, count)
	for range count {
		go func() {
			requestCtx, requestCancel := context.WithTimeout(ctx, 90*time.Second)
			session, err := concurrency.NewSession(client,
				concurrency.WithTTL(20), concurrency.WithContext(requestCtx))
			if err != nil {
				requestCancel()
			}
			outcomes <- sessionOutcome{session: session, cancel: requestCancel, err: err}
		}()
	}
	return outcomes
}

func assertNoSuccessfulSession(t *testing.T, outcomes <-chan sessionOutcome, duration time.Duration) []sessionOutcome {
	t.Helper()
	early := make([]sessionOutcome, 0)
	timer := time.NewTimer(duration)
	defer timer.Stop()
	for {
		select {
		case outcome := <-outcomes:
			if outcome.session != nil {
				_ = outcome.session.Close()
				outcome.cancel()
				require.Failf(t, "session request succeeded during PD total loss",
					"lease=%d", outcome.session.Lease())
			}
			requireTransientSessionError(t, outcome.err)
			early = append(early, outcome)
		case <-timer.C:
			return early
		}
	}
}

func collectSessionWave(
	t *testing.T,
	ctx context.Context,
	outcomes <-chan sessionOutcome,
	early []sessionOutcome,
	count int,
	requireAll bool,
) []clientv3.LeaseID {
	t.Helper()
	ids := make([]clientv3.LeaseID, 0, count)
	consume := func(outcome sessionOutcome) {
		if outcome.err != nil {
			requireTransientSessionError(t, outcome.err)
			return
		}
		require.NotNil(t, outcome.session)
		ids = append(ids, outcome.session.Lease())
		require.NoError(t, outcome.session.Close())
		outcome.cancel()
	}
	for _, outcome := range early {
		consume(outcome)
	}
	for range count - len(early) {
		select {
		case outcome := <-outcomes:
			consume(outcome)
		case <-ctx.Done():
			require.NoError(t, ctx.Err())
		}
	}
	if requireAll {
		require.Len(t, ids, count)
	}
	return ids
}

func requireTransientSessionError(t *testing.T, err error) {
	t.Helper()
	require.Error(t, err)
	if errors.Is(err, rpctypes.ErrTimeoutDueToConnectionLost) || errors.Is(err, context.DeadlineExceeded) {
		return
	}
	code := status.Code(err)
	require.Contains(t, []codes.Code{codes.Unavailable, codes.DeadlineExceeded, codes.Canceled}, code,
		"session overlap must not leak a permanent or Unknown error")
}

func assertSessionLeasesGone(
	t *testing.T,
	ctx context.Context,
	client *clientv3.Client,
	ids []clientv3.LeaseID,
) {
	t.Helper()
	wantGone := make(map[clientv3.LeaseID]struct{}, len(ids))
	for _, id := range ids {
		wantGone[id] = struct{}{}
		response, err := client.TimeToLive(ctx, id)
		require.NoError(t, err)
		require.Equalf(t, int64(-1), response.TTL, "closed session lease %d remained live", id)
	}
	listed, err := client.Leases(ctx)
	require.NoError(t, err)
	for _, lease := range listed.Leases {
		_, leaked := wantGone[lease.ID]
		require.Falsef(t, leaked, "closed session lease %d remained listed", lease.ID)
	}
}
