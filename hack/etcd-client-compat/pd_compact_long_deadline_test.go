package compat

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type pdCompactResult struct {
	target int64
	err    error
}

// TestCompactAcrossPDTotalLoss extends upstream TestKVCompact across a full PD
// blackout. Overlapping requests may return etcd's transient/compacted errors,
// but recovery must durably advance the watermark without losing current data.
func TestCompactAcrossPDTotalLoss(t *testing.T) {
	command := os.Getenv("KUBEBRAIN_PD_COMPACT_LONG_DEADLINE_COMMAND")
	if command == "" {
		t.Skip("set KUBEBRAIN_PD_COMPACT_LONG_DEADLINE_COMMAND to run destructive compact overlap")
	}
	endpoint := os.Getenv("KUBEBRAIN_ETCD_ENDPOINT")
	if endpoint == "" {
		t.Fatal("set KUBEBRAIN_ETCD_ENDPOINT explicitly for compact overlap")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 6*time.Minute)
	defer cancel()
	client, err := clientv3.New(clientv3.Config{Endpoints: []string{endpoint}, DialTimeout: 5 * time.Second})
	require.NoError(t, err)
	defer func() { require.NoError(t, client.Close()) }()

	prefix := fmt.Sprintf("/compat/pd-compact-long/%d/", time.Now().UnixNano())
	var latest int64
	for index := range 12 {
		response, putErr := client.Put(ctx, fmt.Sprintf("%s%02d", prefix, index), fmt.Sprintf("value-%02d", index))
		require.NoError(t, putErr)
		latest = response.Header.Revision
	}
	duringTarget := latest - 4
	boundaryTarget := latest - 2
	require.Greater(t, duringTarget, int64(1))

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
		require.NoErrorf(t, result.err, "compact overlap cleanup: %s", strings.TrimSpace(string(result.output)))
	}()

	require.Eventually(t, func() bool { return reachablePDCount(t, ctx) == 0 },
		30*time.Second, 200*time.Millisecond, "all PD endpoints must become unreachable")
	duringDone := startPDCompact(ctx, client, duringTarget)
	duringEarly := assertNoSuccessfulPDCompact(t, duringDone, 2*time.Second)

	time.Sleep(23 * time.Second)
	require.Equal(t, 0, reachablePDCount(t, ctx), "boundary Compact must start during PD total loss")
	boundaryDone := startPDCompact(ctx, client, boundaryTarget)
	boundaryEarly := assertNoSuccessfulPDCompact(t, boundaryDone, 2*time.Second)

	result := <-commandDone
	commandWaited = true
	require.NoErrorf(t, result.err, "compact overlap command: %s", strings.TrimSpace(string(result.output)))
	t.Logf("compact overlap command: %s", strings.TrimSpace(string(result.output)))
	duringResult := collectPDCompact(t, ctx, duringDone, duringEarly)
	boundaryResult := collectPDCompact(t, ctx, boundaryDone, boundaryEarly)
	t.Logf("overlap outcomes: during=%v boundary=%v", duringResult.err, boundaryResult.err)

	require.Eventually(t, func() bool {
		requestCtx, requestCancel := context.WithTimeout(ctx, 10*time.Second)
		defer requestCancel()
		_, compactErr := client.Compact(requestCtx, boundaryTarget)
		if compactErr == nil || errors.Is(compactErr, rpctypes.ErrCompacted) {
			return true
		}
		requireTransientPDCompactError(t, compactErr)
		return false
	}, 90*time.Second, 500*time.Millisecond, "compact watermark must reach revision %d", boundaryTarget)

	current, err := client.Get(ctx, prefix, clientv3.WithPrefix())
	require.NoError(t, err)
	require.Len(t, current.Kvs, 12)
	_, err = client.Get(ctx, prefix, clientv3.WithPrefix(), clientv3.WithRev(boundaryTarget-1))
	require.ErrorIs(t, err, rpctypes.ErrCompacted)

	watchCtx, watchCancel := context.WithTimeout(ctx, 30*time.Second)
	defer watchCancel()
	watch := client.Watch(watchCtx, prefix, clientv3.WithPrefix(), clientv3.WithRev(boundaryTarget-1))
	response, ok := <-watch
	require.True(t, ok)
	require.True(t, response.Canceled)
	require.ErrorIs(t, response.Err(), rpctypes.ErrCompacted)
	require.GreaterOrEqual(t, response.CompactRevision, boundaryTarget)
}

func startPDCompact(ctx context.Context, client *clientv3.Client, target int64) <-chan pdCompactResult {
	result := make(chan pdCompactResult, 1)
	go func() {
		requestCtx, cancel := context.WithTimeout(ctx, 180*time.Second)
		defer cancel()
		_, err := client.Compact(requestCtx, target)
		result <- pdCompactResult{target: target, err: err}
	}()
	return result
}

func assertNoSuccessfulPDCompact(t *testing.T, result <-chan pdCompactResult, duration time.Duration) []pdCompactResult {
	t.Helper()
	select {
	case outcome := <-result:
		if outcome.err == nil {
			require.Failf(t, "Compact succeeded during PD total loss", "target=%d", outcome.target)
		}
		requirePDCompactOverlapError(t, outcome.err)
		return []pdCompactResult{outcome}
	case <-time.After(duration):
		return nil
	}
}

func collectPDCompact(t *testing.T, ctx context.Context, result <-chan pdCompactResult, early []pdCompactResult) pdCompactResult {
	t.Helper()
	if len(early) == 1 {
		return early[0]
	}
	select {
	case outcome := <-result:
		if outcome.err != nil {
			requirePDCompactOverlapError(t, outcome.err)
		}
		return outcome
	case <-ctx.Done():
		require.NoError(t, ctx.Err())
		return pdCompactResult{}
	}
}

func requirePDCompactOverlapError(t *testing.T, err error) {
	t.Helper()
	if errors.Is(err, rpctypes.ErrCompacted) {
		return
	}
	requireTransientPDCompactError(t, err)
}

func requireTransientPDCompactError(t *testing.T, err error) {
	t.Helper()
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return
	}
	switch status.Code(err) {
	case codes.Unavailable, codes.DeadlineExceeded, codes.Canceled:
		return
	default:
		require.Failf(t, "unexpected Compact overlap error", "code=%s err=%v", status.Code(err), err)
	}
}
