package leasefault

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/kubewharf/kubebrain/hack/production/internal/metricsworker"
	"github.com/stretchr/testify/require"
)

func testFaultLifecycle(t *testing.T, ctx context.Context, prep FaultPreparation, mode string) {
	t.Helper()
	parentCtx := ctx
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	dir := prep.Directory
	require.NoError(t, os.Mkdir(filepath.Join(dir, "deployment-claimed"), 0700))
	log := func(name string) *os.File {
		f, err := os.OpenFile(filepath.Join(dir, name), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
		require.NoError(t, err)
		t.Cleanup(func() { require.NoError(t, f.Close()) })
		return f
	}
	// Synthetic gate scripts exercise process and clock wiring only; they are
	// deliberately not deployable fault or metrics evidence generators.
	fault := metricsworker.Command{Executable: "/bin/bash", Stderr: log("fault.stderr"), Args: []string{"-c", `
set -eu
printf '%s\n' "$$" > "$1/fault.pid"
printf 'FAULT_READY\n'
IFS= read -r origin
printf '%s\n' "$origin" > "$1/fault.origin"
if [[ $2 == lifecycle-child-fail ]]; then exit 7; fi
if [[ $2 == lifecycle-deadline ]]; then exec /bin/sleep 60; fi
printf 'FAULT_DONE\t%s\n' "$origin"
`, "fault", dir, mode}}
	worker := metricsworker.Command{Executable: "/bin/bash", Stderr: log("worker.stderr"), Args: []string{"-c", `
set -eu
printf '%s\n' "$$" > "$1/worker.pid"
printf 'READY\t%s/metrics-worker.abcdefgh\t%s/metrics.abcdefgh\n' "$1" "$1"
IFS= read -r origin
printf '%s\n' "$origin" > "$1/worker.origin"
printf 'CAPTURED\t%s/metrics.ijklmnop\t%s/metrics-schedule.abcdefgh\n' "$1" "$1"
`, "worker", dir}}
	joined := false
	var origin time.Time
	result, err := RunFaultLifecycle(ctx, FaultLifecycle{
		Preparation: prep, Fault: fault, Workers: []metricsworker.Command{worker}, RecoveryConnection: prep.Connection, RecoveryTimeout: 5 * time.Second,
		Metrics: metricsworker.Hooks{
			Baseline: func(context.Context, int, metricsworker.Ready) error {
				if mode == "lifecycle-baseline-fail" {
					return errors.New("baseline rejected")
				}
				return nil
			},
			Origin: func(context.Context) (time.Time, error) {
				origin = time.Now()
				if mode == "lifecycle-parent-cancel" {
					cancel()
				}
				if mode == "lifecycle-deadline" {
					origin = origin.Add(-29500 * time.Millisecond)
				}
				return origin, nil
			},
			Completed: func(_ context.Context, _ int, _ metricsworker.Result, got time.Time) error {
				require.Equal(t, origin, got)
				return nil
			},
		},
		Join: func(recoveryCtx context.Context) error {
			require.NoError(t, recoveryCtx.Err())
			for _, name := range []string{"fault.pid", "worker.pid"} {
				data, err := os.ReadFile(filepath.Join(dir, name))
				require.NoError(t, err)
				pid, err := strconv.Atoi(strings.TrimSpace(string(data)))
				require.NoError(t, err)
				require.ErrorIs(t, syscall.Kill(pid, 0), syscall.ESRCH, "child must join before recovery")
			}
			joined = true
			if mode == "lifecycle-join-fail" {
				return errors.New("external worker remains")
			}
			return nil
		},
		NetworkRestored:  func(context.Context) error { require.True(t, joined); return nil },
		IdentityRestored: func(context.Context) error { require.True(t, joined); return nil },
	})
	require.True(t, joined)
	if mode == "lifecycle-join-fail" {
		require.NoError(t, result.ExecutionError)
		require.ErrorContains(t, result.RecoveryError, "external worker remains")
		require.Error(t, err)
		require.Error(t, VerifyProtocolRecovery(parentCtx, prep.Protocol, prep.Connection))
		return
	}
	require.NoError(t, result.RecoveryError)
	require.NoError(t, VerifyProtocolRecovery(parentCtx, prep.Protocol, prep.Connection))
	if mode == "lifecycle-success" {
		require.NoError(t, err)
		require.NoError(t, result.ExecutionError)
		require.Len(t, result.Metrics, 1)
		for _, name := range []string{"fault.origin", "worker.origin"} {
			data, err := os.ReadFile(filepath.Join(dir, name))
			require.NoError(t, err)
			require.Equal(t, fmt.Sprintln(origin.UnixNano()), string(data))
		}
	} else {
		require.Error(t, err)
		require.Error(t, result.ExecutionError)
		if mode == "lifecycle-deadline" {
			require.ErrorIs(t, err, context.DeadlineExceeded)
			require.NoError(t, ctx.Err())
		}
		if mode == "lifecycle-baseline-fail" {
			_, err := os.Stat(filepath.Join(dir, "fault.origin"))
			require.ErrorIs(t, err, os.ErrNotExist)
		}
	}
}
