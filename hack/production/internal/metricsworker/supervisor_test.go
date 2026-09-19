package metricsworker

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func command(owner, suffix, mode string) Command {
	return Command{Executable: "/bin/bash", Args: []string{"-c", `
set -eu
owner=$1; suffix=$2; mode=$3
printf '%s\n' "$$" > "$owner/pid-$suffix"
if [[ $mode == no-ready ]]; then sleep 60; exit 1; fi
printf 'READY\t%s/metrics-worker.%s\t%s/metrics.%s\n' "$owner" "$suffix" "$owner" "$suffix"
if [[ $mode == exit-ready ]]; then exit 7; fi
IFS= read -r origin
printf '%s\n' "$origin" > "$owner/origin-$suffix"
if [[ $mode == hang ]]; then sleep 60; exit 1; fi
printf 'CAPTURED\t%s/metrics.z%s\t%s/metrics-schedule.%s\n' "$owner" "${suffix:1}" "$owner" "$suffix"
`, "worker", owner, suffix, mode}, Env: os.Environ()}
}

func TestSupervisorBarrierAndFixedOrigin(t *testing.T) {
	owner := t.TempDir()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	baselines, completed, injections := 0, 0, 0
	origin := time.Now()
	results, err := Run(ctx, owner, []Command{command(owner, "abcdefgh", "ok"), command(owner, "ijklmnop", "ok")}, Hooks{
		Baseline: func(context.Context, int, Ready) error { baselines++; return nil },
		Inject: func(context.Context) (time.Time, error) {
			require.Equal(t, 2, baselines)
			injections++
			return origin, nil
		},
		Completed: func(_ context.Context, _ int, _ Result, got time.Time) error {
			require.Equal(t, origin, got)
			completed++
			return nil
		},
	})
	require.NoError(t, err)
	require.Len(t, results, 2)
	require.Equal(t, 1, injections)
	require.Equal(t, 2, completed)
	for _, suffix := range []string{"abcdefgh", "ijklmnop"} {
		data, err := os.ReadFile(filepath.Join(owner, "origin-"+suffix))
		require.NoError(t, err)
		require.Equal(t, fmt.Sprintf("%d\n", origin.UnixNano()), string(data))
		assertJoined(t, owner, suffix)
	}
}

func assertJoined(t *testing.T, owner, suffix string) {
	t.Helper()
	data, err := os.ReadFile(filepath.Join(owner, "pid-"+suffix))
	require.NoError(t, err)
	var pid int
	_, err = fmt.Sscanf(string(data), "%d", &pid)
	require.NoError(t, err)
	_, err = os.Stat(fmt.Sprintf("/proc/%d", pid))
	require.True(t, os.IsNotExist(err), "direct child still exists after Run")
}

func TestSupervisorFailuresJoinBeforeReturn(t *testing.T) {
	for _, mode := range []string{"baseline-rejected", "exit-ready", "hang", "no-ready", "expired", "future", "completed-rejected"} {
		t.Run(mode, func(t *testing.T) {
			owner := t.TempDir()
			ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
			defer cancel()
			injected := false
			_, err := Run(ctx, owner, []Command{command(owner, "abcdefgh", mode)}, Hooks{
				Baseline: func(context.Context, int, Ready) error {
					if mode == "baseline-rejected" {
						return fmt.Errorf("invalid baseline")
					}
					return nil
				},
				Inject: func(context.Context) (time.Time, error) {
					injected = true
					origin := time.Now()
					if mode == "expired" {
						origin = origin.Add(-time.Minute)
					}
					if mode == "future" {
						origin = origin.Add(time.Minute)
					}
					return origin, nil
				},
				Completed: func(context.Context, int, Result, time.Time) error {
					if mode == "completed-rejected" {
						return fmt.Errorf("invalid evidence")
					}
					return nil
				},
			})
			require.Error(t, err)
			if mode == "baseline-rejected" || mode == "no-ready" {
				require.False(t, injected)
			}
			assertJoined(t, owner, "abcdefgh")
		})
	}
}

func TestSupervisorUsesRemainingFaultBudget(t *testing.T) {
	owner := t.TempDir()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	started := time.Now()
	_, err := Run(ctx, owner, []Command{command(owner, "abcdefgh", "hang")}, Hooks{
		Baseline: func(context.Context, int, Ready) error { return nil },
		Inject:   func(context.Context) (time.Time, error) { return time.Now().Add(-29800 * time.Millisecond), nil },
		Completed: func(context.Context, int, Result, time.Time) error {
			t.Error("hung worker cannot complete")
			return nil
		},
	})
	require.Error(t, err)
	require.NoError(t, ctx.Err(), "must stop on original fault budget, not caller deadline")
	require.Less(t, time.Since(started), 2*time.Second)
	assertJoined(t, owner, "abcdefgh")
}

func TestSupervisorRejectsDuplicateWorkersBeforeInjection(t *testing.T) {
	owner := t.TempDir()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err := Run(ctx, owner, []Command{command(owner, "abcdefgh", "ok"), command(owner, "abcdefgh", "ok")}, Hooks{
		Baseline: func(context.Context, int, Ready) error { return nil },
		Inject: func(context.Context) (time.Time, error) {
			t.Error("duplicate workers must not inject")
			return time.Now(), nil
		},
		Completed: func(context.Context, int, Result, time.Time) error { return nil },
	})
	require.ErrorContains(t, err, "reused evidence paths")
}

func TestSupervisorIncompleteBarrierCancelsAllChildren(t *testing.T) {
	owner := t.TempDir()
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()
	_, err := Run(ctx, owner, []Command{command(owner, "abcdefgh", "ok"), command(owner, "ijklmnop", "no-ready")}, Hooks{
		Baseline: func(context.Context, int, Ready) error { return nil },
		Inject: func(context.Context) (time.Time, error) {
			t.Error("incomplete barrier must not inject")
			return time.Now(), nil
		},
		Completed: func(context.Context, int, Result, time.Time) error {
			t.Error("incomplete barrier cannot complete")
			return nil
		},
	})
	require.Error(t, err)
	assertJoined(t, owner, "abcdefgh")
	assertJoined(t, owner, "ijklmnop")
}
