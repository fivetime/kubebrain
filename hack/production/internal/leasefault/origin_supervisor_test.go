package leasefault

import (
	"context"
	"encoding/json"
	"errors"
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

// Exercise the actual supervisor barrier and process cleanup, not only the
// clock callback in isolation. The child is a protocol fixture, not a collector.
func TestDurableOriginSupervisorBarrier(t *testing.T) {
	for _, mode := range []string{"success", "existing-receipt", "lost-after-sync", "expired-after-sync"} {
		t.Run(mode, func(t *testing.T) {
			dir := t.TempDir()
			require.NoError(t, os.Chmod(dir, 0700))
			require.NoError(t, os.Mkdir(filepath.Join(dir, "deployment-claimed"), 0700))
			log, err := os.OpenFile(filepath.Join(dir, "worker.stderr"), os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
			require.NoError(t, err)
			defer log.Close()
			worker := metricsworker.Command{Executable: "/bin/bash", Env: []string{"PATH=/usr/bin:/bin"}, Stderr: log,
				Args: []string{"-c", `set -eu
printf '%s\n' "$$" > "$1/worker.pid"
printf 'READY\t%s/metrics-worker.abcdefgh\t%s/metrics.abcdefgh\n' "$1" "$1"
IFS= read -r origin
printf '%s\n' "$origin" > "$1/worker.origin"
printf 'CAPTURED\t%s/metrics.ijklmnop\t%s/metrics-schedule.abcdefgh\n' "$1" "$1"
`, "worker", dir}}
			baselines, ownership, injections, completions := 0, 0, 0, 0
			if mode == "existing-receipt" {
				require.NoError(t, os.WriteFile(filepath.Join(dir, faultOriginFile), []byte("partial"), 0600))
			}
			originHook, err := NewDurableFaultOrigin(dir, commandPlan(t).Bindings, func(ctx context.Context) error {
				require.Equal(t, 1, baselines, "clock selection follows baseline admission")
				ownership++
				if ownership == 2 {
					require.FileExists(t, filepath.Join(dir, faultOriginFile))
					if mode == "lost-after-sync" {
						return errors.New("owner lost after persistence")
					}
					if mode == "expired-after-sync" {
						<-ctx.Done()
						return nil // A callback's nil must not mask consumed budget.
					}
				}
				return nil
			})
			require.NoError(t, err)
			// This test checks barrier ordering, not a two-second startup/IO
			// SLA. Keep an outer hang bound; the supervisor still derives its
			// unchanged 30-second fault deadline from the selected origin.
			budget := time.Minute
			if mode == "expired-after-sync" {
				budget = 2 * time.Second
			}
			ctx, cancel := context.WithTimeout(context.Background(), budget)
			defer cancel()
			var selected time.Time
			started := time.Now()
			_, err = metricsworker.Run(ctx, dir, []metricsworker.Command{worker}, metricsworker.Hooks{
				Baseline: func(context.Context, int, metricsworker.Ready) error { baselines++; return nil },
				Origin:   originHook,
				Inject: func(ctx context.Context, at time.Time) error {
					injections++
					selected = at
					data, err := os.ReadFile(filepath.Join(dir, faultOriginFile))
					require.NoError(t, err)
					var record faultOriginRecord
					require.NoError(t, json.Unmarshal(data, &record))
					require.Equal(t, strconv.FormatInt(at.UnixNano(), 10), record.OriginNS)
					deadline, ok := ctx.Deadline()
					require.True(t, ok)
					require.False(t, deadline.After(at.Add(30*time.Second)))
					return nil
				},
				Completed: func(_ context.Context, _ int, _ metricsworker.Result, at time.Time) error {
					completions++
					require.Equal(t, selected, at)
					return nil
				},
			})
			if mode == "success" {
				if err != nil {
					// Keep the existing deadline and barrier assertions. Report
					// which boundary was reached rather than guessing whether a
					// CI timeout came from child startup, persistence or Join.
					t.Logf("barrier failure: elapsed=%s baseline=%d ownership=%d injection=%d completion=%d origin=%s context=%v",
						time.Since(started), baselines, ownership, injections, completions, selected.Format(time.RFC3339Nano), ctx.Err())
					for _, name := range []string{"worker.pid", "worker.origin", "worker.stderr", faultOriginFile} {
						data, readErr := os.ReadFile(filepath.Join(dir, name))
						t.Logf("fixture %s: bytes=%q read_error=%v", name, data, readErr)
					}
				}
				require.NoError(t, err)
				require.Equal(t, 1, injections)
				require.Equal(t, 1, completions)
				data, err := os.ReadFile(filepath.Join(dir, "worker.origin"))
				require.NoError(t, err)
				require.Equal(t, strconv.FormatInt(selected.UnixNano(), 10)+"\n", string(data))
			} else {
				require.Error(t, err)
				require.Zero(t, injections)
				require.Zero(t, completions)
				require.NoFileExists(t, filepath.Join(dir, "worker.origin"))
				require.FileExists(t, filepath.Join(dir, faultOriginFile), "retain ambiguous attempts")
			}
			pidData, err := os.ReadFile(filepath.Join(dir, "worker.pid"))
			require.NoError(t, err)
			pid, err := strconv.Atoi(strings.TrimSpace(string(pidData)))
			require.NoError(t, err)
			require.ErrorIs(t, syscall.Kill(pid, 0), syscall.ESRCH, "supervisor must reap the worker")
		})
	}
}
