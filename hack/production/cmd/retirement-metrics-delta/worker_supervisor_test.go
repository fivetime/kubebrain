package main

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/kubewharf/kubebrain/hack/production/internal/metricsworker"
	"github.com/kubewharf/kubebrain/hack/production/internal/retirementmetrics"
	"github.com/stretchr/testify/require"
)

// Only synthetic test artifacts are relocated/rehashed here. Never rewrite real
// evidence manifests to make an existing attempt pass validation.
func moveSyntheticCapture(t *testing.T, source, target string) string {
	t.Helper()
	require.NoError(t, os.Rename(source, target))
	manifest := filepath.Join(target, "evidence.sha256")
	data, err := os.ReadFile(manifest)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(manifest, []byte(strings.ReplaceAll(string(data), source, target)), 0600))
	return target
}

func TestWorkerGatesWithSupervisor(t *testing.T) {
	for _, mode := range []string{"success", "child-fails", "missing-receipt", "below-minimum"} {
		t.Run(mode, func(t *testing.T) {
			owner := t.TempDir()
			require.NoError(t, os.Mkdir(filepath.Join(owner, "deployment-claimed"), 0700))
			now := time.Now().Unix()
			// Synthetic capture history and a still-live original window test
			// callback wiring only, not actual sampling or injection timing.
			origin := time.Unix(now-22, 0)
			before, spec := cliCaptureFixture(t, now-40, 2, "process", false)
			before = moveSyntheticCapture(t, before, filepath.Join(owner, "metrics.abcdefgh"))
			after, _ := cliCaptureFixture(t, now-10, 5, "process", false)
			after = moveSyntheticCapture(t, after, filepath.Join(owner, "metrics.ijklmnop"))
			schedule := cliScheduleFixture(t, after, fmt.Sprint(origin.UnixNano()), "4000000000")
			schedule = moveSyntheticCapture(t, schedule, filepath.Join(owner, "metrics-schedule.abcdefgh"))
			worker := filepath.Join(owner, "metrics-worker.abcdefgh")
			require.NoError(t, os.Mkdir(worker, 0700))
			minimum := uint64(3)
			if mode == "below-minimum" {
				minimum = 4
			}
			retained, completed, baselines := 0, 0, 0
			gates, err := retirementmetrics.NewWorkerGates([]retirementmetrics.WorkerExpectation{{Binding: retirementmetrics.CaptureBinding{NamespaceUID: "ns-uid", StatefulSetUID: "sts-uid", PodUID: "pod", SpecSHA256: spec, Cluster: "test"}, Offset: 4 * time.Second, Key: retirementmetrics.Key{Stage: "peer", Outcome: "confirmed"}, MinimumCount: &minimum}},
				func(context.Context) error { return nil },
				func(ctx context.Context, i int, m retirementmetrics.WorkerMeasurement) error {
					require.NoError(t, ctx.Err())
					require.Equal(t, float64(3), m.Count)
					retained++
					return nil
				})
			require.NoError(t, err)
			log, err := os.OpenFile(filepath.Join(owner, "worker.stderr"), os.O_CREATE|os.O_EXCL|os.O_RDWR, 0600)
			require.NoError(t, err)
			defer log.Close()
			command := metricsworker.Command{Executable: "/bin/bash", Env: []string{"PATH=/usr/bin:/bin"}, Stderr: log, Args: []string{"-c", `
set -eu
umask 077
worker=$1; before=$2; after=$3; schedule=$4; expected=$5; mode=$6
printf '%s\n' "$$" > "$worker/pid"
if [[ $mode != missing-receipt ]]; then
 trap 'rc=$?; printf "%s\n" "$rc" > "$worker/exit-code"' EXIT
fi
printf '%s\n' "$before" > "$worker/baseline-path"
printf 'READY\t%s\t%s\n' "$worker" "$before"
IFS= read -r origin
[[ $origin == "$expected" ]]
printf '%s\n' "$origin" > "$worker/origin"
printf '%s\n' "$schedule" > "$worker/schedule-path"
printf 'CAPTURED\t%s\t%s\n' "$after" "$schedule"
if [[ $mode == child-fails ]]; then exit 7; fi
`, "fixture-worker", worker, before, after, schedule, fmt.Sprint(origin.UnixNano()), mode}}
			ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
			defer cancel()
			results, err := metricsworker.Run(ctx, owner, []metricsworker.Command{command}, metricsworker.Hooks{
				Baseline: func(ctx context.Context, index int, ready metricsworker.Ready) error {
					baselines++
					return gates.Baseline(ctx, index, ready)
				},
				Origin: func(context.Context) (time.Time, error) {
					require.Equal(t, 1, baselines)
					return origin, nil
				},
				Inject: func(ctx context.Context, got time.Time) error {
					require.Equal(t, origin, got)
					deadline, ok := ctx.Deadline()
					require.True(t, ok)
					require.Equal(t, origin.Add(30*time.Second), deadline)
					return nil // Fixture only: this test injects no fault.
				},
				Completed: func(ctx context.Context, index int, result metricsworker.Result, got time.Time) error {
					completed++
					// Wait must precede this callback; an EXIT-trap receipt alone
					// cannot establish that the actual child was reaped.
					assertFixtureWorkerJoined(t, worker)
					return gates.Completed(ctx, index, result, got)
				},
			})
			assertFixtureWorkerJoined(t, worker)
			if mode == "success" {
				require.NoError(t, err)
				require.Len(t, results, 1)
			} else {
				require.Error(t, err)
			}
			if mode == "child-fails" {
				require.Zero(t, completed, "failed child must not reach metric evidence acceptance")
			} else {
				require.Equal(t, 1, completed)
			}
			if mode == "success" || mode == "below-minimum" {
				require.Equal(t, 1, retained)
			} else {
				require.Zero(t, retained)
			}
		})
	}
}

func assertFixtureWorkerJoined(t *testing.T, worker string) {
	t.Helper()
	data, err := os.ReadFile(filepath.Join(worker, "pid"))
	require.NoError(t, err)
	pid, err := strconv.Atoi(strings.TrimSpace(string(data)))
	require.NoError(t, err)
	require.ErrorIs(t, syscall.Kill(pid, 0), syscall.ESRCH)
}
