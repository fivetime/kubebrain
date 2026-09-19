package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/kubewharf/kubebrain/hack/production/internal/metricsworker"
	"github.com/kubewharf/kubebrain/hack/production/internal/retirementmetrics"
	"github.com/stretchr/testify/require"
)

func TestWorkerGateCallbacks(t *testing.T) {
	for _, mode := range []string{"success", "duration", "duration-missing", "below-minimum", "baseline-changed", "ready-changed", "extended-budget", "cancelled", "owner-lost", "retain-failed", "retain-cancelled", "final-owner-lost", "nil-minimum", "frozen-minimum"} {
		t.Run(mode, func(t *testing.T) {
			now := time.Now().Unix()
			before, spec := cliCaptureFixture(t, now-40, 2, "process", mode == "duration")
			after, _ := cliCaptureFixture(t, now-10, 5, "process", mode == "duration")
			origin := time.Unix(now-22, 0)
			schedule := cliScheduleFixture(t, after, fmt.Sprint(origin.UnixNano()), "4000000000")
			worker, err := os.MkdirTemp(filepath.Dir(before), "metrics-worker.")
			require.NoError(t, err)
			for name, value := range map[string]string{"baseline-path": before + "\n", "schedule-path": schedule + "\n", "exit-code": "0\n"} {
				require.NoError(t, os.WriteFile(filepath.Join(worker, name), []byte(value), 0600))
			}
			minimum := uint64(3)
			if mode == "below-minimum" {
				minimum = 4
			}
			expect := retirementmetrics.WorkerExpectation{Binding: retirementmetrics.CaptureBinding{NamespaceUID: "ns-uid", StatefulSetUID: "sts-uid", PodUID: "pod", SpecSHA256: spec, Cluster: "test"}, Offset: 4 * time.Second, Key: retirementmetrics.Key{Stage: "peer", Outcome: "confirmed"}, MinimumCount: &minimum, RequireDuration: strings.HasPrefix(mode, "duration")}
			if mode == "nil-minimum" {
				expect.MinimumCount = nil
			}
			ownerLost, retained := false, 0
			var cancelDuringRetention context.CancelFunc
			gates, err := retirementmetrics.NewWorkerGates([]retirementmetrics.WorkerExpectation{expect}, func(context.Context) error {
				if ownerLost {
					return errors.New("owner lost")
				}
				return nil
			}, func(ctx context.Context, index int, m retirementmetrics.WorkerMeasurement) error {
				require.NoError(t, ctx.Err())
				require.Zero(t, index)
				require.Equal(t, float64(3), m.Count)
				if mode == "duration" {
					require.Equal(t, &retirementmetrics.DurationDelta{Count: 3, Seconds: .75}, m.Duration)
				}
				retained++
				if mode == "retain-cancelled" {
					cancelDuringRetention()
				}
				if mode == "final-owner-lost" {
					ownerLost = true
				}
				if mode == "retain-failed" {
					return errors.New("cannot persist evidence")
				}
				return nil
			})
			if mode == "nil-minimum" {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			if mode == "frozen-minimum" {
				minimum = 100
			}
			ready := metricsworker.Ready{Worker: worker, Baseline: before}
			ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
			defer cancel()
			hooks := metricsworker.Hooks{Baseline: gates.Baseline, Completed: gates.Completed}
			require.NoError(t, hooks.Baseline(ctx, 0, ready))
			result := metricsworker.Result{Ready: ready, Captured: metricsworker.Captured{Capture: after, Schedule: schedule}}
			if mode == "baseline-changed" {
				replacement, _ := cliCaptureFixture(t, now-40, 1, "process", false)
				for _, name := range []string{"metrics.txt", "probe.json", "evidence.sha256"} {
					data, err := os.ReadFile(filepath.Join(replacement, name))
					require.NoError(t, err)
					data = []byte(strings.ReplaceAll(string(data), replacement, before))
					require.NoError(t, os.WriteFile(filepath.Join(before, name), data, 0600))
				}
			}
			if mode == "ready-changed" {
				result.Ready.Worker += "-other"
			}
			deadline := origin.Add(30 * time.Second)
			if mode == "extended-budget" {
				deadline = deadline.Add(time.Second)
			}
			faultCtx, faultCancel := context.WithDeadline(ctx, deadline)
			cancelDuringRetention = faultCancel
			defer faultCancel()
			if mode == "cancelled" {
				faultCancel()
			}
			ownerLost = mode == "owner-lost"
			err = hooks.Completed(faultCtx, 0, result, origin)
			if mode == "success" || mode == "duration" || mode == "frozen-minimum" {
				require.NoError(t, err)
				require.Equal(t, 1, retained)
				require.Error(t, hooks.Completed(faultCtx, 0, result, origin))
				require.Error(t, hooks.Baseline(ctx, 0, ready))
			} else {
				require.Error(t, err)
				if mode == "below-minimum" || mode == "retain-failed" || mode == "retain-cancelled" || mode == "final-owner-lost" {
					require.Equal(t, 1, retained)
				} else {
					require.Zero(t, retained)
				}
			}
		})
	}
}
