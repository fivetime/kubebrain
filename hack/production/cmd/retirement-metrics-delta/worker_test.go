package main

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestWorkerEvidence(t *testing.T) {
	for _, scenario := range []string{"success", "missing-exit", "failed-exit", "noncanonical-exit", "wrong-baseline", "wrong-schedule", "symlink", "public-receipt", "public-directory", "no-schedule", "empty-worker", "tampered-capture", "wrong-clock", "foreign-worker"} {
		t.Run(scenario, func(t *testing.T) {
			before, spec := cliCaptureFixture(t, 1800000000, 2, "process", false)
			after, _ := cliCaptureFixture(t, 1800000020, 5, "process", false)
			schedule := cliScheduleFixture(t, after, "1800000010000000000", "4000000000")
			worker, err := os.MkdirTemp(filepath.Dir(before), "metrics-worker.")
			require.NoError(t, err)
			for name, value := range map[string]string{"baseline-path": before + "\n", "schedule-path": schedule + "\n", "exit-code": "0\n"} {
				require.NoError(t, os.WriteFile(filepath.Join(worker, name), []byte(value), 0600))
			}
			origin := "1800000010000000000"
			exit := filepath.Join(worker, "exit-code")
			switch scenario {
			case "missing-exit":
				require.NoError(t, os.Remove(exit))
			case "failed-exit":
				require.NoError(t, os.WriteFile(exit, []byte("124\n"), 0600))
			case "noncanonical-exit":
				require.NoError(t, os.WriteFile(exit, []byte("00\n"), 0600))
			case "wrong-baseline":
				require.NoError(t, os.WriteFile(filepath.Join(worker, "baseline-path"), []byte(after+"\n"), 0600))
			case "wrong-schedule":
				require.NoError(t, os.WriteFile(filepath.Join(worker, "schedule-path"), []byte(after+"\n"), 0600))
			case "symlink":
				require.NoError(t, os.Rename(exit, exit+"-saved"))
				require.NoError(t, os.Symlink(exit+"-saved", exit))
			case "public-receipt":
				require.NoError(t, os.Chmod(exit, 0644))
			case "public-directory":
				require.NoError(t, os.Chmod(worker, 0755))
			case "empty-worker":
				worker = ""
			case "tampered-capture":
				require.NoError(t, os.WriteFile(filepath.Join(after, "metrics.txt"), []byte("other 1\n"), 0600))
			case "wrong-clock":
				origin = "1800000010000000001"
			case "foreign-worker":
				foreign := filepath.Join(t.TempDir(), "worker")
				require.NoError(t, os.Rename(worker, foreign))
				worker = foreign
			}
			args := []string{"--before", before, "--after", after, "--namespace-uid", "ns-uid", "--sts-uid", "sts-uid", "--pod-uid", "pod", "--spec-sha256", spec, "--cluster", "test", "--stage", "peer", "--outcome", "confirmed", "--worker", worker}
			if scenario != "no-schedule" {
				args = append(args, "--schedule", schedule, "--fault-origin-ns", origin, "--offset-ns", "4000000000")
			}
			var output bytes.Buffer
			err = run(args, &output)
			if scenario != "success" {
				require.Error(t, err)
				require.Empty(t, output.String())
				return
			}
			require.NoError(t, err)
			var result map[string]any
			require.NoError(t, json.Unmarshal(output.Bytes(), &result))
			require.Equal(t, true, result["worker_receipts_verified"])
			require.Equal(t, false, result["worker_join_proven"])
			require.Equal(t, false, result["fault_acceptance_proven"])
			require.Equal(t, float64(3), result["count_delta"])
		})
	}
}
