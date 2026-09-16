package production_test

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestRolloutBackendObserverRunnerLifecycle(t *testing.T) {
	for _, mode := range []string{"success", "callback-failure", "callback-timeout", "probe-failure"} {
		t.Run(mode, func(t *testing.T) {
			root := t.TempDir()
			t.Setenv("TMPDIR", root)
			fake, logPath, state := writeRolloutAvailabilityKubectl(t)
			callback := filepath.Join(root, "backend-capture")
			require.NoError(t, os.WriteFile(callback, []byte(`#!/bin/bash
set -euo pipefail
test -d "$1"
test -f "$2"
touch "$TEST_BACKEND_STARTED"
case "$TEST_MODE" in
  callback-failure) exit 17 ;;
  callback-timeout) sleep 30 ;;
esac
`), 0700))
			wrapper := filepath.Join(root, "kubectl-wrapper")
			require.NoError(t, os.WriteFile(wrapper, []byte(`#!/bin/bash
set -euo pipefail
if [[ " $* " == *" wait --for=jsonpath={.status.phase}=Succeeded pod/kubebrain-rollout-availability-probe "* ]]; then
  for ((i=0;i<200;i++)); do
    [[ ! -f "$TEST_BACKEND_STARTED" ]] || break
    sleep 0.01
  done
  [[ -f "$TEST_BACKEND_STARTED" ]] || exit 91
fi
exec "$TEST_REAL_KUBECTL" "$@"
`), 0700))
			env := []string{"KUBECTL_BIN=" + wrapper, "TEST_REAL_KUBECTL=" + fake,
				"FAKE_KUBECTL_LOG=" + logPath, "FAKE_KUBECTL_STATE=" + state,
				"TEST_BACKEND_STARTED=" + filepath.Join(root, "backend-started"), "TEST_MODE=" + mode,
				"ROLLOUT_BACKEND_DIAGNOSTIC_SAMPLER=" + callback, "KEEP_RUNTIME_EVIDENCE=true",
				"BACKEND_OBSERVER_SAMPLE_SECONDS=1", "BACKEND_OBSERVER_INTERVAL_SECONDS=1",
				"ALLOW_MUTATING_KUBEBRAIN_ROLLOUT=true", "PROBE_ITERATIONS=6000",
				"TARGET_IMAGE=registry.example/kubebrain@sha256:" + strings.Repeat("a", 64),
				"TARGET_RUNTIME_DIGESTS=sha256:" + strings.Repeat("a", 64)}
			if mode == "probe-failure" {
				env = append(env, "FAKE_PROBE_FAILED=true")
			}
			output, err := runProductionScriptCommandWithTimeout(t, "run-kubebrain-rollout-availability.sh", env, 40*time.Second)
			if mode == "probe-failure" {
				require.Error(t, err, string(output))
				require.NoFileExists(t, state, "failed probe must restore original image")
			} else {
				require.NoError(t, err, string(output))
				require.Contains(t, string(output), "rollout availability gate passed")
			}
			dirs, err := filepath.Glob(filepath.Join(root, "kubebrain-backend-observer.*"))
			require.NoError(t, err)
			require.Len(t, dirs, 1)
			executorResult := "0"
			if mode == "probe-failure" {
				executorResult = "1"
			}
			require.Equal(t, executorResult, readOptionalFile(t, filepath.Join(dirs[0], "executor-result")))
			require.Equal(t, "0", readOptionalFile(t, filepath.Join(dirs[0], "observer-result")))
			require.Equal(t, "0", readOptionalFile(t, filepath.Join(dirs[0], "backend-observer", "observer.exit")))
			results, err := filepath.Glob(filepath.Join(dirs[0], "backend-observer", "sample.*", "callback.exit"))
			require.NoError(t, err)
			require.NotEmpty(t, results)
			expected := "0"
			if mode == "callback-failure" {
				expected = "17"
			} else if mode == "callback-timeout" {
				expected = "124"
			}
			for _, result := range results {
				require.Equal(t, expected, readOptionalFile(t, result))
			}
			retained := strings.Index(string(output), "RUNTIME_EVIDENCE_RETAINED directory=")
			finished := strings.Index(string(output), "BACKEND_OBSERVER_FINISHED result=")
			require.GreaterOrEqual(t, retained, 0)
			require.Greater(t, finished, retained, "wait for observer must follow executor cleanup")
			for _, suffix := range []string{".probe", ".owner", ".cleanup"} {
				require.NoFileExists(t, logPath+suffix)
			}
		})
	}
}

func TestRolloutBackendObserverWaitFollowsAllCleanup(t *testing.T) {
	data, err := os.ReadFile("run-kubebrain-rollout-availability.sh")
	require.NoError(t, err)
	script := string(data)
	start := strings.Index(script, "rollout_exit() {")
	require.Positive(t, start)
	end := strings.Index(script[start:], "\ntrap rollout_exit EXIT")
	require.Positive(t, end)
	body := script[start : start+end]
	require.Contains(t, body, "(set -e; cleanup)")
	require.Contains(t, body, "if ! image_prepull_cleanup")
	wait := strings.Index(body, "stop_backend_observer_after_cleanup")
	require.Greater(t, wait, strings.Index(body, "(set -e; cleanup)"))
	require.Greater(t, wait, strings.Index(body, "if ! image_prepull_cleanup"))
	require.Contains(t, body[wait:], "exit \"$status\"")
}
