package production_test

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

const backendObserverPhase = `{"format":"kubebrain.rollout-diagnostic-phase.v1","phase":"stable","probe_uid":"p","statefulset_uid":"s","image":"candidate"}`

func TestRolloutBackendObserverPhaseEvidence(t *testing.T) {
	for _, mode := range []string{"stable", "cleanup", "missing", "invalid", "multiple", "oversized", "transition", "identity-change", "failure", "timeout", "prestopped", "bound"} {
		t.Run(mode, func(t *testing.T) {
			dir := t.TempDir()
			phase := filepath.Join(dir, "phase.json")
			body := backendObserverPhase
			switch mode {
			case "cleanup":
				body = strings.ReplaceAll(body, "stable", "cleanup")
			case "invalid":
				body = "not-json"
			case "multiple":
				body += "\n" + body
			case "oversized":
				body += strings.Repeat(" ", 65537)
			}
			if mode != "missing" {
				require.NoError(t, os.WriteFile(phase, []byte(body), 0600))
			}
			if mode == "prestopped" {
				require.NoError(t, os.WriteFile(filepath.Join(dir, "backend-observer-stop"), nil, 0600))
			}
			callback := filepath.Join(dir, "callback")
			require.NoError(t, os.WriteFile(callback, []byte(`#!/bin/bash
set -euo pipefail
test -d "$1"
test -f "$2"
touch "$OWNER/callback-ran"
if [[ "$MODE" != bound ]]; then touch "$OWNER/backend-observer-stop"; fi
case "$MODE" in
  transition) jq '.phase="cleanup"' "$PHASE" > "$PHASE.next"; mv "$PHASE.next" "$PHASE" ;;
  identity-change) jq '.probe_uid="other"' "$PHASE" > "$PHASE.next"; mv "$PHASE.next" "$PHASE" ;;
  failure) exit 17 ;;
  timeout) sleep 10 ;;
esac
`), 0700))
			cmd := exec.Command("bash", "observe-rollout-backend.sh", dir, callback, phase)
			cmd.Env = append(os.Environ(), "OWNER="+dir, "PHASE="+phase, "MODE="+mode,
				"BACKEND_OBSERVER_MAX_SAMPLES=1", "BACKEND_OBSERVER_MAX_SECONDS=5",
				"BACKEND_OBSERVER_INTERVAL_SECONDS=1", "BACKEND_OBSERVER_SAMPLE_SECONDS=1")
			out, err := cmd.CombinedOutput()
			if mode == "bound" {
				require.Error(t, err, string(out))
				require.Equal(t, 75, cmd.ProcessState.ExitCode())
			} else {
				require.NoError(t, err, string(out))
			}
			samples, err := filepath.Glob(filepath.Join(dir, "backend-observer", "sample.*"))
			require.NoError(t, err)
			if mode == "prestopped" {
				require.Empty(t, samples)
				require.NoFileExists(t, filepath.Join(dir, "callback-ran"))
				return
			}
			require.Len(t, samples, 1)
			require.FileExists(t, filepath.Join(dir, "callback-ran"), "unknown phase must not suppress diagnostics")
			expectedResult := "0\n"
			if mode == "failure" {
				expectedResult = "17\n"
			} else if mode == "timeout" {
				expectedResult = "124\n"
			}
			result, err := os.ReadFile(filepath.Join(samples[0], "callback.exit"))
			require.NoError(t, err)
			require.Equal(t, expectedResult, string(result))
			marker := filepath.Join(samples[0], "phase-consistent")
			if mode == "stable" || mode == "cleanup" || mode == "bound" {
				require.FileExists(t, marker)
			} else {
				require.NoFileExists(t, marker)
			}
			retry := exec.Command("bash", "observe-rollout-backend.sh", dir, callback, phase)
			retry.Env = cmd.Env
			retryOut, err := retry.CombinedOutput()
			require.Error(t, err, "must not reuse observation: %s", retryOut)
		})
	}
}

func TestRolloutBackendObserverSignalReapsCallback(t *testing.T) {
	dir := t.TempDir()
	callback := filepath.Join(dir, "callback")
	pidFile := filepath.Join(dir, "callback.pid")
	require.NoError(t, os.WriteFile(callback, []byte("#!/bin/bash\necho $$ > \"$PID_FILE\"\nexec sleep 30\n"), 0700))
	cmd := exec.Command("bash", "observe-rollout-backend.sh", dir, callback, filepath.Join(dir, "missing-phase"))
	cmd.Env = append(os.Environ(), "PID_FILE="+pidFile)
	require.NoError(t, cmd.Start())
	finished := false
	t.Cleanup(func() {
		if !finished {
			_ = cmd.Process.Signal(syscall.SIGTERM)
			_ = cmd.Wait()
		}
	})
	var pid int
	require.Eventually(t, func() bool {
		data, err := os.ReadFile(pidFile)
		if err != nil {
			return false
		}
		_, err = fmt.Sscan(string(data), &pid)
		return err == nil && pid > 0
	}, 5*time.Second, 10*time.Millisecond)
	require.NoError(t, cmd.Process.Signal(syscall.SIGTERM))
	err := cmd.Wait()
	finished = true
	require.Error(t, err)
	require.Equal(t, 143, cmd.ProcessState.ExitCode())
	require.Eventually(t, func() bool { return syscall.Kill(pid, 0) == syscall.ESRCH }, 3*time.Second, 10*time.Millisecond)
	result, err := os.ReadFile(filepath.Join(dir, "backend-observer", "observer.exit"))
	require.NoError(t, err)
	require.Equal(t, "143\n", string(result))
}

func TestRolloutBackendObserverContinuesAfterSampleFailure(t *testing.T) {
	dir := t.TempDir()
	callback := filepath.Join(dir, "callback")
	require.NoError(t, os.WriteFile(callback, []byte(`#!/bin/bash
set -euo pipefail
if [[ ! -e "$OWNER/first-sample" ]]; then
  touch "$OWNER/first-sample"
  exit 17
fi
touch "$OWNER/backend-observer-stop"
`), 0700))
	cmd := exec.Command("bash", "observe-rollout-backend.sh", dir, callback, filepath.Join(dir, "missing-phase"))
	cmd.Env = append(os.Environ(), "OWNER="+dir, "BACKEND_OBSERVER_INTERVAL_SECONDS=1",
		"BACKEND_OBSERVER_MAX_SAMPLES=2", "BACKEND_OBSERVER_MAX_SECONDS=10", "BACKEND_OBSERVER_SAMPLE_SECONDS=1")
	out, err := cmd.CombinedOutput()
	require.NoError(t, err, string(out))
	results, err := filepath.Glob(filepath.Join(dir, "backend-observer", "sample.*", "callback.exit"))
	require.NoError(t, err)
	require.Len(t, results, 2)
	var values []string
	for _, path := range results {
		data, err := os.ReadFile(path)
		require.NoError(t, err)
		values = append(values, string(data))
	}
	require.ElementsMatch(t, []string{"17\n", "0\n"}, values)
}

func TestRolloutBackendObserverRejectsInvalidBounds(t *testing.T) {
	for _, value := range []string{"0", "-1", "01", "181", "99999999999999999999", "1+1"} {
		t.Run(value, func(t *testing.T) {
			dir := t.TempDir()
			callback := filepath.Join(dir, "callback")
			require.NoError(t, os.WriteFile(callback, []byte("#!/bin/bash\nexit 90\n"), 0700))
			cmd := exec.Command("bash", "observe-rollout-backend.sh", dir, callback, filepath.Join(dir, "phase"))
			cmd.Env = append(os.Environ(), "BACKEND_OBSERVER_MAX_SAMPLES="+value)
			out, err := cmd.CombinedOutput()
			require.Error(t, err, string(out))
			require.Equal(t, 2, cmd.ProcessState.ExitCode())
			require.NoDirExists(t, filepath.Join(dir, "backend-observer"))
		})
	}
}
