package production_test

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestRolloutDiagnosticSamplerPhaseFence(t *testing.T) {
	worker, err := filepath.Abs("rollout-diagnostic-sampler.sh")
	require.NoError(t, err)
	for _, tc := range []struct {
		name, body          string
		captures, completed int
	}{
		{"stable", "exit 0", 8, 8},
		{"failed", "exit 1", 8, 0},
		{"cleanup", `printf '%s' '{"phase":"cleanup"}' > "$PHASE"`, 1, 0},
		{"missing", `rm -- "$PHASE"`, 1, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			phase := filepath.Join(dir, "diagnostic-phase.json")
			require.NoError(t, os.WriteFile(phase, []byte(`{"format":"kubebrain.rollout-diagnostic-phase.v1","phase":"stable","probe_uid":"p","statefulset_uid":"s","image":"candidate"}`), 0600))
			callback := filepath.Join(dir, "capture")
			require.NoError(t, os.WriteFile(callback, []byte("#!/bin/bash\nset -eu\ntest -d \"$1\"\ntest -f \"$2\"\n"+tc.body+"\n"), 0700))
			// Remove only cadence delay; the real timeout and callback path run.
			cmd := exec.Command("bash", "-c", `sleep() { :; }; source "$1" "$2" "$3"`, "test", worker, dir, callback)
			cmd.Env = append(os.Environ(), "PHASE="+phase)
			out, err := cmd.CombinedOutput()
			require.NoError(t, err, string(out))
			captures, err := filepath.Glob(filepath.Join(dir, "diagnostic-sample.*"))
			require.NoError(t, err)
			require.Len(t, captures, tc.captures)
			completed, err := filepath.Glob(filepath.Join(dir, "diagnostic-sample.*", "capture-complete"))
			require.NoError(t, err)
			require.Len(t, completed, tc.completed)
		})
	}
}

func TestRolloutDiagnosticSamplerRejectsRelativeCallback(t *testing.T) {
	cmd := exec.Command("bash", "rollout-diagnostic-sampler.sh", t.TempDir(), "relative")
	err := cmd.Run()
	require.Error(t, err)
	require.Equal(t, 2, cmd.ProcessState.ExitCode())
}

func TestRolloutDiagnosticSamplerRunnerLifecycle(t *testing.T) {
	for _, mode := range []string{"success", "callback-failure", "probe-failure", "timeout"} {
		t.Run(mode, func(t *testing.T) {
			root := t.TempDir()
			t.Setenv("TMPDIR", root)
			fake, logPath, state := writeRolloutAvailabilityKubectl(t)
			callback := filepath.Join(root, "capture")
			require.NoError(t, os.WriteFile(callback, []byte(`#!/bin/bash
set -eu
jq -e '.phase=="stable"' "$2" >/dev/null
printf '%s\n' "$2" > "$TEST_SAMPLE_STARTED"
if [[ "$TEST_MODE" == callback-failure ]]; then exit 17; fi
trap 'echo SAMPLER_STOP >> "$FAKE_KUBECTL_LOG"; exit 0' TERM
while true; do sleep 0.05; done
`), 0700))
			wrapper := filepath.Join(root, "kubectl-wrapper")
			require.NoError(t, os.WriteFile(wrapper, []byte(`#!/bin/bash
set -eu
if [[ " $* " == *" wait --for=jsonpath={.status.phase}=Succeeded pod/kubebrain-rollout-availability-probe "* ]]; then
  for ((i=0;i<200;i++)); do
    [[ ! -f "$TEST_SAMPLE_STARTED" ]] || break
    sleep 0.01
  done
  [[ -f "$TEST_SAMPLE_STARTED" ]] || exit 91
fi
exec "$TEST_REAL_KUBECTL" "$@"
`), 0700))
			env := []string{"KUBECTL_BIN=" + wrapper, "TEST_REAL_KUBECTL=" + fake,
				"FAKE_KUBECTL_LOG=" + logPath, "FAKE_KUBECTL_STATE=" + state,
				"TEST_SAMPLE_STARTED=" + filepath.Join(root, "started"), "TEST_MODE=" + mode,
				"ROLLOUT_DIAGNOSTIC_SAMPLER=" + callback, "KEEP_RUNTIME_EVIDENCE=true",
				"ALLOW_MUTATING_KUBEBRAIN_ROLLOUT=true", "PROBE_ITERATIONS=6000",
				"TARGET_IMAGE=registry.example/kubebrain@sha256:" + strings.Repeat("a", 64),
				"TARGET_RUNTIME_DIGESTS=sha256:" + strings.Repeat("a", 64)}
			if mode == "timeout" {
				env = append(env, "PROBE_COMPLETE_TIMEOUT=3s", "FAKE_KUBECTL_HANG_TARGET=phase")
			} else if mode == "probe-failure" {
				env = append(env, "FAKE_PROBE_FAILED=true")
			}
			output, err := runProductionScriptCommandWithTimeout(t, "run-kubebrain-rollout-availability.sh", env, 40*time.Second)
			if mode == "timeout" || mode == "probe-failure" {
				require.Error(t, err, string(output))
				require.NoFileExists(t, state, "failed gate must restore original image")
			} else {
				require.NoError(t, err, string(output))
				require.Contains(t, string(output), "rollout availability gate passed")
			}
			phaseBefore := strings.TrimSpace(readOptionalFile(t, filepath.Join(root, "started")))
			require.NotEmpty(t, phaseBefore, "callback must actually run")
			phase := filepath.Join(filepath.Dir(filepath.Dir(phaseBefore)), "diagnostic-phase.json")
			require.Contains(t, readOptionalFile(t, phase), `"phase": "cleanup"`)
			log := readOptionalFile(t, logPath)
			if mode != "callback-failure" {
				stop := strings.Index(log, "SAMPLER_STOP")
				require.GreaterOrEqual(t, stop, 0, "active callback must be terminated")
				deletion := strings.Index(log[stop:], " uid-delete ")
				require.Positive(t, deletion, "fixture deletion must follow sampler stop")
			}
			for _, suffix := range []string{".probe", ".owner", ".cleanup"} {
				require.NoFileExists(t, logPath+suffix)
			}
		})
	}
}
