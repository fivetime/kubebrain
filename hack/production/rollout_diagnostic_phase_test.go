package production_test

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRolloutDiagnosticPhasePublication(t *testing.T) {
	data, err := os.ReadFile("run-kubebrain-rollout-availability.sh")
	require.NoError(t, err)
	script := string(data)
	start := strings.Index(script, "record_rollout_diagnostic_phase() {")
	require.Positive(t, start)
	end := strings.Index(script[start:], "\nstop_rollout_observer()")
	require.Positive(t, end)
	function := script[start : start+end]
	for _, failure := range []bool{false, true} {
		name := "normal"
		if failure {
			name = "rename-failure"
		}
		t.Run(name, func(t *testing.T) {
			dir := t.TempDir()
			commands := `
record_rollout_diagnostic_phase stable
jq -e '.phase=="stable" and .probe_uid=="probe-uid" and .statefulset_uid=="sts-uid" and .image=="candidate"' "$runtime_evidence_dir/diagnostic-phase.json"
if record_rollout_diagnostic_phase invalid; then exit 1; fi
`
			if failure {
				commands += `
mv() { return 1; }
if record_rollout_diagnostic_phase cleanup; then exit 1; fi
test ! -e "$runtime_evidence_dir/diagnostic-phase.json"
`
			} else {
				commands += `
record_rollout_diagnostic_phase cleanup
jq -e '.phase=="cleanup"' "$runtime_evidence_dir/diagnostic-phase.json"
`
			}
			cmd := exec.Command("bash", "-c", "set -euo pipefail\n"+function+commands)
			cmd.Env = append(os.Environ(), "runtime_evidence_dir="+dir, "probe_pod_uid=probe-uid", "statefulset_uid=sts-uid", "TARGET_IMAGE=candidate")
			out, err := cmd.CombinedOutput()
			require.NoError(t, err, string(out))
			_, err = os.Stat(filepath.Join(dir, "diagnostic-phase.next"))
			require.True(t, os.IsNotExist(err))
		})
	}
}

func TestRolloutDiagnosticPhaseDoesNotExtendDeadline(t *testing.T) {
	data, err := os.ReadFile("run-kubebrain-rollout-availability.sh")
	require.NoError(t, err)
	script := string(data)
	require.Contains(t, script, "probe_complete_deadline=$((SECONDS + probe_complete_seconds))\nrecord_rollout_diagnostic_phase stable ||")
	cleanup := script[strings.Index(script, "rollout_exit() {"):]
	require.Less(t, strings.Index(cleanup, "record_rollout_diagnostic_phase cleanup ||"), strings.Index(cleanup, "(set -e; cleanup)"))
	require.Less(t, strings.Index(cleanup, "stop_rollout_sampler"), strings.Index(cleanup, "(set -e; cleanup)"))
}

func TestRolloutDiagnosticSamplerStopReapsChild(t *testing.T) {
	data, err := os.ReadFile("run-kubebrain-rollout-availability.sh")
	require.NoError(t, err)
	script := string(data)
	start := strings.Index(script, "stop_rollout_sampler() {")
	require.Positive(t, start)
	end := strings.Index(script[start:], "\nstop_rollout_observer()")
	require.Positive(t, end)
	cmd := exec.Command("bash", "-c", "set -euo pipefail\n"+script[start:start+end]+`
timeout --signal=TERM --kill-after=2s 5s sleep 5 &
rollout_sampler_pid=$!
owned=$rollout_sampler_pid
stop_rollout_sampler
test -z "$rollout_sampler_pid"
if kill -0 "$owned" 2>/dev/null; then exit 1; fi
stop_rollout_sampler
`)
	out, err := cmd.CombinedOutput()
	require.NoError(t, err, string(out))
}
