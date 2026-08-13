package production_test

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRolloutAvailabilityRunnerRequiresExplicitMutationApproval(t *testing.T) {
	fake, logPath, _ := writeRolloutAvailabilityKubectl(t)
	command := exec.Command("bash", "run-kubebrain-rollout-availability.sh")
	command.Env = append(os.Environ(), "KUBECTL_BIN="+fake, "FAKE_KUBECTL_LOG="+logPath)
	output, err := command.CombinedOutput()
	require.Error(t, err)
	require.Contains(t, string(output), "ALLOW_MUTATING_KUBEBRAIN_ROLLOUT=true")
	_, statErr := os.Stat(logPath)
	require.ErrorIs(t, statErr, os.ErrNotExist, "kubectl must not run before mutation approval")
}

func TestRolloutAvailabilityRunnerRejectsMissingDrainBeforeMutation(t *testing.T) {
	fake, logPath, statePath := writeRolloutAvailabilityKubectl(t)
	command := exec.Command("bash", "run-kubebrain-rollout-availability.sh")
	command.Env = append(os.Environ(),
		"KUBECTL_BIN="+fake,
		"FAKE_KUBECTL_LOG="+logPath,
		"FAKE_KUBECTL_STATE="+statePath,
		"FAKE_BAD_PRESTOP=true",
		"ALLOW_MUTATING_KUBEBRAIN_ROLLOUT=true",
	)
	output, err := command.CombinedOutput()
	require.Error(t, err)
	require.Contains(t, string(output), "rollout drain contract mismatch")
	log := readOptionalFile(t, logPath)
	require.NotContains(t, log, " run ")
	require.NotContains(t, log, " rollout restart ")
}

func TestRolloutAvailabilityRunnerBindsProbeAndRevisionPostflight(t *testing.T) {
	fake, logPath, statePath := writeRolloutAvailabilityKubectl(t)
	command := exec.Command("bash", "run-kubebrain-rollout-availability.sh")
	command.Env = append(os.Environ(),
		"KUBECTL_BIN="+fake,
		"FAKE_KUBECTL_LOG="+logPath,
		"FAKE_KUBECTL_STATE="+statePath,
		"ALLOW_MUTATING_KUBEBRAIN_ROLLOUT=true",
		"PROBE_ITERATIONS=3",
	)
	output, err := command.CombinedOutput()
	require.NoError(t, err, string(output))
	require.Contains(t, string(output), "PROBE_SUMMARY ok=3 fail=0 total=3 watch=3 lease=alive max_latency_ms=123")
	require.Contains(t, string(output), "revision=revision-old->revision-new")
	log := readOptionalFile(t, logPath)
	require.Contains(t, log, " run kubebrain-rollout-availability-probe ")
	require.Contains(t, log, "/usr/local/bin/kubebrain-rollout-availability-probe")
	require.Contains(t, log, "--command-timeout=10s")
	require.Contains(t, log, "--max-operation-latency=5s")
	require.Contains(t, log, "--lease-ttl=5")
	require.Contains(t, log, "--pd-endpoints=http://pd-0:2379,http://pd-1:2379,http://pd-2:2379")
	require.Contains(t, log, " rollout restart statefulset/kubebrain")
	require.Contains(t, log, " wait --for=jsonpath={.status.phase}=Succeeded")
	require.Contains(t, log, " delete pod kubebrain-rollout-availability-probe")
}

func writeRolloutAvailabilityKubectl(t *testing.T) (fakePath, logPath, statePath string) {
	t.Helper()
	dir := t.TempDir()
	fakePath = filepath.Join(dir, "kubectl")
	logPath = filepath.Join(dir, "kubectl.log")
	statePath = filepath.Join(dir, "state")
	script := `#!/usr/bin/env bash
set -euo pipefail
printf ' %s' "$@" >>"$FAKE_KUBECTL_LOG"
printf '\n' >>"$FAKE_KUBECTL_LOG"
if [[ " $* " == *" get statefulset kubebrain -o json "* ]]; then
  revision=revision-old
  [[ -e "$FAKE_KUBECTL_STATE" ]] && revision=revision-new
  prestop='["/bin/sh","-c","curl --fail --silent --show-error --max-time 10 --request POST http://127.0.0.1:8080/drain && sleep 5"]'
  [[ "${FAKE_BAD_PRESTOP:-false}" == true ]] && prestop='["/bin/sleep","5"]'
  jq -cn --arg revision "$revision" --argjson prestop "$prestop" '{
    spec:{replicas:3,template:{spec:{containers:[{name:"kubebrain",image:"kubebrain:test",args:["--leader-retry-period=500ms","--pd-addrs=pd-0:2379,pd-1:2379,pd-2:2379"],lifecycle:{preStop:{exec:{command:$prestop}}}}]}}},
    status:{readyReplicas:3,currentRevision:$revision,updateRevision:$revision}}
  '
elif [[ " $* " == *" get pod kubebrain-rollout-availability-probe "* ]]; then
  exit 1
elif [[ " $* " == *" rollout restart statefulset/kubebrain "* ]]; then
  : >"$FAKE_KUBECTL_STATE"
elif [[ " $* " == *" logs kubebrain-rollout-availability-probe "* ]]; then
  printf '%s\n' PROBE_STARTED 'PROBE_SUMMARY ok=3 fail=0 total=3 watch=3 lease=alive max_latency_ms=123'
fi
`
	require.NoError(t, os.WriteFile(fakePath, []byte(script), 0o755))
	return fakePath, logPath, statePath
}

func readOptionalFile(t *testing.T, path string) string {
	t.Helper()
	content, err := os.ReadFile(path)
	if os.IsNotExist(err) {
		return ""
	}
	require.NoError(t, err)
	return strings.TrimSpace(string(content))
}
