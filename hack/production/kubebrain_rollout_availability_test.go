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

func TestRolloutAvailabilityRunnerPreflightDoesNotCallKubernetes(t *testing.T) {
	fake, logPath, _ := writeRolloutAvailabilityKubectl(t)
	command := exec.Command("bash", "run-kubebrain-rollout-availability.sh")
	command.Env = append(os.Environ(),
		"KUBECTL_BIN="+fake,
		"FAKE_KUBECTL_LOG="+logPath,
		"ALLOW_MUTATING_KUBEBRAIN_ROLLOUT=true",
		"PREFLIGHT_ONLY=true",
		"TARGET_IMAGE=registry.example/kubebrain@sha256:"+strings.Repeat("a", 64),
		"TARGET_RUNTIME_DIGESTS=sha256:"+strings.Repeat("b", 64),
	)
	output, err := command.CombinedOutput()
	require.NoError(t, err, string(output))
	require.Equal(t, "rollout availability preflight passed\n", string(output))
	require.NoFileExists(t, logPath, "preflight must not call kubectl")
}

func TestRolloutAvailabilityRunnerRejectsInvalidPreflightModeBeforeKubernetes(t *testing.T) {
	fake, logPath, _ := writeRolloutAvailabilityKubectl(t)
	command := exec.Command("bash", "run-kubebrain-rollout-availability.sh")
	command.Env = append(os.Environ(),
		"KUBECTL_BIN="+fake,
		"FAKE_KUBECTL_LOG="+logPath,
		"ALLOW_MUTATING_KUBEBRAIN_ROLLOUT=true",
		"PREFLIGHT_ONLY=1",
	)
	output, err := command.CombinedOutput()
	require.EqualError(t, err, "exit status 2")
	require.Equal(t, "PREFLIGHT_ONLY must be true or false\n", string(output))
	require.NoFileExists(t, logPath)
}

func TestRolloutAvailabilityRunnerRejectsDurationOverflowBeforeKubernetes(t *testing.T) {
	for _, variable := range []string{
		"PROBE_COMMAND_TIMEOUT", "PROBE_DIAL_TIMEOUT", "PROBE_MAX_OPERATION_LATENCY",
		"PROBE_MAX_PD_TSO_LATENCY", "PROBE_MAX_TIKV_REGION_LATENCY", "PROBE_READY_TIMEOUT",
		"PROBE_COMPLETE_TIMEOUT", "ROLLOUT_TIMEOUT", "KUBECTL_MUTATION_REQUEST_TIMEOUT",
	} {
		t.Run(variable, func(t *testing.T) {
			fake, logPath, _ := writeRolloutAvailabilityKubectl(t)
			command := exec.Command("bash", "run-kubebrain-rollout-availability.sh")
			command.Env = append(os.Environ(), "KUBECTL_BIN="+fake, "FAKE_KUBECTL_LOG="+logPath,
				"ALLOW_MUTATING_KUBEBRAIN_ROLLOUT=true", variable+"=9223372036854775808s")
			output, err := command.CombinedOutput()
			require.Error(t, err)
			require.Contains(t, string(output), variable+" must be a positive ms, s, or m duration representable by Go time.Duration")
			require.NoFileExists(t, logPath)
		})
	}
}

func TestRolloutAvailabilityRunnerRejectsNumericControlsBeforeKubernetes(t *testing.T) {
	for _, tc := range []struct{ name, setting, want string }{
		{name: "replicas overflow", setting: "EXPECTED_REPLICAS=9223372036854775808", want: "EXPECTED_REPLICAS must be a positive int64"},
		{name: "iterations overflow", setting: "PROBE_ITERATIONS=9223372036854775808", want: "PROBE_ITERATIONS must be a positive int64"},
		{name: "lease TTL overflow", setting: "PROBE_LEASE_TTL=9223372036854775808", want: "PROBE_LEASE_TTL must be a positive int64"},
		{name: "port overflow", setting: "KUBEBRAIN_CLIENT_PORT=65536", want: "KUBEBRAIN_CLIENT_PORT must be a positive int64 between 1 and 65535"},
		{name: "zero interval", setting: "PROBE_INTERVAL=0", want: "PROBE_INTERVAL must be a canonical positive decimal seconds value"},
		{name: "interval overflow", setting: "PROBE_INTERVAL=9223372036.854775808", want: "PROBE_INTERVAL must be a canonical positive decimal seconds value"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fake, logPath, _ := writeRolloutAvailabilityKubectl(t)
			command := exec.Command("bash", "run-kubebrain-rollout-availability.sh")
			command.Env = append(os.Environ(), "KUBECTL_BIN="+fake, "FAKE_KUBECTL_LOG="+logPath,
				"ALLOW_MUTATING_KUBEBRAIN_ROLLOUT=true", tc.setting)
			output, err := command.CombinedOutput()
			require.Error(t, err)
			require.Contains(t, string(output), tc.want)
			require.NoFileExists(t, logPath)
		})
	}
}

func TestRolloutAvailabilityRunnerRejectsMutableTargetImageBeforeKubernetes(t *testing.T) {
	fake, logPath, _ := writeRolloutAvailabilityKubectl(t)
	command := exec.Command("bash", "run-kubebrain-rollout-availability.sh")
	command.Env = append(os.Environ(), "KUBECTL_BIN="+fake, "FAKE_KUBECTL_LOG="+logPath,
		"ALLOW_MUTATING_KUBEBRAIN_ROLLOUT=true", "TARGET_IMAGE=registry.example/kubebrain:latest")
	output, err := command.CombinedOutput()
	require.Error(t, err)
	require.Contains(t, string(output), "TARGET_IMAGE must be an immutable image reference")
	require.NoFileExists(t, logPath)
}

func TestRolloutAvailabilityRunnerRequiresCandidateRuntimeDigestsBeforeKubernetes(t *testing.T) {
	fake, logPath, _ := writeRolloutAvailabilityKubectl(t)
	command := exec.Command("bash", "run-kubebrain-rollout-availability.sh")
	command.Env = append(os.Environ(), "KUBECTL_BIN="+fake, "FAKE_KUBECTL_LOG="+logPath,
		"ALLOW_MUTATING_KUBEBRAIN_ROLLOUT=true",
		"TARGET_IMAGE=registry.example/kubebrain@sha256:"+strings.Repeat("a", 64))
	output, err := command.CombinedOutput()
	require.Error(t, err)
	require.Contains(t, string(output), "TARGET_RUNTIME_DIGESTS is required with TARGET_IMAGE")
	require.NoFileExists(t, logPath)
}

func TestRolloutAvailabilityRunnerAcceptsDurationBoundary(t *testing.T) {
	fake, logPath, statePath := writeRolloutAvailabilityKubectl(t)
	command := exec.Command("bash", "run-kubebrain-rollout-availability.sh")
	command.Env = append(os.Environ(),
		"KUBECTL_BIN="+fake, "FAKE_KUBECTL_LOG="+logPath, "FAKE_KUBECTL_STATE="+statePath,
		"ALLOW_MUTATING_KUBEBRAIN_ROLLOUT=true", "PROBE_ITERATIONS=3",
		"KUBEBRAIN_CLIENT_PORT=65535", "PROBE_INTERVAL=9223372036.854775807", "PROBE_LEASE_TTL=9223372036854775807",
		"PROBE_COMMAND_TIMEOUT=9223372036854ms", "PROBE_DIAL_TIMEOUT=9223372036s",
		"PROBE_MAX_OPERATION_LATENCY=153722867m", "PROBE_MAX_PD_TSO_LATENCY=9223372036854ms",
		"PROBE_MAX_TIKV_REGION_LATENCY=9223372036s", "PROBE_READY_TIMEOUT=153722867m",
		"PROBE_COMPLETE_TIMEOUT=153722867m", "ROLLOUT_TIMEOUT=9223372036854ms",
	)
	output, err := command.CombinedOutput()
	require.NoError(t, err, string(output))
	require.Contains(t, string(output), "rollout availability gate passed")
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
	require.Contains(t, string(output), "PROBE_SUMMARY ok=3 fail=0 total=3 watch=3 lease=alive max_latency_ms=123 max_tso_latency_ms=12 max_region_latency_ms=34")
	require.Contains(t, string(output), "revision=revision-old->revision-new")
	log := readOptionalFile(t, logPath)
	require.Contains(t, log, " run kubebrain-rollout-availability-probe ")
	require.Contains(t, log, "/usr/local/bin/kubebrain-rollout-availability-probe")
	require.Contains(t, log, "--command-timeout=10s")
	require.Contains(t, log, "--max-operation-latency=5s")
	require.Contains(t, log, "--max-pd-tso-latency=1s")
	require.Contains(t, log, "--max-tikv-region-latency=1s")
	require.Contains(t, log, "--lease-ttl=5")
	require.Contains(t, log, "--pd-endpoints=http://pd-0:2379,http://pd-1:2379,http://pd-2:2379")
	require.Contains(t, log, "--expected-up-stores=3")
	require.Contains(t, log, "--max-store-heartbeat-age=20s")
	require.Contains(t, log, " rollout restart statefulset/kubebrain")
	require.Contains(t, log, "--request-timeout=10s -n kubebrain-system rollout restart statefulset/kubebrain")
	require.Contains(t, log, " wait --for=jsonpath={.status.phase}=Succeeded")
	require.Contains(t, log, "--request-timeout=10s -n kubebrain-system delete pod kubebrain-rollout-availability-probe --ignore-not-found=true --wait=true --timeout=10s")
	require.Contains(t, log, " delete pod kubebrain-rollout-availability-probe")
}

func TestRolloutAvailabilityRunnerDeploysImmutableCandidateImage(t *testing.T) {
	fake, logPath, statePath := writeRolloutAvailabilityKubectl(t)
	target := "registry.example/kubebrain@sha256:" + strings.Repeat("a", 64)
	command := exec.Command("bash", "run-kubebrain-rollout-availability.sh")
	command.Env = append(os.Environ(),
		"KUBECTL_BIN="+fake, "FAKE_KUBECTL_LOG="+logPath, "FAKE_KUBECTL_STATE="+statePath,
		"ALLOW_MUTATING_KUBEBRAIN_ROLLOUT=true", "PROBE_ITERATIONS=3", "TARGET_IMAGE="+target,
		"TARGET_RUNTIME_DIGESTS=sha256:"+strings.Repeat("a", 64),
	)
	output, err := command.CombinedOutput()
	require.NoError(t, err, string(output))
	require.Contains(t, string(output), "image=kubebrain:test->"+target)
	log := readOptionalFile(t, logPath)
	require.Contains(t, log, " set image statefulset/kubebrain kubebrain="+target)
	require.Contains(t, log, "--request-timeout=10s -n kubebrain-system set image statefulset/kubebrain kubebrain="+target)
	require.NotContains(t, log, " rollout restart ")
	for ordinal := 0; ordinal < 3; ordinal++ {
		require.Contains(t, log, " get pod kubebrain-"+string(rune('0'+ordinal))+" -o json")
	}
}

func TestRolloutAvailabilityRunnerRestoresOriginalImageWhenCandidateFails(t *testing.T) {
	fake, logPath, statePath := writeRolloutAvailabilityKubectl(t)
	target := "registry.example/kubebrain@sha256:" + strings.Repeat("b", 64)
	command := exec.Command("bash", "run-kubebrain-rollout-availability.sh")
	command.Env = append(os.Environ(),
		"KUBECTL_BIN="+fake, "FAKE_KUBECTL_LOG="+logPath, "FAKE_KUBECTL_STATE="+statePath,
		"ALLOW_MUTATING_KUBEBRAIN_ROLLOUT=true", "PROBE_ITERATIONS=3", "TARGET_IMAGE="+target,
		"TARGET_RUNTIME_DIGESTS=sha256:"+strings.Repeat("b", 64),
		"FAKE_ROLLOUT_FAIL=true",
	)
	output, err := command.CombinedOutput()
	require.Error(t, err)
	require.Contains(t, string(output), "candidate rollout failed; restoring original image kubebrain:test")
	log := readOptionalFile(t, logPath)
	require.Contains(t, log, " set image statefulset/kubebrain kubebrain="+target)
	require.Contains(t, log, " set image statefulset/kubebrain kubebrain=kubebrain:test")
	require.Contains(t, log, "--request-timeout=10s -n kubebrain-system set image statefulset/kubebrain kubebrain=kubebrain:test")
}

func TestRolloutAvailabilityRunnerRollsBackCandidateWhenProbeDeletionFails(t *testing.T) {
	fake, logPath, statePath := writeRolloutAvailabilityKubectl(t)
	target := "registry.example/kubebrain@sha256:" + strings.Repeat("e", 64)
	command := exec.Command("bash", "run-kubebrain-rollout-availability.sh")
	command.Env = append(os.Environ(),
		"KUBECTL_BIN="+fake, "FAKE_KUBECTL_LOG="+logPath, "FAKE_KUBECTL_STATE="+statePath,
		"ALLOW_MUTATING_KUBEBRAIN_ROLLOUT=true", "PROBE_ITERATIONS=3", "TARGET_IMAGE="+target,
		"TARGET_RUNTIME_DIGESTS=sha256:"+strings.Repeat("e", 64),
		"FAKE_DELETE_FAIL=true",
	)
	output, err := command.CombinedOutput()
	require.Error(t, err)
	require.Contains(t, string(output), "failed to delete rollout availability probe Pod kubebrain-system/kubebrain-rollout-availability-probe")
	require.Contains(t, string(output), "candidate rollout failed; restoring original image kubebrain:test")
	require.Contains(t, string(output), "CRITICAL: failed to delete rollout availability probe Pod")
	require.NotContains(t, string(output), "KubeBrain rollout availability gate passed")
	log := readOptionalFile(t, logPath)
	require.Contains(t, log, "set image statefulset/kubebrain kubebrain="+target)
	require.Contains(t, log, "set image statefulset/kubebrain kubebrain=kubebrain:test")
}

func TestRolloutAvailabilityRunnerRejectsRuntimeDigestDriftAndRestoresOriginalImage(t *testing.T) {
	fake, logPath, statePath := writeRolloutAvailabilityKubectl(t)
	target := "registry.example/kubebrain@sha256:" + strings.Repeat("d", 64)
	command := exec.Command("bash", "run-kubebrain-rollout-availability.sh")
	command.Env = append(os.Environ(),
		"KUBECTL_BIN="+fake, "FAKE_KUBECTL_LOG="+logPath, "FAKE_KUBECTL_STATE="+statePath,
		"ALLOW_MUTATING_KUBEBRAIN_ROLLOUT=true", "PROBE_ITERATIONS=3", "TARGET_IMAGE="+target,
		"TARGET_RUNTIME_DIGESTS=sha256:"+strings.Repeat("d", 64),
		"FAKE_RUNTIME_DIGEST_DRIFT=true",
	)
	output, err := command.CombinedOutput()
	require.Error(t, err)
	require.Contains(t, string(output), "candidate Pod runtime release mismatch: kubebrain-0")
	require.Contains(t, string(output), "candidate rollout failed; restoring original image kubebrain:test")
	log := readOptionalFile(t, logPath)
	require.Contains(t, log, " set image statefulset/kubebrain kubebrain=kubebrain:test")
}

func TestRolloutAvailabilityRunnerRejectsRollbackIdentityDrift(t *testing.T) {
	fake, logPath, statePath := writeRolloutAvailabilityKubectl(t)
	target := "registry.example/kubebrain@sha256:" + strings.Repeat("f", 64)
	command := exec.Command("bash", "run-kubebrain-rollout-availability.sh")
	command.Env = append(os.Environ(),
		"KUBECTL_BIN="+fake, "FAKE_KUBECTL_LOG="+logPath, "FAKE_KUBECTL_STATE="+statePath,
		"ALLOW_MUTATING_KUBEBRAIN_ROLLOUT=true", "PROBE_ITERATIONS=3", "TARGET_IMAGE="+target,
		"TARGET_RUNTIME_DIGESTS=sha256:"+strings.Repeat("f", 64),
		"FAKE_RUNTIME_DIGEST_DRIFT=true",
		"FAKE_ROLLBACK_IDENTITY_DRIFT=true",
	)
	output, err := command.CombinedOutput()
	require.Error(t, err)
	require.Contains(t, string(output), "candidate rollout failed; restoring original image kubebrain:test")
	require.Contains(t, string(output), "CRITICAL: candidate image rollback identity mismatch: expected image=kubebrain:test revision=revision-old replicas=3")
	log := readOptionalFile(t, logPath)
	require.Contains(t, log, "set image statefulset/kubebrain kubebrain=kubebrain:test")
	require.Contains(t, log, "get statefulset kubebrain -o json")
}

func TestRolloutAvailabilityRunnerRejectsRollbackRuntimeDigestDrift(t *testing.T) {
	fake, logPath, statePath := writeRolloutAvailabilityKubectl(t)
	rollbackMarker := statePath + "-rollback"
	target := "registry.example/kubebrain@sha256:" + strings.Repeat("9", 64)
	command := exec.Command("bash", "run-kubebrain-rollout-availability.sh")
	command.Env = append(os.Environ(),
		"KUBECTL_BIN="+fake, "FAKE_KUBECTL_LOG="+logPath, "FAKE_KUBECTL_STATE="+statePath,
		"ALLOW_MUTATING_KUBEBRAIN_ROLLOUT=true", "PROBE_ITERATIONS=3", "TARGET_IMAGE="+target,
		"TARGET_RUNTIME_DIGESTS=sha256:"+strings.Repeat("9", 64),
		"FAKE_RUNTIME_DIGEST_DRIFT=true",
		"FAKE_ROLLBACK_RUNTIME_DRIFT=true",
		"FAKE_ROLLBACK_MARKER="+rollbackMarker,
	)
	output, err := command.CombinedOutput()
	require.Error(t, err)
	require.Contains(t, string(output), "candidate rollout failed; restoring original image kubebrain:test")
	require.Contains(t, string(output), "CRITICAL: candidate rollback Pod runtime identity mismatch: kubebrain-0")
	log := readOptionalFile(t, logPath)
	require.Contains(t, log, "get pod kubebrain-0 -o json")
}

func TestRolloutAvailabilityRunnerBoundsRuntimeEvidence(t *testing.T) {
	for _, target := range []string{"statefulset", "probe-log"} {
		t.Run(target, func(t *testing.T) {
			fake, logPath, statePath := writeRolloutAvailabilityKubectl(t)
			base := append(os.Environ(), "KUBECTL_BIN="+fake, "FAKE_KUBECTL_LOG="+logPath, "FAKE_KUBECTL_STATE="+statePath, "ALLOW_MUTATING_KUBEBRAIN_ROLLOUT=true", "PROBE_ITERATIONS=3", "FAKE_RUNTIME_RESPONSE_TARGET="+target)
			command := exec.Command("bash", "run-kubebrain-rollout-availability.sh")
			command.Env = append(base, "FAKE_RUNTIME_RESPONSE_BYTES=1048577")
			output, err := command.CombinedOutput()
			require.Error(t, err)
			require.Contains(t, string(output), "runtime evidence exceeds 1048576 bytes")
			require.NoError(t, os.RemoveAll(statePath))
			command = exec.Command("bash", "run-kubebrain-rollout-availability.sh")
			command.Env = append(base, "FAKE_RUNTIME_RESPONSE_BYTES=1048576")
			boundaryOutput, boundaryErr := command.CombinedOutput()
			require.NoError(t, boundaryErr, string(boundaryOutput))
			require.Contains(t, string(boundaryOutput), "rollout availability gate passed")
		})
	}
}

func TestRolloutAvailabilityRunnerBoundsProbePhaseResponse(t *testing.T) {
	fake, logPath, statePath := writeRolloutAvailabilityKubectl(t)
	phaseState := filepath.Join(filepath.Dir(statePath), "phase-state")
	base := append(os.Environ(), "KUBECTL_BIN="+fake, "FAKE_KUBECTL_LOG="+logPath, "FAKE_KUBECTL_STATE="+statePath, "FAKE_PHASE_STATE="+phaseState, "FAKE_PHASE_RESPONSE=true", "ALLOW_MUTATING_KUBEBRAIN_ROLLOUT=true", "PROBE_ITERATIONS=3")
	command := exec.Command("bash", "run-kubebrain-rollout-availability.sh")
	command.Env = append(base, "FAKE_PHASE_BYTES=4097")
	output, err := command.CombinedOutput()
	require.Error(t, err)
	require.Contains(t, string(output), "probe phase response exceeds 4096 bytes")
	require.NoError(t, os.RemoveAll(statePath))
	require.NoError(t, os.RemoveAll(phaseState))
	command = exec.Command("bash", "run-kubebrain-rollout-availability.sh")
	command.Env = append(base, "FAKE_PHASE_BYTES=4096")
	boundaryOutput, boundaryErr := command.CombinedOutput()
	require.NoError(t, boundaryErr, string(boundaryOutput))
	require.Contains(t, string(boundaryOutput), "rollout availability gate passed")
}

func TestRolloutAvailabilityRunnerFailsFastWhenProbeFails(t *testing.T) {
	fake, logPath, statePath := writeRolloutAvailabilityKubectl(t)
	command := exec.Command("bash", "run-kubebrain-rollout-availability.sh")
	command.Env = append(os.Environ(),
		"KUBECTL_BIN="+fake,
		"FAKE_KUBECTL_LOG="+logPath,
		"FAKE_KUBECTL_STATE="+statePath,
		"FAKE_PROBE_FAILED=true",
		"ALLOW_MUTATING_KUBEBRAIN_ROLLOUT=true",
		"PROBE_ITERATIONS=3",
		"PROBE_COMPLETE_TIMEOUT=3m",
	)
	output, err := command.CombinedOutput()
	require.Error(t, err)
	require.Contains(t, string(output), "availability probe failed")
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
  runtime_image=kubebrain:test
  [[ -e "$FAKE_KUBECTL_STATE" && -n "${TARGET_IMAGE:-}" ]] && runtime_image="$TARGET_IMAGE"
  payload="$(jq -cn --arg revision "$revision" --arg image "$runtime_image" --argjson prestop "$prestop" '{
    spec:{replicas:3,template:{spec:{containers:[{name:"kubebrain",image:$image,args:["--leader-retry-period=500ms","--pd-addrs=pd-0:2379,pd-1:2379,pd-2:2379"],lifecycle:{preStop:{exec:{command:$prestop}}}}]}}},
    status:{readyReplicas:3,currentRevision:$revision,updateRevision:$revision}}
  ')"
  printf '%s' "$payload"; [[ "${FAKE_RUNTIME_RESPONSE_TARGET:-}" != statefulset ]] || head -c "$((FAKE_RUNTIME_RESPONSE_BYTES-${#payload}))" /dev/zero | tr '\0' ' '
elif [[ " $* " == *" get pod kubebrain-rollout-availability-probe -o jsonpath={.status.phase} "* ]]; then
  if [[ "${FAKE_PROBE_FAILED:-false}" == true ]]; then printf Failed
  elif [[ "${FAKE_PHASE_RESPONSE:-false}" == true ]]; then printf Running; head -c "$((FAKE_PHASE_BYTES-7))" /dev/zero | tr '\0' ' '
  else printf Running; fi
elif [[ " $* " =~ " get pod kubebrain-"[0-9]+" -o json " ]]; then
  ordinal="$(awk '{for (i=1;i<=NF;i++) if ($i == "pod") print $(i+1)}' <<<"$*")"
  runtime_image=kubebrain:test
  runtime_digest="sha256:0000000000000000000000000000000000000000000000000000000000000000"
  runtime_revision=revision-old
  if [[ -e "$FAKE_KUBECTL_STATE" ]]; then
    runtime_image="${TARGET_IMAGE:-kubebrain:test}"
    runtime_digest="${TARGET_RUNTIME_DIGESTS%%,*}"
    runtime_revision=revision-new
    [[ "${FAKE_RUNTIME_DIGEST_DRIFT:-false}" != true ]] || runtime_digest="sha256:cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"
  elif [[ "${FAKE_ROLLBACK_RUNTIME_DRIFT:-false}" == true && -e "${FAKE_ROLLBACK_MARKER:-/nonexistent}" ]]; then
    runtime_digest="sha256:dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd"
  fi
  jq -cn --arg name "$ordinal" --arg image "$runtime_image" --arg digest "$runtime_digest" --arg revision "$runtime_revision" '{
    metadata:{name:$name,labels:{"controller-revision-hash":$revision}},
    spec:{containers:[{name:"kubebrain",image:$image}]},
    status:{phase:"Running",containerStatuses:[{name:"kubebrain",ready:true,imageID:("containerd://"+$digest)}]}}
  '
elif [[ " $* " == *" get pod kubebrain-rollout-availability-probe "* ]]; then
  exit 1
elif [[ " $* " == *" rollout restart statefulset/kubebrain "* || " $* " == *" set image statefulset/kubebrain "* ]]; then
  if [[ " $* " == *" set image statefulset/kubebrain kubebrain=kubebrain:test "* && "${FAKE_ROLLBACK_IDENTITY_DRIFT:-false}" != true ]]; then
    rm -f -- "$FAKE_KUBECTL_STATE"
    [[ -z "${FAKE_ROLLBACK_MARKER:-}" ]] || : >"$FAKE_ROLLBACK_MARKER"
  else
    : >"$FAKE_KUBECTL_STATE"
  fi
elif [[ " $* " == *" rollout status statefulset/kubebrain "* && "${FAKE_ROLLOUT_FAIL:-false}" == true ]]; then
  exit 1
elif [[ " $* " == *" delete pod kubebrain-rollout-availability-probe "* && "${FAKE_DELETE_FAIL:-false}" == true ]]; then
  exit 1
elif [[ " $* " == *" wait --for=jsonpath={.status.phase}=Succeeded "* ]]; then
  if [[ "${FAKE_PROBE_FAILED:-false}" == true ]]; then exit 1; fi
  if [[ "${FAKE_PHASE_RESPONSE:-false}" == true && ! -e "$FAKE_PHASE_STATE" ]]; then : >"$FAKE_PHASE_STATE"; exit 1; fi
elif [[ " $* " == *" logs kubebrain-rollout-availability-probe "* ]]; then
  payload=$'PROBE_STARTED\nPROBE_SUMMARY ok=3 fail=0 total=3 watch=3 lease=alive max_latency_ms=123 max_tso_latency_ms=12 max_region_latency_ms=34\n'
  printf '%s' "$payload"; [[ "${FAKE_RUNTIME_RESPONSE_TARGET:-}" != probe-log ]] || head -c "$((FAKE_RUNTIME_RESPONSE_BYTES-${#payload}))" /dev/zero | tr '\0' ' '
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
