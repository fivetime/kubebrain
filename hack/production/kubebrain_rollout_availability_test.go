package production_test

import (
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

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

func TestRolloutAvailabilityRunnerDoesNotBypassBoundedKubectlWrappers(t *testing.T) {
	source, err := os.ReadFile("run-kubebrain-rollout-availability.sh")
	require.NoError(t, err)
	for _, directCall := range []string{
		" kctl get ", " kctl logs ", "\nkctl run ", "\nkctl wait ", "\nkctl rollout status ", "\nkctl()",
	} {
		require.NotContains(t, string(source), directCall,
			"kubectl calls must use their request and process-timeout wrappers")
	}
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

func TestRolloutAvailabilityRunnerRequiresTimeoutBinaryBeforeKubernetes(t *testing.T) {
	fake, logPath, _ := writeRolloutAvailabilityKubectl(t)
	command := exec.Command("bash", "run-kubebrain-rollout-availability.sh")
	command.Env = append(os.Environ(),
		"KUBECTL_BIN="+fake,
		"FAKE_KUBECTL_LOG="+logPath,
		"ALLOW_MUTATING_KUBEBRAIN_ROLLOUT=true",
		"TIMEOUT_BIN=/nonexistent/kubebrain-timeout",
	)
	output, err := command.CombinedOutput()
	require.Error(t, err)
	require.Contains(t, string(output), "timeout binary is not executable")
	require.NoFileExists(t, logPath)
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
		"PROBE_COMMAND_TIMEOUT", "PROBE_DIAL_TIMEOUT", "PROBE_MAX_OPERATION_LATENCY", "PROBE_MAX_DIRECT_STREAM_LATENCY",
		"PROBE_MAX_PD_TSO_LATENCY", "PROBE_MAX_TIKV_REGION_LATENCY", "PROBE_READY_TIMEOUT",
		"PROBE_RANGE_STREAM_INTERVAL", "PROBE_SNAPSHOT_START_DELAY", "PROBE_STREAM_ATTEMPT_TIMEOUT",
		"PROBE_STREAM_RETRY_BACKOFF", "PROBE_STREAM_MAX_RETRY_BACKOFF",
		"PROBE_START_TIMEOUT", "PROBE_COMPLETE_TIMEOUT", "ROLLOUT_TIMEOUT", "KUBECTL_EVIDENCE_REQUEST_TIMEOUT",
		"KUBECTL_EVIDENCE_COMMAND_TIMEOUT", "KUBECTL_MUTATION_REQUEST_TIMEOUT",
		"KUBECTL_MUTATION_COMMAND_TIMEOUT", "KUBECTL_READY_WAIT_COMMAND_TIMEOUT",
		"KUBECTL_ROLLOUT_STATUS_COMMAND_TIMEOUT", "KUBECTL_PHASE_WAIT_COMMAND_TIMEOUT",
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

func TestRolloutAvailabilityRunnerRejectsInvertedStreamBackoffBeforeKubernetes(t *testing.T) {
	fake, logPath, _ := writeRolloutAvailabilityKubectl(t)
	command := exec.Command("bash", "run-kubebrain-rollout-availability.sh")
	command.Env = append(os.Environ(),
		"KUBECTL_BIN="+fake,
		"FAKE_KUBECTL_LOG="+logPath,
		"ALLOW_MUTATING_KUBEBRAIN_ROLLOUT=true",
		"PROBE_STREAM_RETRY_BACKOFF=2s",
		"PROBE_STREAM_MAX_RETRY_BACKOFF=1000ms",
	)
	output, err := command.CombinedOutput()
	require.EqualError(t, err, "exit status 2")
	require.Contains(t, string(output), "PROBE_STREAM_RETRY_BACKOFF must not exceed PROBE_STREAM_MAX_RETRY_BACKOFF")
	require.NoFileExists(t, logPath)
}

func TestRolloutAvailabilityRunnerBoundsHungKubectlProcesses(t *testing.T) {
	for _, tc := range []struct {
		name, target, timeoutVariable, want string
		extraEnv                            []string
		wantProbeLogCalls                   int
	}{
		{name: "evidence", target: "evidence", timeoutVariable: "KUBECTL_EVIDENCE_COMMAND_TIMEOUT=100ms", want: "failed to read KubeBrain StatefulSet"},
		{name: "mutation", target: "mutation", timeoutVariable: "KUBECTL_MUTATION_COMMAND_TIMEOUT=100ms", want: "failed to create rollout availability probe Pod"},
		{name: "ready wait", target: "ready", timeoutVariable: "KUBECTL_READY_WAIT_COMMAND_TIMEOUT=100ms", want: "rollout availability probe Pod did not become Ready"},
		{name: "rollout status", target: "rollout", timeoutVariable: "KUBECTL_ROLLOUT_STATUS_COMMAND_TIMEOUT=100ms", want: "KubeBrain rollout did not converge"},
		{name: "phase wait deadline", target: "phase", timeoutVariable: "KUBECTL_PHASE_WAIT_COMMAND_TIMEOUT=30s", want: "availability probe did not complete within 1s", extraEnv: []string{"PROBE_COMPLETE_TIMEOUT=1s"}, wantProbeLogCalls: 1},
		{name: "phase evidence deadline", target: "phase-evidence", timeoutVariable: "KUBECTL_EVIDENCE_COMMAND_TIMEOUT=30s", want: "availability probe did not complete within 1s", extraEnv: []string{"PROBE_COMPLETE_TIMEOUT=1s", "FAKE_PROBE_FAILED=true"}, wantProbeLogCalls: 1},
		{name: "start barrier deadline", target: "start", timeoutVariable: "KUBECTL_EVIDENCE_COMMAND_TIMEOUT=30s", want: "availability probe did not publish its start barrier within 1s", extraEnv: []string{"PROBE_START_TIMEOUT=1s"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fake, logPath, statePath := writeRolloutAvailabilityKubectl(t)
			env := []string{
				"KUBECTL_BIN=" + fake, "FAKE_KUBECTL_LOG=" + logPath, "FAKE_KUBECTL_STATE=" + statePath,
				"ALLOW_MUTATING_KUBEBRAIN_ROLLOUT=true", "PROBE_ITERATIONS=3",
				"FAKE_KUBECTL_HANG_TARGET=" + tc.target, tc.timeoutVariable,
			}
			env = append(env, tc.extraEnv...)
			started := time.Now()
			output, err := runProductionScriptCommandWithTimeout(t,
				"run-kubebrain-rollout-availability.sh", env, 5*time.Second)
			require.Error(t, err)
			require.Contains(t, string(output), tc.want)
			if tc.wantProbeLogCalls > 0 {
				log := readOptionalFile(t, logPath)
				require.Equal(t, tc.wantProbeLogCalls,
					strings.Count(log, " logs kubebrain-rollout-availability-probe"),
					"an expired completion stage must not start a diagnostic log request")
			}
			require.Less(t, time.Since(started), 4*time.Second,
				"the outer command timeout must terminate a kubectl process that never reaches HTTP request handling")
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

func TestRolloutAvailabilityRunnerRejectsMutableProbeImageBeforeKubernetes(t *testing.T) {
	fake, logPath, _ := writeRolloutAvailabilityKubectl(t)
	command := exec.Command("bash", "run-kubebrain-rollout-availability.sh")
	command.Env = append(os.Environ(), "KUBECTL_BIN="+fake, "FAKE_KUBECTL_LOG="+logPath,
		"ALLOW_MUTATING_KUBEBRAIN_ROLLOUT=true", "PROBE_IMAGE=registry.example/kubebrain:latest")
	output, err := command.CombinedOutput()
	require.Error(t, err)
	require.Contains(t, string(output), "PROBE_IMAGE must be an immutable image reference")
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
		"PROBE_RANGE_STREAM_INTERVAL=9223372036854ms", "PROBE_SNAPSHOT_START_DELAY=9223372036s",
		"PROBE_STREAM_ATTEMPT_TIMEOUT=153722867m", "PROBE_STREAM_RETRY_BACKOFF=9223372036s",
		"PROBE_STREAM_MAX_RETRY_BACKOFF=9223372036s",
		"PROBE_START_TIMEOUT=9223372036854ms", "PROBE_COMPLETE_TIMEOUT=153722867m", "ROLLOUT_TIMEOUT=9223372036854ms",
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

func TestRolloutAvailabilityRunnerRejectsDrainBeforeEndpointPropagation(t *testing.T) {
	fake, logPath, statePath := writeRolloutAvailabilityKubectl(t)
	command := exec.Command("bash", "run-kubebrain-rollout-availability.sh")
	command.Env = append(os.Environ(),
		"KUBECTL_BIN="+fake,
		"FAKE_KUBECTL_LOG="+logPath,
		"FAKE_KUBECTL_STATE="+statePath,
		"FAKE_DRAIN_FIRST_PRESTOP=true",
		"ALLOW_MUTATING_KUBEBRAIN_ROLLOUT=true",
	)
	output, err := command.CombinedOutput()
	require.Error(t, err)
	require.Contains(t, string(output), "rollout drain contract mismatch")
	log := readOptionalFile(t, logPath)
	require.NotContains(t, log, " run ")
	require.NotContains(t, log, " patch ")
}

func TestRolloutAvailabilityRunnerRejectsShortEndpointPropagationWindow(t *testing.T) {
	fake, logPath, statePath := writeRolloutAvailabilityKubectl(t)
	command := exec.Command("bash", "run-kubebrain-rollout-availability.sh")
	command.Env = append(os.Environ(),
		"KUBECTL_BIN="+fake,
		"FAKE_KUBECTL_LOG="+logPath,
		"FAKE_KUBECTL_STATE="+statePath,
		"FAKE_SHORT_PROPAGATION_PRESTOP=true",
		"ALLOW_MUTATING_KUBEBRAIN_ROLLOUT=true",
	)
	output, err := command.CombinedOutput()
	require.Error(t, err)
	require.Contains(t, string(output), "rollout drain contract mismatch")
	log := readOptionalFile(t, logPath)
	require.NotContains(t, log, " run ")
	require.NotContains(t, log, " patch ")
}

func TestRolloutAvailabilityRunnerAcceptsDrainImmediatelyAfterEndpointPropagationWindow(t *testing.T) {
	fake, logPath, statePath := writeRolloutAvailabilityKubectl(t)
	command := exec.Command("bash", "run-kubebrain-rollout-availability.sh")
	command.Env = append(os.Environ(),
		"KUBECTL_BIN="+fake,
		"FAKE_KUBECTL_LOG="+logPath,
		"FAKE_KUBECTL_STATE="+statePath,
		"FAKE_DRAIN_AFTER_PROPAGATION_PRESTOP=true",
		"ALLOW_MUTATING_KUBEBRAIN_ROLLOUT=true",
		"PROBE_ITERATIONS=3",
	)
	output, err := command.CombinedOutput()
	require.NoError(t, err, string(output))
	require.Contains(t, string(output), "rollout availability gate passed")
}

func TestRolloutAvailabilityRunnerRejectsShortTerminationGraceBeforeMutation(t *testing.T) {
	fake, logPath, statePath := writeRolloutAvailabilityKubectl(t)
	command := exec.Command("bash", "run-kubebrain-rollout-availability.sh")
	command.Env = append(os.Environ(),
		"KUBECTL_BIN="+fake,
		"FAKE_KUBECTL_LOG="+logPath,
		"FAKE_KUBECTL_STATE="+statePath,
		"FAKE_SHORT_TERMINATION_GRACE=true",
		"ALLOW_MUTATING_KUBEBRAIN_ROLLOUT=true",
	)
	output, err := command.CombinedOutput()
	require.Error(t, err)
	require.Contains(t, string(output), "rollout drain contract mismatch")
	log := readOptionalFile(t, logPath)
	require.NotContains(t, log, " run ")
	require.NotContains(t, log, " patch ")
}

func TestRolloutAvailabilityRunnerRequiresStableHeadlessServiceBeforeMutation(t *testing.T) {
	fake, logPath, statePath := writeRolloutAvailabilityKubectl(t)
	command := exec.Command("bash", "run-kubebrain-rollout-availability.sh")
	command.Env = append(os.Environ(),
		"KUBECTL_BIN="+fake,
		"FAKE_KUBECTL_LOG="+logPath,
		"FAKE_KUBECTL_STATE="+statePath,
		"FAKE_NO_HEADLESS_SERVICE=true",
		"ALLOW_MUTATING_KUBEBRAIN_ROLLOUT=true",
	)
	output, err := command.CombinedOutput()
	require.Error(t, err)
	require.Contains(t, string(output), "rollout drain contract mismatch")
	log := readOptionalFile(t, logPath)
	require.NotContains(t, log, " run ")
	require.NotContains(t, log, " patch ")
}

func TestRolloutAvailabilityRunnerRequiresPublishedHeadlessPodAddressesBeforeMutation(t *testing.T) {
	fake, logPath, statePath := writeRolloutAvailabilityKubectl(t)
	command := exec.Command("bash", "run-kubebrain-rollout-availability.sh")
	command.Env = append(os.Environ(),
		"KUBECTL_BIN="+fake,
		"FAKE_KUBECTL_LOG="+logPath,
		"FAKE_KUBECTL_STATE="+statePath,
		"FAKE_HEADLESS_PUBLISH_NOT_READY=false",
		"ALLOW_MUTATING_KUBEBRAIN_ROLLOUT=true",
		"PROBE_ITERATIONS=3",
	)
	output, err := command.CombinedOutput()
	require.Error(t, err)
	require.Contains(t, string(output), "headless Service rollout DNS contract mismatch")
	log := readOptionalFile(t, logPath)
	require.NotContains(t, log, " run ")
	require.NotContains(t, log, " patch ")
}

func TestRolloutAvailabilityRunnerRejectsInvalidHeadlessServiceIdentityBeforeMutation(t *testing.T) {
	for _, setting := range []string{"FAKE_HEADLESS_CLUSTER_IP=10.96.0.10", "FAKE_HEADLESS_SELECTOR_MISMATCH=true"} {
		t.Run(setting, func(t *testing.T) {
			fake, logPath, statePath := writeRolloutAvailabilityKubectl(t)
			command := exec.Command("bash", "run-kubebrain-rollout-availability.sh")
			command.Env = append(os.Environ(),
				"KUBECTL_BIN="+fake,
				"FAKE_KUBECTL_LOG="+logPath,
				"FAKE_KUBECTL_STATE="+statePath,
				setting,
				"ALLOW_MUTATING_KUBEBRAIN_ROLLOUT=true",
				"PROBE_ITERATIONS=3",
			)
			output, err := command.CombinedOutput()
			require.Error(t, err)
			require.Contains(t, string(output), "headless Service rollout DNS contract mismatch")
			log := readOptionalFile(t, logPath)
			require.NotContains(t, log, " run ")
			require.NotContains(t, log, " patch ")
		})
	}
}

func TestRolloutAvailabilityRunnerRejectsHeadlessServiceDriftDuringRollout(t *testing.T) {
	fake, logPath, statePath := writeRolloutAvailabilityKubectl(t)
	command := exec.Command("bash", "run-kubebrain-rollout-availability.sh")
	command.Env = append(os.Environ(),
		"KUBECTL_BIN="+fake,
		"FAKE_KUBECTL_LOG="+logPath,
		"FAKE_KUBECTL_STATE="+statePath,
		"FAKE_HEADLESS_IDENTITY_DRIFT=true",
		"ALLOW_MUTATING_KUBEBRAIN_ROLLOUT=true",
		"PROBE_ITERATIONS=3",
	)
	output, err := command.CombinedOutput()
	require.Error(t, err)
	require.Contains(t, string(output), "headless Service identity drifted during rollout")
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
		"KUBECTL_EVIDENCE_REQUEST_TIMEOUT=7s",
	)
	output, err := command.CombinedOutput()
	require.NoError(t, err, string(output))
	require.Contains(t, string(output), "PROBE_SUMMARY ok=3 fail=0 total=3 watch=3 direct_watch=3x3 lease=alive direct_lease=alive direct_lease_restarts=3 direct_endpoints=3 range_stream=17 snapshot=2 stream_retries=4 stream_partial_retries=1 max_latency_ms=123 max_direct_latency_ms=456 max_tso_latency_ms=12 max_region_latency_ms=34")
	require.Contains(t, string(output), "revision=revision-old->revision-new")
	log := readOptionalFile(t, logPath)
	require.Contains(t, log, " run kubebrain-rollout-availability-probe ")
	for _, line := range strings.Split(strings.TrimSpace(log), "\n") {
		if strings.Contains(line, " get statefulset ") || strings.Contains(line, " get pod ") || strings.Contains(line, " logs ") {
			require.Contains(t, line, "--request-timeout=7s -n kubebrain-system",
				"every bounded evidence read must carry the dedicated request timeout")
		}
	}
	require.Contains(t, log, "/usr/local/bin/kubebrain-rollout-availability-probe")
	require.Contains(t, log, "--command-timeout=10s")
	require.Contains(t, log, "--max-operation-latency=5s")
	require.Contains(t, log, "--max-direct-stream-latency=30s")
	require.Contains(t, log, "--max-pd-tso-latency=1s")
	require.Contains(t, log, "--max-tikv-region-latency=1s")
	require.Contains(t, log, "--range-stream-interval=1s")
	require.Contains(t, log, "--snapshot-start-delay=25s")
	require.Contains(t, log, "--stream-attempt-timeout=2m")
	require.Contains(t, log, "--stream-retry-backoff=100ms")
	require.Contains(t, log, "--stream-max-retry-backoff=2s")
	require.Contains(t, log, "--snapshot-artifact-dir=/var/run/kubebrain-rollout-availability")
	require.Contains(t, log, `"name":"snapshot-artifact","mountPath":"/var/run/kubebrain-rollout-availability"`)
	require.Contains(t, log, `"name":"snapshot-artifact","emptyDir":{}`)
	require.Contains(t, log, `"runAsNonRoot":true`)
	require.Contains(t, log, "--lease-ttl=5")
	require.Contains(t, log, "--direct-endpoints=http://kubebrain-0.kubebrain-peer.kubebrain-system.svc:3379,http://kubebrain-1.kubebrain-peer.kubebrain-system.svc:3379,http://kubebrain-2.kubebrain-peer.kubebrain-system.svc:3379")
	require.Contains(t, log, "--pd-endpoints=http://pd-0:2379,http://pd-1:2379,http://pd-2:2379")
	require.Contains(t, log, "--expected-up-stores=3")
	require.Contains(t, log, "--max-store-heartbeat-age=20s")
	require.Contains(t, log, "--request-timeout=10s -n kubebrain-system run kubebrain-rollout-availability-probe")
	require.Contains(t, log, "--request-timeout=10s -n kubebrain-system patch statefulset/kubebrain --type=json -p ")
	require.Contains(t, log, `kubectl.kubernetes.io~1restartedAt`)
	require.NotContains(t, log, " rollout restart ")
	require.Contains(t, log, " wait --for=jsonpath={.status.phase}=Succeeded")
	require.Contains(t, log, "--request-timeout=10s -n kubebrain-system delete pod kubebrain-rollout-availability-probe --ignore-not-found=true --wait=true --timeout=10s")
	require.Contains(t, log, " delete pod kubebrain-rollout-availability-probe")
}

func TestRolloutAvailabilityRunnerRequiresCompleteRangeStreamAndSnapshot(t *testing.T) {
	fake, logPath, statePath := writeRolloutAvailabilityKubectl(t)
	content, err := os.ReadFile(fake)
	require.NoError(t, err)
	content = []byte(strings.ReplaceAll(string(content), "range_stream=17 snapshot=2", "range_stream=0 snapshot=0"))
	require.NoError(t, os.WriteFile(fake, content, 0o755))
	command := exec.Command("bash", "run-kubebrain-rollout-availability.sh")
	command.Env = append(os.Environ(),
		"KUBECTL_BIN="+fake,
		"FAKE_KUBECTL_LOG="+logPath,
		"FAKE_KUBECTL_STATE="+statePath,
		"ALLOW_MUTATING_KUBEBRAIN_ROLLOUT=true",
		"PROBE_ITERATIONS=3",
	)
	output, err := command.CombinedOutput()
	require.Error(t, err)
	require.Contains(t, string(output), "availability probe summary mismatch")
	require.NotContains(t, string(output), "KubeBrain rollout availability gate passed")
}

func TestRolloutAvailabilityRunnerBindsMutualTLSProbeIdentity(t *testing.T) {
	fake, logPath, statePath := writeRolloutAvailabilityKubectl(t)
	command := exec.Command("bash", "run-kubebrain-rollout-availability.sh")
	command.Env = append(os.Environ(),
		"KUBECTL_BIN="+fake,
		"FAKE_KUBECTL_LOG="+logPath,
		"FAKE_KUBECTL_STATE="+statePath,
		"FAKE_TLS_STATE=true",
		"ALLOW_MUTATING_KUBEBRAIN_ROLLOUT=true",
		"PROBE_ITERATIONS=3",
	)
	output, err := command.CombinedOutput()
	require.NoError(t, err, string(output))
	require.Contains(t, string(output), "rollout availability gate passed")
	log := readOptionalFile(t, logPath)
	require.Contains(t, log, "--endpoint=https://kubebrain-client.kubebrain-system.svc:3379")
	require.Contains(t, log, "--direct-endpoints=https://kubebrain-0.kubebrain-peer.kubebrain-system.svc:3379,https://kubebrain-1.kubebrain-peer.kubebrain-system.svc:3379,https://kubebrain-2.kubebrain-peer.kubebrain-system.svc:3379")
	require.Contains(t, log, "--cacert=/etc/kubebrain/client-tls/ca.crt")
	require.Contains(t, log, "--cert=/etc/kubebrain/client-tls/tls.crt")
	require.Contains(t, log, "--key=/etc/kubebrain/client-tls/tls.key")
	require.Contains(t, log, "--tls-server-name=kubebrain-client.kubebrain-system.svc")
	require.Contains(t, log, `"secretName":"kubebrain-client-tls"`)
	require.Contains(t, log, `"mountPath":"/etc/kubebrain/client-tls"`)
	var overrideJSON string
	for _, line := range strings.Split(log, "\n") {
		if index := strings.Index(line, "--overrides="); index >= 0 {
			overrideJSON = line[index+len("--overrides="):]
			break
		}
	}
	require.NotEmpty(t, overrideJSON)
	var override struct {
		Spec struct {
			AutomountServiceAccountToken bool   `json:"automountServiceAccountToken"`
			RestartPolicy                string `json:"restartPolicy"`
			SecurityContext              struct {
				RunAsNonRoot bool  `json:"runAsNonRoot"`
				RunAsUser    int64 `json:"runAsUser"`
				RunAsGroup   int64 `json:"runAsGroup"`
				FSGroup      int64 `json:"fsGroup"`
			} `json:"securityContext"`
			Containers []struct {
				Name         string   `json:"name"`
				Image        string   `json:"image"`
				Command      []string `json:"command"`
				Args         []string `json:"args"`
				VolumeMounts []struct {
					Name      string `json:"name"`
					MountPath string `json:"mountPath"`
					ReadOnly  bool   `json:"readOnly"`
				} `json:"volumeMounts"`
			} `json:"containers"`
			Volumes []struct {
				Name     string         `json:"name"`
				EmptyDir map[string]any `json:"emptyDir"`
			} `json:"volumes"`
		} `json:"spec"`
	}
	require.NoError(t, json.Unmarshal([]byte(overrideJSON), &override))
	require.False(t, override.Spec.AutomountServiceAccountToken)
	require.Equal(t, "Never", override.Spec.RestartPolicy)
	require.True(t, override.Spec.SecurityContext.RunAsNonRoot)
	require.EqualValues(t, 65532, override.Spec.SecurityContext.RunAsUser)
	require.EqualValues(t, 65532, override.Spec.SecurityContext.RunAsGroup)
	require.EqualValues(t, 65532, override.Spec.SecurityContext.FSGroup)
	require.Len(t, override.Spec.Containers, 1)
	require.Equal(t, "kubebrain:test", override.Spec.Containers[0].Image)
	require.Equal(t, []string{"/usr/local/bin/kubebrain-rollout-availability-probe"}, override.Spec.Containers[0].Command)
	require.Contains(t, override.Spec.Containers[0].Args, "--endpoint=https://kubebrain-client.kubebrain-system.svc:3379")
	require.Contains(t, override.Spec.Containers[0].Args, "--snapshot-artifact-dir=/var/run/kubebrain-rollout-availability")
	require.Len(t, override.Spec.Containers[0].VolumeMounts, 2)
	require.Equal(t, "/etc/kubebrain/client-tls", override.Spec.Containers[0].VolumeMounts[0].MountPath)
	require.True(t, override.Spec.Containers[0].VolumeMounts[0].ReadOnly)
	require.Equal(t, "snapshot-artifact", override.Spec.Containers[0].VolumeMounts[1].Name)
	require.Equal(t, "/var/run/kubebrain-rollout-availability", override.Spec.Containers[0].VolumeMounts[1].MountPath)
	require.False(t, override.Spec.Containers[0].VolumeMounts[1].ReadOnly)
	require.Len(t, override.Spec.Volumes, 2)
	require.Equal(t, "snapshot-artifact", override.Spec.Volumes[1].Name)
	require.NotNil(t, override.Spec.Volumes[1].EmptyDir)
}

func TestRolloutAvailabilityRunnerRejectsWritableTLSIdentityBeforeMutation(t *testing.T) {
	fake, logPath, statePath := writeRolloutAvailabilityKubectl(t)
	command := exec.Command("bash", "run-kubebrain-rollout-availability.sh")
	command.Env = append(os.Environ(),
		"KUBECTL_BIN="+fake,
		"FAKE_KUBECTL_LOG="+logPath,
		"FAKE_KUBECTL_STATE="+statePath,
		"FAKE_TLS_STATE=true",
		"FAKE_TLS_WRITABLE_MOUNT=true",
		"ALLOW_MUTATING_KUBEBRAIN_ROLLOUT=true",
	)
	output, err := command.CombinedOutput()
	require.Error(t, err)
	require.Contains(t, string(output), "rollout drain contract mismatch")
	log := readOptionalFile(t, logPath)
	require.NotContains(t, log, " run ")
	require.NotContains(t, log, " patch ")
}

func TestRolloutAvailabilityRunnerRejectsRootTLSProbeContextBeforeMutation(t *testing.T) {
	fake, logPath, statePath := writeRolloutAvailabilityKubectl(t)
	command := exec.Command("bash", "run-kubebrain-rollout-availability.sh")
	command.Env = append(os.Environ(),
		"KUBECTL_BIN="+fake,
		"FAKE_KUBECTL_LOG="+logPath,
		"FAKE_KUBECTL_STATE="+statePath,
		"FAKE_TLS_STATE=true",
		"FAKE_TLS_ROOT_CONTEXT=true",
		"ALLOW_MUTATING_KUBEBRAIN_ROLLOUT=true",
	)
	output, err := command.CombinedOutput()
	require.Error(t, err)
	require.Contains(t, string(output), "rollout drain contract mismatch")
	log := readOptionalFile(t, logPath)
	require.NotContains(t, log, " run ")
	require.NotContains(t, log, " patch ")
}

func TestRolloutAvailabilityRunnerRejectsRootPlaintextProbeContextBeforeMutation(t *testing.T) {
	fake, logPath, statePath := writeRolloutAvailabilityKubectl(t)
	command := exec.Command("bash", "run-kubebrain-rollout-availability.sh")
	command.Env = append(os.Environ(),
		"KUBECTL_BIN="+fake,
		"FAKE_KUBECTL_LOG="+logPath,
		"FAKE_KUBECTL_STATE="+statePath,
		"FAKE_ROOT_POD_CONTEXT=true",
		"ALLOW_MUTATING_KUBEBRAIN_ROLLOUT=true",
	)
	output, err := command.CombinedOutput()
	require.Error(t, err)
	require.Contains(t, string(output), "probe security context")
	log := readOptionalFile(t, logPath)
	require.NotContains(t, log, " run ")
	require.NotContains(t, log, " patch ")
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
	require.Contains(t, log, "--request-timeout=10s -n kubebrain-system patch statefulset/kubebrain --type=json -p ")
	require.Contains(t, log, `"value":"`+target+`"`)
	require.NotContains(t, log, " rollout restart ")
	for ordinal := 0; ordinal < 3; ordinal++ {
		require.Contains(t, log, " get pod kubebrain-"+string(rune('0'+ordinal))+" -o json")
	}
}

func TestRolloutAvailabilityRunnerAddsMissingRestartAnnotations(t *testing.T) {
	fake, logPath, statePath := writeRolloutAvailabilityKubectl(t)
	command := exec.Command("bash", "run-kubebrain-rollout-availability.sh")
	command.Env = append(os.Environ(),
		"KUBECTL_BIN="+fake, "FAKE_KUBECTL_LOG="+logPath, "FAKE_KUBECTL_STATE="+statePath,
		"ALLOW_MUTATING_KUBEBRAIN_ROLLOUT=true", "PROBE_ITERATIONS=3", "FAKE_NO_TEMPLATE_ANNOTATIONS=true",
	)
	output, err := command.CombinedOutput()
	require.NoError(t, err, string(output))
	log := readOptionalFile(t, logPath)
	require.Contains(t, log, `"path":"/spec/template/metadata/annotations"`)
	require.NotContains(t, log, " rollout restart ")
}

func TestRolloutAvailabilityRunnerUsesExplicitImmutableProbeImage(t *testing.T) {
	fake, logPath, statePath := writeRolloutAvailabilityKubectl(t)
	probe := "registry.example/rollout-probe@sha256:" + strings.Repeat("c", 64)
	command := exec.Command("bash", "run-kubebrain-rollout-availability.sh")
	command.Env = append(os.Environ(),
		"KUBECTL_BIN="+fake, "FAKE_KUBECTL_LOG="+logPath, "FAKE_KUBECTL_STATE="+statePath,
		"ALLOW_MUTATING_KUBEBRAIN_ROLLOUT=true", "PROBE_ITERATIONS=3", "PROBE_IMAGE="+probe,
	)
	output, err := command.CombinedOutput()
	require.NoError(t, err, string(output))
	require.Contains(t, string(output), "probe_image="+probe)
	log := readOptionalFile(t, logPath)
	require.Contains(t, log, " run kubebrain-rollout-availability-probe --image="+probe+" ")
}

func TestRolloutAvailabilityRunnerFencesStatefulSetUIDBeforeCandidateMutation(t *testing.T) {
	fake, logPath, statePath := writeRolloutAvailabilityKubectl(t)
	target := "registry.example/kubebrain@sha256:" + strings.Repeat("4", 64)
	command := exec.Command("bash", "run-kubebrain-rollout-availability.sh")
	command.Env = append(os.Environ(),
		"KUBECTL_BIN="+fake, "FAKE_KUBECTL_LOG="+logPath, "FAKE_KUBECTL_STATE="+statePath,
		"ALLOW_MUTATING_KUBEBRAIN_ROLLOUT=true", "PROBE_ITERATIONS=3", "TARGET_IMAGE="+target,
		"TARGET_RUNTIME_DIGESTS=sha256:"+strings.Repeat("4", 64),
		"FAKE_PATCH_UID_DRIFT=true",
	)
	output, err := command.CombinedOutput()
	require.Error(t, err)
	require.Contains(t, string(output), "candidate rollout failed; restoring original image kubebrain:test")
	require.Contains(t, string(output), "candidate image mutation was not observed; original StatefulSet spec remains")
	require.NotContains(t, string(output), "CRITICAL:")
	require.NotContains(t, string(output), "KubeBrain rollout availability gate passed")
	require.NoFileExists(t, statePath, "failed UID test must not mutate the replacement StatefulSet")
	log := readOptionalFile(t, logPath)
	require.Contains(t, log, `"path":"/metadata/uid","value":"statefulset-uid"`)
	require.Contains(t, log, `"path":"/metadata/resourceVersion","value":"resource-version-old"`)
	require.Contains(t, log, `"path":"/spec/template/spec/containers/0/name","value":"kubebrain"`)
	require.Contains(t, log, `"path":"/spec/template/spec/containers/0/image","value":"kubebrain:test"`)
	require.Equal(t, 1, strings.Count(log, " patch statefulset/kubebrain --type=json -p "))
}

func TestRolloutAvailabilityRunnerFencesStatefulSetResourceVersionBeforeCandidateMutation(t *testing.T) {
	fake, logPath, statePath := writeRolloutAvailabilityKubectl(t)
	target := "registry.example/kubebrain@sha256:" + strings.Repeat("3", 64)
	command := exec.Command("bash", "run-kubebrain-rollout-availability.sh")
	command.Env = append(os.Environ(),
		"KUBECTL_BIN="+fake, "FAKE_KUBECTL_LOG="+logPath, "FAKE_KUBECTL_STATE="+statePath,
		"ALLOW_MUTATING_KUBEBRAIN_ROLLOUT=true", "PROBE_ITERATIONS=3", "TARGET_IMAGE="+target,
		"TARGET_RUNTIME_DIGESTS=sha256:"+strings.Repeat("3", 64),
		"FAKE_PATCH_RESOURCE_VERSION_DRIFT=true",
	)
	output, err := command.CombinedOutput()
	require.Error(t, err)
	require.Contains(t, string(output), "candidate image mutation was not observed; original StatefulSet spec remains")
	require.NotContains(t, string(output), "CRITICAL:")
	require.NotContains(t, string(output), "KubeBrain rollout availability gate passed")
	require.NoFileExists(t, statePath)
	log := readOptionalFile(t, logPath)
	require.Contains(t, log, `"path":"/metadata/resourceVersion","value":"resource-version-old"`)
	require.Equal(t, 1, strings.Count(log, " patch statefulset/kubebrain --type=json -p "))
}

func TestRolloutAvailabilityRunnerRefusesToOverwriteConcurrentSpecBeforeRollback(t *testing.T) {
	fake, logPath, statePath := writeRolloutAvailabilityKubectl(t)
	target := "registry.example/kubebrain@sha256:" + strings.Repeat("2", 64)
	command := exec.Command("bash", "run-kubebrain-rollout-availability.sh")
	command.Env = append(os.Environ(),
		"KUBECTL_BIN="+fake, "FAKE_KUBECTL_LOG="+logPath, "FAKE_KUBECTL_STATE="+statePath,
		"ALLOW_MUTATING_KUBEBRAIN_ROLLOUT=true", "PROBE_ITERATIONS=3", "TARGET_IMAGE="+target,
		"TARGET_RUNTIME_DIGESTS=sha256:"+strings.Repeat("2", 64),
		"FAKE_ROLLBACK_SPEC_DRIFT=true",
	)
	output, err := command.CombinedOutput()
	require.Error(t, err)
	require.Contains(t, string(output), "CRITICAL: candidate state drifted before rollback; refusing to overwrite concurrent StatefulSet changes")
	require.NotContains(t, string(output), "KubeBrain rollout availability gate passed")
	log := readOptionalFile(t, logPath)
	require.Equal(t, 1, strings.Count(log, " patch statefulset/kubebrain --type=json -p "))
	require.FileExists(t, statePath, "cleanup must not overwrite the concurrent candidate spec")
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
	require.Contains(t, log, `"value":"`+target+`"`)
	require.GreaterOrEqual(t, strings.Count(log, " patch statefulset/kubebrain --type=json -p "), 2)
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
	require.Contains(t, log, `"value":"`+target+`"`)
	require.GreaterOrEqual(t, strings.Count(log, " patch statefulset/kubebrain --type=json -p "), 2)
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
	require.GreaterOrEqual(t, strings.Count(log, " patch statefulset/kubebrain --type=json -p "), 2)
}

func TestRolloutAvailabilityRunnerRejectsRuntimeDigestSuffixSpoof(t *testing.T) {
	fake, logPath, statePath := writeRolloutAvailabilityKubectl(t)
	target := "registry.example/kubebrain@sha256:" + strings.Repeat("8", 64)
	command := exec.Command("bash", "run-kubebrain-rollout-availability.sh")
	command.Env = append(os.Environ(),
		"KUBECTL_BIN="+fake, "FAKE_KUBECTL_LOG="+logPath, "FAKE_KUBECTL_STATE="+statePath,
		"ALLOW_MUTATING_KUBEBRAIN_ROLLOUT=true", "PROBE_ITERATIONS=3", "TARGET_IMAGE="+target,
		"TARGET_RUNTIME_DIGESTS=sha256:"+strings.Repeat("8", 64),
		"FAKE_RUNTIME_DIGEST_SUFFIX_SPOOF=true",
	)
	output, err := command.CombinedOutput()
	require.Error(t, err)
	require.Contains(t, string(output), "candidate Pod runtime release mismatch: kubebrain-0")
	require.Contains(t, string(output), "candidate rollout failed; restoring original image kubebrain:test")
	require.NotContains(t, string(output), "KubeBrain rollout availability gate passed")
	log := readOptionalFile(t, logPath)
	require.GreaterOrEqual(t, strings.Count(log, " patch statefulset/kubebrain --type=json -p "), 2)
}

func TestRolloutAvailabilityRunnerRejectsRestartedCandidateContainer(t *testing.T) {
	fake, logPath, statePath := writeRolloutAvailabilityKubectl(t)
	target := "registry.example/kubebrain@sha256:" + strings.Repeat("6", 64)
	command := exec.Command("bash", "run-kubebrain-rollout-availability.sh")
	command.Env = append(os.Environ(),
		"KUBECTL_BIN="+fake, "FAKE_KUBECTL_LOG="+logPath, "FAKE_KUBECTL_STATE="+statePath,
		"ALLOW_MUTATING_KUBEBRAIN_ROLLOUT=true", "PROBE_ITERATIONS=3", "TARGET_IMAGE="+target,
		"TARGET_RUNTIME_DIGESTS=sha256:"+strings.Repeat("6", 64),
		"FAKE_CANDIDATE_RESTART_COUNT=1",
	)
	output, err := command.CombinedOutput()
	require.Error(t, err)
	require.Contains(t, string(output), "candidate Pod runtime release mismatch: kubebrain-0")
	require.Contains(t, string(output), "candidate rollout failed; restoring original image kubebrain:test")
	require.NotContains(t, string(output), "KubeBrain rollout availability gate passed")
	log := readOptionalFile(t, logPath)
	require.GreaterOrEqual(t, strings.Count(log, " patch statefulset/kubebrain --type=json -p "), 2)
}

func TestRolloutAvailabilityRunnerRejectsCandidatePodReadyConditionDrift(t *testing.T) {
	fake, logPath, statePath := writeRolloutAvailabilityKubectl(t)
	target := "registry.example/kubebrain@sha256:" + strings.Repeat("5", 64)
	command := exec.Command("bash", "run-kubebrain-rollout-availability.sh")
	command.Env = append(os.Environ(),
		"KUBECTL_BIN="+fake, "FAKE_KUBECTL_LOG="+logPath, "FAKE_KUBECTL_STATE="+statePath,
		"ALLOW_MUTATING_KUBEBRAIN_ROLLOUT=true", "PROBE_ITERATIONS=3", "TARGET_IMAGE="+target,
		"TARGET_RUNTIME_DIGESTS=sha256:"+strings.Repeat("5", 64),
		"FAKE_CANDIDATE_POD_READY=false",
	)
	output, err := command.CombinedOutput()
	require.Error(t, err)
	require.Contains(t, string(output), "candidate Pod runtime release mismatch: kubebrain-0")
	require.Contains(t, string(output), "candidate rollout failed; restoring original image kubebrain:test")
	require.NotContains(t, string(output), "KubeBrain rollout availability gate passed")
	log := readOptionalFile(t, logPath)
	require.GreaterOrEqual(t, strings.Count(log, " patch statefulset/kubebrain --type=json -p "), 2)
}

func TestRolloutAvailabilityRunnerAcceptsRuntimeDigestIdentityForms(t *testing.T) {
	for _, style := range []string{"bare", "pullable"} {
		t.Run(style, func(t *testing.T) {
			fake, logPath, statePath := writeRolloutAvailabilityKubectl(t)
			target := "registry.example/kubebrain@sha256:" + strings.Repeat("7", 64)
			command := exec.Command("bash", "run-kubebrain-rollout-availability.sh")
			command.Env = append(os.Environ(),
				"KUBECTL_BIN="+fake, "FAKE_KUBECTL_LOG="+logPath, "FAKE_KUBECTL_STATE="+statePath,
				"ALLOW_MUTATING_KUBEBRAIN_ROLLOUT=true", "PROBE_ITERATIONS=3", "TARGET_IMAGE="+target,
				"TARGET_RUNTIME_DIGESTS=sha256:"+strings.Repeat("7", 64),
				"FAKE_RUNTIME_IMAGE_ID_STYLE="+style,
			)
			output, err := command.CombinedOutput()
			require.NoError(t, err, string(output))
			require.Contains(t, string(output), "KubeBrain rollout availability gate passed")
		})
	}
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
	require.GreaterOrEqual(t, strings.Count(log, " patch statefulset/kubebrain --type=json -p "), 2)
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
			completionPath := filepath.Join(filepath.Dir(statePath), "response-completed")
			base := append(os.Environ(), "KUBECTL_BIN="+fake, "FAKE_KUBECTL_LOG="+logPath, "FAKE_KUBECTL_STATE="+statePath, "ALLOW_MUTATING_KUBEBRAIN_ROLLOUT=true", "PROBE_ITERATIONS=3", "FAKE_RUNTIME_RESPONSE_TARGET="+target, "FAKE_RUNTIME_RESPONSE_COMPLETED="+completionPath)
			command := exec.Command("bash", "run-kubebrain-rollout-availability.sh")
			command.Env = append(base, "FAKE_RUNTIME_RESPONSE_BYTES=67108864")
			output, err := command.CombinedOutput()
			require.Error(t, err)
			require.Contains(t, string(output), "runtime evidence exceeds 1048576 bytes")
			require.NoFileExists(t, completionPath,
				"the bounded consumer must stop an oversized producer before it emits the full response")
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
if [[ "${FAKE_KUBECTL_HANG_TARGET:-}" == evidence && " $* " == *" get statefulset kubebrain -o json "* ]]; then
  sleep 30
fi
if [[ "${FAKE_KUBECTL_HANG_TARGET:-}" == mutation && " $* " == *" run kubebrain-rollout-availability-probe "* ]]; then
  sleep 30
fi
if [[ "${FAKE_KUBECTL_HANG_TARGET:-}" == ready && " $* " == *" wait --for=condition=Ready "* ]]; then
  sleep 30
fi
if [[ "${FAKE_KUBECTL_HANG_TARGET:-}" == rollout && " $* " == *" rollout status statefulset/kubebrain "* ]]; then
  sleep 30
fi
if [[ "${FAKE_KUBECTL_HANG_TARGET:-}" == phase && " $* " == *" wait --for=jsonpath={.status.phase}=Succeeded "* ]]; then
  sleep 30
fi
if [[ "${FAKE_KUBECTL_HANG_TARGET:-}" == phase-evidence && " $* " == *" get pod kubebrain-rollout-availability-probe -o jsonpath={.status.phase} "* ]]; then
  sleep 30
fi
if [[ "${FAKE_KUBECTL_HANG_TARGET:-}" == start && " $* " == *" logs kubebrain-rollout-availability-probe "* ]]; then
  sleep 30
fi
if [[ " $* " == *" get service kubebrain-peer -o json "* ]]; then
  publish_not_ready=true
  [[ "${FAKE_HEADLESS_PUBLISH_NOT_READY:-true}" == true ]] || publish_not_ready=false
  cluster_ip="${FAKE_HEADLESS_CLUSTER_IP:-None}"
  selector_value=kubebrain
  [[ "${FAKE_HEADLESS_SELECTOR_MISMATCH:-false}" != true ]] || selector_value=other
  resource_version=headless-service-rv
  [[ "${FAKE_HEADLESS_IDENTITY_DRIFT:-false}" != true || ! -e "$FAKE_KUBECTL_STATE" ]] || resource_version=headless-service-rv-drifted
  jq -cn --arg clusterIP "$cluster_ip" --arg selector "$selector_value" --arg resourceVersion "$resource_version" --argjson publishNotReady "$publish_not_ready" '{
    metadata:{name:"kubebrain-peer",uid:"headless-service-uid",resourceVersion:$resourceVersion},
    spec:{clusterIP:$clusterIP,publishNotReadyAddresses:$publishNotReady,selector:{app:$selector}}
  }'
elif [[ " $* " == *" get statefulset kubebrain -o json "* ]]; then
  revision=revision-old
  [[ -e "$FAKE_KUBECTL_STATE" ]] && revision=revision-new
  prestop='["/bin/sh","-c","sleep 25 && curl --fail --silent --show-error --max-time 10 --request POST http://127.0.0.1:8080/drain"]'
  [[ "${FAKE_DRAIN_FIRST_PRESTOP:-false}" != true ]] || prestop='["/bin/sh","-c","curl --fail --silent --show-error --max-time 10 --request POST http://127.0.0.1:8080/drain && sleep 5"]'
  [[ "${FAKE_SHORT_PROPAGATION_PRESTOP:-false}" != true ]] || prestop='["/bin/sh","-c","sleep 10 && curl --fail --silent --show-error --max-time 10 --request POST http://127.0.0.1:8080/drain && sleep 5"]'
  [[ "${FAKE_DRAIN_AFTER_PROPAGATION_PRESTOP:-false}" != true ]] || prestop='["/bin/sh","-c","sleep 25 && curl --fail --silent --show-error --max-time 10 --request POST http://127.0.0.1:8080/drain"]'
  [[ "${FAKE_BAD_PRESTOP:-false}" == true ]] && prestop='["/bin/sleep","5"]'
  args='["--leader-retry-period=500ms","--pd-addrs=pd-0:2379,pd-1:2379,pd-2:2379"]'
  volume_mounts='[]'
  volumes='[]'
  pod_security_context='{"runAsNonRoot":true,"runAsUser":65532,"runAsGroup":65532,"fsGroup":65532}'
  if [[ "${FAKE_TLS_STATE:-false}" == true ]]; then
    prestop='["/bin/sh","-c","sleep 25 && curl --insecure --fail --silent --show-error --max-time 10 --request POST https://127.0.0.1:8080/drain"]'
    args='["--leader-retry-period=500ms","--pd-addrs=pd-0:2379,pd-1:2379,pd-2:2379","--allow-insecure=false","--cert-file=/etc/kubebrain/client-tls/tls.crt","--key-file=/etc/kubebrain/client-tls/tls.key","--trusted-ca-file=/etc/kubebrain/client-tls/ca.crt","--tls-server-name=kubebrain-client.kubebrain-system.svc","--client-cert-auth=true"]'
    volume_mounts='[{"name":"client-tls","mountPath":"/etc/kubebrain/client-tls","readOnly":true}]'
    volumes='[{"name":"client-tls","secret":{"secretName":"kubebrain-client-tls","defaultMode":256}}]'
    pod_security_context='{"runAsNonRoot":true,"runAsUser":65532,"runAsGroup":65532,"fsGroup":65532}'
    [[ "${FAKE_TLS_WRITABLE_MOUNT:-false}" != true ]] || volume_mounts='[{"name":"client-tls","mountPath":"/etc/kubebrain/client-tls","readOnly":false}]'
    [[ "${FAKE_TLS_ROOT_CONTEXT:-false}" != true ]] || pod_security_context='{"runAsNonRoot":false,"runAsUser":0,"runAsGroup":0,"fsGroup":0}'
  fi
  [[ "${FAKE_ROOT_POD_CONTEXT:-false}" != true ]] || pod_security_context='{"runAsNonRoot":false,"runAsUser":0,"runAsGroup":0,"fsGroup":0}'
  runtime_image=kubebrain:test
  [[ -e "$FAKE_KUBECTL_STATE" && -n "${TARGET_IMAGE:-}" ]] && runtime_image="$TARGET_IMAGE"
  statefulset_uid="${FAKE_STATEFULSET_UID:-statefulset-uid}"
  [[ "${FAKE_STATEFULSET_UID_DRIFT:-false}" != true || ! -e "$FAKE_KUBECTL_STATE" ]] || statefulset_uid=statefulset-replacement-uid
  resource_version=resource-version-old
  [[ ! -e "$FAKE_KUBECTL_STATE" ]] || resource_version=resource-version-new
  spec_replicas=3
  [[ "${FAKE_ROLLBACK_SPEC_DRIFT:-false}" != true || ! -e "$FAKE_KUBECTL_STATE" ]] || spec_replicas=4
  termination_grace=45
  [[ "${FAKE_SHORT_TERMINATION_GRACE:-false}" != true ]] || termination_grace=30
  restart_annotation=restart-old
  if [[ -e "$FAKE_KUBECTL_STATE" && -z "${TARGET_IMAGE:-}" ]]; then
    restart_annotation="$(<"$FAKE_KUBECTL_STATE")"
  fi
  payload="$(jq -cn --arg uid "$statefulset_uid" --arg resourceVersion "$resource_version" --arg revision "$revision" --arg image "$runtime_image" --arg restart "$restart_annotation" --argjson prestop "$prestop" --argjson args "$args" --argjson volume_mounts "$volume_mounts" --argjson volumes "$volumes" --argjson pod_security_context "$pod_security_context" --argjson replicas "$spec_replicas" --argjson termination_grace "$termination_grace" '{
    metadata:{uid:$uid,resourceVersion:$resourceVersion},
    spec:{replicas:$replicas,serviceName:"kubebrain-peer",template:{metadata:{labels:{app:"kubebrain"},annotations:{"kubectl.kubernetes.io/restartedAt":$restart}},spec:{terminationGracePeriodSeconds:$termination_grace,securityContext:$pod_security_context,containers:[{name:"kubebrain",image:$image,args:$args,volumeMounts:$volume_mounts,lifecycle:{preStop:{exec:{command:$prestop}}}}],volumes:$volumes}}},
    status:{readyReplicas:3,currentRevision:$revision,updateRevision:$revision}}
  ')"
  if [[ "${FAKE_NO_TEMPLATE_ANNOTATIONS:-false}" == true && ! -e "$FAKE_KUBECTL_STATE" ]]; then
    payload="$(jq -c 'del(.spec.template.metadata.annotations)' <<<"$payload")"
  fi
  if [[ "${FAKE_NO_HEADLESS_SERVICE:-false}" == true ]]; then
    payload="$(jq -c 'del(.spec.serviceName)' <<<"$payload")"
  fi
  printf '%s' "$payload"
  if [[ "${FAKE_RUNTIME_RESPONSE_TARGET:-}" == statefulset ]]; then
    head -c "$((FAKE_RUNTIME_RESPONSE_BYTES-${#payload}))" /dev/zero | tr '\0' ' '
    [[ -z "${FAKE_RUNTIME_RESPONSE_COMPLETED:-}" ]] || : >"$FAKE_RUNTIME_RESPONSE_COMPLETED"
  fi
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
  runtime_image_id="containerd://$runtime_digest"
  runtime_restart_count=0
  runtime_ready=True
  [[ ! -e "$FAKE_KUBECTL_STATE" ]] || runtime_restart_count="${FAKE_CANDIDATE_RESTART_COUNT:-0}"
  [[ ! -e "$FAKE_KUBECTL_STATE" ]] || runtime_ready="${FAKE_CANDIDATE_POD_READY:-True}"
  [[ "${FAKE_RUNTIME_IMAGE_ID_STYLE:-}" != bare || ! -e "$FAKE_KUBECTL_STATE" ]] || runtime_image_id="$runtime_digest"
  [[ "${FAKE_RUNTIME_IMAGE_ID_STYLE:-}" != pullable || ! -e "$FAKE_KUBECTL_STATE" ]] || runtime_image_id="registry.example/kubebrain@$runtime_digest"
  [[ "${FAKE_RUNTIME_DIGEST_SUFFIX_SPOOF:-false}" != true || ! -e "$FAKE_KUBECTL_STATE" ]] || runtime_image_id="untrusted-prefix$runtime_digest"
  jq -cn --arg name "$ordinal" --arg image "$runtime_image" --arg imageID "$runtime_image_id" --arg revision "$runtime_revision" --argjson restartCount "$runtime_restart_count" --arg ready "$runtime_ready" '{
    metadata:{name:$name,labels:{"controller-revision-hash":$revision}},
    spec:{containers:[{name:"kubebrain",image:$image}]},
    status:{phase:"Running",conditions:[{type:"Ready",status:$ready}],containerStatuses:[{name:"kubebrain",ready:true,restartCount:$restartCount,imageID:$imageID}]}}
  '
elif [[ " $* " == *" get pod kubebrain-rollout-availability-probe "* ]]; then
  exit 1
elif [[ " $* " == *" patch statefulset/kubebrain --type=json -p "* ]]; then
  patch_payload=""
  previous=""
  for argument in "$@"; do
    [[ "$previous" != -p ]] || patch_payload="$argument"
    previous="$argument"
  done
  expected_uid="$(jq -r '.[0].value // ""' <<<"${patch_payload:-[]}")"
  expected_resource_version="$(jq -r '.[1].value // ""' <<<"${patch_payload:-[]}")"
  expected_name="$(jq -r '.[2].value // ""' <<<"${patch_payload:-[]}")"
  expected_image="$(jq -r '.[3].value // ""' <<<"${patch_payload:-[]}")"
  new_image="$(jq -r '.[-1].value // ""' <<<"${patch_payload:-[]}")"
  current_uid=statefulset-uid
  [[ "${FAKE_PATCH_UID_DRIFT:-false}" != true ]] || current_uid=statefulset-replacement-uid
  current_resource_version=resource-version-old
  [[ ! -e "$FAKE_KUBECTL_STATE" ]] || current_resource_version=resource-version-new
  [[ "${FAKE_PATCH_RESOURCE_VERSION_DRIFT:-false}" != true ]] || current_resource_version=resource-version-concurrent
  current_image=kubebrain:test
  [[ ! -e "$FAKE_KUBECTL_STATE" || -z "${TARGET_IMAGE:-}" ]] || current_image="$TARGET_IMAGE"
  patch_path="$(jq -r '.[-1].path // ""' <<<"${patch_payload:-[]}")"
  if [[ "$patch_path" == /spec/template/metadata/annotations ]]; then
    restart_value="$(jq -r '.[-1].value["kubectl.kubernetes.io/restartedAt"] // ""' <<<"${patch_payload:-[]}")"
    [[ "$expected_uid" == "$current_uid" && "$expected_resource_version" == "$current_resource_version" && -n "$restart_value" ]] || exit 1
    printf '%s' "$restart_value" >"$FAKE_KUBECTL_STATE"
  elif [[ "$patch_path" == /spec/template/metadata/annotations/kubectl.kubernetes.io~1restartedAt ]]; then
    [[ "$expected_uid" == "$current_uid" && "$expected_resource_version" == "$current_resource_version" && -n "$new_image" ]] || exit 1
    printf '%s' "$new_image" >"$FAKE_KUBECTL_STATE"
  elif [[ "$expected_uid" == "$current_uid" && "$expected_resource_version" == "$current_resource_version" &&
    "$expected_name" == kubebrain && "$expected_image" == "$current_image" && -n "$new_image" ]] &&
    [[ "$new_image" == kubebrain:test && "${FAKE_ROLLBACK_IDENTITY_DRIFT:-false}" != true ]]; then
    rm -f -- "$FAKE_KUBECTL_STATE"
    [[ -z "${FAKE_ROLLBACK_MARKER:-}" ]] || : >"$FAKE_ROLLBACK_MARKER"
  elif [[ "$expected_uid" == "$current_uid" && "$expected_resource_version" == "$current_resource_version" &&
    "$expected_name" == kubebrain && "$expected_image" == "$current_image" && -n "$new_image" ]]; then
    : >"$FAKE_KUBECTL_STATE"
  else
    exit 1
  fi
elif [[ " $* " == *" rollout status statefulset/kubebrain "* && "${FAKE_ROLLOUT_FAIL:-false}" == true ]]; then
  exit 1
elif [[ " $* " == *" delete pod kubebrain-rollout-availability-probe "* && "${FAKE_DELETE_FAIL:-false}" == true ]]; then
  exit 1
elif [[ " $* " == *" wait --for=jsonpath={.status.phase}=Succeeded "* ]]; then
  if [[ "${FAKE_PROBE_FAILED:-false}" == true ]]; then exit 1; fi
  if [[ "${FAKE_PHASE_RESPONSE:-false}" == true && ! -e "$FAKE_PHASE_STATE" ]]; then : >"$FAKE_PHASE_STATE"; exit 1; fi
elif [[ " $* " == *" logs kubebrain-rollout-availability-probe "* ]]; then
  payload=$'PROBE_STARTED\nPROBE_SUMMARY ok=3 fail=0 total=3 watch=3 direct_watch=3x3 lease=alive direct_lease=alive direct_lease_restarts=3 direct_endpoints=3 range_stream=17 snapshot=2 stream_retries=4 stream_partial_retries=1 max_latency_ms=123 max_direct_latency_ms=456 max_tso_latency_ms=12 max_region_latency_ms=34\n'
  printf '%s' "$payload"
  if [[ "${FAKE_RUNTIME_RESPONSE_TARGET:-}" == probe-log ]]; then
    head -c "$((FAKE_RUNTIME_RESPONSE_BYTES-${#payload}))" /dev/zero | tr '\0' ' '
    [[ -z "${FAKE_RUNTIME_RESPONSE_COMPLETED:-}" ]] || : >"$FAKE_RUNTIME_RESPONSE_COMPLETED"
  fi
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
