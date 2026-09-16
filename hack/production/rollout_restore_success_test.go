package production_test

import (
	"fmt"
	"os"
	"os/exec"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRolloutAvailabilityRunnerRestoreOriginalAfterSuccess(t *testing.T) {
	for _, tc := range []struct {
		name        string
		fail, drift bool
	}{
		{name: "success"},
		{name: "failure still rolls back", fail: true},
		{name: "restored runtime drift fails", drift: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fake, logPath, statePath := writeRolloutAvailabilityKubectl(t)
			command := exec.Command("bash", "run-kubebrain-rollout-availability.sh")
			command.Env = append(os.Environ(),
				"KUBECTL_BIN="+fake, "FAKE_KUBECTL_LOG="+logPath, "FAKE_KUBECTL_STATE="+statePath,
				"FAKE_TLS_STATE=true", fmt.Sprintf("FAKE_ROLLOUT_FAIL=%t", tc.fail),
				fmt.Sprintf("FAKE_ROLLBACK_RUNTIME_DRIFT=%t", tc.drift), "FAKE_ROLLBACK_MARKER="+statePath+"-rollback",
				"ALLOW_MUTATING_KUBEBRAIN_ROLLOUT=true", "PROBE_ITERATIONS=6000",
				"TARGET_IMAGE=registry.example/kubebrain@sha256:"+strings.Repeat("e", 64),
				"TARGET_RUNTIME_DIGESTS=sha256:"+strings.Repeat("e", 64),
				"ENABLE_TEMPORARY_1PC_EXPERIMENT=false", "ENABLE_TEMPORARY_ASYNC_COMMIT_EXPERIMENT=false",
				"RESTORE_ORIGINAL_AFTER_SUCCESS=true",
			)
			output, err := command.CombinedOutput()
			if tc.fail || tc.drift {
				require.Error(t, err, string(output))
			} else {
				require.NoError(t, err, string(output))
			}
			require.NoFileExists(t, statePath, "temporary candidate must restore with default 2PC too")
			if !tc.fail {
				require.Contains(t, string(output), "temporary candidate rollout completed; restoring original image")
			}
			if tc.drift {
				require.Contains(t, string(output), "CRITICAL: candidate rollback Pod runtime identity mismatch")
			}
			log := readOptionalFile(t, logPath)
			require.Equal(t, 2, strings.Count(log, " patch statefulset/kubebrain --type=json -p "))
			require.NotContains(t, log, "--experimental-tikv-enable-1pc")
			require.NotContains(t, log, "--experimental-tikv-enable-async-commit")
		})
	}
}

func TestRolloutAvailabilityRunnerRestoreOriginalRejectsUnsafeModes(t *testing.T) {
	for _, extra := range []string{
		"RESTORE_ORIGINAL_AFTER_SUCCESS=invalid", "TARGET_IMAGE=", "OBSERVE_ONLY=true",
		"HARD_FAILOVER=true", "ENABLE_HTTP_READINESS_MIGRATION=true", "ENABLE_GRPC_CONNECTION_AGING_MIGRATION=true",
	} {
		t.Run(extra, func(t *testing.T) {
			fake, logPath, _ := writeRolloutAvailabilityKubectl(t)
			command := exec.Command("bash", "run-kubebrain-rollout-availability.sh")
			command.Env = append(os.Environ(), "KUBECTL_BIN="+fake, "FAKE_KUBECTL_LOG="+logPath,
				"ALLOW_MUTATING_KUBEBRAIN_ROLLOUT=true", "RESTORE_ORIGINAL_AFTER_SUCCESS=true",
				"TARGET_IMAGE=registry.example/kubebrain@sha256:"+strings.Repeat("e", 64), extra)
			output, err := command.CombinedOutput()
			require.EqualError(t, err, "exit status 2", string(output))
			require.NoFileExists(t, logPath, "invalid restore request must not reach Kubernetes")
		})
	}
}
