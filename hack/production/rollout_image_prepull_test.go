package production_test

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func runPrepullRollout(t *testing.T, extra ...string) (string, string, error) {
	t.Helper()
	fake, logPath, state := writeRolloutAvailabilityKubectl(t)
	ctx, cancel := context.WithTimeout(t.Context(), 40*time.Second)
	defer cancel()
	command := exec.CommandContext(ctx, "bash", "run-kubebrain-rollout-availability.sh")
	for _, setting := range extra {
		if setting == "FAKE_CLOSE_RUNNER_STDOUT=true" {
			command = exec.CommandContext(ctx, "bash", "-c", "exec 1>&-; exec bash run-kubebrain-rollout-availability.sh")
		}
	}
	command.Env = append(os.Environ(), "KUBECTL_BIN="+fake, "FAKE_KUBECTL_LOG="+logPath, "FAKE_KUBECTL_STATE="+state,
		"ALLOW_MUTATING_KUBEBRAIN_ROLLOUT=true", "TARGET_IMAGE=registry.example/kubebrain@sha256:"+strings.Repeat("a", 64),
		"TARGET_RUNTIME_DIGESTS=sha256:"+strings.Repeat("a", 64))
	command.Env = append(command.Env, extra...)
	output, err := command.CombinedOutput()
	require.NoError(t, ctx.Err(), "runner subprocess exceeded its test deadline: %s", output)
	return string(output), readOptionalFile(t, logPath), err
}

func TestRolloutImagePrepullOrdersPreparationVerificationAndCleanup(t *testing.T) {
	output, log, err := runPrepullRollout(t)
	require.NoError(t, err, output)
	prepare := strings.Index(log, "image-prepull --mode=prepare ")
	fixture := strings.Index(log, " run kubebrain-rollout-availability-probe-cleanup ")
	probe := strings.Index(log, " run kubebrain-rollout-availability-probe ")
	verify := strings.Index(log, "image-prepull --mode=verify ")
	patch := strings.Index(log, " patch statefulset/kubebrain ")
	cleanup := strings.Index(log, "image-prepull --mode=recover-cleanup ")
	require.GreaterOrEqual(t, prepare, 0)
	require.Greater(t, fixture, prepare, "cold pulls must finish before any fixture work")
	require.Greater(t, probe, fixture)
	require.Greater(t, verify, probe)
	require.Greater(t, patch, verify)
	require.Contains(t, log[verify:patch], "get statefulset kubebrain -o json", "fresh source fence must follow helper verification")
	require.Greater(t, cleanup, strings.LastIndex(log, " uid-delete "))
	require.Equal(t, 1, strings.Count(log, "image-prepull --mode=recover-cleanup "))
	require.Contains(t, log, "--context=prepull-explicit-context")
	require.Contains(t, log, "--namespace-uid=test-namespace-uid")
	require.Contains(t, log, "--statefulset-uid=statefulset-uid")
	require.Contains(t, log, "--confirm-create-isolated-jobs")
	require.Less(t, strings.Index(output, "PREPULL_CLEANUP_CONFIRMED"), strings.Index(output, "KubeBrain rollout availability gate passed"))
	directories, err := filepath.Glob(filepath.Join(os.Getenv("IMAGE_PREPULL_RECEIPT_DIRECTORY"), "prepull.*"))
	require.NoError(t, err)
	require.Len(t, directories, 1, "the attempt directory must remain for recovery/audit")
	info, err := os.Stat(directories[0])
	require.NoError(t, err)
	require.Equal(t, os.FileMode(0700), info.Mode().Perm())
}

func TestRolloutImagePrepullFailuresNeverBypassPreparation(t *testing.T) {
	for _, tc := range []struct {
		name, env, want string
		probe, patch    bool
	}{
		{"prepare failure", "FAKE_PREPULL_FAIL_MODE=prepare", "isolated candidate image preparation failed", false, false},
		{"false ready marker", "FAKE_PREPULL_PREPARE_MARKER=NOT_READY", "exact success marker", false, false},
		{"verify failure", "FAKE_PREPULL_FAIL_MODE=verify", "refusing StatefulSet mutation", true, false},
		{"false verified marker", "FAKE_PREPULL_VERIFY_MARKER=NOT_VERIFIED", "exact success marker", true, false},
		{"source spec drift", "FAKE_PREPULL_SOURCE_DRIFT=true", "source StatefulSet drifted", true, false},
		{"cleanup failure", "FAKE_PREPULL_FAIL_MODE=recover-cleanup", "image-holder cleanup unconfirmed", true, true},
		{"false cleanup marker", "FAKE_PREPULL_CLEANUP_MARKER=NOT_CLEANED", "image-holder cleanup unconfirmed", true, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			output, log, err := runPrepullRollout(t, tc.env)
			require.Error(t, err, output)
			require.Contains(t, output, tc.want)
			require.NotContains(t, output, "KubeBrain rollout availability gate passed")
			require.Equal(t, tc.probe, strings.Contains(log, " run kubebrain-rollout-availability-probe "))
			require.Equal(t, tc.patch, strings.Contains(log, " patch statefulset/kubebrain "))
			require.Contains(t, log, "image-prepull --mode=recover-cleanup ")
			if tc.patch {
				require.Contains(t, output, "restoring original image")
				require.Equal(t, 2, strings.Count(log, " patch statefulset/kubebrain "))
				require.Greater(t, strings.LastIndex(log, "image-prepull --mode=recover-cleanup "), strings.LastIndex(log, " patch statefulset/kubebrain "))
			}
		})
	}
}

func TestRolloutImagePrepullAllowsStatusOnlyResourceVersionRefresh(t *testing.T) {
	output, log, err := runPrepullRollout(t, "FAKE_PREPULL_RV_REFRESH=true")
	require.NoError(t, err, output)
	require.Contains(t, log, `"path":"/metadata/resourceVersion","value":"resource-version-refreshed"`)
	require.Contains(t, output, "KubeBrain rollout availability gate passed")
}

func TestRolloutImagePrepullRejectsIncompleteScopeReleaseAndBudget(t *testing.T) {
	for _, tc := range []struct{ env, want string }{
		{"KUBECONFIG=", "requires KUBECONFIG"},
		{"KUBECONFIG=relative", "absolute regular 0600"},
		{"KUBECTL_CONTEXT=", "requires KUBECTL_CONTEXT"},
		{"IMAGE_PREPULL_NAMESPACE_UID=", "requires IMAGE_PREPULL_NAMESPACE_UID"},
		{"IMAGE_PREPULL_RECEIPT_DIRECTORY=", "requires IMAGE_PREPULL_RECEIPT_DIRECTORY"},
		{"IMAGE_PREPULL_INDEX_FILE=", "requires IMAGE_PREPULL_INDEX_FILE"},
		{"IMAGE_PREPULL_AMD64_DIGEST=", "requires IMAGE_PREPULL_AMD64_DIGEST"},
		{"IMAGE_PREPULL_ARM64_DIGEST=", "requires IMAGE_PREPULL_ARM64_DIGEST"},
		{"IMAGE_PREPULL_BIN=/missing", "absolute executable"},
		{"IMAGE_PREPULL_VERIFY_TIMEOUT=0s", "positive bounded Go duration"},
		{"IMAGE_PREPULL_PREPARE_TIMEOUT=3601s", "helper phase limit"},
		{"IMAGE_PREPULL_CLEANUP_TIMEOUT=301s", "helper phase limit"},
		{"FAKE_PREPULL_FAIL_MODE=verify-release", "release identity verification failed"},
		{"FAKE_PREPULL_RELEASE_IMAGE=wrong", "release evidence is malformed"},
		{"FAKE_PREPULL_RELEASE_DIGESTS=sha256:" + strings.Repeat("f", 64), "differs from the verified"},
		{"PROBE_COMPLETE_TIMEOUT=30000s", "24-hour holder limit"},
		{"PROBE_ITERATIONS=5390", "do not cover the full"},
	} {
		t.Run(tc.env, func(t *testing.T) {
			output, log, err := runPrepullRollout(t, tc.env, "PREFLIGHT_ONLY=true")
			require.Error(t, err, output)
			require.Contains(t, output, tc.want)
			require.Empty(t, log, "scope/release/budget checks must precede Kubernetes and receipt creation")
		})
	}
}

func TestRolloutImagePrepullBoundsHungPreparationAndStillCleans(t *testing.T) {
	start := time.Now()
	output, log, err := runPrepullRollout(t, "FAKE_PREPULL_HANG_MODE=prepare", "IMAGE_PREPULL_PREPARE_TIMEOUT=1s", "IMAGE_PREPULL_CLEANUP_TIMEOUT=1s")
	require.Error(t, err)
	require.Less(t, time.Since(start), 15*time.Second)
	require.Contains(t, output, "isolated candidate image preparation failed")
	require.Contains(t, log, "image-prepull --mode=recover-cleanup ")
	require.NotContains(t, log, " run kubebrain-rollout-availability-probe ")
	require.NotContains(t, log, " patch statefulset/kubebrain ")
}

func TestRolloutImagePrepullPublishesReceiptBeforeCreatingHolders(t *testing.T) {
	output, log, err := runPrepullRollout(t, "FAKE_CLOSE_RUNNER_STDOUT=true")
	require.Error(t, err)
	require.Contains(t, output, "Bad file descriptor")
	require.NotContains(t, log, "image-prepull --mode=prepare ", "receipt publication failure must not create holder Jobs")
	require.NotContains(t, log, "image-prepull --mode=recover-cleanup ", "no preparation attempt was started")
	require.NotContains(t, log, " run kubebrain-rollout-availability-probe ")
	require.NotContains(t, log, " patch statefulset/kubebrain ")
}

func TestRolloutImagePrepullExitContainment(t *testing.T) {
	data, err := os.ReadFile("run-kubebrain-rollout-availability.sh")
	require.NoError(t, err)
	source := string(data)
	start := strings.Index(source, "rollout_exit() {")
	end := strings.Index(source, "trap rollout_exit EXIT")
	require.GreaterOrEqual(t, start, 0)
	require.Greater(t, end, start)
	for _, tc := range []struct{ name, cleanup, prepull, original, want string }{
		{"explicit cleanup exit", "exit 17", "return 0", "0", "1"},
		{"errexit in cleanup", "false; echo MUST_NOT_CONTINUE", "return 0", "0", "1"},
		{"preserve original failure", "return 0", "return 0", "23", "23"},
		{"holder cleanup failure", "return 0", "return 1", "0", "1"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			command := exec.Command("bash", "-c", "set -euo pipefail\nstop_rollout_observer() { :; }\ncleanup() { "+tc.cleanup+"; }\n"+
				"image_prepull_cleanup() { echo HOLDER_CLEANUP_CALLED; "+tc.prepull+"; }\n"+
				source[start:end]+"trap rollout_exit EXIT\nexit "+tc.original)
			output, err := command.CombinedOutput()
			require.Error(t, err)
			require.Equal(t, "exit status "+tc.want, err.Error())
			require.Contains(t, string(output), "HOLDER_CLEANUP_CALLED")
			require.NotContains(t, string(output), "MUST_NOT_CONTINUE")
		})
	}
}

func TestRolloutImagePrepullReapsObserverInItsParentBeforeCleanup(t *testing.T) {
	data, err := os.ReadFile("run-kubebrain-rollout-availability.sh")
	require.NoError(t, err)
	source := string(data)
	stopStart, stopEnd := strings.Index(source, "stop_rollout_observer() {"), strings.Index(source, "check_rollout_probe_active() {")
	exitStart, exitEnd := strings.Index(source, "rollout_exit() {"), strings.Index(source, "trap rollout_exit EXIT")
	require.GreaterOrEqual(t, stopStart, 0)
	require.Greater(t, stopEnd, stopStart)
	require.Greater(t, exitStart, stopEnd)
	require.Greater(t, exitEnd, exitStart)
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	command := exec.CommandContext(ctx, "bash", "-c", "set -euo pipefail\n"+source[stopStart:stopEnd]+source[exitStart:exitEnd]+`
sleep 30 &
rollout_observer_pid=$!
observed_pid=$!
cleanup() {
  [[ -z "$rollout_observer_pid" ]] || echo OBSERVER_NOT_REAPED_IN_PARENT
  echo FIXTURE_CLEANUP
}
image_prepull_cleanup() {
  if kill -0 "$observed_pid" 2>/dev/null; then
    echo OBSERVER_STILL_LIVE
    kill -TERM "$observed_pid" 2>/dev/null || true
  fi
  wait "$observed_pid" 2>/dev/null || true
  echo HOLDER_CLEANUP
}
trap rollout_exit EXIT
exit 0
`)
	output, err := command.CombinedOutput()
	require.NoError(t, err, string(output))
	require.Equal(t, "FIXTURE_CLEANUP\nHOLDER_CLEANUP\n", string(output),
		"a subshell cannot wait for the parent's observer; terminate/reap before entering fixture cleanup")
}
