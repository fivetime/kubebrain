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

func TestRolloutAvailabilityRunnerBudgetsSnapshotScaleInitialization(t *testing.T) {
	source, err := os.ReadFile("run-kubebrain-rollout-availability.sh")
	require.NoError(t, err)
	require.Contains(t, string(source), `PROBE_START_TIMEOUT="${PROBE_START_TIMEOUT:-90s}"`)
	require.Contains(t, string(source), "bounded 16 MiB Snapshot scale fixture")
}

func TestRolloutAvailabilityRunnerDurablyReceiptsFixtureBeforeScheduling(t *testing.T) {
	source, err := os.ReadFile("run-kubebrain-rollout-availability.sh")
	require.NoError(t, err)
	text := string(source)
	require.Contains(t, text, `schedulingGates:[{name:"kubebrain.io/fixture-owner-receipt"}]`)
	require.Contains(t, text, `format:"kubebrain.rollout-fixture-owner.v2"`)
	require.Contains(t, text, `receipt_json="$(jq -cnS`)
	require.Contains(t, text, `od -An -N8 -tx1 /dev/urandom`)
	require.Contains(t, text, `first_octet=$((16#${hex:0:2} & 0x7f))`)
	require.Contains(t, text, `immutable:true`)
	require.Contains(t, text, `fixture_owner_finalizer="kubebrain.io/rollout-fixture-cleanup"`)
	require.Contains(t, text, `pin_fixture_owner_receipt`)
	require.Contains(t, text, `--fixture-lease-ids="$fixture_lease_ids"`)
	require.Contains(t, text, `--resource=configmaps`)
	require.Contains(t, text, `{op:"remove",path:"/spec/schedulingGates"}`)
}

func TestRolloutAvailabilityRunnerPinsRecoveredReceiptBeforeCleanup(t *testing.T) {
	fake, logPath, statePath := writeRolloutAvailabilityKubectl(t)
	receipt := `{"format":"kubebrain.rollout-fixture-owner.v2","lease_ids":["7001","7002","7003"],"namespace":"kubebrain-system","prefix":"/kubebrain-rollout-availability/kubebrain-rollout-availability-probe/","probe_pod":"kubebrain-rollout-availability-probe","probe_pod_uid":"33333333-3333-4333-8333-333333333333","statefulset":"kubebrain","statefulset_uid":"statefulset-uid"}`
	owner := map[string]any{
		"apiVersion": "v1",
		"kind":       "ConfigMap",
		"metadata": map[string]any{
			"name":      "kubebrain-rollout-availability-probe-owner",
			"namespace": "kubebrain-system",
			"ownerReferences": []map[string]any{{
				"apiVersion": "apps/v1", "kind": "StatefulSet", "name": "kubebrain",
				"uid": "statefulset-uid", "controller": true,
			}},
		},
		"immutable": true,
		"data":      map[string]string{"receipt.json": receipt},
	}
	encoded, err := json.Marshal(owner)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(logPath+".owner", encoded, 0o600))

	command := exec.Command("bash", "run-kubebrain-rollout-availability.sh")
	command.Env = append(os.Environ(),
		"KUBECTL_BIN="+fake,
		"FAKE_KUBECTL_LOG="+logPath,
		"FAKE_KUBECTL_STATE="+statePath,
		"FAKE_OWNER_FINALIZER_PATCH_DRIFT=true",
		"ALLOW_MUTATING_KUBEBRAIN_ROLLOUT=true",
		"OBSERVE_ONLY=true",
		"PROBE_ITERATIONS=3",
	)
	output, err := command.CombinedOutput()
	require.Error(t, err)
	require.Contains(t, string(output), "failed to pin recovered fixture owner ConfigMap")
	log := readOptionalFile(t, logPath)
	require.NotContains(t, log, " run kubebrain-rollout-availability-probe-cleanup ",
		"cleanup must not mutate etcd after the receipt identity loses its Kubernetes CAS")
}

func TestRolloutAvailabilityRunnerPinsAndReleasesRecoveredReceipt(t *testing.T) {
	fake, logPath, statePath := writeRolloutAvailabilityKubectl(t)
	receipt := `{"format":"kubebrain.rollout-fixture-owner.v2","lease_ids":["7001","7002","7003"],"namespace":"kubebrain-system","prefix":"/kubebrain-rollout-availability/kubebrain-rollout-availability-probe/","probe_pod":"kubebrain-rollout-availability-probe","probe_pod_uid":"33333333-3333-4333-8333-333333333333","statefulset":"kubebrain","statefulset_uid":"statefulset-uid"}`
	owner := map[string]any{
		"apiVersion": "v1", "kind": "ConfigMap", "immutable": true,
		"metadata": map[string]any{
			"name": "kubebrain-rollout-availability-probe-owner", "namespace": "kubebrain-system",
			"ownerReferences": []map[string]any{{
				"apiVersion": "apps/v1", "kind": "StatefulSet", "name": "kubebrain",
				"uid": "statefulset-uid", "controller": true,
			}},
		},
		"data": map[string]string{"receipt.json": receipt},
	}
	encoded, err := json.Marshal(owner)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(logPath+".owner", encoded, 0o600))

	command := exec.Command("bash", "run-kubebrain-rollout-availability.sh")
	command.Env = append(os.Environ(),
		"KUBECTL_BIN="+fake, "FAKE_KUBECTL_LOG="+logPath, "FAKE_KUBECTL_STATE="+statePath,
		"ALLOW_MUTATING_KUBEBRAIN_ROLLOUT=true", "OBSERVE_ONLY=true", "PROBE_ITERATIONS=3",
	)
	output, err := command.CombinedOutput()
	require.NoError(t, err, string(output))
	log := readOptionalFile(t, logPath)
	pin := strings.Index(log, " patch configmap/kubebrain-rollout-availability-probe-owner --type=json -p ")
	cleanup := strings.Index(log, " run kubebrain-rollout-availability-probe-cleanup ")
	require.GreaterOrEqual(t, pin, 0)
	require.Greater(t, cleanup, pin, "the receipt finalizer must be durable before etcd cleanup starts")
	require.GreaterOrEqual(t, strings.Count(log,
		" patch configmap/kubebrain-rollout-availability-probe-owner --type=json -p "), 3,
		"recovered receipt pin/unpin and current receipt unpin must all be CAS guarded")
}

func TestRolloutAvailabilityRunnerReleasesReceiptDeletedDuringCleanup(t *testing.T) {
	fake, logPath, statePath := writeRolloutAvailabilityKubectl(t)
	receipt := `{"format":"kubebrain.rollout-fixture-owner.v2","lease_ids":["7001","7002","7003"],"namespace":"kubebrain-system","prefix":"/kubebrain-rollout-availability/kubebrain-rollout-availability-probe/","probe_pod":"kubebrain-rollout-availability-probe","probe_pod_uid":"33333333-3333-4333-8333-333333333333","statefulset":"kubebrain","statefulset_uid":"statefulset-uid"}`
	owner := map[string]any{
		"apiVersion": "v1", "kind": "ConfigMap", "immutable": true,
		"metadata": map[string]any{
			"name": "kubebrain-rollout-availability-probe-owner", "namespace": "kubebrain-system",
			"ownerReferences": []map[string]any{{
				"apiVersion": "apps/v1", "kind": "StatefulSet", "name": "kubebrain",
				"uid": "statefulset-uid", "controller": true,
			}},
		},
		"data": map[string]string{"receipt.json": receipt},
	}
	encoded, err := json.Marshal(owner)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(logPath+".owner", encoded, 0o600))

	command := exec.Command("bash", "run-kubebrain-rollout-availability.sh")
	command.Env = append(os.Environ(),
		"KUBECTL_BIN="+fake, "FAKE_KUBECTL_LOG="+logPath, "FAKE_KUBECTL_STATE="+statePath,
		"FAKE_OWNER_DELETE_DURING_CLEANUP=true",
		"ALLOW_MUTATING_KUBEBRAIN_ROLLOUT=true", "OBSERVE_ONLY=true", "PROBE_ITERATIONS=3",
	)
	output, err := command.CombinedOutput()
	require.NoError(t, err, string(output))
	require.Contains(t, string(output), "KubeBrain rollout availability gate passed")
	require.NoFileExists(t, logPath+".owner", "the pinned deleting receipt must be released after cleanup")
}

func TestRolloutAvailabilityRunnerRecoversPinnedTerminatingReceiptAfterCrash(t *testing.T) {
	fake, logPath, statePath := writeRolloutAvailabilityKubectl(t)
	receipt := `{"format":"kubebrain.rollout-fixture-owner.v2","lease_ids":["7001","7002","7003"],"namespace":"kubebrain-system","prefix":"/kubebrain-rollout-availability/kubebrain-rollout-availability-probe/","probe_pod":"kubebrain-rollout-availability-probe","probe_pod_uid":"33333333-3333-4333-8333-333333333333","statefulset":"kubebrain","statefulset_uid":"statefulset-uid"}`
	owner := map[string]any{
		"apiVersion": "v1", "kind": "ConfigMap", "immutable": true,
		"metadata": map[string]any{
			"name": "kubebrain-rollout-availability-probe-owner", "namespace": "kubebrain-system",
			"deletionTimestamp": "2026-08-28T09:00:00Z",
			"finalizers":        []string{"kubebrain.io/rollout-fixture-cleanup"},
			"ownerReferences": []map[string]any{{
				"apiVersion": "apps/v1", "kind": "StatefulSet", "name": "kubebrain",
				"uid": "statefulset-uid", "controller": true,
			}},
		},
		"data": map[string]string{"receipt.json": receipt},
	}
	encoded, err := json.Marshal(owner)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(logPath+".owner", encoded, 0o600))

	command := exec.Command("bash", "run-kubebrain-rollout-availability.sh")
	command.Env = append(os.Environ(),
		"KUBECTL_BIN="+fake, "FAKE_KUBECTL_LOG="+logPath, "FAKE_KUBECTL_STATE="+statePath,
		"ALLOW_MUTATING_KUBEBRAIN_ROLLOUT=true", "OBSERVE_ONLY=true", "PROBE_ITERATIONS=3",
	)
	output, err := command.CombinedOutput()
	require.NoError(t, err, string(output))
	require.Contains(t, string(output), "KubeBrain rollout availability gate passed")
	require.NoFileExists(t, logPath+".owner", "the inherited terminating receipt must be released")
	log := readOptionalFile(t, logPath)
	cleanup := strings.Index(log, " run kubebrain-rollout-availability-probe-cleanup ")
	unpin := strings.Index(log, " patch configmap/kubebrain-rollout-availability-probe-owner --type=json -p ")
	require.GreaterOrEqual(t, cleanup, 0)
	require.Greater(t, unpin, cleanup,
		"an inherited finalizer must hold the receipt without an illegal pre-cleanup finalizer mutation")
}

func TestRolloutAvailabilityRunnerJoinsCompletedCleanupForTerminatingReceipt(t *testing.T) {
	fake, logPath, statePath := writeRolloutAvailabilityKubectl(t)
	receipt := `{"format":"kubebrain.rollout-fixture-owner.v2","lease_ids":["7101","7102","7103"],"namespace":"kubebrain-system","prefix":"/kubebrain-rollout-availability/kubebrain-rollout-availability-probe/","probe_pod":"kubebrain-rollout-availability-probe","probe_pod_uid":"33333333-3333-4333-8333-333333333333","statefulset":"kubebrain","statefulset_uid":"statefulset-uid"}`
	owner := map[string]any{
		"apiVersion": "v1", "kind": "ConfigMap", "immutable": true,
		"metadata": map[string]any{
			"name": "kubebrain-rollout-availability-probe-owner", "namespace": "kubebrain-system",
			"uid": "44444444-4444-4444-8444-444444444444", "resourceVersion": "owner-rv-deleting",
			"deletionTimestamp": "2026-08-28T10:00:00Z",
			"finalizers":        []string{"kubebrain.io/rollout-fixture-cleanup"},
			"ownerReferences": []map[string]any{{
				"apiVersion": "apps/v1", "kind": "StatefulSet", "name": "kubebrain",
				"uid": "statefulset-uid", "controller": true,
			}},
		},
		"data": map[string]string{"receipt.json": receipt},
	}
	encoded, err := json.Marshal(owner)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(logPath+".owner", encoded, 0o600))

	command := exec.Command("bash", "run-kubebrain-rollout-availability.sh")
	command.Env = append(os.Environ(),
		"KUBECTL_BIN="+fake, "FAKE_KUBECTL_LOG="+logPath, "FAKE_KUBECTL_STATE="+statePath,
		"FAKE_EXISTING_CLEANUP_POD=true", "ALLOW_MUTATING_KUBEBRAIN_ROLLOUT=true",
		"OBSERVE_ONLY=true", "PROBE_ITERATIONS=3",
	)
	output, err := command.CombinedOutput()
	require.NoError(t, err, string(output))
	require.Contains(t, string(output), "KubeBrain rollout availability gate passed")
	require.NoFileExists(t, logPath+".owner", "the joined cleanup evidence must allow receipt release")
	log := readOptionalFile(t, logPath)
	require.Equal(t, 1, strings.Count(log, " run kubebrain-rollout-availability-probe-cleanup "),
		"preflight must join the receipt-bound cleanup Pod; only the new probe needs a fresh postflight cleanup")
	unpin := strings.Index(log, " patch configmap/kubebrain-rollout-availability-probe-owner --type=json -p ")
	deleteCleanup := strings.LastIndex(log,
		" get pod kubebrain-rollout-availability-probe-cleanup -o json --ignore-not-found")
	require.GreaterOrEqual(t, unpin, 0)
	require.Greater(t, deleteCleanup, unpin,
		"the shared cleanup completion evidence must remain available until the receipt is released")
}

func TestRolloutAvailabilityRunnerRejectsCleanupBoundToDifferentReceipt(t *testing.T) {
	fake, logPath, statePath := writeRolloutAvailabilityKubectl(t)
	receipt := `{"format":"kubebrain.rollout-fixture-owner.v2","lease_ids":["7101","7102","7103"],"namespace":"kubebrain-system","prefix":"/kubebrain-rollout-availability/kubebrain-rollout-availability-probe/","probe_pod":"kubebrain-rollout-availability-probe","probe_pod_uid":"33333333-3333-4333-8333-333333333333","statefulset":"kubebrain","statefulset_uid":"statefulset-uid"}`
	owner := map[string]any{
		"apiVersion": "v1", "kind": "ConfigMap", "immutable": true,
		"metadata": map[string]any{
			"name": "kubebrain-rollout-availability-probe-owner", "namespace": "kubebrain-system",
			"uid": "44444444-4444-4444-8444-444444444444", "resourceVersion": "owner-rv-deleting",
			"deletionTimestamp": "2026-08-28T10:00:00Z",
			"finalizers":        []string{"kubebrain.io/rollout-fixture-cleanup"},
			"ownerReferences": []map[string]any{{
				"apiVersion": "apps/v1", "kind": "StatefulSet", "name": "kubebrain",
				"uid": "statefulset-uid", "controller": true,
			}},
		},
		"data": map[string]string{"receipt.json": receipt},
	}
	encoded, err := json.Marshal(owner)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(logPath+".owner", encoded, 0o600))

	command := exec.Command("bash", "run-kubebrain-rollout-availability.sh")
	command.Env = append(os.Environ(),
		"KUBECTL_BIN="+fake, "FAKE_KUBECTL_LOG="+logPath, "FAKE_KUBECTL_STATE="+statePath,
		"FAKE_EXISTING_CLEANUP_POD=true", "FAKE_EXISTING_CLEANUP_RECEIPT_DRIFT=true",
		"ALLOW_MUTATING_KUBEBRAIN_ROLLOUT=true", "OBSERVE_ONLY=true", "PROBE_ITERATIONS=3",
	)
	output, err := command.CombinedOutput()
	require.Error(t, err)
	require.Contains(t, string(output), "existing fixture cleanup Pod identity is malformed")
	require.FileExists(t, logPath+".owner", "identity drift must preserve the terminating receipt")
	require.NotContains(t, readOptionalFile(t, logPath), " logs kubebrain-rollout-availability-probe-cleanup ",
		"identity drift must be rejected before consuming shared cleanup evidence")
}

func TestRolloutAvailabilityRunnerReportsProbeFailureBeforeStartBarrier(t *testing.T) {
	fake, logPath, statePath := writeRolloutAvailabilityKubectl(t)
	command := exec.Command("bash", "run-kubebrain-rollout-availability.sh")
	command.Env = append(os.Environ(),
		"KUBECTL_BIN="+fake,
		"FAKE_KUBECTL_LOG="+logPath,
		"FAKE_KUBECTL_STATE="+statePath,
		"ALLOW_MUTATING_KUBEBRAIN_ROLLOUT=true",
		"PROBE_ITERATIONS=3",
		"PROBE_START_TIMEOUT=60s",
		"FAKE_PROBE_START_FAIL=true",
	)
	started := time.Now()
	output, err := command.CombinedOutput()
	require.Error(t, err)
	require.Contains(t, string(output), "PROBE_FAIL invalid Snapshot scale put response")
	require.Contains(t, string(output), "availability probe failed before publishing its start barrier")
	require.Less(t, time.Since(started), 5*time.Second)
	require.NoFileExists(t, statePath, "a failed probe must not mutate the StatefulSet")
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

func TestRolloutAvailabilityRunnerRejectsInvalidObserveModeBeforeKubernetes(t *testing.T) {
	fake, logPath, _ := writeRolloutAvailabilityKubectl(t)
	command := exec.Command("bash", "run-kubebrain-rollout-availability.sh")
	command.Env = append(os.Environ(),
		"KUBECTL_BIN="+fake,
		"FAKE_KUBECTL_LOG="+logPath,
		"ALLOW_MUTATING_KUBEBRAIN_ROLLOUT=true",
		"OBSERVE_ONLY=1",
	)
	output, err := command.CombinedOutput()
	require.EqualError(t, err, "exit status 2")
	require.Equal(t, "OBSERVE_ONLY must be true or false\n", string(output))
	require.NoFileExists(t, logPath)
}

func TestRolloutAvailabilityRunnerRequiresExplicitHardFailoverApproval(t *testing.T) {
	fake, logPath, _ := writeRolloutAvailabilityKubectl(t)
	command := exec.Command("bash", "run-kubebrain-rollout-availability.sh")
	command.Env = append(os.Environ(),
		"KUBECTL_BIN="+fake,
		"FAKE_KUBECTL_LOG="+logPath,
		"ALLOW_MUTATING_KUBEBRAIN_ROLLOUT=true",
		"HARD_FAILOVER=true",
	)
	output, err := command.CombinedOutput()
	require.Error(t, err)
	require.Contains(t, string(output), "CONFIRM_KUBEBRAIN_HARD_FAILOVER=delete-current-leader")
	require.NoFileExists(t, logPath, "leader discovery must not run before destructive approval")
}

func TestRolloutAvailabilityRunnerRejectsHardFailoverCombinationBeforeKubernetes(t *testing.T) {
	fake, logPath, _ := writeRolloutAvailabilityKubectl(t)
	uidDelete, _, _ := writeRolloutAvailabilityUIDDelete(t)
	command := exec.Command("bash", "run-kubebrain-rollout-availability.sh")
	command.Env = append(os.Environ(),
		"KUBECTL_BIN="+fake,
		"FAKE_KUBECTL_LOG="+logPath,
		"ALLOW_MUTATING_KUBEBRAIN_ROLLOUT=true",
		"HARD_FAILOVER=true",
		"CONFIRM_KUBEBRAIN_HARD_FAILOVER=delete-current-leader",
		"UID_DELETE_BIN="+uidDelete,
		"OBSERVE_ONLY=true",
	)
	output, err := command.CombinedOutput()
	require.EqualError(t, err, "exit status 2")
	require.Contains(t, string(output), "HARD_FAILOVER cannot be combined")
	require.NoFileExists(t, logPath)
}

func TestRolloutAvailabilityRunnerRejectsInvalidConnectionAgingMigrationBeforeKubernetes(t *testing.T) {
	fake, logPath, _ := writeRolloutAvailabilityKubectl(t)
	command := exec.Command("bash", "run-kubebrain-rollout-availability.sh")
	command.Env = append(os.Environ(),
		"KUBECTL_BIN="+fake,
		"FAKE_KUBECTL_LOG="+logPath,
		"ALLOW_MUTATING_KUBEBRAIN_ROLLOUT=true",
		"ENABLE_GRPC_CONNECTION_AGING_MIGRATION=1",
	)
	output, err := command.CombinedOutput()
	require.EqualError(t, err, "exit status 2")
	require.Equal(t, "ENABLE_GRPC_CONNECTION_AGING_MIGRATION must be true or false\n", string(output))
	require.NoFileExists(t, logPath)
}

func TestRolloutAvailabilityRunnerRejectsInvalidHTTPReadinessMigrationBeforeKubernetes(t *testing.T) {
	fake, logPath, _ := writeRolloutAvailabilityKubectl(t)
	command := exec.Command("bash", "run-kubebrain-rollout-availability.sh")
	command.Env = append(os.Environ(),
		"KUBECTL_BIN="+fake,
		"FAKE_KUBECTL_LOG="+logPath,
		"ALLOW_MUTATING_KUBEBRAIN_ROLLOUT=true",
		"ENABLE_HTTP_READINESS_MIGRATION=1",
	)
	output, err := command.CombinedOutput()
	require.EqualError(t, err, "exit status 2")
	require.Equal(t, "ENABLE_HTTP_READINESS_MIGRATION must be true or false\n", string(output))
	require.NoFileExists(t, logPath)
}

func TestRolloutAvailabilityRunnerRequiresTargetForConnectionAgingMigration(t *testing.T) {
	fake, logPath, _ := writeRolloutAvailabilityKubectl(t)
	command := exec.Command("bash", "run-kubebrain-rollout-availability.sh")
	command.Env = append(os.Environ(),
		"KUBECTL_BIN="+fake,
		"FAKE_KUBECTL_LOG="+logPath,
		"ALLOW_MUTATING_KUBEBRAIN_ROLLOUT=true",
		"ENABLE_GRPC_CONNECTION_AGING_MIGRATION=true",
	)
	output, err := command.CombinedOutput()
	require.EqualError(t, err, "exit status 2")
	require.Equal(t, "ENABLE_GRPC_CONNECTION_AGING_MIGRATION requires TARGET_IMAGE\n", string(output))
	require.NoFileExists(t, logPath)
}

func TestRolloutAvailabilityRunnerRequiresTargetForHTTPReadinessMigration(t *testing.T) {
	fake, logPath, _ := writeRolloutAvailabilityKubectl(t)
	command := exec.Command("bash", "run-kubebrain-rollout-availability.sh")
	command.Env = append(os.Environ(),
		"KUBECTL_BIN="+fake,
		"FAKE_KUBECTL_LOG="+logPath,
		"ALLOW_MUTATING_KUBEBRAIN_ROLLOUT=true",
		"ENABLE_HTTP_READINESS_MIGRATION=true",
	)
	output, err := command.CombinedOutput()
	require.EqualError(t, err, "exit status 2")
	require.Equal(t, "ENABLE_HTTP_READINESS_MIGRATION requires TARGET_IMAGE\n", string(output))
	require.NoFileExists(t, logPath)
}

func TestRolloutAvailabilityRunnerRejectsObserveModeWithCandidateBeforeKubernetes(t *testing.T) {
	fake, logPath, _ := writeRolloutAvailabilityKubectl(t)
	command := exec.Command("bash", "run-kubebrain-rollout-availability.sh")
	command.Env = append(os.Environ(),
		"KUBECTL_BIN="+fake,
		"FAKE_KUBECTL_LOG="+logPath,
		"ALLOW_MUTATING_KUBEBRAIN_ROLLOUT=true",
		"OBSERVE_ONLY=true",
		"TARGET_IMAGE=registry.example/kubebrain@sha256:"+strings.Repeat("a", 64),
		"TARGET_RUNTIME_DIGESTS=sha256:"+strings.Repeat("b", 64),
	)
	output, err := command.CombinedOutput()
	require.EqualError(t, err, "exit status 2")
	require.Equal(t, "OBSERVE_ONLY cannot be combined with TARGET_IMAGE or rollout migrations\n", string(output))
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
		"KUBECTL_ROLLOUT_STATUS_COMMAND_TIMEOUT", "KUBECTL_PHASE_WAIT_COMMAND_TIMEOUT", "UID_DELETE_COMMAND_TIMEOUT",
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
		{name: "evidence", target: "evidence", timeoutVariable: "KUBECTL_EVIDENCE_COMMAND_TIMEOUT=1s", want: "failed to read KubeBrain StatefulSet"},
		{name: "mutation", target: "mutation", timeoutVariable: "KUBECTL_MUTATION_COMMAND_TIMEOUT=1s", want: "failed to create rollout availability probe Pod"},
		{name: "ready wait", target: "ready", timeoutVariable: "KUBECTL_READY_WAIT_COMMAND_TIMEOUT=1s", want: "rollout availability probe Pod did not become Ready"},
		{name: "rollout status", target: "rollout", timeoutVariable: "KUBECTL_ROLLOUT_STATUS_COMMAND_TIMEOUT=1s", want: "KubeBrain rollout did not converge"},
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
				"run-kubebrain-rollout-availability.sh", env, 6*time.Second)
			require.Error(t, err)
			require.Contains(t, string(output), tc.want)
			if tc.wantProbeLogCalls > 0 {
				log := readOptionalFile(t, logPath)
				probeLogCalls := 0
				for _, line := range strings.Split(log, "\n") {
					if strings.HasSuffix(line, " logs kubebrain-rollout-availability-probe") ||
						strings.Contains(line, " logs kubebrain-rollout-availability-probe ") {
						probeLogCalls++
					}
				}
				require.Equal(t, tc.wantProbeLogCalls, probeLogCalls,
					"an expired completion stage must not start a diagnostic log request")
			}
			require.Less(t, time.Since(started), 6*time.Second,
				"the outer command timeout must terminate a kubectl process that never reaches HTTP request handling")
		})
	}
}

func TestRolloutAvailabilityRunnerRejectsNumericControlsBeforeKubernetes(t *testing.T) {
	for _, tc := range []struct{ name, setting, want string }{
		{name: "replicas overflow", setting: "EXPECTED_REPLICAS=9223372036854775808", want: "EXPECTED_REPLICAS must be a positive int64"},
		{name: "iterations overflow", setting: "PROBE_ITERATIONS=9223372036854775808", want: "PROBE_ITERATIONS must be a positive int64"},
		{name: "lease TTL overflow", setting: "PROBE_LEASE_TTL=9223372036854775808", want: "PROBE_LEASE_TTL must be a positive int64"},
		{name: "zero public TCP dials", setting: "PROBE_MIN_PUBLIC_TCP_DIALS=0", want: "PROBE_MIN_PUBLIC_TCP_DIALS must be a positive int64"},
		{name: "direct TCP dials overflow", setting: "PROBE_MIN_DIRECT_TCP_DIALS=9223372036854775808", want: "PROBE_MIN_DIRECT_TCP_DIALS must be a positive int64"},
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

func TestRolloutAvailabilityRunnerObserveOnlyDoesNotRollStatefulSet(t *testing.T) {
	fake, logPath, statePath := writeRolloutAvailabilityKubectl(t)
	command := exec.Command("bash", "run-kubebrain-rollout-availability.sh")
	command.Env = append(os.Environ(),
		"KUBECTL_BIN="+fake,
		"FAKE_KUBECTL_LOG="+logPath,
		"FAKE_KUBECTL_STATE="+statePath,
		"ALLOW_MUTATING_KUBEBRAIN_ROLLOUT=true",
		"OBSERVE_ONLY=true",
		"PROBE_ITERATIONS=3",
		"PROBE_MIN_PUBLIC_TCP_DIALS=2",
		"PROBE_MIN_DIRECT_TCP_DIALS=2",
	)
	output, err := command.CombinedOutput()
	require.NoError(t, err, string(output))
	require.Contains(t, string(output), "mode=observe")
	require.Contains(t, string(output), "revision=revision-old->revision-old")
	require.NoFileExists(t, statePath, "observe-only mode must not mutate the StatefulSet")
	log := readOptionalFile(t, logPath)
	require.Contains(t, log, " run kubebrain-rollout-availability-probe ")
	require.Contains(t, log, " delete pod kubebrain-rollout-availability-probe ")
	require.Equal(t, 2, strings.Count(log, " run kubebrain-rollout-availability-probe-cleanup "))
	require.Equal(t, 2, strings.Count(log, "--cleanup-owned-fixture"))
	require.Contains(t, log, "--fixture-owner-namespace=kubebrain-system")
	require.Contains(t, log, "--fixture-owner-pod=kubebrain-rollout-availability-probe")
	require.Contains(t, log, "--fixture-owner-statefulset=kubebrain")
	require.Contains(t, log, "--fixture-owner-statefulset-uid=statefulset-uid")
	require.Contains(t, log, "--fixture-owner-pod-uid=33333333-3333-4333-8333-333333333333")
	require.Contains(t, log, `"fieldPath":"metadata.uid"`)
	require.NotContains(t, log, " patch statefulset/kubebrain ")
	require.NotContains(t, log, " rollout status statefulset/kubebrain ")
}

func TestRolloutAvailabilityRunnerFailsClosedOnMalformedFixtureCleanupEvidence(t *testing.T) {
	fake, logPath, statePath := writeRolloutAvailabilityKubectl(t)
	command := exec.Command("bash", "run-kubebrain-rollout-availability.sh")
	command.Env = append(os.Environ(),
		"KUBECTL_BIN="+fake,
		"FAKE_KUBECTL_LOG="+logPath,
		"FAKE_KUBECTL_STATE="+statePath,
		"FAKE_FIXTURE_CLEANUP_MALFORMED=true",
		"ALLOW_MUTATING_KUBEBRAIN_ROLLOUT=true",
		"OBSERVE_ONLY=true",
		"PROBE_ITERATIONS=3",
	)
	output, err := command.CombinedOutput()
	require.Error(t, err)
	require.Contains(t, string(output), "fixture cleanup evidence is malformed")
	require.Contains(t, string(output), "rollout fixture preflight cleanup failed")
	log := readOptionalFile(t, logPath)
	require.Contains(t, log, " run kubebrain-rollout-availability-probe-cleanup ")
	require.NotContains(t, log, " run kubebrain-rollout-availability-probe ")
}

func TestRolloutAvailabilityRunnerHardFailoverDeletesStableLeaderIdentityAndRecovers(t *testing.T) {
	fake, logPath, statePath := writeRolloutAvailabilityKubectl(t)
	uidDelete, uidDeleteLog, hardState := writeRolloutAvailabilityUIDDelete(t)
	leaderDiscoveryState := filepath.Join(t.TempDir(), "leader-discovery-state")
	command := exec.Command("bash", "run-kubebrain-rollout-availability.sh")
	command.Env = append(os.Environ(),
		"KUBECTL_BIN="+fake,
		"FAKE_KUBECTL_LOG="+logPath,
		"FAKE_KUBECTL_STATE="+statePath,
		"ALLOW_MUTATING_KUBEBRAIN_ROLLOUT=true",
		"HARD_FAILOVER=true",
		"CONFIRM_KUBEBRAIN_HARD_FAILOVER=delete-current-leader",
		"UID_DELETE_BIN="+uidDelete,
		"FAKE_UID_DELETE_LOG="+uidDeleteLog,
		"FAKE_HARD_FAILOVER_STATE="+hardState,
		"FAKE_LEADER_DISCOVERY_STATE="+leaderDiscoveryState,
		"PROBE_ITERATIONS=3",
	)
	output, err := command.CombinedOutput()
	require.NoError(t, err, string(output))
	require.Contains(t, string(output), "HARD_FAILOVER_STARTED pod=kubebrain-1 uid=kubebrain-1-uid-old member_id=12")
	require.Contains(t, string(output), "HARD_FAILOVER_RECOVERED old_pod=kubebrain-1 old_uid=kubebrain-1-uid-old new_uid=kubebrain-1-uid-new final_leader=kubebrain-2 final_member_id=13")
	require.Contains(t, string(output), "mode=hard-failover")
	require.Contains(t, string(output), "revision=revision-old->revision-old")
	require.FileExists(t, hardState)
	require.NoFileExists(t, statePath, "hard failover must not mutate the StatefulSet spec")
	uidLog := readOptionalFile(t, uidDeleteLog)
	require.Contains(t, uidLog, "--uid=kubebrain-1-uid-old")
	require.Contains(t, uidLog, "--resource-version=rv-kubebrain-1-uid-old")
	require.Contains(t, uidLog, "--grace-period-seconds=0")
	kubectlLog := readOptionalFile(t, logPath)
	require.Equal(t, 2, strings.Count(kubectlLog, " --leader-target-only"))
	require.Contains(t, kubectlLog, "--report-leader-target-on-complete")
	require.Contains(t, kubectlLog, " rollout status statefulset/kubebrain ")
	require.NotContains(t, kubectlLog, " patch statefulset/kubebrain ")
}

func TestRolloutAvailabilityRunnerHardFailoverRequiresFinalLeaderEvidence(t *testing.T) {
	fake, logPath, statePath := writeRolloutAvailabilityKubectl(t)
	uidDelete, uidDeleteLog, hardState := writeRolloutAvailabilityUIDDelete(t)
	leaderDiscoveryState := filepath.Join(t.TempDir(), "leader-discovery-state")
	command := exec.Command("bash", "run-kubebrain-rollout-availability.sh")
	command.Env = append(os.Environ(),
		"KUBECTL_BIN="+fake,
		"FAKE_KUBECTL_LOG="+logPath,
		"FAKE_KUBECTL_STATE="+statePath,
		"ALLOW_MUTATING_KUBEBRAIN_ROLLOUT=true",
		"HARD_FAILOVER=true",
		"CONFIRM_KUBEBRAIN_HARD_FAILOVER=delete-current-leader",
		"UID_DELETE_BIN="+uidDelete,
		"FAKE_UID_DELETE_LOG="+uidDeleteLog,
		"FAKE_HARD_FAILOVER_STATE="+hardState,
		"FAKE_LEADER_DISCOVERY_STATE="+leaderDiscoveryState,
		"FAKE_OMIT_FINAL_LEADER_TARGET=true",
		"PROBE_ITERATIONS=3",
	)
	output, err := command.CombinedOutput()
	require.Error(t, err)
	require.Contains(t, string(output), "final leader target evidence did not return exactly one result")
	require.Contains(t, string(output), "failed to admit final leader evidence after hard failover")
	require.FileExists(t, uidDeleteLog, "the explicit hard-failover mutation must have occurred before final evidence")
	require.FileExists(t, hardState)
	require.NoFileExists(t, statePath, "hard failover must not mutate the StatefulSet spec")
}

func TestRolloutAvailabilityRunnerHardFailoverRefusesLeaderChangeBeforeDelete(t *testing.T) {
	fake, logPath, statePath := writeRolloutAvailabilityKubectl(t)
	uidDelete, uidDeleteLog, hardState := writeRolloutAvailabilityUIDDelete(t)
	leaderDiscoveryState := filepath.Join(t.TempDir(), "leader-discovery-state")
	command := exec.Command("bash", "run-kubebrain-rollout-availability.sh")
	command.Env = append(os.Environ(),
		"KUBECTL_BIN="+fake,
		"FAKE_KUBECTL_LOG="+logPath,
		"FAKE_KUBECTL_STATE="+statePath,
		"ALLOW_MUTATING_KUBEBRAIN_ROLLOUT=true",
		"HARD_FAILOVER=true",
		"CONFIRM_KUBEBRAIN_HARD_FAILOVER=delete-current-leader",
		"UID_DELETE_BIN="+uidDelete,
		"FAKE_UID_DELETE_LOG="+uidDeleteLog,
		"FAKE_HARD_FAILOVER_STATE="+hardState,
		"FAKE_LEADER_DISCOVERY_STATE="+leaderDiscoveryState,
		"FAKE_LEADER_CHANGES_BEFORE_DELETE=true",
		"PROBE_ITERATIONS=3",
	)
	output, err := command.CombinedOutput()
	require.Error(t, err)
	require.Contains(t, string(output), "KubeBrain leader changed before hard-failover deletion; refusing mutation")
	require.NotContains(t, readOptionalFile(t, uidDeleteLog), "--name=kubebrain-1",
		"leader identity drift must prevent the destructive Pod deletion")
	require.NoFileExists(t, hardState)
	require.NoFileExists(t, statePath)
}

func TestRolloutAvailabilityRunnerRejectsInsufficientTCPDialEvidence(t *testing.T) {
	fake, logPath, statePath := writeRolloutAvailabilityKubectl(t)
	command := exec.Command("bash", "run-kubebrain-rollout-availability.sh")
	command.Env = append(os.Environ(),
		"KUBECTL_BIN="+fake,
		"FAKE_KUBECTL_LOG="+logPath,
		"FAKE_KUBECTL_STATE="+statePath,
		"ALLOW_MUTATING_KUBEBRAIN_ROLLOUT=true",
		"OBSERVE_ONLY=true",
		"PROBE_ITERATIONS=3",
		"PROBE_MIN_PUBLIC_TCP_DIALS=3",
	)
	output, err := command.CombinedOutput()
	require.Error(t, err)
	require.Contains(t, string(output), "availability probe TCP dial evidence is below the required minimum")
	require.NoFileExists(t, statePath)
	log := readOptionalFile(t, logPath)
	require.NotContains(t, log, " patch statefulset/kubebrain ")
	require.NotContains(t, log, " rollout status statefulset/kubebrain ")
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
	require.Contains(t, string(output), "PROBE_SUMMARY ok=3 fail=0 total=3 watch=3 direct_watch=3x3 lease=alive lease_responses=7 public_lease_restarts=1 max_public_lease_recovery_ms=3210 direct_lease=alive direct_lease_responses=19 direct_lease_restarts=3 max_direct_lease_recovery_ms=27123 public_tcp_dials=2 min_direct_tcp_dials=2 direct_endpoints=3 range_stream=17 snapshot=2 stream_retries=4 stream_partial_retries=1 max_latency_ms=123 max_put_latency_ms=45 max_watch_after_put_latency_ms=78 max_direct_latency_ms=456 max_tso_latency_ms=12 max_region_latency_ms=34")
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
	require.Contains(t, log, "--min-public-tcp-dials=1")
	require.Contains(t, log, "--min-direct-tcp-dials=1")
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

func TestRolloutAvailabilityRunnerRejectsKeepAliveQueueOverflow(t *testing.T) {
	fake, logPath, statePath := writeRolloutAvailabilityKubectl(t)
	command := exec.Command("bash", "run-kubebrain-rollout-availability.sh")
	command.Env = append(os.Environ(),
		"KUBECTL_BIN="+fake,
		"FAKE_KUBECTL_LOG="+logPath,
		"FAKE_KUBECTL_STATE="+statePath,
		"FAKE_KEEPALIVE_QUEUE_FULL=true",
		"ALLOW_MUTATING_KUBEBRAIN_ROLLOUT=true",
		"PROBE_ITERATIONS=3",
	)
	output, err := command.CombinedOutput()
	require.Error(t, err)
	require.Contains(t, string(output), "availability probe keepalive response queue overflowed")
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
		if strings.Contains(line, " run kubebrain-rollout-availability-probe ") {
			index := strings.Index(line, "--overrides=")
			require.GreaterOrEqual(t, index, 0)
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

func TestRolloutAvailabilityRunnerRejectsTLSWithoutConnectionAgingBeforeMutation(t *testing.T) {
	fake, logPath, statePath := writeRolloutAvailabilityKubectl(t)
	command := exec.Command("bash", "run-kubebrain-rollout-availability.sh")
	command.Env = append(os.Environ(),
		"KUBECTL_BIN="+fake,
		"FAKE_KUBECTL_LOG="+logPath,
		"FAKE_KUBECTL_STATE="+statePath,
		"FAKE_TLS_STATE=true",
		"FAKE_TLS_NO_CONNECTION_AGING=true",
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

func TestRolloutAvailabilityRunnerMigratesTLSConnectionAgingWithCandidate(t *testing.T) {
	fake, logPath, statePath := writeRolloutAvailabilityKubectl(t)
	target := "registry.example/kubebrain@sha256:" + strings.Repeat("f", 64)
	command := exec.Command("bash", "run-kubebrain-rollout-availability.sh")
	command.Env = append(os.Environ(),
		"KUBECTL_BIN="+fake, "FAKE_KUBECTL_LOG="+logPath, "FAKE_KUBECTL_STATE="+statePath,
		"FAKE_TLS_STATE=true", "FAKE_TLS_NO_CONNECTION_AGING=true",
		"ALLOW_MUTATING_KUBEBRAIN_ROLLOUT=true", "PROBE_ITERATIONS=3", "TARGET_IMAGE="+target,
		"TARGET_RUNTIME_DIGESTS=sha256:"+strings.Repeat("f", 64),
		"ENABLE_GRPC_CONNECTION_AGING_MIGRATION=true",
	)
	output, err := command.CombinedOutput()
	require.NoError(t, err, string(output))
	require.Contains(t, string(output), "image=kubebrain:test->"+target)
	log := readOptionalFile(t, logPath)
	require.Contains(t, log, `"path":"/spec"`)
	require.Contains(t, log, `"--grpc-max-connection-age=1h"`)
	require.Contains(t, log, `"--grpc-max-connection-age-grace=5m"`)
}

func TestRolloutAvailabilityRunnerMigratesSemanticReadinessWithCandidate(t *testing.T) {
	fake, logPath, statePath := writeRolloutAvailabilityKubectl(t)
	target := "registry.example/kubebrain@sha256:" + strings.Repeat("e", 64)
	command := exec.Command("bash", "run-kubebrain-rollout-availability.sh")
	command.Env = append(os.Environ(),
		"KUBECTL_BIN="+fake, "FAKE_KUBECTL_LOG="+logPath, "FAKE_KUBECTL_STATE="+statePath,
		"FAKE_TLS_STATE=true", "FAKE_TCP_READINESS=true",
		"ALLOW_MUTATING_KUBEBRAIN_ROLLOUT=true", "PROBE_ITERATIONS=3", "TARGET_IMAGE="+target,
		"TARGET_RUNTIME_DIGESTS=sha256:"+strings.Repeat("e", 64),
		"ENABLE_HTTP_READINESS_MIGRATION=true",
	)
	output, err := command.CombinedOutput()
	require.NoError(t, err, string(output))
	require.Contains(t, string(output), "image=kubebrain:test->"+target)
	log := readOptionalFile(t, logPath)
	require.Contains(t, log, `"path":"/spec"`)
	require.Contains(t, log, `"httpGet":{"path":"/readyz","port":"info","scheme":"HTTPS"}`)
	require.Contains(t, log, `"failureThreshold":1`)
	require.Contains(t, log, `"periodSeconds":1`)
}

func TestRolloutAvailabilityRunnerRejectsTCPReadinessWithoutMigration(t *testing.T) {
	fake, logPath, statePath := writeRolloutAvailabilityKubectl(t)
	command := exec.Command("bash", "run-kubebrain-rollout-availability.sh")
	command.Env = append(os.Environ(),
		"KUBECTL_BIN="+fake, "FAKE_KUBECTL_LOG="+logPath, "FAKE_KUBECTL_STATE="+statePath,
		"FAKE_TCP_READINESS=true", "ALLOW_MUTATING_KUBEBRAIN_ROLLOUT=true", "PROBE_ITERATIONS=3",
	)
	output, err := command.CombinedOutput()
	require.Error(t, err)
	require.Contains(t, string(output), "rollout drain contract mismatch")
	log := readOptionalFile(t, logPath)
	require.NotContains(t, log, " run ")
	require.NotContains(t, log, " patch ")
}

func TestRolloutAvailabilityRunnerRejectsHTTPReadinessMigrationWhenAlreadyConfigured(t *testing.T) {
	fake, logPath, statePath := writeRolloutAvailabilityKubectl(t)
	target := "registry.example/kubebrain@sha256:" + strings.Repeat("e", 64)
	command := exec.Command("bash", "run-kubebrain-rollout-availability.sh")
	command.Env = append(os.Environ(),
		"KUBECTL_BIN="+fake, "FAKE_KUBECTL_LOG="+logPath, "FAKE_KUBECTL_STATE="+statePath,
		"ALLOW_MUTATING_KUBEBRAIN_ROLLOUT=true", "PROBE_ITERATIONS=3", "TARGET_IMAGE="+target,
		"TARGET_RUNTIME_DIGESTS=sha256:"+strings.Repeat("e", 64),
		"ENABLE_HTTP_READINESS_MIGRATION=true",
	)
	output, err := command.CombinedOutput()
	require.Error(t, err)
	require.Contains(t, string(output), "rollout drain contract mismatch")
	log := readOptionalFile(t, logPath)
	require.NotContains(t, log, " run ")
	require.NotContains(t, log, " patch ")
}

func TestRolloutAvailabilityRunnerRollsBackSemanticReadinessMigrationSpec(t *testing.T) {
	fake, logPath, statePath := writeRolloutAvailabilityKubectl(t)
	target := "registry.example/kubebrain@sha256:" + strings.Repeat("e", 64)
	command := exec.Command("bash", "run-kubebrain-rollout-availability.sh")
	command.Env = append(os.Environ(),
		"KUBECTL_BIN="+fake, "FAKE_KUBECTL_LOG="+logPath, "FAKE_KUBECTL_STATE="+statePath,
		"FAKE_TLS_STATE=true", "FAKE_TCP_READINESS=true", "FAKE_ROLLOUT_FAIL=true",
		"ALLOW_MUTATING_KUBEBRAIN_ROLLOUT=true", "PROBE_ITERATIONS=3", "TARGET_IMAGE="+target,
		"TARGET_RUNTIME_DIGESTS=sha256:"+strings.Repeat("e", 64),
		"ENABLE_HTTP_READINESS_MIGRATION=true",
	)
	output, err := command.CombinedOutput()
	require.Error(t, err)
	require.Contains(t, string(output), "candidate rollout failed; restoring original image kubebrain:test")
	require.NoFileExists(t, statePath, "rollback must restore the original TCP readiness spec")
	log := readOptionalFile(t, logPath)
	require.Equal(t, 2, strings.Count(log, " patch statefulset/kubebrain --type=json -p "))
	require.Equal(t, 4, strings.Count(log, `"path":"/spec"`))
	require.Contains(t, log, `"tcpSocket":{"port":"client"}`)
	require.Contains(t, log, `"httpGet":{"path":"/readyz","port":"info","scheme":"HTTPS"}`)
}

func TestRolloutAvailabilityRunnerRejectsConnectionAgingMigrationWhenAlreadyConfigured(t *testing.T) {
	fake, logPath, statePath := writeRolloutAvailabilityKubectl(t)
	target := "registry.example/kubebrain@sha256:" + strings.Repeat("f", 64)
	command := exec.Command("bash", "run-kubebrain-rollout-availability.sh")
	command.Env = append(os.Environ(),
		"KUBECTL_BIN="+fake, "FAKE_KUBECTL_LOG="+logPath, "FAKE_KUBECTL_STATE="+statePath,
		"FAKE_TLS_STATE=true", "ALLOW_MUTATING_KUBEBRAIN_ROLLOUT=true", "PROBE_ITERATIONS=3",
		"TARGET_IMAGE="+target, "TARGET_RUNTIME_DIGESTS=sha256:"+strings.Repeat("f", 64),
		"ENABLE_GRPC_CONNECTION_AGING_MIGRATION=true",
	)
	output, err := command.CombinedOutput()
	require.Error(t, err)
	require.Contains(t, string(output), "rollout drain contract mismatch")
	log := readOptionalFile(t, logPath)
	require.NotContains(t, log, " run ")
	require.NotContains(t, log, " patch ")
}

func TestRolloutAvailabilityRunnerRollsBackConnectionAgingMigrationSpec(t *testing.T) {
	fake, logPath, statePath := writeRolloutAvailabilityKubectl(t)
	target := "registry.example/kubebrain@sha256:" + strings.Repeat("f", 64)
	command := exec.Command("bash", "run-kubebrain-rollout-availability.sh")
	command.Env = append(os.Environ(),
		"KUBECTL_BIN="+fake, "FAKE_KUBECTL_LOG="+logPath, "FAKE_KUBECTL_STATE="+statePath,
		"FAKE_TLS_STATE=true", "FAKE_TLS_NO_CONNECTION_AGING=true", "FAKE_ROLLOUT_FAIL=true",
		"ALLOW_MUTATING_KUBEBRAIN_ROLLOUT=true", "PROBE_ITERATIONS=3", "TARGET_IMAGE="+target,
		"TARGET_RUNTIME_DIGESTS=sha256:"+strings.Repeat("f", 64),
		"ENABLE_GRPC_CONNECTION_AGING_MIGRATION=true",
	)
	output, err := command.CombinedOutput()
	require.Error(t, err)
	require.Contains(t, string(output), "candidate rollout failed; restoring original image kubebrain:test")
	require.NoFileExists(t, statePath, "rollback must restore the original spec")
	log := readOptionalFile(t, logPath)
	require.Equal(t, 2, strings.Count(log, " patch statefulset/kubebrain --type=json -p "))
	require.Equal(t, 4, strings.Count(log, `"path":"/spec"`),
		"each full-spec patch must test and then replace /spec")
	require.Contains(t, log, `"--grpc-max-connection-age=1h"`)
	require.Contains(t, log, `"--grpc-max-connection-age-grace=5m"`)
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
	uidDeletePath := filepath.Join(dir, "uid-delete")
	require.NoError(t, os.WriteFile(uidDeletePath, []byte(`#!/usr/bin/env bash
set -euo pipefail
printf ' uid-delete %s\n' "$*" >>"${FAKE_KUBECTL_LOG}"
if [[ " $* " == *" --resource=configmaps "* ]]; then
  rm -f -- "${FAKE_KUBECTL_LOG}.owner"
  if [[ -e "${FAKE_KUBECTL_LOG}.cleanup" ]] && jq -e '
    [.metadata.ownerReferences[]? | select(.kind == "ConfigMap" and
      .name == "kubebrain-rollout-availability-probe-owner" and .controller == true)] | length == 1
  ' "${FAKE_KUBECTL_LOG}.cleanup" >/dev/null; then
    rm -f -- "${FAKE_KUBECTL_LOG}.cleanup"
    : >"${FAKE_KUBECTL_LOG}.cleanup-deleted"
  fi
fi
if [[ " $* " == *" --resource=pods "* && " $* " == *" --name=kubebrain-rollout-availability-probe-cleanup "* ]]; then
  rm -f -- "${FAKE_KUBECTL_LOG}.cleanup"
  : >"${FAKE_KUBECTL_LOG}.cleanup-deleted"
fi
`), 0o755))
	t.Setenv("UID_DELETE_BIN", uidDeletePath)
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
if [[ "${FAKE_KUBECTL_HANG_TARGET:-}" == phase && " $* " == *" wait --for=jsonpath={.status.phase}=Succeeded pod/kubebrain-rollout-availability-probe "* ]]; then
  sleep 30
fi
if [[ "${FAKE_KUBECTL_HANG_TARGET:-}" == phase-evidence && " $* " == *" get pod kubebrain-rollout-availability-probe -o jsonpath={.status.phase} "* ]]; then
  sleep 30
fi
if [[ "${FAKE_KUBECTL_HANG_TARGET:-}" == start && " $* " == *" logs kubebrain-rollout-availability-probe "* ]]; then
  sleep 30
fi
owner_state="${FAKE_KUBECTL_LOG}.owner"
cleanup_state="${FAKE_KUBECTL_LOG}.cleanup"
if [[ " $* " == *" get configmap kubebrain-rollout-availability-probe-owner "* ]]; then
  if [[ ! -e "$owner_state" ]]; then
    [[ " $* " == *" --ignore-not-found "* ]] && exit 0
    exit 1
  fi
  jq -c '.metadata.uid //= "44444444-4444-4444-8444-444444444444" | .metadata.resourceVersion //= "owner-rv"' "$owner_state"
elif [[ " $* " == *" create -f - "* ]]; then
  payload="$(jq -c .)"
  jq -e '.immutable == true and (.data["receipt.json"] | length > 0) and
    .metadata.finalizers == ["kubebrain.io/rollout-fixture-cleanup"] and
    (.data["receipt.json"] | fromjson | .format == "kubebrain.rollout-fixture-owner.v2")' <<<"$payload" >/dev/null
  printf '%s' "$payload" >"$owner_state"
elif [[ " $* " == *" patch configmap/kubebrain-rollout-availability-probe-owner --type=json -p "* ]]; then
  [[ "${FAKE_OWNER_FINALIZER_PATCH_DRIFT:-false}" != true ]] || exit 1
  patch_payload=""
  previous=""
  for argument in "$@"; do
    [[ "$previous" != -p ]] || patch_payload="$argument"
    previous="$argument"
  done
  [[ -e "$owner_state" && -n "$patch_payload" ]] || exit 1
  current="$(jq -c '.metadata.uid //= "44444444-4444-4444-8444-444444444444" | .metadata.resourceVersion //= "owner-rv"' "$owner_state")"
  expected_uid="$(jq -r '.[0].value // ""' <<<"$patch_payload")"
  expected_resource_version="$(jq -r '.[1].value // ""' <<<"$patch_payload")"
  expected_receipt="$(jq -r '.[3].value // ""' <<<"$patch_payload")"
  [[ "$expected_uid" == "$(jq -r '.metadata.uid' <<<"$current")" &&
    "$expected_resource_version" == "$(jq -r '.metadata.resourceVersion' <<<"$current")" &&
    "$expected_receipt" == "$(jq -r '.data["receipt.json"]' <<<"$current")" ]] || exit 1
  final_operation="$(jq -r '.[-1].op' <<<"$patch_payload")"
  if [[ "$final_operation" == add ]]; then
    [[ "$(jq -r '.metadata.deletionTimestamp // ""' <<<"$current")" == "" ]] || exit 1
    next="$(jq -c --argjson finalizers "$(jq -c '.[-1].value' <<<"$patch_payload")" \
      '.metadata.finalizers=$finalizers | .metadata.resourceVersion="owner-rv-pinned"' <<<"$current")"
  elif [[ "$final_operation" == remove ]]; then
    [[ "$(jq -c '.metadata.finalizers // []' <<<"$current")" == \
      "$(jq -c '.[4].value' <<<"$patch_payload")" ]] || exit 1
    next="$(jq -c 'del(.metadata.finalizers) | .metadata.resourceVersion="owner-rv-unpinned"' <<<"$current")"
  else
    exit 1
  fi
  printf '%s' "$next" >"$owner_state"
  printf '%s' "$next"
elif [[ " $* " == *" patch pod/kubebrain-rollout-availability-probe --type=json -p "* ]]; then
  :
elif [[ " $* " == *" get service kubebrain-peer -o json "* ]]; then
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
  readiness_probe='{"httpGet":{"path":"/readyz","port":"info","scheme":"HTTP"},"initialDelaySeconds":5,"periodSeconds":1,"timeoutSeconds":1,"successThreshold":1,"failureThreshold":1}'
  if [[ "${FAKE_TLS_STATE:-false}" == true ]]; then
    prestop='["/bin/sh","-c","sleep 25 && curl --insecure --fail --silent --show-error --max-time 10 --request POST https://127.0.0.1:8080/drain"]'
    args='["--leader-retry-period=500ms","--pd-addrs=pd-0:2379,pd-1:2379,pd-2:2379","--allow-insecure=false","--info-cert-file=/etc/kubebrain/client-tls/tls.crt","--info-key-file=/etc/kubebrain/client-tls/tls.key","--grpc-max-connection-age=1h","--grpc-max-connection-age-grace=5m","--cert-file=/etc/kubebrain/client-tls/tls.crt","--key-file=/etc/kubebrain/client-tls/tls.key","--trusted-ca-file=/etc/kubebrain/client-tls/ca.crt","--tls-server-name=kubebrain-client.kubebrain-system.svc","--client-cert-auth=true"]'
    [[ "${FAKE_TLS_NO_CONNECTION_AGING:-false}" != true ]] || args='["--leader-retry-period=500ms","--pd-addrs=pd-0:2379,pd-1:2379,pd-2:2379","--allow-insecure=false","--info-cert-file=/etc/kubebrain/client-tls/tls.crt","--info-key-file=/etc/kubebrain/client-tls/tls.key","--cert-file=/etc/kubebrain/client-tls/tls.crt","--key-file=/etc/kubebrain/client-tls/tls.key","--trusted-ca-file=/etc/kubebrain/client-tls/ca.crt","--tls-server-name=kubebrain-client.kubebrain-system.svc","--client-cert-auth=true"]'
    if [[ "${FAKE_TLS_NO_CONNECTION_AGING:-false}" == true && "${ENABLE_GRPC_CONNECTION_AGING_MIGRATION:-false}" == true && -e "$FAKE_KUBECTL_STATE" ]]; then
      args='["--leader-retry-period=500ms","--pd-addrs=pd-0:2379,pd-1:2379,pd-2:2379","--allow-insecure=false","--info-cert-file=/etc/kubebrain/client-tls/tls.crt","--info-key-file=/etc/kubebrain/client-tls/tls.key","--cert-file=/etc/kubebrain/client-tls/tls.crt","--key-file=/etc/kubebrain/client-tls/tls.key","--trusted-ca-file=/etc/kubebrain/client-tls/ca.crt","--tls-server-name=kubebrain-client.kubebrain-system.svc","--client-cert-auth=true","--grpc-max-connection-age=1h","--grpc-max-connection-age-grace=5m"]'
    fi
    volume_mounts='[{"name":"client-tls","mountPath":"/etc/kubebrain/client-tls","readOnly":true}]'
    volumes='[{"name":"client-tls","secret":{"secretName":"kubebrain-client-tls","defaultMode":256}}]'
    pod_security_context='{"runAsNonRoot":true,"runAsUser":65532,"runAsGroup":65532,"fsGroup":65532}'
    readiness_probe='{"httpGet":{"path":"/readyz","port":"info","scheme":"HTTPS"},"initialDelaySeconds":5,"periodSeconds":1,"timeoutSeconds":1,"successThreshold":1,"failureThreshold":1}'
    [[ "${FAKE_TLS_WRITABLE_MOUNT:-false}" != true ]] || volume_mounts='[{"name":"client-tls","mountPath":"/etc/kubebrain/client-tls","readOnly":false}]'
    [[ "${FAKE_TLS_ROOT_CONTEXT:-false}" != true ]] || pod_security_context='{"runAsNonRoot":false,"runAsUser":0,"runAsGroup":0,"fsGroup":0}'
  fi
  if [[ "${FAKE_TCP_READINESS:-false}" == true ]]; then
    if [[ "${ENABLE_HTTP_READINESS_MIGRATION:-false}" != true || ! -e "$FAKE_KUBECTL_STATE" ]]; then
      readiness_probe='{"failureThreshold":3,"initialDelaySeconds":5,"periodSeconds":5,"successThreshold":1,"tcpSocket":{"port":"client"},"timeoutSeconds":1}'
    fi
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
  payload="$(jq -cn --arg uid "$statefulset_uid" --arg resourceVersion "$resource_version" --arg revision "$revision" --arg image "$runtime_image" --arg restart "$restart_annotation" --argjson prestop "$prestop" --argjson args "$args" --argjson readiness_probe "$readiness_probe" --argjson volume_mounts "$volume_mounts" --argjson volumes "$volumes" --argjson pod_security_context "$pod_security_context" --argjson replicas "$spec_replicas" --argjson termination_grace "$termination_grace" '{
    metadata:{uid:$uid,resourceVersion:$resourceVersion},
    spec:{replicas:$replicas,serviceName:"kubebrain-peer",template:{metadata:{labels:{app:"kubebrain"},annotations:{"kubectl.kubernetes.io/restartedAt":$restart}},spec:{terminationGracePeriodSeconds:$termination_grace,securityContext:$pod_security_context,containers:[{name:"kubebrain",image:$image,args:$args,readinessProbe:$readiness_probe,volumeMounts:$volume_mounts,lifecycle:{preStop:{exec:{command:$prestop}}}}],volumes:$volumes}}},
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
elif [[ " $* " == *" get pod kubebrain-rollout-availability-probe-cleanup "* ]]; then
  if [[ -e "${cleanup_state}-deleted" ]]; then
    [[ " $* " == *" --ignore-not-found "* ]] && exit 0
    exit 1
  fi
  if [[ -e "$cleanup_state" ]]; then
    jq -c . "$cleanup_state"
    exit 0
  fi
  if [[ "${FAKE_EXISTING_CLEANUP_POD:-false}" == true ]]; then
    receipt="$(jq -r '.data["receipt.json"]' "$owner_state")"
    [[ "${FAKE_EXISTING_CLEANUP_RECEIPT_DRIFT:-false}" != true ]] || receipt="${receipt}-drift"
    jq -cn --arg receipt "$receipt" '{
      metadata:{name:"kubebrain-rollout-availability-probe-cleanup",namespace:"kubebrain-system",
        uid:"55555555-5555-4555-8555-555555555555",resourceVersion:"cleanup-rv",deletionTimestamp:null,
        annotations:{"kubebrain.io/rollout-fixture-owner-uid":"44444444-4444-4444-8444-444444444444",
          "kubebrain.io/rollout-fixture-owner-receipt":$receipt},
        ownerReferences:[{apiVersion:"v1",kind:"ConfigMap",name:"kubebrain-rollout-availability-probe-owner",
          uid:"44444444-4444-4444-8444-444444444444",controller:true}]},
      spec:{automountServiceAccountToken:false,restartPolicy:"Never",
        securityContext:{runAsNonRoot:true,runAsUser:65532,runAsGroup:65532,fsGroup:65532},volumes:[],
        containers:[{name:"kubebrain-rollout-availability-probe-cleanup",image:"kubebrain:test",
          command:["/usr/local/bin/kubebrain-rollout-availability-probe"],volumeMounts:[],args:[
            "--cleanup-owned-fixture",
            "--prefix=/kubebrain-rollout-availability/kubebrain-rollout-availability-probe/",
            "--direct-endpoints=http://kubebrain-0.kubebrain-peer.kubebrain-system.svc:3379,http://kubebrain-1.kubebrain-peer.kubebrain-system.svc:3379,http://kubebrain-2.kubebrain-peer.kubebrain-system.svc:3379",
            "--command-timeout=10s","--dial-timeout=1s","--fixture-owner-namespace=kubebrain-system",
            "--fixture-owner-pod=kubebrain-rollout-availability-probe","--fixture-owner-statefulset=kubebrain",
            "--fixture-owner-statefulset-uid=statefulset-uid",
            "--fixture-owner-pod-uid=33333333-3333-4333-8333-333333333333",
            "--fixture-lease-ids=7101,7102,7103"]}]},
      status:{phase:"Succeeded"}}
    ' | tee "$cleanup_state"
    exit 0
  fi
  exit 1
elif [[ " $* " == *" run kubebrain-rollout-availability-probe-cleanup "* ]]; then
  overrides=""
  for argument in "$@"; do
    [[ "$argument" != --overrides=* ]] || overrides="${argument#--overrides=}"
  done
  [[ -n "$overrides" ]] || exit 1
  rm -f -- "${cleanup_state}-deleted"
  jq -c '.metadata.namespace="kubebrain-system" |
    .metadata.uid="55555555-5555-4555-8555-555555555555" |
    .metadata.resourceVersion="cleanup-rv" | .metadata.deletionTimestamp=null |
    .status.phase="Pending"' <<<"$overrides" >"$cleanup_state"
  jq -c . "$cleanup_state"
elif [[ " $* " == *" get pod kubebrain-rollout-availability-probe -o json "* ]]; then
  jq -cn '{
    metadata:{name:"kubebrain-rollout-availability-probe",uid:"33333333-3333-4333-8333-333333333333",resourceVersion:"probe-rv"},
    spec:{schedulingGates:[{name:"kubebrain.io/fixture-owner-receipt"}],containers:[{name:"kubebrain-rollout-availability-probe",image:(env.PROBE_IMAGE // "kubebrain:test")}]}}
  '
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
  pod_uid="${ordinal}-uid-old"
  [[ -z "${FAKE_HARD_FAILOVER_STATE:-}" || ! -e "$FAKE_HARD_FAILOVER_STATE" || "$ordinal" != kubebrain-1 ]] || pod_uid="${ordinal}-uid-new"
  jq -cn --arg name "$ordinal" --arg uid "$pod_uid" --arg image "$runtime_image" --arg imageID "$runtime_image_id" --arg revision "$runtime_revision" --argjson restartCount "$runtime_restart_count" --arg ready "$runtime_ready" '{
    metadata:{name:$name,uid:$uid,resourceVersion:("rv-"+$uid),labels:{"controller-revision-hash":$revision},ownerReferences:[{apiVersion:"apps/v1",kind:"StatefulSet",name:"kubebrain",uid:"statefulset-uid",controller:true}]},
    spec:{containers:[{name:"kubebrain",image:$image}]},
    status:{phase:"Running",conditions:[{type:"Ready",status:$ready}],containerStatuses:[{name:"kubebrain",ready:true,restartCount:$restartCount,imageID:$imageID}]}}
  '
elif [[ " $* " == *" exec kubebrain-rollout-availability-probe -- /usr/local/bin/kubebrain-rollout-availability-probe --leader-target-only "* ]]; then
  leader_pod=kubebrain-1
  leader_id=12
  if [[ -n "${FAKE_LEADER_DISCOVERY_STATE:-}" ]]; then
    discovery_count=0
    [[ ! -e "$FAKE_LEADER_DISCOVERY_STATE" ]] || discovery_count="$(<"$FAKE_LEADER_DISCOVERY_STATE")"
    discovery_count=$((discovery_count + 1))
    printf '%s' "$discovery_count" >"$FAKE_LEADER_DISCOVERY_STATE"
    [[ "${FAKE_LEADER_CHANGES_BEFORE_DELETE:-false}" != true || "$discovery_count" -lt 2 ]] || { leader_pod=kubebrain-2; leader_id=13; }
  fi
  if [[ -n "${FAKE_HARD_FAILOVER_STATE:-}" && -e "$FAKE_HARD_FAILOVER_STATE" ]]; then leader_pod=kubebrain-2; leader_id=13; fi
  printf 'LEADER_TARGET {"member_id":%s,"pod":"%s","peer_url":"https://%s.kubebrain-peer.kubebrain-system.svc.cluster.local:3380"}\n' "$leader_id" "$leader_pod" "$leader_pod"
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
  if [[ "$patch_path" == /spec ]]; then
    expected_spec="$(jq -cS '.[2].value // {}' <<<"${patch_payload:-[]}")"
    new_spec="$(jq -cS '.[-1].value // {}' <<<"${patch_payload:-[]}")"
    new_image="$(jq -r '.template.spec.containers[] | select(.name == "kubebrain") | .image // ""' <<<"$new_spec")"
    [[ "$expected_uid" == "$current_uid" && "$expected_resource_version" == "$current_resource_version" && -n "$expected_spec" && -n "$new_image" ]] || exit 1
    if [[ "$new_image" == kubebrain:test && "${FAKE_ROLLBACK_IDENTITY_DRIFT:-false}" != true ]]; then
      rm -f -- "$FAKE_KUBECTL_STATE"
      [[ -z "${FAKE_ROLLBACK_MARKER:-}" ]] || : >"$FAKE_ROLLBACK_MARKER"
    else
      : >"$FAKE_KUBECTL_STATE"
    fi
  elif [[ "$patch_path" == /spec/template/metadata/annotations ]]; then
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
elif [[ " $* " == *" wait --for=jsonpath={.status.phase}=Succeeded pod/kubebrain-rollout-availability-probe-cleanup "* ]]; then
  if [[ -e "$cleanup_state" ]]; then
    jq -c '.status.phase="Succeeded" | .metadata.resourceVersion="cleanup-rv-succeeded"' \
      "$cleanup_state" >"${cleanup_state}.next"
    mv -- "${cleanup_state}.next" "$cleanup_state"
  fi
  :
elif [[ " $* " == *" wait --for=jsonpath={.status.phase}=Succeeded "* ]]; then
  if [[ "${FAKE_PROBE_FAILED:-false}" == true ]]; then exit 1; fi
  if [[ "${FAKE_PHASE_RESPONSE:-false}" == true && ! -e "$FAKE_PHASE_STATE" ]]; then : >"$FAKE_PHASE_STATE"; exit 1; fi
elif [[ " $* " == *" logs kubebrain-rollout-availability-probe-cleanup "* ]]; then
  if [[ "${FAKE_OWNER_DELETE_DURING_CLEANUP:-false}" == true && -e "$owner_state" &&
    ! -e "${owner_state}.delete-injected" ]]; then
    jq -c '.metadata.deletionTimestamp="2026-08-28T08:00:00Z" |
      .metadata.resourceVersion="owner-rv-deleting"' "$owner_state" >"${owner_state}.next"
    mv -- "${owner_state}.next" "$owner_state"
    : >"${owner_state}.delete-injected"
  fi
  if [[ "${FAKE_FIXTURE_CLEANUP_MALFORMED:-false}" == true ]]; then
    printf '%s\n' 'FIXTURE_CLEANUP_OK status=recovered owner_uid=not-a-uid keys=0 users=0 roles=0 leases=0'
  else
    printf '%s\n' 'FIXTURE_CLEANUP_OK status=absent owner_uid= keys=0 users=0 roles=0 leases=0'
  fi
elif [[ " $* " == *" logs kubebrain-rollout-availability-probe "* ]]; then
  payload=$'PROBE_STARTED\nPROBE_SUMMARY ok=3 fail=0 total=3 watch=3 direct_watch=3x3 lease=alive lease_responses=7 public_lease_restarts=1 max_public_lease_recovery_ms=3210 direct_lease=alive direct_lease_responses=19 direct_lease_restarts=3 max_direct_lease_recovery_ms=27123 public_tcp_dials=2 min_direct_tcp_dials=2 direct_endpoints=3 range_stream=17 snapshot=2 stream_retries=4 stream_partial_retries=1 max_latency_ms=123 max_put_latency_ms=45 max_watch_after_put_latency_ms=78 max_direct_latency_ms=456 max_tso_latency_ms=12 max_region_latency_ms=34\n'
  if [[ "${HARD_FAILOVER:-false}" == true && -n "${FAKE_HARD_FAILOVER_STATE:-}" && -e "$FAKE_HARD_FAILOVER_STATE" && "${FAKE_OMIT_FINAL_LEADER_TARGET:-false}" != true ]]; then
    payload+=$'FINAL_LEADER_TARGET {"member_id":13,"pod":"kubebrain-2","peer_url":"https://kubebrain-2.kubebrain-peer.kubebrain-system.svc.cluster.local:3380"}\n'
  fi
  payload="${payload/public_tcp_dials=2/public_tcp_dials=${FAKE_PUBLIC_TCP_DIALS:-2}}"
  payload="${payload/min_direct_tcp_dials=2/min_direct_tcp_dials=${FAKE_MIN_DIRECT_TCP_DIALS:-2}}"
  [[ "${FAKE_PROBE_START_FAIL:-false}" != true ]] || payload=$'PROBE_FAIL invalid Snapshot scale put response\n'
  [[ "${FAKE_KEEPALIVE_QUEUE_FULL:-false}" != true ]] || payload=$'lease keepalive response queue is full; dropping response send\n'"$payload"
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

func writeRolloutAvailabilityUIDDelete(t *testing.T) (fakePath, logPath, statePath string) {
	t.Helper()
	dir := t.TempDir()
	fakePath = filepath.Join(dir, "uid-delete")
	logPath = filepath.Join(dir, "uid-delete.log")
	statePath = filepath.Join(dir, "hard-failover-state")
	script := `#!/usr/bin/env bash
set -euo pipefail
printf '%s\n' "$*" >>"$FAKE_UID_DELETE_LOG"
[[ " $* " == *" --api-version=v1 "* ]]
[[ " $* " == *" --namespace=kubebrain-system "* ]]
if [[ " $* " == *" --resource=configmaps "* ]]; then
  rm -f -- "${FAKE_KUBECTL_LOG}.owner"
  if [[ -e "${FAKE_KUBECTL_LOG}.cleanup" ]] && jq -e '
    [.metadata.ownerReferences[]? | select(.kind == "ConfigMap" and
      .name == "kubebrain-rollout-availability-probe-owner" and .controller == true)] | length == 1
  ' "${FAKE_KUBECTL_LOG}.cleanup" >/dev/null; then
    rm -f -- "${FAKE_KUBECTL_LOG}.cleanup"
    : >"${FAKE_KUBECTL_LOG}.cleanup-deleted"
  fi
else
  [[ " $* " == *" --resource=pods "* ]]
  if [[ " $* " == *" --name=kubebrain-rollout-availability-probe-cleanup "* ]]; then
    rm -f -- "${FAKE_KUBECTL_LOG}.cleanup"
    : >"${FAKE_KUBECTL_LOG}.cleanup-deleted"
  else
    [[ " $* " == *" --name=kubebrain-1 "* ]]
    [[ " $* " == *" --uid=kubebrain-1-uid-old "* ]]
    [[ " $* " == *" --resource-version=rv-kubebrain-1-uid-old "* ]]
    [[ " $* " == *" --grace-period-seconds=0 "* ]]
    : >"$FAKE_HARD_FAILOVER_STATE"
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
