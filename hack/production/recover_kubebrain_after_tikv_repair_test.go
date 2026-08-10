package production_test

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRecoverKubeBrainAfterTiKVRepair(t *testing.T) {
	t.Run("success runs storage gate on both sides of scale and verifies transaction", func(t *testing.T) {
		env, logPath := recoveryFixture(t)
		output, err := runProductionScriptCommand(t, "recover-kubebrain-after-tikv-repair.sh", env)
		require.NoError(t, err, string(output))
		require.Contains(t, string(output), "KubeBrain recovery succeeded")
		receipt, readErr := os.ReadFile(recoveryEnvValue(env, "RECEIPT_OUTPUT"))
		require.NoError(t, readErr)
		require.JSONEq(t, `{"attempt_id":"recovery-1","cluster_id":7671,"completed_at_unix":1786380000,"format":"kubebrain.tikv-repair-recovery.receipt.v1","kubebrain_statefulset_uid":"kb-uid","ready_replicas":3,"request_id":"change-2026-001","storage_health_verified":true,"tidb_cluster_uid":"tc-uid","transaction_verified":true}`, string(receipt))
		log := readRecoveryLog(t, logPath)
		require.Equal(t, 2, strings.Count(log, "region-health\n"), log)
		requireOrder(t, log, "region-health", "scale statefulset kubebrain --replicas=3", "rollout status statefulset/kubebrain", "exec kubebrain-0 -c kubebrain -- etcdctl")
		require.NotContains(t, log, "--replicas=0")
	})

	t.Run("storage failure has no scale side effect", func(t *testing.T) {
		env, logPath := recoveryFixture(t)
		env = append(env, "FAKE_REGION_FAIL_AT=1")
		output, err := runProductionScriptCommand(t, "recover-kubebrain-after-tikv-repair.sh", env)
		require.Error(t, err, string(output))
		log := readRecoveryLog(t, logPath)
		require.NotContains(t, log, "scale statefulset")
	})

	t.Run("zero replica identity drift after health gate has no scale side effect", func(t *testing.T) {
		env, logPath := recoveryFixture(t)
		env = append(env, "FAKE_REGION_DRIFT_UID_AT=1")
		output, err := runProductionScriptCommand(t, "recover-kubebrain-after-tikv-repair.sh", env)
		require.Error(t, err)
		require.Contains(t, string(output), "identity changed after the storage health gate")
		log := readRecoveryLog(t, logPath)
		require.NotContains(t, log, "scale statefulset")
	})

	t.Run("post-scale health failure rolls the same StatefulSet back to zero", func(t *testing.T) {
		env, logPath := recoveryFixture(t)
		env = append(env, "FAKE_REGION_FAIL_AT=2")
		output, err := runProductionScriptCommand(t, "recover-kubebrain-after-tikv-repair.sh", env)
		require.Error(t, err, string(output))
		log := readRecoveryLog(t, logPath)
		requireOrder(t, log, "scale statefulset kubebrain --replicas=3", "region-health", "scale statefulset kubebrain --replicas=0")
	})

	t.Run("post-scale UID replacement is never rolled back by name", func(t *testing.T) {
		env, logPath := recoveryFixture(t)
		env = append(env, "FAKE_REGION_DRIFT_UID_AT=2", "FAKE_REGION_FAIL_AT=2")
		output, err := runProductionScriptCommand(t, "recover-kubebrain-after-tikv-repair.sh", env)
		require.Error(t, err, string(output))
		log := readRecoveryLog(t, logPath)
		require.Contains(t, log, "scale statefulset kubebrain --replicas=3")
		require.NotContains(t, log, "--replicas=0",
			"rollback must not mutate a replacement StatefulSet with the same namespace/name")
	})

	t.Run("transaction failure rolls back to zero", func(t *testing.T) {
		env, logPath := recoveryFixture(t)
		env = append(env, "FAKE_TRANSACTION_FAIL=true")
		output, err := runProductionScriptCommand(t, "recover-kubebrain-after-tikv-repair.sh", env)
		require.Error(t, err)
		require.Contains(t, string(output), "transaction verification failed")
		log := readRecoveryLog(t, logPath)
		requireOrder(t, log, "etcdctl --endpoints=http://kubebrain:3379 put", "scale statefulset kubebrain --replicas=0")
	})
}

func TestRecoverKubeBrainAfterTiKVRepairRequiresExplicitAuthorization(t *testing.T) {
	output, err := runProductionScriptCommand(t, "recover-kubebrain-after-tikv-repair.sh", []string{"KUBE_CONTEXT=test"})
	require.Error(t, err)
	require.Contains(t, string(output), "ALLOW_KUBEBRAIN_RECOVERY=true")
}

func recoveryFixture(t *testing.T) ([]string, string) {
	t.Helper()
	dir := t.TempDir()
	statePath := filepath.Join(dir, "state")
	uidPath := filepath.Join(dir, "uid")
	logPath := filepath.Join(dir, "calls.log")
	regionCountPath := filepath.Join(dir, "region-count")
	receiptPath := filepath.Join(dir, "recovery.receipt.json")
	require.NoError(t, os.WriteFile(statePath, []byte("0"), 0o600))
	require.NoError(t, os.WriteFile(uidPath, []byte("kb-uid"), 0o600))
	dateCommand := filepath.Join(dir, "date")
	require.NoError(t, os.WriteFile(dateCommand, []byte("#!/usr/bin/env bash\nprintf '1786380000\\n'\n"), 0o755))

	fakeKubectl := filepath.Join(dir, "kubectl")
	require.NoError(t, os.WriteFile(fakeKubectl, []byte(`#!/usr/bin/env bash
set -euo pipefail
printf '%s\n' "$*" >>"$RECOVERY_LOG"
args=" $* "
if [[ "$args" == *" get statefulset kubebrain -o json "* ]]; then
  replicas="$(<"$RECOVERY_STATE")"
  uid="$(<"$RECOVERY_UID")"
  printf '{"metadata":{"uid":"%s"},"spec":{"replicas":%s},"status":{"readyReplicas":%s}}\n' "$uid" "$replicas" "$replicas"
elif [[ "$args" == *" get statefulset kubebrain -o jsonpath={.metadata.uid} "* ]]; then
  cat "$RECOVERY_UID"
elif [[ "$args" == *" get statefulset kubebrain -o jsonpath="* ]]; then
  printf ''
elif [[ "$args" == *" get tidbcluster kb -o json "* ]]; then
  printf '{"metadata":{"uid":"tc-uid"},"spec":{"pd":{"replicas":3},"tikv":{"replicas":3}},"status":{"clusterID":7671,"conditions":[{"type":"Ready","status":"True"}]}}\n'
elif [[ "$args" == *" scale statefulset kubebrain --replicas="* ]]; then
  replicas="${args##*--replicas=}"
  replicas="${replicas%% *}"
  printf '%s' "$replicas" >"$RECOVERY_STATE"
elif [[ "$args" == *" rollout status statefulset/kubebrain "* ]]; then
  :
elif [[ "$args" == *" exec kubebrain-0 -c kubebrain -- etcdctl "* ]]; then
  if [[ "$args" == *" put "* ]]; then
    [[ "${FAKE_TRANSACTION_FAIL:-false}" != "true" ]] || exit 9
    printf 'OK\n'
  elif [[ "$args" == *" get "* ]]; then
    printf 'recovered-tc-uid\n'
  elif [[ "$args" == *" del "* ]]; then
    printf '1\n'
  fi
else
  echo "unsupported fake kubectl call: $*" >&2
  exit 97
fi
`), 0o755))

	regionGate := filepath.Join(dir, "region-health")
	require.NoError(t, os.WriteFile(regionGate, []byte(`#!/usr/bin/env bash
set -euo pipefail
count=0
[[ ! -f "$REGION_COUNT" ]] || count="$(<"$REGION_COUNT")"
count=$((count + 1))
printf '%s' "$count" >"$REGION_COUNT"
printf 'region-health\n' >>"$RECOVERY_LOG"
if [[ "${FAKE_REGION_DRIFT_UID_AT:-0}" == "$count" ]]; then
  printf 'drifted-kb-uid' >"$RECOVERY_UID"
fi
[[ "${FAKE_REGION_FAIL_AT:-0}" != "$count" ]]
`), 0o755))

	return []string{
		"KUBE_CONTEXT=test-context",
		"ALLOW_KUBEBRAIN_RECOVERY=true",
		"KUBEBRAIN_NAMESPACE=kubebrain-dev",
		"EXPECTED_KUBEBRAIN_STATEFULSET_UID=kb-uid",
		"EXPECTED_TIDB_CLUSTER_UID=tc-uid",
		"EXPECTED_CLUSTER_ID=7671",
		"ENDPOINT=http://kubebrain:3379",
		"RECOVERY_ATTEMPT_ID=recovery-1",
		"RECOVERY_REQUEST_ID=change-2026-001",
		"RECEIPT_OUTPUT=" + receiptPath,
		"DATE=" + dateCommand,
		"KUBECTL=" + fakeKubectl,
		"REGION_HEALTH_COMMAND=" + regionGate,
		"RECOVERY_LOG=" + logPath,
		"RECOVERY_STATE=" + statePath,
		"RECOVERY_UID=" + uidPath,
		"REGION_COUNT=" + regionCountPath,
	}, logPath
}

func recoveryEnvValue(env []string, key string) string {
	prefix := key + "="
	for _, value := range env {
		if strings.HasPrefix(value, prefix) {
			return strings.TrimPrefix(value, prefix)
		}
	}
	return ""
}

func readRecoveryLog(t *testing.T, path string) string {
	t.Helper()
	data, err := os.ReadFile(path)
	require.NoError(t, err)
	return string(data)
}
