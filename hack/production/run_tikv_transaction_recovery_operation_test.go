package production_test

import (
	"crypto/sha256"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRunTiKVTransactionRecoveryOperation(t *testing.T) {
	dir := t.TempDir()
	parameters := filepath.Join(dir, "parameters.json")
	parameterBytes := []byte(`{"endpoint":"http://kubebrain-client.kubebrain-system.svc:3379","expected_cluster_id":7671,"expected_kubebrain_statefulset_uid":"kb-uid","expected_tidb_cluster_uid":"tc-uid","kubebrain_namespace":"kubebrain-system","kubebrain_statefulset":"kubebrain","pod_ready_timeout_seconds":300,"probe_timeout_seconds":10,"request_id":"change-2026-001","tidb_cluster":"kb","tidb_namespace":"tidb-cluster"}`)
	require.NoError(t, os.WriteFile(parameters, parameterBytes, 0o600))
	digest := fmt.Sprintf("%x", sha256.Sum256(parameterBytes))
	operationLog := filepath.Join(dir, "operation.log")
	recoveryLog := filepath.Join(dir, "recovery.log")
	operationctl := filepath.Join(dir, "operationctl")
	require.NoError(t, os.WriteFile(operationctl, []byte(`#!/usr/bin/env bash
set -euo pipefail
printf '%s\n' "$*" >>"$OPERATION_LOG"
if [[ "$*" == *"--action claim"* ]]; then
  printf '{"namespace":"%s","name":"tikv-recovery-dbea00c4a1e7fae49690","operation_id":"tikv-recovery-dbea00c4a1e7fae49690","requested_by":"%s","instance":"kubebrain","type":"%s","parameters_sha256":"%s","parameters_secret":"tikv-recovery-dbea00c4a1e7fae49690-parameters","parameters_key":"parameters.json","owner":"%s","attempt":2}\n' \
    "${CLAIM_NAMESPACE:-kubebrain-repair-operations}" "${CLAIM_REQUESTER:-platform:tikv-repair-recovery}" \
    "${CLAIM_TYPE:-TiKVTransactionRecovery}" "$EXPECTED_DIGEST" "${CLAIM_OWNER:-$WORKER_ID}"
fi
`), 0o755))
	recovery := filepath.Join(dir, "recovery")
	require.NoError(t, os.WriteFile(recovery, []byte(`#!/usr/bin/env bash
set -euo pipefail
env | sort >"$RECOVERY_LOG"
[[ "${FAKE_RECOVERY_FAIL:-false}" != "true" ]] || exit 9
printf '{"attempt_id":"%s","cluster_id":%s,"completed_at_unix":1786380000,"format":"kubebrain.tikv-repair-recovery.receipt.v1","kubebrain_statefulset_uid":"%s","ready_replicas":3,"request_id":"%s","storage_health_verified":true,"tidb_cluster_uid":"%s","transaction_verified":true}\n' \
  "$RECOVERY_ATTEMPT_ID" "$EXPECTED_CLUSTER_ID" "$EXPECTED_KUBEBRAIN_STATEFULSET_UID" "$RECOVERY_REQUEST_ID" "$EXPECTED_TIDB_CLUSTER_UID" >"$RECEIPT_OUTPUT"
`), 0o755))

	baseEnv := []string{
		"WORKER_ID=worker-1", "PARAMETERS_INPUT=" + parameters,
		"OPERATIONCTL=" + operationctl, "RECOVERY_COMMAND=" + recovery, "WORK_DIR=" + dir,
		"HEARTBEAT_INTERVAL_SECONDS=0.1", "EXPECTED_DIGEST=" + digest,
		"OPERATION_LOG=" + operationLog, "RECOVERY_LOG=" + recoveryLog,
	}
	output, err := runProductionScriptCommand(t, "run-tikv-transaction-recovery-operation.sh", baseEnv)
	require.NoError(t, err, string(output))
	operationData, err := os.ReadFile(operationLog)
	require.NoError(t, err)
	require.Contains(t, string(operationData), "--type TiKVTransactionRecovery")
	require.Contains(t, string(operationData), "--action succeed")
	require.Contains(t, string(operationData), "--receipt-sha256")
	recoveryData, err := os.ReadFile(recoveryLog)
	require.NoError(t, err)
	recoveryEnv := string(recoveryData)
	require.Contains(t, recoveryEnv, "ALLOW_KUBEBRAIN_RECOVERY=true")
	require.Contains(t, recoveryEnv, "KUBE_CONTEXT=in-cluster")
	require.Contains(t, recoveryEnv, "EXPECTED_TIDB_CLUSTER_UID=tc-uid")
	require.Contains(t, recoveryEnv, "RECOVERY_ATTEMPT_ID=op-")
	require.Contains(t, recoveryEnv, "RECOVERY_REQUEST_ID=change-2026-001")

	require.NoError(t, os.WriteFile(recoveryLog, nil, 0o600))
	takeoverOutput, takeoverErr := runProductionScriptCommand(t, "run-tikv-transaction-recovery-operation.sh", append(baseEnv, "WORKER_ID=worker-2"))
	require.NoError(t, takeoverErr, string(takeoverOutput))
	takeoverRecovery, err := os.ReadFile(recoveryLog)
	require.NoError(t, err)
	require.Empty(t, takeoverRecovery, "takeover must verify the durable receipt without scaling twice")

	receipts, err := filepath.Glob(filepath.Join(dir, "tikv-recovery-*.receipt.json"))
	require.NoError(t, err)
	require.Len(t, receipts, 1)
	require.NoError(t, os.Remove(receipts[0]))
	require.NoError(t, os.WriteFile(operationLog, nil, 0o600))
	failureOutput, failureErr := runProductionScriptCommand(t, "run-tikv-transaction-recovery-operation.sh", append(baseEnv, "WORKER_ID=worker-3", "FAKE_RECOVERY_FAIL=true"))
	require.Error(t, failureErr, string(failureOutput))
	failureOperations, err := os.ReadFile(operationLog)
	require.NoError(t, err)
	require.Contains(t, string(failureOperations), "--action fail")
	require.NotContains(t, string(failureOperations), "--action retry")

	extraBytes := []byte(strings.TrimSuffix(string(parameterBytes), "}") + `,"unreviewed":true}`)
	require.NoError(t, os.WriteFile(parameters, extraBytes, 0o600))
	extraDigest := fmt.Sprintf("%x", sha256.Sum256(extraBytes))
	require.NoError(t, os.WriteFile(recoveryLog, nil, 0o600))
	extraOutput, extraErr := runProductionScriptCommand(t, "run-tikv-transaction-recovery-operation.sh", []string{
		"WORKER_ID=worker-4", "PARAMETERS_INPUT=" + parameters, "OPERATIONCTL=" + operationctl,
		"RECOVERY_COMMAND=" + recovery, "WORK_DIR=" + dir, "HEARTBEAT_INTERVAL_SECONDS=0.1",
		"EXPECTED_DIGEST=" + extraDigest, "OPERATION_LOG=" + operationLog, "RECOVERY_LOG=" + recoveryLog,
	})
	require.Error(t, extraErr)
	require.Contains(t, string(extraOutput), "recovery parameter schema is invalid")
	extraRecovery, err := os.ReadFile(recoveryLog)
	require.NoError(t, err)
	require.Empty(t, extraRecovery)

	driftedBytes := []byte(strings.Replace(string(parameterBytes),
		`"request_id":"change-2026-001"`, `"request_id":"change-2026-002"`, 1))
	require.NoError(t, os.WriteFile(parameters, driftedBytes, 0o600))
	driftedDigest := fmt.Sprintf("%x", sha256.Sum256(driftedBytes))
	require.NoError(t, os.WriteFile(recoveryLog, nil, 0o600))
	driftOutput, driftErr := runProductionScriptCommand(t, "run-tikv-transaction-recovery-operation.sh", []string{
		"WORKER_ID=worker-5", "PARAMETERS_INPUT=" + parameters, "OPERATIONCTL=" + operationctl,
		"RECOVERY_COMMAND=" + recovery, "WORK_DIR=" + dir, "HEARTBEAT_INTERVAL_SECONDS=0.1",
		"EXPECTED_DIGEST=" + driftedDigest, "OPERATION_LOG=" + operationLog, "RECOVERY_LOG=" + recoveryLog,
	})
	require.Error(t, driftErr)
	require.Contains(t, string(driftOutput), "recovery request identity does not match the operation")
	driftRecovery, err := os.ReadFile(recoveryLog)
	require.NoError(t, err)
	require.Empty(t, driftRecovery, "a replaced request ID must fail before scaling")
}

func TestRunTiKVTransactionRecoveryOperationRejectsInvalidClaimNamespace(t *testing.T) {
	dir := t.TempDir()
	operationctlLog := filepath.Join(dir, "operationctl.log")
	operationctl := filepath.Join(dir, "operationctl")
	require.NoError(t, os.WriteFile(operationctl, []byte(`#!/usr/bin/env bash
set -euo pipefail
printf called >"$OPERATIONCTL_LOG"
printf '{"namespace":"%s","type":"TiKVTransactionRecovery","requested_by":"platform:tikv-repair-recovery","owner":"worker-a","parameters_secret":"p","parameters_key":"parameters.json"}\n' "$CLAIM_NAMESPACE"
`), 0o755))
	recoveryLog := filepath.Join(dir, "recovery.log")
	recovery := filepath.Join(dir, "recovery")
	require.NoError(t, os.WriteFile(recovery, []byte("#!/usr/bin/env bash\nprintf called >\"$RECOVERY_LOG\"\n"), 0o755))
	output, err := runProductionScriptCommand(t, "run-tikv-transaction-recovery-operation.sh", []string{
		"WORKER_ID=worker-a", "OPERATIONCTL=" + operationctl, "RECOVERY_COMMAND=" + recovery,
		"WORK_DIR=" + dir, "OPERATIONCTL_LOG=" + operationctlLog, "RECOVERY_LOG=" + recoveryLog,
		"CLAIM_NAMESPACE=tenant/a",
	})
	require.Error(t, err)
	require.Contains(t, string(output), "OPERATION_NAMESPACE must be a lowercase DNS label")
	require.NoFileExists(t, recoveryLog)
}
