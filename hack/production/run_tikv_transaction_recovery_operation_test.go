package production_test

import (
	"crypto/sha256"
	"fmt"
	"os"
	"os/exec"
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
  claim_name="${CLAIM_NAME:-tikv-recovery-dbea00c4a1e7fae49690}"
  printf '{"namespace":"%s","name":"%s","operation_id":"%s","requested_by":"%s","instance":"kubebrain","type":"%s","parameters_sha256":"%s","parameters_secret":"%s-parameters","parameters_key":"parameters.json","owner":"%s","attempt":2}\n' \
    "${CLAIM_NAMESPACE:-kubebrain-repair-operations}" "$claim_name" "$claim_name" "${CLAIM_REQUESTER:-platform:tikv-repair-recovery}" \
    "${CLAIM_TYPE:-TiKVTransactionRecovery}" "$EXPECTED_DIGEST" "$claim_name" "${CLAIM_OWNER:-$WORKER_ID}"
elif [[ "$*" == *"--action heartbeat"* ]]; then
  [[ "${FAIL_HEARTBEAT:-false}" != true ]] || exit 1
elif [[ "$*" == *"--action parameters"* ]]; then
  cat "$PARAMETERS_SOURCE"
fi
`), 0o755))
	recovery := filepath.Join(dir, "recovery")
	require.NoError(t, os.WriteFile(recovery, []byte(`#!/usr/bin/env bash
set -euo pipefail
env | sort >"$RECOVERY_LOG"
[[ "${FAKE_RECOVERY_FAIL:-false}" != "true" ]] || exit 9
printf '{"attempt_id":"%s","cluster_id":%s,"completed_at_unix":%s,"format":"kubebrain.tikv-repair-recovery.receipt.v1","kubebrain_statefulset_uid":"%s","ready_replicas":3,"request_id":"%s","storage_health_verified":true,"tidb_cluster_uid":"%s","transaction_verified":true}\n' \
  "$RECOVERY_ATTEMPT_ID" "$EXPECTED_CLUSTER_ID" "${FAKE_COMPLETED_AT_UNIX:-1786380000}" "$EXPECTED_KUBEBRAIN_STATEFULSET_UID" "$RECOVERY_REQUEST_ID" "$EXPECTED_TIDB_CLUSTER_UID" >"$RECEIPT_OUTPUT"
`), 0o755))
	realCP, err := exec.LookPath("cp")
	require.NoError(t, err)
	cp := filepath.Join(dir, "cp")
	writeTrafficExecutable(t, cp, `#!/usr/bin/env bash
set -euo pipefail
"$REAL_CP" "$@"
if [[ "${TAMPER_FROZEN_PARAMETERS_SIZE:-false}" == true && "$#" == 3 && "$1" == -- ]]; then
  printf '%65536s' '' >>"$3"
fi
`)

	baseEnv := []string{
		"WORKER_ID=worker-1", "PARAMETERS_INPUT=" + parameters,
		"OPERATIONCTL=" + operationctl, "RECOVERY_COMMAND=" + recovery, "WORK_DIR=" + dir,
		"HEARTBEAT_INTERVAL_SECONDS=0.1", "EXPECTED_DIGEST=" + digest,
		"OPERATION_LOG=" + operationLog, "RECOVERY_LOG=" + recoveryLog,
		"PATH=" + dir + string(os.PathListSeparator) + os.Getenv("PATH"), "REAL_CP=" + realCP,
	}
	output, err := runProductionScriptCommand(t, "run-tikv-transaction-recovery-operation.sh", baseEnv)
	require.NoError(t, err, string(output))
	operationData, err := os.ReadFile(operationLog)
	require.NoError(t, err)
	require.Contains(t, string(operationData), "--type TiKVTransactionRecovery")
	require.Contains(t, string(operationData), "--action succeed")
	require.Contains(t, string(operationData), "--receipt-sha256")
	lastHeartbeat := strings.LastIndex(string(operationData), "--action heartbeat")
	require.GreaterOrEqual(t, lastHeartbeat, 0)
	require.Greater(t, strings.LastIndex(string(operationData), "--action succeed"), lastHeartbeat)
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

	require.NoError(t, os.WriteFile(operationLog, nil, 0o600))
	heartbeatOutput, heartbeatErr := runProductionScriptCommand(t, "run-tikv-transaction-recovery-operation.sh", append(baseEnv,
		"WORKER_ID=worker-fenced", "FAKE_RECOVERY_FAIL=false", "FAIL_HEARTBEAT=true", "HEARTBEAT_INTERVAL_SECONDS=10"))
	require.Error(t, heartbeatErr, string(heartbeatOutput))
	require.Contains(t, string(heartbeatOutput), "final heartbeat failed; recovery worker was fenced")
	heartbeatOperations, err := os.ReadFile(operationLog)
	require.NoError(t, err)
	require.NotContains(t, string(heartbeatOperations), "--action succeed")
	require.NotContains(t, string(heartbeatOperations), "--action fail")

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

	oversizedBytes := append(append([]byte{}, parameterBytes...), []byte(strings.Repeat(" ", 65536))...)
	require.NoError(t, os.WriteFile(parameters, oversizedBytes, 0o600))
	oversizedDigest := fmt.Sprintf("%x", sha256.Sum256(oversizedBytes))
	require.NoError(t, os.WriteFile(operationLog, nil, 0o600))
	require.NoError(t, os.WriteFile(recoveryLog, nil, 0o600))
	oversizedOutput, oversizedErr := runProductionScriptCommand(t, "run-tikv-transaction-recovery-operation.sh", []string{
		"WORKER_ID=worker-oversized", "PARAMETERS_INPUT=" + parameters,
		"OPERATIONCTL=" + operationctl, "RECOVERY_COMMAND=" + recovery, "WORK_DIR=" + dir,
		"HEARTBEAT_INTERVAL_SECONDS=0.1", "EXPECTED_DIGEST=" + oversizedDigest,
		"OPERATION_LOG=" + operationLog, "RECOVERY_LOG=" + recoveryLog,
	})
	require.Error(t, oversizedErr)
	require.Contains(t, string(oversizedOutput), "operation parameters exceed 65536 bytes")
	oversizedOperations := string(mustRead(t, operationLog))
	require.Contains(t, oversizedOperations, "--action retry")
	require.NotContains(t, oversizedOperations, "--action succeed")
	require.Empty(t, mustRead(t, recoveryLog))

	require.NoError(t, os.WriteFile(operationLog, nil, 0o600))
	managedOutput, managedErr := runProductionScriptCommand(t, "run-tikv-transaction-recovery-operation.sh", []string{
		"WORKER_ID=worker-managed-oversized", "OPERATIONCTL=" + operationctl,
		"RECOVERY_COMMAND=" + recovery, "WORK_DIR=" + dir, "HEARTBEAT_INTERVAL_SECONDS=0.1",
		"EXPECTED_DIGEST=" + oversizedDigest, "PARAMETERS_SOURCE=" + parameters,
		"OPERATION_LOG=" + operationLog, "RECOVERY_LOG=" + recoveryLog,
	})
	require.Error(t, managedErr)
	require.Contains(t, string(managedOutput), "operation parameters exceed 65536 bytes")
	require.Contains(t, string(mustRead(t, operationLog)), "--action parameters")

	require.NoError(t, os.WriteFile(parameters, parameterBytes, 0o600))
	require.NoError(t, os.WriteFile(operationLog, nil, 0o600))
	tamperOutput, tamperErr := runProductionScriptCommand(t, "run-tikv-transaction-recovery-operation.sh", append(baseEnv,
		"WORKER_ID=worker-frozen-oversized", "TAMPER_FROZEN_PARAMETERS_SIZE=true"))
	require.Error(t, tamperErr)
	require.Contains(t, string(tamperOutput), "operation parameters exceed 65536 bytes")
	tamperOperations := string(mustRead(t, operationLog))
	require.Contains(t, tamperOperations, "--action retry")
	require.NotContains(t, tamperOperations, "--action succeed")

	exactBytes := append(append([]byte{}, parameterBytes...), []byte(strings.Repeat(" ", 65536-len(parameterBytes)))...)
	require.Len(t, exactBytes, 65536)
	require.NoError(t, os.WriteFile(parameters, exactBytes, 0o600))
	exactDigest := fmt.Sprintf("%x", sha256.Sum256(exactBytes))
	require.NoError(t, os.WriteFile(operationLog, nil, 0o600))
	exactOutput, exactErr := runProductionScriptCommand(t, "run-tikv-transaction-recovery-operation.sh", []string{
		"WORKER_ID=worker-exact-limit", "PARAMETERS_INPUT=" + parameters,
		"OPERATIONCTL=" + operationctl, "RECOVERY_COMMAND=" + recovery, "WORK_DIR=" + dir,
		"HEARTBEAT_INTERVAL_SECONDS=0.1", "EXPECTED_DIGEST=" + exactDigest,
		"OPERATION_LOG=" + operationLog, "RECOVERY_LOG=" + recoveryLog,
	})
	require.NoError(t, exactErr, string(exactOutput))
	require.Contains(t, string(mustRead(t, operationLog)), "--action succeed")

	maxBytes := []byte(strings.Replace(string(parameterBytes), `"expected_cluster_id":7671`, `"expected_cluster_id":18446744073709551615`, 1))
	require.NoError(t, os.WriteFile(parameters, maxBytes, 0o600))
	maxDigest := fmt.Sprintf("%x", sha256.Sum256(maxBytes))
	for _, receipt := range receipts {
		_ = os.Remove(receipt)
	}
	require.NoError(t, os.WriteFile(recoveryLog, nil, 0o600))
	maxOutput, maxErr := runProductionScriptCommand(t, "run-tikv-transaction-recovery-operation.sh", []string{
		"WORKER_ID=worker-max", "PARAMETERS_INPUT=" + parameters, "OPERATIONCTL=" + operationctl,
		"RECOVERY_COMMAND=" + recovery, "WORK_DIR=" + dir, "HEARTBEAT_INTERVAL_SECONDS=0.1",
		"EXPECTED_DIGEST=" + maxDigest, "OPERATION_LOG=" + operationLog, "RECOVERY_LOG=" + recoveryLog,
		"CLAIM_NAME=tikv-recovery-6c91f9b2718f23c8950e",
	})
	require.NoError(t, maxErr, string(maxOutput))
	require.Contains(t, string(mustRead(t, recoveryLog)), "EXPECTED_CLUSTER_ID=18446744073709551615")

	maxReceipts, err := filepath.Glob(filepath.Join(dir, "tikv-recovery-*.receipt.json"))
	require.NoError(t, err)
	require.Len(t, maxReceipts, 1)
	require.Contains(t, string(mustRead(t, maxReceipts[0])), `"cluster_id":18446744073709551615`)
	require.NoError(t, os.Remove(maxReceipts[0]))
	overflowBytes := []byte(strings.Replace(string(parameterBytes), `"expected_cluster_id":7671`, `"expected_cluster_id":18446744073709551616`, 1))
	require.NoError(t, os.WriteFile(parameters, overflowBytes, 0o600))
	overflowDigest := fmt.Sprintf("%x", sha256.Sum256(overflowBytes))
	require.NoError(t, os.WriteFile(recoveryLog, nil, 0o600))
	overflowOutput, overflowErr := runProductionScriptCommand(t, "run-tikv-transaction-recovery-operation.sh", []string{
		"WORKER_ID=worker-overflow", "PARAMETERS_INPUT=" + parameters, "OPERATIONCTL=" + operationctl,
		"RECOVERY_COMMAND=" + recovery, "WORK_DIR=" + dir, "HEARTBEAT_INTERVAL_SECONDS=0.1",
		"EXPECTED_DIGEST=" + overflowDigest, "OPERATION_LOG=" + operationLog, "RECOVERY_LOG=" + recoveryLog,
	})
	require.Error(t, overflowErr)
	require.Contains(t, string(overflowOutput), "cluster identity is not a positive uint64")
	require.Empty(t, mustRead(t, recoveryLog))

	for _, field := range []string{"probe_timeout_seconds", "pod_ready_timeout_seconds"} {
		overflowNumeric := []byte(strings.Replace(string(parameterBytes), `"`+field+`":`+map[string]string{
			"probe_timeout_seconds": "10", "pod_ready_timeout_seconds": "300",
		}[field], `"`+field+`":9223372036854775808`, 1))
		require.NoError(t, os.WriteFile(parameters, overflowNumeric, 0o600))
		numericDigest := fmt.Sprintf("%x", sha256.Sum256(overflowNumeric))
		require.NoError(t, os.WriteFile(recoveryLog, nil, 0o600))
		numericOutput, numericErr := runProductionScriptCommand(t, "run-tikv-transaction-recovery-operation.sh", []string{
			"WORKER_ID=worker-" + field, "PARAMETERS_INPUT=" + parameters, "OPERATIONCTL=" + operationctl,
			"RECOVERY_COMMAND=" + recovery, "WORK_DIR=" + dir, "HEARTBEAT_INTERVAL_SECONDS=0.1",
			"EXPECTED_DIGEST=" + numericDigest, "OPERATION_LOG=" + operationLog, "RECOVERY_LOG=" + recoveryLog,
		})
		require.Error(t, numericErr)
		require.Contains(t, string(numericOutput), "not a positive int64")
		require.Empty(t, mustRead(t, recoveryLog))
	}

	require.NoError(t, os.WriteFile(parameters, parameterBytes, 0o600))
	for _, receipt := range maxReceipts {
		_ = os.Remove(receipt)
	}
	require.NoError(t, os.WriteFile(recoveryLog, nil, 0o600))
	maxReceiptTimeOutput, maxReceiptTimeErr := runProductionScriptCommand(t, "run-tikv-transaction-recovery-operation.sh", append(baseEnv,
		"WORKER_ID=worker-max-receipt-time", "FAKE_COMPLETED_AT_UNIX=9223372036854775807"))
	require.NoError(t, maxReceiptTimeErr, string(maxReceiptTimeOutput))
	timeReceipts, err := filepath.Glob(filepath.Join(dir, "tikv-recovery-*.receipt.json"))
	require.NoError(t, err)
	require.Len(t, timeReceipts, 1)
	require.Contains(t, string(mustRead(t, timeReceipts[0])), `"completed_at_unix":9223372036854775807`)
	require.NoError(t, os.Remove(timeReceipts[0]))
	require.NoError(t, os.WriteFile(recoveryLog, nil, 0o600))
	receiptTimeOutput, receiptTimeErr := runProductionScriptCommand(t, "run-tikv-transaction-recovery-operation.sh", append(baseEnv,
		"WORKER_ID=worker-receipt-time", "FAKE_COMPLETED_AT_UNIX=9223372036854775808"))
	require.Error(t, receiptTimeErr)
	require.Empty(t, receiptTimeOutput)
	require.Contains(t, string(mustRead(t, operationLog)), "recovery receipt invalid")
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
