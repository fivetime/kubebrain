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

func TestRunTiKVTransactionRepairOperation(t *testing.T) {
	tempDir := t.TempDir()
	parameters := filepath.Join(tempDir, "parameters.json")
	parameterBytes := []byte(`{"alert_fingerprint":"abcdef0123456789abcdef0123456789","alert_occurrence_id":"1bb5a469de669cd3422d","alert_starts_at":"2026-08-09T05:00:00Z","endpoint":"http://kubebrain-client.kubebrain-system.svc:3379","kubebrain_namespace":"kubebrain-system","kubebrain_statefulset":"kubebrain","tidb_namespace":"tidb-cluster","tidb_cluster":"kb","expected_kubebrain_statefulset_uid":"kb-uid","expected_tidb_cluster_uid":"tc-uid","expected_cluster_id":7671,"required_failed_probes":3,"probe_interval_seconds":5,"probe_timeout_seconds":10,"pod_ready_timeout_seconds":300,"repair_cooldown_seconds":3600}`)
	require.NoError(t, os.WriteFile(parameters, parameterBytes, 0o600))
	digest := fmt.Sprintf("%x", sha256.Sum256(parameterBytes))
	operationctl := filepath.Join(tempDir, "operationctl")
	operationLog := filepath.Join(tempDir, "operation.log")
	require.NoError(t, os.WriteFile(operationctl, []byte(`#!/usr/bin/env bash
set -euo pipefail
printf '%s\n' "$*" >>"$OPERATION_LOG"
if [[ "$*" == *"--action claim"* ]]; then
  printf '{"name":"tikv-repair-1bb5a469de669cd3422d","operation_id":"tikv-repair-1bb5a469de669cd3422d","instance":"kubebrain","attempt":2,"parameters_sha256":"%s"}\n' "$EXPECTED_DIGEST"
fi
`), 0o755))
	repair := filepath.Join(tempDir, "repair")
	repairLog := filepath.Join(tempDir, "repair.log")
	require.NoError(t, os.WriteFile(repair, []byte(`#!/usr/bin/env bash
set -euo pipefail
env | sort >"$REPAIR_LOG"
[[ "${FAKE_REPAIR_FAIL:-false}" != "true" ]] || exit 9
printf '{"attempt_id":"%s","cluster_id":%s,"completed_at_unix":1786250000,"format":"kubebrain.tikv-transaction-repair.receipt.v1","kubebrain_statefulset_uid":"%s","pvc_preserved":true,"repaired_tikv_pods":3,"tidb_cluster_uid":"%s","transaction_verified":true}\n' "$REPAIR_ATTEMPT_ID" "$EXPECTED_CLUSTER_ID" "$EXPECTED_KUBEBRAIN_STATEFULSET_UID" "$EXPECTED_TIDB_CLUSTER_UID" >"$RECEIPT_OUTPUT"
`), 0o755))

	output, err := runProductionScriptCommand(t, "run-tikv-transaction-repair-operation.sh", []string{
		"WORKER_ID=worker-1",
		"PARAMETERS_INPUT=" + parameters,
		"OPERATIONCTL=" + operationctl,
		"REPAIR_COMMAND=" + repair,
		"WORK_DIR=" + tempDir,
		"HEARTBEAT_INTERVAL_SECONDS=0.1",
		"EXPECTED_DIGEST=" + digest,
		"OPERATION_LOG=" + operationLog,
		"REPAIR_LOG=" + repairLog,
	})
	require.NoError(t, err, string(output))
	operationData, err := os.ReadFile(operationLog)
	require.NoError(t, err)
	operationCalls := string(operationData)
	require.Contains(t, operationCalls, "--action claim")
	require.Contains(t, operationCalls, "--type TiKVTransactionRepair")
	require.Contains(t, operationCalls, "--action succeed")
	require.Contains(t, operationCalls, "--receipt-sha256")
	require.NotContains(t, operationCalls, "--action retry")
	repairData, err := os.ReadFile(repairLog)
	require.NoError(t, err)
	repairEnv := string(repairData)
	require.Contains(t, repairEnv, "KUBE_CONTEXT=in-cluster")
	require.Contains(t, repairEnv, "ALLOW_TIKV_POD_REPAIR=true")
	require.Contains(t, repairEnv, "EXPECTED_TIDB_CLUSTER_UID=tc-uid")
	require.Contains(t, repairEnv, "REPAIR_COOLDOWN_SECONDS=3600")
	require.Contains(t, repairEnv, "REPAIR_STATE_NAMESPACE=kubebrain-repair-state")
	require.True(t, strings.Contains(repairEnv, "REPAIR_ATTEMPT_ID=op-"))

	require.NoError(t, os.WriteFile(repairLog, nil, 0o600))
	takeoverOutput, takeoverErr := runProductionScriptCommand(t, "run-tikv-transaction-repair-operation.sh", []string{
		"WORKER_ID=worker-2",
		"PARAMETERS_INPUT=" + parameters,
		"OPERATIONCTL=" + operationctl,
		"REPAIR_COMMAND=" + repair,
		"WORK_DIR=" + tempDir,
		"HEARTBEAT_INTERVAL_SECONDS=0.1",
		"EXPECTED_DIGEST=" + digest,
		"OPERATION_LOG=" + operationLog,
		"REPAIR_LOG=" + repairLog,
	})
	require.NoError(t, takeoverErr, string(takeoverOutput))
	takeoverRepairData, err := os.ReadFile(repairLog)
	require.NoError(t, err)
	require.Empty(t, takeoverRepairData, "takeover must verify the durable receipt without repairing TiKV twice")

	receipts, err := filepath.Glob(filepath.Join(tempDir, "tikv-repair-*.receipt.json"))
	require.NoError(t, err)
	require.Len(t, receipts, 1)
	require.NoError(t, os.Remove(receipts[0]))
	require.NoError(t, os.WriteFile(operationLog, nil, 0o600))
	failureOutput, failureErr := runProductionScriptCommand(t, "run-tikv-transaction-repair-operation.sh", []string{
		"WORKER_ID=worker-3", "PARAMETERS_INPUT=" + parameters,
		"OPERATIONCTL=" + operationctl, "REPAIR_COMMAND=" + repair, "WORK_DIR=" + tempDir,
		"HEARTBEAT_INTERVAL_SECONDS=0.1", "EXPECTED_DIGEST=" + digest,
		"OPERATION_LOG=" + operationLog, "REPAIR_LOG=" + repairLog, "FAKE_REPAIR_FAIL=true",
	})
	require.Error(t, failureErr, string(failureOutput))
	failureOperationData, err := os.ReadFile(operationLog)
	require.NoError(t, err)
	require.Contains(t, string(failureOperationData), "--action fail")
	require.NotContains(t, string(failureOperationData), "--action retry")

	driftedBytes := []byte(strings.Replace(string(parameterBytes),
		`"alert_occurrence_id":"1bb5a469de669cd3422d"`,
		`"alert_occurrence_id":"00000000000000000000"`, 1))
	require.NoError(t, os.WriteFile(parameters, driftedBytes, 0o600))
	driftedDigest := fmt.Sprintf("%x", sha256.Sum256(driftedBytes))
	require.NoError(t, os.WriteFile(repairLog, nil, 0o600))
	driftOutput, driftErr := runProductionScriptCommand(t, "run-tikv-transaction-repair-operation.sh", []string{
		"WORKER_ID=worker-4", "PARAMETERS_INPUT=" + parameters,
		"OPERATIONCTL=" + operationctl, "REPAIR_COMMAND=" + repair, "WORK_DIR=" + tempDir,
		"HEARTBEAT_INTERVAL_SECONDS=0.1", "EXPECTED_DIGEST=" + driftedDigest,
		"OPERATION_LOG=" + operationLog, "REPAIR_LOG=" + repairLog,
	})
	require.Error(t, driftErr)
	require.Contains(t, string(driftOutput), "repair alert occurrence identity does not match the operation")
	driftRepairData, err := os.ReadFile(repairLog)
	require.NoError(t, err)
	require.Empty(t, driftRepairData, "occurrence drift must fail before starting the destructive repair primitive")
}
