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
  printf '{"namespace":"%s","name":"tikv-repair-1bb5a469de669cd3422d","operation_id":"tikv-repair-1bb5a469de669cd3422d","requested_by":"%s","instance":"%s","type":"%s","parameters_sha256":"%s","parameters_secret":"tikv-repair-1bb5a469de669cd3422d-parameters","parameters_key":"parameters.json","owner":"%s","attempt":2}\n' \
    "${CLAIM_NAMESPACE:-kubebrain-operations}" "${CLAIM_REQUESTER:-alertmanager:transaction-path-policy}" \
    "${CLAIM_INSTANCE:-kubebrain}" "${CLAIM_TYPE:-TiKVTransactionRepair}" "$EXPECTED_DIGEST" "${CLAIM_OWNER:-$WORKER_ID}"
elif [[ "$*" == *"--action heartbeat"* ]]; then
  [[ "${FAIL_HEARTBEAT:-false}" != true ]] || exit 1
fi
`), 0o755))
	repair := filepath.Join(tempDir, "repair")
	repairLog := filepath.Join(tempDir, "repair.log")
	require.NoError(t, os.WriteFile(repair, []byte(`#!/usr/bin/env bash
set -euo pipefail
env | sort >"$REPAIR_LOG"
[[ "${FAKE_REPAIR_FAIL:-false}" != "true" ]] || exit 9
printf '{"attempt_id":"%s","cluster_id":%s,"completed_at_unix":1786250000,"format":"kubebrain.tikv-transaction-repair.receipt.v1","kubebrain_statefulset_uid":"%s","pvc_preserved":true,"repaired_tikv_pods":%s,"tidb_cluster_uid":"%s","transaction_verified":true}\n' "$REPAIR_ATTEMPT_ID" "$EXPECTED_CLUSTER_ID" "$EXPECTED_KUBEBRAIN_STATEFULSET_UID" "${FAKE_REPAIRED_TIKV_PODS:-3}" "$EXPECTED_TIDB_CLUSTER_UID" >"$RECEIPT_OUTPUT"
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
		"FAKE_REPAIRED_TIKV_PODS=1",
	})
	require.NoError(t, err, string(output))
	operationData, err := os.ReadFile(operationLog)
	require.NoError(t, err)
	operationCalls := string(operationData)
	require.Contains(t, operationCalls, "--action claim")
	require.Contains(t, operationCalls, "--type TiKVTransactionRepair")
	require.Contains(t, operationCalls, "--action succeed")
	require.Contains(t, operationCalls, "--receipt-sha256")
	lastHeartbeat := strings.LastIndex(operationCalls, "--action heartbeat")
	require.GreaterOrEqual(t, lastHeartbeat, 0)
	require.Greater(t, strings.LastIndex(operationCalls, "--action succeed"), lastHeartbeat)
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

	require.NoError(t, os.WriteFile(operationLog, nil, 0o600))
	heartbeatOutput, heartbeatErr := runProductionScriptCommand(t, "run-tikv-transaction-repair-operation.sh", []string{
		"WORKER_ID=worker-fenced", "PARAMETERS_INPUT=" + parameters,
		"OPERATIONCTL=" + operationctl, "REPAIR_COMMAND=" + repair, "WORK_DIR=" + tempDir,
		"HEARTBEAT_INTERVAL_SECONDS=10", "EXPECTED_DIGEST=" + digest,
		"OPERATION_LOG=" + operationLog, "REPAIR_LOG=" + repairLog,
		"FAKE_REPAIRED_TIKV_PODS=1", "FAIL_HEARTBEAT=true",
	})
	require.Error(t, heartbeatErr, string(heartbeatOutput))
	require.Contains(t, string(heartbeatOutput), "final heartbeat failed; repair worker was fenced")
	heartbeatOperations, err := os.ReadFile(operationLog)
	require.NoError(t, err)
	require.NotContains(t, string(heartbeatOperations), "--action succeed")
	require.NotContains(t, string(heartbeatOperations), "--action fail")

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

	extraFieldBytes := []byte(strings.TrimSuffix(string(parameterBytes), "}") + `,"unreviewed":true}`)
	require.NoError(t, os.WriteFile(parameters, extraFieldBytes, 0o600))
	extraFieldDigest := fmt.Sprintf("%x", sha256.Sum256(extraFieldBytes))
	require.NoError(t, os.WriteFile(repairLog, nil, 0o600))
	extraOutput, extraErr := runProductionScriptCommand(t, "run-tikv-transaction-repair-operation.sh", []string{
		"WORKER_ID=worker-5", "PARAMETERS_INPUT=" + parameters,
		"OPERATIONCTL=" + operationctl, "REPAIR_COMMAND=" + repair, "WORK_DIR=" + tempDir,
		"HEARTBEAT_INTERVAL_SECONDS=0.1", "EXPECTED_DIGEST=" + extraFieldDigest,
		"OPERATION_LOG=" + operationLog, "REPAIR_LOG=" + repairLog,
	})
	require.Error(t, extraErr)
	require.Contains(t, string(extraOutput), "repair parameter schema is invalid")
	extraRepairData, err := os.ReadFile(repairLog)
	require.NoError(t, err)
	require.Empty(t, extraRepairData, "unknown parameter fields must fail before starting the repair primitive")

	require.NoError(t, os.WriteFile(parameters, parameterBytes, 0o600))
	require.NoError(t, os.WriteFile(repairLog, nil, 0o600))
	claimDriftOutput, claimDriftErr := runProductionScriptCommand(t, "run-tikv-transaction-repair-operation.sh", []string{
		"WORKER_ID=worker-6", "PARAMETERS_INPUT=" + parameters,
		"OPERATIONCTL=" + operationctl, "REPAIR_COMMAND=" + repair, "WORK_DIR=" + tempDir,
		"HEARTBEAT_INTERVAL_SECONDS=0.1", "EXPECTED_DIGEST=" + digest,
		"OPERATION_LOG=" + operationLog, "REPAIR_LOG=" + repairLog, "CLAIM_TYPE=Destroy",
	})
	require.Error(t, claimDriftErr)
	require.Contains(t, string(claimDriftOutput), "repair claim identity does not match the approved alert operation")
	claimDriftRepairData, err := os.ReadFile(repairLog)
	require.NoError(t, err)
	require.Empty(t, claimDriftRepairData, "claim type drift must fail before starting the repair primitive")

	oversizedBytes := append(append([]byte{}, parameterBytes...), []byte(strings.Repeat(" ", 65536))...)
	require.NoError(t, os.WriteFile(parameters, oversizedBytes, 0o600))
	oversizedDigest := fmt.Sprintf("%x", sha256.Sum256(oversizedBytes))
	require.NoError(t, os.WriteFile(operationLog, nil, 0o600))
	require.NoError(t, os.WriteFile(repairLog, nil, 0o600))
	oversizedOutput, oversizedErr := runProductionScriptCommand(t, "run-tikv-transaction-repair-operation.sh", []string{
		"WORKER_ID=worker-oversized", "PARAMETERS_INPUT=" + parameters,
		"OPERATIONCTL=" + operationctl, "REPAIR_COMMAND=" + repair, "WORK_DIR=" + tempDir,
		"HEARTBEAT_INTERVAL_SECONDS=0.1", "EXPECTED_DIGEST=" + oversizedDigest,
		"OPERATION_LOG=" + operationLog, "REPAIR_LOG=" + repairLog,
	})
	require.Error(t, oversizedErr)
	require.Contains(t, string(oversizedOutput), "operation parameters exceed 65536 bytes")
	oversizedOperations := string(mustRead(t, operationLog))
	require.Contains(t, oversizedOperations, "--action retry")
	require.NotContains(t, oversizedOperations, "--action succeed")
	require.Empty(t, mustRead(t, repairLog))

	require.NoError(t, os.WriteFile(parameters, []byte(strings.Replace(string(parameterBytes), `"expected_cluster_id":7671`, `"expected_cluster_id":18446744073709551615`, 1)), 0o600))
	maxClusterBytes := mustRead(t, parameters)
	maxClusterDigest := fmt.Sprintf("%x", sha256.Sum256(maxClusterBytes))
	for _, receipt := range receipts {
		_ = os.Remove(receipt)
	}
	require.NoError(t, os.WriteFile(repairLog, nil, 0o600))
	maxClusterOutput, maxClusterErr := runProductionScriptCommand(t, "run-tikv-transaction-repair-operation.sh", []string{
		"WORKER_ID=worker-max-cluster", "PARAMETERS_INPUT=" + parameters,
		"OPERATIONCTL=" + operationctl, "REPAIR_COMMAND=" + repair, "WORK_DIR=" + tempDir,
		"HEARTBEAT_INTERVAL_SECONDS=0.1", "EXPECTED_DIGEST=" + maxClusterDigest,
		"OPERATION_LOG=" + operationLog, "REPAIR_LOG=" + repairLog,
	})
	require.NoError(t, maxClusterErr, string(maxClusterOutput))
	require.Contains(t, string(mustRead(t, repairLog)), "EXPECTED_CLUSTER_ID=18446744073709551615")

	maxClusterReceipts, err := filepath.Glob(filepath.Join(tempDir, "tikv-repair-*.receipt.json"))
	require.NoError(t, err)
	require.Len(t, maxClusterReceipts, 1)
	require.Contains(t, string(mustRead(t, maxClusterReceipts[0])), `"cluster_id":18446744073709551615`)
	require.NoError(t, os.Remove(maxClusterReceipts[0]))
	overflowClusterBytes := []byte(strings.Replace(string(parameterBytes), `"expected_cluster_id":7671`, `"expected_cluster_id":18446744073709551616`, 1))
	require.NoError(t, os.WriteFile(parameters, overflowClusterBytes, 0o600))
	overflowClusterDigest := fmt.Sprintf("%x", sha256.Sum256(overflowClusterBytes))
	require.NoError(t, os.WriteFile(repairLog, nil, 0o600))
	overflowClusterOutput, overflowClusterErr := runProductionScriptCommand(t, "run-tikv-transaction-repair-operation.sh", []string{
		"WORKER_ID=worker-overflow-cluster", "PARAMETERS_INPUT=" + parameters,
		"OPERATIONCTL=" + operationctl, "REPAIR_COMMAND=" + repair, "WORK_DIR=" + tempDir,
		"HEARTBEAT_INTERVAL_SECONDS=0.1", "EXPECTED_DIGEST=" + overflowClusterDigest,
		"OPERATION_LOG=" + operationLog, "REPAIR_LOG=" + repairLog,
	})
	require.Error(t, overflowClusterErr)
	require.Contains(t, string(overflowClusterOutput), "cluster identity is not a positive uint64")
	require.Empty(t, mustRead(t, repairLog))
}

func TestRunTiKVQuiescedRepairOperation(t *testing.T) {
	dir := t.TempDir()
	parameters := filepath.Join(dir, "parameters.json")
	parameterBytes := []byte(`{"endpoint":"http://kubebrain-client.kubebrain-system.svc:3379","expected_abnormal_store_ids":[1005],"expected_cluster_id":7671,"expected_kubebrain_statefulset_uid":"kb-uid","expected_tidb_cluster_uid":"tc-uid","kubebrain_namespace":"kubebrain-system","kubebrain_statefulset":"kubebrain","pod_ready_timeout_seconds":300,"probe_timeout_seconds":10,"repair_cooldown_seconds":3600,"request_id":"change-2026-002","tidb_cluster":"kb","tidb_namespace":"tidb-cluster"}`)
	require.NoError(t, os.WriteFile(parameters, parameterBytes, 0o600))
	digest := fmt.Sprintf("%x", sha256.Sum256(parameterBytes))
	operationLog := filepath.Join(dir, "operation.log")
	repairLog := filepath.Join(dir, "repair.log")
	operationctl := filepath.Join(dir, "operationctl")
	require.NoError(t, os.WriteFile(operationctl, []byte(`#!/usr/bin/env bash
set -euo pipefail
printf '%s\n' "$*" >>"$OPERATION_LOG"
if [[ "$*" == *"--action claim"* ]]; then
  claim_name="${CLAIM_NAME:-tikv-quiesced-repair-55b125a68aa87753e96b}"
  printf '{"namespace":"kubebrain-repair-operations","name":"%s","operation_id":"%s","requested_by":"platform:tikv-quiesced-repair","instance":"kubebrain","type":"TiKVTransactionRepair","parameters_sha256":"%s","parameters_secret":"%s-parameters","parameters_key":"parameters.json","owner":"%s","attempt":2}\n' "$claim_name" "$claim_name" "$EXPECTED_DIGEST" "$claim_name" "$WORKER_ID"
fi
`), 0o755))
	repair := filepath.Join(dir, "repair")
	require.NoError(t, os.WriteFile(repair, []byte(`#!/usr/bin/env bash
set -euo pipefail
env | sort >"$REPAIR_LOG"
printf '{"attempt_id":"%s","cluster_id":%s,"completed_at_unix":1786381000,"format":"kubebrain.tikv-quiesced-repair.receipt.v1","kubebrain_quiesced":true,"kubebrain_statefulset_uid":"%s","pvc_preserved":true,"regions_verified":true,"repaired_store_ids":[%s],"repaired_tikv_pods":1,"tidb_cluster_uid":"%s"}\n' \
  "$REPAIR_ATTEMPT_ID" "$EXPECTED_CLUSTER_ID" "$EXPECTED_KUBEBRAIN_STATEFULSET_UID" "$EXPECTED_ABNORMAL_STORE_IDS" "$EXPECTED_TIDB_CLUSTER_UID" >"$RECEIPT_OUTPUT"
`), 0o755))
	env := []string{
		"WORKER_ID=worker-q1", "PARAMETERS_INPUT=" + parameters,
		"OPERATIONCTL=" + operationctl, "REPAIR_COMMAND=" + repair, "WORK_DIR=" + dir,
		"HEARTBEAT_INTERVAL_SECONDS=0.1", "EXPECTED_DIGEST=" + digest,
		"OPERATION_LOG=" + operationLog, "REPAIR_LOG=" + repairLog,
	}
	output, err := runProductionScriptCommand(t, "run-tikv-transaction-repair-operation.sh", env)
	require.NoError(t, err, string(output))
	repairData, err := os.ReadFile(repairLog)
	require.NoError(t, err)
	repairEnv := string(repairData)
	require.Contains(t, repairEnv, "REPAIR_MODE=quiesced")
	require.Contains(t, repairEnv, "EXPECTED_ABNORMAL_STORE_IDS=1005")
	operationData, err := os.ReadFile(operationLog)
	require.NoError(t, err)
	require.Contains(t, string(operationData), "--action succeed")
	require.Contains(t, string(operationData), "TiKV quiesced repair completed")

	require.NoError(t, os.WriteFile(repairLog, nil, 0o600))
	takeoverOutput, takeoverErr := runProductionScriptCommand(t, "run-tikv-transaction-repair-operation.sh", append(env, "WORKER_ID=worker-q2"))
	require.NoError(t, takeoverErr, string(takeoverOutput))
	takeoverRepair, err := os.ReadFile(repairLog)
	require.NoError(t, err)
	require.Empty(t, takeoverRepair, "takeover must verify the quiesced receipt without replacing TiKV twice")

	receipts, err := filepath.Glob(filepath.Join(dir, "tikv-repair-*.receipt.json"))
	require.NoError(t, err)
	require.Len(t, receipts, 1)
	receiptData, err := os.ReadFile(receipts[0])
	require.NoError(t, err)
	badReceipt := strings.Replace(string(receiptData), `"repaired_store_ids":[1005]`, `"repaired_store_ids":[1004]`, 1)
	require.NoError(t, os.WriteFile(receipts[0], []byte(badReceipt), 0o600))
	badReceiptOutput, badReceiptErr := runProductionScriptCommand(t, "run-tikv-transaction-repair-operation.sh", append(env, "WORKER_ID=worker-q3"))
	require.Error(t, badReceiptErr)
	require.Empty(t, badReceiptOutput)
	operationData, err = os.ReadFile(operationLog)
	require.NoError(t, err)
	require.Contains(t, string(operationData), "repair receipt invalid; a new approved operation is required")
	badReceiptRepair, err := os.ReadFile(repairLog)
	require.NoError(t, err)
	require.Empty(t, badReceiptRepair, "a receipt for a different store must fail without another replacement")

	driftedParameters := []byte(strings.Replace(string(parameterBytes), `"expected_abnormal_store_ids":[1005]`, `"expected_abnormal_store_ids":[1004]`, 1))
	require.NoError(t, os.WriteFile(parameters, driftedParameters, 0o600))
	driftedDigest := fmt.Sprintf("%x", sha256.Sum256(driftedParameters))
	driftOutput, driftErr := runProductionScriptCommand(t, "run-tikv-transaction-repair-operation.sh", []string{
		"WORKER_ID=worker-q4", "PARAMETERS_INPUT=" + parameters,
		"OPERATIONCTL=" + operationctl, "REPAIR_COMMAND=" + repair, "WORK_DIR=" + dir,
		"HEARTBEAT_INTERVAL_SECONDS=0.1", "EXPECTED_DIGEST=" + driftedDigest,
		"OPERATION_LOG=" + operationLog, "REPAIR_LOG=" + repairLog,
	})
	require.Error(t, driftErr)
	require.Contains(t, string(driftOutput), "quiesced repair request identity does not match the operation")

	for _, receipt := range receipts {
		_ = os.Remove(receipt)
	}
	require.NoError(t, os.WriteFile(repairLog, nil, 0o600))
	maxStoreID := "18446744073709551615"
	maxParameters := []byte(strings.Replace(string(parameterBytes), `"expected_abnormal_store_ids":[1005]`, `"expected_abnormal_store_ids":[`+maxStoreID+`]`, 1))
	require.NoError(t, os.WriteFile(parameters, maxParameters, 0o600))
	maxDigest := fmt.Sprintf("%x", sha256.Sum256(maxParameters))
	maxOutput, maxErr := runProductionScriptCommand(t, "run-tikv-transaction-repair-operation.sh", []string{
		"WORKER_ID=worker-q5", "PARAMETERS_INPUT=" + parameters,
		"OPERATIONCTL=" + operationctl, "REPAIR_COMMAND=" + repair, "WORK_DIR=" + dir,
		"HEARTBEAT_INTERVAL_SECONDS=0.1", "EXPECTED_DIGEST=" + maxDigest,
		"OPERATION_LOG=" + operationLog, "REPAIR_LOG=" + repairLog,
		"CLAIM_NAME=tikv-quiesced-repair-e9e8b3003c163ddc35ad",
	})
	require.NoError(t, maxErr, string(maxOutput))
	require.Contains(t, string(mustRead(t, repairLog)), "EXPECTED_ABNORMAL_STORE_IDS="+maxStoreID)

	maxReceipts, err := filepath.Glob(filepath.Join(dir, "tikv-repair-*.receipt.json"))
	require.NoError(t, err)
	require.Len(t, maxReceipts, 1)
	require.Contains(t, string(mustRead(t, maxReceipts[0])), `"repaired_store_ids":[`+maxStoreID+`]`)
	require.NoError(t, os.Remove(maxReceipts[0]))
	require.NoError(t, os.WriteFile(repairLog, nil, 0o600))
	overflowParameters := []byte(strings.Replace(string(parameterBytes), `"expected_abnormal_store_ids":[1005]`, `"expected_abnormal_store_ids":[18446744073709551616]`, 1))
	require.NoError(t, os.WriteFile(parameters, overflowParameters, 0o600))
	overflowDigest := fmt.Sprintf("%x", sha256.Sum256(overflowParameters))
	overflowOutput, overflowErr := runProductionScriptCommand(t, "run-tikv-transaction-repair-operation.sh", []string{
		"WORKER_ID=worker-q6", "PARAMETERS_INPUT=" + parameters,
		"OPERATIONCTL=" + operationctl, "REPAIR_COMMAND=" + repair, "WORK_DIR=" + dir,
		"HEARTBEAT_INTERVAL_SECONDS=0.1", "EXPECTED_DIGEST=" + overflowDigest,
		"OPERATION_LOG=" + operationLog, "REPAIR_LOG=" + repairLog,
	})
	require.Error(t, overflowErr)
	require.Contains(t, string(overflowOutput), "store identity is not a positive uint64")
	require.Empty(t, mustRead(t, repairLog))

	maxClusterParameters := []byte(strings.Replace(string(parameterBytes), `"expected_cluster_id":7671`, `"expected_cluster_id":18446744073709551615`, 1))
	require.NoError(t, os.WriteFile(parameters, maxClusterParameters, 0o600))
	maxClusterDigest := fmt.Sprintf("%x", sha256.Sum256(maxClusterParameters))
	require.NoError(t, os.WriteFile(repairLog, nil, 0o600))
	maxClusterOutput, maxClusterErr := runProductionScriptCommand(t, "run-tikv-transaction-repair-operation.sh", []string{
		"WORKER_ID=worker-q7", "PARAMETERS_INPUT=" + parameters,
		"OPERATIONCTL=" + operationctl, "REPAIR_COMMAND=" + repair, "WORK_DIR=" + dir,
		"HEARTBEAT_INTERVAL_SECONDS=0.1", "EXPECTED_DIGEST=" + maxClusterDigest,
		"OPERATION_LOG=" + operationLog, "REPAIR_LOG=" + repairLog,
		"CLAIM_NAME=tikv-quiesced-repair-ddd81153220d45151bbf",
	})
	require.NoError(t, maxClusterErr, string(maxClusterOutput))
	require.Contains(t, string(mustRead(t, repairLog)), "EXPECTED_CLUSTER_ID=18446744073709551615")
	clusterReceipts, err := filepath.Glob(filepath.Join(dir, "tikv-repair-*.receipt.json"))
	require.NoError(t, err)
	require.Len(t, clusterReceipts, 1)
	require.Contains(t, string(mustRead(t, clusterReceipts[0])), `"cluster_id":18446744073709551615`)
	require.NoError(t, os.Remove(clusterReceipts[0]))

	overflowClusterParameters := []byte(strings.Replace(string(parameterBytes), `"expected_cluster_id":7671`, `"expected_cluster_id":18446744073709551616`, 1))
	require.NoError(t, os.WriteFile(parameters, overflowClusterParameters, 0o600))
	overflowClusterDigest := fmt.Sprintf("%x", sha256.Sum256(overflowClusterParameters))
	require.NoError(t, os.WriteFile(repairLog, nil, 0o600))
	overflowClusterOutput, overflowClusterErr := runProductionScriptCommand(t, "run-tikv-transaction-repair-operation.sh", []string{
		"WORKER_ID=worker-q8", "PARAMETERS_INPUT=" + parameters,
		"OPERATIONCTL=" + operationctl, "REPAIR_COMMAND=" + repair, "WORK_DIR=" + dir,
		"HEARTBEAT_INTERVAL_SECONDS=0.1", "EXPECTED_DIGEST=" + overflowClusterDigest,
		"OPERATION_LOG=" + operationLog, "REPAIR_LOG=" + repairLog,
	})
	require.Error(t, overflowClusterErr)
	require.Contains(t, string(overflowClusterOutput), "cluster identity is not a positive uint64")
	require.Empty(t, mustRead(t, repairLog))
}

func TestRunTiKVTransactionRepairOperationRejectsInvalidClaimNamespace(t *testing.T) {
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
  printf '{"namespace":"%s","name":"tikv-repair-1bb5a469de669cd3422d","operation_id":"tikv-repair-1bb5a469de669cd3422d","requested_by":"alertmanager:transaction-path-policy","instance":"kubebrain","type":"TiKVTransactionRepair","parameters_sha256":"%s","parameters_secret":"tikv-repair-1bb5a469de669cd3422d-parameters","parameters_key":"parameters.json","owner":"worker-invalid-namespace","attempt":2}\n' "$CLAIM_NAMESPACE" "$EXPECTED_DIGEST"
fi
`), 0o755))
	repair := filepath.Join(tempDir, "repair")
	repairLog := filepath.Join(tempDir, "repair.log")
	require.NoError(t, os.WriteFile(repair, []byte("#!/usr/bin/env bash\nset -euo pipefail\nprintf started >\"$REPAIR_LOG\"\n"), 0o755))

	output, err := runProductionScriptCommand(t, "run-tikv-transaction-repair-operation.sh", []string{
		"WORKER_ID=worker-invalid-namespace", "PARAMETERS_INPUT=" + parameters,
		"OPERATIONCTL=" + operationctl, "REPAIR_COMMAND=" + repair, "WORK_DIR=" + tempDir,
		"HEARTBEAT_INTERVAL_SECONDS=0.1", "EXPECTED_DIGEST=" + digest,
		"OPERATION_LOG=" + operationLog, "REPAIR_LOG=" + repairLog, "CLAIM_NAMESPACE=tenant/a",
	})
	require.Error(t, err)
	require.Contains(t, string(output), "OPERATION_NAMESPACE must be a lowercase DNS label")
	operationData, readErr := os.ReadFile(operationLog)
	require.NoError(t, readErr)
	require.Contains(t, string(operationData), "--action claim")
	require.NotContains(t, string(operationData), "--action heartbeat")
	_, statErr := os.Stat(repairLog)
	require.ErrorIs(t, statErr, os.ErrNotExist, "invalid claim namespace must fail before repair side effects")
}
