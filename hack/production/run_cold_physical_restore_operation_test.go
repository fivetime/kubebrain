package production_test

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRunColdPhysicalRestoreOperationIsBoundAndTerminal(t *testing.T) {
	dir := t.TempDir()
	source := []byte(`{"format":"kubebrain.cold-physical-snapshot.v2","operation_id":"cold-snapshot-0123456789abcdefabcd","inventory":{"storage":{"namespace":"tidb-cluster","tidb_cluster":"kb"}}}`)
	manifest := []byte("{\"apiVersion\":\"v1\",\"kind\":\"List\",\"items\":[]}\n")
	sourceSHA := fmt.Sprintf("%x", sha256.Sum256(source))
	manifestSHA := fmt.Sprintf("%x", sha256.Sum256(manifest))
	requestID := "change-2026-002"
	kubeUID := "target-kube-uid"
	namespaceUID := "target-namespace-uid"
	requestHash := fmt.Sprintf("%x", sha256.Sum256([]byte(requestID+"\n"+sourceSHA+"\n"+manifestSHA+"\n"+kubeUID+"\n"+namespaceUID+"\n")))[:20]
	name := "cold-restore-" + requestHash
	parameterBytes, err := json.Marshal(map[string]any{"request_id": requestID, "source_receipt": string(source), "source_receipt_sha256": sourceSHA,
		"restore_manifest": string(manifest), "restore_manifest_sha256": manifestSHA, "target_kube_system_uid": kubeUID,
		"target_namespace_uid": namespaceUID, "wait_timeout": "15m"})
	require.NoError(t, err)
	parameters := filepath.Join(dir, "parameters.json")
	require.NoError(t, os.WriteFile(parameters, parameterBytes, 0o600))
	digest := fmt.Sprintf("%x", sha256.Sum256(parameterBytes))
	operationLog := filepath.Join(dir, "operation.log")
	operationctl := filepath.Join(dir, "operationctl")
	require.NoError(t, os.WriteFile(operationctl, []byte(`#!/usr/bin/env bash
set -euo pipefail
printf '%s\n' "$*" >>"$OPERATION_LOG"
if [[ "$*" == *"--action claim"* ]]; then
 printf '{"namespace":"kubebrain-operations","name":"%s","operation_id":"%s","instance":"kb","type":"ColdPhysicalRestore","requested_by":"platform:cold-physical-restore","owner":"%s","parameters_secret":"%s-parameters","parameters_key":"parameters.json","attempt":%s,"parameters_sha256":"%s"}\n' "$OPERATION_NAME" "$OPERATION_NAME" "$WORKER_ID" "$OPERATION_NAME" "${ATTEMPT:-1}" "$EXPECTED_DIGEST"
fi
`), 0o755))
	restoreLog := filepath.Join(dir, "restore.log")
	restore := filepath.Join(dir, "restore")
	require.NoError(t, os.WriteFile(restore, []byte(`#!/usr/bin/env bash
set -euo pipefail
env | sort >"$RESTORE_LOG"
[[ "${FAIL_RESTORE:-false}" != true ]] || exit 9
jq -cn --arg source "$SOURCE_OPERATION" --arg sha "$EXPECTED_SOURCE_SHA" --arg manifest "$EXPECTED_MANIFEST_SHA" --arg kube "$EXPECTED_TARGET_KUBE_SYSTEM_UID" --arg ns "$EXPECTED_TARGET_NAMESPACE_UID" '{format:"kubebrain.cold-physical-restore.v1",operation_id:$source,source_receipt_sha256:$sha,restore_manifest:{format:"kubernetes-list.canonical-json.v1",sha256:$manifest,item_count:1,volume_snapshot_contents:0,volume_snapshots:0,persistent_volume_claims:0,tidbclusters:1},target:{kube_system_uid:$kube,namespace_uid:$ns,namespace:"tidb-cluster",tidb_cluster:"kb",tidb_cluster_uid:"tc-restored",cluster_id:"12345"},volume_snapshots:[],volume_snapshot_contents:[],pvs:[],pvcs:[],completed_at:"2026-08-22T00:00:00Z"}' >"$RESTORE_RECEIPT_FILE"
chmod 600 "$RESTORE_RECEIPT_FILE"
[[ "${EXIT_AFTER_RECEIPT:-0}" == 0 ]] || exit "$EXIT_AFTER_RECEIPT"
`), 0o755))
	env := []string{"WORKER_ID=worker-1", "PARAMETERS_INPUT=" + parameters, "OPERATIONCTL=" + operationctl, "RESTORE_COMMAND=" + restore,
		"WORK_DIR=" + dir, "HEARTBEAT_INTERVAL_SECONDS=0.1", "EXPECTED_DIGEST=" + digest, "OPERATION_NAME=" + name,
		"OPERATION_LOG=" + operationLog, "RESTORE_LOG=" + restoreLog, "SOURCE_OPERATION=cold-snapshot-0123456789abcdefabcd", "EXPECTED_SOURCE_SHA=" + sourceSHA, "EXPECTED_MANIFEST_SHA=" + manifestSHA}
	output, err := runProductionScriptCommand(t, "run-cold-physical-restore-operation.sh", env)
	require.NoError(t, err, string(output))
	operations := string(mustRead(t, operationLog))
	require.Contains(t, operations, "--type ColdPhysicalRestore")
	require.Contains(t, operations, "--action succeed")
	restoreEnvironment := string(mustRead(t, restoreLog))
	require.Contains(t, restoreEnvironment, "ALLOW_COLD_PHYSICAL_RESTORE=true")
	require.Contains(t, restoreEnvironment, "KUBE_CONTEXT=in-cluster")
	receiptPath := filepath.Join(dir, name+".receipt.json")
	require.NoError(t, os.Remove(restoreLog))
	require.NoError(t, os.WriteFile(operationLog, nil, 0o600))
	reconcileOutput, reconcileErr := runProductionScriptCommand(t, "run-cold-physical-restore-operation.sh", append(env, "ATTEMPT=2"))
	require.NoError(t, reconcileErr, string(reconcileOutput))
	require.NoFileExists(t, restoreLog, "attempt 2 must not recreate target resources")
	require.Contains(t, string(mustRead(t, operationLog)), "reconciled durable cold physical restore receipt without recreating target resources")
	require.NoError(t, os.Chmod(receiptPath, 0o640))
	require.NoError(t, os.WriteFile(operationLog, nil, 0o600))
	unsafeOutput, unsafeErr := runProductionScriptCommand(t, "run-cold-physical-restore-operation.sh", append(env, "ATTEMPT=2"))
	require.Error(t, unsafeErr, string(unsafeOutput))
	require.Contains(t, string(mustRead(t, operationLog)), "cold restore exhausted without a valid durable receipt")
	require.NoError(t, os.Chmod(receiptPath, 0o600))
	require.NoError(t, os.Remove(receiptPath))
	require.NoError(t, os.WriteFile(operationLog, nil, 0o600))
	uncertainOutput, uncertainErr := runProductionScriptCommand(t, "run-cold-physical-restore-operation.sh", append(env, "EXIT_AFTER_RECEIPT=9"))
	require.Error(t, uncertainErr)
	require.Contains(t, string(uncertainOutput), "later claim must inspect durable receipt")
	require.NotContains(t, string(mustRead(t, operationLog)), "--action fail")
	require.FileExists(t, receiptPath)
	require.NoError(t, os.Remove(restoreLog))
	require.NoError(t, os.WriteFile(operationLog, nil, 0o600))
	uncertainReconcileOutput, uncertainReconcileErr := runProductionScriptCommand(t, "run-cold-physical-restore-operation.sh", append(env, "ATTEMPT=2"))
	require.NoError(t, uncertainReconcileErr, string(uncertainReconcileOutput))
	require.NoFileExists(t, restoreLog, "publication uncertainty takeover must not recreate target resources")
	require.NoError(t, os.Remove(receiptPath))
	require.NoError(t, os.WriteFile(operationLog, nil, 0o600))
	failure, failureErr := runProductionScriptCommand(t, "run-cold-physical-restore-operation.sh", append(env, "FAIL_RESTORE=true"))
	require.Error(t, failureErr, string(failure))
	failureOperations := string(mustRead(t, operationLog))
	require.NotContains(t, failureOperations, "--action fail")
	require.NotContains(t, failureOperations, "--action retry")
	require.NoError(t, os.WriteFile(parameters, []byte(strings.Replace(string(parameterBytes), `"wait_timeout":"15m"`, `"wait_timeout":"9223372036854775808m"`, 1)), 0o600))
	overflowBytes := mustRead(t, parameters)
	overflowDigest := fmt.Sprintf("%x", sha256.Sum256(overflowBytes))
	require.NoError(t, os.WriteFile(restoreLog, nil, 0o600))
	overflowOutput, overflowErr := runProductionScriptCommand(t, "run-cold-physical-restore-operation.sh", append(env,
		"WORKER_ID=worker-numeric-overflow", "EXPECTED_DIGEST="+overflowDigest))
	require.Error(t, overflowErr)
	require.Contains(t, string(overflowOutput), "restore wait timeout does not contain a positive int64 duration")
	require.Empty(t, mustRead(t, restoreLog), "wait overflow must fail before the destructive restore primitive")
	require.NoError(t, os.WriteFile(parameters, parameterBytes, 0o600))

	oversized := append(append([]byte{}, parameterBytes...), []byte(strings.Repeat(" ", 65536))...)
	require.NoError(t, os.WriteFile(parameters, oversized, 0o600))
	oversizedDigest := fmt.Sprintf("%x", sha256.Sum256(oversized))
	require.NoError(t, os.WriteFile(operationLog, nil, 0o600))
	oversizedOutput, oversizedErr := runProductionScriptCommand(t, "run-cold-physical-restore-operation.sh", append(env,
		"WORKER_ID=worker-oversized", "EXPECTED_DIGEST="+oversizedDigest))
	require.Error(t, oversizedErr)
	require.Contains(t, string(oversizedOutput), "operation parameters exceed 65536 bytes")
	oversizedOperations := string(mustRead(t, operationLog))
	require.Contains(t, oversizedOperations, "--action fail")
	require.NotContains(t, oversizedOperations, "--action succeed")
}
