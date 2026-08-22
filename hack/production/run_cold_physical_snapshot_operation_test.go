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

func TestRunColdPhysicalSnapshotOperationIsBoundAndTerminal(t *testing.T) {
	dir := t.TempDir()
	witness := "witness-record"
	witnessSHA := fmt.Sprintf("%x", sha256.Sum256([]byte(witness)))
	requestID := "change-2026-001"
	requestHash := fmt.Sprintf("%x", sha256.Sum256([]byte(requestID+"\nkb-uid\ntc-uid\n7671\n"+witnessSHA+"\n")))[:20]
	name := "cold-snapshot-" + requestHash
	parameterBytes := []byte(fmt.Sprintf(`{"expected_witness_prefix":"/","fence_settle_seconds":5,"inventory":{"format":"kubebrain.cold-physical-snapshot-preflight.v2","kubebrain":{"namespace":"kubebrain-system","statefulset":"kubebrain","uid":"kb-uid"},"pd_pvcs":[{"name":"pd-0"}],"recovery_blueprint":{"tidbcluster":{"apiVersion":"pingcap.com/v1alpha1","kind":"TidbCluster"}},"storage":{"cluster_id":"7671","namespace":"tidb-cluster","tidb_cluster":"kb","uid":"tc-uid"},"tikv_pvcs":[{"name":"tikv-0"}],"volume_snapshot_class":{"deletion_policy":"Retain","driver":"csi.test","name":"retained"}},"request_id":"%s","semantic_witness":"%s","semantic_witness_sha256":"%s","wait_timeout":"10m","witness_max_age_seconds":300}`, requestID, witness, witnessSHA))
	parameters := filepath.Join(dir, "parameters.json")
	require.NoError(t, os.WriteFile(parameters, parameterBytes, 0o600))
	digest := fmt.Sprintf("%x", sha256.Sum256(parameterBytes))
	operationLog := filepath.Join(dir, "operation.log")
	operationctl := filepath.Join(dir, "operationctl")
	require.NoError(t, os.WriteFile(operationctl, []byte(`#!/usr/bin/env bash
set -euo pipefail
printf '%s\n' "$*" >>"$OPERATION_LOG"
if [[ "$*" == *"--action claim"* ]]; then
 printf '{"namespace":"kubebrain-operations","name":"%s","operation_id":"%s","instance":"kubebrain","type":"ColdPhysicalSnapshot","requested_by":"platform:cold-physical-snapshot","owner":"%s","parameters_secret":"%s-parameters","parameters_key":"parameters.json","attempt":%s,"parameters_sha256":"%s"}\n' "$OPERATION_NAME" "$OPERATION_NAME" "$WORKER_ID" "$OPERATION_NAME" "${ATTEMPT:-1}" "$EXPECTED_DIGEST"
elif [[ "$*" == *"--action heartbeat"* ]]; then
 [[ ! -f "$SUCCEED_ACTIVE" ]] || touch "$HEARTBEAT_DURING_SUCCEED"
 [[ "${FAIL_HEARTBEAT:-false}" != true ]] || exit 1
elif [[ "$*" == *"--action succeed"* ]]; then
 touch "$SUCCEED_ACTIVE"
 sleep 0.3
 rm -f "$SUCCEED_ACTIVE"
elif [[ "$*" == *"--action parameters"* ]]; then
 cat "$PARAMETERS_SOURCE"
fi
`), 0o755))
	snapshotLog := filepath.Join(dir, "snapshot.log")
	snapshot := filepath.Join(dir, "snapshot")
	require.NoError(t, os.WriteFile(snapshot, []byte(`#!/usr/bin/env bash
set -euo pipefail
env | sort >"$SNAPSHOT_LOG"
[[ "${FAIL_SNAPSHOT:-false}" != true ]] || exit 9
jq -cn --arg id "$OPERATION_ID" --arg witness "$EXPECTED_WITNESS_SHA" '{format:"kubebrain.cold-physical-snapshot.v2",operation_id:$id,created_at:"2026-08-10T00:00:00Z",inventory:{kubebrain:{uid:"kb-uid"},storage:{uid:"tc-uid"},pd_pvcs:[{}],tikv_pvcs:[{}]},snapshots:[{},{}],semantic_witness:{format:"kubebrain.logical.v2",prefix:"/",revision:1,created_at_unix:1,records:0,leases:0,sha256:$witness,file_sha256:$witness}}' >"$RECEIPT_FILE"
chmod 600 "$RECEIPT_FILE"
[[ "${EXIT_AFTER_RECEIPT:-0}" == 0 ]] || exit "$EXIT_AFTER_RECEIPT"
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
	env := []string{"WORKER_ID=worker-1", "PARAMETERS_INPUT=" + parameters, "OPERATIONCTL=" + operationctl,
		"SNAPSHOT_COMMAND=" + snapshot, "WORK_DIR=" + dir, "HEARTBEAT_INTERVAL_SECONDS=0.1",
		"EXPECTED_DIGEST=" + digest, "OPERATION_NAME=" + name, "OPERATION_LOG=" + operationLog,
		"SNAPSHOT_LOG=" + snapshotLog, "EXPECTED_WITNESS_SHA=" + witnessSHA}
	env = append(env, "SUCCEED_ACTIVE="+filepath.Join(dir, "succeed-active"), "HEARTBEAT_DURING_SUCCEED="+filepath.Join(dir, "heartbeat-during-succeed"))
	env = append(env, "PATH="+dir+string(os.PathListSeparator)+os.Getenv("PATH"), "REAL_CP="+realCP)
	output, err := runProductionScriptCommand(t, "run-cold-physical-snapshot-operation.sh", env)
	require.NoError(t, err, string(output))
	operations, err := os.ReadFile(operationLog)
	require.NoError(t, err)
	require.Contains(t, string(operations), "--type ColdPhysicalSnapshot")
	require.Contains(t, string(operations), "--action succeed")
	require.NoFileExists(t, filepath.Join(dir, "heartbeat-during-succeed"))
	lastHeartbeat := strings.LastIndex(string(operations), "--action heartbeat")
	succeed := strings.LastIndex(string(operations), "--action succeed")
	require.GreaterOrEqual(t, lastHeartbeat, 0)
	require.Greater(t, succeed, lastHeartbeat)
	snapshotEnvironment, err := os.ReadFile(snapshotLog)
	require.NoError(t, err)
	require.Contains(t, string(snapshotEnvironment), "ALLOW_COLD_PHYSICAL_SNAPSHOT=true")
	require.Contains(t, string(snapshotEnvironment), "KUBE_CONTEXT=in-cluster")
	receiptPath := filepath.Join(dir, name+".receipt.json")

	require.NoError(t, os.Remove(snapshotLog))
	require.NoError(t, os.WriteFile(operationLog, nil, 0o600))
	reconcileOutput, reconcileErr := runProductionScriptCommand(t, "run-cold-physical-snapshot-operation.sh", append(env, "ATTEMPT=2"))
	require.NoError(t, reconcileErr, string(reconcileOutput))
	require.NoFileExists(t, snapshotLog, "attempt 2 must not repeat CSI snapshot creation")
	require.Contains(t, string(mustRead(t, operationLog)), "reconciled durable cold physical snapshot receipt without repeating CSI snapshots")
	require.NoError(t, os.Chmod(receiptPath, 0o640))
	require.NoError(t, os.WriteFile(operationLog, nil, 0o600))
	unsafeOutput, unsafeErr := runProductionScriptCommand(t, "run-cold-physical-snapshot-operation.sh", append(env, "ATTEMPT=2"))
	require.Error(t, unsafeErr, string(unsafeOutput))
	require.Contains(t, string(mustRead(t, operationLog)), "cold snapshot exhausted without a valid durable receipt")
	require.NoError(t, os.Chmod(receiptPath, 0o600))

	require.NoError(t, os.Remove(receiptPath))
	require.NoError(t, os.WriteFile(operationLog, nil, 0o600))
	uncertainOutput, uncertainErr := runProductionScriptCommand(t, "run-cold-physical-snapshot-operation.sh", append(env, "EXIT_AFTER_RECEIPT=9"))
	require.Error(t, uncertainErr)
	require.Contains(t, string(uncertainOutput), "later claim must inspect retained snapshots")
	require.NotContains(t, string(mustRead(t, operationLog)), "--action fail")
	require.FileExists(t, receiptPath)
	require.NoError(t, os.Remove(snapshotLog))
	require.NoError(t, os.WriteFile(operationLog, nil, 0o600))
	uncertainReconcileOutput, uncertainReconcileErr := runProductionScriptCommand(t, "run-cold-physical-snapshot-operation.sh", append(env, "ATTEMPT=2"))
	require.NoError(t, uncertainReconcileErr, string(uncertainReconcileOutput))
	require.NoFileExists(t, snapshotLog, "publication uncertainty takeover must not repeat CSI snapshot creation")
	require.NoError(t, os.Remove(receiptPath))

	require.NoError(t, os.WriteFile(operationLog, nil, 0o600))
	failure, failureErr := runProductionScriptCommand(t, "run-cold-physical-snapshot-operation.sh", append(env, "FAIL_SNAPSHOT=true"))
	require.Error(t, failureErr, string(failure))
	failureOperations, err := os.ReadFile(operationLog)
	require.NoError(t, err)
	require.NotContains(t, string(failureOperations), "--action fail")
	require.NotContains(t, string(failureOperations), "--action retry")

	require.NoError(t, os.WriteFile(operationLog, nil, 0o600))
	heartbeatFailure, heartbeatFailureErr := runProductionScriptCommand(t, "run-cold-physical-snapshot-operation.sh", append(env,
		"FAIL_HEARTBEAT=true", "HEARTBEAT_INTERVAL_SECONDS=10"))
	require.Error(t, heartbeatFailureErr, string(heartbeatFailure))
	require.Contains(t, string(heartbeatFailure), "final heartbeat failed; snapshot worker was fenced")
	heartbeatFailureOperations, err := os.ReadFile(operationLog)
	require.NoError(t, err)
	require.NotContains(t, string(heartbeatFailureOperations), "--action succeed")
	require.NotContains(t, string(heartbeatFailureOperations), "--action fail")
	require.NoError(t, os.Remove(receiptPath))

	oversized := append(append([]byte{}, parameterBytes...), []byte(strings.Repeat(" ", 65536))...)
	require.NoError(t, os.WriteFile(parameters, oversized, 0o600))
	oversizedDigest := fmt.Sprintf("%x", sha256.Sum256(oversized))
	require.NoError(t, os.WriteFile(operationLog, nil, 0o600))
	oversizedOutput, oversizedErr := runProductionScriptCommand(t, "run-cold-physical-snapshot-operation.sh", append(env,
		"WORKER_ID=worker-oversized", "EXPECTED_DIGEST="+oversizedDigest))
	require.Error(t, oversizedErr)
	require.Contains(t, string(oversizedOutput), "operation parameters exceed 65536 bytes")
	oversizedOperations := string(mustRead(t, operationLog))
	require.Contains(t, oversizedOperations, "--action fail")
	require.NotContains(t, oversizedOperations, "--action succeed")

	managedEnv := make([]string, 0, len(env)+3)
	for _, value := range env {
		if !strings.HasPrefix(value, "PARAMETERS_INPUT=") {
			managedEnv = append(managedEnv, value)
		}
	}
	managedEnv = append(managedEnv, "WORKER_ID=worker-managed-oversized", "EXPECTED_DIGEST="+oversizedDigest,
		"PARAMETERS_SOURCE="+parameters)
	require.NoError(t, os.WriteFile(operationLog, nil, 0o600))
	managedOutput, managedErr := runProductionScriptCommand(t, "run-cold-physical-snapshot-operation.sh", managedEnv)
	require.Error(t, managedErr)
	require.Contains(t, string(managedOutput), "operation parameters exceed 65536 bytes")
	require.Contains(t, string(mustRead(t, operationLog)), "--action parameters")

	require.NoError(t, os.WriteFile(parameters, parameterBytes, 0o600))
	require.NoError(t, os.WriteFile(operationLog, nil, 0o600))
	tamperOutput, tamperErr := runProductionScriptCommand(t, "run-cold-physical-snapshot-operation.sh", append(env,
		"WORKER_ID=worker-frozen-oversized", "TAMPER_FROZEN_PARAMETERS_SIZE=true"))
	require.Error(t, tamperErr)
	require.Contains(t, string(tamperOutput), "operation parameters exceed 65536 bytes")
	require.Contains(t, string(mustRead(t, operationLog)), "--action fail")

	for _, tc := range []struct{ old, replacement, want string }{
		{old: `"witness_max_age_seconds":300`, replacement: `"witness_max_age_seconds":9223372036854775808`, want: "snapshot witness max age is not a positive int64"},
		{old: `"wait_timeout":"10m"`, replacement: `"wait_timeout":"9223372036854775808m"`, want: "snapshot wait timeout does not contain a positive int64 duration"},
		{old: `"fence_settle_seconds":5`, replacement: `"fence_settle_seconds":9223372036854775808`, want: "snapshot fence settle is not a non-negative int64"},
	} {
		overflow := []byte(strings.Replace(string(parameterBytes), tc.old, tc.replacement, 1))
		require.NoError(t, os.WriteFile(parameters, overflow, 0o600))
		require.NoError(t, os.WriteFile(snapshotLog, nil, 0o600))
		overflowDigest := fmt.Sprintf("%x", sha256.Sum256(overflow))
		overflowOutput, overflowErr := runProductionScriptCommand(t, "run-cold-physical-snapshot-operation.sh", append(env,
			"WORKER_ID=worker-numeric-overflow", "EXPECTED_DIGEST="+overflowDigest))
		require.Error(t, overflowErr)
		require.Contains(t, string(overflowOutput), tc.want)
		require.Empty(t, mustRead(t, snapshotLog), "numeric overflow must fail before the destructive snapshot primitive")
	}
	require.NoError(t, os.WriteFile(parameters, parameterBytes, 0o600))

	exact := append(append([]byte{}, parameterBytes...), []byte(strings.Repeat(" ", 65536-len(parameterBytes)))...)
	require.Len(t, exact, 65536)
	require.NoError(t, os.WriteFile(parameters, exact, 0o600))
	exactDigest := fmt.Sprintf("%x", sha256.Sum256(exact))
	require.NoError(t, os.WriteFile(operationLog, nil, 0o600))
	exactOutput, exactErr := runProductionScriptCommand(t, "run-cold-physical-snapshot-operation.sh", append(env,
		"WORKER_ID=worker-exact-limit", "EXPECTED_DIGEST="+exactDigest))
	require.NoError(t, exactErr, string(exactOutput))
	require.Contains(t, string(mustRead(t, operationLog)), "--action succeed")
}
