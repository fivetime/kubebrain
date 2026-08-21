package production_test

import (
	"crypto/sha256"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestNativePITRTargetRetirementDeletesOnlyAuthorizedUIDsAndPublishesReceipt(t *testing.T) {
	dir := t.TempDir()
	parameters, digest := writeRetirementParameters(t, dir)
	log := filepath.Join(dir, "log")
	authorize := writeRetirementExecutable(t, dir, "authorize", `#!/usr/bin/env sh
if [ -n "${BLOCK_MARKER:-}" ]; then
  printf started >"$BLOCK_MARKER"
  trap 'printf terminated >"$BLOCK_MARKER"; exit 143' TERM
  while :; do sleep 0.05; done
fi
out=""; for arg in "$@"; do case "$arg" in --output=*) out="${arg#*=}";; esac; done
printf '%s' '{"namespace":"tidb-cluster","tidb_cluster":"kb","tidb_cluster_uid":"tc-old","volumes":[{"pvc_name":"pd-kb-pd-0","pvc_uid":"pvc-pd"},{"pvc_name":"tikv-kb-tikv-0","pvc_uid":"pvc-tikv"}]}' >"$out"
`)
	uidDelete := writeRetirementExecutable(t, dir, "uid-delete", `#!/usr/bin/env sh
printf 'delete %s\n' "$*" >>"$RETIRE_LOG"
`)
	kubectl := writeRetirementExecutable(t, dir, "kubectl", `#!/usr/bin/env sh
if [ "${RETIRE_REPLACED_UID:-false}" = true ] && echo "$*" | grep -q 'get tidbcluster'; then printf '%s' tc-replacement; fi
`)
	inspector := writeRetirementExecutable(t, dir, "inspect", `#!/usr/bin/env sh
printf '%s\n' durable >"$OUTPUT"
`)
	verifier := writeRetirementExecutable(t, dir, "verify", `#!/usr/bin/env sh
exit 0
`)
	operationctl := writeRetirementExecutable(t, dir, "operationctl", `#!/usr/bin/env bash
printf 'ctl %s\n' "$*" >>"$RETIRE_LOG"
if [[ "$*" == *"--action claim"* ]]; then printf '{"name":"native-pitr-retire-%s","operation_id":"native-pitr-retire-%s","instance":"kubebrain","type":"NativePITRTargetRetirement","requested_by":"platform:native-pitr-target-retirement","attempt":1,"parameters_sha256":"%s"}\n' "${EXPECTED_DIGEST:0:20}" "${EXPECTED_DIGEST:0:20}" "$EXPECTED_DIGEST"; fi
if [[ "$*" == *"--action heartbeat"* && -s "${BLOCK_MARKER:-/nonexistent}" ]]; then exit 1; fi
`)
	output, err := runProductionScriptCommand(t, "run-native-pitr-target-retirement-operation.sh", []string{"WORKER_ID=worker", "PARAMETERS_INPUT=" + parameters, "EXPECTED_DIGEST=" + digest, "INPUT_ROOT=" + dir, "WORK_DIR=" + dir, "OPERATIONCTL=" + operationctl, "RETIRE_AUTHORIZE=" + authorize, "UID_DELETE=" + uidDelete, "KUBECTL=" + kubectl, "RETIRE_INSPECT=" + inspector, "RECEIPT_VERIFY=" + verifier, "RETIRE_LOG=" + log})
	require.NoError(t, err, string(output))
	text := string(mustReadProductionFile(t, log))
	require.Contains(t, text, "--resource=tidbclusters --namespace=tidb-cluster --name=kb --uid=tc-old")
	require.Contains(t, text, "--name=pd-kb-pd-0 --uid=pvc-pd")
	require.Contains(t, text, "--name=tikv-kb-tikv-0 --uid=pvc-tikv")
	require.Contains(t, text, "--action succeed")
	require.NoError(t, os.Remove(log))
	marker := filepath.Join(dir, "blocked-retirement")
	output, err = runProductionScriptCommand(t, "run-native-pitr-target-retirement-operation.sh", []string{
		"WORKER_ID=worker", "PARAMETERS_INPUT=" + parameters, "EXPECTED_DIGEST=" + digest,
		"INPUT_ROOT=" + dir, "WORK_DIR=" + dir, "OPERATIONCTL=" + operationctl,
		"RETIRE_AUTHORIZE=" + authorize, "UID_DELETE=" + uidDelete, "KUBECTL=" + kubectl,
		"RETIRE_INSPECT=" + inspector, "RECEIPT_VERIFY=" + verifier, "RETIRE_LOG=" + log,
		"HEARTBEAT_INTERVAL_SECONDS=0.05", "BLOCK_MARKER=" + marker,
	})
	require.Error(t, err)
	require.Contains(t, string(output), "target retirement ownership was fenced")
	require.Equal(t, "terminated", string(mustReadProductionFile(t, marker)))
	require.NotContains(t, string(mustReadProductionFile(t, log)), "--action succeed")
	output, err = runProductionScriptCommand(t, "run-native-pitr-target-retirement-operation.sh", []string{"WORKER_ID=worker", "PARAMETERS_INPUT=" + parameters, "EXPECTED_DIGEST=" + digest, "INPUT_ROOT=" + dir, "WORK_DIR=" + dir, "OPERATIONCTL=" + operationctl, "RETIRE_AUTHORIZE=" + authorize, "UID_DELETE=" + uidDelete, "KUBECTL=" + kubectl, "RETIRE_INSPECT=" + inspector, "RECEIPT_VERIFY=" + verifier, "RETIRE_LOG=" + log, "RETIRE_REPLACED_UID=true"})
	require.Error(t, err, string(output))
	require.Contains(t, string(output), "name was replaced")
}

func TestNativePITRTargetRetirementSecondAttemptOnlyReconcilesReceipt(t *testing.T) {
	dir := t.TempDir()
	parameters, digest := writeRetirementParameters(t, dir)
	name := "native-pitr-retire-" + digest[:20]
	require.NoError(t, os.WriteFile(filepath.Join(dir, name+".native-pitr-target-retirement.json"), []byte("durable\n"), 0600))
	log := filepath.Join(dir, "log")
	operationctl := writeRetirementExecutable(t, dir, "operationctl", fmt.Sprintf(`#!/usr/bin/env bash
printf 'ctl %%s\n' "$*" >>"$RETIRE_LOG"
if [[ "$*" == *"--action claim"* ]]; then printf '%s\n' '{"name":"%s","operation_id":"%s","instance":"kubebrain","type":"NativePITRTargetRetirement","requested_by":"platform:native-pitr-target-retirement","attempt":2,"parameters_sha256":"%s"}'; fi
`, "%s", name, name, digest))
	verifier := writeRetirementExecutable(t, dir, "verify", `#!/usr/bin/env sh
exit 0
`)
	output, err := runProductionScriptCommand(t, "run-native-pitr-target-retirement-operation.sh", []string{"WORKER_ID=worker", "PARAMETERS_INPUT=" + parameters, "EXPECTED_DIGEST=" + digest, "INPUT_ROOT=" + dir, "WORK_DIR=" + dir, "OPERATIONCTL=" + operationctl, "RECEIPT_VERIFY=" + verifier, "RETIRE_AUTHORIZE=/bin/false", "UID_DELETE=/bin/false", "RETIRE_INSPECT=/bin/false", "KUBECTL=/bin/false", "RETIRE_LOG=" + log})
	require.NoError(t, err, string(output))
	require.Contains(t, string(mustReadProductionFile(t, log)), "reconciled verified durable target retirement receipt")
}

func TestRequestNativePITRTargetRetirementIsApprovalBound(t *testing.T) {
	data, err := os.ReadFile("request-native-pitr-target-retirement.sh")
	require.NoError(t, err)
	text := string(data)
	for _, expected := range []string{"NativePITRTargetRetirement", "platform:native-pitr-target-retirement", "--max-attempts 2", ".immutable=true", "native-pitr-retire-${digest:0:20}"} {
		require.Contains(t, text, expected)
	}
	require.NotContains(t, text, "--approve")
}

func writeRetirementParameters(t *testing.T, dir string) (string, string) {
	t.Helper()
	keys := []string{"failed_operation_audit", "failed_operation_parameters", "old_plan", "old_restore_admission", "old_target_provisioning", "old_target_snapshot_empty"}
	values := map[string]string{}
	for _, k := range keys {
		p := filepath.Join(dir, k+".json")
		b := []byte(k + "\n")
		require.NoError(t, os.WriteFile(p, b, 0600))
		values[k] = p
		values[k+"_sha256"] = fmt.Sprintf("%x", sha256.Sum256(b))
	}
	data := []byte(fmt.Sprintf(`{"failed_operation_audit":"%s","failed_operation_audit_sha256":"%s","failed_operation_parameters":"%s","failed_operation_parameters_sha256":"%s","old_plan":"%s","old_plan_sha256":"%s","old_restore_admission":"%s","old_restore_admission_sha256":"%s","old_target_provisioning":"%s","old_target_provisioning_sha256":"%s","old_target_snapshot_empty":"%s","old_target_snapshot_empty_sha256":"%s"}`, values[keys[0]], values[keys[0]+"_sha256"], values[keys[1]], values[keys[1]+"_sha256"], values[keys[2]], values[keys[2]+"_sha256"], values[keys[3]], values[keys[3]+"_sha256"], values[keys[4]], values[keys[4]+"_sha256"], values[keys[5]], values[keys[5]+"_sha256"]))
	p := filepath.Join(dir, "parameters.json")
	require.NoError(t, os.WriteFile(p, data, 0600))
	return p, fmt.Sprintf("%x", sha256.Sum256(data))
}

func writeRetirementExecutable(t *testing.T, dir, name, content string) string {
	t.Helper()
	p := filepath.Join(dir, name)
	require.NoError(t, os.WriteFile(p, []byte(content), 0755))
	return p
}
