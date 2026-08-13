package production_test

import (
	"crypto/sha256"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestRunNativePITRFullRestoreOperationPublishesDurableReceipt(t *testing.T) {
	dir := t.TempDir()
	parameters := filepath.Join(dir, "parameters.json")
	parameterBytes := []byte(`{"admission":"/var/lib/kubebrain-operation/inputs/admission.json","approve_plan_sha256":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","artifact_root":"/var/lib/kubebrain-operation/inputs/artifact","full_artifacts":"/var/lib/kubebrain-operation/inputs/artifacts.json","full_snapshot":"/var/lib/kubebrain-operation/inputs/full.json","pd_addrs":["pd-1:2379","pd-0:2379"],"plan":"/var/lib/kubebrain-operation/inputs/plan.json","remote_inventory":"/var/lib/kubebrain-operation/inputs/inventory.json","source_range_exclusive":"/var/lib/kubebrain-operation/inputs/source.json","target_snapshot_empty":"/var/lib/kubebrain-operation/inputs/target.json"}`)
	require.NoError(t, os.WriteFile(parameters, parameterBytes, 0o600))
	digest := fmt.Sprintf("%x", sha256.Sum256(parameterBytes))
	operationLog := filepath.Join(dir, "operation.log")
	restoreLog := filepath.Join(dir, "restore.log")
	operationctl := writeNativeRestoreOperationctl(t, dir)
	restore := filepath.Join(dir, "restore")
	require.NoError(t, os.WriteFile(restore, []byte(`#!/usr/bin/env bash
printf '%s\n' "$*" >"$RESTORE_LOG"
sleep 0.2
printf '{"format":"kubebrain.native-pitr-full-restore.v3"}\n'
`), 0o755))
	br := filepath.Join(dir, "br")
	require.NoError(t, os.WriteFile(br, []byte("#!/usr/bin/env sh\nexit 0\n"), 0o755))
	tlsDir := filepath.Join(dir, "tls")
	require.NoError(t, os.Mkdir(tlsDir, 0o700))
	for _, name := range []string{"ca.crt", "tls.crt", "tls.key"} {
		require.NoError(t, os.WriteFile(filepath.Join(tlsDir, name), []byte("test"), 0o600))
	}
	base := []string{
		"WORKER_ID=worker-1", "PARAMETERS_INPUT=" + parameters, "EXPECTED_DIGEST=" + digest,
		"OPERATIONCTL=" + operationctl, "RESTORE_COMMAND=" + restore, "RECEIPT_VERIFY=" + writeNativeRestoreVerifier(t, dir, true), "BR_BINARY=" + br,
		"WORK_DIR=" + dir, "TLS_DIR=" + tlsDir, "HEARTBEAT_INTERVAL_SECONDS=0.1",
		"OPERATION_LOG=" + operationLog, "RESTORE_LOG=" + restoreLog, "ATTEMPT=1",
	}
	output, err := runProductionScriptCommand(t, "run-native-pitr-full-restore-operation.sh", base)
	require.NoError(t, err, string(output))
	operations := string(mustReadProductionFile(t, operationLog))
	require.Contains(t, operations, "--type NativePITRFullRestore")
	require.Contains(t, operations, "--action succeed")
	require.Contains(t, operations, "--receipt-sha256")
	args := string(mustReadProductionFile(t, restoreLog))
	require.Contains(t, args, "--pd-addrs=pd-1:2379,pd-0:2379")
	require.Contains(t, args, "--approve-plan-sha256="+strings.Repeat("a", 64))
	require.Contains(t, args, "--restore-admission=/var/lib/kubebrain-operation/inputs/admission.json")
	name := "native-pitr-restore-" + digest[:20]
	receipt := filepath.Join(dir, name+".native-pitr-full-restore.json")
	require.FileExists(t, receipt)
	info, err := os.Stat(receipt)
	require.NoError(t, err)
	require.Equal(t, os.FileMode(0o600), info.Mode().Perm())
}

func TestRunNativePITRFullRestoreOperationReconcilesReceiptWithoutBR(t *testing.T) {
	dir := t.TempDir()
	parameters, digest := writeNativeRestoreParameters(t, dir)
	name := "native-pitr-restore-" + digest[:20]
	require.NoError(t, os.WriteFile(filepath.Join(dir, name+".native-pitr-full-restore.json"), []byte("durable\n"), 0o600))
	operationLog := filepath.Join(dir, "operation.log")
	restoreLog := filepath.Join(dir, "restore.log")
	output, err := runProductionScriptCommand(t, "run-native-pitr-full-restore-operation.sh", []string{
		"WORKER_ID=worker-2", "PARAMETERS_INPUT=" + parameters, "EXPECTED_DIGEST=" + digest,
		"OPERATIONCTL=" + writeNativeRestoreOperationctl(t, dir), "RESTORE_COMMAND=/bin/false", "RECEIPT_VERIFY=" + writeNativeRestoreVerifier(t, dir, true), "BR_BINARY=/bin/true",
		"WORK_DIR=" + dir, "OPERATION_LOG=" + operationLog, "RESTORE_LOG=" + restoreLog, "ATTEMPT=2",
	})
	require.NoError(t, err, string(output))
	operations := string(mustReadProductionFile(t, operationLog))
	require.Contains(t, operations, "reconciled verified durable native PITR full restore receipt without re-executing BR")
	require.NoFileExists(t, restoreLog)
}

func TestNativePITRReplacementRestorePassesHandoffToVerifier(t *testing.T) {
	dir := t.TempDir()
	parameters := filepath.Join(dir, "parameters.json")
	handoff := filepath.Join(dir, "replacement.json")
	handoffBytes := []byte("replacement\n")
	require.NoError(t, os.WriteFile(handoff, handoffBytes, 0o600))
	handoffSHA := fmt.Sprintf("%x", sha256.Sum256(handoffBytes))
	provisioning := filepath.Join(dir, "provisioning.json")
	provisioningBytes := []byte("provisioning\n")
	require.NoError(t, os.WriteFile(provisioning, provisioningBytes, 0o600))
	provisioningSHA := fmt.Sprintf("%x", sha256.Sum256(provisioningBytes))
	oldTarget, oldTargetSHA, oldProvisioning, oldProvisioningSHA, oldRetirement, oldRetirementSHA, oldAdmission, oldAdmissionSHA := writeOldReplacementEvidence(t, dir)
	data := []byte(fmt.Sprintf(`{"admission":"%[1]s/a","approve_plan_sha256":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","artifact_root":"%[1]s/root","full_artifacts":"%[1]s/fa","full_snapshot":"%[1]s/fs","old_restore_admission":"%[12]s","old_restore_admission_sha256":"%[13]s","old_target_provisioning":"%[8]s","old_target_provisioning_sha256":"%[9]s","old_target_retirement":"%[10]s","old_target_retirement_sha256":"%[11]s","old_target_snapshot_empty":"%[6]s","old_target_snapshot_empty_sha256":"%[7]s","pd_addrs":["pd:2379"],"plan":"%[1]s/p","remote_inventory":"%[1]s/ri","source_range_exclusive":"%[1]s/s","target_provisioning":"%[4]s","target_provisioning_sha256":"%[5]s","target_replacement_handoff":"%[2]s","target_replacement_handoff_sha256":"%[3]s","target_snapshot_empty":"%[1]s/t"}`, dir, handoff, handoffSHA, provisioning, provisioningSHA, oldTarget, oldTargetSHA, oldProvisioning, oldProvisioningSHA, oldRetirement, oldRetirementSHA, oldAdmission, oldAdmissionSHA))
	require.NoError(t, os.WriteFile(parameters, data, 0o600))
	digest := fmt.Sprintf("%x", sha256.Sum256(data))
	name := "native-pitr-restore-" + digest[:20]
	require.NoError(t, os.WriteFile(filepath.Join(dir, name+".native-pitr-full-restore.json"), []byte("durable\n"), 0o600))
	verifyLog := filepath.Join(dir, "verify.log")
	verifier := filepath.Join(dir, "verifier")
	require.NoError(t, os.WriteFile(verifier, []byte("#!/usr/bin/env bash\nprintf '%s\\n' \"$*\" >\"$VERIFY_LOG\"\n"), 0o755))
	output, err := runProductionScriptCommand(t, "run-native-pitr-full-restore-operation.sh", []string{
		"WORKER_ID=worker-2", "PARAMETERS_INPUT=" + parameters, "EXPECTED_DIGEST=" + digest,
		"OPERATIONCTL=" + writeNativeRestoreOperationctl(t, dir), "RESTORE_COMMAND=/bin/false", "RECEIPT_VERIFY=" + verifier, "BR_BINARY=/bin/true",
		"WORK_DIR=" + dir, "INPUT_ROOT=" + dir, "OPERATION_LOG=" + filepath.Join(dir, "operation.log"), "VERIFY_LOG=" + verifyLog, "ATTEMPT=2",
	})
	require.NoError(t, err, string(output))
	require.Contains(t, string(mustReadProductionFile(t, verifyLog)), "--target-replacement-handoff="+handoff)
	require.Contains(t, string(mustReadProductionFile(t, verifyLog)), "--target-provisioning="+provisioning)
	require.Contains(t, string(mustReadProductionFile(t, verifyLog)), "--old-restore-admission="+oldAdmission)
}

func TestNativePITRReplacementRestoreRejectsHandoffDigestDrift(t *testing.T) {
	dir := t.TempDir()
	handoff := filepath.Join(dir, "replacement.json")
	require.NoError(t, os.WriteFile(handoff, []byte("replacement\n"), 0o600))
	approvedHandoffSHA := fmt.Sprintf("%x", sha256.Sum256([]byte("replacement\n")))
	provisioning := filepath.Join(dir, "provisioning.json")
	provisioningBytes := []byte("provisioning\n")
	require.NoError(t, os.WriteFile(provisioning, provisioningBytes, 0o600))
	provisioningSHA := fmt.Sprintf("%x", sha256.Sum256(provisioningBytes))
	oldTarget, oldTargetSHA, oldProvisioning, oldProvisioningSHA, oldRetirement, oldRetirementSHA, oldAdmission, oldAdmissionSHA := writeOldReplacementEvidence(t, dir)
	parameters := filepath.Join(dir, "parameters.json")
	data := []byte(fmt.Sprintf(`{"admission":"%[1]s/a","approve_plan_sha256":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","artifact_root":"%[1]s/root","full_artifacts":"%[1]s/fa","full_snapshot":"%[1]s/fs","old_restore_admission":"%[12]s","old_restore_admission_sha256":"%[13]s","old_target_provisioning":"%[8]s","old_target_provisioning_sha256":"%[9]s","old_target_retirement":"%[10]s","old_target_retirement_sha256":"%[11]s","old_target_snapshot_empty":"%[6]s","old_target_snapshot_empty_sha256":"%[7]s","pd_addrs":["pd:2379"],"plan":"%[1]s/p","remote_inventory":"%[1]s/ri","source_range_exclusive":"%[1]s/s","target_provisioning":"%[4]s","target_provisioning_sha256":"%[5]s","target_replacement_handoff":"%[2]s","target_replacement_handoff_sha256":"%[3]s","target_snapshot_empty":"%[1]s/t"}`, dir, handoff, approvedHandoffSHA, provisioning, provisioningSHA, oldTarget, oldTargetSHA, oldProvisioning, oldProvisioningSHA, oldRetirement, oldRetirementSHA, oldAdmission, oldAdmissionSHA))
	require.NoError(t, os.WriteFile(parameters, data, 0o600))
	parametersSHA := fmt.Sprintf("%x", sha256.Sum256(data))
	name := "native-pitr-restore-" + parametersSHA[:20]
	require.NoError(t, os.WriteFile(filepath.Join(dir, name+".native-pitr-full-restore.json"), []byte("durable\n"), 0o600))
	require.NoError(t, os.WriteFile(handoff, []byte("drifted\n"), 0o600))
	operationLog := filepath.Join(dir, "operation.log")
	output, err := runProductionScriptCommand(t, "run-native-pitr-full-restore-operation.sh", []string{
		"WORKER_ID=worker-2", "PARAMETERS_INPUT=" + parameters, "EXPECTED_DIGEST=" + parametersSHA,
		"OPERATIONCTL=" + writeNativeRestoreOperationctl(t, dir), "RESTORE_COMMAND=/bin/false", "RECEIPT_VERIFY=/bin/false", "BR_BINARY=/bin/true",
		"WORK_DIR=" + dir, "INPUT_ROOT=" + dir, "OPERATION_LOG=" + operationLog, "ATTEMPT=2",
	})
	require.Error(t, err, string(output))
	require.Contains(t, string(mustReadProductionFile(t, operationLog)), "target replacement handoff digest mismatch")
	require.NotContains(t, string(mustReadProductionFile(t, operationLog)), "--action succeed")
}

func TestNativePITRReplacementRestoreRejectsProvisioningDigestDrift(t *testing.T) {
	dir := t.TempDir()
	handoff, provisioning := filepath.Join(dir, "replacement.json"), filepath.Join(dir, "provisioning.json")
	handoffBytes, provisioningBytes := []byte("replacement\n"), []byte("provisioning\n")
	require.NoError(t, os.WriteFile(handoff, handoffBytes, 0o600))
	require.NoError(t, os.WriteFile(provisioning, provisioningBytes, 0o600))
	handoffSHA := fmt.Sprintf("%x", sha256.Sum256(handoffBytes))
	provisioningSHA := fmt.Sprintf("%x", sha256.Sum256(provisioningBytes))
	oldTarget, oldTargetSHA, oldProvisioning, oldProvisioningSHA, oldRetirement, oldRetirementSHA, oldAdmission, oldAdmissionSHA := writeOldReplacementEvidence(t, dir)
	parameters := filepath.Join(dir, "parameters.json")
	data := []byte(fmt.Sprintf(`{"admission":"%[1]s/a","approve_plan_sha256":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","artifact_root":"%[1]s/root","full_artifacts":"%[1]s/fa","full_snapshot":"%[1]s/fs","old_restore_admission":"%[12]s","old_restore_admission_sha256":"%[13]s","old_target_provisioning":"%[8]s","old_target_provisioning_sha256":"%[9]s","old_target_retirement":"%[10]s","old_target_retirement_sha256":"%[11]s","old_target_snapshot_empty":"%[6]s","old_target_snapshot_empty_sha256":"%[7]s","pd_addrs":["pd:2379"],"plan":"%[1]s/p","remote_inventory":"%[1]s/ri","source_range_exclusive":"%[1]s/s","target_provisioning":"%[4]s","target_provisioning_sha256":"%[5]s","target_replacement_handoff":"%[2]s","target_replacement_handoff_sha256":"%[3]s","target_snapshot_empty":"%[1]s/t"}`, dir, handoff, handoffSHA, provisioning, provisioningSHA, oldTarget, oldTargetSHA, oldProvisioning, oldProvisioningSHA, oldRetirement, oldRetirementSHA, oldAdmission, oldAdmissionSHA))
	require.NoError(t, os.WriteFile(parameters, data, 0o600))
	parametersSHA := fmt.Sprintf("%x", sha256.Sum256(data))
	name := "native-pitr-restore-" + parametersSHA[:20]
	require.NoError(t, os.WriteFile(filepath.Join(dir, name+".native-pitr-full-restore.json"), []byte("durable\n"), 0o600))
	require.NoError(t, os.WriteFile(provisioning, []byte("drifted\n"), 0o600))
	operationLog := filepath.Join(dir, "operation.log")
	output, err := runProductionScriptCommand(t, "run-native-pitr-full-restore-operation.sh", []string{
		"WORKER_ID=worker-2", "PARAMETERS_INPUT=" + parameters, "EXPECTED_DIGEST=" + parametersSHA,
		"OPERATIONCTL=" + writeNativeRestoreOperationctl(t, dir), "RESTORE_COMMAND=/bin/false", "RECEIPT_VERIFY=/bin/false", "BR_BINARY=/bin/true",
		"WORK_DIR=" + dir, "INPUT_ROOT=" + dir, "OPERATION_LOG=" + operationLog, "ATTEMPT=2",
	})
	require.Error(t, err, string(output))
	require.Contains(t, string(mustReadProductionFile(t, operationLog)), "target provisioning receipt digest mismatch")
	require.NotContains(t, string(mustReadProductionFile(t, operationLog)), "--action succeed")
}

func TestRunNativePITRFullRestoreOperationFailsClosedWithoutReceipt(t *testing.T) {
	dir := t.TempDir()
	parameters, digest := writeNativeRestoreParameters(t, dir)
	operationLog := filepath.Join(dir, "operation.log")
	output, err := runProductionScriptCommand(t, "run-native-pitr-full-restore-operation.sh", []string{
		"WORKER_ID=worker-2", "PARAMETERS_INPUT=" + parameters, "EXPECTED_DIGEST=" + digest,
		"OPERATIONCTL=" + writeNativeRestoreOperationctl(t, dir), "RESTORE_COMMAND=/bin/false", "RECEIPT_VERIFY=" + writeNativeRestoreVerifier(t, dir, true), "BR_BINARY=/bin/true",
		"WORK_DIR=" + dir, "OPERATION_LOG=" + operationLog, "ATTEMPT=2",
	})
	require.Error(t, err, string(output))
	operations := string(mustReadProductionFile(t, operationLog))
	require.Contains(t, operations, "previous restore attempt expired without a valid durable receipt")
	require.Contains(t, operations, "keep admission fence closed and rebuild the target")
}

func TestRequestNativePITRFullRestoreIsApprovalBoundAndNonReentrant(t *testing.T) {
	data, err := os.ReadFile("request-native-pitr-full-restore.sh")
	require.NoError(t, err)
	text := string(data)
	for _, expected := range []string{
		"--type NativePITRFullRestore", "--requested-by platform:native-pitr-full-restore",
		"--max-attempts 2", "native-pitr-restore-${parameters_sha:0:20}",
		"/var/lib/kubebrain-operation/inputs/", ".immutable=true", "target_replacement_handoff_sha256", "target_provisioning_sha256",
		"old_target_retirement_sha256", "old_restore_admission_sha256",
	} {
		require.Contains(t, text, expected)
	}
	require.NotContains(t, text, "--approve", "approval belongs to immutable Operation metadata, not a self-approved requester flag")
}

func TestNativePITRFullRestoreWorkerCrashAfterReceiptReconcilesWithoutSecondBR(t *testing.T) {
	dir := t.TempDir()
	parameters, digest := writeNativeRestoreParameters(t, dir)
	name := "native-pitr-restore-" + digest[:20]
	receipt := filepath.Join(dir, name+".native-pitr-full-restore.json")
	marker := filepath.Join(dir, "final-heartbeat.blocked")
	callLog := filepath.Join(dir, "restore-calls.log")
	operationLog := filepath.Join(dir, "operation.log")
	restore := filepath.Join(dir, "restore")
	require.NoError(t, os.WriteFile(restore, []byte(`#!/usr/bin/env bash
printf 'restore\n' >>"$RESTORE_CALL_LOG"
printf '{"format":"kubebrain.native-pitr-full-restore.v3"}\n'
`), 0o755))
	tlsDir := filepath.Join(dir, "tls")
	require.NoError(t, os.Mkdir(tlsDir, 0o700))
	for _, file := range []string{"ca.crt", "tls.crt", "tls.key"} {
		require.NoError(t, os.WriteFile(filepath.Join(tlsDir, file), []byte("test"), 0o600))
	}
	base := append(os.Environ(),
		"WORKER_ID=worker-1", "PARAMETERS_INPUT="+parameters, "EXPECTED_DIGEST="+digest,
		"OPERATIONCTL="+writeNativeRestoreOperationctl(t, dir), "RESTORE_COMMAND="+restore, "RECEIPT_VERIFY="+writeNativeRestoreVerifier(t, dir, true), "BR_BINARY=/bin/true",
		"WORK_DIR="+dir, "TLS_DIR="+tlsDir, "OPERATION_LOG="+operationLog, "RESTORE_CALL_LOG="+callLog,
		"ATTEMPT=1", "HEARTBEAT_INTERVAL_SECONDS=30", "BLOCK_FINAL_HEARTBEAT=true", "DURABLE_RECEIPT="+receipt, "BLOCK_MARKER="+marker,
	)
	command := exec.Command("bash", "run-native-pitr-full-restore-operation.sh")
	command.Env = base
	command.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	require.NoError(t, command.Start())
	require.Eventually(t, func() bool {
		_, receiptErr := os.Stat(receipt)
		_, markerErr := os.Stat(marker)
		return receiptErr == nil && markerErr == nil
	}, 5*time.Second, 20*time.Millisecond, "worker did not reach the receipt-before-status crash window")
	require.NoError(t, syscall.Kill(-command.Process.Pid, syscall.SIGKILL))
	_ = command.Wait()

	output, err := runProductionScriptCommand(t, "run-native-pitr-full-restore-operation.sh", []string{
		"WORKER_ID=worker-2", "PARAMETERS_INPUT=" + parameters, "EXPECTED_DIGEST=" + digest,
		"OPERATIONCTL=" + writeNativeRestoreOperationctl(t, dir), "RESTORE_COMMAND=" + restore, "RECEIPT_VERIFY=" + writeNativeRestoreVerifier(t, dir, true), "BR_BINARY=/bin/true",
		"WORK_DIR=" + dir, "OPERATION_LOG=" + operationLog, "RESTORE_CALL_LOG=" + callLog, "ATTEMPT=2",
	})
	require.NoError(t, err, string(output))
	require.Equal(t, "restore\n", string(mustReadProductionFile(t, callLog)), "attempt 2 must not execute BR-backed restore again")
	require.Contains(t, string(mustReadProductionFile(t, operationLog)), "reconciled verified durable native PITR full restore receipt without re-executing BR")
}

func TestNativePITRFullRestoreRejectsInvalidDurableReceiptWithoutSecondBR(t *testing.T) {
	dir := t.TempDir()
	parameters, digest := writeNativeRestoreParameters(t, dir)
	name := "native-pitr-restore-" + digest[:20]
	require.NoError(t, os.WriteFile(filepath.Join(dir, name+".native-pitr-full-restore.json"), []byte("forged\n"), 0o600))
	operationLog := filepath.Join(dir, "operation.log")
	output, err := runProductionScriptCommand(t, "run-native-pitr-full-restore-operation.sh", []string{
		"WORKER_ID=worker-2", "PARAMETERS_INPUT=" + parameters, "EXPECTED_DIGEST=" + digest,
		"OPERATIONCTL=" + writeNativeRestoreOperationctl(t, dir), "RESTORE_COMMAND=/bin/false", "RECEIPT_VERIFY=" + writeNativeRestoreVerifier(t, dir, false), "BR_BINARY=/bin/true",
		"WORK_DIR=" + dir, "OPERATION_LOG=" + operationLog, "ATTEMPT=2",
	})
	require.Error(t, err, string(output))
	operations := string(mustReadProductionFile(t, operationLog))
	require.Contains(t, operations, "without a valid durable receipt")
	require.NotContains(t, operations, "--action succeed")
}

func writeNativeRestoreParameters(t *testing.T, dir string) (string, string) {
	t.Helper()
	path := filepath.Join(dir, "parameters.json")
	data := []byte(`{"admission":"/var/lib/kubebrain-operation/inputs/a","approve_plan_sha256":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","artifact_root":"/var/lib/kubebrain-operation/inputs/root","full_artifacts":"/var/lib/kubebrain-operation/inputs/fa","full_snapshot":"/var/lib/kubebrain-operation/inputs/fs","pd_addrs":["pd:2379"],"plan":"/var/lib/kubebrain-operation/inputs/p","remote_inventory":"/var/lib/kubebrain-operation/inputs/ri","source_range_exclusive":"/var/lib/kubebrain-operation/inputs/s","target_snapshot_empty":"/var/lib/kubebrain-operation/inputs/t"}`)
	require.NoError(t, os.WriteFile(path, data, 0o600))
	return path, fmt.Sprintf("%x", sha256.Sum256(data))
}

func writeOldReplacementEvidence(t *testing.T, dir string) (string, string, string, string, string, string, string, string) {
	t.Helper()
	paths := []string{filepath.Join(dir, "old-target.json"), filepath.Join(dir, "old-provisioning.json"), filepath.Join(dir, "old-retirement.json"), filepath.Join(dir, "old-admission.json")}
	contents := [][]byte{[]byte("old-target\n"), []byte("old-provisioning\n"), []byte("old-retirement\n"), []byte("old-admission\n")}
	digests := make([]string, len(paths))
	for i := range paths {
		require.NoError(t, os.WriteFile(paths[i], contents[i], 0o600))
		digests[i] = fmt.Sprintf("%x", sha256.Sum256(contents[i]))
	}
	return paths[0], digests[0], paths[1], digests[1], paths[2], digests[2], paths[3], digests[3]
}

func writeNativeRestoreOperationctl(t *testing.T, dir string) string {
	t.Helper()
	path := filepath.Join(dir, "operationctl")
	require.NoError(t, os.WriteFile(path, []byte(`#!/usr/bin/env bash
set -euo pipefail
printf '%s\n' "$*" >>"$OPERATION_LOG"
if [[ "$*" == *"--action claim"* ]]; then
  short="${EXPECTED_DIGEST:0:20}"
  printf '{"namespace":"kubebrain-operations","name":"native-pitr-restore-%s","operation_id":"native-pitr-restore-%s","instance":"kubebrain","type":"NativePITRFullRestore","requested_by":"platform:native-pitr-full-restore","parameters_sha256":"%s","parameters_secret":"native-pitr-restore-%s-parameters","parameters_key":"parameters.json","owner":"%s","attempt":%s}\n' "$short" "$short" "$EXPECTED_DIGEST" "$short" "$WORKER_ID" "$ATTEMPT"
elif [[ "$*" == *"--action heartbeat"* && "${BLOCK_FINAL_HEARTBEAT:-false}" == true && -s "${DURABLE_RECEIPT:-/nonexistent}" ]]; then
  : >"$BLOCK_MARKER"
  sleep 30
fi
`), 0o755))
	return path
}

func writeNativeRestoreVerifier(t *testing.T, dir string, success bool) string {
	t.Helper()
	path := filepath.Join(dir, fmt.Sprintf("receipt-verify-%t", success))
	exit := "1"
	if success {
		exit = "0"
	}
	require.NoError(t, os.WriteFile(path, []byte("#!/usr/bin/env sh\nexit "+exit+"\n"), 0o755))
	return path
}

func mustReadProductionFile(t *testing.T, path string) []byte {
	t.Helper()
	data, err := os.ReadFile(path)
	require.NoError(t, err)
	return data
}
