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

func TestRunNativePITRFullBackupOperation(t *testing.T) {
	dir := t.TempDir()
	parameters := filepath.Join(dir, "parameters.json")
	parameterBytes := []byte(`{"backup_ts":"468294813545660418","cipher_method":"aes256-ctr","encryption_key_id":"kms/prod/backup/versions/7","pd_addrs":["pd-1:2379","pd-0:2379"],"storage_prefix":"s3://immutable/instance/run/full"}`)
	require.NoError(t, os.WriteFile(parameters, parameterBytes, 0o600))
	digest := fmt.Sprintf("%x", sha256.Sum256(parameterBytes))
	operationLog := filepath.Join(dir, "operation.log")
	backupLog := filepath.Join(dir, "backup.log")
	operationctl := filepath.Join(dir, "operationctl")
	require.NoError(t, os.WriteFile(operationctl, []byte(`#!/usr/bin/env bash
set -euo pipefail
printf '%s\n' "$*" >>"$OPERATION_LOG"
if [[ "$*" == *"--action claim"* ]]; then
  short="${CLAIM_SHORT:-${EXPECTED_DIGEST:0:20}}"
	  printf '{"namespace":"kubebrain-operations","name":"native-pitr-full-%s","operation_id":"native-pitr-full-%s","instance":"kubebrain","type":"NativePITRFullBackup","requested_by":"platform:native-pitr-full-backup","parameters_sha256":"%s","parameters_secret":"native-pitr-full-%s-parameters","parameters_key":"parameters.json","owner":"%s","attempt":%s}\n' "$short" "$short" "$EXPECTED_DIGEST" "$short" "$WORKER_ID" "${ATTEMPT:-1}"
elif [[ "$*" == *"--action heartbeat"* ]]; then
  [[ "${FAIL_HEARTBEAT:-false}" != true ]] || exit 1
fi
`), 0o755))
	backup := filepath.Join(dir, "backup")
	require.NoError(t, os.WriteFile(backup, []byte(`#!/usr/bin/env bash
set -euo pipefail
for arg in "$@"; do case "$arg" in --verify-attestation=*) exit "${VERIFY_EXIT:-0}";; esac; done
printf '%s\n' "$*" >"$BACKUP_LOG"
if [[ -n "${FENCED_DESCENDANT_MARKER:-}" ]]; then
  (
    trap 'printf terminated >"$FENCED_DESCENDANT_MARKER"; exit 143' TERM
    printf started >"$FENCED_DESCENDANT_MARKER"
    while :; do sleep 0.05; done
  ) &
  wait
fi
sleep 0.2
for arg in "$@"; do case "$arg" in --attestation-output=*) output="${arg#*=}";; esac; done
printf '%s\n' "$ATTESTATION_CONTENT" >"$output"
chmod 600 "$output"
[[ "${BACKUP_EXIT_AFTER_ATTESTATION:-0}" == 0 ]] || exit "$BACKUP_EXIT_AFTER_ATTESTATION"
`), 0o755))
	br := filepath.Join(dir, "br")
	require.NoError(t, os.WriteFile(br, []byte("#!/usr/bin/env sh\nexit 0\n"), 0o755))
	tlsDir := filepath.Join(dir, "tls")
	require.NoError(t, os.Mkdir(tlsDir, 0o700))
	for _, name := range []string{"ca.crt", "tls.crt", "tls.key"} {
		require.NoError(t, os.WriteFile(filepath.Join(tlsDir, name), []byte("test"), 0o600))
	}
	encryptionDir := filepath.Join(dir, "encryption")
	require.NoError(t, os.Mkdir(encryptionDir, 0o700))
	require.NoError(t, os.WriteFile(filepath.Join(encryptionDir, "key"), []byte(strings.Repeat("a", 64)), 0o400))
	require.NoError(t, os.WriteFile(filepath.Join(encryptionDir, "key-id"), []byte("kms/prod/backup/versions/7"), 0o400))

	base := []string{
		"WORKER_ID=worker-1", "PARAMETERS_INPUT=" + parameters, "EXPECTED_DIGEST=" + digest,
		"OPERATIONCTL=" + operationctl, "BACKUP_COMMAND=" + backup, "BR_BINARY=" + br,
		"WORK_DIR=" + dir, "TLS_DIR=" + tlsDir, "ENCRYPTION_DIR=" + encryptionDir, "HEARTBEAT_INTERVAL_SECONDS=0.1",
		"OPERATION_LOG=" + operationLog, "BACKUP_LOG=" + backupLog,
		"ATTESTATION_CONTENT=" + validFullBackupAttestation(t, "468294813545660418", "s3://immutable/instance/run/full", []string{"pd-0:2379", "pd-1:2379"}, "aes256-ctr", "kms/prod/backup/versions/7"),
	}
	output, err := runProductionScriptCommand(t, "run-native-pitr-full-backup-operation.sh", base)
	require.NoError(t, err, string(output))
	operations, err := os.ReadFile(operationLog)
	require.NoError(t, err)
	require.Contains(t, string(operations), "--type NativePITRFullBackup")
	require.Contains(t, string(operations), "--action heartbeat")
	require.Contains(t, string(operations), "--action succeed")
	require.Contains(t, string(operations), "--receipt-sha256")
	args, err := os.ReadFile(backupLog)
	require.NoError(t, err)
	require.Contains(t, string(args), "--pd-addrs=pd-1:2379,pd-0:2379")
	require.Contains(t, string(args), "--backup-ts=468294813545660418")
	require.Contains(t, string(args), "--storage-prefix=s3://immutable/instance/run/full")
	require.Contains(t, string(args), "--crypter-method=aes256-ctr")
	require.Contains(t, string(args), "--encryption-key-id=kms/prod/backup/versions/7")
	require.Contains(t, string(args), "--encryption-key-file="+filepath.Join(encryptionDir, "key"))
	operationName := "native-pitr-full-" + digest[:20]
	attestationPath := filepath.Join(dir, operationName+".native-pitr-full-backup-attestation.json")
	require.FileExists(t, attestationPath)

	require.NoError(t, os.Remove(backupLog))
	require.NoError(t, os.WriteFile(operationLog, nil, 0o600))
	reconcileOutput, reconcileErr := runProductionScriptCommand(t, "run-native-pitr-full-backup-operation.sh", append(base, "ATTEMPT=2"))
	require.NoError(t, reconcileErr, string(reconcileOutput))
	require.NoFileExists(t, backupLog, "attempt 2 must not repeat BR")
	reconcileOperations := string(mustReadProductionFile(t, operationLog))
	require.Contains(t, reconcileOperations, "--attempt 2")
	require.Contains(t, reconcileOperations, "reconciled durable native PITR full backup attestation without repeating BR")

	require.NoError(t, os.Chmod(attestationPath, 0o640))
	require.NoError(t, os.WriteFile(operationLog, nil, 0o600))
	unsafeOutput, unsafeErr := runProductionScriptCommand(t, "run-native-pitr-full-backup-operation.sh", append(base, "ATTEMPT=2"))
	require.Error(t, unsafeErr, string(unsafeOutput))
	require.NoFileExists(t, backupLog, "attempt 2 must not repeat BR for an invalid attestation")
	require.Contains(t, string(mustReadProductionFile(t, operationLog)), "exhausted without a valid durable attestation")

	require.NoError(t, os.Remove(attestationPath))
	require.NoError(t, os.WriteFile(operationLog, nil, 0o600))
	uncertainOutput, uncertainErr := runProductionScriptCommand(t, "run-native-pitr-full-backup-operation.sh", append(base, "BACKUP_EXIT_AFTER_ATTESTATION=9"))
	require.Error(t, uncertainErr)
	require.Contains(t, string(uncertainOutput), "later claim must inspect the immutable prefix")
	require.NotContains(t, string(mustReadProductionFile(t, operationLog)), "--action fail")
	require.FileExists(t, attestationPath)
	require.NoError(t, os.Remove(backupLog))
	require.NoError(t, os.WriteFile(operationLog, nil, 0o600))
	uncertainReconcileOutput, uncertainReconcileErr := runProductionScriptCommand(t, "run-native-pitr-full-backup-operation.sh", append(base, "ATTEMPT=2"))
	require.NoError(t, uncertainReconcileErr, string(uncertainReconcileOutput))
	require.NoFileExists(t, backupLog, "takeover after producer publication uncertainty must not repeat BR")
	require.Contains(t, string(mustReadProductionFile(t, operationLog)), "reconciled durable native PITR full backup attestation without repeating BR")
	require.NoError(t, os.Remove(attestationPath))

	require.NoError(t, os.Chmod(filepath.Join(encryptionDir, "key-id"), 0o600))
	require.NoError(t, os.WriteFile(filepath.Join(encryptionDir, "key-id"), []byte("kms/prod/backup/versions/8"), 0o400))
	mismatchOutput, mismatchErr := runProductionScriptCommand(t, "run-native-pitr-full-backup-operation.sh", base)
	require.Error(t, mismatchErr)
	require.Contains(t, string(mismatchOutput), "key version does not match")
	require.NoFileExists(t, backupLog, "a mismatched mounted key version must fail before producer execution")
	require.NoError(t, os.Chmod(filepath.Join(encryptionDir, "key-id"), 0o600))
	require.NoError(t, os.WriteFile(filepath.Join(encryptionDir, "key-id"), []byte("kms/prod/backup/versions/7"), 0o400))
	identityOutput, identityErr := runProductionScriptCommand(t, "run-native-pitr-full-backup-operation.sh", append(base,
		"CLAIM_SHORT=00000000000000000000"))
	require.Error(t, identityErr)
	require.Contains(t, string(identityOutput), "operation name does not bind the parameter digest")
	require.NoFileExists(t, backupLog)
	require.NoError(t, os.WriteFile(operationLog, nil, 0o600))
	descendantMarker := filepath.Join(dir, "fenced-descendant")
	fencedOutput, fencedErr := runProductionScriptCommand(t, "run-native-pitr-full-backup-operation.sh", append(base,
		"FAIL_HEARTBEAT=true", "FENCED_DESCENDANT_MARKER="+descendantMarker))
	require.Error(t, fencedErr)
	require.Contains(t, string(fencedOutput), "native PITR worker was fenced")
	require.Equal(t, "terminated", string(mustReadProductionFile(t, descendantMarker)))
	fencedOperations, err := os.ReadFile(operationLog)
	require.NoError(t, err)
	require.NotContains(t, string(fencedOperations), "--action succeed")
}

func validFullBackupAttestation(t *testing.T, backupTS, storage string, pdAddresses []string, cipher, keyID string) string {
	t.Helper()
	pdJSON, err := json.Marshal(pdAddresses)
	require.NoError(t, err)
	pdSHA := fmt.Sprintf("%x", sha256.Sum256(pdJSON))
	args := []string{"backup", "txn", "--storage=" + storage, "--backupts=" + backupTS, "--crypter.method=" + cipher}
	if cipher == "aes256-ctr" {
		args = append(args, "--crypter.key-id="+keyID)
	}
	argsJSON, err := json.Marshal(args)
	require.NoError(t, err)
	attestation := map[string]any{
		"format":           "kubebrain.native-pitr-full-backup-attestation.v3",
		"br_version":       "Release Version: v7.5.1\nGit Commit Hash: 7d16cc79e81bbf573124df3fd9351c26963f3e70\n",
		"br_binary_sha256": strings.Repeat("a", 64), "canonical_args": args,
		"canonical_args_sha256": fmt.Sprintf("%x", sha256.Sum256(argsJSON)),
		"pd_addresses_sha256":   pdSHA, "storage_prefix": storage,
		"backup_ts": json.Number(backupTS), "cipher_method": cipher,
		"completed_at_unix": 2_000_000_000, "exit_successful": true,
	}
	if cipher == "aes256-ctr" {
		attestation["encryption_key_id"] = keyID
	}
	body, err := json.Marshal(attestation)
	require.NoError(t, err)
	return string(body)
}

func TestRunNativePITRFullBackupOperationRejectsParameterDrift(t *testing.T) {
	dir := t.TempDir()
	parameters := filepath.Join(dir, "parameters.json")
	parameterBytes := []byte(`{"backup_ts":"1","pd_addrs":["pd:2379"],"storage_prefix":"s3://bucket/full","unreviewed":true}`)
	require.NoError(t, os.WriteFile(parameters, parameterBytes, 0o600))
	digest := fmt.Sprintf("%x", sha256.Sum256(parameterBytes))
	operationctl := filepath.Join(dir, "operationctl")
	require.NoError(t, os.WriteFile(operationctl, []byte(`#!/usr/bin/env bash
if [[ "$*" == *"--action claim"* ]]; then short="${EXPECTED_DIGEST:0:20}"; printf '{"namespace":"kubebrain-operations","name":"native-pitr-full-%s","operation_id":"native-pitr-full-%s","instance":"kubebrain","type":"NativePITRFullBackup","requested_by":"platform:native-pitr-full-backup","parameters_sha256":"%s","parameters_secret":"native-pitr-full-%s-parameters","parameters_key":"parameters.json","owner":"worker-1","attempt":1}\n' "$short" "$short" "$EXPECTED_DIGEST" "$short"; fi
`), 0o755))
	backup := filepath.Join(dir, "backup")
	require.NoError(t, os.WriteFile(backup, []byte("#!/usr/bin/env sh\nprintf called >\"$BACKUP_LOG\"\n"), 0o755))
	br := filepath.Join(dir, "br")
	require.NoError(t, os.WriteFile(br, []byte("#!/usr/bin/env sh\nexit 0\n"), 0o755))
	output, err := runProductionScriptCommand(t, "run-native-pitr-full-backup-operation.sh", []string{
		"WORKER_ID=worker-1", "PARAMETERS_INPUT=" + parameters, "EXPECTED_DIGEST=" + digest,
		"OPERATIONCTL=" + operationctl, "BACKUP_COMMAND=" + backup, "BR_BINARY=" + br,
		"WORK_DIR=" + dir, "BACKUP_LOG=" + filepath.Join(dir, "backup.log"),
	})
	require.Error(t, err)
	require.Contains(t, string(output), "native PITR parameter schema is invalid")
	require.False(t, strings.Contains(string(output), "called"))
	require.NoFileExists(t, filepath.Join(dir, "backup.log"))
}

func TestRunNativePITRFullBackupOperationLeavesExecutionUncertaintyForTakeover(t *testing.T) {
	dir := t.TempDir()
	parameters := filepath.Join(dir, "parameters.json")
	parameterBytes := []byte(`{"backup_ts":"1","pd_addrs":["pd:2379"],"storage_prefix":"s3://bucket/full"}`)
	require.NoError(t, os.WriteFile(parameters, parameterBytes, 0o600))
	digest := fmt.Sprintf("%x", sha256.Sum256(parameterBytes))
	logPath := filepath.Join(dir, "operation.log")
	operationctl := filepath.Join(dir, "operationctl")
	require.NoError(t, os.WriteFile(operationctl, []byte(`#!/usr/bin/env bash
printf '%s\n' "$*" >>"$OPERATION_LOG"
if [[ "$*" == *"--action claim"* ]]; then short="${EXPECTED_DIGEST:0:20}"; printf '{"namespace":"kubebrain-operations","name":"native-pitr-full-%s","operation_id":"native-pitr-full-%s","instance":"kubebrain","type":"NativePITRFullBackup","requested_by":"platform:native-pitr-full-backup","parameters_sha256":"%s","parameters_secret":"native-pitr-full-%s-parameters","parameters_key":"parameters.json","owner":"worker-1","attempt":1}\n' "$short" "$short" "$EXPECTED_DIGEST" "$short"; fi
`), 0o755))
	backup := filepath.Join(dir, "backup")
	require.NoError(t, os.WriteFile(backup, []byte("#!/usr/bin/env sh\nexit 9\n"), 0o755))
	br := filepath.Join(dir, "br")
	require.NoError(t, os.WriteFile(br, []byte("#!/usr/bin/env sh\nexit 0\n"), 0o755))
	tlsDir := filepath.Join(dir, "tls")
	require.NoError(t, os.Mkdir(tlsDir, 0o700))
	for _, name := range []string{"ca.crt", "tls.crt", "tls.key"} {
		require.NoError(t, os.WriteFile(filepath.Join(tlsDir, name), []byte("test"), 0o600))
	}
	output, err := runProductionScriptCommand(t, "run-native-pitr-full-backup-operation.sh", []string{
		"WORKER_ID=worker-1", "PARAMETERS_INPUT=" + parameters, "EXPECTED_DIGEST=" + digest,
		"OPERATIONCTL=" + operationctl, "BACKUP_COMMAND=" + backup, "BR_BINARY=" + br,
		"WORK_DIR=" + dir, "TLS_DIR=" + tlsDir, "OPERATION_LOG=" + logPath,
	})
	require.Error(t, err, string(output))
	operations, readErr := os.ReadFile(logPath)
	require.NoError(t, readErr)
	require.NotContains(t, string(operations), "--action fail")
	require.Contains(t, string(output), "later claim must inspect the immutable prefix")
	require.NotContains(t, string(operations), "--action retry")
}
