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

func TestRunNativePITRFullBackupOperation(t *testing.T) {
	dir := t.TempDir()
	parameters := filepath.Join(dir, "parameters.json")
	parameterBytes := []byte(`{"backup_ts":"468294813545660418","pd_addrs":["pd-1:2379","pd-0:2379"],"storage_prefix":"s3://immutable/instance/run/full"}`)
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
  printf '{"namespace":"kubebrain-operations","name":"native-pitr-full-%s","operation_id":"native-pitr-full-%s","instance":"kubebrain","type":"NativePITRFullBackup","requested_by":"platform:native-pitr-full-backup","parameters_sha256":"%s","parameters_secret":"native-pitr-full-%s-parameters","parameters_key":"parameters.json","owner":"%s","attempt":1}\n' "$short" "$short" "$EXPECTED_DIGEST" "$short" "$WORKER_ID"
elif [[ "$*" == *"--action heartbeat"* ]]; then
  [[ "${FAIL_HEARTBEAT:-false}" != true ]] || exit 1
fi
`), 0o755))
	backup := filepath.Join(dir, "backup")
	require.NoError(t, os.WriteFile(backup, []byte(`#!/usr/bin/env bash
set -euo pipefail
printf '%s\n' "$*" >"$BACKUP_LOG"
sleep 0.2
for arg in "$@"; do case "$arg" in --attestation-output=*) output="${arg#*=}";; esac; done
printf '{"format":"kubebrain.native-pitr-full-backup-attestation.v2"}\n' >"$output"
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
		"OPERATIONCTL=" + operationctl, "BACKUP_COMMAND=" + backup, "BR_BINARY=" + br,
		"WORK_DIR=" + dir, "TLS_DIR=" + tlsDir, "HEARTBEAT_INTERVAL_SECONDS=0.1",
		"OPERATION_LOG=" + operationLog, "BACKUP_LOG=" + backupLog,
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
	operationName := "native-pitr-full-" + digest[:20]
	require.FileExists(t, filepath.Join(dir, operationName+".native-pitr-full-backup-attestation.json"))

	require.NoError(t, os.Remove(filepath.Join(dir, operationName+".native-pitr-full-backup-attestation.json")))
	require.NoError(t, os.Remove(backupLog))
	identityOutput, identityErr := runProductionScriptCommand(t, "run-native-pitr-full-backup-operation.sh", append(base,
		"CLAIM_SHORT=00000000000000000000"))
	require.Error(t, identityErr)
	require.Contains(t, string(identityOutput), "operation name does not bind the parameter digest")
	require.NoFileExists(t, backupLog)
	require.NoError(t, os.WriteFile(operationLog, nil, 0o600))
	fencedOutput, fencedErr := runProductionScriptCommand(t, "run-native-pitr-full-backup-operation.sh", append(base, "FAIL_HEARTBEAT=true"))
	require.Error(t, fencedErr)
	require.Contains(t, string(fencedOutput), "native PITR worker was fenced")
	fencedOperations, err := os.ReadFile(operationLog)
	require.NoError(t, err)
	require.NotContains(t, string(fencedOperations), "--action succeed")
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

func TestRunNativePITRFullBackupOperationFailsTerminalAfterExecutionStarts(t *testing.T) {
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
	require.Contains(t, string(operations), "--action fail")
	require.Contains(t, string(operations), "inspect immutable prefix before a new operation")
	require.NotContains(t, string(operations), "--action retry")
}
