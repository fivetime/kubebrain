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

const backupArtifactSHA256 = "abcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcd"

func TestBackupOperationCompletesProtectedUpload(t *testing.T) {
	f := newBackupRunnerFixture(t, false)
	f.run(t, true, "")
	log := f.log(t)
	require.Contains(t, log, "export\n")
	require.Contains(t, log, "status\n")
	require.Contains(t, log, "object\n")
	require.Contains(t, log, "--action succeed")
	require.Contains(t, log, "--namespace tenant-a-operations --action succeed")
	require.NotContains(t, log, "--namespace ops --namespace tenant-a-operations")
	require.FileExists(t, f.receipt)
}

func TestBackupOperationPassesFrozenArtifactToStatusAndObject(t *testing.T) {
	f := newBackupRunnerFixture(t, false)
	f.run(t, true, "ASSERT_FROZEN_ARTIFACT=true")
	log := f.log(t)
	require.NotContains(t, log, f.artifact)
}

func TestBackupOperationIgnoresOriginalArtifactDriftAfterCapture(t *testing.T) {
	f := newBackupRunnerFixture(t, false)
	f.run(t, true, "TAMPER_ORIGINAL_ARTIFACT_BEFORE_OBJECT=true")
	require.Contains(t, f.log(t), "--action succeed")
}

func TestBackupOperationRejectsInvalidObjectReceipt(t *testing.T) {
	f := newBackupRunnerFixture(t, false)
	f.run(t, false, "INVALID_OBJECT_RECEIPT=true", "invalid object receipt")
	log := f.log(t)
	require.Contains(t, log, "--action retry")
	require.NotContains(t, log, "--action succeed")
}

func TestBackupOperationRevalidatesReceiptBeforeSucceed(t *testing.T) {
	f := newBackupRunnerFixture(t, false)
	f.run(t, false, "FINAL_TAMPER_RECEIPT=true", "invalid object receipt")
	log := f.log(t)
	require.Contains(t, log, "--action retry")
	require.NotContains(t, log, "--action succeed")
}

func TestBackupOperationRejectsReceiptTamperedDuringDigest(t *testing.T) {
	f := newBackupRunnerFixture(t, false)
	f.run(t, false, "TAMPER_RECEIPT_DURING_SHA256=true", "invalid object receipt")
	log := f.log(t)
	require.Contains(t, log, "--action retry")
	require.NotContains(t, log, "--action succeed")
}

func TestBackupOperationRejectsValidReceiptChangedAfterDigest(t *testing.T) {
	f := newBackupRunnerFixture(t, true)
	artifact := filepath.Join(f.dir, "artifact.jsonl")
	tamperedReceipt := filepath.Join(f.dir, "valid-tampered-receipt.json")
	require.NoError(t, os.WriteFile(tamperedReceipt, []byte(fmt.Sprintf(
		`{"format":"kubebrain.object-backup.receipt.v1","instance":"instance-a","backup_id":"backup-1","object_store_id":"store-a","bucket":"backups","object_key":"instance-a/backup-1.jsonl","version_id":"version-1","artifact_file_sha256":"%s","artifact_format":"kubebrain.logical.v2","artifact_sha256":"%s","snapshot_revision":42,"created_at_unix":100,"records":2,"leases":1,"object_bytes":%d,"retention_mode":"COMPLIANCE","retain_until_unix":2000000000,"remote_verified":true,"uploaded_at_unix":1001}`+"\n",
		fileDigest(t, artifact), backupArtifactSHA256, len(mustRead(t, artifact)),
	)), 0o600))
	f.env = withReceiptAfterSHA256Tamper(f.env, tamperedReceipt)

	f.run(t, false, "", "invalid object receipt")
	log := f.log(t)
	require.Contains(t, log, "--action retry")
	require.NotContains(t, log, "--action succeed")
}

func TestBackupOperationReusesExistingArtifact(t *testing.T) {
	f := newBackupRunnerFixture(t, true)
	f.run(t, true, "")
	require.NotContains(t, f.log(t), "export\n")
	require.Contains(t, f.log(t), "status\n")
	require.Contains(t, f.log(t), "object\n")
}

func TestBackupOperationRequeuesUploadFailure(t *testing.T) {
	f := newBackupRunnerFixture(t, false)
	f.run(t, false, "OBJECT_FAIL=true", "failed and was requeued")
	log := f.log(t)
	require.Contains(t, log, "--action retry")
	require.NotContains(t, log, "--action succeed")
}

func TestBackupOperationRejectsParameterDrift(t *testing.T) {
	f := newBackupRunnerFixture(t, false)
	f.run(t, false, "CLAIM_DIGEST="+strings.Repeat("f", 64), "parameters digest")
	require.Contains(t, f.log(t), "--action retry")
}

func TestBackupOperationRejectsParametersTamperedDuringDigest(t *testing.T) {
	f := newBackupRunnerFixture(t, false)
	f.run(t, false, "TAMPER_PARAMETERS_DURING_SHA256=true", "parameters digest")
	log := f.log(t)
	require.Contains(t, log, "--action retry")
	require.NotContains(t, log, "export\n")
	require.NotContains(t, log, "object\n")
	require.NotContains(t, log, "--action succeed")
}

func TestBackupOperationRejectsArtifactTamperedDuringCapture(t *testing.T) {
	f := newBackupRunnerFixture(t, false)
	f.run(t, false, "TAMPER_BACKUP_DURING_SHA256=true", "backup artifact changed")
	log := f.log(t)
	require.Contains(t, log, "--action retry")
	require.NotContains(t, log, "status\n")
	require.NotContains(t, log, "object\n")
	require.NotContains(t, log, "--action succeed")
}

func TestBackupOperationRejectsEmptyRequiredParameters(t *testing.T) {
	f := newBackupRunnerFixture(t, false)
	parameters := strings.ReplaceAll(
		string(mustRead(t, f.parameters)),
		`"endpoint":"https://etcd:2379"`,
		`"endpoint":""`,
	)
	require.NoError(t, os.WriteFile(f.parameters, []byte(parameters), 0o600))

	f.run(t, false, "CLAIM_DIGEST="+fileDigest(t, f.parameters), "empty required field")
	log := f.log(t)
	require.NotContains(t, log, "export\n")
	require.NotContains(t, log, "--action retry")
	require.NotContains(t, log, "--action succeed")
}

func TestBackupOperationLoadsManagedParameters(t *testing.T) {
	f := newBackupRunnerFixture(t, false)
	f.env = append(f.env, "MANAGED_PARAMETERS="+f.parameters, "PARAMETERS_INPUT=")
	f.run(t, true, "")
	require.Contains(t, f.log(t), "--action parameters --name backup-1")
}

func TestBackupOperationStopsWhenHeartbeatIsFenced(t *testing.T) {
	f := newBackupRunnerFixture(t, false)
	f.run(t, false, "OBJECT_SLEEP=3", "heartbeat failed")
	log := f.log(t)
	require.Contains(t, log, "--action heartbeat")
	require.NotContains(t, log, "--action succeed")
	require.NotContains(t, log, "--action retry")
}

type backupRunnerFixture struct {
	dir, parameters, artifact, receipt string
	env                                []string
}

func newBackupRunnerFixture(t *testing.T, existingArtifact bool) *backupRunnerFixture {
	t.Helper()
	dir := t.TempDir()
	artifact := filepath.Join(dir, "artifact.jsonl")
	receipt := filepath.Join(dir, "receipt.json")
	parameters := filepath.Join(dir, "parameters.json")
	require.NoError(t, os.WriteFile(parameters, []byte(fmt.Sprintf(`{
	  "endpoint":"https://etcd:2379","prefix":"/registry","artifact_output":%q,
	  "batch_size":100,"metrics_output":"","backup_id":"backup-1",
	  "object_store_id":"store-a","s3_endpoint":"https://s3.example",
	  "s3_bucket":"backups","s3_object_key":"instance-a/backup-1.jsonl",
	  "s3_force_path_style":false,"aws_region":"us-east-1","retention_mode":"COMPLIANCE",
	  "retain_until_unix":2000000000,"min_records":1,"max_age_seconds":3600,
	  "receipt_output":%q
	}`, artifact, receipt)), 0o600))
	if existingArtifact {
		require.NoError(t, os.WriteFile(artifact, []byte("existing"), 0o600))
	}
	data, err := os.ReadFile(parameters)
	require.NoError(t, err)
	digest := fmt.Sprintf("%x", sha256.Sum256(data))

	operationctl := filepath.Join(dir, "operationctl")
	writeTrafficExecutable(t, operationctl, `#!/usr/bin/env bash
set -euo pipefail
printf 'operationctl %s\n' "$*" >>"$FAKE_DIR/actions.log"
if [[ " $* " == *" --action claim "* ]]; then
  digest="${CLAIM_DIGEST:-$PARAMETERS_DIGEST}"
  printf '{"namespace":"tenant-a-operations","name":"backup-1","uid":"uid-op","resource_version":"1","operation_id":"backup-1","instance":"instance-a","type":"Backup","parameters_sha256":"%s","owner":"worker-a","attempt":1,"lease_until_unix":999999}\n' "$digest"
elif [[ " $* " == *" --action parameters "* ]]; then
  cat "$MANAGED_PARAMETERS"
elif [[ " $* " == *" --action heartbeat "* && "${HEARTBEAT_FAIL:-true}" == true ]]; then
  exit 1
else
  echo '{}'
fi
`)
	exportCommand := filepath.Join(dir, "export")
	writeTrafficExecutable(t, exportCommand, `#!/usr/bin/env bash
set -euo pipefail
printf 'export\n' >>"$FAKE_DIR/actions.log"
printf 'artifact\n' >"$OUTPUT"
`)
	statusCommand := filepath.Join(dir, "status")
	writeTrafficExecutable(t, statusCommand, `#!/usr/bin/env bash
set -euo pipefail
printf 'status\n' >>"$FAKE_DIR/actions.log"
if [[ "${ASSERT_FROZEN_ARTIFACT:-false}" == true ]]; then
  [[ "$INPUT" != "$ORIGINAL_ARTIFACT_OUTPUT" ]] ||
    { echo "status input was not frozen" >&2; exit 9; }
  [[ -f "$INPUT" ]] || { echo "frozen status input is missing" >&2; exit 9; }
fi
printf 'status input %s\n' "$INPUT" >>"$FAKE_DIR/actions.log"
[[ -f "$INPUT" ]]
count_file="$FAKE_DIR/status.count"
count=0
[[ ! -f "$count_file" ]] || read -r count <"$count_file"
count=$((count + 1))
printf '%s\n' "$count" >"$count_file"
printf '{"format":"kubebrain.logical.v2","prefix":"/registry","revision":42,"created_at_unix":100,"records":2,"leases":1,"sha256":"`+backupArtifactSHA256+`"}\n'
if [[ "${FINAL_TAMPER_RECEIPT:-false}" == true && "$count" -ge 3 ]]; then
  printf '{"format":"kubebrain.object-backup.receipt.v1","artifact_sha256":"short"}\n' >"$BACKUP_RECEIPT_OUTPUT"
fi
`)
	objectCommand := filepath.Join(dir, "object")
	writeTrafficExecutable(t, objectCommand, `#!/usr/bin/env bash
set -euo pipefail
printf 'object\n' >>"$FAKE_DIR/actions.log"
if [[ "${ASSERT_FROZEN_ARTIFACT:-false}" == true ]]; then
  [[ "$INPUT" != "$ORIGINAL_ARTIFACT_OUTPUT" ]] ||
    { echo "object input was not frozen" >&2; exit 9; }
  [[ -f "$INPUT" ]] || { echo "frozen object input is missing" >&2; exit 9; }
fi
printf 'object input %s\n' "$INPUT" >>"$FAKE_DIR/actions.log"
[[ "${OBJECT_FAIL:-false}" != true ]] || exit 9
sleep "${OBJECT_SLEEP:-0}"
[[ "${TAMPER_ORIGINAL_ARTIFACT_BEFORE_OBJECT:-false}" != true ]] || printf 'changed\n' >"$ORIGINAL_ARTIFACT_OUTPUT"
artifact_sha="`+backupArtifactSHA256+`"
[[ "${INVALID_OBJECT_RECEIPT:-false}" != true ]] || artifact_sha=abc123
object_bytes="$(wc -c <"$INPUT" | tr -d ' ')"
artifact_file_sha="$(sha256sum "$INPUT" | cut -d ' ' -f1)"
printf '{"format":"kubebrain.object-backup.receipt.v1","instance":"%s","backup_id":"%s","object_store_id":"%s","bucket":"%s","object_key":"%s","version_id":"version-1","artifact_file_sha256":"%s","artifact_format":"kubebrain.logical.v2","artifact_sha256":"%s","snapshot_revision":42,"created_at_unix":100,"records":2,"leases":1,"object_bytes":%s,"retention_mode":"%s","retain_until_unix":%s,"remote_verified":true,"uploaded_at_unix":1000}\n' \
  "$INSTANCE" "$BACKUP_ID" "$OBJECT_STORE_ID" "$S3_BUCKET" "$S3_OBJECT_KEY" "$artifact_file_sha" "$artifact_sha" "$object_bytes" "$RETENTION_MODE" "$RETAIN_UNTIL_UNIX" >"$RECEIPT_OUTPUT"
chmod 600 "$RECEIPT_OUTPUT"
`)
	env := []string{
		"WORKER_ID=worker-a", "PARAMETERS_INPUT=" + parameters,
		"OPERATION_NAMESPACE=ops", "LEASE_SECONDS=6", "OPERATIONCTL=" + operationctl,
		"EXPORT_COMMAND=" + exportCommand, "STATUS_COMMAND=" + statusCommand,
		"OBJECT_COMMAND=" + objectCommand, "FAKE_DIR=" + dir, "PARAMETERS_DIGEST=" + digest,
		"BACKUP_RECEIPT_OUTPUT=" + receipt,
		"RUNNER_PARAMETERS_INPUT=" + parameters,
		"RUNNER_BACKUP_INPUT=" + artifact,
		"ORIGINAL_ARTIFACT_OUTPUT=" + artifact,
	}
	env = append(env, receiptDigestTamperEnv(t, dir, receipt)...)
	return &backupRunnerFixture{
		dir: dir, parameters: parameters, artifact: artifact, receipt: receipt,
		env: env,
	}
}

func (f *backupRunnerFixture) run(t *testing.T, ok bool, extra string, outputs ...string) {
	t.Helper()
	cmd := exec.Command("bash", "run-backup-operation.sh")
	cmd.Env = append(os.Environ(), append(f.env, extra)...)
	out, err := cmd.CombinedOutput()
	if ok {
		require.NoError(t, err, string(out))
	} else {
		require.Error(t, err, string(out))
	}
	for _, wanted := range outputs {
		require.Contains(t, string(out), wanted)
	}
}

func (f *backupRunnerFixture) log(t *testing.T) string {
	t.Helper()
	data, err := os.ReadFile(filepath.Join(f.dir, "actions.log"))
	require.NoError(t, err)
	return string(data)
}
