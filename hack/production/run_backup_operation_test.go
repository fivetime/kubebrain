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
	dir, parameters, receipt string
	env                      []string
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
[[ -f "$INPUT" ]]
`)
	objectCommand := filepath.Join(dir, "object")
	writeTrafficExecutable(t, objectCommand, `#!/usr/bin/env bash
set -euo pipefail
printf 'object\n' >>"$FAKE_DIR/actions.log"
[[ "${OBJECT_FAIL:-false}" != true ]] || exit 9
sleep "${OBJECT_SLEEP:-0}"
printf '{"format":"kubebrain.object-backup.receipt.v1"}\n' >"$RECEIPT_OUTPUT"
chmod 600 "$RECEIPT_OUTPUT"
`)
	return &backupRunnerFixture{
		dir: dir, parameters: parameters, receipt: receipt,
		env: []string{
			"WORKER_ID=worker-a", "PARAMETERS_INPUT=" + parameters,
			"OPERATION_NAMESPACE=ops", "LEASE_SECONDS=6", "OPERATIONCTL=" + operationctl,
			"EXPORT_COMMAND=" + exportCommand, "STATUS_COMMAND=" + statusCommand,
			"OBJECT_COMMAND=" + objectCommand, "FAKE_DIR=" + dir, "PARAMETERS_DIGEST=" + digest,
		},
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
