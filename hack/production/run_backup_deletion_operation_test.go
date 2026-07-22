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

func TestBackupDeletionOperationCompletesThreeGatesAndRetries(t *testing.T) {
	f := newBackupDeletionFixture(t)
	f.run(t, true, "")
	first := mustRead(t, f.operationReceipt)
	f.run(t, true, "")
	require.Equal(t, first, mustRead(t, f.operationReceipt))
	log := f.log(t)
	require.Contains(t, log, "object pre\nobject delete\nobject post\n")
	require.Contains(t, log, "--type BackupDeletion")
	require.Contains(t, log, "--action succeed")
	require.Contains(t, log, "--namespace tenant-a-operations --action succeed")
	require.NotContains(t, log, "--namespace ops --namespace tenant-a-operations")
	require.NotContains(t, log, "--action retry")
}

func TestBackupDeletionOperationRequeuesEveryGateFailure(t *testing.T) {
	for _, failure := range []string{"pre", "delete", "post"} {
		t.Run(failure, func(t *testing.T) {
			f := newBackupDeletionFixture(t)
			f.run(t, false, "FAIL_STAGE="+failure, "failed and was requeued")
			require.Contains(t, f.log(t), "--action retry")
			require.NotContains(t, f.log(t), "--action succeed")
			require.NoFileExists(t, f.operationReceipt)
		})
	}
}

func TestBackupDeletionOperationRejectsDriftAndInvalidEvidence(t *testing.T) {
	t.Run("parameters", func(t *testing.T) {
		f := newBackupDeletionFixture(t)
		f.run(t, false, "CLAIM_DIGEST="+strings.Repeat("f", 64), "parameters digest")
		require.Contains(t, f.log(t), "--action retry")
	})
	t.Run("manifest bytes", func(t *testing.T) {
		f := newBackupDeletionFixture(t)
		require.NoError(t, os.WriteFile(f.preManifest, append(mustRead(t, f.preManifest), '\n'), 0o600))
		f.run(t, false, "", "evidence digest mismatch")
		require.NotContains(t, f.log(t), "object ")
	})
	t.Run("deletion receipt identity", func(t *testing.T) {
		f := newBackupDeletionFixture(t)
		f.run(t, false, "INVALID_DELETE_RECEIPT=true", "deletion receipt is invalid")
		require.NotContains(t, f.log(t), "--action succeed")
	})
	t.Run("post manifest still contains version", func(t *testing.T) {
		f := newBackupDeletionFixture(t)
		require.NoError(t, os.WriteFile(f.postManifest, mustRead(t, f.preManifest), 0o600))
		f.rewriteParameters(t)
		f.run(t, false, "", "absent inventory manifest")
		require.NotContains(t, f.log(t), "object ")
	})
}

func TestBackupDeletionOperationStopsWhenHeartbeatIsFenced(t *testing.T) {
	f := newBackupDeletionFixture(t)
	f.run(t, false, "OBJECT_SLEEP=3\nHEARTBEAT_FAIL=true", "heartbeat failed")
	require.NotContains(t, f.log(t), "--action succeed")
	require.NotContains(t, f.log(t), "--action retry")
}

type backupDeletionFixture struct {
	dir, parameters, sourceReceipt, preManifest, postManifest, operationReceipt string
	env                                                                         []string
}

func newBackupDeletionFixture(t *testing.T) *backupDeletionFixture {
	t.Helper()
	dir := t.TempDir()
	f := &backupDeletionFixture{
		dir:              dir,
		parameters:       filepath.Join(dir, "parameters.json"),
		sourceReceipt:    filepath.Join(dir, "source.json"),
		preManifest:      filepath.Join(dir, "pre-manifest.json"),
		postManifest:     filepath.Join(dir, "post-manifest.json"),
		operationReceipt: filepath.Join(dir, "operation-receipt.json"),
	}
	require.NoError(t, os.WriteFile(f.sourceReceipt, []byte(`{"format":"kubebrain.object-backup.receipt.v1","instance":"instance-a","backup_id":"backup-1","object_store_id":"store-a","bucket":"backups","object_key":"instance-a/backup-1.jsonl","version_id":"version-1","artifact_format":"kubebrain.logical.v2","artifact_sha256":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","snapshot_revision":1,"created_at_unix":1,"records":1,"leases":0,"object_bytes":1,"retention_mode":"COMPLIANCE","retain_until_unix":2,"remote_verified":true,"uploaded_at_unix":1}
`), 0o600))
	require.NoError(t, os.WriteFile(f.preManifest, []byte(`{"format":"kubebrain.object-inventory-manifest.v1","object_store_id":"store-a","bucket":"backups","prefix":"instance-a/","entries":[{"artifact_format":"kubebrain.logical.v2","object_key":"instance-a/backup-1.jsonl","version_id":"version-1","artifact_sha256":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","object_bytes":1,"retention_mode":"COMPLIANCE","retain_until_unix":2}]}
`), 0o600))
	require.NoError(t, os.WriteFile(f.postManifest, []byte(`{"format":"kubebrain.object-inventory-manifest.v1","object_store_id":"store-a","bucket":"backups","prefix":"instance-a/","entries":[]}
`), 0o600))
	f.rewriteParameters(t)

	operationctl := filepath.Join(dir, "operationctl")
	writeTrafficExecutable(t, operationctl, `#!/usr/bin/env bash
set -euo pipefail
printf 'operationctl %s\n' "$*" >>"$FAKE_DIR/actions.log"
if [[ " $* " == *" --action claim "* ]]; then
  digest="${CLAIM_DIGEST:-$PARAMETERS_DIGEST}"
  printf '{"namespace":"tenant-a-operations","name":"delete-1","operation_id":"delete-1","instance":"instance-a","parameters_sha256":"%s","attempt":1}\n' "$digest"
elif [[ " $* " == *" --action heartbeat "* && "${HEARTBEAT_FAIL:-false}" == true ]]; then
  exit 1
else
  echo '{}'
fi
`)
	object := filepath.Join(dir, "object")
	writeTrafficExecutable(t, object, `#!/usr/bin/env bash
set -euo pipefail
stage=
if [[ "$ACTION" == inventory && "$INVENTORY_INPUT" == "$PRE_MANIFEST" ]]; then
  stage=pre
elif [[ "$ACTION" == inventory ]]; then
  stage=post
else
  stage=delete
fi
printf 'object %s\n' "$stage" >>"$FAKE_DIR/actions.log"
[[ "${FAIL_STAGE:-}" != "$stage" ]] || exit 9
sleep "${OBJECT_SLEEP:-0}"
case "$stage" in
  pre)
    printf '{"format":"kubebrain.object-inventory.receipt.v1","object_store_id":"store-a","bucket":"backups","prefix":"instance-a/","manifest_sha256":"%s","expected_versions":1,"remote_versions":1,"delete_markers":0,"all_matched":true,"checked_at_unix":1}\n' "$PRE_SHA" >"$RECEIPT_OUTPUT"
    ;;
  post)
    printf '{"format":"kubebrain.object-inventory.receipt.v1","object_store_id":"store-a","bucket":"backups","prefix":"instance-a/","manifest_sha256":"%s","expected_versions":0,"remote_versions":0,"delete_markers":0,"all_matched":true,"checked_at_unix":1}\n' "$POST_SHA" >"$RECEIPT_OUTPUT"
    ;;
  delete)
    artifact=aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa
    [[ "${INVALID_DELETE_RECEIPT:-false}" != true ]] || artifact=bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb
    printf '{"format":"kubebrain.object-backup-deletion.receipt.v1","instance":"instance-a","backup_id":"backup-1","object_store_id":"store-a","bucket":"backups","object_key":"instance-a/backup-1.jsonl","version_id":"version-1","artifact_sha256":"%s","retention_mode":"COMPLIANCE","retain_until_unix":2,"version_absent":true,"deleted_at_unix":2}\n' "$artifact" >"$DELETE_RECEIPT_OUTPUT"
    ;;
esac
`)
	f.env = []string{
		"WORKER_ID=worker-a", "PARAMETERS_INPUT=" + f.parameters,
		"OPERATION_NAMESPACE=ops", "LEASE_SECONDS=6", "OPERATIONCTL=" + operationctl,
		"OBJECT_COMMAND=" + object, "FAKE_DIR=" + dir, "PRE_MANIFEST=" + f.preManifest,
		"PRE_SHA=" + fileDigest(t, f.preManifest), "POST_SHA=" + fileDigest(t, f.postManifest),
	}
	return f
}

func (f *backupDeletionFixture) rewriteParameters(t *testing.T) {
	t.Helper()
	content := fmt.Sprintf(`{
  "backup_id":"backup-1","object_store_id":"store-a","s3_endpoint":"https://s3.example",
  "s3_force_path_style":false,"aws_region":"us-east-1",
  "source_receipt_input":%q,"source_receipt_sha256":"%s",
  "pre_manifest_input":%q,"pre_manifest_sha256":"%s",
  "pre_inventory_receipt_output":%q,"deletion_receipt_output":%q,
  "post_manifest_input":%q,"post_manifest_sha256":"%s",
  "post_inventory_receipt_output":%q,"operation_receipt_output":%q
}`, f.sourceReceipt, fileDigest(t, f.sourceReceipt), f.preManifest, fileDigest(t, f.preManifest),
		filepath.Join(f.dir, "pre-inventory.json"), filepath.Join(f.dir, "deletion.json"),
		f.postManifest, fileDigest(t, f.postManifest), filepath.Join(f.dir, "post-inventory.json"),
		f.operationReceipt)
	require.NoError(t, os.WriteFile(f.parameters, []byte(content), 0o600))
}

func (f *backupDeletionFixture) run(t *testing.T, ok bool, extra string, outputs ...string) {
	t.Helper()
	env := append([]string{}, f.env...)
	env = append(env, "PARAMETERS_DIGEST="+fileDigest(t, f.parameters))
	for _, item := range strings.Split(extra, "\n") {
		if item != "" {
			env = append(env, item)
		}
	}
	cmd := exec.Command("bash", "run-backup-deletion-operation.sh")
	cmd.Env = append(os.Environ(), env...)
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

func (f *backupDeletionFixture) log(t *testing.T) string {
	t.Helper()
	return string(mustRead(t, filepath.Join(f.dir, "actions.log")))
}

func fileDigest(t *testing.T, path string) string {
	t.Helper()
	sum := sha256.Sum256(mustRead(t, path))
	return fmt.Sprintf("%x", sum)
}
