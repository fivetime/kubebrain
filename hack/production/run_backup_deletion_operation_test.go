package production_test

import (
	"crypto/sha256"
	"encoding/json"
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

func TestBackupDeletionOperationTreatsConcurrentReceiptPublishAsIdempotent(t *testing.T) {
	f := newBackupDeletionFixture(t)
	f.run(t, true, "PUBLISH_BACKUP_DELETION_OPERATION_RECEIPT_DURING_JQ=valid")

	var receipt map[string]any
	require.NoError(t, json.Unmarshal(mustRead(t, f.operationReceipt), &receipt))
	require.Equal(t, "kubebrain.backup-deletion-operation.receipt.v1", receipt["format"])
	require.Equal(t, float64(1), receipt["completed_at_unix"])
	require.Empty(t, f.temporaryReceiptFiles(t))
	require.Contains(t, f.log(t), "--action succeed")
	require.NotContains(t, f.log(t), "--action retry")
}

func TestBackupDeletionOperationCleansTemporaryReceiptWhenConcurrentReceiptDrifts(t *testing.T) {
	f := newBackupDeletionFixture(t)
	f.run(t, false, "PUBLISH_BACKUP_DELETION_OPERATION_RECEIPT_DURING_JQ=drift", "existing backup deletion operation receipt differs")

	require.Empty(t, f.temporaryReceiptFiles(t))
	require.FileExists(t, f.operationReceipt)
	require.NotContains(t, f.log(t), "--action succeed")
}

func TestBackupDeletionOperationRejectsReceiptTamperedDuringDigest(t *testing.T) {
	f := newBackupDeletionFixture(t)
	f.run(t, false, "TAMPER_RECEIPT_DURING_SHA256=true", "operation receipt is invalid")
	require.NotContains(t, f.log(t), "--action succeed")
}

func TestBackupDeletionOperationRejectsEvidenceTamperedDuringDigest(t *testing.T) {
	for _, tc := range []struct {
		name string
		path func(*backupDeletionFixture) string
		want string
	}{
		{
			name: "pre inventory",
			path: func(f *backupDeletionFixture) string {
				return f.preInventoryReceipt
			},
			want: "pre-delete inventory receipt is invalid",
		},
		{
			name: "deletion",
			path: func(f *backupDeletionFixture) string {
				return f.deletionReceipt
			},
			want: "backup deletion receipt is invalid",
		},
		{
			name: "post inventory",
			path: func(f *backupDeletionFixture) string {
				return f.postInventoryReceipt
			},
			want: "post-delete inventory receipt is invalid",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newBackupDeletionFixture(t)
			f.run(t, false, "TAMPER_RECEIPT_DURING_SHA256=true\nRUNNER_RECEIPT_OUTPUT="+tc.path(f), tc.want)
			require.NotContains(t, f.log(t), "--action succeed")
		})
	}
}

func TestBackupDeletionOperationRejectsSourceReceiptTamperedDuringParse(t *testing.T) {
	f := newBackupDeletionFixture(t)
	f.run(t, false, "TAMPER_SOURCE_RECEIPT_DURING_JQ=true", "evidence digest mismatch")
	log := f.log(t)
	require.NotContains(t, log, "object ")
	require.NotContains(t, log, "--action succeed")
}

func TestBackupDeletionOperationRejectsExistingReceiptWithUnknownFields(t *testing.T) {
	f := newBackupDeletionFixture(t)
	f.run(t, true, "")

	receipt := strings.TrimSpace(string(mustRead(t, f.operationReceipt)))
	receipt = strings.TrimSuffix(receipt, "}") + `,"unexpected":true}` + "\n"
	require.NoError(t, os.WriteFile(f.operationReceipt, []byte(receipt), 0o600))

	f.run(t, false, "", "existing backup deletion operation receipt differs")
	require.Equal(t, receipt, string(mustRead(t, f.operationReceipt)))
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
	t.Run("empty required parameter", func(t *testing.T) {
		f := newBackupDeletionFixture(t)
		parameters := strings.ReplaceAll(
			string(mustRead(t, f.parameters)),
			`"s3_endpoint":"https://s3.example"`,
			`"s3_endpoint":""`,
		)
		require.NoError(t, os.WriteFile(f.parameters, []byte(parameters), 0o600))

		f.run(t, false, "", "empty required field")
		log := f.log(t)
		require.NotContains(t, log, "object ")
		require.NotContains(t, log, "--action retry")
		require.NotContains(t, log, "--action succeed")
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
	t.Run("pre inventory receipt unknown field", func(t *testing.T) {
		f := newBackupDeletionFixture(t)
		f.run(t, false, "EXTRA_PRE_RECEIPT=true", "pre-delete inventory receipt is invalid")
		require.NotContains(t, f.log(t), "--action succeed")
	})
	t.Run("deletion receipt unknown field", func(t *testing.T) {
		f := newBackupDeletionFixture(t)
		f.run(t, false, "EXTRA_DELETE_RECEIPT=true", "backup deletion receipt is invalid")
		require.NotContains(t, f.log(t), "--action succeed")
	})
	t.Run("post inventory receipt unknown field", func(t *testing.T) {
		f := newBackupDeletionFixture(t)
		f.run(t, false, "EXTRA_POST_RECEIPT=true", "post-delete inventory receipt is invalid")
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
	dir, parameters, sourceReceipt, preManifest, postManifest  string
	preInventoryReceipt, deletionReceipt, postInventoryReceipt string
	operationReceipt                                           string
	env                                                        []string
}

func newBackupDeletionFixture(t *testing.T) *backupDeletionFixture {
	t.Helper()
	dir := t.TempDir()
	f := &backupDeletionFixture{
		dir:                  dir,
		parameters:           filepath.Join(dir, "parameters.json"),
		sourceReceipt:        filepath.Join(dir, "source.json"),
		preManifest:          filepath.Join(dir, "pre-manifest.json"),
		postManifest:         filepath.Join(dir, "post-manifest.json"),
		preInventoryReceipt:  filepath.Join(dir, "pre-inventory.json"),
		deletionReceipt:      filepath.Join(dir, "deletion.json"),
		postInventoryReceipt: filepath.Join(dir, "post-inventory.json"),
		operationReceipt:     filepath.Join(dir, "operation-receipt.json"),
	}
	require.NoError(t, os.WriteFile(f.sourceReceipt, []byte(`{"format":"kubebrain.object-backup.receipt.v1","instance":"instance-a","backup_id":"backup-1","object_store_id":"store-a","bucket":"backups","object_key":"instance-a/backup-1.jsonl","version_id":"version-1","artifact_format":"kubebrain.logical.v2","artifact_sha256":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","snapshot_revision":1,"created_at_unix":1,"records":1,"leases":0,"object_bytes":1,"retention_mode":"COMPLIANCE","retain_until_unix":2,"remote_verified":true,"uploaded_at_unix":1}
`), 0o600))
	require.NoError(t, os.WriteFile(f.preManifest, []byte(`{"format":"kubebrain.object-inventory-manifest.v1","object_store_id":"store-a","bucket":"backups","prefix":"instance-a/","entries":[{"artifact_format":"kubebrain.logical.v2","object_key":"instance-a/backup-1.jsonl","version_id":"version-1","artifact_sha256":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","object_bytes":1,"retention_mode":"COMPLIANCE","retain_until_unix":2}]}
`), 0o600))
	require.NoError(t, os.WriteFile(f.postManifest, []byte(`{"format":"kubebrain.object-inventory-manifest.v1","object_store_id":"store-a","bucket":"backups","prefix":"instance-a/","entries":[]}
`), 0o600))
	tamperedSourceReceipt := filepath.Join(dir, "tampered-source.json")
	require.NoError(t, os.WriteFile(tamperedSourceReceipt, []byte(`{"format":"kubebrain.object-backup.receipt.v1","instance":"instance-a","backup_id":"backup-1","object_store_id":"store-a","bucket":"backups","object_key":"instance-a/backup-1.jsonl","version_id":"version-1","artifact_format":"kubebrain.logical.v2","artifact_sha256":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","snapshot_revision":1,"created_at_unix":1,"records":2,"leases":0,"object_bytes":1,"retention_mode":"COMPLIANCE","retain_until_unix":2,"remote_verified":true,"uploaded_at_unix":1}
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
    extra=
    [[ "${EXTRA_PRE_RECEIPT:-false}" != true ]] || extra=',"unexpected":true'
    printf '{"format":"kubebrain.object-inventory.receipt.v1","object_store_id":"store-a","bucket":"backups","prefix":"instance-a/","manifest_sha256":"%s","expected_versions":1,"remote_versions":1,"delete_markers":0,"all_matched":true,"checked_at_unix":1%s}\n' "$PRE_SHA" "$extra" >"$RECEIPT_OUTPUT"
    ;;
  post)
    extra=
    [[ "${EXTRA_POST_RECEIPT:-false}" != true ]] || extra=',"unexpected":true'
    printf '{"format":"kubebrain.object-inventory.receipt.v1","object_store_id":"store-a","bucket":"backups","prefix":"instance-a/","manifest_sha256":"%s","expected_versions":0,"remote_versions":0,"delete_markers":0,"all_matched":true,"checked_at_unix":1%s}\n' "$POST_SHA" "$extra" >"$RECEIPT_OUTPUT"
    ;;
  delete)
    artifact=aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa
    [[ "${INVALID_DELETE_RECEIPT:-false}" != true ]] || artifact=bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb
    extra=
    [[ "${EXTRA_DELETE_RECEIPT:-false}" != true ]] || extra=',"unexpected":true'
    printf '{"format":"kubebrain.object-backup-deletion.receipt.v1","instance":"instance-a","backup_id":"backup-1","object_store_id":"store-a","bucket":"backups","object_key":"instance-a/backup-1.jsonl","version_id":"version-1","artifact_sha256":"%s","retention_mode":"COMPLIANCE","retain_until_unix":2,"version_absent":true,"deleted_at_unix":2%s}\n' "$artifact" "$extra" >"$DELETE_RECEIPT_OUTPUT"
    ;;
esac
`)
	realJQ, err := exec.LookPath("jq")
	require.NoError(t, err)
	jq := filepath.Join(dir, "jq-wrapper")
	writeTrafficExecutable(t, jq, `#!/usr/bin/env bash
set -euo pipefail
if [[ "${TAMPER_SOURCE_RECEIPT_DURING_JQ:-false}" == true && "$#" -ge 1 ]]; then
  last_arg="${@: -1}"
  if [[ "$last_arg" == "$SOURCE_RECEIPT_INPUT" &&
    ! -f "$FAKE_DIR/source-receipt-tampered-during-jq" ]]; then
    cp "$TAMPERED_SOURCE_RECEIPT" "$SOURCE_RECEIPT_INPUT"
    chmod 600 "$SOURCE_RECEIPT_INPUT"
    touch "$FAKE_DIR/source-receipt-tampered-during-jq"
  fi
fi
"$REAL_JQ" "$@"
mode="${PUBLISH_BACKUP_DELETION_OPERATION_RECEIPT_DURING_JQ:-}"
if [[ -n "$mode" && "$mode" != false &&
  " $* " == *" -cnS "* && " $* " == *"kubebrain.backup-deletion-operation.receipt.v1"* &&
  ! -f "$OPERATION_RECEIPT_OUTPUT" ]]; then
  object_key="$BACKUP_DELETION_OBJECT_KEY"
  if [[ "$mode" == drift ]]; then
    object_key="${object_key}.drift"
  fi
  pre_inventory_sha="$(sha256sum "$PRE_INVENTORY_RECEIPT" | cut -d ' ' -f1)"
  deletion_sha="$(sha256sum "$DELETION_RECEIPT" | cut -d ' ' -f1)"
  post_inventory_sha="$(sha256sum "$POST_INVENTORY_RECEIPT" | cut -d ' ' -f1)"
  printf '{"completed_at_unix":1,"deletion_receipt_sha256":"%s","format":"kubebrain.backup-deletion-operation.receipt.v1","instance":"%s","object_key":"%s","operation_id":"%s","post_inventory_receipt_sha256":"%s","post_manifest_sha256":"%s","pre_inventory_receipt_sha256":"%s","pre_manifest_sha256":"%s","source_receipt_sha256":"%s","version_id":"%s"}\n' \
    "$deletion_sha" "$BACKUP_DELETION_INSTANCE" "$object_key" "$BACKUP_DELETION_OPERATION_ID" \
    "$post_inventory_sha" "$POST_SHA" "$pre_inventory_sha" "$PRE_SHA" "$SOURCE_SHA" \
    "$BACKUP_DELETION_VERSION_ID" >"$OPERATION_RECEIPT_OUTPUT"
  chmod 600 "$OPERATION_RECEIPT_OUTPUT"
fi
`)
	f.env = []string{
		"WORKER_ID=worker-a", "PARAMETERS_INPUT=" + f.parameters,
		"OPERATION_NAMESPACE=ops", "LEASE_SECONDS=6", "OPERATIONCTL=" + operationctl,
		"OBJECT_COMMAND=" + object, "FAKE_DIR=" + dir, "PRE_MANIFEST=" + f.preManifest,
		"PRE_SHA=" + fileDigest(t, f.preManifest), "POST_SHA=" + fileDigest(t, f.postManifest),
		"SOURCE_SHA=" + fileDigest(t, f.sourceReceipt), "JQ=" + jq, "REAL_JQ=" + realJQ,
		"PRE_INVENTORY_RECEIPT=" + f.preInventoryReceipt, "DELETION_RECEIPT=" + f.deletionReceipt,
		"POST_INVENTORY_RECEIPT=" + f.postInventoryReceipt,
		"OPERATION_RECEIPT_OUTPUT=" + f.operationReceipt,
		"SOURCE_RECEIPT_INPUT=" + f.sourceReceipt,
		"TAMPERED_SOURCE_RECEIPT=" + tamperedSourceReceipt,
		"BACKUP_DELETION_OPERATION_ID=delete-1", "BACKUP_DELETION_INSTANCE=instance-a",
		"BACKUP_DELETION_OBJECT_KEY=instance-a/backup-1.jsonl",
		"BACKUP_DELETION_VERSION_ID=version-1",
	}
	f.env = append(f.env, receiptDigestTamperEnv(t, dir, f.operationReceipt)...)
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
		f.preInventoryReceipt, f.deletionReceipt,
		f.postManifest, fileDigest(t, f.postManifest), f.postInventoryReceipt,
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

func (f *backupDeletionFixture) temporaryReceiptFiles(t *testing.T) []string {
	t.Helper()
	matches, err := filepath.Glob(filepath.Join(f.dir, ".backup-deletion-receipt.*"))
	require.NoError(t, err)
	return matches
}

func fileDigest(t *testing.T, path string) string {
	t.Helper()
	sum := sha256.Sum256(mustRead(t, path))
	return fmt.Sprintf("%x", sum)
}
