package production_test

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRequestBackupDeletionCreatesOnlyBoundUnapprovedOperation(t *testing.T) {
	dir := t.TempDir()
	source := filepath.Join(dir, "source.json")
	pre := filepath.Join(dir, "pre.json")
	post := filepath.Join(dir, "post.json")
	for _, path := range []string{source, pre, post} {
		require.NoError(t, os.WriteFile(path, []byte("{}\n"), 0o600))
	}
	kubectlLog := filepath.Join(dir, "kubectl.log")
	operationLog := filepath.Join(dir, "operation.log")
	kubectl := filepath.Join(dir, "kubectl")
	operationctl := filepath.Join(dir, "operationctl")
	writeTrafficExecutable(t, kubectl, `#!/usr/bin/env bash
set -euo pipefail
printf '%s\n' "$*" >>"$KUBECTL_LOG"
if [[ "$*" == *" get secret "* ]]; then exit 1; fi
if [[ "$*" == *" --dry-run=client "* ]]; then printf '{"apiVersion":"v1","kind":"Secret","metadata":{},"type":"Opaque","data":{}}\n'; exit 0; fi
if [[ "$*" == *" create -f -"* ]]; then cat >/dev/null; exit 0; fi
exit 99
`)
	writeTrafficExecutable(t, operationctl, "#!/usr/bin/env bash\nset -euo pipefail\nprintf '%s\\n' \"$*\" >>\"$OPERATION_LOG\"\n")
	output, err := runProductionScriptCommand(t, "request-backup-deletion.sh", []string{
		"REQUEST_ID=change-2026-delete-1", "INSTANCE=instance-a", "BACKUP_ID=backup-1", "OBJECT_STORE_ID=store-a",
		"S3_ENDPOINT=https://s3.example", "AWS_REGION=us-east-1", "WORK_DIR=" + dir,
		"SOURCE_RECEIPT_INPUT=" + source, "PRE_MANIFEST_INPUT=" + pre, "POST_MANIFEST_INPUT=" + post,
		"KUBE_CONTEXT=production", "KUBECTL=" + kubectl, "OPERATIONCTL=" + operationctl,
		"KUBECTL_LOG=" + kubectlLog, "OPERATION_LOG=" + operationLog,
	})
	require.NoError(t, err, string(output))
	require.Contains(t, string(output), "unapproved Pending BackupDeletion")
	operation := string(mustRead(t, operationLog))
	require.Contains(t, operation, "--requested-by platform:backup-deletion")
	require.Contains(t, operation, "--type BackupDeletion")
	require.Contains(t, operation, "--max-attempts 5")
	require.Contains(t, operation, "--parameters-key parameters.json")
	require.NotContains(t, operation, "approve")
	require.NotContains(t, string(mustRead(t, kubectlLog)), " patch ")
}

func TestRequestBackupDeletionRejectsEvidenceOutsideWorkspaceBeforeKubernetes(t *testing.T) {
	dir := t.TempDir()
	outside := filepath.Join(t.TempDir(), "source.json")
	require.NoError(t, os.WriteFile(outside, []byte("{}\n"), 0o600))
	inside := filepath.Join(dir, "manifest.json")
	require.NoError(t, os.WriteFile(inside, []byte("{}\n"), 0o600))
	marker := filepath.Join(dir, "called")
	command := filepath.Join(dir, "command")
	writeTrafficExecutable(t, command, "#!/usr/bin/env bash\nprintf called >\"$MARKER\"\n")
	output, err := runProductionScriptCommand(t, "request-backup-deletion.sh", []string{
		"REQUEST_ID=change-2026-delete-2", "INSTANCE=instance-a", "BACKUP_ID=backup-1", "OBJECT_STORE_ID=store-a",
		"S3_ENDPOINT=https://s3.example", "AWS_REGION=us-east-1",
		"WORK_DIR=" + dir, "SOURCE_RECEIPT_INPUT=" + outside, "PRE_MANIFEST_INPUT=" + inside, "POST_MANIFEST_INPUT=" + inside,
		"KUBE_CONTEXT=production", "KUBECTL=" + command, "OPERATIONCTL=" + command, "MARKER=" + marker,
	})
	require.Error(t, err)
	require.Contains(t, string(output), "executor workspace")
	require.NoFileExists(t, marker)
}
