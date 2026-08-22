package production_test

import (
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRequestBackupCreatesBoundUnapprovedOperation(t *testing.T) {
	dir := t.TempDir()
	kubectlLog, operationLog := filepath.Join(dir, "kubectl.log"), filepath.Join(dir, "operation.log")
	kubectl, operationctl := filepath.Join(dir, "kubectl"), filepath.Join(dir, "operationctl")
	writeTrafficExecutable(t, kubectl, `#!/usr/bin/env bash
set -euo pipefail
printf '%s\n' "$*" >>"$KUBECTL_LOG"
if [[ "$*" == *" get secret "* ]]; then exit 1; fi
if [[ "$*" == *" --dry-run=client "* ]]; then
  echo '{"apiVersion":"v1","kind":"Secret","data":{}}'
  exit 0
fi
if [[ "$*" == *" create -f -"* ]]; then cat >/dev/null; exit 0; fi
exit 99
`)
	writeTrafficExecutable(t, operationctl, "#!/usr/bin/env bash\nprintf '%s\\n' \"$*\" >>\"$OPERATION_LOG\"\n")
	out, err := runProductionScriptCommand(t, "request-backup.sh", []string{
		"REQUEST_ID=backup-change-1", "INSTANCE=instance-a", "ENDPOINT=https://etcd:2379", "WORK_DIR=" + dir,
		"OBJECT_STORE_ID=store-a", "S3_ENDPOINT=https://s3.example", "S3_BUCKET=backups",
		"S3_OBJECT_KEY=instance-a/b.jsonl", "AWS_REGION=us-east-1", "RETAIN_UNTIL_UNIX=2000000000",
		"KUBE_CONTEXT=prod", "KUBECTL=" + kubectl, "OPERATIONCTL=" + operationctl,
		"KUBECTL_LOG=" + kubectlLog, "OPERATION_LOG=" + operationLog,
	})
	require.NoError(t, err, string(out))
	operation := string(mustRead(t, operationLog))
	require.Contains(t, operation, "--requested-by platform:backup")
	require.Contains(t, operation, "--max-attempts 5")
	require.NotContains(t, operation, "approve")
	require.NotContains(t, string(mustRead(t, kubectlLog)), " patch ")
}

func TestRequestBackupRejectsInvalidWorkspaceBeforeKubernetes(t *testing.T) {
	dir := t.TempDir()
	marker, command := filepath.Join(dir, "called"), filepath.Join(dir, "command")
	writeTrafficExecutable(t, command, "#!/usr/bin/env bash\nprintf x >\"$MARKER\"\n")
	out, err := runProductionScriptCommand(t, "request-backup.sh", []string{
		"REQUEST_ID=backup-change-2", "INSTANCE=instance-a", "ENDPOINT=https://etcd:2379",
		"WORK_DIR=" + filepath.Join(dir, "missing"), "OBJECT_STORE_ID=store-a",
		"S3_ENDPOINT=https://s3.example", "S3_BUCKET=backups", "S3_OBJECT_KEY=b.jsonl",
		"AWS_REGION=us-east-1", "RETAIN_UNTIL_UNIX=2000000000", "KUBE_CONTEXT=prod",
		"KUBECTL=" + command, "OPERATIONCTL=" + command, "MARKER=" + marker,
	})
	require.Error(t, err)
	require.Contains(t, string(out), "workspace")
	require.NoFileExists(t, marker)
}
