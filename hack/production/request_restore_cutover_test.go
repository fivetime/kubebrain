package production_test

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRequestRestoreCutoverCreatesOnlyBoundUnapprovedOperation(t *testing.T) {
	dir := t.TempDir()
	restore := filepath.Join(dir, "restore.json")
	backup := filepath.Join(dir, "backup.jsonl")
	for _, path := range []string{restore, backup} {
		require.NoError(t, os.WriteFile(path, []byte("evidence\n"), 0o600))
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
	output, err := runProductionScriptCommand(t, "request-restore-cutover.sh", []string{
		"REQUEST_ID=change-2026-cutover-1", "INSTANCE=instance-a", "WORK_DIR=" + dir,
		"RESTORE_RECEIPT_INPUT=" + restore, "BACKUP_INPUT=" + backup,
		"SERVICE_NAMESPACE=instance-a", "SERVICE_NAME=kubebrain", "SOURCE_INSTANCE=source", "TARGET_INSTANCE=target",
		"EXPECTED_REPLICAS=3", "PUBLIC_ENDPOINT=https://kubebrain.example:2379",
		"KUBE_CONTEXT=production", "KUBECTL=" + kubectl, "OPERATIONCTL=" + operationctl,
		"KUBECTL_LOG=" + kubectlLog, "OPERATION_LOG=" + operationLog,
	})
	require.NoError(t, err, string(output))
	require.Contains(t, string(output), "unapproved Pending RestoreCutover")
	operation := string(mustRead(t, operationLog))
	require.Contains(t, operation, "--requested-by platform:restore-cutover")
	require.Contains(t, operation, "--type RestoreCutover")
	require.Contains(t, operation, "--max-attempts 5")
	require.Contains(t, operation, "--parameters-key parameters.json")
	require.NotContains(t, operation, "approve")
	require.NotContains(t, string(mustRead(t, kubectlLog)), " patch ")
}

func TestRequestRestoreCutoverRejectsEvidenceOutsideWorkspaceBeforeKubernetes(t *testing.T) {
	dir := t.TempDir()
	inside := filepath.Join(dir, "backup.jsonl")
	require.NoError(t, os.WriteFile(inside, []byte("backup\n"), 0o600))
	outside := filepath.Join(t.TempDir(), "restore.json")
	require.NoError(t, os.WriteFile(outside, []byte("{}\n"), 0o600))
	marker := filepath.Join(dir, "called")
	command := filepath.Join(dir, "command")
	writeTrafficExecutable(t, command, "#!/usr/bin/env bash\nprintf called >\"$MARKER\"\n")
	output, err := runProductionScriptCommand(t, "request-restore-cutover.sh", []string{
		"REQUEST_ID=change-2026-cutover-2", "INSTANCE=instance-a", "WORK_DIR=" + dir,
		"RESTORE_RECEIPT_INPUT=" + outside, "BACKUP_INPUT=" + inside,
		"SERVICE_NAMESPACE=instance-a", "SERVICE_NAME=kubebrain", "SOURCE_INSTANCE=source", "TARGET_INSTANCE=target",
		"EXPECTED_REPLICAS=3", "PUBLIC_ENDPOINT=https://kubebrain.example:2379",
		"KUBE_CONTEXT=production", "KUBECTL=" + command, "OPERATIONCTL=" + command, "MARKER=" + marker,
	})
	require.Error(t, err)
	require.Contains(t, string(output), "executor workspace")
	require.NoFileExists(t, marker)
}
