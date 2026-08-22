package production_test

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRequestDestroyCreatesOnlyBoundUnapprovedOperation(t *testing.T) {
	dir := t.TempDir()
	backup := filepath.Join(dir, "final.jsonl")
	require.NoError(t, os.WriteFile(backup, []byte("backup\n"), 0o600))
	kubectlLog := filepath.Join(dir, "kubectl.log")
	operationLog := filepath.Join(dir, "operation.log")
	kubectl := filepath.Join(dir, "kubectl")
	operationctl := filepath.Join(dir, "operationctl")
	require.NoError(t, os.WriteFile(kubectl, []byte(`#!/usr/bin/env bash
set -euo pipefail
printf '%s\n' "$*" >>"$KUBECTL_LOG"
if [[ "$*" == *" get secret "* ]]; then exit 1; fi
if [[ "$*" == *" --dry-run=client "* ]]; then printf '{"apiVersion":"v1","kind":"Secret","metadata":{},"type":"Opaque","data":{}}\n'; exit 0; fi
if [[ "$*" == *" create -f -"* ]]; then cat >/dev/null; exit 0; fi
exit 99
`), 0o755))
	require.NoError(t, os.WriteFile(operationctl, []byte(`#!/usr/bin/env bash
set -euo pipefail
printf '%s\n' "$*" >>"$OPERATION_LOG"
`), 0o755))
	env := []string{
		"REQUEST_ID=change-2026-destroy-1", "INSTANCE=instance-a", "WORK_DIR=" + dir,
		"BACKUP_INPUT=" + backup, "KUBEBRAIN_NAMESPACE=instance-a", "TIDB_NAMESPACE=storage-a", "TIDB_CLUSTER=kb",
		"KUBE_CONTEXT=production", "KUBECTL=" + kubectl, "OPERATIONCTL=" + operationctl,
		"KUBECTL_LOG=" + kubectlLog, "OPERATION_LOG=" + operationLog,
	}
	output, err := runProductionScriptCommand(t, "request-destroy.sh", env)
	require.NoError(t, err, string(output))
	require.Contains(t, string(output), "unapproved Pending Destroy")
	operation := string(mustRead(t, operationLog))
	require.Contains(t, operation, "--requested-by platform:destroy")
	require.Contains(t, operation, "--type Destroy")
	require.Contains(t, operation, "--max-attempts 5")
	require.Contains(t, operation, "--parameters-key parameters.json")
	require.NotContains(t, operation, "approve")
	require.NotContains(t, string(mustRead(t, kubectlLog)), " patch ")
}

func TestRequestDestroyRejectsBackupOutsideWorkspaceBeforeKubernetes(t *testing.T) {
	dir := t.TempDir()
	outside := filepath.Join(t.TempDir(), "backup.jsonl")
	require.NoError(t, os.WriteFile(outside, []byte("backup\n"), 0o600))
	marker := filepath.Join(dir, "called")
	command := filepath.Join(dir, "command")
	require.NoError(t, os.WriteFile(command, []byte("#!/usr/bin/env bash\nprintf called >\"$MARKER\"\n"), 0o755))
	output, err := runProductionScriptCommand(t, "request-destroy.sh", []string{
		"REQUEST_ID=change-2026-destroy-2", "INSTANCE=instance-a", "WORK_DIR=" + dir,
		"BACKUP_INPUT=" + outside, "KUBEBRAIN_NAMESPACE=instance-a", "TIDB_NAMESPACE=storage-a", "TIDB_CLUSTER=kb",
		"KUBE_CONTEXT=production", "KUBECTL=" + command, "OPERATIONCTL=" + command, "MARKER=" + marker,
	})
	require.Error(t, err)
	require.Contains(t, string(output), "executor workspace")
	require.NoFileExists(t, marker)
}
