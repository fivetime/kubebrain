package production_test

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRequestPostRestoreAuditCreatesBoundUnapprovedOperation(t *testing.T) {
	dir := t.TempDir()
	state, receipt := filepath.Join(dir, "cutover.state"), filepath.Join(dir, "cutover.json")
	require.NoError(t, os.WriteFile(state, []byte("state\n"), 0o600))
	require.NoError(t, os.WriteFile(receipt, []byte("{}\n"), 0o600))
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
	out, err := runProductionScriptCommand(t, "request-post-restore-audit.sh", []string{
		"REQUEST_ID=audit-change-1", "INSTANCE=instance-a", "WORK_DIR=" + dir,
		"CUTOVER_STATE_INPUT=" + state, "CUTOVER_RECEIPT_INPUT=" + receipt,
		"SERVICE_NAMESPACE=ns-a", "SERVICE_NAME=kubebrain", "TARGET_INSTANCE=target",
		"EXPECTED_REPLICAS=3", "PUBLIC_ENDPOINT=https://service:2379",
		"AUDIT_DURATION_SECONDS=60", "AUDIT_INTERVAL_SECONDS=5", "MIN_SAMPLES=3", "AUDIT_PREFIX=/audit",
		"KUBE_CONTEXT=prod", "KUBECTL=" + kubectl, "OPERATIONCTL=" + operationctl,
		"KUBECTL_LOG=" + kubectlLog, "OPERATION_LOG=" + operationLog,
	})
	require.NoError(t, err, string(out))
	operation := string(mustRead(t, operationLog))
	require.Contains(t, operation, "--requested-by platform:post-restore-audit")
	require.Contains(t, operation, "--type PostRestoreAudit")
	require.Contains(t, operation, "--max-attempts 5")
	require.NotContains(t, operation, "approve")
	require.NotContains(t, string(mustRead(t, kubectlLog)), " patch ")
}

func TestRequestPostRestoreAuditRejectsOutsideEvidenceBeforeKubernetes(t *testing.T) {
	dir := t.TempDir()
	outsideDir := t.TempDir()
	outside := filepath.Join(outsideDir, "cutover.state")
	inside := filepath.Join(dir, "cutover.json")
	require.NoError(t, os.WriteFile(outside, []byte("state\n"), 0o600))
	require.NoError(t, os.WriteFile(inside, []byte("{}\n"), 0o600))
	marker, command := filepath.Join(dir, "called"), filepath.Join(dir, "command")
	writeTrafficExecutable(t, command, "#!/usr/bin/env bash\nprintf x >\"$MARKER\"\n")
	out, err := runProductionScriptCommand(t, "request-post-restore-audit.sh", []string{
		"REQUEST_ID=audit-change-2", "INSTANCE=instance-a", "WORK_DIR=" + dir,
		"CUTOVER_STATE_INPUT=" + outside, "CUTOVER_RECEIPT_INPUT=" + inside,
		"SERVICE_NAMESPACE=ns-a", "SERVICE_NAME=kubebrain", "TARGET_INSTANCE=target",
		"EXPECTED_REPLICAS=3", "PUBLIC_ENDPOINT=https://service:2379",
		"AUDIT_DURATION_SECONDS=60", "AUDIT_INTERVAL_SECONDS=5", "MIN_SAMPLES=3", "AUDIT_PREFIX=/audit",
		"KUBE_CONTEXT=prod", "KUBECTL=" + command, "OPERATIONCTL=" + command, "MARKER=" + marker,
	})
	require.Error(t, err)
	require.Contains(t, string(out), "in WORK_DIR")
	require.NoFileExists(t, marker)
}
