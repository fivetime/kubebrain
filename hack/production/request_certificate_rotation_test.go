package production_test

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRequestCertificateRotationCreatesOnlyBoundUnapprovedOperation(t *testing.T) {
	dir := t.TempDir()
	paths := map[string]string{}
	for _, name := range []string{"old-ca", "old-cert", "old-key", "new-ca", "new-cert", "new-key", "overlap-ca"} {
		paths[name] = filepath.Join(dir, name)
		require.NoError(t, os.WriteFile(paths[name], []byte(name), 0o600))
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
	output, err := runProductionScriptCommand(t, "request-certificate-rotation.sh", []string{
		"REQUEST_ID=change-2026-cert-1", "INSTANCE=instance-a", "WORK_DIR=" + dir,
		"ENDPOINT=https://instance.example:2379", "OLD_CACERT=" + paths["old-ca"], "OLD_CERT=" + paths["old-cert"], "OLD_KEY=" + paths["old-key"],
		"NEW_CACERT=" + paths["new-ca"], "NEW_CERT=" + paths["new-cert"], "NEW_KEY=" + paths["new-key"], "OVERLAP_CACERT=" + paths["overlap-ca"],
		"KUBEBRAIN_NAMESPACE=instance-a", "POD_SELECTOR=app.kubernetes.io/name=kubebrain", "EXPECTED_REPLICAS=3",
		"KUBE_CONTEXT=production", "KUBECTL=" + kubectl, "OPERATIONCTL=" + operationctl,
		"KUBECTL_LOG=" + kubectlLog, "OPERATION_LOG=" + operationLog,
	})
	require.NoError(t, err, string(output))
	require.Contains(t, string(output), "unapproved Pending CertificateRotation")
	operation := string(mustRead(t, operationLog))
	require.Contains(t, operation, "--requested-by platform:certificate-rotation")
	require.Contains(t, operation, "--type CertificateRotation")
	require.Contains(t, operation, "--max-attempts 5")
	require.Contains(t, operation, "--parameters-key parameters.json")
	require.NotContains(t, operation, "approve")
	require.NotContains(t, string(mustRead(t, kubectlLog)), " patch ")
}

func TestRequestCertificateRotationRejectsCredentialOutsideWorkspaceBeforeKubernetes(t *testing.T) {
	dir := t.TempDir()
	inside := filepath.Join(dir, "credential")
	require.NoError(t, os.WriteFile(inside, []byte("credential"), 0o600))
	outside := filepath.Join(t.TempDir(), "key")
	require.NoError(t, os.WriteFile(outside, []byte("key"), 0o600))
	marker := filepath.Join(dir, "called")
	command := filepath.Join(dir, "command")
	writeTrafficExecutable(t, command, "#!/usr/bin/env bash\nprintf called >\"$MARKER\"\n")
	output, err := runProductionScriptCommand(t, "request-certificate-rotation.sh", []string{
		"REQUEST_ID=change-2026-cert-2", "INSTANCE=instance-a", "WORK_DIR=" + dir,
		"ENDPOINT=https://instance.example:2379", "OLD_CACERT=" + inside, "OLD_CERT=" + inside, "OLD_KEY=" + outside,
		"NEW_CACERT=" + inside, "NEW_CERT=" + inside, "NEW_KEY=" + inside, "OVERLAP_CACERT=" + inside,
		"KUBEBRAIN_NAMESPACE=instance-a", "POD_SELECTOR=app=kubebrain", "EXPECTED_REPLICAS=3",
		"KUBE_CONTEXT=production", "KUBECTL=" + command, "OPERATIONCTL=" + command, "MARKER=" + marker,
	})
	require.Error(t, err)
	require.Contains(t, string(output), "executor workspace")
	require.NoFileExists(t, marker)
}
