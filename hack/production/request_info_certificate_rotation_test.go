package production_test

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRequestInfoCertificateRotationCreatesOnlyBoundUnapprovedOperation(t *testing.T) {
	dir := t.TempDir()
	paths := map[string]string{}
	for _, name := range []string{"old-ca", "old-cert", "new-ca", "new-cert", "prometheus-ca"} {
		paths[name] = filepath.Join(dir, name)
		require.NoError(t, os.WriteFile(paths[name], []byte(name), 0o600))
	}
	kubectlLog, operationLog := filepath.Join(dir, "kubectl.log"), filepath.Join(dir, "operation.log")
	kubectl, operationctl := filepath.Join(dir, "kubectl"), filepath.Join(dir, "operationctl")
	writeTrafficExecutable(t, kubectl, "#!/usr/bin/env bash\nset -euo pipefail\nprintf '%s\\n' \"$*\" >>\"$KUBECTL_LOG\"\nif [[ \"$*\" == *\" get secret \"* ]]; then exit 1; fi\nif [[ \"$*\" == *\" --dry-run=client \"* ]]; then printf '{\"apiVersion\":\"v1\",\"kind\":\"Secret\",\"metadata\":{},\"type\":\"Opaque\",\"data\":{}}\\n'; exit 0; fi\nif [[ \"$*\" == *\" create -f -\"* ]]; then cat >/dev/null; exit 0; fi\nexit 99\n")
	writeTrafficExecutable(t, operationctl, "#!/usr/bin/env bash\nset -euo pipefail\nprintf '%s\\n' \"$*\" >>\"$OPERATION_LOG\"\n")
	output, err := runProductionScriptCommand(t, "request-info-certificate-rotation.sh", []string{
		"REQUEST_ID=change-2026-info-cert-1", "INSTANCE=instance-a", "WORK_DIR=" + dir, "INFO_ENDPOINT=https://info.example:9090", "INFO_SERVER_NAME=info.example",
		"OLD_INFO_CACERT=" + paths["old-ca"], "OLD_INFO_CERT=" + paths["old-cert"], "NEW_INFO_CACERT=" + paths["new-ca"], "NEW_INFO_CERT=" + paths["new-cert"],
		"KUBEBRAIN_NAMESPACE=kubebrain-system", "POD_SELECTOR=app=kubebrain", "KUBEBRAIN_SERVICE=kubebrain-peer", "EXPECTED_REPLICAS=3",
		"PROMETHEUS_URL=https://prometheus.example", "PROMETHEUS_CA_FILE=" + paths["prometheus-ca"], "KUBE_CONTEXT=production", "KUBECTL=" + kubectl, "OPERATIONCTL=" + operationctl, "KUBECTL_LOG=" + kubectlLog, "OPERATION_LOG=" + operationLog,
	})
	require.NoError(t, err, string(output))
	require.Contains(t, string(output), "unapproved Pending InfoCertificateRotation")
	operation := string(mustRead(t, operationLog))
	require.Contains(t, operation, "--requested-by platform:info-certificate-rotation")
	require.Contains(t, operation, "--type InfoCertificateRotation")
	require.Contains(t, operation, "--max-attempts 5")
	require.NotContains(t, operation, "approve")
	require.NotContains(t, string(mustRead(t, kubectlLog)), " patch ")
}

func TestRequestInfoCertificateRotationRejectsCredentialOutsideWorkspaceBeforeKubernetes(t *testing.T) {
	dir := t.TempDir()
	inside := filepath.Join(dir, "credential")
	require.NoError(t, os.WriteFile(inside, []byte("credential"), 0o600))
	outside := filepath.Join(t.TempDir(), "cert")
	require.NoError(t, os.WriteFile(outside, []byte("cert"), 0o600))
	marker, command := filepath.Join(dir, "called"), filepath.Join(dir, "command")
	writeTrafficExecutable(t, command, "#!/usr/bin/env bash\nprintf called >\"$MARKER\"\n")
	output, err := runProductionScriptCommand(t, "request-info-certificate-rotation.sh", []string{"REQUEST_ID=change-2026-info-cert-2", "INSTANCE=instance-a", "WORK_DIR=" + dir, "INFO_ENDPOINT=https://info.example:9090", "INFO_SERVER_NAME=info.example", "OLD_INFO_CACERT=" + outside, "OLD_INFO_CERT=" + inside, "NEW_INFO_CACERT=" + inside, "NEW_INFO_CERT=" + inside, "KUBEBRAIN_NAMESPACE=kubebrain-system", "POD_SELECTOR=app=kubebrain", "KUBEBRAIN_SERVICE=kubebrain-peer", "EXPECTED_REPLICAS=3", "PROMETHEUS_URL=https://prometheus.example", "PROMETHEUS_CA_FILE=" + inside, "KUBE_CONTEXT=production", "KUBECTL=" + command, "OPERATIONCTL=" + command, "MARKER=" + marker})
	require.Error(t, err)
	require.Contains(t, string(output), "executor workspace")
	require.NoFileExists(t, marker)
}
