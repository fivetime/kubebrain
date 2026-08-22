package production_test

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestOperationRequesterGuardrailInventoryIsComplete(t *testing.T) {
	out, err := runProductionCommand(t, "bash", []string{"apply-operation-requester-guardrails.sh", "--verify"}, nil)
	require.NoError(t, err, string(out))
	require.Contains(t, string(out), "verified 16 operation requester guardrail pairs and the self-contained TiKV repair alert receiver")
}

func TestOperationRequesterGuardrailsApplyAdmissionBeforeRBAC(t *testing.T) {
	dir := t.TempDir()
	logPath, kubectl := filepath.Join(dir, "kubectl.log"), filepath.Join(dir, "kubectl")
	writeTrafficExecutable(t, kubectl, `#!/usr/bin/env bash
set -euo pipefail
printf '%s\n' "$*" >>"$KUBECTL_LOG"
if [[ "$*" == *" api-resources "* ]]; then
  printf '%s\n' validatingadmissionpolicies.admissionregistration.k8s.io validatingadmissionpolicybindings.admissionregistration.k8s.io
fi
`)
	out, err := runProductionCommand(t, "bash", []string{"apply-operation-requester-guardrails.sh", "--apply"}, []string{
		"KUBE_CONTEXT=production", "KUBECTL=" + kubectl, "KUBECTL_LOG=" + logPath,
	})
	require.NoError(t, err, string(out))
	lines := strings.Split(strings.TrimSpace(string(mustRead(t, logPath))), "\n")
	require.Len(t, lines, 33)
	require.Contains(t, lines[0], "--context production api-resources")
	for i, line := range lines[1:17] {
		require.Contains(t, line, "requester-admission.yaml", "apply call %d", i)
		require.Contains(t, line, "--server-side")
	}
	for i, line := range lines[17:] {
		require.Contains(t, line, "requester-rbac.yaml", "apply call %d", i+16)
		require.Contains(t, line, "--server-side")
	}
	require.Contains(t, string(out), "applied 16 fail-closed operation requester guardrail pairs")
}

func TestOperationRequesterGuardrailsRequireExplicitContextBeforeKubectl(t *testing.T) {
	dir := t.TempDir()
	marker, kubectl := filepath.Join(dir, "called"), filepath.Join(dir, "kubectl")
	writeTrafficExecutable(t, kubectl, "#!/usr/bin/env bash\nprintf x >\"$MARKER\"\n")
	out, err := runProductionCommand(t, "bash", []string{"apply-operation-requester-guardrails.sh", "--apply"}, []string{
		"KUBECTL=" + kubectl, "MARKER=" + marker,
	})
	require.Error(t, err)
	require.Contains(t, string(out), "KUBE_CONTEXT is required")
	_, statErr := os.Stat(marker)
	require.ErrorIs(t, statErr, os.ErrNotExist)
}

func TestOperationRequesterGuardrailsRejectUnsupportedAPIServerBeforeApply(t *testing.T) {
	dir := t.TempDir()
	logPath, kubectl := filepath.Join(dir, "kubectl.log"), filepath.Join(dir, "kubectl")
	writeTrafficExecutable(t, kubectl, "#!/usr/bin/env bash\nprintf '%s\\n' \"$*\" >>\"$KUBECTL_LOG\"\n")
	out, err := runProductionCommand(t, "bash", []string{"apply-operation-requester-guardrails.sh", "--apply"}, []string{
		"KUBE_CONTEXT=production", "KUBECTL=" + kubectl, "KUBECTL_LOG=" + logPath,
	})
	require.Error(t, err)
	require.Contains(t, string(out), "does not expose ValidatingAdmissionPolicy")
	lines := strings.Split(strings.TrimSpace(string(mustRead(t, logPath))), "\n")
	require.Len(t, lines, 1)
	require.Contains(t, lines[0], "api-resources")
}
