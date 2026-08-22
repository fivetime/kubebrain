package production_test

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestOperationControlPlaneFoundationInventory(t *testing.T) {
	out, err := runProductionCommand(t, "bash", []string{"apply-operation-control-plane-foundation.sh", "--verify"}, nil)
	require.NoError(t, err, string(out))
	require.Contains(t, string(out), "verified Operation control-plane foundation inventory")
}

func TestOperationControlPlaneFoundationAppliesAdmissionBeforeRBAC(t *testing.T) {
	dir := t.TempDir()
	logPath := filepath.Join(dir, "kubectl.log")
	kubectl := writeOperationFoundationKubectl(t, dir)
	out, err := runProductionCommand(t, "bash", []string{"apply-operation-control-plane-foundation.sh", "--apply"}, []string{
		"KUBE_CONTEXT=production", "KUBECTL=" + kubectl, "KUBECTL_LOG=" + logPath,
	})
	require.NoError(t, err, string(out))
	lines := strings.Split(strings.TrimSpace(string(mustRead(t, logPath))), "\n")
	require.Len(t, lines, 17)
	require.Contains(t, lines[0], "api-resources")
	require.Contains(t, lines[1], "kubebrain-operation-crd.yaml")
	require.Contains(t, lines[2], "wait --for=condition=Established")
	require.Contains(t, lines[3], "apply --server-side")
	require.Contains(t, lines[3], "-f -")
	require.Contains(t, lines[4], "kubebrain-operation-worker-admission.yaml")
	require.Contains(t, lines[5], "kubebrain-operation-audit-admission.yaml")
	for _, line := range lines[6:10] {
		require.Contains(t, line, "validatingadmissionpolicy")
	}
	for _, line := range lines[10:] {
		require.NotContains(t, line, "-admission.yaml")
		require.NotContains(t, line, "kubebrain-operation-submitter-rbac.yaml")
		require.NotContains(t, line, "kubebrain-operation-api.yaml")
		require.Contains(t, line, "--server-side")
	}
	require.Contains(t, string(out), "applied fail-closed Operation control-plane foundation with disabled workloads")
}

func TestOperationControlPlaneFoundationRequiresContextBeforeKubectl(t *testing.T) {
	dir := t.TempDir()
	marker, kubectl := filepath.Join(dir, "called"), filepath.Join(dir, "kubectl")
	writeTrafficExecutable(t, kubectl, "#!/usr/bin/env bash\nprintf x >\"$MARKER\"\n")
	out, err := runProductionCommand(t, "bash", []string{"apply-operation-control-plane-foundation.sh", "--apply"}, []string{
		"KUBECTL=" + kubectl, "MARKER=" + marker,
	})
	require.Error(t, err)
	require.Contains(t, string(out), "KUBE_CONTEXT is required")
	_, statErr := os.Stat(marker)
	require.ErrorIs(t, statErr, os.ErrNotExist)
}

func TestOperationControlPlaneFoundationStopsBeforeRBACWhenAdmissionApplyFails(t *testing.T) {
	dir := t.TempDir()
	logPath := filepath.Join(dir, "kubectl.log")
	kubectl := writeOperationFoundationKubectl(t, dir)
	out, err := runProductionCommand(t, "bash", []string{"apply-operation-control-plane-foundation.sh", "--apply"}, []string{
		"KUBE_CONTEXT=production", "KUBECTL=" + kubectl, "KUBECTL_LOG=" + logPath, "FAIL_ADMISSION=true",
	})
	require.Error(t, err, string(out))
	log := string(mustRead(t, logPath))
	require.Contains(t, log, "kubebrain-operation-worker-admission.yaml")
	require.NotContains(t, log, "kubebrain-operation-worker-rbac.yaml")
}

func writeOperationFoundationKubectl(t *testing.T, dir string) string {
	t.Helper()
	kubectl := filepath.Join(dir, "kubectl")
	writeTrafficExecutable(t, kubectl, `#!/usr/bin/env bash
set -euo pipefail
printf '%s\n' "$*" >>"$KUBECTL_LOG"
if [[ "${1:-}" == --context ]]; then shift 2; fi
case "${1:-}" in
  api-resources)
    printf '%s\n' validatingadmissionpolicies.admissionregistration.k8s.io validatingadmissionpolicybindings.admissionregistration.k8s.io
    ;;
  apply)
    if [[ "${FAIL_ADMISSION:-false}" == true && "$*" == *"-admission.yaml"* ]]; then exit 1; fi
    if [[ "$*" == *" -f -"* ]]; then cat >/dev/null; fi
    ;;
  wait) ;;
  get)
    kind="$2"; name="$3"
    if [[ "$kind" == validatingadmissionpolicy ]]; then
      printf '{"metadata":{"name":"%s","generation":1},"status":{"observedGeneration":1,"typeChecking":{}}}\n' "$name"
    else
      printf '{"metadata":{"name":"%s"},"spec":{"policyName":"%s","validationActions":["Deny"]}}\n' "$name" "$name"
    fi
    ;;
  *) exit 99 ;;
esac
`)
	return kubectl
}
