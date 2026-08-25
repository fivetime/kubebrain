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
	require.Contains(t, string(out), "verified 17 operation requester guardrail pairs and the self-contained TiKV repair alert receiver")
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
	require.Len(t, lines, 35)
	require.Contains(t, lines[0], "--context production api-resources")
	for i, line := range lines[1:18] {
		require.Contains(t, line, "requester-admission.yaml", "apply call %d", i)
		require.Contains(t, line, "--server-side")
	}
	for i, line := range lines[18:] {
		require.Contains(t, line, "requester-rbac.yaml", "apply call %d", i+17)
		require.Contains(t, line, "--server-side")
	}
	require.Contains(t, string(out), "applied 17 fail-closed operation requester guardrail pairs")
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

func TestOperationRequesterGuardrailsCheckCompiledPoliciesAndIdentityMatrix(t *testing.T) {
	dir := t.TempDir()
	kubectl := writeGuardrailCheckKubectl(t, dir)
	out, err := runProductionCommandWithTimeout(t, "bash", []string{"apply-operation-requester-guardrails.sh", "--check"}, []string{
		"KUBE_CONTEXT=production", "KUBECTL=" + kubectl,
	}, productionScriptCommandTimeout)
	require.NoError(t, err, string(out))
	require.Contains(t, string(out), "checked 36 compiled Deny policies and 18 requester RBAC/admission identities")
}

func TestOperationRequesterGuardrailsRejectPolicyWarningsBeforeIdentityProbes(t *testing.T) {
	dir := t.TempDir()
	kubectl := writeGuardrailCheckKubectl(t, dir)
	out, err := runProductionCommand(t, "bash", []string{"apply-operation-requester-guardrails.sh", "--check"}, []string{
		"KUBE_CONTEXT=production", "KUBECTL=" + kubectl, "POLICY_WARNING=true",
	})
	require.Error(t, err)
	require.Contains(t, string(out), "type checking is incomplete or has warnings")
}

func writeGuardrailCheckKubectl(t *testing.T, dir string) string {
	t.Helper()
	kubectl := filepath.Join(dir, "kubectl")
	writeTrafficExecutable(t, kubectl, `#!/usr/bin/env bash
set -euo pipefail
if [[ "${1:-}" == --context ]]; then shift 2; fi
case "${1:-}" in
  api-resources)
    printf '%s\n' validatingadmissionpolicies.admissionregistration.k8s.io validatingadmissionpolicybindings.admissionregistration.k8s.io
    ;;
  create)
    if [[ " $* " == *" --dry-run=client "* ]]; then
      file=""
      while [[ "$#" -gt 0 ]]; do
        if [[ "$1" == -f ]]; then file="$2"; break; fi
        shift
      done
      stem="${file##*/kubebrain-}"; stem="${stem%-requester-admission.yaml}"
      printf 'validatingadmissionpolicy.admissionregistration.k8s.io/%s-operation\n' "$stem"
      printf 'validatingadmissionpolicybinding.admissionregistration.k8s.io/%s-operation\n' "$stem"
      printf 'validatingadmissionpolicy.admissionregistration.k8s.io/%s-parameters\n' "$stem"
      printf 'validatingadmissionpolicybinding.admissionregistration.k8s.io/%s-parameters\n' "$stem"
    else
      payload="$(cat)"
      [[ "$payload" != *unexpected* && "$payload" != *invalid-requester* && "$payload" != *conformance-wrong-identity* ]] || exit 1
    fi
    ;;
  get)
    kind="$2"; name="$3"
    if [[ "$kind" == validatingadmissionpolicy ]]; then
      if [[ "${POLICY_WARNING:-false}" == true ]]; then
        printf '{"metadata":{"name":"%s","generation":1},"status":{"observedGeneration":1,"typeChecking":{"expressionWarnings":[{"fieldRef":"spec.validations[0].expression","warning":"bad"}]}}}\n' "$name"
      else
        printf '{"metadata":{"name":"%s","generation":1},"status":{"observedGeneration":1,"typeChecking":{}}}\n' "$name"
      fi
    else
      printf '{"metadata":{"name":"%s"},"spec":{"policyName":"%s","validationActions":["Deny"]}}\n' "$name" "$name"
    fi
    ;;
  auth)
    verb="$3"
    case "$verb" in
      create|get) echo yes ;;
      *) echo no; exit 1 ;;
    esac
    ;;
  *) exit 99 ;;
esac
`)
	return kubectl
}
