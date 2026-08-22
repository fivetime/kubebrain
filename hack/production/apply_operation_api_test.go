package production_test

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestOperationAPIInstallerInventory(t *testing.T) {
	out, err := runProductionCommand(t, "bash", []string{"apply-operation-api.sh", "--verify"}, nil)
	require.NoError(t, err, string(out))
	require.Contains(t, string(out), "verified fail-closed Operation API installation inventory")
}

func TestOperationAPIInstallerChecksRequesterGuardrailsBeforeGrant(t *testing.T) {
	dir := t.TempDir()
	logPath := filepath.Join(dir, "calls.log")
	guardrails := filepath.Join(dir, "guardrails")
	writeTrafficExecutable(t, guardrails, `#!/usr/bin/env bash
set -euo pipefail
printf 'guardrails %s\n' "$*" >>"$CALL_LOG"
`)
	kubectl := writeOperationAPIKubectl(t, dir)
	out, err := runProductionCommand(t, "bash", []string{"apply-operation-api.sh", "--apply"}, []string{
		"KUBE_CONTEXT=production", "KUBECTL=" + kubectl, "REQUESTER_GUARDRAILS=" + guardrails, "CALL_LOG=" + logPath,
	})
	require.NoError(t, err, string(out))
	lines := strings.Split(strings.TrimSpace(string(mustRead(t, logPath))), "\n")
	require.Equal(t, "guardrails --check", lines[0])
	require.Contains(t, lines[1], "kubebrain-operation-api-admission.yaml")
	apiApply := -1
	for i, line := range lines {
		if strings.Contains(line, "kubebrain-operation-api.yaml") && !strings.Contains(line, "-admission.yaml") {
			apiApply = i
			break
		}
	}
	require.Greater(t, apiApply, 3)
	require.Contains(t, lines[2], "get validatingadmissionpolicy kubebrain-operation-api-submit")
	require.Contains(t, lines[3], "get validatingadmissionpolicybinding kubebrain-operation-api-submit")
	require.Contains(t, string(out), "checked fail-closed Operation API delegation with the workload disabled")
}

func TestOperationAPIInstallerStopsBeforeKubectlWhenRequesterCheckFails(t *testing.T) {
	dir := t.TempDir()
	marker := filepath.Join(dir, "kubectl-called")
	guardrails := filepath.Join(dir, "guardrails")
	kubectl := filepath.Join(dir, "kubectl")
	writeTrafficExecutable(t, guardrails, "#!/usr/bin/env bash\nexit 1\n")
	writeTrafficExecutable(t, kubectl, "#!/usr/bin/env bash\nprintf x >\"$MARKER\"\n")
	out, err := runProductionCommand(t, "bash", []string{"apply-operation-api.sh", "--apply"}, []string{
		"KUBE_CONTEXT=production", "KUBECTL=" + kubectl, "REQUESTER_GUARDRAILS=" + guardrails, "MARKER=" + marker,
	})
	require.Error(t, err, string(out))
	_, statErr := os.Stat(marker)
	require.ErrorIs(t, statErr, os.ErrNotExist)
}

func writeOperationAPIKubectl(t *testing.T, dir string) string {
	t.Helper()
	kubectl := filepath.Join(dir, "kubectl")
	writeTrafficExecutable(t, kubectl, `#!/usr/bin/env bash
set -euo pipefail
printf 'kubectl %s\n' "$*" >>"$CALL_LOG"
if [[ "${1:-}" == --context ]]; then shift 2; fi
case "${1:-}" in
  apply) ;;
  get)
    case "$2" in
      validatingadmissionpolicy)
        printf '{"metadata":{"generation":1},"status":{"observedGeneration":1,"typeChecking":{}}}\n'
        ;;
      validatingadmissionpolicybinding)
        printf '{"spec":{"policyName":"kubebrain-operation-api-submit","validationActions":["Deny"]}}\n'
        ;;
      deployment)
        printf '{"spec":{"replicas":0}}\n'
        ;;
      *) exit 99 ;;
    esac
    ;;
  auth)
    verb="$3"; resource="$4"
    if [[ "$resource" == kubebrainoperations.dbaas.kubebrain.io && ( "$verb" == create || "$verb" == get ) ]]; then
      echo yes
    else
      echo no
      exit 1
    fi
    ;;
  create)
    payload="$(cat)"
    [[ "$payload" == *parametersSecretRef* && "$payload" != *tenant-b-backup* && "$payload" != *api-conformance-bbbbbbbb* ]] || exit 1
    ;;
  *) exit 99 ;;
esac
`)
	return kubectl
}
