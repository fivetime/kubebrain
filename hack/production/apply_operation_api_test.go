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

func TestOperationAPIInstallerEnablesAndSmokesThreeReplicas(t *testing.T) {
	dir := t.TempDir()
	logPath, scaleMarker := filepath.Join(dir, "calls.log"), filepath.Join(dir, "scaled")
	guardrails := filepath.Join(dir, "guardrails")
	writeTrafficExecutable(t, guardrails, "#!/usr/bin/env bash\nprintf 'guardrails %s\\n' \"$*\" >>\"$CALL_LOG\"\n")
	kubectl := writeOperationAPIKubectl(t, dir)
	curl := writeOperationAPICurl(t, dir)
	caFile, tokenFile := filepath.Join(dir, "ca.crt"), filepath.Join(dir, "token")
	require.NoError(t, os.WriteFile(caFile, []byte("test-ca"), 0o600))
	require.NoError(t, os.WriteFile(tokenFile, []byte("header.payload.signature"), 0o600))
	out, err := runProductionCommand(t, "bash", []string{"apply-operation-api.sh", "--enable"}, []string{
		"KUBE_CONTEXT=production", "KUBECTL=" + kubectl, "CURL=" + curl,
		"REQUESTER_GUARDRAILS=" + guardrails, "CALL_LOG=" + logPath, "SCALE_MARKER=" + scaleMarker,
		"OPERATION_API_ENDPOINT=https://operation-api.example.test", "OPERATION_API_CA_FILE=" + caFile,
		"OPERATION_API_TOKEN_FILE=" + tokenFile,
	})
	require.NoError(t, err, string(out))
	log := string(mustRead(t, logPath))
	require.Contains(t, log, "scale deployment/kubebrain-operation-api --replicas=3")
	require.Contains(t, log, "rollout status deployment/kubebrain-operation-api")
	require.Contains(t, log, "/readyz")
	require.Contains(t, log, "/v1/operations/api-auth-conformance-missing")
	require.NotContains(t, log, "header.payload.signature")
	require.NotContains(t, log, "--replicas=0")
	require.Contains(t, string(out), "enabled three ready Operation API replicas")
}

func TestOperationAPIInstallerRollsBackWhenHTTPSSmokeFails(t *testing.T) {
	dir := t.TempDir()
	logPath, scaleMarker := filepath.Join(dir, "calls.log"), filepath.Join(dir, "scaled")
	guardrails := filepath.Join(dir, "guardrails")
	writeTrafficExecutable(t, guardrails, "#!/usr/bin/env bash\nexit 0\n")
	kubectl := writeOperationAPIKubectl(t, dir)
	curl := writeOperationAPICurl(t, dir)
	caFile, tokenFile := filepath.Join(dir, "ca.crt"), filepath.Join(dir, "token")
	require.NoError(t, os.WriteFile(caFile, []byte("test-ca"), 0o600))
	require.NoError(t, os.WriteFile(tokenFile, []byte("header.payload.signature"), 0o600))
	out, err := runProductionCommand(t, "bash", []string{"apply-operation-api.sh", "--enable"}, []string{
		"KUBE_CONTEXT=production", "KUBECTL=" + kubectl, "CURL=" + curl, "CURL_FAIL=true",
		"REQUESTER_GUARDRAILS=" + guardrails, "CALL_LOG=" + logPath, "SCALE_MARKER=" + scaleMarker,
		"OPERATION_API_ENDPOINT=https://operation-api.example.test", "OPERATION_API_CA_FILE=" + caFile,
		"OPERATION_API_TOKEN_FILE=" + tokenFile,
	})
	require.Error(t, err)
	require.Contains(t, string(out), "readiness did not return 204")
	require.Contains(t, string(out), "requested rollback to zero replicas")
	require.Contains(t, string(mustRead(t, logPath)), "scale deployment/kubebrain-operation-api --replicas=0")
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
        if [[ -n "${SCALE_MARKER:-}" && -f "$SCALE_MARKER" ]]; then
          printf '{"metadata":{"generation":2},"spec":{"replicas":3},"status":{"observedGeneration":2,"updatedReplicas":3,"readyReplicas":3,"availableReplicas":3}}\n'
        else
          printf '{"metadata":{"generation":1},"spec":{"replicas":0},"status":{"observedGeneration":1}}\n'
        fi
        ;;
      secret)
        if [[ "$3" == kubebrain-operation-api-oidc ]]; then
          printf '{"type":"Opaque","data":{"issuer":"aHR0cHM6Ly9pZHAuZXhhbXBsZS50ZXN0","audience":"a3ViZWJyYWluLW9wZXJhdGlvbi1hcGk="}}\n'
        else
          printf '{"type":"kubernetes.io/tls","data":{"tls.crt":"Y2VydA==","tls.key":"a2V5"}}\n'
        fi
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
  scale)
    if [[ "$*" == *"--replicas=3"* ]]; then
      : >"$SCALE_MARKER"
    else
      rm -f -- "$SCALE_MARKER"
    fi
    ;;
  rollout) ;;
  *) exit 99 ;;
esac
`)
	return kubectl
}

func writeOperationAPICurl(t *testing.T, dir string) string {
	t.Helper()
	curl := filepath.Join(dir, "curl")
	writeTrafficExecutable(t, curl, `#!/usr/bin/env bash
set -euo pipefail
printf 'curl %s\n' "$*" >>"$CALL_LOG"
if [[ "$*" == *"/readyz" ]]; then
  if [[ "${CURL_FAIL:-false}" == true ]]; then printf 500; else printf 204; fi
else
  printf 404
fi
`)
	return curl
}
