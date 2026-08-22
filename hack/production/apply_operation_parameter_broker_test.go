package production_test

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestOperationParameterBrokerInstallerInventory(t *testing.T) {
	out, err := runProductionCommand(t, "bash", []string{"apply-operation-parameter-broker.sh", "--verify"}, nil)
	require.NoError(t, err, string(out))
	require.Contains(t, string(out), "verified fail-closed Operation parameter broker inventory")
}

func TestOperationParameterBrokerInstallerEnablesTwoReplicas(t *testing.T) {
	dir := t.TempDir()
	logPath, marker := filepath.Join(dir, "calls.log"), filepath.Join(dir, "scaled")
	kubectl := writeParameterBrokerKubectl(t, dir)
	curl := writeParameterBrokerCurl(t, dir)
	caFile, tokenFile := filepath.Join(dir, "ca.crt"), filepath.Join(dir, "token")
	require.NoError(t, os.WriteFile(caFile, []byte("ca"), 0o600))
	require.NoError(t, os.WriteFile(tokenFile, []byte("projected.token"), 0o600))
	out, err := runProductionCommand(t, "bash", []string{"apply-operation-parameter-broker.sh", "--enable"}, []string{
		"KUBE_CONTEXT=production", "KUBECTL=" + kubectl, "CURL=" + curl, "CALL_LOG=" + logPath,
		"SCALE_MARKER=" + marker, "BROKER_CA_FILE=" + caFile, "BROKER_TOKEN_FILE=" + tokenFile,
	})
	require.NoError(t, err, string(out))
	log := string(mustRead(t, logPath))
	require.Contains(t, log, "scale deployment/kubebrain-operation-parameter-broker --replicas=2")
	require.Contains(t, log, "port-forward --address 127.0.0.1")
	require.Contains(t, log, "/readyz")
	require.Contains(t, log, "/v1/parameters?")
	require.NotContains(t, log, "projected.token")
	require.NotContains(t, log, "--replicas=0")
	require.Contains(t, string(out), "enabled two ready parameter broker replicas")
}

func TestOperationParameterBrokerInstallerRollsBackFailedSmoke(t *testing.T) {
	dir := t.TempDir()
	logPath, marker := filepath.Join(dir, "calls.log"), filepath.Join(dir, "scaled")
	kubectl := writeParameterBrokerKubectl(t, dir)
	curl := writeParameterBrokerCurl(t, dir)
	caFile, tokenFile := filepath.Join(dir, "ca.crt"), filepath.Join(dir, "token")
	require.NoError(t, os.WriteFile(caFile, []byte("ca"), 0o600))
	require.NoError(t, os.WriteFile(tokenFile, []byte("projected.token"), 0o600))
	out, err := runProductionCommand(t, "bash", []string{"apply-operation-parameter-broker.sh", "--enable"}, []string{
		"KUBE_CONTEXT=production", "KUBECTL=" + kubectl, "CURL=" + curl, "CURL_FAIL=true", "CALL_LOG=" + logPath,
		"SCALE_MARKER=" + marker, "BROKER_CA_FILE=" + caFile, "BROKER_TOKEN_FILE=" + tokenFile,
	})
	require.Error(t, err)
	require.Contains(t, string(out), "readiness did not return 204")
	require.Contains(t, string(out), "requested rollback to zero replicas")
	require.Contains(t, string(mustRead(t, logPath)), "scale deployment/kubebrain-operation-parameter-broker --replicas=0")
}

func TestOperationParameterBrokerInstallerChecksEnabledWithoutMutation(t *testing.T) {
	dir := t.TempDir()
	logPath, marker := filepath.Join(dir, "calls.log"), filepath.Join(dir, "scaled")
	require.NoError(t, os.WriteFile(marker, []byte("2"), 0o600))
	kubectl := writeParameterBrokerKubectl(t, dir)
	curl := writeParameterBrokerCurl(t, dir)
	caFile, tokenFile := filepath.Join(dir, "ca.crt"), filepath.Join(dir, "token")
	require.NoError(t, os.WriteFile(caFile, []byte("ca"), 0o600))
	require.NoError(t, os.WriteFile(tokenFile, []byte("projected.token"), 0o600))
	out, err := runProductionCommand(t, "bash", []string{"apply-operation-parameter-broker.sh", "--check-enabled"}, []string{
		"KUBE_CONTEXT=production", "KUBECTL=" + kubectl, "CURL=" + curl, "CALL_LOG=" + logPath,
		"SCALE_MARKER=" + marker, "BROKER_CA_FILE=" + caFile, "BROKER_TOKEN_FILE=" + tokenFile,
	})
	require.NoError(t, err, string(out))
	log := string(mustRead(t, logPath))
	require.NotContains(t, log, " scale ")
	require.NotContains(t, log, " rollout ")
	require.Contains(t, string(out), "checked two ready parameter broker replicas")
}

func writeParameterBrokerKubectl(t *testing.T, dir string) string {
	t.Helper()
	kubectl := filepath.Join(dir, "kubectl")
	writeTrafficExecutable(t, kubectl, `#!/usr/bin/env bash
set -euo pipefail
printf 'kubectl %s\n' "$*" >>"$CALL_LOG"
if [[ "${1:-}" == --context ]]; then shift 2; fi
case "$1" in
  auth)
    verb="$3"; resource="$4"; identity=""
    for arg in "$@"; do case "$arg" in --as=*) identity="${arg#--as=}" ;; esac; done
    if [[ "$identity" == *kubebrain-operation-parameter-broker && ( ( "$resource" == tokenreviews.authentication.k8s.io && "$verb" == create ) || ( "$resource" != tokenreviews.authentication.k8s.io && "$verb" == get ) ) ]]; then echo yes; else echo no; exit 1; fi
    ;;
  get)
    case "$2" in
      deployment)
        if [[ -f "$SCALE_MARKER" ]]; then
          printf '{"metadata":{"generation":2},"spec":{"replicas":2},"status":{"observedGeneration":2,"updatedReplicas":2,"readyReplicas":2,"availableReplicas":2}}\n'
        else
          printf '{"metadata":{"generation":1},"spec":{"replicas":0},"status":{"observedGeneration":1}}\n'
        fi
        ;;
      secret) printf '{"type":"kubernetes.io/tls","data":{"tls.crt":"Y2VydA==","tls.key":"a2V5"}}\n' ;;
      configmap) printf '{"data":{"ca.crt":"ca"}}\n' ;;
      *) exit 99 ;;
    esac
    ;;
  scale)
    if [[ "$*" == *--replicas=2* ]]; then : >"$SCALE_MARKER"; else rm -f -- "$SCALE_MARKER"; fi
    ;;
  rollout) ;;
  port-forward)
    printf 'Forwarding from 127.0.0.1:45679 -> 443\n'
    while true; do sleep 1; done
    ;;
  *) exit 99 ;;
esac
`)
	return kubectl
}

func writeParameterBrokerCurl(t *testing.T, dir string) string {
	t.Helper()
	curl := filepath.Join(dir, "curl")
	writeTrafficExecutable(t, curl, `#!/usr/bin/env bash
set -euo pipefail
printf 'curl %s\n' "$*" >>"$CALL_LOG"
if [[ "$*" == *readyz ]]; then
  if [[ "${CURL_FAIL:-false}" == true ]]; then printf 500; else printf 204; fi
else
  printf 403
fi
`)
	return curl
}
