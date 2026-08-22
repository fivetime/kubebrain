package production_test

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestOperationParameterBrokerTokenReviewDrillRecoversWithoutPodRestart(t *testing.T) {
	dir := t.TempDir()
	logPath, statePath := filepath.Join(dir, "calls.log"), filepath.Join(dir, "binding-state")
	caFile, tokenFile := filepath.Join(dir, "ca.crt"), filepath.Join(dir, "token")
	require.NoError(t, os.WriteFile(caFile, []byte("ca"), 0o600))
	require.NoError(t, os.WriteFile(tokenFile, []byte("token"), 0o600))
	checker, kubectl, curl := writeParameterBrokerTokenReviewDrillFakes(t, dir)
	out, err := runProductionCommand(t, "bash", []string{"drill-operation-parameter-broker-tokenreview-recovery.sh"}, []string{
		"KUBE_CONTEXT=production", "KUBECTL=" + kubectl, "CURL=" + curl, "BROKER_CHECKER=" + checker,
		"BROKER_CA_FILE=" + caFile, "BROKER_TOKEN_FILE=" + tokenFile, "CALL_LOG=" + logPath,
		"BINDING_STATE=" + statePath, "CONFIRM_PARAMETER_BROKER_TOKENREVIEW_DRILL=yes", "DRILL_TIMEOUT_SECONDS=5",
	})
	require.NoError(t, err, string(out))
	require.Contains(t, string(out), "204 -> 503/NotReady -> 204/Ready recovery")
	require.Equal(t, "active", string(mustRead(t, statePath)))
	require.Equal(t, 2, strings.Count(string(mustRead(t, logPath)), "patch clusterrolebinding"))
}

func TestOperationParameterBrokerTokenReviewDrillRestoresBindingOnFailure(t *testing.T) {
	dir := t.TempDir()
	logPath, statePath := filepath.Join(dir, "calls.log"), filepath.Join(dir, "binding-state")
	caFile, tokenFile := filepath.Join(dir, "ca.crt"), filepath.Join(dir, "token")
	require.NoError(t, os.WriteFile(caFile, []byte("ca"), 0o600))
	require.NoError(t, os.WriteFile(tokenFile, []byte("token"), 0o600))
	checker, kubectl, curl := writeParameterBrokerTokenReviewDrillFakes(t, dir)
	out, err := runProductionCommand(t, "bash", []string{"drill-operation-parameter-broker-tokenreview-recovery.sh"}, []string{
		"KUBE_CONTEXT=production", "KUBECTL=" + kubectl, "CURL=" + curl, "BROKER_CHECKER=" + checker,
		"BROKER_CA_FILE=" + caFile, "BROKER_TOKEN_FILE=" + tokenFile, "CALL_LOG=" + logPath,
		"BINDING_STATE=" + statePath, "CONFIRM_PARAMETER_BROKER_TOKENREVIEW_DRILL=yes",
		"DRILL_TIMEOUT_SECONDS=5", "BAD_REVOKED_CODE=true",
	})
	require.Error(t, err)
	require.Contains(t, string(out), "want 503")
	require.Equal(t, "active", string(mustRead(t, statePath)))
	require.Equal(t, 2, strings.Count(string(mustRead(t, logPath)), "patch clusterrolebinding"))
}

func TestOperationParameterBrokerTokenReviewDrillRequiresExplicitConfirmation(t *testing.T) {
	out, err := runProductionCommand(t, "bash", []string{"drill-operation-parameter-broker-tokenreview-recovery.sh"}, []string{
		"KUBE_CONTEXT=production", "KUBECTL=/bin/false",
	})
	require.Error(t, err)
	require.Contains(t, string(out), "CONFIRM_PARAMETER_BROKER_TOKENREVIEW_DRILL=yes")
}

func writeParameterBrokerTokenReviewDrillFakes(t *testing.T, dir string) (string, string, string) {
	t.Helper()
	checker := filepath.Join(dir, "checker")
	writeTrafficExecutable(t, checker, "#!/usr/bin/env bash\n[[ \"$1\" == --check-enabled ]]\nprintf active >\"$BINDING_STATE\"\n")
	kubectl := filepath.Join(dir, "kubectl")
	writeTrafficExecutable(t, kubectl, `#!/usr/bin/env bash
set -euo pipefail
printf 'kubectl %s\n' "$*" >>"$CALL_LOG"
if [[ "${1:-}" == --context ]]; then shift 2; fi
state="$(cat "$BINDING_STATE")"
if [[ "$1" == get && "$2" == clusterrolebinding ]]; then
  if [[ "$state" == active ]]; then subjects='[{"kind":"ServiceAccount","name":"kubebrain-operation-parameter-broker","namespace":"kubebrain-operations"}]'; rv=1; else subjects='[]'; rv=2; fi
  printf '{"metadata":{"uid":"binding-uid","resourceVersion":"%s"},"roleRef":{"apiGroup":"rbac.authorization.k8s.io","kind":"ClusterRole","name":"kubebrain-operation-parameter-broker-token-review"},"subjects":%s}\n' "$rv" "$subjects"
  exit 0
fi
if [[ "$1" == patch ]]; then
  if [[ "$*" == *'"value":[]'* ]]; then printf revoked >"$BINDING_STATE"; else printf active >"$BINDING_STATE"; fi
  exit 0
fi
if [[ "$1" == auth ]]; then
  if [[ "$state" == active ]]; then echo yes; else echo no; exit 1; fi
fi
if [[ "$1" == get && "$2" == pods ]]; then
  if [[ "$state" == active ]]; then ready=True; else ready=False; fi
  printf '{"items":[{"metadata":{"name":"broker-0","uid":"uid-0"},"status":{"conditions":[{"type":"Ready","status":"%s"}]}},{"metadata":{"name":"broker-1","uid":"uid-1"},"status":{"conditions":[{"type":"Ready","status":"%s"}]}}]}\n' "$ready" "$ready"
  exit 0
fi
if [[ "$1" == get && "$2" == pod ]]; then
  case "$3" in broker-0) uid=uid-0 ;; broker-1) uid=uid-1 ;; *) exit 99 ;; esac
  printf '{"metadata":{"uid":"%s"}}\n' "$uid"
  exit 0
fi
if [[ "$1" == port-forward ]]; then
  printf 'Forwarding from 127.0.0.1:45680 -> 8443\n'
  while true; do sleep 1; done
fi
exit 99
`)
	curl := filepath.Join(dir, "curl")
	writeTrafficExecutable(t, curl, `#!/usr/bin/env bash
set -euo pipefail
if [[ "$(cat "$BINDING_STATE")" == active ]]; then printf 204
elif [[ "${BAD_REVOKED_CODE:-false}" == true ]]; then printf 204
else printf 503
fi
`)
	return checker, kubectl, curl
}
