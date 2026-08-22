package production_test

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestOperationArchiverInstallerInventory(t *testing.T) {
	out, err := runProductionCommand(t, "bash", []string{"apply-operation-archiver.sh", "--verify"}, nil)
	require.NoError(t, err, string(out))
	require.Contains(t, string(out), "verified fail-closed Operation archiver inventory")
}

func TestOperationArchiverInstallerEnablesAndProbesBothPods(t *testing.T) {
	dir := t.TempDir()
	logPath, marker := filepath.Join(dir, "calls.log"), filepath.Join(dir, "scaled")
	kubectl := writeOperationArchiverKubectl(t, dir)
	out, err := runProductionCommand(t, "bash", []string{"apply-operation-archiver.sh", "--enable"}, []string{
		"KUBE_CONTEXT=production", "KUBECTL=" + kubectl, "CALL_LOG=" + logPath, "SCALE_MARKER=" + marker,
	})
	require.NoError(t, err, string(out))
	require.Contains(t, string(out), "enabled two ready Operation archiver replicas")
	log := string(mustRead(t, logPath))
	require.Contains(t, log, "scale deployment/kubebrain-operation-archiver --replicas=2")
	require.Equal(t, 2, strings.Count(log, "exec -n kubebrain-operations broker-"))
	require.NotContains(t, log, "--replicas=0")
}

func TestOperationArchiverInstallerRollsBackFailedPodProbe(t *testing.T) {
	dir := t.TempDir()
	logPath, marker := filepath.Join(dir, "calls.log"), filepath.Join(dir, "scaled")
	kubectl := writeOperationArchiverKubectl(t, dir)
	out, err := runProductionCommand(t, "bash", []string{"apply-operation-archiver.sh", "--enable"}, []string{
		"KUBE_CONTEXT=production", "KUBECTL=" + kubectl, "CALL_LOG=" + logPath,
		"SCALE_MARKER=" + marker, "PROBE_FAIL_POD=broker-1",
	})
	require.Error(t, err)
	require.Contains(t, string(out), "Pod Object Lock probe failed")
	require.Contains(t, string(out), "requested rollback to zero replicas")
	require.Contains(t, string(mustRead(t, logPath)), "--replicas=0")
}

func TestOperationArchiverInstallerChecksEnabledWithoutMutation(t *testing.T) {
	dir := t.TempDir()
	logPath, marker := filepath.Join(dir, "calls.log"), filepath.Join(dir, "scaled")
	require.NoError(t, os.WriteFile(marker, []byte("2"), 0o600))
	kubectl := writeOperationArchiverKubectl(t, dir)
	out, err := runProductionCommand(t, "bash", []string{"apply-operation-archiver.sh", "--check-enabled"}, []string{
		"KUBE_CONTEXT=production", "KUBECTL=" + kubectl, "CALL_LOG=" + logPath, "SCALE_MARKER=" + marker,
	})
	require.NoError(t, err, string(out))
	require.Contains(t, string(out), "checked two ready Operation archiver replicas")
	log := string(mustRead(t, logPath))
	require.NotContains(t, log, " scale ")
	require.NotContains(t, log, " rollout ")
}

func TestOperationArchiverInstallerRejectsMissingManagedNamespaceRBACBeforeScale(t *testing.T) {
	dir := t.TempDir()
	logPath, marker := filepath.Join(dir, "calls.log"), filepath.Join(dir, "scaled")
	kubectl := writeOperationArchiverKubectl(t, dir)
	out, err := runProductionCommand(t, "bash", []string{"apply-operation-archiver.sh", "--enable"}, []string{
		"KUBE_CONTEXT=production", "KUBECTL=" + kubectl, "CALL_LOG=" + logPath, "SCALE_MARKER=" + marker,
		"INCLUDE_TENANT=true", "DENY_TENANT_RBAC=true",
	})
	require.Error(t, err)
	require.Contains(t, string(out), "authorization must allow get kubebrainoperations")
	require.NotContains(t, string(mustRead(t, logPath)), " scale ")
}

func writeOperationArchiverKubectl(t *testing.T, dir string) string {
	t.Helper()
	kubectl := filepath.Join(dir, "kubectl")
	writeTrafficExecutable(t, kubectl, `#!/usr/bin/env bash
set -euo pipefail
printf 'kubectl %s\n' "$*" >>"$CALL_LOG"
if [[ "${1:-}" == --context ]]; then shift 2; fi
if [[ "$1" == auth ]]; then
  verb="$3"; resource="$4"
	if [[ "${DENY_TENANT_RBAC:-false}" == true && "$*" == *"-n tenant-a-operations"* ]]; then echo no; exit 1; fi
  if [[ "$resource" == "configmap/kubebrain-backup-scheduler-inventory" && "$verb" == get ]]; then echo yes; exit 0; fi
  if [[ "$resource" == kubebrainoperations.dbaas.kubebrain.io && ( "$verb" == get || "$verb" == list || "$verb" == update ) ]]; then echo yes; exit 0; fi
  echo no; exit 1
fi
if [[ "$1" == get && "$2" == validatingadmissionpolicy ]]; then
  printf '{"metadata":{"generation":2},"status":{"observedGeneration":2,"typeChecking":{"expressionWarnings":[]}}}\n'; exit 0
fi
if [[ "$1" == get && "$2" == validatingadmissionpolicybinding ]]; then
  printf '{"spec":{"policyName":"kubebrain-operation-audit","validationActions":["Deny"]}}\n'; exit 0
fi
if [[ "$1" == get && "$2" == deployment ]]; then
  if [[ -f "$SCALE_MARKER" ]]; then
    printf '{"metadata":{"generation":2},"spec":{"replicas":2},"status":{"observedGeneration":2,"updatedReplicas":2,"readyReplicas":2,"availableReplicas":2}}\n'
  else
    printf '{"metadata":{"generation":1},"spec":{"replicas":0},"status":{"observedGeneration":1}}\n'
  fi
  exit 0
fi
if [[ "$1" == get && "$2" == secret ]]; then
  printf '{"data":{"endpoint":"aHR0cHM6Ly9zMy5leGFtcGxlLnRlc3Q=","region":"dXMtZWFzdC0x","access-key-id":"YWNjZXNz","secret-access-key":"c2VjcmV0","force-path-style":"ZmFsc2U=","object-store-id":"c3RvcmUtYQ==","bucket":"YXVkaXQtYnVja2V0"}}\n'; exit 0
fi
if [[ "$1" == get && "$2" == configmap ]]; then
	if [[ "${INCLUDE_TENANT:-false}" == true ]]; then
		printf '%s\n' '{"data":{"namespaces.json":"[\"kubebrain-operations\",\"tenant-a-operations\"]"}}'
	else
		printf '%s\n' '{"data":{"namespaces.json":"[\"kubebrain-operations\"]"}}'
	fi
	exit 0
fi
if [[ "$1" == get && "$2" == pods ]]; then
  printf '{"items":[{"metadata":{"name":"broker-0","uid":"uid-0"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},{"metadata":{"name":"broker-1","uid":"uid-1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}]}\n'; exit 0
fi
if [[ "$1" == get && "$2" == pod ]]; then
  case "$3" in broker-0) uid=uid-0 ;; broker-1) uid=uid-1 ;; *) exit 99 ;; esac
  printf '{"metadata":{"uid":"%s"}}\n' "$uid"; exit 0
fi
if [[ "$1" == exec ]]; then
  pod="$4"
  [[ "${PROBE_FAIL_POD:-}" != "$pod" ]] || exit 42
  printf '{"format":"kubebrain.object-store-bucket-probe.v1","object_store_id":"store-a","bucket":"audit-bucket","versioning_enabled":true,"object_lock_enabled":true,"checked_at_unix":2000000000}\n'
  exit 0
fi
if [[ "$1" == scale ]]; then
  if [[ "$*" == *--replicas=2* ]]; then : >"$SCALE_MARKER"; else rm -f -- "$SCALE_MARKER"; fi
  exit 0
fi
[[ "$1" == rollout ]] && exit 0
exit 99
`)
	return kubectl
}
