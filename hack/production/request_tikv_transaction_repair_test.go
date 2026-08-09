package production_test

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRequestTiKVTransactionRepairCreatesOnlyPendingUnapprovedOperation(t *testing.T) {
	tempDir := t.TempDir()
	alertPath := filepath.Join(tempDir, "alert.json")
	require.NoError(t, os.WriteFile(alertPath, []byte(`{"alerts":[{"status":"firing","labels":{"alertname":"KubeBrainTransactionPathUnavailableWithHealthyTiKVControlPlane","namespace":"kubebrain-system","statefulset":"kubebrain"},"startsAt":"2026-08-09T05:00:00Z","fingerprint":"abcdef0123456789abcdef0123456789"}]}`), 0o600))
	kubectl := filepath.Join(tempDir, "kubectl")
	kubectlLog := filepath.Join(tempDir, "kubectl.log")
	require.NoError(t, os.WriteFile(kubectl, []byte(`#!/usr/bin/env bash
set -euo pipefail
printf '%s\n' "$*" >>"$KUBECTL_LOG"
if [[ "$*" == *"get statefulset kubebrain"* ]]; then
  printf 'kb-uid'
elif [[ "$*" == *"get tidbcluster kb"* ]]; then
  printf 'tc-uid\t7671'
elif [[ "$*" == *"get secret tikv-repair-"*"-parameters"* ]]; then
  exit 1
elif [[ "$*" == *"create secret generic"* ]]; then
  printf '{"apiVersion":"v1","kind":"Secret","metadata":{"name":"probe"},"data":{}}\n'
elif [[ "$*" == *"create -f -"* ]]; then
  payload="$(cat)"
  jq -e '.immutable == true' <<<"$payload" >/dev/null
else
  echo "unexpected kubectl call: $*" >&2
  exit 99
fi
`), 0o755))
	operationctl := filepath.Join(tempDir, "operationctl")
	operationLog := filepath.Join(tempDir, "operation.log")
	require.NoError(t, os.WriteFile(operationctl, []byte("#!/usr/bin/env bash\nprintf '%s\\n' \"$*\" >>\"$OPERATION_LOG\"\n"), 0o755))

	output, err := runProductionScriptCommand(t, "request-tikv-transaction-repair.sh", []string{
		"ALERT_INPUT=" + alertPath,
		"KUBE_CONTEXT=test-context",
		"ENDPOINT=http://kubebrain-client.kubebrain-system.svc:3379",
		"NOW_UNIX=1786252000",
		"KUBECTL=" + kubectl,
		"KUBECTL_LOG=" + kubectlLog,
		"OPERATIONCTL=" + operationctl,
		"OPERATION_LOG=" + operationLog,
	})
	if err != nil {
		kubectlCalls, _ := os.ReadFile(kubectlLog)
		operationCalls, _ := os.ReadFile(operationLog)
		t.Logf("output=%q kubectl=%q operationctl=%q", output, kubectlCalls, operationCalls)
	}
	require.NoError(t, err, string(output))
	require.Contains(t, string(output), "unapproved Pending TiKVTransactionRepair")
	operationData, err := os.ReadFile(operationLog)
	require.NoError(t, err)
	operationCall := string(operationData)
	require.Contains(t, operationCall, "--action submit")
	require.Contains(t, operationCall, "--type TiKVTransactionRepair")
	require.Contains(t, operationCall, "--max-attempts 1")
	require.Contains(t, operationCall, "--name tikv-repair-1bb5a469de669cd3422d")
	require.NotContains(t, operationCall, "approve")
	require.NotContains(t, operationCall, "approved-by")
	kubectlData, err := os.ReadFile(kubectlLog)
	require.NoError(t, err)
	require.NotContains(t, string(kubectlData), "delete")
	require.NotContains(t, string(kubectlData), "scale")

	require.NoError(t, os.WriteFile(alertPath, []byte(`{"alerts":[{"status":"firing","labels":{"alertname":"KubeBrainTransactionPathUnavailableWithHealthyTiKVControlPlane","namespace":"kubebrain-system","statefulset":"kubebrain"},"startsAt":"2026-08-09T05:10:00Z","fingerprint":"abcdef0123456789abcdef0123456789"}]}`), 0o600))
	secondOutput, secondErr := runProductionScriptCommand(t, "request-tikv-transaction-repair.sh", []string{
		"ALERT_INPUT=" + alertPath,
		"KUBE_CONTEXT=test-context",
		"ENDPOINT=http://kubebrain-client.kubebrain-system.svc:3379",
		"NOW_UNIX=1786253000",
		"KUBECTL=" + kubectl,
		"KUBECTL_LOG=" + kubectlLog,
		"OPERATIONCTL=" + operationctl,
		"OPERATION_LOG=" + operationLog,
	})
	require.NoError(t, secondErr, string(secondOutput))
	operationData, err = os.ReadFile(operationLog)
	require.NoError(t, err)
	operationCall = string(operationData)
	require.Contains(t, operationCall, "--name tikv-repair-1bb5a469de669cd3422d")
	require.Contains(t, operationCall, "--name tikv-repair-20592d90baca78bfdbac")
	require.Equal(t, 2, strings.Count(operationCall, "--action submit"))
}

func TestRequestTiKVTransactionRepairRejectsResolvedAlertBeforeKubernetes(t *testing.T) {
	tempDir := t.TempDir()
	alertPath := filepath.Join(tempDir, "alert.json")
	require.NoError(t, os.WriteFile(alertPath, []byte(`{"alerts":[{"status":"resolved","labels":{"alertname":"KubeBrainTransactionPathUnavailableWithHealthyTiKVControlPlane","namespace":"kubebrain-system","statefulset":"kubebrain"},"startsAt":"2026-08-09T05:00:00Z","fingerprint":"abcdef0123456789"}]}`), 0o600))
	logPath := filepath.Join(tempDir, "kubectl.log")
	kubectl := filepath.Join(tempDir, "kubectl")
	require.NoError(t, os.WriteFile(kubectl, []byte("#!/usr/bin/env bash\nprintf called >>\"$KUBECTL_LOG\"\n"), 0o755))
	operationctl := filepath.Join(tempDir, "operationctl")
	require.NoError(t, os.WriteFile(operationctl, []byte("#!/usr/bin/env bash\nexit 99\n"), 0o755))
	output, err := runProductionScriptCommand(t, "request-tikv-transaction-repair.sh", []string{
		"ALERT_INPUT=" + alertPath, "KUBE_CONTEXT=test-context",
		"ENDPOINT=http://kubebrain-client.kubebrain-system.svc:3379",
		"NOW_UNIX=1786252000", "KUBECTL=" + kubectl, "KUBECTL_LOG=" + logPath,
		"OPERATIONCTL=" + operationctl,
	})
	require.Error(t, err)
	require.Contains(t, string(output), "exactly one matching firing")
	require.NoFileExists(t, logPath)
}
