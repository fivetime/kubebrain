package production_test

import (
	"os"
	"path/filepath"
	"regexp"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRequestTiKVTransactionRecoveryCreatesOnlyPendingUnapprovedOperation(t *testing.T) {
	dir := t.TempDir()
	kubectlLog := filepath.Join(dir, "kubectl.log")
	kubectl := filepath.Join(dir, "kubectl")
	require.NoError(t, os.WriteFile(kubectl, []byte(`#!/usr/bin/env bash
set -euo pipefail
printf '%s\n' "$*" >>"$KUBECTL_LOG"
if [[ "$*" == *"get statefulset kubebrain -o json"* ]]; then
  printf '{"metadata":{"uid":"kb-uid"},"spec":{"replicas":0},"status":{}}\n'
elif [[ "$*" == *"get tidbcluster kb -o json"* ]]; then
  printf '{"metadata":{"uid":"tc-uid"},"spec":{"pd":{"replicas":3},"tikv":{"replicas":3}},"status":{"clusterID":%s,"conditions":[{"type":"Ready","status":"True"}]}}\n' "${FAKE_CLUSTER_ID:-7671}"
elif [[ "$*" == *"get secret tikv-recovery-"* ]]; then
  exit 1
elif [[ "$*" == *"create secret generic"* ]]; then
  parameters_file=""
  for arg in "$@"; do
    [[ "$arg" == --from-file=parameters.json=* ]] && parameters_file="${arg#--from-file=parameters.json=}"
  done
  jq -e 'keys == ["endpoint","expected_cluster_id","expected_kubebrain_statefulset_uid","expected_tidb_cluster_uid","kubebrain_namespace","kubebrain_statefulset","pod_ready_timeout_seconds","probe_timeout_seconds","request_id","tidb_cluster","tidb_namespace"] and .request_id == "change-2026-001"' "$parameters_file" >/dev/null
  printf '{"apiVersion":"v1","kind":"Secret","metadata":{"name":"probe"},"data":{}}\n'
elif [[ "$*" == *"create -f -"* ]]; then
  payload="$(cat)"
  jq -e '.immutable == true' <<<"$payload" >/dev/null
else
  echo "unexpected kubectl call: $*" >&2
  exit 99
fi
`), 0o755))
	operationLog := filepath.Join(dir, "operation.log")
	operationctl := filepath.Join(dir, "operationctl")
	require.NoError(t, os.WriteFile(operationctl, []byte("#!/usr/bin/env bash\nprintf '%s\\n' \"$*\" >>\"$OPERATION_LOG\"\n"), 0o755))

	output, err := runProductionScriptCommand(t, "request-tikv-transaction-recovery.sh", []string{
		"REQUEST_ID=change-2026-001", "KUBE_CONTEXT=test-context",
		"ENDPOINT=http://kubebrain-client.kubebrain-system.svc:3379",
		"KUBECTL=" + kubectl, "KUBECTL_LOG=" + kubectlLog,
		"OPERATIONCTL=" + operationctl, "OPERATION_LOG=" + operationLog,
	})
	require.NoError(t, err, string(output))
	require.Contains(t, string(output), "unapproved Pending TiKVTransactionRecovery")
	operationData, err := os.ReadFile(operationLog)
	require.NoError(t, err)
	operationCall := string(operationData)
	require.Contains(t, operationCall, "--requested-by platform:tikv-repair-recovery")
	require.Contains(t, operationCall, "--type TiKVTransactionRecovery")
	require.Contains(t, operationCall, "--max-attempts 2")
	require.Contains(t, operationCall, "--name tikv-recovery-dbea00c4a1e7fae49690")
	require.Regexp(t, regexp.MustCompile(`--name tikv-recovery-[a-f0-9]{20}`), operationCall)
	require.NotContains(t, operationCall, "approve")
	kubectlData, err := os.ReadFile(kubectlLog)
	require.NoError(t, err)
	require.NotContains(t, string(kubectlData), "scale")
	require.NotContains(t, string(kubectlData), "exec")
	require.NotContains(t, string(kubectlData), "delete")

	require.NoError(t, os.WriteFile(operationLog, nil, 0o600))
	maxOutput, maxErr := runProductionScriptCommand(t, "request-tikv-transaction-recovery.sh", []string{
		"REQUEST_ID=change-2026-001", "KUBE_CONTEXT=test-context",
		"ENDPOINT=http://kubebrain-client.kubebrain-system.svc:3379",
		"KUBECTL=" + kubectl, "KUBECTL_LOG=" + kubectlLog,
		"OPERATIONCTL=" + operationctl, "OPERATION_LOG=" + operationLog,
		"FAKE_CLUSTER_ID=18446744073709551615",
	})
	require.NoError(t, maxErr, string(maxOutput))
	require.Contains(t, string(mustRead(t, operationLog)), "--name tikv-recovery-6c91f9b2718f23c8950e")

	require.NoError(t, os.WriteFile(operationLog, nil, 0o600))
	overflowOutput, overflowErr := runProductionScriptCommand(t, "request-tikv-transaction-recovery.sh", []string{
		"REQUEST_ID=change-2026-001", "KUBE_CONTEXT=test-context",
		"ENDPOINT=http://kubebrain-client.kubebrain-system.svc:3379",
		"KUBECTL=" + kubectl, "KUBECTL_LOG=" + kubectlLog,
		"OPERATIONCTL=" + operationctl, "OPERATION_LOG=" + operationLog,
		"FAKE_CLUSTER_ID=18446744073709551616",
	})
	require.Error(t, overflowErr)
	require.Contains(t, string(overflowOutput), "positive uint64")
	require.Empty(t, mustRead(t, operationLog))
}

func TestRequestTiKVTransactionRecoveryRejectsNonZeroDataPlaneBeforeWrites(t *testing.T) {
	dir := t.TempDir()
	logPath := filepath.Join(dir, "kubectl.log")
	kubectl := filepath.Join(dir, "kubectl")
	require.NoError(t, os.WriteFile(kubectl, []byte(`#!/usr/bin/env bash
set -euo pipefail
printf '%s\n' "$*" >>"$KUBECTL_LOG"
printf '{"metadata":{"uid":"kb-uid"},"spec":{"replicas":3},"status":{"readyReplicas":3}}\n'
`), 0o755))
	operationctl := filepath.Join(dir, "operationctl")
	require.NoError(t, os.WriteFile(operationctl, []byte("#!/usr/bin/env bash\nexit 99\n"), 0o755))
	output, err := runProductionScriptCommand(t, "request-tikv-transaction-recovery.sh", []string{
		"REQUEST_ID=change-2026-001", "KUBE_CONTEXT=test-context",
		"ENDPOINT=http://kubebrain:3379", "KUBECTL=" + kubectl,
		"KUBECTL_LOG=" + logPath, "OPERATIONCTL=" + operationctl,
	})
	require.Error(t, err)
	require.Contains(t, string(output), "exactly zero replicas")
	data, readErr := os.ReadFile(logPath)
	require.NoError(t, readErr)
	require.NotContains(t, string(data), "secret")
}

func TestRequestTiKVTransactionRecoveryRejectsNumericOverflowBeforeKubernetes(t *testing.T) {
	for _, variable := range []string{"PROBE_TIMEOUT_SECONDS", "POD_READY_TIMEOUT_SECONDS"} {
		dir := t.TempDir()
		logPath := filepath.Join(dir, "kubectl.log")
		kubectl := filepath.Join(dir, "kubectl")
		require.NoError(t, os.WriteFile(kubectl, []byte("#!/usr/bin/env bash\nprintf called >\"$KUBECTL_LOG\"\n"), 0o755))
		output, err := runProductionScriptCommand(t, "request-tikv-transaction-recovery.sh", []string{
			"REQUEST_ID=change-2026-001", "KUBE_CONTEXT=test-context", "ENDPOINT=http://kubebrain:3379",
			"KUBECTL=" + kubectl, "KUBECTL_LOG=" + logPath, "OPERATIONCTL=/bin/true",
			variable + "=9223372036854775808",
		})
		require.Error(t, err)
		require.Contains(t, string(output), variable+" must be a positive int64")
		require.NoFileExists(t, logPath)
	}
}
