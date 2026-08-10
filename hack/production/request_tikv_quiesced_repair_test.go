package production_test

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRequestTiKVQuiescedRepairCreatesOnlyPendingFrozenTargetOperation(t *testing.T) {
	tempDir := t.TempDir()
	kubectl := filepath.Join(tempDir, "kubectl")
	kubectlLog := filepath.Join(tempDir, "kubectl.log")
	operationctl := filepath.Join(tempDir, "operationctl")
	operationLog := filepath.Join(tempDir, "operation.log")
	require.NoError(t, os.WriteFile(kubectl, []byte(`#!/usr/bin/env bash
set -euo pipefail
printf '%s\n' "$*" >>"$KUBECTL_LOG"
args="$*"
if [[ "$args" == *"get statefulset kubebrain -o json"* ]]; then
  printf '{"metadata":{"uid":"kb-uid"},"spec":{"replicas":0},"status":{"readyReplicas":0}}'
elif [[ "$args" == *"get tidbcluster kb -o json"* ]]; then
  printf '{"metadata":{"uid":"tc-uid"},"spec":{"pd":{"replicas":3},"tikv":{"replicas":3}},"status":{"clusterID":7671,"conditions":[{"type":"Ready","status":"True"}]}}'
elif [[ "$args" == *"pending-peer"* ]]; then
  printf '{"count":1,"regions":[{"pending_peers":[{"store_id":1005}],"down_peers":[]}]}'
elif [[ "$args" == *"down-peer"* ]]; then
  printf '{"count":1,"regions":[{"pending_peers":[],"down_peers":[{"peer":{"store_id":1005}}]}]}'
elif [[ "$args" == *"get secret tikv-quiesced-repair-55b125a68aa87753e96b-parameters"* ]]; then
  exit 1
elif [[ "$args" == *"create secret generic tikv-quiesced-repair-55b125a68aa87753e96b-parameters"* ]]; then
  parameters_file=""
  for arg in "$@"; do
    [[ "$arg" == --from-file=parameters.json=* ]] && parameters_file="${arg#--from-file=parameters.json=}"
  done
  jq -e 'keys == ["endpoint","expected_abnormal_store_ids","expected_cluster_id","expected_kubebrain_statefulset_uid","expected_tidb_cluster_uid","kubebrain_namespace","kubebrain_statefulset","pod_ready_timeout_seconds","probe_timeout_seconds","repair_cooldown_seconds","request_id","tidb_cluster","tidb_namespace"] and .expected_abnormal_store_ids == [1005] and .request_id == "change-2026-002"' "$parameters_file" >/dev/null
  printf '{"apiVersion":"v1","kind":"Secret","metadata":{"name":"probe"},"type":"Opaque","data":{}}\n'
elif [[ "$args" == *"create -f -"* ]]; then
  payload="$(cat)"
  jq -e '.immutable == true and .type == "Opaque"' <<<"$payload" >/dev/null
else
  echo "unexpected kubectl call: $args" >&2
  exit 99
fi
`), 0o755))
	require.NoError(t, os.WriteFile(operationctl, []byte("#!/usr/bin/env bash\nprintf '%s\\n' \"$*\" >>\"$OPERATION_LOG\"\n"), 0o755))

	output, err := runProductionScriptCommand(t, "request-tikv-quiesced-repair.sh", []string{
		"REQUEST_ID=change-2026-002", "KUBE_CONTEXT=test-context",
		"ENDPOINT=http://kubebrain-client.kubebrain-system.svc:3379",
		"KUBECTL=" + kubectl, "KUBECTL_LOG=" + kubectlLog,
		"OPERATIONCTL=" + operationctl, "OPERATION_LOG=" + operationLog,
	})
	require.NoError(t, err, string(output))
	require.Contains(t, string(output), "approved stores=1005")
	operationData, err := os.ReadFile(operationLog)
	require.NoError(t, err)
	operationCall := string(operationData)
	require.Contains(t, operationCall, "--action submit")
	require.Contains(t, operationCall, "--name tikv-quiesced-repair-55b125a68aa87753e96b")
	require.Contains(t, operationCall, "--requested-by platform:tikv-quiesced-repair")
	require.Contains(t, operationCall, "--type TiKVTransactionRepair")
	require.Contains(t, operationCall, "--max-attempts 1")
	require.NotContains(t, operationCall, "approve")
	kubectlData, err := os.ReadFile(kubectlLog)
	require.NoError(t, err)
	for _, forbidden := range []string{"delete", " scale ", " exec ", " patch "} {
		require.NotContains(t, string(kubectlData), forbidden)
	}
}

func TestRequestTiKVQuiescedRepairRejectsHealthyRegionsBeforeSubmission(t *testing.T) {
	tempDir := t.TempDir()
	logPath := filepath.Join(tempDir, "kubectl.log")
	kubectl := filepath.Join(tempDir, "kubectl")
	require.NoError(t, os.WriteFile(kubectl, []byte(`#!/usr/bin/env bash
set -euo pipefail
printf '%s\n' "$*" >>"$KUBECTL_LOG"
if [[ "$*" == *"get statefulset"* ]]; then
  printf '{"metadata":{"uid":"kb-uid"},"spec":{"replicas":0},"status":{}}'
elif [[ "$*" == *"get tidbcluster"* ]]; then
  printf '{"metadata":{"uid":"tc-uid"},"spec":{"pd":{"replicas":3},"tikv":{"replicas":3}},"status":{"clusterID":7671,"conditions":[{"type":"Ready","status":"True"}]}}'
elif [[ "$*" == *"/regions/check/"* ]]; then
  printf '{"count":0,"regions":[]}'
else
  exit 98
fi
`), 0o755))
	operationctl := filepath.Join(tempDir, "operationctl")
	require.NoError(t, os.WriteFile(operationctl, []byte("#!/usr/bin/env bash\nexit 99\n"), 0o755))
	output, err := runProductionScriptCommand(t, "request-tikv-quiesced-repair.sh", []string{
		"REQUEST_ID=change-2026-002", "KUBE_CONTEXT=test-context",
		"ENDPOINT=http://kubebrain-client.kubebrain-system.svc:3379",
		"KUBECTL=" + kubectl, "KUBECTL_LOG=" + logPath, "OPERATIONCTL=" + operationctl,
	})
	require.Error(t, err)
	require.Contains(t, string(output), "at least one valid pending/down store target")
	logData, readErr := os.ReadFile(logPath)
	require.NoError(t, readErr)
	require.False(t, strings.Contains(string(logData), "secret"))
}
