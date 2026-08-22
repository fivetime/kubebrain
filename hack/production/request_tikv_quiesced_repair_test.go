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
  printf '{"metadata":{"uid":"tc-uid"},"spec":{"pd":{"replicas":3},"tikv":{"replicas":3}},"status":{"clusterID":%s,"conditions":[{"type":"Ready","status":"True"}]}}' "${FAKE_CLUSTER_ID:-7671}"
elif [[ "$args" == *"pending-peer"* ]]; then
  printf '{"count":1,"regions":[{"pending_peers":[{"store_id":%s}],"down_peers":[]}]}' "${FAKE_STORE_ID:-1005}"
elif [[ "$args" == *"down-peer"* ]]; then
  printf '{"count":1,"regions":[{"pending_peers":[],"down_peers":[{"peer":{"store_id":%s}}]}]}' "${FAKE_STORE_ID:-1005}"
elif [[ "$args" == *"get secret ${EXPECTED_OPERATION_NAME:-tikv-quiesced-repair-55b125a68aa87753e96b}-parameters"* ]]; then
  exit 1
elif [[ "$args" == *"create secret generic ${EXPECTED_OPERATION_NAME:-tikv-quiesced-repair-55b125a68aa87753e96b}-parameters"* ]]; then
  parameters_file=""
  for arg in "$@"; do
    [[ "$arg" == --from-file=parameters.json=* ]] && parameters_file="${arg#--from-file=parameters.json=}"
  done
  jq -e --argjson store "${FAKE_STORE_ID:-1005}" 'keys == ["endpoint","expected_abnormal_store_ids","expected_cluster_id","expected_kubebrain_statefulset_uid","expected_tidb_cluster_uid","kubebrain_namespace","kubebrain_statefulset","pod_ready_timeout_seconds","probe_timeout_seconds","repair_cooldown_seconds","request_id","tidb_cluster","tidb_namespace"] and .expected_abnormal_store_ids == [$store] and .request_id == "change-2026-002"' "$parameters_file" >/dev/null
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
	require.Contains(t, operationCall, "--max-attempts 2")
	require.NotContains(t, operationCall, "approve")
	kubectlData, err := os.ReadFile(kubectlLog)
	require.NoError(t, err)
	for _, forbidden := range []string{"delete", " scale ", " exec ", " patch "} {
		require.NotContains(t, string(kubectlData), forbidden)
	}

	require.NoError(t, os.WriteFile(kubectlLog, nil, 0o600))
	require.NoError(t, os.WriteFile(operationLog, nil, 0o600))
	maxStoreID := "18446744073709551615"
	maxOperationName := "tikv-quiesced-repair-e9e8b3003c163ddc35ad"
	maxOutput, maxErr := runProductionScriptCommand(t, "request-tikv-quiesced-repair.sh", []string{
		"REQUEST_ID=change-2026-002", "KUBE_CONTEXT=test-context",
		"ENDPOINT=http://kubebrain-client.kubebrain-system.svc:3379",
		"KUBECTL=" + kubectl, "KUBECTL_LOG=" + kubectlLog,
		"OPERATIONCTL=" + operationctl, "OPERATION_LOG=" + operationLog,
		"FAKE_STORE_ID=" + maxStoreID, "EXPECTED_OPERATION_NAME=" + maxOperationName,
	})
	require.NoError(t, maxErr, string(maxOutput))
	require.Contains(t, string(maxOutput), "approved stores="+maxStoreID)
	require.Contains(t, string(mustRead(t, operationLog)), "--name "+maxOperationName)

	require.NoError(t, os.WriteFile(kubectlLog, nil, 0o600))
	require.NoError(t, os.WriteFile(operationLog, nil, 0o600))
	maxClusterID := "18446744073709551615"
	maxClusterOperation := "tikv-quiesced-repair-ddd81153220d45151bbf"
	maxClusterOutput, maxClusterErr := runProductionScriptCommand(t, "request-tikv-quiesced-repair.sh", []string{
		"REQUEST_ID=change-2026-002", "KUBE_CONTEXT=test-context",
		"ENDPOINT=http://kubebrain-client.kubebrain-system.svc:3379",
		"KUBECTL=" + kubectl, "KUBECTL_LOG=" + kubectlLog,
		"OPERATIONCTL=" + operationctl, "OPERATION_LOG=" + operationLog,
		"FAKE_CLUSTER_ID=" + maxClusterID, "EXPECTED_OPERATION_NAME=" + maxClusterOperation,
	})
	require.NoError(t, maxClusterErr, string(maxClusterOutput))
	require.Contains(t, string(mustRead(t, operationLog)), "--name "+maxClusterOperation)

	require.NoError(t, os.WriteFile(kubectlLog, nil, 0o600))
	require.NoError(t, os.WriteFile(operationLog, nil, 0o600))
	clusterOverflowOutput, clusterOverflowErr := runProductionScriptCommand(t, "request-tikv-quiesced-repair.sh", []string{
		"REQUEST_ID=change-2026-002", "KUBE_CONTEXT=test-context",
		"ENDPOINT=http://kubebrain-client.kubebrain-system.svc:3379",
		"KUBECTL=" + kubectl, "KUBECTL_LOG=" + kubectlLog,
		"OPERATIONCTL=" + operationctl, "OPERATION_LOG=" + operationLog,
		"FAKE_CLUSTER_ID=18446744073709551616",
	})
	require.Error(t, clusterOverflowErr)
	require.Contains(t, string(clusterOverflowOutput), "live cluster ID must be a positive uint64")
	require.NotContains(t, string(mustRead(t, kubectlLog)), " secret ")
	require.Empty(t, mustRead(t, operationLog))

	require.NoError(t, os.WriteFile(kubectlLog, nil, 0o600))
	require.NoError(t, os.WriteFile(operationLog, nil, 0o600))
	overflowOutput, overflowErr := runProductionScriptCommand(t, "request-tikv-quiesced-repair.sh", []string{
		"REQUEST_ID=change-2026-002", "KUBE_CONTEXT=test-context",
		"ENDPOINT=http://kubebrain-client.kubebrain-system.svc:3379",
		"KUBECTL=" + kubectl, "KUBECTL_LOG=" + kubectlLog,
		"OPERATIONCTL=" + operationctl, "OPERATION_LOG=" + operationLog,
		"FAKE_STORE_ID=18446744073709551616",
	})
	require.Error(t, overflowErr)
	require.Contains(t, string(overflowOutput), "store target is not a positive uint64")
	require.NotContains(t, string(mustRead(t, kubectlLog)), " secret ")
	require.Empty(t, mustRead(t, operationLog))
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

func TestRequestTiKVQuiescedRepairRejectsNumericOverflowBeforeKubernetes(t *testing.T) {
	for _, tc := range []struct {
		name, variable, value, want string
	}{
		{name: "probe timeout overflow", variable: "PROBE_TIMEOUT_SECONDS", value: "9223372036854775808", want: "PROBE_TIMEOUT_SECONDS must be a positive int64"},
		{name: "Pod timeout overflow", variable: "POD_READY_TIMEOUT_SECONDS", value: "9223372036854775808", want: "POD_READY_TIMEOUT_SECONDS must be a positive int64"},
		{name: "cooldown overflow", variable: "REPAIR_COOLDOWN_SECONDS", value: "9223372036854775808", want: "REPAIR_COOLDOWN_SECONDS must be a positive int64"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			output, err := runProductionScriptCommand(t, "request-tikv-quiesced-repair.sh", []string{
				"REQUEST_ID=change-2026-overflow", "KUBE_CONTEXT=production",
				"ENDPOINT=https://kubebrain.example:3379", tc.variable + "=" + tc.value,
			})
			require.Error(t, err)
			require.Contains(t, string(output), tc.want)
			require.NotContains(t, string(output), "kubectl")
		})
	}
}

func TestProductionRunbookUsesAuthorizedTiKVQuiescedRepairPath(t *testing.T) {
	docPath := filepath.Join("..", "..", "docs", "production_readiness_cn.md")
	data, err := os.ReadFile(docPath)
	require.NoError(t, err)
	doc := string(data)
	for _, required := range []string{
		"hack/production/request-tikv-quiesced-repair.sh",
		"deploy/production/kubebrain-tikv-quiesced-repair-requester-admission.yaml",
		"deploy/production/kubebrain-tikv-quiesced-repair-requester-rbac.yaml",
		"deploy/production/kubebrain-tikv-repair-alert-receiver.yaml",
		"deploy/production/kubebrain-operation-parameter-broker.yaml",
		"deploy/production/kubebrain-tikv-transaction-repair-rbac.yaml",
		"deploy/production/kubebrain-operation-executors.yaml",
		"repaired_store_ids",
		"TiKVTransactionRecovery",
	} {
		require.Contains(t, doc, required)
	}
	require.NotContains(t, doc, "该模式目前只是下一阶段审批 runner 的执行原语")
	requestIndex := strings.Index(doc, "hack/production/request-tikv-quiesced-repair.sh")
	recoveryIndex := strings.Index(doc, "hack/production/request-tikv-transaction-recovery.sh")
	require.Greater(t, recoveryIndex, requestIndex, "quiesced repair must precede the independently approved recovery step")
}
