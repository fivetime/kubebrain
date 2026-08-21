package production_test

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRequestColdPhysicalSnapshotCreatesOnlyPendingUnapprovedOperation(t *testing.T) {
	dir := t.TempDir()
	preflight := filepath.Join(dir, "preflight.json")
	require.NoError(t, os.WriteFile(preflight, []byte(`{"format":"kubebrain.cold-physical-snapshot-preflight.v2","kubebrain":{"namespace":"kubebrain-system","statefulset":"kubebrain","uid":"kb-uid"},"storage":{"namespace":"tidb-cluster","tidb_cluster":"kb","uid":"tc-uid","cluster_id":"7671"}}`), 0o600))
	witness := filepath.Join(dir, "witness.jsonl")
	require.NoError(t, os.WriteFile(witness, []byte("verified-witness\n"), 0o600))
	kubectlLog := filepath.Join(dir, "kubectl.log")
	kubectl := filepath.Join(dir, "kubectl")
	require.NoError(t, os.WriteFile(kubectl, []byte(`#!/usr/bin/env bash
set -euo pipefail
printf '%s\n' "$*" >>"$KUBECTL_LOG"
if [[ "$*" == *"get secret cold-snapshot-"* ]]; then exit 1; fi
if [[ "$*" == *"create secret generic"* ]]; then printf '{"apiVersion":"v1","kind":"Secret","data":{}}\n'; exit 0; fi
if [[ "$*" == *"create -f -"* ]]; then jq -e '.immutable == true' >/dev/null; exit 0; fi
exit 99
`), 0o755))
	operationLog := filepath.Join(dir, "operation.log")
	operationctl := filepath.Join(dir, "operationctl")
	require.NoError(t, os.WriteFile(operationctl, []byte("#!/usr/bin/env bash\nprintf '%s\\n' \"$*\" >>\"$OPERATION_LOG\"\n"), 0o755))
	output, err := runProductionScriptCommand(t, "request-cold-physical-snapshot.sh", []string{
		"REQUEST_ID=change-2026-001", "PREFLIGHT_FILE=" + preflight, "SEMANTIC_WITNESS_FILE=" + witness,
		"EXPECTED_WITNESS_PREFIX=/", "KUBE_CONTEXT=test", "KUBECTL=" + kubectl,
		"KUBECTL_LOG=" + kubectlLog, "OPERATIONCTL=" + operationctl, "OPERATION_LOG=" + operationLog,
	})
	require.NoError(t, err, string(output))
	require.Contains(t, string(output), "unapproved Pending ColdPhysicalSnapshot")
	operation, err := os.ReadFile(operationLog)
	require.NoError(t, err)
	require.Contains(t, string(operation), "--requested-by platform:cold-physical-snapshot")
	require.Contains(t, string(operation), "--type ColdPhysicalSnapshot")
	require.Contains(t, string(operation), "--max-attempts 1")
	require.NotContains(t, string(operation), "approve")
	kubectlCalls, err := os.ReadFile(kubectlLog)
	require.NoError(t, err)
	require.NotContains(t, string(kubectlCalls), "scale")
	require.NotContains(t, string(kubectlCalls), "patch")
	require.NotContains(t, string(kubectlCalls), "volumesnapshot")
}

func TestRequestColdPhysicalSnapshotRejectsNumericOverflowBeforeKubernetes(t *testing.T) {
	dir := t.TempDir()
	preflight := filepath.Join(dir, "preflight.json")
	witness := filepath.Join(dir, "witness.jsonl")
	require.NoError(t, os.WriteFile(preflight, []byte(`{"format":"kubebrain.cold-physical-snapshot-preflight.v2"}`), 0o600))
	require.NoError(t, os.WriteFile(witness, []byte("witness\n"), 0o600))
	kubectlLog := filepath.Join(dir, "kubectl.log")
	kubectl := filepath.Join(dir, "kubectl")
	require.NoError(t, os.WriteFile(kubectl, []byte("#!/usr/bin/env bash\nprintf called >>\"$KUBECTL_LOG\"\n"), 0o755))

	for _, tc := range []struct{ name, setting, want string }{
		{name: "witness age", setting: "WITNESS_MAX_AGE_SECONDS=9223372036854775808", want: "WITNESS_MAX_AGE_SECONDS must be a positive int64"},
		{name: "wait timeout", setting: "WAIT_TIMEOUT=9223372036854775808s", want: "WAIT_TIMEOUT must contain a positive int64"},
		{name: "fence settle", setting: "FENCE_SETTLE_SECONDS=9223372036854775808", want: "FENCE_SETTLE_SECONDS must be a non-negative int64"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			output, err := runProductionScriptCommand(t, "request-cold-physical-snapshot.sh", []string{
				"REQUEST_ID=change-overflow", "PREFLIGHT_FILE=" + preflight, "SEMANTIC_WITNESS_FILE=" + witness,
				"EXPECTED_WITNESS_PREFIX=/", "KUBE_CONTEXT=test", "KUBECTL=" + kubectl,
				"KUBECTL_LOG=" + kubectlLog, "OPERATIONCTL=/bin/true", tc.setting,
			})
			require.Error(t, err)
			require.Contains(t, string(output), tc.want)
			require.NoFileExists(t, kubectlLog)
		})
	}
}
