package production_test

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRequestColdPhysicalRestoreWritesOnlyTargetQueueEvidence(t *testing.T) {
	dir := t.TempDir()
	receipt := filepath.Join(dir, "snapshot.json")
	var source map[string]any
	require.NoError(t, json.Unmarshal(coldRestoreSnapshotReceipt(t), &source))
	source["operation_id"] = "cold-snapshot-0123456789abcdefabcd"
	sourceBytes, err := json.Marshal(source)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(receipt, sourceBytes, 0o600))
	renderer := filepath.Join(dir, "renderer")
	require.NoError(t, os.WriteFile(renderer, []byte("#!/usr/bin/env bash\nset -euo pipefail\nout=\"\"; while [[ $# -gt 0 ]]; do [[ $1 == --output ]] && { out=$2; shift 2; continue; }; shift; done\nprintf '{\"apiVersion\":\"v1\",\"kind\":\"List\",\"items\":[]}\\n' >\"$out\"\n"), 0o755))
	kubectlLog := filepath.Join(dir, "kubectl.log")
	kubectl := filepath.Join(dir, "kubectl")
	require.NoError(t, os.WriteFile(kubectl, []byte(`#!/usr/bin/env bash
set -euo pipefail
printf '%s\n' "$*" >>"$KUBECTL_LOG"
if [[ "$*" == *"get namespace kube-system"* ]]; then printf target-kube-uid; exit 0; fi
if [[ "$*" == *"get namespace tidb-cluster"* ]]; then printf target-namespace-uid; exit 0; fi
if [[ "$*" == *"get secret cold-restore-"* ]]; then exit 1; fi
if [[ "$*" == *"create secret generic"* ]]; then printf '{"apiVersion":"v1","kind":"Secret","data":{}}\n'; exit 0; fi
if [[ "$*" == *"create -f -"* ]]; then jq -e '.immutable==true' >/dev/null; exit 0; fi
exit 99
`), 0o755))
	operationLog := filepath.Join(dir, "operation.log")
	operationctl := filepath.Join(dir, "operationctl")
	require.NoError(t, os.WriteFile(operationctl, []byte("#!/usr/bin/env bash\nprintf '%s\\n' \"$*\" >>\"$OPERATION_LOG\"\n"), 0o755))
	output, err := runProductionScriptCommand(t, "request-cold-physical-restore.sh", []string{"REQUEST_ID=change-2026-002", "RECEIPT_FILE=" + receipt,
		"KUBE_CONTEXT=isolated", "TARGET_SNAPSHOT_CLASS=retained", "TARGET_STORAGE_CLASS=fast", "KUBECTL=" + kubectl,
		"KUBECTL_LOG=" + kubectlLog, "OPERATIONCTL=" + operationctl, "OPERATION_LOG=" + operationLog, "COLD_RESTORE_RENDER=" + renderer})
	require.NoError(t, err, string(output))
	require.Contains(t, string(output), "unapproved Pending ColdPhysicalRestore")
	operation := string(mustRead(t, operationLog))
	require.Contains(t, operation, "--requested-by platform:cold-physical-restore")
	require.Contains(t, operation, "--type ColdPhysicalRestore")
	require.Contains(t, operation, "--max-attempts 1")
	require.NotContains(t, operation, "approve")
	calls := string(mustRead(t, kubectlLog))
	require.NotContains(t, calls, "patch")
	require.NotContains(t, calls, "delete")
	require.NotContains(t, calls, "tidbcluster")
}
