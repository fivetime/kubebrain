package production_test

import (
	"fmt"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestTransactionRequestersBoundStructuredControlPlaneResponses(t *testing.T) {
	for _, tc := range []struct{ name, script string }{
		{"recovery", "request-tikv-transaction-recovery.sh"},
		{"quiesced repair", "request-tikv-quiesced-repair.sh"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			controlLog := filepath.Join(dir, "control.log")
			kubectl := filepath.Join(dir, "kubectl")
			writeExecutable(t, kubectl, fmt.Sprintf(`#!/usr/bin/env bash
printf 'kubectl %%s\n' "$*" >>%q
if [[ "$*" == *"get statefulset kubebrain"* ]]; then
  payload='{"metadata":{"uid":"kb-uid"},"spec":{"replicas":0},"status":{"readyReplicas":0}}'
  printf '%%s' "$payload"; head -c "$((CONTROL_BYTES-${#payload}))" /dev/zero | tr '\0' ' '; exit 0
fi
if [[ "$*" == *"get tidbcluster kb"* ]]; then printf '%%s' '{"metadata":{"uid":"tc-uid"},"spec":{"pd":{"replicas":3},"tikv":{"replicas":3}},"status":{"clusterID":7671,"conditions":[{"type":"Ready","status":"True"}]}}'; exit 0; fi
if [[ "$*" == *"pending-peer"* ]]; then printf '%%s' '{"count":1,"regions":[{"pending_peers":[{"store_id":1}]}]}'; exit 0; fi
if [[ "$*" == *"down-peer"* ]]; then printf '%%s' '{"count":0,"regions":[]}'; exit 0; fi
if [[ "$*" == *"get secret "* ]]; then exit 1; fi
if [[ "$*" == *"create secret generic"* ]]; then printf '%%s\n' '{"kind":"Secret"}'; else cat >/dev/null; fi
`, controlLog))
			operationctl := filepath.Join(dir, "operationctl")
			writeExecutable(t, operationctl, fmt.Sprintf("#!/usr/bin/env bash\nprintf 'operationctl %%s\\n' \"$*\" >>%q\n", controlLog))
			base := []string{"REQUEST_ID=change-control-budget", "KUBE_CONTEXT=in-cluster", "ENDPOINT=http://kubebrain:3379", "KUBECTL=" + kubectl, "OPERATIONCTL=" + operationctl}
			output, err := runProductionScriptCommand(t, tc.script, append(base, "CONTROL_BYTES=1048577"))
			require.Error(t, err)
			require.Contains(t, string(output), "control-plane response exceeds 1048576 bytes")
			calls := string(mustReadProductionFile(t, controlLog))
			require.NotContains(t, calls, "get tidbcluster")
			require.NotContains(t, calls, "operationctl")
			boundaryOutput, boundaryErr := runProductionScriptCommand(t, tc.script, append(base, "CONTROL_BYTES=1048576"))
			require.NoError(t, boundaryErr, string(boundaryOutput))
			require.Contains(t, string(mustReadProductionFile(t, controlLog)), "operationctl")
		})
	}
}
