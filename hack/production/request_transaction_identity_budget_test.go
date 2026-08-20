package production_test

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestTransactionRepairRequesterBoundsIdentityResponses(t *testing.T) {
	dir := t.TempDir()
	alert := filepath.Join(dir, "alert.json")
	require.NoError(t, os.WriteFile(alert, []byte(`{"alerts":[{"status":"firing","labels":{"alertname":"KubeBrainTransactionPathUnavailableWithHealthyTiKVControlPlane","namespace":"kubebrain-system","statefulset":"kubebrain"},"startsAt":"2026-08-09T05:00:00Z","fingerprint":"abcdef0123456789"}]}`), 0o600))
	controlLog := filepath.Join(dir, "control.log")
	kubectl := filepath.Join(dir, "kubectl")
	writeExecutable(t, kubectl, fmt.Sprintf(`#!/usr/bin/env bash
printf 'kubectl %%s\n' "$*" >>%q
if [[ "$*" == *"get statefulset kubebrain"* ]]; then head -c "$IDENTITY_BYTES" /dev/zero | tr '\0' k; exit 0; fi
if [[ "$*" == *"get tidbcluster kb"* ]]; then printf 'tc-uid\t7671'; exit 0; fi
if [[ "$*" == *"get secret "* ]]; then exit 1; fi
if [[ "$*" == *"create secret generic"* ]]; then printf '%%s\n' '{"kind":"Secret"}'; else cat >/dev/null; fi
`, controlLog))
	operationctl := filepath.Join(dir, "operationctl")
	writeExecutable(t, operationctl, fmt.Sprintf("#!/usr/bin/env bash\nprintf 'operationctl %%s\\n' \"$*\" >>%q\n", controlLog))
	base := []string{"ALERT_INPUT=" + alert, "NOW_UNIX=1786252000", "KUBE_CONTEXT=in-cluster", "ENDPOINT=http://kubebrain:3379", "KUBECTL=" + kubectl, "OPERATIONCTL=" + operationctl}
	output, err := runProductionScriptCommand(t, "request-tikv-transaction-repair.sh", append(base, "IDENTITY_BYTES=4097"))
	require.Error(t, err)
	require.Contains(t, string(output), "identity response exceeds 4096 bytes")
	calls := string(mustReadProductionFile(t, controlLog))
	require.NotContains(t, calls, "get tidbcluster")
	require.NotContains(t, calls, "operationctl")
	boundaryOutput, boundaryErr := runProductionScriptCommand(t, "request-tikv-transaction-repair.sh", append(base, "IDENTITY_BYTES=4096"))
	require.NoError(t, boundaryErr, string(boundaryOutput))
	require.Contains(t, string(mustReadProductionFile(t, controlLog)), "operationctl")
}
