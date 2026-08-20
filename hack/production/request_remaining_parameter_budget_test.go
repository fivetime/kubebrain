package production_test

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRemainingRequestersRejectExecutorIncompatibleParameters(t *testing.T) {
	tests := []struct {
		name, script string
		extra        []string
	}{
		{"legacy remediation", "request-legacy-snapshot-remediation.sh", []string{"REQUEST_ID=change-large-endpoint", "INSTANCE=kubebrain", "DIAGNOSE_COMMAND={{DIAGNOSE}}"}},
		{"transaction recovery", "request-tikv-transaction-recovery.sh", []string{"REQUEST_ID=change-large-endpoint"}},
		{"transaction repair", "request-tikv-transaction-repair.sh", []string{"ALERT_INPUT={{ALERT}}", "NOW_UNIX=1786252000"}},
		{"quiesced repair", "request-tikv-quiesced-repair.sh", []string{"REQUEST_ID=change-large-endpoint"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dir := t.TempDir()
			controlLog := filepath.Join(dir, "control.log")
			kubectl := filepath.Join(dir, "kubectl")
			writeExecutable(t, kubectl, fmt.Sprintf(`#!/usr/bin/env bash
printf 'kubectl %%s\n' "$*" >>%q
if [[ "$*" == *"get statefulset kubebrain"* && "$*" == *"jsonpath"* ]]; then printf kb-uid; exit 0; fi
if [[ "$*" == *"get statefulset kubebrain"* && "$*" == *"-o json"* ]]; then printf '%%s' '{"metadata":{"uid":"kb-uid"},"spec":{"replicas":0},"status":{"readyReplicas":0}}'; exit 0; fi
if [[ "$*" == *"get statefulset kubebrain"* ]]; then printf kb-uid; exit 0; fi
if [[ "$*" == *"get tidbcluster kb"* && "$*" == *"jsonpath"* ]]; then printf 'tc-uid\t7671'; exit 0; fi
if [[ "$*" == *"get tidbcluster kb"* && "$*" == *"-o json"* ]]; then printf '%%s' '{"metadata":{"uid":"tc-uid"},"spec":{"pd":{"replicas":3},"tikv":{"replicas":3}},"status":{"clusterID":7671,"conditions":[{"type":"Ready","status":"True"}]}}'; exit 0; fi
if [[ "$*" == *"get tidbcluster kb"* ]]; then printf 'tc-uid\t7671'; exit 0; fi
if [[ "$*" == *"pending-peer"* ]]; then printf '%%s' '{"count":1,"regions":[{"pending_peers":[{"store_id":1}]}]}'; exit 0; fi
if [[ "$*" == *"down-peer"* ]]; then printf '%%s' '{"count":0,"regions":[]}'; exit 0; fi
if [[ "$*" == *"get secret "* ]]; then exit 1; fi
if [[ "$*" == *"create secret generic"* ]]; then printf '%%s\n' '{"kind":"Secret"}'; else cat >/dev/null; fi
`, controlLog))
			operationctl := filepath.Join(dir, "operationctl")
			writeExecutable(t, operationctl, fmt.Sprintf("#!/usr/bin/env bash\nprintf 'operationctl %%s\\n' \"$*\" >>%q\n", controlLog))
			diagnose := filepath.Join(dir, "diagnose")
			writeExecutable(t, diagnose, "#!/usr/bin/env bash\nprintf 'cluster_id=7671\\nrevision=10\\nminimum_compact_revision=5\\nsnapshot_status=legacy_lease_history_ambiguous\\n'\nexit 3\n")
			alert := filepath.Join(dir, "alert.json")
			require.NoError(t, os.WriteFile(alert, []byte(`{"alerts":[{"status":"firing","labels":{"alertname":"KubeBrainTransactionPathUnavailableWithHealthyTiKVControlPlane","namespace":"kubebrain-system","statefulset":"kubebrain"},"startsAt":"2026-08-09T05:00:00Z","fingerprint":"abcdef0123456789"}]}`), 0o600))
			extra := make([]string, 0, len(tt.extra))
			for _, value := range tt.extra {
				value = strings.ReplaceAll(value, "{{DIAGNOSE}}", diagnose)
				value = strings.ReplaceAll(value, "{{ALERT}}", alert)
				extra = append(extra, value)
			}
			endpoint := "http://example/" + strings.Repeat("x", 70000)
			env := append([]string{"ENDPOINT=" + endpoint, "KUBE_CONTEXT=in-cluster", "KUBECTL=" + kubectl, "OPERATIONCTL=" + operationctl}, extra...)
			output, err := runProductionScriptCommand(t, tt.script, env)
			require.Error(t, err)
			require.Contains(t, string(output), "operation parameters exceed 65536 bytes")
			if calls, readErr := os.ReadFile(controlLog); readErr == nil {
				require.NotContains(t, string(calls), "create secret")
				require.NotContains(t, string(calls), "operationctl")
			}
			jq := filepath.Join(dir, "jq")
			writeExecutable(t, jq, `#!/usr/bin/env bash
tmp="$(mktemp)"; trap 'rm -f -- "$tmp"' EXIT
/usr/bin/jq "$@" >"$tmp"; rc=$?
if [[ $rc == 0 && " ${*} " == *" -cnS "* ]]; then
  size="$(stat -Lc '%s' -- "$tmp")"; (( size <= 65536 )) || exit 97
  cat "$tmp"; head -c "$((65536-size))" /dev/zero | tr '\0' ' '
else
  cat "$tmp"
fi
exit "$rc"
`)
			boundaryEnv := append([]string{"ENDPOINT=http://example/ok", "KUBE_CONTEXT=in-cluster", "KUBECTL=" + kubectl, "OPERATIONCTL=" + operationctl, "JQ=" + jq}, extra...)
			boundaryOutput, boundaryErr := runProductionScriptCommand(t, tt.script, boundaryEnv)
			require.NoError(t, boundaryErr, string(boundaryOutput))
			require.Contains(t, string(mustReadProductionFile(t, controlLog)), "operationctl")
		})
	}
}
