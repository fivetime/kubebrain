package production_test

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestTransactionRequestersBoundExistingSecretResponses(t *testing.T) {
	for _, tc := range []struct {
		name, script string
		extra        func(string) []string
	}{
		{"recovery", "request-tikv-transaction-recovery.sh", func(string) []string { return []string{"REQUEST_ID=change-secret-budget"} }},
		{"repair", "request-tikv-transaction-repair.sh", func(alert string) []string { return []string{"ALERT_INPUT=" + alert, "NOW_UNIX=1786252000"} }},
		{"quiesced repair", "request-tikv-quiesced-repair.sh", func(string) []string { return []string{"REQUEST_ID=change-secret-budget"} }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			alert := filepath.Join(dir, "alert.json")
			require.NoError(t, os.WriteFile(alert, []byte(`{"alerts":[{"status":"firing","labels":{"alertname":"KubeBrainTransactionPathUnavailableWithHealthyTiKVControlPlane","namespace":"kubebrain-system","statefulset":"kubebrain"},"startsAt":"2026-08-09T05:00:00Z","fingerprint":"abcdef0123456789"}]}`), 0o600))
			canonical := filepath.Join(dir, "canonical.json")
			jq := filepath.Join(dir, "jq")
			writeExecutable(t, jq, fmt.Sprintf(`#!/usr/bin/env bash
tmp="$(mktemp)"; trap 'rm -f -- "$tmp"' EXIT
/usr/bin/jq "$@" >"$tmp" || exit $?
if [[ " ${*} " == *" -cnS "* ]]; then
  size="$(stat -Lc '%%s' -- "$tmp")"; head -c "$((65536-size))" /dev/zero | tr '\0' ' ' >>"$tmp"
  cp -- "$tmp" %q
fi
cat "$tmp"
`, canonical))
			controlLog := filepath.Join(dir, "control.log")
			kubectl := filepath.Join(dir, "kubectl")
			writeExecutable(t, kubectl, fmt.Sprintf(`#!/usr/bin/env bash
printf 'kubectl %%s\n' "$*" >>%q
if [[ "$*" == *"get statefulset kubebrain"* && "$*" == *"jsonpath"* ]]; then printf kb-uid; exit 0; fi
if [[ "$*" == *"get statefulset kubebrain"* ]]; then printf '%%s' '{"metadata":{"uid":"kb-uid"},"spec":{"replicas":0},"status":{"readyReplicas":0}}'; exit 0; fi
if [[ "$*" == *"get tidbcluster kb"* && "$*" == *"jsonpath"* ]]; then printf 'tc-uid\t7671'; exit 0; fi
if [[ "$*" == *"get tidbcluster kb"* ]]; then printf '%%s' '{"metadata":{"uid":"tc-uid"},"spec":{"pd":{"replicas":3},"tikv":{"replicas":3}},"status":{"clusterID":7671,"conditions":[{"type":"Ready","status":"True"}]}}'; exit 0; fi
if [[ "$*" == *"pending-peer"* ]]; then printf '%%s' '{"count":1,"regions":[{"pending_peers":[{"store_id":1}]}]}'; exit 0; fi
if [[ "$*" == *"down-peer"* ]]; then printf '%%s' '{"count":0,"regions":[]}'; exit 0; fi
if [[ "$*" == *"get secret "* ]]; then
  if [[ "$SECRET_RESPONSE" == oversized ]]; then head -c 87390 /dev/zero | tr '\0' k; else printf 'true\t'; base64 -w0 %q; fi
  exit 0
fi
cat >/dev/null
`, controlLog, canonical))
			operationctl := filepath.Join(dir, "operationctl")
			writeExecutable(t, operationctl, fmt.Sprintf("#!/usr/bin/env bash\nprintf 'operationctl %%s\\n' \"$*\" >>%q\n", controlLog))
			base := append([]string{"KUBE_CONTEXT=in-cluster", "ENDPOINT=http://kubebrain:3379", "KUBECTL=" + kubectl, "OPERATIONCTL=" + operationctl, "JQ=" + jq}, tc.extra(alert)...)
			output, err := runProductionScriptCommand(t, tc.script, append(base, "SECRET_RESPONSE=oversized"))
			require.Error(t, err)
			require.Contains(t, string(output), "existing Secret response exceeds 87389 bytes")
			require.NotContains(t, string(mustReadProductionFile(t, controlLog)), "operationctl")
			boundaryOutput, boundaryErr := runProductionScriptCommand(t, tc.script, append(base, "SECRET_RESPONSE=boundary"))
			require.NoError(t, boundaryErr, string(boundaryOutput))
			require.Contains(t, string(mustReadProductionFile(t, controlLog)), "operationctl")
		})
	}
}
