package production_test

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestColdPhysicalRequestersRejectExecutorIncompatibleParameters(t *testing.T) {
	t.Run("snapshot", func(t *testing.T) {
		dir := t.TempDir()
		preflight := filepath.Join(dir, "preflight.json")
		require.NoError(t, os.WriteFile(preflight, []byte(`{"format":"kubebrain.cold-physical-snapshot-preflight.v2","kubebrain":{"namespace":"kubebrain-system","statefulset":"kubebrain","uid":"kb-uid"},"storage":{"namespace":"tidb-cluster","tidb_cluster":"kb","uid":"tc-uid","cluster_id":"7671"}}`), 0o600))
		witness := filepath.Join(dir, "witness.jsonl")
		require.NoError(t, os.WriteFile(witness, []byte("verified-witness\n"), 0o600))
		jq := writeOversizedCanonicalJQ(t, dir)
		kubectl, operationctl, controlLog := writeColdRequesterControls(t, dir, false)
		env := []string{
			"REQUEST_ID=change-oversized-snapshot", "PREFLIGHT_FILE=" + preflight, "SEMANTIC_WITNESS_FILE=" + witness,
			"EXPECTED_WITNESS_PREFIX=/", "KUBE_CONTEXT=in-cluster", "KUBECTL=" + kubectl,
			"OPERATIONCTL=" + operationctl, "JQ=" + jq,
		}
		output, err := runProductionScriptCommand(t, "request-cold-physical-snapshot.sh", env)
		require.Error(t, err)
		require.Contains(t, string(output), "operation parameters exceed 65536 bytes")
		if calls, readErr := os.ReadFile(controlLog); readErr == nil {
			require.NotContains(t, string(calls), "create secret")
			require.NotContains(t, string(calls), "operationctl")
		}
		boundaryOutput, boundaryErr := runProductionScriptCommand(t, "request-cold-physical-snapshot.sh", append(env, "CANONICAL_BYTES=65536"))
		require.NoError(t, boundaryErr, string(boundaryOutput))
		require.Contains(t, string(mustReadProductionFile(t, controlLog)), "operationctl")
	})

	t.Run("restore", func(t *testing.T) {
		dir := t.TempDir()
		var source map[string]any
		require.NoError(t, json.Unmarshal(coldRestoreSnapshotReceipt(t), &source))
		source["operation_id"] = "cold-snapshot-0123456789abcdefabcd"
		data, err := json.Marshal(source)
		require.NoError(t, err)
		receipt := filepath.Join(dir, "receipt.json")
		require.NoError(t, os.WriteFile(receipt, data, 0o600))
		renderer := filepath.Join(dir, "renderer")
		writeExecutable(t, renderer, "#!/usr/bin/env bash\nout=\"\"; while [[ $# -gt 0 ]]; do [[ $1 == --output ]] && { out=$2; shift 2; continue; }; shift; done\nprintf '{\"apiVersion\":\"v1\",\"kind\":\"List\",\"items\":[]}\\n' >\"$out\"\n")
		jq := writeOversizedCanonicalJQ(t, dir)
		kubectl, operationctl, controlLog := writeColdRequesterControls(t, dir, true)
		env := []string{
			"REQUEST_ID=change-oversized-restore", "RECEIPT_FILE=" + receipt, "KUBE_CONTEXT=in-cluster",
			"TARGET_SNAPSHOT_CLASS=retained", "TARGET_STORAGE_CLASS=fast", "KUBECTL=" + kubectl,
			"OPERATIONCTL=" + operationctl, "COLD_RESTORE_RENDER=" + renderer, "JQ=" + jq,
		}
		output, runErr := runProductionScriptCommand(t, "request-cold-physical-restore.sh", env)
		require.Error(t, runErr)
		require.Contains(t, string(output), "operation parameters exceed 65536 bytes")
		calls := string(mustReadProductionFile(t, controlLog))
		require.NotContains(t, calls, "create secret")
		require.NotContains(t, calls, "operationctl")
		boundaryOutput, boundaryErr := runProductionScriptCommand(t, "request-cold-physical-restore.sh", append(env, "CANONICAL_BYTES=65536"))
		require.NoError(t, boundaryErr, string(boundaryOutput))
		require.Contains(t, string(mustReadProductionFile(t, controlLog)), "operationctl")
	})
}

func writeOversizedCanonicalJQ(t *testing.T, dir string) string {
	t.Helper()
	path := filepath.Join(dir, "jq")
	writeExecutable(t, path, `#!/usr/bin/env bash
tmp="$(mktemp)"; trap 'rm -f -- "$tmp"' EXIT
/usr/bin/jq "$@" >"$tmp"; rc=$?
cat "$tmp"
if [[ $rc == 0 && " ${*} " == *" -cnS "* ]]; then
  target="${CANONICAL_BYTES:-70000}"; size="$(stat -Lc '%s' -- "$tmp")"
  (( size <= target )) || exit 97
  head -c "$((target-size))" /dev/zero | tr '\0' ' '
fi
exit "$rc"
`)
	return path
}

func writeColdRequesterControls(t *testing.T, dir string, restore bool) (string, string, string) {
	t.Helper()
	logPath := filepath.Join(dir, "control.log")
	kubectl := filepath.Join(dir, "kubectl")
	extra := ""
	if restore {
		extra = "if [[ \"$*\" == *\"get namespace kube-system\"* ]]; then printf target-kube-uid; exit 0; fi\nif [[ \"$*\" == *\"get namespace tidb-cluster\"* ]]; then printf target-namespace-uid; exit 0; fi\n"
	}
	writeExecutable(t, kubectl, fmt.Sprintf("#!/usr/bin/env bash\nprintf 'kubectl %%s\\n' \"$*\" >>%q\n%sif [[ \"$*\" == *\"get secret \"* ]]; then exit 1; fi\nif [[ \"$*\" == *\"create secret generic\"* ]]; then printf '{\"kind\":\"Secret\"}\\n'; else cat >/dev/null; fi\n", logPath, extra))
	operationctl := filepath.Join(dir, "operationctl")
	writeExecutable(t, operationctl, fmt.Sprintf("#!/usr/bin/env bash\nprintf 'operationctl %%s\\n' \"$*\" >>%q\n", logPath))
	return kubectl, operationctl, logPath
}
