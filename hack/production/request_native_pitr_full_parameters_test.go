package production_test

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestNativePITRFullRequestersBoundExistingSecretResponses(t *testing.T) {
	for _, tt := range nativePITRFullRequesterCases() {
		t.Run(tt.name, func(t *testing.T) {
			dir := t.TempDir()
			data, err := json.Marshal(tt.parameters)
			require.NoError(t, err)
			parameters := filepath.Join(dir, "parameters.json")
			require.NoError(t, os.WriteFile(parameters, data, 0o600))
			canonical := filepath.Join(dir, "canonical.json")
			jq := filepath.Join(dir, "jq")
			writeExecutable(t, jq, fmt.Sprintf(`#!/usr/bin/env bash
tmp="$(mktemp)"; trap 'rm -f -- "$tmp"' EXIT
/usr/bin/jq "$@" >"$tmp" || exit $?
if [[ " ${*} " == *" -ceS "* ]]; then
  size="$(stat -Lc '%%s' -- "$tmp")"; head -c "$((65536-size))" /dev/zero | tr '\0' ' ' >>"$tmp"
  cp -- "$tmp" %q
fi
cat "$tmp"
`, canonical))
			controlLog := filepath.Join(dir, "control.log")
			kubectl := filepath.Join(dir, "kubectl")
			writeExecutable(t, kubectl, fmt.Sprintf(`#!/usr/bin/env bash
printf 'kubectl %%s\n' "$*" >>%q
if [[ "$*" == *" get secret "* ]]; then
  if [[ "$SECRET_RESPONSE" == oversized ]]; then head -c 87397 /dev/zero | tr '\0' k; else printf 'true\tOpaque\t'; base64 -w0 %q; fi
  exit 0
fi
cat >/dev/null
`, controlLog, canonical))
			operationctl := filepath.Join(dir, "operationctl")
			writeExecutable(t, operationctl, fmt.Sprintf("#!/usr/bin/env bash\nprintf 'operationctl %%s\\n' \"$*\" >>%q\n", controlLog))
			base := []string{"PARAMETERS_FILE=" + parameters, "KUBE_CONTEXT=in-cluster", "KUBECTL=" + kubectl, "OPERATIONCTL=" + operationctl, "JQ=" + jq}
			output, runErr := runProductionScriptCommand(t, tt.script, append(base, "SECRET_RESPONSE=oversized"))
			require.Error(t, runErr)
			require.Contains(t, string(output), "existing Secret response exceeds 87396 bytes")
			require.NotContains(t, string(mustReadProductionFile(t, controlLog)), "operationctl")
			boundaryOutput, boundaryErr := runProductionScriptCommand(t, tt.script, append(base, "SECRET_RESPONSE=boundary"))
			require.NoError(t, boundaryErr, string(boundaryOutput))
			require.Contains(t, string(mustReadProductionFile(t, controlLog)), "operationctl")
		})
	}
}

type nativePITRFullRequesterCase struct {
	name, script string
	parameters   map[string]any
}

func nativePITRFullRequesterCases() []nativePITRFullRequesterCase {
	return []nativePITRFullRequesterCase{
		{
			name:   "backup",
			script: "request-native-pitr-full-backup.sh",
			parameters: map[string]any{
				"backup_ts": "1", "pd_addrs": []string{"pd:2379"}, "storage_prefix": "s3://bucket/full",
			},
		},
		{
			name:   "restore",
			script: "request-native-pitr-full-restore.sh",
			parameters: map[string]any{
				"admission": "/var/lib/kubebrain-operation/inputs/admission.json", "approve_plan_sha256": stringOfBytes('a', 64),
				"artifact_root": "/var/lib/kubebrain-operation/inputs/artifacts", "full_artifacts": "/var/lib/kubebrain-operation/inputs/full-artifacts.json",
				"full_snapshot": "/var/lib/kubebrain-operation/inputs/full.json", "pd_addrs": []string{"pd:2379"},
				"plan": "/var/lib/kubebrain-operation/inputs/plan.json", "remote_inventory": "/var/lib/kubebrain-operation/inputs/inventory.json",
				"source_range_exclusive": "/var/lib/kubebrain-operation/inputs/range.json", "target_provisioning": "/var/lib/kubebrain-operation/inputs/provisioning.json",
				"target_provisioning_sha256": stringOfBytes('b', 64), "target_qualification": "/var/lib/kubebrain-operation/inputs/qualification.json",
				"target_qualification_sha256": stringOfBytes('c', 64), "target_snapshot_empty": "/var/lib/kubebrain-operation/inputs/empty.json",
				"target_writer_exclusion": "/var/lib/kubebrain-operation/inputs/writers.json", "target_writer_exclusion_sha256": stringOfBytes('d', 64),
			},
		},
	}
}

func TestNativePITRFullRequestersRejectOversizedFrozenCopyBeforeKubernetes(t *testing.T) {
	tests := nativePITRFullRequesterCases()
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dir := t.TempDir()
			data, err := json.Marshal(tt.parameters)
			require.NoError(t, err)
			parameters := filepath.Join(dir, "parameters.json")
			require.NoError(t, os.WriteFile(parameters, data, 0o600))
			cpWrapper := filepath.Join(dir, "cp")
			writeExecutable(t, cpWrapper, "#!/usr/bin/env bash\n/bin/cp \"$@\"\ndest=\"${@: -1}\"\nhead -c 65537 /dev/zero >>\"$dest\"\n")
			kubectl, operationctl, controlLog := writeNativePITRTargetRequesterControls(t, dir)
			output, runErr := runProductionScriptCommand(t, tt.script, []string{
				"PATH=" + dir + ":" + os.Getenv("PATH"), "PARAMETERS_FILE=" + parameters,
				"KUBE_CONTEXT=in-cluster", "KUBECTL=" + kubectl, "OPERATIONCTL=" + operationctl,
			})
			require.Error(t, runErr)
			require.Contains(t, string(output), "parameters exceed 65536 bytes")
			require.NoFileExists(t, controlLog)
		})
	}
}

func TestNativePITRFullRequestersRejectOversizedParametersBeforeKubernetes(t *testing.T) {
	for _, tt := range nativePITRFullRequesterCases() {
		t.Run(tt.name, func(t *testing.T) {
			dir := t.TempDir()
			base, err := json.Marshal(tt.parameters)
			require.NoError(t, err)
			data := append(base, []byte(strings.Repeat(" ", 65537-len(base)))...)
			parameters := filepath.Join(dir, "parameters.json")
			require.NoError(t, os.WriteFile(parameters, data, 0o600))
			kubectl, operationctl, controlLog := writeNativePITRTargetRequesterControls(t, dir)
			output, runErr := runProductionScriptCommand(t, tt.script, []string{"PARAMETERS_FILE=" + parameters, "KUBE_CONTEXT=in-cluster", "KUBECTL=" + kubectl, "OPERATIONCTL=" + operationctl})
			require.Error(t, runErr)
			require.Contains(t, string(output), "parameters exceed 65536 bytes")
			require.NoFileExists(t, controlLog)
		})
	}
}

func TestNativePITRFullRequestersAcceptParametersAtSizeLimit(t *testing.T) {
	for _, tt := range nativePITRFullRequesterCases() {
		t.Run(tt.name, func(t *testing.T) {
			dir := t.TempDir()
			base, err := json.Marshal(tt.parameters)
			require.NoError(t, err)
			data := append(base, []byte(strings.Repeat(" ", 65536-len(base)))...)
			parameters := filepath.Join(dir, "parameters.json")
			require.NoError(t, os.WriteFile(parameters, data, 0o600))
			kubectl, operationctl, controlLog := writeNativePITRTargetRequesterControls(t, dir)
			output, runErr := runProductionScriptCommand(t, tt.script, []string{"PARAMETERS_FILE=" + parameters, "KUBE_CONTEXT=in-cluster", "KUBECTL=" + kubectl, "OPERATIONCTL=" + operationctl})
			require.NoError(t, runErr, string(output))
			require.Contains(t, string(mustReadProductionFile(t, controlLog)), "operationctl --namespace kubebrain-operations --action submit")
		})
	}
}

func stringOfBytes(value byte, count int) string {
	data := make([]byte, count)
	for i := range data {
		data[i] = value
	}
	return string(data)
}
