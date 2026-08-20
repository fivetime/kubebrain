package production_test

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

type nativePITRTargetRequesterCase struct {
	name, script, payload string
}

func nativePITRTargetRequesterCases() []nativePITRTargetRequesterCase {
	return []nativePITRTargetRequesterCase{
		{"provisioning", "request-native-pitr-target-provisioning.sh", `{"manifest":"/var/lib/kubebrain-operation/inputs/manifest.json","manifest_sha256":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","old_target_provisioning":"/var/lib/kubebrain-operation/inputs/old.json","old_target_provisioning_sha256":"bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb","retirement":"/var/lib/kubebrain-operation/inputs/retirement.json","retirement_sha256":"cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"}`},
		{"retirement", "request-native-pitr-target-retirement.sh", `{"failed_operation_audit":"/var/lib/kubebrain-operation/inputs/audit.json","failed_operation_audit_sha256":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","failed_operation_parameters":"/var/lib/kubebrain-operation/inputs/parameters.json","failed_operation_parameters_sha256":"bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb","old_plan":"/var/lib/kubebrain-operation/inputs/plan.json","old_plan_sha256":"cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc","old_restore_admission":"/var/lib/kubebrain-operation/inputs/admission.json","old_restore_admission_sha256":"dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd","old_target_provisioning":"/var/lib/kubebrain-operation/inputs/provisioning.json","old_target_provisioning_sha256":"eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee","old_target_snapshot_empty":"/var/lib/kubebrain-operation/inputs/empty.json","old_target_snapshot_empty_sha256":"ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff"}`},
	}
}

func writeNativePITRTargetRequesterControls(t *testing.T, dir string) (string, string, string) {
	t.Helper()
	controlLog := filepath.Join(dir, "control.log")
	kubectl := filepath.Join(dir, "kubectl")
	writeExecutable(t, kubectl, fmt.Sprintf(`#!/usr/bin/env bash
printf 'kubectl %%s\n' "$*" >>%q
if [[ "$*" == *" get secret "* ]]; then exit 1; fi
if [[ "$*" == *" create secret generic "* ]]; then printf '%%s\n' '{"kind":"Secret"}'; else cat >/dev/null; fi
`, controlLog))
	operationctl := filepath.Join(dir, "operationctl")
	writeExecutable(t, operationctl, fmt.Sprintf("#!/usr/bin/env bash\nprintf 'operationctl %%s\\n' \"$*\" >>%q\n", controlLog))
	return kubectl, operationctl, controlLog
}

func TestNativePITRTargetRequestersBoundExistingSecretResponses(t *testing.T) {
	for _, tt := range nativePITRTargetRequesterCases() {
		t.Run(tt.name, func(t *testing.T) {
			dir := t.TempDir()
			parameters := filepath.Join(dir, "parameters.json")
			require.NoError(t, os.WriteFile(parameters, []byte(tt.payload), 0o600))
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
			output, err := runProductionScriptCommand(t, tt.script, append(base, "SECRET_RESPONSE=oversized"))
			require.Error(t, err)
			require.Contains(t, string(output), "existing Secret response exceeds 87396 bytes")
			require.NotContains(t, string(mustReadProductionFile(t, controlLog)), "operationctl")
			boundaryOutput, boundaryErr := runProductionScriptCommand(t, tt.script, append(base, "SECRET_RESPONSE=boundary"))
			require.NoError(t, boundaryErr, string(boundaryOutput))
			require.Contains(t, string(mustReadProductionFile(t, controlLog)), "operationctl")
		})
	}
}

func TestNativePITRTargetRequestersRejectOversizedParametersBeforeKubernetes(t *testing.T) {
	for _, tt := range nativePITRTargetRequesterCases() {
		t.Run(tt.name, func(t *testing.T) {
			dir := t.TempDir()
			data := []byte(tt.payload + strings.Repeat(" ", 65537-len(tt.payload)))
			require.Len(t, data, 65537)
			parameters := filepath.Join(dir, "parameters.json")
			require.NoError(t, os.WriteFile(parameters, data, 0o600))
			kubectl, operationctl, controlLog := writeNativePITRTargetRequesterControls(t, dir)
			output, err := runProductionScriptCommand(t, tt.script, []string{"PARAMETERS_FILE=" + parameters, "KUBE_CONTEXT=in-cluster", "KUBECTL=" + kubectl, "OPERATIONCTL=" + operationctl})
			require.Error(t, err)
			require.Contains(t, string(output), "parameters exceed 65536 bytes")
			require.NoFileExists(t, controlLog)
		})
	}
}

func TestNativePITRTargetRequestersRejectOversizedFrozenCopy(t *testing.T) {
	for _, tt := range nativePITRTargetRequesterCases() {
		t.Run(tt.name, func(t *testing.T) {
			dir := t.TempDir()
			parameters := filepath.Join(dir, "parameters.json")
			require.NoError(t, os.WriteFile(parameters, []byte(tt.payload), 0o600))
			cpWrapper := filepath.Join(dir, "cp")
			writeExecutable(t, cpWrapper, "#!/usr/bin/env bash\n/bin/cp \"$@\"\ndest=\"${@: -1}\"\nhead -c 65537 /dev/zero >>\"$dest\"\n")
			kubectl, operationctl, controlLog := writeNativePITRTargetRequesterControls(t, dir)
			output, err := runProductionScriptCommand(t, tt.script, []string{"PATH=" + dir + ":" + os.Getenv("PATH"), "PARAMETERS_FILE=" + parameters, "KUBE_CONTEXT=in-cluster", "KUBECTL=" + kubectl, "OPERATIONCTL=" + operationctl})
			require.Error(t, err)
			require.Contains(t, string(output), "parameters exceed 65536 bytes")
			require.NoFileExists(t, controlLog)
		})
	}
}

func TestNativePITRTargetRequestersAcceptParametersAtSizeLimit(t *testing.T) {
	for _, tt := range nativePITRTargetRequesterCases() {
		t.Run(tt.name, func(t *testing.T) {
			dir := t.TempDir()
			data := []byte(tt.payload + strings.Repeat(" ", 65536-len(tt.payload)))
			require.Len(t, data, 65536)
			parameters := filepath.Join(dir, "parameters.json")
			require.NoError(t, os.WriteFile(parameters, data, 0o600))
			kubectl, operationctl, controlLog := writeNativePITRTargetRequesterControls(t, dir)
			output, err := runProductionScriptCommand(t, tt.script, []string{"PARAMETERS_FILE=" + parameters, "KUBE_CONTEXT=in-cluster", "KUBECTL=" + kubectl, "OPERATIONCTL=" + operationctl})
			require.NoError(t, err, string(output))
			require.Contains(t, string(mustReadProductionFile(t, controlLog)), "operationctl --namespace kubebrain-operations --action submit")
		})
	}
}
