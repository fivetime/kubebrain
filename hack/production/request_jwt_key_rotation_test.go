package production_test

import (
	"encoding/base64"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRequestJWTKeyRotationCreatesOnlyBoundUnapprovedOperation(t *testing.T) {
	dir := t.TempDir()
	oldKey, newKey := filepath.Join(dir, "old.key"), filepath.Join(dir, "new.key")
	require.NoError(t, os.WriteFile(oldKey, []byte("old-key-material"), 0o600))
	require.NoError(t, os.WriteFile(newKey, []byte("new-key-material"), 0o600))
	kubectlLog, operationLog, secretCapture := filepath.Join(dir, "kubectl.log"), filepath.Join(dir, "operation.log"), filepath.Join(dir, "secret.json")
	kubectl, operationctl := filepath.Join(dir, "kubectl"), filepath.Join(dir, "operationctl")
	writeTrafficExecutable(t, kubectl, `#!/usr/bin/env bash
set -euo pipefail
printf '%s\n' "$*" >>"$KUBECTL_LOG"
if [[ "$*" == *" get secret "* ]]; then exit 1; fi
if [[ "$*" == *" --dry-run=client "* ]]; then
  for arg in "$@"; do [[ "$arg" == --from-file=parameters.json=* ]] && source="${arg#--from-file=parameters.json=}"; done
  printf '{"apiVersion":"v1","kind":"Secret","metadata":{},"type":"Opaque","data":{"parameters.json":"%s"}}\n' "$(base64 -w0 <"$source")"
  exit 0
fi
if [[ "$*" == *" create -f -"* ]]; then cat >"$SECRET_CAPTURE"; exit 0; fi
exit 99
`)
	writeTrafficExecutable(t, operationctl, "#!/usr/bin/env bash\nset -euo pipefail\nprintf '%s\\n' \"$*\" >>\"$OPERATION_LOG\"\n")
	output, err := runProductionScriptCommand(t, "request-jwt-key-rotation.sh", []string{
		"REQUEST_ID=change-2026-jwt-1", "INSTANCE=instance-a", "WORK_DIR=" + dir,
		"KUBEBRAIN_NAMESPACE=instance-a", "KUBEBRAIN_STATEFULSET=kubebrain",
		"OLD_KEY_SOURCE=" + oldKey, "NEW_KEY_SOURCE=" + newKey, "SIGN_METHOD=HS256",
		`ENDPOINTS_JSON=["https://member-0:2379","https://member-1:2379","https://member-2:2379"]`,
		"EXPECTED_REPLICAS=3", "JWT_TTL_SECONDS=300", "MAX_CLOCK_SKEW_SECONDS=2",
		"KUBE_CONTEXT=production", "KUBECTL=" + kubectl, "OPERATIONCTL=" + operationctl,
		"KUBECTL_LOG=" + kubectlLog, "OPERATION_LOG=" + operationLog, "SECRET_CAPTURE=" + secretCapture,
	})
	require.NoError(t, err, string(output))
	require.Contains(t, string(output), "unapproved Pending JWTKeyRotation")
	operation := string(mustRead(t, operationLog))
	require.Contains(t, operation, "--requested-by platform:jwt-key-rotation")
	require.Contains(t, operation, "--type JWTKeyRotation")
	require.Contains(t, operation, "--max-attempts 5")
	require.NotContains(t, operation, "approve")
	require.NotContains(t, string(mustRead(t, kubectlLog)), " patch ")

	var secret struct {
		Immutable bool              `json:"immutable"`
		Data      map[string]string `json:"data"`
	}
	require.NoError(t, json.Unmarshal(mustRead(t, secretCapture), &secret))
	require.True(t, secret.Immutable)
	parameters, err := base64.StdEncoding.DecodeString(secret.Data["parameters.json"])
	require.NoError(t, err)
	var values map[string]any
	require.NoError(t, json.Unmarshal(parameters, &values))
	require.Equal(t, "HS256", values["sign_method"])
	require.Regexp(t, `^jwt-key-rotate-[a-f0-9]{20}-keys$`, values["key_secret"])
	require.Equal(t, float64(300), values["jwt_ttl_seconds"])
	require.Len(t, values["endpoints"], 3)
	require.NotEqual(t, values["old_key_sha256"], values["new_key_sha256"])
}

func TestRequestJWTKeyRotationRejectsUnsafeInputsBeforeKubernetes(t *testing.T) {
	for _, tc := range []struct {
		name, endpoints string
		mode            os.FileMode
		want            string
	}{
		{name: "permissive private key", endpoints: `["https://member-0:2379"]`, mode: 0o640, want: "inaccessible to group/other"},
		{name: "duplicate endpoints", endpoints: `["https://member-0:2379","https://member-0:2379"]`, mode: 0o600, want: "exact unique HTTPS member set"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			oldKey, newKey := filepath.Join(dir, "old.key"), filepath.Join(dir, "new.key")
			require.NoError(t, os.WriteFile(oldKey, []byte("old"), tc.mode))
			require.NoError(t, os.WriteFile(newKey, []byte("new"), 0o600))
			marker, command := filepath.Join(dir, "called"), filepath.Join(dir, "command")
			writeTrafficExecutable(t, command, "#!/usr/bin/env bash\nprintf called >\"$MARKER\"\n")
			replicas := "1"
			if tc.name == "duplicate endpoints" {
				replicas = "2"
			}
			output, err := runProductionScriptCommand(t, "request-jwt-key-rotation.sh", []string{
				"REQUEST_ID=change-2026-jwt-2", "INSTANCE=instance-a", "WORK_DIR=" + dir,
				"KUBEBRAIN_NAMESPACE=instance-a", "OLD_KEY_SOURCE=" + oldKey, "NEW_KEY_SOURCE=" + newKey,
				"SIGN_METHOD=HS256", "ENDPOINTS_JSON=" + tc.endpoints, "EXPECTED_REPLICAS=" + replicas, "JWT_TTL_SECONDS=300", "MAX_CLOCK_SKEW_SECONDS=2",
				"KUBE_CONTEXT=production", "KUBECTL=" + command, "OPERATIONCTL=" + command, "MARKER=" + marker,
			})
			require.Error(t, err)
			require.Contains(t, string(output), tc.want)
			require.NoFileExists(t, marker)
		})
	}
}
