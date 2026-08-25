package production_test

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRequestJWTKeyRotationFromKMSExportsBothVersionsThenRequests(t *testing.T) {
	dir := t.TempDir()
	client, kubectl := filepath.Join(dir, "client"), filepath.Join(dir, "kubectl")
	clientLog := filepath.Join(dir, "client.log")
	for _, path := range []string{filepath.Join(dir, "token"), filepath.Join(dir, "ca"), filepath.Join(dir, "public-key")} {
		require.NoError(t, os.WriteFile(path, []byte("value"), 0o600))
	}
	writeTrafficExecutable(t, client, `#!/usr/bin/env bash
set -euo pipefail
printf '%s\n' "$*" >>"$CLIENT_LOG"
version=""; material=""; receipt=""
while [[ "$#" -gt 0 ]]; do case "$1" in --version-id) version="$2"; shift 2;; --material-output) material="$2"; shift 2;; --receipt-output) receipt="$2"; shift 2;; *) shift;; esac; done
printf 'material:%s' "$version" >"$material"; chmod 600 "$material"
printf 'receipt:%s' "$version" >"$receipt"; chmod 600 "$receipt"
`)
	writeTrafficExecutable(t, kubectl, `#!/usr/bin/env bash
set -euo pipefail
if [[ "$*" == *" get secret "* ]]; then exit 1; fi
if [[ "$*" == *" --dry-run=client "* ]]; then
  for arg in "$@"; do case "$arg" in --from-file=parameters.json=*) parameters="${arg#--from-file=parameters.json=}";; --from-file=jwt-old-key=*) old="${arg#--from-file=jwt-old-key=}";; --from-file=jwt-new-key=*) new="${arg#--from-file=jwt-new-key=}";; esac; done
  printf '{"apiVersion":"v1","kind":"Secret","metadata":{},"type":"Opaque","data":{"parameters.json":"%s","jwt-old-key":"%s","jwt-new-key":"%s"}}\n' "$(base64 -w0 <"$parameters")" "$(base64 -w0 <"$old")" "$(base64 -w0 <"$new")"
  exit 0
fi
if [[ "$*" == *" create -f "* ]]; then exit 0; fi
exit 99
`)
	output, err := runProductionScriptCommand(t, "request-jwt-key-rotation-from-kms.sh", []string{
		"WORK_DIR=" + dir, "REQUEST_ID=change-1", "INSTANCE=instance-a", "OLD_KEY_VERSION_ID=kms/v/41", "NEW_KEY_VERSION_ID=kms/v/42",
		"KMS_PROVIDER_ENDPOINT=https://kms.example", "KMS_PROVIDER_TOKEN_FILE=" + filepath.Join(dir, "token"), "KMS_PROVIDER_CA_FILE=" + filepath.Join(dir, "ca"), "KMS_RECEIPT_PUBLIC_KEY=" + filepath.Join(dir, "public-key"),
		"KMS_EXPORT_CLIENT=" + client, "CLIENT_LOG=" + clientLog, "KMS_EXPORT_VERIFIER=/bin/true", "KUBE_CONTEXT=production", "KUBECTL=" + kubectl, "OPERATIONCTL=/bin/true",
		"KUBEBRAIN_NAMESPACE=instance-a", "SIGN_METHOD=HS256", `ENDPOINTS_JSON=["https://member-0:2379"]`, "EXPECTED_REPLICAS=1", "JWT_TTL_SECONDS=300", "MAX_CLOCK_SKEW_SECONDS=2",
	})
	require.NoError(t, err, string(output))
	calls := strings.Split(strings.TrimSpace(string(mustRead(t, clientLog))), "\n")
	require.Len(t, calls, 2)
	require.Contains(t, calls[0], "--version-id kms/v/41")
	require.Contains(t, calls[1], "--version-id kms/v/42")
	entries, err := os.ReadDir(dir)
	require.NoError(t, err)
	for _, entry := range entries {
		require.False(t, strings.HasPrefix(entry.Name(), ".jwt-kms-export."), "private export capture must be removed")
	}
}

func TestRequestJWTKeyRotationFromKMSStopsBeforeRequestOnExportFailure(t *testing.T) {
	dir := t.TempDir()
	client, kubectl := filepath.Join(dir, "client"), filepath.Join(dir, "kubectl")
	for _, path := range []string{filepath.Join(dir, "token"), filepath.Join(dir, "ca"), filepath.Join(dir, "public-key")} {
		require.NoError(t, os.WriteFile(path, []byte("value"), 0o600))
	}
	writeTrafficExecutable(t, client, "#!/usr/bin/env bash\nexit 1\n")
	writeTrafficExecutable(t, kubectl, "#!/usr/bin/env bash\nprintf called >\"$MARKER\"\n")
	marker := filepath.Join(dir, "called")
	_, err := runProductionScriptCommand(t, "request-jwt-key-rotation-from-kms.sh", []string{
		"WORK_DIR=" + dir, "REQUEST_ID=change-1", "INSTANCE=instance-a", "OLD_KEY_VERSION_ID=kms/v/41", "NEW_KEY_VERSION_ID=kms/v/42", "KMS_PROVIDER_ENDPOINT=https://kms.example",
		"KMS_PROVIDER_TOKEN_FILE=" + filepath.Join(dir, "token"), "KMS_PROVIDER_CA_FILE=" + filepath.Join(dir, "ca"), "KMS_RECEIPT_PUBLIC_KEY=" + filepath.Join(dir, "public-key"), "KMS_EXPORT_CLIENT=" + client, "KUBECTL=" + kubectl, "MARKER=" + marker,
	})
	require.Error(t, err)
	require.NoFileExists(t, marker)
}
