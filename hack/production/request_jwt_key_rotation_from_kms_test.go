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
	client, request := filepath.Join(dir, "client"), filepath.Join(dir, "request")
	clientLog, requestLog := filepath.Join(dir, "client.log"), filepath.Join(dir, "request.log")
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
	writeTrafficExecutable(t, request, `#!/usr/bin/env bash
set -euo pipefail
printf '%s\n' "$OLD_KEY_SOURCE|$NEW_KEY_SOURCE|$OLD_KEY_EXPORT_RECEIPT|$NEW_KEY_EXPORT_RECEIPT|$KMS_RECEIPT_PUBLIC_KEY" >"$REQUEST_LOG"
[[ "$(cat "$OLD_KEY_SOURCE")" == "material:kms/v/41" && "$(cat "$NEW_KEY_SOURCE")" == "material:kms/v/42" ]]
`)
	output, err := runProductionScriptCommand(t, "request-jwt-key-rotation-from-kms.sh", []string{
		"WORK_DIR=" + dir, "REQUEST_ID=change-1", "INSTANCE=instance-a", "OLD_KEY_VERSION_ID=kms/v/41", "NEW_KEY_VERSION_ID=kms/v/42",
		"KMS_PROVIDER_ENDPOINT=https://kms.example", "KMS_PROVIDER_TOKEN_FILE=" + filepath.Join(dir, "token"), "KMS_PROVIDER_CA_FILE=" + filepath.Join(dir, "ca"), "KMS_RECEIPT_PUBLIC_KEY=" + filepath.Join(dir, "public-key"),
		"KMS_EXPORT_CLIENT=" + client, "REQUEST_COMMAND=" + request, "CLIENT_LOG=" + clientLog, "REQUEST_LOG=" + requestLog,
	})
	require.NoError(t, err, string(output))
	calls := strings.Split(strings.TrimSpace(string(mustRead(t, clientLog))), "\n")
	require.Len(t, calls, 2)
	require.Contains(t, calls[0], "--version-id kms/v/41")
	require.Contains(t, calls[1], "--version-id kms/v/42")
	require.FileExists(t, requestLog)
	entries, err := os.ReadDir(dir)
	require.NoError(t, err)
	for _, entry := range entries {
		require.False(t, strings.HasPrefix(entry.Name(), ".jwt-kms-export."), "private export capture must be removed")
	}
}

func TestRequestJWTKeyRotationFromKMSStopsBeforeRequestOnExportFailure(t *testing.T) {
	dir := t.TempDir()
	client, request := filepath.Join(dir, "client"), filepath.Join(dir, "request")
	for _, path := range []string{filepath.Join(dir, "token"), filepath.Join(dir, "ca"), filepath.Join(dir, "public-key")} {
		require.NoError(t, os.WriteFile(path, []byte("value"), 0o600))
	}
	writeTrafficExecutable(t, client, "#!/usr/bin/env bash\nexit 1\n")
	writeTrafficExecutable(t, request, "#!/usr/bin/env bash\nprintf called >\"$MARKER\"\n")
	marker := filepath.Join(dir, "called")
	_, err := runProductionScriptCommand(t, "request-jwt-key-rotation-from-kms.sh", []string{
		"WORK_DIR=" + dir, "REQUEST_ID=change-1", "INSTANCE=instance-a", "OLD_KEY_VERSION_ID=kms/v/41", "NEW_KEY_VERSION_ID=kms/v/42", "KMS_PROVIDER_ENDPOINT=https://kms.example",
		"KMS_PROVIDER_TOKEN_FILE=" + filepath.Join(dir, "token"), "KMS_PROVIDER_CA_FILE=" + filepath.Join(dir, "ca"), "KMS_RECEIPT_PUBLIC_KEY=" + filepath.Join(dir, "public-key"), "KMS_EXPORT_CLIENT=" + client, "REQUEST_COMMAND=" + request, "MARKER=" + marker,
	})
	require.Error(t, err)
	require.NoFileExists(t, marker)
}
