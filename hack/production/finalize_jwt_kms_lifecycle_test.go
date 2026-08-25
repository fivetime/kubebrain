package production_test

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestFinalizeJWTKMSLifecyclePromotesVerifiesThenRevokes(t *testing.T) {
	dir := t.TempDir()
	operation := filepath.Join(dir, "operation.json")
	require.NoError(t, os.WriteFile(operation, []byte(`{"operation_id":"jwt-key-rotate-0123456789abcdefabcd"}`), 0o600))
	client, verifier := filepath.Join(dir, "client"), filepath.Join(dir, "verifier")
	log := filepath.Join(dir, "events.log")
	writeTrafficExecutable(t, client, `#!/usr/bin/env bash
set -euo pipefail
action=""; output=""
while [[ "$#" -gt 0 ]]; do case "$1" in --action) action="$2"; shift 2;; --receipt-output) output="$2"; shift 2;; *) shift;; esac; done
printf 'client:%s\n' "$action" >>"$EVENT_LOG"
printf '%s-receipt' "$action" >"$output"; chmod 600 "$output"
`)
	writeTrafficExecutable(t, verifier, `#!/usr/bin/env bash
set -euo pipefail
action=""; while [[ "$#" -gt 0 ]]; do case "$1" in --action) action="$2"; shift 2;; *) shift;; esac; done
printf 'verify:%s\n' "$action" >>"$EVENT_LOG"
`)
	for _, path := range []string{filepath.Join(dir, "token"), filepath.Join(dir, "ca"), filepath.Join(dir, "public")} {
		require.NoError(t, os.WriteFile(path, []byte("value"), 0o600))
	}
	env := []string{"OPERATION_RECEIPT=" + operation, "LIFECYCLE_OUTPUT_DIR=" + dir, "KMS_PROVIDER_ENDPOINT=https://kms.example", "KMS_LIFECYCLE_TOKEN_FILE=" + filepath.Join(dir, "token"), "KMS_PROVIDER_CA_FILE=" + filepath.Join(dir, "ca"), "KMS_RECEIPT_PUBLIC_KEY=" + filepath.Join(dir, "public"), "KMS_LIFECYCLE_CLIENT=" + client, "KMS_LIFECYCLE_VERIFIER=" + verifier, "EVENT_LOG=" + log}
	output, err := runProductionScriptCommand(t, "finalize-jwt-kms-lifecycle.sh", env)
	require.NoError(t, err, string(output))
	require.Equal(t, []string{"client:promote", "verify:promote", "client:revoke", "verify:revoke"}, strings.Split(strings.TrimSpace(string(mustRead(t, log))), "\n"))
	output, err = runProductionScriptCommand(t, "finalize-jwt-kms-lifecycle.sh", env)
	require.NoError(t, err, string(output))
	require.Equal(t, 4, strings.Count(string(mustRead(t, log)), "verify:"))
	require.Equal(t, 2, strings.Count(string(mustRead(t, log)), "client:"))
}

func TestFinalizeJWTKMSLifecycleNeverRevokesAfterUnverifiedPromotion(t *testing.T) {
	dir := t.TempDir()
	operation := filepath.Join(dir, "operation.json")
	require.NoError(t, os.WriteFile(operation, []byte(`{"operation_id":"jwt-key-rotate-0123456789abcdefabcd"}`), 0o600))
	client, verifier, log := filepath.Join(dir, "client"), filepath.Join(dir, "verifier"), filepath.Join(dir, "client.log")
	writeTrafficExecutable(t, client, `#!/usr/bin/env bash
set -euo pipefail
printf '%s\n' "$*" >>"$CLIENT_LOG"
while [[ "$#" -gt 0 ]]; do if [[ "$1" == --receipt-output ]]; then printf receipt >"$2"; chmod 600 "$2"; exit 0; fi; shift; done
`)
	writeTrafficExecutable(t, verifier, "#!/usr/bin/env bash\nexit 1\n")
	for _, path := range []string{filepath.Join(dir, "token"), filepath.Join(dir, "ca"), filepath.Join(dir, "public")} {
		require.NoError(t, os.WriteFile(path, []byte("value"), 0o600))
	}
	_, err := runProductionScriptCommand(t, "finalize-jwt-kms-lifecycle.sh", []string{"OPERATION_RECEIPT=" + operation, "LIFECYCLE_OUTPUT_DIR=" + dir, "KMS_PROVIDER_ENDPOINT=https://kms.example", "KMS_LIFECYCLE_TOKEN_FILE=" + filepath.Join(dir, "token"), "KMS_PROVIDER_CA_FILE=" + filepath.Join(dir, "ca"), "KMS_RECEIPT_PUBLIC_KEY=" + filepath.Join(dir, "public"), "KMS_LIFECYCLE_CLIENT=" + client, "KMS_LIFECYCLE_VERIFIER=" + verifier, "CLIENT_LOG=" + log})
	require.Error(t, err)
	calls := string(mustRead(t, log))
	require.Contains(t, calls, "--action promote")
	require.NotContains(t, calls, "--action revoke")
}
