package production_test

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestArchiveJWTKMSLifecycleVerifiesArchivesAndReadsExactEvidence(t *testing.T) {
	dir := t.TempDir()
	operationID := "jwt-key-rotate-0123456789abcdefabcd"
	inputs := make([]string, 4)
	for i, name := range []string{"operation", "promotion", "revocation", "public"} {
		inputs[i] = filepath.Join(dir, name)
		require.NoError(t, os.WriteFile(inputs[i], []byte(name), 0o600))
	}
	verifier, logical := filepath.Join(dir, "verifier"), filepath.Join(dir, "logical")
	store, calls := filepath.Join(dir, "store"), filepath.Join(dir, "calls")
	writeTrafficExecutable(t, verifier, `#!/usr/bin/env bash
set -euo pipefail
printf '%s\n' "$*" >>"$CALLS"
while [[ $# -gt 0 ]]; do if [[ "$1" == --artifact-output ]]; then printf '{"format":"kubebrain.jwt-kms-lifecycle-artifact.v1","instance":"instance-a","operation_id":"jwt-key-rotate-0123456789abcdefabcd"}\n' >"$2"; chmod 600 "$2"; exit 0; fi; shift; done
exit 1
`)
	writeTrafficExecutable(t, logical, `#!/usr/bin/env bash
set -euo pipefail
printf '%s %s %s %s\n' "$ACTION" "$ARTIFACT_FORMAT" "$ARTIFACT_ID" "$S3_OBJECT_KEY" >>"$CALLS"
if [[ "$ACTION" == blob ]]; then cp "$INPUT" "$STORE"; printf '{}\n' >"$RECEIPT_OUTPUT"; chmod 600 "$RECEIPT_OUTPUT"; else cp "$STORE" "$OUTPUT"; fi
`)
	receipt := filepath.Join(dir, "archive-receipt.json")
	env := []string{
		"OPERATION_RECEIPT=" + inputs[0], "PROMOTION_RECEIPT=" + inputs[1], "REVOCATION_RECEIPT=" + inputs[2], "KMS_RECEIPT_PUBLIC_KEY=" + inputs[3],
		"ARCHIVE_RECEIPT_OUTPUT=" + receipt, "OBJECT_STORE_ID=store-a", "S3_BUCKET=audit", "RETENTION_MODE=COMPLIANCE", "RETAIN_UNTIL_UNIX=2100000000",
		"KMS_LIFECYCLE_VERIFIER=" + verifier, "LOGICAL_OBJECT=" + logical, "CALLS=" + calls, "STORE=" + store,
	}
	output, err := runProductionScriptCommand(t, "archive-jwt-kms-lifecycle.sh", env)
	require.NoError(t, err, string(output))
	require.FileExists(t, receipt)
	log := string(mustRead(t, calls))
	require.Contains(t, log, "--action revoke")
	require.Contains(t, log, "--artifact-output")
	require.Contains(t, log, "blob kubebrain.jwt-kms-lifecycle-artifact.v1 "+operationID+" jwt-kms-lifecycle/instance-a/"+operationID+".json")
	require.Contains(t, log, "blob-read kubebrain.jwt-kms-lifecycle-artifact.v1 "+operationID+" jwt-kms-lifecycle/instance-a/"+operationID+".json")
}

func TestArchiveJWTKMSLifecycleDoesNotWriteObjectWhenVerificationFails(t *testing.T) {
	dir := t.TempDir()
	paths := make([]string, 4)
	for i := range paths {
		paths[i] = filepath.Join(dir, string(rune('a'+i)))
		require.NoError(t, os.WriteFile(paths[i], []byte("evidence"), 0o600))
	}
	verifier, logical, calls := filepath.Join(dir, "verifier"), filepath.Join(dir, "logical"), filepath.Join(dir, "calls")
	writeTrafficExecutable(t, verifier, "#!/usr/bin/env bash\nexit 1\n")
	writeTrafficExecutable(t, logical, "#!/usr/bin/env bash\nprintf called >\"$CALLS\"\n")
	_, err := runProductionScriptCommand(t, "archive-jwt-kms-lifecycle.sh", []string{
		"OPERATION_RECEIPT=" + paths[0], "PROMOTION_RECEIPT=" + paths[1], "REVOCATION_RECEIPT=" + paths[2], "KMS_RECEIPT_PUBLIC_KEY=" + paths[3],
		"ARCHIVE_RECEIPT_OUTPUT=" + filepath.Join(dir, "receipt"), "OBJECT_STORE_ID=store-a", "S3_BUCKET=audit", "RETENTION_MODE=COMPLIANCE", "RETAIN_UNTIL_UNIX=2100000000",
		"KMS_LIFECYCLE_VERIFIER=" + verifier, "LOGICAL_OBJECT=" + logical, "CALLS=" + calls,
	})
	require.Error(t, err)
	require.NoFileExists(t, calls)
}
