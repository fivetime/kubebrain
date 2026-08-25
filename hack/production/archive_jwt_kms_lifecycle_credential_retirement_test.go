package production_test

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestArchiveJWTKMSCredentialRetirementVerifiesWritesAndReadsExactArtifact(t *testing.T) {
	dir := t.TempDir()
	receipt, key := filepath.Join(dir, "retirement"), filepath.Join(dir, "key")
	require.NoError(t, os.WriteFile(receipt, []byte("receipt"), 0o600))
	require.NoError(t, os.WriteFile(key, []byte("key"), 0o600))
	verifier, logical, store, calls := filepath.Join(dir, "verifier"), filepath.Join(dir, "logical"), filepath.Join(dir, "store"), filepath.Join(dir, "calls")
	writeTrafficExecutable(t, verifier, `#!/usr/bin/env bash
set -euo pipefail
printf 'verify %s\n' "$*" >>"$CALLS"
while [[ $# -gt 0 ]]; do if [[ "$1" == --artifact-output ]]; then printf '{"format":"kubebrain.jwt-kms-lifecycle-credential-retirement-artifact.v1","retired_credential_id":"old-2026q2","retired_secret_uid":"old-uid","replacement_credential_id":"new-2026q3"}\n' >"$2"; chmod 600 "$2"; exit 0; fi; shift; done
exit 1
`)
	writeTrafficExecutable(t, logical, `#!/usr/bin/env bash
set -euo pipefail
printf '%s %s %s\n' "$ACTION" "$ARTIFACT_ID" "$S3_OBJECT_KEY" >>"$CALLS"
if [[ "$ACTION" == blob ]]; then cp "$INPUT" "$STORE"; printf '{}\n' >"$RECEIPT_OUTPUT"; chmod 600 "$RECEIPT_OUTPUT"; else cp "$STORE" "$OUTPUT"; fi
`)
	archiveReceipt := filepath.Join(dir, "archive.json")
	env := retirementArchiveEnv(receipt, key, archiveReceipt, verifier, logical, calls, store)
	output, err := runProductionScriptCommand(t, "archive-jwt-kms-lifecycle-credential-retirement.sh", env)
	require.NoError(t, err, string(output))
	log := string(mustRead(t, calls))
	require.Contains(t, log, "--artifact-output")
	require.Contains(t, log, "blob old-2026q2:old-uid jwt-kms-lifecycle-credential-retirement/old-2026q2/old-uid.json")
	require.Contains(t, log, "blob-read old-2026q2:old-uid jwt-kms-lifecycle-credential-retirement/old-2026q2/old-uid.json")
}

func TestArchiveJWTKMSCredentialRetirementRejectsRemoteMismatch(t *testing.T) {
	dir := t.TempDir()
	receipt, key := filepath.Join(dir, "retirement"), filepath.Join(dir, "key")
	require.NoError(t, os.WriteFile(receipt, []byte("receipt"), 0o600))
	require.NoError(t, os.WriteFile(key, []byte("key"), 0o600))
	verifier, logical := filepath.Join(dir, "verifier"), filepath.Join(dir, "logical")
	writeTrafficExecutable(t, verifier, "#!/usr/bin/env bash\nwhile [[ $# -gt 0 ]]; do if [[ \"$1\" == --artifact-output ]]; then printf '{\"retired_credential_id\":\"old-2026q2\",\"retired_secret_uid\":\"old-uid\",\"replacement_credential_id\":\"new-2026q3\"}\\n' >\"$2\"; chmod 600 \"$2\"; exit; fi; shift; done\n")
	writeTrafficExecutable(t, logical, "#!/usr/bin/env bash\nif [[ \"$ACTION\" == blob ]]; then printf '{}\\n' >\"$RECEIPT_OUTPUT\"; chmod 600 \"$RECEIPT_OUTPUT\"; else printf mismatch >\"$OUTPUT\"; fi\n")
	_, err := runProductionScriptCommand(t, "archive-jwt-kms-lifecycle-credential-retirement.sh", retirementArchiveEnv(receipt, key, filepath.Join(dir, "archive"), verifier, logical, filepath.Join(dir, "calls"), filepath.Join(dir, "store")))
	require.Error(t, err)
}

func retirementArchiveEnv(receipt, key, output, verifier, logical, calls, store string) []string {
	sha := strings.Repeat("a", 64)
	return []string{"RETIREMENT_RECEIPT=" + receipt, "KMS_RECEIPT_PUBLIC_KEY=" + key, "ARCHIVE_RECEIPT_OUTPUT=" + output, "RETIRED_CREDENTIAL_ID=old-2026q2", "RETIRED_SECRET_UID=old-uid", "RETIRED_SECRET_DATA_SHA256=" + sha, "REPLACEMENT_CREDENTIAL_ID=new-2026q3", "REPLACEMENT_SECRET_UID=new-uid", "REPLACEMENT_SECRET_DATA_SHA256=" + sha, "REPLACEMENT_READINESS_RECEIPT_SHA256=" + sha, "OBJECT_STORE_ID=store-a", "S3_BUCKET=audit", "RETENTION_MODE=COMPLIANCE", "RETAIN_UNTIL_UNIX=2100000000", "KMS_LIFECYCLE_CREDENTIAL_RETIREMENT_VERIFIER=" + verifier, "LOGICAL_OBJECT=" + logical, "CALLS=" + calls, "STORE=" + store}
}
