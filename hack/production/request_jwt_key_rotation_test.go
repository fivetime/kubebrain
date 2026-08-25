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
	oldExport, newExport, publicKey := filepath.Join(dir, "old-export.json"), filepath.Join(dir, "new-export.json"), filepath.Join(dir, "kms-public.pem")
	require.NoError(t, os.WriteFile(oldExport, []byte("old-export-receipt"), 0o600))
	require.NoError(t, os.WriteFile(newExport, []byte("new-export-receipt"), 0o600))
	require.NoError(t, os.WriteFile(publicKey, []byte("public-key"), 0o644))
	kubectlLog, operationLog, secretCapture := filepath.Join(dir, "kubectl.log"), filepath.Join(dir, "operation.log"), filepath.Join(dir, "secret.json")
	kubectl, operationctl, verifier := filepath.Join(dir, "kubectl"), filepath.Join(dir, "operationctl"), filepath.Join(dir, "verifier")
	writeTrafficExecutable(t, kubectl, `#!/usr/bin/env bash
set -euo pipefail
printf '%s\n' "$*" >>"$KUBECTL_LOG"
if [[ "$*" == *" get secret "* ]]; then exit 1; fi
if [[ "$*" == *" --dry-run=client "* ]]; then
  for arg in "$@"; do case "$arg" in --from-file=parameters.json=*) parameters="${arg#--from-file=parameters.json=}";; --from-file=jwt-old-key=*) old="${arg#--from-file=jwt-old-key=}";; --from-file=jwt-new-key=*) new="${arg#--from-file=jwt-new-key=}";; esac; done
  printf '{"apiVersion":"v1","kind":"Secret","metadata":{},"type":"Opaque","data":{"parameters.json":"%s","jwt-old-key":"%s","jwt-new-key":"%s"}}\n' "$(base64 -w0 <"$parameters")" "$(base64 -w0 <"$old")" "$(base64 -w0 <"$new")"
  exit 0
fi
if [[ "$*" == *" create -f "* ]]; then while [[ "$#" -gt 0 ]]; do if [[ "$1" == -f ]]; then cp "$2" "$SECRET_CAPTURE"; exit 0; fi; shift; done; fi
exit 99
`)
	writeTrafficExecutable(t, operationctl, "#!/usr/bin/env bash\nset -euo pipefail\nprintf '%s\\n' \"$*\" >>\"$OPERATION_LOG\"\n")
	writeTrafficExecutable(t, verifier, "#!/usr/bin/env bash\nset -euo pipefail\nprintf '%s\\n' \"$*\" >>\"$VERIFIER_LOG\"\n")
	verifierLog := filepath.Join(dir, "verifier.log")
	output, err := runProductionScriptCommand(t, "request-jwt-key-rotation.sh", []string{
		"REQUEST_ID=change-2026-jwt-1", "INSTANCE=instance-a", "WORK_DIR=" + dir,
		"KUBEBRAIN_NAMESPACE=instance-a", "KUBEBRAIN_STATEFULSET=kubebrain",
		"OLD_KEY_SOURCE=" + oldKey, "NEW_KEY_SOURCE=" + newKey, "SIGN_METHOD=HS256",
		"OLD_KEY_VERSION_ID=kms/prod/jwt/versions/41", "NEW_KEY_VERSION_ID=kms/prod/jwt/versions/42",
		"OLD_KEY_EXPORT_RECEIPT=" + oldExport, "NEW_KEY_EXPORT_RECEIPT=" + newExport, "KMS_RECEIPT_PUBLIC_KEY=" + publicKey, "KMS_EXPORT_VERIFIER=" + verifier,
		`ENDPOINTS_JSON=["https://member-0:2379","https://member-1:2379","https://member-2:2379"]`,
		"EXPECTED_REPLICAS=3", "JWT_TTL_SECONDS=300", "MAX_CLOCK_SKEW_SECONDS=2",
		"KUBE_CONTEXT=production", "KUBECTL=" + kubectl, "OPERATIONCTL=" + operationctl,
		"KUBECTL_LOG=" + kubectlLog, "OPERATION_LOG=" + operationLog, "SECRET_CAPTURE=" + secretCapture, "VERIFIER_LOG=" + verifierLog,
	})
	require.NoError(t, err, string(output))
	require.Contains(t, string(output), "unapproved Pending JWTKeyRotation")
	operation := string(mustRead(t, operationLog))
	require.Contains(t, operation, "--requested-by platform:jwt-key-rotation")
	require.Contains(t, operation, "--type JWTKeyRotation")
	require.Contains(t, operation, "--max-attempts 5")
	require.NotContains(t, operation, "approve")
	require.NotContains(t, string(mustRead(t, kubectlLog)), " patch ")
	verifyCalls := string(mustRead(t, verifierLog))
	require.Contains(t, verifyCalls, "--request-id change-2026-jwt-1 --instance instance-a --version-id kms/prod/jwt/versions/41")
	require.Contains(t, verifyCalls, "--request-id change-2026-jwt-1 --instance instance-a --version-id kms/prod/jwt/versions/42")
	require.Contains(t, verifyCalls, "/old-export-receipt.json")
	require.Contains(t, verifyCalls, "/new-export-receipt.json")

	var secret struct {
		Immutable bool              `json:"immutable"`
		Data      map[string]string `json:"data"`
	}
	require.NoError(t, json.Unmarshal(mustRead(t, secretCapture), &secret))
	require.True(t, secret.Immutable)
	require.Len(t, secret.Data, 3)
	oldMaterial, err := base64.StdEncoding.DecodeString(secret.Data["jwt-old-key"])
	require.NoError(t, err)
	require.Equal(t, "old-key-material", string(oldMaterial))
	newMaterial, err := base64.StdEncoding.DecodeString(secret.Data["jwt-new-key"])
	require.NoError(t, err)
	require.Equal(t, "new-key-material", string(newMaterial))
	parameters, err := base64.StdEncoding.DecodeString(secret.Data["parameters.json"])
	require.NoError(t, err)
	var values map[string]any
	require.NoError(t, json.Unmarshal(parameters, &values))
	require.Equal(t, "HS256", values["sign_method"])
	require.Equal(t, "change-2026-jwt-1", values["request_id"])
	require.Regexp(t, `^jwt-key-rotate-[a-f0-9]{20}-keys$`, values["key_secret"])
	require.Equal(t, float64(300), values["jwt_ttl_seconds"])
	require.Len(t, values["endpoints"], 3)
	require.NotEqual(t, values["old_key_sha256"], values["new_key_sha256"])
	require.Equal(t, "jwt-old-key", values["old_key_material_key"])
	require.Equal(t, "jwt-new-key", values["new_key_material_key"])
	require.Equal(t, "kms/prod/jwt/versions/41", values["old_key_version_id"])
	require.Equal(t, "kms/prod/jwt/versions/42", values["new_key_version_id"])
	require.Regexp(t, `^[a-f0-9]{64}$`, values["kms_receipt_public_key_sha256"])
	require.Regexp(t, `^[a-f0-9]{64}$`, values["old_key_export_receipt_sha256"])
	require.Regexp(t, `^[a-f0-9]{64}$`, values["new_key_export_receipt_sha256"])
	require.NotContains(t, values, "old_key_source")
	require.NotContains(t, values, "new_key_source")
}

func TestRequestJWTKeyRotationRejectsUnsafeInputsBeforeKubernetes(t *testing.T) {
	for _, tc := range []struct {
		name, endpoints, oldVersion, newVersion string
		mode                                    os.FileMode
		want                                    string
	}{
		{name: "permissive private key", endpoints: `["https://member-0:2379"]`, oldVersion: "kms/prod/jwt/versions/41", newVersion: "kms/prod/jwt/versions/42", mode: 0o640, want: "inaccessible to group/other"},
		{name: "duplicate endpoints", endpoints: `["https://member-0:2379","https://member-0:2379"]`, oldVersion: "kms/prod/jwt/versions/41", newVersion: "kms/prod/jwt/versions/42", mode: 0o600, want: "exact unique HTTPS member set"},
		{name: "same KMS version", endpoints: `["https://member-0:2379"]`, oldVersion: "kms/prod/jwt/versions/41", newVersion: "kms/prod/jwt/versions/41", mode: 0o600, want: "distinct canonical external KMS versions"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			oldKey, newKey := filepath.Join(dir, "old.key"), filepath.Join(dir, "new.key")
			require.NoError(t, os.WriteFile(oldKey, []byte("old"), tc.mode))
			require.NoError(t, os.WriteFile(newKey, []byte("new"), 0o600))
			oldExport, newExport, publicKey := filepath.Join(dir, "old-export.json"), filepath.Join(dir, "new-export.json"), filepath.Join(dir, "kms-public.pem")
			require.NoError(t, os.WriteFile(oldExport, []byte("old-export"), 0o600))
			require.NoError(t, os.WriteFile(newExport, []byte("new-export"), 0o600))
			require.NoError(t, os.WriteFile(publicKey, []byte("public-key"), 0o644))
			marker, command := filepath.Join(dir, "called"), filepath.Join(dir, "command")
			writeTrafficExecutable(t, command, "#!/usr/bin/env bash\nprintf called >\"$MARKER\"\n")
			replicas := "1"
			if tc.name == "duplicate endpoints" {
				replicas = "2"
			}
			output, err := runProductionScriptCommand(t, "request-jwt-key-rotation.sh", []string{
				"REQUEST_ID=change-2026-jwt-2", "INSTANCE=instance-a", "WORK_DIR=" + dir,
				"KUBEBRAIN_NAMESPACE=instance-a", "OLD_KEY_SOURCE=" + oldKey, "NEW_KEY_SOURCE=" + newKey,
				"OLD_KEY_VERSION_ID=" + tc.oldVersion, "NEW_KEY_VERSION_ID=" + tc.newVersion,
				"OLD_KEY_EXPORT_RECEIPT=" + oldExport, "NEW_KEY_EXPORT_RECEIPT=" + newExport, "KMS_RECEIPT_PUBLIC_KEY=" + publicKey, "KMS_EXPORT_VERIFIER=/bin/true",
				"SIGN_METHOD=HS256", "ENDPOINTS_JSON=" + tc.endpoints, "EXPECTED_REPLICAS=" + replicas, "JWT_TTL_SECONDS=300", "MAX_CLOCK_SKEW_SECONDS=2",
				"KUBE_CONTEXT=production", "KUBECTL=" + command, "OPERATIONCTL=" + command, "MARKER=" + marker,
			})
			require.Error(t, err)
			require.Contains(t, string(output), tc.want)
			require.NoFileExists(t, marker)
		})
	}
}

func TestRequestJWTKeyRotationRejectsUntrustedKMSExportBeforeKubernetes(t *testing.T) {
	dir := t.TempDir()
	oldKey, newKey := filepath.Join(dir, "old.key"), filepath.Join(dir, "new.key")
	oldExport, newExport, publicKey := filepath.Join(dir, "old-export.json"), filepath.Join(dir, "new-export.json"), filepath.Join(dir, "kms-public.pem")
	for path, contents := range map[string]string{oldKey: "old", newKey: "new", oldExport: "old-export", newExport: "new-export", publicKey: "public-key"} {
		mode := os.FileMode(0o600)
		if path == publicKey {
			mode = 0o644
		}
		require.NoError(t, os.WriteFile(path, []byte(contents), mode))
	}
	marker, kubectl, verifier := filepath.Join(dir, "kubernetes-called"), filepath.Join(dir, "kubectl"), filepath.Join(dir, "verifier")
	writeTrafficExecutable(t, kubectl, "#!/usr/bin/env bash\nprintf called >\"$MARKER\"\n")
	writeTrafficExecutable(t, verifier, "#!/usr/bin/env bash\necho 'signature rejected' >&2\nexit 1\n")
	output, err := runProductionScriptCommand(t, "request-jwt-key-rotation.sh", []string{
		"REQUEST_ID=change-2026-jwt-kms", "INSTANCE=instance-a", "WORK_DIR=" + dir, "KUBEBRAIN_NAMESPACE=instance-a",
		"OLD_KEY_SOURCE=" + oldKey, "NEW_KEY_SOURCE=" + newKey, "OLD_KEY_VERSION_ID=kms/prod/jwt/versions/41", "NEW_KEY_VERSION_ID=kms/prod/jwt/versions/42",
		"OLD_KEY_EXPORT_RECEIPT=" + oldExport, "NEW_KEY_EXPORT_RECEIPT=" + newExport, "KMS_RECEIPT_PUBLIC_KEY=" + publicKey,
		"SIGN_METHOD=HS256", `ENDPOINTS_JSON=["https://member-0:2379"]`, "EXPECTED_REPLICAS=1", "JWT_TTL_SECONDS=300", "MAX_CLOCK_SKEW_SECONDS=2",
		"KUBE_CONTEXT=production", "KUBECTL=" + kubectl, "OPERATIONCTL=" + kubectl, "KMS_EXPORT_VERIFIER=" + verifier, "MARKER=" + marker,
	})
	require.Error(t, err)
	require.Contains(t, string(output), "signature rejected")
	require.NoFileExists(t, marker)
}
