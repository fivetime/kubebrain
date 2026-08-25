package production_test

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestApplyJWTKMSLifecycleCredentialVerify(t *testing.T) {
	output, err := runProductionScriptCommand(t, "apply-jwt-kms-lifecycle-credential.sh", []string{})
	require.Error(t, err)
	require.Contains(t, string(output), "Usage:")
	output, err = runProductionCommand(t, "bash", []string{"apply-jwt-kms-lifecycle-credential.sh", "--verify"}, nil)
	require.NoError(t, err, string(output))
	require.Contains(t, string(output), "isolated immutable")
}

func TestProvisionJWTKMSLifecycleCredentialIsVersionedAndImmutable(t *testing.T) {
	dir := t.TempDir()
	state, kubectl := filepath.Join(dir, "secret.json"), filepath.Join(dir, "kubectl")
	writeTrafficExecutable(t, kubectl, `#!/usr/bin/env bash
set -euo pipefail
args="$*"
if [[ "$args" == *"get validatingadmissionpolicy kubebrain-jwt-kms-lifecycle-credential"* ]]; then printf '{"metadata":{"generation":1},"status":{"observedGeneration":1,"typeChecking":{"expressionWarnings":[]}}}\n'; exit 0; fi
if [[ "$args" == *"get validatingadmissionpolicybinding kubebrain-jwt-kms-lifecycle-credential"* ]]; then printf '{"spec":{"policyName":"kubebrain-jwt-kms-lifecycle-credential","validationActions":["Deny"]}}\n'; exit 0; fi
if [[ "$args" == *"auth can-i"* ]]; then printf 'no\n'; exit 0; fi
if [[ "$args" == *"get secret kubebrain-jwt-kms-lifecycle-2026q3"* ]]; then [[ -f "$SECRET_STATE" ]] || exit 1; cat "$SECRET_STATE"; exit 0; fi
if [[ "$args" == *"create secret generic kubebrain-jwt-kms-lifecycle-2026q3"* ]]; then
  printf '{"apiVersion":"v1","kind":"Secret","metadata":{"name":"kubebrain-jwt-kms-lifecycle-2026q3","namespace":"kubebrain-kms-lifecycle"},"type":"Opaque","data":{"endpoint":"aHR0cHM6Ly9rbXMuZXhhbXBsZQ==","lifecycle-token":"dG9rZW4=","ca.crt":"Y2E=","receipt-public-key.pem":"cHVibGlj"}}\n'; exit 0
fi
if [[ "$args" == *"create -f -"* ]]; then tee "$SECRET_STATE" >/dev/null; exit 0; fi
exit 1
`)
	ca, public, private := filepath.Join(dir, "ca"), filepath.Join(dir, "public"), filepath.Join(dir, "private")
	output, err := runProductionCommand(t, "openssl", []string{"req", "-x509", "-newkey", "rsa:2048", "-nodes", "-subj", "/CN=kms.test", "-keyout", filepath.Join(dir, "ca-key"), "-out", ca, "-days", "1"}, nil)
	require.NoError(t, err, string(output))
	output, err = runProductionCommand(t, "openssl", []string{"genpkey", "-algorithm", "Ed25519", "-out", private}, nil)
	require.NoError(t, err, string(output))
	output, err = runProductionCommand(t, "openssl", []string{"pkey", "-in", private, "-pubout", "-out", public}, nil)
	require.NoError(t, err, string(output))
	require.NoError(t, os.Chmod(ca, 0o600))
	require.NoError(t, os.Chmod(public, 0o600))
	files := map[string]string{"token": "token\n"}
	env := []string{"KUBE_CONTEXT=test", "KUBECTL=" + kubectl, "SECRET_STATE=" + state, "CREDENTIAL_ID=2026q3", "KMS_PROVIDER_ENDPOINT=https://kms.example"}
	for name, contents := range files {
		path := filepath.Join(dir, name)
		require.NoError(t, os.WriteFile(path, []byte(contents), 0o600))
		switch name {
		case "token":
			env = append(env, "KMS_LIFECYCLE_TOKEN_FILE="+path)
		}
	}
	env = append(env, "KMS_PROVIDER_CA_FILE="+ca, "KMS_RECEIPT_PUBLIC_KEY="+public)
	output, err = runProductionCommand(t, "bash", []string{"apply-jwt-kms-lifecycle-credential.sh", "--provision"}, env)
	require.NoError(t, err, string(output))
	object := string(mustRead(t, state))
	require.Contains(t, object, `"immutable": true`)
	require.Contains(t, object, `"dbaas.kubebrain.io/credential-kind": "jwt-kms-lifecycle"`)
	_, err = runProductionCommand(t, "bash", []string{"apply-jwt-kms-lifecycle-credential.sh", "--provision"}, env)
	require.Error(t, err, "a credential version must never be overwritten")
}
