package production_test

import (
	"encoding/base64"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

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

func TestActivateJWTKMSLifecycleCredentialRequiresBoundReadiness(t *testing.T) {
	dir := t.TempDir()
	token, ca, public, private := filepath.Join(dir, "token"), filepath.Join(dir, "ca"), filepath.Join(dir, "public"), filepath.Join(dir, "private")
	require.NoError(t, os.WriteFile(token, []byte("token\n"), 0o600))
	for _, command := range [][]string{
		{"req", "-x509", "-newkey", "rsa:2048", "-nodes", "-subj", "/CN=kms.test", "-keyout", filepath.Join(dir, "ca-key"), "-out", ca, "-days", "1"},
		{"genpkey", "-algorithm", "Ed25519", "-out", private},
		{"pkey", "-in", private, "-pubout", "-out", public},
	} {
		output, err := runProductionCommand(t, "openssl", command, nil)
		require.NoError(t, err, string(output))
	}
	require.NoError(t, os.Chmod(ca, 0o600))
	require.NoError(t, os.Chmod(public, 0o600))
	data := map[string]string{
		"endpoint":               base64.StdEncoding.EncodeToString([]byte("https://kms.example")),
		"lifecycle-token":        base64.StdEncoding.EncodeToString(mustRead(t, token)),
		"ca.crt":                 base64.StdEncoding.EncodeToString(mustRead(t, ca)),
		"receipt-public-key.pem": base64.StdEncoding.EncodeToString(mustRead(t, public)),
	}
	secretObject := map[string]any{"metadata": map[string]any{"name": "kubebrain-jwt-kms-lifecycle-2026q3", "namespace": "kubebrain-kms-lifecycle", "uid": "secret-uid", "resourceVersion": "10", "labels": map[string]string{"dbaas.kubebrain.io/credential-kind": "jwt-kms-lifecycle"}}, "immutable": true, "type": "Opaque", "data": data}
	secretJSON, err := json.Marshal(secretObject)
	require.NoError(t, err)
	secretState, activeState := filepath.Join(dir, "secret.json"), filepath.Join(dir, "active.json")
	require.NoError(t, os.WriteFile(secretState, secretJSON, 0o600))
	kubectl, probe, probeLog := filepath.Join(dir, "kubectl"), filepath.Join(dir, "probe"), filepath.Join(dir, "probe.log")
	writeTrafficExecutable(t, kubectl, `#!/usr/bin/env bash
set -euo pipefail
args="$*"
if [[ "$args" == *"get validatingadmissionpolicy "* ]]; then printf '{"metadata":{"generation":1},"status":{"observedGeneration":1,"typeChecking":{"expressionWarnings":[]}}}\n'; exit 0; fi
if [[ "$args" == *"get validatingadmissionpolicybinding kubebrain-jwt-kms-lifecycle-credential"* ]]; then printf '{"spec":{"policyName":"kubebrain-jwt-kms-lifecycle-credential","validationActions":["Deny"]}}\n'; exit 0; fi
if [[ "$args" == *"get validatingadmissionpolicybinding kubebrain-jwt-kms-lifecycle-active"* ]]; then printf '{"spec":{"policyName":"kubebrain-jwt-kms-lifecycle-active","validationActions":["Deny"]}}\n'; exit 0; fi
if [[ "$args" == *"auth can-i"* ]]; then printf 'no\n'; exit 0; fi
if [[ "$args" == *"get secret kubebrain-jwt-kms-lifecycle-2026q3"* ]]; then cat "$SECRET_STATE"; exit 0; fi
if [[ "$args" == *"get configmap kubebrain-jwt-kms-lifecycle-active"* ]]; then exit 1; fi
if [[ "$args" == *"create -f -"* ]]; then tee "$ACTIVE_STATE" >/dev/null; exit 0; fi
exit 1
`)
	writeTrafficExecutable(t, probe, `#!/usr/bin/env bash
set -euo pipefail
printf '%s\n' "$*" >"$PROBE_LOG"
output=""; while [[ $# -gt 0 ]]; do if [[ "$1" == --receipt-output ]]; then output="$2"; shift 2; else shift; fi; done
printf '{"format":"kubebrain.jwt-kms-lifecycle-credential-readiness-envelope.v1","payload":"%s","signature":"c2ln"}\n' "$READY_PAYLOAD_BASE64" >"$output"; chmod 600 "$output"
if [[ "${MUTATE_SECRET_AFTER_PROBE:-}" == true ]]; then jq '.metadata.uid="replacement-uid"' "$SECRET_STATE" >"$SECRET_STATE.tmp"; mv "$SECRET_STATE.tmp" "$SECRET_STATE"; fi
`)
	payload, err := json.Marshal(map[string]any{"expires_at_unix": time.Now().Add(10 * time.Minute).Unix()})
	require.NoError(t, err)
	readiness := filepath.Join(dir, "readiness.json")
	env := []string{"KUBE_CONTEXT=test", "KUBECTL=" + kubectl, "CREDENTIAL_ID=2026q3", "KMS_PROVIDER_ENDPOINT=https://kms.example", "KMS_LIFECYCLE_TOKEN_FILE=" + token, "KMS_PROVIDER_CA_FILE=" + ca, "KMS_RECEIPT_PUBLIC_KEY=" + public, "KMS_LIFECYCLE_CREDENTIAL_PROBE=" + probe, "READINESS_RECEIPT_OUTPUT=" + readiness, "SECRET_STATE=" + secretState, "ACTIVE_STATE=" + activeState, "PROBE_LOG=" + probeLog, "READY_PAYLOAD_BASE64=" + base64.StdEncoding.EncodeToString(payload)}
	output, err := runProductionCommand(t, "bash", []string{"apply-jwt-kms-lifecycle-credential.sh", "--activate"}, env)
	require.NoError(t, err, string(output))
	require.Contains(t, string(mustRead(t, probeLog)), "--credential-secret-uid secret-uid")
	var active map[string]any
	require.NoError(t, json.Unmarshal(mustRead(t, activeState), &active))
	require.Equal(t, "2026q3", active["data"].(map[string]any)["credential-id"])
	require.Equal(t, "secret-uid", active["data"].(map[string]any)["secret-uid"])
	require.NoError(t, os.Remove(activeState))
	require.NoError(t, os.Remove(readiness))
	require.NoError(t, os.WriteFile(secretState, secretJSON, 0o600))
	_, err = runProductionCommand(t, "bash", []string{"apply-jwt-kms-lifecycle-credential.sh", "--activate"}, append(env, "MUTATE_SECRET_AFTER_PROBE=true"))
	require.Error(t, err)
	require.NoFileExists(t, activeState, "Secret replacement after probe must prevent activation")
}

func TestProvisionJWTKMSLifecycleCredentialIsVersionedAndImmutable(t *testing.T) {
	dir := t.TempDir()
	state, kubectl := filepath.Join(dir, "secret.json"), filepath.Join(dir, "kubectl")
	writeTrafficExecutable(t, kubectl, `#!/usr/bin/env bash
set -euo pipefail
args="$*"
if [[ "$args" == *"get validatingadmissionpolicy kubebrain-jwt-kms-lifecycle-credential"* || "$args" == *"get validatingadmissionpolicy kubebrain-jwt-kms-lifecycle-active"* ]]; then printf '{"metadata":{"generation":1},"status":{"observedGeneration":1,"typeChecking":{"expressionWarnings":[]}}}\n'; exit 0; fi
if [[ "$args" == *"get validatingadmissionpolicybinding kubebrain-jwt-kms-lifecycle-credential"* ]]; then printf '{"spec":{"policyName":"kubebrain-jwt-kms-lifecycle-credential","validationActions":["Deny"]}}\n'; exit 0; fi
if [[ "$args" == *"get validatingadmissionpolicybinding kubebrain-jwt-kms-lifecycle-active"* ]]; then printf '{"spec":{"policyName":"kubebrain-jwt-kms-lifecycle-active","validationActions":["Deny"]}}\n'; exit 0; fi
if [[ "$args" == *"auth can-i"* ]]; then printf 'no\n'; exit 0; fi
if [[ "$args" == *"get secret kubebrain-jwt-kms-lifecycle-2026q3"* ]]; then [[ -f "$SECRET_STATE" ]] || exit 1; cat "$SECRET_STATE"; exit 0; fi
if [[ "$args" == *"create secret generic kubebrain-jwt-kms-lifecycle-2026q3"* ]]; then
  printf '{"apiVersion":"v1","kind":"Secret","metadata":{"name":"kubebrain-jwt-kms-lifecycle-2026q3","namespace":"kubebrain-kms-lifecycle","uid":"secret-uid","resourceVersion":"10"},"type":"Opaque","data":{"endpoint":"aHR0cHM6Ly9rbXMuZXhhbXBsZQ==","lifecycle-token":"dG9rZW4=","ca.crt":"Y2E=","receipt-public-key.pem":"cHVibGlj"}}\n'; exit 0
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
