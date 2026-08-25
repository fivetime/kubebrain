package main

import (
	"crypto/ed25519"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestVerifyRetirementBindsOldReplacementAndReadiness(t *testing.T) {
	publicKey, privateKey, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)
	now := time.Unix(2_000_000_000, 0)
	shaA, shaB, readiness := strings.Repeat("a", 64), strings.Repeat("b", 64), strings.Repeat("c", 64)
	want := expected{"old-2026q2", "old-uid", shaA, "new-2026q3", "new-uid", shaB, readiness}
	receipt := retirementReceipt{Format: "kubebrain.jwt-kms-lifecycle-credential-retirement.v1", State: "revoked", Principal: "identity-admin", RetiredCredentialID: want.retiredID, RetiredSecretUID: want.retiredUID, RetiredSecretDataSHA256: want.retiredSHA, ReplacementCredentialID: want.replacementID, ReplacementSecretUID: want.replacementUID, ReplacementSecretDataSHA256: want.replacementSHA, ReplacementReadinessSHA256: want.readinessSHA, RevokedAtUnix: now.Unix() - 1, ExpiresAtUnix: now.Unix() + 300}
	data := signRetirement(t, privateKey, receipt)
	require.NoError(t, verify(data, publicKey, want, now))
	require.NoError(t, verify(data, publicKey, want, now.Add(24*time.Hour)), "a signed revoked state is permanent after its bounded issuance window")
	artifactPath := filepath.Join(t.TempDir(), "artifact.json")
	require.NoError(t, writeArtifact(artifactPath, data, want))
	var artifact retirementArtifact
	require.NoError(t, json.Unmarshal(mustRead(t, artifactPath), &artifact))
	require.Equal(t, "kubebrain.jwt-kms-lifecycle-credential-retirement-artifact.v1", artifact.Format)
	require.Equal(t, data, mustDecode(t, artifact.RetirementReceiptBase64))
	require.Error(t, writeArtifact(artifactPath, data, want))
	wrong := want
	wrong.replacementUID = "other-uid"
	require.ErrorContains(t, verify(data, publicKey, wrong, now), "binding")
	receipt.State = "disabled"
	require.ErrorContains(t, verify(signRetirement(t, privateKey, receipt), publicKey, want, now), "state")
}

func mustRead(t *testing.T, path string) []byte {
	t.Helper()
	data, err := os.ReadFile(path)
	require.NoError(t, err)
	return data
}

func mustDecode(t *testing.T, value string) []byte {
	t.Helper()
	data, err := base64.StdEncoding.Strict().DecodeString(value)
	require.NoError(t, err)
	return data
}

func TestVerifyRetirementRejectsSameCredentialAndLongWindow(t *testing.T) {
	publicKey, privateKey, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)
	now := time.Unix(2_000_000_000, 0)
	sha := strings.Repeat("d", 64)
	want := expected{"same", "old-uid", sha, "same", "new-uid", sha, sha}
	require.ErrorContains(t, verify([]byte(`{}`), publicKey, want, now), "expected")
	want.replacementID = "replacement"
	receipt := retirementReceipt{Format: "kubebrain.jwt-kms-lifecycle-credential-retirement.v1", State: "revoked", Principal: "admin", RetiredCredentialID: want.retiredID, RetiredSecretUID: want.retiredUID, RetiredSecretDataSHA256: want.retiredSHA, ReplacementCredentialID: want.replacementID, ReplacementSecretUID: want.replacementUID, ReplacementSecretDataSHA256: want.replacementSHA, ReplacementReadinessSHA256: want.readinessSHA, RevokedAtUnix: now.Unix() - 1, ExpiresAtUnix: now.Unix() + 901}
	require.ErrorContains(t, verify(signRetirement(t, privateKey, receipt), publicKey, want, now), "window")
}

func signRetirement(t *testing.T, key ed25519.PrivateKey, receipt retirementReceipt) []byte {
	t.Helper()
	payload, err := json.Marshal(receipt)
	require.NoError(t, err)
	data, err := json.Marshal(envelope{Format: "kubebrain.jwt-kms-lifecycle-credential-retirement-envelope.v1", Payload: base64.StdEncoding.EncodeToString(payload), Signature: base64.StdEncoding.EncodeToString(ed25519.Sign(key, payload))})
	require.NoError(t, err)
	return data
}
