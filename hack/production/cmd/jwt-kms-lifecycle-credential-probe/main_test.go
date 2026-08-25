package main

import (
	"crypto/ed25519"
	"crypto/rand"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestProbeBindsCredentialIdentityScopesAndProviderSignature(t *testing.T) {
	publicKey, privateKey, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)
	now := time.Unix(2_000_000_000, 0)
	var endpoint string
	server := httptest.NewTLSServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		require.Equal(t, "/v1/jwt-key-versions:lifecycle-probe", request.URL.Path)
		require.Equal(t, "Bearer lifecycle-token", request.Header.Get("Authorization"))
		var input probeRequest
		require.NoError(t, json.NewDecoder(request.Body).Decode(&input))
		require.Equal(t, "credential-2026q3", input.CredentialID)
		payload, marshalErr := json.Marshal(readinessReceipt{Format: "kubebrain.jwt-kms-lifecycle-credential-readiness.v1", CredentialID: input.CredentialID, CredentialSecretUID: input.CredentialSecretUID, CredentialSecretDataSHA256: input.CredentialSecretDataSHA256, Endpoint: endpoint, Principal: "kms-role/lifecycle", Scopes: []string{"promote", "revoke"}, ObservedAtUnix: now.Unix() - 1, ExpiresAtUnix: now.Unix() + 300})
		require.NoError(t, marshalErr)
		receipt, marshalErr := json.Marshal(envelope{Format: "kubebrain.jwt-kms-lifecycle-credential-readiness-envelope.v1", Payload: base64.StdEncoding.EncodeToString(payload), Signature: base64.StdEncoding.EncodeToString(ed25519.Sign(privateKey, payload))})
		require.NoError(t, marshalErr)
		response.Header().Set("Content-Type", "application/json")
		require.NoError(t, json.NewEncoder(response).Encode(probeResponse{Format: "kubebrain.jwt-kms-lifecycle-credential-probe-response.v1", Receipt: receipt}))
	}))
	defer server.Close()
	endpoint = server.URL
	dir := t.TempDir()
	token, ca, key := filepath.Join(dir, "token"), filepath.Join(dir, "ca"), filepath.Join(dir, "key")
	require.NoError(t, os.WriteFile(token, []byte("lifecycle-token\n"), 0o600))
	require.NoError(t, os.WriteFile(ca, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: server.Certificate().Raw}), 0o644))
	encodedKey, err := x509.MarshalPKIXPublicKey(publicKey)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(key, pem.EncodeToMemory(&pem.Block{Type: "PUBLIC KEY", Bytes: encodedKey}), 0o600))
	sha := strings.Repeat("a", 64)
	receipt, err := run(t.Context(), endpoint, token, ca, key, "credential-2026q3", "secret-uid", sha, "", now)
	require.NoError(t, err)
	require.NoError(t, verify(receipt, publicKey, endpoint, "credential-2026q3", "secret-uid", sha, now))
	require.ErrorContains(t, verify(receipt, publicKey, endpoint, "credential-other", "secret-uid", sha, now), "binding")
}

func TestProbeRejectsScopeOrderExpiryAndNoClobber(t *testing.T) {
	publicKey, privateKey, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)
	now := time.Unix(2_000_000_000, 0)
	sha := strings.Repeat("b", 64)
	payload := readinessReceipt{Format: "kubebrain.jwt-kms-lifecycle-credential-readiness.v1", CredentialID: "credential-a", CredentialSecretUID: "uid-a", CredentialSecretDataSHA256: sha, Endpoint: "https://kms.example", Principal: "principal-a", Scopes: []string{"revoke", "promote"}, ObservedAtUnix: now.Unix() - 1, ExpiresAtUnix: now.Unix() + 300}
	receipt := signedReadiness(t, privateKey, payload)
	require.ErrorContains(t, verify(receipt, publicKey, payload.Endpoint, payload.CredentialID, payload.CredentialSecretUID, sha, now), "scope")
	payload.Scopes = []string{"promote", "revoke"}
	payload.ExpiresAtUnix = payload.ObservedAtUnix + 901
	require.ErrorContains(t, verify(signedReadiness(t, privateKey, payload), publicKey, payload.Endpoint, payload.CredentialID, payload.CredentialSecretUID, sha, now), "window")
	destination := filepath.Join(t.TempDir(), "receipt")
	require.NoError(t, writeNoClobber(destination, []byte("first")))
	require.Error(t, writeNoClobber(destination, []byte("second")))
	require.Equal(t, "first", string(mustRead(t, destination)))
}

func signedReadiness(t *testing.T, key ed25519.PrivateKey, receipt readinessReceipt) []byte {
	t.Helper()
	payload, err := json.Marshal(receipt)
	require.NoError(t, err)
	data, err := json.Marshal(envelope{Format: "kubebrain.jwt-kms-lifecycle-credential-readiness-envelope.v1", Payload: base64.StdEncoding.EncodeToString(payload), Signature: base64.StdEncoding.EncodeToString(ed25519.Sign(key, payload))})
	require.NoError(t, err)
	return data
}

func mustRead(t *testing.T, path string) []byte {
	t.Helper()
	data, err := os.ReadFile(path)
	require.NoError(t, err)
	return data
}
