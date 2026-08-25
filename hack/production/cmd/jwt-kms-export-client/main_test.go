package main

import (
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestExportUsesTLSBearerAndExactProviderContract(t *testing.T) {
	material := []byte("private-material")
	receipt := json.RawMessage(`{"format":"kubebrain.jwt-kms-export-envelope.v1","payload":"e30=","signature":"c2ln"}`)
	server := httptest.NewTLSServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		require.Equal(t, http.MethodPost, request.Method)
		require.Equal(t, "/v1/jwt-key-versions:export", request.URL.Path)
		require.Equal(t, "Bearer short-lived-token", request.Header.Get("Authorization"))
		require.Equal(t, "application/json", request.Header.Get("Content-Type"))
		var input exportRequest
		require.NoError(t, json.NewDecoder(request.Body).Decode(&input))
		require.Equal(t, exportRequest{Format: "kubebrain.jwt-kms-export-request.v1", RequestID: "change-1", Instance: "instance-a", VersionID: "kms/prod/jwt/versions/42"}, input)
		response.Header().Set("Content-Type", "application/json")
		require.NoError(t, json.NewEncoder(response).Encode(exportResponse{Format: "kubebrain.jwt-kms-export-response.v1", Material: base64.StdEncoding.EncodeToString(material), Receipt: receipt}))
	}))
	defer server.Close()
	dir := t.TempDir()
	tokenPath, caPath := filepath.Join(dir, "token"), filepath.Join(dir, "ca.pem")
	require.NoError(t, os.WriteFile(tokenPath, []byte("short-lived-token\n"), 0o600))
	certPool, err := x509.SystemCertPool()
	require.NoError(t, err)
	_ = certPool
	require.NoError(t, os.WriteFile(caPath, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: server.Certificate().Raw}), 0o644))
	gotMaterial, gotReceipt, err := export(t.Context(), server.URL, tokenPath, caPath, "change-1", "instance-a", "kms/prod/jwt/versions/42")
	require.NoError(t, err)
	require.Equal(t, material, gotMaterial)
	require.JSONEq(t, string(receipt), string(gotReceipt))
}

func TestExportFailsClosedOnTransportAndResponseDrift(t *testing.T) {
	for _, endpoint := range []string{"http://provider.example", "https://user@provider.example", "https://provider.example/path", "https://provider.example?debug=1"} {
		t.Run(endpoint, func(t *testing.T) {
			_, _, err := export(t.Context(), endpoint, "missing", "missing", "change-1", "instance-a", "kms/v/2")
			require.ErrorContains(t, err, "HTTPS origin")
		})
	}
	server := httptest.NewTLSServer(http.HandlerFunc(func(response http.ResponseWriter, _ *http.Request) {
		response.Header().Set("Content-Type", "text/plain")
		_, _ = response.Write([]byte("secret provider error"))
	}))
	defer server.Close()
	dir := t.TempDir()
	tokenPath, caPath := filepath.Join(dir, "token"), filepath.Join(dir, "ca.pem")
	require.NoError(t, os.WriteFile(tokenPath, []byte("token"), 0o600))
	require.NoError(t, os.WriteFile(caPath, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: server.Certificate().Raw}), 0o644))
	_, _, err := export(t.Context(), server.URL, tokenPath, caPath, "change-1", "instance-a", "kms/v/2")
	require.ErrorContains(t, err, "content type")
	require.NotContains(t, err.Error(), "secret provider error")
}

func TestPublishPairIsPrivateNoClobberAndAtomicOnSecondFailure(t *testing.T) {
	dir := t.TempDir()
	materialPath, receiptPath := filepath.Join(dir, "material"), filepath.Join(dir, "receipt")
	require.NoError(t, publishPair(materialPath, receiptPath, []byte("material"), []byte("receipt")))
	require.Equal(t, os.FileMode(0o600), mustMode(t, materialPath))
	require.Equal(t, os.FileMode(0o600), mustMode(t, receiptPath))
	require.Error(t, publishPair(materialPath, filepath.Join(dir, "other-receipt"), []byte("changed"), []byte("receipt")))
	require.Equal(t, "material", string(mustRead(t, materialPath)))

	material2 := filepath.Join(dir, "material-2")
	require.NoError(t, os.WriteFile(filepath.Join(dir, "occupied"), []byte("occupied"), 0o600))
	require.Error(t, publishPair(material2, filepath.Join(dir, "occupied"), []byte("material"), []byte("receipt")))
	require.NoFileExists(t, material2)
}

func mustMode(t *testing.T, path string) os.FileMode {
	t.Helper()
	info, err := os.Stat(path)
	require.NoError(t, err)
	return info.Mode().Perm()
}
func mustRead(t *testing.T, path string) []byte {
	t.Helper()
	data, err := os.ReadFile(path)
	require.NoError(t, err)
	return data
}
