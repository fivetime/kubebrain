package main

import (
	"bytes"
	"encoding/pem"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestDefaultKubeconfigOnlyUsesExplicitEnvironment(t *testing.T) {
	t.Setenv("HOME", t.TempDir())
	t.Setenv("KUBECONFIG", "")
	require.Empty(t, defaultKubeconfig(),
		"an empty default must allow ServiceAccount in-cluster configuration")

	t.Setenv("KUBECONFIG", "/explicit/config")
	require.Equal(t, "/explicit/config", defaultKubeconfig())
}

func TestBrokerParametersUsesTLSBearerAndFencingIdentity(t *testing.T) {
	server := httptest.NewTLSServer(http.HandlerFunc(func(
		response http.ResponseWriter, request *http.Request,
	) {
		require.Equal(t, "/v1/parameters", request.URL.Path)
		require.Equal(t, "tenant-a", request.URL.Query().Get("namespace"))
		require.Equal(t, "backup-1", request.URL.Query().Get("name"))
		require.Equal(t, "worker-a", request.URL.Query().Get("owner"))
		require.Equal(t, "2", request.URL.Query().Get("attempt"))
		require.Equal(t, "Bearer projected-token", request.Header.Get("Authorization"))
		response.WriteHeader(http.StatusOK)
		_, _ = response.Write([]byte("{\"bound\":true}\n"))
	}))
	defer server.Close()

	dir := t.TempDir()
	tokenPath := filepath.Join(dir, "token")
	caPath := filepath.Join(dir, "ca.crt")
	require.NoError(t, os.WriteFile(tokenPath, []byte("projected-token\n"), 0o600))
	require.NoError(t, os.WriteFile(caPath, pem.EncodeToMemory(&pem.Block{
		Type: "CERTIFICATE", Bytes: server.Certificate().Raw,
	}), 0o600))
	parameters, err := brokerParameters(
		t.Context(), server.URL, tokenPath, caPath,
		"tenant-a", "backup-1", "worker-a", 2,
	)
	require.NoError(t, err)
	require.JSONEq(t, `{"bound":true}`, string(parameters))
}

func TestBrokerParametersRejectsInsecureEndpointAndNonSuccess(t *testing.T) {
	for _, endpoint := range []string{
		"http://parameters.example",
		"https://user@parameters.example",
		"https://parameters.example?debug=true",
		"https://parameters.example#fragment",
	} {
		t.Run(endpoint, func(t *testing.T) {
			_, err := brokerParameters(
				t.Context(), endpoint, "missing", "missing",
				"tenant-a", "backup-1", "worker-a", 1,
			)
			require.ErrorContains(t, err, "HTTPS origin")
		})
	}

	server := httptest.NewTLSServer(http.HandlerFunc(func(
		response http.ResponseWriter, _ *http.Request,
	) {
		http.Error(response, "denied", http.StatusForbidden)
	}))
	defer server.Close()
	dir := t.TempDir()
	tokenPath := filepath.Join(dir, "token")
	caPath := filepath.Join(dir, "ca.crt")
	require.NoError(t, os.WriteFile(tokenPath, []byte("token"), 0o600))
	require.NoError(t, os.WriteFile(caPath, pem.EncodeToMemory(&pem.Block{
		Type: "CERTIFICATE", Bytes: server.Certificate().Raw,
	}), 0o600))
	_, err := brokerParameters(
		t.Context(), server.URL, tokenPath, caPath,
		"tenant-a", "backup-1", "worker-a", 1,
	)
	require.ErrorContains(t, err, "HTTP 403")
	require.NotContains(t, err.Error(), "denied")
}

func TestBrokerParametersRejectsOversizedResponse(t *testing.T) {
	server := httptest.NewTLSServer(http.HandlerFunc(func(
		response http.ResponseWriter, _ *http.Request,
	) {
		response.WriteHeader(http.StatusOK)
		_, _ = response.Write(bytes.Repeat([]byte("x"), maxBrokerParametersBytes+1))
	}))
	defer server.Close()
	dir := t.TempDir()
	tokenPath := filepath.Join(dir, "token")
	caPath := filepath.Join(dir, "ca.crt")
	require.NoError(t, os.WriteFile(tokenPath, []byte("token"), 0o600))
	require.NoError(t, os.WriteFile(caPath, pem.EncodeToMemory(&pem.Block{
		Type: "CERTIFICATE", Bytes: server.Certificate().Raw,
	}), 0o600))
	_, err := brokerParameters(
		t.Context(), server.URL, tokenPath, caPath,
		"tenant-a", "backup-1", "worker-a", 1,
	)
	require.ErrorContains(t, err, "response exceeds")
}

func TestClientConfigFallsBackToStandardLocalKubeconfig(t *testing.T) {
	t.Setenv("KUBERNETES_SERVICE_HOST", "")
	t.Setenv("KUBERNETES_SERVICE_PORT", "")
	dir := t.TempDir()
	path := dir + "/config"
	require.NoError(t, os.WriteFile(path, []byte(`apiVersion: v1
kind: Config
current-context: test
clusters:
- name: test
  cluster:
    server: https://127.0.0.1:6443
contexts:
- name: test
  context:
    cluster: test
    user: test
users:
- name: test
  user:
    token: test
`), 0o600))
	config, err := clientConfig(path, "")
	require.NoError(t, err)
	require.Equal(t, "https://127.0.0.1:6443", config.Host)
}
