package main

import (
	"bytes"
	"encoding/pem"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"

	"github.com/kubewharf/kubebrain/hack/production/internal/testcommand"
	"github.com/stretchr/testify/require"
	"k8s.io/client-go/rest"
)

func TestDefaultKubeconfigIgnoresAmbientEnvironment(t *testing.T) {
	t.Setenv("HOME", t.TempDir())
	t.Setenv("KUBECONFIG", "/ambient/config")
	require.Empty(t, defaultKubeconfig(),
		"ambient KUBECONFIG must not preempt ServiceAccount in-cluster configuration")
}

func TestMainRejectsInvalidNamespaceBeforeKubeconfig(t *testing.T) {
	output, err := testcommand.GoRun(t, ".",
		"--namespace", "ops.ns",
		"--action", "get",
		"--name", "backup-1",
	)
	require.Error(t, err)
	require.Contains(t, string(output), "invalid namespace ops.ns")

	output, err = testcommand.GoRun(t, ".",
		"--namespace", "ops",
		"--namespace-inventory-configmap", "inventory",
		"--namespace-inventory-namespace", "ops.ns",
		"--action", "claim",
		"--owner", "worker-a",
		"--type", "Backup",
	)
	require.Error(t, err)
	require.Contains(t, string(output), "invalid namespace ops.ns")

	output, err = testcommand.GoRun(t, ".",
		"--namespace", "ops",
		"--namespace-inventory-configmap", "inventory/name",
		"--action", "claim",
		"--owner", "worker-a",
		"--type", "Backup",
	)
	require.Error(t, err)
	require.Contains(t, string(output), "inventory configmap")

	output, err = testcommand.GoRun(t, ".",
		"--namespace", "ops",
		"--namespace-inventory-configmap", "inventory",
		"--namespace-inventory-key", "namespaces/json",
		"--action", "claim",
		"--owner", "worker-a",
		"--type", "Backup",
	)
	require.Error(t, err)
	require.Contains(t, string(output), "inventory data key")
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
		response.Header().Set("Content-Type", "application/json")
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

func TestBrokerParametersRejectsNonJSONSuccessResponse(t *testing.T) {
	server := httptest.NewTLSServer(http.HandlerFunc(func(
		response http.ResponseWriter, _ *http.Request,
	) {
		response.Header().Set("Content-Type", "text/html")
		response.WriteHeader(http.StatusOK)
		_, _ = response.Write([]byte("<html>not parameters</html>"))
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
	require.ErrorContains(t, err, "non-JSON")
	require.NotContains(t, err.Error(), "not parameters")
}

func TestBrokerParametersRejectsOversizedResponse(t *testing.T) {
	server := httptest.NewTLSServer(http.HandlerFunc(func(
		response http.ResponseWriter, _ *http.Request,
	) {
		response.Header().Set("Content-Type", "application/json")
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

func TestBrokerParametersRejectsInvalidTokenFile(t *testing.T) {
	dir := t.TempDir()
	for _, tc := range []struct {
		name  string
		token []byte
		want  string
	}{
		{name: "empty", token: []byte("\n"), want: "empty or too large"},
		{name: "oversized", token: bytes.Repeat([]byte("x"), maxBrokerTokenBytes+2), want: "token exceeds"},
		{name: "leading space", token: []byte(" token\n"), want: "malformed"},
		{name: "trailing space", token: []byte("token \n"), want: "malformed"},
		{name: "embedded space", token: []byte("projected token\n"), want: "malformed"},
		{name: "embedded newline", token: []byte("projected\ntoken\n"), want: "malformed"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			tokenPath := filepath.Join(dir, tc.name+".token")
			require.NoError(t, os.WriteFile(tokenPath, tc.token, 0o600))
			_, err := brokerParameters(
				t.Context(), "https://parameters.example", tokenPath, filepath.Join(dir, "missing-ca.crt"),
				"tenant-a", "backup-1", "worker-a", 1,
			)
			require.ErrorContains(t, err, tc.want)
		})
	}
}

func TestBrokerParametersRejectsOversizedCAFile(t *testing.T) {
	dir := t.TempDir()
	tokenPath := filepath.Join(dir, "token")
	caPath := filepath.Join(dir, "ca.crt")
	require.NoError(t, os.WriteFile(tokenPath, []byte("token"), 0o600))
	require.NoError(t, os.WriteFile(caPath, bytes.Repeat([]byte("x"), maxBrokerCABytes+1), 0o600))
	_, err := brokerParameters(
		t.Context(), "https://parameters.example", tokenPath, caPath,
		"tenant-a", "backup-1", "worker-a", 1,
	)
	require.ErrorContains(t, err, "CA exceeds")
}

func TestClientConfigPrefersInClusterWhenKubeconfigIsEmpty(t *testing.T) {
	t.Setenv("KUBECONFIG", writeKubeconfig(t, "https://ambient.example.invalid"))
	original := inClusterConfig
	t.Cleanup(func() { inClusterConfig = original })
	inClusterConfig = func() (*rest.Config, error) {
		return &rest.Config{Host: "https://kubernetes.default.svc"}, nil
	}

	config, err := clientConfig("", "")
	require.NoError(t, err)
	require.Equal(t, "https://kubernetes.default.svc", config.Host)
}

func TestClientConfigSkipsInClusterWhenExplicitKubeconfigIsProvided(t *testing.T) {
	original := inClusterConfig
	t.Cleanup(func() { inClusterConfig = original })
	called := false
	inClusterConfig = func() (*rest.Config, error) {
		called = true
		return nil, errors.New("in-cluster should not be used")
	}

	kubeconfig := writeKubeconfig(t, "https://api.example.invalid")

	config, err := clientConfig(kubeconfig, "")
	require.NoError(t, err)
	require.False(t, called)
	require.Equal(t, "https://api.example.invalid", config.Host)
}

func TestClientConfigFallsBackToStandardLocalKubeconfigOutsideCluster(t *testing.T) {
	t.Setenv("KUBECONFIG", writeKubeconfig(t, "https://local.example.invalid"))
	original := inClusterConfig
	t.Cleanup(func() { inClusterConfig = original })
	inClusterConfig = func() (*rest.Config, error) {
		return nil, errors.New("not running in a Kubernetes pod")
	}

	config, err := clientConfig("", "")
	require.NoError(t, err)
	require.Equal(t, "https://local.example.invalid", config.Host)
}

func writeKubeconfig(t *testing.T, server string) string {
	t.Helper()
	kubeconfig := filepath.Join(t.TempDir(), "config")
	require.NoError(t, os.WriteFile(kubeconfig, []byte(`apiVersion: v1
kind: Config
clusters:
- name: test
  cluster:
    server: `+server+`
contexts:
- name: test
  context:
    cluster: test
    user: test
current-context: test
users:
- name: test
  user:
    token: token
`), 0o600))
	return kubeconfig
}
