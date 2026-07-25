package main

import (
	"context"
	"crypto/tls"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/kubewharf/kubebrain/hack/production/internal/testcommand"
	"github.com/stretchr/testify/require"
	"k8s.io/client-go/rest"
)

type fakeReadyCertificate struct {
	err error
}

func (c fakeReadyCertificate) ValidAt(time.Time) error {
	return c.err
}

type fakeReadyDependency struct {
	err error
}

func (d fakeReadyDependency) Ready(context.Context) error {
	return d.err
}

type countingReadyCertificate struct {
	calls int
}

func (c *countingReadyCertificate) ValidAt(time.Time) error {
	c.calls++
	return nil
}

type countingReadyDependency struct {
	calls int
}

func (d *countingReadyDependency) Ready(context.Context) error {
	d.calls++
	return nil
}

type fakeServerCertificate struct{}

func (fakeServerCertificate) GetCertificate(*tls.ClientHelloInfo) (*tls.Certificate, error) {
	return &tls.Certificate{}, nil
}

func TestMainRejectsInvalidNamespaceBeforeKubeconfig(t *testing.T) {
	output, err := testcommand.GoRun(t, ".",
		"--namespace", "ops.ns",
		"--tls-cert-file", "cert.pem",
		"--tls-key-file", "key.pem",
	)
	require.Error(t, err)
	require.Contains(t, string(output), "invalid namespace ops.ns")
}

func TestNewHTTPServerUsesHardenedTLSAndTimeouts(t *testing.T) {
	handler := http.NewServeMux()
	server := newHTTPServer("127.0.0.1:8443", handler, fakeServerCertificate{})

	require.Equal(t, "127.0.0.1:8443", server.Addr)
	require.Same(t, handler, server.Handler)
	require.Equal(t, 5*time.Second, server.ReadHeaderTimeout)
	require.Equal(t, 15*time.Second, server.ReadTimeout)
	require.Equal(t, 15*time.Second, server.WriteTimeout)
	require.Equal(t, time.Minute, server.IdleTimeout)
	require.Equal(t, 16<<10, server.MaxHeaderBytes)
	require.NotNil(t, server.TLSConfig)
	require.Equal(t, uint16(tls.VersionTLS12), server.TLSConfig.MinVersion)
	certificate, err := server.TLSConfig.GetCertificate(nil)
	require.NoError(t, err)
	require.NotNil(t, certificate)
}

func TestReadyzHandlerSetsNoStoreHeaders(t *testing.T) {
	for _, tc := range []struct {
		name          string
		certErr       error
		dependencyErr error
		status        int
	}{
		{name: "ready", status: http.StatusNoContent},
		{name: "certificate not ready", certErr: errors.New("expired"), status: http.StatusServiceUnavailable},
		{name: "dependency not ready", dependencyErr: errors.New("unavailable"), status: http.StatusServiceUnavailable},
	} {
		t.Run(tc.name, func(t *testing.T) {
			response := httptest.NewRecorder()
			readyzHandler(
				fakeReadyCertificate{err: tc.certErr},
				fakeReadyDependency{err: tc.dependencyErr},
			)(response, httptest.NewRequest(http.MethodGet, "/readyz", nil))

			require.Equal(t, tc.status, response.Code)
			require.Equal(t, "no-store", response.Header().Get("Cache-Control"))
			require.Equal(t, "nosniff", response.Header().Get("X-Content-Type-Options"))
		})
	}
}

func TestReadyzHandlerRejectsNonGETBeforeDependencyChecks(t *testing.T) {
	certificate := &countingReadyCertificate{}
	dependency := &countingReadyDependency{}
	response := httptest.NewRecorder()
	readyzHandler(certificate, dependency)(
		response, httptest.NewRequest(http.MethodPost, "/readyz", nil),
	)

	require.Equal(t, http.StatusMethodNotAllowed, response.Code)
	require.Equal(t, http.MethodGet, response.Header().Get("Allow"))
	require.Equal(t, "no-store", response.Header().Get("Cache-Control"))
	require.Equal(t, "nosniff", response.Header().Get("X-Content-Type-Options"))
	require.Zero(t, certificate.calls)
	require.Zero(t, dependency.calls)
}

func TestKubernetesConfigPrefersInClusterWhenKubeconfigIsEmpty(t *testing.T) {
	t.Setenv("KUBECONFIG", writeKubeconfig(t, "https://api.example.invalid"))
	original := inClusterConfig
	t.Cleanup(func() { inClusterConfig = original })
	inClusterConfig = func() (*rest.Config, error) {
		return &rest.Config{Host: "https://kubernetes.default.svc"}, nil
	}

	config, err := kubernetesConfig("")
	require.NoError(t, err)
	require.Equal(t, "https://kubernetes.default.svc", config.Host)
}

func TestKubernetesConfigSkipsInClusterWhenExplicitKubeconfigIsProvided(t *testing.T) {
	original := inClusterConfig
	t.Cleanup(func() { inClusterConfig = original })
	called := false
	inClusterConfig = func() (*rest.Config, error) {
		called = true
		return nil, errors.New("in-cluster should not be used")
	}

	config, err := kubernetesConfig(writeKubeconfig(t, "https://explicit.example.invalid"))
	require.NoError(t, err)
	require.False(t, called)
	require.Equal(t, "https://explicit.example.invalid", config.Host)
}

func TestKubernetesConfigFallsBackToStandardKubeconfigOutsideCluster(t *testing.T) {
	t.Setenv("KUBECONFIG", writeKubeconfig(t, "https://local.example.invalid"))
	original := inClusterConfig
	t.Cleanup(func() { inClusterConfig = original })
	inClusterConfig = func() (*rest.Config, error) {
		return nil, errors.New("not running in a Kubernetes pod")
	}

	config, err := kubernetesConfig("")
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
