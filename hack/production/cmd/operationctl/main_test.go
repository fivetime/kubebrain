package main

import (
	"os"
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
