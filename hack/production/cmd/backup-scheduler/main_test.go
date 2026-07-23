package main

import (
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"k8s.io/client-go/rest"
)

func TestParseNamespaces(t *testing.T) {
	namespaces, err := parseNamespaces("default", "tenant-a, tenant-b")
	require.NoError(t, err)
	require.Equal(t, []string{"tenant-a", "tenant-b"}, namespaces)

	namespaces, err = parseNamespaces("single", "")
	require.NoError(t, err)
	require.Equal(t, []string{"single"}, namespaces)
}

func TestParseNamespacesRejectsUnsafeAllowlist(t *testing.T) {
	for _, value := range []string{"", "tenant-a,", "tenant-a,tenant-a", "Tenant_A", "tenant.a"} {
		_, err := parseNamespaces(value, value)
		require.Error(t, err, value)
	}
}

func TestMainRejectsInvalidNamespaceBeforeKubeconfig(t *testing.T) {
	command := exec.Command("go", "run", ".", "--namespace", "tenant.a", "--once")
	output, err := command.CombinedOutput()
	require.Error(t, err)
	require.Contains(t, string(output), "invalid namespace tenant.a")

	command = exec.Command("go", "run", ".",
		"--namespace-inventory-configmap", "inventory",
		"--namespace-inventory-namespace", "ops.ns",
		"--once",
	)
	output, err = command.CombinedOutput()
	require.Error(t, err)
	require.Contains(t, string(output), "invalid namespace ops.ns")

	command = exec.Command("go", "run", ".",
		"--namespace-inventory-configmap", "inventory/name",
		"--once",
	)
	output, err = command.CombinedOutput()
	require.Error(t, err)
	require.Contains(t, string(output), "inventory configmap")

	command = exec.Command("go", "run", ".",
		"--namespace-inventory-configmap", "inventory",
		"--namespace-inventory-key", "namespaces/json",
		"--once",
	)
	output, err = command.CombinedOutput()
	require.Error(t, err)
	require.Contains(t, string(output), "inventory data key")
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
