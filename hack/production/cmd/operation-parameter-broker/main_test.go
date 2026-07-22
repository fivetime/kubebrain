package main

import (
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"k8s.io/client-go/rest"
)

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
