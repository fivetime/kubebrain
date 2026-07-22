package main

import (
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"k8s.io/client-go/rest"
)

func TestDefaultKubeconfigOnlyUsesExplicitEnvironment(t *testing.T) {
	t.Setenv("HOME", t.TempDir())
	t.Setenv("KUBECONFIG", "")
	require.Empty(t, defaultKubeconfig(),
		"an empty default must allow ServiceAccount in-cluster configuration")

	t.Setenv("KUBECONFIG", "/explicit/config")
	require.Equal(t, "/explicit/config", defaultKubeconfig())
}

func TestClientConfigPrefersInClusterWhenKubeconfigIsEmpty(t *testing.T) {
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

	kubeconfig := filepath.Join(t.TempDir(), "config")
	require.NoError(t, os.WriteFile(kubeconfig, []byte(`apiVersion: v1
kind: Config
clusters:
- name: test
  cluster:
    server: https://api.example.invalid
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

	config, err := clientConfig(kubeconfig, "")
	require.NoError(t, err)
	require.False(t, called)
	require.Equal(t, "https://api.example.invalid", config.Host)
}
