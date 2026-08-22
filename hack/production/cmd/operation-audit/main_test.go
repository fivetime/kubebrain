package main

import (
	"errors"
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
		"--name", "backup-1",
		"--output", filepath.Join(t.TempDir(), "artifact.json"),
	)
	require.Error(t, err)
	require.Contains(t, string(output), "invalid namespace ops.ns")
}

func TestMainRejectsInvalidReapContractBeforeKubeconfig(t *testing.T) {
	base := []string{
		"--action", "reap", "--namespace", "ops", "--name", "backup-1",
		"--object-store-id", "store-a", "--bucket", "audit", "--retention-mode", "GOVERNANCE",
		"--expected-uid", "uid-1", "--expected-resource-version", "7",
	}
	for _, tc := range []struct {
		name  string
		extra []string
		want  string
	}{
		{name: "equal retention", extra: []string{"--delete-after", "8760h"}, want: "delete-after < retention-duration"},
		{name: "zero timeout", extra: []string{"--timeout", "0s"}, want: "positive timeout"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			args := append([]string{"."}, base...)
			args = append(args, tc.extra...)
			output, err := testcommand.GoRun(t, args...)
			require.Error(t, err)
			require.Contains(t, string(output), tc.want)
		})
	}
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
