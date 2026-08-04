package compat

import (
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestAuthDifferentialRunnerFailsClosed(t *testing.T) {
	script, err := os.ReadFile("run-auth-differential.sh")
	require.NoError(t, err)
	require.Contains(t, string(script), "ALLOW_DESTRUCTIVE_AUTH_DIFFERENTIAL")
	require.Contains(t, string(script), "disposable KubeBrain Auth endpoint is not empty")
	require.Contains(t, string(script), "already has authentication enabled")
	require.Contains(t, string(script), "auth revision must be 1")
	require.Contains(t, string(script), "still has users, roles, or leases")
	require.Contains(t, string(script), "-run '^TestAuthDifferentialAgainstEtcd$'")
}

func TestAuthDifferentialRunnerRejectsInvalidApprovalBeforeDependencies(t *testing.T) {
	output, err := runCompatScriptCommand(t, "run-auth-differential.sh", []string{
		"PATH=" + t.TempDir() + ":/usr/bin:/bin",
		"ALLOW_DESTRUCTIVE_AUTH_DIFFERENTIAL=maybe",
	})
	require.Error(t, err)
	require.Contains(t, string(output), "ALLOW_DESTRUCTIVE_AUTH_DIFFERENTIAL must be true or false")
	require.NotContains(t, string(output), "missing required command")
}

func TestAuthDifferentialRunnerRequiresDisposableEndpoint(t *testing.T) {
	output, err := runCompatScriptCommand(t, "run-auth-differential.sh", []string{
		"PATH=" + t.TempDir() + ":/usr/bin:/bin",
		"ALLOW_DESTRUCTIVE_AUTH_DIFFERENTIAL=true",
	})
	require.Error(t, err)
	require.Contains(t, string(output), "set KUBEBRAIN_AUTH_DIFF_ENDPOINT")
	require.NotContains(t, string(output), "missing required command")
}

func TestAuthDifferentialRunnerRequiresDestructiveApproval(t *testing.T) {
	output, err := runCompatScriptCommand(t, "run-auth-differential.sh", []string{
		"PATH=" + t.TempDir() + ":/usr/bin:/bin",
		"KUBEBRAIN_AUTH_DIFF_ENDPOINT=127.0.0.1:23379",
	})
	require.Error(t, err)
	require.Contains(t, string(output), "refusing destructive Auth differential")
	require.NotContains(t, string(output), "missing required command")
}
