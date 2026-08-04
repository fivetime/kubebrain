package compat

import (
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRestartPersistenceRunnerFailsClosed(t *testing.T) {
	script, err := os.ReadFile("../dev/restart-persistence-smoke.sh")
	require.NoError(t, err)
	contents := string(script)
	require.Contains(t, contents, "ALLOW_DESTRUCTIVE_FULL_RESTART")
	require.Contains(t, contents, "set KUBE_CONTEXT explicitly")
	require.Contains(t, contents, `kubectl --context "$KUBE_CONTEXT"`)
	require.Contains(t, contents, "KUBEBRAIN_RESTART_PODS")
	require.Contains(t, contents, "KUBEBRAIN_RESTART_PD_PODS")
	require.Contains(t, contents, "KUBEBRAIN_RESTART_TIKV_PODS")
	require.Contains(t, contents, "alarm disarm")
	require.Contains(t, contents, "restart endpoint leaked compat keys")
	require.Contains(t, contents, `tidbcluster/$TIDB_CLUSTER`)
	require.NotContains(t, contents, "KUBEBRAIN_RESTART_PERSISTENCE_COMMAND")
	require.NotContains(t, contents, `bash -c`)
}

func TestRestartPersistenceRunnerRejectsInvalidApprovalBeforeDependencies(t *testing.T) {
	output, err := runCompatCommandContext(t, t.Context(), "bash", []string{"../dev/restart-persistence-smoke.sh"}, []string{
		"PATH=" + t.TempDir(),
		"ALLOW_DESTRUCTIVE_FULL_RESTART=maybe",
	})
	require.Error(t, err)
	require.Contains(t, string(output), "ALLOW_DESTRUCTIVE_FULL_RESTART must be true or false")
	require.NotContains(t, string(output), "missing required command")
}

func TestRestartPersistenceRunnerRequiresExplicitContext(t *testing.T) {
	output, err := runCompatCommandContext(t, t.Context(), "bash", []string{"../dev/restart-persistence-smoke.sh"}, []string{
		"PATH=" + t.TempDir(),
		"ALLOW_DESTRUCTIVE_FULL_RESTART=true",
	})
	require.Error(t, err)
	require.Contains(t, string(output), "set KUBE_CONTEXT explicitly")
	require.NotContains(t, string(output), "missing required command")
}
