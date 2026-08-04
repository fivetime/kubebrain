package compat

import (
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestMakeMirrorDifferentialRunnerFailsClosed(t *testing.T) {
	script, err := os.ReadFile("run-make-mirror-differential.sh")
	require.NoError(t, err)
	require.Contains(t, string(script), "ALLOW_DESTRUCTIVE_MIRROR_DIFFERENTIAL")
	require.Contains(t, string(script), "auth revision must be 1")
	require.Contains(t, string(script), "assert_clean_endpoint postflight")
	require.Contains(t, string(script), "TestMakeMirrorAuthenticatedBidirectionalDifferential")
	require.Contains(t, string(script), "TestMakeMirrorRevisionAndCompactionDifferential")
}

func TestMakeMirrorDifferentialRunnerRejectsInvalidApprovalBeforeDependencies(t *testing.T) {
	output, err := runCompatScriptCommand(t, "run-make-mirror-differential.sh", []string{
		"PATH=" + t.TempDir() + ":/usr/bin:/bin",
		"ALLOW_DESTRUCTIVE_MIRROR_DIFFERENTIAL=maybe",
	})
	require.Error(t, err)
	require.Contains(t, string(output), "ALLOW_DESTRUCTIVE_MIRROR_DIFFERENTIAL must be true or false")
	require.NotContains(t, string(output), "missing required command")
}

func TestMakeMirrorDifferentialRunnerRequiresDisposableEndpoint(t *testing.T) {
	output, err := runCompatScriptCommand(t, "run-make-mirror-differential.sh", []string{
		"PATH=" + t.TempDir() + ":/usr/bin:/bin",
		"ALLOW_DESTRUCTIVE_MIRROR_DIFFERENTIAL=true",
	})
	require.Error(t, err)
	require.Contains(t, string(output), "set KUBEBRAIN_MIRROR_ETCD_ENDPOINT")
}

func TestMakeMirrorDifferentialRunnerRequiresDestructiveApproval(t *testing.T) {
	output, err := runCompatScriptCommand(t, "run-make-mirror-differential.sh", []string{
		"PATH=" + t.TempDir() + ":/usr/bin:/bin",
		"KUBEBRAIN_MIRROR_ETCD_ENDPOINT=127.0.0.1:24379",
	})
	require.Error(t, err)
	require.Contains(t, string(output), "refusing destructive make-mirror differential")
}
