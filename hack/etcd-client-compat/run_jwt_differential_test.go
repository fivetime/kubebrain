package compat

import (
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestJWTDifferentialRunnerFailsClosed(t *testing.T) {
	script, err := os.ReadFile("run-jwt-differential.sh")
	require.NoError(t, err)
	require.Contains(t, string(script), "ALLOW_DESTRUCTIVE_JWT_DIFFERENTIAL")
	require.Contains(t, string(script), "auth revision must be 1")
	require.Contains(t, string(script), "still has keys, users, roles, or leases")
	require.Contains(t, string(script), "JWT HS256 key file is missing or empty")
	require.Contains(t, string(script), "GO_TEST_RACE")
	require.Contains(t, string(script), "find \"$data_dir\" -depth -delete")
	require.Contains(t, string(script), "-run '^TestJWTAuthDifferentialAgainstReferenceEtcd$'")
}

func TestJWTDifferentialRunnerRejectsInvalidRaceBeforeDependencies(t *testing.T) {
	output, err := runCompatScriptCommand(t, "run-jwt-differential.sh", []string{
		"PATH=" + t.TempDir() + ":/usr/bin:/bin",
		"GO_TEST_RACE=maybe",
	})
	require.Error(t, err)
	require.Contains(t, string(output), "GO_TEST_RACE must be true or false")
	require.NotContains(t, string(output), "missing required command")
}

func TestJWTDifferentialRunnerRejectsInvalidApprovalBeforeDependencies(t *testing.T) {
	output, err := runCompatScriptCommand(t, "run-jwt-differential.sh", []string{
		"PATH=" + t.TempDir() + ":/usr/bin:/bin",
		"ALLOW_DESTRUCTIVE_JWT_DIFFERENTIAL=maybe",
	})
	require.Error(t, err)
	require.Contains(t, string(output), "ALLOW_DESTRUCTIVE_JWT_DIFFERENTIAL must be true or false")
	require.NotContains(t, string(output), "missing required command")
}

func TestJWTDifferentialRunnerRequiresDisposableEndpoint(t *testing.T) {
	output, err := runCompatScriptCommand(t, "run-jwt-differential.sh", []string{
		"PATH=" + t.TempDir() + ":/usr/bin:/bin",
		"ALLOW_DESTRUCTIVE_JWT_DIFFERENTIAL=true",
	})
	require.Error(t, err)
	require.Contains(t, string(output), "set KUBEBRAIN_JWT_ETCD_ENDPOINT")
}

func TestJWTDifferentialRunnerRequiresDestructiveApproval(t *testing.T) {
	output, err := runCompatScriptCommand(t, "run-jwt-differential.sh", []string{
		"PATH=" + t.TempDir() + ":/usr/bin:/bin",
		"KUBEBRAIN_JWT_ETCD_ENDPOINT=127.0.0.1:24379",
	})
	require.Error(t, err)
	require.Contains(t, string(output), "refusing destructive JWT differential")
}
