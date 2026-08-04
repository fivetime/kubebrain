package compat

import (
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRangeStreamCompactionDifferentialRunnerFailsClosed(t *testing.T) {
	script, err := os.ReadFile("run-rangestream-compaction-differential.sh")
	require.NoError(t, err)
	require.Contains(t, string(script), "ALLOW_DESTRUCTIVE_RANGESTREAM_COMPACTION")
	require.Contains(t, string(script), "data revision must be 1")
	require.Contains(t, string(script), "auth revision must be 1")
	require.Contains(t, string(script), "assert_clean_endpoint postflight")
	require.Contains(t, string(script), "-run '^TestRangeStreamPartialCompactionDifferential$'")
}

func TestRangeStreamCompactionDifferentialRunnerRejectsInvalidApprovalBeforeDependencies(t *testing.T) {
	output, err := runCompatScriptCommand(t, "run-rangestream-compaction-differential.sh", []string{
		"PATH=" + t.TempDir() + ":/usr/bin:/bin",
		"ALLOW_DESTRUCTIVE_RANGESTREAM_COMPACTION=maybe",
	})
	require.Error(t, err)
	require.Contains(t, string(output), "ALLOW_DESTRUCTIVE_RANGESTREAM_COMPACTION must be true or false")
	require.NotContains(t, string(output), "missing required command")
}

func TestRangeStreamCompactionDifferentialRunnerRequiresDisposableEndpoint(t *testing.T) {
	output, err := runCompatScriptCommand(t, "run-rangestream-compaction-differential.sh", []string{
		"PATH=" + t.TempDir() + ":/usr/bin:/bin",
		"ALLOW_DESTRUCTIVE_RANGESTREAM_COMPACTION=true",
	})
	require.Error(t, err)
	require.Contains(t, string(output), "set KUBEBRAIN_RANGESTREAM_COMPACTION_ENDPOINT")
}

func TestRangeStreamCompactionDifferentialRunnerRequiresDestructiveApproval(t *testing.T) {
	output, err := runCompatScriptCommand(t, "run-rangestream-compaction-differential.sh", []string{
		"PATH=" + t.TempDir() + ":/usr/bin:/bin",
		"KUBEBRAIN_RANGESTREAM_COMPACTION_ENDPOINT=127.0.0.1:24379",
	})
	require.Error(t, err)
	require.Contains(t, string(output), "refusing destructive RangeStream compaction differential")
}
