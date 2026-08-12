package production_test

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestPhysicalOperationWorkersRejectZeroHeartbeatBeforeClaim(t *testing.T) {
	dir := t.TempDir()
	executable := filepath.Join(dir, "tool")
	require.NoError(t, os.WriteFile(executable, []byte("#!/usr/bin/env bash\nexit 0\n"), 0o755))

	for _, tc := range []struct {
		name      string
		script    string
		commandKV string
		claimErr  string
	}{
		{name: "cold snapshot", script: "run-cold-physical-snapshot-operation.sh", commandKV: "SNAPSHOT_COMMAND=" + executable, claimErr: "snapshot claim is incomplete"},
		{name: "cold restore", script: "run-cold-physical-restore-operation.sh", commandKV: "RESTORE_COMMAND=" + executable, claimErr: "restore claim is incomplete"},
		{name: "legacy remediation", script: "run-legacy-snapshot-remediation-operation.sh", commandKV: "REMEDIATION_COMMAND=" + executable, claimErr: "remediation claim is incomplete"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			output, err := runProductionScriptCommand(t, tc.script, []string{
				"WORKER_ID=worker-1",
				"LEASE_SECONDS=2",
				"HEARTBEAT_INTERVAL_SECONDS=0",
				"OPERATIONCTL=" + executable,
				tc.commandKV,
				"WORK_DIR=" + dir,
			})
			require.Error(t, err)
			require.Contains(t, string(output), "HEARTBEAT_INTERVAL_SECONDS must be positive")

			defaultOutput, defaultErr := runProductionScriptCommand(t, tc.script, []string{
				"WORKER_ID=worker-1",
				"LEASE_SECONDS=2",
				"OPERATIONCTL=" + executable,
				tc.commandKV,
				"WORK_DIR=" + dir,
			})
			require.Error(t, defaultErr)
			require.Contains(t, string(defaultOutput), tc.claimErr)
			require.NotContains(t, string(defaultOutput), "HEARTBEAT_INTERVAL_SECONDS")
		})
	}
}
