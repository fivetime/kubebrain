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

func TestDurableOperationWorkersRejectHeartbeatAtLeaseBeforeClaim(t *testing.T) {
	dir := t.TempDir()
	called := filepath.Join(dir, "called")
	executable := filepath.Join(dir, "tool")
	require.NoError(t, os.WriteFile(executable, []byte("#!/usr/bin/env bash\nprintf called >\"$CALLED\"\nexit 0\n"), 0o755))

	for _, tc := range []struct {
		name   string
		script string
		env    []string
	}{
		{name: "backup", script: "run-backup-operation.sh", env: []string{"EXPORT_COMMAND=" + executable, "STATUS_COMMAND=" + executable, "OBJECT_COMMAND=" + executable}},
		{name: "backup deletion", script: "run-backup-deletion-operation.sh", env: []string{"OBJECT_COMMAND=" + executable}},
		{name: "certificate rotation", script: "run-certificate-rotation-operation.sh", env: []string{"ROTATION_COMMAND=" + executable, "PUBLISH_OVERLAP_COMMAND=" + executable, "PUBLISH_FINAL_COMMAND=" + executable}},
		{name: "destroy", script: "run-destroy-operation.sh", env: []string{"DESTROY_COMMAND=" + executable}},
		{name: "post-restore audit", script: "run-post-restore-audit-operation.sh", env: []string{"AUDIT_COMMAND=" + executable}},
		{name: "restore cutover", script: "run-restore-cutover-operation.sh", env: []string{"CUTOVER_COMMAND=" + executable}},
		{name: "TiKV recovery", script: "run-tikv-transaction-recovery-operation.sh", env: []string{"RECOVERY_COMMAND=" + executable, "WORK_DIR=" + dir}},
		{name: "TiKV repair", script: "run-tikv-transaction-repair-operation.sh", env: []string{"REPAIR_COMMAND=" + executable, "WORK_DIR=" + dir}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			require.NoError(t, os.RemoveAll(called))
			env := []string{
				"WORKER_ID=worker-1", "LEASE_SECONDS=6", "HEARTBEAT_INTERVAL_SECONDS=6",
				"OPERATIONCTL=" + executable, "CALLED=" + called,
			}
			env = append(env, tc.env...)
			output, err := runProductionScriptCommand(t, tc.script, env)
			require.Error(t, err)
			require.Contains(t, string(output), "HEARTBEAT_INTERVAL_SECONDS must be positive and less than LEASE_SECONDS")
			require.NoFileExists(t, called, "invalid heartbeat configuration must fail before operationctl")
		})
	}
}
