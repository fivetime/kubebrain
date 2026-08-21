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
		{name: "cold snapshot", script: "run-cold-physical-snapshot-operation.sh", env: []string{"SNAPSHOT_COMMAND=" + executable, "WORK_DIR=" + dir}},
		{name: "cold restore", script: "run-cold-physical-restore-operation.sh", env: []string{"RESTORE_COMMAND=" + executable, "WORK_DIR=" + dir}},
		{name: "legacy remediation", script: "run-legacy-snapshot-remediation-operation.sh", env: []string{"REMEDIATION_COMMAND=" + executable, "WORK_DIR=" + dir}},
		{name: "native PITR backup", script: "run-native-pitr-full-backup-operation.sh", env: []string{"BACKUP_COMMAND=" + executable, "BR_BINARY=" + executable, "WORK_DIR=" + dir}},
		{name: "native PITR restore", script: "run-native-pitr-full-restore-operation.sh", env: []string{"RESTORE_COMMAND=" + executable, "RECEIPT_VERIFY=" + executable, "BR_BINARY=" + executable, "WORK_DIR=" + dir, "INPUT_ROOT=" + dir}},
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

func TestHeartbeatOperationWorkersRejectNonCanonicalTimeBeforeClaim(t *testing.T) {
	dir := t.TempDir()
	called := filepath.Join(dir, "called")
	executable := filepath.Join(dir, "operationctl")
	require.NoError(t, os.WriteFile(executable, []byte("#!/usr/bin/env bash\nprintf called >\"$CALLED\"\nexit 0\n"), 0o755))

	for _, script := range []string{
		"run-backup-operation.sh",
		"run-backup-deletion-operation.sh",
		"run-certificate-rotation-operation.sh",
		"run-cold-physical-restore-operation.sh",
		"run-cold-physical-snapshot-operation.sh",
		"run-destroy-operation.sh",
		"run-legacy-snapshot-remediation-operation.sh",
		"run-native-pitr-full-backup-operation.sh",
		"run-native-pitr-full-restore-operation.sh",
		"run-post-restore-audit-operation.sh",
		"run-restore-cutover-operation.sh",
		"run-tikv-transaction-recovery-operation.sh",
		"run-tikv-transaction-repair-operation.sh",
	} {
		t.Run(script, func(t *testing.T) {
			toolEnv := []string{
				"WORK_DIR=" + dir,
				"EXPORT_COMMAND=" + executable, "STATUS_COMMAND=" + executable, "OBJECT_COMMAND=" + executable,
				"ROTATION_COMMAND=" + executable, "PUBLISH_OVERLAP_COMMAND=" + executable, "PUBLISH_FINAL_COMMAND=" + executable,
				"DESTROY_COMMAND=" + executable, "AUDIT_COMMAND=" + executable, "CUTOVER_COMMAND=" + executable,
				"SNAPSHOT_COMMAND=" + executable, "RESTORE_COMMAND=" + executable, "REMEDIATION_COMMAND=" + executable,
				"BACKUP_COMMAND=" + executable, "RECEIPT_VERIFY=" + executable, "BR_BINARY=" + executable,
				"RECOVERY_COMMAND=" + executable, "REPAIR_COMMAND=" + executable,
			}
			for _, invalid := range []string{"9223372036854775808", ".1", "01", "1.", "0.0000000001"} {
				require.NoError(t, os.RemoveAll(called))
				env := []string{
					"WORKER_ID=worker-1", "LEASE_SECONDS=9223372036854775807",
					"HEARTBEAT_INTERVAL_SECONDS=" + invalid,
					"OPERATIONCTL=" + executable, "CALLED=" + called,
				}
				env = append(env, toolEnv...)
				output, err := runProductionScriptCommand(t, script, env)
				require.Error(t, err)
				require.Contains(t, string(output), "canonical positive decimal int64")
				require.NoFileExists(t, called, "invalid heartbeat must fail before operationctl")
			}

			require.NoError(t, os.RemoveAll(called))
			overflowEnv := []string{
				"WORKER_ID=worker-1", "LEASE_SECONDS=9223372036854775808",
				"OPERATIONCTL=" + executable, "CALLED=" + called,
			}
			overflowEnv = append(overflowEnv, toolEnv...)
			output, err := runProductionScriptCommand(t, script, overflowEnv)
			require.Error(t, err)
			require.NotContains(t, string(output), "HEARTBEAT_INTERVAL_SECONDS")
			require.NoFileExists(t, called, "overflow lease must fail before operationctl")

			require.NoError(t, os.RemoveAll(called))
			boundaryEnv := []string{
				"WORKER_ID=worker-1", "LEASE_SECONDS=9223372036854775807",
				"HEARTBEAT_INTERVAL_SECONDS=9223372036854775806.999999999",
				"OPERATIONCTL=" + executable, "CALLED=" + called,
			}
			boundaryEnv = append(boundaryEnv, toolEnv...)
			output, err = runProductionScriptCommand(t, script, boundaryEnv)
			require.Error(t, err, "empty fake claim must stop the worker")
			require.NotContains(t, string(output), "HEARTBEAT_INTERVAL_SECONDS")
			require.FileExists(t, called, "exact MaxInt64 boundary must reach operationctl")
		})
	}
}

func TestNativePITRRestoreRejectsNonCanonicalWriterCheckBeforeClaim(t *testing.T) {
	dir := t.TempDir()
	called := filepath.Join(dir, "called")
	executable := filepath.Join(dir, "operationctl")
	require.NoError(t, os.WriteFile(executable, []byte("#!/usr/bin/env bash\nprintf called >\"$CALLED\"\nexit 0\n"), 0o755))

	output, err := runProductionScriptCommand(t, "run-native-pitr-full-restore-operation.sh", []string{
		"WORKER_ID=worker-1", "LEASE_SECONDS=9223372036854775807",
		"HEARTBEAT_INTERVAL_SECONDS=1", "WRITER_CHECK_INTERVAL_SECONDS=9223372036854775808",
		"OPERATIONCTL=" + executable, "CALLED=" + called,
	})
	require.Error(t, err)
	require.Contains(t, string(output), "WRITER_CHECK_INTERVAL_SECONDS must be positive and less than LEASE_SECONDS")
	require.Contains(t, string(output), "canonical positive decimal int64")
	require.NoFileExists(t, called)
}
