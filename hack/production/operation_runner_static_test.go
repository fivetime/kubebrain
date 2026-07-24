package production_test

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestOperationRunnersExposeHeartbeatIntervalOverride(t *testing.T) {
	for _, script := range []string{
		"run-backup-operation.sh",
		"run-backup-deletion-operation.sh",
		"run-certificate-rotation-operation.sh",
		"run-destroy-operation.sh",
		"run-post-restore-audit-operation.sh",
		"run-restore-cutover-operation.sh",
	} {
		t.Run(script, func(t *testing.T) {
			data, err := os.ReadFile(filepath.Join(".", script))
			require.NoError(t, err)
			text := string(data)
			require.Contains(t, text, `HEARTBEAT_INTERVAL_SECONDS="${HEARTBEAT_INTERVAL_SECONDS:-}"`)
			require.Contains(t, text, `heartbeat_interval="${HEARTBEAT_INTERVAL_SECONDS:-$((LEASE_SECONDS / 3))}"`)
			require.Contains(t, text, `HEARTBEAT_INTERVAL_SECONDS must be positive`)
			require.NotContains(t, text, "heartbeat_interval=$((LEASE_SECONDS / 3))")
		})
	}
}

func TestOperationRunnerTestsUseBoundedCommandHelper(t *testing.T) {
	for _, tc := range []struct {
		testFile string
		script   string
	}{
		{testFile: "run_backup_operation_test.go", script: "run-backup-operation.sh"},
		{testFile: "run_backup_deletion_operation_test.go", script: "run-backup-deletion-operation.sh"},
		{testFile: "run_certificate_rotation_operation_test.go", script: "run-certificate-rotation-operation.sh"},
		{testFile: "run_destroy_operation_test.go", script: "run-destroy-operation.sh"},
		{testFile: "run_post_restore_audit_operation_test.go", script: "run-post-restore-audit-operation.sh"},
		{testFile: "run_restore_cutover_operation_test.go", script: "run-restore-cutover-operation.sh"},
	} {
		t.Run(tc.testFile, func(t *testing.T) {
			data, err := os.ReadFile(filepath.Join(".", tc.testFile))
			require.NoError(t, err)
			text := string(data)
			require.Contains(t, text, `runProductionRunnerCommand(t, "`+tc.script+`", env)`)
			require.NotContains(t, text, `exec.Command("bash", "`+tc.script+`")`)
		})
	}
}

func TestReleaseGateScriptTestsUseBoundedCommandHelper(t *testing.T) {
	for _, tc := range []struct {
		testFile string
		script   string
	}{
		{testFile: "validate_network_policy_test.go", script: "validate-network-policy.sh"},
		{testFile: "validate_instance_ready_test.go", script: "validate-instance-ready.sh"},
		{testFile: "validate_certificate_rotation_test.go", script: "validate-certificate-rotation.sh"},
		{testFile: "wait_tidbcluster_ready_test.go", script: "wait-tidbcluster-ready.sh"},
	} {
		t.Run(tc.testFile, func(t *testing.T) {
			data, err := os.ReadFile(filepath.Join(".", tc.testFile))
			require.NoError(t, err)
			text := string(data)
			switch tc.script {
			case "validate-instance-ready.sh":
				require.Contains(t, text, `runValidateInstanceReady(t, env)`)
			case "validate-certificate-rotation.sh":
				require.Contains(t, text, `runValidateCertificateRotation(t, env)`)
			default:
				require.Contains(t, text, `runProductionScriptCommand(t, "`+tc.script+`", env)`)
			}
			require.NotContains(t, text, `exec.Command("bash", "`+tc.script+`")`)
		})
	}
}

func TestProductionLifecycleScriptTestsUseBoundedCommandHelper(t *testing.T) {
	for _, tc := range []struct {
		testFile string
		script   string
		helper   string
	}{
		{testFile: "archive_operation_audit_test.go", script: "archive-operation-audit.sh", helper: "runArchiveOperationAudit"},
		{testFile: "cleanup_instance_boundaries_test.go", script: "cleanup-instance-boundaries.sh", helper: "runBoundaryCleanup"},
		{testFile: "destroy_instance_test.go", script: "destroy-instance.sh", helper: "runDestroyInstance"},
		{testFile: "switch_restore_traffic_test.go", script: "switch-restore-traffic.sh", helper: "runSwitchRestoreTraffic"},
		{testFile: "audit_restored_instance_test.go", script: "audit-restored-instance.sh", helper: "runAuditRestoredInstance"},
	} {
		t.Run(tc.testFile, func(t *testing.T) {
			data, err := os.ReadFile(filepath.Join(".", tc.testFile))
			require.NoError(t, err)
			text := string(data)
			require.Contains(t, text, tc.helper+`(t, env)`)
			require.NotContains(t, text, `exec.Command("bash", "`+tc.script+`")`)
		})
	}
}

func TestProductionShellEntrypointNamespaceTestsUseBoundedCommandHelper(t *testing.T) {
	data, err := os.ReadFile(filepath.Join(".", "namespace_validation_test.go"))
	require.NoError(t, err)
	text := string(data)
	require.Contains(t, text, `runProductionScriptCommand(t, tc.script, env)`)
	require.NotContains(t, text, `exec.Command("bash", tc.script)`)
}

func TestColdBackupScriptTestsUseBoundedCommandHelper(t *testing.T) {
	for _, tc := range []struct {
		testFile     string
		script       string
		helper       string
		renderHelper string
	}{
		{testFile: "cold_snapshot_preflight_test.go", script: "../backup/cold-snapshot-preflight.sh", helper: "runColdSnapshotPreflight"},
		{testFile: "cold_snapshot_execute_test.go", script: "../backup/cold-snapshot-execute.sh", helper: "runColdSnapshotExecute"},
		{testFile: "cold_restore_execute_test.go", script: "../backup/cold-restore-execute.sh", helper: "runColdRestoreExecute", renderHelper: "runColdRestoreRender"},
	} {
		t.Run(tc.testFile, func(t *testing.T) {
			data, err := os.ReadFile(filepath.Join(".", tc.testFile))
			require.NoError(t, err)
			text := string(data)
			require.Contains(t, text, tc.helper+`(t, env)`)
			if tc.renderHelper != "" {
				require.Contains(t, text, tc.renderHelper+`(t, receiptPath, manifestPath)`)
				require.NotContains(t, text, `exec.Command("go", "run", "../backup/cmd/cold-restore-render"`)
			}
			require.NotContains(t, text, `exec.Command("bash", "`+tc.script+`")`)
		})
	}
}

func TestProductionCmdGoRunTestsUseBoundedCommandHelper(t *testing.T) {
	for _, testFile := range []string{
		filepath.Join("cmd", "backup-scheduler", "main_test.go"),
		filepath.Join("cmd", "operation-api", "main_test.go"),
		filepath.Join("cmd", "operation-archiver", "main_test.go"),
		filepath.Join("cmd", "operation-audit", "main_test.go"),
		filepath.Join("cmd", "operation-parameter-broker", "main_test.go"),
		filepath.Join("cmd", "operationctl", "main_test.go"),
		filepath.Join("cmd", "uid-delete", "main_test.go"),
	} {
		t.Run(testFile, func(t *testing.T) {
			data, err := os.ReadFile(testFile)
			require.NoError(t, err)
			text := string(data)
			require.Contains(t, text, `testcommand.GoRun(t, "."`)
			require.NotContains(t, text, `exec.Command("go", "run"`)
			require.NotContains(t, text, `.CombinedOutput()`)
		})
	}
}

func TestProductionRuntimeExecutorCommandsUseProcessGroupAndBoundedOutput(t *testing.T) {
	for _, sourceFile := range []string{
		filepath.Join("internal", "meteringarchive", "archive.go"),
		filepath.Join("internal", "meteringbilling", "biller.go"),
		filepath.Join("internal", "meteringstorage", "archiver.go"),
		filepath.Join("internal", "operationarchiver", "processor.go"),
	} {
		t.Run(sourceFile, func(t *testing.T) {
			data, err := os.ReadFile(sourceFile)
			require.NoError(t, err)
			text := string(data)
			require.Contains(t, text, "processgroup.Configure(command)")
			require.Contains(t, text, "command.WaitDelay = 5 * time.Second")
			require.Contains(t, text, "processgroup.CombinedOutput(command, processgroup.DefaultOutputLimitBytes)")
			require.NotContains(t, text, ".CombinedOutput()")
		})
	}
}

func TestProductionOperationWorkerStreamsExecutorWithProcessGroup(t *testing.T) {
	data, err := os.ReadFile(filepath.Join("cmd", "operation-worker", "main.go"))
	require.NoError(t, err)
	text := string(data)
	require.Contains(t, text, "processgroup.ValidateExecutable(executable)")
	require.Contains(t, text, "processgroup.Configure(command)")
	require.Contains(t, text, "command.Stdin = os.Stdin")
	require.Contains(t, text, "command.Stdout = os.Stdout")
	require.Contains(t, text, "command.Stderr = os.Stderr")
	require.NotContains(t, text, "processgroup.CombinedOutput(")
}

func TestProductionTestCommandHelpersUseWaitDelay(t *testing.T) {
	for _, sourceFile := range []string{
		"runner_command_test.go",
		filepath.Join("internal", "testcommand", "command.go"),
	} {
		t.Run(sourceFile, func(t *testing.T) {
			data, err := os.ReadFile(sourceFile)
			require.NoError(t, err)
			text := string(data)
			require.Contains(t, text, "command.WaitDelay =")
			require.Contains(t, text, "5 * time.Second")
		})
	}
}
