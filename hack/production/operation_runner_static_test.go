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
		{testFile: "wait_tidbcluster_ready_test.go", script: "wait-tidbcluster-ready.sh"},
	} {
		t.Run(tc.testFile, func(t *testing.T) {
			data, err := os.ReadFile(filepath.Join(".", tc.testFile))
			require.NoError(t, err)
			text := string(data)
			require.Contains(t, text, `runProductionScriptCommand(t, "`+tc.script+`", env)`)
			require.NotContains(t, text, `exec.Command("bash", "`+tc.script+`")`)
		})
	}
}
