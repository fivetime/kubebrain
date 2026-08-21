package production_test

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestTiKVWorkersValidateLeaseBeforeArithmetic(t *testing.T) {
	for _, script := range []string{
		"run-tikv-transaction-repair-operation.sh",
		"run-tikv-transaction-recovery-operation.sh",
	} {
		t.Run(script, func(t *testing.T) {
			dir := t.TempDir()
			logPath := filepath.Join(dir, "operationctl.log")
			operationctl := filepath.Join(dir, "operationctl")
			require.NoError(t, os.WriteFile(operationctl, []byte("#!/usr/bin/env bash\nprintf '%s\\n' \"$*\" >>\"$OPERATIONCTL_LOG\"\nprintf '{}\\n'\n"), 0o755))
			commandVariable := "REPAIR_COMMAND=/bin/true"
			if script == "run-tikv-transaction-recovery-operation.sh" {
				commandVariable = "RECOVERY_COMMAND=/bin/true"
			}

			maxOutput, maxErr := runProductionScriptCommand(t, script, []string{
				"WORKER_ID=worker-max-lease", "WORK_DIR=" + dir, "OPERATIONCTL=" + operationctl,
				"OPERATIONCTL_LOG=" + logPath, commandVariable, "LEASE_SECONDS=9223372036854775807",
			})
			require.Error(t, maxErr)
			require.NotContains(t, string(maxOutput), "LEASE_SECONDS")
			require.Contains(t, string(mustRead(t, logPath)), "--lease 9223372036854775807s")

			require.NoError(t, os.Remove(logPath))
			overflowOutput, overflowErr := runProductionScriptCommand(t, script, []string{
				"WORKER_ID=worker-overflow-lease", "WORK_DIR=" + dir, "OPERATIONCTL=" + operationctl,
				"OPERATIONCTL_LOG=" + logPath, commandVariable, "LEASE_SECONDS=9223372036854775808",
			})
			require.Error(t, overflowErr)
			require.Contains(t, string(overflowOutput), "LEASE_SECONDS must be a positive int64")
			require.NoFileExists(t, logPath)
		})
	}
}
