package production_test

import (
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestOperationRunnersRejectInvalidSubcommandBeforeClaim(t *testing.T) {
	for _, tc := range []struct {
		name      string
		script    string
		command   string
		extraEnvs []string
	}{
		{name: "backup object command", script: "run-backup-operation.sh", command: "OBJECT_COMMAND"},
		{name: "backup deletion object command", script: "run-backup-deletion-operation.sh", command: "OBJECT_COMMAND"},
		{name: "restore cutover command", script: "run-restore-cutover-operation.sh", command: "CUTOVER_COMMAND"},
		{name: "post restore audit command", script: "run-post-restore-audit-operation.sh", command: "AUDIT_COMMAND"},
		{
			name:    "certificate rotation command",
			script:  "run-certificate-rotation-operation.sh",
			command: "ROTATION_COMMAND",
			extraEnvs: []string{
				"PUBLISH_OVERLAP_COMMAND={hook}",
				"PUBLISH_FINAL_COMMAND={hook}",
			},
		},
		{name: "destroy command", script: "run-destroy-operation.sh", command: "DESTROY_COMMAND"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			operationctlLog := filepath.Join(dir, "operationctl.log")
			operationctl := filepath.Join(dir, "operationctl")
			writeExecutable(t, operationctl, "#!/usr/bin/env bash\nprintf called >\""+operationctlLog+"\"\n")
			hook := filepath.Join(dir, "hook")
			writeExecutable(t, hook, "#!/usr/bin/env bash\nexit 0\n")
			missing := filepath.Join(dir, "missing-command")
			env := []string{
				"WORKER_ID=worker-a",
				"OPERATIONCTL=" + operationctl,
				tc.command + "=" + missing,
			}
			for _, item := range tc.extraEnvs {
				env = append(env, replaceHookPath(item, hook))
			}

			output, err := runProductionScriptCommand(t, tc.script, env)
			require.Error(t, err, string(output))
			require.Contains(t, string(output), tc.command+" is required and must be an executable file")
			require.NoFileExists(t, operationctlLog)
		})
	}
}

func replaceHookPath(value, hook string) string {
	if value == "PUBLISH_OVERLAP_COMMAND={hook}" {
		return "PUBLISH_OVERLAP_COMMAND=" + hook
	}
	if value == "PUBLISH_FINAL_COMMAND={hook}" {
		return "PUBLISH_FINAL_COMMAND=" + hook
	}
	return value
}
