package production_test

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestValidateInfoExecutorRuntimeContract(t *testing.T) {
	workspace := t.TempDir()
	out, err := runProductionScriptCommand(t, "validate-info-executor-runtime.sh", []string{
		"INFO_EXECUTOR_PROBE_PARENT=" + workspace,
	})
	require.NoError(t, err, string(out))
	require.Contains(t, string(out), "runtime tool contract passed")
	probes, err := filepath.Glob(filepath.Join(workspace, ".info-executor-runtime.*"))
	require.NoError(t, err)
	require.Empty(t, probes)

	out, err = runProductionScriptCommand(t, "validate-info-executor-runtime.sh", []string{
		"PATH=" + filepath.Join(t.TempDir(), "missing-tools"),
	})
	require.Error(t, err)
	require.Contains(t, string(out), "required info executor command is missing")

	out, err = runProductionScriptCommand(t, "validate-info-executor-runtime.sh", []string{
		"REQUIRE_INFO_EXECUTOR_BINARIES=invalid",
	})
	require.Error(t, err)
	require.Contains(t, string(out), "must be true or false")

	out, err = runProductionScriptCommand(t, "validate-info-executor-runtime.sh", []string{
		"INFO_EXECUTOR_PROBE_PARENT=relative",
	})
	require.Error(t, err)
	require.Contains(t, string(out), "must be an existing absolute directory")

	data, err := os.ReadFile("validate-info-executor-runtime.sh")
	require.NoError(t, err)
	for _, required := range []string{
		`mktemp "${probe_dir}/.evidence.tmp.XXXXXX"`,
		`stat -Lc '%a:%u:%h:%s'`,
		`realpath -m`,
		`sync -f "$temporary"`,
		`ln -- "$temporary" "$published"`,
		`sync -f "$probe_dir"`,
	} {
		require.Contains(t, string(data), required)
	}
}
