package production_test

import (
	"context"
	"os"
	"os/exec"
	"testing"
	"time"

	"github.com/kubewharf/kubebrain/hack/production/internal/processgroup"
	"github.com/stretchr/testify/require"
)

const productionScriptCommandTimeout = 30 * time.Second

func runProductionScriptCommand(t *testing.T, script string, env []string) ([]byte, error) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), productionScriptCommandTimeout)
	defer cancel()

	command := exec.CommandContext(ctx, "bash", script)
	processgroup.Configure(command)
	command.Env = append(os.Environ(), env...)
	output, err := processgroup.CombinedOutput(command, processgroup.DefaultOutputLimitBytes)
	if ctx.Err() == context.DeadlineExceeded {
		require.Failf(t, "production script timed out", "script=%s timeout=%s output:\n%s", script, productionScriptCommandTimeout, string(output))
	}
	return output, err
}

func runProductionRunnerCommand(t *testing.T, script string, env []string) ([]byte, error) {
	t.Helper()
	return runProductionScriptCommand(t, script, env)
}
