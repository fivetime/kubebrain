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
	return runProductionScriptCommandWithTimeout(t, script, env, productionScriptCommandTimeout)
}

func runProductionScriptCommandWithTimeout(t *testing.T, script string, env []string, timeout time.Duration) ([]byte, error) {
	t.Helper()
	return runProductionCommandWithTimeout(t, "bash", []string{script}, env, timeout)
}

func runProductionCommand(t *testing.T, commandName string, args []string, env []string) ([]byte, error) {
	t.Helper()
	return runProductionCommandWithTimeout(t, commandName, args, env, productionScriptCommandTimeout)
}

func runProductionCommandWithTimeout(t *testing.T, commandName string, args []string, env []string, timeout time.Duration) ([]byte, error) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	command := exec.CommandContext(ctx, commandName, args...)
	processgroup.Configure(command)
	command.WaitDelay = processgroup.DefaultWaitDelay
	command.Env = append(os.Environ(), env...)
	output, err := processgroup.CombinedOutput(command, processgroup.DefaultOutputLimitBytes)
	if ctx.Err() == context.DeadlineExceeded {
		require.Failf(t, "production command timed out", "command=%s args=%q timeout=%s output:\n%s", commandName, args, timeout, string(output))
	}
	return output, err
}

func runProductionRunnerCommand(t *testing.T, script string, env []string) ([]byte, error) {
	t.Helper()
	return runProductionScriptCommand(t, script, env)
}
