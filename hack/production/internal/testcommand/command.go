package testcommand

import (
	"context"
	"os"
	"os/exec"
	"testing"
	"time"

	"github.com/kubewharf/kubebrain/hack/production/internal/processgroup"
	"github.com/stretchr/testify/require"
)

const defaultTimeout = 30 * time.Second
const defaultWaitDelay = 5 * time.Second

func GoRun(t *testing.T, args ...string) ([]byte, error) {
	t.Helper()
	return Run(t, "go", append([]string{"run"}, args...), nil)
}

func Run(t *testing.T, commandName string, args []string, env []string) ([]byte, error) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), defaultTimeout)
	defer cancel()

	command := exec.CommandContext(ctx, commandName, args...)
	processgroup.Configure(command)
	command.WaitDelay = defaultWaitDelay
	command.Env = append(os.Environ(), env...)
	output, err := processgroup.CombinedOutput(command, processgroup.DefaultOutputLimitBytes)
	if ctx.Err() == context.DeadlineExceeded {
		require.Failf(t, "test command timed out", "command=%s args=%q timeout=%s output:\n%s", commandName, args, defaultTimeout, string(output))
	}
	return output, err
}
