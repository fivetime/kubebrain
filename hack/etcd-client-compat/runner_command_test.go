package compat

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

const compatScriptCommandTimeout = 30 * time.Second
const compatScriptOutputLimitBytes = 1 << 20

func runDifferentialScript(t *testing.T, env []string) ([]byte, error) {
	t.Helper()
	return runCompatScriptCommand(t, "run-differential.sh", env)
}

func runCompatScriptCommand(t *testing.T, script string, env []string) ([]byte, error) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), compatScriptCommandTimeout)
	defer cancel()

	output, err := runCompatCommandContext(t, ctx, "bash", []string{script}, env)
	if ctx.Err() == context.DeadlineExceeded {
		require.Failf(t, "compat script timed out", "script=%s timeout=%s output:\n%s", script, compatScriptCommandTimeout, string(output))
	}
	return output, err
}

func runCompatShellCommandContext(t *testing.T, ctx context.Context, command string) ([]byte, error) {
	t.Helper()
	return runCompatCommandContext(t, ctx, "bash", []string{"-c", command}, nil)
}

func runCompatKubectlContext(t *testing.T, ctx context.Context, args ...string) ([]byte, error) {
	t.Helper()
	return runCompatCommandContext(t, ctx, "kubectl", args, nil)
}

func runCompatCommandContext(t *testing.T, ctx context.Context, commandName string, args []string, env []string) ([]byte, error) {
	t.Helper()
	return runCompatCommand(ctx, commandName, args, env)
}

func runCompatCommand(ctx context.Context, commandName string, args []string, env []string) ([]byte, error) {
	command := exec.CommandContext(ctx, commandName, args...)
	configureCompatProcessGroup(command)
	command.Env = append(os.Environ(), env...)
	return compatCombinedOutput(command, compatScriptOutputLimitBytes)
}

func configureCompatProcessGroup(command *exec.Cmd) {
	command.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	command.Cancel = func() error {
		if command.Process == nil {
			return nil
		}
		err := syscall.Kill(-command.Process.Pid, syscall.SIGKILL)
		if errors.Is(err, syscall.ESRCH) {
			return os.ErrProcessDone
		}
		return err
	}
}

func compatCombinedOutput(command *exec.Cmd, limit int64) ([]byte, error) {
	if limit <= 0 {
		return nil, errors.New("process output limit must be positive")
	}
	if command.Stdout != nil {
		return nil, errors.New("exec: Stdout already set")
	}
	if command.Stderr != nil {
		return nil, errors.New("exec: Stderr already set")
	}
	output := &compatBoundedOutput{limit: limit, cancel: command.Cancel}
	command.Stdout = output
	command.Stderr = output
	err := command.Run()
	if output.exceeded() {
		return output.bytes(), fmt.Errorf("process output exceeds %d bytes", limit)
	}
	return output.bytes(), err
}

type compatBoundedOutput struct {
	mu     sync.Mutex
	buffer bytes.Buffer
	limit  int64
	cancel func() error
	over   bool
}

func (o *compatBoundedOutput) Write(data []byte) (int, error) {
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.over {
		return 0, fmt.Errorf("process output exceeds %d bytes", o.limit)
	}
	remaining := o.limit - int64(o.buffer.Len())
	if remaining <= 0 {
		o.over = true
		if o.cancel != nil {
			_ = o.cancel()
		}
		return 0, fmt.Errorf("process output exceeds %d bytes", o.limit)
	}
	if int64(len(data)) > remaining {
		o.buffer.Write(data[:remaining])
		o.over = true
		if o.cancel != nil {
			_ = o.cancel()
		}
		return int(remaining), fmt.Errorf("process output exceeds %d bytes", o.limit)
	}
	return o.buffer.Write(data)
}

func (o *compatBoundedOutput) bytes() []byte {
	o.mu.Lock()
	defer o.mu.Unlock()
	return append([]byte(nil), o.buffer.Bytes()...)
}

func (o *compatBoundedOutput) exceeded() bool {
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.over
}

func TestCompatFailoverCommandsUseBoundedHelpers(t *testing.T) {
	for _, testFile := range []string{
		"backend_quorum_failover_test.go",
		"http_gateway_concurrency_zero_lease_failover_test.go",
		"lease_checkpoint_failover_test.go",
		"lease_expiry_spread_failover_test.go",
		"lease_read_failover_test.go",
		"lease_renewal_soak_failover_test.go",
		"mutation_failover_test.go",
		"restart_persistence_test.go",
	} {
		t.Run(testFile, func(t *testing.T) {
			data, err := os.ReadFile(testFile)
			require.NoError(t, err)
			text := string(data)
			require.Contains(t, text, "runCompatShellCommandContext(t, ctx,")
			require.NotContains(t, text, `exec.CommandContext(ctx, "bash", "-c"`)
		})
	}

	helper, err := os.ReadFile("failover_helpers_test.go")
	require.NoError(t, err)
	helperText := string(helper)
	require.Contains(t, helperText, "runCompatKubectlContext(")
	require.NotContains(t, helperText, `exec.CommandContext(`)
	require.NotContains(t, helperText, ".CombinedOutput()")
}

func TestCompatKubernetesRestartCommandsUseBoundedHelpers(t *testing.T) {
	for _, testFile := range []string{
		"admission_replica_restart_test.go",
		"auth_ha_test.go",
		"concurrency_recipes_test.go",
		"corrupt_alarm_restart_test.go",
		"lease_id_extremes_test.go",
		"linearizability_test.go",
		"ordering_replica_restart_test.go",
		"serializable_read_differential_test.go",
		"watch_quota_test.go",
	} {
		t.Run(testFile, func(t *testing.T) {
			data, err := os.ReadFile(testFile)
			require.NoError(t, err)
			text := string(data)
			require.NotContains(t, text, `exec.CommandContext(`)
			require.NotContains(t, text, ".CombinedOutput()")
			require.NotContains(t, text, ".Output()")
			require.Contains(t, text, "runCompat")
		})
	}
}
