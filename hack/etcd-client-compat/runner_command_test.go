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

	command := exec.CommandContext(ctx, "bash", script)
	configureCompatProcessGroup(command)
	command.Env = append(os.Environ(), env...)
	output, err := compatCombinedOutput(command, compatScriptOutputLimitBytes)
	if ctx.Err() == context.DeadlineExceeded {
		require.Failf(t, "compat script timed out", "script=%s timeout=%s output:\n%s", script, compatScriptCommandTimeout, string(output))
	}
	return output, err
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
