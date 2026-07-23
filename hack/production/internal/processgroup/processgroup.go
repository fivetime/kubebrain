package processgroup

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"syscall"
)

const DefaultOutputLimitBytes = 1 << 20

// Configure makes cancellation terminate the command and all descendants that
// remain in its process group.
func Configure(command *exec.Cmd) {
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

func ValidateExecutable(path string) error {
	if path == "" || !filepath.IsAbs(path) {
		return errors.New("executor must be an absolute path")
	}
	info, err := os.Stat(path)
	if err != nil {
		return fmt.Errorf("stat executor: %w", err)
	}
	if info.IsDir() || info.Mode()&0o111 == 0 {
		return errors.New("executor must be an executable file")
	}
	return nil
}

// CombinedOutput is like exec.Cmd.CombinedOutput, but it bounds captured
// stdout/stderr and cancels the process group when that bound is exceeded.
func CombinedOutput(command *exec.Cmd, limit int64) ([]byte, error) {
	if limit <= 0 {
		return nil, errors.New("process output limit must be positive")
	}
	if command.Stdout != nil {
		return nil, errors.New("exec: Stdout already set")
	}
	if command.Stderr != nil {
		return nil, errors.New("exec: Stderr already set")
	}
	output := &boundedOutput{limit: limit, cancel: command.Cancel}
	command.Stdout = output
	command.Stderr = output
	err := command.Run()
	if output.exceeded() {
		return output.bytes(), fmt.Errorf("process output exceeds %d bytes", limit)
	}
	return output.bytes(), err
}

type boundedOutput struct {
	mu     sync.Mutex
	buffer bytes.Buffer
	limit  int64
	cancel func() error
	over   bool
}

func (o *boundedOutput) Write(data []byte) (int, error) {
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

func (o *boundedOutput) bytes() []byte {
	o.mu.Lock()
	defer o.mu.Unlock()
	return append([]byte(nil), o.buffer.Bytes()...)
}

func (o *boundedOutput) exceeded() bool {
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.over
}
