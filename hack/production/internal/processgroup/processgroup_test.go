package processgroup

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"
)

func TestDefaultWaitDelayBoundsExecutorTeardown(t *testing.T) {
	if DefaultWaitDelay != 5*time.Second {
		t.Fatalf("DefaultWaitDelay = %s, want 5s", DefaultWaitDelay)
	}
}

func TestCombinedOutputCapturesStdoutAndStderr(t *testing.T) {
	command := processgroupHelperCommand("mixed")
	output, err := CombinedOutput(command, DefaultOutputLimitBytes)
	if err != nil {
		t.Fatalf("CombinedOutput returned error: %v", err)
	}
	value := string(output)
	if !strings.Contains(value, "stdout-line") || !strings.Contains(value, "stderr-line") {
		t.Fatalf("CombinedOutput did not capture both streams: %q", value)
	}
}

func TestCombinedOutputBoundsOutputAndCancelsProcess(t *testing.T) {
	command := processgroupHelperCommand("spam")
	Configure(command)
	output, err := CombinedOutput(command, 64)
	if err == nil || !strings.Contains(err.Error(), "process output exceeds 64 bytes") {
		t.Fatalf("CombinedOutput error = %v, want output limit error", err)
	}
	if len(output) != 64 {
		t.Fatalf("captured output length = %d, want 64", len(output))
	}
}

func TestCombinedOutputPreservesExitError(t *testing.T) {
	command := processgroupHelperCommand("fail")
	output, err := CombinedOutput(command, DefaultOutputLimitBytes)
	var exitErr *exec.ExitError
	if !errors.As(err, &exitErr) {
		t.Fatalf("CombinedOutput error = %T %v, want *exec.ExitError", err, err)
	}
	if !strings.Contains(string(output), "failure detail") {
		t.Fatalf("CombinedOutput output = %q, want failure detail", string(output))
	}
}

func TestCombinedOutputCleansProcessGroupDescendantsAfterParentExit(t *testing.T) {
	command := processgroupHelperCommand("daemonize")
	Configure(command)
	output, err := CombinedOutput(command, DefaultOutputLimitBytes)
	if err != nil {
		t.Fatalf("CombinedOutput returned error: %v, output=%q", err, string(output))
	}
	pid, err := strconv.Atoi(strings.TrimSpace(string(output)))
	if err != nil {
		t.Fatalf("helper output %q did not contain child pid: %v", string(output), err)
	}
	t.Cleanup(func() {
		_ = syscall.Kill(pid, syscall.SIGKILL)
	})
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if !processExists(pid) {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("descendant process %d still exists after CombinedOutput returned", pid)
}

func TestValidateExecutable(t *testing.T) {
	dir := t.TempDir()
	ok := filepath.Join(dir, "ok")
	if err := os.WriteFile(ok, []byte("#!/bin/sh\n"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := ValidateExecutable(ok); err != nil {
		t.Fatalf("ValidateExecutable returned error: %v", err)
	}
	fifo := filepath.Join(dir, "fifo")
	if err := syscall.Mkfifo(fifo, 0o755); err != nil {
		t.Fatal(err)
	}

	for _, tc := range []struct {
		name string
		path string
	}{
		{name: "relative", path: "relative"},
		{name: "missing", path: filepath.Join(dir, "missing")},
		{name: "directory", path: dir},
		{name: "non-regular", path: fifo},
		{name: "not executable", path: filepath.Join(dir, "not-executable")},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if tc.name == "not executable" {
				if err := os.WriteFile(tc.path, []byte("#!/bin/sh\n"), 0o644); err != nil {
					t.Fatal(err)
				}
			}
			if err := ValidateExecutable(tc.path); err == nil {
				t.Fatal("ValidateExecutable returned nil, want error")
			}
		})
	}
}

func processExists(pid int) bool {
	err := syscall.Kill(pid, 0)
	return err == nil || !errors.Is(err, syscall.ESRCH)
}

func processgroupHelperCommand(mode string) *exec.Cmd {
	command := exec.CommandContext(context.Background(), os.Args[0], "-test.run=TestProcessgroupHelper", "--", mode)
	command.Env = append(os.Environ(), "KUBEBRAIN_PROCESSGROUP_HELPER=1")
	return command
}

func TestProcessgroupHelper(t *testing.T) {
	if os.Getenv("KUBEBRAIN_PROCESSGROUP_HELPER") != "1" {
		return
	}
	mode := os.Args[len(os.Args)-1]
	switch mode {
	case "mixed":
		_, _ = os.Stdout.WriteString("stdout-line\n")
		_, _ = os.Stderr.WriteString("stderr-line\n")
	case "spam":
		for i := 0; i < 4096; i++ {
			_, _ = os.Stdout.WriteString("x")
		}
	case "fail":
		_, _ = os.Stderr.WriteString("failure detail\n")
		os.Exit(7)
	case "daemonize":
		child := exec.Command(os.Args[0], "-test.run=TestProcessgroupHelper", "--", "park")
		child.Env = append(os.Environ(), "KUBEBRAIN_PROCESSGROUP_HELPER=1")
		if err := child.Start(); err != nil {
			_, _ = os.Stderr.WriteString("start child: " + err.Error() + "\n")
			os.Exit(3)
		}
		_, _ = os.Stdout.WriteString(strconv.Itoa(child.Process.Pid) + "\n")
	case "park":
		time.Sleep(30 * time.Second)
	default:
		os.Exit(2)
	}
	os.Exit(0)
}
