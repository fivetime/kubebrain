package processgroup

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"strings"
	"testing"
)

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
	default:
		os.Exit(2)
	}
	os.Exit(0)
}
