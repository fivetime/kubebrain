package leasefault

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/kubewharf/kubebrain/hack/production/internal/planinput"
	"github.com/stretchr/testify/require"
)

func TestCaptureIsolatedJoinRefusesHost(t *testing.T) {
	if os.Getpid() == 1 {
		t.Skip("this test checks non-init refusal")
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	dir := t.TempDir()
	digest, err := CaptureIsolatedJoinIdentity(ctx, dir)
	require.ErrorContains(t, err, "requires PID 1")
	require.Empty(t, digest)
	require.NoFileExists(t, filepath.Join(dir, IsolatedJoinIdentity))
}

func TestCaptureIsolatedJoinRealNamespace(t *testing.T) {
	if _, err := exec.LookPath("unshare"); err != nil {
		t.Skip("unshare unavailable; PID-1 integration not executed")
	}
	if out, err := exec.Command("unshare", "--fork", "--pid", "--mount-proc", "/bin/true").CombinedOutput(); err != nil {
		if strings.Contains(string(out), "Operation not permitted") {
			t.Skip("PID namespace creation denied; integration not executed")
		}
		require.NoError(t, err, string(out))
	}
	executable, err := os.Executable()
	require.NoError(t, err)
	script, err := filepath.Abs("../../" + IsolatedJoinScript)
	require.NoError(t, err)
	for _, mode := range []string{"capture-and-join", "escaped-child-after-capture", "existing-file", "live-child", "cancelled"} {
		t.Run(mode, func(t *testing.T) {
			dir := t.TempDir()
			require.NoError(t, os.Chmod(dir, 0700))
			ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
			defer cancel()
			cmd := exec.CommandContext(ctx, "unshare", "--fork", "--pid", "--mount-proc", "--kill-child=KILL", executable, "-test.run=^TestCaptureIsolatedJoinHelper$", "-test.count=1")
			cmd.Env = append(os.Environ(), "KB_FAULT_JOIN_HELPER="+mode, "KB_FAULT_JOIN_OWNER="+dir, "KB_FAULT_JOIN_SCRIPT="+script)
			out, err := cmd.CombinedOutput()
			require.NoError(t, ctx.Err(), string(out))
			require.NoError(t, err, string(out))
		})
	}
}

func TestCaptureIsolatedJoinHelper(t *testing.T) {
	mode := os.Getenv("KB_FAULT_JOIN_HELPER")
	if mode == "" {
		return
	}
	require.Equal(t, 1, os.Getpid())
	dir, script := os.Getenv("KB_FAULT_JOIN_OWNER"), os.Getenv("KB_FAULT_JOIN_SCRIPT")
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	identity := filepath.Join(dir, IsolatedJoinIdentity)
	switch mode {
	case "existing-file":
		require.NoError(t, os.WriteFile(identity, []byte("preserve incomplete identity"), 0600))
	case "live-child":
		child := exec.Command("/bin/sleep", "30")
		require.NoError(t, child.Start())
		defer func() { _ = child.Process.Kill(); _ = child.Wait() }()
	case "cancelled":
		cancel()
	}
	digest, err := CaptureIsolatedJoinIdentity(ctx, dir)
	if mode != "capture-and-join" && mode != "escaped-child-after-capture" {
		require.Error(t, err)
		require.Empty(t, digest)
		if mode == "existing-file" {
			data, err := os.ReadFile(identity)
			require.NoError(t, err)
			require.Equal(t, "preserve incomplete identity", string(data))
		} else {
			require.NoFileExists(t, identity)
		}
		return
	}
	require.NoError(t, err)
	data, err := os.ReadFile(identity)
	require.NoError(t, err)
	require.Equal(t, planinput.SHA256(data), digest)
	files := map[string]string{identity: digest}
	admit := func(ctx context.Context) error { return VerifyJoinInputs(ctx, dir, script, files) }
	// This uses the actual Go Join runner and packaged Bash checker, with the
	// Go test process as PID 1. No shell fixture fabricates the recorded identity.
	if mode == "escaped-child-after-capture" {
		child := exec.Command("/bin/sleep", "30")
		child.SysProcAttr = &syscall.SysProcAttr{Setsid: true}
		require.NoError(t, child.Start())
		defer func() { _ = child.Process.Kill(); _ = child.Wait() }()
		err := RunFaultJoin(ctx, dir, script, admit)
		require.ErrorContains(t, err, "exit status 75")
		require.NoError(t, child.Process.Signal(syscall.Signal(0)), "Join is a check, never broad process cleanup")
	} else {
		require.NoError(t, RunFaultJoin(ctx, dir, script, admit))
	}
	proof, err := filepath.Glob(filepath.Join(dir, "recovery-join.*.json"))
	require.NoError(t, err)
	require.Len(t, proof, 1)
	record, err := os.ReadFile(proof[0])
	require.NoError(t, err)
	if mode == "escaped-child-after-capture" {
		require.Contains(t, string(record), "exit status 75", "retain the failed Join, never a successful recovery")
	}
	_, err = CaptureIsolatedJoinIdentity(ctx, dir)
	require.Error(t, err, "never overwrite an existing startup identity")
	after, err := os.ReadFile(identity)
	require.NoError(t, err)
	require.Equal(t, data, after)
}
