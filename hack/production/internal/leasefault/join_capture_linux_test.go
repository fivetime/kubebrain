package leasefault

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
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
	for _, mode := range []string{"capture-and-join", "escaped-child-after-capture", "adopted-zombie", "reap-live", "reap-wrong-identity", "reap-cancelled", "existing-file", "live-child", "cancelled"} {
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
	if mode == "existing-file" || mode == "live-child" || mode == "cancelled" {
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
	if mode == "capture-and-join" {
		reaped, err := ReapIsolatedJoinZombies(ctx, dir, digest)
		require.NoError(t, err)
		require.Empty(t, reaped)
	}
	if mode == "adopted-zombie" || mode == "reap-wrong-identity" || mode == "reap-cancelled" {
		pid := adoptedJoinZombie(t, ctx, dir)
		if mode == "reap-wrong-identity" {
			fields := strings.Split(string(data), "\t")
			fields[1] = "0:0"
			changed := []byte(strings.Join(fields, "\t"))
			require.NoError(t, os.WriteFile(identity, changed, 0600))
			reaped, err := ReapIsolatedJoinZombies(ctx, dir, planinput.SHA256(changed))
			require.ErrorContains(t, err, "live isolated Join identity differs")
			require.Empty(t, reaped)
			require.FileExists(t, "/proc/"+strconv.Itoa(pid)+"/stat", "wrong identity must not consume exit status")
			require.NoError(t, os.WriteFile(identity, data, 0600))
		}
		if mode == "reap-cancelled" {
			cancelled, stop := context.WithCancel(ctx)
			stop()
			reaped, err := ReapIsolatedJoinZombies(cancelled, dir, digest)
			require.ErrorIs(t, err, context.Canceled)
			require.Empty(t, reaped)
			require.FileExists(t, "/proc/"+strconv.Itoa(pid)+"/stat")
		}
		reaped, err := ReapIsolatedJoinZombies(ctx, dir, digest)
		require.NoError(t, err)
		require.Len(t, reaped, 1)
		require.Equal(t, pid, reaped[0].PID)
		require.Equal(t, 23, syscall.WaitStatus(reaped[0].Status).ExitStatus(), "retain nonzero orphan exit status")
		require.ErrorIs(t, syscall.Kill(pid, 0), syscall.ESRCH)
	}
	if mode == "reap-live" {
		child := exec.Command("/bin/sleep", "30")
		require.NoError(t, child.Start())
		defer func() { _ = child.Process.Kill(); _ = child.Wait() }()
		reaped, err := ReapIsolatedJoinZombies(ctx, dir, digest)
		require.ErrorContains(t, err, "live children")
		require.Empty(t, reaped)
		require.NoError(t, child.Process.Signal(syscall.Signal(0)))
		require.NoError(t, child.Process.Kill())
		require.Error(t, child.Wait())
	}
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

func adoptedJoinZombie(t *testing.T, ctx context.Context, directory string) int {
	t.Helper()
	// Wait for the managed intermediate parent normally. Its child is adopted
	// by this PID-1 Go process, exits 23, and is deliberately not waited yet.
	parent := exec.CommandContext(ctx, "/bin/bash", "-c", `/bin/bash -c 'while [[ ! -f "$1/release-orphan" ]]; do /bin/sleep 0.01; done; exit 23' orphan "$1" >/dev/null 2>&1 & printf '%s\n' "$!"`, "parent", directory)
	out, err := parent.Output()
	require.NoError(t, err)
	pid, err := strconv.Atoi(strings.TrimSpace(string(out)))
	require.NoError(t, err)
	require.Greater(t, pid, 1)
	require.NoError(t, os.WriteFile(filepath.Join(directory, "release-orphan"), nil, 0600))
	for {
		require.NoError(t, ctx.Err())
		data, err := os.ReadFile("/proc/" + strconv.Itoa(pid) + "/stat")
		require.NoError(t, err)
		fields := strings.Fields(string(data)[strings.LastIndex(string(data), ") ")+2:])
		if fields[0] == "Z" && fields[1] == "1" {
			return pid
		}
		time.Sleep(10 * time.Millisecond)
	}
}
