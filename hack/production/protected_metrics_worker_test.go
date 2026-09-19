package production_test

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/kubewharf/kubebrain/hack/production/internal/processgroup"
	"github.com/stretchr/testify/require"
)

func TestProtectedMetricsWorkerHandshakeAndCleanup(t *testing.T) {
	worker, err := os.ReadFile("protected-metrics-worker.sh")
	require.NoError(t, err)
	for _, scenario := range []string{"success", "baseline-failed", "capture-failed", "missing-binding", "eof", "malformed", "future", "terminate", "hold-after-baseline"} {
		t.Run(scenario, func(t *testing.T) {
			owner := t.TempDir()
			bundle := t.TempDir()
			require.NoError(t, os.Mkdir(filepath.Join(owner, "deployment-claimed"), 0700))
			workerPath := filepath.Join(bundle, "protected-metrics-worker.sh")
			libraryPath := filepath.Join(bundle, "protected-stack-session.sh")
			require.NoError(t, os.WriteFile(workerPath, worker, 0700))
			require.NoError(t, os.WriteFile(libraryPath, []byte(metricsWorkerLibraryFixture), 0600))
			manifest := fmt.Sprintf("%x  %s\n", sha256.Sum256([]byte(metricsWorkerLibraryFixture)), libraryPath)
			if scenario != "missing-binding" {
				manifest += fmt.Sprintf("%x  %s\n", sha256.Sum256(worker), workerPath)
			}
			require.NoError(t, os.WriteFile(filepath.Join(owner, "tools.sha256"), []byte(manifest), 0600))
			ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
			defer cancel()
			cmd := exec.CommandContext(ctx, "bash", workerPath, "brain-0", "27000000000")
			processgroup.Configure(cmd)
			cmd.WaitDelay = time.Second
			cmd.Env = append(os.Environ(), "stack_owner="+owner, "worker_scenario="+scenario, "stack_info_port=", "stack_anonymous_port=")
			var stderr bytes.Buffer
			cmd.Stderr = &stderr
			stdout, err := cmd.StdoutPipe()
			require.NoError(t, err)
			stdin, err := cmd.StdinPipe()
			require.NoError(t, err)
			require.NoError(t, cmd.Start())
			reader := bufio.NewReader(stdout)
			ready, readErr := reader.ReadString('\n')
			if scenario == "baseline-failed" || scenario == "missing-binding" || scenario == "hold-after-baseline" {
				require.Error(t, readErr)
				require.Empty(t, ready)
			} else {
				require.NoError(t, readErr)
				require.True(t, strings.HasPrefix(ready, "READY\t"), ready)
				if scenario == "terminate" {
					require.NoError(t, syscall.Kill(-cmd.Process.Pid, syscall.SIGTERM))
				} else if scenario != "eof" {
					origin := fmt.Sprintf("%d\n", time.Now().UnixNano())
					if scenario == "malformed" {
						origin = "18000000000000000000\n"
					}
					if scenario == "future" {
						origin = fmt.Sprintf("%d\n", time.Now().Add(time.Hour).UnixNano())
					}
					_, err = io.WriteString(stdin, origin)
					require.NoError(t, err)
				}
			}
			_ = stdin.Close()
			rest, err := io.ReadAll(reader)
			require.NoError(t, err)
			waitErr := cmd.Wait()
			require.NoError(t, ctx.Err(), stderr.String())
			if scenario == "success" {
				require.NoError(t, waitErr, stderr.String())
				require.True(t, strings.HasPrefix(string(rest), "CAPTURED\t"), string(rest))
			} else {
				require.Error(t, waitErr)
				require.Empty(t, string(rest))
			}
			receipts, err := filepath.Glob(filepath.Join(owner, "metrics-worker.*", "exit-code"))
			require.NoError(t, err)
			require.Len(t, receipts, 1)
			receipt, err := os.ReadFile(receipts[0])
			require.NoError(t, err)
			if scenario == "success" {
				require.Equal(t, "0\n", string(receipt))
			} else {
				require.NotEqual(t, "0\n", string(receipt))
			}
			if pid, err := os.ReadFile(filepath.Join(owner, "child-pid")); err == nil {
				require.Error(t, exec.Command("kill", "-0", strings.TrimSpace(string(pid))).Run(), "worker child survived")
			}
			if scenario == "success" {
				ports, err := os.ReadFile(filepath.Join(owner, "ports"))
				require.NoError(t, err)
				require.Equal(t, "18586 18587\n", string(ports))
			}
		})
	}
}

const metricsWorkerLibraryFixture = `
stack_pids=()
stack_session_owner_active() { [[ -d $stack_owner/deployment-claimed && ! -e $stack_owner/HOLD && ! -e $stack_owner/final-exit-code ]]; }
stack_session_verify_inputs() { sha256sum -c "$stack_owner/tools.sha256" >/dev/null; }
stack_session_run() { "$@"; }
stack_session_prepare() {
 [[ $1 == brain-0 ]]
 /usr/bin/sleep 60 >/dev/null 2>&1 &
 stack_pids+=("$!")
 printf '%s\n' "$!" > "$stack_owner/child-pid"
 printf '%s %s\n' "$stack_info_port" "$stack_anonymous_port" > "$stack_owner/ports"
}
stack_session_close() { local child; for child in "${stack_pids[@]}"; do kill -TERM "$child" 2>/dev/null || true; wait "$child" 2>/dev/null || true; done; }
stack_session_capture_metrics() {
 [[ $1 == before-fault && ${#stack_pids[@]} == 1 ]]
 [[ $worker_scenario != baseline-failed ]] || return 19
 stack_capture=$stack_owner/baseline
 mkdir "$stack_capture"
 touch "$stack_capture/COMPLETE"
 [[ $worker_scenario != hold-after-baseline ]] || touch "$stack_owner/HOLD"
}
stack_session_capture_metrics_at() {
 [[ $2 == 27000000000 && $1 -le $(date -u +%s%N) ]] || return 2
 [[ $worker_scenario != capture-failed ]] || return 20
 stack_capture=$stack_owner/capture
 stack_schedule=$stack_owner/schedule
 mkdir "$stack_capture" "$stack_schedule"
 touch "$stack_capture/COMPLETE" "$stack_schedule/COMPLETE"
}
stack_session_budget() { return 0; }
`
