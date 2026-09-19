package production_test

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/kubewharf/kubebrain/hack/production/internal/metricsworker"
	"github.com/kubewharf/kubebrain/hack/production/internal/processgroup"
	"github.com/kubewharf/kubebrain/hack/production/internal/retirementmetrics"
	"github.com/stretchr/testify/require"
)

func TestProtectedMetricsWorkerRealSessionArtifacts(t *testing.T) {
	for _, reject := range []bool{false, true} {
		t.Run(fmt.Sprintf("reject-baseline-%t", reject), func(t *testing.T) {
			testProtectedMetricsWorkerRealSessionArtifacts(t, reject)
		})
	}
}

func testProtectedMetricsWorkerRealSessionArtifacts(t *testing.T, reject bool) {
	library, err := filepath.Abs("protected-stack-session.sh")
	require.NoError(t, err)
	owner := t.TempDir()
	require.NoError(t, os.Mkdir(filepath.Join(owner, "deployment-claimed"), 0700))
	require.NoError(t, os.Mkdir(filepath.Join(owner, "bin"), 0700))
	probe := strings.Replace(metricsProbeFixture(), `printf 'x 1\n' > "$2"`, `value=0
if [[ -e $stack_owner/baseline-written ]]; then value=1; else touch "$stack_owner/baseline-written"; fi
printf '# TYPE leader_retirement_peer_result counter\nleader_retirement_peer_result{cluster="test",outcome="confirmed"} %s\n' "$value" > "$2"`, 1)
	require.NotEqual(t, metricsProbeFixture(), probe)
	require.NoError(t, os.WriteFile(filepath.Join(owner, "bin", "info-diagnostic-probe"), []byte(probe), 0700))
	// Reuse only the Kubernetes/transport fixtures, not the session exercise.
	setup, _, ok := strings.Cut(stackSessionFixture, "trap stack_session_close EXIT")
	require.True(t, ok)
	// Claim exists before the supervisor starts; the fixture must not recreate it.
	require.Contains(t, setup, `mkdir "$stack_owner/deployment-claimed"`)
	setup = strings.Replace(setup, `mkdir "$stack_owner/deployment-claimed"`, `test -d "$stack_owner/deployment-claimed"`, 1)
	setup += `
sha256sum "$stack_library_dir/protected-metrics-worker.sh" >> "$stack_owner/tools.sha256"
export stack_kubeconfig stack_context stack_namespace stack_namespace_uid stack_sts stack_sts_uid stack_tls stack_server_name
export -f kubectl timeout ss openssl
unset stack_info_port stack_anonymous_port
exec bash "$stack_library_dir/protected-metrics-worker.sh" brain-0 0
`
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	stderr, err := os.CreateTemp(owner, "supervisor-stderr-*")
	require.NoError(t, err)
	defer stderr.Close()
	var before retirementmetrics.Sample
	var binding retirementmetrics.CaptureBinding
	var origin int64
	var workerDir string
	baselineChecked, completedChecked := false, false
	results, runErr := metricsworker.Run(ctx, owner, []metricsworker.Command{{
		Executable: "/bin/bash", Args: []string{"-c", setup, "test", library, owner, "success"}, Env: os.Environ(), Stderr: stderr,
	}}, metricsworker.Hooks{
		Baseline: func(_ context.Context, index int, r metricsworker.Ready) error {
			workerDir = r.Worker
			require.Equal(t, 0, index)
			ready := []string{"READY", r.Worker, r.Baseline}
			require.Equal(t, owner, filepath.Dir(ready[1]))
			require.Equal(t, owner, filepath.Dir(ready[2]))
			raw, err := os.ReadFile(filepath.Join(owner, "diagnostic-spec.json"))
			require.NoError(t, err)
			var spec map[string]any
			require.NoError(t, json.Unmarshal(raw, &spec))
			canonical, err := json.Marshal(spec)
			require.NoError(t, err)
			hash := sha256.Sum256(canonical)
			binding = retirementmetrics.CaptureBinding{NamespaceUID: "ns-uid", StatefulSetUID: "sts-uid", PodUID: "pod-uid", SpecSHA256: hex.EncodeToString(hash[:]), Cluster: "test"}
			before, err = retirementmetrics.LoadCapture(ready[2], binding)
			require.NoError(t, err)
			baselineChecked = true
			if reject {
				return fmt.Errorf("deliberate baseline rejection")
			}
			return nil
		},
		Inject: func(context.Context) (time.Time, error) {
			require.True(t, baselineChecked)
			now := time.Now()
			origin = now.UnixNano()
			return now, nil // Clock delivery only: no real Kubernetes fault injected.
		},
		Completed: func(_ context.Context, index int, result metricsworker.Result, fault time.Time) error {
			require.Equal(t, 0, index)
			require.Equal(t, origin, fault.UnixNano())
			c := result.Captured
			ready := []string{"READY", result.Ready.Worker, result.Ready.Baseline}
			captured := []string{"CAPTURED", c.Capture, c.Schedule}
			after, err := retirementmetrics.LoadCapture(captured[1], binding)
			require.NoError(t, err)
			delta, err := retirementmetrics.SampleDelta(before, after, retirementmetrics.Key{Stage: "peer", Outcome: "confirmed"})
			require.NoError(t, err)
			require.Equal(t, float64(1), delta)
			input, err := os.ReadFile(filepath.Join(captured[2], "input.tsv"))
			require.NoError(t, err)
			require.Equal(t, fmt.Sprintf("%d\t0\n", origin), string(input))
			check, err := exec.Command("sha256sum", "-c", filepath.Join(captured[2], "evidence.sha256")).CombinedOutput()
			require.NoError(t, err, string(check))
			complete, err := os.ReadFile(filepath.Join(captured[2], "COMPLETE"))
			require.NoError(t, err)
			require.Equal(t, "SCHEDULE_COMPLETE_NOT_FAULT_ACCEPTANCE\n", string(complete))
			receipt, err := os.ReadFile(filepath.Join(ready[1], "exit-code"))
			require.NoError(t, err)
			require.Equal(t, "0\n", string(receipt))
			completedChecked = true
			return nil
		},
	})
	log, err := os.ReadFile(stderr.Name())
	require.NoError(t, err)
	if reject {
		require.ErrorContains(t, runErr, "deliberate baseline rejection")
		require.Zero(t, origin, "must not inject on a rejected baseline")
		require.False(t, completedChecked)
		receipt, err := os.ReadFile(filepath.Join(workerDir, "exit-code"))
		require.NoError(t, err, string(log))
		require.Equal(t, "143\n", string(receipt))
	} else {
		require.NoError(t, runErr, string(log))
		require.Len(t, results, 1)
		require.True(t, completedChecked)
	}
	require.NoError(t, ctx.Err())
	pids, err := os.ReadFile(filepath.Join(owner, "pids"))
	require.NoError(t, err)
	require.Len(t, strings.Fields(string(pids)), 3)
	for _, pid := range strings.Fields(string(pids)) {
		require.Error(t, exec.Command("kill", "-0", pid).Run(), "forward survived worker exit")
	}
}

func TestProtectedMetricsWorkerHandshakeAndCleanup(t *testing.T) {
	worker, err := os.ReadFile("protected-metrics-worker.sh")
	require.NoError(t, err)
	for _, scenario := range []string{"success", "fragmented", "late-control", "baseline-failed", "capture-failed", "missing-binding", "eof", "malformed", "future", "terminate", "hold-after-baseline", "hold-while-waiting", "terminal-while-waiting"} {
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
			t.Cleanup(func() {
				if cmd.ProcessState == nil {
					cancel()
					_ = cmd.Wait()
				}
			})
			reader := bufio.NewReader(stdout)
			ready, readErr := reader.ReadString('\n')
			if scenario == "baseline-failed" || scenario == "missing-binding" || scenario == "hold-after-baseline" {
				require.Error(t, readErr)
				require.Empty(t, ready)
			} else {
				require.NoError(t, readErr)
				require.True(t, strings.HasPrefix(ready, "READY\t"), ready)
				if scenario == "hold-while-waiting" || scenario == "terminal-while-waiting" {
					marker := "HOLD"
					if scenario == "terminal-while-waiting" {
						marker = "final-exit-code"
					}
					require.NoError(t, os.WriteFile(filepath.Join(owner, marker), []byte("stop\n"), 0600))
				} else if scenario == "terminate" {
					require.NoError(t, syscall.Kill(-cmd.Process.Pid, syscall.SIGTERM))
				} else if scenario != "eof" && scenario != "late-control" {
					origin := fmt.Sprintf("%d\n", time.Now().UnixNano())
					if scenario == "malformed" {
						origin = "18000000000000000000\n"
					}
					if scenario == "future" {
						origin = fmt.Sprintf("%d\n", time.Now().Add(time.Hour).UnixNano())
					}
					if scenario == "fragmented" {
						_, err = io.WriteString(stdin, origin[:9])
						require.NoError(t, err)
						time.Sleep(1200 * time.Millisecond)
						origin = origin[9:]
					}
					_, err = io.WriteString(stdin, origin)
					require.NoError(t, err)
				}
			}
			if scenario != "hold-while-waiting" && scenario != "terminal-while-waiting" {
				_ = stdin.Close()
			}
			rest, err := io.ReadAll(reader)
			require.NoError(t, err)
			waitErr := cmd.Wait()
			_ = stdin.Close()
			require.NoError(t, ctx.Err(), stderr.String())
			if scenario == "success" || scenario == "fragmented" {
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
			if scenario == "success" || scenario == "fragmented" {
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
if [[ $worker_scenario == late-control ]]; then
 read() { worker_piece=$(date -u +%s%N); SECONDS=$worker_input_deadline; return 0; }
fi
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
stack_session_close() {
 local child n
 for child in "${stack_pids[@]}"; do
  kill -TERM "$child" 2>/dev/null || true
  for n in {1..20}; do
   [[ $'\n'$(jobs -pr)$'\n' == *$'\n'"$child"$'\n'* ]] || break
   sleep 0.01
  done
  if [[ $'\n'$(jobs -pr)$'\n' == *$'\n'"$child"$'\n'* ]]; then kill -KILL "$child" 2>/dev/null || true; fi
  wait "$child" 2>/dev/null || true
 done
}
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
