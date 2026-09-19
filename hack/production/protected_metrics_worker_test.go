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
	for _, count := range []int{1, 2} {
		for _, reject := range []bool{false, true} {
			t.Run(fmt.Sprintf("workers-%d-reject-baseline-%t", count, reject), func(t *testing.T) {
				testProtectedMetricsWorkerRealSessionArtifacts(t, count, reject)
			})
		}
	}
}

func testProtectedMetricsWorkerRealSessionArtifacts(t *testing.T, count int, reject bool) {
	library, err := filepath.Abs("protected-stack-session.sh")
	require.NoError(t, err)
	owner := t.TempDir()
	require.NoError(t, os.Mkdir(filepath.Join(owner, "deployment-claimed"), 0700))
	require.NoError(t, os.Mkdir(filepath.Join(owner, "bin"), 0700))
	probe := strings.Replace(metricsProbeFixture(), `printf 'x 1\n' > "$2"`, `value=0
if [[ -e $stack_owner/baseline-written-$stack_info_port ]]; then value=1; else touch "$stack_owner/baseline-written-$stack_info_port"; fi
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
declare -f kubectl timeout ss openssl
declare -p stack_owner scenario stack_library_dir stack_kubeconfig stack_context stack_namespace stack_namespace_uid stack_sts stack_sts_uid stack_tls stack_server_name
printf '%s\n' 'export stack_owner scenario stack_kubeconfig stack_context stack_namespace stack_namespace_uid stack_sts stack_sts_uid stack_tls stack_server_name' 'export -f kubectl timeout ss openssl'
`
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	// Create shared immutable fixture files once, before starting workers. This
	// shell never prepares a session or owns any port-forward jobs.
	prepare := exec.CommandContext(ctx, "/bin/bash", "-c", setup, "test", library, owner, "success")
	processgroup.Configure(prepare)
	prepare.WaitDelay = time.Second
	bootstrap, err := prepare.Output()
	require.NoError(t, err)
	workerSetup := "set -euo pipefail\n" + string(bootstrap) + `
export stack_info_port=$1 stack_anonymous_port=$2
exec bash "$stack_library_dir/protected-metrics-worker.sh" brain-0 "$3"
`
	stderr, err := os.CreateTemp(owner, "supervisor-stderr-*")
	require.NoError(t, err)
	defer stderr.Close()
	faultLog, err := os.CreateTemp(owner, "fault-command-*")
	require.NoError(t, err)
	defer faultLog.Close()
	before := make([]retirementmetrics.Sample, count)
	var binding retirementmetrics.CaptureBinding
	var origin int64
	workerDirs := make([]string, count)
	baselineDirs := make([]string, count)
	baselineChecked, completedChecked, injected := 0, 0, false
	commands := make([]metricsworker.Command, count)
	for i := range commands {
		commands[i] = metricsworker.Command{Executable: "/bin/bash", Args: []string{"-c", workerSetup, "worker", fmt.Sprint(18586 + 2*i), fmt.Sprint(18587 + 2*i), fmt.Sprint(i * 100000000)}, Env: os.Environ(), Stderr: stderr}
	}
	results, runErr := metricsworker.Run(ctx, owner, commands, metricsworker.Hooks{
		Inject: func(ctx context.Context, fault time.Time) error {
			require.Equal(t, count, baselineChecked)
			require.Equal(t, origin, fault.UnixNano())
			injected = true
			// A synthetic external fault callback waits for the real worker
			// scripts' scheduled artifacts. This proves workers can progress
			// before Inject returns; no Kubernetes fault is executed.
			return metricsworker.RunFaultCommand(ctx, metricsworker.Command{
				Executable: "/bin/bash", Stderr: faultLog,
				Args: []string{"-c", `set -eu
printf '%s\n' "$3" "$$"
while true; do
 seen=0
 for marker in "$1"/metrics-schedule.*/COMPLETE; do
  if [[ -f $marker ]]; then seen=$((seen+1)); fi
 done
 if (( seen == $2 )); then break; fi
 /bin/sleep 0.01
done
printf 'workers-captured-before-inject-return\n'
`, "synthetic-fault", owner, fmt.Sprint(count)},
			}, fault)
		},
		Baseline: func(_ context.Context, index int, r metricsworker.Ready) error {
			require.GreaterOrEqual(t, index, 0)
			require.Less(t, index, count)
			workerDirs[index] = r.Worker
			baselineDirs[index] = r.Baseline
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
			before[index], err = retirementmetrics.LoadCapture(ready[2], binding)
			require.NoError(t, err)
			baselineChecked++
			if reject && baselineChecked == count {
				return fmt.Errorf("deliberate baseline rejection")
			}
			return nil
		},
		Origin: func(context.Context) (time.Time, error) {
			require.Equal(t, count, baselineChecked)
			now := time.Now()
			origin = now.UnixNano()
			for _, baseline := range baselineDirs {
				_, err := retirementmetrics.LoadPrefaultCapture(baseline, binding, now)
				require.NoError(t, err)
			}
			return now, nil // Clock delivery only: no real Kubernetes fault injected.
		},
		Completed: func(_ context.Context, index int, result metricsworker.Result, fault time.Time) error {
			require.True(t, injected)
			require.GreaterOrEqual(t, index, 0)
			require.Less(t, index, count)
			require.Equal(t, origin, fault.UnixNano())
			c := result.Captured
			ready := []string{"READY", result.Ready.Worker, result.Ready.Baseline}
			captured := []string{"CAPTURED", c.Capture, c.Schedule}
			offset := time.Duration(index) * 100 * time.Millisecond
			after, err := retirementmetrics.LoadScheduledCapture(captured[2], captured[1], binding, fault, offset)
			require.NoError(t, err)
			delta, err := retirementmetrics.SampleDelta(before[index], after, retirementmetrics.Key{Stage: "peer", Outcome: "confirmed"})
			require.NoError(t, err)
			require.Equal(t, float64(1), delta)
			input, err := os.ReadFile(filepath.Join(captured[2], "input.tsv"))
			require.NoError(t, err)
			require.Equal(t, fmt.Sprintf("%d\t%d\n", origin, int64(offset)), string(input))
			check, err := exec.Command("sha256sum", "-c", filepath.Join(captured[2], "evidence.sha256")).CombinedOutput()
			require.NoError(t, err, string(check))
			complete, err := os.ReadFile(filepath.Join(captured[2], "COMPLETE"))
			require.NoError(t, err)
			require.Equal(t, "SCHEDULE_COMPLETE_NOT_FAULT_ACCEPTANCE\n", string(complete))
			receipt, err := os.ReadFile(filepath.Join(ready[1], "exit-code"))
			require.NoError(t, err)
			require.Equal(t, "0\n", string(receipt))
			completedChecked++
			return nil
		},
	})
	log, err := os.ReadFile(stderr.Name())
	require.NoError(t, err)
	faultOutput, err := os.ReadFile(faultLog.Name())
	require.NoError(t, err)
	if reject {
		require.ErrorContains(t, runErr, "deliberate baseline rejection")
		require.Zero(t, origin, "must not inject on a rejected baseline")
		require.False(t, injected)
		require.Zero(t, completedChecked)
		require.Empty(t, faultOutput, "rejected baseline must not start external command")
		for _, workerDir := range workerDirs {
			receipt, err := os.ReadFile(filepath.Join(workerDir, "exit-code"))
			require.NoError(t, err, string(log))
			require.Equal(t, "143\n", string(receipt))
		}
	} else {
		require.NoError(t, runErr, string(log))
		require.Len(t, results, count)
		require.Equal(t, count, completedChecked)
		lines := strings.Split(strings.TrimSpace(string(faultOutput)), "\n")
		require.Len(t, lines, 3)
		require.Equal(t, fmt.Sprint(origin), lines[0])
		require.Regexp(t, `^[1-9][0-9]*$`, lines[1])
		require.Equal(t, "workers-captured-before-inject-return", lines[2])
		_, statErr := os.Stat("/proc/" + lines[1])
		require.True(t, os.IsNotExist(statErr), "fault command survived supervisor return")
		if count == 2 {
			require.NotEqual(t, results[0].Ready.Worker, results[1].Ready.Worker)
			require.NotEqual(t, results[0].Captured.Capture, results[1].Captured.Capture)
		}
	}
	require.NoError(t, ctx.Err())
	pids, err := os.ReadFile(filepath.Join(owner, "pids"))
	require.NoError(t, err)
	require.Len(t, strings.Fields(string(pids)), 3*count)
	sessions, err := filepath.Glob(filepath.Join(owner, "stack-session.*"))
	require.NoError(t, err)
	require.Len(t, sessions, count)
	for port := 18586; port < 18586+2*count; port++ {
		logs, err := filepath.Glob(filepath.Join(owner, "stack-session.*", fmt.Sprintf("forward-%d.log", port)))
		require.NoError(t, err)
		require.Len(t, logs, 1, "each configured port belongs to exactly one session")
	}
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
