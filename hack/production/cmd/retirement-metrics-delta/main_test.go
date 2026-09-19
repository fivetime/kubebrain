package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestRejectInvalidArgumentsWithoutSuccess(t *testing.T) {
	for _, args := range [][]string{nil, {"--stage", "other"}, {"--before", "/missing", "--after", "/missing", "--stage", "peer", "--outcome", "confirmed"}, {"--unknown"}} {
		var output bytes.Buffer
		require.Error(t, run(args, &output))
		require.Empty(t, output.String())
	}
}

func TestScheduledArgumentsNeverFallBack(t *testing.T) {
	base := []string{"--before", "/missing", "--after", "/missing", "--stage", "peer", "--outcome", "confirmed"}
	for _, extra := range [][]string{
		{"--schedule="}, {"--fault-origin-ns=1800000010000000000"}, {"--offset-ns=0"},
		{"--schedule=/missing", "--fault-origin-ns=1800000010000000000"},
		{"--schedule=/missing", "--fault-origin-ns=+1800000010000000000", "--offset-ns=0"},
		{"--schedule=/missing", "--fault-origin-ns=1800000010000000000", "--offset-ns=00"},
		{"--schedule=/missing", "--fault-origin-ns=1800000010000000000", "--offset-ns=-1"},
		{"--schedule=/missing", "--fault-origin-ns=1800000010000000000", "--offset-ns=30000000000"},
		{"--schedule=/missing", "--fault-origin-ns=9223372036854775808", "--offset-ns=0"},
	} {
		t.Run(strings.Join(extra, " "), func(t *testing.T) {
			var output bytes.Buffer
			err := run(append(append([]string{}, base...), extra...), &output)
			require.ErrorContains(t, err, "scheduled mode requires")
			require.Empty(t, output.String())
		})
	}
}

// Run the real main entrypoint in a child test process to cover stdout and exit
// status together. These are synthetic artifacts, not live cluster evidence.
func TestCLIProcessHelper(t *testing.T) {
	if os.Getenv("KUBEBRAIN_RETIREMENT_CLI_TEST") != "1" {
		return
	}
	for i, arg := range os.Args {
		if arg == "--" {
			os.Args = append([]string{"retirement-metrics-delta"}, os.Args[i+1:]...)
			main()
			os.Exit(0)
		}
	}
	os.Exit(2)
}

func TestCLICompletedCapturePair(t *testing.T) {
	for _, scenario := range []string{"success", "duration-success", "duration-missing", "missing-baseline", "reset", "changed-process", "reversed", "overlap", "tampered", "incomplete", "wrong-admission", "wrong-cluster", "missing-cluster", "scheduled-success", "scheduled-duration-success", "scheduled-wrong-clock", "scheduled-wrong-offset", "scheduled-early", "scheduled-late", "scheduled-late-baseline", "scheduled-incomplete", "scheduled-tampered"} {
		t.Run(scenario, func(t *testing.T) {
			baseline := 2
			if scenario == "missing-baseline" {
				baseline = -1
			}
			withDuration := strings.HasSuffix(scenario, "duration-success")
			beforeTime := int64(1800000000)
			if scenario == "scheduled-late-baseline" {
				// Probe itself finishes before origin, but final identity stages
				// finish afterward. The pair otherwise remains nonoverlapping.
				beforeTime += 8
			}
			before, specHash := cliCaptureFixture(t, beforeTime, baseline, "process", withDuration)
			afterCount, afterTime, afterProcess := 5, int64(1800000020), "process"
			if scenario == "reset" {
				afterCount = 1
			}
			if scenario == "changed-process" {
				afterProcess = "other-process"
			}
			if scenario == "overlap" {
				afterTime = 1800000000
			}
			if scenario == "scheduled-late" {
				afterTime = 1800000040
			}
			after, _ := cliCaptureFixture(t, afterTime, afterCount, afterProcess, withDuration)
			if scenario == "reversed" {
				before, after = after, before
			}
			if scenario == "tampered" {
				require.NoError(t, os.WriteFile(filepath.Join(after, "metrics.txt"), []byte("unrelated 9\n"), 0600))
			}
			if scenario == "incomplete" {
				require.NoError(t, os.Remove(filepath.Join(after, "COMPLETE")))
			}
			podUID := "pod"
			if scenario == "wrong-admission" {
				podUID = "wrong"
			}
			cluster := "test"
			if scenario == "wrong-cluster" {
				cluster = "other"
			}
			if scenario == "missing-cluster" {
				cluster = ""
			}
			args := []string{"-test.run=^TestCLIProcessHelper$", "--", "--before", before, "--after", after, "--namespace-uid", "ns-uid", "--sts-uid", "sts-uid", "--pod-uid", podUID, "--spec-sha256", specHash, "--cluster", cluster, "--stage", "peer", "--outcome", "confirmed"}
			if strings.HasPrefix(scenario, "scheduled-") {
				origin, offset := "1800000010000000000", "4000000000"
				if scenario == "scheduled-early" {
					offset = "15000000000"
				}
				schedule := cliScheduleFixture(t, after, origin, offset)
				if scenario == "scheduled-wrong-clock" {
					origin = "1800000010000000001"
				}
				if scenario == "scheduled-wrong-offset" {
					offset = "4000000001"
				}
				if scenario == "scheduled-incomplete" {
					require.NoError(t, os.Remove(filepath.Join(schedule, "COMPLETE")))
				}
				if scenario == "scheduled-tampered" {
					require.NoError(t, os.WriteFile(filepath.Join(schedule, "evidence.sha256"), []byte("tampered\n"), 0600))
				}
				args = append(args, "--schedule", schedule, "--fault-origin-ns", origin, "--offset-ns", offset)
			}
			if withDuration || scenario == "duration-missing" {
				args = append(args, "--duration")
			}
			binary, err := os.Executable()
			require.NoError(t, err)
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			cmd := exec.CommandContext(ctx, binary, args...)
			cmd.Env = append(os.Environ(), "KUBEBRAIN_RETIREMENT_CLI_TEST=1")
			var stdout, stderr bytes.Buffer
			cmd.Stdout = &stdout
			cmd.Stderr = &stderr
			err = cmd.Run()
			require.NoError(t, ctx.Err())
			if scenario != "success" && !withDuration && scenario != "scheduled-success" {
				var exit *exec.ExitError
				require.ErrorAs(t, err, &exit)
				require.Equal(t, 1, exit.ExitCode())
				require.Empty(t, stdout.String())
				require.NotEmpty(t, stderr.String())
				return
			}
			require.NoError(t, err, stderr.String())
			require.Empty(t, stderr.String())
			if scenario == "scheduled-duration-success" {
				require.JSONEq(t, `{"stage":"peer","outcome":"confirmed","count_delta":3,"duration_delta":{"count":3,"seconds":0.75},"scope":"same_process_retirement_completed_operations","fault_acceptance_proven":false,"successor_readiness_proven":false,"event_latency_proven":false,"scheduled_capture_verified":true,"fault_origin_ns":"1800000010000000000","offset_ns":"4000000000"}`, stdout.String())
				return
			}
			if scenario == "scheduled-success" {
				require.JSONEq(t, `{"stage":"peer","outcome":"confirmed","count_delta":3,"scope":"same_process_retirement_counter_only","fault_acceptance_proven":false,"successor_readiness_proven":false,"event_latency_proven":false,"scheduled_capture_verified":true,"fault_origin_ns":"1800000010000000000","offset_ns":"4000000000"}`, stdout.String())
				return
			}
			if scenario == "duration-success" {
				require.JSONEq(t, `{"stage":"peer","outcome":"confirmed","count_delta":3,"duration_delta":{"count":3,"seconds":0.75},"scope":"same_process_retirement_completed_operations","fault_acceptance_proven":false,"successor_readiness_proven":false,"event_latency_proven":false}`, stdout.String())
				return
			}
			require.JSONEq(t, `{"stage":"peer","outcome":"confirmed","count_delta":3,"scope":"same_process_retirement_counter_only","fault_acceptance_proven":false,"successor_readiness_proven":false,"event_latency_proven":false}`, stdout.String())
		})
	}
}

func cliScheduleFixture(t *testing.T, capture, origin, offset string) string {
	t.Helper()
	dir, err := os.MkdirTemp(filepath.Dir(capture), "schedule.")
	require.NoError(t, err)
	input, path := []byte(origin+"\t"+offset+"\n"), []byte(capture+"\n")
	captureManifest, err := os.ReadFile(filepath.Join(capture, "evidence.sha256"))
	require.NoError(t, err)
	manifest := fmt.Sprintf("%x  %s\n%x  %s\n%x  %s\n", sha256.Sum256(input), filepath.Join(dir, "input.tsv"), sha256.Sum256(path), filepath.Join(dir, "capture-path"), sha256.Sum256(captureManifest), filepath.Join(capture, "evidence.sha256"))
	for name, data := range map[string][]byte{"input.tsv": input, "capture-path": path, "evidence.sha256": []byte(manifest), "COMPLETE": []byte("SCHEDULE_COMPLETE_NOT_FAULT_ACCEPTANCE\n")} {
		require.NoError(t, os.WriteFile(filepath.Join(dir, name), data, 0600))
	}
	return dir
}

func cliCaptureFixture(t *testing.T, started int64, count int, process string, withDuration bool) (string, string) {
	t.Helper()
	dir := t.TempDir()
	raw := []byte(fmt.Sprintf("# TYPE leader_retirement_peer_result counter\nleader_retirement_peer_result{cluster=\"test\",outcome=\"confirmed\"} %d\n", count))
	if count < 0 {
		raw = []byte("unrelated 1\n")
	}
	if withDuration {
		raw = append(raw, []byte(fmt.Sprintf(`# TYPE leader_retirement_peer_duration_seconds histogram
leader_retirement_peer_duration_seconds_bucket{cluster="test",outcome="confirmed",le="1"} %d
leader_retirement_peer_duration_seconds_bucket{cluster="test",outcome="confirmed",le="+Inf"} %d
leader_retirement_peer_duration_seconds_sum{cluster="test",outcome="confirmed"} %g
leader_retirement_peer_duration_seconds_count{cluster="test",outcome="confirmed"} %d
`, count, count, float64(count)*.25, count))...)
	}
	hash := sha256.Sum256(raw)
	summary, err := json.Marshal(map[string]any{"mode": "protected-metrics", "started": time.Unix(started, 0).UTC(), "completed": time.Unix(started+1, 0).UTC(), "metrics_bytes": len(raw), "metrics_sha256": fmt.Sprintf("%x", hash), "metrics_text_syntax_validated": true, "metric_semantics_proven": false, "readiness_checked": false, "pod_identity_proven": false, "fault_acceptance_proven": false})
	require.NoError(t, err)
	spec := `{"template":{"spec":{"containers":[{"image":"fixed","name":"brain"}]}}}`
	specHash := sha256.Sum256([]byte(spec))
	pod := fmt.Sprintf(`{"apiVersion":"v1","kind":"Pod","metadata":{"name":"brain-0","namespace":"test","uid":"pod","ownerReferences":[{"kind":"StatefulSet","uid":"sts-uid","controller":true}]},"spec":{"nodeName":"worker","containers":[{"name":"brain","image":"fixed"}]},"status":{"podIP":"10.0.0.1","containerStatuses":[{"name":"brain","imageID":"image-id","containerID":%q,"restartCount":0,"state":{"running":{"startedAt":"2026-09-19T00:00:00Z"}}}]}}`, process)
	trace := ""
	for i, stage := range []string{"verify-inputs", "snapshot-before", "identity-before", "protected-probe", "snapshot-after", "identity-after"} {
		sec := started + int64(i-3)*2
		trace += fmt.Sprintf("%d.000000\t%s\tstart\t-\n%d.000000\t%s\tend\t0\n", sec, stage, sec+1, stage)
	}
	files := map[string][]byte{"namespace.json": []byte(`{"metadata":{"name":"test","uid":"ns-uid"}}`), "sts.json": []byte(`{"metadata":{"namespace":"test","uid":"sts-uid","generation":1},"status":{"observedGeneration":1},"spec":` + spec + `}`), "pod-before.json": []byte(pod), "pod-after.json": []byte(pod), "metrics.txt": raw, "probe.json": summary, "probe.stderr": {}, "timing.tsv": []byte(trace)}
	manifest := ""
	for name, data := range files {
		path := filepath.Join(dir, name)
		require.NoError(t, os.WriteFile(path, data, 0600))
		sum := sha256.Sum256(data)
		manifest += fmt.Sprintf("%x  %s\n", sum, path)
	}
	require.NoError(t, os.WriteFile(filepath.Join(dir, "evidence.sha256"), []byte(manifest), 0600))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "COMPLETE"), []byte("CAPTURE_COMPLETE_WITHIN_CALLER_BUDGET\n"), 0600))
	return dir, fmt.Sprintf("%x", specHash)
}
