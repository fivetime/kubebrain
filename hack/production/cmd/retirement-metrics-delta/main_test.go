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
	for _, scenario := range []string{"success", "missing-baseline", "reset", "changed-process", "reversed", "overlap", "tampered", "incomplete", "wrong-admission", "wrong-cluster", "missing-cluster"} {
		t.Run(scenario, func(t *testing.T) {
			baseline := 2
			if scenario == "missing-baseline" {
				baseline = -1
			}
			before, specHash := cliCaptureFixture(t, 1800000000, baseline, "process")
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
			after, _ := cliCaptureFixture(t, afterTime, afterCount, afterProcess)
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
			if scenario != "success" {
				var exit *exec.ExitError
				require.ErrorAs(t, err, &exit)
				require.Equal(t, 1, exit.ExitCode())
				require.Empty(t, stdout.String())
				require.NotEmpty(t, stderr.String())
				return
			}
			require.NoError(t, err, stderr.String())
			require.Empty(t, stderr.String())
			require.JSONEq(t, `{"stage":"peer","outcome":"confirmed","count_delta":3,"scope":"same_process_retirement_counter_only","fault_acceptance_proven":false,"successor_readiness_proven":false,"event_latency_proven":false}`, stdout.String())
		})
	}
}

func cliCaptureFixture(t *testing.T, started int64, count int, process string) (string, string) {
	t.Helper()
	dir := t.TempDir()
	raw := []byte(fmt.Sprintf("# TYPE leader_retirement_peer_result counter\nleader_retirement_peer_result{cluster=\"test\",outcome=\"confirmed\"} %d\n", count))
	if count < 0 {
		raw = []byte("unrelated 1\n")
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
