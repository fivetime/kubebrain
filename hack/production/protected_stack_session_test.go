package production_test

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/kubewharf/kubebrain/hack/production/internal/retirementmetrics"
	"github.com/stretchr/testify/require"
)

func TestProtectedStackSession(t *testing.T) {
	library, err := filepath.Abs("protected-stack-session.sh")
	require.NoError(t, err)
	for _, scenario := range []string{"success", "anonymous-reset", "repeated-anonymous-reset", "rearm-occupied", "rearm-start-failed", "ready-before", "ready-after", "restart-before", "restart-after", "wrong-namespace", "wrong-sts", "wrong-spec", "probe-failed", "wrong-mode", "occupied-port", "dead-channel", "expired", "future", "reset-origin", "reset-budget", "slow-probe", "consumed", "missing-binding", "tampered-probe"} {
		t.Run(scenario, func(t *testing.T) {
			owner := t.TempDir()
			require.NoError(t, os.Mkdir(filepath.Join(owner, "bin"), 0700))
			require.NoError(t, os.WriteFile(filepath.Join(owner, "bin", "info-diagnostic-probe"), []byte(stackProbeFixture), 0700))
			ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
			defer cancel()
			cmd := exec.CommandContext(ctx, "bash", "-c", stackSessionFixture, "test", library, owner, scenario)
			output, err := cmd.CombinedOutput()
			require.NoError(t, ctx.Err(), string(output))
			require.NoError(t, err, string(output))
			require.Contains(t, string(output), "EXPECTED_RESULT_AND_CLEANUP")
			captures, err := filepath.Glob(filepath.Join(owner, "stack.*", "timing.tsv"))
			require.NoError(t, err)
			if scenario == "success" || scenario == "probe-failed" || scenario == "slow-probe" {
				require.NotEmpty(t, captures)
				for _, path := range captures {
					data, err := os.ReadFile(path)
					require.NoError(t, err)
					trace := string(data)
					require.Contains(t, trace, "\tverify-inputs\tend\t0\n")
					require.Contains(t, trace, "\tprotected-probe\tstart\t-\n")
					switch scenario {
					case "success":
						require.Contains(t, trace, "\tidentity-after\tend\t0\n")
					case "probe-failed":
						require.Contains(t, trace, "\tprotected-probe\tend\t17\n")
						require.NotContains(t, trace, "\tsnapshot-after\t")
					case "slow-probe":
						require.Contains(t, trace, "\tprotected-probe\tend\t124\n")
						require.NotContains(t, trace, "\tsnapshot-after\t")
					}
				}
			}
		})
	}
}

const stackProbeFixture = `#!/usr/bin/env bash
set -eu
touch "$stack_owner/probe-called"
[[ $scenario != probe-failed ]] || exit 17
if [[ $scenario == slow-probe || $scenario == cancel-group ]]; then
 printf '%s\n' "$BASHPID" >> "$stack_owner/pids"
 exec /usr/bin/sleep 60
fi
while [[ $1 != --stack-output ]]; do shift; done
printf 'synthetic stack\n' > "$2"
mode=protected-stack
[[ $scenario != wrong-mode ]] || mode=other
printf '{"mode":"%s","readiness_checked":false,"fault_acceptance_proven":false,"pod_identity_proven":false}\n' "$mode"
`

func TestProtectedMetricsSession(t *testing.T) {
	library, err := filepath.Abs("protected-stack-session.sh")
	require.NoError(t, err)
	probe := metricsProbeFixture()
	testProtectedMetricsSession(t, library, probe)
}

func metricsProbeFixture() string {
	probe := strings.ReplaceAll(stackProbeFixture, "--stack-output", "--metrics-output")
	probe = strings.ReplaceAll(probe, "set -eu", "set -eu\nstarted=$(date -u +%Y-%m-%dT%H:%M:%S.%NZ)")
	probe = strings.ReplaceAll(probe, "synthetic stack", "x 1")
	probe = strings.ReplaceAll(probe, "mode=protected-stack", "mode=protected-metrics")
	probe = strings.ReplaceAll(probe, `printf '{"mode":"%s","readiness_checked":false,"fault_acceptance_proven":false,"pod_identity_proven":false}\n' "$mode"`, `
hash=$(sha256sum "$2"); size=$(stat -c %s "$2")
[[ $scenario != bad-hash ]] || hash=bad
[[ $scenario != bad-size ]] || size=$((size+1))
[[ $scenario != changed-body ]] || printf 'x 2\n' >> "$2"
printf '{"mode":"%s","readiness_checked":false,"fault_acceptance_proven":false,"pod_identity_proven":false,"metric_semantics_proven":false,"metrics_text_syntax_validated":true,"metrics_bytes":%s,"metrics_sha256":"%s","started":"%s","completed":"%s"}\n' "$mode" "$size" "${hash%% *}" "$started" "$(date -u +%Y-%m-%dT%H:%M:%S.%NZ)"`)
	return probe
}

func testProtectedMetricsSession(t *testing.T, library, probe string) {
	for _, scenario := range []string{"success", "anonymous-reset", "ready-before", "ready-after", "restart-before", "restart-after", "wrong-namespace", "wrong-sts", "wrong-spec", "probe-failed", "wrong-mode", "expired", "reset-origin", "reset-budget", "slow-probe", "consumed", "missing-binding", "tampered-probe", "bad-hash", "bad-size", "changed-body"} {
		t.Run(scenario, func(t *testing.T) {
			owner := t.TempDir()
			require.NoError(t, os.Mkdir(filepath.Join(owner, "bin"), 0700))
			require.NoError(t, os.WriteFile(filepath.Join(owner, "bin", "info-diagnostic-probe"), []byte(probe), 0700))
			ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
			defer cancel()
			fixture := strings.ReplaceAll(stackSessionFixture, "stack_session_capture ", "stack_session_capture_metrics ")
			output, err := exec.CommandContext(ctx, "bash", "-c", fixture, "test", library, owner, scenario).CombinedOutput()
			require.NoError(t, ctx.Err(), string(output))
			require.NoError(t, err, string(output))
			require.Contains(t, string(output), "EXPECTED_RESULT_AND_CLEANUP")
			stacks, err := filepath.Glob(filepath.Join(owner, "metrics.*", "goroutines.txt"))
			require.NoError(t, err)
			require.Empty(t, stacks, "metrics capture must not produce a stack artifact")
			if scenario == "success" {
				raw, err := os.ReadFile(filepath.Join(owner, "diagnostic-spec.json"))
				require.NoError(t, err)
				var spec map[string]any
				require.NoError(t, json.Unmarshal(raw, &spec))
				canonical, err := json.Marshal(spec)
				require.NoError(t, err)
				hash := sha256.Sum256(canonical)
				binding := retirementmetrics.CaptureBinding{NamespaceUID: "ns-uid", StatefulSetUID: "sts-uid", PodUID: "pod-uid", SpecSHA256: hex.EncodeToString(hash[:]), Cluster: "test"}
				captures, err := filepath.Glob(filepath.Join(owner, "metrics.*", "COMPLETE"))
				require.NoError(t, err)
				require.Len(t, captures, 2)
				for _, complete := range captures {
					_, err := retirementmetrics.LoadCapture(filepath.Dir(complete), binding)
					require.NoError(t, err)
				}
			}
		})
	}
}

const stackSessionFixture = `
set -euo pipefail
umask 077
source "$1"
export stack_owner=$2 scenario=$3
stack_kubeconfig=synthetic-config
stack_context=synthetic-context
stack_namespace=test
stack_namespace_uid=ns-uid
stack_sts=brain
stack_sts_uid=sts-uid
stack_tls=$stack_owner
stack_server_name=info.test
mkdir "$stack_owner/deployment-claimed"
[[ $scenario != consumed ]] || touch "$stack_owner/final-exit-code"
spec='{"template":{"spec":{"containers":[{"name":"brain","image":"image@sha256:fixed","args":["--enable-pprof=true","--info-client-cert-auth=true"]}]}}}'
printf '%s\n' "$spec" > "$stack_owner/diagnostic-spec.json"
jq -n --argjson spec "$spec" '{metadata:{namespace:"test",uid:"sts-uid",generation:1},spec:$spec,status:{observedGeneration:1}}' > "$stack_owner/sts.json"
jq -n --argjson spec "$spec" '{apiVersion:"v1",kind:"Pod",metadata:{name:"brain-0",namespace:"test",uid:"pod-uid",ownerReferences:[{kind:"StatefulSet",uid:"sts-uid",controller:true}]},spec:($spec.template.spec+{nodeName:"worker1"}),status:{podIP:"10.0.0.1",containerStatuses:[{name:"brain",containerID:"cri-o://process",imageID:"image@sha256:fixed",restartCount:0,ready:true,state:{running:{startedAt:"2026-09-18T22:49:32Z"}}}]}}' > "$stack_owner/pod.json"
touch "$stack_owner/info.crt"
sha256sum "$stack_owner/diagnostic-spec.json" "$stack_owner/info.crt" > "$stack_owner/diagnostic-inputs.sha256"
sha256sum "$1" "$stack_library_dir/same-pod-process.jq" "$stack_owner/bin/info-diagnostic-probe" > "$stack_owner/tools.sha256"
if [[ $scenario == missing-binding ]]; then
 sha256sum "$stack_owner/bin/info-diagnostic-probe" > "$stack_owner/tools.sha256"
fi
ss() { if [[ $scenario == occupied-port || ( $scenario == rearm-occupied && -e $stack_owner/probe-called ) ]]; then echo occupied; fi; }
openssl() { if [[ $1 == x509 ]]; then printf fake-public-key; else /bin/cat; fi; }
timeout() {
 [[ $1 == --foreground && $2 == --kill-after=1s ]] || return 99
 if [[ $4 == kubectl ]]; then shift 3; "$@"; else
  local rc=0
  /usr/bin/timeout "$@" || rc=$?
  if [[ $4 == "$stack_owner/bin/info-diagnostic-probe" && ( $scenario == anonymous-reset || $scenario == repeated-anonymous-reset ) && $rc == 0 ]]; then
   kill -TERM "${stack_pids[1]}"
   wait "${stack_pids[1]}" || true
  fi
  return "$rc"
 fi
}
kubectl() {
 while [[ $# -gt 0 && $1 != get && $1 != port-forward ]]; do shift; done
 if [[ $1 == port-forward ]]; then
  printf '%s\n' "$BASHPID" >> "$stack_owner/pids"
  if [[ -e $stack_owner/probe-called ]]; then
   [[ $scenario != rearm-start-failed ]] || return 91
   if [[ $scenario == cancel-rearm ]]; then exec /usr/bin/sleep 60; fi
  fi
  printf 'Forwarding from 127.0.0.1:%s -> 8080\n' "${4%:*}"
  exec /usr/bin/sleep 60
 fi
 local filter=.
 case $2 in
  namespace)
   if [[ $scenario == wrong-namespace ]]; then printf '{"metadata":{"uid":"wrong"}}'
   else printf '{"metadata":{"name":"test","uid":"ns-uid"}}'; fi;;
  sts)
   [[ $scenario != wrong-sts ]] || filter='.metadata.uid="wrong"'
   [[ $scenario != wrong-spec ]] || filter='.spec.template.spec.containers[0].image="other"'
   jq "$filter" "$stack_owner/sts.json";;
  pod)
   if [[ -e $stack_owner/capture-started ]]; then
    [[ $scenario != ready-before ]] || filter='.status.containerStatuses[0].ready=false'
    [[ $scenario != restart-before ]] || filter='.status.containerStatuses[0].restartCount=1'
   fi
   if [[ -e $stack_owner/probe-called ]]; then
    [[ $scenario != ready-after ]] || filter='.status.containerStatuses[0].ready=false'
    [[ $scenario != restart-after ]] || filter='.status.containerStatuses[0].restartCount=1'
   fi
   jq "$filter" "$stack_owner/pod.json";;
  *) return 91;;
 esac
}
trap stack_session_close EXIT
trap 'exit 143' TERM
trap 'exit 130' INT
exercise() {
 stack_session_prepare brain-0 || return
 if [[ $scenario == tampered-probe ]]; then printf '# changed\n' >> "$stack_owner/bin/info-diagnostic-probe"; fi
 touch "$stack_owner/capture-started"
 if [[ $scenario == dead-channel ]]; then
  kill -TERM "${stack_pids[0]}"
  wait "${stack_pids[0]}" || true
 fi
 local start=before-fault
 case $scenario in
  expired) start=$(($(date -u +%s%N)-30000000001));;
  future) start=$(($(date -u +%s%N)+30000000000));;
  slow-probe) start=$(($(date -u +%s%N)-28000000000));;
 esac
 stack_session_capture "$start" || return
 [[ -s $stack_capture/COMPLETE ]] || return
 sha256sum -c "$stack_capture/evidence.sha256" >/dev/null || return
 first=$stack_capture
 local expected_forwards=3 authenticated_pid=${stack_pids[0]}
 if [[ $scenario == repeated-anonymous-reset ]]; then
  stack_session_capture before-fault || return
  [[ -s $stack_capture/COMPLETE && $stack_capture != "$first" && ${stack_pids[0]} == "$authenticated_pid" ]] || return
  sha256sum -c "$stack_capture/evidence.sha256" >/dev/null || return
  [[ $(awk '$2=="rearm-anonymous" && $3=="end" && $4==0 {n++} END {print n+0}' "$stack_capture/timing.tsv") == 1 ]] || return
  expected_forwards=4
 fi
 start=$(date -u +%s%N)
 stack_session_capture "$start" || return
 [[ $first != "$stack_capture" && $(wc -l < "$stack_owner/pids") == "$expected_forwards" && ${stack_pids[0]} == "$authenticated_pid" && $stack_fault_start == "$start" ]] || return
 [[ -s $stack_capture/COMPLETE ]] || return
 sha256sum -c "$stack_capture/evidence.sha256" >/dev/null || return
 # Fault-time capture must not create another tunnel or reset its clock.
 [[ $(awk '$2=="rearm-anonymous" && $3=="end" && $4==0 {n++} END {print n+0}' "$first/timing.tsv") == 1 ]] || return
 ! rg -q rearm-anonymous "$stack_capture/timing.tsv" || return
 case $scenario in
  reset-origin) stack_session_capture "$((start+1))";;
  reset-budget) stack_session_capture before-fault;;
 esac
}
rc=0
exercise || rc=$?
stack_session_close
case $scenario in
 success|anonymous-reset|repeated-anonymous-reset|ready-before|ready-after) [[ $rc == 0 ]];;
 expired|slow-probe) [[ $rc == 124 ]];;
 *) [[ $rc != 0 ]];;
esac
case $scenario in
 success|anonymous-reset|repeated-anonymous-reset|ready-before|ready-after|reset-origin|reset-budget) ;;
 *) [[ -z $(find "$stack_owner" -name COMPLETE -print) ]];;
esac
if [[ -e $stack_owner/pids ]]; then
 while read -r pid; do
  if kill -0 "$pid" 2>/dev/null; then echo 'forward survived' >&2; exit 1; fi
 done < "$stack_owner/pids"
fi
printf 'EXPECTED_RESULT_AND_CLEANUP\n'
`

func TestProtectedStackSessionRejectsMultiplePodDocuments(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "pods.json")
	require.NoError(t, os.WriteFile(path, []byte("{}\n{}\n"), 0600))
	cmd := exec.Command("bash", "-c", `source ./protected-stack-session.sh; stack_session_same_process "$1" "$1"`, "test", path)
	output, err := cmd.CombinedOutput()
	require.Error(t, err)
	require.True(t, strings.Contains(string(output), "one Pod per file required"), string(output))
}

func TestProtectedStackSessionOuterCancellation(t *testing.T) {
	testProtectedSessionOuterCancellation(t, "stack", stackProbeFixture, stackSessionFixture)
}

func TestProtectedMetricsSessionOuterCancellation(t *testing.T) {
	fixture := strings.ReplaceAll(stackSessionFixture, "stack_session_capture ", "stack_session_capture_metrics ")
	testProtectedSessionOuterCancellation(t, "metrics", metricsProbeFixture(), fixture)
}

func testProtectedSessionOuterCancellation(t *testing.T, kind, probe, fixture string) {
	t.Helper()
	library, err := filepath.Abs("protected-stack-session.sh")
	require.NoError(t, err)
	owner := t.TempDir()
	require.NoError(t, os.Mkdir(filepath.Join(owner, "bin"), 0700))
	require.NoError(t, os.WriteFile(filepath.Join(owner, "bin", "info-diagnostic-probe"), []byte(probe), 0700))
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, "timeout", "--kill-after=2s", "3s", "bash", "-c", fixture, "test", library, owner, "cancel-group")
	output, err := cmd.CombinedOutput()
	require.NoError(t, ctx.Err(), string(output))
	var exit *exec.ExitError
	require.ErrorAs(t, err, &exit, string(output))
	require.Equal(t, 124, exit.ExitCode(), string(output))
	pids, err := os.ReadFile(filepath.Join(owner, "pids"))
	require.NoError(t, err)
	require.Len(t, strings.Fields(string(pids)), 3, "two forwards and the blocked probe")
	for _, pid := range strings.Fields(string(pids)) {
		require.Error(t, exec.Command("kill", "-0", pid).Run(), "child survived: %s", pid)
	}
	markers, err := filepath.Glob(filepath.Join(owner, kind+".*", "COMPLETE"))
	require.NoError(t, err)
	require.Empty(t, markers)
	traces, err := filepath.Glob(filepath.Join(owner, kind+".*", "timing.tsv"))
	require.NoError(t, err)
	require.Len(t, traces, 1)
	trace, err := os.ReadFile(traces[0])
	require.NoError(t, err)
	require.Contains(t, string(trace), "\tprotected-probe\tstart\t-\n")
	require.NotContains(t, string(trace), "\tsnapshot-after\t")
}

func TestProtectedStackSessionRearmCancellation(t *testing.T) {
	library, err := filepath.Abs("protected-stack-session.sh")
	require.NoError(t, err)
	owner := t.TempDir()
	require.NoError(t, os.Mkdir(filepath.Join(owner, "bin"), 0700))
	require.NoError(t, os.WriteFile(filepath.Join(owner, "bin", "info-diagnostic-probe"), []byte(stackProbeFixture), 0700))
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	output, err := exec.CommandContext(ctx, "timeout", "--kill-after=2s", "3s", "bash", "-c", stackSessionFixture, "test", library, owner, "cancel-rearm").CombinedOutput()
	require.NoError(t, ctx.Err(), string(output))
	var exit *exec.ExitError
	require.ErrorAs(t, err, &exit, string(output))
	require.Equal(t, 124, exit.ExitCode())
	pids, err := os.ReadFile(filepath.Join(owner, "pids"))
	require.NoError(t, err)
	require.Len(t, strings.Fields(string(pids)), 3, "original two tunnels and replacement anonymous tunnel")
	for _, pid := range strings.Fields(string(pids)) {
		require.Error(t, exec.Command("kill", "-0", pid).Run(), "child survived: %s", pid)
	}
	markers, err := filepath.Glob(filepath.Join(owner, "stack.*", "COMPLETE"))
	require.NoError(t, err)
	require.Empty(t, markers)
	traces, err := filepath.Glob(filepath.Join(owner, "stack.*", "timing.tsv"))
	require.NoError(t, err)
	require.Len(t, traces, 1)
	trace, err := os.ReadFile(traces[0])
	require.NoError(t, err)
	require.Contains(t, string(trace), "\trearm-anonymous\tstart\t-\n")
}
