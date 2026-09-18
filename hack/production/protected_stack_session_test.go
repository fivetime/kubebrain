package production_test

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestProtectedStackSession(t *testing.T) {
	library, err := filepath.Abs("protected-stack-session.sh")
	require.NoError(t, err)
	for _, scenario := range []string{"success", "ready-before", "ready-after", "restart-before", "restart-after", "wrong-namespace", "wrong-sts", "wrong-spec", "probe-failed", "wrong-mode", "occupied-port", "dead-channel", "expired", "future", "reset-origin", "reset-budget", "slow-probe", "consumed", "missing-binding", "tampered-probe"} {
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
jq -n --argjson spec "$spec" '{metadata:{uid:"sts-uid",generation:1},spec:$spec,status:{observedGeneration:1}}' > "$stack_owner/sts.json"
jq -n --argjson spec "$spec" '{apiVersion:"v1",kind:"Pod",metadata:{name:"brain-0",namespace:"test",uid:"pod-uid",ownerReferences:[{kind:"StatefulSet",uid:"sts-uid",controller:true}]},spec:($spec.template.spec+{nodeName:"worker1"}),status:{podIP:"10.0.0.1",containerStatuses:[{name:"brain",containerID:"cri-o://process",imageID:"image@sha256:fixed",restartCount:0,ready:true,state:{running:{startedAt:"2026-09-18T22:49:32Z"}}}]}}' > "$stack_owner/pod.json"
touch "$stack_owner/info.crt"
sha256sum "$stack_owner/diagnostic-spec.json" "$stack_owner/info.crt" > "$stack_owner/diagnostic-inputs.sha256"
sha256sum "$1" "$stack_library_dir/same-pod-process.jq" "$stack_owner/bin/info-diagnostic-probe" > "$stack_owner/tools.sha256"
if [[ $scenario == missing-binding ]]; then
 sha256sum "$stack_owner/bin/info-diagnostic-probe" > "$stack_owner/tools.sha256"
fi
ss() { if [[ $scenario == occupied-port ]]; then echo occupied; fi; }
openssl() { if [[ $1 == x509 ]]; then printf fake-public-key; else /bin/cat; fi; }
timeout() {
 [[ $1 == --foreground && $2 == --kill-after=1s ]] || return 99
 if [[ $4 == kubectl ]]; then shift 3; "$@"; else /usr/bin/timeout "$@"; fi
}
kubectl() {
 while [[ $# -gt 0 && $1 != get && $1 != port-forward ]]; do shift; done
 if [[ $1 == port-forward ]]; then
  printf '%s\n' "$BASHPID" >> "$stack_owner/pids"
  printf 'Forwarding from 127.0.0.1:%s -> 8080\n' "${4%:*}"
  exec /usr/bin/sleep 60
 fi
 local filter=.
 case $2 in
  namespace)
   if [[ $scenario == wrong-namespace ]]; then printf '{"metadata":{"uid":"wrong"}}'
   else printf '{"metadata":{"uid":"ns-uid"}}'; fi;;
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
  slow-probe) start=$(($(date -u +%s%N)-29000000000));;
 esac
 stack_session_capture "$start" || return
 [[ -s $stack_capture/COMPLETE ]] || return
 sha256sum -c "$stack_capture/evidence.sha256" >/dev/null || return
 first=$stack_capture
 start=$(date -u +%s%N)
 stack_session_capture "$start" || return
 [[ $first != "$stack_capture" && $(wc -l < "$stack_owner/pids") == 2 ]] || return
 case $scenario in
  reset-origin) stack_session_capture "$((start+1))";;
  reset-budget) stack_session_capture before-fault;;
 esac
}
rc=0
exercise || rc=$?
stack_session_close
case $scenario in
 success|ready-before|ready-after) [[ $rc == 0 ]];;
 expired|slow-probe) [[ $rc == 124 ]];;
 *) [[ $rc != 0 ]];;
esac
case $scenario in
 success|ready-before|ready-after|reset-origin|reset-budget) ;;
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
	library, err := filepath.Abs("protected-stack-session.sh")
	require.NoError(t, err)
	owner := t.TempDir()
	require.NoError(t, os.Mkdir(filepath.Join(owner, "bin"), 0700))
	require.NoError(t, os.WriteFile(filepath.Join(owner, "bin", "info-diagnostic-probe"), []byte(stackProbeFixture), 0700))
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, "timeout", "--kill-after=2s", "3s", "bash", "-c", stackSessionFixture, "test", library, owner, "cancel-group")
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
	markers, err := filepath.Glob(filepath.Join(owner, "stack.*", "COMPLETE"))
	require.NoError(t, err)
	require.Empty(t, markers)
}
