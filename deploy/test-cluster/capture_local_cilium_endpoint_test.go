package testcluster_test

import (
	"context"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestCaptureLocalCiliumEndpointProcessIdentity(t *testing.T) {
	slots := make(chan struct{}, 4)
	for _, scenario := range []string{"stable", "ready-restored", "ready-lost", "agent-ready-change", "pod-restart", "agent-restart", "pod-spec-change", "pod-uid-change", "cep-change", "endpoint-not-ready", "wrong-namespace", "missing-image-id"} {
		t.Run(scenario, func(t *testing.T) {
			t.Parallel()
			slots <- struct{}{}
			defer func() { <-slots }()
			dir := t.TempDir()
			require.NoError(t, os.Chmod(dir, 0700))
			bin := filepath.Join(dir, "bin")
			require.NoError(t, os.Mkdir(bin, 0700))
			require.NoError(t, os.WriteFile(filepath.Join(bin, "kubectl"), []byte(ciliumCaptureMock), 0700))
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			cmd := exec.CommandContext(ctx, "bash", "capture-local-cilium-endpoint.sh", dir, "kubebrain-local-0")
			cmd.Env = append(os.Environ(), "PATH="+bin+":"+os.Getenv("PATH"), "CAPTURE_FIXTURE="+dir, "CAPTURE_SCENARIO="+scenario)
			output, err := cmd.CombinedOutput()
			require.NoError(t, ctx.Err(), string(output))
			accepted := scenario == "stable" || scenario == "ready-restored" || scenario == "ready-lost" || scenario == "agent-ready-change"
			if accepted {
				require.NoError(t, err, string(output))
				require.Contains(t, string(output), "SAME_POD_CEP_AGENT_ENDPOINT_CAPTURED_NOT_ENFORCEMENT_PROOF")
			} else {
				require.Error(t, err, string(output))
				require.NotContains(t, string(output), "SAME_POD_CEP_AGENT_ENDPOINT_CAPTURED_NOT_ENFORCEMENT_PROOF")
			}
			captures, err := filepath.Glob(filepath.Join(dir, "endpoint.*"))
			require.NoError(t, err)
			require.Len(t, captures, 1)
			code, err := os.ReadFile(filepath.Join(captures[0], "capture.exit"))
			require.NoError(t, err)
			if accepted {
				require.Equal(t, "0", strings.TrimSpace(string(code)))
				verify := exec.Command("sha256sum", "-c", filepath.Join(captures[0], "evidence.sha256"))
				result, err := verify.CombinedOutput()
				require.NoError(t, err, string(result))
				// Recheck the exact regular Pod objects used by both identity comparisons.
				for _, name := range []string{"pod.json", "pod-after.json", "agent-before.json", "agent-after.json"} {
					data, err := os.ReadFile(filepath.Join(captures[0], name))
					require.NoError(t, err)
					require.True(t, json.Valid(data))
				}
			} else {
				require.NotEqual(t, "0", strings.TrimSpace(string(code)))
				_, err := os.Stat(filepath.Join(captures[0], "evidence.sha256"))
				require.True(t, os.IsNotExist(err))
			}
		})
	}
}

const ciliumCaptureMock = `#!/usr/bin/env bash
set -euo pipefail
while [[ $# -gt 0 && $1 != get && $1 != exec ]]; do shift; done
[[ $# -ge 2 ]] || exit 99
scenario=$CAPTURE_SCENARIO
pod() {
 local role=$1 change=$2
 jq -n --arg role "$role" --arg change "$change" '
 {apiVersion:"v1",kind:"Pod",metadata:{name:(if $role=="app" then "kubebrain-local-0" else "cilium-worker1" end),namespace:(if $role=="app" then "kubebrain-dbaas-test" else "kube-system" end),uid:$role,ownerReferences:[{uid:"7d760f53-5bb5-4429-a2f8-651b89665616",kind:"StatefulSet",controller:true}]},spec:{nodeName:"worker1",containers:[{name:$role,image:"fixed"}]},status:{podIP:"10.0.0.1",conditions:[{type:"Ready",status:"True"}],containerStatuses:[{name:$role,containerID:($role+"://process"),imageID:"image@sha256:fixed",restartCount:0,ready:true,state:{running:{startedAt:"2026-09-18T23:35:18Z"}}}]}} |
 if $change=="ready" then .status.containerStatuses[0].ready=false
 elif $change=="restart" then .status.containerStatuses[0].restartCount=1
 elif $change=="spec" then .spec.nodeName="other"
 elif $change=="uid" then .metadata.uid="other"
 elif $change=="image" then del(.status.containerStatuses[0].imageID)
 else . end'
}
if [[ $1 == exec ]]; then
 jq -n --arg state "$(if [[ $scenario == endpoint-not-ready ]]; then echo regenerating; else echo ready; fi)" '[{id:42,status:{state:$state,"external-identifiers":{"k8s-namespace":"kubebrain-dbaas-test","k8s-pod-name":"kubebrain-local-0"},identity:{id:123},networking:{addressing:[{ipv4:"10.0.0.1"}]}}}]'
 exit
fi
case $2 in
 namespace)
  uid=6c57c242-912b-41bb-9020-f4fdb3225ef3
  [[ $scenario != wrong-namespace ]] || uid=wrong
  jq -n --arg uid "$uid" '{metadata:{uid:$uid}}';;
 pod)
  change=none
  if [[ $3 == kubebrain-local-0 ]]; then
   if [[ -e $CAPTURE_FIXTURE/app-seen ]]; then
    case $scenario in
     ready-lost) change=ready;; pod-restart) change=restart;; pod-spec-change) change=spec;; pod-uid-change) change=uid;; missing-image-id) change=image;;
    esac
   else
    touch "$CAPTURE_FIXTURE/app-seen"
    [[ $scenario != ready-restored ]] || change=ready
   fi
   pod app "$change"
  else
   [[ $scenario != agent-ready-change ]] || change=ready
   [[ $scenario != agent-restart ]] || change=restart
   pod agent "$change"
  fi;;
 pods) pod agent none | jq '{items:[.]}';;
 ciliumendpoint)
  id=42
  if [[ -e $CAPTURE_FIXTURE/cep-seen && $scenario == cep-change ]]; then id=43; fi
  touch "$CAPTURE_FIXTURE/cep-seen"
  jq -n --argjson id "$id" '{metadata:{uid:"cep-uid",ownerReferences:[{kind:"Pod",uid:"app"}]},status:{id:$id,identity:{id:123},networking:{addressing:[{ipv4:"10.0.0.1"}]}}}';;
 *) exit 99;;
esac
`
