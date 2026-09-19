package testcluster_test

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestObserveLocalNonces(t *testing.T) {
	slots := make(chan struct{}, 4)
	for _, mode := range []string{"success", "collision", "missing-agent", "agent-restart", "node-replaced", "cep-replaced", "wrong-namespace", "wrong-controller", "foreign-label", "incomplete-list", "expired"} {
		t.Run(mode, func(t *testing.T) {
			t.Parallel()
			slots <- struct{}{}
			defer func() { <-slots }()
			owner := t.TempDir()
			require.NoError(t, os.Chmod(owner, 0700))
			bin := filepath.Join(owner, "bin")
			require.NoError(t, os.Mkdir(bin, 0700))
			mock := filepath.Join(bin, "kubectl")
			require.NoError(t, os.WriteFile(mock, []byte(nonceCaptureMock), 0700))
			env := append(os.Environ(), "PATH="+bin+":"+os.Getenv("PATH"), "NONCE_FIXTURE="+owner, "NONCE_MODE="+mode)
			fixture := exec.Command(mock, "fixture-pod")
			fixture.Env = env
			pod, err := fixture.Output()
			require.NoError(t, err)
			expected := filepath.Join(owner, "expected.json")
			require.NoError(t, os.WriteFile(expected, pod, 0600))
			deadline := time.Now().Add(20 * time.Second).UnixNano()
			if mode == "expired" {
				deadline = time.Now().Add(-time.Second).UnixNano()
			}
			ctx, cancel := context.WithTimeout(context.Background(), 25*time.Second)
			defer cancel()
			cmd := exec.CommandContext(ctx, "bash", "observe-local-nonces.sh", owner, expected, "term-test", "reserved-test", fmt.Sprint(deadline))
			cmd.Env = env
			out, err := cmd.CombinedOutput()
			require.NoError(t, ctx.Err(), string(out))
			if mode == "success" {
				require.NoError(t, err, string(out))
				require.Contains(t, string(out), "COMPLETE_AGENT_NONCE_SNAPSHOT_NOT_CONTINUOUS_ABSENCE_OR_ENFORCEMENT")
			} else {
				require.Error(t, err, string(out))
				require.NotContains(t, string(out), "COMPLETE_AGENT_NONCE_SNAPSHOT")
			}
			captures, err := filepath.Glob(filepath.Join(owner, "nonce-observation.*"))
			require.NoError(t, err)
			if mode == "expired" {
				require.Empty(t, captures)
				return
			}
			require.Len(t, captures, 1)
			code, err := os.ReadFile(filepath.Join(captures[0], "observation.exit"))
			require.NoError(t, err)
			if mode == "success" {
				require.Equal(t, "0\n", string(code))
				verify := exec.Command("sha256sum", "-c", filepath.Join(captures[0], "evidence.sha256"))
				out, err := verify.CombinedOutput()
				require.NoError(t, err, string(out))
			} else {
				require.NotEqual(t, "0\n", string(code))
			}
		})
	}
}

const nonceCaptureMock = `#!/usr/bin/env bash
set -euo pipefail
while [[ $# -gt 0 && $1 != get && $1 != exec && $1 != fixture-pod ]]; do shift; done
pod() {
 jq -n --arg role "$1" --argjson restarts "$2" '
 {apiVersion:"v1",kind:"Pod",metadata:{name:(if $role=="app" then "kubebrain-local-0" else "cilium-worker1" end),namespace:(if $role=="app" then "kubebrain-dbaas-test" else "kube-system" end),uid:$role,ownerReferences:[{apiVersion:"apps/v1",name:"kubebrain-local",kind:"StatefulSet",uid:"7d760f53-5bb5-4429-a2f8-651b89665616",controller:true}]},spec:{nodeName:"worker1",containers:[{name:$role,image:"fixed"}]},status:{podIP:"10.0.0.1",conditions:[{type:"Ready",status:"True"}],containerStatuses:[{name:$role,containerID:"runtime://process",imageID:"image@sha256:fixed",restartCount:$restarts,state:{running:{startedAt:"start"}}}]}}'
}
if [[ $1 == fixture-pod ]]; then pod app 0; exit; fi
if [[ $1 == exec ]]; then
 jq -n --arg mode "$NONCE_MODE" '[{id:42,status:{identity:{labels:["k8s:app=brain"]+(if $mode=="collision" then ["container:kubebrain.io/fault-owner=reserved-test"] else [] end)},"external-identifiers":{"k8s-namespace":"kubebrain-dbaas-test","k8s-pod-name":"kubebrain-local-0"},networking:{addressing:[{ipv4:"10.0.0.1"}]}}}]'
 exit
fi
[[ $1 == get ]] || exit 99
case $2 in
 namespace)
  uid=6c57c242-912b-41bb-9020-f4fdb3225ef3
  [[ $NONCE_MODE != wrong-namespace ]] || uid=wrong
  jq -n --arg uid "$uid" '{metadata:{uid:$uid}}';;
 statefulset) jq -n '{metadata:{uid:"7d760f53-5bb5-4429-a2f8-651b89665616"}}';;
 pod) pod app 0 | jq --arg mode "$NONCE_MODE" '
  if $mode=="wrong-controller" then .metadata.ownerReferences[0].uid="other"
  elif $mode=="foreign-label" then .metadata.labels={"kubebrain.io/fault-owner":"term-other"}
  else . end';;
 nodes)
  uid=node
  if [[ -e $NONCE_FIXTURE/nodes-seen && $NONCE_MODE == node-replaced ]]; then uid=other; fi
  touch "$NONCE_FIXTURE/nodes-seen"
  jq -n --arg uid "$uid" '{items:[{metadata:{name:"worker1",uid:$uid},status:{conditions:[{type:"Ready",status:"True"}]}}]}';;
 pods)
  if [[ $NONCE_MODE == missing-agent ]]; then echo '{"items":[]}'; exit; fi
  restarts=0
  if [[ -e $NONCE_FIXTURE/agents-seen && $NONCE_MODE == agent-restart ]]; then restarts=1; fi
  touch "$NONCE_FIXTURE/agents-seen"
  pod agent "$restarts" | jq --arg mode "$NONCE_MODE" '{metadata:{continue:(if $mode=="incomplete-list" then "next" else "" end)},items:[.]}' ;;
 ciliumendpoint)
  uid=cep
  if [[ -e $NONCE_FIXTURE/cep-seen && $NONCE_MODE == cep-replaced ]]; then uid=other; fi
  touch "$NONCE_FIXTURE/cep-seen"
  jq -n --arg uid "$uid" '{metadata:{uid:$uid,ownerReferences:[{kind:"Pod",uid:"app"}]},status:{id:42,networking:{addressing:[{ipv4:"10.0.0.1"}]}}}';;
 *) exit 99;;
esac
`
