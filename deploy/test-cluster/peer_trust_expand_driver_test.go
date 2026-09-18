package testcluster_test

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	jsonpatch "gopkg.in/evanphx/json-patch.v4"
)

const trustMockKubectl = `#!/usr/bin/env bash
set -euo pipefail
printf '%s\n' "$*" >> "$FIXTURE/calls"
while [[ $# -gt 0 ]]; do
 case "$1" in
  --kubeconfig=*|--context=*|--request-timeout=*) shift;;
  -n) shift 2;;
  *) break;;
 esac
done
case "$1" in
 get)
  case "$2" in
   namespace) jq '.namespace' "$FIXTURE/receipt.json";;
   sts) cat "$FIXTURE/current.json";;
   secret)
    if [[ $3 == kubebrain-local-peer-tls ]]; then jq '.original_secret' "$FIXTURE/receipt.json"
    elif [[ $3 == peer-members-dual-test ]]; then
     if [[ $SCENARIO == member-secret-drift || ( $SCENARIO == member-secret-after-dry-run && -f "$FIXTURE/dry-ran" ) ]]; then
      jq '.member_secret|.metadata.uid="recreated"' "$FIXTURE/receipt.json"
     else jq '.member_secret' "$FIXTURE/receipt.json"; fi
    else jq '.expanded_secret' "$FIXTURE/receipt.json"; fi;;
   pvc|pv) echo '{"items":[]}' ;;
   pods) jq '{items:[range(0;3) as $i | {
    metadata:{name:("kubebrain-local-"+($i|tostring)),uid:("pod-"+($i|tostring)),ownerReferences:[{uid:.metadata.uid,controller:true}],labels:{"controller-revision-hash":.status.updateRevision}},
    spec:.spec.template.spec,
    status:{conditions:[{type:"Ready",status:"True"}],containerStatuses:[{name:"kubebrain",ready:true,state:{running:{}}}]}}]}' "$FIXTURE/current.json";;
   *) exit 3;;
  esac;;
 patch)
  patch=''; dry=false
  for arg in "$@"; do
   case "$arg" in --patch-file=*) patch=${arg#*=};; --dry-run=server) dry=true;; esac
  done
  if [[ $dry == true ]]; then touch "$FIXTURE/dry-ran"; cat "$FIXTURE/current.json"; exit 0; fi
  if [[ $SCENARIO == conflict && ! -f "$FIXTURE/conflicted" ]]; then touch "$FIXTURE/conflicted"; exit 1; fi
  if [[ $SCENARIO == drift && ! -f "$FIXTURE/drifted" ]]; then
   touch "$FIXTURE/drifted"
   jq '.spec.replicas=2' "$FIXTURE/current.json" > "$FIXTURE/next.json"
   mv "$FIXTURE/next.json" "$FIXTURE/current.json"
   exit 1
  fi
  jq --slurpfile p "$patch" 'reduce $p[0][] as $op (.;
    ($op.path|split("/")|.[1:]|map(if test("^[0-9]+$") then tonumber else . end)) as $path |
    if $op.op=="test" then (if getpath($path)==$op.value then . else error("conflict") end)
    elif $op.op=="replace" then setpath($path;$op.value) else error("unexpected op") end)' \
    "$FIXTURE/current.json" > "$FIXTURE/next.json"
  mv "$FIXTURE/next.json" "$FIXTURE/current.json"
  if [[ $SCENARIO == lost-response && ! -f "$FIXTURE/response-lost" ]]; then touch "$FIXTURE/response-lost"; exit 124; fi
  cat "$FIXTURE/current.json";;
 rollout)
  expanded=$(jq -r --slurpfile receipt "$FIXTURE/receipt.json" '($receipt[0]|if .phase=="members" then .member_secret.metadata.name else .expanded_secret.metadata.name end) as $target | any(.spec.template.spec.volumes[];.name=="peer-tls" and .secret.secretName==$target)' "$FIXTURE/current.json")
  if [[ $SCENARIO == rollout-failure && $expanded == true ]]; then exit 1; fi
  if [[ $SCENARIO == rollout-timeout && $expanded == true ]]; then exit 124; fi
  ;;
 *) exit 3;;
esac
`

func TestPeerTrustExpansionDriverRecovery(t *testing.T) {
	testPeerTrustDriverRecovery(t, false)
}

func TestPeerTrustMembersDriverRecovery(t *testing.T) {
	testPeerTrustDriverRecovery(t, true)
}

func testPeerTrustDriverRecovery(t *testing.T, members bool) {
	scenarios := []string{"success", "preflight-failure", "resumed-preflight-failure", "conflict", "lost-response", "drift", "rollout-failure", "rollout-timeout", "verify-failure"}
	if members {
		scenarios = append(scenarios, "member-secret-drift", "member-secret-after-dry-run")
	}
	for _, scenario := range scenarios {
		t.Run(scenario, func(t *testing.T) {
			t.Parallel() // Each case has isolated mock commands, state and child env.
			dir := t.TempDir()
			write := func(name, content string, mode os.FileMode) string {
				t.Helper()
				path := filepath.Join(dir, name)
				require.NoError(t, os.WriteFile(path, []byte(content), mode))
				return path
			}
			in := trustPlanInput(t)
			if members {
				in = memberTrustInput(t)
			}
			data, err := json.Marshal(in)
			require.NoError(t, err)
			receipt := write("receipt.json", string(data), 0600)
			sum := sha256.Sum256(data)
			current, err := json.Marshal(in["current"])
			require.NoError(t, err)
			if scenario == "resumed-preflight-failure" {
				planned, err := trustPlan(t, in)
				require.NoError(t, err, string(planned))
				patch, err := jsonpatch.DecodePatch(planned)
				require.NoError(t, err)
				current, err = patch.Apply(current)
				require.NoError(t, err)
			}
			write("current.json", string(current), 0600)
			write("kubectl", trustMockKubectl, 0700)
			kubeconfig := write("kubeconfig", "mock", 0600)
			verifier := write("verifier.sh", `#!/usr/bin/env bash
set -euo pipefail
printf '%s\n' "$1 $2" >> "$FIXTURE/verifications"
[[ $# == 5 ]]
if [[ $SCENARIO == preflight-failure && $2 == preflight ]]; then exit 1; fi
if [[ $SCENARIO == resumed-preflight-failure && $1 == expand && $2 == preflight ]]; then exit 1; fi
if [[ $SCENARIO == verify-failure && $1 == expand && $2 == after ]]; then exit 1; fi
`, 0600)
			out := filepath.Join(dir, "attempt")
			args := []string{"run-peer-trust-expand.sh", "--execute", "expand", receipt, hex.EncodeToString(sum[:]), out, kubeconfig, "test-context", verifier}
			run := func() ([]byte, error) {
				cmd := exec.Command("bash", args...)
				cmd.Env = append(os.Environ(), "PATH="+dir+":"+os.Getenv("PATH"), "FIXTURE="+dir, "SCENARIO="+scenario)
				return cmd.CombinedOutput()
			}
			output, err := run()
			if scenario == "success" {
				require.NoError(t, err, string(output))
			} else {
				require.Error(t, err, string(output))
			}
			exitCode, err := os.ReadFile(filepath.Join(out, "exit-code"))
			require.NoError(t, err)
			if scenario == "success" {
				require.Equal(t, "0\n", string(exitCode))
			} else {
				require.NotEqual(t, "0\n", string(exitCode))
			}
			after, err := os.ReadFile(filepath.Join(dir, "current.json"))
			require.NoError(t, err)
			var object map[string]any
			require.NoError(t, json.Unmarshal(after, &object))
			switch scenario {
			case "success":
				if members {
					require.Contains(t, string(after), "peer-members-dual-test")
				} else {
					require.Contains(t, string(after), "peer-old-dual-test")
				}
				require.NoFileExists(t, filepath.Join(out, "recovery-exit-code"))
			case "preflight-failure", "member-secret-drift", "member-secret-after-dry-run":
				require.Equal(t, in["current"], object)
				require.NoFileExists(t, filepath.Join(out, "recovery-exit-code"))
			case "drift":
				require.EqualValues(t, 2, trustMap(object, "spec")["replicas"])
				recovery, err := os.ReadFile(filepath.Join(out, "recovery-exit-code"))
				require.NoError(t, err)
				require.NotEqual(t, "0\n", string(recovery))
			default:
				require.Equal(t, in["current"], object, "recovery must return exact original spec without declaring expansion success")
				if members {
					require.NotEqual(t, trustMap(in, "baseline")["spec"], object["spec"], "member recovery must retain dual roots")
				}
				recovery, err := os.ReadFile(filepath.Join(out, "recovery-exit-code"))
				require.NoError(t, err)
				require.Equal(t, "0\n", string(recovery))
			}
			calls, err := os.ReadFile(filepath.Join(dir, "calls"))
			require.NoError(t, err)
			require.NotContains(t, string(calls), " delete ")
			require.NotContains(t, string(calls), " create ")
			if members && scenario == "success" {
				require.GreaterOrEqual(t, strings.Count(string(calls), "get secret peer-members-dual-test "), 5, "refresh member Secret in every captured phase")
			}
			if strings.HasPrefix(scenario, "member-secret-") {
				for _, call := range strings.Split(string(calls), "\n") {
					if strings.Contains(call, " patch sts ") {
						require.Contains(t, call, "--dry-run=server", "Secret drift must prevent persisted writes")
					}
				}
			}
			info, err := os.Stat(filepath.Join(out, "receipt.json"))
			require.NoError(t, err)
			require.Equal(t, os.FileMode(0600), info.Mode().Perm())
			beforeRetry := string(calls)
			_, err = run()
			require.Error(t, err, "claimed output must never be reused")
			calls, err = os.ReadFile(filepath.Join(dir, "calls"))
			require.NoError(t, err)
			require.Equal(t, beforeRetry, string(calls), "reused attempt must stop before API access")
		})
	}
}

func TestPeerTrustDriverRejectsUnknownPhaseBeforeAPIAccess(t *testing.T) {
	dir := t.TempDir()
	in := memberTrustInput(t)
	in["phase"] = "unknown"
	data, err := json.Marshal(in)
	require.NoError(t, err)
	receipt := filepath.Join(dir, "receipt.json")
	require.NoError(t, os.WriteFile(receipt, data, 0600))
	sum := sha256.Sum256(data)
	kubeconfig := filepath.Join(dir, "kubeconfig")
	verifier := filepath.Join(dir, "verifier.sh")
	require.NoError(t, os.WriteFile(kubeconfig, []byte("unused"), 0600))
	require.NoError(t, os.WriteFile(verifier, []byte("exit 99\n"), 0600))
	marker := filepath.Join(dir, "unexpected-api")
	require.NoError(t, os.WriteFile(filepath.Join(dir, "kubectl"), []byte("#!/bin/sh\ntouch \"$UNEXPECTED_API\"\nexit 77\n"), 0700))
	out := filepath.Join(dir, "attempt")
	cmd := exec.Command("bash", "run-peer-trust-expand.sh", "--execute", "expand", receipt, hex.EncodeToString(sum[:]), out, kubeconfig, "test", verifier)
	cmd.Env = append(os.Environ(), "PATH="+dir+":"+os.Getenv("PATH"), "UNEXPECTED_API="+marker)
	output, err := cmd.CombinedOutput()
	require.Error(t, err, string(output))
	require.NoDirExists(t, out, "unsupported phase must fail before claiming an execution")
	require.NoFileExists(t, marker)
}
