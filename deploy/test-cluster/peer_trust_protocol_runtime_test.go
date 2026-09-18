package testcluster_test

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// This tests the actual shell verifier's orchestration and cleanup, not real
// Kubernetes, TLS or TiKV. Those are separate acceptance gates.
func TestPeerProtocolRuntimeVerifierOrchestration(t *testing.T) {
	for _, scenario := range []string{"expand", "restore", "wrong-image", "wrong-version", "control-failure", "restore-route-live"} {
		t.Run(scenario, func(t *testing.T) {
			t.Parallel()
			dir := t.TempDir()
			write := func(name string, data []byte, mode os.FileMode) string {
				p := filepath.Join(dir, name)
				require.NoError(t, os.WriteFile(p, data, mode))
				return p
			}
			in := memberStageFixture(t, dir, false)
			for _, name := range []string{"original_secret", "expanded_secret", "member_secret"} {
				in["live_"+name] = cloneTrustObject(t, trustMap(in, name))
			}
			p, err := trustPlan(t, in)
			require.NoError(t, err, string(p))
			in["current"] = applyTrustPatch(t, in["current"], p)
			in["phase"] = "protocol"
			image := "ghcr.io/fivetime/kubebrain@sha256:" + strings.Repeat("a", 64)
			in["candidate_image"] = image
			mode := "expand"
			if strings.HasPrefix(scenario, "restore") {
				mode = "restore"
			}
			current := trustMap(in, "current")
			if mode == "expand" {
				p, err = trustPlan(t, in)
				require.NoError(t, err, string(p))
				current = applyTrustPatch(t, current, p)
			}
			audit, err := json.Marshal(map[string]any{"scope": "published_image_identity_only", "executed_platform": "linux/amd64", "image": image, "source": strings.Repeat("b", 40), "amd64_digest": "sha256:" + strings.Repeat("c", 64), "image_ci": 1, "probe_ci": 2})
			require.NoError(t, err)
			v := trustMap(in, "verification")
			v["image_audit"] = write("audit.json", audit, 0600)
			hash := sha256.Sum256(audit)
			v["image_audit_sha256"] = hex.EncodeToString(hash[:])
			probe := []byte(`#!/usr/bin/env bash
set -euo pipefail
printf '%s\n' "$*" >> "$FIXTURE/control-calls"
[[ $# == 9 ]]
[[ $1 == --endpoint=https://127.0.0.1:1888? && $2 == --server-name=peer.test && $3 == --server-pin=* ]]
[[ $4 == --cacert="$FIXTURE/verify-after/kubebrain-local-"*/ca.crt ]]
[[ $5 == --cert="$FIXTURE/verify-after/kubebrain-local-"*/tls.crt && $6 == --key="$FIXTURE/verify-after/kubebrain-local-"*/tls.key ]]
[[ $7 == --scope=retirement-v1:* && $8 == --sender=* ]]
[[ $9 == --receiver=* && ${8#--sender=} != ${9#--receiver=} ]]
receiver=${9#--receiver=}
for i in 0 1 2; do
 if [[ $receiver == kubebrain-local-$i.peer.test:3380 ]]; then
  sender=kubebrain-local-$(((i+1)%3))
  [[ $8 == --sender=$sender.peer.test:3380 ]]
  [[ $5 == --cert=$FIXTURE/verify-after/$sender/tls.crt && $6 == --key=$FIXTURE/verify-after/$sender/tls.key ]]
  [[ $3 == --server-pin="$(jq -r --arg r "$receiver" '.[$r][0]' "$FIXTURE/verify-after/expected-pins.json")" ]]
 fi
done
if [[ $SCENARIO == control-failure ]]; then exit 73; fi
echo VERIFIED
`)
			v["control_probe"] = write("control-probe", probe, 0700)
			hash = sha256.Sum256(probe)
			v["control_probe_sha256"] = hex.EncodeToString(hash[:])
			raw, err := json.Marshal(in)
			require.NoError(t, err)
			write("receipt.json", raw, 0600)
			pods := []any{}
			for i := 0; i < 3; i++ {
				spec := cloneTrustObject(t, trustMap(current, "spec", "template", "spec"))
				if scenario == "wrong-image" {
					spec["containers"].([]any)[0].(map[string]any)["image"] = "other"
				}
				pod := map[string]any{"metadata": map[string]any{"name": fmt.Sprintf("kubebrain-local-%d", i), "uid": fmt.Sprintf("pod-%d", i), "ownerReferences": []any{map[string]any{"uid": "sts-uid", "controller": true}}}, "spec": spec, "status": map[string]any{"conditions": []any{map[string]any{"type": "Ready", "status": "True"}}, "containerStatuses": []any{map[string]any{"name": "kubebrain", "imageID": image, "ready": true}}}}
				pods = append(pods, pod)
				raw, err = json.Marshal(pod)
				require.NoError(t, err)
				write(fmt.Sprintf("pod-%d.json", i), raw, 0600)
			}
			require.NoError(t, os.Mkdir(filepath.Join(dir, "after"), 0700))
			raw, err = json.Marshal(map[string]any{"items": pods})
			require.NoError(t, err)
			write("after/pods.json", raw, 0600)
			write("db.json", []byte("{}"), 0600)
			write("ss", []byte("#!/bin/sh\nexit 0\n"), 0700)
			write("kubectl", []byte(protocolRuntimeKubectl), 0700)
			write("curl", []byte(protocolRuntimeCurl), 0700)
			cmd := exec.Command("bash", "verify-peer-trust-stage.sh", mode, "after", dir, "/unused/config", "test")
			cmd.Env = append(os.Environ(), "PATH="+dir+":"+os.Getenv("PATH"), "FIXTURE="+dir, "SCENARIO="+scenario)
			output, err := cmd.CombinedOutput()
			if scenario == "expand" || scenario == "restore" {
				require.NoError(t, err, string(output))
				for i := 0; i < 3; i++ {
					require.FileExists(t, filepath.Join(dir, "verify-after", fmt.Sprintf("pod-%d-after.json", i)))
				}
				data, err := os.ReadFile(filepath.Join(dir, "db.json"))
				require.NoError(t, err)
				require.JSONEq(t, "{}", string(data))
				if scenario == "expand" {
					data, err := os.ReadFile(filepath.Join(dir, "control-calls"))
					require.NoError(t, err)
					require.Equal(t, 3, strings.Count(string(data), "--endpoint="))
				} else {
					require.NoFileExists(t, filepath.Join(dir, "control-calls"))
				}
			} else {
				require.Error(t, err, string(output))
				require.NoFileExists(t, filepath.Join(dir, "verify-after", "fixture-0.json"), "bad protocol evidence must stop before ordinary writes")
			}
		})
	}
}

const protocolRuntimeKubectl = `#!/usr/bin/env bash
set -euo pipefail
while [[ $# -gt 0 ]]; do case "$1" in --kubeconfig=*|--context=*|--request-timeout=*) shift;; -n) shift 2;; *) break;; esac; done
case "$1" in
 get) cat "$FIXTURE/pod-${3##*-}.json";;
 exec)
  pod=$2
  if [[ $6 == sha256sum ]]; then
   shift 6
   for path in "$@"; do printf '%s  %s\n' "$(sha256sum "$FIXTURE/verify-after/$pod/${path##*/}" | cut -d ' ' -f1)" "$path"; done
  elif [[ $6 == /usr/local/bin/kube-brain ]]; then
   if [[ $SCENARIO == wrong-version ]]; then echo 'Git SHA: wrong'; else printf 'Git SHA: %s\n' "$(jq -r '.source' "$FIXTURE/audit.json")"; fi
  elif [[ $6 == /bin/sh ]]; then :
  else exit 88; fi;;
 port-forward)
  echo "Forwarding from 127.0.0.1:${4%:*} -> ${4#*:}"
  trap 'exit 0' TERM INT
  while :; do sleep 0.1; done;;
 *) exit 89;;
esac
`

const protocolRuntimeCurl = `#!/usr/bin/env bash
set -euo pipefail
out='';body='';url=''
while [[ $# -gt 0 ]]; do
 case "$1" in --output) out=$2;shift 2;; --data) body=$2;shift 2;; https://*) url=$1;shift;; *) shift;; esac
done
if [[ $url == */__peer_trust_runtime_check__ ]]; then : > "$out";printf 404;exit 0;fi
if [[ $url == */internal/* ]]; then
 : > "$out"
 if [[ $SCENARIO == restore-route-live ]]; then printf 204;else printf 404;fi
 exit 0
fi
if [[ $url == */maintenance/status ]]; then
 port=${url#https://};port=${port#*:};port=${port%%/*}
 jq -n --arg id "$((port-18889))" '{header:{cluster_id:"42",member_id:$id},leader:"1"}' > "$out"
elif [[ $url == */kv/txn ]]; then
 target=$(jq -r '.compare[0].target' <<< "$body")
 key=$(jq -r '.compare[0].key' <<< "$body")
 if [[ $target == VERSION ]]; then
  value=$(jq -r '.success[0].request_put.value' <<< "$body")
  jq --arg k "$key" --arg v "$value" '.[$k]=$v' "$FIXTURE/db.json" > "$FIXTURE/next.json"
  printf '{"header":{"cluster_id":"42"},"succeeded":true}' > "$out"
 else
  value=$(jq -r '.compare[0].value' <<< "$body")
  jq -e --arg k "$key" --arg v "$value" '.[$k]==$v' "$FIXTURE/db.json" >/dev/null
  jq --arg k "$key" 'del(.[$k])' "$FIXTURE/db.json" > "$FIXTURE/next.json"
  printf '{"header":{"cluster_id":"42"},"succeeded":true,"responses":[{"response_delete_range":{"deleted":"1"}}]}' > "$out"
 fi
 mv "$FIXTURE/next.json" "$FIXTURE/db.json"
elif [[ $url == */kv/range ]]; then
 key=$(jq -r '.key' <<< "$body")
 jq --arg k "$key" '{header:{cluster_id:"42"},kvs:(if has($k) then [{key:$k,value:.[$k]}] else [] end)}' "$FIXTURE/db.json" > "$out"
else exit 90;fi
`
