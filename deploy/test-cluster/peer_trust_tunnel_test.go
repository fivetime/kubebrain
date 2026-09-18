package testcluster_test

import (
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestPeerTrustRejectedClientCannotPoisonFollowingPositiveProbe(t *testing.T) {
	dir := t.TempDir()
	receipt := memberStageFixture(t, dir, false)
	receipt["phase"] = "roots"
	write := func(name, stringData string, mode os.FileMode) {
		require.NoError(t, os.WriteFile(filepath.Join(dir, name), []byte(stringData), mode))
	}
	raw, err := json.Marshal(receipt)
	require.NoError(t, err)
	write("receipt.json", string(raw), 0600)
	pod := map[string]any{"metadata": map[string]any{"name": "kubebrain-local-0", "uid": "pod-0", "ownerReferences": []any{map[string]any{"uid": "sts-uid", "controller": true}}}, "spec": trustMap(receipt, "baseline", "spec", "template")["spec"], "status": map[string]any{"conditions": []any{map[string]any{"type": "Ready", "status": "True"}}}}
	raw, err = json.Marshal(pod)
	require.NoError(t, err)
	write("pod.json", string(raw), 0600)
	require.NoError(t, os.Mkdir(filepath.Join(dir, "after"), 0700))
	raw, err = json.Marshal(map[string]any{"items": []any{pod}})
	require.NoError(t, err)
	write("after/pods.json", string(raw), 0600)
	write("ss", "#!/bin/sh\nexit 0\n", 0700)
	write("kubectl", `#!/usr/bin/env bash
set -euo pipefail
while [[ $# -gt 0 ]]; do case "$1" in --kubeconfig=*|--context=*|--request-timeout=*) shift;; -n) shift 2;; *) break;; esac; done
case "$1" in
 get) cat "$FIXTURE/pod.json";;
 exec)
  for file in ca.crt tls.crt tls.key; do
   hash=$(sha256sum "$FIXTURE/original/$file" | cut -d ' ' -f1)
   printf '%s  /etc/kubebrain/peer-tls/%s\n' "$hash" "$file"
  done;;
 port-forward)
  port=${4%:*}
  echo "$BASHPID" > "$FIXTURE/forward-$port.pid"
  printf '%s %s\n' "$port" "$BASHPID" >> "$FIXTURE/forwards"
  echo "Forwarding from 127.0.0.1:$port -> ${4#*:}"
  trap 'exit 0' TERM INT
  while :; do sleep 0.1; done;;
 *) exit 88;;
esac
`, 0700)
	write("curl", `#!/usr/bin/env bash
set -euo pipefail
port=''; cert=''; url=''
while [[ $# -gt 0 ]]; do
 case "$1" in
  --resolve) IFS=: read -r host port ip <<< "$2"; shift 2;;
  --cert) cert=$2; shift 2;;
  https://*) url=$1; shift;;
  *) shift;;
 esac
done
if [[ $url == */v3/maintenance/status ]]; then touch "$FIXTURE/reached-business-probe"; exit 77; fi
pid=$(<"$FIXTURE/forward-$port.pid")
if ! kill -0 "$pid" 2>/dev/null; then echo 000; exit 7; fi
if [[ $cert == "$FIXTURE/bundle/"* ]]; then
 kill -TERM "$pid"
 for n in {1..100}; do if ! kill -0 "$pid" 2>/dev/null; then break; fi; sleep 0.01; done
 echo 'TLS alert unknown ca' >&2
 printf 000
 exit 56
fi
printf 404
`, 0700)
	cmd := exec.Command("bash", "verify-peer-trust-stage.sh", "restore", "after", dir, "/unused/kubeconfig", "test")
	cmd.Env = append(os.Environ(), "PATH="+dir+":"+os.Getenv("PATH"), "FIXTURE="+dir)
	output, err := cmd.CombinedOutput()
	require.Error(t, err, "mock intentionally stops at the business probe boundary: %s", output)
	require.FileExists(t, filepath.Join(dir, "reached-business-probe"), "old/new-rejected/old TLS checks must all finish despite dead negative-probe tunnel: %s", output)
	status, err := os.ReadFile(filepath.Join(dir, "verify-after", "peer-0-old-after.status"))
	require.NoError(t, err)
	require.Equal(t, "404", string(status))
}
