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
)

func TestDiagnosticStageVerifier(t *testing.T) {
	for _, scenario := range []string{"preflight", "hash", "dns", "key", "restore", "expand", "probe-failure", "health-failure", "pod-replaced"} {
		t.Run(scenario, func(t *testing.T) {
			dir := t.TempDir()
			trustMaterialFixture(t, dir, false, false, false)
			write := func(name string, data []byte, mode os.FileMode) string {
				path := filepath.Join(dir, name)
				require.NoError(t, os.WriteFile(path, data, mode))
				return path
			}
			read := func(name string) []byte {
				data, err := os.ReadFile(filepath.Join(dir, name))
				require.NoError(t, err)
				return data
			}
			hash := func(data []byte) string { sum := sha256.Sum256(data); return hex.EncodeToString(sum[:]) }
			write("original/probe.crt", read("original/tls.crt"), 0600)
			write("original/probe.key", read("original/tls.key"), 0600)
			probe := []byte(`#!/usr/bin/env bash
set -euo pipefail
printf '%s\n' "$*" >> "$FIXTURE/probe-calls"
[[ $SCENARIO != probe-failure ]]
if [[ $* == *'--mode protected'* ]]; then [[ $* == *'--anonymous-endpoint https://127.0.0.1:18585'* ]]; fi
echo '{"scope":"info_listener_transport_only"}'
`)
			probePath := write("probe", probe, 0700)
			in := diagnosticInput(t)
			in["phase"], in["info_dns"] = "diagnostics", "peer.test"
			in["verification"] = map[string]any{"diagnostic_probe": probePath, "diagnostic_probe_sha256": hash(probe),
				"info_server_cert": filepath.Join(dir, "original/tls.crt"), "info_server_cert_sha256": hash(read("original/tls.crt")),
				"client_tls_dir": filepath.Join(dir, "original")}
			mode, phase := "expand", "after"
			if scenario == "restore" {
				mode = "restore"
			}
			if scenario == "preflight" || scenario == "hash" || scenario == "dns" || scenario == "key" {
				phase = "preflight"
			}
			if scenario == "hash" {
				in["verification"].(map[string]any)["diagnostic_probe_sha256"] = strings.Repeat("0", 64)
			}
			if scenario == "dns" {
				in["info_dns"] = "wrong.test"
			}
			if scenario == "key" {
				write("original/probe.key", read("member/tls.key"), 0600)
			}
			marshal := func(value any) []byte { data, err := json.Marshal(value); require.NoError(t, err); return data }
			write("receipt.json", marshal(in), 0600)
			current := in["current"].(map[string]any)
			if mode == "expand" && phase == "after" {
				patch, err := diagnosticPlan(t, in)
				require.NoError(t, err, string(patch))
				current = applyTrustPatch(t, current, patch)
			}
			require.NoError(t, os.Mkdir(filepath.Join(dir, "after"), 0700))
			write("after/current.json", marshal(current), 0600)
			write("namespace.json", marshal(in["namespace"]), 0600)
			var pods []any
			for _, suffix := range []string{"0", "1", "2"} {
				name := trustMap(current, "metadata")["name"].(string) + "-" + suffix
				pod := map[string]any{"metadata": map[string]any{"name": name, "uid": "pod-" + suffix, "ownerReferences": []any{map[string]any{"uid": trustMap(current, "metadata")["uid"], "controller": true}}},
					"spec": trustMap(current, "spec", "template")["spec"], "status": map[string]any{"conditions": []any{map[string]any{"type": "Ready", "status": "True"}}}}
				pods = append(pods, pod)
				write(name+".json", marshal(pod), 0600)
			}
			write("after/pods.json", marshal(map[string]any{"items": pods}), 0600)
			write("kubectl", []byte(`#!/usr/bin/env bash
set -euo pipefail
printf '%s\n' "$*" >> "$FIXTURE/kubectl-calls"
shift 5
case "$1 $2" in
 'get namespace') cat "$FIXTURE/namespace.json";;
 'get sts') cat "$FIXTURE/after/current.json";;
 'get pod')
  if [[ $SCENARIO == pod-replaced ]]; then jq '.metadata.uid="replaced"' "$FIXTURE/$3.json"; else cat "$FIXTURE/$3.json"; fi;;
 port-forward*)
  echo "Forwarding from 127.0.0.1:${4%:8080} -> 8080"
  exec sleep 30;;
 exec*) [[ $SCENARIO != health-failure ]];;
 *) exit 90;;
esac
`), 0700)
			kubeconfig := write("kubeconfig", []byte("mock"), 0600)
			cmd := exec.Command("bash", "verify-diagnostic-stage.sh", mode, phase, dir, kubeconfig, "test")
			cmd.Env = append(os.Environ(), "PATH="+dir+":"+os.Getenv("PATH"), "FIXTURE="+dir, "SCENARIO="+scenario)
			output, err := cmd.CombinedOutput()
			if scenario == "preflight" || scenario == "restore" || scenario == "expand" {
				require.NoError(t, err, string(output))
			} else {
				require.Error(t, err, string(output))
			}
			if phase == "preflight" {
				require.NoFileExists(t, filepath.Join(dir, "kubectl-calls"))
			} else {
				calls := string(read("kubectl-calls"))
				for _, forbidden := range []string{" get secret ", " patch ", " delete ", " apply "} {
					require.NotContains(t, calls, forbidden)
				}
				if scenario == "expand" || scenario == "restore" {
					require.Equal(t, 3, strings.Count(string(read("probe-calls")), "--mode "))
				}
				if scenario == "probe-failure" {
					require.Contains(t, string(read("probe-calls")), "--mode protected")
				}
				if scenario == "health-failure" {
					require.Contains(t, calls, "-- curl ")
					require.NoFileExists(t, filepath.Join(dir, "probe-calls"))
				}
			}
		})
	}
}
