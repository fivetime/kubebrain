package testcluster_test

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestLocalBackendTCPObserver(t *testing.T) {
	for _, mode := range []string{"success", "connection-failure", "target-replaced", "source-restarted", "invalid-ip", "pagination"} {
		t.Run(mode, func(t *testing.T) {
			dir := t.TempDir()
			require.NoError(t, os.Chmod(dir, 0700))
			bin := filepath.Join(dir, "bin")
			require.NoError(t, os.Mkdir(bin, 0700))
			require.NoError(t, os.WriteFile(filepath.Join(dir, "mock-api"), []byte(ciliumCaptureMock), 0600))
			require.NoError(t, os.WriteFile(filepath.Join(bin, "kubectl"), []byte(tcpObserverMock), 0700))
			env := append(os.Environ(), "PATH="+bin+":"+os.Getenv("PATH"), "CAPTURE_FIXTURE="+dir, "CAPTURE_SCENARIO=stable", "TCP_SCENARIO="+mode)
			seed := exec.Command("bash", filepath.Join(dir, "mock-api"), "get", "pod", "kubebrain-local-0")
			seed.Env = env
			pod, err := seed.Output()
			require.NoError(t, err)
			require.NoError(t, os.WriteFile(filepath.Join(dir, "expected.json"), pod, 0600))
			require.NoError(t, os.Remove(filepath.Join(dir, "app-seen")))
			var targets, pods []map[string]interface{}
			for _, role := range []string{"pd", "tikv"} {
				for i := 0; i < 3; i++ {
					name := fmt.Sprintf("kb-local-%s-%d", role, i)
					ip := fmt.Sprintf("10.0.0.%d", len(targets)+1)
					port := 2379
					if role == "tikv" {
						port = 20160
					}
					targets = append(targets, map[string]interface{}{"name": name, "uid": name, "ip": ip, "port": port})
					pods = append(pods, map[string]interface{}{"apiVersion": "v1", "kind": "Pod", "metadata": map[string]string{"name": name, "uid": name, "namespace": "kubebrain-dbaas-test"}, "status": map[string]string{"podIP": ip}})
				}
			}
			if mode == "invalid-ip" {
				targets[0]["ip"] = "10.0.0.999"
			}
			data, err := json.Marshal(targets)
			require.NoError(t, err)
			require.NoError(t, os.WriteFile(filepath.Join(dir, "targets.json"), data, 0600))
			data, err = json.Marshal(map[string]interface{}{"items": pods})
			require.NoError(t, err)
			require.NoError(t, os.WriteFile(filepath.Join(dir, "pods.json"), data, 0600))
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			cmd := exec.CommandContext(ctx, "bash", "observe-local-backend-tcp.sh", dir, filepath.Join(dir, "expected.json"), filepath.Join(dir, "targets.json"))
			cmd.Env = env
			output, err := cmd.CombinedOutput()
			require.NoError(t, ctx.Err(), string(output))
			if mode == "success" {
				require.NoError(t, err, string(output))
			} else {
				require.Error(t, err, string(output))
			}
			observations, err := filepath.Glob(filepath.Join(dir, "backend-tcp.*"))
			require.NoError(t, err)
			require.Len(t, observations, 1)
			proof := filepath.Join(observations[0], "evidence.sha256")
			if mode == "success" {
				check := exec.Command("sha256sum", "-c", proof)
				result, err := check.CombinedOutput()
				require.NoError(t, err, string(result))
				results, err := os.ReadFile(filepath.Join(observations[0], "results.tsv"))
				require.NoError(t, err)
				require.Contains(t, string(results), "kb-local-tikv-2")
			} else {
				_, err := os.Stat(proof)
				require.True(t, os.IsNotExist(err))
			}
		})
	}
}

const tcpObserverMock = `#!/usr/bin/env bash
set -euo pipefail
while [[ $# -gt 0 && $1 != get && $1 != exec ]]; do shift; done
if [[ $1 == exec ]]; then
 [[ " $* " == *'set -e; exec 3<>'* ]] || exit 99
 [[ $TCP_SCENARIO != connection-failure ]] || exit 23
 exit 0
fi
if [[ $2 == pods ]]; then
 jq --arg mode "$TCP_SCENARIO" '
 if $mode=="target-replaced" then .items[0].metadata.uid="replacement"
 elif $mode=="pagination" then .metadata.continue="next" else . end' "$CAPTURE_FIXTURE/pods.json"
 exit
fi
if [[ $TCP_SCENARIO == source-restarted ]]; then export CAPTURE_SCENARIO=pod-restart; fi
exec bash "$CAPTURE_FIXTURE/mock-api" "$@"
`
