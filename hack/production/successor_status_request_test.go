package production_test

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"github.com/stretchr/testify/require"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

func TestSuccessorStatusCallbackBindsTLSAndBudget(t *testing.T) {
	for _, scenario := range []string{"success", "changed-config", "redirect", "curl-error", "bad-port", "relative-key", "bad-budget"} {
		t.Run(scenario, func(t *testing.T) {
			dir := t.TempDir()
			config := map[string]string{"server_name": "peer.test", "port": "18890"}
			for _, name := range []string{"ca", "cert", "key"} {
				config[name] = filepath.Join(dir, name)
				require.NoError(t, os.WriteFile(config[name], []byte("not printed"), 0600))
			}
			if scenario == "bad-port" {
				config["port"] = "65536"
			}
			if scenario == "relative-key" {
				config["key"] = "key"
			}
			data, err := json.Marshal(config)
			require.NoError(t, err)
			hash := sha256.Sum256(data)
			path := filepath.Join(dir, "config.json")
			if scenario == "changed-config" {
				data = append(data, '\n')
			}
			require.NoError(t, os.WriteFile(path, data, 0600))
			require.NoError(t, os.WriteFile(filepath.Join(dir, "curl"), []byte(`#!/usr/bin/env bash
set -euo pipefail
printf '%s\n' "$@" > "$FIXTURE/curl-args"
[[ $1 == --disable ]]
while [[ $# -gt 0 ]]; do
 if [[ $1 == --output ]];then output=$2;shift 2;else shift;fi
done
printf '{"header":{"cluster_id":"42"}}' > "$output"
case "$SCENARIO" in redirect) printf 302;;curl-error) printf 000;exit 7;;*) printf 200;;esac
`), 0700))
			out := filepath.Join(dir, "sample")
			require.NoError(t, os.Mkdir(out, 0700))
			budget := "0.125000000"
			if scenario == "bad-budget" {
				budget = "5.000000001"
			}
			cmd := exec.Command("bash", "successor-status-request.sh", budget, filepath.Join(out, "status.json"))
			cmd.Env = append(os.Environ(), "PATH="+dir+":"+os.Getenv("PATH"), "FIXTURE="+dir, "SCENARIO="+scenario, "KB_SUCCESSOR_STATUS_CONFIG="+path, "KB_SUCCESSOR_STATUS_CONFIG_SHA256="+hex.EncodeToString(hash[:]))
			output, err := cmd.CombinedOutput()
			if scenario == "success" {
				require.NoError(t, err, string(output))
				args, err := os.ReadFile(filepath.Join(dir, "curl-args"))
				require.NoError(t, err)
				for _, part := range []string{"--max-time\n0.125000000\n", "--connect-timeout\n0.125000000\n", "--retry\n0\n", "--resolve\npeer.test:18890:127.0.0.1\n", "--cacert\n" + config["ca"], "--cert\n" + config["cert"], "--key\n" + config["key"], "https://peer.test:18890/v3/maintenance/status"} {
					require.Contains(t, string(args), part)
				}
				for _, arg := range strings.Fields(string(args)) {
					require.NotContains(t, []string{"--insecure", "-k", "--location", "-L"}, arg)
				}
			} else {
				require.Error(t, err, string(output))
			}
			require.NotContains(t, string(output), "not printed")
			if scenario == "changed-config" || scenario == "bad-port" || scenario == "relative-key" || scenario == "bad-budget" {
				require.NoFileExists(t, filepath.Join(dir, "curl-args"))
			}
		})
	}
}
