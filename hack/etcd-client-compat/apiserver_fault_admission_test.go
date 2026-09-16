package compat

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestAPIServerFaultWaitsForAcknowledgedWatch(t *testing.T) {
	for _, tc := range []struct {
		name, marker, alive, timeout string
		ok                           bool
	}{
		{"ready", "APISERVER_WATCH_READY\n", "true", "10", true},
		{"no-marker", "starting\n", "true", "10", false},
		{"substring", "not APISERVER_WATCH_READY\n", "true", "10", false},
		{"dead-child", "APISERVER_WATCH_READY\n", "false", "10", false},
		{"invalid-timeout", "APISERVER_WATCH_READY\n", "true", "1+2", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			log := filepath.Join(t.TempDir(), "soak.log")
			require.NoError(t, os.WriteFile(log, []byte(tc.marker), 0600))
			out, err := runCompatCommandContext(t, context.Background(), "bash", []string{"-c", `
set -euo pipefail
source ../dev/wait-apiserver-watch-ready.sh
kill() { [[ "$CHILD_ALIVE" == true ]]; }
sleep() { SECONDS=$((SECONDS+100)); }
wait_apiserver_watch_ready 123 "$WATCH_LOG" "$READY_TIMEOUT"
echo FAULT_ADMITTED
`}, []string{"WATCH_LOG=" + log, "CHILD_ALIVE=" + tc.alive, "READY_TIMEOUT=" + tc.timeout})
			if tc.ok {
				require.NoError(t, err, "%s", out)
				require.Contains(t, string(out), "FAULT_ADMITTED")
			} else {
				require.Error(t, err)
				require.NotContains(t, string(out), "FAULT_ADMITTED")
			}
		})
	}
}

func TestAPIServerRolloutUsesWatchBarrierBeforeMutation(t *testing.T) {
	data, err := os.ReadFile(filepath.Join("..", "dev", "apiserver-rollout-smoke.sh"))
	require.NoError(t, err)
	body := string(data)
	wait := strings.Index(body, `wait_apiserver_watch_ready "$soak_pid" "$log_file" "$WATCH_TIMEOUT_SECONDS"`)
	restart := strings.Index(body, `rollout restart "$WORKLOAD"`)
	require.Greater(t, wait, 0)
	require.Greater(t, restart, wait)
	require.NotContains(t, body, "sleep 12")
	data, err = os.ReadFile(filepath.Join("..", "dev", "apiserver-watch-soak.sh"))
	require.NoError(t, err)
	body = string(data)
	marker := strings.Index(body, `echo "APISERVER_WATCH_READY"`)
	require.Greater(t, marker, strings.Index(body, `watch did not acknowledge the first workload mutation`))
	require.Contains(t, body, `if [[ "$update" == 1 && "$i" == 1 ]]; then`)
}
