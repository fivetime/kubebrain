package production_test

import (
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestWaitTidbClusterReady(t *testing.T) {
	for _, tc := range []struct {
		name       string
		ready      string
		pdStatus   string
		tikvStatus string
		tikvProbe  string
		tikvRPC    bool
		wantOK     bool
		wantOutput string
	}{
		{
			name:       "converged",
			ready:      "True",
			pdStatus:   "5\t5\t3\t3\t3\tpd-new\tpd-new",
			tikvStatus: "7\t7\t3\t3\t3\ttikv-new\ttikv-new",
			tikvProbe:  "20160\t10\t5",
			tikvRPC:    true,
			wantOK:     true,
			wantOutput: "converged",
		},
		{
			name:       "http ready but tikv grpc request service is unavailable",
			ready:      "True",
			pdStatus:   "5\t5\t3\t3\t3\tpd-new\tpd-new",
			tikvStatus: "7\t7\t3\t3\t3\ttikv-new\ttikv-new",
			tikvProbe:  "20160\t10\t5",
			wantOutput: "TiKV-Debug-RPC=not-ready",
		},
		{
			name:       "tikv has no 20160 tcp readiness probe",
			ready:      "True",
			pdStatus:   "5\t5\t3\t3\t3\tpd-new\tpd-new",
			tikvStatus: "7\t7\t3\t3\t3\ttikv-new\ttikv-new",
			wantOutput: "TiKV-readiness-probe=missing",
		},
		{
			name:       "partitioned rollout is not complete",
			ready:      "True",
			pdStatus:   "5\t5\t3\t3\t1\tpd-old\tpd-new",
			tikvStatus: "7\t7\t3\t3\t3\ttikv-old\ttikv-old",
			wantOutput: "timed out",
		},
		{
			name:       "unobserved generation is not complete",
			ready:      "True",
			pdStatus:   "6\t5\t3\t3\t3\tpd-new\tpd-new",
			tikvStatus: "7\t7\t3\t3\t3\ttikv-new\ttikv-new",
			wantOutput: "timed out",
		},
		{
			name:       "cluster ready is required",
			ready:      "False",
			pdStatus:   "5\t5\t3\t3\t3\tpd-new\tpd-new",
			tikvStatus: "7\t7\t3\t3\t3\ttikv-new\ttikv-new",
			wantOutput: "Ready=False",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fakeKubectl := filepath.Join(t.TempDir(), "kubectl")
			require.NoError(t, os.WriteFile(fakeKubectl, []byte(`#!/usr/bin/env bash
set -euo pipefail
if [[ "$*" == *"get tidbcluster"* && "$*" == *"jsonpath="* ]]; then
  printf '%s' "$FAKE_READY"
elif [[ "$*" == *"get statefulset kb-pd"* && "$*" == *"jsonpath="* ]]; then
  printf '%s' "$FAKE_PD_STATUS"
elif [[ "$*" == *"get statefulset kb-tikv"* && "$*" == *"jsonpath="* ]]; then
  if [[ "$*" == *"readinessProbe.tcpSocket.port"* ]]; then
    printf '%s' "$FAKE_TIKV_PROBE"
  else
    printf '%s' "$FAKE_TIKV_STATUS"
  fi
elif [[ "$*" == *"get pods"* && "$*" == *"component=tikv"* ]]; then
  printf 'kb-tikv-0\nkb-tikv-1\nkb-tikv-2\n'
elif [[ "$*" == *"exec"* && "$*" == *"/tikv-ctl --host 127.0.0.1:20160 metrics"* ]]; then
  printf '%s\n' "$*" >>"$TIKV_RPC_LOG"
  [[ "$FAKE_TIKV_RPC_READY" == "true" ]]
else
  printf 'diagnostic output\n'
fi
`), 0o755))

			env := []string{
				"KUBECTL=" + fakeKubectl,
				"TIMEOUT_SECONDS=1",
				"POLL_INTERVAL_SECONDS=1",
				"FAKE_READY=" + tc.ready,
				"FAKE_PD_STATUS=" + tc.pdStatus,
				"FAKE_TIKV_STATUS=" + tc.tikvStatus,
				"FAKE_TIKV_PROBE=" + tc.tikvProbe,
				"FAKE_TIKV_RPC_READY=" + strconv.FormatBool(tc.tikvRPC),
				"TIKV_RPC_LOG=" + filepath.Join(t.TempDir(), "tikv-rpc.log"),
			}
			output, err := runProductionScriptCommand(t, "wait-tidbcluster-ready.sh", env)
			if tc.wantOK {
				require.NoError(t, err, string(output))
			} else {
				require.Error(t, err, string(output))
			}
			require.Contains(t, strings.TrimSpace(string(output)), tc.wantOutput)
			if tc.wantOK {
				probeLog, readErr := os.ReadFile(strings.TrimPrefix(env[len(env)-1], "TIKV_RPC_LOG="))
				require.NoError(t, readErr)
				require.Len(t, strings.Split(strings.TrimSpace(string(probeLog)), "\n"), 3,
					"every desired TiKV pod must receive a 20160 Debug RPC probe")
			}
		})
	}
}

func TestWaitTidbClusterReadyRejectsInvalidInputsBeforeKubectl(t *testing.T) {
	for _, tc := range []struct {
		name       string
		env        string
		wantOutput string
	}{
		{
			name:       "namespace",
			env:        "NAMESPACE=bad_namespace",
			wantOutput: "NAMESPACE must be a lowercase DNS label",
		},
		{
			name:       "tidb cluster",
			env:        "TIDB_CLUSTER=BadCluster",
			wantOutput: "TIDB_CLUSTER must be a lowercase DNS label",
		},
		{
			name:       "rpc timeout",
			env:        "TIKV_RPC_PROBE_TIMEOUT_SECONDS=0",
			wantOutput: "TIKV_RPC_PROBE_TIMEOUT_SECONDS must be a positive integer",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			tempDir := t.TempDir()
			kubectlLog := filepath.Join(tempDir, "kubectl.log")
			fakeKubectl := filepath.Join(tempDir, "kubectl")
			require.NoError(t, os.WriteFile(fakeKubectl, []byte(`#!/usr/bin/env bash
set -euo pipefail
printf 'called\n' >>"$KUBECTL_LOG"
exit 99
`), 0o755))

			env := []string{
				"KUBECTL=" + fakeKubectl,
				"KUBECTL_LOG=" + kubectlLog,
				"TIMEOUT_SECONDS=1",
				"POLL_INTERVAL_SECONDS=0",
				tc.env,
			}
			output, err := runProductionScriptCommand(t, "wait-tidbcluster-ready.sh", env)
			require.Error(t, err, string(output))
			require.Contains(t, string(output), tc.wantOutput)
			require.NoFileExists(t, kubectlLog)
		})
	}
}
