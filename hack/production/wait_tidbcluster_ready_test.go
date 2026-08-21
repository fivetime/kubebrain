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
		controls   []string
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
			controls: []string{
				"TIMEOUT_SECONDS=86400",
				"POLL_INTERVAL_SECONDS=86400",
				"TIKV_RPC_PROBE_TIMEOUT_SECONDS=86400",
			},
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
			name:       "generation overflow is not complete",
			ready:      "True",
			pdStatus:   "9223372036854775808\t9223372036854775808\t3\t3\t3\tpd-new\tpd-new",
			tikvStatus: "7\t7\t3\t3\t3\ttikv-new\ttikv-new",
			wantOutput: "timed out",
		},
		{
			name:       "desired replicas above int32 is not complete",
			ready:      "True",
			pdStatus:   "5\t5\t3\t3\t3\tpd-new\tpd-new",
			tikvStatus: "7\t7\t2147483648\t2147483648\t2147483648\ttikv-new\ttikv-new",
			tikvProbe:  "20160\t10\t5",
			wantOutput: "TiKV-Debug-RPC=not-checked",
		},
		{
			name:       "maximum int32 desired replicas reaches rpc topology check",
			ready:      "True",
			pdStatus:   "5\t5\t3\t3\t3\tpd-new\tpd-new",
			tikvStatus: "7\t7\t2147483647\t2147483647\t2147483647\ttikv-new\ttikv-new",
			tikvProbe:  "20160\t10\t5",
			wantOutput: "TiKV-Debug-RPC=not-ready",
		},
		{
			name:       "desired replicas overflow is not complete",
			ready:      "True",
			pdStatus:   "5\t5\t3\t3\t3\tpd-new\tpd-new",
			tikvStatus: "7\t7\t9223372036854775808\t9223372036854775808\t9223372036854775808\ttikv-new\ttikv-new",
			tikvProbe:  "20160\t10\t5",
			wantOutput: "TiKV-Debug-RPC=not-checked",
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
			tikvRPCLog := filepath.Join(t.TempDir(), "tikv-rpc.log")
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
				"TIKV_RPC_LOG=" + tikvRPCLog,
			}
			env = append(env, tc.controls...)
			output, err := runProductionScriptCommand(t, "wait-tidbcluster-ready.sh", env)
			if tc.wantOK {
				require.NoError(t, err, string(output))
			} else {
				require.Error(t, err, string(output))
			}
			require.Contains(t, strings.TrimSpace(string(output)), tc.wantOutput)
			if tc.wantOK {
				probeLog, readErr := os.ReadFile(tikvRPCLog)
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
			wantOutput: "TIKV_RPC_PROBE_TIMEOUT_SECONDS must be a positive int64",
		},
		{name: "timeout overflow", env: "TIMEOUT_SECONDS=9223372036854775808", wantOutput: "TIMEOUT_SECONDS must be a positive int64"},
		{name: "timeout above one day", env: "TIMEOUT_SECONDS=86401", wantOutput: "TIMEOUT_SECONDS must be a positive int64"},
		{name: "poll overflow", env: "POLL_INTERVAL_SECONDS=9223372036854775808", wantOutput: "POLL_INTERVAL_SECONDS must be a non-negative int64"},
		{name: "poll above timeout", env: "POLL_INTERVAL_SECONDS=2", wantOutput: "POLL_INTERVAL_SECONDS must be a non-negative int64"},
		{name: "rpc timeout overflow", env: "TIKV_RPC_PROBE_TIMEOUT_SECONDS=9223372036854775808", wantOutput: "TIKV_RPC_PROBE_TIMEOUT_SECONDS must be a positive int64"},
		{name: "rpc timeout above one day", env: "TIKV_RPC_PROBE_TIMEOUT_SECONDS=86401", wantOutput: "TIKV_RPC_PROBE_TIMEOUT_SECONDS must be a positive int64"},
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
