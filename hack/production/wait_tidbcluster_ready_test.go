package production_test

import (
	"os"
	"os/exec"
	"path/filepath"
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
		wantOK     bool
		wantOutput string
	}{
		{
			name:       "converged",
			ready:      "True",
			pdStatus:   "5\t5\t3\t3\t3\tpd-new\tpd-new",
			tikvStatus: "7\t7\t3\t3\t3\ttikv-new\ttikv-new",
			wantOK:     true,
			wantOutput: "converged",
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
  printf '%s' "$FAKE_TIKV_STATUS"
else
  printf 'diagnostic output\n'
fi
`), 0o755))

			command := exec.Command("bash", "wait-tidbcluster-ready.sh")
			command.Env = append(os.Environ(),
				"KUBECTL="+fakeKubectl,
				"TIMEOUT_SECONDS=1",
				"POLL_INTERVAL_SECONDS=0",
				"FAKE_READY="+tc.ready,
				"FAKE_PD_STATUS="+tc.pdStatus,
				"FAKE_TIKV_STATUS="+tc.tikvStatus,
			)
			output, err := command.CombinedOutput()
			if tc.wantOK {
				require.NoError(t, err, string(output))
			} else {
				require.Error(t, err, string(output))
			}
			require.Contains(t, strings.TrimSpace(string(output)), tc.wantOutput)
		})
	}
}
