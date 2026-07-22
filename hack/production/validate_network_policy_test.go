package production_test

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestValidateNetworkPolicy(t *testing.T) {
	for _, tc := range []struct {
		name        string
		mode        string
		wantOK      bool
		wantOutput  string
		wantDeletes int
		wantRuns    int
	}{
		{name: "all allowed and denied paths enforced", mode: "success", wantOK: true, wantOutput: "release gate passed", wantDeletes: 3, wantRuns: 3},
		{name: "denied path reachable", mode: "denied-reachable", wantOutput: "denied network path was reachable", wantDeletes: 3, wantRuns: 3},
		{name: "allowed path blocked", mode: "allowed-blocked", wantOutput: "allowed network path failed", wantDeletes: 3, wantRuns: 3},
		{name: "client namespace label missing", mode: "wrong-label", wantOutput: "client namespace must have", wantDeletes: 0, wantRuns: 0},
		{name: "ready failure cleans created pods", mode: "wait-failure", wantDeletes: 2, wantRuns: 2},
		{name: "existing pod is never reused", mode: "existing-pod", wantOutput: "refusing to reuse existing probe Pod", wantDeletes: 0, wantRuns: 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			logPath := filepath.Join(dir, "kubectl.log")
			fakeKubectl := filepath.Join(dir, "kubectl")
			require.NoError(t, os.WriteFile(fakeKubectl, []byte(`#!/usr/bin/env bash
set -euo pipefail
printf '%s\n' "$*" >>"$FAKE_LOG"
args="$*"
if [[ "$args" == *"get namespace client-ns"* ]]; then
  if [[ "$FAKE_MODE" != "wrong-label" && "$args" == *"client-access"* ]]; then printf 'true'; fi
  exit 0
fi
if [[ "$args" == *"get namespace monitor-ns"* ]]; then
  if [[ "$args" == *"monitoring-access"* ]]; then printf 'true'; fi
  exit 0
fi
if [[ "$args" == *"get namespace denied-ns"* ]]; then
  exit 0
fi
if [[ "$args" == *" get pod "* ]]; then
  if [[ "$FAKE_MODE" == "existing-pod" && "$args" == *"kb-net-client-probe1"* ]]; then exit 0; fi
  exit 1
fi
if [[ "$args" == *" wait pod "* && "$FAKE_MODE" == "wait-failure" && "$args" == *"kb-net-monitor-probe1"* ]]; then
  exit 1
fi
if [[ "$args" == *" exec "* ]]; then
  if [[ "$FAKE_MODE" == "allowed-blocked" && "$args" == *"kb-net-monitor-probe1"* && "$args" == *"/dev/tcp/kb-pd.tidb-cluster.svc/2379"* ]]; then
    exit 1
  fi
  if [[ "$args" == *"kb-net-denied-probe1"* ]]; then
    if [[ "$FAKE_MODE" == "denied-reachable" ]]; then exit 0; fi
    exit 1
  fi
fi
exit 0
`), 0o755))

			command := exec.Command("bash", "validate-network-policy.sh")
			command.Env = append(os.Environ(),
				"KUBECTL="+fakeKubectl,
				"KUBE_CONTEXT=production",
				"PROBE_ID=probe1",
				"PROBE_IMAGE=registry.example/probe@sha256:"+strings.Repeat("a", 64),
				"CLIENT_NAMESPACE=client-ns",
				"MONITORING_NAMESPACE=monitor-ns",
				"DENIED_NAMESPACE=denied-ns",
				"FAKE_MODE="+tc.mode,
				"FAKE_LOG="+logPath,
			)
			output, err := command.CombinedOutput()
			if tc.wantOK {
				require.NoError(t, err, string(output))
			} else {
				require.Error(t, err, string(output))
			}
			if tc.wantOutput != "" {
				require.Contains(t, string(output), tc.wantOutput)
			}
			logData, readErr := os.ReadFile(logPath)
			require.NoError(t, readErr)
			log := string(logData)
			require.Equal(t, tc.wantDeletes, strings.Count(log, " delete pod "), log)
			require.Equal(t, tc.wantRuns, strings.Count(log, " run kb-net-"), log)
			require.NotContains(t, log, "--context  production")
			if log != "" {
				require.Contains(t, log, "--context production")
			}
		})
	}
}

func TestValidateNetworkPolicyRejectsMutableProbeImageBeforeKubectl(t *testing.T) {
	for _, probeImage := range []string{
		"registry.example/probe:latest",
		"registry.example/probe @sha256:" + strings.Repeat("a", 64),
		"registry.example/probe@sha256:" + strings.Repeat("A", 64),
	} {
		t.Run(probeImage, func(t *testing.T) {
			dir := t.TempDir()
			logPath := filepath.Join(dir, "kubectl.log")
			fakeKubectl := filepath.Join(dir, "kubectl")
			require.NoError(t, os.WriteFile(
				fakeKubectl,
				[]byte("#!/usr/bin/env bash\nprintf called >>\"$FAKE_LOG\"\n"),
				0o755,
			))

			command := exec.Command("bash", "validate-network-policy.sh")
			command.Env = append(os.Environ(),
				"KUBECTL="+fakeKubectl,
				"PROBE_ID=probe1",
				"PROBE_IMAGE="+probeImage,
				"CLIENT_NAMESPACE=client-ns",
				"MONITORING_NAMESPACE=monitor-ns",
				"DENIED_NAMESPACE=denied-ns",
				"FAKE_LOG="+logPath,
			)
			output, err := command.CombinedOutput()
			require.Error(t, err)
			require.Contains(t, string(output), "immutable sha256 digest")
			_, statErr := os.Stat(logPath)
			require.ErrorIs(t, statErr, os.ErrNotExist)
		})
	}
}
