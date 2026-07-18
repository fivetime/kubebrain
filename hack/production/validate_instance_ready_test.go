package production_test

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestValidateInstanceReady(t *testing.T) {
	for _, tc := range []struct {
		name       string
		image      string
		kubeStatus string
		topology   string
		healthOK   bool
		wantOK     bool
		wantOutput string
	}{
		{
			name:       "converged release",
			image:      "registry/kubebrain@sha256:abc",
			kubeStatus: "8\t8\t3\t3\t3\tkb-new\tkb-new\tregistry/kubebrain@sha256:abc",
			topology:   "3\t3",
			healthOK:   true,
			wantOK:     true,
			wantOutput: "release gate passed",
		},
		{
			name:       "wrong image",
			image:      "registry/kubebrain@sha256:wanted",
			kubeStatus: "8\t8\t3\t3\t3\tkb-new\tkb-new\tregistry/kubebrain@sha256:old",
			topology:   "3\t3",
			healthOK:   true,
			wantOutput: "not the expected converged release",
		},
		{
			name:       "stale rollout",
			image:      "registry/kubebrain@sha256:abc",
			kubeStatus: "9\t8\t3\t3\t2\tkb-old\tkb-new\tregistry/kubebrain@sha256:abc",
			topology:   "3\t3",
			healthOK:   true,
			wantOutput: "not the expected converged release",
		},
		{
			name:       "wrong storage topology",
			image:      "registry/kubebrain@sha256:abc",
			kubeStatus: "8\t8\t3\t3\t3\tkb-new\tkb-new\tregistry/kubebrain@sha256:abc",
			topology:   "1\t3",
			healthOK:   true,
			wantOutput: "topology mismatch",
		},
		{
			name:       "unhealthy endpoint",
			image:      "registry/kubebrain@sha256:abc",
			kubeStatus: "8\t8\t3\t3\t3\tkb-new\tkb-new\tregistry/kubebrain@sha256:abc",
			topology:   "3\t3",
			wantOutput: "endpoint health failed",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			fakeKubectl := filepath.Join(dir, "kubectl")
			require.NoError(t, os.WriteFile(fakeKubectl, []byte(`#!/usr/bin/env bash
set -euo pipefail
if [[ "$*" == *"get tidbcluster"* && "$*" == *".status.conditions"* ]]; then
  printf 'True'
elif [[ "$*" == *"get statefulset kb-pd"* && "$*" == *"jsonpath="* ]]; then
  printf '5\t5\t3\t3\t3\tpd-new\tpd-new'
elif [[ "$*" == *"get statefulset kb-tikv"* && "$*" == *"jsonpath="* ]]; then
  printf '7\t7\t3\t3\t3\ttikv-new\ttikv-new'
elif [[ "$*" == *"get tidbcluster"* && "$*" == *".spec.pd.replicas"* ]]; then
  printf '%s' "$FAKE_TOPOLOGY"
elif [[ "$*" == *"get statefulset kubebrain"* && "$*" == *"jsonpath="* ]]; then
  printf '%s' "$FAKE_KUBEBRAIN_STATUS"
else
  printf 'diagnostic output\n'
fi
`), 0o755))
			fakeEtcdctl := filepath.Join(dir, "etcdctl")
			require.NoError(t, os.WriteFile(fakeEtcdctl, []byte(`#!/usr/bin/env bash
set -euo pipefail
if [[ "$FAKE_HEALTH_OK" == "true" && "$*" == *"endpoint health"* ]]; then
  printf 'healthy\n'
  exit 0
fi
printf 'unhealthy\n' >&2
exit 1
`), 0o755))

			command := exec.Command("bash", "validate-instance-ready.sh")
			command.Env = append(os.Environ(),
				"KUBECTL="+fakeKubectl,
				"ETCDCTL="+fakeEtcdctl,
				"EXPECTED_IMAGE="+tc.image,
				"ENDPOINT=https://instance.example:2379",
				"TIMEOUT_SECONDS=1",
				"POLL_INTERVAL_SECONDS=0",
				"FAKE_KUBEBRAIN_STATUS="+tc.kubeStatus,
				"FAKE_TOPOLOGY="+tc.topology,
				"FAKE_HEALTH_OK="+boolString(tc.healthOK),
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

func TestValidateInstanceReadyRequiresImmutableInputs(t *testing.T) {
	command := exec.Command("bash", "validate-instance-ready.sh")
	command.Env = append(os.Environ(), "EXPECTED_IMAGE=", "ENDPOINT=")
	output, err := command.CombinedOutput()
	require.Error(t, err)
	require.Contains(t, string(output), "EXPECTED_IMAGE is required")
}

func boolString(value bool) string {
	if value {
		return "true"
	}
	return "false"
}
