package production_test

import (
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestEnvoyRolloutMigrationUsesTwoFailClosedPhases(t *testing.T) {
	root := productionRepositoryRoot(t)
	temporary := t.TempDir()
	fakeKubectl := filepath.Join(temporary, "kubectl")
	logFile := filepath.Join(temporary, "commands.log")
	stateFile := filepath.Join(temporary, "state")
	require.NoError(t, os.WriteFile(stateFile, []byte("0\n"), 0o600))
	require.NoError(t, os.WriteFile(fakeKubectl, []byte(fakeEnvoyMigrationKubectl), 0o700))

	command := exec.Command("bash", filepath.Join(root, "hack/production/migrate-envoy-zero-unavailable.sh"))
	command.Env = append(os.Environ(),
		"KUBECTL="+fakeKubectl,
		"KUBE_CONTEXT=production-test",
		"PROFILE_DIR="+filepath.Join(root, "deploy/production/envoy"),
		"FAKE_KUBECTL_LOG="+logFile,
		"FAKE_KUBECTL_STATE="+stateFile,
		"FAKE_POD_OVERLAP_ONCE=true",
	)
	output, err := command.CombinedOutput()
	require.NoError(t, err, string(output))
	require.Contains(t, string(output), "migrated to maxUnavailable=0/maxSurge=1")

	commands, err := os.ReadFile(logFile)
	require.NoError(t, err)
	lines := strings.Split(strings.TrimSpace(string(commands)), "\n")
	require.Equal(t, 1, countExactLine(lines, "apply maxUnavailable=1"))
	require.Equal(t, 1, countExactLine(lines, "apply maxUnavailable=0"))
	require.Equal(t, 2, countExactLine(lines, "rollout status"))
	require.Equal(t, 1, countExactLine(lines, "terminating pod overlap"))
	firstMigration := indexExactLine(lines, "apply maxUnavailable=1")
	firstCanonical := indexExactLine(lines, "apply maxUnavailable=0")
	require.Greater(t, firstCanonical, firstMigration)
}

func TestEnvoyRolloutMigrationRejectsImplicitContext(t *testing.T) {
	root := productionRepositoryRoot(t)
	command := exec.Command("bash", filepath.Join(root, "hack/production/migrate-envoy-zero-unavailable.sh"))
	command.Env = append(os.Environ(), "KUBE_CONTEXT=")
	output, err := command.CombinedOutput()
	require.Error(t, err)
	require.Contains(t, string(output), "current context is never accepted implicitly")
}

func productionRepositoryRoot(t *testing.T) string {
	t.Helper()
	_, file, _, ok := runtime.Caller(0)
	require.True(t, ok)
	return filepath.Clean(filepath.Join(filepath.Dir(file), "../.."))
}

func countExactLine(lines []string, target string) int {
	count := 0
	for _, line := range lines {
		if line == target {
			count++
		}
	}
	return count
}

func indexExactLine(lines []string, target string) int {
	for index, line := range lines {
		if line == target {
			return index
		}
	}
	return -1
}

const fakeEnvoyMigrationKubectl = `#!/usr/bin/env bash
set -euo pipefail
log="${FAKE_KUBECTL_LOG:?}"
state_file="${FAKE_KUBECTL_STATE:?}"
if [[ "${1:-}" == config ]]; then
  exit 0
fi
if [[ "${1:-}" == kustomize ]]; then
  cat <<'YAML'
apiVersion: apps/v1
kind: Deployment
metadata:
  name: kubebrain-envoy
  namespace: kubebrain-system
spec:
  strategy:
    rollingUpdate:
      maxUnavailable: 0
      maxSurge: 1
  template:
    spec:
      topologySpreadConstraints:
        - minDomains: 3
          nodeTaintsPolicy: Honor
          matchLabelKeys:
            - pod-template-hash
YAML
  exit 0
fi
while [[ "${1:-}" == --context || "${1:-}" == --namespace ]]; do
  shift 2
done
state="$(<"$state_file")"
if [[ "${1:-}" == apply ]]; then
  input="$(cat)"
  if grep -q '^      maxUnavailable: 1$' <<<"$input"; then
    printf '%s\n' 'apply maxUnavailable=1' >>"$log"
    printf '1\n' >"$state_file"
  elif grep -q '^      maxUnavailable: 0$' <<<"$input"; then
    printf '%s\n' 'apply maxUnavailable=0' >>"$log"
    printf '2\n' >"$state_file"
  else
    exit 9
  fi
  exit 0
fi
if [[ "${1:-}" == rollout && "${2:-}" == status ]]; then
  printf '%s\n' 'rollout status' >>"$log"
  exit 0
fi
if [[ "${1:-}" == get && "${2:-}" == deployment ]]; then
  if [[ "$*" == *requiredDuringSchedulingIgnoredDuringExecution* ]]; then
    [[ "$state" == 0 ]] && printf '%s\n' kubernetes.io/hostname
    exit 0
  fi
  if [[ "$state" == 2 ]]; then
    printf '3\t3\t3\t0\t1\n'
  else
    printf '3\t3\t3\t1\t1\n'
  fi
  exit 0
fi
if [[ "${1:-}" == get && "${2:-}" == pods ]]; then
  if [[ "${FAKE_POD_OVERLAP_ONCE:-}" == true && ! -e "${state_file}.pods-seen" ]]; then
    : >"${state_file}.pods-seen"
    printf '%s\n' 'terminating pod overlap' >>"$log"
    printf 'uid-old\tnode-a\tFalse\thash-old\nuid-a\tnode-a\tTrue\thash-new\nuid-b\tnode-b\tTrue\thash-new\nuid-c\tnode-c\tTrue\thash-new\n'
    exit 0
  fi
  printf 'uid-a\tnode-a\tTrue\thash-new\nuid-b\tnode-b\tTrue\thash-new\nuid-c\tnode-c\tTrue\thash-new\n'
  exit 0
fi
exit 10
`
