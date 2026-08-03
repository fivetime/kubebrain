package compat

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestDifferentialRunnerRejectsInvalidDestructiveApprovalBeforeDependencies(t *testing.T) {
	dir := t.TempDir()
	env := []string{
		"PATH=" + dir + ":/usr/bin:/bin",
		"ALLOW_DESTRUCTIVE_DIFFERENTIAL=maybe",
	}
	output, err := runDifferentialScript(t, env)
	require.Error(t, err)
	require.Contains(t, string(output), "ALLOW_DESTRUCTIVE_DIFFERENTIAL must be true or false, got maybe")
	require.NotContains(t, string(output), "missing required command")
}

func TestDifferentialRunnerRejectsMissingDestructiveApprovalBeforeDependencies(t *testing.T) {
	dir := t.TempDir()
	env := []string{
		"PATH=" + dir + ":/usr/bin:/bin",
		"KUBEBRAIN_ETCD_ENDPOINT=127.0.0.1:22379",
	}
	output, err := runDifferentialScript(t, env)
	require.Error(t, err)
	require.Contains(t, string(output), "refusing destructive differential suite")
	require.NotContains(t, string(output), "missing required command")
}

func TestDifferentialRunnerRejectsUnreachableAdvertisedClientURL(t *testing.T) {
	dir := t.TempDir()
	fakeEtcdctl := filepath.Join(dir, "etcdctl")
	require.NoError(t, os.WriteFile(fakeEtcdctl, []byte(`#!/usr/bin/env bash
set -euo pipefail
if [[ "$*" == *"member list -w json"* ]]; then
  printf '%s\n' '{"members":[{"name":"kubebrain-0","clientURLs":["http://internal.invalid:3379"]}]}'
  exit 0
fi
if [[ "$*" == *"--endpoints=http://internal.invalid:3379"* ]]; then
  exit 1
fi
if [[ "$*" == *"endpoint health"* ]]; then
  exit 0
fi
exit 1
`), 0o755))
	fakeCurl := filepath.Join(dir, "curl")
	require.NoError(t, os.WriteFile(fakeCurl, []byte("#!/usr/bin/env bash\nexit 1\n"), 0o755))

	env := []string{
		"PATH=" + dir + ":" + os.Getenv("PATH"),
		"KUBEBRAIN_ETCD_ENDPOINT=127.0.0.1:22379",
		"ALLOW_DESTRUCTIVE_DIFFERENTIAL=true",
		"REFERENCE_ETCD_BIN=/bin/true",
		"ETCDCTL_BIN=" + fakeEtcdctl,
	}
	output, err := runDifferentialScript(t, env)
	require.Error(t, err)
	require.Contains(t, string(output), "advertised client URL is unreachable")
	require.Contains(t, string(output), "http://internal.invalid:3379")
}

func TestDifferentialRunnerRejectsMissingAdvertisedClientURLs(t *testing.T) {
	dir := t.TempDir()
	fakeEtcdctl := filepath.Join(dir, "etcdctl")
	require.NoError(t, os.WriteFile(fakeEtcdctl, []byte(`#!/usr/bin/env bash
set -euo pipefail
if [[ "$*" == *"member list -w json"* ]]; then
  printf '%s\n' '{"members":[{"name":"kubebrain-0","clientURLs":[]}]}'
  exit 0
fi
if [[ "$*" == *"endpoint health"* ]]; then
  exit 0
fi
exit 1
`), 0o755))
	fakeCurl := filepath.Join(dir, "curl")
	require.NoError(t, os.WriteFile(fakeCurl, []byte("#!/usr/bin/env bash\nexit 1\n"), 0o755))

	env := []string{
		"PATH=" + dir + ":" + os.Getenv("PATH"),
		"KUBEBRAIN_ETCD_ENDPOINT=127.0.0.1:22379",
		"ALLOW_DESTRUCTIVE_DIFFERENTIAL=true",
		"REFERENCE_ETCD_BIN=/bin/true",
		"ETCDCTL_BIN=" + fakeEtcdctl,
	}
	output, err := runDifferentialScript(t, env)
	require.Error(t, err)
	require.Contains(t, string(output), "returned no advertised client URLs")
}

func TestDifferentialRunnerChecksClusterLocalAdvertisedURLInSelectedPod(t *testing.T) {
	dir := t.TempDir()
	fakeEtcdctl := filepath.Join(dir, "etcdctl")
	require.NoError(t, os.WriteFile(fakeEtcdctl, []byte(`#!/usr/bin/env bash
set -euo pipefail
if [[ "$*" == *"member list -w json"* ]]; then
  printf '%s\n' '{"members":[{"name":"kubebrain-0","clientURLs":["http://internal.invalid:3379"]}]}'
  exit 0
fi
if [[ "$*" == *"endpoint health"* ]]; then
  exit 0
fi
exit 1
`), 0o755))
	fakeKubectl := filepath.Join(dir, "kubectl")
	kubectlLog := filepath.Join(dir, "kubectl.log")
	require.NoError(t, os.WriteFile(fakeKubectl, []byte(`#!/usr/bin/env bash
set -euo pipefail
printf '%s\n' "$*" >>"$KUBECTL_LOG"
while [[ "$1" != "--" ]]; do shift; done
shift
exec "$@"
`), 0o755))
	fakeCurl := filepath.Join(dir, "curl")
	require.NoError(t, os.WriteFile(fakeCurl, []byte("#!/usr/bin/env bash\nexit 1\n"), 0o755))

	env := []string{
		"PATH=" + dir + ":" + os.Getenv("PATH"),
		"KUBEBRAIN_ETCD_ENDPOINT=127.0.0.1:22379",
		"ALLOW_DESTRUCTIVE_DIFFERENTIAL=true",
		"REFERENCE_ETCD_BIN=/bin/true",
		"ETCDCTL_BIN=" + fakeEtcdctl,
		"KUBECTL=" + fakeKubectl,
		"KUBECTL_LOG=" + kubectlLog,
		"ETCDCTL_EXEC_POD=kubebrain-0",
		"ETCDCTL_EXEC_NAMESPACE=kubebrain-dev",
		"ETCDCTL_EXEC_CONTAINER=kubebrain",
		"ETCDCTL_EXEC_BIN=" + fakeEtcdctl,
	}
	output, err := runDifferentialScript(t, env)
	require.Error(t, err)
	require.NotContains(t, string(output), "advertised client URL is unreachable")
	require.Contains(t, string(output), "reference etcd exited before becoming healthy")
	log, readErr := os.ReadFile(kubectlLog)
	require.NoError(t, readErr)
	require.Contains(t, string(log), "-n kubebrain-dev exec kubebrain-0 -c kubebrain -- "+fakeEtcdctl)
	require.Contains(t, string(log), "--endpoints=http://internal.invalid:3379 endpoint health")
}

func TestDifferentialRunnerTestsUseBoundedScriptHelper(t *testing.T) {
	text, err := os.ReadFile("run_differential_test.go")
	require.NoError(t, err)
	forbiddenCommand := `exec.Command("bash", "` + `run-differential.sh")`
	forbiddenCombinedOutput := "." + "CombinedOutput()"
	require.Contains(t, string(text), "runDifferentialScript(t, env)")
	require.NotContains(t, string(text), forbiddenCommand)
	require.NotContains(t, string(text), forbiddenCombinedOutput)
}

func TestDifferentialRunnerSelectsScenariosNotRunnerSelfTests(t *testing.T) {
	script, err := os.ReadFile("run-differential.sh")
	require.NoError(t, err)
	require.Contains(t, string(script), "-run 'Differential(Against|$)'",
		"the live runner must not inherit its own opt-in environment into runner unit tests")
	require.NotContains(t, string(script), "-run Differential -count=1")
}
