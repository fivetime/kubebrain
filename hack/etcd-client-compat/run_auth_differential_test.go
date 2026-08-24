package compat

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestAuthDifferentialRunnerFailsClosed(t *testing.T) {
	script, err := os.ReadFile("run-auth-differential.sh")
	require.NoError(t, err)
	content := string(script)
	require.Contains(t, content, "ALLOW_DESTRUCTIVE_AUTH_DIFFERENTIAL")
	require.Contains(t, content, "auth revision must be 1")
	require.Contains(t, content, "has keys, users, roles, leases, or alarms during ${phase}")
	require.Contains(t, content, "advertised client URL is unreachable")
	require.Contains(t, content, "assert_clean_endpoint preflight")
	require.Contains(t, content, "assert_clean_endpoint postflight")
	require.Contains(t, content, "alarm list -w json")
	require.Contains(t, content, ") || test_status=$?")
	require.Contains(t, content, "Auth differential test package failed with status")
	require.Contains(t, content, "-run '^TestAuthDifferentialAgainstEtcd$'")
}

func TestAuthDifferentialRunnerRejectsUnreachableAdvertisedClientURLBeforeMutation(t *testing.T) {
	dir := t.TempDir()
	fakeEtcd := writeFakeReferenceEtcd(t, dir)
	fakeEtcdctl := filepath.Join(dir, "etcdctl")
	require.NoError(t, os.WriteFile(fakeEtcdctl, []byte(`#!/usr/bin/env bash
set -euo pipefail
if [[ "${1:-}" == "--version" ]]; then
  printf 'Git SHA: d947b2086\n'
  exit 0
fi
if [[ "$*" == *"member list -w json"* ]]; then
  printf '%s\n' '{"members":[{"name":"kubebrain-0","clientURLs":["http://internal.invalid:3379"]}]}'
  exit 0
fi
if [[ "$*" == *"--endpoints=http://internal.invalid:3379"* ]]; then
  exit 1
fi
if [[ "$*" == *"auth status"* || "$*" == *"user list"* || "$*" == *"role list"* || "$*" == *"lease list"* ]]; then
  echo "mutation-adjacent preflight reached" >&2
  exit 9
fi
if [[ "$*" == *"endpoint health"* ]]; then
  exit 0
fi
exit 1
`), 0o755))
	fakeCurl := filepath.Join(dir, "curl")
	require.NoError(t, os.WriteFile(fakeCurl, []byte("#!/usr/bin/env bash\nexit 1\n"), 0o755))

	output, err := runCompatScriptCommand(t, "run-auth-differential.sh", []string{
		"PATH=" + dir + ":" + os.Getenv("PATH"),
		"KUBEBRAIN_AUTH_DIFF_ENDPOINT=127.0.0.1:22379",
		"ALLOW_DESTRUCTIVE_AUTH_DIFFERENTIAL=true",
		"REFERENCE_ETCD_BIN=" + fakeEtcd,
		"REFERENCE_ETCD_EXPECTED_GIT_SHA=d947b2086",
		"ETCDCTL_BIN=" + fakeEtcdctl,
	})
	require.Error(t, err)
	require.Contains(t, string(output), "advertised client URL is unreachable")
	require.Contains(t, string(output), "http://internal.invalid:3379")
	require.NotContains(t, string(output), "mutation-adjacent preflight reached")
}

func TestAuthRunnersRejectDirtyAlarmStateBeforeReferenceStart(t *testing.T) {
	for _, testCase := range []struct {
		name     string
		script   string
		endpoint string
		approval string
	}{
		{name: "simple-token", script: "run-auth-differential.sh", endpoint: "KUBEBRAIN_AUTH_DIFF_ENDPOINT", approval: "ALLOW_DESTRUCTIVE_AUTH_DIFFERENTIAL"},
		{name: "jwt", script: "run-jwt-differential.sh", endpoint: "KUBEBRAIN_JWT_ETCD_ENDPOINT", approval: "ALLOW_DESTRUCTIVE_JWT_DIFFERENTIAL"},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			dir := t.TempDir()
			fakeEtcd := writeFakeReferenceEtcd(t, dir)
			fakeEtcdctl := filepath.Join(dir, "etcdctl")
			require.NoError(t, os.WriteFile(fakeEtcdctl, []byte(`#!/usr/bin/env bash
set -euo pipefail
if [[ "${1:-}" == "--version" ]]; then
  printf 'Git SHA: d947b2086\n'
  exit 0
fi
case "$*" in
  *"member list -w json"*) printf '%s\n' '{"members":[{"name":"kubebrain-0","clientURLs":["http://127.0.0.1:22379"]}]}' ;;
  *"auth status -w json"*) printf '%s\n' '{"enabled":false,"authRevision":1}' ;;
  *"get  --from-key --limit=1 -w json"*) printf '%s\n' '{"count":0}' ;;
  *"user list -w json"*) printf '%s\n' '{"users":[]}' ;;
  *"role list -w json"*) printf '%s\n' '{"roles":[]}' ;;
  *"lease list -w json"*) printf '%s\n' '{"leases":[]}' ;;
  *"alarm list -w json"*) printf '%s\n' '{"alarms":[{"memberID":7,"alarm":1}]}' ;;
  *"endpoint health"*) ;;
  *) exit 1 ;;
esac
`), 0o755))
			require.NoError(t, os.WriteFile(filepath.Join(dir, "curl"), []byte("#!/usr/bin/env bash\nexit 1\n"), 0o755))

			output, err := runCompatScriptCommand(t, testCase.script, []string{
				"PATH=" + dir + ":" + os.Getenv("PATH"),
				testCase.endpoint + "=127.0.0.1:22379",
				testCase.approval + "=true",
				"REFERENCE_ETCD_BIN=" + fakeEtcd,
				"REFERENCE_ETCD_EXPECTED_GIT_SHA=d947b2086",
				"ETCDCTL_BIN=" + fakeEtcdctl,
			})
			require.Error(t, err)
			require.Contains(t, string(output), "has keys, users, roles, leases, or alarms during preflight")
			require.NotContains(t, string(output), "etcd exited before becoming healthy")
		})
	}
}

func TestAuthDifferentialRunnerRejectsInvalidApprovalBeforeDependencies(t *testing.T) {
	output, err := runCompatScriptCommand(t, "run-auth-differential.sh", []string{
		"PATH=" + t.TempDir() + ":/usr/bin:/bin",
		"ALLOW_DESTRUCTIVE_AUTH_DIFFERENTIAL=maybe",
	})
	require.Error(t, err)
	require.Contains(t, string(output), "ALLOW_DESTRUCTIVE_AUTH_DIFFERENTIAL must be true or false")
	require.NotContains(t, string(output), "missing required command")
}

func TestAuthDifferentialRunnerRejectsInvalidRaceModeBeforeDependencies(t *testing.T) {
	output, err := runCompatScriptCommand(t, "run-auth-differential.sh", []string{
		"PATH=" + t.TempDir() + ":/usr/bin:/bin",
		"GO_TEST_RACE=maybe",
	})
	require.Error(t, err)
	require.Contains(t, string(output), "GO_TEST_RACE must be true or false")
	require.NotContains(t, string(output), "missing required command")
}

func TestAuthDifferentialRunnerRequiresDisposableEndpoint(t *testing.T) {
	output, err := runCompatScriptCommand(t, "run-auth-differential.sh", []string{
		"PATH=" + t.TempDir() + ":/usr/bin:/bin",
		"ALLOW_DESTRUCTIVE_AUTH_DIFFERENTIAL=true",
	})
	require.Error(t, err)
	require.Contains(t, string(output), "set KUBEBRAIN_AUTH_DIFF_ENDPOINT")
	require.NotContains(t, string(output), "missing required command")
}

func TestAuthDifferentialRunnerRequiresDestructiveApproval(t *testing.T) {
	output, err := runCompatScriptCommand(t, "run-auth-differential.sh", []string{
		"PATH=" + t.TempDir() + ":/usr/bin:/bin",
		"KUBEBRAIN_AUTH_DIFF_ENDPOINT=127.0.0.1:23379",
	})
	require.Error(t, err)
	require.Contains(t, string(output), "refusing destructive Auth differential")
	require.NotContains(t, string(output), "missing required command")
}
