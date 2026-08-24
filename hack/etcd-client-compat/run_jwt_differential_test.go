package compat

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestJWTDifferentialRunnerFailsClosed(t *testing.T) {
	script, err := os.ReadFile("run-jwt-differential.sh")
	require.NoError(t, err)
	content := string(script)
	require.Contains(t, content, "ALLOW_DESTRUCTIVE_JWT_DIFFERENTIAL")
	require.Contains(t, content, "auth revision must be 1")
	require.Contains(t, content, "has keys, users, roles, leases, or alarms during ${phase}")
	require.Contains(t, content, "JWT HS256 key file is missing or empty")
	require.Contains(t, content, "advertised client URL is unreachable")
	require.Contains(t, content, "GO_TEST_RACE")
	require.Contains(t, content, "find \"$data_dir\" -depth -delete")
	require.Contains(t, content, "assert_clean_endpoint preflight")
	require.Contains(t, content, "assert_clean_endpoint postflight")
	require.Contains(t, content, "alarm list -w json")
	require.Contains(t, content, ") || test_status=$?")
	require.Contains(t, content, "JWT differential test package failed with status")
	require.Contains(t, content, "-run '^TestJWTAuthDifferentialAgainstReferenceEtcd$'")
}

func TestJWTDifferentialRunnerRejectsUnreachableAdvertisedClientURLBeforeMutation(t *testing.T) {
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

	output, err := runCompatScriptCommand(t, "run-jwt-differential.sh", []string{
		"PATH=" + dir + ":" + os.Getenv("PATH"),
		"KUBEBRAIN_JWT_ETCD_ENDPOINT=127.0.0.1:24379",
		"ALLOW_DESTRUCTIVE_JWT_DIFFERENTIAL=true",
		"REFERENCE_ETCD_BIN=" + fakeEtcd,
		"REFERENCE_ETCD_EXPECTED_GIT_SHA=d947b2086",
		"ETCDCTL_BIN=" + fakeEtcdctl,
	})
	require.Error(t, err)
	require.Contains(t, string(output), "advertised client URL is unreachable")
	require.Contains(t, string(output), "http://internal.invalid:3379")
	require.NotContains(t, string(output), "mutation-adjacent preflight reached")
}

func TestJWTDifferentialRunnerRejectsInvalidRaceBeforeDependencies(t *testing.T) {
	output, err := runCompatScriptCommand(t, "run-jwt-differential.sh", []string{
		"PATH=" + t.TempDir() + ":/usr/bin:/bin",
		"GO_TEST_RACE=maybe",
	})
	require.Error(t, err)
	require.Contains(t, string(output), "GO_TEST_RACE must be true or false")
	require.NotContains(t, string(output), "missing required command")
}

func TestJWTDifferentialRunnerRejectsInvalidApprovalBeforeDependencies(t *testing.T) {
	output, err := runCompatScriptCommand(t, "run-jwt-differential.sh", []string{
		"PATH=" + t.TempDir() + ":/usr/bin:/bin",
		"ALLOW_DESTRUCTIVE_JWT_DIFFERENTIAL=maybe",
	})
	require.Error(t, err)
	require.Contains(t, string(output), "ALLOW_DESTRUCTIVE_JWT_DIFFERENTIAL must be true or false")
	require.NotContains(t, string(output), "missing required command")
}

func TestJWTDifferentialRunnerRequiresDisposableEndpoint(t *testing.T) {
	output, err := runCompatScriptCommand(t, "run-jwt-differential.sh", []string{
		"PATH=" + t.TempDir() + ":/usr/bin:/bin",
		"ALLOW_DESTRUCTIVE_JWT_DIFFERENTIAL=true",
	})
	require.Error(t, err)
	require.Contains(t, string(output), "set KUBEBRAIN_JWT_ETCD_ENDPOINT")
}

func TestJWTDifferentialRunnerRequiresDestructiveApproval(t *testing.T) {
	output, err := runCompatScriptCommand(t, "run-jwt-differential.sh", []string{
		"PATH=" + t.TempDir() + ":/usr/bin:/bin",
		"KUBEBRAIN_JWT_ETCD_ENDPOINT=127.0.0.1:24379",
	})
	require.Error(t, err)
	require.Contains(t, string(output), "refusing destructive JWT differential")
}
