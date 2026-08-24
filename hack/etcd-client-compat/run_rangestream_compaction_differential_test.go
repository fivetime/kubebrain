package compat

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRangeStreamCompactionRunnerPostflightsAfterTestFailure(t *testing.T) {
	dir := t.TempDir()
	started := filepath.Join(dir, "reference.started")
	etcdctlLog := filepath.Join(dir, "etcdctl.log")
	fakeEtcd := filepath.Join(dir, "etcd")
	require.NoError(t, os.WriteFile(fakeEtcd, []byte(`#!/usr/bin/env bash
set -euo pipefail
if [[ "${1:-}" == "--version" ]]; then
  printf 'etcd Version: 3.8.0-alpha.0\nGit SHA: d947b2086\n'
  exit 0
fi
touch "$REFERENCE_STARTED"
while true; do sleep 1; done
`), 0o755))
	fakeEtcdctl := filepath.Join(dir, "etcdctl")
	require.NoError(t, os.WriteFile(fakeEtcdctl, []byte(`#!/usr/bin/env bash
set -euo pipefail
printf '%s\n' "$*" >>"$ETCDCTL_LOG"
if [[ "${1:-}" == "--version" ]]; then
  printf 'Git SHA: d947b2086\n'
  exit 0
fi
case "$*" in
  *"member list -w json"*) printf '%s\n' '{"members":[{"name":"kubebrain-0","clientURLs":["http://127.0.0.1:22379"]}]}' ;;
  *"auth status -w json"*) printf '%s\n' '{"enabled":false,"authRevision":1}' ;;
  *"endpoint status -w json"*) printf '%s\n' '[{"Status":{"header":{"revision":1}}}]' ;;
  *"get  --from-key --limit=1 -w json"*) printf '%s\n' '{"count":0}' ;;
  *"user list -w json"*) printf '%s\n' '{"users":[]}' ;;
  *"role list -w json"*) printf '%s\n' '{"roles":[]}' ;;
  *"lease list -w json"*) printf '%s\n' '{"leases":[]}' ;;
  *"alarm list -w json"*) printf '%s\n' '{"alarms":[]}' ;;
  *"endpoint health"*) ;;
  *) exit 1 ;;
esac
`), 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "curl"), []byte(`#!/usr/bin/env bash
set -euo pipefail
[[ -f "$REFERENCE_STARTED" ]]
`), 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "go"), []byte("#!/usr/bin/env bash\nexit 23\n"), 0o755))

	output, err := runCompatScriptCommand(t, "run-rangestream-compaction-differential.sh", []string{
		"PATH=" + dir + ":" + os.Getenv("PATH"),
		"KUBEBRAIN_RANGESTREAM_COMPACTION_ENDPOINT=127.0.0.1:22379",
		"ALLOW_DESTRUCTIVE_RANGESTREAM_COMPACTION=true",
		"REFERENCE_ETCD_BIN=" + fakeEtcd,
		"REFERENCE_ETCD_EXPECTED_GIT_SHA=d947b2086",
		"ETCDCTL_BIN=" + fakeEtcdctl,
		"REFERENCE_STARTED=" + started,
		"ETCDCTL_LOG=" + etcdctlLog,
	})
	require.Error(t, err)
	require.Contains(t, string(output), "RangeStream compaction differential test package failed with status 23")
	log, readErr := os.ReadFile(etcdctlLog)
	require.NoError(t, readErr)
	require.Equal(t, 2, strings.Count(string(log), "auth status -w json"), "preflight and postflight must both query target state")
}

func TestRangeStreamCompactionDifferentialRunnerFailsClosed(t *testing.T) {
	script, err := os.ReadFile("run-rangestream-compaction-differential.sh")
	require.NoError(t, err)
	require.Contains(t, string(script), "ALLOW_DESTRUCTIVE_RANGESTREAM_COMPACTION")
	require.Contains(t, string(script), "data revision must be 1")
	require.Contains(t, string(script), "auth revision must be 1")
	require.Contains(t, string(script), "advertised client URL is unreachable")
	require.Contains(t, string(script), "assert_clean_endpoint postflight")
	require.Contains(t, string(script), "alarm list -w json")
	require.Contains(t, string(script), "-run '^TestRangeStream(Partial|Client)CompactionDifferential$'")
	require.Contains(t, string(script), "GO_TEST_RACE")
}

func TestRangeStreamCompactionDifferentialRunnerRejectsUnreachableAdvertisedClientURLBeforeMutation(t *testing.T) {
	requireRunnerRejectsUnreachableAdvertisedClientURLBeforeMutation(t,
		"run-rangestream-compaction-differential.sh",
		"KUBEBRAIN_RANGESTREAM_COMPACTION_ENDPOINT",
		"RangeStream advertised client URL is unreachable")
}

func TestRangeStreamCompactionDifferentialRunnerRejectsInvalidRaceBeforeDependencies(t *testing.T) {
	output, err := runCompatScriptCommand(t, "run-rangestream-compaction-differential.sh", []string{
		"PATH=" + t.TempDir() + ":/usr/bin:/bin",
		"GO_TEST_RACE=maybe",
	})
	require.Error(t, err)
	require.Contains(t, string(output), "GO_TEST_RACE must be true or false")
	require.NotContains(t, string(output), "missing required command")
}

func TestRangeStreamCompactionDifferentialRunnerRejectsInvalidApprovalBeforeDependencies(t *testing.T) {
	output, err := runCompatScriptCommand(t, "run-rangestream-compaction-differential.sh", []string{
		"PATH=" + t.TempDir() + ":/usr/bin:/bin",
		"ALLOW_DESTRUCTIVE_RANGESTREAM_COMPACTION=maybe",
	})
	require.Error(t, err)
	require.Contains(t, string(output), "ALLOW_DESTRUCTIVE_RANGESTREAM_COMPACTION must be true or false")
	require.NotContains(t, string(output), "missing required command")
}

func TestRangeStreamCompactionDifferentialRunnerRequiresDisposableEndpoint(t *testing.T) {
	output, err := runCompatScriptCommand(t, "run-rangestream-compaction-differential.sh", []string{
		"PATH=" + t.TempDir() + ":/usr/bin:/bin",
		"ALLOW_DESTRUCTIVE_RANGESTREAM_COMPACTION=true",
	})
	require.Error(t, err)
	require.Contains(t, string(output), "set KUBEBRAIN_RANGESTREAM_COMPACTION_ENDPOINT")
}

func TestRangeStreamCompactionDifferentialRunnerRequiresDestructiveApproval(t *testing.T) {
	output, err := runCompatScriptCommand(t, "run-rangestream-compaction-differential.sh", []string{
		"PATH=" + t.TempDir() + ":/usr/bin:/bin",
		"KUBEBRAIN_RANGESTREAM_COMPACTION_ENDPOINT=127.0.0.1:24379",
	})
	require.Error(t, err)
	require.Contains(t, string(output), "refusing destructive RangeStream compaction differential")
}
