package production_test

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func writeLegacySnapshotFakeTools(t *testing.T) (string, string, string) {
	t.Helper()
	dir := t.TempDir()
	state := filepath.Join(dir, "state")
	etcdctl := filepath.Join(dir, "etcdctl")
	etcdutl := filepath.Join(dir, "etcdutl")
	require.NoError(t, os.WriteFile(etcdctl, []byte(`#!/usr/bin/env bash
set -euo pipefail
state="${FAKE_STATE:?}"
case "$*" in
  *"endpoint status"*)
    printf '"ClusterID" : %s\n' "${FAKE_CLUSTER_ID:-7301}"
    printf '%s\n' '"MemberID" : 17' '"Revision" : 41' '"RaftTerm" : 9'
    ;;
  *"snapshot save"*)
    output="${!#}"
    if [[ ! -e "$state/compacted" ]]; then
      case "${FAKE_SNAPSHOT_MODE:-ambiguous}" in
        healthy) printf 'already healthy\n' >"$output" ;;
        other) echo 'rpc error: code = Unavailable desc = etcdserver: no leader' >&2; exit 1 ;;
        ambiguous)
          echo 'rpc error: code = FailedPrecondition desc = snapshot cannot determine lease for retained legacy version: key "/old" revision 2' >&2
          exit 1
          ;;
        *) exit 9 ;;
      esac
    fi
    printf 'valid snapshot\n' >"$output"
    printf 'snapshot\n' >>"$state/calls"
    ;;
  *" compact "*" --physical")
    mkdir -p "$state"
    : >"$state/compacted"
    printf 'compact:%s\n' "${*: -2:1}" >>"$state/calls"
    ;;
  *) echo "unexpected etcdctl call: $*" >&2; exit 9 ;;
esac
`), 0o755))
	require.NoError(t, os.WriteFile(etcdutl, []byte(`#!/usr/bin/env bash
set -euo pipefail
[[ "$*" == snapshot\ status* ]] || exit 9
[[ -s "$3" ]] || exit 8
printf 'status\n' >>"${FAKE_STATE:?}/calls"
`), 0o755))
	return etcdctl, etcdutl, state
}

func TestLegacySnapshotHistoryRemediationNeverCompactsOtherSnapshotFailures(t *testing.T) {
	for _, mode := range []string{"healthy", "other"} {
		t.Run(mode, func(t *testing.T) {
			etcdctl, etcdutl, state := writeLegacySnapshotFakeTools(t)
			out, err := runProductionScriptCommand(t, "../backup/remediate-legacy-snapshot-history.sh", []string{
				"ACTION=compact", "ENDPOINT=https://kb:2379", "ETCDCTL_BIN=" + etcdctl,
				"ETCDUTL_BIN=" + etcdutl, "FAKE_STATE=" + state, "FAKE_SNAPSHOT_MODE=" + mode,
				"ALLOW_IRREVERSIBLE_LEGACY_HISTORY_COMPACTION=true",
				"CONFIRM_ENDPOINT=https://kb:2379", "EXPECTED_CLUSTER_ID=7301", "EXPECTED_REVISION=41",
				"OUTPUT=" + filepath.Join(t.TempDir(), "snapshot.db"),
			})
			require.Error(t, err, string(out))
			require.Contains(t, string(out), "refusing")
			require.NoFileExists(t, filepath.Join(state, "compacted"))
		})
	}
}

func TestLegacySnapshotHistoryRemediationDiagnosesWithoutMutation(t *testing.T) {
	etcdctl, etcdutl, state := writeLegacySnapshotFakeTools(t)
	out, err := runProductionScriptCommand(t, "../backup/remediate-legacy-snapshot-history.sh", []string{
		"ACTION=diagnose", "ENDPOINT=https://kb:2379", "ETCDCTL_BIN=" + etcdctl,
		"ETCDUTL_BIN=" + etcdutl, "FAKE_STATE=" + state,
	})
	require.Error(t, err, string(out))
	require.Contains(t, string(out), "cluster_id=7301")
	require.Contains(t, string(out), "revision=41")
	require.Contains(t, string(out), "snapshot_status=legacy_lease_history_ambiguous")
	require.NoFileExists(t, filepath.Join(state, "compacted"))
}

func TestLegacySnapshotHistoryRemediationPreservesUint64ClusterIDExactly(t *testing.T) {
	etcdctl, etcdutl, state := writeLegacySnapshotFakeTools(t)
	const clusterID = "18446744073709551615"
	out, err := runProductionScriptCommand(t, "../backup/remediate-legacy-snapshot-history.sh", []string{
		"ACTION=diagnose", "ENDPOINT=https://kb:2379", "ETCDCTL_BIN=" + etcdctl,
		"ETCDUTL_BIN=" + etcdutl, "FAKE_STATE=" + state, "FAKE_CLUSTER_ID=" + clusterID,
	})
	require.Error(t, err, string(out))
	require.Contains(t, string(out), "cluster_id="+clusterID+"\n")
}

func TestLegacySnapshotHistoryRemediationRequiresExactConfirmations(t *testing.T) {
	etcdctl, etcdutl, state := writeLegacySnapshotFakeTools(t)
	out, err := runProductionScriptCommand(t, "../backup/remediate-legacy-snapshot-history.sh", []string{
		"ACTION=compact", "ENDPOINT=https://kb:2379", "ETCDCTL_BIN=" + etcdctl,
		"ETCDUTL_BIN=" + etcdutl, "FAKE_STATE=" + state,
		"ALLOW_IRREVERSIBLE_LEGACY_HISTORY_COMPACTION=true",
		"CONFIRM_ENDPOINT=https://kb:2379", "EXPECTED_CLUSTER_ID=7301", "EXPECTED_REVISION=40",
		"OUTPUT=" + filepath.Join(t.TempDir(), "snapshot.db"),
	})
	require.Error(t, err, string(out))
	require.Contains(t, string(out), "EXPECTED_REVISION does not match")
	require.NoFileExists(t, filepath.Join(state, "compacted"))
}

func TestLegacySnapshotHistoryRemediationRefusesExistingOutputBeforeCompaction(t *testing.T) {
	etcdctl, etcdutl, state := writeLegacySnapshotFakeTools(t)
	output := filepath.Join(t.TempDir(), "snapshot.db")
	require.NoError(t, os.WriteFile(output, []byte("keep me"), 0o600))
	out, err := runProductionScriptCommand(t, "../backup/remediate-legacy-snapshot-history.sh", []string{
		"ACTION=compact", "ENDPOINT=https://kb:2379", "ETCDCTL_BIN=" + etcdctl,
		"ETCDUTL_BIN=" + etcdutl, "FAKE_STATE=" + state,
		"ALLOW_IRREVERSIBLE_LEGACY_HISTORY_COMPACTION=true",
		"CONFIRM_ENDPOINT=https://kb:2379", "EXPECTED_CLUSTER_ID=7301", "EXPECTED_REVISION=41",
		"OUTPUT=" + output,
	})
	require.Error(t, err, string(out))
	require.Contains(t, string(out), "refusing to overwrite existing OUTPUT")
	require.Equal(t, "keep me", string(requireFile(t, output)))
	require.NoFileExists(t, filepath.Join(state, "compacted"))
}

func TestLegacySnapshotHistoryRemediationCompactsAndPublishesValidatedSnapshot(t *testing.T) {
	etcdctl, etcdutl, state := writeLegacySnapshotFakeTools(t)
	output := filepath.Join(t.TempDir(), "snapshot.db")
	out, err := runProductionScriptCommand(t, "../backup/remediate-legacy-snapshot-history.sh", []string{
		"ACTION=compact", "ENDPOINT=https://kb:2379", "ETCDCTL_BIN=" + etcdctl,
		"ETCDUTL_BIN=" + etcdutl, "FAKE_STATE=" + state,
		"ALLOW_IRREVERSIBLE_LEGACY_HISTORY_COMPACTION=true",
		"CONFIRM_ENDPOINT=https://kb:2379", "EXPECTED_CLUSTER_ID=7301", "EXPECTED_REVISION=41",
		"OUTPUT=" + output,
	})
	require.NoError(t, err, string(out))
	require.Equal(t, "compact:41\nsnapshot\nstatus\n", string(requireFile(t, filepath.Join(state, "calls"))))
	require.Equal(t, "valid snapshot\n", string(requireFile(t, output)))
	require.Contains(t, string(out), "snapshot_status=remediated")
	require.Contains(t, string(out), "compacted_revision=41")
}

func requireFile(t *testing.T, path string) []byte {
	t.Helper()
	data, err := os.ReadFile(path)
	require.NoError(t, err)
	return data
}
