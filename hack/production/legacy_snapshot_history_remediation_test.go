package production_test

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func writeLegacyRemediationCommand(t *testing.T, name string) (string, string) {
	t.Helper()
	dir := t.TempDir()
	capture := filepath.Join(dir, "capture")
	command := filepath.Join(dir, name)
	require.NoError(t, os.WriteFile(command, []byte(`#!/usr/bin/env bash
set -euo pipefail
printf 'action=%s\nendpoint=%s\n' "${ACTION:-}" "${ENDPOINT:-}" >"${WRAPPER_CAPTURE:?}"
exit "${WRAPPER_EXIT_CODE:-0}"
`), 0o755))
	return command, capture
}

func TestLegacySnapshotHistoryRemediationDispatchesExplicitNativeCommand(t *testing.T) {
	command, capture := writeLegacyRemediationCommand(t, "native-remediation")
	out, err := runProductionScriptCommand(t, "../backup/remediate-legacy-snapshot-history.sh", []string{
		"KUBEBRAIN_LEGACY_REMEDIATION_COMMAND=" + command,
		"WRAPPER_CAPTURE=" + capture,
		"WRAPPER_EXIT_CODE=3",
		"ACTION=diagnose",
		"ENDPOINT=https://kb:2379",
	})
	require.Error(t, err, string(out))
	require.Equal(t, "action=diagnose\nendpoint=https://kb:2379\n", string(requireFile(t, capture)))
}

func TestLegacySnapshotHistoryRemediationFindsInstalledNativeCommand(t *testing.T) {
	command, capture := writeLegacyRemediationCommand(t, "kubebrain-legacy-snapshot-remediation")
	out, err := runProductionScriptCommand(t, "../backup/remediate-legacy-snapshot-history.sh", []string{
		"PATH=" + filepath.Dir(command) + ":" + os.Getenv("PATH"),
		"WRAPPER_CAPTURE=" + capture,
		"ACTION=compact",
		"ENDPOINT=https://kb:2379",
	})
	require.NoError(t, err, string(out))
	require.Equal(t, "action=compact\nendpoint=https://kb:2379\n", string(requireFile(t, capture)))
}

func TestLegacySnapshotHistoryRemediationRejectsMissingOverride(t *testing.T) {
	out, err := runProductionScriptCommand(t, "../backup/remediate-legacy-snapshot-history.sh", []string{
		"KUBEBRAIN_LEGACY_REMEDIATION_COMMAND=" + filepath.Join(t.TempDir(), "missing"),
	})
	require.Error(t, err, string(out))
	require.Contains(t, string(out), "not executable")
}

func TestLegacySnapshotHistoryRemediationHelpDescribesSingleNativePath(t *testing.T) {
	out, err := runProductionCommand(t, "bash", []string{"../backup/remediate-legacy-snapshot-history.sh", "--help"}, nil)
	require.NoError(t, err, string(out))
	require.Contains(t, string(out), "contains no independent etcdctl remediation path")
	require.NotContains(t, string(out), "ETCDCTL_BIN")
}

func requireFile(t *testing.T, path string) []byte {
	t.Helper()
	data, err := os.ReadFile(path)
	require.NoError(t, err)
	return data
}
