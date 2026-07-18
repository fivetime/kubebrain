package backupmetrics

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/kubewharf/kubebrain/hack/backup/internal/backupfile"
)

func TestWriteSuccessPublishesPrometheusTextAtomically(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "logical-backup.prom")
	status := backupfile.Status{Revision: 42, Records: 7, Leases: 2}

	require.NoError(t, WriteSuccess(path, "tenant\"one\n", status, 1234, time.Unix(1700000000, 999)))

	data, err := os.ReadFile(path)
	require.NoError(t, err)
	require.Contains(t, string(data),
		"kubebrain_logical_backup_last_success_timestamp_seconds{instance=\"tenant\\\"one\\n\"} 1700000000\n")
	require.Contains(t, string(data),
		"kubebrain_logical_backup_artifact_bytes{instance=\"tenant\\\"one\\n\"} 1234\n")
	require.Contains(t, string(data),
		"kubebrain_logical_backup_records{instance=\"tenant\\\"one\\n\"} 7\n")
	require.Contains(t, string(data),
		"kubebrain_logical_backup_leases{instance=\"tenant\\\"one\\n\"} 2\n")
	require.Contains(t, string(data),
		"kubebrain_logical_backup_snapshot_revision{instance=\"tenant\\\"one\\n\"} 42\n")
	info, err := os.Stat(path)
	require.NoError(t, err)
	require.Equal(t, os.FileMode(0o644), info.Mode().Perm())

	matches, err := filepath.Glob(filepath.Join(dir, ".*.tmp-*"))
	require.NoError(t, err)
	require.Empty(t, matches)
}

func TestWriteSuccessRejectsInvalidInputsWithoutReplacingLastSuccess(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "logical-backup.prom")
	require.NoError(t, os.WriteFile(path, []byte("previous\n"), 0o644))

	err := WriteSuccess(path, "", backupfile.Status{}, 1, time.Now())
	require.ErrorContains(t, err, "instance is empty")
	data, readErr := os.ReadFile(path)
	require.NoError(t, readErr)
	require.Equal(t, "previous\n", string(data))
}
