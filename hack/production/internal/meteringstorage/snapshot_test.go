package meteringstorage

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSnapshotCanonicalRoundTrip(t *testing.T) {
	snapshot := validSnapshot()
	path := filepath.Join(t.TempDir(), "sample.json")
	first, err := WriteSnapshotAtomic(path, snapshot)
	require.NoError(t, err)
	second, err := WriteSnapshotAtomic(path, snapshot)
	require.NoError(t, err)
	require.Equal(t, first, second)

	data, err := os.ReadFile(path)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(path, append([]byte(" "), data...), 0o600))
	_, err = ReadSnapshot(path)
	require.ErrorContains(t, err, "canonical")
}

func TestSnapshotRejectsOversizedInputs(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "sample.json")
	require.NoError(t, os.WriteFile(path, make([]byte, maxMeteringStorageJSONBytes+1), 0o600))
	_, err := ReadSnapshot(path)
	require.ErrorContains(t, err, "object storage sample exceeds")

	output := filepath.Join(dir, "existing.json")
	require.NoError(t, os.WriteFile(output, make([]byte, maxMeteringStorageJSONBytes+1), 0o600))
	_, err = WriteSnapshotAtomic(output, validSnapshot())
	require.ErrorContains(t, err, "existing object storage sample exceeds")
}

func TestSnapshotRejectsInvalidEvidenceAndSlot(t *testing.T) {
	snapshot := validSnapshot()
	snapshot.DeleteMarkers = 1
	require.Error(t, snapshot.Validate())
	snapshot = validSnapshot()
	snapshot.CheckedAtUnix = snapshot.SlotEndUnix + 2701
	require.Error(t, snapshot.Validate())
}

func TestSnapshotRejectsImpossibleVersionByteTotals(t *testing.T) {
	snapshot := validSnapshot()
	snapshot.RemoteVersions = 0
	snapshot.TotalObjectBytes = 1
	require.ErrorContains(t, snapshot.Validate(), "inconsistent")

	snapshot = validSnapshot()
	snapshot.RemoteVersions = 1
	snapshot.TotalObjectBytes = 0
	require.ErrorContains(t, snapshot.Validate(), "inconsistent")

	snapshot = validSnapshot()
	snapshot.RemoteVersions = 0
	snapshot.TotalObjectBytes = 0
	require.NoError(t, snapshot.Validate())
}

func validSnapshot() Snapshot {
	return Snapshot{
		Format: SnapshotFormat, Instance: "instance-a",
		SlotStartUnix: 1_784_505_600, SlotEndUnix: 1_784_509_200,
		ObjectStoreID: "backup-store", Bucket: "backups",
		Prefix: "instance-a/", AllowedFormats: []string{"kubebrain.logical.v2"},
		RemoteVersions: 2, DeleteMarkers: 0, TotalObjectBytes: 303,
		VersionsSHA256: strings.Repeat("a", 64), CheckedAtUnix: 1_784_509_201,
	}
}
