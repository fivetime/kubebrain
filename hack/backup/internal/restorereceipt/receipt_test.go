package restorereceipt

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestWriteAtomicPublishesCompleteReceipt(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "restore-receipt.json")
	receipt := Receipt{
		Format: Format, ArtifactFormat: "kubebrain.logical.v2",
		ArtifactSHA256: "abc", SnapshotRevision: 42, ArtifactCreatedAtUnix: 100,
		SourcePrefix: "/registry", TargetPrefix: "/restored", Records: 7,
		ArtifactLeases: 2, VerifiedTargetLeases: 2, VerifiedAtUnix: 200,
	}

	require.NoError(t, WriteAtomic(path, receipt))
	data, err := os.ReadFile(path)
	require.NoError(t, err)
	var actual Receipt
	require.NoError(t, json.Unmarshal(data, &actual))
	require.Equal(t, receipt, actual)
	info, err := os.Stat(path)
	require.NoError(t, err)
	require.Equal(t, os.FileMode(0o600), info.Mode().Perm())
	matches, err := filepath.Glob(filepath.Join(dir, ".*.tmp-*"))
	require.NoError(t, err)
	require.Empty(t, matches)
}

func TestWriteAtomicRejectsIncompleteReceiptWithoutReplacingEvidence(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "restore-receipt.json")
	require.NoError(t, os.WriteFile(path, []byte("previous\n"), 0o600))

	require.Error(t, WriteAtomic(path, Receipt{}))
	data, err := os.ReadFile(path)
	require.NoError(t, err)
	require.Equal(t, "previous\n", string(data))
}

func TestWriteAtomicDoesNotReplacePublishedReceipt(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "restore-receipt.json")
	first := Receipt{
		Format: Format, ArtifactFormat: "kubebrain.logical.v2", ArtifactSHA256: "first",
		SnapshotRevision: 42, SourcePrefix: "/source", TargetPrefix: "/target",
		Records: 1, VerifiedAtUnix: 100,
	}
	require.NoError(t, WriteAtomic(path, first))

	second := first
	second.ArtifactSHA256 = "second"
	require.Error(t, WriteAtomic(path, second))
	data, err := os.ReadFile(path)
	require.NoError(t, err)
	var actual Receipt
	require.NoError(t, json.Unmarshal(data, &actual))
	require.Equal(t, first, actual)
}
