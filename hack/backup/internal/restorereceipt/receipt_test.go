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
		ArtifactLeases: 2, VerifiedTargetLeases: 2, VerifiedTargetRevision: 84,
		VerifiedTargetClusterID: 7, VerifiedAtUnix: 200,
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

	require.ErrorContains(t, WriteAtomic(path, Receipt{}), "restore verification receipt is incomplete")
	data, err := os.ReadFile(path)
	require.NoError(t, err)
	require.Equal(t, "previous\n", string(data))
}

func TestWriteAtomicRequiresTargetRevisionBinding(t *testing.T) {
	receipt := Receipt{
		Format: Format, ArtifactFormat: "kubebrain.logical.v2", ArtifactSHA256: "abc",
		SnapshotRevision: 42, SourcePrefix: "/source", TargetPrefix: "/target",
		Records: 1, VerifiedAtUnix: 100,
	}
	require.ErrorContains(t, WriteAtomic(filepath.Join(t.TempDir(), "receipt.json"), receipt), "incomplete")
	receipt.Format = LegacyFormat
	receipt.VerifiedTargetRevision = 84
	receipt.VerifiedTargetClusterID = 7
	require.ErrorContains(t, WriteAtomic(filepath.Join(t.TempDir(), "legacy.json"), receipt), "incomplete")
}

func TestWriteAtomicRequiresTargetClusterBinding(t *testing.T) {
	receipt := Receipt{
		Format: Format, ArtifactFormat: "kubebrain.logical.v2", ArtifactSHA256: "abc",
		SnapshotRevision: 42, SourcePrefix: "/source", TargetPrefix: "/target",
		Records: 1, VerifiedTargetRevision: 84, VerifiedAtUnix: 100,
	}
	require.ErrorContains(t, WriteAtomic(filepath.Join(t.TempDir(), "receipt.json"), receipt), "incomplete")
}

func TestWriteAtomicRejectsVerificationBeforeArtifactCreation(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "restore-receipt.json")
	receipt := Receipt{
		Format: Format, ArtifactFormat: "kubebrain.logical.v2",
		ArtifactSHA256: "abc", SnapshotRevision: 42, ArtifactCreatedAtUnix: 201,
		SourcePrefix: "/source", TargetPrefix: "/target", Records: 1,
		VerifiedTargetRevision: 84, VerifiedTargetClusterID: 7, VerifiedAtUnix: 200,
	}

	require.ErrorContains(t, WriteAtomic(path, receipt), "verification predates artifact creation")
	_, err := os.Stat(path)
	require.ErrorIs(t, err, os.ErrNotExist)
}

func TestWriteAtomicRejectsNegativeArtifactCreationTime(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "restore-receipt.json")
	receipt := Receipt{
		Format: Format, ArtifactFormat: "kubebrain.logical.v2",
		ArtifactSHA256: "abc", SnapshotRevision: 42, ArtifactCreatedAtUnix: -1,
		SourcePrefix: "/source", TargetPrefix: "/target", Records: 1,
		VerifiedTargetRevision: 84, VerifiedTargetClusterID: 7, VerifiedAtUnix: 200,
	}

	require.ErrorContains(t, WriteAtomic(path, receipt), "restore verification receipt is incomplete")
	_, err := os.Stat(path)
	require.ErrorIs(t, err, os.ErrNotExist)
}

func TestWriteAtomicAllowsVerificationAtArtifactCreationSecond(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "restore-receipt.json")
	receipt := Receipt{
		Format: Format, ArtifactFormat: "kubebrain.logical.v2",
		ArtifactSHA256: "abc", SnapshotRevision: 42, ArtifactCreatedAtUnix: 200,
		SourcePrefix: "/source", TargetPrefix: "/target", Records: 1,
		VerifiedTargetRevision: 84, VerifiedTargetClusterID: 7, VerifiedAtUnix: 200,
	}

	require.NoError(t, WriteAtomic(path, receipt))
}

func TestWriteAtomicDoesNotReplacePublishedReceipt(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "restore-receipt.json")
	first := Receipt{
		Format: Format, ArtifactFormat: "kubebrain.logical.v2", ArtifactSHA256: "first",
		SnapshotRevision: 42, SourcePrefix: "/source", TargetPrefix: "/target",
		Records: 1, VerifiedTargetRevision: 84, VerifiedTargetClusterID: 7, VerifiedAtUnix: 100,
	}
	require.NoError(t, WriteAtomic(path, first))

	second := first
	second.ArtifactSHA256 = "second"
	require.ErrorIs(t, WriteAtomic(path, second), os.ErrExist)
	data, err := os.ReadFile(path)
	require.NoError(t, err)
	var actual Receipt
	require.NoError(t, json.Unmarshal(data, &actual))
	require.Equal(t, first, actual)
	temps, err := filepath.Glob(filepath.Join(dir, ".*.tmp-*"))
	require.NoError(t, err)
	require.Empty(t, temps, "failed no-overwrite publication must clean its temporary receipt")
}
