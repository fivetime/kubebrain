package objectstore

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestWriteReceiptAtomicIsIdempotentAndNonOverwriting(t *testing.T) {
	path := filepath.Join(t.TempDir(), "receipt.json")
	receipt := completeReceipt()
	require.NoError(t, WriteReceiptAtomic(path, receipt))
	require.NoError(t, WriteReceiptAtomic(path, receipt))

	info, err := os.Stat(path)
	require.NoError(t, err)
	require.Equal(t, os.FileMode(0o600), info.Mode().Perm())

	changed := receipt
	changed.BackupID = "other"
	require.ErrorContains(t, WriteReceiptAtomic(path, changed), "refusing to overwrite")
	actual, err := ReadReceipt(path)
	require.NoError(t, err)
	require.Equal(t, receipt, actual)
}

func TestReceiptReadersRejectAmbiguousJSON(t *testing.T) {
	dir := t.TempDir()
	tests := []struct {
		name  string
		value any
		read  func(string) error
	}{
		{
			name: "backup", value: completeReceipt(),
			read: func(path string) error { _, err := ReadReceipt(path); return err },
		},
		{
			name: "deletion", value: completeDeletionReceipt(),
			read: func(path string) error { _, err := ReadDeletionReceipt(path); return err },
		},
		{
			name: "audit", value: completeAuditReceipt(),
			read: func(path string) error { _, err := ReadAuditReceipt(path); return err },
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			canonical, err := json.Marshal(tc.value)
			require.NoError(t, err)
			canonical = append(canonical, '\n')

			path := filepath.Join(dir, tc.name+".json")
			require.NoError(t, os.WriteFile(path, canonical, 0o600))
			require.NoError(t, tc.read(path))

			require.NoError(t, os.WriteFile(path, append([]byte("  "), canonical...), 0o600))
			require.ErrorContains(t, tc.read(path), "not canonical")

			withUnknown := append(
				append([]byte{}, canonical[:len(canonical)-2]...),
				[]byte(`,"unknown":true}`+"\n")...,
			)
			require.NoError(t, os.WriteFile(path, withUnknown, 0o600))
			require.ErrorContains(t, tc.read(path), "unknown field")

			withTrailing := append(append([]byte{}, canonical...), []byte("{}\n")...)
			require.NoError(t, os.WriteFile(path, withTrailing, 0o600))
			require.ErrorContains(t, tc.read(path), "trailing JSON")
		})
	}
}

func TestObjectStoreJSONReadersRejectOversizedInput(t *testing.T) {
	tests := []struct {
		name string
		read func(string) error
		want string
	}{
		{
			name: "backup receipt",
			read: func(path string) error { _, err := ReadReceipt(path); return err },
			want: "object backup receipt exceeds",
		},
		{
			name: "deletion receipt",
			read: func(path string) error { _, err := ReadDeletionReceipt(path); return err },
			want: "object backup deletion receipt exceeds",
		},
		{
			name: "audit receipt",
			read: func(path string) error { _, err := ReadAuditReceipt(path); return err },
			want: "object operation audit receipt exceeds",
		},
		{
			name: "blob receipt",
			read: func(path string) error { _, err := ReadBlobReceipt(path); return err },
			want: "object immutable blob receipt exceeds",
		},
		{
			name: "usage receipt",
			read: func(path string) error { _, err := ReadUsageReceipt(path); return err },
			want: "object usage receipt exceeds",
		},
		{
			name: "inventory manifest",
			read: func(path string) error { _, err := InspectInventoryManifest(path); return err },
			want: "inventory manifest exceeds",
		},
		{
			name: "inventory receipt",
			read: func(path string) error { _, err := ReadInventoryReceipt(path); return err },
			want: "inventory receipt exceeds",
		},
		{
			name: "inventory receipt header",
			read: func(path string) error {
				_, err := BuildInventoryManifest(
					[]string{path}, "store-a", "bucket-a", "audits/",
					filepath.Join(t.TempDir(), "manifest.json"),
				)
				return err
			},
			want: "inventory receipt exceeds",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "input.json")
			require.NoError(t, os.WriteFile(path, make([]byte, maxObjectStoreJSONBytes+1), 0o600))
			require.ErrorContains(t, tc.read(path), tc.want)
		})
	}
}

func completeDeletionReceipt() DeletionReceipt {
	source := completeReceipt()
	return DeletionReceipt{
		Format: DeletionReceiptFormat, Instance: source.Instance, BackupID: source.BackupID,
		ObjectStoreID: source.ObjectStoreID, Bucket: source.Bucket, ObjectKey: source.ObjectKey,
		VersionID: source.VersionID, ArtifactSHA256: source.ArtifactSHA256,
		RetentionMode: source.RetentionMode, RetainUntilUnix: source.RetainUntilUnix,
		VersionAbsent: true, DeletedAtUnix: source.RetainUntilUnix,
	}
}

func TestDeletionReceiptRequiresDeterministicRetentionBoundary(t *testing.T) {
	receipt := completeDeletionReceipt()
	receipt.DeletedAtUnix++
	require.ErrorContains(t, receipt.Validate(), "incomplete")
}

func TestReceiptsRejectNonLowercaseArtifactDigest(t *testing.T) {
	backup := completeReceipt()
	backup.ArtifactSHA256 = strings.Repeat("A", 64)
	require.ErrorContains(t, backup.Validate(), "incomplete")

	deletion := completeDeletionReceipt()
	deletion.ArtifactSHA256 = strings.Repeat("B", 64)
	require.ErrorContains(t, deletion.Validate(), "incomplete")

	audit := completeAuditReceipt()
	audit.ExecutionReceiptSHA256 = strings.Repeat("C", 64)
	require.ErrorContains(t, audit.Validate(), "incomplete")

	blob := completeBlobReceipt()
	blob.ArtifactSHA256 = strings.Repeat("D", 64)
	require.ErrorContains(t, blob.Validate(), "incomplete")

	read := completeBlobReadReceipt()
	read.ArtifactSHA256 = strings.Repeat("E", 64)
	require.ErrorContains(t, read.Validate(), "incomplete")
}

func completeAuditReceipt() AuditReceipt {
	return AuditReceipt{
		Format: AuditReceiptFormat, OperationID: "operation-1", OperationUID: "uid-1",
		Instance: "instance-a", OperationType: "Backup", Phase: "Succeeded",
		ExecutionReceiptSHA256: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		ObjectStoreID:          "store-a", Bucket: "backups", ObjectKey: "audit/operation-1.json",
		VersionID: "version-audit", ArtifactSHA256: "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
		ObjectBytes: 1, RetentionMode: "COMPLIANCE", RetainUntilUnix: 20,
		RemoteVerified: true, ArchivedAtUnix: 10,
	}
}

func completeBlobReceipt() BlobReceipt {
	return BlobReceipt{
		Format: BlobReceiptFormat, ArtifactFormat: "sample.v1", ArtifactID: "sample-1",
		Instance: "instance-a", ObjectStoreID: "store-a", Bucket: "backups",
		ObjectKey: "blobs/sample-1.json", VersionID: "version-blob",
		ArtifactSHA256: strings.Repeat("a", 64), ObjectBytes: 1,
		RetentionMode: "COMPLIANCE", RetainUntilUnix: 20, RemoteVerified: true,
		ArchivedAtUnix: 10,
	}
}

func completeBlobReadReceipt() BlobReadReceipt {
	return BlobReadReceipt{
		Format: BlobReadReceiptFormat, ArtifactFormat: "sample.v1", ArtifactID: "sample-1",
		Instance: "instance-a", ObjectStoreID: "store-a", Bucket: "backups",
		ObjectKey: "blobs/sample-1.json", VersionID: "version-blob",
		ArtifactSHA256: strings.Repeat("a", 64), ObjectBytes: 1,
		RetentionMode: "COMPLIANCE", RetainUntilUnix: 20, RemoteVerified: true,
	}
}
