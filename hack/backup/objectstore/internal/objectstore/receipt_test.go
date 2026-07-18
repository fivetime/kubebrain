package objectstore

import (
	"encoding/json"
	"os"
	"path/filepath"
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
