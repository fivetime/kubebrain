package operationaudit

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestOperationAuditArtifactIsCanonicalAndNonOverwriting(t *testing.T) {
	artifact := terminalArtifact()
	path := filepath.Join(t.TempDir(), "audit.json")
	require.NoError(t, WriteAtomic(path, artifact))
	require.NoError(t, WriteAtomic(path, artifact))

	status, data, err := InspectBytes(path)
	require.NoError(t, err)
	require.Equal(t, artifact, status.Artifact)
	require.Len(t, status.SHA256, 64)
	require.Len(t, data, int(status.Bytes))
	info, err := os.Stat(path)
	require.NoError(t, err)
	require.Equal(t, os.FileMode(0o600), info.Mode().Perm())

	changed := artifact
	changed.Message = "changed"
	require.ErrorContains(t, WriteAtomic(path, changed), "refusing to overwrite")
}

func TestOperationAuditReadersRejectOversizedJSON(t *testing.T) {
	dir := t.TempDir()
	artifactPath := filepath.Join(dir, "audit.json")
	require.NoError(t, os.WriteFile(artifactPath, make([]byte, maxOperationAuditJSONBytes+1), 0o600))
	_, err := Inspect(artifactPath)
	require.ErrorContains(t, err, "operation audit artifact exceeds")

	receiptPath := filepath.Join(dir, "receipt.json")
	require.NoError(t, os.WriteFile(receiptPath, make([]byte, maxOperationAuditJSONBytes+1), 0o600))
	_, _, err = InspectArchiveReceipt(receiptPath)
	require.ErrorContains(t, err, "operation audit receipt exceeds")
}

func TestOperationAuditRejectsNonterminalAndInvalidReceipt(t *testing.T) {
	artifact := terminalArtifact()
	artifact.Phase = "Running"
	err := artifact.Validate()
	require.ErrorContains(t, err, "incomplete")

	artifact = terminalArtifact()
	artifact.ReceiptSHA256 = ""
	err = artifact.Validate()
	require.ErrorContains(t, err, "requires a receipt")

	artifact = terminalArtifact()
	artifact.Generation = 2
	require.NoError(t, artifact.Validate())
	artifact.ObservedGeneration = 3
	require.ErrorContains(t, artifact.Validate(), "incomplete")

	artifact = terminalArtifact()
	artifact.ParametersSHA256 = strings.Repeat("A", 64)
	require.ErrorContains(t, artifact.Validate(), "incomplete")

	artifact = terminalArtifact()
	artifact.ReceiptSHA256 = strings.Repeat("B", 64)
	require.ErrorContains(t, artifact.Validate(), "requires a receipt")

	artifact = terminalArtifact()
	artifact.Owner = strings.Repeat("w", maxOperationOwnerLength+1)
	require.ErrorContains(t, artifact.Validate(), "incomplete")

	artifact = terminalArtifact()
	artifact.Message = strings.Repeat("m", maxOperationMessageLength+1)
	require.ErrorContains(t, artifact.Validate(), "incomplete")
}

func TestOperationAuditRejectsUnsafeIdentityFields(t *testing.T) {
	for _, tc := range []struct {
		name   string
		mutate func(*Artifact)
	}{
		{
			name: "namespace_parent_segment",
			mutate: func(artifact *Artifact) {
				artifact.Namespace = "../operations"
			},
		},
		{
			name: "namespace_unicode_space",
			mutate: func(artifact *Artifact) {
				artifact.Namespace = "operations\u00a0a"
			},
		},
		{
			name: "name_slash",
			mutate: func(artifact *Artifact) {
				artifact.Name = "backup/1"
			},
		},
		{
			name: "name_dot",
			mutate: func(artifact *Artifact) {
				artifact.Name = "."
			},
		},
		{
			name: "uid_control_byte",
			mutate: func(artifact *Artifact) {
				artifact.UID = "uid-1\x00"
			},
		},
		{
			name: "uid_invalid_utf8",
			mutate: func(artifact *Artifact) {
				artifact.UID = string([]byte{'u', 0xff})
			},
		},
		{
			name: "operation_id_slash",
			mutate: func(artifact *Artifact) {
				artifact.OperationID = "backup/1"
			},
		},
		{
			name: "instance_control_byte",
			mutate: func(artifact *Artifact) {
				artifact.Instance = "instance-a\x00"
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			artifact := terminalArtifact()
			tc.mutate(&artifact)
			require.ErrorContains(t, artifact.Validate(), "incomplete")
		})
	}
}

func TestArchiveReceiptRejectsUnsafeOperationIdentity(t *testing.T) {
	artifact := terminalArtifact()
	for _, tc := range []struct {
		name   string
		mutate func(*ArchiveReceipt)
	}{
		{
			name: "operation_id_slash",
			mutate: func(receipt *ArchiveReceipt) {
				receipt.OperationID = "backup/1"
			},
		},
		{
			name: "operation_uid_control_byte",
			mutate: func(receipt *ArchiveReceipt) {
				receipt.OperationUID = "uid-1\x00"
			},
		},
		{
			name: "instance_unicode_space",
			mutate: func(receipt *ArchiveReceipt) {
				receipt.Instance = "instance\u00a0a"
			},
		},
		{
			name: "operation_type_unknown",
			mutate: func(receipt *ArchiveReceipt) {
				receipt.OperationType = "Backup/Deletion"
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			receipt := validArchiveReceipt(artifact)
			tc.mutate(&receipt)
			require.ErrorContains(t, receipt.Validate(), "incomplete")
		})
	}
}

func TestArchiveReceiptRejectsUppercaseDigest(t *testing.T) {
	artifact := terminalArtifact()
	receipt := validArchiveReceipt(artifact)
	receipt.ArtifactSHA256 = strings.Repeat("C", 64)
	require.ErrorContains(t, receipt.Validate(), "incomplete")
}

func TestArchiveReceiptRejectsUnsafeObjectKey(t *testing.T) {
	artifact := terminalArtifact()
	for _, objectKey := range []string{
		"/audit/backup-1.json",
		"../audit/backup-1.json",
		"audit/../backup-1.json",
		"audit//backup-1.json",
		"audit/backup 1.json",
	} {
		t.Run(objectKey, func(t *testing.T) {
			receipt := validArchiveReceipt(artifact)
			receipt.ObjectKey = objectKey
			require.ErrorContains(t, receipt.Validate(), "incomplete")
		})
	}
}

func TestArchiveReceiptRejectsUnsafeScopeFields(t *testing.T) {
	artifact := terminalArtifact()
	for _, tc := range []struct {
		name   string
		mutate func(*ArchiveReceipt)
	}{
		{
			name: "object_store_id_with_leading_space",
			mutate: func(receipt *ArchiveReceipt) {
				receipt.ObjectStoreID = " store-a"
			},
		},
		{
			name: "object_store_id_with_control_byte",
			mutate: func(receipt *ArchiveReceipt) {
				receipt.ObjectStoreID = "store-a\x00"
			},
		},
		{
			name: "bucket_with_internal_tab",
			mutate: func(receipt *ArchiveReceipt) {
				receipt.Bucket = "bucket\ta"
			},
		},
		{
			name: "version_id_with_trailing_newline",
			mutate: func(receipt *ArchiveReceipt) {
				receipt.VersionID = "version-1\n"
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			receipt := validArchiveReceipt(artifact)
			tc.mutate(&receipt)
			require.ErrorContains(t, receipt.Validate(), "incomplete")
		})
	}
}

func TestOperationAuditAcceptsBackupDeletion(t *testing.T) {
	artifact := terminalArtifact()
	artifact.Type = "BackupDeletion"
	artifact.ApprovedBy = ApproverUsername
	artifact.ApprovalID = "change-123"
	require.NoError(t, artifact.Validate())
}

func TestOperationAuditRequiresApprovalForHighRiskTypes(t *testing.T) {
	for _, operationType := range []string{
		"RestoreCutover", "CertificateRotation", "Destroy", "BackupDeletion",
	} {
		artifact := terminalArtifact()
		artifact.Type = operationType
		require.ErrorContains(t, artifact.Validate(), "approval evidence")
		artifact.ApprovedBy = ApproverUsername
		artifact.ApprovalID = "change-123"
		require.NoError(t, artifact.Validate())
		artifact.ApprovedBy = "forged"
		require.ErrorContains(t, artifact.Validate(), "approval evidence")
	}
}

func TestOperationAuditRejectsApprovalOnLowRiskType(t *testing.T) {
	artifact := terminalArtifact()
	artifact.ApprovedBy = ApproverUsername
	artifact.ApprovalID = "change-123"
	require.ErrorContains(t, artifact.Validate(), "cannot carry approval")
}

func validArchiveReceipt(artifact Artifact) ArchiveReceipt {
	return ArchiveReceipt{
		Format: ArchiveReceiptFormat, OperationID: artifact.OperationID, OperationUID: artifact.UID,
		Instance: artifact.Instance, OperationType: artifact.Type, Phase: artifact.Phase,
		ExecutionReceiptSHA256: artifact.ReceiptSHA256, ObjectStoreID: "store-a",
		Bucket: "bucket-a", ObjectKey: "audit/backup-1.json", VersionID: "version-1",
		ArtifactSHA256: strings.Repeat("c", 64), ObjectBytes: 1,
		RetentionMode: "COMPLIANCE", RetainUntilUnix: 200, RemoteVerified: true,
		ArchivedAtUnix: 100,
	}
}

func terminalArtifact() Artifact {
	return Artifact{
		Format: Format, APIVersion: "dbaas.kubebrain.io/v1alpha1",
		Namespace: "operations", Name: "backup-1", UID: "uid-1", Generation: 1,
		OperationID: "backup-1", Tenant: "tenant-a", RequestedBy: "user-123",
		Instance: "instance-a", Type: "Backup",
		ParametersSHA256: strings.Repeat("a", 64), MaxAttempts: 3,
		Phase: "Succeeded", Owner: "worker-a", Attempt: 1, ObservedGeneration: 1,
		StartedAtUnix: 100, StartedAtUnixNano: 100_000_000_001, CompletedAtUnix: 101,
		ReceiptSHA256: strings.Repeat("b", 64), Message: "done",
	}
}
