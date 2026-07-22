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
