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

	status, err := Inspect(path)
	require.NoError(t, err)
	require.Equal(t, artifact, status.Artifact)
	require.Len(t, status.SHA256, 64)
	info, err := os.Stat(path)
	require.NoError(t, err)
	require.Equal(t, os.FileMode(0o600), info.Mode().Perm())

	changed := artifact
	changed.Message = "changed"
	require.ErrorContains(t, WriteAtomic(path, changed), "refusing to overwrite")
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

func terminalArtifact() Artifact {
	return Artifact{
		Format: Format, APIVersion: "dbaas.kubebrain.io/v1alpha1",
		Namespace: "operations", Name: "backup-1", UID: "uid-1", Generation: 1,
		OperationID: "backup-1", Instance: "instance-a", Type: "Backup",
		ParametersSHA256: strings.Repeat("a", 64), MaxAttempts: 3,
		Phase: "Succeeded", Owner: "worker-a", Attempt: 1, ObservedGeneration: 1,
		StartedAtUnix: 100, StartedAtUnixNano: 100_000_000_001, CompletedAtUnix: 101,
		ReceiptSHA256: strings.Repeat("b", 64), Message: "done",
	}
}
