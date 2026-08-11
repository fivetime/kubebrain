package nativepitr

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func validFullRestoreExecution() FullRestoreExecutionReceipt {
	return FullRestoreExecutionReceipt{Format: FullRestoreExecutionFormat, PlanSHA256: digest, SourceExclusiveSHA256: digest, FullArtifactSHA256: digest, ArtifactManifestSHA256: digest, RestoreAdmissionSHA256: digest, PreWriteTarget: validTarget(), BRVersion: "Release Version: v7.5.1\nGit Commit Hash: 7d16cc79e81bbf573124df3fd9351c26963f3e70\nGit Branch: refs/tags/v7.5.1", BRBinarySHA256: digest, StartedAtUnix: 10, CompletedAtUnix: 11, WholeClusterTxnImport: true, SourceVisibleRangeExclusive: true, TargetWriteFenceProven: true, FullImportAdmissionProven: true, FullSnapshotRestored: true}
}

func TestFullRestoreReceiptCannotClaimPITR(t *testing.T) {
	r := validFullRestoreExecution()
	require.NoError(t, r.Validate())
	for _, edit := range []func(*FullRestoreExecutionReceipt){func(r *FullRestoreExecutionReceipt) { r.PITRComplete = true }, func(r *FullRestoreExecutionReceipt) { r.LogReplayCompleted = true }, func(r *FullRestoreExecutionReceipt) { r.PostRestoreSemanticValidated = true }, func(r *FullRestoreExecutionReceipt) { r.TargetWriteFenceProven = false }, func(r *FullRestoreExecutionReceipt) { r.FullImportAdmissionProven = false }} {
		r = validFullRestoreExecution()
		edit(&r)
		require.ErrorContains(t, r.Validate(), "full-only")
	}
}
func TestDecodeFullRestoreReceiptIsStrict(t *testing.T) {
	b, err := json.Marshal(validFullRestoreExecution())
	require.NoError(t, err)
	_, err = DecodeFullRestoreExecution(strings.NewReader(string(b)))
	require.NoError(t, err)
	_, err = DecodeFullRestoreExecution(strings.NewReader(string(b) + `{}`))
	require.ErrorContains(t, err, "trailing")
}
