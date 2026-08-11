package nativepitr

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/kubewharf/kubebrain/hack/backup/internal/backupfile"
	"github.com/stretchr/testify/require"
)

func semanticFixture(t *testing.T) (Plan, FullSnapshotReceipt, FullRestoreExecutionReceipt, backupfile.Status, FullSemanticVerificationInput) {
	t.Helper()
	plan := validReceiptPlan(t)
	plan.RestoreTS = plan.Full.BackupTS
	plan.SourceCapture.CaptureTS = plan.Full.BackupTS
	task, _ := readyTask(t)
	full, err := BuildFullSnapshot(task, digest, "s3://bucket/immutable/full-1", fullMeta(t, task))
	require.NoError(t, err)
	restore := validFullRestoreExecution()
	restore.FullArtifactSHA256 = plan.Full.ArtifactReceiptSHA256
	restore.ArtifactManifestSHA256 = plan.Full.ArtifactManifestSHA
	restore.StartedAtUnix, restore.CompletedAtUnix = 100, 110
	witness := backupfile.Status{Format: backupfile.Format, Prefix: "/", Revision: 119, CreatedAtUnix: 90, Records: 2, Leases: 1, SHA256: digest}
	plan.SourceWitness = SourceWitness{Format: witness.Format, FileSHA256: digest, ContentSHA256: witness.SHA256, Prefix: witness.Prefix, Revision: witness.Revision, CreatedAtUnix: witness.CreatedAtUnix, Records: witness.Records, Leases: witness.Leases}
	in := FullSemanticVerificationInput{PlanSHA256: digest, FullSnapshotSHA256: digest, FullRestoreSHA256: digest, WitnessFileSHA256: digest, HistoricalHeaderRevision: 120, CurrentHeaderRevision: 120, ProbePutRevision: 121, ProbeDeleteRevision: 122, HistoricalExact: true, CurrentExact: true, LeaseIdentityExact: true, WatchProbeSucceeded: true, TargetProbeHistoryExact: true, VerifiedAtUnix: 111}
	return plan, full, restore, witness, in
}

func TestBuildFullSemanticVerificationRemainsFullOnly(t *testing.T) {
	plan, full, restore, witness, in := semanticFixture(t)
	receipt, err := BuildFullSemanticVerification(plan, full, restore, witness, in)
	require.NoError(t, err)
	require.True(t, receipt.FullRestoreSemanticValidated)
	require.False(t, receipt.LogReplayValidated)
	require.False(t, receipt.PITRComplete)
	require.NoError(t, receipt.Validate())
}

func TestBuildFullSemanticVerificationRejectsBrokenChain(t *testing.T) {
	for _, test := range []struct {
		name string
		edit func(*Plan, *FullSnapshotReceipt, *FullRestoreExecutionReceipt, *backupfile.Status, *FullSemanticVerificationInput)
	}{
		{"wrong plan digest", func(_ *Plan, _ *FullSnapshotReceipt, r *FullRestoreExecutionReceipt, _ *backupfile.Status, _ *FullSemanticVerificationInput) {
			r.PlanSHA256 = strings.Repeat("f", 64)
		}},
		{"partial witness", func(_ *Plan, _ *FullSnapshotReceipt, _ *FullRestoreExecutionReceipt, w *backupfile.Status, _ *FullSemanticVerificationInput) {
			w.Prefix = "/registry"
		}},
		{"substituted witness", func(_ *Plan, _ *FullSnapshotReceipt, _ *FullRestoreExecutionReceipt, w *backupfile.Status, _ *FullSemanticVerificationInput) {
			w.SHA256 = strings.Repeat("f", 64)
		}},
		{"witness after backup", func(_ *Plan, _ *FullSnapshotReceipt, _ *FullRestoreExecutionReceipt, w *backupfile.Status, _ *FullSemanticVerificationInput) {
			w.Revision = 121
		}},
		{"missing watch proof", func(_ *Plan, _ *FullSnapshotReceipt, _ *FullRestoreExecutionReceipt, _ *backupfile.Status, in *FullSemanticVerificationInput) {
			in.WatchProbeSucceeded = false
		}},
		{"missing physical target binding", func(_ *Plan, _ *FullSnapshotReceipt, _ *FullRestoreExecutionReceipt, _ *backupfile.Status, in *FullSemanticVerificationInput) {
			in.TargetProbeHistoryExact = false
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			plan, full, restore, witness, in := semanticFixture(t)
			test.edit(&plan, &full, &restore, &witness, &in)
			_, err := BuildFullSemanticVerification(plan, full, restore, witness, in)
			require.Error(t, err)
		})
	}
}

func TestDecodeFullSemanticVerificationIsStrict(t *testing.T) {
	plan, full, restore, witness, in := semanticFixture(t)
	receipt, err := BuildFullSemanticVerification(plan, full, restore, witness, in)
	require.NoError(t, err)
	b, err := json.Marshal(receipt)
	require.NoError(t, err)
	_, err = DecodeFullSemanticVerification(strings.NewReader(string(b)))
	require.NoError(t, err)
	_, err = DecodeFullSemanticVerification(strings.NewReader(string(b) + `{}`))
	require.ErrorContains(t, err, "trailing")
}
