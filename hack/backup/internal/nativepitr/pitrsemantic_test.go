package nativepitr

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"

	"github.com/kubewharf/kubebrain/hack/backup/internal/backupfile"
	"github.com/stretchr/testify/require"
)

func pitrSemanticFixture(t *testing.T) (Plan, FullSnapshotReceipt, FullRestoreExecutionReceipt, LogReplayExecutionReceipt, AdmissionHandoffReceipt, RestorationFenceHandoffReceipt, backupfile.Status, PITRSemanticVerificationInput) {
	t.Helper()
	plan := validReceiptPlan(t)
	task, _ := readyTask(t)
	full, err := BuildFullSnapshot(task, digest, "s3://bucket/immutable/full-1", fullMeta(t, task))
	require.NoError(t, err)
	restore := validFullRestoreExecution()
	restore.FullArtifactSHA256, restore.ArtifactManifestSHA256 = plan.Full.ArtifactReceiptSHA256, plan.Full.ArtifactManifestSHA
	restore.StartedAtUnix, restore.CompletedAtUnix = 100, 101
	replay := validHandoffReplay(plan, 110)
	fence, _, err := BuildRestorationFenceReceipt(plan, digest, "restore-1", 102, false)
	require.NoError(t, err)
	admission, _, err := BuildRestoreAdmissionReceipt(plan, digest, "restore-1", 99, false)
	require.NoError(t, err)
	admissionHandoff, err := BuildAdmissionHandoff(plan, digest, admission, digest, restore, digest, fence, digest, 103)
	require.NoError(t, err)
	handoff, err := BuildRestorationFenceHandoff(plan, digest, fence, digest, replay, digest, 111)
	require.NoError(t, err)
	witness := backupfile.Status{Format: backupfile.Format, Prefix: "/", Revision: 119, CreatedAtUnix: 105, Records: 3, Leases: 1, SHA256: digest}
	base := FullSemanticVerificationInput{PlanSHA256: digest, FullSnapshotSHA256: digest, FullRestoreSHA256: digest, WitnessFileSHA256: digest, HistoricalHeaderRevision: 120, CurrentHeaderRevision: 120, ProbePutRevision: 121, ProbeDeleteRevision: 122, HistoricalExact: true, CurrentExact: true, LeaseIdentityExact: true, WatchProbeSucceeded: true, TargetProbeHistoryExact: true, VerifiedAtUnix: 112}
	return plan, full, restore, replay, admissionHandoff, handoff, witness, PITRSemanticVerificationInput{FullSemanticVerificationInput: base, LogReplaySHA256: digest, FenceHandoffSHA256: digest, AdmissionHandoffSHA256: digest}
}

func TestBuildPITRSemanticVerificationCompletesContinuousPITRChain(t *testing.T) {
	plan, full, restore, replay, admissionHandoff, handoff, witness, in := pitrSemanticFixture(t)
	receipt, err := BuildPITRSemanticVerification(plan, full, restore, replay, admissionHandoff, handoff, witness, in)
	require.NoError(t, err)
	require.True(t, receipt.ReplayWriteFenceProven)
	require.True(t, receipt.FenceHandoffProven)
	require.True(t, receipt.PostRestoreSemanticValidated)
	require.True(t, receipt.TargetFullImportFenceProven)
	require.True(t, receipt.ContinuousWriterExclusion)
	require.True(t, receipt.PITRComplete)
	var encoded bytes.Buffer
	require.NoError(t, json.NewEncoder(&encoded).Encode(receipt))
	_, err = DecodePITRSemanticVerification(&encoded)
	require.NoError(t, err)
}

func TestBuildPITRSemanticVerificationRejectsBrokenHandoff(t *testing.T) {
	plan, full, restore, replay, admissionHandoff, handoff, witness, in := pitrSemanticFixture(t)
	handoff.LogReplayReceiptSHA256 = strings.Repeat("f", 64)
	_, err := BuildPITRSemanticVerification(plan, full, restore, replay, admissionHandoff, handoff, witness, in)
	require.ErrorContains(t, err, "does not match")
}

func TestBuildPITRSemanticVerificationRejectsReplayFromAnotherRestore(t *testing.T) {
	plan, full, restore, replay, admissionHandoff, handoff, witness, in := pitrSemanticFixture(t)
	replay.FullRestoreReceiptSHA256 = strings.Repeat("f", 64)
	_, err := BuildPITRSemanticVerification(plan, full, restore, replay, admissionHandoff, handoff, witness, in)
	require.ErrorContains(t, err, "does not match")
}

func TestBuildPITRSemanticVerificationRejectsCrossOperationHandoff(t *testing.T) {
	plan, full, restore, replay, admissionHandoff, handoff, witness, in := pitrSemanticFixture(t)
	handoff.OperationID = "restore-2"
	_, err := BuildPITRSemanticVerification(plan, full, restore, replay, admissionHandoff, handoff, witness, in)
	require.ErrorContains(t, err, "does not match")
}
