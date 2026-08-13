package nativepitr

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func validFullRestoreExecution() FullRestoreExecutionReceipt {
	return FullRestoreExecutionReceipt{Format: FullRestoreExecutionFormat, PlanSHA256: digest, SourceExclusiveSHA256: digest, FullArtifactSHA256: digest, ArtifactManifestSHA256: digest, RestoreAdmissionSHA256: digest, PreWriteTarget: validTarget(), BRVersion: "Release Version: v7.5.1\nGit Commit Hash: 7d16cc79e81bbf573124df3fd9351c26963f3e70\nGit Branch: refs/tags/v7.5.1", BRBinarySHA256: digest, Encryption: CipherMethodPlaintext, StartedAtUnix: 10, CompletedAtUnix: 11, WholeClusterTxnImport: true, SourceVisibleRangeExclusive: true, TargetWriteFenceProven: true, FullImportAdmissionProven: true, FullSnapshotRestored: true}
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

func TestFullRestoreOperationBindingRejectsDrift(t *testing.T) {
	plan := validReceiptPlan(t)
	target := validTarget()
	receipt := validFullRestoreExecution()
	receipt.PlanSHA256 = digest
	receipt.SourceExclusiveSHA256 = digest
	receipt.FullArtifactSHA256 = plan.Full.ArtifactReceiptSHA256
	receipt.ArtifactManifestSHA256 = plan.Full.ArtifactManifestSHA
	receipt.RestoreAdmissionSHA256 = digest
	receipt.PreWriteTarget = target
	receipt.Encryption = plan.Full.Encryption
	receipt.EncryptionKeyID = plan.Full.EncryptionKeyID
	artifacts := ArtifactReceipt{ManifestSHA256: plan.Full.ArtifactManifestSHA}
	source := validSourceExclusive(t)
	admission, _, err := BuildRestoreAdmissionReceipt(plan, digest, "restore-1", 9, false)
	require.NoError(t, err)
	binding := FullRestoreOperationBinding{PlanSHA256: digest, SourceExclusiveSHA256: digest, FullArtifactSHA256: plan.Full.ArtifactReceiptSHA256, TargetSnapshotSHA256: plan.Target.SnapshotEmptyReceiptSHA256, RestoreAdmissionSHA256: digest, Encryption: plan.Full.Encryption, EncryptionKeyID: plan.Full.EncryptionKeyID}
	require.NoError(t, VerifyFullRestoreOperationBinding(receipt, plan, artifacts, source, target, admission, binding))

	for _, edit := range []func(*FullRestoreExecutionReceipt){
		func(r *FullRestoreExecutionReceipt) { r.PlanSHA256 = strings.Repeat("f", 64) },
		func(r *FullRestoreExecutionReceipt) { r.RestoreAdmissionSHA256 = strings.Repeat("f", 64) },
		func(r *FullRestoreExecutionReceipt) { r.PreWriteTarget.ClusterID++ },
		func(r *FullRestoreExecutionReceipt) { r.PreWriteTarget.Stores[0].Address = "other:20160" },
		func(r *FullRestoreExecutionReceipt) {
			r.Encryption = CipherMethodAES256CTR
			r.EncryptionKeyID = "other/key"
		},
	} {
		drifted := receipt
		drifted.PreWriteTarget.PDAddrs = append([]string(nil), receipt.PreWriteTarget.PDAddrs...)
		drifted.PreWriteTarget.Stores = append([]TargetStore(nil), receipt.PreWriteTarget.Stores...)
		edit(&drifted)
		require.ErrorContains(t, VerifyFullRestoreOperationBinding(drifted, plan, artifacts, source, target, admission, binding), "does not match")
	}

	driftedSource := source
	driftedSource.ClusterID++
	require.ErrorContains(t, VerifyFullRestoreOperationBinding(receipt, plan, artifacts, driftedSource, target, admission, binding), "source-exclusive cluster")
	driftedAdmission := admission
	driftedAdmission.TargetClusterID++
	require.ErrorContains(t, VerifyFullRestoreOperationBinding(receipt, plan, artifacts, source, target, driftedAdmission, binding), "admission target cluster")
	driftedBinding := binding
	driftedBinding.TargetSnapshotSHA256 = strings.Repeat("f", 64)
	require.ErrorContains(t, VerifyFullRestoreOperationBinding(receipt, plan, artifacts, source, target, admission, driftedBinding), "plan target-empty receipt")
}
