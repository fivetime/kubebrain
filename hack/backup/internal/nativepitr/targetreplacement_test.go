package nativepitr

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func validTargetReplacement(t *testing.T) (Plan, Plan, TargetSnapshotEmptyReceipt, TargetSnapshotEmptyReceipt, TargetProvisioningReceipt, TargetProvisioningReceipt, TargetQualificationReceipt, TargetWriterExclusionEvidence, TargetRetirementReceipt, RestoreAdmissionReceipt, RestoreAdmissionReceipt, TargetReplacementInputs) {
	t.Helper()
	oldPlan := validReceiptPlan(t)
	oldTarget := validTarget()
	newTarget := oldTarget
	newTarget.ClusterID++
	newTarget.SnapshotTS++
	newTarget.CheckedAtUnix++
	newTarget.PDAddrs = []string{"new-pd:2379"}
	newTarget.Stores = []TargetStore{{ID: 9, Address: "new-tikv:20160"}}
	newPlan := oldPlan
	newPlan.Target.ClusterID = newTarget.ClusterID
	newPlan.Target.SnapshotEmptyReceiptSHA256 = strings.Repeat("e", 64)
	newPlan.Target.SnapshotTS = newTarget.SnapshotTS
	newPlan.Target.CheckedAtUnix = newTarget.CheckedAtUnix
	newAdmission, _, err := BuildRestoreAdmissionReceipt(newPlan, strings.Repeat("b", 64), "replacement-1", 12, false)
	require.NoError(t, err)
	oldAdmission, _, err := BuildRestoreAdmissionReceipt(oldPlan, digest, "old-restore", 8, false)
	require.NoError(t, err)
	oldProvisioning, newProvisioning := provisioningReceipt(oldTarget.ClusterID, "old"), provisioningReceipt(newTarget.ClusterID, "new")
	writers := TargetWriterExclusionEvidence{Namespace: "kubebrain-system", StatefulSet: "kubebrain", StatefulSetUID: "writer-uid", ResourceVersion: "12", ObservedAtUnix: 11, ReadOnlyInspection: true}
	qualification, err := BuildTargetQualificationReceipt(newProvisioning, newTarget, writers, strings.Repeat("2", 64), strings.Repeat("e", 64), strings.Repeat("6", 64), newTarget.PDAddrs, newTarget.CheckedAtUnix)
	require.NoError(t, err)
	oldRetirement := retirementReceipt(oldProvisioning, oldTarget)
	oldRetirement.OldTargetProvisioningSHA256 = strings.Repeat("1", 64)
	oldRetirement.OldRestoreAdmissionSHA256 = strings.Repeat("4", 64)
	in := TargetReplacementInputs{
		FailedOperation: FailedRestoreOperationEvidence{AuditArtifactSHA256: strings.Repeat("a", 64), OperationName: "native-pitr-restore-" + strings.Repeat("c", 20), ParametersSHA256: strings.Repeat("c", 64), Attempt: 2, MaxAttempts: 2},
		OldPlanSHA256:   digest, NewPlanSHA256: strings.Repeat("b", 64), OldTargetReceiptSHA256: oldPlan.Target.SnapshotEmptyReceiptSHA256,
		NewTargetReceiptSHA256: newPlan.Target.SnapshotEmptyReceiptSHA256, NewRestoreAdmissionSHA256: strings.Repeat("d", 64),
		OldTargetProvisioningSHA256: strings.Repeat("1", 64), NewTargetProvisioningSHA256: strings.Repeat("2", 64),
		NewTargetQualificationSHA256: strings.Repeat("5", 64), WriterExclusionSHA256: strings.Repeat("6", 64),
		OldTargetRetirementSHA256: strings.Repeat("3", 64),
		OldRestoreAdmissionSHA256: strings.Repeat("4", 64),
		SourceExclusiveSHA256:     newPlan.Source.RangeExclusiveReceiptSHA256, FullArtifactSHA256: newPlan.Full.ArtifactReceiptSHA256, CreatedAtUnix: 13,
	}
	return oldPlan, newPlan, oldTarget, newTarget, oldProvisioning, newProvisioning, qualification, writers, oldRetirement, oldAdmission, newAdmission, in
}

func TestTargetReplacementHandoffBindsExhaustedRestoreAndNewCluster(t *testing.T) {
	oldPlan, newPlan, oldTarget, newTarget, oldProvisioning, newProvisioning, qualification, writers, retirement, oldAdmission, admission, in := validTargetReplacement(t)
	handoff, err := BuildTargetReplacementHandoff(oldPlan, newPlan, oldTarget, newTarget, oldProvisioning, newProvisioning, qualification, writers, retirement, oldAdmission, admission, in)
	require.NoError(t, err)
	require.Equal(t, oldTarget.ClusterID, handoff.OldTargetClusterID)
	require.Equal(t, newTarget.ClusterID, handoff.NewTargetClusterID)
	b, err := json.Marshal(handoff)
	require.NoError(t, err)
	_, err = DecodeTargetReplacementHandoff(strings.NewReader(string(b)))
	require.NoError(t, err)
	_, err = DecodeTargetReplacementHandoff(strings.NewReader(string(b) + `{}`))
	require.ErrorContains(t, err, "trailing")
}

func TestTargetReplacementHandoffRejectsLineageDrift(t *testing.T) {
	oldPlan, newPlan, oldTarget, newTarget, oldProvisioning, newProvisioning, qualification, writers, retirement, oldAdmission, admission, in := validTargetReplacement(t)
	for _, mutate := range []func(*Plan, *TargetReplacementInputs){
		func(plan *Plan, _ *TargetReplacementInputs) { plan.RestoreTS++ },
		func(plan *Plan, _ *TargetReplacementInputs) { plan.Full.EncryptionKeyID = "other/key" },
		func(_ *Plan, inputs *TargetReplacementInputs) { inputs.FailedOperation.Attempt = 1 },
		func(_ *Plan, inputs *TargetReplacementInputs) {
			inputs.NewTargetReceiptSHA256 = strings.Repeat("f", 64)
		},
	} {
		driftedPlan, driftedInputs := newPlan, in
		mutate(&driftedPlan, &driftedInputs)
		_, err := BuildTargetReplacementHandoff(oldPlan, driftedPlan, oldTarget, newTarget, oldProvisioning, newProvisioning, qualification, writers, retirement, oldAdmission, admission, driftedInputs)
		require.Error(t, err)
	}
}

func TestTargetReplacementHandoffRejectsOldAdmissionFromAnotherPlan(t *testing.T) {
	oldPlan, newPlan, oldTarget, newTarget, oldProvisioning, newProvisioning, qualification, writers, retirement, oldAdmission, admission, in := validTargetReplacement(t)
	in.OldPlanSHA256 = strings.Repeat("9", 64)
	_, err := BuildTargetReplacementHandoff(oldPlan, newPlan, oldTarget, newTarget, oldProvisioning, newProvisioning, qualification, writers, retirement, oldAdmission, admission, in)
	require.ErrorContains(t, err, "old plan")
}

func TestTargetReplacementHandoffOperationBinding(t *testing.T) {
	oldPlan, newPlan, oldTarget, newTarget, oldProvisioning, newProvisioning, qualification, writers, retirement, oldAdmission, admission, in := validTargetReplacement(t)
	handoff, err := BuildTargetReplacementHandoff(oldPlan, newPlan, oldTarget, newTarget, oldProvisioning, newProvisioning, qualification, writers, retirement, oldAdmission, admission, in)
	require.NoError(t, err)
	binding := FullRestoreOperationBinding{PlanSHA256: in.NewPlanSHA256, SourceExclusiveSHA256: in.SourceExclusiveSHA256, FullArtifactSHA256: in.FullArtifactSHA256, TargetSnapshotSHA256: in.NewTargetReceiptSHA256, RestoreAdmissionSHA256: in.NewRestoreAdmissionSHA256}
	require.NoError(t, VerifyTargetReplacementHandoffBinding(handoff, newPlan, oldTarget, newTarget, oldProvisioning, newProvisioning, qualification, writers, retirement, oldAdmission, in.OldTargetReceiptSHA256, in.OldTargetProvisioningSHA256, in.NewTargetProvisioningSHA256, in.NewTargetQualificationSHA256, in.WriterExclusionSHA256, in.OldTargetRetirementSHA256, retirement.OldRestoreAdmissionSHA256, binding))
	binding.TargetSnapshotSHA256 = strings.Repeat("f", 64)
	require.ErrorContains(t, VerifyTargetReplacementHandoffBinding(handoff, newPlan, oldTarget, newTarget, oldProvisioning, newProvisioning, qualification, writers, retirement, oldAdmission, in.OldTargetReceiptSHA256, in.OldTargetProvisioningSHA256, in.NewTargetProvisioningSHA256, in.NewTargetQualificationSHA256, in.WriterExclusionSHA256, in.OldTargetRetirementSHA256, retirement.OldRestoreAdmissionSHA256, binding), "replacement target receipt")
}

func TestTargetReplacementHandoffRejectsQualificationOrWriterReplay(t *testing.T) {
	oldPlan, newPlan, oldTarget, newTarget, oldProvisioning, newProvisioning, qualification, writers, retirement, oldAdmission, admission, in := validTargetReplacement(t)
	driftedQualification := qualification
	driftedQualification.WriterExclusionSHA256 = strings.Repeat("7", 64)
	_, err := BuildTargetReplacementHandoff(oldPlan, newPlan, oldTarget, newTarget, oldProvisioning, newProvisioning, driftedQualification, writers, retirement, oldAdmission, admission, in)
	require.ErrorContains(t, err, "qualification")
	driftedWriters := writers
	driftedWriters.ResourceVersion = "13"
	_, err = BuildTargetReplacementHandoff(oldPlan, newPlan, oldTarget, newTarget, oldProvisioning, newProvisioning, qualification, driftedWriters, retirement, oldAdmission, admission, in)
	require.ErrorContains(t, err, "qualification")
}
