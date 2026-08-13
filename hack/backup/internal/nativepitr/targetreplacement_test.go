package nativepitr

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func validTargetReplacement(t *testing.T) (Plan, Plan, TargetSnapshotEmptyReceipt, TargetSnapshotEmptyReceipt, RestoreAdmissionReceipt, TargetReplacementInputs) {
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
	in := TargetReplacementInputs{
		FailedOperation: FailedRestoreOperationEvidence{AuditArtifactSHA256: strings.Repeat("a", 64), OperationName: "native-pitr-restore-" + strings.Repeat("c", 20), ParametersSHA256: strings.Repeat("c", 64), Attempt: 2, MaxAttempts: 2},
		OldPlanSHA256:   digest, NewPlanSHA256: strings.Repeat("b", 64), OldTargetReceiptSHA256: oldPlan.Target.SnapshotEmptyReceiptSHA256,
		NewTargetReceiptSHA256: newPlan.Target.SnapshotEmptyReceiptSHA256, NewRestoreAdmissionSHA256: strings.Repeat("d", 64),
		SourceExclusiveSHA256: newPlan.Source.RangeExclusiveReceiptSHA256, FullArtifactSHA256: newPlan.Full.ArtifactReceiptSHA256, CreatedAtUnix: 13,
	}
	return oldPlan, newPlan, oldTarget, newTarget, newAdmission, in
}

func TestTargetReplacementHandoffBindsExhaustedRestoreAndNewCluster(t *testing.T) {
	oldPlan, newPlan, oldTarget, newTarget, admission, in := validTargetReplacement(t)
	handoff, err := BuildTargetReplacementHandoff(oldPlan, newPlan, oldTarget, newTarget, admission, in)
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
	oldPlan, newPlan, oldTarget, newTarget, admission, in := validTargetReplacement(t)
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
		_, err := BuildTargetReplacementHandoff(oldPlan, driftedPlan, oldTarget, newTarget, admission, driftedInputs)
		require.Error(t, err)
	}
}

func TestTargetReplacementHandoffOperationBinding(t *testing.T) {
	oldPlan, newPlan, oldTarget, newTarget, admission, in := validTargetReplacement(t)
	handoff, err := BuildTargetReplacementHandoff(oldPlan, newPlan, oldTarget, newTarget, admission, in)
	require.NoError(t, err)
	binding := FullRestoreOperationBinding{PlanSHA256: in.NewPlanSHA256, SourceExclusiveSHA256: in.SourceExclusiveSHA256, FullArtifactSHA256: in.FullArtifactSHA256, TargetSnapshotSHA256: in.NewTargetReceiptSHA256, RestoreAdmissionSHA256: in.NewRestoreAdmissionSHA256}
	require.NoError(t, VerifyTargetReplacementHandoffBinding(handoff, newPlan, binding))
	binding.TargetSnapshotSHA256 = strings.Repeat("f", 64)
	require.ErrorContains(t, VerifyTargetReplacementHandoffBinding(handoff, newPlan, binding), "replacement target receipt")
}
