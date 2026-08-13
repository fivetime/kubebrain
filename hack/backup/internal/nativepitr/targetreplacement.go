package nativepitr

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
)

const TargetReplacementHandoffFormat = "kubebrain.native-pitr-target-replacement-handoff.v1"

type FailedRestoreOperationEvidence struct {
	AuditArtifactSHA256 string
	OperationName       string
	ParametersSHA256    string
	Attempt             int64
	MaxAttempts         int64
}

// TargetReplacementHandoff proves that an exhausted, receipt-less full restore
// is being moved to a different empty PD/TiKV cluster without changing any
// source snapshot, log, witness, capture, encryption, or restore-TS evidence.
type TargetReplacementHandoff struct {
	Format                       string `json:"format"`
	FailedOperationAuditSHA256   string `json:"failed_operation_audit_sha256"`
	FailedOperationName          string `json:"failed_operation_name"`
	FailedOperationParametersSHA string `json:"failed_operation_parameters_sha256"`
	OldPlanSHA256                string `json:"old_plan_sha256"`
	NewPlanSHA256                string `json:"new_plan_sha256"`
	OldTargetReceiptSHA256       string `json:"old_target_snapshot_empty_receipt_sha256"`
	NewTargetReceiptSHA256       string `json:"new_target_snapshot_empty_receipt_sha256"`
	OldTargetProvisioningSHA256  string `json:"old_target_provisioning_receipt_sha256"`
	NewTargetProvisioningSHA256  string `json:"new_target_provisioning_receipt_sha256"`
	NewRestoreAdmissionSHA256    string `json:"new_restore_admission_receipt_sha256"`
	SourceExclusiveSHA256        string `json:"source_range_exclusive_receipt_sha256"`
	FullArtifactSHA256           string `json:"full_artifact_receipt_sha256"`
	OldTargetClusterID           uint64 `json:"old_target_cluster_id"`
	NewTargetClusterID           uint64 `json:"new_target_cluster_id"`
	ReplacementTargetEmpty       bool   `json:"replacement_target_empty"`
	AdmissionFenceReacquired     bool   `json:"admission_fence_reacquired"`
	CreatedAtUnix                int64  `json:"created_at_unix"`
}

type TargetReplacementInputs struct {
	FailedOperation                                          FailedRestoreOperationEvidence
	OldPlanSHA256, NewPlanSHA256                             string
	OldTargetReceiptSHA256, NewTargetReceiptSHA256           string
	OldTargetProvisioningSHA256, NewTargetProvisioningSHA256 string
	NewRestoreAdmissionSHA256, SourceExclusiveSHA256         string
	FullArtifactSHA256                                       string
	CreatedAtUnix                                            int64
}

func BuildTargetReplacementHandoff(oldPlan, newPlan Plan, oldTarget, newTarget TargetSnapshotEmptyReceipt, oldProvisioning, newProvisioning TargetProvisioningReceipt, newAdmission RestoreAdmissionReceipt, in TargetReplacementInputs) (TargetReplacementHandoff, error) {
	if err := oldPlan.Validate(); err != nil {
		return TargetReplacementHandoff{}, fmt.Errorf("old plan: %w", err)
	}
	if err := newPlan.Validate(); err != nil {
		return TargetReplacementHandoff{}, fmt.Errorf("new plan: %w", err)
	}
	if err := oldTarget.Validate(); err != nil {
		return TargetReplacementHandoff{}, fmt.Errorf("old target: %w", err)
	}
	if err := newTarget.Validate(); err != nil {
		return TargetReplacementHandoff{}, fmt.Errorf("new target: %w", err)
	}
	if err := newAdmission.Validate(); err != nil {
		return TargetReplacementHandoff{}, fmt.Errorf("new admission: %w", err)
	}
	if err := VerifyReplacementProvisioning(oldProvisioning, newProvisioning, oldTarget, newTarget); err != nil {
		return TargetReplacementHandoff{}, err
	}
	for _, value := range []string{in.FailedOperation.AuditArtifactSHA256, in.FailedOperation.ParametersSHA256, in.OldPlanSHA256, in.NewPlanSHA256, in.OldTargetReceiptSHA256, in.NewTargetReceiptSHA256, in.OldTargetProvisioningSHA256, in.NewTargetProvisioningSHA256, in.NewRestoreAdmissionSHA256, in.SourceExclusiveSHA256, in.FullArtifactSHA256} {
		if !sha256RE.MatchString(value) {
			return TargetReplacementHandoff{}, errors.New("target replacement input contains an invalid digest")
		}
	}
	if in.FailedOperation.Attempt != 2 || in.FailedOperation.MaxAttempts != 2 || in.FailedOperation.OperationName != "native-pitr-restore-"+in.FailedOperation.ParametersSHA256[:20] {
		return TargetReplacementHandoff{}, errors.New("target replacement requires the exhausted exact restore operation")
	}
	if in.CreatedAtUnix <= 0 || oldPlan.Target.ClusterID == newPlan.Target.ClusterID || oldTarget.ClusterID != oldPlan.Target.ClusterID || newTarget.ClusterID != newPlan.Target.ClusterID {
		return TargetReplacementHandoff{}, errors.New("target replacement does not identify distinct exact old and new clusters")
	}
	if oldPlan.Target.SnapshotEmptyReceiptSHA256 != in.OldTargetReceiptSHA256 || newPlan.Target.SnapshotEmptyReceiptSHA256 != in.NewTargetReceiptSHA256 || newPlan.Source.RangeExclusiveReceiptSHA256 != in.SourceExclusiveSHA256 || newPlan.Full.ArtifactReceiptSHA256 != in.FullArtifactSHA256 {
		return TargetReplacementHandoff{}, errors.New("target replacement receipt digests do not match the plans")
	}
	if oldPlan.Source != newPlan.Source || oldPlan.SourceWitness != newPlan.SourceWitness || oldPlan.SourceCapture != newPlan.SourceCapture || oldPlan.Full != newPlan.Full || oldPlan.Log != newPlan.Log || oldPlan.RestoreTS != newPlan.RestoreTS || oldPlan.ReadOnly != newPlan.ReadOnly {
		return TargetReplacementHandoff{}, errors.New("replacement plan changes immutable source restore evidence")
	}
	if newAdmission.PlanSHA256 != in.NewPlanSHA256 || newAdmission.TargetClusterID != newPlan.Target.ClusterID || newAdmission.Keyspace != newPlan.Source.Keyspace {
		return TargetReplacementHandoff{}, errors.New("replacement admission does not bind the new plan and target")
	}
	r := TargetReplacementHandoff{
		Format: TargetReplacementHandoffFormat, FailedOperationAuditSHA256: in.FailedOperation.AuditArtifactSHA256,
		FailedOperationName: in.FailedOperation.OperationName, FailedOperationParametersSHA: in.FailedOperation.ParametersSHA256,
		OldPlanSHA256: in.OldPlanSHA256, NewPlanSHA256: in.NewPlanSHA256, OldTargetReceiptSHA256: in.OldTargetReceiptSHA256,
		NewTargetReceiptSHA256: in.NewTargetReceiptSHA256, OldTargetProvisioningSHA256: in.OldTargetProvisioningSHA256,
		NewTargetProvisioningSHA256: in.NewTargetProvisioningSHA256, NewRestoreAdmissionSHA256: in.NewRestoreAdmissionSHA256,
		SourceExclusiveSHA256: in.SourceExclusiveSHA256, FullArtifactSHA256: in.FullArtifactSHA256,
		OldTargetClusterID: oldPlan.Target.ClusterID, NewTargetClusterID: newPlan.Target.ClusterID,
		ReplacementTargetEmpty: true, AdmissionFenceReacquired: true, CreatedAtUnix: in.CreatedAtUnix,
	}
	return r, r.Validate()
}

func (r TargetReplacementHandoff) Validate() error {
	if r.Format != TargetReplacementHandoffFormat || r.FailedOperationName == "" || r.OldTargetClusterID == 0 || r.NewTargetClusterID == 0 || r.OldTargetClusterID == r.NewTargetClusterID || !r.ReplacementTargetEmpty || !r.AdmissionFenceReacquired || r.CreatedAtUnix <= 0 {
		return errors.New("invalid native PITR target replacement handoff")
	}
	for _, value := range []string{r.FailedOperationAuditSHA256, r.FailedOperationParametersSHA, r.OldPlanSHA256, r.NewPlanSHA256, r.OldTargetReceiptSHA256, r.NewTargetReceiptSHA256, r.OldTargetProvisioningSHA256, r.NewTargetProvisioningSHA256, r.NewRestoreAdmissionSHA256, r.SourceExclusiveSHA256, r.FullArtifactSHA256} {
		if !sha256RE.MatchString(value) {
			return errors.New("target replacement handoff contains invalid digest evidence")
		}
	}
	if r.FailedOperationName != "native-pitr-restore-"+r.FailedOperationParametersSHA[:20] {
		return errors.New("target replacement handoff operation identity mismatch")
	}
	return nil
}

func VerifyTargetReplacementHandoffBinding(handoff TargetReplacementHandoff, plan Plan, target TargetSnapshotEmptyReceipt, newProvisioning TargetProvisioningReceipt, newProvisioningSHA256 string, binding FullRestoreOperationBinding) error {
	checks := []struct {
		ok    bool
		field string
	}{
		{handoff.NewPlanSHA256 == binding.PlanSHA256, "replacement plan digest"},
		{handoff.NewTargetReceiptSHA256 == binding.TargetSnapshotSHA256, "replacement target receipt digest"},
		{handoff.NewTargetProvisioningSHA256 == newProvisioningSHA256, "replacement provisioning receipt digest"},
		{handoff.NewRestoreAdmissionSHA256 == binding.RestoreAdmissionSHA256, "replacement admission digest"},
		{handoff.SourceExclusiveSHA256 == binding.SourceExclusiveSHA256, "replacement source-exclusive digest"},
		{handoff.FullArtifactSHA256 == binding.FullArtifactSHA256, "replacement artifact digest"},
		{handoff.NewTargetClusterID == plan.Target.ClusterID, "replacement target cluster"},
		{newProvisioning.ClusterID == plan.Target.ClusterID, "provisioning target cluster"},
		{newProvisioning.PDReplicas == len(target.PDAddrs), "provisioning PD replicas"},
		{newProvisioning.TiKVReplicas == len(target.Stores), "provisioning TiKV replicas"},
	}
	for _, check := range checks {
		if !check.ok {
			return fmt.Errorf("target replacement handoff does not match the approved restore operation: %s", check.field)
		}
	}
	return nil
}

func DecodeTargetReplacementHandoff(reader io.Reader) (TargetReplacementHandoff, error) {
	var handoff TargetReplacementHandoff
	dec := json.NewDecoder(reader)
	dec.DisallowUnknownFields()
	if err := dec.Decode(&handoff); err != nil {
		return handoff, fmt.Errorf("decode target replacement handoff: %w", err)
	}
	if err := requireEOF(dec); err != nil {
		return handoff, errors.New("target replacement handoff contains trailing JSON")
	}
	return handoff, handoff.Validate()
}
