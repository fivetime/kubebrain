package nativepitr

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
)

const TargetReplacementHandoffFormat = "kubebrain.native-pitr-target-replacement-handoff.v2"

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
	NewTargetQualificationSHA256 string `json:"new_target_qualification_receipt_sha256"`
	WriterExclusionSHA256        string `json:"writer_exclusion_sha256"`
	OldTargetRetirementSHA256    string `json:"old_target_retirement_receipt_sha256"`
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
	NewTargetQualificationSHA256, WriterExclusionSHA256      string
	OldTargetRetirementSHA256                                string
	OldRestoreAdmissionSHA256                                string
	NewRestoreAdmissionSHA256, SourceExclusiveSHA256         string
	FullArtifactSHA256                                       string
	CreatedAtUnix                                            int64
}

func BuildTargetReplacementHandoff(oldPlan, newPlan Plan, oldTarget, newTarget TargetSnapshotEmptyReceipt, oldProvisioning, newProvisioning TargetProvisioningReceipt, qualification TargetQualificationReceipt, writers TargetWriterExclusionEvidence, oldRetirement TargetRetirementReceipt, oldAdmission, newAdmission RestoreAdmissionReceipt, in TargetReplacementInputs) (TargetReplacementHandoff, error) {
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
	if err := VerifyTargetQualificationBinding(qualification, newProvisioning, newTarget, writers, in.NewTargetProvisioningSHA256, in.NewTargetReceiptSHA256, in.WriterExclusionSHA256, newTarget.PDAddrs); err != nil {
		return TargetReplacementHandoff{}, fmt.Errorf("replacement target qualification: %w", err)
	}
	if in.NewTargetQualificationSHA256 == "" {
		return TargetReplacementHandoff{}, errors.New("replacement target qualification digest is required")
	}
	if err := VerifyTargetRetirementBinding(oldRetirement, oldProvisioning, in.OldTargetProvisioningSHA256, oldTarget, oldAdmission, in.OldRestoreAdmissionSHA256); err != nil {
		return TargetReplacementHandoff{}, err
	}
	if oldAdmission.PlanSHA256 != in.OldPlanSHA256 || oldAdmission.Keyspace != oldPlan.Source.Keyspace {
		return TargetReplacementHandoff{}, errors.New("old restore admission does not bind the old plan and keyspace")
	}
	for _, value := range []string{in.FailedOperation.AuditArtifactSHA256, in.FailedOperation.ParametersSHA256, in.OldPlanSHA256, in.NewPlanSHA256, in.OldTargetReceiptSHA256, in.NewTargetReceiptSHA256, in.OldTargetProvisioningSHA256, in.NewTargetProvisioningSHA256, in.NewTargetQualificationSHA256, in.WriterExclusionSHA256, in.OldTargetRetirementSHA256, in.OldRestoreAdmissionSHA256, in.NewRestoreAdmissionSHA256, in.SourceExclusiveSHA256, in.FullArtifactSHA256} {
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
		NewTargetProvisioningSHA256: in.NewTargetProvisioningSHA256, OldTargetRetirementSHA256: in.OldTargetRetirementSHA256,
		NewTargetQualificationSHA256: in.NewTargetQualificationSHA256, WriterExclusionSHA256: in.WriterExclusionSHA256,
		NewRestoreAdmissionSHA256: in.NewRestoreAdmissionSHA256,
		SourceExclusiveSHA256:     in.SourceExclusiveSHA256, FullArtifactSHA256: in.FullArtifactSHA256,
		OldTargetClusterID: oldPlan.Target.ClusterID, NewTargetClusterID: newPlan.Target.ClusterID,
		ReplacementTargetEmpty: true, AdmissionFenceReacquired: true, CreatedAtUnix: in.CreatedAtUnix,
	}
	return r, r.Validate()
}

func (r TargetReplacementHandoff) Validate() error {
	if r.Format != TargetReplacementHandoffFormat || r.FailedOperationName == "" || r.OldTargetClusterID == 0 || r.NewTargetClusterID == 0 || r.OldTargetClusterID == r.NewTargetClusterID || !r.ReplacementTargetEmpty || !r.AdmissionFenceReacquired || r.CreatedAtUnix <= 0 {
		return errors.New("invalid native PITR target replacement handoff")
	}
	for _, value := range []string{r.FailedOperationAuditSHA256, r.FailedOperationParametersSHA, r.OldPlanSHA256, r.NewPlanSHA256, r.OldTargetReceiptSHA256, r.NewTargetReceiptSHA256, r.OldTargetProvisioningSHA256, r.NewTargetProvisioningSHA256, r.NewTargetQualificationSHA256, r.WriterExclusionSHA256, r.OldTargetRetirementSHA256, r.NewRestoreAdmissionSHA256, r.SourceExclusiveSHA256, r.FullArtifactSHA256} {
		if !sha256RE.MatchString(value) {
			return errors.New("target replacement handoff contains invalid digest evidence")
		}
	}
	if r.FailedOperationName != "native-pitr-restore-"+r.FailedOperationParametersSHA[:20] {
		return errors.New("target replacement handoff operation identity mismatch")
	}
	return nil
}

func VerifyTargetReplacementHandoffBinding(handoff TargetReplacementHandoff, plan Plan, oldTarget, target TargetSnapshotEmptyReceipt, oldProvisioning, newProvisioning TargetProvisioningReceipt, qualification TargetQualificationReceipt, writers TargetWriterExclusionEvidence, oldRetirement TargetRetirementReceipt, oldAdmission RestoreAdmissionReceipt, oldTargetSHA, oldProvisioningSHA, newProvisioningSHA, qualificationSHA, writerSHA, oldRetirementSHA, oldAdmissionSHA string, binding FullRestoreOperationBinding) error {
	if err := handoff.Validate(); err != nil {
		return err
	}
	checks := []struct {
		ok    bool
		field string
	}{
		{handoff.NewPlanSHA256 == binding.PlanSHA256, "replacement plan digest"},
		{handoff.NewTargetReceiptSHA256 == binding.TargetSnapshotSHA256, "replacement target receipt digest"},
		{handoff.OldTargetReceiptSHA256 == oldTargetSHA, "old target receipt digest"},
		{handoff.OldTargetProvisioningSHA256 == oldProvisioningSHA, "old provisioning receipt digest"},
		{handoff.NewTargetProvisioningSHA256 == newProvisioningSHA, "replacement provisioning receipt digest"},
		{handoff.NewTargetQualificationSHA256 == qualificationSHA, "replacement qualification receipt digest"},
		{handoff.WriterExclusionSHA256 == writerSHA, "writer exclusion evidence digest"},
		{handoff.OldTargetRetirementSHA256 == oldRetirementSHA, "old retirement receipt digest"},
		{oldAdmission.PlanSHA256 == handoff.OldPlanSHA256, "old restore admission plan digest"},
		{oldAdmission.Keyspace == plan.Source.Keyspace, "old restore admission keyspace"},
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
	if err := VerifyTargetQualificationBinding(qualification, newProvisioning, target, writers, newProvisioningSHA, binding.TargetSnapshotSHA256, writerSHA, target.PDAddrs); err != nil {
		return fmt.Errorf("target replacement handoff qualification: %w", err)
	}
	return VerifyTargetRetirementBinding(oldRetirement, oldProvisioning, oldProvisioningSHA, oldTarget, oldAdmission, oldAdmissionSHA)
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
