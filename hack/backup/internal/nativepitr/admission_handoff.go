package nativepitr

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
)

const AdmissionHandoffReceiptFormat = "kubebrain.native-pitr-admission-handoff.v1"

type AdmissionHandoffReceipt struct {
	Format                        string `json:"format"`
	PlanSHA256                    string `json:"plan_sha256"`
	RestoreAdmissionReceiptSHA256 string `json:"restore_admission_receipt_sha256"`
	FullRestoreReceiptSHA256      string `json:"full_restore_receipt_sha256"`
	RestorationFenceReceiptSHA256 string `json:"restoration_fence_receipt_sha256"`
	OperationID                   string `json:"operation_id"`
	TargetClusterID               uint64 `json:"target_cluster_id"`
	Keyspace                      string `json:"keyspace"`
	FullImportAdmissionProven     bool   `json:"full_import_admission_proven"`
	RestorationFenceHeld          bool   `json:"restoration_fence_held"`
	AdmissionGateReopened         bool   `json:"admission_gate_reopened"`
	ContinuousWriterExclusion     bool   `json:"continuous_writer_exclusion"`
	ReleasedAtUnix                int64  `json:"released_at_unix"`
}

func BuildAdmissionHandoff(plan Plan, planSHA string, admission RestoreAdmissionReceipt, admissionSHA string, full FullRestoreExecutionReceipt, fullSHA string, fence RestorationFenceReceipt, fenceSHA string, releasedAt int64) (AdmissionHandoffReceipt, error) {
	if err := plan.Validate(); err != nil {
		return AdmissionHandoffReceipt{}, err
	}
	if err := admission.Validate(); err != nil {
		return AdmissionHandoffReceipt{}, err
	}
	if err := full.Validate(); err != nil {
		return AdmissionHandoffReceipt{}, err
	}
	if err := fence.Validate(); err != nil {
		return AdmissionHandoffReceipt{}, err
	}
	for _, value := range []string{planSHA, admissionSHA, fullSHA, fenceSHA} {
		if !sha256RE.MatchString(value) {
			return AdmissionHandoffReceipt{}, errors.New("admission handoff has invalid digest")
		}
	}
	if admission.PlanSHA256 != planSHA || admission.TargetClusterID != plan.Target.ClusterID || admission.Keyspace != plan.Source.Keyspace || admission.OperationID != fence.OperationID ||
		admission.AcquiredAtUnix > full.StartedAtUnix ||
		full.PlanSHA256 != planSHA || full.RestoreAdmissionSHA256 != admissionSHA || full.PreWriteTarget.ClusterID != plan.Target.ClusterID || !planRestoreEncryptionMatches(plan, full) || !full.FullImportAdmissionProven || !full.TargetWriteFenceProven ||
		fence.PlanSHA256 != planSHA || fence.TargetClusterID != plan.Target.ClusterID || fence.Keyspace != plan.Source.Keyspace || fence.VerifiedAtUnix < full.CompletedAtUnix || releasedAt < fence.VerifiedAtUnix {
		return AdmissionHandoffReceipt{}, errors.New("admission handoff evidence does not form a continuous restore chain")
	}
	r := AdmissionHandoffReceipt{Format: AdmissionHandoffReceiptFormat, PlanSHA256: planSHA, RestoreAdmissionReceiptSHA256: admissionSHA, FullRestoreReceiptSHA256: fullSHA, RestorationFenceReceiptSHA256: fenceSHA, OperationID: admission.OperationID, TargetClusterID: plan.Target.ClusterID, Keyspace: plan.Source.Keyspace, FullImportAdmissionProven: true, RestorationFenceHeld: true, AdmissionGateReopened: true, ContinuousWriterExclusion: true, ReleasedAtUnix: releasedAt}
	return r, r.Validate()
}

func (r AdmissionHandoffReceipt) Validate() error {
	if r.Format != AdmissionHandoffReceiptFormat || !operationIDRE.MatchString(r.OperationID) || r.TargetClusterID == 0 || r.Keyspace == "" || !r.FullImportAdmissionProven || !r.RestorationFenceHeld || !r.AdmissionGateReopened || !r.ContinuousWriterExclusion || r.ReleasedAtUnix <= 0 {
		return errors.New("invalid native PITR admission handoff receipt")
	}
	for _, value := range []string{r.PlanSHA256, r.RestoreAdmissionReceiptSHA256, r.FullRestoreReceiptSHA256, r.RestorationFenceReceiptSHA256} {
		if !sha256RE.MatchString(value) {
			return errors.New("admission handoff receipt has invalid digest")
		}
	}
	return nil
}

func DecodeAdmissionHandoff(reader io.Reader) (AdmissionHandoffReceipt, error) {
	var receipt AdmissionHandoffReceipt
	dec := json.NewDecoder(reader)
	dec.DisallowUnknownFields()
	if err := dec.Decode(&receipt); err != nil {
		return receipt, fmt.Errorf("decode admission handoff receipt: %w", err)
	}
	if err := requireEOF(dec); err != nil {
		return receipt, errors.New("admission handoff receipt contains trailing JSON")
	}
	return receipt, receipt.Validate()
}
