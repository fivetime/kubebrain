package nativepitr

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"

	"github.com/kubewharf/kubebrain/hack/backup/internal/backupfile"
)

const PITRSemanticVerificationFormat = "kubebrain.native-pitr-semantic-verify.v1"

type PITRSemanticVerificationReceipt struct {
	Format                        string `json:"format"`
	PlanSHA256                    string `json:"plan_sha256"`
	FullSnapshotReceiptSHA256     string `json:"full_snapshot_receipt_sha256"`
	FullRestoreReceiptSHA256      string `json:"full_restore_receipt_sha256"`
	LogReplayReceiptSHA256        string `json:"log_replay_receipt_sha256"`
	FenceHandoffReceiptSHA256     string `json:"fence_handoff_receipt_sha256"`
	AdmissionHandoffReceiptSHA256 string `json:"admission_handoff_receipt_sha256"`
	SourceClusterID               uint64 `json:"source_cluster_id"`
	TargetClusterID               uint64 `json:"target_cluster_id"`
	Keyspace                      string `json:"keyspace"`
	BackupTS                      uint64 `json:"backup_ts"`
	RestoreTS                     uint64 `json:"restore_ts"`
	WitnessFormat                 string `json:"witness_format"`
	WitnessFileSHA256             string `json:"witness_file_sha256"`
	WitnessContentSHA256          string `json:"witness_content_sha256"`
	WitnessRevision               int64  `json:"witness_revision"`
	WitnessRecords                int    `json:"witness_records"`
	WitnessLeases                 int    `json:"witness_leases"`
	HistoricalHeaderRevision      int64  `json:"historical_header_revision"`
	CurrentHeaderRevision         int64  `json:"current_header_revision"`
	ProbePutRevision              int64  `json:"probe_put_revision"`
	ProbeDeleteRevision           int64  `json:"probe_delete_revision"`
	HistoricalExact               bool   `json:"historical_exact"`
	CurrentExact                  bool   `json:"current_exact"`
	LeaseIdentityExact            bool   `json:"lease_identity_exact"`
	WatchProbeSucceeded           bool   `json:"watch_probe_succeeded"`
	TargetProbeHistoryExact       bool   `json:"target_probe_history_exact"`
	ReplayWriteFenceProven        bool   `json:"replay_write_fence_proven"`
	ContinuousWriterExclusion     bool   `json:"continuous_writer_exclusion"`
	FenceHandoffProven            bool   `json:"fence_handoff_proven"`
	PostRestoreSemanticValidated  bool   `json:"post_restore_semantic_validated"`
	TargetFullImportFenceProven   bool   `json:"target_full_import_fence_proven"`
	PITRComplete                  bool   `json:"pitr_complete"`
	VerifiedAtUnix                int64  `json:"verified_at_unix"`
}

type PITRSemanticVerificationInput struct {
	FullSemanticVerificationInput
	LogReplaySHA256        string
	FenceHandoffSHA256     string
	AdmissionHandoffSHA256 string
}

func BuildPITRSemanticVerification(plan Plan, full FullSnapshotReceipt, restore FullRestoreExecutionReceipt, replay LogReplayExecutionReceipt, admissionHandoff AdmissionHandoffReceipt, handoff RestorationFenceHandoffReceipt, witness backupfile.Status, in PITRSemanticVerificationInput) (PITRSemanticVerificationReceipt, error) {
	if err := plan.Validate(); err != nil {
		return PITRSemanticVerificationReceipt{}, err
	}
	if err := validateFullSnapshotReceipt(full); err != nil {
		return PITRSemanticVerificationReceipt{}, err
	}
	if err := restore.Validate(); err != nil {
		return PITRSemanticVerificationReceipt{}, err
	}
	if err := replay.Validate(); err != nil {
		return PITRSemanticVerificationReceipt{}, err
	}
	if err := admissionHandoff.Validate(); err != nil {
		return PITRSemanticVerificationReceipt{}, err
	}
	if err := handoff.Validate(); err != nil {
		return PITRSemanticVerificationReceipt{}, err
	}
	base := in.FullSemanticVerificationInput
	for _, value := range []string{base.PlanSHA256, base.FullSnapshotSHA256, base.FullRestoreSHA256, base.WitnessFileSHA256, in.LogReplaySHA256, in.FenceHandoffSHA256, in.AdmissionHandoffSHA256} {
		if !sha256RE.MatchString(value) {
			return PITRSemanticVerificationReceipt{}, errors.New("PITR semantic verification input contains invalid digest")
		}
	}
	if plan.RestoreTS <= plan.Full.BackupTS || plan.Full.ReceiptSHA256 != base.FullSnapshotSHA256 || full.BackupTS != plan.Full.BackupTS || full.ClusterID != plan.Source.ClusterID || full.Keyspace != plan.Source.Keyspace ||
		restore.PlanSHA256 != base.PlanSHA256 || restore.PreWriteTarget.ClusterID != plan.Target.ClusterID || restore.FullArtifactSHA256 != plan.Full.ArtifactReceiptSHA256 || restore.ArtifactManifestSHA256 != plan.Full.ArtifactManifestSHA ||
		replay.PlanSHA256 != base.PlanSHA256 || replay.FullRestoreReceiptSHA256 != base.FullRestoreSHA256 || replay.LogArtifactReceiptSHA256 != plan.Log.ArtifactReceiptSHA || replay.AdmissionHandoffReceiptSHA256 != in.AdmissionHandoffSHA256 || !replay.TargetWriteFenceProven || !replay.ContinuousWriterExclusion || replay.TargetClusterID != plan.Target.ClusterID || replay.Keyspace != plan.Source.Keyspace || replay.RestoreTS != plan.RestoreTS ||
		admissionHandoff.PlanSHA256 != base.PlanSHA256 || admissionHandoff.RestoreAdmissionReceiptSHA256 != restore.RestoreAdmissionSHA256 || admissionHandoff.FullRestoreReceiptSHA256 != base.FullRestoreSHA256 || admissionHandoff.RestorationFenceReceiptSHA256 != replay.RestorationFenceReceiptSHA256 || admissionHandoff.TargetClusterID != plan.Target.ClusterID || admissionHandoff.Keyspace != plan.Source.Keyspace || admissionHandoff.ReleasedAtUnix > replay.StartedAtUnix || !admissionHandoff.ContinuousWriterExclusion ||
		handoff.PlanSHA256 != base.PlanSHA256 || handoff.OperationID != admissionHandoff.OperationID || handoff.TargetClusterID != plan.Target.ClusterID || handoff.Keyspace != plan.Source.Keyspace || handoff.LogReplayReceiptSHA256 != in.LogReplaySHA256 || handoff.RestorationFenceReceiptSHA256 != replay.RestorationFenceReceiptSHA256 || handoff.ReleasedAtUnix < replay.CompletedAtUnix || !handoff.ReplayWriteFenceProven || !handoff.AllKeysReopened {
		return PITRSemanticVerificationReceipt{}, errors.New("PITR semantic evidence does not match restore plan")
	}
	if witness.Format != backupfile.Format || witness.Prefix != "/" || witness.Revision <= 0 || witness.Records < 0 || witness.Leases < 0 || !sha256RE.MatchString(witness.SHA256) || witness.CreatedAtUnix <= 0 || witness.CreatedAtUnix > replay.StartedAtUnix {
		return PITRSemanticVerificationReceipt{}, errors.New("PITR semantic witness is not a pre-replay full-keyspace logical.v2 artifact")
	}
	if plan.SourceWitness.FileSHA256 != base.WitnessFileSHA256 || plan.SourceWitness.ContentSHA256 != witness.SHA256 || plan.SourceWitness.Format != witness.Format || plan.SourceWitness.Prefix != witness.Prefix || plan.SourceWitness.Revision != witness.Revision || plan.SourceWitness.CreatedAtUnix != witness.CreatedAtUnix || plan.SourceWitness.Records != witness.Records || plan.SourceWitness.Leases != witness.Leases {
		return PITRSemanticVerificationReceipt{}, errors.New("PITR semantic witness does not match restore plan")
	}
	if base.VerifiedAtUnix < handoff.ReleasedAtUnix {
		return PITRSemanticVerificationReceipt{}, errors.New("PITR semantic verification predates fence handoff")
	}
	r := PITRSemanticVerificationReceipt{
		Format: PITRSemanticVerificationFormat, PlanSHA256: base.PlanSHA256, FullSnapshotReceiptSHA256: base.FullSnapshotSHA256,
		FullRestoreReceiptSHA256: base.FullRestoreSHA256, LogReplayReceiptSHA256: in.LogReplaySHA256, FenceHandoffReceiptSHA256: in.FenceHandoffSHA256, AdmissionHandoffReceiptSHA256: in.AdmissionHandoffSHA256,
		SourceClusterID: plan.Source.ClusterID, TargetClusterID: plan.Target.ClusterID, Keyspace: plan.Source.Keyspace, BackupTS: plan.Full.BackupTS, RestoreTS: plan.RestoreTS,
		WitnessFormat: witness.Format, WitnessFileSHA256: base.WitnessFileSHA256, WitnessContentSHA256: witness.SHA256, WitnessRevision: witness.Revision, WitnessRecords: witness.Records, WitnessLeases: witness.Leases,
		HistoricalHeaderRevision: base.HistoricalHeaderRevision, CurrentHeaderRevision: base.CurrentHeaderRevision, ProbePutRevision: base.ProbePutRevision, ProbeDeleteRevision: base.ProbeDeleteRevision,
		HistoricalExact: base.HistoricalExact, CurrentExact: base.CurrentExact, LeaseIdentityExact: base.LeaseIdentityExact, WatchProbeSucceeded: base.WatchProbeSucceeded, TargetProbeHistoryExact: base.TargetProbeHistoryExact,
		ReplayWriteFenceProven: true, ContinuousWriterExclusion: true, FenceHandoffProven: true, PostRestoreSemanticValidated: true, TargetFullImportFenceProven: true, PITRComplete: true, VerifiedAtUnix: base.VerifiedAtUnix,
	}
	return r, r.Validate()
}

func (r PITRSemanticVerificationReceipt) Validate() error {
	if r.Format != PITRSemanticVerificationFormat || r.SourceClusterID == 0 || r.TargetClusterID == 0 || r.SourceClusterID == r.TargetClusterID || r.Keyspace == "" || r.BackupTS == 0 || r.RestoreTS <= r.BackupTS || r.WitnessFormat != backupfile.Format || r.WitnessRevision <= 0 || r.WitnessRecords < 0 || r.WitnessLeases < 0 || r.HistoricalHeaderRevision < r.WitnessRevision || r.CurrentHeaderRevision < r.WitnessRevision || r.ProbePutRevision <= 0 || r.ProbeDeleteRevision <= r.ProbePutRevision || !r.HistoricalExact || !r.CurrentExact || !r.LeaseIdentityExact || !r.WatchProbeSucceeded || !r.TargetProbeHistoryExact || !r.ReplayWriteFenceProven || !r.ContinuousWriterExclusion || !r.FenceHandoffProven || !r.PostRestoreSemanticValidated || !r.TargetFullImportFenceProven || !r.PITRComplete || r.VerifiedAtUnix <= 0 {
		return errors.New("native PITR semantic verification receipt is incomplete")
	}
	for _, value := range []string{r.PlanSHA256, r.FullSnapshotReceiptSHA256, r.FullRestoreReceiptSHA256, r.LogReplayReceiptSHA256, r.FenceHandoffReceiptSHA256, r.AdmissionHandoffReceiptSHA256, r.WitnessFileSHA256, r.WitnessContentSHA256} {
		if !sha256RE.MatchString(value) {
			return errors.New("native PITR semantic verification receipt has invalid digest evidence")
		}
	}
	return nil
}

func DecodePITRSemanticVerification(reader io.Reader) (PITRSemanticVerificationReceipt, error) {
	var receipt PITRSemanticVerificationReceipt
	decoder := json.NewDecoder(reader)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&receipt); err != nil {
		return receipt, fmt.Errorf("decode native PITR semantic verification receipt: %w", err)
	}
	if err := requireEOF(decoder); err != nil {
		return receipt, errors.New("native PITR semantic verification receipt contains trailing JSON")
	}
	return receipt, receipt.Validate()
}
