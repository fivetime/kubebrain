package nativepitr

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"

	"github.com/kubewharf/kubebrain/hack/backup/internal/backupfile"
)

const FullSemanticVerificationFormat = "kubebrain.native-pitr-full-semantic-verify.v1"

type FullSemanticVerificationReceipt struct {
	Format                       string `json:"format"`
	PlanSHA256                   string `json:"plan_sha256"`
	FullSnapshotReceiptSHA256    string `json:"full_snapshot_receipt_sha256"`
	FullRestoreReceiptSHA256     string `json:"full_restore_receipt_sha256"`
	SourceClusterID              uint64 `json:"source_cluster_id"`
	TargetClusterID              uint64 `json:"target_cluster_id"`
	Keyspace                     string `json:"keyspace"`
	BackupTS                     uint64 `json:"backup_ts"`
	RestoreCompletedAtUnix       int64  `json:"restore_completed_at_unix"`
	WitnessFormat                string `json:"witness_format"`
	WitnessFileSHA256            string `json:"witness_file_sha256"`
	WitnessContentSHA256         string `json:"witness_content_sha256"`
	WitnessRevision              int64  `json:"witness_revision"`
	WitnessRecords               int    `json:"witness_records"`
	WitnessLeases                int    `json:"witness_leases"`
	HistoricalHeaderRevision     int64  `json:"historical_header_revision"`
	CurrentHeaderRevision        int64  `json:"current_header_revision"`
	ProbePutRevision             int64  `json:"probe_put_revision"`
	ProbeDeleteRevision          int64  `json:"probe_delete_revision"`
	HistoricalExact              bool   `json:"historical_exact"`
	CurrentExact                 bool   `json:"current_exact"`
	LeaseIdentityExact           bool   `json:"lease_identity_exact"`
	WatchProbeSucceeded          bool   `json:"watch_probe_succeeded"`
	TargetProbeHistoryExact      bool   `json:"target_probe_history_exact"`
	FullRestoreSemanticValidated bool   `json:"full_restore_semantic_validated"`
	LogReplayValidated           bool   `json:"log_replay_validated"`
	PITRComplete                 bool   `json:"pitr_complete"`
	VerifiedAtUnix               int64  `json:"verified_at_unix"`
}

type FullSemanticVerificationInput struct {
	PlanSHA256               string
	FullSnapshotSHA256       string
	FullRestoreSHA256        string
	WitnessFileSHA256        string
	HistoricalHeaderRevision int64
	CurrentHeaderRevision    int64
	ProbePutRevision         int64
	ProbeDeleteRevision      int64
	HistoricalExact          bool
	CurrentExact             bool
	LeaseIdentityExact       bool
	WatchProbeSucceeded      bool
	TargetProbeHistoryExact  bool
	VerifiedAtUnix           int64
}

func BuildFullSemanticVerification(plan Plan, full FullSnapshotReceipt, restore FullRestoreExecutionReceipt, witness backupfile.Status, in FullSemanticVerificationInput) (FullSemanticVerificationReceipt, error) {
	if err := plan.Validate(); err != nil {
		return FullSemanticVerificationReceipt{}, err
	}
	if err := validateFullSnapshotReceipt(full); err != nil {
		return FullSemanticVerificationReceipt{}, err
	}
	if err := restore.Validate(); err != nil {
		return FullSemanticVerificationReceipt{}, err
	}
	if !sha256RE.MatchString(in.PlanSHA256) || !sha256RE.MatchString(in.FullSnapshotSHA256) || !sha256RE.MatchString(in.FullRestoreSHA256) || !sha256RE.MatchString(in.WitnessFileSHA256) {
		return FullSemanticVerificationReceipt{}, errors.New("semantic verification input contains invalid digest")
	}
	if plan.Full.ReceiptSHA256 != in.FullSnapshotSHA256 || plan.Full.BackupTS != full.BackupTS || plan.Source.ClusterID != full.ClusterID || plan.Source.Keyspace != full.Keyspace || plan.RestoreTS != full.BackupTS {
		return FullSemanticVerificationReceipt{}, errors.New("semantic verification full snapshot does not match plan")
	}
	if restore.PlanSHA256 != in.PlanSHA256 || restore.PreWriteTarget.ClusterID != plan.Target.ClusterID || restore.FullArtifactSHA256 != plan.Full.ArtifactReceiptSHA256 || restore.ArtifactManifestSHA256 != plan.Full.ArtifactManifestSHA {
		return FullSemanticVerificationReceipt{}, errors.New("semantic verification restore receipt does not match plan")
	}
	if witness.Format != backupfile.Format || witness.Prefix != "/" || witness.Revision <= 0 || witness.Records < 0 || witness.Leases < 0 || !sha256RE.MatchString(witness.SHA256) || witness.CreatedAtUnix <= 0 {
		return FullSemanticVerificationReceipt{}, errors.New("semantic witness is not a complete full-keyspace logical.v2 artifact")
	}
	if uint64(witness.Revision) > full.BackupTS || witness.CreatedAtUnix > restore.StartedAtUnix {
		return FullSemanticVerificationReceipt{}, errors.New("semantic witness was not captured before the full snapshot restore chain")
	}
	if in.VerifiedAtUnix < restore.CompletedAtUnix {
		return FullSemanticVerificationReceipt{}, errors.New("semantic verification predates restore completion")
	}
	receipt := FullSemanticVerificationReceipt{Format: FullSemanticVerificationFormat, PlanSHA256: in.PlanSHA256, FullSnapshotReceiptSHA256: in.FullSnapshotSHA256, FullRestoreReceiptSHA256: in.FullRestoreSHA256, SourceClusterID: plan.Source.ClusterID, TargetClusterID: plan.Target.ClusterID, Keyspace: plan.Source.Keyspace, BackupTS: full.BackupTS, RestoreCompletedAtUnix: restore.CompletedAtUnix, WitnessFormat: witness.Format, WitnessFileSHA256: in.WitnessFileSHA256, WitnessContentSHA256: witness.SHA256, WitnessRevision: witness.Revision, WitnessRecords: witness.Records, WitnessLeases: witness.Leases, HistoricalHeaderRevision: in.HistoricalHeaderRevision, CurrentHeaderRevision: in.CurrentHeaderRevision, ProbePutRevision: in.ProbePutRevision, ProbeDeleteRevision: in.ProbeDeleteRevision, HistoricalExact: in.HistoricalExact, CurrentExact: in.CurrentExact, LeaseIdentityExact: in.LeaseIdentityExact, WatchProbeSucceeded: in.WatchProbeSucceeded, TargetProbeHistoryExact: in.TargetProbeHistoryExact, FullRestoreSemanticValidated: true, LogReplayValidated: false, PITRComplete: false, VerifiedAtUnix: in.VerifiedAtUnix}
	if err := receipt.Validate(); err != nil {
		return FullSemanticVerificationReceipt{}, err
	}
	return receipt, nil
}

func (r FullSemanticVerificationReceipt) Validate() error {
	if r.Format != FullSemanticVerificationFormat || r.SourceClusterID == 0 || r.TargetClusterID == 0 || r.SourceClusterID == r.TargetClusterID || r.Keyspace == "" || r.BackupTS == 0 || r.RestoreCompletedAtUnix <= 0 || r.WitnessFormat != backupfile.Format || r.WitnessRevision <= 0 || uint64(r.WitnessRevision) > r.BackupTS || r.WitnessRecords < 0 || r.WitnessLeases < 0 || r.HistoricalHeaderRevision < r.WitnessRevision || r.CurrentHeaderRevision < r.WitnessRevision || r.ProbePutRevision <= 0 || r.ProbeDeleteRevision <= r.ProbePutRevision || !r.HistoricalExact || !r.CurrentExact || !r.LeaseIdentityExact || !r.WatchProbeSucceeded || !r.TargetProbeHistoryExact || !r.FullRestoreSemanticValidated || r.LogReplayValidated || r.PITRComplete || r.VerifiedAtUnix < r.RestoreCompletedAtUnix {
		return errors.New("native full semantic verification receipt is incomplete")
	}
	for _, digest := range []string{r.PlanSHA256, r.FullSnapshotReceiptSHA256, r.FullRestoreReceiptSHA256, r.WitnessFileSHA256, r.WitnessContentSHA256} {
		if !sha256RE.MatchString(digest) {
			return errors.New("native full semantic verification receipt has invalid digest evidence")
		}
	}
	return nil
}

func DecodeFullSemanticVerification(r io.Reader) (FullSemanticVerificationReceipt, error) {
	var receipt FullSemanticVerificationReceipt
	dec := json.NewDecoder(r)
	dec.DisallowUnknownFields()
	if err := dec.Decode(&receipt); err != nil {
		return receipt, fmt.Errorf("decode native full semantic verification receipt: %w", err)
	}
	if err := requireEOF(dec); err != nil {
		return receipt, errors.New("native full semantic verification receipt contains trailing JSON")
	}
	return receipt, receipt.Validate()
}
