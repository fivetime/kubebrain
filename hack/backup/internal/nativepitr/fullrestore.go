package nativepitr

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
)

const (
	FullRestoreExecutionFormat       = "kubebrain.native-pitr-full-restore.v3"
	legacyFullRestoreExecutionFormat = "kubebrain.native-pitr-full-restore.v2"
)

// FullRestoreExecutionReceipt records only BR's transactional full-snapshot
// import. It deliberately cannot represent completed log replay or PITR.
type FullRestoreExecutionReceipt struct {
	Format                       string                     `json:"format"`
	PlanSHA256                   string                     `json:"plan_sha256"`
	SourceExclusiveSHA256        string                     `json:"source_range_exclusive_receipt_sha256"`
	FullArtifactSHA256           string                     `json:"full_artifact_receipt_sha256"`
	ArtifactManifestSHA256       string                     `json:"artifact_manifest_sha256"`
	RestoreAdmissionSHA256       string                     `json:"restore_admission_receipt_sha256"`
	PreWriteTarget               TargetSnapshotEmptyReceipt `json:"pre_write_target"`
	BRVersion                    string                     `json:"br_version"`
	BRBinarySHA256               string                     `json:"br_binary_sha256"`
	Encryption                   string                     `json:"encryption"`
	EncryptionKeyID              string                     `json:"encryption_key_id,omitempty"`
	StartedAtUnix                int64                      `json:"started_at_unix"`
	CompletedAtUnix              int64                      `json:"completed_at_unix"`
	WholeClusterTxnImport        bool                       `json:"whole_cluster_txn_import"`
	SourceVisibleRangeExclusive  bool                       `json:"source_visible_range_exclusive"`
	TargetWriteFenceProven       bool                       `json:"target_write_fence_proven"`
	FullImportAdmissionProven    bool                       `json:"full_import_admission_proven"`
	FullSnapshotRestored         bool                       `json:"full_snapshot_restored"`
	LogReplayCompleted           bool                       `json:"log_replay_completed"`
	PostRestoreSemanticValidated bool                       `json:"post_restore_semantic_validated"`
	PITRComplete                 bool                       `json:"pitr_complete"`
}

func (r FullRestoreExecutionReceipt) Validate() error {
	if (r.Format != FullRestoreExecutionFormat && r.Format != legacyFullRestoreExecutionFormat) || !r.WholeClusterTxnImport || !r.SourceVisibleRangeExclusive || !r.FullSnapshotRestored || !r.TargetWriteFenceProven || !r.FullImportAdmissionProven || r.LogReplayCompleted || r.PostRestoreSemanticValidated || r.PITRComplete {
		return errors.New("receipt is not an admission-fenced full-only native restore")
	}
	encryption := EncryptionIdentity{Method: r.Encryption, KeyID: r.EncryptionKeyID}
	if r.Format == legacyFullRestoreExecutionFormat && encryption.Method == "" {
		encryption.Method = CipherMethodPlaintext
	}
	if err := encryption.Validate(); err != nil || (r.Format == legacyFullRestoreExecutionFormat && encryption.Method != CipherMethodPlaintext) {
		return errors.New("full restore receipt has invalid encryption identity")
	}
	for _, value := range []string{r.PlanSHA256, r.SourceExclusiveSHA256, r.FullArtifactSHA256, r.ArtifactManifestSHA256, r.RestoreAdmissionSHA256, r.BRBinarySHA256} {
		if !sha256RE.MatchString(value) {
			return errors.New("full restore receipt contains invalid digest evidence")
		}
	}
	if err := r.PreWriteTarget.Validate(); err != nil {
		return fmt.Errorf("full restore pre-write target: %w", err)
	}
	if r.StartedAtUnix <= 0 || r.CompletedAtUnix < r.StartedAtUnix {
		return errors.New("full restore receipt contains invalid execution times")
	}
	if !strings.Contains(r.BRVersion, "Release Version: v7.5.1\n") || !strings.Contains(r.BRVersion, "Git Commit Hash: 7d16cc79e81bbf573124df3fd9351c26963f3e70\n") {
		return errors.New("full restore receipt was not produced by the pinned BR build")
	}
	return nil
}

func DecodeFullRestoreExecution(r io.Reader) (FullRestoreExecutionReceipt, error) {
	var receipt FullRestoreExecutionReceipt
	dec := json.NewDecoder(r)
	dec.DisallowUnknownFields()
	if err := dec.Decode(&receipt); err != nil {
		return receipt, fmt.Errorf("decode full restore receipt: %w", err)
	}
	if err := requireEOF(dec); err != nil {
		return receipt, errors.New("full restore receipt contains trailing JSON")
	}
	if err := receipt.Validate(); err != nil {
		return receipt, err
	}
	return receipt, nil
}
