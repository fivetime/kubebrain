package nativepitr

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"

	"github.com/kubewharf/kubebrain/hack/backup/internal/backupfile"
	"github.com/kubewharf/kubebrain/pkg/backend"
	"github.com/kubewharf/kubebrain/pkg/backend/coder"
	"github.com/kubewharf/kubebrain/pkg/backend/restorationfence"
	"github.com/kubewharf/kubebrain/pkg/storage"
)

const (
	SourceCaptureFenceReceiptFormat = "kubebrain.native-pitr-source-capture-fence.v1"
	SourceCaptureReceiptFormat      = "kubebrain.native-pitr-source-capture.v1"
)

type SourceCaptureFenceReceipt struct {
	Format                  string `json:"format"`
	OperationID             string `json:"operation_id"`
	TaskCreateSHA256        string `json:"task_create_sha256"`
	WitnessFileSHA256       string `json:"witness_file_sha256"`
	WitnessContentSHA256    string `json:"witness_content_sha256"`
	SourceClusterID         uint64 `json:"source_cluster_id"`
	Keyspace                string `json:"keyspace"`
	CoordinationPrefix      string `json:"coordination_prefix"`
	TokenSHA256             string `json:"token_sha256"`
	FenceKeyCount           int    `json:"fence_key_count"`
	WitnessRevision         int64  `json:"witness_revision"`
	ObservedSourceRevision  int64  `json:"observed_source_revision"`
	WitnessCreatedAtUnix    int64  `json:"witness_created_at_unix"`
	FenceSnapshotTS         uint64 `json:"fence_snapshot_ts"`
	VerifiedAtUnix          int64  `json:"verified_at_unix"`
	Resumed                 bool   `json:"resumed_existing_ownership"`
	AllFenceKeysHeld        bool   `json:"all_fence_keys_held"`
	WitnessRevisionExact    bool   `json:"witness_revision_exact"`
	SourceWriterFenceProven bool   `json:"source_writer_fence_proven"`
}

type SourceCaptureReceipt struct {
	Format                    string `json:"format"`
	SourceCaptureFenceSHA256  string `json:"source_capture_fence_receipt_sha256"`
	TaskCreateSHA256          string `json:"task_create_sha256"`
	FullSnapshotSHA256        string `json:"full_snapshot_receipt_sha256"`
	WitnessFileSHA256         string `json:"witness_file_sha256"`
	WitnessContentSHA256      string `json:"witness_content_sha256"`
	OperationID               string `json:"operation_id"`
	SourceClusterID           uint64 `json:"source_cluster_id"`
	Keyspace                  string `json:"keyspace"`
	WitnessRevision           int64  `json:"witness_revision"`
	FullBackupTS              uint64 `json:"full_backup_ts"`
	CaptureTS                 uint64 `json:"capture_ts"`
	FenceSnapshotTS           uint64 `json:"fence_snapshot_ts"`
	ContinuousSourceExclusion bool   `json:"continuous_source_writer_exclusion"`
	AllFenceKeysReopened      bool   `json:"all_fence_keys_reopened"`
	FinalizedAtUnix           int64  `json:"finalized_at_unix"`
}

func BuildSourceCaptureFenceReceipt(task TaskCreateReceipt, taskSHA, witnessFileSHA, operationID string, witness backupfile.Status, observedRevision int64, fenceSnapshotTS uint64, verifiedAt int64, resumed bool) (SourceCaptureFenceReceipt, restorationfence.Token, error) {
	if err := validateTaskCreateReceipt(task); err != nil {
		return SourceCaptureFenceReceipt{}, restorationfence.Token{}, err
	}
	if !sha256RE.MatchString(taskSHA) || !sha256RE.MatchString(witnessFileSHA) || !operationIDRE.MatchString(operationID) || witness.Format != backupfile.Format || witness.Prefix != "/" || witness.Revision <= 0 || witness.Revision != observedRevision || witness.CreatedAtUnix <= 0 || witness.CreatedAtUnix > verifiedAt || witness.Records < 0 || witness.Leases < 0 || !sha256RE.MatchString(witness.SHA256) || fenceSnapshotTS == 0 || verifiedAt <= 0 {
		return SourceCaptureFenceReceipt{}, restorationfence.Token{}, errors.New("invalid source capture fence evidence")
	}
	token, err := restorationfence.NewToken(operationID, taskSHA, task.ClusterID, task.Keyspace)
	if err != nil {
		return SourceCaptureFenceReceipt{}, restorationfence.Token{}, err
	}
	tokenSHA, err := token.SHA256()
	if err != nil {
		return SourceCaptureFenceReceipt{}, restorationfence.Token{}, err
	}
	r := SourceCaptureFenceReceipt{Format: SourceCaptureFenceReceiptFormat, OperationID: operationID, TaskCreateSHA256: taskSHA, WitnessFileSHA256: witnessFileSHA, WitnessContentSHA256: witness.SHA256, SourceClusterID: task.ClusterID, Keyspace: task.Keyspace, CoordinationPrefix: CoordinationPrefix(task.Keyspace), TokenSHA256: tokenSHA, FenceKeyCount: restorationfence.ShardCount + 1, WitnessRevision: witness.Revision, ObservedSourceRevision: observedRevision, WitnessCreatedAtUnix: witness.CreatedAtUnix, FenceSnapshotTS: fenceSnapshotTS, VerifiedAtUnix: verifiedAt, Resumed: resumed, AllFenceKeysHeld: true, WitnessRevisionExact: true, SourceWriterFenceProven: true}
	return r, token, r.Validate()
}

func (r SourceCaptureFenceReceipt) Validate() error {
	if r.Format != SourceCaptureFenceReceiptFormat || !operationIDRE.MatchString(r.OperationID) || !sha256RE.MatchString(r.TaskCreateSHA256) || !sha256RE.MatchString(r.WitnessFileSHA256) || !sha256RE.MatchString(r.WitnessContentSHA256) || r.SourceClusterID == 0 || r.Keyspace == "" || r.CoordinationPrefix != CoordinationPrefix(r.Keyspace) || !sha256RE.MatchString(r.TokenSHA256) || r.FenceKeyCount != restorationfence.ShardCount+1 || r.WitnessRevision <= 0 || r.ObservedSourceRevision != r.WitnessRevision || r.WitnessCreatedAtUnix <= 0 || r.FenceSnapshotTS == 0 || r.VerifiedAtUnix < r.WitnessCreatedAtUnix || !r.AllFenceKeysHeld || !r.WitnessRevisionExact || !r.SourceWriterFenceProven {
		return errors.New("invalid native PITR source capture fence receipt")
	}
	token, err := restorationfence.NewToken(r.OperationID, r.TaskCreateSHA256, r.SourceClusterID, r.Keyspace)
	if err != nil {
		return err
	}
	want, err := token.SHA256()
	if err != nil || want != r.TokenSHA256 {
		return errors.New("source capture fence token digest mismatch")
	}
	return nil
}

func (r SourceCaptureFenceReceipt) Token() (restorationfence.Token, error) {
	if err := r.Validate(); err != nil {
		return restorationfence.Token{}, err
	}
	return restorationfence.NewToken(r.OperationID, r.TaskCreateSHA256, r.SourceClusterID, r.Keyspace)
}

func BuildSourceCaptureReceipt(task TaskCreateReceipt, taskSHA string, fence SourceCaptureFenceReceipt, fenceSHA string, full FullSnapshotReceipt, fullSHA string, captureTS uint64, finalizedAt int64) (SourceCaptureReceipt, error) {
	if err := validateTaskCreateReceipt(task); err != nil {
		return SourceCaptureReceipt{}, err
	}
	if err := fence.Validate(); err != nil {
		return SourceCaptureReceipt{}, err
	}
	if err := validateFullSnapshotReceipt(full); err != nil {
		return SourceCaptureReceipt{}, err
	}
	if !sha256RE.MatchString(taskSHA) || !sha256RE.MatchString(fenceSHA) || !sha256RE.MatchString(fullSHA) || fence.TaskCreateSHA256 != taskSHA || fence.SourceClusterID != task.ClusterID || fence.Keyspace != task.Keyspace || full.TaskCreateSHA256 != taskSHA || full.ClusterID != task.ClusterID || full.Keyspace != task.Keyspace || full.TaskName != task.TaskName || full.BackupTS > captureTS || captureTS < fence.FenceSnapshotTS || captureTS >= task.EndTS || finalizedAt < fence.VerifiedAtUnix {
		return SourceCaptureReceipt{}, errors.New("source capture evidence does not form a continuous fenced snapshot")
	}
	r := SourceCaptureReceipt{Format: SourceCaptureReceiptFormat, SourceCaptureFenceSHA256: fenceSHA, TaskCreateSHA256: taskSHA, FullSnapshotSHA256: fullSHA, WitnessFileSHA256: fence.WitnessFileSHA256, WitnessContentSHA256: fence.WitnessContentSHA256, OperationID: fence.OperationID, SourceClusterID: task.ClusterID, Keyspace: task.Keyspace, WitnessRevision: fence.WitnessRevision, FullBackupTS: full.BackupTS, CaptureTS: captureTS, FenceSnapshotTS: fence.FenceSnapshotTS, ContinuousSourceExclusion: true, AllFenceKeysReopened: true, FinalizedAtUnix: finalizedAt}
	return r, r.Validate()
}

func (r SourceCaptureReceipt) Validate() error {
	if r.Format != SourceCaptureReceiptFormat || !operationIDRE.MatchString(r.OperationID) || r.SourceClusterID == 0 || r.Keyspace == "" || r.WitnessRevision <= 0 || r.FullBackupTS == 0 || r.CaptureTS < r.FullBackupTS || r.CaptureTS < r.FenceSnapshotTS || r.FenceSnapshotTS == 0 || !r.ContinuousSourceExclusion || !r.AllFenceKeysReopened || r.FinalizedAtUnix <= 0 {
		return errors.New("invalid native PITR source capture receipt")
	}
	for _, value := range []string{r.SourceCaptureFenceSHA256, r.TaskCreateSHA256, r.FullSnapshotSHA256, r.WitnessFileSHA256, r.WitnessContentSHA256} {
		if !sha256RE.MatchString(value) {
			return errors.New("source capture receipt has invalid digest")
		}
	}
	return nil
}

func DecodeSourceCaptureFenceReceipt(reader io.Reader) (SourceCaptureFenceReceipt, error) {
	var r SourceCaptureFenceReceipt
	dec := json.NewDecoder(reader)
	dec.DisallowUnknownFields()
	if err := dec.Decode(&r); err != nil {
		return r, fmt.Errorf("decode source capture fence receipt: %w", err)
	}
	if err := requireEOF(dec); err != nil {
		return r, errors.New("source capture fence receipt contains trailing JSON")
	}
	return r, r.Validate()
}

func DecodeSourceCaptureReceipt(reader io.Reader) (SourceCaptureReceipt, error) {
	var r SourceCaptureReceipt
	dec := json.NewDecoder(reader)
	dec.DisallowUnknownFields()
	if err := dec.Decode(&r); err != nil {
		return r, fmt.Errorf("decode source capture receipt: %w", err)
	}
	if err := requireEOF(dec); err != nil {
		return r, errors.New("source capture receipt contains trailing JSON")
	}
	return r, r.Validate()
}

// InspectFencedSourceRevision returns the highest committed etcd revision
// visible in a tenant after its restoration fence has excluded all writers.
// The durable watermark covers compacted/deleted latest versions; the physical
// object scan detects a lagging watermark and rejects unknown encodings.
func InspectFencedSourceRevision(ctx context.Context, store storage.KvStorage, keyspace string) (revisionResult uint64, retErr error) {
	ks, err := coder.NewKeyspace(keyspace)
	if err != nil {
		return 0, err
	}
	durable, err := backend.ReadDurableRevision(ctx, store, keyspace)
	if err != nil {
		return 0, fmt.Errorf("read source durable revision: %w", err)
	}
	ts, err := store.GetTimestampOracle(ctx)
	if err != nil {
		return 0, fmt.Errorf("get fenced source snapshot TSO: %w", err)
	}
	it, err := store.Iter(ctx, ks.ObjectKeyspaceStart(), ks.ObjectKeyspaceEnd(), ts, 0)
	if err != nil {
		return 0, fmt.Errorf("scan fenced source revisions: %w", err)
	}
	defer func() { retErr = errors.Join(retErr, it.Close()) }()
	maxRevision := durable
	c := ks.NewCoder()
	for {
		if err := it.Next(ctx); err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return 0, fmt.Errorf("scan fenced source revisions: %w", err)
		}
		key := append([]byte(nil), it.Key()...)
		if ks.IsInternalStorageKey(key) {
			continue
		}
		_, revision, err := c.Decode(key)
		if err != nil {
			return 0, fmt.Errorf("decode fenced source object key: %w", err)
		}
		if revision > maxRevision {
			maxRevision = revision
		}
	}
	if maxRevision == 0 {
		return 0, errors.New("fenced source revision is zero")
	}
	return maxRevision, nil
}
