// Package nativepitr defines fail-closed artifact contracts for native TiKV
// transactional PITR. A Plan is not a backup receipt: it binds independently
// produced evidence before a restore operation is allowed to mutate a target.
package nativepitr

import (
	"bytes"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"regexp"
	"strings"

	"github.com/kubewharf/kubebrain/hack/backup/internal/backupfile"
	"github.com/kubewharf/kubebrain/pkg/backend/coder"
)

const (
	PreflightFormat  = "kubebrain.native-pitr-preflight.v1"
	PlanFormat       = "kubebrain.native-pitr-restore-plan.v13"
	legacyPlanFormat = "kubebrain.native-pitr-restore-plan.v12"
)

var (
	dnsLabel = regexp.MustCompile(`^[a-z0-9]([a-z0-9-]{0,62}[a-z0-9])?$`)
	sha256RE = regexp.MustCompile(`^[0-9a-f]{64}$`)
)

type Preflight struct {
	Format        string   `json:"format"`
	ClusterID     uint64   `json:"cluster_id"`
	PDAddrs       []string `json:"pd_addrs"`
	Keyspace      string   `json:"keyspace"`
	TaskName      string   `json:"task_name"`
	StartKeyHex   string   `json:"start_key_hex"`
	EndKeyHex     string   `json:"end_key_hex"`
	TaskInfoKey   string   `json:"task_info_key"`
	TaskRangesKey string   `json:"task_ranges_prefix"`
	OwnershipKeys []string `json:"ownership_paths_checked"`
	TaskAvailable bool     `json:"task_name_available"`
	TaskCount     int64    `json:"existing_task_count"`
	Stores        []struct {
		ID      uint64 `json:"id"`
		Address string `json:"address"`
		Service string `json:"log_backup_service"`
	} `json:"stores"`
	ReadOnly bool `json:"read_only"`
}

type Source struct {
	ClusterID                   uint64 `json:"cluster_id"`
	Keyspace                    string `json:"keyspace"`
	StartKeyHex                 string `json:"start_key_hex"`
	EndKeyHex                   string `json:"end_key_hex"`
	TaskName                    string `json:"task_name"`
	RangeExclusiveReceiptSHA256 string `json:"range_exclusive_receipt_sha256"`
	ExclusiveSnapshotTS         uint64 `json:"exclusive_snapshot_ts"`
	OutsideVisibleKeyCount      uint64 `json:"outside_visible_key_count"`
	HistoricalMVCCAbsenceProven bool   `json:"historical_mvcc_absence_proven"`
	PDAddressCount              int    `json:"pd_address_count"`
}

type FullSnapshot struct {
	BackupTS              uint64 `json:"backup_ts"`
	BackupMetaSHA256      string `json:"backupmeta_sha256"`
	StoragePrefix         string `json:"storage_prefix"`
	Mode                  string `json:"mode"`
	ReceiptSHA256         string `json:"receipt_sha256"`
	ArtifactReceiptSHA256 string `json:"artifact_receipt_sha256"`
	ArtifactManifestSHA   string `json:"artifact_manifest_sha256"`
	ArtifactObjectCount   int    `json:"artifact_object_count"`
	ArtifactTotalBytes    uint64 `json:"artifact_total_bytes"`
	RemoteInventorySHA256 string `json:"remote_inventory_sha256"`
	ObjectStoreID         string `json:"object_store_id"`
	Bucket                string `json:"bucket"`
	ObjectPrefix          string `json:"object_prefix"`
	MinRetainUntilUnix    int64  `json:"min_retain_until_unix"`
	InventoryCheckedAt    int64  `json:"inventory_checked_at_unix"`
	RemoteExact           bool   `json:"remote_exact_versions_verified"`
	Encryption            string `json:"encryption"`
	EncryptionKeyID       string `json:"encryption_key_id,omitempty"`
}

type LogWindow struct {
	StartTS            uint64 `json:"start_ts"`
	TaskCommittedAtTS  uint64 `json:"task_committed_at_ts"`
	GlobalCheckpointTS uint64 `json:"global_checkpoint_ts"`
	AdvancerOwner      string `json:"advancer_owner"`
	StoragePrefix      string `json:"storage_prefix"`
	StorageSHA256      string `json:"storage_backend_sha256"`
	ReadyReceiptSHA256 string `json:"task_ready_receipt_sha256"`
	ArtifactReceiptSHA string `json:"artifact_receipt_sha256"`
	ArtifactManifest   string `json:"artifact_manifest_sha256"`
	ArtifactObjects    int    `json:"artifact_object_count"`
	ArtifactSegments   int    `json:"artifact_verified_segment_count"`
	ArtifactTotalBytes uint64 `json:"artifact_total_bytes"`
	MetadataResolvedTS uint64 `json:"metadata_max_resolved_ts"`
	RemoteInventorySHA string `json:"remote_inventory_sha256"`
	ObjectStoreID      string `json:"object_store_id"`
	Bucket             string `json:"bucket"`
	ObjectPrefix       string `json:"object_prefix"`
	ArtifactMetadata   int    `json:"artifact_metadata_count"`
	ArtifactData       int    `json:"artifact_data_object_count"`
	ArtifactControl    int    `json:"artifact_control_object_count"`
	RemoteExact        bool   `json:"remote_exact_versions_verified"`
	MinRetainUntilUnix int64  `json:"min_retain_until_unix"`
	InventoryCheckedAt int64  `json:"inventory_checked_at_unix"`
}

type Target struct {
	ClusterID                   uint64 `json:"cluster_id"`
	SnapshotEmptyReceiptSHA256  string `json:"snapshot_empty_receipt_sha256"`
	SnapshotTS                  uint64 `json:"snapshot_ts"`
	ScanScope                   string `json:"scan_scope"`
	VisibleCommittedKeyCount    uint64 `json:"visible_committed_key_count"`
	HistoricalMVCCAbsenceProven bool   `json:"historical_mvcc_absence_proven"`
	RawKVAbsenceProven          bool   `json:"raw_kv_absence_proven"`
	CheckedAtUnix               int64  `json:"checked_at_unix"`
	PDAddressCount              int    `json:"pd_address_count"`
	UpStoreCount                int    `json:"up_store_count"`
}

type SourceWitness struct {
	Format        string `json:"format"`
	FileSHA256    string `json:"file_sha256"`
	ContentSHA256 string `json:"content_sha256"`
	Prefix        string `json:"prefix"`
	Revision      int64  `json:"revision"`
	CreatedAtUnix int64  `json:"created_at_unix"`
	Records       int    `json:"records"`
	Leases        int    `json:"leases"`
}

type SourceCapture struct {
	ReceiptSHA256             string `json:"receipt_sha256"`
	TaskCreateSHA256          string `json:"task_create_sha256"`
	OperationID               string `json:"operation_id"`
	CaptureTS                 uint64 `json:"capture_ts"`
	FenceSnapshotTS           uint64 `json:"fence_snapshot_ts"`
	ContinuousWriterExclusion bool   `json:"continuous_writer_exclusion"`
}

type Plan struct {
	Format        string        `json:"format"`
	Source        Source        `json:"source"`
	SourceWitness SourceWitness `json:"source_witness"`
	SourceCapture SourceCapture `json:"source_capture"`
	Full          FullSnapshot  `json:"full_snapshot"`
	Log           LogWindow     `json:"log_window"`
	Target        Target        `json:"target"`
	RestoreTS     uint64        `json:"restore_ts"`
	ReadOnly      bool          `json:"read_only"`
}

type ReceiptPlanInputs struct {
	TaskCreateSHA256      string
	FullSnapshotSHA256    string
	ArtifactReceiptSHA256 string
	TaskReadySHA256       string
	LogArtifactSHA256     string
	SourceExclusiveSHA256 string
	TargetReceiptSHA256   string
	WitnessFileSHA256     string
	Witness               backupfile.Status
	SourceCaptureSHA256   string
	SourceCapture         SourceCaptureReceipt
	RestoreTS             uint64
}

func DecodePreflight(r io.Reader) (Preflight, error) {
	var p Preflight
	dec := json.NewDecoder(r)
	dec.DisallowUnknownFields()
	if err := dec.Decode(&p); err != nil {
		return p, fmt.Errorf("decode source preflight: %w", err)
	}
	if err := requireEOF(dec); err != nil {
		return p, err
	}
	if err := validatePreflight(p); err != nil {
		return p, err
	}
	return p, nil
}

// ValidatePreflight verifies that a receipt proves the exact tenant range and
// every metadata ownership path needed before task creation.
func ValidatePreflight(p Preflight) error { return validatePreflight(p) }

func DecodePlan(r io.Reader) (Plan, error) {
	var plan Plan
	dec := json.NewDecoder(r)
	dec.DisallowUnknownFields()
	if err := dec.Decode(&plan); err != nil {
		return plan, fmt.Errorf("decode native PITR restore plan: %w", err)
	}
	if err := requireEOF(dec); err != nil {
		return plan, errors.New("native PITR restore plan contains trailing JSON")
	}
	if err := plan.Validate(); err != nil {
		return plan, err
	}
	return plan, nil
}

// BuildFromReceipts constructs a restore plan without operator-supplied source
// timestamps, artifact digests, storage locations, or advancer identities.
func BuildFromReceipts(task TaskCreateReceipt, full FullSnapshotReceipt, artifacts ArtifactReceipt, ready TaskReadyReceipt, logs LogArtifactReceipt, sourceExclusive SourceRangeExclusiveReceipt, target TargetSnapshotEmptyReceipt, in ReceiptPlanInputs) (Plan, error) {
	if err := validateTaskCreateReceipt(task); err != nil {
		return Plan{}, err
	}
	if err := validateFullSnapshotReceipt(full); err != nil {
		return Plan{}, err
	}
	if err := validateTaskReadyReceipt(ready); err != nil {
		return Plan{}, err
	}
	if err := artifacts.Validate(); err != nil {
		return Plan{}, err
	}
	if err := logs.Validate(); err != nil {
		return Plan{}, err
	}
	if err := target.Validate(); err != nil {
		return Plan{}, err
	}
	if err := sourceExclusive.Validate(); err != nil {
		return Plan{}, err
	}
	if !sha256RE.MatchString(in.SourceExclusiveSHA256) || sourceExclusive.FullSnapshotReceiptSHA256 != in.FullSnapshotSHA256 || sourceExclusive.ClusterID != full.ClusterID || sourceExclusive.Keyspace != full.Keyspace || sourceExclusive.StartKeyHex != full.StartKeyHex || sourceExclusive.EndKeyHex != full.EndKeyHex || sourceExclusive.SnapshotTS != full.BackupTS {
		return Plan{}, errors.New("source range-exclusive receipt does not bind the exact full snapshot")
	}
	if !sha256RE.MatchString(in.TargetReceiptSHA256) {
		return Plan{}, errors.New("invalid exact target snapshot-empty receipt SHA-256")
	}
	if !sha256RE.MatchString(in.WitnessFileSHA256) || in.Witness.Format != backupfile.Format || in.Witness.Prefix != "/" || in.Witness.Revision <= 0 || in.Witness.CreatedAtUnix <= 0 || in.Witness.Records < 0 || in.Witness.Leases < 0 || !sha256RE.MatchString(in.Witness.SHA256) {
		return Plan{}, errors.New("source witness is not an exact full-keyspace logical.v2 artifact")
	}
	if err := in.SourceCapture.Validate(); err != nil {
		return Plan{}, err
	}
	if !sha256RE.MatchString(in.SourceCaptureSHA256) || in.SourceCapture.TaskCreateSHA256 != in.TaskCreateSHA256 || in.SourceCapture.FullSnapshotSHA256 != in.FullSnapshotSHA256 || in.SourceCapture.WitnessFileSHA256 != in.WitnessFileSHA256 || in.SourceCapture.WitnessContentSHA256 != in.Witness.SHA256 || in.SourceCapture.SourceClusterID != task.ClusterID || in.SourceCapture.Keyspace != task.Keyspace || in.SourceCapture.WitnessRevision != in.Witness.Revision || in.SourceCapture.FullBackupTS != full.BackupTS || in.SourceCapture.CaptureTS != in.RestoreTS {
		return Plan{}, errors.New("source capture receipt does not bind the exact task, full snapshot, witness, and restore TSO")
	}
	if !sha256RE.MatchString(in.TaskCreateSHA256) || full.TaskCreateSHA256 != in.TaskCreateSHA256 {
		return Plan{}, errors.New("full snapshot does not bind the exact task-create receipt")
	}
	if !readyMatchesTask(ready, task) || full.ClusterID != task.ClusterID || full.Keyspace != task.Keyspace || full.TaskName != task.TaskName || full.TaskStartTS != task.StartTS || full.TaskCommittedAtTS != task.CommittedAtTS || full.TaskEndTS != task.EndTS || full.StartKeyHex != task.StartKeyHex || full.EndKeyHex != task.EndKeyHex {
		return Plan{}, errors.New("native PITR receipts describe different source tasks")
	}
	if !sha256RE.MatchString(in.FullSnapshotSHA256) || !sha256RE.MatchString(in.ArtifactReceiptSHA256) || artifacts.FullReceiptSHA256 != in.FullSnapshotSHA256 || artifacts.ClusterID != full.ClusterID || artifacts.Keyspace != full.Keyspace || artifacts.TaskName != full.TaskName || artifacts.BackupTS != full.BackupTS || artifacts.StoragePrefix != full.StoragePrefix || artifacts.BackupMetaSHA256 != full.BackupMetaSHA256 {
		return Plan{}, errors.New("full artifact receipt does not bind the exact full snapshot")
	}
	if !sha256RE.MatchString(in.TaskReadySHA256) || !sha256RE.MatchString(in.LogArtifactSHA256) || logs.TaskCreateSHA256 != in.TaskCreateSHA256 || logs.TaskReadySHA256 != in.TaskReadySHA256 || logs.ClusterID != task.ClusterID || logs.Keyspace != task.Keyspace || logs.TaskName != task.TaskName || logs.StartTS != task.StartTS || logs.GlobalCheckpointTS != ready.GlobalCheckpointTS || logs.StoragePrefix != task.LogStoragePrefix || logs.StorageSHA256 != task.LogStorageSHA256 {
		return Plan{}, errors.New("log artifact receipt does not bind the exact task and task-ready receipts")
	}
	plan := Plan{
		Format:        PlanFormat,
		Source:        Source{ClusterID: task.ClusterID, Keyspace: task.Keyspace, StartKeyHex: task.StartKeyHex, EndKeyHex: task.EndKeyHex, TaskName: task.TaskName, RangeExclusiveReceiptSHA256: in.SourceExclusiveSHA256, ExclusiveSnapshotTS: sourceExclusive.SnapshotTS, OutsideVisibleKeyCount: sourceExclusive.OutsideVisibleKeyCount, HistoricalMVCCAbsenceProven: sourceExclusive.HistoricalMVCCAbsenceProven, PDAddressCount: len(sourceExclusive.PDAddrs)},
		SourceWitness: SourceWitness{Format: in.Witness.Format, FileSHA256: in.WitnessFileSHA256, ContentSHA256: in.Witness.SHA256, Prefix: in.Witness.Prefix, Revision: in.Witness.Revision, CreatedAtUnix: in.Witness.CreatedAtUnix, Records: in.Witness.Records, Leases: in.Witness.Leases},
		SourceCapture: SourceCapture{ReceiptSHA256: in.SourceCaptureSHA256, TaskCreateSHA256: in.SourceCapture.TaskCreateSHA256, OperationID: in.SourceCapture.OperationID, CaptureTS: in.SourceCapture.CaptureTS, FenceSnapshotTS: in.SourceCapture.FenceSnapshotTS, ContinuousWriterExclusion: in.SourceCapture.ContinuousSourceExclusion},
		Full:          FullSnapshot{BackupTS: full.BackupTS, BackupMetaSHA256: full.BackupMetaSHA256, StoragePrefix: full.StoragePrefix, Mode: "br-txn", ReceiptSHA256: in.FullSnapshotSHA256, ArtifactReceiptSHA256: in.ArtifactReceiptSHA256, ArtifactManifestSHA: artifacts.ManifestSHA256, ArtifactObjectCount: artifacts.ObjectCount, ArtifactTotalBytes: artifacts.TotalBytes, RemoteInventorySHA256: artifacts.RemoteInventorySHA256, ObjectStoreID: artifacts.ObjectStoreID, Bucket: artifacts.Bucket, ObjectPrefix: artifacts.ObjectPrefix, MinRetainUntilUnix: artifacts.MinRetainUntilUnix, InventoryCheckedAt: artifacts.InventoryCheckedAtUnix, RemoteExact: artifacts.RemoteVersionsVerified, Encryption: artifacts.Encryption, EncryptionKeyID: artifacts.EncryptionKeyID},
		Log:           LogWindow{StartTS: task.StartTS, TaskCommittedAtTS: task.CommittedAtTS, GlobalCheckpointTS: ready.GlobalCheckpointTS, AdvancerOwner: ready.AdvancerOwner, StoragePrefix: task.LogStoragePrefix, StorageSHA256: task.LogStorageSHA256, ReadyReceiptSHA256: in.TaskReadySHA256, ArtifactReceiptSHA: in.LogArtifactSHA256, ArtifactManifest: logs.ManifestSHA256, ArtifactObjects: logs.ObjectCount, ArtifactSegments: logs.VerifiedSegmentCount, ArtifactTotalBytes: logs.TotalBytes, MetadataResolvedTS: logs.MetadataMaxResolvedTS, RemoteInventorySHA: logs.RemoteInventorySHA256, ObjectStoreID: logs.ObjectStoreID, Bucket: logs.Bucket, ObjectPrefix: logs.ObjectPrefix, ArtifactMetadata: logs.MetadataCount, ArtifactData: logs.DataObjectCount, ArtifactControl: logs.ControlObjectCount, RemoteExact: logs.RemoteVersionsVerified, MinRetainUntilUnix: logs.MinRetainUntilUnix, InventoryCheckedAt: logs.InventoryCheckedAtUnix},
		Target: Target{
			ClusterID: target.ClusterID, SnapshotEmptyReceiptSHA256: in.TargetReceiptSHA256,
			SnapshotTS: target.SnapshotTS, ScanScope: target.ScanScope,
			VisibleCommittedKeyCount:    target.VisibleCommittedKeyCount,
			HistoricalMVCCAbsenceProven: target.HistoricalMVCCAbsenceProven,
			RawKVAbsenceProven:          target.RawKVAbsenceProven, CheckedAtUnix: target.CheckedAtUnix,
			PDAddressCount: len(target.PDAddrs), UpStoreCount: len(target.Stores),
		},
		RestoreTS: in.RestoreTS, ReadOnly: true,
	}
	if err := plan.Validate(); err != nil {
		return Plan{}, err
	}
	return plan, nil
}

func (p Plan) Validate() error {
	if p.Format != PlanFormat && p.Format != legacyPlanFormat {
		return fmt.Errorf("unsupported plan format %q", p.Format)
	}
	if p.Source.ClusterID == 0 || p.Target.ClusterID == 0 {
		return errors.New("source and target cluster IDs must be non-zero")
	}
	if p.Source.ClusterID == p.Target.ClusterID {
		return errors.New("target cluster must differ from source cluster")
	}
	if !dnsLabel.MatchString(p.Source.TaskName) {
		return errors.New("invalid task name")
	}
	ks, err := coder.NewKeyspace(p.Source.Keyspace)
	if err != nil {
		return fmt.Errorf("invalid source keyspace: %w", err)
	}
	start, err := hex.DecodeString(p.Source.StartKeyHex)
	if err != nil || len(start) == 0 {
		return errors.New("invalid source start key")
	}
	end, err := hex.DecodeString(p.Source.EndKeyHex)
	if err != nil || len(end) == 0 || bytes.Compare(start, end) >= 0 {
		return errors.New("invalid source key range")
	}
	if !bytes.Equal(start, ks.ObjectKeyspaceStart()) || !bytes.Equal(end, ks.ObjectKeyspaceEnd()) {
		return errors.New("source key range does not match keyspace")
	}
	if !sha256RE.MatchString(p.Source.RangeExclusiveReceiptSHA256) || p.Source.ExclusiveSnapshotTS != p.Full.BackupTS || p.Source.OutsideVisibleKeyCount != 0 || p.Source.HistoricalMVCCAbsenceProven || p.Source.PDAddressCount <= 0 {
		return errors.New("invalid source range-exclusive receipt evidence")
	}
	if p.SourceWitness.Format != backupfile.Format || p.SourceWitness.Prefix != "/" || !sha256RE.MatchString(p.SourceWitness.FileSHA256) || !sha256RE.MatchString(p.SourceWitness.ContentSHA256) || p.SourceWitness.Revision <= 0 || p.SourceWitness.CreatedAtUnix <= 0 || p.SourceWitness.Records < 0 || p.SourceWitness.Leases < 0 {
		return errors.New("invalid plan-bound source witness evidence")
	}
	if !sha256RE.MatchString(p.SourceCapture.ReceiptSHA256) || !sha256RE.MatchString(p.SourceCapture.TaskCreateSHA256) || !operationIDRE.MatchString(p.SourceCapture.OperationID) || p.SourceCapture.CaptureTS != p.RestoreTS || p.SourceCapture.FenceSnapshotTS == 0 || p.SourceCapture.FenceSnapshotTS > p.SourceCapture.CaptureTS || !p.SourceCapture.ContinuousWriterExclusion {
		return errors.New("invalid plan-bound source capture evidence")
	}
	if p.Log.StartTS == 0 || p.Log.TaskCommittedAtTS == 0 || p.Full.BackupTS == 0 || p.RestoreTS == 0 || p.Log.GlobalCheckpointTS == 0 {
		return errors.New("all PITR timestamps must be non-zero")
	}
	if p.Log.StartTS > p.Full.BackupTS {
		return errors.New("log task must start no later than the full snapshot")
	}
	if p.Log.StartTS > p.Log.TaskCommittedAtTS || p.Log.TaskCommittedAtTS > p.Full.BackupTS {
		return errors.New("log task metadata must commit before the full snapshot")
	}
	if p.Full.BackupTS > p.RestoreTS {
		return errors.New("restore timestamp precedes full snapshot")
	}
	if p.RestoreTS > p.Log.GlobalCheckpointTS {
		return errors.New("restore timestamp exceeds durable global checkpoint")
	}
	if p.Full.Mode != "br-txn" {
		return errors.New("full snapshot mode must be br-txn")
	}
	encryption := EncryptionIdentity{Method: p.Full.Encryption, KeyID: p.Full.EncryptionKeyID}
	if p.Format == legacyPlanFormat && encryption.Method == "" {
		encryption.Method = CipherMethodPlaintext
	}
	if err := encryption.Validate(); err != nil || (p.Format == legacyPlanFormat && encryption.Method != CipherMethodPlaintext) {
		return errors.New("invalid full snapshot encryption identity")
	}
	if !sha256RE.MatchString(p.Full.BackupMetaSHA256) {
		return errors.New("invalid backupmeta SHA-256")
	}
	if !sha256RE.MatchString(p.Full.ReceiptSHA256) || !sha256RE.MatchString(p.Full.ArtifactReceiptSHA256) || !sha256RE.MatchString(p.Full.ArtifactManifestSHA) || p.Full.ArtifactObjectCount < 2 || p.Full.ArtifactTotalBytes == 0 {
		return errors.New("invalid full snapshot artifact receipt evidence")
	}
	if !sha256RE.MatchString(p.Full.RemoteInventorySHA256) || safeText("full object store ID", p.Full.ObjectStoreID) != nil || !p.Full.RemoteExact || p.Full.MinRetainUntilUnix <= p.Full.InventoryCheckedAt || p.Full.InventoryCheckedAt <= 0 {
		return errors.New("invalid full snapshot remote inventory evidence")
	}
	if !sha256RE.MatchString(p.Target.SnapshotEmptyReceiptSHA256) || p.Target.SnapshotTS == 0 || p.Target.ScanScope != WholeTransactionalKeyspace || p.Target.VisibleCommittedKeyCount != 0 || p.Target.HistoricalMVCCAbsenceProven || p.Target.RawKVAbsenceProven || p.Target.CheckedAtUnix <= 0 || p.Target.PDAddressCount <= 0 || p.Target.UpStoreCount <= 0 {
		return errors.New("invalid target snapshot-empty receipt evidence")
	}
	if err := validateS3Prefix(p.Full.StoragePrefix); err != nil {
		return fmt.Errorf("full snapshot storage: %w", err)
	}
	fullBucket, fullPrefix, err := splitS3Prefix(p.Full.StoragePrefix)
	if err != nil || p.Full.Bucket != fullBucket || p.Full.ObjectPrefix != fullPrefix {
		return errors.New("full remote inventory scope does not match storage prefix")
	}
	if err := safeText("advancer owner", p.Log.AdvancerOwner); err != nil {
		return err
	}
	if err := validateS3Prefix(p.Log.StoragePrefix); err != nil {
		return fmt.Errorf("log storage: %w", err)
	}
	logBucket, logPrefix, err := splitS3Prefix(p.Log.StoragePrefix)
	if err != nil || p.Log.Bucket != logBucket || p.Log.ObjectPrefix != logPrefix || safeText("log object store ID", p.Log.ObjectStoreID) != nil {
		return errors.New("log remote inventory scope does not match storage prefix")
	}
	if !sha256RE.MatchString(p.Log.StorageSHA256) {
		return errors.New("invalid log storage backend SHA-256")
	}
	if !sha256RE.MatchString(p.Log.ReadyReceiptSHA256) || !sha256RE.MatchString(p.Log.ArtifactReceiptSHA) || !sha256RE.MatchString(p.Log.ArtifactManifest) || !sha256RE.MatchString(p.Log.RemoteInventorySHA) || p.Log.ObjectStoreID == "" || p.Log.Bucket == "" || p.Log.ObjectPrefix == "" || !p.Log.RemoteExact || p.Log.MinRetainUntilUnix <= p.Log.InventoryCheckedAt || p.Log.InventoryCheckedAt <= 0 || p.Log.ArtifactObjects < 0 || p.Log.ArtifactSegments < 0 || p.Log.ArtifactMetadata < 0 || p.Log.ArtifactData < 0 || p.Log.ArtifactControl < 0 || p.Log.ArtifactObjects != p.Log.ArtifactMetadata+p.Log.ArtifactData+p.Log.ArtifactControl || (p.Log.ArtifactMetadata == 0) != (p.Log.ArtifactData == 0) || (p.Log.ArtifactData == 0) != (p.Log.ArtifactSegments == 0) || (p.Log.ArtifactMetadata > 0 && p.Log.MetadataResolvedTS == 0) || (p.Log.ArtifactObjects == 0) != (p.Log.ArtifactTotalBytes == 0) {
		return errors.New("invalid log artifact receipt evidence")
	}
	if !p.ReadOnly {
		return errors.New("restore plan must be marked read-only")
	}
	return nil
}

func validatePreflight(p Preflight) error {
	if p.Format != PreflightFormat || !p.ReadOnly {
		return errors.New("source is not a read-only native PITR preflight v1 receipt")
	}
	if p.ClusterID == 0 || !p.TaskAvailable || p.TaskCount != 0 || len(p.Stores) == 0 {
		return errors.New("source preflight did not prove an available task and TiKV stores")
	}
	if !dnsLabel.MatchString(p.TaskName) {
		return errors.New("source preflight has invalid task name")
	}
	ks, err := coder.NewKeyspace(p.Keyspace)
	if err != nil {
		return fmt.Errorf("source preflight keyspace: %w", err)
	}
	wantStart, wantEnd := hex.EncodeToString(ks.ObjectKeyspaceStart()), hex.EncodeToString(ks.ObjectKeyspaceEnd())
	if p.StartKeyHex != wantStart || p.EndKeyHex != wantEnd {
		return errors.New("source preflight range does not match its KubeBrain keyspace")
	}
	wantInfo := "/tidb/br-stream/info/" + p.TaskName
	wantRanges := "/tidb/br-stream/ranges/" + p.TaskName + "/"
	if p.TaskInfoKey != wantInfo || p.TaskRangesKey != wantRanges {
		return errors.New("source preflight task metadata paths do not match task name")
	}
	wantOwnership := []string{wantInfo, wantRanges, "/tidb/br-stream/checkpoint/" + p.TaskName + "/", "/tidb/br-stream/storage-checkpoint/" + p.TaskName + "/", "/tidb/br-stream/pause/" + p.TaskName, "/tidb/br-stream/last-error/" + p.TaskName + "/"}
	if len(p.OwnershipKeys) != len(wantOwnership) {
		return errors.New("source preflight did not check every task ownership path")
	}
	for i := range wantOwnership {
		if p.OwnershipKeys[i] != wantOwnership[i] {
			return errors.New("source preflight ownership paths do not match task name")
		}
	}
	if len(p.PDAddrs) == 0 {
		return errors.New("source preflight contains no PD addresses")
	}
	seenPD := map[string]bool{}
	for _, addr := range p.PDAddrs {
		if addr == "" || seenPD[addr] {
			return errors.New("source preflight contains invalid PD addresses")
		}
		seenPD[addr] = true
	}
	seenStore := map[uint64]bool{}
	for _, s := range p.Stores {
		if s.ID == 0 || seenStore[s.ID] || s.Address == "" || s.Service != "available" {
			return errors.New("source preflight contains an unproven TiKV log-backup service")
		}
		seenStore[s.ID] = true
	}
	start, e1 := hex.DecodeString(p.StartKeyHex)
	end, e2 := hex.DecodeString(p.EndKeyHex)
	if e1 != nil || e2 != nil || len(start) == 0 || bytes.Compare(start, end) >= 0 {
		return errors.New("source preflight contains an invalid key range")
	}
	return nil
}

func safeText(name, value string) error {
	if value == "" || strings.TrimSpace(value) != value || strings.ContainsAny(value, "\x00\r\n\t") {
		return fmt.Errorf("invalid %s", name)
	}
	return nil
}

func requireEOF(dec *json.Decoder) error {
	var extra any
	if err := dec.Decode(&extra); err == io.EOF {
		return nil
	}
	return errors.New("source preflight contains trailing JSON")
}
