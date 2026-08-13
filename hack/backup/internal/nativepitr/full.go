package nativepitr

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/url"
	"strings"

	"github.com/kubewharf/kubebrain/pkg/backend/coder"
	backuppb "github.com/pingcap/kvproto/pkg/brpb"
)

const FullSnapshotFormat = "kubebrain.native-pitr-full-snapshot.v3"

type FullSnapshotReceipt struct {
	Format                 string `json:"format"`
	ClusterID              uint64 `json:"cluster_id"`
	Keyspace               string `json:"keyspace"`
	TaskName               string `json:"task_name"`
	TaskStartTS            uint64 `json:"task_start_ts"`
	TaskCommittedAtTS      uint64 `json:"task_committed_at_ts"`
	TaskEndTS              uint64 `json:"task_end_ts"`
	BackupStartTS          uint64 `json:"backup_start_ts"`
	BackupTS               uint64 `json:"backup_ts"`
	StartKeyHex            string `json:"start_key_hex"`
	EndKeyHex              string `json:"end_key_hex"`
	StoragePrefix          string `json:"storage_prefix"`
	BackupMetaSHA256       string `json:"backupmeta_sha256"`
	BackupMetaBytes        int64  `json:"backupmeta_bytes"`
	TaskCreateSHA256       string `json:"task_create_sha256"`
	BRVersion              string `json:"br_version"`
	ClusterVersion         string `json:"cluster_version"`
	BackupMetaVersion      int32  `json:"backupmeta_version"`
	HasFileIndex           bool   `json:"has_file_index"`
	LegacyFileCount        int    `json:"legacy_file_count"`
	Transactional          bool   `json:"transactional"`
	Scope                  string `json:"scope"`
	BackupMetaInventory    bool   `json:"backupmeta_inventory_present"`
	ObjectExistenceChecked bool   `json:"object_existence_checked"`
}

func BuildFullSnapshot(task TaskCreateReceipt, taskCreateSHA256, storagePrefix string, backupMetaBytes []byte) (FullSnapshotReceipt, error) {
	return BuildFullSnapshotWithEncryption(task, taskCreateSHA256, storagePrefix, backupMetaBytes, EncryptionIdentity{Method: CipherMethodPlaintext}, nil)
}

// BuildFullSnapshotWithEncryption validates metadata after decrypting it in
// memory, while binding the receipt digest and byte count to the immutable
// encrypted object exactly as stored.
func BuildFullSnapshotWithEncryption(task TaskCreateReceipt, taskCreateSHA256, storagePrefix string, backupMetaBytes []byte, encryption EncryptionIdentity, encryptionKey []byte) (FullSnapshotReceipt, error) {
	if err := validateTaskCreateReceipt(task); err != nil {
		return FullSnapshotReceipt{}, err
	}
	if !sha256RE.MatchString(taskCreateSHA256) {
		return FullSnapshotReceipt{}, errors.New("invalid task-create receipt SHA-256")
	}
	if err := validateS3Prefix(storagePrefix); err != nil {
		return FullSnapshotReceipt{}, err
	}
	if len(backupMetaBytes) == 0 {
		return FullSnapshotReceipt{}, errors.New("backupmeta is empty")
	}
	plaintextMeta, err := decryptBRContent(backupMetaBytes, encryption, encryptionKey, nil, encryption.Method != CipherMethodPlaintext)
	if err != nil {
		return FullSnapshotReceipt{}, fmt.Errorf("decrypt backupmeta: %w", err)
	}
	var meta backuppb.BackupMeta
	if err := meta.Unmarshal(plaintextMeta); err != nil {
		return FullSnapshotReceipt{}, fmt.Errorf("decode backupmeta protobuf: %w", err)
	}
	if meta.ClusterId == 0 || meta.ClusterId != task.ClusterID {
		return FullSnapshotReceipt{}, errors.New("backupmeta cluster ID does not match task-create receipt")
	}
	if !meta.IsTxnKv || meta.IsRawKv {
		return FullSnapshotReceipt{}, errors.New("backupmeta is not a transactional KV backup")
	}
	if meta.StartVersion != 0 || meta.EndVersion == 0 {
		return FullSnapshotReceipt{}, errors.New("backupmeta is not a full point-in-time snapshot")
	}
	// BR v7.5.1 backup txn materializes empty schema/raw/DDL MetaFile
	// messages even though a transactional KV backup has no such inventory.
	// Reject inventory content, not the protobuf presence bit.
	if metaFileHasContent(meta.SchemaIndex) || metaFileHasContent(meta.RawRangeIndex) || metaFileHasContent(meta.DdlIndexes) || len(meta.Schemas) != 0 || len(meta.RawRanges) != 0 || ddlInventoryHasContent(meta.Ddls) {
		return FullSnapshotReceipt{}, fmt.Errorf("transactional KV backupmeta contains unexpected schema, raw-range, or DDL inventory: schema=%d raw=%d ddl=%d schema_index=%s raw_index=%s ddl_index=%s", len(meta.Schemas), len(meta.RawRanges), len(meta.Ddls), metaFileInventory(meta.SchemaIndex), metaFileInventory(meta.RawRangeIndex), metaFileInventory(meta.DdlIndexes))
	}
	if meta.EndVersion < task.CommittedAtTS {
		return FullSnapshotReceipt{}, errors.New("full snapshot precedes task metadata commit")
	}
	if meta.EndVersion >= task.EndTS {
		return FullSnapshotReceipt{}, errors.New("full snapshot timestamp is outside the log task interval")
	}
	hasFileIndex := meta.FileIndex != nil && (len(meta.FileIndex.DataFiles) > 0 || len(meta.FileIndex.MetaFiles) > 0)
	if !hasFileIndex && len(meta.Files) == 0 {
		return FullSnapshotReceipt{}, errors.New("backupmeta contains no data-file inventory")
	}
	digest := sha256.Sum256(backupMetaBytes)
	return FullSnapshotReceipt{
		Format: FullSnapshotFormat, ClusterID: task.ClusterID, Keyspace: task.Keyspace, TaskName: task.TaskName,
		TaskStartTS: task.StartTS, TaskCommittedAtTS: task.CommittedAtTS, TaskEndTS: task.EndTS, BackupStartTS: meta.StartVersion, BackupTS: meta.EndVersion,
		StartKeyHex: task.StartKeyHex, EndKeyHex: task.EndKeyHex, StoragePrefix: storagePrefix,
		BackupMetaSHA256: hex.EncodeToString(digest[:]), BackupMetaBytes: int64(len(backupMetaBytes)), TaskCreateSHA256: taskCreateSHA256,
		BRVersion: meta.BrVersion, ClusterVersion: meta.ClusterVersion, BackupMetaVersion: meta.Version,
		HasFileIndex: hasFileIndex, LegacyFileCount: len(meta.Files), Transactional: true,
		Scope: "whole-cluster", BackupMetaInventory: true, ObjectExistenceChecked: false,
	}, nil
}

func ddlInventoryHasContent(ddls []byte) bool {
	var entries []json.RawMessage
	return len(ddls) != 0 && (json.Unmarshal(ddls, &entries) != nil || len(entries) != 0)
}

func metaFileHasContent(index *backuppb.MetaFile) bool {
	return index != nil && (len(index.MetaFiles) != 0 || len(index.DataFiles) != 0 || len(index.Schemas) != 0 || len(index.RawRanges) != 0 || len(index.Ddls) != 0)
}

func metaFileInventory(index *backuppb.MetaFile) string {
	if index == nil {
		return "nil"
	}
	return fmt.Sprintf("meta:%d,data:%d,schema:%d,raw:%d,ddl:%d", len(index.MetaFiles), len(index.DataFiles), len(index.Schemas), len(index.RawRanges), len(index.Ddls))
}

func DecodeFullSnapshot(r io.Reader) (FullSnapshotReceipt, error) {
	var receipt FullSnapshotReceipt
	decoder := json.NewDecoder(r)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&receipt); err != nil {
		return receipt, fmt.Errorf("decode full-snapshot receipt: %w", err)
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return receipt, errors.New("full-snapshot receipt has trailing JSON")
	}
	if err := validateFullSnapshotReceipt(receipt); err != nil {
		return receipt, err
	}
	return receipt, nil
}

func validateFullSnapshotReceipt(r FullSnapshotReceipt) error {
	if r.Format != FullSnapshotFormat || r.ClusterID == 0 || !r.Transactional || r.Scope != "whole-cluster" || !r.BackupMetaInventory || r.ObjectExistenceChecked {
		return errors.New("input is not a successful native PITR full-snapshot v3 receipt")
	}
	if !dnsLabel.MatchString(r.TaskName) || r.Keyspace == "" || r.TaskStartTS == 0 || r.TaskCommittedAtTS < r.TaskStartTS || r.BackupStartTS != 0 || r.BackupTS < r.TaskCommittedAtTS || r.BackupTS >= r.TaskEndTS {
		return errors.New("full-snapshot receipt has invalid identity or timestamp chain")
	}
	ks, err := coder.NewKeyspace(r.Keyspace)
	if err != nil || r.StartKeyHex != hex.EncodeToString(ks.ObjectKeyspaceStart()) || r.EndKeyHex != hex.EncodeToString(ks.ObjectKeyspaceEnd()) {
		return errors.New("full-snapshot receipt range does not match its keyspace")
	}
	if !sha256RE.MatchString(r.BackupMetaSHA256) || !sha256RE.MatchString(r.TaskCreateSHA256) || r.BackupMetaBytes <= 0 {
		return errors.New("full-snapshot receipt has invalid artifact evidence")
	}
	if !r.HasFileIndex && r.LegacyFileCount <= 0 {
		return errors.New("full-snapshot receipt has no data-file inventory")
	}
	if err := validateS3Prefix(r.StoragePrefix); err != nil {
		return err
	}
	return nil
}

func validateS3Prefix(value string) error {
	if err := safeText("storage prefix", value); err != nil {
		return err
	}
	u, err := url.Parse(value)
	if err != nil || u.Scheme != "s3" || u.Host == "" || strings.Trim(u.Path, "/") == "" || u.User != nil || u.RawQuery != "" || u.Fragment != "" {
		return errors.New("storage prefix must be an immutable s3://bucket/non-empty-prefix URI without credentials, query, or fragment")
	}
	return nil
}
