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

const FullSnapshotFormat = "kubebrain.native-pitr-full-snapshot.v1"

type FullSnapshotReceipt struct {
	Format                 string `json:"format"`
	ClusterID              uint64 `json:"cluster_id"`
	Keyspace               string `json:"keyspace"`
	TaskName               string `json:"task_name"`
	TaskStartTS            uint64 `json:"task_start_ts"`
	TaskEndTS              uint64 `json:"task_end_ts"`
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
	var meta backuppb.BackupMeta
	if err := meta.Unmarshal(backupMetaBytes); err != nil {
		return FullSnapshotReceipt{}, fmt.Errorf("decode backupmeta protobuf: %w", err)
	}
	if meta.ClusterId == 0 || meta.ClusterId != task.ClusterID {
		return FullSnapshotReceipt{}, errors.New("backupmeta cluster ID does not match task-create receipt")
	}
	if !meta.IsTxnKv || meta.IsRawKv {
		return FullSnapshotReceipt{}, errors.New("backupmeta is not a transactional KV backup")
	}
	if meta.StartVersion == 0 || meta.StartVersion != meta.EndVersion {
		return FullSnapshotReceipt{}, errors.New("backupmeta is not a full point-in-time snapshot")
	}
	if meta.EndVersion < task.StartTS || meta.EndVersion >= task.EndTS {
		return FullSnapshotReceipt{}, errors.New("full snapshot timestamp is outside the log task interval")
	}
	hasFileIndex := meta.FileIndex != nil && (len(meta.FileIndex.DataFiles) > 0 || len(meta.FileIndex.MetaFiles) > 0)
	if !hasFileIndex && len(meta.Files) == 0 {
		return FullSnapshotReceipt{}, errors.New("backupmeta contains no data-file inventory")
	}
	digest := sha256.Sum256(backupMetaBytes)
	return FullSnapshotReceipt{
		Format: FullSnapshotFormat, ClusterID: task.ClusterID, Keyspace: task.Keyspace, TaskName: task.TaskName,
		TaskStartTS: task.StartTS, TaskEndTS: task.EndTS, BackupTS: meta.EndVersion,
		StartKeyHex: task.StartKeyHex, EndKeyHex: task.EndKeyHex, StoragePrefix: storagePrefix,
		BackupMetaSHA256: hex.EncodeToString(digest[:]), BackupMetaBytes: int64(len(backupMetaBytes)), TaskCreateSHA256: taskCreateSHA256,
		BRVersion: meta.BrVersion, ClusterVersion: meta.ClusterVersion, BackupMetaVersion: meta.Version,
		HasFileIndex: hasFileIndex, LegacyFileCount: len(meta.Files), Transactional: true,
		Scope: "whole-cluster", BackupMetaInventory: true, ObjectExistenceChecked: false,
	}, nil
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
		return errors.New("input is not a successful native PITR full-snapshot v1 receipt")
	}
	if !dnsLabel.MatchString(r.TaskName) || r.Keyspace == "" || r.TaskStartTS == 0 || r.BackupTS < r.TaskStartTS || r.BackupTS >= r.TaskEndTS {
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
