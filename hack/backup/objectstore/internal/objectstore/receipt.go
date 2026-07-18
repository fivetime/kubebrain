package objectstore

import (
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
)

const ReceiptFormat = "kubebrain.object-backup.receipt.v1"
const DeletionReceiptFormat = "kubebrain.object-backup-deletion.receipt.v1"
const AuditReceiptFormat = "kubebrain.object-operation-audit.receipt.v1"

type Receipt struct {
	Format           string `json:"format"`
	Instance         string `json:"instance"`
	BackupID         string `json:"backup_id"`
	ObjectStoreID    string `json:"object_store_id"`
	Bucket           string `json:"bucket"`
	ObjectKey        string `json:"object_key"`
	VersionID        string `json:"version_id"`
	ArtifactFormat   string `json:"artifact_format"`
	ArtifactSHA256   string `json:"artifact_sha256"`
	SnapshotRevision int64  `json:"snapshot_revision"`
	CreatedAtUnix    int64  `json:"created_at_unix"`
	Records          int    `json:"records"`
	Leases           int    `json:"leases"`
	ObjectBytes      int64  `json:"object_bytes"`
	RetentionMode    string `json:"retention_mode"`
	RetainUntilUnix  int64  `json:"retain_until_unix"`
	RemoteVerified   bool   `json:"remote_verified"`
	UploadedAtUnix   int64  `json:"uploaded_at_unix"`
}

type DeletionReceipt struct {
	Format          string `json:"format"`
	Instance        string `json:"instance"`
	BackupID        string `json:"backup_id"`
	ObjectStoreID   string `json:"object_store_id"`
	Bucket          string `json:"bucket"`
	ObjectKey       string `json:"object_key"`
	VersionID       string `json:"version_id"`
	ArtifactSHA256  string `json:"artifact_sha256"`
	RetentionMode   string `json:"retention_mode"`
	RetainUntilUnix int64  `json:"retain_until_unix"`
	VersionAbsent   bool   `json:"version_absent"`
	DeletedAtUnix   int64  `json:"deleted_at_unix"`
}

type AuditReceipt struct {
	Format                 string `json:"format"`
	OperationID            string `json:"operation_id"`
	OperationUID           string `json:"operation_uid"`
	Instance               string `json:"instance"`
	OperationType          string `json:"operation_type"`
	Phase                  string `json:"phase"`
	ExecutionReceiptSHA256 string `json:"execution_receipt_sha256,omitempty"`
	ObjectStoreID          string `json:"object_store_id"`
	Bucket                 string `json:"bucket"`
	ObjectKey              string `json:"object_key"`
	VersionID              string `json:"version_id"`
	ArtifactSHA256         string `json:"artifact_sha256"`
	ObjectBytes            int64  `json:"object_bytes"`
	RetentionMode          string `json:"retention_mode"`
	RetainUntilUnix        int64  `json:"retain_until_unix"`
	RemoteVerified         bool   `json:"remote_verified"`
	ArchivedAtUnix         int64  `json:"archived_at_unix"`
}

func (r Receipt) Validate() error {
	if r.Format != ReceiptFormat || r.Instance == "" || r.BackupID == "" || r.ObjectStoreID == "" ||
		r.Bucket == "" || r.ObjectKey == "" || r.VersionID == "" ||
		r.ArtifactFormat == "" || r.ArtifactSHA256 == "" || r.SnapshotRevision <= 0 ||
		r.CreatedAtUnix <= 0 || r.Records < 0 || r.Leases < 0 || r.ObjectBytes <= 0 ||
		(r.RetentionMode != "COMPLIANCE" && r.RetentionMode != "GOVERNANCE") ||
		r.RetainUntilUnix <= r.UploadedAtUnix || !r.RemoteVerified || r.UploadedAtUnix <= 0 {
		return errors.New("object backup receipt is incomplete")
	}
	return nil
}

func ReadReceipt(path string) (Receipt, error) {
	var receipt Receipt
	data, err := os.ReadFile(path)
	if err != nil {
		return receipt, err
	}
	if err := json.Unmarshal(data, &receipt); err != nil {
		return receipt, fmt.Errorf("decode receipt: %w", err)
	}
	if err := receipt.Validate(); err != nil {
		return receipt, err
	}
	return receipt, nil
}

func (r DeletionReceipt) Validate() error {
	if r.Format != DeletionReceiptFormat || r.Instance == "" || r.BackupID == "" || r.ObjectStoreID == "" ||
		r.Bucket == "" || r.ObjectKey == "" || r.VersionID == "" || r.ArtifactSHA256 == "" ||
		(r.RetentionMode != "COMPLIANCE" && r.RetentionMode != "GOVERNANCE") ||
		r.RetainUntilUnix <= 0 || !r.VersionAbsent || r.DeletedAtUnix < r.RetainUntilUnix {
		return errors.New("object backup deletion receipt is incomplete")
	}
	return nil
}

func ReadDeletionReceipt(path string) (DeletionReceipt, error) {
	var receipt DeletionReceipt
	data, err := os.ReadFile(path)
	if err != nil {
		return receipt, err
	}
	if err := json.Unmarshal(data, &receipt); err != nil {
		return receipt, fmt.Errorf("decode deletion receipt: %w", err)
	}
	if err := receipt.Validate(); err != nil {
		return receipt, err
	}
	return receipt, nil
}

func (r AuditReceipt) Validate() error {
	if r.Format != AuditReceiptFormat || r.OperationID == "" || r.OperationUID == "" ||
		r.Instance == "" || r.OperationType == "" ||
		(r.Phase != "Succeeded" && r.Phase != "Failed") ||
		(r.Phase == "Succeeded" && !validHexSHA256(r.ExecutionReceiptSHA256)) ||
		(r.Phase == "Failed" && r.ExecutionReceiptSHA256 != "") ||
		r.ObjectStoreID == "" || r.Bucket == "" || r.ObjectKey == "" || r.VersionID == "" ||
		!validHexSHA256(r.ArtifactSHA256) || r.ObjectBytes <= 0 ||
		(r.RetentionMode != "COMPLIANCE" && r.RetentionMode != "GOVERNANCE") ||
		r.RetainUntilUnix <= r.ArchivedAtUnix || !r.RemoteVerified || r.ArchivedAtUnix <= 0 {
		return errors.New("object operation audit receipt is incomplete")
	}
	return nil
}

func validHexSHA256(value string) bool {
	if len(value) != 64 {
		return false
	}
	_, err := hex.DecodeString(value)
	return err == nil
}

func ReadAuditReceipt(path string) (AuditReceipt, error) {
	var receipt AuditReceipt
	data, err := os.ReadFile(path)
	if err != nil {
		return receipt, err
	}
	if err := json.Unmarshal(data, &receipt); err != nil {
		return receipt, fmt.Errorf("decode operation audit receipt: %w", err)
	}
	if err := receipt.Validate(); err != nil {
		return receipt, err
	}
	return receipt, nil
}

func WriteReceiptAtomic(path string, receipt Receipt) error {
	if path == "" {
		return errors.New("receipt output path is empty")
	}
	if err := receipt.Validate(); err != nil {
		return err
	}
	data, err := json.Marshal(receipt)
	if err != nil {
		return err
	}
	data = append(data, '\n')
	dir := filepath.Dir(path)
	temp, err := os.CreateTemp(dir, "."+filepath.Base(path)+".tmp-*")
	if err != nil {
		return err
	}
	tempName := temp.Name()
	defer os.Remove(tempName)
	if err := temp.Chmod(0o600); err != nil {
		temp.Close()
		return err
	}
	if _, err := temp.Write(data); err != nil {
		temp.Close()
		return err
	}
	if err := temp.Sync(); err != nil {
		temp.Close()
		return err
	}
	if err := temp.Close(); err != nil {
		return err
	}
	if err := os.Link(tempName, path); err != nil {
		if errors.Is(err, os.ErrExist) {
			existing, readErr := ReadReceipt(path)
			if readErr == nil && existing == receipt {
				return nil
			}
			return fmt.Errorf("refusing to overwrite existing receipt %q", path)
		}
		return err
	}
	directory, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer directory.Close()
	return directory.Sync()
}

func WriteDeletionReceiptAtomic(path string, receipt DeletionReceipt) error {
	if err := receipt.Validate(); err != nil {
		return err
	}
	return writeJSONAtomic(path, receipt, func(path string) (any, error) {
		return ReadDeletionReceipt(path)
	})
}

func WriteAuditReceiptAtomic(path string, receipt AuditReceipt) error {
	if err := receipt.Validate(); err != nil {
		return err
	}
	return writeJSONAtomic(path, receipt, func(path string) (any, error) {
		return ReadAuditReceipt(path)
	})
}

func writeJSONAtomic(path string, value any, readExisting func(string) (any, error)) error {
	if path == "" {
		return errors.New("receipt output path is empty")
	}
	data, err := json.Marshal(value)
	if err != nil {
		return err
	}
	data = append(data, '\n')
	dir := filepath.Dir(path)
	temp, err := os.CreateTemp(dir, "."+filepath.Base(path)+".tmp-*")
	if err != nil {
		return err
	}
	tempName := temp.Name()
	defer os.Remove(tempName)
	if err := temp.Chmod(0o600); err != nil {
		temp.Close()
		return err
	}
	if _, err := temp.Write(data); err != nil {
		temp.Close()
		return err
	}
	if err := temp.Sync(); err != nil {
		temp.Close()
		return err
	}
	if err := temp.Close(); err != nil {
		return err
	}
	if err := os.Link(tempName, path); err != nil {
		if errors.Is(err, os.ErrExist) {
			existing, readErr := readExisting(path)
			if readErr == nil {
				existingData, _ := json.Marshal(existing)
				valueData, _ := json.Marshal(value)
				if string(existingData) == string(valueData) {
					return nil
				}
			}
			return fmt.Errorf("refusing to overwrite existing receipt %q", path)
		}
		return err
	}
	directory, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer directory.Close()
	return directory.Sync()
}
