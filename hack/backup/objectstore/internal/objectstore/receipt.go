package objectstore

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/kubewharf/kubebrain/hack/production/operationaudit"
)

const ReceiptFormat = "kubebrain.object-backup.receipt.v1"
const DeletionReceiptFormat = "kubebrain.object-backup-deletion.receipt.v1"
const AuditReceiptFormat = "kubebrain.object-operation-audit.receipt.v1"
const BlobReceiptFormat = "kubebrain.object-immutable-blob.receipt.v1"
const BlobReadReceiptFormat = "kubebrain.object-immutable-blob-read.receipt.v1"
const maxObjectStoreJSONBytes = 1 << 20

type Receipt struct {
	Format             string `json:"format"`
	Instance           string `json:"instance"`
	BackupID           string `json:"backup_id"`
	ObjectStoreID      string `json:"object_store_id"`
	Bucket             string `json:"bucket"`
	ObjectKey          string `json:"object_key"`
	VersionID          string `json:"version_id"`
	ArtifactFileSHA256 string `json:"artifact_file_sha256,omitempty"`
	ArtifactFormat     string `json:"artifact_format"`
	ArtifactSHA256     string `json:"artifact_sha256"`
	SnapshotRevision   int64  `json:"snapshot_revision"`
	CreatedAtUnix      int64  `json:"created_at_unix"`
	Records            int    `json:"records"`
	Leases             int    `json:"leases"`
	ObjectBytes        int64  `json:"object_bytes"`
	RetentionMode      string `json:"retention_mode"`
	RetainUntilUnix    int64  `json:"retain_until_unix"`
	RemoteVerified     bool   `json:"remote_verified"`
	UploadedAtUnix     int64  `json:"uploaded_at_unix"`
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

type BlobReceipt struct {
	Format          string `json:"format"`
	ArtifactFormat  string `json:"artifact_format"`
	ArtifactID      string `json:"artifact_id"`
	Instance        string `json:"instance"`
	ObjectStoreID   string `json:"object_store_id"`
	Bucket          string `json:"bucket"`
	ObjectKey       string `json:"object_key"`
	VersionID       string `json:"version_id"`
	ArtifactSHA256  string `json:"artifact_sha256"`
	ObjectBytes     int64  `json:"object_bytes"`
	RetentionMode   string `json:"retention_mode"`
	RetainUntilUnix int64  `json:"retain_until_unix"`
	RemoteVerified  bool   `json:"remote_verified"`
	ArchivedAtUnix  int64  `json:"archived_at_unix"`
}

type BlobReadReceipt struct {
	Format          string `json:"format"`
	ArtifactFormat  string `json:"artifact_format"`
	ArtifactID      string `json:"artifact_id"`
	Instance        string `json:"instance"`
	ObjectStoreID   string `json:"object_store_id"`
	Bucket          string `json:"bucket"`
	ObjectKey       string `json:"object_key"`
	VersionID       string `json:"version_id"`
	ArtifactSHA256  string `json:"artifact_sha256"`
	ObjectBytes     int64  `json:"object_bytes"`
	RetentionMode   string `json:"retention_mode"`
	RetainUntilUnix int64  `json:"retain_until_unix"`
	RemoteVerified  bool   `json:"remote_verified"`
}

func (r Receipt) Validate() error {
	if r.Format != ReceiptFormat || r.Instance == "" || r.BackupID == "" || r.ObjectStoreID == "" ||
		r.Bucket == "" || r.ObjectKey == "" || r.VersionID == "" ||
		(r.ArtifactFileSHA256 != "" && !validHexSHA256(r.ArtifactFileSHA256)) ||
		r.ArtifactFormat == "" || !validHexSHA256(r.ArtifactSHA256) || r.SnapshotRevision <= 0 ||
		r.CreatedAtUnix <= 0 || r.Records < 0 || r.Leases < 0 || r.ObjectBytes <= 0 ||
		(r.RetentionMode != "COMPLIANCE" && r.RetentionMode != "GOVERNANCE") ||
		r.RetainUntilUnix <= r.UploadedAtUnix || !r.RemoteVerified || r.UploadedAtUnix <= 0 {
		return errors.New("object backup receipt is incomplete")
	}
	return nil
}

func ReadReceipt(path string) (Receipt, error) {
	var receipt Receipt
	data, err := readBoundedObjectStoreJSONFile(path, "object backup receipt")
	if err != nil {
		return receipt, err
	}
	if err := decodeCanonicalReceipt(data, &receipt, "object backup receipt"); err != nil {
		return receipt, err
	}
	if err := receipt.Validate(); err != nil {
		return receipt, err
	}
	return receipt, nil
}

func (r DeletionReceipt) Validate() error {
	if r.Format != DeletionReceiptFormat || r.Instance == "" || r.BackupID == "" || r.ObjectStoreID == "" ||
		r.Bucket == "" || r.ObjectKey == "" || r.VersionID == "" || !validHexSHA256(r.ArtifactSHA256) ||
		(r.RetentionMode != "COMPLIANCE" && r.RetentionMode != "GOVERNANCE") ||
		r.RetainUntilUnix <= 0 || !r.VersionAbsent || r.DeletedAtUnix != r.RetainUntilUnix {
		return errors.New("object backup deletion receipt is incomplete")
	}
	return nil
}

func ReadDeletionReceipt(path string) (DeletionReceipt, error) {
	var receipt DeletionReceipt
	data, err := readBoundedObjectStoreJSONFile(path, "object backup deletion receipt")
	if err != nil {
		return receipt, err
	}
	if err := decodeCanonicalReceipt(data, &receipt, "object backup deletion receipt"); err != nil {
		return receipt, err
	}
	if err := receipt.Validate(); err != nil {
		return receipt, err
	}
	return receipt, nil
}

func (r AuditReceipt) Validate() error {
	return operationaudit.ArchiveReceipt{
		Format: r.Format, OperationID: r.OperationID, OperationUID: r.OperationUID,
		Instance: r.Instance, OperationType: r.OperationType, Phase: r.Phase,
		ExecutionReceiptSHA256: r.ExecutionReceiptSHA256, ObjectStoreID: r.ObjectStoreID,
		Bucket: r.Bucket, ObjectKey: r.ObjectKey, VersionID: r.VersionID,
		ArtifactSHA256: r.ArtifactSHA256, ObjectBytes: r.ObjectBytes,
		RetentionMode: r.RetentionMode, RetainUntilUnix: r.RetainUntilUnix,
		RemoteVerified: r.RemoteVerified, ArchivedAtUnix: r.ArchivedAtUnix,
	}.Validate()
}

func validHexSHA256(value string) bool {
	if len(value) != 64 {
		return false
	}
	for i := 0; i < len(value); i++ {
		switch {
		case value[i] >= '0' && value[i] <= '9':
		case value[i] >= 'a' && value[i] <= 'f':
		default:
			return false
		}
	}
	return true
}

func ReadAuditReceipt(path string) (AuditReceipt, error) {
	var receipt AuditReceipt
	data, err := readBoundedObjectStoreJSONFile(path, "object operation audit receipt")
	if err != nil {
		return receipt, err
	}
	if err := decodeCanonicalReceipt(data, &receipt, "object operation audit receipt"); err != nil {
		return receipt, err
	}
	if err := receipt.Validate(); err != nil {
		return receipt, err
	}
	return receipt, nil
}

func (r BlobReceipt) Validate() error {
	if r.Format != BlobReceiptFormat || r.ArtifactFormat == "" || r.ArtifactID == "" ||
		r.Instance == "" || r.ObjectStoreID == "" || r.Bucket == "" || r.ObjectKey == "" ||
		r.VersionID == "" || !validHexSHA256(r.ArtifactSHA256) || r.ObjectBytes <= 0 ||
		r.ObjectBytes > maxImmutableBlobBytes ||
		(r.RetentionMode != "COMPLIANCE" && r.RetentionMode != "GOVERNANCE") ||
		r.RetainUntilUnix <= r.ArchivedAtUnix || !r.RemoteVerified || r.ArchivedAtUnix <= 0 {
		return errors.New("object immutable blob receipt is incomplete")
	}
	return nil
}

func ReadBlobReceipt(path string) (BlobReceipt, error) {
	var receipt BlobReceipt
	data, err := readBoundedObjectStoreJSONFile(path, "object immutable blob receipt")
	if err != nil {
		return receipt, err
	}
	if err := decodeCanonicalReceipt(data, &receipt, "object immutable blob receipt"); err != nil {
		return receipt, err
	}
	if err := receipt.Validate(); err != nil {
		return receipt, err
	}
	return receipt, nil
}

func (r BlobReadReceipt) Validate() error {
	if r.Format != BlobReadReceiptFormat || r.ArtifactFormat == "" || r.ArtifactID == "" ||
		r.Instance == "" || r.ObjectStoreID == "" || r.Bucket == "" || r.ObjectKey == "" ||
		r.VersionID == "" || !validHexSHA256(r.ArtifactSHA256) || r.ObjectBytes <= 0 ||
		r.ObjectBytes > maxImmutableBlobBytes ||
		(r.RetentionMode != "COMPLIANCE" && r.RetentionMode != "GOVERNANCE") ||
		r.RetainUntilUnix <= 0 || !r.RemoteVerified {
		return errors.New("object immutable blob read receipt is incomplete")
	}
	return nil
}

func decodeCanonicalReceipt(data []byte, destination any, description string) error {
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(destination); err != nil {
		return fmt.Errorf("decode %s: %w", description, err)
	}
	var trailing any
	if err := decoder.Decode(&trailing); err != io.EOF {
		return fmt.Errorf("%s contains trailing JSON", description)
	}
	canonical, err := json.Marshal(destination)
	if err != nil {
		return err
	}
	canonical = append(canonical, '\n')
	if !bytes.Equal(data, canonical) {
		return fmt.Errorf("%s is not canonical", description)
	}
	return nil
}

func readBoundedObjectStoreJSONFile(path, description string) ([]byte, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	data, err := io.ReadAll(io.LimitReader(file, maxObjectStoreJSONBytes+1))
	if err != nil {
		return nil, err
	}
	if len(data) > maxObjectStoreJSONBytes {
		return nil, fmt.Errorf("%s exceeds %d bytes", description, maxObjectStoreJSONBytes)
	}
	return data, nil
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
	if err := validateObjectStoreJSONSize("object backup receipt", data); err != nil {
		return err
	}
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
	return writeJSONAtomic(path, receipt, "object backup deletion receipt", func(path string) (any, error) {
		return ReadDeletionReceipt(path)
	})
}

func WriteAuditReceiptAtomic(path string, receipt AuditReceipt) error {
	if err := receipt.Validate(); err != nil {
		return err
	}
	return writeJSONAtomic(path, receipt, "object operation audit receipt", func(path string) (any, error) {
		return ReadAuditReceipt(path)
	})
}

func WriteBlobReceiptAtomic(path string, receipt BlobReceipt) error {
	if err := receipt.Validate(); err != nil {
		return err
	}
	return writeJSONAtomic(path, receipt, "object immutable blob receipt", func(path string) (any, error) {
		return ReadBlobReceipt(path)
	})
}

func writeJSONAtomic(path string, value any, description string, readExisting func(string) (any, error)) error {
	if path == "" {
		return errors.New("receipt output path is empty")
	}
	data, err := json.Marshal(value)
	if err != nil {
		return err
	}
	data = append(data, '\n')
	if err := validateObjectStoreJSONSize(description, data); err != nil {
		return err
	}
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

func validateObjectStoreJSONSize(description string, data []byte) error {
	if len(data) > maxObjectStoreJSONBytes {
		return fmt.Errorf("%s exceeds %d bytes", description, maxObjectStoreJSONBytes)
	}
	return nil
}
