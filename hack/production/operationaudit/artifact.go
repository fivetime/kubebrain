package operationaudit

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
)

const Format = "kubebrain.operation-audit.v1"
const ArchiveReceiptFormat = "kubebrain.object-operation-audit.receipt.v1"
const Finalizer = "dbaas.kubebrain.io/operation-audit"
const ApproverUsername = "system:serviceaccount:kubebrain-operations:kubebrain-operation-approver"

const (
	ReceiptSHAAnnotation  = "dbaas.kubebrain.io/audit-receipt-sha256"
	ArtifactSHAAnnotation = "dbaas.kubebrain.io/audit-artifact-sha256"
	VersionAnnotation     = "dbaas.kubebrain.io/audit-version-id"
	ApprovedByAnnotation  = "dbaas.kubebrain.io/approved-by"
	ApprovalIDAnnotation  = "dbaas.kubebrain.io/approval-id"
)

type Artifact struct {
	Format             string `json:"format"`
	APIVersion         string `json:"api_version"`
	Namespace          string `json:"namespace"`
	Name               string `json:"name"`
	UID                string `json:"uid"`
	Generation         int64  `json:"generation"`
	OperationID        string `json:"operation_id"`
	Instance           string `json:"instance"`
	Type               string `json:"type"`
	ApprovedBy         string `json:"approved_by,omitempty"`
	ApprovalID         string `json:"approval_id,omitempty"`
	ParametersSHA256   string `json:"parameters_sha256"`
	MaxAttempts        int64  `json:"max_attempts"`
	Phase              string `json:"phase"`
	Owner              string `json:"owner"`
	Attempt            int64  `json:"attempt"`
	ObservedGeneration int64  `json:"observed_generation"`
	StartedAtUnix      int64  `json:"started_at_unix"`
	StartedAtUnixNano  int64  `json:"started_at_unix_nano,omitempty"`
	CompletedAtUnix    int64  `json:"completed_at_unix"`
	ReceiptSHA256      string `json:"receipt_sha256,omitempty"`
	Message            string `json:"message"`
}

type Status struct {
	Artifact Artifact
	SHA256   string
	Bytes    int64
}

var approvalIDPattern = regexp.MustCompile(`^[a-z0-9]([-a-z0-9.]{0,126}[a-z0-9])?$`)

type ArchiveReceipt struct {
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

func (a Artifact) Validate() error {
	operationTypeValid := a.Type == "Backup" || a.Type == "BackupDeletion" || a.Type == "RestoreCutover" ||
		a.Type == "PostRestoreAudit" || a.Type == "CertificateRotation" || a.Type == "Destroy"
	if a.Format != Format || a.APIVersion != "dbaas.kubebrain.io/v1alpha1" ||
		a.Namespace == "" || a.Name == "" || a.UID == "" || a.Generation <= 0 ||
		a.OperationID == "" || a.Instance == "" || !operationTypeValid ||
		!validSHA256(a.ParametersSHA256) || a.MaxAttempts <= 0 ||
		(a.Phase != "Succeeded" && a.Phase != "Failed") ||
		a.Owner == "" || a.Attempt <= 0 || a.Attempt > a.MaxAttempts ||
		a.ObservedGeneration <= 0 || a.ObservedGeneration > a.Generation || a.StartedAtUnix <= 0 ||
		a.CompletedAtUnix < a.StartedAtUnix {
		return errors.New("terminal operation audit artifact is incomplete")
	}
	if a.Phase == "Succeeded" && !validSHA256(a.ReceiptSHA256) {
		return errors.New("succeeded operation audit requires a receipt SHA-256")
	}
	if a.Phase == "Failed" && a.ReceiptSHA256 != "" {
		return errors.New("failed operation audit cannot carry a receipt SHA-256")
	}
	approvalRequired := a.Type == "RestoreCutover" || a.Type == "CertificateRotation" ||
		a.Type == "Destroy" || a.Type == "BackupDeletion"
	if approvalRequired &&
		(a.ApprovedBy != ApproverUsername || !approvalIDPattern.MatchString(a.ApprovalID)) {
		return errors.New("high-risk operation audit requires immutable approval evidence")
	}
	if !approvalRequired && (a.ApprovedBy != "" || a.ApprovalID != "") {
		return errors.New("low-risk operation audit cannot carry approval evidence")
	}
	return nil
}

func validSHA256(value string) bool {
	if len(value) != 64 {
		return false
	}
	_, err := hex.DecodeString(value)
	return err == nil
}

func (r ArchiveReceipt) Validate() error {
	if r.Format != ArchiveReceiptFormat || r.OperationID == "" || r.OperationUID == "" ||
		r.Instance == "" || r.OperationType == "" ||
		(r.Phase != "Succeeded" && r.Phase != "Failed") ||
		(r.Phase == "Succeeded" && !validSHA256(r.ExecutionReceiptSHA256)) ||
		(r.Phase == "Failed" && r.ExecutionReceiptSHA256 != "") ||
		r.ObjectStoreID == "" || r.Bucket == "" || r.ObjectKey == "" || r.VersionID == "" ||
		!validSHA256(r.ArtifactSHA256) || r.ObjectBytes <= 0 ||
		(r.RetentionMode != "COMPLIANCE" && r.RetentionMode != "GOVERNANCE") ||
		r.RetainUntilUnix <= r.ArchivedAtUnix || !r.RemoteVerified || r.ArchivedAtUnix <= 0 {
		return errors.New("object operation audit receipt is incomplete")
	}
	return nil
}

func InspectArchiveReceipt(path string) (ArchiveReceipt, string, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return ArchiveReceipt{}, "", err
	}
	var receipt ArchiveReceipt
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&receipt); err != nil {
		return ArchiveReceipt{}, "", fmt.Errorf("decode operation audit receipt: %w", err)
	}
	var trailing any
	if err := decoder.Decode(&trailing); err != io.EOF {
		if err == nil {
			return ArchiveReceipt{}, "", errors.New("operation audit receipt contains trailing JSON")
		}
		return ArchiveReceipt{}, "", fmt.Errorf("decode trailing operation audit receipt data: %w", err)
	}
	canonical, err := json.Marshal(receipt)
	if err != nil {
		return ArchiveReceipt{}, "", err
	}
	canonical = append(canonical, '\n')
	if !bytes.Equal(data, canonical) {
		return ArchiveReceipt{}, "", errors.New("operation audit receipt is not canonical")
	}
	if err := receipt.Validate(); err != nil {
		return ArchiveReceipt{}, "", err
	}
	sum := sha256.Sum256(data)
	return receipt, hex.EncodeToString(sum[:]), nil
}

func (r ArchiveReceipt) Matches(status Status) bool {
	artifact := status.Artifact
	return r.OperationID == artifact.OperationID && r.OperationUID == artifact.UID &&
		r.Instance == artifact.Instance && r.OperationType == artifact.Type &&
		r.Phase == artifact.Phase && r.ExecutionReceiptSHA256 == artifact.ReceiptSHA256 &&
		r.ArtifactSHA256 == status.SHA256 && r.ObjectBytes == status.Bytes
}

func WriteAtomic(path string, artifact Artifact) error {
	if path == "" {
		return errors.New("audit artifact output is empty")
	}
	if err := artifact.Validate(); err != nil {
		return err
	}
	data, err := json.Marshal(artifact)
	if err != nil {
		return err
	}
	data = append(data, '\n')
	if existing, err := os.ReadFile(path); err == nil {
		if bytes.Equal(existing, data) {
			return nil
		}
		return fmt.Errorf("refusing to overwrite existing audit artifact %q", path)
	} else if !errors.Is(err, os.ErrNotExist) {
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
		return err
	}
	directory, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer directory.Close()
	return directory.Sync()
}

func Inspect(path string) (Status, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return Status{}, err
	}
	var artifact Artifact
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&artifact); err != nil {
		return Status{}, fmt.Errorf("decode operation audit artifact: %w", err)
	}
	var trailing any
	if err := decoder.Decode(&trailing); err != io.EOF {
		if err == nil {
			return Status{}, errors.New("operation audit artifact contains trailing JSON")
		}
		return Status{}, fmt.Errorf("decode trailing operation audit data: %w", err)
	}
	canonical, err := json.Marshal(artifact)
	if err != nil {
		return Status{}, err
	}
	canonical = append(canonical, '\n')
	if !bytes.Equal(data, canonical) {
		return Status{}, errors.New("operation audit artifact is not canonical")
	}
	if err := artifact.Validate(); err != nil {
		return Status{}, err
	}
	sum := sha256.Sum256(data)
	return Status{
		Artifact: artifact, SHA256: hex.EncodeToString(sum[:]), Bytes: int64(len(data)),
	}, nil
}
