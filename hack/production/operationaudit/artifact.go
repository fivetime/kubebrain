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
	"path"
	"path/filepath"
	"regexp"
	"strings"
	"unicode"
	"unicode/utf8"
)

const Format = "kubebrain.operation-audit.v1"
const ArchiveReceiptFormat = "kubebrain.object-operation-audit.receipt.v1"
const Finalizer = "dbaas.kubebrain.io/operation-audit"
const ApproverUsername = "system:serviceaccount:kubebrain-operations:kubebrain-operation-approver"
const maxOperationAuditJSONBytes = 1 << 20
const maxOperationIDLength = 128
const maxOperationInstanceLength = 128
const maxOperationNamespaceLength = 63
const maxOperationNameLength = 253
const maxOperationUIDLength = 253
const maxOperationTenantLength = 63
const maxOperationRequesterLength = 253
const maxOperationAttempts = 100
const maxOperationOwnerLength = 253
const maxOperationMessageLength = 4096

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
	Tenant             string `json:"tenant,omitempty"`
	RequestedBy        string `json:"requested_by,omitempty"`
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

func (s Status) valid() bool {
	return s.Artifact.Validate() == nil &&
		validSHA256(s.SHA256) &&
		s.Bytes > 0 &&
		s.Bytes <= maxOperationAuditJSONBytes
}

var approvalIDPattern = regexp.MustCompile(`^[a-z0-9]([-a-z0-9.]{0,126}[a-z0-9])?$`)
var dns1123SubdomainPattern = regexp.MustCompile(`^[a-z0-9]([-a-z0-9]*[a-z0-9])?(\.[a-z0-9]([-a-z0-9]*[a-z0-9])?)*$`)
var operationIdentifierPattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._-]*$`)
var tenantPattern = regexp.MustCompile(`^[a-z0-9]([-a-z0-9]*[a-z0-9])?$`)

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
	if a.Format != Format || a.APIVersion != "dbaas.kubebrain.io/v1alpha1" ||
		!validKubernetesNamespace(a.Namespace) ||
		!validKubernetesName(a.Name) ||
		!validAuditIdentityValue(a.UID, maxOperationUIDLength) || a.Generation <= 0 ||
		!validOperationIdentifier(a.OperationID, maxOperationIDLength) ||
		!validOptionalTenant(a.Tenant) ||
		!validOptionalAuditText(a.RequestedBy, maxOperationRequesterLength) ||
		!validOperationIdentifier(a.Instance, maxOperationInstanceLength) ||
		!validOperationType(a.Type) ||
		!validSHA256(a.ParametersSHA256) || a.MaxAttempts <= 0 ||
		a.MaxAttempts > maxOperationAttempts ||
		(a.Phase != "Succeeded" && a.Phase != "Failed") ||
		!validRequiredAuditText(a.Owner, maxOperationOwnerLength) ||
		a.Attempt <= 0 || a.Attempt > a.MaxAttempts ||
		!validOptionalAuditText(a.Message, maxOperationMessageLength) ||
		a.ObservedGeneration <= 0 || a.ObservedGeneration > a.Generation || a.StartedAtUnix <= 0 ||
		!validStartedAtUnixNano(a.StartedAtUnix, a.StartedAtUnixNano) ||
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

func validOperationType(value string) bool {
	return value == "Backup" || value == "BackupDeletion" || value == "RestoreCutover" ||
		value == "PostRestoreAudit" || value == "CertificateRotation" || value == "Destroy"
}

func validAuditIdentityValue(value string, maxRunes int) bool {
	return validReceiptScopeValue(value) &&
		utf8.RuneCountInString(value) <= maxRunes &&
		!strings.Contains(value, "/") &&
		value != "." && value != ".."
}

func validKubernetesNamespace(value string) bool {
	return len(value) <= maxOperationNamespaceLength &&
		utf8.ValidString(value) &&
		tenantPattern.MatchString(value)
}

func validKubernetesName(value string) bool {
	return len(value) <= maxOperationNameLength &&
		utf8.ValidString(value) &&
		dns1123SubdomainPattern.MatchString(value)
}

func validOperationIdentifier(value string, maxLength int) bool {
	return len(value) <= maxLength &&
		utf8.ValidString(value) &&
		operationIdentifierPattern.MatchString(value)
}

func validOptionalTenant(value string) bool {
	return value == "" ||
		(utf8.ValidString(value) &&
			utf8.RuneCountInString(value) <= maxOperationTenantLength &&
			tenantPattern.MatchString(value))
}

func validRequiredAuditText(value string, maxRunes int) bool {
	return value != "" && validOptionalAuditText(value, maxRunes)
}

func validOptionalAuditText(value string, maxRunes int) bool {
	return utf8.ValidString(value) &&
		utf8.RuneCountInString(value) <= maxRunes &&
		strings.IndexFunc(value, unicode.IsControl) == -1
}

func validStartedAtUnixNano(startedAtUnix, startedAtUnixNano int64) bool {
	return startedAtUnixNano == 0 ||
		(startedAtUnixNano > 0 && startedAtUnixNano/1_000_000_000 == startedAtUnix)
}

func validSHA256(value string) bool {
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

func (r ArchiveReceipt) Validate() error {
	if r.Format != ArchiveReceiptFormat ||
		!validOperationIdentifier(r.OperationID, maxOperationIDLength) ||
		!validAuditIdentityValue(r.OperationUID, maxOperationUIDLength) ||
		!validOperationIdentifier(r.Instance, maxOperationInstanceLength) ||
		!validOperationType(r.OperationType) ||
		(r.Phase != "Succeeded" && r.Phase != "Failed") ||
		(r.Phase == "Succeeded" && !validSHA256(r.ExecutionReceiptSHA256)) ||
		(r.Phase == "Failed" && r.ExecutionReceiptSHA256 != "") ||
		!validReceiptScopeValue(r.ObjectStoreID) || !validReceiptScopeValue(r.Bucket) ||
		!validRelativeObjectKey(r.ObjectKey) ||
		!validReceiptScopeValue(r.VersionID) ||
		!validSHA256(r.ArtifactSHA256) || r.ObjectBytes <= 0 ||
		r.ObjectBytes > maxOperationAuditJSONBytes ||
		(r.RetentionMode != "COMPLIANCE" && r.RetentionMode != "GOVERNANCE") ||
		r.RetainUntilUnix <= r.ArchivedAtUnix || !r.RemoteVerified || r.ArchivedAtUnix <= 0 {
		return errors.New("object operation audit receipt is incomplete")
	}
	return nil
}

func validRelativeObjectKey(key string) bool {
	if !validReceiptScopeValue(key) {
		return false
	}
	if strings.HasPrefix(key, "/") || key == "." || key == ".." || strings.HasPrefix(key, "../") {
		return false
	}
	return path.Clean(key) == key
}

func validReceiptScopeValue(value string) bool {
	if value == "" || !utf8.ValidString(value) {
		return false
	}
	return strings.IndexFunc(value, func(r rune) bool {
		return unicode.IsSpace(r) || unicode.IsControl(r)
	}) == -1
}

func InspectArchiveReceipt(path string) (ArchiveReceipt, string, error) {
	data, err := readBoundedJSONFile(path, "operation audit receipt")
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
	if r.Validate() != nil || !status.valid() {
		return false
	}
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
	if existing, err := readBoundedJSONFile(path, "existing operation audit artifact"); err == nil {
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
	status, _, err := InspectBytes(path)
	return status, err
}

func InspectBytes(path string) (Status, []byte, error) {
	data, err := readBoundedJSONFile(path, "operation audit artifact")
	if err != nil {
		return Status{}, nil, err
	}
	var artifact Artifact
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&artifact); err != nil {
		return Status{}, nil, fmt.Errorf("decode operation audit artifact: %w", err)
	}
	var trailing any
	if err := decoder.Decode(&trailing); err != io.EOF {
		if err == nil {
			return Status{}, nil, errors.New("operation audit artifact contains trailing JSON")
		}
		return Status{}, nil, fmt.Errorf("decode trailing operation audit data: %w", err)
	}
	canonical, err := json.Marshal(artifact)
	if err != nil {
		return Status{}, nil, err
	}
	canonical = append(canonical, '\n')
	if !bytes.Equal(data, canonical) {
		return Status{}, nil, errors.New("operation audit artifact is not canonical")
	}
	if err := artifact.Validate(); err != nil {
		return Status{}, nil, err
	}
	sum := sha256.Sum256(data)
	return Status{
		Artifact: artifact, SHA256: hex.EncodeToString(sum[:]), Bytes: int64(len(data)),
	}, data, nil
}

func readBoundedJSONFile(path, description string) ([]byte, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	data, err := io.ReadAll(io.LimitReader(file, maxOperationAuditJSONBytes+1))
	if err != nil {
		return nil, err
	}
	if len(data) > maxOperationAuditJSONBytes {
		return nil, fmt.Errorf("%s exceeds %d bytes", description, maxOperationAuditJSONBytes)
	}
	return data, nil
}
