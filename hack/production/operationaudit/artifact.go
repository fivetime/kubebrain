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
)

const Format = "kubebrain.operation-audit.v1"

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

func (a Artifact) Validate() error {
	operationTypeValid := a.Type == "Backup" || a.Type == "RestoreCutover" ||
		a.Type == "PostRestoreAudit" || a.Type == "CertificateRotation" || a.Type == "Destroy"
	if a.Format != Format || a.APIVersion != "dbaas.kubebrain.io/v1alpha1" ||
		a.Namespace == "" || a.Name == "" || a.UID == "" || a.Generation <= 0 ||
		a.OperationID == "" || a.Instance == "" || !operationTypeValid ||
		!validSHA256(a.ParametersSHA256) || a.MaxAttempts <= 0 ||
		(a.Phase != "Succeeded" && a.Phase != "Failed") ||
		a.Owner == "" || a.Attempt <= 0 || a.Attempt > a.MaxAttempts ||
		a.ObservedGeneration != a.Generation || a.StartedAtUnix <= 0 ||
		a.CompletedAtUnix < a.StartedAtUnix {
		return errors.New("terminal operation audit artifact is incomplete")
	}
	if a.Phase == "Succeeded" && !validSHA256(a.ReceiptSHA256) {
		return errors.New("succeeded operation audit requires a receipt SHA-256")
	}
	if a.Phase == "Failed" && a.ReceiptSHA256 != "" {
		return errors.New("failed operation audit cannot carry a receipt SHA-256")
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
