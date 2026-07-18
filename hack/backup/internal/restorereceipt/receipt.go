package restorereceipt

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
)

const Format = "kubebrain.restore-verification.v1"

type Receipt struct {
	Format                string `json:"format"`
	ArtifactFormat        string `json:"artifact_format"`
	ArtifactSHA256        string `json:"artifact_sha256"`
	SnapshotRevision      int64  `json:"snapshot_revision"`
	ArtifactCreatedAtUnix int64  `json:"artifact_created_at_unix,omitempty"`
	SourcePrefix          string `json:"source_prefix"`
	TargetPrefix          string `json:"target_prefix"`
	Records               int    `json:"records"`
	ArtifactLeases        int    `json:"artifact_leases"`
	VerifiedTargetLeases  int    `json:"verified_target_leases"`
	VerifiedAtUnix        int64  `json:"verified_at_unix"`
}

func WriteAtomic(path string, receipt Receipt) error {
	if path == "" {
		return errors.New("receipt output path is empty")
	}
	if receipt.Format != Format || receipt.ArtifactFormat == "" || receipt.ArtifactSHA256 == "" ||
		receipt.SnapshotRevision <= 0 || receipt.SourcePrefix == "" || receipt.TargetPrefix == "" ||
		receipt.Records < 0 || receipt.ArtifactLeases < 0 || receipt.VerifiedTargetLeases < 0 ||
		receipt.VerifiedAtUnix <= 0 {
		return errors.New("restore verification receipt is incomplete")
	}

	dir := filepath.Dir(path)
	temp, err := os.CreateTemp(dir, "."+filepath.Base(path)+".tmp-*")
	if err != nil {
		return err
	}
	cleanup := func() {
		_ = temp.Close()
		_ = os.Remove(temp.Name())
	}
	if err := temp.Chmod(0o600); err != nil {
		cleanup()
		return err
	}
	encoder := json.NewEncoder(temp)
	encoder.SetEscapeHTML(false)
	if err := encoder.Encode(receipt); err != nil {
		cleanup()
		return err
	}
	if err := temp.Sync(); err != nil {
		cleanup()
		return err
	}
	if err := temp.Close(); err != nil {
		_ = os.Remove(temp.Name())
		return err
	}
	if err := os.Link(temp.Name(), path); err != nil {
		_ = os.Remove(temp.Name())
		return err
	}
	if err := os.Remove(temp.Name()); err != nil {
		return err
	}
	directory, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer directory.Close()
	return directory.Sync()
}
