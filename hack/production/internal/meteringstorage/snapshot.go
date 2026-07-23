package meteringstorage

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

const SnapshotFormat = "kubebrain.object-storage-sample.v1"
const UsageReceiptFormat = "kubebrain.object-usage.receipt.v1"
const maxSnapshotFinalizationDelay = int64(45 * 60)
const maxMeteringStorageJSONBytes = 1 << 20

var identifierPattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9_.-]{0,127}$`)
var digestPattern = regexp.MustCompile(`^[0-9a-f]{64}$`)
var emptyUsageVersionsSHA256 = func() string {
	sum := sha256.Sum256([]byte("[]\n"))
	return hex.EncodeToString(sum[:])
}()

type Snapshot struct {
	Format           string   `json:"format"`
	Instance         string   `json:"instance"`
	SlotStartUnix    int64    `json:"slot_start_unix"`
	SlotEndUnix      int64    `json:"slot_end_unix"`
	ObjectStoreID    string   `json:"object_store_id"`
	Bucket           string   `json:"bucket"`
	Prefix           string   `json:"prefix"`
	AllowedFormats   []string `json:"allowed_formats"`
	RemoteVersions   int      `json:"remote_versions"`
	DeleteMarkers    int      `json:"delete_markers"`
	TotalObjectBytes int64    `json:"total_object_bytes"`
	VersionsSHA256   string   `json:"versions_sha256"`
	CheckedAtUnix    int64    `json:"checked_at_unix"`
}

type SnapshotStatus struct {
	Snapshot Snapshot
	SHA256   string
	Bytes    int64
}

type UsageReceipt struct {
	Format           string   `json:"format"`
	ObjectStoreID    string   `json:"object_store_id"`
	Bucket           string   `json:"bucket"`
	Prefix           string   `json:"prefix"`
	AllowedFormats   []string `json:"allowed_formats"`
	RemoteVersions   int      `json:"remote_versions"`
	DeleteMarkers    int      `json:"delete_markers"`
	TotalObjectBytes int64    `json:"total_object_bytes"`
	VersionsSHA256   string   `json:"versions_sha256"`
	CheckedAtUnix    int64    `json:"checked_at_unix"`
}

func (r UsageReceipt) Validate() error {
	if r.Format != UsageReceiptFormat ||
		r.ObjectStoreID == "" || r.Bucket == "" || r.Prefix == "" ||
		len(r.AllowedFormats) == 0 || len(r.AllowedFormats) > 16 ||
		r.RemoteVersions < 0 || r.DeleteMarkers != 0 || r.TotalObjectBytes < 0 ||
		!digestPattern.MatchString(r.VersionsSHA256) || r.CheckedAtUnix <= 0 {
		return errors.New("object usage receipt is incomplete")
	}
	for i, format := range r.AllowedFormats {
		if format == "" || (i > 0 && format <= r.AllowedFormats[i-1]) {
			return errors.New("object usage receipt formats are not canonical")
		}
	}
	if (r.RemoteVersions == 0) != (r.TotalObjectBytes == 0) {
		return errors.New("object usage receipt version and byte counts are inconsistent")
	}
	if r.RemoteVersions == 0 && r.VersionsSHA256 != emptyUsageVersionsSHA256 {
		return errors.New("empty object usage receipt has invalid versions digest")
	}
	return nil
}

func BuildSnapshot(instance string, slotStart, slotEnd int64, usage UsageReceipt) (Snapshot, error) {
	snapshot := Snapshot{
		Format: SnapshotFormat, Instance: instance, SlotStartUnix: slotStart, SlotEndUnix: slotEnd,
		ObjectStoreID: usage.ObjectStoreID, Bucket: usage.Bucket, Prefix: usage.Prefix,
		AllowedFormats: append([]string(nil), usage.AllowedFormats...),
		RemoteVersions: usage.RemoteVersions, DeleteMarkers: usage.DeleteMarkers,
		TotalObjectBytes: usage.TotalObjectBytes, VersionsSHA256: usage.VersionsSHA256,
		CheckedAtUnix: usage.CheckedAtUnix,
	}
	if err := usage.Validate(); err != nil {
		return Snapshot{}, err
	}
	if err := snapshot.Validate(); err != nil {
		return Snapshot{}, err
	}
	return snapshot, nil
}

func (s Snapshot) Validate() error {
	if s.Format != SnapshotFormat || !identifierPattern.MatchString(s.Instance) ||
		s.SlotStartUnix <= 0 || s.SlotEndUnix-s.SlotStartUnix != 3600 ||
		s.SlotStartUnix%3600 != 0 ||
		s.ObjectStoreID == "" || s.Bucket == "" || s.Prefix == "" ||
		len(s.AllowedFormats) == 0 || len(s.AllowedFormats) > 16 ||
		s.RemoteVersions < 0 || s.DeleteMarkers != 0 || s.TotalObjectBytes < 0 ||
		!digestPattern.MatchString(s.VersionsSHA256) ||
		s.CheckedAtUnix < s.SlotEndUnix ||
		s.CheckedAtUnix-s.SlotEndUnix > maxSnapshotFinalizationDelay {
		return errors.New("object storage sample is incomplete")
	}
	for i, format := range s.AllowedFormats {
		if format == "" || (i > 0 && format <= s.AllowedFormats[i-1]) {
			return errors.New("object storage sample formats are not canonical")
		}
	}
	if (s.RemoteVersions == 0) != (s.TotalObjectBytes == 0) {
		return errors.New("object storage sample version and byte counts are inconsistent")
	}
	if s.RemoteVersions == 0 && s.VersionsSHA256 != emptyUsageVersionsSHA256 {
		return errors.New("empty object storage sample has invalid versions digest")
	}
	return nil
}

func ReadSnapshot(path string) (SnapshotStatus, error) {
	var snapshot Snapshot
	data, err := readBoundedFile(path, "object storage sample", maxMeteringStorageJSONBytes)
	if err != nil {
		return SnapshotStatus{}, err
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&snapshot); err != nil {
		return SnapshotStatus{}, fmt.Errorf("decode object storage sample: %w", err)
	}
	var trailing any
	if err := decoder.Decode(&trailing); err != io.EOF {
		return SnapshotStatus{}, errors.New("object storage sample contains trailing JSON")
	}
	if err := snapshot.Validate(); err != nil {
		return SnapshotStatus{}, err
	}
	canonical, err := json.Marshal(snapshot)
	if err != nil {
		return SnapshotStatus{}, err
	}
	canonical = append(canonical, '\n')
	if !bytes.Equal(data, canonical) {
		return SnapshotStatus{}, errors.New("object storage sample is not canonical")
	}
	sum := sha256.Sum256(data)
	return SnapshotStatus{
		Snapshot: snapshot, SHA256: hex.EncodeToString(sum[:]), Bytes: int64(len(data)),
	}, nil
}

func WriteSnapshotAtomic(path string, snapshot Snapshot) (SnapshotStatus, error) {
	if path == "" {
		return SnapshotStatus{}, errors.New("object storage sample output is empty")
	}
	if err := snapshot.Validate(); err != nil {
		return SnapshotStatus{}, err
	}
	data, err := json.Marshal(snapshot)
	if err != nil {
		return SnapshotStatus{}, err
	}
	data = append(data, '\n')
	if err := ensureMeteringStorageJSONWithinLimit(data, "object storage sample"); err != nil {
		return SnapshotStatus{}, err
	}
	if existing, err := readBoundedFile(path, "existing object storage sample", int64(len(data))); err == nil {
		if bytes.Equal(existing, data) {
			return ReadSnapshot(path)
		}
		return SnapshotStatus{}, fmt.Errorf("refusing to overwrite object storage sample %q", path)
	} else if !errors.Is(err, os.ErrNotExist) {
		return SnapshotStatus{}, err
	}
	dir := filepath.Dir(path)
	temp, err := os.CreateTemp(dir, "."+filepath.Base(path)+".tmp-*")
	if err != nil {
		return SnapshotStatus{}, err
	}
	tempName := temp.Name()
	defer os.Remove(tempName)
	if err := temp.Chmod(0o600); err != nil {
		temp.Close()
		return SnapshotStatus{}, err
	}
	if _, err := temp.Write(data); err != nil {
		temp.Close()
		return SnapshotStatus{}, err
	}
	if err := temp.Sync(); err != nil {
		temp.Close()
		return SnapshotStatus{}, err
	}
	if err := temp.Close(); err != nil {
		return SnapshotStatus{}, err
	}
	if err := os.Link(tempName, path); err != nil {
		return SnapshotStatus{}, err
	}
	directory, err := os.Open(dir)
	if err != nil {
		return SnapshotStatus{}, err
	}
	defer directory.Close()
	if err := directory.Sync(); err != nil {
		return SnapshotStatus{}, err
	}
	return ReadSnapshot(path)
}

func ensureMeteringStorageJSONWithinLimit(data []byte, description string) error {
	if int64(len(data)) > maxMeteringStorageJSONBytes {
		return fmt.Errorf("%s exceeds %d bytes", description, maxMeteringStorageJSONBytes)
	}
	return nil
}

func readBoundedFile(path, description string, limit int64) ([]byte, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	data, err := io.ReadAll(io.LimitReader(file, limit+1))
	if err != nil {
		return nil, err
	}
	if int64(len(data)) > limit {
		return nil, fmt.Errorf("%s exceeds %d bytes", description, limit)
	}
	return data, nil
}
