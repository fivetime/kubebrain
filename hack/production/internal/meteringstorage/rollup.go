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
	"time"
)

const RollupFormat = "kubebrain.object-storage-rollup.v1"

type SnapshotSource struct {
	SlotStartUnix   int64  `json:"slot_start_unix"`
	SlotEndUnix     int64  `json:"slot_end_unix"`
	ObjectKey       string `json:"object_key"`
	VersionID       string `json:"version_id"`
	ArtifactSHA256  string `json:"artifact_sha256"`
	ObjectBytes     int64  `json:"object_bytes"`
	RetainUntilUnix int64  `json:"retain_until_unix"`
}

type VerifiedSnapshot struct {
	Snapshot Snapshot
	Source   SnapshotSource
}

type Rollup struct {
	Format                   string           `json:"format"`
	Instance                 string           `json:"instance"`
	PeriodStartUnix          int64            `json:"period_start_unix"`
	PeriodEndUnix            int64            `json:"period_end_unix"`
	SlotSeconds              int64            `json:"slot_seconds"`
	Complete                 bool             `json:"complete"`
	ObjectStoreID            string           `json:"object_store_id"`
	Bucket                   string           `json:"bucket"`
	Prefix                   string           `json:"prefix"`
	AllowedFormats           []string         `json:"allowed_formats"`
	Sources                  []SnapshotSource `json:"sources"`
	ObjectStorageByteSeconds int64            `json:"object_storage_byte_seconds"`
}

type RollupStatus struct {
	Rollup Rollup
	SHA256 string
	Bytes  int64
}

func BuildRollup(instance string, periodStart, periodEnd time.Time, inputs []VerifiedSnapshot) (Rollup, error) {
	periodStart, periodEnd = periodStart.UTC(), periodEnd.UTC()
	if !identifierPattern.MatchString(instance) || periodEnd.Sub(periodStart) != 24*time.Hour ||
		periodStart.Unix()%86400 != 0 || len(inputs) != 24 {
		return Rollup{}, errors.New("object storage rollup period is invalid")
	}
	sources := make([]SnapshotSource, len(inputs))
	var total int64
	var first Snapshot
	for i, input := range inputs {
		snapshot, source := input.Snapshot, input.Source
		expectedStart := periodStart.Add(time.Duration(i) * time.Hour).Unix()
		if err := snapshot.Validate(); err != nil {
			return Rollup{}, fmt.Errorf("validate object storage sample %d: %w", i, err)
		}
		if snapshot.Instance != instance || snapshot.SlotStartUnix != expectedStart ||
			snapshot.SlotEndUnix != expectedStart+3600 ||
			source.SlotStartUnix != expectedStart || source.SlotEndUnix != expectedStart+3600 ||
			source.ObjectKey == "" || source.VersionID == "" ||
			!digestPattern.MatchString(source.ArtifactSHA256) || source.ObjectBytes <= 0 ||
			source.RetainUntilUnix < periodEnd.Unix() {
			return Rollup{}, fmt.Errorf("object storage sample %d source does not match period", i)
		}
		if i == 0 {
			first = snapshot
		} else if snapshot.ObjectStoreID != first.ObjectStoreID ||
			snapshot.Bucket != first.Bucket || snapshot.Prefix != first.Prefix ||
			!equalStrings(snapshot.AllowedFormats, first.AllowedFormats) {
			return Rollup{}, errors.New("object storage sample scope changed within period")
		}
		if snapshot.TotalObjectBytes > int64(^uint64(0)>>1)/3600 {
			return Rollup{}, errors.New("object storage byte-seconds overflow int64")
		}
		value := snapshot.TotalObjectBytes * 3600
		if value > int64(^uint64(0)>>1)-total {
			return Rollup{}, errors.New("object storage byte-seconds overflow int64")
		}
		total += value
		sources[i] = source
	}
	rollup := Rollup{
		Format: RollupFormat, Instance: instance,
		PeriodStartUnix: periodStart.Unix(), PeriodEndUnix: periodEnd.Unix(),
		SlotSeconds: 3600, Complete: true, ObjectStoreID: first.ObjectStoreID,
		Bucket: first.Bucket, Prefix: first.Prefix,
		AllowedFormats: append([]string(nil), first.AllowedFormats...),
		Sources:        sources, ObjectStorageByteSeconds: total,
	}
	if err := rollup.Validate(); err != nil {
		return Rollup{}, err
	}
	return rollup, nil
}

func (r Rollup) Validate() error {
	if r.Format != RollupFormat || !identifierPattern.MatchString(r.Instance) ||
		r.PeriodStartUnix <= 0 || r.PeriodEndUnix-r.PeriodStartUnix != 86400 ||
		r.PeriodStartUnix%86400 != 0 || r.SlotSeconds != 3600 || !r.Complete ||
		r.ObjectStoreID == "" || r.Bucket == "" || r.Prefix == "" ||
		!formatsSortedUnique(r.AllowedFormats) || len(r.Sources) != 24 ||
		r.ObjectStorageByteSeconds < 0 {
		return errors.New("object storage rollup is incomplete")
	}
	for i, source := range r.Sources {
		expectedStart := r.PeriodStartUnix + int64(i)*3600
		if source.SlotStartUnix != expectedStart || source.SlotEndUnix != expectedStart+3600 ||
			source.ObjectKey == "" || source.VersionID == "" ||
			!digestPattern.MatchString(source.ArtifactSHA256) || source.ObjectBytes <= 0 ||
			source.RetainUntilUnix < r.PeriodEndUnix {
			return errors.New("object storage rollup contains an invalid source")
		}
	}
	return nil
}

func WriteRollupAtomic(path string, rollup Rollup) (RollupStatus, error) {
	if path == "" {
		return RollupStatus{}, errors.New("object storage rollup output is empty")
	}
	if err := rollup.Validate(); err != nil {
		return RollupStatus{}, err
	}
	data, err := json.Marshal(rollup)
	if err != nil {
		return RollupStatus{}, err
	}
	data = append(data, '\n')
	if existing, err := os.ReadFile(path); err == nil {
		if bytes.Equal(existing, data) {
			return ReadRollup(path)
		}
		return RollupStatus{}, fmt.Errorf("refusing to overwrite object storage rollup %q", path)
	} else if !errors.Is(err, os.ErrNotExist) {
		return RollupStatus{}, err
	}
	dir := filepath.Dir(path)
	temp, err := os.CreateTemp(dir, "."+filepath.Base(path)+".tmp-*")
	if err != nil {
		return RollupStatus{}, err
	}
	tempName := temp.Name()
	defer os.Remove(tempName)
	if err := temp.Chmod(0o600); err != nil {
		temp.Close()
		return RollupStatus{}, err
	}
	if _, err := temp.Write(data); err != nil {
		temp.Close()
		return RollupStatus{}, err
	}
	if err := temp.Sync(); err != nil {
		temp.Close()
		return RollupStatus{}, err
	}
	if err := temp.Close(); err != nil {
		return RollupStatus{}, err
	}
	if err := os.Link(tempName, path); err != nil {
		return RollupStatus{}, err
	}
	directory, err := os.Open(dir)
	if err != nil {
		return RollupStatus{}, err
	}
	defer directory.Close()
	if err := directory.Sync(); err != nil {
		return RollupStatus{}, err
	}
	return ReadRollup(path)
}

func ReadRollup(path string) (RollupStatus, error) {
	var rollup Rollup
	data, err := os.ReadFile(path)
	if err != nil {
		return RollupStatus{}, err
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&rollup); err != nil {
		return RollupStatus{}, err
	}
	var trailing any
	if err := decoder.Decode(&trailing); err != io.EOF {
		return RollupStatus{}, errors.New("object storage rollup contains trailing JSON")
	}
	if err := rollup.Validate(); err != nil {
		return RollupStatus{}, err
	}
	canonical, err := json.Marshal(rollup)
	if err != nil {
		return RollupStatus{}, err
	}
	canonical = append(canonical, '\n')
	if !bytes.Equal(data, canonical) {
		return RollupStatus{}, errors.New("object storage rollup is not canonical")
	}
	sum := sha256.Sum256(data)
	return RollupStatus{
		Rollup: rollup, SHA256: hex.EncodeToString(sum[:]), Bytes: int64(len(data)),
	}, nil
}

func formatsSortedUnique(values []string) bool {
	if len(values) == 0 {
		return false
	}
	for i, value := range values {
		if value == "" || (i > 0 && value <= values[i-1]) {
			return false
		}
	}
	return true
}
