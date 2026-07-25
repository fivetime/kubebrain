package meteringstorage

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path"
	"strconv"
	"strings"
	"time"
)

type Roller struct {
	Instance                string
	Executor                string
	ObjectStoreID           string
	Bucket                  string
	SnapshotPrefix          string
	RollupPrefix            string
	RetentionMode           string
	RetentionDuration       time.Duration
	FinalizationDelay       time.Duration
	SampleFinalizationDelay time.Duration
	PeriodEnd               time.Time
	Now                     func() time.Time
	Run                     CommandRunner
}

func (r *Roller) Validate() error {
	r.SnapshotPrefix = strings.Trim(r.SnapshotPrefix, "/")
	r.RollupPrefix = strings.Trim(r.RollupPrefix, "/")
	if !identifierPattern.MatchString(r.Instance) || r.Executor == "" ||
		r.ObjectStoreID == "" || r.Bucket == "" || r.SnapshotPrefix == "" ||
		r.RollupPrefix == "" || r.SnapshotPrefix == r.RollupPrefix ||
		(r.RetentionMode != "COMPLIANCE" && r.RetentionMode != "GOVERNANCE") ||
		r.RetentionDuration <= 24*time.Hour || r.FinalizationDelay < 0 ||
		r.SampleFinalizationDelay <= 0 || r.SampleFinalizationDelay >= time.Hour {
		return errors.New("object storage roller configuration is incomplete")
	}
	if !validObjectScopeValue(r.ObjectStoreID) || !validObjectScopeValue(r.Bucket) ||
		!validRelativeObjectPrefix(r.SnapshotPrefix) || !validRelativeObjectPrefix(r.RollupPrefix) {
		return errors.New("object storage roller object identity is invalid")
	}
	if r.Now == nil {
		r.Now = time.Now
	}
	if r.Run == nil {
		r.Run = runCommand
	}
	return nil
}

func (r *Roller) Process(ctx context.Context) (Rollup, []byte, error) {
	if err := r.Validate(); err != nil {
		return Rollup{}, nil, err
	}
	now := r.Now().UTC()
	eligibleEnd := now.Add(-r.FinalizationDelay).Truncate(24 * time.Hour)
	periodEnd := eligibleEnd
	if !r.PeriodEnd.IsZero() {
		periodEnd = r.PeriodEnd.UTC()
		if periodEnd.Unix()%86400 != 0 || periodEnd.After(eligibleEnd) {
			return Rollup{}, nil, errors.New("explicit object storage period end is not eligible")
		}
	}
	periodStart := periodEnd.Add(-24 * time.Hour)
	if periodStart.Unix() <= 0 {
		return Rollup{}, nil, errors.New("object storage rollup period is before the Unix epoch")
	}
	dir, err := os.MkdirTemp("", "kubebrain-object-storage-rollup-*")
	if err != nil {
		return Rollup{}, nil, err
	}
	defer os.RemoveAll(dir)
	inputs := make([]VerifiedSnapshot, 0, 24)
	for i := 0; i < 24; i++ {
		slotStart := periodStart.Add(time.Duration(i) * time.Hour)
		slotEnd := slotStart.Add(time.Hour)
		artifactID := periodArtifactID(r.Instance, slotStart, slotEnd)
		objectKey := snapshotObjectKey(r.SnapshotPrefix, r.Instance, slotStart, slotEnd)
		outputPath := path.Join(dir, fmt.Sprintf("sample-%02d.json", i))
		minRetainUntil := slotEnd.Add(r.RetentionDuration).Unix()
		output, err := r.Run(ctx, r.Executor, []string{
			"ACTION=blob-read", "OUTPUT=" + outputPath,
			"ARTIFACT_FORMAT=" + SnapshotFormat, "ARTIFACT_ID=" + artifactID,
			"INSTANCE=" + r.Instance, "OBJECT_STORE_ID=" + r.ObjectStoreID,
			"S3_BUCKET=" + r.Bucket, "S3_OBJECT_KEY=" + objectKey,
			"MIN_RETAIN_UNTIL_UNIX=" + strconv.FormatInt(minRetainUntil, 10),
		})
		if err != nil {
			return Rollup{}, output, fmt.Errorf(
				"read object storage sample %d: %w: %s", i, err, strings.TrimSpace(string(output)),
			)
		}
		receipt, err := parseBlobReadReceipt(output, SnapshotFormat, artifactID, r.Instance,
			r.ObjectStoreID, r.Bucket, objectKey, minRetainUntil)
		if err != nil {
			return Rollup{}, output, err
		}
		status, err := ReadSnapshot(outputPath)
		if err != nil {
			return Rollup{}, output, fmt.Errorf("validate object storage sample %d: %w", i, err)
		}
		if status.Snapshot.CheckedAtUnix-status.Snapshot.SlotEndUnix >
			int64(r.SampleFinalizationDelay/time.Second) {
			return Rollup{}, output, fmt.Errorf(
				"object storage sample %d exceeds configured finalization window", i,
			)
		}
		if status.Bytes != receipt.ObjectBytes || status.SHA256 != receipt.ArtifactSHA256 {
			return Rollup{}, output, errors.New("object storage sample receipt does not match bytes")
		}
		inputs = append(inputs, VerifiedSnapshot{
			Snapshot: status.Snapshot,
			Source: SnapshotSource{
				SlotStartUnix: slotStart.Unix(), SlotEndUnix: slotEnd.Unix(),
				ObjectKey: objectKey, VersionID: receipt.VersionID,
				ArtifactSHA256: receipt.ArtifactSHA256, ObjectBytes: receipt.ObjectBytes,
				RetainUntilUnix: receipt.RetainUntilUnix,
			},
		})
	}
	rollup, err := BuildRollup(r.Instance, periodStart, periodEnd, inputs)
	if err != nil {
		return Rollup{}, nil, err
	}
	rollupPath := path.Join(dir, "rollup.json")
	receiptPath := path.Join(dir, "receipt.json")
	status, err := WriteRollupAtomic(rollupPath, rollup)
	if err != nil {
		return Rollup{}, nil, err
	}
	artifactID := periodArtifactID(r.Instance, periodStart, periodEnd)
	objectKey := path.Join(r.RollupPrefix, r.Instance, periodStart.Format("2006/01/02"),
		fmt.Sprintf("%d-%d.json", periodStart.Unix(), periodEnd.Unix()))
	retainUntil := periodStart.Add(time.Hour).Add(r.RetentionDuration).Unix()
	if retainUntil <= now.Unix() {
		return Rollup{}, nil, errors.New("object storage rollup retention is not in the future")
	}
	output, err := r.Run(ctx, r.Executor, []string{
		"ACTION=blob", "INPUT=" + rollupPath, "ARTIFACT_FORMAT=" + RollupFormat,
		"ARTIFACT_ID=" + artifactID, "INSTANCE=" + r.Instance,
		"OBJECT_STORE_ID=" + r.ObjectStoreID, "S3_BUCKET=" + r.Bucket,
		"S3_OBJECT_KEY=" + objectKey, "RETENTION_MODE=" + r.RetentionMode,
		"RETAIN_UNTIL_UNIX=" + strconv.FormatInt(retainUntil, 10),
		"RECEIPT_OUTPUT=" + receiptPath,
	})
	if err != nil {
		return Rollup{}, output, fmt.Errorf(
			"archive object storage rollup: %w: %s", err, strings.TrimSpace(string(output)),
		)
	}
	if err := validateRollupBlobReceipt(output, artifactID, r.Instance,
		r.ObjectStoreID, r.Bucket, objectKey, r.RetentionMode, retainUntil, status); err != nil {
		return Rollup{}, output, err
	}
	return rollup, output, nil
}

func validateRollupBlobReceipt(data []byte, artifactID, instance, store, bucket, key string,
	retentionMode string, retainUntil int64, status RollupStatus) error {
	var receipt blobReceipt
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&receipt); err != nil {
		return err
	}
	var trailing any
	if err := decoder.Decode(&trailing); err != io.EOF {
		return errors.New("object storage rollup archive receipt contains trailing JSON")
	}
	if receipt.Format != "kubebrain.object-immutable-blob.receipt.v1" ||
		receipt.ArtifactFormat != RollupFormat || receipt.ArtifactID != artifactID ||
		receipt.Instance != instance || receipt.ObjectStoreID != store ||
		receipt.Bucket != bucket || receipt.ObjectKey != key || receipt.VersionID == "" ||
		receipt.ArtifactSHA256 != status.SHA256 || receipt.ObjectBytes != status.Bytes ||
		receipt.RetentionMode != retentionMode || receipt.RetainUntilUnix != retainUntil || !receipt.RemoteVerified ||
		receipt.ArchivedAtUnix <= 0 || receipt.ArchivedAtUnix >= retainUntil {
		return errors.New("object storage rollup archive receipt does not match artifact")
	}
	return nil
}

type blobReadReceipt struct {
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

func parseBlobReadReceipt(
	data []byte,
	format, artifactID, instance, store, bucket, key string,
	minRetainUntil int64,
) (blobReadReceipt, error) {
	var receipt blobReadReceipt
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&receipt); err != nil {
		return receipt, err
	}
	var trailing any
	if err := decoder.Decode(&trailing); err != io.EOF {
		return receipt, errors.New("object storage sample read receipt contains trailing JSON")
	}
	if receipt.Format != "kubebrain.object-immutable-blob-read.receipt.v1" ||
		receipt.ArtifactFormat != format || receipt.ArtifactID != artifactID ||
		receipt.Instance != instance || receipt.ObjectStoreID != store ||
		receipt.Bucket != bucket || receipt.ObjectKey != key || receipt.VersionID == "" ||
		!digestPattern.MatchString(receipt.ArtifactSHA256) || receipt.ObjectBytes <= 0 ||
		(receipt.RetentionMode != "COMPLIANCE" && receipt.RetentionMode != "GOVERNANCE") ||
		receipt.RetainUntilUnix < minRetainUntil || !receipt.RemoteVerified {
		return receipt, errors.New("object storage sample read receipt does not match request")
	}
	return receipt, nil
}

func periodArtifactID(instance string, start, end time.Time) string {
	return fmt.Sprintf("%s:%d:%d", instance, start.Unix(), end.Unix())
}

func snapshotObjectKey(prefix, instance string, start, end time.Time) string {
	return path.Join(prefix, instance, start.Format("2006/01/02/15"),
		fmt.Sprintf("%d-%d.json", start.Unix(), end.Unix()))
}
