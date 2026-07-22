package meteringarchive

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
	Instance          string
	Executor          string
	ObjectStoreID     string
	Bucket            string
	SamplePrefix      string
	RollupPrefix      string
	RetentionMode     string
	RetentionDuration time.Duration
	PeriodDuration    time.Duration
	FinalizationDelay time.Duration
	SlotDuration      time.Duration
	MaxStaleness      time.Duration
	PeriodEnd         time.Time
	Now               func() time.Time
	Run               CommandRunner
}

func (r *Roller) Validate() error {
	r.SamplePrefix = strings.Trim(r.SamplePrefix, "/")
	r.RollupPrefix = strings.Trim(r.RollupPrefix, "/")
	if !instancePattern.MatchString(r.Instance) || r.Executor == "" || r.ObjectStoreID == "" ||
		r.Bucket == "" || r.SamplePrefix == "" || r.RollupPrefix == "" ||
		r.SamplePrefix == r.RollupPrefix ||
		(r.RetentionMode != "COMPLIANCE" && r.RetentionMode != "GOVERNANCE") ||
		r.RetentionDuration <= 0 || r.PeriodDuration != 24*time.Hour ||
		r.SlotDuration < time.Minute || r.PeriodDuration%r.SlotDuration != 0 ||
		r.FinalizationDelay < 0 || r.MaxStaleness <= 0 {
		return errors.New("metering roller configuration is incomplete")
	}
	if r.RetentionDuration <= r.FinalizationDelay+r.PeriodDuration {
		return errors.New("metering retention must exceed the period and finalization delay")
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
	eligibleEnd := now.Add(-r.FinalizationDelay).Truncate(r.PeriodDuration)
	periodEnd := eligibleEnd
	if !r.PeriodEnd.IsZero() {
		periodEnd = r.PeriodEnd.UTC()
		if periodEnd.Unix()%int64(r.PeriodDuration/time.Second) != 0 ||
			periodEnd.After(eligibleEnd) {
			return Rollup{}, nil, errors.New("explicit metering period end is not eligible")
		}
	}
	periodStart := periodEnd.Add(-r.PeriodDuration)
	if periodStart.Unix() <= 0 {
		return Rollup{}, nil, errors.New("metering rollup period is before the Unix epoch")
	}
	dir, err := os.MkdirTemp("", "kubebrain-metering-rollup-*")
	if err != nil {
		return Rollup{}, nil, err
	}
	defer os.RemoveAll(dir)
	count := int(r.PeriodDuration / r.SlotDuration)
	inputs := make([]VerifiedSample, 0, count)
	for i := 0; i < count; i++ {
		slotStart := periodStart.Add(time.Duration(i) * r.SlotDuration)
		slotEnd := slotStart.Add(r.SlotDuration)
		artifactID := sampleArtifactID(r.Instance, slotStart, slotEnd)
		objectKey := sampleObjectKey(r.SamplePrefix, r.Instance, slotStart, slotEnd)
		outputPath := path.Join(dir, fmt.Sprintf("sample-%03d.json", i))
		minRetainUntil := slotEnd.Add(r.RetentionDuration).Unix()
		output, err := r.Run(ctx, r.Executor, []string{
			"ACTION=blob-read",
			"OUTPUT=" + outputPath,
			`ARTIFACT_FORMATS_JSON=["` + LegacyFormat + `","` + Format + `","` + FormatV3 + `"]`,
			"ARTIFACT_ID=" + artifactID,
			"INSTANCE=" + r.Instance,
			"OBJECT_STORE_ID=" + r.ObjectStoreID,
			"S3_BUCKET=" + r.Bucket,
			"S3_OBJECT_KEY=" + objectKey,
			"MIN_RETAIN_UNTIL_UNIX=" + strconv.FormatInt(minRetainUntil, 10),
		})
		if err != nil {
			return Rollup{}, output, fmt.Errorf(
				"read metering sample %d: %w: %s", i, err, strings.TrimSpace(string(output)),
			)
		}
		receipt, err := parseBlobReadReceipt(
			output, artifactID, r.Instance, r.ObjectStoreID, r.Bucket, objectKey,
			minRetainUntil,
		)
		if err != nil {
			return Rollup{}, output, err
		}
		status, err := ReadSampleStatus(outputPath, r.MaxStaleness)
		if err != nil {
			return Rollup{}, output, fmt.Errorf("validate metering sample %d: %w", i, err)
		}
		if status.Bytes != receipt.ObjectBytes || status.SHA256 != receipt.ArtifactSHA256 {
			return Rollup{}, output, errors.New("metering sample read receipt does not match downloaded bytes")
		}
		inputs = append(inputs, VerifiedSample{
			Sample: status.Sample,
			Source: SampleSource{
				SlotStartUnix: status.Sample.SlotStartUnix, SlotEndUnix: status.Sample.SlotEndUnix,
				ArtifactFormat: receipt.ArtifactFormat,
				ObjectKey:      objectKey, VersionID: receipt.VersionID,
				ArtifactSHA256: receipt.ArtifactSHA256, ObjectBytes: receipt.ObjectBytes,
				RetainUntilUnix: receipt.RetainUntilUnix,
			},
		})
	}
	rollup, err := BuildRollup(
		r.Instance, periodStart, periodEnd, r.SlotDuration, r.MaxStaleness, inputs,
	)
	if err != nil {
		return Rollup{}, nil, err
	}
	rollupPath := path.Join(dir, "rollup.json")
	receiptPath := path.Join(dir, "receipt.json")
	status, err := WriteRollupAtomic(rollupPath, rollup)
	if err != nil {
		return Rollup{}, nil, err
	}
	artifactID := fmt.Sprintf("%s:%d:%d", r.Instance, periodStart.Unix(), periodEnd.Unix())
	objectKey := path.Join(
		r.RollupPrefix, r.Instance, periodStart.Format("2006/01/02"),
		fmt.Sprintf("%d-%d.json", periodStart.Unix(), periodEnd.Unix()),
	)
	retainUntil := periodStart.Add(r.SlotDuration).Add(r.RetentionDuration).Unix()
	if retainUntil <= now.Unix() {
		return Rollup{}, nil, errors.New("metering rollup retention deadline is not in the future")
	}
	output, err := r.Run(ctx, r.Executor, []string{
		"ACTION=blob",
		"INPUT=" + rollupPath,
		"ARTIFACT_FORMAT=" + rollup.Format,
		"ARTIFACT_ID=" + artifactID,
		"INSTANCE=" + r.Instance,
		"OBJECT_STORE_ID=" + r.ObjectStoreID,
		"S3_BUCKET=" + r.Bucket,
		"S3_OBJECT_KEY=" + objectKey,
		"RETENTION_MODE=" + r.RetentionMode,
		"RETAIN_UNTIL_UNIX=" + strconv.FormatInt(retainUntil, 10),
		"RECEIPT_OUTPUT=" + receiptPath,
	})
	if err != nil {
		return Rollup{}, output, fmt.Errorf(
			"metering rollup Object Lock executor failed: %w: %s",
			err, strings.TrimSpace(string(output)),
		)
	}
	if err := validateRollupExecutorReceipt(
		output, artifactID, r.Instance, r.ObjectStoreID, r.Bucket, objectKey,
		r.RetentionMode, retainUntil, status,
	); err != nil {
		return Rollup{}, output, err
	}
	return rollup, output, nil
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
	artifactID, instance, objectStoreID, bucket, objectKey string,
	minRetainUntil int64,
) (blobReadReceipt, error) {
	var receipt blobReadReceipt
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&receipt); err != nil {
		return receipt, fmt.Errorf("decode metering sample read receipt: %w", err)
	}
	var trailing any
	if err := decoder.Decode(&trailing); err != io.EOF {
		return receipt, errors.New("metering sample read receipt contains trailing JSON")
	}
	if receipt.Format != "kubebrain.object-immutable-blob-read.receipt.v1" ||
		(receipt.ArtifactFormat != FormatV3 && receipt.ArtifactFormat != Format &&
			receipt.ArtifactFormat != LegacyFormat) ||
		receipt.ArtifactID != artifactID ||
		receipt.Instance != instance || receipt.ObjectStoreID != objectStoreID ||
		receipt.Bucket != bucket || receipt.ObjectKey != objectKey ||
		receipt.VersionID == "" || !digestPattern.MatchString(receipt.ArtifactSHA256) ||
		receipt.ObjectBytes <= 0 ||
		(receipt.RetentionMode != "COMPLIANCE" && receipt.RetentionMode != "GOVERNANCE") ||
		receipt.RetainUntilUnix < minRetainUntil || !receipt.RemoteVerified {
		return receipt, errors.New("metering sample read receipt does not match the request")
	}
	return receipt, nil
}

func validateRollupExecutorReceipt(
	data []byte,
	artifactID, instance, objectStoreID, bucket, objectKey, retentionMode string,
	retainUntil int64,
	status RollupStatus,
) error {
	return validateExecutorReceipt(
		data, status.Rollup.Format, artifactID, instance, objectStoreID, bucket, objectKey,
		retentionMode, retainUntil, status.SHA256, status.Bytes,
	)
}

func sampleArtifactID(instance string, slotStart, slotEnd time.Time) string {
	return fmt.Sprintf("%s:%d:%d", instance, slotStart.Unix(), slotEnd.Unix())
}

func sampleObjectKey(prefix, instance string, slotStart, slotEnd time.Time) string {
	return path.Join(
		prefix, instance, slotEnd.UTC().Format("2006/01/02"),
		fmt.Sprintf("%d-%d.json", slotStart.Unix(), slotEnd.Unix()),
	)
}
